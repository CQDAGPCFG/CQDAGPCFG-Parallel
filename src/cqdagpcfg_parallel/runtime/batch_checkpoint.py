from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import time
from typing import Mapping

from .batch_ledger import BatchRetryLedger, BatchState
from .batch_transport import BinaryCandidateBatchCodec
from .candidate_batch import CandidateBatch


@dataclass(frozen=True, slots=True)
class DurableBatchCheckpoint:
    """Durable restart point for the CandidateBatch delivery layer.

    The tracker checkpoint says which logical candidates have been emitted.
    This batch checkpoint says which emitted batches have not been acknowledged
    yet and keeps only those payloads for replay.
    """

    next_batch_id: int
    next_start_rank: int
    ledger: BatchRetryLedger
    inflight_batches: Mapping[int, CandidateBatch]
    created_at: float = field(default_factory=time)

    def __post_init__(self) -> None:
        if self.next_batch_id < 0:
            raise ValueError("next_batch_id cannot be negative")
        if self.next_start_rank < 0:
            raise ValueError("next_start_rank cannot be negative")
        ledger_batch_ids = {entry.batch_id for entry in self.ledger.entries()}
        for batch_id, batch in self.inflight_batches.items():
            if batch_id != batch.batch_id:
                raise ValueError("inflight batch key does not match batch_id")
            if batch_id not in ledger_batch_ids:
                raise ValueError("inflight batch is missing from ledger")
            entry = self.ledger.entry(batch_id)
            if entry is not None and entry.state == BatchState.DONE:
                raise ValueError("DONE batch cannot be stored as inflight payload")

    @classmethod
    def create(
        cls,
        *,
        next_batch_id: int,
        next_start_rank: int,
        ledger: BatchRetryLedger,
        inflight_batches: Mapping[int, CandidateBatch],
    ) -> "DurableBatchCheckpoint":
        return cls(
            next_batch_id=next_batch_id,
            next_start_rank=next_start_rank,
            ledger=BatchRetryLedger.from_dict(ledger.to_dict()),
            inflight_batches=dict(inflight_batches),
        )

    def pending_batches(self) -> tuple[CandidateBatch, ...]:
        return tuple(
            batch
            for batch_id, batch in sorted(self.inflight_batches.items())
            if (entry := self.ledger.entry(batch_id)) is not None
            and entry.state != BatchState.DONE
        )

    def to_dict(self) -> dict:
        return {
            "schema_version": 1,
            "next_batch_id": self.next_batch_id,
            "next_start_rank": self.next_start_rank,
            "ledger": self.ledger.to_dict(),
            "inflight_batches": {
                str(batch_id): base64.b64encode(
                    BinaryCandidateBatchCodec.dumps(batch)
                ).decode("ascii")
                for batch_id, batch in sorted(self.inflight_batches.items())
            },
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: Mapping) -> "DurableBatchCheckpoint":
        if int(payload.get("schema_version", 1)) != 1:
            raise ValueError("unsupported durable batch checkpoint schema version")
        ledger = BatchRetryLedger.from_dict(dict(payload["ledger"]))
        inflight_batches = {
            int(batch_id): BinaryCandidateBatchCodec.loads(base64.b64decode(encoded))
            for batch_id, encoded in dict(payload.get("inflight_batches", {})).items()
        }
        return cls(
            next_batch_id=int(payload["next_batch_id"]),
            next_start_rank=int(payload["next_start_rank"]),
            ledger=ledger,
            inflight_batches=inflight_batches,
            created_at=float(payload.get("created_at", 0.0)),
        )

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, payload: str | bytes) -> "DurableBatchCheckpoint":
        data = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        return cls.from_dict(json.loads(data))

    def write_atomic(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            handle.write(self.to_json())
            handle.write("\n")
        temp_path.replace(path)

    @classmethod
    def read(cls, path: Path) -> "DurableBatchCheckpoint":
        return cls.from_json(path.read_text(encoding="utf-8"))


__all__ = [
    "DurableBatchCheckpoint",
]
