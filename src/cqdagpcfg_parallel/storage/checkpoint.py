from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import time
from typing import Mapping, Sequence

from cqdagpcfg_parallel.protocol import NodeId, NodeRuntimeState
from cqdagpcfg_parallel.storage.manifest import ChunkManifestEntry


@dataclass(frozen=True, slots=True)
class NodeCheckpoint:
    node_id: NodeId
    ready_end: int
    target_end: int
    entropy: float
    exhausted: bool

    @classmethod
    def from_state(cls, state: NodeRuntimeState) -> "NodeCheckpoint":
        return cls(
            node_id=state.node_id,
            ready_end=state.ready_end,
            target_end=state.target_end,
            entropy=state.entropy,
            exhausted=state.exhausted,
        )


@dataclass(slots=True)
class ProtocolCheckpoint:
    nodes: list[NodeCheckpoint] = field(default_factory=list)
    chunks: list[ChunkManifestEntry] = field(default_factory=list)

    def node_ready_end(self, node_id: NodeId) -> int:
        for node in self.nodes:
            if node.node_id == node_id:
                return node.ready_end
        return 0


@dataclass(frozen=True, slots=True)
class DistributedTrackerCheckpoint:
    """Durable restart point for the distributed tracker.

    The checkpoint stores logical emission progress, not worker leases. After a
    tracker crash, workers can reconnect to a fresh tracker and regenerate any
    missing node-local chunks from the persisted shard cursors.
    """

    emitted_count: int
    shard_cursors: Mapping[NodeId, int]
    emitted_stable_records: tuple[str, ...] = ()
    emitted_log_uri: str | None = None
    created_at: float = field(default_factory=time)

    def __post_init__(self) -> None:
        if self.emitted_count < 0:
            raise ValueError("emitted_count cannot be negative")
        if self.emitted_stable_records and len(self.emitted_stable_records) != self.emitted_count:
            raise ValueError("emitted_stable_records must be empty or match emitted_count")
        if self.emitted_count > 0 and not self.emitted_stable_records and not self.emitted_log_uri:
            raise ValueError("checkpoint must include inline records or emitted_log_uri")
        for cursor in self.shard_cursors.values():
            if cursor < 0:
                raise ValueError("shard cursor cannot be negative")

    @classmethod
    def create(
        cls,
        *,
        emitted_count: int,
        shard_cursors: Mapping[NodeId, int],
        emitted_stable_records: Sequence[str] = (),
        emitted_log_uri: str | None = None,
    ) -> "DistributedTrackerCheckpoint":
        return cls(
            emitted_count=emitted_count,
            shard_cursors=dict(shard_cursors),
            emitted_stable_records=tuple(emitted_stable_records),
            emitted_log_uri=emitted_log_uri,
        )

    def cursor_for(self, node_id: NodeId) -> int:
        return int(self.shard_cursors.get(node_id, 0))

    def stable_records_for_resume(self) -> tuple[str, ...]:
        if self.emitted_stable_records:
            return self.emitted_stable_records
        if self.emitted_count == 0:
            return ()
        if self.emitted_log_uri is None:
            raise ValueError("checkpoint has no stable records for resume")
        records = StableRecordLog.read_all(Path(self.emitted_log_uri))
        if len(records) < self.emitted_count:
            raise ValueError("emitted log is shorter than checkpoint emitted_count")
        return records[: self.emitted_count]

    def to_dict(self) -> dict:
        return {
            "schema_version": 1,
            "emitted_count": self.emitted_count,
            "shard_cursors": {
                str(node_id): int(cursor)
                for node_id, cursor in self.shard_cursors.items()
            },
            "emitted_stable_records": list(self.emitted_stable_records),
            "emitted_log_uri": self.emitted_log_uri,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: Mapping) -> "DistributedTrackerCheckpoint":
        if payload.get("schema_version") != 1:
            raise ValueError("unsupported distributed tracker checkpoint schema version")
        return cls(
            emitted_count=int(payload["emitted_count"]),
            shard_cursors={
                NodeId(str(node_id)): int(cursor)
                for node_id, cursor in dict(payload["shard_cursors"]).items()
            },
            emitted_stable_records=tuple(
                str(record) for record in payload.get("emitted_stable_records", ())
            ),
            emitted_log_uri=(
                None
                if payload.get("emitted_log_uri") is None
                else str(payload["emitted_log_uri"])
            ),
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
    def from_json(cls, payload: str | bytes) -> "DistributedTrackerCheckpoint":
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
    def read(cls, path: Path) -> "DistributedTrackerCheckpoint":
        return cls.from_json(path.read_text(encoding="utf-8"))


class StableRecordLog:
    """Append-only JSONL log for emitted stable GuessRecord strings."""

    @staticmethod
    def append_suffix(path: Path, records: Sequence[str], *, start_index: int) -> int:
        if start_index < 0:
            raise ValueError("start_index cannot be negative")
        if start_index > len(records):
            raise ValueError("start_index cannot exceed record count")
        if start_index == len(records):
            return start_index
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            for record in records[start_index:]:
                handle.write(json.dumps(str(record), ensure_ascii=False, separators=(",", ":")))
                handle.write("\n")
        return len(records)

    @staticmethod
    def read_all(path: Path) -> tuple[str, ...]:
        if not path.exists():
            raise FileNotFoundError(path)
        records = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    records.append(str(json.loads(line)))
        return tuple(records)

    @staticmethod
    def count(path: Path) -> int:
        if not path.exists():
            return 0
        with path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())


class CompactDistributedTrackerCheckpointWriter:
    """Writes small tracker checkpoints and keeps emitted records in JSONL."""

    def __init__(self, *, checkpoint_path: Path, stable_log_path: Path) -> None:
        self.checkpoint_path = checkpoint_path
        self.stable_log_path = stable_log_path
        self._written_records = StableRecordLog.count(stable_log_path)

    def write(self, checkpoint: DistributedTrackerCheckpoint) -> None:
        records = checkpoint.stable_records_for_resume()
        if len(records) != checkpoint.emitted_count:
            raise ValueError("checkpoint stable record count mismatch")
        self._written_records = StableRecordLog.append_suffix(
            self.stable_log_path,
            records,
            start_index=min(self._written_records, len(records)),
        )
        compact = DistributedTrackerCheckpoint.create(
            emitted_count=checkpoint.emitted_count,
            shard_cursors=checkpoint.shard_cursors,
            emitted_stable_records=(),
            emitted_log_uri=str(self.stable_log_path),
        )
        compact.write_atomic(self.checkpoint_path)

    def __call__(self, checkpoint: DistributedTrackerCheckpoint) -> None:
        self.write(checkpoint)


__all__ = [
    "CompactDistributedTrackerCheckpointWriter",
    "DistributedTrackerCheckpoint",
    "NodeCheckpoint",
    "ProtocolCheckpoint",
    "StableRecordLog",
]
