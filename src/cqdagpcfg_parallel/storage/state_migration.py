from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha256
from time import time
from typing import Any
from uuid import uuid4

from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.protocol import NodeId, WorkerId


STATE_SNAPSHOT_FORMAT_VERSION = "cqdagpcfg-state-snapshot/v1"

RankKeyValue = int | tuple["RankKeyValue", ...]


@dataclass(frozen=True, slots=True)
class LogProbRankEntry:
    log_prob: float
    rank_key: RankKeyValue

    def to_dict(self) -> dict[str, Any]:
        return {
            "log_prob": self.log_prob,
            "rank_key": _rank_key_to_json(self.rank_key),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LogProbRankEntry":
        return cls(
            log_prob=float(payload["log_prob"]),
            rank_key=_rank_key_from_json(payload["rank_key"]),
        )


@dataclass(frozen=True, slots=True)
class ResultWindowSnapshot:
    """Absolute-indexed CQDAG result/cache window.

    ``base`` is the absolute index of ``entries[0]``. The list contains only
    live entries, so block internals such as ``results_head`` are normalized
    out of the wire format.
    """

    base: int = 0
    entries: tuple[LogProbRankEntry, ...] = ()

    def __post_init__(self) -> None:
        if self.base < 0:
            raise ValueError("window base cannot be negative")

    @property
    def end(self) -> int:
        return self.base + len(self.entries)

    def to_dict(self) -> dict[str, Any]:
        return {
            "base": self.base,
            "entries": [entry.to_dict() for entry in self.entries],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "ResultWindowSnapshot":
        if payload is None:
            return cls()
        return cls(
            base=int(payload["base"]),
            entries=tuple(
                LogProbRankEntry.from_dict(entry)
                for entry in payload.get("entries", ())
            ),
        )


@dataclass(frozen=True, slots=True)
class GuessRecordSnapshot:
    prob: float
    guess: str
    structure_index: int
    structure_name: str
    ranks: tuple[int, ...]

    @classmethod
    def from_record(cls, record: GuessRecord) -> "GuessRecordSnapshot":
        return cls(
            prob=record.prob,
            guess=record.guess,
            structure_index=record.structure_index,
            structure_name=record.structure_name,
            ranks=tuple(record.ranks),
        )

    def to_record(self) -> GuessRecord:
        return GuessRecord(
            prob=self.prob,
            guess=self.guess,
            structure_index=self.structure_index,
            structure_name=self.structure_name,
            ranks=self.ranks,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "prob": self.prob,
            "guess": self.guess,
            "structure_index": self.structure_index,
            "structure_name": self.structure_name,
            "ranks": list(self.ranks),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GuessRecordSnapshot":
        return cls(
            prob=float(payload["prob"]),
            guess=str(payload["guess"]),
            structure_index=int(payload["structure_index"]),
            structure_name=str(payload["structure_name"]),
            ranks=tuple(int(rank) for rank in payload.get("ranks", ())),
        )


@dataclass(frozen=True, slots=True)
class GuessRecordWindowSnapshot:
    base: int = 0
    entries: tuple[GuessRecordSnapshot, ...] = ()

    def __post_init__(self) -> None:
        if self.base < 0:
            raise ValueError("guess window base cannot be negative")

    @property
    def end(self) -> int:
        return self.base + len(self.entries)

    def to_dict(self) -> dict[str, Any]:
        return {
            "base": self.base,
            "entries": [entry.to_dict() for entry in self.entries],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "GuessRecordWindowSnapshot":
        if payload is None:
            return cls()
        return cls(
            base=int(payload["base"]),
            entries=tuple(
                GuessRecordSnapshot.from_dict(entry)
                for entry in payload.get("entries", ())
            ),
        )


@dataclass(frozen=True, slots=True)
class FrontierEntrySnapshot:
    neg_log_prob: float
    rank_key: RankKeyValue
    left_index: int

    def __post_init__(self) -> None:
        if self.left_index < 0:
            raise ValueError("frontier left_index cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "neg_log_prob": self.neg_log_prob,
            "rank_key": _rank_key_to_json(self.rank_key),
            "left_index": self.left_index,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FrontierEntrySnapshot":
        return cls(
            neg_log_prob=float(payload["neg_log_prob"]),
            rank_key=_rank_key_from_json(payload["rank_key"]),
            left_index=int(payload["left_index"]),
        )


@dataclass(frozen=True, slots=True)
class IndexCountSnapshot:
    index: int
    value: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError("index cannot be negative")
        if self.value < 0:
            raise ValueError("value cannot be negative")

    def to_dict(self) -> dict[str, int]:
        return {"index": self.index, "value": self.value}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "IndexCountSnapshot":
        return cls(index=int(payload["index"]), value=int(payload["value"]))


@dataclass(frozen=True, slots=True)
class MergedBlockStateSnapshot:
    left_signature: tuple[str, ...]
    right_signature: tuple[str, ...]
    left_consumer: int | None = None
    right_consumer: int | None = None
    left_cache: ResultWindowSnapshot = field(default_factory=ResultWindowSnapshot)
    right_cache: ResultWindowSnapshot = field(default_factory=ResultWindowSnapshot)
    frontier: tuple[FrontierEntrySnapshot, ...] = ()
    active_rows: tuple[IndexCountSnapshot, ...] = ()
    active_right_counts: tuple[IndexCountSnapshot, ...] = ()
    right_min_heap: tuple[int, ...] = ()
    min_active_left: int | None = None
    min_active_right: int | None = None
    max_started_row: int = 0
    initialized: bool = False
    seeded_zero: bool = False
    refines_since_reclaim: int = 0
    reclaim_units: int = 0
    next_reclaim_after: int = 1

    def __post_init__(self) -> None:
        if not self.left_signature:
            raise ValueError("left_signature cannot be empty")
        if not self.right_signature:
            raise ValueError("right_signature cannot be empty")
        if self.max_started_row < 0:
            raise ValueError("max_started_row cannot be negative")
        if self.refines_since_reclaim < 0:
            raise ValueError("refines_since_reclaim cannot be negative")
        if self.reclaim_units < 0:
            raise ValueError("reclaim_units cannot be negative")
        if self.next_reclaim_after <= 0:
            raise ValueError("next_reclaim_after must be positive")

    def to_dict(self) -> dict[str, Any]:
        return {
            "left_signature": list(self.left_signature),
            "right_signature": list(self.right_signature),
            "left_consumer": self.left_consumer,
            "right_consumer": self.right_consumer,
            "left_cache": self.left_cache.to_dict(),
            "right_cache": self.right_cache.to_dict(),
            "frontier": [entry.to_dict() for entry in self.frontier],
            "active_rows": [entry.to_dict() for entry in self.active_rows],
            "active_right_counts": [
                entry.to_dict() for entry in self.active_right_counts
            ],
            "right_min_heap": list(self.right_min_heap),
            "min_active_left": self.min_active_left,
            "min_active_right": self.min_active_right,
            "max_started_row": self.max_started_row,
            "initialized": self.initialized,
            "seeded_zero": self.seeded_zero,
            "refines_since_reclaim": self.refines_since_reclaim,
            "reclaim_units": self.reclaim_units,
            "next_reclaim_after": self.next_reclaim_after,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MergedBlockStateSnapshot":
        return cls(
            left_signature=tuple(str(symbol) for symbol in payload["left_signature"]),
            right_signature=tuple(str(symbol) for symbol in payload["right_signature"]),
            left_consumer=_optional_int(payload.get("left_consumer")),
            right_consumer=_optional_int(payload.get("right_consumer")),
            left_cache=ResultWindowSnapshot.from_dict(payload.get("left_cache")),
            right_cache=ResultWindowSnapshot.from_dict(payload.get("right_cache")),
            frontier=tuple(
                FrontierEntrySnapshot.from_dict(entry)
                for entry in payload.get("frontier", ())
            ),
            active_rows=tuple(
                IndexCountSnapshot.from_dict(entry)
                for entry in payload.get("active_rows", ())
            ),
            active_right_counts=tuple(
                IndexCountSnapshot.from_dict(entry)
                for entry in payload.get("active_right_counts", ())
            ),
            right_min_heap=tuple(int(value) for value in payload.get("right_min_heap", ())),
            min_active_left=_optional_int(payload.get("min_active_left")),
            min_active_right=_optional_int(payload.get("min_active_right")),
            max_started_row=int(payload.get("max_started_row", 0)),
            initialized=bool(payload.get("initialized", False)),
            seeded_zero=bool(payload.get("seeded_zero", False)),
            refines_since_reclaim=int(payload.get("refines_since_reclaim", 0)),
            reclaim_units=int(payload.get("reclaim_units", 0)),
            next_reclaim_after=int(payload.get("next_reclaim_after", 1)),
        )


@dataclass(frozen=True, slots=True)
class BlockStateSnapshot:
    signature: tuple[str, ...]
    kind: str
    results: ResultWindowSnapshot = field(default_factory=ResultWindowSnapshot)
    seed_result: LogProbRankEntry | None = None
    consumer_upto: tuple[int, ...] = ()
    expected_consumers: int = 1
    registered_consumers: int = 0
    promotion_pins: int = 0
    merged: MergedBlockStateSnapshot | None = None

    def __post_init__(self) -> None:
        if not self.signature:
            raise ValueError("block signature cannot be empty")
        if not self.kind:
            raise ValueError("block kind cannot be empty")
        if self.expected_consumers <= 0:
            raise ValueError("expected_consumers must be positive")
        if self.registered_consumers < 0:
            raise ValueError("registered_consumers cannot be negative")
        if self.promotion_pins < 0:
            raise ValueError("promotion_pins cannot be negative")
        if self.kind.endswith("merged") and self.merged is None:
            raise ValueError("merged block snapshot is missing merged state")

    @property
    def produced_count(self) -> int:
        return self.results.end

    def to_dict(self) -> dict[str, Any]:
        return {
            "signature": list(self.signature),
            "kind": self.kind,
            "results": self.results.to_dict(),
            "seed_result": (
                None if self.seed_result is None else self.seed_result.to_dict()
            ),
            "consumer_upto": list(self.consumer_upto),
            "expected_consumers": self.expected_consumers,
            "registered_consumers": self.registered_consumers,
            "promotion_pins": self.promotion_pins,
            "merged": None if self.merged is None else self.merged.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "BlockStateSnapshot":
        seed_result = payload.get("seed_result")
        return cls(
            signature=tuple(str(symbol) for symbol in payload["signature"]),
            kind=str(payload["kind"]),
            results=ResultWindowSnapshot.from_dict(payload.get("results")),
            seed_result=(
                None if seed_result is None else LogProbRankEntry.from_dict(seed_result)
            ),
            consumer_upto=tuple(int(value) for value in payload.get("consumer_upto", ())),
            expected_consumers=int(payload.get("expected_consumers", 1)),
            registered_consumers=int(payload.get("registered_consumers", 0)),
            promotion_pins=int(payload.get("promotion_pins", 0)),
            merged=(
                None
                if payload.get("merged") is None
                else MergedBlockStateSnapshot.from_dict(payload["merged"])
            ),
        )


@dataclass(frozen=True, slots=True)
class StructureStreamStateSnapshot:
    node_id: NodeId
    structure_index: int
    structure_name: str
    symbols: tuple[str, ...]
    root_signature: tuple[str, ...]
    max_records: int
    stream_base: int
    ready_end: int
    consumer_id: int
    guess_cache: GuessRecordWindowSnapshot = field(
        default_factory=GuessRecordWindowSnapshot
    )

    def __post_init__(self) -> None:
        if self.structure_index < 0:
            raise ValueError("structure_index cannot be negative")
        if not self.structure_name:
            raise ValueError("structure_name cannot be empty")
        if not self.symbols:
            raise ValueError("symbols cannot be empty")
        if self.root_signature != self.symbols:
            raise ValueError("root_signature must match structure symbols")
        if self.max_records < 0:
            raise ValueError("max_records cannot be negative")
        if self.stream_base < 0:
            raise ValueError("stream_base cannot be negative")
        if self.ready_end < self.stream_base:
            raise ValueError("ready_end cannot be smaller than stream_base")
        if self.consumer_id < 0:
            raise ValueError("consumer_id cannot be negative")
        if self.guess_cache.entries and self.guess_cache.base != self.stream_base:
            raise ValueError("guess_cache base must match stream_base")
        if self.guess_cache.end > self.ready_end:
            raise ValueError("guess_cache cannot extend beyond ready_end")

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": str(self.node_id),
            "structure_index": self.structure_index,
            "structure_name": self.structure_name,
            "symbols": list(self.symbols),
            "root_signature": list(self.root_signature),
            "max_records": self.max_records,
            "stream_base": self.stream_base,
            "ready_end": self.ready_end,
            "consumer_id": self.consumer_id,
            "guess_cache": self.guess_cache.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StructureStreamStateSnapshot":
        return cls(
            node_id=NodeId(str(payload["node_id"])),
            structure_index=int(payload["structure_index"]),
            structure_name=str(payload["structure_name"]),
            symbols=tuple(str(symbol) for symbol in payload["symbols"]),
            root_signature=tuple(str(symbol) for symbol in payload["root_signature"]),
            max_records=int(payload["max_records"]),
            stream_base=int(payload["stream_base"]),
            ready_end=int(payload["ready_end"]),
            consumer_id=int(payload["consumer_id"]),
            guess_cache=GuessRecordWindowSnapshot.from_dict(payload.get("guess_cache")),
        )


@dataclass(frozen=True, slots=True)
class NodeWatermarkSnapshot:
    node_id: NodeId
    ready_end: int
    reclaim_before: int
    target_end: int | None = None

    def __post_init__(self) -> None:
        if self.ready_end < 0:
            raise ValueError("ready_end cannot be negative")
        if self.reclaim_before < 0:
            raise ValueError("reclaim_before cannot be negative")
        if self.reclaim_before > self.ready_end:
            raise ValueError("reclaim_before cannot exceed ready_end")
        if self.target_end is not None and self.target_end < self.ready_end:
            raise ValueError("target_end cannot be smaller than ready_end")

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": str(self.node_id),
            "ready_end": self.ready_end,
            "reclaim_before": self.reclaim_before,
            "target_end": self.target_end,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "NodeWatermarkSnapshot":
        return cls(
            node_id=NodeId(str(payload["node_id"])),
            ready_end=int(payload["ready_end"]),
            reclaim_before=int(payload["reclaim_before"]),
            target_end=_optional_int(payload.get("target_end")),
        )


@dataclass(frozen=True, slots=True)
class StateMigrationSnapshot:
    snapshot_id: str
    model_fingerprint: str
    source_worker_id: WorkerId
    streams: tuple[StructureStreamStateSnapshot, ...]
    blocks: tuple[BlockStateSnapshot, ...]
    watermarks: tuple[NodeWatermarkSnapshot, ...] = ()
    created_at_unix_ms: int = field(default_factory=lambda: int(time() * 1000))
    target_worker_id: WorkerId | None = None
    reason: str = "rebalance"
    format_version: str = STATE_SNAPSHOT_FORMAT_VERSION

    def __post_init__(self) -> None:
        if self.format_version != STATE_SNAPSHOT_FORMAT_VERSION:
            raise ValueError(f"unsupported snapshot format: {self.format_version}")
        if not self.snapshot_id:
            raise ValueError("snapshot_id cannot be empty")
        if not self.model_fingerprint:
            raise ValueError("model_fingerprint cannot be empty")
        if not self.streams:
            raise ValueError("state migration snapshot must include at least one stream")
        if not self.blocks:
            raise ValueError("state migration snapshot must include at least one block")
        signatures = [block.signature for block in self.blocks]
        if len(set(signatures)) != len(signatures):
            raise ValueError("state migration snapshot contains duplicate block signatures")
        block_signatures = set(signatures)
        for stream in self.streams:
            if stream.root_signature not in block_signatures:
                raise ValueError(
                    f"stream root block is missing from snapshot: {stream.root_signature}"
                )

    @classmethod
    def create(
        cls,
        *,
        model_fingerprint: str,
        source_worker_id: WorkerId,
        streams: tuple[StructureStreamStateSnapshot, ...],
        blocks: tuple[BlockStateSnapshot, ...],
        watermarks: tuple[NodeWatermarkSnapshot, ...] = (),
        target_worker_id: WorkerId | None = None,
        reason: str = "rebalance",
    ) -> "StateMigrationSnapshot":
        return cls(
            snapshot_id=str(uuid4()),
            model_fingerprint=model_fingerprint,
            source_worker_id=source_worker_id,
            target_worker_id=target_worker_id,
            reason=reason,
            streams=streams,
            blocks=blocks,
            watermarks=watermarks,
        )

    @property
    def block_count(self) -> int:
        return len(self.blocks)

    @property
    def stream_count(self) -> int:
        return len(self.streams)

    def content_digest(self) -> str:
        return sha256(self.to_json()).hexdigest()

    def payload_bytes(self) -> int:
        return len(self.to_json())

    def to_dict(self) -> dict[str, Any]:
        return {
            "format_version": self.format_version,
            "snapshot_id": self.snapshot_id,
            "model_fingerprint": self.model_fingerprint,
            "source_worker_id": str(self.source_worker_id),
            "target_worker_id": (
                None if self.target_worker_id is None else str(self.target_worker_id)
            ),
            "created_at_unix_ms": self.created_at_unix_ms,
            "reason": self.reason,
            "streams": [stream.to_dict() for stream in self.streams],
            "blocks": [block.to_dict() for block in self.blocks],
            "watermarks": [watermark.to_dict() for watermark in self.watermarks],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StateMigrationSnapshot":
        target_worker_id = payload.get("target_worker_id")
        return cls(
            format_version=str(payload["format_version"]),
            snapshot_id=str(payload["snapshot_id"]),
            model_fingerprint=str(payload["model_fingerprint"]),
            source_worker_id=WorkerId(str(payload["source_worker_id"])),
            target_worker_id=(
                None if target_worker_id is None else WorkerId(str(target_worker_id))
            ),
            created_at_unix_ms=int(payload["created_at_unix_ms"]),
            reason=str(payload.get("reason", "rebalance")),
            streams=tuple(
                StructureStreamStateSnapshot.from_dict(stream)
                for stream in payload.get("streams", ())
            ),
            blocks=tuple(
                BlockStateSnapshot.from_dict(block)
                for block in payload.get("blocks", ())
            ),
            watermarks=tuple(
                NodeWatermarkSnapshot.from_dict(watermark)
                for watermark in payload.get("watermarks", ())
            ),
        )

    def to_json(self) -> bytes:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @classmethod
    def from_json(cls, payload: str | bytes) -> "StateMigrationSnapshot":
        data = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        return cls.from_dict(json.loads(data))


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _rank_key_to_json(value: RankKeyValue) -> Any:
    if isinstance(value, int):
        return value
    return [_rank_key_to_json(child) for child in value]


def _rank_key_from_json(value: Any) -> RankKeyValue:
    if isinstance(value, int):
        return value
    if isinstance(value, list):
        return tuple(_rank_key_from_json(child) for child in value)
    raise TypeError(f"invalid rank key value: {value!r}")


__all__ = [
    "BlockStateSnapshot",
    "FrontierEntrySnapshot",
    "GuessRecordSnapshot",
    "GuessRecordWindowSnapshot",
    "IndexCountSnapshot",
    "LogProbRankEntry",
    "MergedBlockStateSnapshot",
    "NodeWatermarkSnapshot",
    "RankKeyValue",
    "ResultWindowSnapshot",
    "STATE_SNAPSHOT_FORMAT_VERSION",
    "StateMigrationSnapshot",
    "StructureStreamStateSnapshot",
]
