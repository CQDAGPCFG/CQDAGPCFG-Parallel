from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from typing import Iterable, NewType, Sequence

from CQDAGPCFG import GuessRecord


NodeId = NewType("NodeId", str)
WorkerId = NewType("WorkerId", str)


class ChunkSizePolicy(str, Enum):
    CQDAG_ADAPTIVE = "cqdag_adaptive"
    FIXED = "fixed"
    GAP_ADAPTIVE = "gap_adaptive"
    ENTROPY_ADAPTIVE = "entropy_adaptive"


class LeaseStrategyName(str, Enum):
    RANGE = "range"
    PROBABILITY_MASS = "probability_mass"
    RANK_WINDOW_PROBABILITY_MASS = "rank_window_probability_mass"


@dataclass(frozen=True, slots=True)
class ChunkRange:
    node_id: NodeId
    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start < 0:
            raise ValueError("chunk range start cannot be negative")
        if self.end < self.start:
            raise ValueError("chunk range end cannot be smaller than start")

    @property
    def size(self) -> int:
        return self.end - self.start


@dataclass(frozen=True, slots=True)
class Demand:
    node_id: NodeId
    target_end: int
    urgency: float = 1.0

    def __post_init__(self) -> None:
        if self.target_end <= 0:
            raise ValueError("demand target_end must be positive")
        if self.urgency < 0.0:
            raise ValueError("demand urgency cannot be negative")


@dataclass(frozen=True, slots=True)
class Lease:
    node_id: NodeId
    worker_id: WorkerId
    epoch: int
    acquired_at: float
    expires_at: float
    start: int = 0
    end: int | None = None

    def __post_init__(self) -> None:
        if self.start < 0:
            raise ValueError("lease start cannot be negative")
        if self.end is not None and self.end <= self.start:
            raise ValueError("lease end must be greater than start")

    def is_expired(self, now: float) -> bool:
        return now >= self.expires_at

    def overlaps(self, start: int, end: int | None) -> bool:
        if end is not None and end <= start:
            raise ValueError("range end must be greater than start")
        if self.end is None or end is None:
            return True
        return start < self.end and self.start < end


@dataclass(frozen=True, slots=True)
class WorkItem:
    node_id: NodeId
    start: int
    end: int
    worker_id: WorkerId
    epoch: int
    reclaim_before: int = 0
    estimated_mass: float = 0.0
    mass_budget: float = 0.0

    def __post_init__(self) -> None:
        if self.start < 0:
            raise ValueError("work item start cannot be negative")
        if self.end <= self.start:
            raise ValueError("work item end must be larger than start")
        if self.reclaim_before < 0:
            raise ValueError("work item reclaim_before cannot be negative")
        if self.reclaim_before > self.start:
            raise ValueError("work item reclaim_before cannot exceed start")
        if self.estimated_mass < 0.0:
            raise ValueError("work item estimated_mass cannot be negative")
        if self.mass_budget < 0.0:
            raise ValueError("work item mass_budget cannot be negative")

    @property
    def range(self) -> ChunkRange:
        return ChunkRange(self.node_id, self.start, self.end)

    @property
    def size(self) -> int:
        return self.end - self.start


@dataclass(frozen=True, slots=True)
class EnumerationChunk:
    node_id: NodeId
    start: int
    records: tuple[GuessRecord, ...]
    worker_id: WorkerId
    epoch: int

    @classmethod
    def from_records(
        cls,
        *,
        node_id: NodeId,
        start: int,
        records: Sequence[GuessRecord],
        worker_id: WorkerId,
        epoch: int,
    ) -> "EnumerationChunk":
        return cls(
            node_id=node_id,
            start=start,
            records=tuple(records),
            worker_id=worker_id,
            epoch=epoch,
        )

    def __post_init__(self) -> None:
        if self.start < 0:
            raise ValueError("enumeration chunk start cannot be negative")
        if not self.records:
            raise ValueError("enumeration chunk cannot be empty")

    @property
    def end(self) -> int:
        return self.start + len(self.records)

    @property
    def range(self) -> ChunkRange:
        return ChunkRange(self.node_id, self.start, self.end)


STABLE_PROBABILITY_DIGITS = 9
STABLE_FINGERPRINT_BASE1 = 0x100000001B3
STABLE_FINGERPRINT_BASE2 = 0x9E3779B185EBCA87
STABLE_FINGERPRINT_MASK = (1 << 64) - 1


def stable_record_string(record: GuessRecord) -> str:
    rank_str = ",".join(str(value) for value in record.ranks)
    probability = f"{record.prob:.{STABLE_PROBABILITY_DIGITS}g}"
    return (
        f"{probability}|{record.structure_index}|{record.structure_name}|"
        f"{rank_str}|{record.guess}"
    )


def stable_digest(records: Iterable[GuessRecord]) -> str:
    digest = sha256()
    for record in records:
        digest.update(stable_record_string(record).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class StableStreamFingerprint:
    """Associative fingerprint over stable record bytes."""

    byte_length: int = 0
    h1: int = 0
    h2: int = 0

    def __post_init__(self) -> None:
        if self.byte_length < 0:
            raise ValueError("byte_length cannot be negative")

    def update_bytes(self, payload: bytes) -> "StableStreamFingerprint":
        h1 = self.h1
        h2 = self.h2
        for value in payload:
            item = value + 1
            h1 = ((h1 * STABLE_FINGERPRINT_BASE1) + item) & STABLE_FINGERPRINT_MASK
            h2 = ((h2 * STABLE_FINGERPRINT_BASE2) + item) & STABLE_FINGERPRINT_MASK
        return StableStreamFingerprint(
            byte_length=self.byte_length + len(payload),
            h1=h1,
            h2=h2,
        )

    def update_stable_record(self, record: GuessRecord) -> "StableStreamFingerprint":
        return self.update_bytes(
            stable_record_string(record).encode("utf-8") + b"\n",
        )

    def combine(self, chunk: "StableStreamFingerprint") -> "StableStreamFingerprint":
        factor1 = pow(STABLE_FINGERPRINT_BASE1, chunk.byte_length, 1 << 64)
        factor2 = pow(STABLE_FINGERPRINT_BASE2, chunk.byte_length, 1 << 64)
        return StableStreamFingerprint(
            byte_length=self.byte_length + chunk.byte_length,
            h1=((self.h1 * factor1) + chunk.h1) & STABLE_FINGERPRINT_MASK,
            h2=((self.h2 * factor2) + chunk.h2) & STABLE_FINGERPRINT_MASK,
        )

    def to_string(self, scheme: str = "sfp-v1") -> str:
        if scheme not in {"sfp-v1", "rfp-v1"}:
            raise ValueError("unsupported stable stream fingerprint scheme")
        return f"{scheme}:{self.byte_length}:{self.h1:016x}:{self.h2:016x}"

    @classmethod
    def from_string(cls, value: str) -> "StableStreamFingerprint":
        parts = value.split(":")
        if len(parts) != 4 or parts[0] not in {"sfp-v1", "rfp-v1"}:
            raise ValueError("unsupported stable stream fingerprint")
        return cls(
            byte_length=int(parts[1]),
            h1=int(parts[2], 16),
            h2=int(parts[3], 16),
        )


def stable_stream_fingerprint(records: Iterable[GuessRecord]) -> str:
    fingerprint = StableStreamFingerprint()
    for record in records:
        fingerprint = fingerprint.update_stable_record(record)
    return fingerprint.to_string()


def record_stream_fingerprint(records: Iterable[GuessRecord]) -> str:
    fingerprint = StableStreamFingerprint()
    for record in records:
        fingerprint = fingerprint.update_bytes(canonical_record_bytes(record))
    return fingerprint.to_string("rfp-v1")


def canonical_record_bytes(record: GuessRecord) -> bytes:
    probability = f"{record.prob:.{STABLE_PROBABILITY_DIGITS}g}".encode("utf-8")
    name = record.structure_name.encode("utf-8")
    guess = record.guess.encode("utf-8")
    out = bytearray()
    out.extend(b"R")
    _append_length_prefixed(out, probability)
    out.extend(int(record.structure_index).to_bytes(4, "big", signed=True))
    _append_length_prefixed(out, name)
    out.extend(len(record.ranks).to_bytes(4, "big", signed=False))
    for rank in record.ranks:
        out.extend(int(rank).to_bytes(4, "big", signed=True))
    _append_length_prefixed(out, guess)
    return bytes(out)


def _append_length_prefixed(out: bytearray, payload: bytes) -> None:
    out.extend(len(payload).to_bytes(4, "big", signed=False))
    out.extend(payload)


__all__ = [
    "ChunkRange",
    "ChunkSizePolicy",
    "Demand",
    "EnumerationChunk",
    "Lease",
    "LeaseStrategyName",
    "NodeId",
    "STABLE_PROBABILITY_DIGITS",
    "StableStreamFingerprint",
    "WorkItem",
    "WorkerId",
    "canonical_record_bytes",
    "record_stream_fingerprint",
    "stable_digest",
    "stable_record_string",
    "stable_stream_fingerprint",
]
