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

    def __post_init__(self) -> None:
        if self.start < 0:
            raise ValueError("work item start cannot be negative")
        if self.end <= self.start:
            raise ValueError("work item end must be larger than start")
        if self.reclaim_before < 0:
            raise ValueError("work item reclaim_before cannot be negative")
        if self.reclaim_before > self.start:
            raise ValueError("work item reclaim_before cannot exceed start")

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


STABLE_PROBABILITY_DIGITS = 13


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


__all__ = [
    "ChunkRange",
    "ChunkSizePolicy",
    "Demand",
    "EnumerationChunk",
    "Lease",
    "NodeId",
    "STABLE_PROBABILITY_DIGITS",
    "WorkItem",
    "WorkerId",
    "stable_digest",
    "stable_record_string",
]
