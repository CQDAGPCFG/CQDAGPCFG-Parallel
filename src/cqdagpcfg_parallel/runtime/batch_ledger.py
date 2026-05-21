from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from time import monotonic
from typing import Mapping

from .candidate_batch import CandidateBatch


class BatchState(str, Enum):
    PUBLISHED = "published"
    INFLIGHT = "inflight"
    DONE = "done"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class BatchLedgerEntry:
    batch_id: int
    start_rank: int
    end_rank: int
    state: BatchState
    attempts: int = 0
    consumer_id: str | None = None
    updated_at: float = 0.0

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "start_rank": self.start_rank,
            "end_rank": self.end_rank,
            "state": self.state.value,
            "attempts": self.attempts,
            "consumer_id": self.consumer_id,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, payload: Mapping) -> "BatchLedgerEntry":
        return cls(
            batch_id=int(payload["batch_id"]),
            start_rank=int(payload["start_rank"]),
            end_rank=int(payload["end_rank"]),
            state=BatchState(str(payload["state"])),
            attempts=int(payload.get("attempts", 0)),
            consumer_id=(
                None
                if payload.get("consumer_id") is None
                else str(payload["consumer_id"])
            ),
            updated_at=float(payload.get("updated_at", 0.0)),
        )


@dataclass(frozen=True, slots=True)
class BatchLedgerStats:
    published: int = 0
    started: int = 0
    completed: int = 0
    failed: int = 0
    duplicates: int = 0
    retries: int = 0

    def to_dict(self) -> dict:
        return {
            "published": self.published,
            "started": self.started,
            "completed": self.completed,
            "failed": self.failed,
            "duplicates": self.duplicates,
            "retries": self.retries,
        }

    @classmethod
    def from_dict(cls, payload: Mapping) -> "BatchLedgerStats":
        return cls(
            published=int(payload.get("published", 0)),
            started=int(payload.get("started", 0)),
            completed=int(payload.get("completed", 0)),
            failed=int(payload.get("failed", 0)),
            duplicates=int(payload.get("duplicates", 0)),
            retries=int(payload.get("retries", 0)),
        )


class BatchRetryLedger:
    """Idempotent CandidateBatch attempt tracker.

    It records rank ranges rather than payloads, so a failed consumer can be
    retried without keeping all candidate strings in the tracker.
    """

    def __init__(self) -> None:
        self._entries: dict[int, BatchLedgerEntry] = {}
        self._stats = BatchLedgerStats()

    @property
    def stats(self) -> BatchLedgerStats:
        return self._stats

    def publish(self, batch: CandidateBatch, *, now: float | None = None) -> BatchLedgerEntry:
        current = self._entries.get(batch.batch_id)
        if current is not None:
            if current.start_rank != batch.start_rank or current.end_rank != batch.end_rank:
                raise ValueError("batch_id was reused with a different rank range")
            self._stats = _replace_stats(self._stats, duplicates=self._stats.duplicates + 1)
            return current
        entry = BatchLedgerEntry(
            batch_id=batch.batch_id,
            start_rank=batch.start_rank,
            end_rank=batch.end_rank,
            state=BatchState.PUBLISHED,
            updated_at=monotonic() if now is None else now,
        )
        self._entries[batch.batch_id] = entry
        self._stats = _replace_stats(self._stats, published=self._stats.published + 1)
        return entry

    def start(
        self,
        batch_id: int,
        *,
        consumer_id: str,
        now: float | None = None,
    ) -> BatchLedgerEntry:
        entry = self._require(batch_id)
        if entry.state == BatchState.DONE:
            return entry
        retries = self._stats.retries
        if entry.state in {BatchState.INFLIGHT, BatchState.FAILED}:
            retries += 1
        updated = BatchLedgerEntry(
            batch_id=entry.batch_id,
            start_rank=entry.start_rank,
            end_rank=entry.end_rank,
            state=BatchState.INFLIGHT,
            attempts=entry.attempts + 1,
            consumer_id=consumer_id,
            updated_at=monotonic() if now is None else now,
        )
        self._entries[batch_id] = updated
        self._stats = _replace_stats(
            self._stats,
            started=self._stats.started + 1,
            retries=retries,
        )
        return updated

    def complete(
        self,
        batch_id: int,
        *,
        consumer_id: str | None = None,
        now: float | None = None,
    ) -> BatchLedgerEntry:
        entry = self._require(batch_id)
        if entry.state == BatchState.DONE:
            return entry
        if consumer_id is not None and entry.consumer_id not in {None, consumer_id}:
            raise ValueError("completion consumer_id does not match inflight owner")
        updated = BatchLedgerEntry(
            batch_id=entry.batch_id,
            start_rank=entry.start_rank,
            end_rank=entry.end_rank,
            state=BatchState.DONE,
            attempts=entry.attempts,
            consumer_id=consumer_id or entry.consumer_id,
            updated_at=monotonic() if now is None else now,
        )
        self._entries[batch_id] = updated
        self._stats = _replace_stats(self._stats, completed=self._stats.completed + 1)
        return updated

    def fail(
        self,
        batch_id: int,
        *,
        consumer_id: str | None = None,
        now: float | None = None,
    ) -> BatchLedgerEntry:
        entry = self._require(batch_id)
        if entry.state == BatchState.DONE:
            return entry
        if consumer_id is not None and entry.consumer_id not in {None, consumer_id}:
            raise ValueError("failure consumer_id does not match inflight owner")
        updated = BatchLedgerEntry(
            batch_id=entry.batch_id,
            start_rank=entry.start_rank,
            end_rank=entry.end_rank,
            state=BatchState.FAILED,
            attempts=entry.attempts,
            consumer_id=consumer_id or entry.consumer_id,
            updated_at=monotonic() if now is None else now,
        )
        self._entries[batch_id] = updated
        self._stats = _replace_stats(self._stats, failed=self._stats.failed + 1)
        return updated

    def retryable(self) -> tuple[BatchLedgerEntry, ...]:
        return tuple(
            entry for entry in self._entries.values() if entry.state == BatchState.FAILED
        )

    def entries(self) -> tuple[BatchLedgerEntry, ...]:
        return tuple(sorted(self._entries.values(), key=lambda entry: entry.batch_id))

    def entry(self, batch_id: int) -> BatchLedgerEntry | None:
        return self._entries.get(batch_id)

    def to_dict(self) -> dict:
        return {
            "schema_version": 1,
            "entries": [entry.to_dict() for entry in self.entries()],
            "stats": self._stats.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping) -> "BatchRetryLedger":
        if int(payload.get("schema_version", 1)) != 1:
            raise ValueError("unsupported batch ledger schema version")
        ledger = cls()
        entries = [
            BatchLedgerEntry.from_dict(entry)
            for entry in payload.get("entries", ())
        ]
        ledger._entries = {entry.batch_id: entry for entry in entries}
        ledger._stats = BatchLedgerStats.from_dict(payload.get("stats", {}))
        return ledger

    def _require(self, batch_id: int) -> BatchLedgerEntry:
        try:
            return self._entries[batch_id]
        except KeyError as exc:
            raise KeyError(f"unknown batch_id: {batch_id}") from exc


def _replace_stats(stats: BatchLedgerStats, **changes: int) -> BatchLedgerStats:
    payload = {
        "published": stats.published,
        "started": stats.started,
        "completed": stats.completed,
        "failed": stats.failed,
        "duplicates": stats.duplicates,
        "retries": stats.retries,
    }
    payload.update(changes)
    return BatchLedgerStats(**payload)


__all__ = [
    "BatchLedgerEntry",
    "BatchLedgerStats",
    "BatchRetryLedger",
    "BatchState",
]
