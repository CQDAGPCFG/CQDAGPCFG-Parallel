from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Condition

from .candidate_batch import CandidateBatch


@dataclass(slots=True)
class QueueWatermark:
    pending_batches: int = 0
    pending_candidates: int = 0
    pending_payload_bytes: int = 0


@dataclass(slots=True)
class CandidateQueueStats:
    peak_pending_batches: int = 0
    peak_pending_candidates: int = 0
    peak_pending_payload_bytes: int = 0
    producer_waits: int = 0


class BoundedCandidateQueue:
    def __init__(
        self,
        *,
        max_pending_batches: int,
        max_pending_candidates: int,
        max_pending_payload_bytes: int,
    ) -> None:
        if max_pending_batches <= 0:
            raise ValueError("max_pending_batches must be positive")
        if max_pending_candidates <= 0:
            raise ValueError("max_pending_candidates must be positive")
        if max_pending_payload_bytes <= 0:
            raise ValueError("max_pending_payload_bytes must be positive")

        self.max_pending_batches = max_pending_batches
        self.max_pending_candidates = max_pending_candidates
        self.max_pending_payload_bytes = max_pending_payload_bytes
        self._items: deque[CandidateBatch] = deque()
        self._watermark = QueueWatermark()
        self._stats = CandidateQueueStats()
        self._closed = False
        self._condition = Condition()

    @property
    def stats(self) -> CandidateQueueStats:
        with self._condition:
            return CandidateQueueStats(
                peak_pending_batches=self._stats.peak_pending_batches,
                peak_pending_candidates=self._stats.peak_pending_candidates,
                peak_pending_payload_bytes=self._stats.peak_pending_payload_bytes,
                producer_waits=self._stats.producer_waits,
            )

    @property
    def watermark(self) -> QueueWatermark:
        with self._condition:
            return QueueWatermark(
                pending_batches=self._watermark.pending_batches,
                pending_candidates=self._watermark.pending_candidates,
                pending_payload_bytes=self._watermark.pending_payload_bytes,
            )

    def put(self, batch: CandidateBatch) -> None:
        if batch.record_count > self.max_pending_candidates:
            raise ValueError("single batch exceeds max_pending_candidates")
        if batch.payload_bytes > self.max_pending_payload_bytes:
            raise ValueError("single batch exceeds max_pending_payload_bytes")

        with self._condition:
            while not self._closed and not self._can_accept(batch):
                self._stats.producer_waits += 1
                self._condition.wait()
            if self._closed:
                raise RuntimeError("cannot publish to a closed candidate queue")

            self._items.append(batch)
            self._watermark.pending_batches += 1
            self._watermark.pending_candidates += batch.record_count
            self._watermark.pending_payload_bytes += batch.payload_bytes
            self._record_peak_locked()
            self._condition.notify_all()

    def get(self) -> CandidateBatch | None:
        with self._condition:
            while not self._items and not self._closed:
                self._condition.wait()
            if not self._items:
                return None

            batch = self._items.popleft()
            self._watermark.pending_batches -= 1
            self._watermark.pending_candidates -= batch.record_count
            self._watermark.pending_payload_bytes -= batch.payload_bytes
            self._condition.notify_all()
            return batch

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()

    def _can_accept(self, batch: CandidateBatch) -> bool:
        return (
            self._watermark.pending_batches + 1 <= self.max_pending_batches
            and self._watermark.pending_candidates + batch.record_count
            <= self.max_pending_candidates
            and self._watermark.pending_payload_bytes + batch.payload_bytes
            <= self.max_pending_payload_bytes
        )

    def _record_peak_locked(self) -> None:
        self._stats.peak_pending_batches = max(
            self._stats.peak_pending_batches,
            self._watermark.pending_batches,
        )
        self._stats.peak_pending_candidates = max(
            self._stats.peak_pending_candidates,
            self._watermark.pending_candidates,
        )
        self._stats.peak_pending_payload_bytes = max(
            self._stats.peak_pending_payload_bytes,
            self._watermark.pending_payload_bytes,
        )


__all__ = [
    "BoundedCandidateQueue",
    "CandidateQueueStats",
    "QueueWatermark",
]
