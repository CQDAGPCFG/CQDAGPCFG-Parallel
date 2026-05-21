from __future__ import annotations

import json
import struct
from dataclasses import dataclass, field
from threading import Lock, Thread
from time import sleep
from typing import Iterable, Protocol

from CQDAGPCFG import GuessRecord

from .batching import make_candidate_batches
from .candidate_batch import CandidateBatch
from .candidate_queue import BoundedCandidateQueue


@dataclass(frozen=True, slots=True)
class BatchEndOfStream:
    reason: str = "complete"


class CandidateBatchSink(Protocol):
    def publish(self, batch: CandidateBatch) -> None: ...

    def close(self) -> None: ...


class CandidateBatchSource(Protocol):
    def receive(self, *, timeout_ms: int | None = None) -> CandidateBatch | None: ...

    def close(self) -> None: ...


class JsonCandidateBatchCodec:
    schema_version = 1

    @classmethod
    def dumps(cls, batch: CandidateBatch) -> bytes:
        payload = {
            "schema_version": cls.schema_version,
            "type": "batch",
            "batch_id": batch.batch_id,
            "start_rank": batch.start_rank,
            "records": [
                {
                    "prob": record.prob,
                    "guess": record.guess,
                    "structure_index": record.structure_index,
                    "structure_name": record.structure_name,
                    "ranks": list(record.ranks),
                }
                for record in batch.records
            ],
        }
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @classmethod
    def dumps_end(cls, end: BatchEndOfStream | None = None) -> bytes:
        message = BatchEndOfStream() if end is None else end
        payload = {
            "schema_version": cls.schema_version,
            "type": "end",
            "reason": message.reason,
        }
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @classmethod
    def loads_envelope(cls, payload: bytes) -> CandidateBatch | BatchEndOfStream:
        raw = json.loads(payload.decode("utf-8"))
        if raw.get("schema_version") != cls.schema_version:
            raise ValueError("unsupported CandidateBatch schema version")

        message_type = raw.get("type", "batch")
        if message_type == "end":
            return BatchEndOfStream(reason=str(raw.get("reason", "complete")))
        if message_type != "batch":
            raise ValueError(f"unsupported CandidateBatch message type: {message_type}")

        records = tuple(
            GuessRecord(
                prob=float(record["prob"]),
                guess=str(record["guess"]),
                structure_index=int(record["structure_index"]),
                structure_name=str(record["structure_name"]),
                ranks=tuple(int(rank) for rank in record["ranks"]),
            )
            for record in raw["records"]
        )
        return CandidateBatch.from_records(
            batch_id=int(raw["batch_id"]),
            start_rank=int(raw["start_rank"]),
            records=records,
        )

    @classmethod
    def loads(cls, payload: bytes) -> CandidateBatch:
        envelope = cls.loads_envelope(payload)
        if isinstance(envelope, BatchEndOfStream):
            raise ValueError("expected CandidateBatch but received end-of-stream")
        return envelope


class BinaryCandidateBatchCodec:
    schema_version = 1
    _magic = b"CQB"
    _header = struct.Struct("!3sBB")
    _batch_header = struct.Struct("!QQI")
    _record_header = struct.Struct("!diiI")
    _rank_item = struct.Struct("!i")
    _end_header = struct.Struct("!I")
    _type_batch = 1
    _type_end = 2

    @classmethod
    def dumps(cls, batch: CandidateBatch) -> bytes:
        parts = [
            cls._header.pack(cls._magic, cls.schema_version, cls._type_batch),
            cls._batch_header.pack(batch.batch_id, batch.start_rank, len(batch.records)),
        ]
        for record in batch.records:
            guess = record.guess.encode("utf-8")
            structure_name = record.structure_name.encode("utf-8")
            parts.append(
                cls._record_header.pack(
                    record.prob,
                    record.structure_index,
                    len(record.ranks),
                    len(guess),
                )
            )
            parts.append(guess)
            parts.append(cls._end_header.pack(len(structure_name)))
            parts.append(structure_name)
            for rank in record.ranks:
                parts.append(cls._rank_item.pack(rank))
        return b"".join(parts)

    @classmethod
    def dumps_end(cls, end: BatchEndOfStream | None = None) -> bytes:
        message = BatchEndOfStream() if end is None else end
        reason = message.reason.encode("utf-8")
        return b"".join(
            (
                cls._header.pack(cls._magic, cls.schema_version, cls._type_end),
                cls._end_header.pack(len(reason)),
                reason,
            )
        )

    @classmethod
    def loads_envelope(cls, payload: bytes) -> CandidateBatch | BatchEndOfStream:
        offset = 0
        magic, version, message_type = cls._header.unpack_from(payload, offset)
        offset += cls._header.size
        if magic != cls._magic:
            raise ValueError("unsupported CandidateBatch binary magic")
        if version != cls.schema_version:
            raise ValueError("unsupported CandidateBatch binary schema version")

        if message_type == cls._type_end:
            reason_length, offset = cls._read_end_length(payload, offset)
            reason = payload[offset : offset + reason_length].decode("utf-8")
            return BatchEndOfStream(reason=reason)
        if message_type != cls._type_batch:
            raise ValueError(f"unsupported CandidateBatch binary message type: {message_type}")

        batch_id, start_rank, record_count = cls._batch_header.unpack_from(payload, offset)
        offset += cls._batch_header.size
        records = []
        for _ in range(record_count):
            prob, structure_index, rank_count, guess_length = cls._record_header.unpack_from(
                payload,
                offset,
            )
            offset += cls._record_header.size
            guess = payload[offset : offset + guess_length].decode("utf-8")
            offset += guess_length
            structure_name_length, offset = cls._read_end_length(payload, offset)
            structure_name = payload[offset : offset + structure_name_length].decode("utf-8")
            offset += structure_name_length
            ranks = []
            for _ in range(rank_count):
                (rank,) = cls._rank_item.unpack_from(payload, offset)
                offset += cls._rank_item.size
                ranks.append(rank)
            records.append(
                GuessRecord(
                    prob=prob,
                    guess=guess,
                    structure_index=structure_index,
                    structure_name=structure_name,
                    ranks=tuple(ranks),
                )
            )
        return CandidateBatch.from_records(
            batch_id=batch_id,
            start_rank=start_rank,
            records=records,
        )

    @classmethod
    def loads(cls, payload: bytes) -> CandidateBatch:
        envelope = cls.loads_envelope(payload)
        if isinstance(envelope, BatchEndOfStream):
            raise ValueError("expected CandidateBatch but received end-of-stream")
        return envelope

    @classmethod
    def _read_end_length(cls, payload: bytes, offset: int) -> tuple[int, int]:
        (length,) = cls._end_header.unpack_from(payload, offset)
        return length, offset + cls._end_header.size


@dataclass(slots=True)
class MemoryBatchSink:
    delay_seconds: float = 0.0
    batches: list[CandidateBatch] = field(default_factory=list)
    closed: bool = False

    def publish(self, batch: CandidateBatch) -> None:
        if self.closed:
            raise RuntimeError("cannot publish to a closed memory sink")
        if self.delay_seconds:
            sleep(self.delay_seconds)
        self.batches.append(batch)

    def close(self) -> None:
        self.closed = True


@dataclass(slots=True)
class BoundedBatchSinkStats:
    forwarded_batches: int = 0
    forwarded_candidates: int = 0
    peak_pending_batches: int = 0
    peak_pending_candidates: int = 0
    peak_pending_payload_bytes: int = 0
    producer_waits: int = 0


class BoundedBatchSink:
    """Decorator that adds bounded buffering in front of another batch sink."""

    def __init__(
        self,
        downstream: CandidateBatchSink,
        *,
        max_pending_batches: int,
        max_pending_candidates: int,
        max_pending_payload_bytes: int,
    ) -> None:
        self.downstream = downstream
        self._queue = BoundedCandidateQueue(
            max_pending_batches=max_pending_batches,
            max_pending_candidates=max_pending_candidates,
            max_pending_payload_bytes=max_pending_payload_bytes,
        )
        self._thread: Thread | None = None
        self._started = False
        self._closed = False
        self._errors: list[BaseException] = []
        self._lock = Lock()
        self._forwarded_batches = 0
        self._forwarded_candidates = 0

    def __enter__(self) -> "BoundedBatchSink":
        self.start()
        return self

    def __exit__(self, _exc_type, exc, _tb) -> None:
        self.close()

    @property
    def stats(self) -> BoundedBatchSinkStats:
        queue_stats = self._queue.stats
        with self._lock:
            return BoundedBatchSinkStats(
                forwarded_batches=self._forwarded_batches,
                forwarded_candidates=self._forwarded_candidates,
                peak_pending_batches=queue_stats.peak_pending_batches,
                peak_pending_candidates=queue_stats.peak_pending_candidates,
                peak_pending_payload_bytes=queue_stats.peak_pending_payload_bytes,
                producer_waits=queue_stats.producer_waits,
            )

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._thread = Thread(target=self._drain, name="bounded-batch-sink", daemon=True)
            self._thread.start()

    def publish(self, batch: CandidateBatch) -> None:
        self.start()
        self._raise_if_failed()
        self._queue.put(batch)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                self._raise_if_failed()
                return
            self._closed = True
        self._queue.close()
        if self._thread is not None:
            self._thread.join()
        self.downstream.close()
        self._raise_if_failed()

    def _drain(self) -> None:
        try:
            while True:
                batch = self._queue.get()
                if batch is None:
                    return
                self.downstream.publish(batch)
                with self._lock:
                    self._forwarded_batches += 1
                    self._forwarded_candidates += len(batch.records)
        except BaseException as exc:  # pragma: no cover - re-raised by caller
            with self._lock:
                self._errors.append(exc)
            self._queue.close()

    def _raise_if_failed(self) -> None:
        with self._lock:
            if self._errors:
                raise RuntimeError("bounded batch sink failed") from self._errors[0]


def publish_candidate_batches(
    batches: Iterable[CandidateBatch],
    sink: CandidateBatchSink,
    *,
    close: bool = True,
) -> None:
    try:
        for batch in batches:
            sink.publish(batch)
    finally:
        if close:
            sink.close()


def publish_record_batches(
    records: Iterable[GuessRecord],
    sink: CandidateBatchSink,
    *,
    batch_size: int,
    max_batch_payload_bytes: int,
    close: bool = True,
) -> None:
    publish_candidate_batches(
        make_candidate_batches(
            records,
            batch_size=batch_size,
            max_batch_payload_bytes=max_batch_payload_bytes,
        ),
        sink,
        close=close,
    )


__all__ = [
    "BatchEndOfStream",
    "BinaryCandidateBatchCodec",
    "BoundedBatchSink",
    "BoundedBatchSinkStats",
    "CandidateBatchSink",
    "CandidateBatchSource",
    "JsonCandidateBatchCodec",
    "MemoryBatchSink",
    "publish_candidate_batches",
    "publish_record_batches",
]
