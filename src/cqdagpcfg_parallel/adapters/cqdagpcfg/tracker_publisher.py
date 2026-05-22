from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, perf_counter
from typing import Callable, Mapping

from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.framework_logging import log_event
from cqdagpcfg_parallel.runtime import (
    BatchAck,
    BatchAckStatus,
    BatchRetryLedger,
    BatchState,
    CandidateBatch,
    DurableBatchCheckpoint,
    ZmqPullBatchAckSource,
    guess_payload_bytes,
)
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqPushBatchSink


LOGGER = logging.getLogger("cqdagpcfg.tracker.publisher")


@dataclass(frozen=True, slots=True)
class BatchRetryPayload:
    batch_id: int
    start_rank: int
    end_rank: int
    reason: str
    attempts: int
    pending_batches: int
    consumer_id: str | None = None
    error: str | None = None


class StreamingRecordBatchPublisher:
    def __init__(
        self,
        sink: ZmqPushBatchSink,
        *,
        ack_source: ZmqPullBatchAckSource,
        batch_size: int,
        max_batch_payload_bytes: int,
        ack_retry_interval_seconds: float,
        metrics_path: Path | None,
        metrics_flush_interval_seconds: float,
        outputs_path: Path | None = None,
        role_metrics_provider: Callable[[], Mapping[str, int]] | None = None,
        extra_metrics_provider: Callable[[], Mapping[str, object]] | None = None,
        batch_publish_callback: Callable[[CandidateBatch], None] | None = None,
        batch_retry_callback: Callable[[BatchRetryPayload], None] | None = None,
        initial_start_rank: int = 0,
        initial_batch_id: int = 0,
        batch_checkpoint_path: Path | None = None,
        resume_batch_checkpoint: DurableBatchCheckpoint | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if max_batch_payload_bytes <= 0:
            raise ValueError("max_batch_payload_bytes must be positive")
        if ack_retry_interval_seconds <= 0.0:
            raise ValueError("ack_retry_interval_seconds must be positive")
        self.sink = sink
        self.ack_source = ack_source
        self.batch_size = batch_size
        self.max_batch_payload_bytes = max_batch_payload_bytes
        self.role_metrics_provider = role_metrics_provider
        self.extra_metrics_provider = extra_metrics_provider
        self.batch_publish_callback = batch_publish_callback
        self.ack_retry_interval_seconds = ack_retry_interval_seconds
        self.batch_retry_callback = batch_retry_callback
        self.metrics_path = metrics_path
        self.outputs_path = outputs_path
        self.metrics_flush_interval_seconds = metrics_flush_interval_seconds
        self.started_at = monotonic()
        self.next_metrics_write_at = 0.0
        self.batch_checkpoint_path = batch_checkpoint_path
        self.batch_id = initial_batch_id
        self.start_rank = initial_start_rank
        self.current: list[GuessRecord] = []
        self.current_payload = 0
        self.published_batches = 0
        self.published_candidates = 0
        self.published_payload_bytes = 0
        self.ledger = BatchRetryLedger()
        self.inflight_batches: dict[int, CandidateBatch] = {}
        self.last_publish_at_by_batch: dict[int, float] = {}
        if resume_batch_checkpoint is not None:
            self.batch_id = max(self.batch_id, resume_batch_checkpoint.next_batch_id)
            self.start_rank = max(
                self.start_rank,
                resume_batch_checkpoint.next_start_rank,
            )
            self.ledger = resume_batch_checkpoint.ledger
            self.inflight_batches = dict(resume_batch_checkpoint.inflight_batches)
            self.last_publish_at_by_batch = {
                batch_id: 0.0 for batch_id in self.inflight_batches
            }
        self.ack_messages = 0
        self.ack_failures = 0
        self.republished_batches = 0
        self.completed_batches = 0
        self.consumer_outputs: list[dict[str, object]] = []
        self.protocol_metrics: dict[str, int] = {}
        self.metrics_write_count = 0
        self.metrics_write_seconds = 0.0
        self._write_batch_checkpoint()
        self._write_outputs()
        self.write_metrics(final=False)

    def publish(self, record: GuessRecord) -> None:
        record_bytes = guess_payload_bytes(record.guess)
        if record_bytes > self.max_batch_payload_bytes:
            raise ValueError("single guess exceeds max_batch_payload_bytes")
        if self.current and (
            len(self.current) >= self.batch_size
            or self.current_payload + record_bytes > self.max_batch_payload_bytes
        ):
            self.flush()
        self.current.append(record)
        self.current_payload += record_bytes

    def flush(self) -> None:
        if not self.current:
            self.drain_acks(timeout_ms=0)
            self.republish_stale()
            return
        batch = CandidateBatch.from_records(
            batch_id=self.batch_id,
            start_rank=self.start_rank,
            records=self.current,
        )
        if self.batch_publish_callback is not None:
            self.batch_publish_callback(batch)
        self.ledger.publish(batch)
        self.inflight_batches[batch.batch_id] = batch
        self.sink.publish(batch)
        self.last_publish_at_by_batch[batch.batch_id] = monotonic()
        self.batch_id += 1
        self.start_rank += len(self.current)
        self.published_batches += 1
        self.published_candidates += len(self.current)
        self.published_payload_bytes += batch.payload_bytes
        self.current = []
        self.current_payload = 0
        self.drain_acks(timeout_ms=0)
        self.republish_stale()
        self._write_batch_checkpoint()
        self.write_metrics(final=False)

    def republish_pending(self) -> None:
        for batch_id, batch in sorted(self.inflight_batches.items()):
            entry = self.ledger.entry(batch_id)
            if entry is None or entry.state == BatchState.DONE:
                continue
            self.sink.publish(batch)
            self.last_publish_at_by_batch[batch_id] = monotonic()
            self.republished_batches += 1
            self._notify_batch_retry(
                batch,
                reason="resume_pending",
                attempts=entry.attempts,
                consumer_id=entry.consumer_id,
            )
        self._write_batch_checkpoint()
        self.write_metrics(final=False)

    def drain_acks(self, *, timeout_ms: int) -> None:
        first = True
        while True:
            ack = self.ack_source.receive(timeout_ms=timeout_ms if first else 0)
            first = False
            if ack is None:
                return
            self._handle_ack(ack)

    def wait_for_acks(self, *, timeout_seconds: float) -> None:
        deadline = monotonic() + timeout_seconds
        while self.inflight_batches:
            remaining = deadline - monotonic()
            if remaining <= 0.0:
                pending = sorted(self.inflight_batches)
                raise TimeoutError(f"timed out waiting for batch ack: {pending}")
            self.drain_acks(timeout_ms=min(100, max(1, int(remaining * 1000))))
            self.republish_stale()

    def republish_stale(self) -> None:
        now = monotonic()
        republished = False
        for batch_id, batch in sorted(self.inflight_batches.items()):
            entry = self.ledger.entry(batch_id)
            if entry is None or entry.state == BatchState.DONE:
                continue
            last_publish_at = self.last_publish_at_by_batch.get(batch_id, 0.0)
            if now - last_publish_at < self.ack_retry_interval_seconds:
                continue
            self.sink.publish(batch)
            self.last_publish_at_by_batch[batch_id] = now
            self.republished_batches += 1
            self._notify_batch_retry(
                batch,
                reason="ack_timeout",
                attempts=entry.attempts,
                consumer_id=entry.consumer_id,
            )
            republished = True
        if republished:
            self._write_batch_checkpoint()
            self.write_metrics(final=False)

    def _handle_ack(self, ack: BatchAck) -> None:
        batch = self.inflight_batches.get(ack.batch_id)
        if batch is None:
            return
        entry = self.ledger.entry(ack.batch_id)
        if entry is None:
            return
        if entry.state != BatchState.DONE:
            self.ledger.start(ack.batch_id, consumer_id=ack.consumer_id)
        self.ack_messages += 1
        if ack.status == BatchAckStatus.DONE:
            self._record_ack_outputs(ack)
            self.ledger.complete(ack.batch_id, consumer_id=ack.consumer_id)
            del self.inflight_batches[ack.batch_id]
            self.last_publish_at_by_batch.pop(ack.batch_id, None)
            self.completed_batches += 1
            self._write_outputs()
            self._write_batch_checkpoint()
            return

        failed_entry = self.ledger.fail(ack.batch_id, consumer_id=ack.consumer_id)
        self.ack_failures += 1
        self.sink.publish(batch)
        self.last_publish_at_by_batch[ack.batch_id] = monotonic()
        self.republished_batches += 1
        self._notify_batch_retry(
            batch,
            reason="consumer_failed",
            attempts=failed_entry.attempts,
            consumer_id=ack.consumer_id,
            error=ack.error,
        )
        self._write_batch_checkpoint()

    def set_protocol_result(self, result) -> None:
        self.protocol_metrics = {
            "emitted_records": result.emitted_count,
            "chunkstore_resident_records": result.stats.resident_records,
            "chunkstore_peak_resident_records": result.stats.peak_resident_records,
            "chunkstore_reclaimed_records": result.stats.reclaimed_records,
            "scheduler_affinity_hits": result.stats.affinity_hits,
            "scheduler_affinity_misses": result.stats.affinity_misses,
            "expired_leases": result.expired_leases,
            "automatic_migrations": result.automatic_migrations,
            "failed_migration_triggers": result.failed_migration_triggers,
        }

    def write_metrics(self, *, final: bool, force: bool = False) -> None:
        if self.metrics_path is None:
            return
        now = monotonic()
        if not final and not force and now < self.next_metrics_write_at:
            return
        self.next_metrics_write_at = now + self.metrics_flush_interval_seconds
        elapsed = max(monotonic() - self.started_at, 1e-12)
        network = self.sink.stats
        ack_network = self.ack_source.stats
        ledger = self.ledger.stats
        role_metrics = (
            dict(self.role_metrics_provider())
            if self.role_metrics_provider is not None
            else {}
        )
        extra_metrics = (
            dict(self.extra_metrics_provider())
            if self.extra_metrics_provider is not None
            else {}
        )
        started_at = perf_counter()
        write_json(
            self.metrics_path,
            {
                "role": "tracker",
                "published_batches": self.published_batches,
                "published_candidates": self.published_candidates,
                "published_payload_bytes": self.published_payload_bytes,
                "candidate_rate": self.published_candidates / elapsed,
                "network_messages": network.messages,
                "network_batch_messages": network.batch_messages,
                "network_end_messages": network.end_messages,
                "network_bytes": network.bytes,
                "network_serialize_seconds": network.serialize_seconds,
                "network_send_seconds": network.send_seconds,
                "ack_messages": self.ack_messages,
                "ack_failures": self.ack_failures,
                "ack_completed_batches": self.completed_batches,
                "consumer_outputs": len(self.consumer_outputs),
                "ack_republished_batches": self.republished_batches,
                "ack_pending_batches": len(self.inflight_batches),
                "ack_network_messages": ack_network.messages,
                "ack_network_bytes": ack_network.bytes,
                "ack_network_recv_seconds": ack_network.recv_seconds,
                "ack_network_deserialize_seconds": ack_network.deserialize_seconds,
                "ledger_published": ledger.published,
                "ledger_started": ledger.started,
                "ledger_completed": ledger.completed,
                "ledger_failed": ledger.failed,
                "ledger_retries": ledger.retries,
                "metrics_write_count": self.metrics_write_count,
                "metrics_write_seconds": self.metrics_write_seconds,
                "elapsed_seconds": elapsed,
                "final": final,
                **extra_metrics,
                **role_metrics,
                **self.protocol_metrics,
            },
        )
        self.metrics_write_seconds += perf_counter() - started_at
        self.metrics_write_count += 1

    def _write_batch_checkpoint(self) -> None:
        if self.batch_checkpoint_path is None:
            return
        DurableBatchCheckpoint.create(
            next_batch_id=self.batch_id,
            next_start_rank=self.start_rank,
            ledger=self.ledger,
            inflight_batches=self.inflight_batches,
        ).write_atomic(self.batch_checkpoint_path)

    def _record_ack_outputs(self, ack: BatchAck) -> None:
        for output in ack.outputs:
            normalized = dict(output)
            normalized.setdefault("batch_id", ack.batch_id)
            normalized.setdefault("consumer_id", ack.consumer_id)
            self.consumer_outputs.append(normalized)

    def _write_outputs(self) -> None:
        if self.outputs_path is None:
            return
        write_json(
            self.outputs_path,
            {
                "consumer_outputs": self.consumer_outputs,
                "count": len(self.consumer_outputs),
            },
        )

    def _notify_batch_retry(
        self,
        batch: CandidateBatch,
        *,
        reason: str,
        attempts: int,
        consumer_id: str | None = None,
        error: str | None = None,
    ) -> None:
        log_event(
            LOGGER,
            logging.WARNING,
            "tracker.batch_retry",
            batch_id=batch.batch_id,
            start_rank=batch.start_rank,
            end_rank=batch.end_rank,
            reason=reason,
            attempts=attempts,
            pending_batches=len(self.inflight_batches),
            consumer_id=consumer_id,
            error=error,
        )
        if self.batch_retry_callback is None:
            return
        self.batch_retry_callback(
            BatchRetryPayload(
                batch_id=batch.batch_id,
                start_rank=batch.start_rank,
                end_rank=batch.end_rank,
                reason=reason,
                attempts=attempts,
                pending_batches=len(self.inflight_batches),
                consumer_id=consumer_id,
                error=error,
            )
        )


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


__all__ = ["BatchRetryPayload", "StreamingRecordBatchPublisher"]
