from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from threading import Event, RLock, Thread
from time import monotonic, perf_counter
from typing import Callable, Mapping
from urllib.parse import unquote, urlparse

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
from cqdagpcfg_parallel.distributed.memory_policy import BatchMemoryLimits
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqPushBatchSink


LOGGER = logging.getLogger("cqdagpcfg.tracker.publisher")


RANK_PROGRESS_MARKERS = (100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000)
MASS_PROGRESS_MARKERS = (
    ("1ppm", 1e-6),
    ("10ppm", 1e-5),
    ("100ppm", 1e-4),
    ("0_1pct", 1e-3),
    ("1pct", 1e-2),
    ("10pct", 1e-1),
    ("50pct", 0.5),
    ("90pct", 0.9),
)


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
        batch_limits_provider: Callable[[], BatchMemoryLimits] | None = None,
        initial_start_rank: int = 0,
        initial_batch_id: int = 0,
        batch_checkpoint_path: Path | None = None,
        resume_batch_checkpoint: DurableBatchCheckpoint | None = None,
        candidate_block_dir: Path | None = None,
        candidate_block_base_uri: str | None = None,
        delete_candidate_blocks_on_ack: bool = True,
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
        self.batch_limits_provider = batch_limits_provider
        self.ack_retry_interval_seconds = ack_retry_interval_seconds
        self.batch_retry_callback = batch_retry_callback
        self.metrics_path = metrics_path
        self.outputs_path = outputs_path
        self.metrics_flush_interval_seconds = metrics_flush_interval_seconds
        self.candidate_block_dir = candidate_block_dir
        self.candidate_block_base_uri = candidate_block_base_uri
        self.delete_candidate_blocks_on_ack = delete_candidate_blocks_on_ack
        if self.candidate_block_dir is not None:
            self.candidate_block_dir.mkdir(parents=True, exist_ok=True)
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
        self.cumulative_probability_mass = 0.0
        self.time_to_rank_seconds: dict[int, float] = {}
        self.time_to_mass_seconds: dict[str, float] = {}
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
        self.artifact_batches = 0
        self.artifact_bytes = 0
        self.artifact_write_seconds = 0.0
        self.artifact_deleted = 0
        self.artifact_delete_failures = 0
        self.artifact_backpressure_waits = 0
        self.artifact_backpressure_seconds = 0.0
        self.artifact_pending_bytes_high_watermark = 0
        self.artifact_pending_batches_high_watermark = 0
        self.consumer_outputs: list[dict[str, object]] = []
        self.protocol_metrics: dict[str, object] = {}
        self.metrics_write_count = 0
        self.metrics_write_seconds = 0.0
        self._lock = RLock()
        self._ack_drain_stop: Event | None = None
        self._ack_drain_thread: Thread | None = None
        self._write_batch_checkpoint()
        self._write_outputs()
        self.write_metrics(final=False)

    def publish(self, record: GuessRecord) -> None:
        batch_size, max_batch_payload_bytes = self._effective_batch_limits()
        record_bytes = guess_payload_bytes(record.guess)
        if record_bytes > max_batch_payload_bytes:
            raise ValueError("single guess exceeds max_batch_payload_bytes")
        self._record_progress(record)
        if self.current and (
            len(self.current) >= batch_size
            or self.current_payload + record_bytes > max_batch_payload_bytes
        ):
            self.flush()
        self.current.append(record)
        self.current_payload += record_bytes

    def publish_many(self, records: tuple[GuessRecord, ...]) -> None:
        """Append an already ordered record chunk without re-entering per record."""
        batch_size, max_batch_payload_bytes = self._effective_batch_limits()
        for record in records:
            record_bytes = guess_payload_bytes(record.guess)
            if record_bytes > max_batch_payload_bytes:
                raise ValueError("single guess exceeds max_batch_payload_bytes")
            self._record_progress(record)
            if self.current and (
                len(self.current) >= batch_size
                or self.current_payload + record_bytes > max_batch_payload_bytes
            ):
                self.flush()
            self.current.append(record)
            self.current_payload += record_bytes

    def publish_artifact(
        self,
        *,
        record_count: int,
        payload_bytes: int,
        artifact_uri: str,
        artifact_sha256: str,
        artifact_bytes: int,
        probability_mass: float = 0.0,
        artifact_format: str = "guess-lines-v1",
    ) -> None:
        if record_count <= 0:
            return
        with self._lock:
            if self.current:
                self.flush()
            self._record_artifact_progress(
                record_count=record_count,
                probability_mass=probability_mass,
            )
            batch = CandidateBatch.from_artifact(
                batch_id=self.batch_id,
                start_rank=self.start_rank,
                record_count=record_count,
                payload_bytes=payload_bytes,
                artifact_uri=artifact_uri,
                artifact_sha256=artifact_sha256,
                artifact_bytes=artifact_bytes,
                artifact_format=artifact_format,
            )
            if self.batch_publish_callback is not None:
                self.batch_publish_callback(batch)
            self.ledger.publish(batch)
            self.inflight_batches[batch.batch_id] = batch
            self.sink.publish(batch)
            self.last_publish_at_by_batch[batch.batch_id] = monotonic()
            self.batch_id += 1
            self.start_rank += record_count
            self.published_batches += 1
            self.published_candidates += record_count
            self.published_payload_bytes += payload_bytes
            self.artifact_batches += 1
            self.artifact_bytes += artifact_bytes
            self._record_artifact_pending_high_watermark_locked()
        self.drain_acks(timeout_ms=0)
        self.republish_stale()
        self._write_batch_checkpoint()
        self.write_metrics(final=False)

    def flush(self) -> None:
        with self._lock:
            if not self.current:
                self.drain_acks(timeout_ms=0)
                self.republish_stale()
                return
            record_batch = CandidateBatch.from_records(
                batch_id=self.batch_id,
                start_rank=self.start_rank,
                records=self.current,
            )
            if self.batch_publish_callback is not None:
                self.batch_publish_callback(record_batch)
            batch = self._materialize_publish_batch(record_batch)
            self.ledger.publish(batch)
            self.inflight_batches[batch.batch_id] = batch
            self.sink.publish(batch)
            self.last_publish_at_by_batch[batch.batch_id] = monotonic()
            self.batch_id += 1
            self.start_rank += record_batch.record_count
            self.published_batches += 1
            self.published_candidates += record_batch.record_count
            self.published_payload_bytes += batch.payload_bytes
            self.current = []
            self.current_payload = 0
        self.drain_acks(timeout_ms=0)
        self.republish_stale()
        self._write_batch_checkpoint()
        self.write_metrics(final=False)

    def _materialize_publish_batch(self, batch: CandidateBatch) -> CandidateBatch:
        if self.candidate_block_dir is None:
            return batch
        started_at = perf_counter()
        path = self._candidate_block_path(batch)
        sha256 = hashlib.sha256()
        with path.open("wb") as handle:
            for record in batch.records:
                payload = record.guess.encode("utf-8") + b"\n"
                sha256.update(payload)
                handle.write(payload)
        artifact_bytes = path.stat().st_size
        self.artifact_batches += 1
        self.artifact_bytes += artifact_bytes
        self.artifact_write_seconds += perf_counter() - started_at
        return CandidateBatch.from_artifact(
            batch_id=batch.batch_id,
            start_rank=batch.start_rank,
            record_count=batch.record_count,
            payload_bytes=batch.payload_bytes,
            artifact_uri=self._candidate_block_uri(path),
            artifact_sha256=sha256.hexdigest(),
            artifact_bytes=artifact_bytes,
        )

    def _candidate_block_path(self, batch: CandidateBatch) -> Path:
        if self.candidate_block_dir is None:
            raise RuntimeError("candidate block directory is not configured")
        filename = (
            f"candidate-block-{batch.batch_id:012d}-"
            f"{batch.start_rank:012d}-{batch.end_rank:012d}.txt"
        )
        return self.candidate_block_dir / filename

    def _candidate_block_uri(self, path: Path) -> str:
        if self.candidate_block_base_uri:
            return f"{self.candidate_block_base_uri.rstrip('/')}/{path.name}"
        return path.resolve().as_uri()

    def _delete_artifact_if_safe(self, batch: CandidateBatch) -> None:
        if (
            not self.delete_candidate_blocks_on_ack
            or not batch.is_artifact
            or self.candidate_block_dir is None
        ):
            return
        artifact_uri = str(batch.artifact_uri)
        parsed = urlparse(artifact_uri)
        filename = Path(unquote(parsed.path) if parsed.scheme else artifact_uri).name
        path = self.candidate_block_dir / filename
        if not path.exists():
            return
        try:
            path.unlink()
            self.artifact_deleted += 1
        except OSError:
            self.artifact_delete_failures += 1
            LOGGER.exception(
                "event=tracker.candidate_block_delete_failed batch_id=%s path=%s",
                batch.batch_id,
                path,
            )

    def republish_pending(self) -> None:
        with self._lock:
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
            with self._lock:
                self._handle_ack(ack)

    def wait_for_acks(self, *, timeout_seconds: float) -> None:
        deadline = monotonic() + timeout_seconds
        while True:
            with self._lock:
                if not self.inflight_batches:
                    return
                remaining = deadline - monotonic()
                if remaining <= 0.0:
                    pending = sorted(self.inflight_batches)
                    raise TimeoutError(f"timed out waiting for batch ack: {pending}")
            self.drain_acks(timeout_ms=min(100, max(1, int(remaining * 1000))))
            self.republish_stale()

    def republish_stale(self) -> None:
        with self._lock:
            now = monotonic()
            republished = False
            for batch_id, batch in sorted(self.inflight_batches.items()):
                entry = self.ledger.entry(batch_id)
                if entry is None or entry.state == BatchState.DONE:
                    continue
                if batch.is_artifact:
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
            self._delete_artifact_if_safe(batch)
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

    def artifact_backpressure_active(self) -> bool:
        with self._lock:
            pending_bytes = self._pending_artifact_bytes_locked()
            active = pending_bytes >= self._artifact_backpressure_limit_locked()
            if active:
                self.artifact_backpressure_waits += 1
            return active

    def _artifact_backpressure_limit_locked(self) -> int:
        artifact_bytes = max(
            (
                int(batch.artifact_bytes or 0)
                for batch in self.inflight_batches.values()
                if batch.is_artifact
            ),
            default=max(self.max_batch_payload_bytes, 64 * 1024 * 1024),
        )
        role_metrics = (
            dict(self.role_metrics_provider())
            if self.role_metrics_provider is not None
            else {}
        )
        consumer_count = max(1, int(role_metrics.get("consumer_count", 1) or 1))
        return artifact_bytes * (2 * consumer_count + 2)

    def _record_artifact_pending_high_watermark_locked(self) -> None:
        pending_batches = sum(1 for batch in self.inflight_batches.values() if batch.is_artifact)
        pending_bytes = self._pending_artifact_bytes_locked()
        self.artifact_pending_batches_high_watermark = max(
            self.artifact_pending_batches_high_watermark,
            pending_batches,
        )
        self.artifact_pending_bytes_high_watermark = max(
            self.artifact_pending_bytes_high_watermark,
            pending_bytes,
        )

    def _pending_artifact_bytes_locked(self) -> int:
        return sum(
            int(batch.artifact_bytes or 0)
            for batch in self.inflight_batches.values()
            if batch.is_artifact
        )

    def start_background_ack_drain(self, *, timeout_ms: int = 50) -> None:
        if self._ack_drain_thread is not None:
            return
        if timeout_ms <= 0:
            raise ValueError("timeout_ms must be positive")
        stop_event = Event()
        self._ack_drain_stop = stop_event

        def run() -> None:
            while not stop_event.is_set():
                self.drain_acks(timeout_ms=timeout_ms)
                self.republish_stale()

        thread = Thread(
            target=run,
            name="cqpcfg-batch-ack-drain",
            daemon=True,
        )
        self._ack_drain_thread = thread
        thread.start()

    def stop_background_ack_drain(self) -> None:
        stop_event = self._ack_drain_stop
        thread = self._ack_drain_thread
        self._ack_drain_stop = None
        self._ack_drain_thread = None
        if stop_event is not None:
            stop_event.set()
        if thread is not None:
            thread.join(timeout=1.0)
        self.drain_acks(timeout_ms=0)

    def set_protocol_result(self, result) -> None:
        emitted = max(result.emitted_count, 1)
        requested_records = result.stats.scheduled_records
        requested_overgenerated_records = max(0, requested_records - result.emitted_count)
        overgenerated_records = max(0, result.received_records - result.emitted_count)
        worker_chunk_caps = tuple(cap for _, cap in result.worker_chunk_caps)
        self.protocol_metrics = {
            "emitted_records": result.emitted_count,
            "scheduler_scheduled_items": result.stats.scheduled_items,
            "scheduler_scheduled_records": result.stats.scheduled_records,
            "scheduler_requested_records": requested_records,
            "requested_overgenerated_records": requested_overgenerated_records,
            "requested_overgeneration_ratio": requested_overgenerated_records / emitted,
            "received_chunks": result.received_chunks,
            "received_records": result.received_records,
            "overgenerated_records": overgenerated_records,
            "overgeneration_ratio": overgenerated_records / emitted,
            "received_overgenerated_records": overgenerated_records,
            "received_overgeneration_ratio": overgenerated_records / emitted,
            "chunkstore_resident_records": result.stats.resident_records,
            "chunkstore_peak_resident_records": result.stats.peak_resident_records,
            "chunkstore_reclaimed_records": result.stats.reclaimed_records,
            "scheduler_affinity_hits": result.stats.affinity_hits,
            "scheduler_affinity_misses": result.stats.affinity_misses,
            "scheduler_parallel_items": result.stats.parallel_items,
            "scheduler_tail_steal_attempts": result.stats.tail_steal_attempts,
            "scheduler_tail_steals": result.stats.tail_steals,
            "scheduler_tail_steal_denials": result.stats.tail_steal_denials,
            "scheduler_rank_window_waits": result.stats.rank_window_waits,
            "scheduler_rank_window_forced_items": result.stats.rank_window_forced_items,
            "scheduler_rank_window_peak_outstanding_records": (
                result.stats.rank_window_peak_outstanding_records
            ),
            "expired_leases": result.expired_leases,
            "automatic_migrations": result.automatic_migrations,
            "failed_migration_triggers": result.failed_migration_triggers,
            "worker_chunk_cap_min": min(worker_chunk_caps) if worker_chunk_caps else 0,
            "worker_chunk_cap_max": max(worker_chunk_caps) if worker_chunk_caps else 0,
            "assigned_records_by_worker": [
                [str(worker_id), records]
                for worker_id, records in result.assigned_records_by_worker
            ],
            "received_records_by_worker": [
                [str(worker_id), records]
                for worker_id, records in result.received_records_by_worker
            ],
            "received_records_by_node_top": [
                [str(node_id), records]
                for node_id, records in sorted(
                    result.received_records_by_node,
                    key=lambda item: item[1],
                    reverse=True,
                )[:16]
            ],
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
        effective_batch_size, effective_max_payload_bytes = self._effective_batch_limits()
        started_at = perf_counter()
        write_json(
            self.metrics_path,
            {
                "role": "tracker",
                "configured_batch_size": self.batch_size,
                "effective_batch_size": effective_batch_size,
                "configured_max_batch_payload_bytes": self.max_batch_payload_bytes,
                "effective_max_batch_payload_bytes": effective_max_payload_bytes,
                "published_batches": self.published_batches,
                "published_candidates": self.published_candidates,
                "published_payload_bytes": self.published_payload_bytes,
                "candidate_artifact_batches": self.artifact_batches,
                "candidate_artifact_bytes": self.artifact_bytes,
                "candidate_artifact_write_seconds": self.artifact_write_seconds,
                "candidate_artifact_deleted": self.artifact_deleted,
                "candidate_artifact_delete_failures": self.artifact_delete_failures,
                "candidate_artifact_backpressure_waits": self.artifact_backpressure_waits,
                "candidate_artifact_backpressure_seconds": self.artifact_backpressure_seconds,
                "candidate_artifact_pending_bytes": self._pending_artifact_bytes_locked(),
                "candidate_artifact_pending_bytes_high_watermark": (
                    self.artifact_pending_bytes_high_watermark
                ),
                "candidate_artifact_pending_batches_high_watermark": (
                    self.artifact_pending_batches_high_watermark
                ),
                "candidate_rate": self.published_candidates / elapsed,
                **self._progress_metrics(elapsed),
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

    def _effective_batch_limits(self) -> tuple[int, int]:
        if self.batch_limits_provider is None:
            return self.batch_size, self.max_batch_payload_bytes
        limits = self.batch_limits_provider()
        return (
            min(self.batch_size, limits.batch_size),
            min(self.max_batch_payload_bytes, limits.max_payload_bytes),
        )

    def _record_progress(self, record: GuessRecord) -> None:
        elapsed = monotonic() - self.started_at
        next_rank_count = self.start_rank + len(self.current) + 1
        self.cumulative_probability_mass += max(0.0, record.prob)
        for marker in RANK_PROGRESS_MARKERS:
            if next_rank_count >= marker and marker not in self.time_to_rank_seconds:
                self.time_to_rank_seconds[marker] = elapsed
        for label, threshold in MASS_PROGRESS_MARKERS:
            if (
                self.cumulative_probability_mass >= threshold
                and label not in self.time_to_mass_seconds
            ):
                self.time_to_mass_seconds[label] = elapsed

    def _record_artifact_progress(
        self,
        *,
        record_count: int,
        probability_mass: float,
    ) -> None:
        elapsed = monotonic() - self.started_at
        start_rank = self.start_rank
        end_rank = start_rank + record_count
        for marker in RANK_PROGRESS_MARKERS:
            if start_rank < marker <= end_rank and marker not in self.time_to_rank_seconds:
                self.time_to_rank_seconds[marker] = elapsed
        previous_mass = self.cumulative_probability_mass
        self.cumulative_probability_mass += max(0.0, probability_mass)
        for label, threshold in MASS_PROGRESS_MARKERS:
            if (
                previous_mass < threshold <= self.cumulative_probability_mass
                and label not in self.time_to_mass_seconds
            ):
                self.time_to_mass_seconds[label] = elapsed

    def progress_metrics(self) -> dict[str, float | int]:
        return self._progress_metrics(max(monotonic() - self.started_at, 1e-12))

    def _progress_metrics(self, elapsed: float) -> dict[str, float | int]:
        metrics: dict[str, float | int] = {
            "cumulative_probability_mass": self.cumulative_probability_mass,
            "probability_mass_rate": self.cumulative_probability_mass / elapsed,
            "mass_coverage_per_second": self.cumulative_probability_mass / elapsed,
        }
        for marker in RANK_PROGRESS_MARKERS:
            value = self.time_to_rank_seconds.get(marker, 0.0)
            metrics[f"time_to_rank_{marker}_seconds"] = value
            metrics[f"rank_{marker}_reached"] = int(marker in self.time_to_rank_seconds)
        for label, _threshold in MASS_PROGRESS_MARKERS:
            value = self.time_to_mass_seconds.get(label, 0.0)
            metrics[f"time_to_mass_{label}_seconds"] = value
            metrics[f"mass_{label}_reached"] = int(label in self.time_to_mass_seconds)
        return metrics

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
