from __future__ import annotations

from dataclasses import dataclass
from threading import Lock, Thread
from time import sleep
from typing import Iterable

from CQDAGPCFG import GuessRecord

from .batching import make_candidate_batches
from .candidate_queue import BoundedCandidateQueue


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    batch_size: int
    max_batch_payload_bytes: int
    max_pending_batches: int
    max_pending_candidates: int
    max_pending_payload_bytes: int
    consumer_count: int = 1
    collect_outputs: bool = False
    consumer_delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.max_batch_payload_bytes <= 0:
            raise ValueError("max_batch_payload_bytes must be positive")
        if self.consumer_count <= 0:
            raise ValueError("consumer_count must be positive")
        if self.consumer_delay_seconds < 0.0:
            raise ValueError("consumer_delay_seconds cannot be negative")


@dataclass(slots=True)
class PipelineStats:
    produced_batches: int = 0
    produced_candidates: int = 0
    consumed_batches: int = 0
    consumed_candidates: int = 0
    peak_pending_batches: int = 0
    peak_pending_candidates: int = 0
    peak_pending_payload_bytes: int = 0
    peak_inflight_batches: int = 0
    peak_inflight_payload_bytes: int = 0
    producer_waits: int = 0
    duplicate_batches: int = 0
    completed_batch_ids: tuple[int, ...] = ()
    collected_stable_records: tuple[str, ...] = ()


def run_candidate_pipeline(
    records: Iterable[GuessRecord],
    config: PipelineConfig,
) -> PipelineStats:
    queue = BoundedCandidateQueue(
        max_pending_batches=config.max_pending_batches,
        max_pending_candidates=config.max_pending_candidates,
        max_pending_payload_bytes=config.max_pending_payload_bytes,
    )
    errors: list[BaseException] = []
    errors_lock = Lock()
    completed_ids: list[int] = []
    collected_stable_records: list[str] = []
    completed_ids_seen: set[int] = set()
    consumed_batches = 0
    consumed_candidates = 0
    duplicate_batches = 0
    inflight_batches = 0
    inflight_payload_bytes = 0
    peak_inflight_batches = 0
    peak_inflight_payload_bytes = 0
    consume_lock = Lock()

    def capture_error(exc: BaseException) -> None:
        with errors_lock:
            errors.append(exc)

    def producer() -> None:
        try:
            for batch in make_candidate_batches(
                records,
                batch_size=config.batch_size,
                max_batch_payload_bytes=config.max_batch_payload_bytes,
            ):
                queue.put(batch)
        except BaseException as exc:  # pragma: no cover - re-raised below
            capture_error(exc)
        finally:
            queue.close()

    def consumer() -> None:
        nonlocal consumed_batches, consumed_candidates, duplicate_batches
        nonlocal inflight_batches, inflight_payload_bytes
        nonlocal peak_inflight_batches, peak_inflight_payload_bytes
        try:
            while True:
                batch = queue.get()
                if batch is None:
                    return
                with consume_lock:
                    inflight_batches += 1
                    inflight_payload_bytes += batch.payload_bytes
                    peak_inflight_batches = max(peak_inflight_batches, inflight_batches)
                    peak_inflight_payload_bytes = max(
                        peak_inflight_payload_bytes,
                        inflight_payload_bytes,
                    )
                if config.consumer_delay_seconds:
                    sleep(config.consumer_delay_seconds)
                with consume_lock:
                    consumed_batches += 1
                    consumed_candidates += len(batch.records)
                    if batch.batch_id in completed_ids_seen:
                        duplicate_batches += 1
                    completed_ids_seen.add(batch.batch_id)
                    completed_ids.append(batch.batch_id)
                    if config.collect_outputs:
                        collected_stable_records.extend(
                            record.stable_string() for record in batch.records
                        )
                    inflight_batches -= 1
                    inflight_payload_bytes -= batch.payload_bytes
        except BaseException as exc:  # pragma: no cover - re-raised below
            capture_error(exc)

    producer_thread = Thread(target=producer, name="candidate-producer")
    consumer_threads = [
        Thread(target=consumer, name=f"mock-consumer-{index}")
        for index in range(config.consumer_count)
    ]

    producer_thread.start()
    for thread in consumer_threads:
        thread.start()
    producer_thread.join()
    for thread in consumer_threads:
        thread.join()

    if errors:
        raise RuntimeError("candidate pipeline failed") from errors[0]

    queue_stats = queue.stats
    produced_candidates = consumed_candidates
    produced_batches = len(completed_ids_seen)

    return PipelineStats(
        produced_batches=produced_batches,
        produced_candidates=produced_candidates,
        consumed_batches=consumed_batches,
        consumed_candidates=consumed_candidates,
        peak_pending_batches=queue_stats.peak_pending_batches,
        peak_pending_candidates=queue_stats.peak_pending_candidates,
        peak_pending_payload_bytes=queue_stats.peak_pending_payload_bytes,
        peak_inflight_batches=peak_inflight_batches,
        peak_inflight_payload_bytes=peak_inflight_payload_bytes,
        producer_waits=queue_stats.producer_waits,
        duplicate_batches=duplicate_batches,
        completed_batch_ids=tuple(completed_ids),
        collected_stable_records=tuple(collected_stable_records),
    )


__all__ = [
    "PipelineConfig",
    "PipelineStats",
    "run_candidate_pipeline",
]
