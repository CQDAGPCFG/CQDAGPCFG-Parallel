#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from threading import Event, Thread
from time import monotonic, perf_counter, sleep

from common import ensure_project_paths, read_json, write_json

ensure_project_paths()

from CQDAGPCFG import GuessRecord, load_model

from cqdagpcfg_parallel.adapters.cqdagpcfg import CQDAGBlockGraphAdapter
from cqdagpcfg_parallel.distributed import DistributedProtocolConfig, DistributedProtocolTracker
from cqdagpcfg_parallel.protocol import NodeSchedulingFeatures, SchedulerConfig
from cqdagpcfg_parallel.storage import (
    CompactDistributedTrackerCheckpointWriter,
    DistributedTrackerCheckpoint,
    FilePagedModelArtifactStore,
)
from cqdagpcfg_parallel.runtime import (
    BatchAck,
    BatchAckStatus,
    BatchRetryLedger,
    BatchState,
    CandidateBatch,
    DurableBatchCheckpoint,
    ZmqModelArtifactServer,
    ZmqPullBatchAckSource,
    guess_payload_bytes,
)
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint, ZmqPushBatchSink


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the CQDAGPCFG E2E tracker and publish CandidateBatch data.",
    )
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--model-id", default="cqdagpcfg-e2e-model")
    parser.add_argument("--model-serve-bind", default=None)
    parser.add_argument("--model-chunk-size", type=int, default=1 << 20)
    parser.add_argument("--model-slot-page-size", type=int, default=1024)
    parser.add_argument("--model-structure-page-size", type=int, default=4096)
    parser.add_argument("--targets-path", type=Path, required=True)
    parser.add_argument("--control-bind", default="cqpcfg://0.0.0.0:5555")
    parser.add_argument("--batch-bind", default=None)
    parser.add_argument("--batch-connect", default="cqpcfg://127.0.0.1:5556")
    parser.add_argument("--ack-bind", default="cqpcfg://0.0.0.0:5558")
    parser.add_argument("--consumer-count", type=int, default=1)
    parser.add_argument("--ack-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--ack-retry-interval-seconds", type=float, default=5.0)
    parser.add_argument("--batch-startup-grace-seconds", type=float, default=0.2)
    parser.add_argument("--expected-workers", type=int, default=None)
    parser.add_argument("--shutdown-grace-seconds", type=float, default=0.5)
    parser.add_argument("--metrics-path", type=Path, default=None)
    parser.add_argument("--metrics-flush-interval-seconds", type=float, default=0.25)
    parser.add_argument("--checkpoint-path", type=Path, default=None)
    parser.add_argument("--resume-checkpoint-path", type=Path, default=None)
    parser.add_argument("--checkpoint-stable-log-path", type=Path, default=None)
    parser.add_argument("--checkpoint-interval-records", type=int, default=1)
    parser.add_argument("--batch-checkpoint-path", type=Path, default=None)
    parser.add_argument("--resume-batch-checkpoint-path", type=Path, default=None)
    parser.add_argument("--source-mode", choices=("root", "structure"), default="root")
    parser.add_argument("--demand-window", type=int, default=8)
    parser.add_argument("--max-chunk-size", type=int, default=32)
    parser.add_argument("--max-parallel-leases-per-node", type=int, default=2)
    parser.add_argument("--disable-node-affinity", action="store_true")
    parser.add_argument("--node-affinity-bonus", type=float, default=0.5)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--max-batch-payload-bytes", type=int, default=4096)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--disable-reclaim", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.consumer_count <= 0:
        raise SystemExit("--consumer-count must be positive")
    if args.ack_timeout_seconds < 0.0:
        raise SystemExit("--ack-timeout-seconds cannot be negative")
    if args.ack_retry_interval_seconds <= 0.0:
        raise SystemExit("--ack-retry-interval-seconds must be positive")
    if args.batch_startup_grace_seconds < 0.0:
        raise SystemExit("--batch-startup-grace-seconds cannot be negative")
    if args.shutdown_grace_seconds < 0.0:
        raise SystemExit("--shutdown-grace-seconds cannot be negative")
    if args.metrics_flush_interval_seconds < 0.0:
        raise SystemExit("--metrics-flush-interval-seconds cannot be negative")
    if args.checkpoint_interval_records <= 0:
        raise SystemExit("--checkpoint-interval-records must be positive")
    if args.model_chunk_size <= 0:
        raise SystemExit("--model-chunk-size must be positive")
    if args.model_slot_page_size <= 0:
        raise SystemExit("--model-slot-page-size must be positive")
    if args.model_structure_page_size <= 0:
        raise SystemExit("--model-structure-page-size must be positive")
    targets = read_json(args.targets_path)
    limit = int(targets["limit"])
    model_server = start_model_artifact_server(args)
    model = load_model(args.model_path)
    adapter = CQDAGBlockGraphAdapter(model)
    if args.source_mode == "structure":
        protocol_nodes = adapter.structure_nodes()
        node_ids = tuple(node.node_id for node in protocol_nodes)
        node_features = adapter.scheduling_features()
    else:
        root_node = adapter.root_node
        protocol_nodes = (root_node,)
        node_ids = (root_node.node_id,)
        node_features = (
            NodeSchedulingFeatures(
                node_id=root_node.node_id,
                entropy=root_node.slot_dispersion,
                priority=root_node.priority,
                estimated_cost=root_node.estimated_cost,
            ),
        )

    config = DistributedProtocolConfig(
        scheduler=SchedulerConfig(
            max_chunk_size=args.max_chunk_size,
            max_parallel_leases_per_node=args.max_parallel_leases_per_node,
            node_affinity_enabled=not args.disable_node_affinity,
            node_affinity_bonus=args.node_affinity_bonus,
        ),
        node_ids=node_ids,
        node_features=node_features,
        demand_window=args.demand_window,
        record_order_key=adapter.serial_order_key,
        reclaim_emitted_chunks=not args.disable_reclaim,
        model_fingerprint=targets.get("model_fingerprint"),
    )
    tracker = DistributedProtocolTracker(
        endpoint=ZmqEndpoint.from_uri(args.control_bind, bind=True),
        config=config,
    )
    resume_checkpoint = (
        DistributedTrackerCheckpoint.read(args.resume_checkpoint_path)
        if args.resume_checkpoint_path is not None
        else None
    )
    resume_batch_checkpoint = (
        DurableBatchCheckpoint.read(args.resume_batch_checkpoint_path)
        if args.resume_batch_checkpoint_path is not None
        else None
    )
    checkpoint_writer = None
    if args.checkpoint_path is not None:
        if args.checkpoint_stable_log_path is None:
            checkpoint_writer = lambda checkpoint: checkpoint.write_atomic(args.checkpoint_path)
        else:
            checkpoint_writer = CompactDistributedTrackerCheckpointWriter(
                checkpoint_path=args.checkpoint_path,
                stable_log_path=args.checkpoint_stable_log_path,
            )

    batch_endpoint = (
        ZmqEndpoint.from_uri(args.batch_bind, bind=True, linger_ms=1000)
        if args.batch_bind is not None
        else ZmqEndpoint.from_uri(args.batch_connect, bind=False, linger_ms=1000)
    )
    ack_endpoint = ZmqEndpoint.from_uri(args.ack_bind, bind=True, linger_ms=1000)
    try:
        with ZmqPushBatchSink(batch_endpoint) as sink, ZmqPullBatchAckSource(ack_endpoint) as ack_source:
            sink.open()
            ack_source.open()
            if args.batch_startup_grace_seconds:
                sleep(args.batch_startup_grace_seconds)

            publisher = StreamingRecordBatchPublisher(
                sink,
                ack_source=ack_source,
                batch_size=args.batch_size,
                max_batch_payload_bytes=args.max_batch_payload_bytes,
                ack_retry_interval_seconds=args.ack_retry_interval_seconds,
                metrics_path=args.metrics_path,
                metrics_flush_interval_seconds=args.metrics_flush_interval_seconds,
                initial_start_rank=0 if resume_checkpoint is None else resume_checkpoint.emitted_count,
                initial_batch_id=0 if resume_checkpoint is None else resume_checkpoint.emitted_count,
                batch_checkpoint_path=args.batch_checkpoint_path,
                resume_batch_checkpoint=resume_batch_checkpoint,
            )
            publisher.republish_pending()

            def checkpoint_callback(checkpoint: DistributedTrackerCheckpoint) -> None:
                if checkpoint_writer is None:
                    return
                publisher.flush()
                checkpoint_writer(checkpoint)

            started_at = monotonic()
            result = tracker.run(
                limit=limit,
                expected_workers=args.expected_workers,
                timeout_seconds=args.timeout_seconds,
                shutdown_grace_seconds=args.shutdown_grace_seconds,
                output_callback=publisher.publish,
                collect_outputs=False,
                resume_checkpoint=resume_checkpoint,
                checkpoint_callback=checkpoint_callback if checkpoint_writer is not None else None,
                checkpoint_interval_records=args.checkpoint_interval_records,
            )
            elapsed = monotonic() - started_at

            if result.digest != targets["serial_digest"]:
                raise SystemExit("distributed output did not match prepared serial digest")

            publisher.set_protocol_result(result)
            publisher.flush()
            publisher.wait_for_acks(timeout_seconds=args.ack_timeout_seconds)
            sink.publish_end_of_stream(args.consumer_count)
            publisher.write_metrics(final=True)
    finally:
        if model_server is not None:
            stop_model_artifact_server(model_server)

    print("tracker completed CQDAGPCFG E2E generation")
    print(f"  limit             : {limit}")
    print(f"  source mode       : {args.source_mode}")
    print(f"  protocol nodes    : {len(protocol_nodes)}")
    print(f"  expected workers  : {args.expected_workers}")
    print(f"  hash consumers    : {args.consumer_count}")
    print(f"  digest            : {result.digest}")
    print(f"  emitted records   : {result.emitted_count}")
    print(f"  collected outputs : {len(result.outputs)}")
    print(f"  resident records  : {result.stats.resident_records}")
    print(f"  peak resident     : {result.stats.peak_resident_records}")
    print(f"  reclaimed records : {result.stats.reclaimed_records}")
    print(f"  affinity hits     : {result.stats.affinity_hits}")
    print(f"  affinity misses   : {result.stats.affinity_misses}")
    print(f"  elapsed seconds   : {elapsed:.6f}")
    print("  assigned records  :")
    for node_id, count in result.assigned_records_by_node:
        print(f"    {node_id}: {count}")


def start_model_artifact_server(args: argparse.Namespace):
    if args.model_serve_bind is None:
        return None
    store = FilePagedModelArtifactStore.from_path(
        args.model_path,
        model_id=args.model_id,
        chunk_size=args.model_chunk_size,
        slot_page_size=args.model_slot_page_size,
        structure_page_size=args.model_structure_page_size,
    )
    endpoint = ZmqEndpoint.from_uri(args.model_serve_bind, bind=True, linger_ms=0)
    stop_event = Event()
    ready_event = Event()
    failures: list[BaseException] = []

    def serve() -> None:
        try:
            with ZmqModelArtifactServer(endpoint, store) as server:
                ready_event.set()
                while not stop_event.is_set():
                    server.serve_once(timeout_ms=100)
        except BaseException as exc:
            failures.append(exc)
            ready_event.set()

    thread = Thread(target=serve, name="cqdagpcfg-model-artifact-server", daemon=True)
    thread.start()
    if not ready_event.wait(timeout=5.0):
        raise RuntimeError("model artifact server did not start")
    if failures:
        raise RuntimeError(f"model artifact server failed: {failures[0]}")
    return stop_event, thread


def stop_model_artifact_server(handle) -> None:
    stop_event, thread = handle
    stop_event.set()
    thread.join(timeout=2.0)


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
        self.ack_retry_interval_seconds = ack_retry_interval_seconds
        self.metrics_path = metrics_path
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
        self.protocol_metrics: dict[str, int] = {}
        self.metrics_write_count = 0
        self.metrics_write_seconds = 0.0
        self._write_batch_checkpoint()
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
            self.ledger.complete(ack.batch_id, consumer_id=ack.consumer_id)
            del self.inflight_batches[ack.batch_id]
            self.last_publish_at_by_batch.pop(ack.batch_id, None)
            self.completed_batches += 1
            self._write_batch_checkpoint()
            return

        self.ledger.fail(ack.batch_id, consumer_id=ack.consumer_id)
        self.ack_failures += 1
        self.sink.publish(batch)
        self.last_publish_at_by_batch[ack.batch_id] = monotonic()
        self.republished_batches += 1
        self._write_batch_checkpoint()

    def set_protocol_result(self, result) -> None:
        self.protocol_metrics = {
            "emitted_records": result.emitted_count,
            "collected_outputs": len(result.outputs),
            "chunkstore_resident_records": result.stats.resident_records,
            "chunkstore_peak_resident_records": result.stats.peak_resident_records,
            "chunkstore_reclaimed_records": result.stats.reclaimed_records,
            "scheduler_affinity_hits": result.stats.affinity_hits,
            "scheduler_affinity_misses": result.stats.affinity_misses,
            "expired_leases": result.expired_leases,
            "automatic_migrations": result.automatic_migrations,
            "failed_migration_triggers": result.failed_migration_triggers,
        }

    def write_metrics(self, *, final: bool) -> None:
        if self.metrics_path is None:
            return
        now = monotonic()
        if not final and now < self.next_metrics_write_at:
            return
        self.next_metrics_write_at = now + self.metrics_flush_interval_seconds
        elapsed = max(monotonic() - self.started_at, 1e-12)
        network = self.sink.stats
        ack_network = self.ack_source.stats
        ledger = self.ledger.stats
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


if __name__ == "__main__":
    main()
