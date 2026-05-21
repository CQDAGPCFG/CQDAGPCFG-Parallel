from __future__ import annotations

import json
import logging
from dataclasses import dataclass, replace
from inspect import signature
from pathlib import Path
from threading import Event, Thread
from time import monotonic, perf_counter, sleep
from typing import Any, Callable, Mapping

from CQDAGPCFG import GuessRecord, load_model

from cqdagpcfg_parallel.distributed import (
    DistributedProtocolConfig,
    DistributedProtocolTracker,
    JobContext,
    RoleController,
    RoleResourcePolicy,
    WorkerResourceSpec,
    parse_byte_size,
)
from cqdagpcfg_parallel.framework_logging import (
    configure_framework_logging,
    log_event,
)
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
from cqdagpcfg_parallel.runtime.zmq_transport import (
    ZmqEndpoint,
    ZmqEndpointBundle,
    ZmqPushBatchSink,
)

from .block_graph import CQDAGBlockGraphAdapter


LOGGER = logging.getLogger("cqdagpcfg.tracker")


@dataclass(slots=True)
class CqdagTrackerServiceConfig:
    model_path: Path
    targets_path: Path
    model_id: str = "cqdagpcfg-e2e-model"
    model_serve_bind: str | None = None
    model_chunk_size: int = 1 << 20
    model_slot_page_size: int = 1024
    model_structure_page_size: int = 4096
    bind: str | None = None
    advertise_host: str = "127.0.0.1"
    control_bind: str = "cqpcfg://0.0.0.0:5555"
    public_control_connect: str | None = None
    batch_bind: str | None = None
    batch_connect: str = "cqpcfg://127.0.0.1:5556"
    public_batch_connect: str | None = None
    ack_bind: str = "cqpcfg://0.0.0.0:5558"
    public_ack_connect: str | None = None
    public_model_connect: str | None = None
    role_bind: str | None = None
    total_nodes: int | None = None
    min_generators: int = 1
    min_consumers: int = 1
    initial_generators: int | None = None
    initial_consumers: int | None = None
    late_worker_role: str = "generator"
    generator_min_cpus: float | None = None
    generator_min_memory: str | None = None
    generator_min_gpus: int | None = None
    consumer_min_cpus: float | None = None
    consumer_min_memory: str | None = None
    consumer_min_gpus: int | None = None
    consumer_count: int | None = None
    ack_timeout_seconds: float = 30.0
    ack_retry_interval_seconds: float = 5.0
    batch_startup_grace_seconds: float = 0.2
    expected_workers: int | None = None
    shutdown_grace_seconds: float = 0.5
    metrics_path: Path | None = None
    metrics_flush_interval_seconds: float = 0.25
    checkpoint_path: Path | None = None
    resume_checkpoint_path: Path | None = None
    checkpoint_stable_log_path: Path | None = None
    checkpoint_interval_records: int = 1
    batch_checkpoint_path: Path | None = None
    resume_batch_checkpoint_path: Path | None = None
    source_mode: str = "root"
    demand_window: int = 8
    max_chunk_size: int = 32
    max_parallel_leases_per_node: int = 2
    disable_node_affinity: bool = False
    node_affinity_bonus: float = 0.5
    batch_size: int = 16
    max_batch_payload_bytes: int = 4096
    timeout_seconds: float = 3600.0
    disable_reclaim: bool = False

    def __post_init__(self) -> None:
        if self.consumer_count is not None and self.consumer_count <= 0:
            raise ValueError("consumer_count must be positive")
        if self.min_generators <= 0:
            raise ValueError("min_generators must be positive")
        if self.min_consumers <= 0:
            raise ValueError("min_consumers must be positive")
        if self.ack_timeout_seconds < 0.0:
            raise ValueError("ack_timeout_seconds cannot be negative")
        if self.ack_retry_interval_seconds <= 0.0:
            raise ValueError("ack_retry_interval_seconds must be positive")
        if self.batch_startup_grace_seconds < 0.0:
            raise ValueError("batch_startup_grace_seconds cannot be negative")
        if self.shutdown_grace_seconds < 0.0:
            raise ValueError("shutdown_grace_seconds cannot be negative")
        if self.metrics_flush_interval_seconds < 0.0:
            raise ValueError("metrics_flush_interval_seconds cannot be negative")
        if self.checkpoint_interval_records <= 0:
            raise ValueError("checkpoint_interval_records must be positive")
        if self.model_chunk_size <= 0:
            raise ValueError("model_chunk_size must be positive")
        if self.model_slot_page_size <= 0:
            raise ValueError("model_slot_page_size must be positive")
        if self.model_structure_page_size <= 0:
            raise ValueError("model_structure_page_size must be positive")
        if self.late_worker_role not in {"generator", "consumer", "idle"}:
            raise ValueError("late_worker_role must be generator, consumer, or idle")
        if self.source_mode not in {"root", "structure"}:
            raise ValueError("source_mode must be root or structure")


@dataclass(frozen=True, slots=True)
class CQDAGPCFGTrackerJob:
    model_path: Path
    targets_path: Path
    model_id: str
    limit: int
    target_count: int
    source_mode: str
    control_bind: str
    batch_bind: str | None
    role_bind: str | None
    ack_bind: str
    model_serve_bind: str | None
    expected_workers: int | None
    total_nodes: int | None
    model_fingerprint: str | None


@dataclass(frozen=True, slots=True)
class CQDAGPCFGTrackerSummary:
    limit: int
    source_mode: str
    protocol_nodes: int
    expected_workers: int | None
    hash_consumers: int
    digest: str
    serial_digest: str
    emitted_records: int
    collected_outputs: int
    resident_records: int
    peak_resident_records: int
    reclaimed_records: int
    affinity_hits: int
    affinity_misses: int
    elapsed_seconds: float
    assigned_records_by_node: tuple[tuple[Any, int], ...]


@dataclass(frozen=True, slots=True)
class CQDAGPCFGNodeEvent:
    node_id: str
    role: str
    reason: str
    current_role: str | None = None
    resources: WorkerResourceSpec = WorkerResourceSpec()


@dataclass(frozen=True, slots=True)
class CQDAGPCFGRoleChangeEvent:
    node_id: str
    previous_role: str | None
    new_role: str
    reason: str
    current_role: str | None = None
    resources: WorkerResourceSpec = WorkerResourceSpec()


@dataclass(frozen=True, slots=True)
class CQDAGPCFGMemorySnapshot:
    emitted_records: int
    resident_records: int
    peak_resident_records: int
    reclaimed_records: int
    published_batches: int
    published_candidates: int
    published_payload_bytes: int
    pending_batches: int
    max_batch_payload_bytes: int
    model_chunk_size: int
    model_slot_page_size: int
    model_structure_page_size: int


@dataclass(frozen=True, slots=True)
class CQDAGPCFGCheckpointEvent:
    emitted_count: int
    checkpoint_path: Path | None
    stable_log_path: Path | None
    batch_checkpoint_path: Path | None


@dataclass(frozen=True, slots=True)
class CQDAGPCFGBatchRetryEvent:
    batch_id: int
    start_rank: int
    end_rank: int
    reason: str
    attempts: int
    pending_batches: int
    consumer_id: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class CQDAGPCFGTrackerError:
    stage: str
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class CQDAGPCFGTracker:
    config: CqdagTrackerServiceConfig
    tracker_class: type[Any] | None = None

    def run(self) -> None:
        _run_cqdag_tracker_service(self.config, tracker_class=self.tracker_class)


@dataclass(frozen=True, slots=True)
class AnnotatedCQDAGPCFGTracker:
    tracker_class: type[Any]
    config: CqdagTrackerServiceConfig

    def build(self) -> CQDAGPCFGTracker:
        return CQDAGPCFGTracker(
            config=self.config,
            tracker_class=self.tracker_class,
        )

    def run(self) -> None:
        self.build().run()


def cqdagpcfg_tracker(
    config: CqdagTrackerServiceConfig | None = None,
    **overrides,
) -> Callable[[type[Any]], AnnotatedCQDAGPCFGTracker]:
    if config is None:
        resolved_config = CqdagTrackerServiceConfig(**overrides)
    elif overrides:
        resolved_config = replace(config, **overrides)
    else:
        resolved_config = config

    def decorator(tracker_class: type[Any]) -> AnnotatedCQDAGPCFGTracker:
        if not isinstance(tracker_class, type):
            raise TypeError("@cqdagpcfg_tracker must decorate a class")
        return AnnotatedCQDAGPCFGTracker(
            tracker_class=tracker_class,
            config=resolved_config,
        )

    return decorator


def _run_cqdag_tracker_service(
    config: CqdagTrackerServiceConfig,
    *,
    tracker_class: type[Any] | None = None,
) -> None:
    configure_framework_logging()
    args = replace(config)
    apply_endpoint_bundle(args)
    tracker_hooks = _instantiate_tracker_hooks(tracker_class, args)
    targets = read_json(args.targets_path)
    limit = int(targets["limit"])
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.start",
        model_path=args.model_path,
        targets_path=args.targets_path,
        model_id=args.model_id,
        limit=limit,
        source_mode=args.source_mode,
        control_bind=args.control_bind,
        batch_bind=args.batch_bind,
        role_bind=args.role_bind,
        ack_bind=args.ack_bind,
        model_serve_bind=args.model_serve_bind,
        expected_workers=args.expected_workers,
        total_nodes=args.total_nodes,
        target_count=len(targets.get("targets", ())),
        model_fingerprint=targets.get("model_fingerprint"),
    )
    _call_tracker_hook(
        tracker_hooks,
        "on_start",
        CQDAGPCFGTrackerJob(
            model_path=args.model_path,
            targets_path=args.targets_path,
            model_id=args.model_id,
            limit=limit,
            target_count=len(targets.get("targets", ())),
            source_mode=args.source_mode,
            control_bind=args.control_bind,
            batch_bind=args.batch_bind,
            role_bind=args.role_bind,
            ack_bind=args.ack_bind,
            model_serve_bind=args.model_serve_bind,
            expected_workers=args.expected_workers,
            total_nodes=args.total_nodes,
            model_fingerprint=targets.get("model_fingerprint"),
        ),
    )
    model_server = start_model_artifact_server(args)
    role_controller = start_role_controller(args, targets, tracker_hooks=tracker_hooks)
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
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.protocol_ready",
        protocol_nodes=len(protocol_nodes),
        source_mode=args.source_mode,
        demand_window=args.demand_window,
        max_chunk_size=args.max_chunk_size,
        max_parallel_leases_per_node=args.max_parallel_leases_per_node,
        node_affinity_enabled=not args.disable_node_affinity,
        reclaim_enabled=not args.disable_reclaim,
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
            def checkpoint_writer(checkpoint):
                checkpoint.write_atomic(args.checkpoint_path)
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

            expected_workers = effective_expected_workers(args, role_controller)
            publisher = StreamingRecordBatchPublisher(
                sink,
                ack_source=ack_source,
                batch_size=args.batch_size,
                max_batch_payload_bytes=args.max_batch_payload_bytes,
                ack_retry_interval_seconds=args.ack_retry_interval_seconds,
                batch_retry_callback=lambda event: _call_tracker_hook(
                    tracker_hooks,
                    "on_batch_retry",
                    event,
                ),
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
                log_event(
                    LOGGER,
                    logging.INFO,
                    "tracker.checkpoint",
                    emitted_count=checkpoint.emitted_count,
                    checkpoint_path=args.checkpoint_path,
                    stable_log_path=args.checkpoint_stable_log_path,
                    batch_checkpoint_path=args.batch_checkpoint_path,
                )
                _call_tracker_hook(
                    tracker_hooks,
                    "on_checkpoint",
                    CQDAGPCFGCheckpointEvent(
                        emitted_count=checkpoint.emitted_count,
                        checkpoint_path=args.checkpoint_path,
                        stable_log_path=args.checkpoint_stable_log_path,
                        batch_checkpoint_path=args.batch_checkpoint_path,
                    ),
                )

            started_at = monotonic()
            result = tracker.run(
                limit=limit,
                expected_workers=expected_workers,
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
                log_event(
                    LOGGER,
                    logging.ERROR,
                    "tracker.digest_mismatch",
                    digest=result.digest,
                    serial_digest=targets["serial_digest"],
                    emitted_records=result.emitted_count,
                )
                _call_tracker_hook(
                    tracker_hooks,
                    "on_error",
                    CQDAGPCFGTrackerError(
                        stage="digest_verification",
                        error_type="RuntimeError",
                        message="distributed output did not match prepared serial digest",
                    ),
                )
                raise RuntimeError("distributed output did not match prepared serial digest")

            publisher.set_protocol_result(result)
            publisher.flush()
            publisher.wait_for_acks(timeout_seconds=args.ack_timeout_seconds)
            hash_consumers = end_of_stream_consumer_count(args, role_controller)
            sink.publish_end_of_stream(hash_consumers)
            publisher.write_metrics(final=True)
            log_event(
                LOGGER,
                logging.INFO,
                "tracker.batch_stream_drained",
                published_batches=publisher.published_batches,
                published_candidates=publisher.published_candidates,
                published_payload_bytes=publisher.published_payload_bytes,
                republished_batches=publisher.republished_batches,
                completed_batches=publisher.completed_batches,
                hash_consumers=hash_consumers,
            )
    finally:
        if role_controller is not None:
            stop_role_controller(role_controller, tracker_hooks=tracker_hooks)
        if model_server is not None:
            stop_model_artifact_server(model_server)

    _call_tracker_hook(
        tracker_hooks,
        "on_memory_snapshot",
        CQDAGPCFGMemorySnapshot(
            emitted_records=result.emitted_count,
            resident_records=result.stats.resident_records,
            peak_resident_records=result.stats.peak_resident_records,
            reclaimed_records=result.stats.reclaimed_records,
            published_batches=publisher.published_batches,
            published_candidates=publisher.published_candidates,
            published_payload_bytes=publisher.published_payload_bytes,
            pending_batches=len(publisher.inflight_batches),
            max_batch_payload_bytes=args.max_batch_payload_bytes,
            model_chunk_size=args.model_chunk_size,
            model_slot_page_size=args.model_slot_page_size,
            model_structure_page_size=args.model_structure_page_size,
        ),
    )
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.memory_snapshot",
        emitted_records=result.emitted_count,
        resident_records=result.stats.resident_records,
        peak_resident_records=result.stats.peak_resident_records,
        reclaimed_records=result.stats.reclaimed_records,
        published_payload_bytes=publisher.published_payload_bytes,
        pending_batches=len(publisher.inflight_batches),
    )
    summary = CQDAGPCFGTrackerSummary(
        limit=limit,
        source_mode=args.source_mode,
        protocol_nodes=len(protocol_nodes),
        expected_workers=expected_workers,
        hash_consumers=hash_consumers,
        digest=result.digest,
        serial_digest=targets["serial_digest"],
        emitted_records=result.emitted_count,
        collected_outputs=len(result.outputs),
        resident_records=result.stats.resident_records,
        peak_resident_records=result.stats.peak_resident_records,
        reclaimed_records=result.stats.reclaimed_records,
        affinity_hits=result.stats.affinity_hits,
        affinity_misses=result.stats.affinity_misses,
        elapsed_seconds=elapsed,
        assigned_records_by_node=result.assigned_records_by_node,
    )
    _call_tracker_hook(tracker_hooks, "on_complete", summary)
    _print_tracker_summary(summary)


def _instantiate_tracker_hooks(
    tracker_class: type[Any] | None,
    config: CqdagTrackerServiceConfig,
):
    if tracker_class is None:
        return None
    parameters = tuple(signature(tracker_class).parameters)
    if len(parameters) == 0:
        return tracker_class()
    if len(parameters) == 1:
        return tracker_class(config)
    raise TypeError(
        "@cqdagpcfg_tracker class constructor must accept no arguments "
        "or one config argument"
    )


def _call_tracker_hook(instance, name: str, payload: object) -> None:
    if instance is None:
        return
    hook = getattr(instance, name, None)
    if hook is None:
        return
    if not callable(hook):
        raise TypeError(f"tracker hook {name} must be callable")
    parameters = tuple(signature(hook).parameters)
    try:
        if len(parameters) == 0:
            hook()
            return
        if len(parameters) == 1:
            hook(payload)
            return
    except BaseException:
        LOGGER.exception("event=tracker.hook_failed hook=%s", name)
        raise
    raise TypeError(f"tracker hook {name} must accept zero or one argument")


def _print_tracker_summary(summary: CQDAGPCFGTrackerSummary) -> None:
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.complete",
        limit=summary.limit,
        source_mode=summary.source_mode,
        protocol_nodes=summary.protocol_nodes,
        expected_workers=summary.expected_workers,
        hash_consumers=summary.hash_consumers,
        digest=summary.digest,
        digest_match=summary.digest == summary.serial_digest,
        emitted_records=summary.emitted_records,
        collected_outputs=summary.collected_outputs,
        resident_records=summary.resident_records,
        peak_resident_records=summary.peak_resident_records,
        reclaimed_records=summary.reclaimed_records,
        affinity_hits=summary.affinity_hits,
        affinity_misses=summary.affinity_misses,
        elapsed_seconds=f"{summary.elapsed_seconds:.6f}",
    )
    for node_id, count in summary.assigned_records_by_node:
        log_event(
            LOGGER,
            logging.DEBUG,
            "tracker.assigned_records",
            node_id=node_id,
            count=count,
        )


def start_model_artifact_server(args: CqdagTrackerServiceConfig):
    if args.model_serve_bind is None:
        return None
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.model_server_starting",
        endpoint=args.model_serve_bind,
        model_path=args.model_path,
        model_id=args.model_id,
        chunk_size=args.model_chunk_size,
        slot_page_size=args.model_slot_page_size,
        structure_page_size=args.model_structure_page_size,
    )
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
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.model_server_ready",
        endpoint=args.model_serve_bind,
        model_id=args.model_id,
    )
    return stop_event, thread


def stop_model_artifact_server(handle) -> None:
    stop_event, thread = handle
    stop_event.set()
    thread.join(timeout=2.0)
    log_event(LOGGER, logging.INFO, "tracker.model_server_stopped")


def start_role_controller(
    args: CqdagTrackerServiceConfig,
    targets: dict,
    *,
    tracker_hooks=None,
):
    if args.role_bind is None:
        return None
    if args.model_serve_bind is None and args.public_model_connect is None:
        raise ValueError("role_bind requires model_serve_bind or public_model_connect")

    generator_count, consumer_count = resolve_initial_role_counts(args)
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.role_controller_starting",
        endpoint=args.role_bind,
        initial_generators=generator_count,
        initial_consumers=consumer_count,
        late_worker_role=args.late_worker_role,
    )
    job_context = JobContext.from_targets_payload(
        targets,
        job_id=args.model_id,
        model_id=args.model_id,
        model_connect=args.public_model_connect
        or advertised_connect_uri(args.model_serve_bind, args.advertise_host),
        control_connect=args.public_control_connect
        or advertised_connect_uri(args.control_bind, args.advertise_host),
        batch_connect=args.public_batch_connect
        or advertised_connect_uri(args.batch_bind or args.batch_connect, args.advertise_host),
        ack_connect=args.public_ack_connect
        or advertised_connect_uri(args.ack_bind, args.advertise_host),
        source_mode=args.source_mode,
        demand_window=args.demand_window,
    )
    controller = RoleController(
        endpoint=ZmqEndpoint.from_uri(args.role_bind, bind=True, linger_ms=0),
        roles={},
        auto_assign_roles=(
            ("consumer",) * consumer_count + ("generator",) * generator_count
        ),
        default_role=args.late_worker_role,
        assign_default_role=True,
        resource_policy=role_resource_policy(args),
        job_context=job_context,
    )
    stop_event = Event()
    known_nodes: set[str] = set()
    last_role_by_node: dict[str, str] = {}
    hook_failures: list[BaseException] = []

    def serve() -> None:
        while not stop_event.is_set():
            try:
                controller.poll(timeout_ms=100)
                _emit_role_controller_events(
                    controller,
                    tracker_hooks=tracker_hooks,
                    known_nodes=known_nodes,
                    last_role_by_node=last_role_by_node,
                )
            except BaseException as exc:
                hook_failures.append(exc)
                stop_event.set()

    thread = Thread(target=serve, name="cqdagpcfg-role-controller", daemon=True)
    thread.start()
    return {
        "controller": controller,
        "generator_count": generator_count,
        "consumer_count": consumer_count,
        "stop_event": stop_event,
        "thread": thread,
        "known_nodes": known_nodes,
        "last_role_by_node": last_role_by_node,
        "hook_failures": hook_failures,
    }


def stop_role_controller(handle, *, tracker_hooks=None) -> None:
    controller = handle["controller"]
    controller.set_stop(True)
    deadline = monotonic() + 1.0
    while monotonic() < deadline:
        sleep(0.05)
    handle["stop_event"].set()
    handle["thread"].join(timeout=2.0)
    for node_id in sorted(handle["known_nodes"]):
        status = controller.status_by_node.get(node_id, {})
        role = handle["last_role_by_node"].get(node_id, "idle")
        current_role = _status_current_role(status)
        log_event(
            LOGGER,
            logging.INFO,
            "tracker.node_leave",
            node_id=node_id,
            assigned_role=role,
            current_role=current_role,
            reason="controller_stop",
        )
        _call_tracker_hook(
            tracker_hooks,
            "on_node_leave",
            CQDAGPCFGNodeEvent(
                node_id=node_id,
                role=role,
                current_role=current_role,
                reason="controller_stop",
                resources=_status_resources(status),
            ),
        )
    controller.close()
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.role_controller_stopped",
        observed_nodes=len(handle["known_nodes"]),
    )
    if handle["hook_failures"]:
        failure = handle["hook_failures"][0]
        _call_tracker_hook(
            tracker_hooks,
            "on_error",
            CQDAGPCFGTrackerError(
                stage="role_controller",
                error_type=type(failure).__name__,
                message=str(failure),
            ),
        )
        raise RuntimeError(f"role controller failed: {failure}")


def _emit_role_controller_events(
    controller,
    *,
    tracker_hooks,
    known_nodes: set[str],
    last_role_by_node: dict[str, str],
) -> None:
    for node_id, status in sorted(controller.status_by_node.items()):
        resources = _status_resources(status)
        role = _effective_role(controller, node_id, resources)
        current_role = _status_current_role(status)
        if node_id not in known_nodes:
            known_nodes.add(node_id)
            log_event(
                LOGGER,
                logging.INFO,
                "tracker.node_join",
                node_id=node_id,
                assigned_role=role,
                current_role=current_role,
                cpu_cores=resources.cpu_cores,
                memory_bytes=resources.memory_bytes,
                gpu_count=resources.gpu_count,
                model_json_page_cache=resources.model_json_page_cache,
            )
            _call_tracker_hook(
                tracker_hooks,
                "on_node_join",
                CQDAGPCFGNodeEvent(
                    node_id=node_id,
                    role=role,
                    current_role=current_role,
                    reason="role_poll",
                    resources=resources,
                ),
            )
        previous_role = last_role_by_node.get(node_id)
        if previous_role != role:
            last_role_by_node[node_id] = role
            log_event(
                LOGGER,
                logging.INFO,
                "tracker.role_change",
                node_id=node_id,
                previous_role=previous_role,
                new_role=role,
                current_role=current_role,
            )
            _call_tracker_hook(
                tracker_hooks,
                "on_role_change",
                CQDAGPCFGRoleChangeEvent(
                    node_id=node_id,
                    previous_role=previous_role,
                    new_role=role,
                    current_role=current_role,
                    reason="role_assignment",
                    resources=resources,
                ),
            )


def _effective_role(controller, node_id: str, resources: WorkerResourceSpec) -> str:
    role = controller.roles.get(node_id, controller.default_role)
    if not resources.fits(controller.resource_policy.requirement_for(role)):
        return "idle"
    return role


def _status_current_role(status: object) -> str | None:
    if not isinstance(status, Mapping):
        return None
    value = status.get("current_role")
    return None if value is None else str(value)


def _status_resources(status: object) -> WorkerResourceSpec:
    if not isinstance(status, Mapping):
        return WorkerResourceSpec()
    resources = status.get("resources")
    if not isinstance(resources, Mapping):
        return WorkerResourceSpec()
    return WorkerResourceSpec.from_dict(resources)


def resolve_initial_role_counts(args: CqdagTrackerServiceConfig) -> tuple[int, int]:
    consumer_count = (
        args.initial_consumers
        if args.initial_consumers is not None
        else args.consumer_count if args.consumer_count is not None else args.min_consumers
    )
    total_nodes = args.total_nodes
    generator_count = args.initial_generators
    if total_nodes is None:
        if generator_count is None:
            generator_count = (
                args.expected_workers
                if args.expected_workers is not None
                else args.min_generators
            )
        if generator_count <= 0:
            raise ValueError("initial generator count must be positive")
        if consumer_count <= 0:
            raise ValueError("initial consumer count must be positive")
        return generator_count, consumer_count
    if generator_count is None:
        generator_count = total_nodes - consumer_count
    if total_nodes <= 0:
        raise ValueError("total_nodes must be positive")
    if generator_count <= 0:
        raise ValueError("initial generator count must be positive")
    if consumer_count <= 0:
        raise ValueError("initial consumer count must be positive")
    if generator_count + consumer_count != total_nodes:
        raise ValueError("initial generator and consumer counts must sum to total_nodes")
    if args.consumer_count is not None and consumer_count != args.consumer_count:
        raise ValueError("initial_consumers must match consumer_count")
    return generator_count, consumer_count


def effective_expected_workers(args: CqdagTrackerServiceConfig, role_controller) -> int | None:
    if args.expected_workers is not None:
        return args.expected_workers
    if role_controller is None or args.total_nodes is None:
        return None
    return int(role_controller["generator_count"])


def end_of_stream_consumer_count(args: CqdagTrackerServiceConfig, role_controller) -> int:
    if role_controller is None:
        if args.consumer_count is None:
            raise ValueError("consumer_count is required without role_bind")
        return args.consumer_count
    assigned_consumers = role_controller["controller"].role_count("consumer")
    initial_consumers = int(role_controller["consumer_count"])
    return max(assigned_consumers, initial_consumers, 1)


def role_resource_policy(args: CqdagTrackerServiceConfig) -> RoleResourcePolicy:
    return RoleResourcePolicy(
        generator_min=WorkerResourceSpec(
            cpu_cores=args.generator_min_cpus,
            memory_bytes=parse_byte_size(args.generator_min_memory),
            gpu_count=args.generator_min_gpus,
        ),
        consumer_min=WorkerResourceSpec(
            cpu_cores=args.consumer_min_cpus,
            memory_bytes=parse_byte_size(args.consumer_min_memory),
            gpu_count=args.consumer_min_gpus,
        ),
    )


def advertised_connect_uri(uri: str | None, advertise_host: str) -> str:
    if uri is None:
        raise ValueError("cannot advertise an empty endpoint")
    if uri.startswith("cqpcfg://0.0.0.0:"):
        return uri.replace("cqpcfg://0.0.0.0:", f"cqpcfg://{advertise_host}:", 1)
    if uri.startswith("tcp://0.0.0.0:"):
        return uri.replace("tcp://0.0.0.0:", f"tcp://{advertise_host}:", 1)
    return uri


def apply_endpoint_bundle(args: CqdagTrackerServiceConfig) -> None:
    if args.bind is None:
        return
    bind_bundle = ZmqEndpointBundle.from_base_uri(args.bind)
    public_bundle = ZmqEndpointBundle.from_base_uri(
        args.bind,
        advertise_host=args.advertise_host,
    )
    args.control_bind = bind_bundle.control
    args.batch_bind = bind_bundle.batch
    args.role_bind = bind_bundle.role
    args.ack_bind = bind_bundle.ack
    args.model_serve_bind = bind_bundle.model
    args.public_control_connect = public_bundle.control
    args.public_batch_connect = public_bundle.batch
    args.public_ack_connect = public_bundle.ack
    args.public_model_connect = public_bundle.model


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
        batch_retry_callback: Callable[[CQDAGPCFGBatchRetryEvent], None] | None = None,
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
        self.batch_retry_callback = batch_retry_callback
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
            self.ledger.complete(ack.batch_id, consumer_id=ack.consumer_id)
            del self.inflight_batches[ack.batch_id]
            self.last_publish_at_by_batch.pop(ack.batch_id, None)
            self.completed_batches += 1
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
            CQDAGPCFGBatchRetryEvent(
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

def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


__all__ = [
    "AnnotatedCQDAGPCFGTracker",
    "CQDAGPCFGBatchRetryEvent",
    "CQDAGPCFGCheckpointEvent",
    "CQDAGPCFGMemorySnapshot",
    "CQDAGPCFGNodeEvent",
    "CQDAGPCFGTrackerJob",
    "CQDAGPCFGTrackerSummary",
    "CQDAGPCFGTrackerError",
    "CQDAGPCFGRoleChangeEvent",
    "CQDAGPCFGTracker",
    "CqdagTrackerServiceConfig",
    "StreamingRecordBatchPublisher",
    "cqdagpcfg_tracker",
]
