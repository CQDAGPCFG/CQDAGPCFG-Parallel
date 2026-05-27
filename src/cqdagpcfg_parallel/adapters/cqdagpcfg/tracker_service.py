from __future__ import annotations

import logging
import os
from dataclasses import dataclass, replace
from inspect import signature
from pathlib import Path
from threading import Event, Thread
from time import monotonic, sleep
from typing import Any, Callable, Mapping

from CQDAGPCFG import load_model

from cqdagpcfg_parallel.distributed import (
    BatchMemoryLimits,
    DistributedProtocolConfig,
    DistributedProtocolTracker,
    JobContext,
    CqdagAwareElasticRoleAllocator,
    RoleController,
    RoleAllocationInput,
    RoleResourcePolicy,
    WorkerResourceSpec,
    memory_limited_batch_limits,
    parse_byte_size,
)
from cqdagpcfg_parallel.distributed.memory_policy import (
    DEFAULT_STREAMING_ARTIFACT_RECORD_BYTES,
)
from cqdagpcfg_parallel.framework_logging import (
    configure_framework_logging,
    log_event,
)
from cqdagpcfg_parallel.protocol import (
    ChunkSizePolicy,
    LeaseStrategyName,
    NodeSchedulingFeatures,
    SchedulerConfig,
    StableStreamFingerprint,
)
from cqdagpcfg_parallel.storage import (
    CompactDistributedTrackerCheckpointWriter,
    DistributedTrackerCheckpoint,
    FilePagedModelArtifactStore,
    ModelManifest,
)
from cqdagpcfg_parallel.runtime import (
    DurableBatchCheckpoint,
    ZmqModelArtifactServer,
    ZmqPullBatchAckSource,
)
from cqdagpcfg_parallel.runtime.zmq_transport import (
    ZmqEndpoint,
    ZmqEndpointBundle,
    ZmqPushBatchSink,
)

from .block_graph import CQDAGBlockGraphAdapter, ROOT_NODE_ID, BlockNodeDescriptor
from .job_spec import CQDAGJobSpec
from .tracker_publisher import BatchRetryPayload, StreamingRecordBatchPublisher


LOGGER = logging.getLogger("cqdagpcfg.tracker")

DEFAULT_SAFE_RECORD_CHUNK_SIZE = 8192
DEFAULT_ROOT_ARTIFACT_TARGET_BYTES = 128 * 1024 * 1024
DEFAULT_CRACKING_ROOT_ARTIFACT_TARGET_BYTES = 128 * 1024 * 1024
DEFAULT_SHARD_PARALLEL_PIPELINE_DEPTH = 4
DEFAULT_SHARD_LANE_SPLIT_STRATEGY = "equal_rank"
DEFAULT_SHARD_MASS_LANE_BIAS = 2.0


@dataclass(slots=True)
class CqdagTrackerServiceConfig:
    model_path: Path
    job_spec_path: Path
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
    role_heartbeat_timeout_seconds: float = 5.0
    disable_elastic_role_allocation: bool = False
    role_rebalance_interval_seconds: float = 0.5
    role_switch_min_improvement: float = 0.05
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
    outputs_path: Path | None = None
    metrics_flush_interval_seconds: float = 0.25
    checkpoint_path: Path | None = None
    resume_checkpoint_path: Path | None = None
    checkpoint_stable_log_path: Path | None = None
    checkpoint_interval_records: int = 1
    batch_checkpoint_path: Path | None = None
    resume_batch_checkpoint_path: Path | None = None
    source_mode: str = "root"
    demand_window: int = 8
    max_chunk_size: int = DEFAULT_SAFE_RECORD_CHUNK_SIZE
    max_parallel_leases_per_node: int = 2
    lease_ttl_seconds: float = 30.0
    lease_strategy: str = "rank_window_probability_mass"
    target_chunk_probability_mass: float = 0.01
    rank_window_size: int = 65536
    rank_window_frontier_multiplier: float = 4.0
    tail_steal_min_gap: int = 1
    tail_steal_pending_limit_multiplier: float = 2.0
    tail_steal_score_threshold: float = 0.0
    disable_tail_stealing: bool = False
    disable_node_affinity: bool = False
    node_affinity_bonus: float = 0.5
    batch_size: int = 65536
    max_batch_payload_bytes: int = 1 << 20
    optimization_profile: str = "balanced"
    root_artifact_target_bytes: int = DEFAULT_ROOT_ARTIFACT_TARGET_BYTES
    candidate_block_dir: Path | None = None
    candidate_block_base_uri: str | None = None
    delete_candidate_blocks_on_ack: bool = True
    timeout_seconds: float = 3600.0
    disable_reclaim: bool = False
    disable_lazy_shard_activation: bool = False
    validate_serial_digest: bool = True

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
        if self.optimization_profile not in {"balanced", "cracking"}:
            raise ValueError("optimization_profile must be balanced or cracking")
        if self.root_artifact_target_bytes <= 0:
            raise ValueError("root_artifact_target_bytes must be positive")
        if self.late_worker_role not in {"generator", "consumer", "idle"}:
            raise ValueError("late_worker_role must be generator, consumer, or idle")
        if self.role_heartbeat_timeout_seconds <= 0.0:
            raise ValueError("role_heartbeat_timeout_seconds must be positive")
        if self.role_rebalance_interval_seconds <= 0.0:
            raise ValueError("role_rebalance_interval_seconds must be positive")
        if self.role_switch_min_improvement < 0.0:
            raise ValueError("role_switch_min_improvement cannot be negative")
        if self.source_mode not in {"root", "structure", "shard"}:
            raise ValueError("source_mode must be root, structure, or shard")
        if self.lease_ttl_seconds <= 0.0:
            raise ValueError("lease_ttl_seconds must be positive")
        LeaseStrategyName(self.lease_strategy)
        if self.target_chunk_probability_mass < 0.0:
            raise ValueError("target_chunk_probability_mass cannot be negative")
        if self.rank_window_size < 0:
            raise ValueError("rank_window_size cannot be negative")
        if self.rank_window_frontier_multiplier < 0.0:
            raise ValueError("rank_window_frontier_multiplier cannot be negative")
        if self.tail_steal_min_gap <= 0:
            raise ValueError("tail_steal_min_gap must be positive")
        if self.tail_steal_pending_limit_multiplier < 0.0:
            raise ValueError("tail_steal_pending_limit_multiplier cannot be negative")
        if self.tail_steal_score_threshold < 0.0:
            raise ValueError("tail_steal_score_threshold cannot be negative")
        if self.candidate_block_base_uri is not None and self.candidate_block_dir is None:
            raise ValueError("candidate_block_base_uri requires candidate_block_dir")


@dataclass(frozen=True, slots=True)
class CQDAGPCFGTrackerJob:
    model_path: Path
    job_spec_path: Path
    model_id: str
    limit: int
    job_payload_items: int
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
    consumer_count: int
    digest: str
    serial_digest: str
    digest_validation_enabled: bool
    stable_fingerprint: str
    serial_stream_fingerprint: str | None
    emitted_records: int
    collected_outputs: int
    resident_records: int
    peak_resident_records: int
    reclaimed_records: int
    affinity_hits: int
    affinity_misses: int
    elapsed_seconds: float
    assigned_records_by_node: tuple[tuple[Any, int], ...]


def effective_max_parallel_leases_per_node(args: CqdagTrackerServiceConfig) -> int:
    return args.max_parallel_leases_per_node


def _direct_artifact_chunk_size(
    args: CqdagTrackerServiceConfig,
    *,
    limit: int,
    generator_slots: int,
) -> int:
    if args.source_mode not in {"root", "shard"}:
        return 0
    if limit <= 0:
        return 0
    memory_limited_records = max(
        args.max_chunk_size,
        args.root_artifact_target_bytes
        // DEFAULT_STREAMING_ARTIFACT_RECORD_BYTES,
    )
    if args.source_mode == "root":
        return min(limit, memory_limited_records)

    parallel_slots = max(1, generator_slots) * max(
        1,
        args.max_parallel_leases_per_node,
    )
    pipeline_slots = max(
        parallel_slots,
        parallel_slots * DEFAULT_SHARD_PARALLEL_PIPELINE_DEPTH,
    )
    parallel_limited_records = max(
        args.max_chunk_size,
        (limit + pipeline_slots - 1) // pipeline_slots,
    )
    return min(memory_limited_records, parallel_limited_records)


def _apply_tracker_optimization_profile(args: CqdagTrackerServiceConfig) -> None:
    if args.optimization_profile != "cracking":
        return
    args.validate_serial_digest = False
    args.disable_reclaim = False
    if args.root_artifact_target_bytes == DEFAULT_ROOT_ARTIFACT_TARGET_BYTES:
        args.root_artifact_target_bytes = DEFAULT_CRACKING_ROOT_ARTIFACT_TARGET_BYTES


def _effective_lease_strategy(args: CqdagTrackerServiceConfig) -> LeaseStrategyName:
    if args.source_mode in {"root", "shard"}:
        return LeaseStrategyName.RANGE
    return LeaseStrategyName(args.lease_strategy)


def _effective_chunk_policy(args: CqdagTrackerServiceConfig) -> ChunkSizePolicy:
    if args.source_mode in {"root", "shard"}:
        return ChunkSizePolicy.FIXED
    return ChunkSizePolicy.CQDAG_ADAPTIVE


def _effective_rank_window_size(args: CqdagTrackerServiceConfig) -> int:
    if args.source_mode in {"root", "shard"}:
        return 0
    return args.rank_window_size


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
    *,
    env_prefix: str | None = None,
    **overrides,
) -> Callable[[type[Any]], AnnotatedCQDAGPCFGTracker]:
    if env_prefix is not None and not isinstance(env_prefix, str):
        raise TypeError("env_prefix must be a string")
    if config is None:
        values = (
            _tracker_config_values_from_env(env_prefix)
            if env_prefix is not None
            else {}
        )
        values.update(overrides)
        resolved_config = CqdagTrackerServiceConfig(**values)
    elif env_prefix is not None:
        raise ValueError("use either config or env_prefix, not both")
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


def _tracker_config_values_from_env(prefix: str) -> dict[str, Any]:
    normalized_prefix = prefix.rstrip("_")
    values: dict[str, Any] = {}
    readers: dict[str, Callable[[str], Any]] = {
        "model_path": _env_path,
        "job_spec_path": _env_path,
        "model_id": _env_str,
        "model_serve_bind": _env_str,
        "model_chunk_size": _env_int,
        "model_slot_page_size": _env_int,
        "model_structure_page_size": _env_int,
        "bind": _env_str,
        "advertise_host": _env_str,
        "control_bind": _env_str,
        "public_control_connect": _env_str,
        "batch_bind": _env_str,
        "batch_connect": _env_str,
        "public_batch_connect": _env_str,
        "ack_bind": _env_str,
        "public_ack_connect": _env_str,
        "public_model_connect": _env_str,
        "role_bind": _env_str,
        "total_nodes": _env_int,
        "min_generators": _env_int,
        "min_consumers": _env_int,
        "initial_generators": _env_int,
        "initial_consumers": _env_int,
        "late_worker_role": _env_str,
        "role_heartbeat_timeout_seconds": _env_float,
        "disable_elastic_role_allocation": _env_bool,
        "role_rebalance_interval_seconds": _env_float,
        "role_switch_min_improvement": _env_float,
        "generator_min_cpus": _env_float,
        "generator_min_memory": _env_str,
        "generator_min_gpus": _env_int,
        "consumer_min_cpus": _env_float,
        "consumer_min_memory": _env_str,
        "consumer_min_gpus": _env_int,
        "consumer_count": _env_int,
        "ack_timeout_seconds": _env_float,
        "ack_retry_interval_seconds": _env_float,
        "batch_startup_grace_seconds": _env_float,
        "expected_workers": _env_int,
        "shutdown_grace_seconds": _env_float,
        "metrics_path": _env_path,
        "outputs_path": _env_path,
        "metrics_flush_interval_seconds": _env_float,
        "checkpoint_path": _env_path,
        "resume_checkpoint_path": _env_path,
        "checkpoint_stable_log_path": _env_path,
        "checkpoint_interval_records": _env_int,
        "batch_checkpoint_path": _env_path,
        "resume_batch_checkpoint_path": _env_path,
        "source_mode": _env_str,
        "demand_window": _env_int,
        "max_chunk_size": _env_int,
        "max_parallel_leases_per_node": _env_int,
        "lease_ttl_seconds": _env_float,
        "lease_strategy": _env_str,
        "target_chunk_probability_mass": _env_float,
        "rank_window_size": _env_int,
        "rank_window_frontier_multiplier": _env_float,
        "tail_steal_min_gap": _env_int,
        "tail_steal_pending_limit_multiplier": _env_float,
        "tail_steal_score_threshold": _env_float,
        "disable_tail_stealing": _env_bool,
        "disable_node_affinity": _env_bool,
        "node_affinity_bonus": _env_float,
        "batch_size": _env_int,
        "max_batch_payload_bytes": _env_int,
        "optimization_profile": _env_str,
        "root_artifact_target_bytes": _env_int,
        "candidate_block_dir": _env_path,
        "candidate_block_base_uri": _env_str,
        "delete_candidate_blocks_on_ack": _env_bool,
        "timeout_seconds": _env_float,
        "disable_reclaim": _env_bool,
        "disable_lazy_shard_activation": _env_bool,
        "validate_serial_digest": _env_bool,
    }
    for field_name, reader in readers.items():
        env_name = f"{normalized_prefix}_{field_name.upper()}"
        value = os.environ.get(env_name)
        if value is None or value == "":
            continue
        values[field_name] = reader(value)
    return values


def _env_str(value: str) -> str:
    return value


def _env_path(value: str) -> Path:
    return Path(value)


def _env_int(value: str) -> int:
    return int(value)


def _env_float(value: str) -> float:
    return float(value)


def _env_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def model_fingerprint_from_path(model_path: Path, *, model_id: str) -> str:
    manifest = ModelManifest.from_json_payload(
        model_path.read_bytes(),
        model_id=model_id,
        artifact_uri=str(model_path),
    )
    return manifest.model_fingerprint


def _run_cqdag_tracker_service(
    config: CqdagTrackerServiceConfig,
    *,
    tracker_class: type[Any] | None = None,
) -> None:
    configure_framework_logging()
    args = replace(config)
    apply_endpoint_bundle(args)
    _apply_tracker_optimization_profile(args)
    tracker_hooks = _instantiate_tracker_hooks(tracker_class, args)
    job_spec_path = args.job_spec_path
    try:
        job_spec = CQDAGJobSpec.read(job_spec_path)
    except BaseException as exc:
        _call_tracker_hook(
            tracker_hooks,
            "on_error",
            CQDAGPCFGTrackerError(
                stage="job_spec",
                error_type=type(exc).__name__,
                message=str(exc),
            ),
        )
        raise
    job_payload = dict(job_spec.payload)
    model_fingerprint = model_fingerprint_from_path(args.model_path, model_id=args.model_id)
    expected_model_fingerprint = job_spec.model_fingerprint
    if (
        expected_model_fingerprint is not None
        and expected_model_fingerprint != model_fingerprint
    ):
        _call_tracker_hook(
            tracker_hooks,
            "on_error",
            CQDAGPCFGTrackerError(
                stage="model_fingerprint",
                error_type="RuntimeError",
                message=(
                    "tracker model fingerprint does not match job spec: "
                    f"{model_fingerprint} != {expected_model_fingerprint}"
                ),
            ),
        )
        raise RuntimeError(
            "tracker model fingerprint does not match job spec: "
            f"{model_fingerprint} != {expected_model_fingerprint}"
        )
    job_payload["model_fingerprint"] = model_fingerprint
    job_spec = job_spec.with_model_fingerprint(model_fingerprint)
    limit = job_spec.limit
    job_payload_items = job_payload_item_count(job_payload)
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.start",
        model_path=args.model_path,
        job_spec_path=job_spec_path,
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
        job_payload_items=job_payload_items,
        model_fingerprint=model_fingerprint,
    )
    _call_tracker_hook(
        tracker_hooks,
        "on_start",
        CQDAGPCFGTrackerJob(
            model_path=args.model_path,
            job_spec_path=job_spec_path,
            model_id=args.model_id,
            limit=limit,
            job_payload_items=job_payload_items,
            source_mode=args.source_mode,
            control_bind=args.control_bind,
            batch_bind=args.batch_bind,
            role_bind=args.role_bind,
            ack_bind=args.ack_bind,
            model_serve_bind=args.model_serve_bind,
            expected_workers=args.expected_workers,
            total_nodes=args.total_nodes,
            model_fingerprint=model_fingerprint,
        ),
    )
    if args.source_mode in {"structure", "shard"}:
        model = load_model(args.model_path)
        adapter = CQDAGBlockGraphAdapter(model)
        protocol_nodes = adapter.structure_nodes()
        node_ids = tuple(node.node_id for node in protocol_nodes)
        node_features = adapter.scheduling_features()
    else:
        root_node = BlockNodeDescriptor(
            node_id=ROOT_NODE_ID,
            name="root",
            entropy=0.0,
            slot_dispersion=0.0,
            priority=1.0,
            estimated_cost=1.0,
            base_prob=1.0,
            cardinality=max(limit, 1),
        )
        protocol_nodes = (root_node,)
        node_ids = (root_node.node_id,)
        node_features = (
            NodeSchedulingFeatures(
                node_id=root_node.node_id,
                entropy=root_node.slot_dispersion,
                priority=root_node.priority,
                estimated_cost=root_node.estimated_cost,
                cardinality=root_node.cardinality,
            ),
        )
        adapter = None
    model_server = start_model_artifact_server(args)
    role_controller = start_role_controller(args, job_payload, tracker_hooks=tracker_hooks)
    generator_slots = expected_generator_slots(args, role_controller)
    max_parallel_leases_per_node = effective_max_parallel_leases_per_node(args)
    shard_rank_lane_count = 1
    if args.source_mode == "shard" and adapter is not None:
        shard_rank_lane_count = max(
            1,
            expected_generator_capacity(args, role_controller),
            max_parallel_leases_per_node,
        )
        protocol_nodes = adapter.structure_rank_lane_nodes(
            lane_count=shard_rank_lane_count,
            rank_horizon=(
                limit
                if DEFAULT_SHARD_LANE_SPLIT_STRATEGY == "probability_mass"
                else None
            ),
            split_strategy=DEFAULT_SHARD_LANE_SPLIT_STRATEGY,
            mass_bias=DEFAULT_SHARD_MASS_LANE_BIAS,
        )
        node_ids = tuple(node.node_id for node in protocol_nodes)
        node_features = adapter.scheduling_features_for(protocol_nodes)
    root_artifact_chunk_size = _direct_artifact_chunk_size(
        args,
        limit=limit,
        generator_slots=generator_slots,
    )
    effective_max_chunk_size = max(args.max_chunk_size, root_artifact_chunk_size)
    root_parallel_window = (
        min(limit, root_artifact_chunk_size * max_parallel_leases_per_node)
        if root_artifact_chunk_size > 0
        else 0
    )
    effective_demand_window = max(
        args.demand_window,
        args.batch_size,
        root_parallel_window,
    )
    effective_lease_strategy = _effective_lease_strategy(args)
    effective_chunk_policy = _effective_chunk_policy(args)
    effective_rank_window_size = _effective_rank_window_size(args)
    effective_tail_steal_pending_limit_multiplier = (
        max(args.tail_steal_pending_limit_multiplier, float(max_parallel_leases_per_node))
        if args.source_mode == "root"
        else args.tail_steal_pending_limit_multiplier
    )
    effective_fixed_chunk_size = (
        effective_max_chunk_size
        if effective_chunk_policy == ChunkSizePolicy.FIXED
        else 8
    )
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.protocol_ready",
        protocol_nodes=len(protocol_nodes),
        source_mode=args.source_mode,
        optimization_profile=args.optimization_profile,
        demand_window=effective_demand_window,
        configured_demand_window=args.demand_window,
        max_chunk_size=effective_max_chunk_size,
        safe_record_chunk_size=args.max_chunk_size,
        root_artifact_chunk_size=root_artifact_chunk_size,
        root_artifact_target_bytes=args.root_artifact_target_bytes,
        root_parallel_window=root_parallel_window,
        generator_slots=generator_slots,
        shard_rank_lane_count=shard_rank_lane_count,
        shard_parallel_pipeline_depth=DEFAULT_SHARD_PARALLEL_PIPELINE_DEPTH,
        shard_lane_split_strategy=DEFAULT_SHARD_LANE_SPLIT_STRATEGY,
        shard_mass_lane_bias=DEFAULT_SHARD_MASS_LANE_BIAS,
        max_parallel_leases_per_node=max_parallel_leases_per_node,
        requested_max_parallel_leases_per_node=args.max_parallel_leases_per_node,
        lease_ttl_seconds=args.lease_ttl_seconds,
        lease_strategy=effective_lease_strategy.value,
        configured_lease_strategy=args.lease_strategy,
        chunk_policy=effective_chunk_policy.value,
        target_chunk_probability_mass=args.target_chunk_probability_mass,
        rank_window_size=effective_rank_window_size,
        configured_rank_window_size=args.rank_window_size,
        rank_window_frontier_multiplier=args.rank_window_frontier_multiplier,
        tail_stealing_enabled=(
            not args.disable_tail_stealing and args.source_mode != "shard"
        ),
        tail_steal_min_gap=args.tail_steal_min_gap,
        tail_steal_pending_limit_multiplier=effective_tail_steal_pending_limit_multiplier,
        configured_tail_steal_pending_limit_multiplier=(
            args.tail_steal_pending_limit_multiplier
        ),
        tail_steal_score_threshold=args.tail_steal_score_threshold,
        node_affinity_enabled=not args.disable_node_affinity,
        reclaim_enabled=not args.disable_reclaim,
        lazy_shard_activation_enabled=not args.disable_lazy_shard_activation,
    )

    config = DistributedProtocolConfig(
        scheduler=SchedulerConfig(
            policy=effective_chunk_policy,
            fixed_chunk_size=effective_fixed_chunk_size,
            max_chunk_size=effective_max_chunk_size,
            max_parallel_leases_per_node=max_parallel_leases_per_node,
            lease_strategy=effective_lease_strategy,
            target_chunk_probability_mass=args.target_chunk_probability_mass,
            rank_window_size=effective_rank_window_size,
            rank_window_frontier_multiplier=args.rank_window_frontier_multiplier,
            tail_stealing_enabled=(
                not args.disable_tail_stealing and args.source_mode != "shard"
            ),
            tail_steal_min_gap=args.tail_steal_min_gap,
            tail_steal_pending_limit_multiplier=(
                effective_tail_steal_pending_limit_multiplier
            ),
            tail_steal_score_threshold=args.tail_steal_score_threshold,
            node_affinity_enabled=not args.disable_node_affinity,
            node_affinity_bonus=args.node_affinity_bonus,
        ),
        node_ids=node_ids,
        node_features=node_features,
        demand_window=effective_demand_window,
        record_order_key=(
            adapter.serial_order_key
            if adapter is not None and args.source_mode == "structure"
            else None
        ),
        reclaim_emitted_chunks=not args.disable_reclaim,
        model_fingerprint=model_fingerprint,
        lease_ttl_seconds=args.lease_ttl_seconds,
        lazy_shard_activation=not args.disable_lazy_shard_activation,
        direct_unordered_chunk_emission=args.source_mode == "shard",
        direct_unordered_pipeline_depth=DEFAULT_SHARD_PARALLEL_PIPELINE_DEPTH,
        track_output_digest=args.validate_serial_digest and args.source_mode != "shard",
        safe_record_chunk_size=args.max_chunk_size,
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
                role_metrics_provider=lambda: role_controller_metrics(role_controller),
                extra_metrics_provider=lambda: _tracker_metrics_snapshot(tracker_hooks),
                batch_publish_callback=lambda batch: _call_tracker_hook(
                    tracker_hooks,
                    "on_candidate_batch",
                    batch,
                ),
                batch_limits_provider=lambda: _active_consumer_batch_limits(
                    args,
                    role_controller,
                ),
                ack_retry_interval_seconds=args.ack_retry_interval_seconds,
                batch_retry_callback=lambda payload: _call_tracker_hook(
                    tracker_hooks,
                    "on_batch_retry",
                    _batch_retry_event(payload),
                ),
                metrics_path=args.metrics_path,
                metrics_flush_interval_seconds=args.metrics_flush_interval_seconds,
                outputs_path=args.outputs_path,
                initial_start_rank=0 if resume_checkpoint is None else resume_checkpoint.emitted_count,
                initial_batch_id=0 if resume_checkpoint is None else resume_checkpoint.emitted_count,
                batch_checkpoint_path=args.batch_checkpoint_path,
                resume_batch_checkpoint=resume_batch_checkpoint,
                candidate_block_dir=args.candidate_block_dir,
                candidate_block_base_uri=args.candidate_block_base_uri,
                delete_candidate_blocks_on_ack=args.delete_candidate_blocks_on_ack,
            )
            if role_controller is not None:
                role_controller["metrics_callback"][0] = lambda: publisher.write_metrics(
                    final=False,
                    force=True,
                )
                role_controller["rebalance_callback"][0] = lambda: rebalance_elastic_roles(
                    args,
                    role_controller,
                    publisher,
                    limit=limit,
                )
                publisher.write_metrics(final=False, force=True)
            publisher.republish_pending()
            publisher.start_background_ack_drain()

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
            try:
                result = tracker.run(
                    limit=limit,
                    expected_workers=expected_workers,
                    timeout_seconds=args.timeout_seconds,
                    shutdown_grace_seconds=args.shutdown_grace_seconds,
                    output_callback=publisher.publish,
                    output_records_callback=publisher.publish_many,
                    output_artifact_callback=publisher.publish_artifact,
                    schedule_backpressure_callback=publisher.artifact_backpressure_active,
                    collect_outputs=False,
                    resume_checkpoint=resume_checkpoint,
                    checkpoint_callback=checkpoint_callback if checkpoint_writer is not None else None,
                    checkpoint_interval_records=args.checkpoint_interval_records,
                )
            finally:
                publisher.stop_background_ack_drain()
            elapsed = monotonic() - started_at

            if (
                args.validate_serial_digest
                and args.source_mode != "shard"
                and not _result_matches_oracle(
                result,
                job_spec,
                )
            ):
                log_event(
                    LOGGER,
                    logging.ERROR,
                    "tracker.digest_mismatch",
                    digest=result.digest,
                    serial_digest=job_spec.serial_digest,
                    stable_fingerprint=result.stable_fingerprint,
                    serial_stream_fingerprint=job_spec.payload.get(
                        "serial_stream_fingerprint",
                    ),
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
            try:
                publisher.wait_for_acks(timeout_seconds=args.ack_timeout_seconds)
            except BaseException as exc:
                _call_tracker_hook(
                    tracker_hooks,
                    "on_error",
                    CQDAGPCFGTrackerError(
                        stage="batch_ack",
                        error_type=type(exc).__name__,
                        message=str(exc),
                    ),
                )
                raise
            consumer_count = end_of_stream_consumer_count(args, role_controller)
            sink.publish_end_of_stream(consumer_count)
            publisher.write_metrics(final=True)
            log_event(
                LOGGER,
                logging.INFO,
                "tracker.probability_mass_progress",
                **publisher.progress_metrics(),
                **publisher.protocol_metrics,
            )
            log_event(
                LOGGER,
                logging.INFO,
                "tracker.batch_stream_drained",
                published_batches=publisher.published_batches,
                published_candidates=publisher.published_candidates,
                published_payload_bytes=publisher.published_payload_bytes,
                republished_batches=publisher.republished_batches,
                completed_batches=publisher.completed_batches,
                consumer_count=consumer_count,
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
        consumer_count=consumer_count,
        digest=result.digest,
        serial_digest=job_spec.serial_digest,
        digest_validation_enabled=args.validate_serial_digest,
        stable_fingerprint=result.stable_fingerprint,
        serial_stream_fingerprint=(
            None
            if job_spec.payload.get("serial_stream_fingerprint") is None
            else str(job_spec.payload["serial_stream_fingerprint"])
        ),
        emitted_records=result.emitted_count,
        collected_outputs=len(publisher.consumer_outputs),
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


def _result_matches_oracle(result, job_spec: CQDAGJobSpec) -> bool:
    if result.digest == job_spec.serial_digest:
        return True
    expected_fingerprint = job_spec.payload.get("serial_stream_fingerprint")
    if expected_fingerprint is None:
        return False
    return _fingerprints_equal(result.stable_fingerprint, str(expected_fingerprint))


def _fingerprints_equal(left: str, right: str) -> bool:
    try:
        return StableStreamFingerprint.from_string(left) == StableStreamFingerprint.from_string(
            right,
        )
    except ValueError:
        return False


def _tracker_metrics_snapshot(instance) -> Mapping[str, object]:
    if instance is None:
        return {}
    hook = getattr(instance, "metrics_snapshot", None)
    if hook is None:
        return {}
    if not callable(hook):
        raise TypeError("tracker hook metrics_snapshot must be callable")
    payload = hook()
    if payload is None:
        return {}
    if not isinstance(payload, Mapping):
        raise TypeError("tracker hook metrics_snapshot must return a mapping")
    return payload


def _batch_retry_event(payload: BatchRetryPayload) -> CQDAGPCFGBatchRetryEvent:
    return CQDAGPCFGBatchRetryEvent(
        batch_id=payload.batch_id,
        start_rank=payload.start_rank,
        end_rank=payload.end_rank,
        reason=payload.reason,
        attempts=payload.attempts,
        pending_batches=payload.pending_batches,
        consumer_id=payload.consumer_id,
        error=payload.error,
    )


def _print_tracker_summary(summary: CQDAGPCFGTrackerSummary) -> None:
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.complete",
        limit=summary.limit,
        source_mode=summary.source_mode,
        protocol_nodes=summary.protocol_nodes,
        expected_workers=summary.expected_workers,
        consumer_count=summary.consumer_count,
        digest=summary.digest,
        digest_validation_enabled=summary.digest_validation_enabled,
        digest_match=(
            summary.digest == summary.serial_digest
            or (
                summary.serial_stream_fingerprint is not None
                and _fingerprints_equal(
                    summary.stable_fingerprint,
                    summary.serial_stream_fingerprint,
                )
            )
            if summary.digest_validation_enabled
            else None
        ),
        stable_fingerprint=summary.stable_fingerprint,
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
    job_payload: dict,
    *,
    tracker_hooks=None,
):
    if args.role_bind is None:
        return None

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
    job_context = None
    if args.model_serve_bind is not None or args.public_model_connect is not None:
        effective_demand_window = max(args.demand_window, args.batch_size)
        job_context = JobContext.from_job_payload(
            job_payload,
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
            demand_window=effective_demand_window,
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
    last_current_role_by_node: dict[str, str | None] = {}
    hook_failures: list[BaseException] = []
    metrics_callback: list[Callable[[], None] | None] = [None]
    rebalance_callback: list[Callable[[], bool] | None] = [None]
    last_rebalance_at: list[float] = [0.0]

    def serve() -> None:
        while not stop_event.is_set():
            try:
                controller.poll(timeout_ms=100)
                expired_nodes = controller.expire_stale_nodes(
                    timeout_seconds=args.role_heartbeat_timeout_seconds,
                )
                changed = _emit_role_controller_events(
                    controller,
                    tracker_hooks=tracker_hooks,
                    known_nodes=known_nodes,
                    last_role_by_node=last_role_by_node,
                    last_current_role_by_node=last_current_role_by_node,
                )
                if expired_nodes:
                    changed = True
                    _emit_role_controller_expirations(
                        expired_nodes,
                        tracker_hooks=tracker_hooks,
                        known_nodes=known_nodes,
                        last_role_by_node=last_role_by_node,
                        last_current_role_by_node=last_current_role_by_node,
                    )
                if (
                    rebalance_callback[0] is not None
                    and monotonic() - last_rebalance_at[0] >= args.role_rebalance_interval_seconds
                ):
                    last_rebalance_at[0] = monotonic()
                    changed = rebalance_callback[0]() or changed
                if changed and metrics_callback[0] is not None:
                    metrics_callback[0]()
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
        "last_current_role_by_node": last_current_role_by_node,
        "hook_failures": hook_failures,
        "metrics_callback": metrics_callback,
        "rebalance_callback": rebalance_callback,
        "last_rebalance_at": last_rebalance_at,
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
    last_current_role_by_node: dict[str, str | None],
) -> bool:
    changed = False
    for node_id, status in sorted(controller.status_by_node.items()):
        resources = _status_resources(status)
        role = _effective_role(controller, node_id, resources)
        current_role = _status_current_role(status)
        if last_current_role_by_node.get(node_id) != current_role:
            last_current_role_by_node[node_id] = current_role
            changed = True
        if node_id not in known_nodes:
            known_nodes.add(node_id)
            changed = True
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
            changed = True
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
    return changed


def _emit_role_controller_expirations(
    expired_nodes: tuple[tuple[str, str, dict], ...],
    *,
    tracker_hooks,
    known_nodes: set[str],
    last_role_by_node: dict[str, str],
    last_current_role_by_node: dict[str, str | None],
) -> None:
    for node_id, role, status in expired_nodes:
        known_nodes.discard(node_id)
        last_role_by_node.pop(node_id, None)
        last_current_role_by_node.pop(node_id, None)
        current_role = _status_current_role(status)
        resources = _status_resources(status)
        log_event(
            LOGGER,
            logging.INFO,
            "tracker.node_leave",
            node_id=node_id,
            assigned_role=role,
            current_role=current_role,
            reason="heartbeat_timeout",
        )
        _call_tracker_hook(
            tracker_hooks,
            "on_node_leave",
            CQDAGPCFGNodeEvent(
                node_id=node_id,
                role=role,
                current_role=current_role,
                reason="heartbeat_timeout",
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


def _active_consumer_batch_limits(
    args: CqdagTrackerServiceConfig,
    role_controller,
) -> BatchMemoryLimits:
    resources = _active_consumer_resources(args, role_controller)
    return memory_limited_batch_limits(
        resources,
        configured_batch_size=args.batch_size,
        configured_max_payload_bytes=args.max_batch_payload_bytes,
    )


def _active_consumer_resources(
    args: CqdagTrackerServiceConfig,
    role_controller,
) -> tuple[WorkerResourceSpec, ...]:
    if role_controller is None:
        fallback = parse_byte_size(args.consumer_min_memory)
        return (
            (WorkerResourceSpec(memory_bytes=fallback),)
            if fallback is not None
            else ()
        )
    controller = role_controller["controller"]
    active: list[WorkerResourceSpec] = []
    for node_id, status in controller.status_by_node.items():
        resources = _status_resources(status)
        if _effective_role(controller, node_id, resources) == "consumer":
            active.append(resources)
    if active:
        return tuple(active)
    fallback = parse_byte_size(args.consumer_min_memory)
    return (
        (WorkerResourceSpec(memory_bytes=fallback),)
        if fallback is not None
        else ()
    )


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


def expected_generator_slots(args: CqdagTrackerServiceConfig, role_controller) -> int:
    if role_controller is not None:
        return max(1, int(role_controller["generator_count"]))
    generator_count, _ = resolve_initial_role_counts(args)
    return max(1, generator_count)


def expected_generator_capacity(args: CqdagTrackerServiceConfig, role_controller) -> int:
    if args.total_nodes is not None:
        return max(1, args.total_nodes - max(0, args.min_consumers))
    if args.expected_workers is not None:
        return max(1, args.expected_workers)
    return expected_generator_slots(args, role_controller)


def role_controller_metrics(role_controller) -> dict[str, int]:
    if role_controller is None:
        return {
            "assigned_generator_nodes": 0,
            "assigned_consumer_nodes": 0,
            "assigned_idle_nodes": 0,
            "current_generator_nodes": 0,
            "current_consumer_nodes": 0,
            "current_idle_nodes": 0,
            "observed_worker_nodes": 0,
        }
    controller = role_controller["controller"]
    current_counts = {"generator": 0, "consumer": 0, "idle": 0}
    for node_id, status in controller.status_by_node.items():
        resources = _status_resources(status)
        role = _effective_role(controller, node_id, resources)
        current_role = _status_current_role(status) or role
        if current_role not in current_counts:
            current_role = "idle"
        current_counts[current_role] += 1
    return {
        "assigned_generator_nodes": controller.role_count("generator"),
        "assigned_consumer_nodes": controller.role_count("consumer"),
        "assigned_idle_nodes": controller.role_count("idle"),
        "current_generator_nodes": current_counts["generator"],
        "current_consumer_nodes": current_counts["consumer"],
        "current_idle_nodes": current_counts["idle"],
        "observed_worker_nodes": len(controller.status_by_node),
    }


def rebalance_elastic_roles(
    args: CqdagTrackerServiceConfig,
    role_controller,
    publisher: StreamingRecordBatchPublisher,
    *,
    limit: int,
) -> bool:
    if args.disable_elastic_role_allocation or role_controller is None:
        return False
    controller = role_controller["controller"]
    node_ids = tuple(sorted(controller.status_by_node))
    if len(node_ids) < args.min_generators + args.min_consumers:
        return False
    current_generator_count = sum(
        1
        for node_id in node_ids
        if _effective_role(controller, node_id, _status_resources(controller.status_by_node[node_id]))
        == "generator"
    )
    current_consumer_count = sum(
        1
        for node_id in node_ids
        if _effective_role(controller, node_id, _status_resources(controller.status_by_node[node_id]))
        == "consumer"
    )
    if current_generator_count <= 0 or current_consumer_count <= 0:
        return False
    snapshot = _role_allocation_snapshot(
        args,
        controller,
        publisher,
        node_ids=node_ids,
        current_generator_count=current_generator_count,
        current_consumer_count=current_consumer_count,
        limit=limit,
    )
    allocator = CqdagAwareElasticRoleAllocator()
    plan = allocator.plan(snapshot)
    desired_generators = min(
        max(plan.generator_count, args.min_generators),
        len(node_ids) - args.min_consumers,
    )
    if desired_generators == current_generator_count:
        return False
    current_throughput = allocator.throughput_for(snapshot, current_generator_count)
    improvement = (
        (plan.expected_throughput - current_throughput) / max(current_throughput, 1e-9)
    )
    pressure_extreme = (
        desired_generators < current_generator_count
        and (plan.queue_pressure >= 0.80 or plan.cqdag_reclaim_pressure >= 0.80)
    ) or (
        desired_generators > current_generator_count
        and plan.cqdag_frontier_pressure >= 0.60
    )
    if improvement < args.role_switch_min_improvement and not pressure_extreme:
        return False
    payback = allocator.payback_for(snapshot, plan, current_generator_count)
    if not payback.should_switch:
        log_event(
            LOGGER,
            logging.DEBUG,
            "tracker.elastic_role_rebalance_skipped",
            reason=payback.reason,
            desired_generators=desired_generators,
            current_generators=current_generator_count,
            remaining_candidates=payback.remaining_candidates,
            current_seconds=f"{payback.current_seconds:.6f}",
            planned_seconds=f"{payback.planned_seconds:.6f}",
            saved_seconds=f"{payback.saved_seconds:.6f}",
            swap_count=payback.swap_count,
        )
        return False

    new_roles = dict(controller.roles)
    if desired_generators < current_generator_count:
        for node_id in _role_switch_candidates(controller, node_ids, "generator"):
            if current_generator_count <= desired_generators:
                break
            new_roles[node_id] = "consumer"
            current_generator_count -= 1
    else:
        for node_id in _role_switch_candidates(controller, node_ids, "consumer"):
            if current_generator_count >= desired_generators:
                break
            new_roles[node_id] = "generator"
            current_generator_count += 1
    if new_roles == controller.roles:
        return False
    controller.set_roles(new_roles)
    log_event(
        LOGGER,
        logging.INFO,
        "tracker.elastic_role_rebalance",
        desired_generators=desired_generators,
        desired_consumers=len(node_ids) - desired_generators,
        expected_throughput=f"{plan.expected_throughput:.6f}",
        current_throughput=f"{current_throughput:.6f}",
        improvement=f"{improvement:.6f}",
        payback_saved_seconds=f"{payback.saved_seconds:.6f}",
        payback_current_seconds=f"{payback.current_seconds:.6f}",
        payback_planned_seconds=f"{payback.planned_seconds:.6f}",
        payback_swap_count=payback.swap_count,
        remaining_candidates=payback.remaining_candidates,
        queue_pressure=f"{plan.queue_pressure:.6f}",
        cqdag_frontier_pressure=f"{plan.cqdag_frontier_pressure:.6f}",
        cqdag_reclaim_pressure=f"{plan.cqdag_reclaim_pressure:.6f}",
    )
    return True


def _role_allocation_snapshot(
    args: CqdagTrackerServiceConfig,
    controller,
    publisher: StreamingRecordBatchPublisher,
    *,
    node_ids: tuple[str, ...],
    current_generator_count: int,
    current_consumer_count: int,
    limit: int,
) -> RoleAllocationInput:
    generator_rate = 0.0
    consumer_rate = 0.0
    generator_waits = 0
    generator_completed_items = 0
    source_cached_records = 0
    source_peak_cached_records = 0
    source_reclaimed_records = 0
    page_units = 0
    consumer_idle_seconds = 0.0
    consumer_elapsed_seconds = 0.0
    consumed_candidates = 0
    for node_id in node_ids:
        status = controller.status_by_node[node_id]
        role = _effective_role(controller, node_id, _status_resources(status))
        if role == "generator":
            generator_rate += float(status.get("generation_rate", 0.0) or 0.0)
            generator_waits += int(status.get("waits", 0) or 0)
            generator_completed_items += int(status.get("completed_items", 0) or 0)
            source_cached_records += int(status.get("source_cached_records", 0) or 0)
            source_peak_cached_records += int(status.get("source_peak_cached_records", 0) or 0)
            source_reclaimed_records += int(status.get("source_reclaimed_records", 0) or 0)
            page_units += int(status.get("source_dag_repository_active_units", 0) or 0)
            page_units += int(status.get("source_dag_stream_active_units", 0) or 0)
        elif role == "consumer":
            consumer_rate += float(status.get("consumer_rate", 0.0) or 0.0)
            consumer_idle_seconds += float(status.get("network_poll_seconds", 0.0) or 0.0)
            consumer_elapsed_seconds += float(status.get("elapsed_seconds", 0.0) or 0.0)
            consumed_candidates += int(status.get("consumed_candidates", 0) or 0)

    pending_candidates = max(0, publisher.published_candidates - consumed_candidates)
    remaining_candidates = max(0, limit - publisher.published_candidates)
    max_pending_candidates = args.batch_size * max(1, current_consumer_count) * 4
    queue_pressure = _bounded_ratio(pending_candidates, max_pending_candidates)
    generator_rate_per_node = max(generator_rate / current_generator_count, 1e-9)
    consumer_rate_per_node = max(consumer_rate / current_consumer_count, 1e-9)
    generator_idle_ratio = _bounded_ratio(
        generator_waits,
        generator_waits + generator_completed_items,
    )
    consumer_idle_ratio = _bounded_ratio(consumer_idle_seconds, consumer_elapsed_seconds)
    cache_pressure = _bounded_ratio(
        source_cached_records,
        max(source_cached_records, source_peak_cached_records, 1),
    )
    reclaim_pressure = max(
        queue_pressure,
        cache_pressure
        * (
            1.0
            - _bounded_ratio(
                source_reclaimed_records,
                source_reclaimed_records + source_cached_records,
            )
        ),
    )
    frontier_pressure = _bounded_ratio(
        (1.0 - queue_pressure) * consumer_idle_ratio
        + max(0.0, consumer_rate_per_node - generator_rate_per_node)
        / max(generator_rate_per_node + consumer_rate_per_node, 1e-9),
        1.0,
    )
    priority_pressure = 1.0 - _bounded_ratio(
        publisher.published_candidates,
        max(1, int(publisher.protocol_metrics.get("emitted_records", 0) or 0)),
    )
    page_locality = _bounded_ratio(
        page_units,
        max(1, current_generator_count * args.model_slot_page_size),
    )
    return RoleAllocationInput(
        total_nodes=len(node_ids),
        generator_rate_per_node=generator_rate_per_node,
        consumer_rate_per_node=consumer_rate_per_node,
        current_generator_count=current_generator_count,
        remaining_candidates=remaining_candidates,
        pending_candidates=pending_candidates,
        max_pending_candidates=max_pending_candidates,
        generator_idle_ratio=generator_idle_ratio,
        consumer_idle_ratio=consumer_idle_ratio,
        migration_cost_per_role_swap=args.batch_size,
        role_swap_cost_seconds=_role_swap_cost_seconds(args),
        cqdag_frontier_pressure=frontier_pressure,
        cqdag_priority_pressure=priority_pressure,
        cqdag_reclaim_pressure=reclaim_pressure,
        cqdag_page_locality=page_locality,
    )


def _role_swap_cost_seconds(args: CqdagTrackerServiceConfig) -> float:
    return max(
        args.role_rebalance_interval_seconds,
        args.batch_startup_grace_seconds,
        0.5,
    )


def _role_switch_candidates(controller, node_ids: tuple[str, ...], role: str) -> tuple[str, ...]:
    selected = tuple(
        node_id
        for node_id in node_ids
        if _effective_role(controller, node_id, _status_resources(controller.status_by_node[node_id]))
        == role
    )
    if role == "generator":
        return tuple(
            sorted(
                selected,
                key=lambda node_id: (
                    int(controller.status_by_node[node_id].get("source_cached_records", 0) or 0)
                    + int(
                        controller.status_by_node[node_id].get(
                            "source_dag_repository_active_units",
                            0,
                        )
                        or 0
                    )
                    + int(
                        controller.status_by_node[node_id].get(
                            "source_dag_stream_active_units",
                            0,
                        )
                        or 0
                    ),
                    int(controller.status_by_node[node_id].get("completed_records", 0) or 0),
                    node_id,
                ),
            )
        )
    return tuple(
        sorted(
            selected,
            key=lambda node_id: (
                -float(controller.status_by_node[node_id].get("network_poll_seconds", 0.0) or 0.0),
                float(controller.status_by_node[node_id].get("consumer_rate", 0.0) or 0.0),
                node_id,
            ),
        )
    )


def _bounded_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return min(1.0, max(0.0, numerator / denominator))


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


def job_payload_item_count(payload: Mapping[str, Any]) -> int:
    targets = payload.get("targets")
    if isinstance(targets, (tuple, list)):
        return len(targets)
    return len(payload)


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
    if args.public_control_connect is None:
        args.public_control_connect = public_bundle.control
    if args.public_batch_connect is None:
        args.public_batch_connect = public_bundle.batch
    if args.public_ack_connect is None:
        args.public_ack_connect = public_bundle.ack
    if args.public_model_connect is None:
        args.public_model_connect = public_bundle.model


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
