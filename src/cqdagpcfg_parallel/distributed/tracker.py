from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import Event
from time import monotonic
from typing import Any, Callable
from urllib.parse import unquote, urlparse

from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.protocol import (
    EnumerationChunk,
    InMemoryChunkStore,
    LeaseTable,
    NodeDependency,
    NodeId,
    NodeSchedulingFeatures,
    NodeStateTable,
    PriorityCostScheduler,
    SchedulerConfig,
    StableStreamFingerprint,
    StaleLeaseError,
    WorkerId,
    WorkItem,
    stable_record_string,
)
from cqdagpcfg_parallel.runtime.zmq_transport import (
    ZmqEndpoint,
    _require_zmq,
    configure_zmq_socket,
)
from cqdagpcfg_parallel.simulation import (
    GlobalMerger,
    ProtocolRunStats,
    RecordOrderKey,
    RootShard,
    default_record_order_key,
)
from cqdagpcfg_parallel.storage import DistributedTrackerCheckpoint

from .messages import (
    ControlMessage,
    ControlMessageCodec,
    error_message,
    migrate_abort_message,
    migrate_commit_message,
    migrate_install_message,
    migrate_prepare_message,
    stop_message,
    wait_message,
    work_message,
)
from .memory_policy import (
    DEFAULT_STREAMING_ARTIFACT_RECORD_BYTES,
    DEFAULT_STREAMING_ARTIFACT_WORK_FRACTION,
    memory_limited_chunk_size,
)
from .migration import (
    MigrationCoordinator,
    MigrationStats,
    MigrationTicket,
    SnapshotPolicy,
    content_digest,
)
from .resources import WorkerResourceSpec


@dataclass(frozen=True, slots=True)
class DistributedProtocolConfig:
    scheduler: SchedulerConfig = SchedulerConfig()
    node_id: NodeId = NodeId("root")
    node_ids: tuple[NodeId, ...] | None = None
    demand_window: int = 16
    entropy: float = 0.0
    priority: float = 1.0
    estimated_cost: float = 1.0
    node_features: tuple[NodeSchedulingFeatures, ...] = ()
    node_dependencies: tuple[NodeDependency, ...] = ()
    record_order_key: Callable[[GuessRecord], RecordOrderKey] | None = None
    poll_timeout_ms: int = 50
    reclaim_emitted_chunks: bool = True
    model_fingerprint: str | None = None
    snapshot_policy: SnapshotPolicy = SnapshotPolicy()
    lease_ttl_seconds: float = 30.0
    lease_recovery_enabled: bool = True
    automatic_migration_enabled: bool = True
    lazy_shard_activation: bool = True
    direct_root_chunk_emission: bool = True
    direct_unordered_chunk_emission: bool = False
    direct_unordered_pipeline_depth: int = 1
    track_output_digest: bool = True
    safe_record_chunk_size: int = 8192

    def __post_init__(self) -> None:
        if self.demand_window <= 0:
            raise ValueError("demand_window must be positive")
        if self.entropy < 0.0:
            raise ValueError("entropy cannot be negative")
        if self.priority < 0.0:
            raise ValueError("priority cannot be negative")
        if self.estimated_cost <= 0.0:
            raise ValueError("estimated_cost must be positive")
        if self.poll_timeout_ms <= 0:
            raise ValueError("poll_timeout_ms must be positive")
        if self.node_ids is not None and not self.node_ids:
            raise ValueError("node_ids cannot be empty")
        if self.lease_ttl_seconds <= 0.0:
            raise ValueError("lease_ttl_seconds must be positive")
        if self.direct_unordered_pipeline_depth <= 0:
            raise ValueError("direct_unordered_pipeline_depth must be positive")
        if self.safe_record_chunk_size <= 0:
            raise ValueError("safe_record_chunk_size must be positive")

    @property
    def root_node_ids(self) -> tuple[NodeId, ...]:
        return self.node_ids if self.node_ids is not None else (self.node_id,)

    def feature_for(self, node_id: NodeId) -> NodeSchedulingFeatures:
        for feature in self.node_features:
            if feature.node_id == node_id:
                return feature
        return NodeSchedulingFeatures(
            node_id=node_id,
            entropy=self.entropy,
            priority=self.priority,
            estimated_cost=self.estimated_cost,
        )


@dataclass(frozen=True, slots=True)
class DistributedRunResult:
    outputs: tuple[GuessRecord, ...]
    digest: str
    stable_fingerprint: str
    emitted_count: int
    stats: ProtocolRunStats
    seen_workers: tuple[WorkerId, ...]
    stopped_workers: tuple[WorkerId, ...]
    received_chunks: int
    received_records: int
    assigned_items_by_worker: tuple[tuple[WorkerId, int], ...] = ()
    assigned_records_by_worker: tuple[tuple[WorkerId, int], ...] = ()
    assigned_items_by_node: tuple[tuple[NodeId, int], ...] = ()
    assigned_records_by_node: tuple[tuple[NodeId, int], ...] = ()
    received_records_by_worker: tuple[tuple[WorkerId, int], ...] = ()
    received_records_by_node: tuple[tuple[NodeId, int], ...] = ()
    migration_stats: MigrationStats = MigrationStats()
    expired_leases: int = 0
    automatic_migrations: int = 0
    failed_migration_triggers: int = 0
    worker_chunk_caps: tuple[tuple[WorkerId, int], ...] = ()

    @property
    def stable_records(self) -> tuple[str, ...]:
        return tuple(stable_record_string(record) for record in self.outputs)


class DistributedProtocolTracker:
    def __init__(
        self,
        *,
        endpoint: ZmqEndpoint,
        config: DistributedProtocolConfig | None = None,
        context: Any | None = None,
    ) -> None:
        if not endpoint.bind:
            raise ValueError("tracker endpoint must bind")
        self.endpoint = endpoint
        self.config = DistributedProtocolConfig() if config is None else config
        self.context = context
        self.chunk_store = InMemoryChunkStore()
        self.states = NodeStateTable()
        self.leases = LeaseTable(default_ttl_seconds=self.config.lease_ttl_seconds)
        self.scheduler = PriorityCostScheduler(
            states=self.states,
            chunk_store=self.chunk_store,
            leases=self.leases,
            config=self.config.scheduler,
        )
        self.migrations = MigrationCoordinator(
            self.leases,
            snapshot_policy=self.config.snapshot_policy,
        )
        self._assigned_items_by_worker: dict[WorkerId, int] = {}
        self._assigned_records_by_worker: dict[WorkerId, int] = {}
        self._assigned_items_by_node: dict[NodeId, int] = {}
        self._assigned_records_by_node: dict[NodeId, int] = {}
        self._received_records_by_worker: dict[WorkerId, int] = {}
        self._received_records_by_node: dict[NodeId, int] = {}
        self._resources_by_worker: dict[WorkerId, WorkerResourceSpec] = {}
        self._worker_chunk_caps_by_worker: dict[WorkerId, int] = {}
        self._control_outbox: dict[WorkerId, deque[ControlMessage]] = {}
        self._last_seen_by_worker: dict[WorkerId, float] = {}
        self._stopped_workers: set[WorkerId] = set()
        self._pending_root_chunks: dict[int, ControlMessage] = {}
        self._direct_unordered_active_nodes: set[NodeId] = set()
        self._direct_unordered_shard_cursor = 0
        self._expired_leases = 0
        self._automatic_migrations = 0
        self._failed_migration_triggers = 0
        shards: list[RootShard] = []
        for node_id in self.config.root_node_ids:
            feature = self.config.feature_for(node_id)
            shards.append(
                RootShard(
                    node_id=node_id,
                    chunk_store=self.chunk_store,
                    states=self.states,
                    demand_window=self.config.demand_window,
                    entropy=feature.entropy,
                    priority=feature.priority,
                    estimated_cost=feature.estimated_cost,
                    cardinality=feature.cardinality,
                    order_key=self.config.record_order_key or default_record_order_key,
                )
            )
        self.shards = tuple(shards)
        self._shard_by_node_id = {shard.node_id: shard for shard in self.shards}
        self._direct_unordered_shards_by_priority = tuple(
            sorted(
                self.shards,
                key=lambda shard: (-shard.priority, str(shard.node_id)),
            )
        )
        for dependency in self.config.node_dependencies:
            self.states.register_dependency(
                dependency.parent_id,
                dependency.child_id,
                donation_weight=dependency.donation_weight,
            )
        self.merger = GlobalMerger(
            self.shards,
            lazy_shard_activation=self.config.lazy_shard_activation,
        )

    def request_node_migration(
        self,
        *,
        node_id: NodeId,
        source_worker_id: WorkerId,
        target_worker_id: WorkerId,
        source_epoch: int,
        reason: str = "rebalance",
    ) -> MigrationTicket:
        ticket = self.migrations.prepare(
            node_id=node_id,
            source_worker_id=source_worker_id,
            target_worker_id=target_worker_id,
            source_epoch=source_epoch,
            model_fingerprint=self.config.model_fingerprint,
            reason=reason,
        )
        self._queue_control(
            source_worker_id,
            migrate_prepare_message(
                migration_id=ticket.migration_id,
                node_id=node_id,
                source_worker_id=source_worker_id,
                target_worker_id=target_worker_id,
                source_epoch=source_epoch,
                model_fingerprint=self.config.model_fingerprint,
            ),
        )
        return ticket

    def run(
        self,
        *,
        limit: int,
        expected_workers: int | None,
        timeout_seconds: float = 10.0,
        started_event: Event | None = None,
        shutdown_grace_seconds: float = 0.2,
        output_callback: Callable[[GuessRecord], None] | None = None,
        output_records_callback: Callable[[tuple[GuessRecord, ...]], None] | None = None,
        output_artifact_callback: Callable[..., None] | None = None,
        schedule_backpressure_callback: Callable[[], bool] | None = None,
        collect_outputs: bool = False,
        resume_checkpoint: DistributedTrackerCheckpoint | None = None,
        checkpoint_callback: Callable[[DistributedTrackerCheckpoint], None] | None = None,
        checkpoint_interval_records: int = 1,
    ) -> DistributedRunResult:
        if limit < 0:
            raise ValueError("limit cannot be negative")
        if expected_workers is not None and expected_workers <= 0:
            raise ValueError("expected_workers must be positive")
        if timeout_seconds <= 0.0:
            raise ValueError("timeout_seconds must be positive")
        if shutdown_grace_seconds < 0.0:
            raise ValueError("shutdown_grace_seconds cannot be negative")
        if checkpoint_interval_records <= 0:
            raise ValueError("checkpoint_interval_records must be positive")
        if (
            resume_checkpoint is not None
            and resume_checkpoint.emitted_count > 0
            and not resume_checkpoint.stable_records_for_resume()
        ):
            raise ValueError("resume checkpoint must include stable records or emitted_log_uri")

        zmq = _require_zmq()
        owns_context = self.context is None
        context = zmq.Context() if self.context is None else self.context
        socket = context.socket(zmq.ROUTER)
        configure_zmq_socket(
            socket,
            self.endpoint,
            zmq_module=zmq,
            send=True,
            recv=True,
        )
        socket.bind(self.endpoint.address)

        if resume_checkpoint is not None:
            self._apply_checkpoint(resume_checkpoint)
        collector = _OutputCollector(
            collect_outputs=collect_outputs,
            initial_stable_records=(
                ()
                if resume_checkpoint is None
                else resume_checkpoint.stable_records_for_resume()
            ),
            track_stable_records=checkpoint_callback is not None
            or resume_checkpoint is not None,
            track_digest=self.config.track_output_digest,
        )
        seen_workers: set[WorkerId] = set()
        stopped_workers: set[WorkerId] = set()
        received_chunks = 0
        received_records = 0
        deadline = monotonic() + timeout_seconds
        shutdown_deadline: float | None = None
        if started_event is not None:
            started_event.set()

        try:
            if self.config.direct_unordered_chunk_emission:
                self._register_direct_unordered_demands(limit)
            self._advance_outputs(
                collector,
                limit,
                output_callback=output_callback,
                checkpoint_callback=checkpoint_callback,
                checkpoint_interval_records=checkpoint_interval_records,
            )
            while True:
                if self._is_done(collector.emitted_count, limit):
                    if shutdown_deadline is None:
                        shutdown_deadline = monotonic() + shutdown_grace_seconds
                    if monotonic() >= shutdown_deadline:
                        break
                if expected_workers is not None and len(stopped_workers) >= expected_workers:
                    break
                if (
                    self.config.direct_unordered_chunk_emission
                    and self._direct_unordered_exhausted()
                ):
                    if shutdown_deadline is None:
                        shutdown_deadline = monotonic() + shutdown_grace_seconds
                    if monotonic() >= shutdown_deadline:
                        break
                if monotonic() > deadline:
                    raise TimeoutError("distributed protocol tracker timed out")
                if socket.poll(self.config.poll_timeout_ms, zmq.POLLIN) == 0:
                    self._recover_expired_leases()
                    continue

                identity, payload = socket.recv_multipart()
                worker_id = WorkerId(identity.decode("utf-8"))
                seen_workers.add(worker_id)
                self._last_seen_by_worker[worker_id] = monotonic()

                try:
                    message = ControlMessageCodec.loads(payload)
                    reply = self._handle_message(
                        message,
                        worker_id=worker_id,
                        collector=collector,
                        limit=limit,
                        output_callback=output_callback,
                        output_records_callback=output_records_callback,
                        output_artifact_callback=output_artifact_callback,
                        schedule_backpressure_callback=schedule_backpressure_callback,
                        checkpoint_callback=checkpoint_callback,
                        checkpoint_interval_records=checkpoint_interval_records,
                    )
                    if message.type == "chunk":
                        received_chunks += 1
                        received_records += _message_record_count(message)
                    if reply.type == "stop":
                        stopped_workers.add(worker_id)
                        self._stopped_workers.add(worker_id)
                except BaseException as exc:  # pragma: no cover - covered by timeout if fatal
                    reply = error_message(str(exc))

                socket.send_multipart([identity, ControlMessageCodec.dumps(reply)])
        finally:
            socket.close()
            if owns_context:
                context.term()

        chunk_stats = self.chunk_store.stats()
        schedule_stats = self.scheduler.stats
        if checkpoint_callback is not None:
            checkpoint_callback(self.checkpoint(collector))
        return DistributedRunResult(
            outputs=collector.outputs,
            digest=collector.digest,
            stable_fingerprint=collector.stable_fingerprint,
            emitted_count=collector.emitted_count,
            stats=ProtocolRunStats(
                scheduled_items=schedule_stats.scheduled_items,
                scheduled_records=schedule_stats.scheduled_records,
                publish_count=chunk_stats.publish_count,
                duplicate_publish_count=chunk_stats.duplicate_publish_count,
                ready_end=sum(
                    self.chunk_store.ready_end(node_id)
                    for node_id in self.config.root_node_ids
                ),
                resident_records=chunk_stats.record_count,
                peak_resident_records=chunk_stats.peak_record_count,
                reclaimed_records=chunk_stats.reclaimed_record_count,
                affinity_hits=schedule_stats.affinity_hits,
                affinity_misses=schedule_stats.affinity_misses,
                parallel_items=schedule_stats.parallel_items,
                tail_steal_attempts=schedule_stats.tail_steal_attempts,
                tail_steals=schedule_stats.tail_steals,
                tail_steal_denials=schedule_stats.tail_steal_denials,
                rank_window_waits=schedule_stats.rank_window_waits,
                rank_window_forced_items=schedule_stats.rank_window_forced_items,
                rank_window_peak_outstanding_records=(
                    schedule_stats.rank_window_peak_outstanding_records
                ),
            ),
            seen_workers=tuple(sorted(seen_workers, key=str)),
            stopped_workers=tuple(sorted(stopped_workers, key=str)),
            received_chunks=received_chunks,
            received_records=received_records,
            assigned_items_by_worker=_sorted_items(self._assigned_items_by_worker),
            assigned_records_by_worker=_sorted_items(self._assigned_records_by_worker),
            assigned_items_by_node=_sorted_items(self._assigned_items_by_node),
            assigned_records_by_node=_sorted_items(self._assigned_records_by_node),
            received_records_by_worker=_sorted_items(self._received_records_by_worker),
            received_records_by_node=_sorted_items(self._received_records_by_node),
            migration_stats=self.migrations.stats,
            expired_leases=self._expired_leases,
            automatic_migrations=self._automatic_migrations,
            failed_migration_triggers=self._failed_migration_triggers,
            worker_chunk_caps=_sorted_items(self._worker_chunk_caps_by_worker),
        )

    def _handle_message(
        self,
        message: ControlMessage,
        *,
        worker_id: WorkerId,
        collector: "_OutputCollector",
        limit: int,
        output_callback: Callable[[GuessRecord], None] | None,
        output_records_callback: Callable[[tuple[GuessRecord, ...]], None] | None = None,
        output_artifact_callback: Callable[..., None] | None = None,
        schedule_backpressure_callback: Callable[[], bool] | None = None,
        checkpoint_callback: Callable[[DistributedTrackerCheckpoint], None] | None = None,
        checkpoint_interval_records: int = 1,
    ) -> ControlMessage:
        self._validate_worker_identity(message, worker_id)
        self._record_worker_resources(message, worker_id)
        if self._is_done(collector.emitted_count, limit):
            self._delete_unpublished_artifacts(message)
            return stop_message()

        if message.type == "chunk":
            migration_reply = self._accept_chunk(
                message,
                worker_id=worker_id,
                collector=collector,
                limit=limit,
                output_callback=output_callback,
                output_records_callback=output_records_callback,
                output_artifact_callback=output_artifact_callback,
            )
            if migration_reply is not None:
                return migration_reply
        elif message.type == "exhausted":
            self._accept_exhausted(message, worker_id=worker_id)
        elif message.type == "retire":
            return stop_message()
        elif message.type.startswith("migrate_"):
            return self._handle_migration_message(message, worker_id=worker_id)
        elif message.type != "ready":
            raise RuntimeError(f"unexpected worker message: {message.type}")

        queued = self._pop_control(worker_id)
        if queued is not None:
            return queued

        self._advance_outputs(
            collector,
            limit,
            output_callback=output_callback,
            checkpoint_callback=checkpoint_callback,
            checkpoint_interval_records=checkpoint_interval_records,
        )
        if message.retire or self._is_done(collector.emitted_count, limit):
            return stop_message()
        if (
            schedule_backpressure_callback is not None
            and schedule_backpressure_callback()
        ):
            return wait_message()

        worker_chunk_cap = self._worker_chunk_cap(worker_id)
        if self.config.direct_unordered_chunk_emission:
            remaining_budget = (
                limit
                - collector.emitted_count
                - self._direct_unordered_inflight_records()
            )
            if remaining_budget <= 0:
                return wait_message()
            worker_chunk_cap = min(worker_chunk_cap, remaining_budget)

        item = self.scheduler.schedule(
            worker_id,
            max_chunk_size=worker_chunk_cap,
        )
        if item is None:
            return wait_message()
        self._record_assignment(item)
        return work_message(item)

    def _record_worker_resources(
        self,
        message: ControlMessage,
        worker_id: WorkerId,
    ) -> None:
        if message.worker_resources is not None:
            self._resources_by_worker[worker_id] = message.worker_resources

    def _worker_chunk_cap(self, worker_id: WorkerId) -> int:
        resources = self._resources_by_worker.get(worker_id)
        configured_max = self.config.scheduler.max_chunk_size
        if self._worker_supports_streaming_artifacts(resources):
            cap = memory_limited_chunk_size(
                resources,
                configured_max,
                work_fraction=DEFAULT_STREAMING_ARTIFACT_WORK_FRACTION,
                estimated_record_bytes=DEFAULT_STREAMING_ARTIFACT_RECORD_BYTES,
            )
            self._worker_chunk_caps_by_worker[worker_id] = cap
            return cap
        else:
            configured_max = min(configured_max, self.config.safe_record_chunk_size)
        cap = memory_limited_chunk_size(
            resources,
            configured_max,
        )
        self._worker_chunk_caps_by_worker[worker_id] = cap
        return cap

    def _worker_supports_streaming_artifacts(
        self,
        resources: WorkerResourceSpec | None,
    ) -> bool:
        if resources is None:
            return False
        return resources.labels.get("cqpcfg.streaming_artifacts") == "1"

    def _accept_chunk(
        self,
        message: ControlMessage,
        *,
        worker_id: WorkerId,
        collector: "_OutputCollector",
        limit: int,
        output_callback: Callable[[GuessRecord], None] | None,
        output_records_callback: Callable[[tuple[GuessRecord, ...]], None] | None,
        output_artifact_callback: Callable[..., None] | None,
    ) -> ControlMessage | None:
        if message.work_item is None:
            raise RuntimeError("chunk message is missing work_item")
        item = message.work_item
        self._record_received(item, worker_id, _message_record_count(message))
        try:
            self.leases.require_valid(
                node_id=item.node_id,
                worker_id=worker_id,
                epoch=item.epoch,
                start=item.start,
                end=item.end,
            )
        except StaleLeaseError:
            self._delete_unpublished_artifacts(message)
            return wait_message()

        if self._is_direct_root_chunk(item) and (
            self._is_artifact_chunk(message) or message.records
        ):
            self._record_runtime_feedback(message)
            self._accept_direct_root_chunk(
                message,
                collector=collector,
                limit=limit,
                output_callback=output_callback,
                output_records_callback=output_records_callback,
                output_artifact_callback=output_artifact_callback,
            )
            return None
        if self._is_direct_unordered_chunk(item) and (
            self._is_artifact_chunk(message) or message.records
        ):
            self._record_runtime_feedback(message)
            self._accept_direct_unordered_chunk(
                message,
                collector=collector,
                limit=limit,
                output_callback=output_callback,
                output_records_callback=output_records_callback,
                output_artifact_callback=output_artifact_callback,
            )
            return None
        if self._is_artifact_chunk(message):
            raise RuntimeError(
                "artifact chunks are only supported for direct root range emission"
            )
        elif message.records:
            chunk = EnumerationChunk.from_records(
                node_id=item.node_id,
                start=item.start,
                records=message.records,
                worker_id=worker_id,
                epoch=item.epoch,
            )
            ready_end = self.chunk_store.publish(chunk)
            state = self.states.update_ready_end(item.node_id, ready_end)
            if len(message.records) < item.size:
                state.mark_exhausted(item.start + len(message.records))
        else:
            self.states.ensure_node(item.node_id).mark_exhausted(item.start)
        self._record_runtime_feedback(message)

        lease = self.leases.lease_for(item.node_id, item.epoch)
        if lease is not None:
            migration_reply = self._maybe_migrate_retiring_worker(
                message,
                worker_id=worker_id,
                item=item,
            )
            if migration_reply is not None:
                return migration_reply
            self.leases.release(lease)
        return None

    def _is_artifact_chunk(self, message: ControlMessage) -> bool:
        return (
            message.artifact_uri is not None
            and message.artifact_sha256 is not None
            and message.artifact_record_count is not None
        )

    def _is_direct_root_chunk(self, item: WorkItem) -> bool:
        return (
            self.config.direct_root_chunk_emission
            and len(self.shards) == 1
            and item.node_id == self.shards[0].node_id
        )

    def _is_direct_unordered_chunk(self, item: WorkItem) -> bool:
        return (
            self.config.direct_unordered_chunk_emission
            and item.node_id in self._shard_by_node_id
        )

    def _register_direct_unordered_demands(self, limit: int) -> None:
        if limit <= 0:
            return
        if self._is_done_count_only(limit):
            return
        if self._direct_unordered_scheduled_count() >= limit:
            return
        self._direct_unordered_active_nodes = {
            node_id
            for node_id in self._direct_unordered_active_nodes
            if not self.states.get(node_id).exhausted
        }
        active_target = max(
            1,
            self.config.scheduler.max_parallel_leases_per_node
            * self.config.direct_unordered_pipeline_depth,
        )
        while (
            len(self._direct_unordered_active_nodes) < active_target
            and self._direct_unordered_shard_cursor < len(self._direct_unordered_shards_by_priority)
        ):
            shard = self._direct_unordered_shards_by_priority[
                self._direct_unordered_shard_cursor
            ]
            self._direct_unordered_shard_cursor += 1
            if self.states.get(shard.node_id).exhausted:
                continue
            self._direct_unordered_active_nodes.add(shard.node_id)
        planned_count = self._direct_unordered_scheduled_count()
        for node_id in tuple(self._direct_unordered_active_nodes):
            remaining = limit - planned_count
            if remaining <= 0:
                return
            shard = self._shard_by_node_id.get(node_id)
            if shard is None:
                continue
            state = self.states.get(shard.node_id)
            if state.exhausted:
                continue
            ready_end = self.chunk_store.ready_end(shard.node_id)
            target_base = max(ready_end, state.scheduled_end)
            target_end = target_base + self.config.demand_window
            target_end = min(target_end, target_base + remaining)
            target_end = min(target_end, limit)
            if shard.cardinality is not None:
                target_end = min(target_end, shard.cardinality)
            if target_end <= target_base:
                state.mark_exhausted(target_base)
                continue
            self.states.register_demand(
                shard.node_id,
                target_end,
                entropy=shard.entropy,
                priority=shard.priority,
                estimated_cost=shard.estimated_cost,
            )
            planned_count += target_end - target_base

    def _is_done_count_only(self, limit: int) -> bool:
        # Direct unordered emission may intentionally overrun by the final
        # artifact chunk because artifact files are immutable batch payloads.
        return sum(shard.cursor for shard in self.shards) >= limit

    def _direct_unordered_scheduled_count(self) -> int:
        count = 0
        for shard in self.shards:
            state = self.states.get(shard.node_id)
            count += max(shard.cursor, state.scheduled_end, state.effective_target_end)
        return count

    def _direct_unordered_inflight_records(self) -> int:
        total = 0
        for shard in self.shards:
            for lease in self.leases.active_leases(shard.node_id):
                if lease.end is not None:
                    total += max(0, lease.end - lease.start)
        return total

    def _direct_unordered_exhausted(self) -> bool:
        if not self.shards:
            return True
        for shard in self.shards:
            if not self.states.get(shard.node_id).exhausted:
                return False
            if self.leases.active_count(shard.node_id) > 0:
                return False
        return True

    def _can_direct_emit_root_chunk(
        self,
        item: WorkItem,
        collector: "_OutputCollector",
    ) -> bool:
        return (
            self.config.direct_root_chunk_emission
            and len(self.shards) == 1
            and item.node_id == self.shards[0].node_id
            and item.start == collector.emitted_count
        )

    def _accept_direct_root_chunk(
        self,
        message: ControlMessage,
        *,
        collector: "_OutputCollector",
        limit: int,
        output_callback: Callable[[GuessRecord], None] | None,
        output_records_callback: Callable[[tuple[GuessRecord, ...]], None] | None,
        output_artifact_callback: Callable[..., None] | None,
    ) -> None:
        if message.work_item is None:
            raise RuntimeError("root chunk is missing work_item")
        item = message.work_item
        record_count = _message_record_count(message)
        state = self.states.ensure_node(item.node_id)
        if record_count < item.size:
            state.mark_exhausted(item.start + record_count)
        if record_count <= 0:
            self._release_lease_for_item(item)
            return
        if item.start < collector.emitted_count:
            self._delete_unpublished_artifacts(message)
            self._release_lease_for_item(item)
            return

        self._pending_root_chunks[item.start] = message
        while collector.emitted_count < limit:
            pending = self._pending_root_chunks.pop(collector.emitted_count, None)
            if pending is None:
                return
            self._emit_pending_root_chunk(
                pending,
                collector=collector,
                limit=limit,
                output_callback=output_callback,
                output_records_callback=output_records_callback,
                output_artifact_callback=output_artifact_callback,
            )

    def _accept_direct_unordered_chunk(
        self,
        message: ControlMessage,
        *,
        collector: "_OutputCollector",
        limit: int,
        output_callback: Callable[[GuessRecord], None] | None,
        output_records_callback: Callable[[tuple[GuessRecord, ...]], None] | None,
        output_artifact_callback: Callable[..., None] | None,
    ) -> None:
        if message.work_item is None:
            raise RuntimeError("direct chunk is missing work_item")
        item = message.work_item
        record_count = _message_record_count(message)
        ready_end = item.start + record_count
        state = self.states.ensure_node(item.node_id)
        if record_count < item.size:
            state.mark_exhausted(ready_end)
        if record_count <= 0:
            self._release_lease_for_item(item)
            return
        state = self.states.ensure_node(item.node_id)
        if ready_end > state.ready_end:
            self.states.update_ready_end(item.node_id, ready_end)
        if ready_end > self.chunk_store.base_offset(item.node_id):
            self.chunk_store.advance_base_offset(item.node_id, ready_end)
        if self._is_artifact_chunk(message):
            artifact_limit = limit
            if self.config.direct_unordered_chunk_emission:
                artifact_limit = max(limit, collector.emitted_count + record_count)
            self._emit_direct_artifact(
                item,
                message,
                collector=collector,
                limit=artifact_limit,
                output_artifact_callback=output_artifact_callback,
            )
        else:
            self._emit_direct_records(
                item,
                message.records,
                collector=collector,
                limit=limit,
                output_callback=output_callback,
                output_records_callback=output_records_callback,
            )
        self._release_lease_for_item(item)
        self._register_direct_unordered_demands(limit)

    def _emit_pending_root_chunk(
        self,
        message: ControlMessage,
        *,
        collector: "_OutputCollector",
        limit: int,
        output_callback: Callable[[GuessRecord], None] | None,
        output_records_callback: Callable[[tuple[GuessRecord, ...]], None] | None,
        output_artifact_callback: Callable[..., None] | None,
    ) -> None:
        if message.work_item is None:
            raise RuntimeError("root chunk is missing work_item")
        item = message.work_item
        ready_end = item.start + _message_record_count(message)
        self.states.update_ready_end(item.node_id, ready_end)
        if self._is_artifact_chunk(message):
            self._emit_direct_artifact(
                item,
                message,
                collector=collector,
                limit=limit,
                output_artifact_callback=output_artifact_callback,
            )
        else:
            self._emit_direct_records(
                item,
                message.records,
                collector=collector,
                limit=limit,
                output_callback=output_callback,
                output_records_callback=output_records_callback,
            )
        self._release_lease_for_item(item)

    def _release_lease_for_item(self, item: WorkItem) -> None:
        lease = self.leases.lease_for(item.node_id, item.epoch)
        if lease is not None:
            self.leases.release(lease)


    def _emit_direct_records(
        self,
        item: WorkItem,
        records: tuple[GuessRecord, ...],
        *,
        collector: "_OutputCollector",
        limit: int,
        output_callback: Callable[[GuessRecord], None] | None,
        output_records_callback: Callable[[tuple[GuessRecord, ...]], None] | None,
    ) -> None:
        remaining = limit - collector.emitted_count
        if remaining <= 0:
            return
        emitted = records[:remaining]
        collector.extend(emitted)
        if output_records_callback is not None:
            output_records_callback(emitted)
        elif output_callback is not None:
            for record in emitted:
                output_callback(record)

        shard = self._shard_for_item(item)
        shard.cursor = max(shard.cursor, item.start + len(emitted))
        self._update_direct_frontier_start(item.node_id, shard.cursor)
        if self.config.reclaim_emitted_chunks:
            self.chunk_store.advance_base_offset(item.node_id, shard.cursor)

    def _emit_direct_artifact(
        self,
        item: WorkItem,
        message: ControlMessage,
        *,
        collector: "_OutputCollector",
        limit: int,
        output_artifact_callback: Callable[..., None] | None,
    ) -> None:
        record_count = message.artifact_record_count or 0
        if record_count <= 0:
            return
        remaining = limit - collector.emitted_count
        emit_count = min(record_count, max(0, remaining))
        if emit_count <= 0:
            self._delete_unpublished_artifacts(message)
            return
        if emit_count != record_count:
            raise RuntimeError("direct artifact chunk cannot be partially emitted")
        if self.config.track_output_digest:
            if message.stable_artifact_uri is not None and message.stable_artifact_sha256 is not None:
                collector.extend_stable_artifact(
                    message.stable_artifact_uri,
                    expected_sha256=message.stable_artifact_sha256,
                    record_count=emit_count,
                )
                _delete_file_uri(message.stable_artifact_uri)
            elif (
                message.stable_fingerprint is not None
                and message.stable_fingerprint_bytes is not None
            ):
                collector.extend_stable_fingerprint(
                    message.stable_fingerprint,
                    byte_length=message.stable_fingerprint_bytes,
                    record_count=emit_count,
                )
            else:
                raise RuntimeError(
                    "artifact chunk is missing stable digest or fingerprint"
                )
        else:
            if message.stable_artifact_uri is not None:
                _delete_file_uri(message.stable_artifact_uri)
            collector.extend_count(emit_count)

        if output_artifact_callback is not None:
            output_artifact_callback(
                record_count=emit_count,
                payload_bytes=message.artifact_payload_bytes or 0,
                artifact_uri=message.artifact_uri or "",
                artifact_sha256=message.artifact_sha256 or "",
                artifact_bytes=message.artifact_bytes or 0,
                probability_mass=message.artifact_probability_mass or 0.0,
                artifact_format=message.artifact_format or "guess-lines-v1",
            )

        shard = self._shard_for_item(item)
        shard.cursor = max(shard.cursor, item.start + emit_count)
        self._update_direct_frontier_start(item.node_id, shard.cursor)
        if self.config.reclaim_emitted_chunks:
            self.chunk_store.advance_base_offset(item.node_id, shard.cursor)

    def _update_direct_frontier_start(self, node_id: NodeId, cursor: int) -> None:
        state = self.states.get(node_id)
        if cursor > state.frontier_start:
            self.states.update_frontier_start(node_id, cursor)

    def _delete_unpublished_artifacts(self, message: ControlMessage) -> None:
        if message.artifact_uri is not None:
            _delete_file_uri(message.artifact_uri)
        if message.stable_artifact_uri is not None:
            _delete_file_uri(message.stable_artifact_uri)

    def _shard_for_item(self, item: WorkItem) -> RootShard:
        shard = self._shard_by_node_id.get(item.node_id)
        if shard is None:
            raise RuntimeError(f"unknown direct chunk node: {item.node_id}")
        return shard

    def _handle_migration_message(
        self,
        message: ControlMessage,
        *,
        worker_id: WorkerId,
    ) -> ControlMessage:
        if message.type == "migrate_state":
            return self._accept_migration_state(message, worker_id=worker_id)
        if message.type == "migrate_ack":
            return self._accept_migration_ack(message, worker_id=worker_id)
        if message.type == "migrate_abort":
            return self._accept_migration_abort(message, worker_id=worker_id)
        raise RuntimeError(f"unexpected tracker-side migration message: {message.type}")

    def _accept_migration_state(
        self,
        message: ControlMessage,
        *,
        worker_id: WorkerId,
    ) -> ControlMessage:
        if message.migration_id is None:
            raise RuntimeError("migration state is missing migration_id")
        if message.node_id is None:
            raise RuntimeError("migration state is missing node_id")
        if message.source_worker_id not in {None, worker_id}:
            raise RuntimeError("migration state source does not match sender")
        if message.target_worker_id is None:
            raise RuntimeError("migration state is missing target_worker_id")
        if message.source_epoch is None:
            raise RuntimeError("migration state is missing source_epoch")
        if message.snapshot_payload is None:
            raise RuntimeError("migration state is missing snapshot_payload")
        if message.snapshot_digest is not None:
            actual_digest = content_digest(message.snapshot_payload)
            if actual_digest != message.snapshot_digest:
                raise RuntimeError("migration state digest mismatch")

        ticket = self.migrations.attach_snapshot(
            message.migration_id,
            snapshot_payload=message.snapshot_payload,
            snapshot_digest=message.snapshot_digest,
        )
        snapshot_digest = ticket.snapshot_digest or content_digest(message.snapshot_payload)
        self._queue_control(
            ticket.target_worker_id,
            migrate_install_message(
                migration_id=ticket.migration_id,
                node_id=ticket.node_id,
                source_worker_id=ticket.source_worker_id,
                target_worker_id=ticket.target_worker_id,
                source_epoch=ticket.source_epoch,
                snapshot_payload=message.snapshot_payload,
                snapshot_digest=snapshot_digest,
                snapshot_bytes=ticket.snapshot_bytes,
                model_fingerprint=ticket.model_fingerprint,
            ),
        )
        return wait_message()

    def _accept_migration_ack(
        self,
        message: ControlMessage,
        *,
        worker_id: WorkerId,
    ) -> ControlMessage:
        if message.migration_id is None:
            raise RuntimeError("migration ack is missing migration_id")
        if message.target_worker_id not in {None, worker_id}:
            raise RuntimeError("migration ack target does not match sender")

        commit = self.migrations.commit(message.migration_id)
        commit_message = migrate_commit_message(
            migration_id=commit.ticket.migration_id,
            node_id=commit.ticket.node_id,
            source_worker_id=commit.ticket.source_worker_id,
            target_worker_id=commit.ticket.target_worker_id,
            source_epoch=commit.ticket.source_epoch,
            target_epoch=commit.target_lease.epoch,
            model_fingerprint=commit.ticket.model_fingerprint,
        )
        self._queue_control(commit.ticket.source_worker_id, commit_message)
        return commit_message

    def _accept_migration_abort(
        self,
        message: ControlMessage,
        *,
        worker_id: WorkerId,
    ) -> ControlMessage:
        if message.migration_id is None:
            raise RuntimeError("migration abort is missing migration_id")
        ticket = self.migrations.abort(message.migration_id)
        abort_message = migrate_abort_message(
            migration_id=ticket.migration_id,
            node_id=ticket.node_id,
            source_worker_id=ticket.source_worker_id,
            target_worker_id=ticket.target_worker_id,
            source_epoch=ticket.source_epoch,
            error=message.error,
            model_fingerprint=ticket.model_fingerprint,
        )
        counterpart = (
            ticket.target_worker_id
            if worker_id == ticket.source_worker_id
            else ticket.source_worker_id
        )
        self._queue_control(counterpart, abort_message)
        return wait_message()

    def _validate_worker_identity(self, message: ControlMessage, worker_id: WorkerId) -> None:
        if message.worker_id is not None and message.worker_id != worker_id:
            raise RuntimeError("message worker_id does not match ZMQ identity")
        if self.config.model_fingerprint is None:
            return
        if message.model_fingerprint is None:
            raise RuntimeError("worker did not report model_fingerprint")
        if message.model_fingerprint != self.config.model_fingerprint:
            raise RuntimeError("worker model_fingerprint does not match tracker model")

    def _accept_exhausted(self, message: ControlMessage, *, worker_id: WorkerId) -> None:
        if message.work_item is None:
            raise RuntimeError("exhausted message is missing work_item")
        item = message.work_item
        try:
            self.leases.require_valid(
                node_id=item.node_id,
                worker_id=worker_id,
                epoch=item.epoch,
                start=item.start,
                end=item.end,
            )
        except StaleLeaseError:
            return
        self.states.ensure_node(item.node_id).mark_exhausted(item.start)
        self._record_runtime_feedback(message)
        lease = self.leases.lease_for(item.node_id, item.epoch)
        if lease is not None:
            self.leases.release(lease)

    def _maybe_migrate_retiring_worker(
        self,
        message: ControlMessage,
        *,
        worker_id: WorkerId,
        item: WorkItem,
    ) -> ControlMessage | None:
        if not self.config.automatic_migration_enabled:
            return None
        if not message.retire or not message.records:
            return None
        if self.leases.active_count(item.node_id) > 1:
            return None
        target_worker = self._select_migration_target(worker_id)
        if target_worker is None:
            self._failed_migration_triggers += 1
            return None
        self._automatic_migrations += 1
        return self._prepare_migration_reply(
            node_id=item.node_id,
            source_worker_id=worker_id,
            target_worker_id=target_worker,
            source_epoch=item.epoch,
            reason="retire",
        )

    def _prepare_migration_reply(
        self,
        *,
        node_id: NodeId,
        source_worker_id: WorkerId,
        target_worker_id: WorkerId,
        source_epoch: int,
        reason: str,
    ) -> ControlMessage:
        ticket = self.request_node_migration(
            node_id=node_id,
            source_worker_id=source_worker_id,
            target_worker_id=target_worker_id,
            source_epoch=source_epoch,
            reason=reason,
        )
        queued = self._pop_control(source_worker_id)
        if queued is None:
            raise RuntimeError(f"migration ticket {ticket.migration_id} was not queued")
        return queued

    def _select_migration_target(self, source_worker_id: WorkerId) -> WorkerId | None:
        candidates = [
            worker_id
            for worker_id in self._last_seen_by_worker
            if worker_id != source_worker_id and worker_id not in self._stopped_workers
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda worker_id: self._last_seen_by_worker[worker_id])

    def _record_runtime_feedback(self, message: ControlMessage) -> None:
        if message.work_item is None or message.runtime_feedback is None:
            return
        feedback = message.runtime_feedback
        chunk_probability_mass = (
            message.artifact_probability_mass
            if message.artifact_probability_mass is not None
            else sum(max(0.0, record.prob) for record in message.records)
        )
        self.states.record_runtime_feedback(
            message.work_item.node_id,
            chunk_latency_seconds=feedback.chunk_latency_seconds,
            records_requested=feedback.records_requested,
            records_produced=feedback.records_produced,
            ewma_alpha=self.config.scheduler.feedback_ewma_alpha,
            chunk_probability_mass=chunk_probability_mass,
        )

    def _advance_outputs(
        self,
        collector: "_OutputCollector",
        limit: int,
        *,
        output_callback: Callable[[GuessRecord], None] | None,
        checkpoint_callback: Callable[[DistributedTrackerCheckpoint], None] | None,
        checkpoint_interval_records: int,
    ) -> None:
        while collector.emitted_count < limit:
            record = self.merger.next_ready()
            if record is None:
                return
            collector.append(record)
            if output_callback is not None:
                output_callback(record)
            if self.config.reclaim_emitted_chunks:
                self._reclaim_consumed_records()
            if (
                checkpoint_callback is not None
                and collector.emitted_count % checkpoint_interval_records == 0
            ):
                checkpoint_callback(self.checkpoint(collector))

    def checkpoint(self, collector: "_OutputCollector") -> DistributedTrackerCheckpoint:
        return DistributedTrackerCheckpoint.create(
            emitted_count=collector.emitted_count,
            shard_cursors={shard.node_id: shard.cursor for shard in self.shards},
            emitted_stable_records=collector.stable_records,
        )

    def _apply_checkpoint(self, checkpoint: DistributedTrackerCheckpoint) -> None:
        for shard in self.shards:
            cursor = checkpoint.cursor_for(shard.node_id)
            shard.cursor = cursor
            self.chunk_store.advance_base_offset(shard.node_id, cursor)
            self.states.update_ready_end(shard.node_id, cursor)

    def _is_done(self, emitted_count: int, limit: int) -> bool:
        states = self.states.values()
        return emitted_count >= limit or (
            bool(states) and all(state.exhausted for state in states)
        )

    def _record_assignment(self, item: WorkItem) -> None:
        _increment(self._assigned_items_by_worker, item.worker_id, 1)
        _increment(self._assigned_records_by_worker, item.worker_id, item.size)
        _increment(self._assigned_items_by_node, item.node_id, 1)
        _increment(self._assigned_records_by_node, item.node_id, item.size)

    def _record_received(
        self,
        item: WorkItem,
        worker_id: WorkerId,
        record_count: int,
    ) -> None:
        if record_count <= 0:
            return
        _increment(self._received_records_by_worker, worker_id, record_count)
        _increment(self._received_records_by_node, item.node_id, record_count)

    def _reclaim_consumed_records(self) -> None:
        for shard in self.shards:
            self.chunk_store.reclaim_before(shard.node_id, shard.cursor)

    def _recover_expired_leases(self) -> None:
        if not self.config.lease_recovery_enabled:
            return
        expired = self.leases.release_expired()
        if not expired:
            return
        self._expired_leases += len(expired)
        for lease in expired:
            ready_end = self.chunk_store.ready_end(lease.node_id)
            state = self.states.update_ready_end(lease.node_id, ready_end)
            state.reset_scheduled_end(
                self.leases.contiguous_reserved_end(lease.node_id, ready_end),
            )

    def _queue_control(self, worker_id: WorkerId, message: ControlMessage) -> None:
        self._control_outbox.setdefault(worker_id, deque()).append(message)

    def _pop_control(self, worker_id: WorkerId) -> ControlMessage | None:
        queue = self._control_outbox.get(worker_id)
        if not queue:
            return None
        message = queue.popleft()
        if not queue:
            del self._control_outbox[worker_id]
        return message


def _increment(mapping: dict, key: object, amount: int) -> None:
    mapping[key] = mapping.get(key, 0) + amount


def _sorted_items(mapping: dict) -> tuple:
    return tuple(sorted(mapping.items(), key=lambda item: str(item[0])))


def _message_record_count(message: ControlMessage) -> int:
    if message.records:
        return len(message.records)
    if message.artifact_record_count is not None:
        return int(message.artifact_record_count)
    return 0


def _open_file_uri(uri: str):
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path)).open("rb")
    if parsed.scheme == "":
        return Path(uri).open("rb")
    raise RuntimeError(f"unsupported tracker artifact URI scheme: {parsed.scheme}")


def _delete_file_uri(uri: str) -> None:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        path = Path(unquote(parsed.path))
    elif parsed.scheme == "":
        path = Path(uri)
    else:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        return


class _OutputCollector:
    def __init__(
        self,
        *,
        collect_outputs: bool,
        initial_stable_records: tuple[str, ...] = (),
        track_stable_records: bool = False,
        track_digest: bool = True,
    ) -> None:
        self._outputs: list[GuessRecord] | None = [] if collect_outputs else None
        self._track_digest = track_digest
        self._digest = sha256()
        self._fingerprint = StableStreamFingerprint()
        if self._track_digest:
            for stable_record in initial_stable_records:
                payload = stable_record.encode("utf-8") + b"\n"
                self._digest.update(payload)
                self._fingerprint = self._fingerprint.update_bytes(payload)
        self.emitted_count = len(initial_stable_records)
        self._stable_records: list[str] | None = (
            list(initial_stable_records) if track_stable_records else None
        )

    @property
    def outputs(self) -> tuple[GuessRecord, ...]:
        if self._outputs is None:
            return ()
        return tuple(self._outputs)

    @property
    def digest(self) -> str:
        return self._digest.hexdigest()

    @property
    def stable_fingerprint(self) -> str:
        return self._fingerprint.to_string()

    @property
    def stable_records(self) -> tuple[str, ...]:
        if self._stable_records is None:
            return ()
        return tuple(self._stable_records)

    def append(self, record: GuessRecord) -> None:
        stable_record = stable_record_string(record)
        if self._outputs is not None:
            self._outputs.append(record)
        if self._stable_records is not None:
            self._stable_records.append(stable_record)
        payload = stable_record.encode("utf-8") + b"\n"
        if self._track_digest:
            self._digest.update(payload)
        self._fingerprint = self._fingerprint.update_bytes(payload)
        self.emitted_count += 1

    def extend(self, records: tuple[GuessRecord, ...]) -> None:
        for record in records:
            self.append(record)

    def extend_count(self, record_count: int) -> None:
        if record_count < 0:
            raise ValueError("record_count cannot be negative")
        if self._outputs is not None or self._stable_records is not None:
            raise RuntimeError("count-only emission cannot collect outputs")
        self.emitted_count += record_count

    def extend_stable_fingerprint(
        self,
        fingerprint: str,
        *,
        byte_length: int,
        record_count: int,
    ) -> None:
        if record_count < 0:
            raise ValueError("record_count cannot be negative")
        if self._outputs is not None or self._stable_records is not None:
            raise RuntimeError(
                "stable fingerprint emission cannot collect materialized outputs"
            )
        chunk = StableStreamFingerprint.from_string(fingerprint)
        if chunk.byte_length != byte_length:
            raise RuntimeError(
                "stable fingerprint byte length mismatch: "
                f"{chunk.byte_length} != {byte_length}"
            )
        self._fingerprint = self._fingerprint.combine(chunk)
        self.emitted_count += record_count

    def extend_stable_artifact(
        self,
        uri: str,
        *,
        expected_sha256: str,
        record_count: int,
    ) -> None:
        if record_count < 0:
            raise ValueError("record_count cannot be negative")
        if self._outputs is not None or self._stable_records is not None:
            raise RuntimeError(
                "stable artifact emission cannot collect materialized outputs"
            )
        artifact_digest = sha256()
        with _open_file_uri(uri) as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                artifact_digest.update(chunk)
                if self._track_digest:
                    self._digest.update(chunk)
                self._fingerprint = self._fingerprint.update_bytes(chunk)
        actual = artifact_digest.hexdigest()
        if actual != expected_sha256:
            raise RuntimeError(
                "stable artifact sha256 mismatch: "
                f"{actual} != {expected_sha256}"
            )
        self.emitted_count += record_count


__all__ = [
    "DistributedProtocolConfig",
    "DistributedProtocolTracker",
    "DistributedRunResult",
]
