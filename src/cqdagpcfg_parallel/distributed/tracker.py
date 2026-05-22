from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from hashlib import sha256
from threading import Event
from time import monotonic
from typing import Any, Callable

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
    StaleLeaseError,
    WorkerId,
    WorkItem,
    stable_record_string,
)
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint, _require_zmq
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
from .migration import (
    MigrationCoordinator,
    MigrationStats,
    MigrationTicket,
    SnapshotPolicy,
    content_digest,
)


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
    migration_stats: MigrationStats = MigrationStats()
    expired_leases: int = 0
    automatic_migrations: int = 0
    failed_migration_triggers: int = 0

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
        self._control_outbox: dict[WorkerId, deque[ControlMessage]] = {}
        self._last_seen_by_worker: dict[WorkerId, float] = {}
        self._stopped_workers: set[WorkerId] = set()
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
                    order_key=self.config.record_order_key or default_record_order_key,
                )
            )
        self.shards = tuple(shards)
        for dependency in self.config.node_dependencies:
            self.states.register_dependency(
                dependency.parent_id,
                dependency.child_id,
                donation_weight=dependency.donation_weight,
            )
        self.merger = GlobalMerger(self.shards)

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
        socket.setsockopt(zmq.SNDHWM, self.endpoint.high_watermark)
        socket.setsockopt(zmq.RCVHWM, self.endpoint.high_watermark)
        socket.setsockopt(zmq.LINGER, self.endpoint.linger_ms)
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
            self._advance_outputs(
                collector,
                limit,
                output_callback=output_callback,
                checkpoint_callback=checkpoint_callback,
                checkpoint_interval_records=checkpoint_interval_records,
            )
            while True:
                if expected_workers is not None and len(stopped_workers) >= expected_workers:
                    break
                if expected_workers is None and self._is_done(collector.emitted_count, limit):
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
                        checkpoint_callback=checkpoint_callback,
                        checkpoint_interval_records=checkpoint_interval_records,
                    )
                    if message.type == "chunk":
                        received_chunks += 1
                        received_records += len(message.records)
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
            ),
            seen_workers=tuple(sorted(seen_workers, key=str)),
            stopped_workers=tuple(sorted(stopped_workers, key=str)),
            received_chunks=received_chunks,
            received_records=received_records,
            assigned_items_by_worker=_sorted_items(self._assigned_items_by_worker),
            assigned_records_by_worker=_sorted_items(self._assigned_records_by_worker),
            assigned_items_by_node=_sorted_items(self._assigned_items_by_node),
            assigned_records_by_node=_sorted_items(self._assigned_records_by_node),
            migration_stats=self.migrations.stats,
            expired_leases=self._expired_leases,
            automatic_migrations=self._automatic_migrations,
            failed_migration_triggers=self._failed_migration_triggers,
        )

    def _handle_message(
        self,
        message: ControlMessage,
        *,
        worker_id: WorkerId,
        collector: "_OutputCollector",
        limit: int,
        output_callback: Callable[[GuessRecord], None] | None,
        checkpoint_callback: Callable[[DistributedTrackerCheckpoint], None] | None = None,
        checkpoint_interval_records: int = 1,
    ) -> ControlMessage:
        self._validate_worker_identity(message, worker_id)
        if self._is_done(collector.emitted_count, limit):
            return stop_message()

        if message.type == "chunk":
            migration_reply = self._accept_chunk(message, worker_id=worker_id)
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

        item = self.scheduler.schedule(worker_id)
        if item is None:
            return wait_message()
        self._record_assignment(item)
        return work_message(item)

    def _accept_chunk(
        self,
        message: ControlMessage,
        *,
        worker_id: WorkerId,
    ) -> ControlMessage | None:
        if message.work_item is None:
            raise RuntimeError("chunk message is missing work_item")
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
            return wait_message()

        if message.records:
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
        self.states.record_runtime_feedback(
            message.work_item.node_id,
            chunk_latency_seconds=feedback.chunk_latency_seconds,
            records_requested=feedback.records_requested,
            records_produced=feedback.records_produced,
            ewma_alpha=self.config.scheduler.feedback_ewma_alpha,
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


class _OutputCollector:
    def __init__(
        self,
        *,
        collect_outputs: bool,
        initial_stable_records: tuple[str, ...] = (),
        track_stable_records: bool = False,
    ) -> None:
        self._outputs: list[GuessRecord] | None = [] if collect_outputs else None
        self._digest = sha256()
        for stable_record in initial_stable_records:
            self._digest.update(stable_record.encode("utf-8"))
            self._digest.update(b"\n")
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
        self._digest.update(stable_record.encode("utf-8"))
        self._digest.update(b"\n")
        self.emitted_count += 1


__all__ = [
    "DistributedProtocolConfig",
    "DistributedProtocolTracker",
    "DistributedRunResult",
]
