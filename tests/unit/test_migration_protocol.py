from __future__ import annotations

import pytest
from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.distributed import (
    DistributedProtocolConfig,
    DistributedProtocolTracker,
    MigrationCoordinator,
    MigrationStatus,
    SnapshotPolicy,
    chunk_message,
    content_digest,
    migrate_ack_message,
    migrate_state_message,
    ready_message,
)
from cqdagpcfg_parallel.protocol import LeaseTable, NodeId, StaleLeaseError, WorkerId, WorkItem
from cqdagpcfg_parallel.runtime import ZmqEndpoint
from cqdagpcfg_parallel.distributed.tracker import _OutputCollector


def _record(index: int) -> GuessRecord:
    return GuessRecord(
        prob=1.0 / (index + 1),
        guess=f"g{index}",
        structure_index=0,
        structure_name="A",
        ranks=(index,),
    )


def test_migration_commit_transfers_lease_and_fences_source_epoch() -> None:
    leases = LeaseTable(default_ttl_seconds=10.0)
    source = leases.acquire(NodeId("node-a"), WorkerId("source"))
    coordinator = MigrationCoordinator(leases)

    ticket = coordinator.prepare(
        node_id=source.node_id,
        source_worker_id=source.worker_id,
        target_worker_id=WorkerId("target"),
        source_epoch=source.epoch,
    )
    ticket = coordinator.attach_snapshot(
        ticket.migration_id,
        snapshot_payload='{"state":1}',
    )
    commit = coordinator.commit(ticket.migration_id, ttl_seconds=10.0)

    assert commit.ticket.status == MigrationStatus.COMMITTED
    assert commit.target_lease.worker_id == WorkerId("target")
    assert commit.target_lease.epoch == source.epoch + 1
    assert ticket.snapshot_digest == content_digest('{"state":1}')

    with pytest.raises(StaleLeaseError):
        leases.require_valid(
            node_id=source.node_id,
            worker_id=source.worker_id,
            epoch=source.epoch,
        )

    leases.require_valid(
        node_id=source.node_id,
        worker_id=WorkerId("target"),
        epoch=commit.target_lease.epoch,
    )


def test_migration_abort_keeps_source_lease_valid() -> None:
    leases = LeaseTable(default_ttl_seconds=10.0)
    source = leases.acquire(NodeId("node-a"), WorkerId("source"))
    coordinator = MigrationCoordinator(leases)

    ticket = coordinator.prepare(
        node_id=source.node_id,
        source_worker_id=source.worker_id,
        target_worker_id=WorkerId("target"),
        source_epoch=source.epoch,
    )
    aborted = coordinator.abort(ticket.migration_id)

    assert aborted.status == MigrationStatus.ABORTED
    leases.require_valid(
        node_id=source.node_id,
        worker_id=source.worker_id,
        epoch=source.epoch,
    )


def test_snapshot_policy_rejects_large_snapshot() -> None:
    policy = SnapshotPolicy(max_snapshot_bytes=100, max_snapshot_to_warmup_ratio=0.5)

    assert policy.should_migrate(
        snapshot_bytes=40,
        estimated_warmup_bytes=100,
        node_priority=0.0,
    )
    assert not policy.should_migrate(
        snapshot_bytes=80,
        estimated_warmup_bytes=100,
        node_priority=0.0,
    )
    assert not policy.should_migrate(
        snapshot_bytes=101,
        estimated_warmup_bytes=1000,
        node_priority=10.0,
    )


def test_expired_leases_can_be_released_for_retry() -> None:
    leases = LeaseTable(default_ttl_seconds=1.0)
    lease = leases.acquire(NodeId("node-a"), WorkerId("source"), now=1.0)

    expired = leases.release_expired(now=3.0)
    replacement = leases.acquire(NodeId("node-a"), WorkerId("target"), now=3.0)

    assert expired == (lease,)
    assert replacement.worker_id == WorkerId("target")
    assert replacement.epoch == lease.epoch + 1


def test_tracker_routes_migration_as_base_control_flow() -> None:
    source_worker = WorkerId("source")
    target_worker = WorkerId("target")
    node_id = NodeId("node-a")
    tracker = DistributedProtocolTracker(
        endpoint=ZmqEndpoint("inproc://unused", bind=True),
        config=DistributedProtocolConfig(model_fingerprint="sha256:model"),
    )
    source_lease = tracker.leases.acquire(
        node_id,
        source_worker,
        start=0,
        end=2,
    )
    ticket = tracker.request_node_migration(
        node_id=node_id,
        source_worker_id=source_worker,
        target_worker_id=target_worker,
        source_epoch=source_lease.epoch,
    )
    collector = _OutputCollector(collect_outputs=False)

    prepare = tracker._handle_message(
        ready_message(source_worker, model_fingerprint="sha256:model"),
        worker_id=source_worker,
        collector=collector,
        limit=1,
        output_callback=None,
    )
    assert prepare.type == "migrate_prepare"
    assert prepare.migration_id == ticket.migration_id

    payload = '{"snapshot":true}'
    tracker_reply = tracker._handle_message(
        migrate_state_message(
            migration_id=ticket.migration_id,
            node_id=node_id,
            source_worker_id=source_worker,
            target_worker_id=target_worker,
            source_epoch=source_lease.epoch,
            snapshot_payload=payload,
            snapshot_digest=content_digest(payload),
            snapshot_bytes=len(payload),
            model_fingerprint="sha256:model",
        ),
        worker_id=source_worker,
        collector=collector,
        limit=1,
        output_callback=None,
    )
    assert tracker_reply.type == "wait"

    install = tracker._handle_message(
        ready_message(target_worker, model_fingerprint="sha256:model"),
        worker_id=target_worker,
        collector=collector,
        limit=1,
        output_callback=None,
    )
    assert install.type == "migrate_install"
    assert install.snapshot_payload == payload

    target_commit = tracker._handle_message(
        migrate_ack_message(
            migration_id=ticket.migration_id,
            node_id=node_id,
            source_worker_id=source_worker,
            target_worker_id=target_worker,
            source_epoch=source_lease.epoch,
            model_fingerprint="sha256:model",
        ),
        worker_id=target_worker,
        collector=collector,
        limit=1,
        output_callback=None,
    )
    assert target_commit.type == "migrate_commit"
    assert target_commit.target_epoch == source_lease.epoch + 1

    source_commit = tracker._handle_message(
        ready_message(source_worker, model_fingerprint="sha256:model"),
        worker_id=source_worker,
        collector=collector,
        limit=1,
        output_callback=None,
    )
    assert source_commit.type == "migrate_commit"
    assert source_commit.target_epoch == target_commit.target_epoch

    with pytest.raises(StaleLeaseError):
        tracker.leases.require_valid(
            node_id=node_id,
            worker_id=source_worker,
            epoch=source_lease.epoch,
        )
    tracker.leases.require_valid(
        node_id=node_id,
        worker_id=target_worker,
        epoch=target_commit.target_epoch,
    )


def test_tracker_automatically_prepares_migration_when_worker_retires_after_chunk() -> None:
    source_worker = WorkerId("source")
    target_worker = WorkerId("target")
    node_id = NodeId("node-a")
    tracker = DistributedProtocolTracker(
        endpoint=ZmqEndpoint("inproc://unused", bind=True),
        config=DistributedProtocolConfig(model_fingerprint="sha256:model"),
    )
    tracker._last_seen_by_worker[target_worker] = 2.0
    source_lease = tracker.leases.acquire(
        node_id,
        source_worker,
        start=0,
        end=2,
    )
    item = WorkItem(
        node_id=node_id,
        start=0,
        end=2,
        worker_id=source_worker,
        epoch=source_lease.epoch,
    )
    collector = _OutputCollector(collect_outputs=False)

    reply = tracker._handle_message(
        chunk_message(
            item,
            (_record(0), _record(1)),
            retire=True,
            model_fingerprint="sha256:model",
        ),
        worker_id=source_worker,
        collector=collector,
        limit=10,
        output_callback=None,
    )

    assert reply.type == "migrate_prepare"
    assert reply.source_worker_id == source_worker
    assert reply.target_worker_id == target_worker
    assert tracker._automatic_migrations == 1
    tracker.leases.require_valid(
        node_id=node_id,
        worker_id=source_worker,
        epoch=source_lease.epoch,
    )


def test_tracker_recovers_expired_lease_and_scheduler_reassigns_node() -> None:
    source_worker = WorkerId("source")
    target_worker = WorkerId("target")
    node_id = NodeId("node-a")
    tracker = DistributedProtocolTracker(
        endpoint=ZmqEndpoint("inproc://unused", bind=True),
        config=DistributedProtocolConfig(model_fingerprint="sha256:model"),
    )
    tracker.states.register_demand(node_id, target_end=4)
    source_lease = tracker.leases.acquire(node_id, source_worker)
    object.__setattr__(source_lease, "expires_at", 0.0)

    tracker._recover_expired_leases()
    replacement = tracker.scheduler.schedule(target_worker)

    assert tracker._expired_leases == 1
    assert replacement is not None
    assert replacement.worker_id == target_worker
    assert replacement.epoch == source_lease.epoch + 1
