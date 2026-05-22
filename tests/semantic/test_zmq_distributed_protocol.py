from __future__ import annotations

from threading import Event, Thread
from uuid import uuid4

import pytest
from CQDAGPCFG import GuessRecord
from CQDAGPCFG.training import PCFGTrainer

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGBlockGraphAdapter,
    CQDAGStructureRecordSource,
    SerialCQDAGOracle,
)
from cqdagpcfg_parallel.distributed import (
    ControlMessage,
    ControlMessageCodec,
    DistributedProtocolConfig,
    DistributedProtocolTracker,
    DistributedProtocolWorker,
    content_digest,
    migrate_ack_message,
    migrate_state_message,
    ready_message,
    retire_message,
    run_distributed_protocol,
    run_distributed_sequence_protocol,
)
from cqdagpcfg_parallel.protocol import (
    ChunkSizePolicy,
    NodeId,
    SchedulerConfig,
    WorkerId,
    stable_digest,
    stable_record_string,
)
from cqdagpcfg_parallel.runtime import ZmqEndpoint
from cqdagpcfg_parallel.simulation import MappingRecordSource
from cqdagpcfg_parallel.storage import DistributedTrackerCheckpoint


pytest.importorskip("zmq")


def _record(index: int) -> GuessRecord:
    return GuessRecord(
        prob=1.0 / (index + 1),
        guess=f"g{index}",
        structure_index=0,
        structure_name="A",
        ranks=(index,),
    )


def _toy_model():
    return PCFGTrainer().train(
        [
            "ab12!",
            "ab12!",
            "cd12!",
            "ab34@",
            "p@ssw0rd",
            "password12",
            "dragonball99",
            "moon24@",
            "star77#",
            "1990abc!",
            "asdf12!",
            "hello2024!",
            "hello2024!",
            "elite99",
        ]
    )


@pytest.mark.parametrize("policy", list(ChunkSizePolicy))
def test_zmq_distributed_sequence_protocol_preserves_prefix_with_workers(
    policy: ChunkSizePolicy,
) -> None:
    records = tuple(_record(index) for index in range(60))
    limit = 45

    result, workers = run_distributed_sequence_protocol(
        records,
        limit=limit,
        worker_count=3,
        policy=policy,
        demand_window=6,
        fixed_chunk_size=6,
        entropy=4.0,
        timeout_seconds=5.0,
        worker_delay_seconds=0.002,
        root_shard_count=3,
    )

    assert result.digest == stable_digest(records[:limit])
    assert result.outputs == records[:limit]
    assert len(result.seen_workers) == 3
    assert len(result.stopped_workers) == 3
    assert result.received_records >= limit
    assert sum(dict(result.assigned_records_by_worker).values()) >= limit
    assert len(result.assigned_records_by_node) == 3
    assert sum(dict(result.assigned_records_by_node).values()) >= limit
    assert sum(worker.completed_records for worker in workers) >= limit
    assert sum(1 for worker in workers if worker.completed_records > 0) >= 2


def test_zmq_distributed_protocol_preserves_cqdagpcfg_prefix() -> None:
    model = _toy_model()
    limit = 80
    baseline = SerialCQDAGOracle(model).run(limit)
    records = baseline.outputs

    result, workers = run_distributed_sequence_protocol(
        records,
        limit=limit,
        worker_count=3,
        policy=ChunkSizePolicy.ENTROPY_ADAPTIVE,
        demand_window=8,
        fixed_chunk_size=8,
        entropy=4.0,
        timeout_seconds=5.0,
        worker_delay_seconds=0.001,
    )

    assert result.digest == baseline.digest
    assert result.stable_records == tuple(stable_record_string(record) for record in records)
    assert len(result.seen_workers) == 3
    assert sum(worker.completed_records for worker in workers) >= limit


def test_zmq_distributed_structure_protocol_preserves_cqdagpcfg_prefix() -> None:
    model = _toy_model()
    adapter = CQDAGBlockGraphAdapter(model)
    structure_nodes = adapter.structure_nodes()
    limit = 80
    demand_window = 8
    baseline = SerialCQDAGOracle(model).run(limit)

    result, workers = run_distributed_protocol(
        source_factory=lambda _worker_id: CQDAGStructureRecordSource(
            model,
            max_records_per_structure=limit + demand_window,
            adapter=adapter,
        ),
        limit=limit,
        worker_count=3,
        config=DistributedProtocolConfig(
            scheduler=SchedulerConfig(
                policy=ChunkSizePolicy.ENTROPY_ADAPTIVE,
                fixed_chunk_size=8,
                max_chunk_size=32,
            ),
            node_ids=tuple(node.node_id for node in structure_nodes),
            node_features=adapter.scheduling_features(),
            demand_window=demand_window,
            record_order_key=adapter.serial_order_key,
        ),
        timeout_seconds=5.0,
        worker_delay_seconds=0.001,
    )

    assert result.digest == baseline.digest
    assert result.stable_records == tuple(
        stable_record_string(record) for record in baseline.outputs
    )
    assert len(result.assigned_records_by_node) > 1
    assert sum(worker.completed_records for worker in workers) >= limit


def test_zmq_tracker_accepts_worker_added_while_running() -> None:
    records = tuple(_record(index) for index in range(120))
    limit = 80
    node_ids = tuple(NodeId(f"root:{index}") for index in range(4))
    records_by_node = _round_robin_records(records, node_ids)

    result, workers = _run_tracker_with_workers(
        records_by_node=records_by_node,
        node_ids=node_ids,
        limit=limit,
        worker_ids=(WorkerId("worker-0"), WorkerId("worker-late")),
        worker_start_delays=(0.0, 0.05),
        worker_delay_seconds=0.003,
        expected_workers=None,
        timeout_seconds=6.0,
    )

    worker_counts = {worker.worker_id: worker.completed_records for worker in workers}
    assert result.digest == stable_digest(records[:limit])
    assert WorkerId("worker-late") in result.seen_workers
    assert worker_counts[WorkerId("worker-late")] > 0


def test_zmq_tracker_recovers_from_durable_checkpoint_after_restart() -> None:
    records = tuple(_record(index) for index in range(120))
    node_ids = tuple(NodeId(f"root:{index}") for index in range(4))
    records_by_node = _round_robin_records(records, node_ids)
    checkpoints: list[DistributedTrackerCheckpoint] = []

    partial, _ = _run_tracker_with_workers(
        records_by_node=records_by_node,
        node_ids=node_ids,
        limit=30,
        worker_ids=(WorkerId("worker-a"), WorkerId("worker-b")),
        worker_start_delays=(0.0, 0.0),
        worker_delay_seconds=0.001,
        expected_workers=2,
        timeout_seconds=5.0,
        checkpoint_callback=checkpoints.append,
        checkpoint_interval_records=1,
    )
    assert partial.digest == stable_digest(records[:30])
    assert checkpoints[-1].emitted_count == 30

    recovered, workers = _run_tracker_with_workers(
        records_by_node=records_by_node,
        node_ids=node_ids,
        limit=80,
        worker_ids=(WorkerId("worker-c"), WorkerId("worker-d")),
        worker_start_delays=(0.0, 0.0),
        worker_delay_seconds=0.001,
        expected_workers=2,
        timeout_seconds=5.0,
        resume_checkpoint=checkpoints[-1],
    )

    assert recovered.digest == stable_digest(records[:80])
    assert recovered.emitted_count == 80
    assert sum(worker.completed_records for worker in workers) >= 50


def test_zmq_tracker_routes_state_migration_over_wire() -> None:
    zmq = pytest.importorskip("zmq")
    context = zmq.Context()
    address = "inproc://migration-wire"
    source_worker = WorkerId("source")
    target_worker = WorkerId("target")
    node_id = NodeId("node-a")
    tracker = DistributedProtocolTracker(
        endpoint=ZmqEndpoint(address, bind=True),
        config=DistributedProtocolConfig(model_fingerprint="sha256:model"),
        context=context,
    )
    source_lease = tracker.leases.acquire(node_id, source_worker)
    ticket = tracker.request_node_migration(
        node_id=node_id,
        source_worker_id=source_worker,
        target_worker_id=target_worker,
        source_epoch=source_lease.epoch,
    )
    started = Event()
    result_holder = []
    error_holder = []

    def run_tracker() -> None:
        try:
            result_holder.append(
                tracker.run(
                    limit=1,
                    expected_workers=2,
                    timeout_seconds=3.0,
                    started_event=started,
                    collect_outputs=False,
                )
            )
        except BaseException as exc:
            error_holder.append(exc)

    tracker_thread = Thread(target=run_tracker, daemon=True)
    tracker_thread.start()
    assert started.wait(2.0)

    source = _dealer(context, address, source_worker)
    target = _dealer(context, address, target_worker)
    try:
        prepare = _exchange(
            source,
            ready_message(source_worker, model_fingerprint="sha256:model"),
        )
        assert prepare.type == "migrate_prepare"
        assert prepare.migration_id == ticket.migration_id

        payload = '{"snapshot":true}'
        state_reply = _exchange(
            source,
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
        )
        assert state_reply.type == "wait"

        install = _exchange(
            target,
            ready_message(target_worker, model_fingerprint="sha256:model"),
        )
        assert install.type == "migrate_install"
        assert install.snapshot_payload == payload

        target_commit = _exchange(
            target,
            migrate_ack_message(
                migration_id=ticket.migration_id,
                node_id=node_id,
                source_worker_id=source_worker,
                target_worker_id=target_worker,
                source_epoch=source_lease.epoch,
                model_fingerprint="sha256:model",
            ),
        )
        assert target_commit.type == "migrate_commit"
        assert target_commit.target_epoch == source_lease.epoch + 1

        source_commit = _exchange(
            source,
            ready_message(source_worker, model_fingerprint="sha256:model"),
        )
        assert source_commit.type == "migrate_commit"
        assert source_commit.target_epoch == target_commit.target_epoch

        assert _exchange(
            source,
            retire_message(source_worker, model_fingerprint="sha256:model"),
        ).type == "stop"
        assert _exchange(
            target,
            retire_message(target_worker, model_fingerprint="sha256:model"),
        ).type == "stop"
    finally:
        source.close()
        target.close()
        tracker_thread.join(3.0)
        context.term()

    assert not tracker_thread.is_alive()
    assert not error_holder
    assert result_holder[0].seen_workers == (source_worker, target_worker)


def _dealer(context, address: str, worker_id: WorkerId):
    zmq = pytest.importorskip("zmq")
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, str(worker_id).encode("utf-8"))
    socket.setsockopt(zmq.RCVTIMEO, 2000)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(address)
    return socket


def _exchange(socket, message: ControlMessage) -> ControlMessage:
    socket.send(ControlMessageCodec.dumps(message))
    return ControlMessageCodec.loads(socket.recv())


def _run_tracker_with_workers(
    *,
    records_by_node: dict[NodeId, tuple[GuessRecord, ...]],
    node_ids: tuple[NodeId, ...],
    limit: int,
    worker_ids: tuple[WorkerId, ...],
    worker_start_delays: tuple[float, ...],
    worker_delay_seconds: float,
    expected_workers: int | None,
    timeout_seconds: float,
    resume_checkpoint: DistributedTrackerCheckpoint | None = None,
    checkpoint_callback=None,
    checkpoint_interval_records: int = 1,
):
    assert len(worker_ids) == len(worker_start_delays)
    zmq = pytest.importorskip("zmq")
    context = zmq.Context()
    address = f"inproc://dynamic-workers-{uuid4()}"
    tracker = DistributedProtocolTracker(
        endpoint=ZmqEndpoint(address, bind=True, linger_ms=0),
        config=DistributedProtocolConfig(
            scheduler=SchedulerConfig(max_chunk_size=4),
            node_ids=node_ids,
            demand_window=8,
        ),
        context=context,
    )
    started = Event()
    result_holder = []
    worker_results = []
    errors = []

    def tracker_task() -> None:
        try:
            result_holder.append(
                tracker.run(
                    limit=limit,
                    expected_workers=expected_workers,
                    timeout_seconds=timeout_seconds,
                    started_event=started,
                    shutdown_grace_seconds=0.5,
                    collect_outputs=True,
                    resume_checkpoint=resume_checkpoint,
                    checkpoint_callback=checkpoint_callback,
                    checkpoint_interval_records=checkpoint_interval_records,
                )
            )
        except BaseException as exc:
            errors.append(exc)

    tracker_thread = Thread(target=tracker_task, daemon=True)
    tracker_thread.start()
    assert started.wait(2.0)

    def worker_task(worker_id: WorkerId, start_delay: float) -> None:
        try:
            if start_delay:
                import time

                time.sleep(start_delay)
            worker = DistributedProtocolWorker(
                worker_id=worker_id,
                endpoint=ZmqEndpoint(address, bind=False, linger_ms=0),
                source=MappingRecordSource(records_by_node),
                context=context,
                work_delay_seconds=worker_delay_seconds,
            )
            worker_results.append(worker.run())
        except BaseException as exc:
            errors.append(exc)

    worker_threads = [
        Thread(
            target=worker_task,
            args=(worker_id, start_delay),
            daemon=True,
        )
        for worker_id, start_delay in zip(worker_ids, worker_start_delays)
    ]
    for thread in worker_threads:
        thread.start()
    for thread in worker_threads:
        thread.join(timeout_seconds)
    tracker_thread.join(timeout_seconds)
    context.term()

    assert not tracker_thread.is_alive()
    assert all(not thread.is_alive() for thread in worker_threads)
    if errors:
        raise RuntimeError("distributed test run failed") from errors[0]
    assert result_holder
    return result_holder[0], tuple(sorted(worker_results, key=lambda item: str(item.worker_id)))


def _round_robin_records(
    records: tuple[GuessRecord, ...],
    node_ids: tuple[NodeId, ...],
) -> dict[NodeId, tuple[GuessRecord, ...]]:
    shards: dict[NodeId, list[GuessRecord]] = {node_id: [] for node_id in node_ids}
    for index, record in enumerate(records):
        shards[node_ids[index % len(node_ids)]].append(record)
    return {node_id: tuple(shard_records) for node_id, shard_records in shards.items()}
