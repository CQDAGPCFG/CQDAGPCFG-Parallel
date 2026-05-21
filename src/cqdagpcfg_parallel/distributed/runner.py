from __future__ import annotations

from threading import Event, Thread
from typing import Callable, Sequence
from uuid import uuid4

from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.protocol import (
    ChunkSizePolicy,
    NodeId,
    SchedulerConfig,
    WorkerId,
)
from cqdagpcfg_parallel.runtime.worker import LocalResultSource
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint, _require_zmq
from cqdagpcfg_parallel.simulation import MappingRecordSource, SequenceRecordSource

from .tracker import DistributedProtocolConfig, DistributedProtocolTracker, DistributedRunResult
from .worker import DistributedProtocolWorker, DistributedWorkerStats


SourceFactory = Callable[[WorkerId], LocalResultSource]


def run_distributed_protocol(
    *,
    source_factory: SourceFactory,
    limit: int,
    worker_count: int,
    endpoint: ZmqEndpoint | None = None,
    config: DistributedProtocolConfig | None = None,
    timeout_seconds: float = 10.0,
    worker_delay_seconds: float = 0.0,
    collect_outputs: bool = True,
) -> tuple[DistributedRunResult, tuple[DistributedWorkerStats, ...]]:
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")

    zmq = _require_zmq()
    context = zmq.Context()
    address = f"inproc://cqpcfg-distributed-{uuid4()}"
    tracker_endpoint = endpoint or ZmqEndpoint(address=address, bind=True, linger_ms=0)
    worker_endpoint = ZmqEndpoint(
        address=tracker_endpoint.address,
        bind=False,
        high_watermark=tracker_endpoint.high_watermark,
        linger_ms=tracker_endpoint.linger_ms,
    )

    started = Event()
    result_holder: list[DistributedRunResult] = []
    worker_results: list[DistributedWorkerStats] = []
    errors: list[BaseException] = []

    tracker = DistributedProtocolTracker(
        endpoint=tracker_endpoint,
        config=config,
        context=context,
    )
    worker_model_fingerprint = config.model_fingerprint if config is not None else None

    def tracker_task() -> None:
        try:
            result_holder.append(
                tracker.run(
                    limit=limit,
                    expected_workers=worker_count,
                    timeout_seconds=timeout_seconds,
                    started_event=started,
                    collect_outputs=collect_outputs,
                )
            )
        except BaseException as exc:  # pragma: no cover - surfaced after join
            errors.append(exc)

    tracker_thread = Thread(target=tracker_task, name="cqpcfg-tracker", daemon=True)
    tracker_thread.start()
    if not started.wait(timeout_seconds):
        raise TimeoutError("distributed tracker did not start")

    worker_threads: list[Thread] = []
    for index in range(worker_count):
        worker_id = WorkerId(f"worker-{index}")
        worker = DistributedProtocolWorker(
            worker_id=worker_id,
            endpoint=worker_endpoint,
            source=source_factory(worker_id),
            context=context,
            work_delay_seconds=worker_delay_seconds,
            model_fingerprint=worker_model_fingerprint,
        )

        def worker_task(worker_instance: DistributedProtocolWorker = worker) -> None:
            try:
                worker_results.append(worker_instance.run())
            except BaseException as exc:  # pragma: no cover - surfaced after join
                errors.append(exc)

        thread = Thread(target=worker_task, name=f"cqpcfg-worker-{index}", daemon=True)
        worker_threads.append(thread)
        thread.start()

    for thread in worker_threads:
        thread.join(timeout_seconds)
    tracker_thread.join(timeout_seconds)
    context.term()

    if errors:
        raise RuntimeError("distributed protocol run failed") from errors[0]
    if not result_holder:
        raise RuntimeError("distributed tracker did not produce a result")
    return result_holder[0], tuple(sorted(worker_results, key=lambda stats: str(stats.worker_id)))


def run_distributed_sequence_protocol(
    records: Sequence[GuessRecord],
    *,
    limit: int,
    worker_count: int = 2,
    policy: ChunkSizePolicy = ChunkSizePolicy.CQDAG_ADAPTIVE,
    demand_window: int = 16,
    fixed_chunk_size: int = 8,
    entropy: float = 0.0,
    node_id: NodeId = NodeId("root"),
    root_shard_count: int | None = None,
    timeout_seconds: float = 10.0,
    worker_delay_seconds: float = 0.0,
    collect_outputs: bool = True,
) -> tuple[DistributedRunResult, tuple[DistributedWorkerStats, ...]]:
    shard_count = 1 if root_shard_count is None else root_shard_count
    if shard_count <= 0:
        raise ValueError("root_shard_count must be positive")
    node_ids = (
        (node_id,)
        if shard_count == 1
        else tuple(NodeId(f"{node_id}:{index}") for index in range(shard_count))
    )
    records_by_node = _round_robin_records(records, node_ids)
    config = DistributedProtocolConfig(
        scheduler=SchedulerConfig(policy=policy, fixed_chunk_size=fixed_chunk_size),
        node_id=node_ids[0],
        node_ids=node_ids,
        demand_window=demand_window,
        entropy=entropy,
    )
    return run_distributed_protocol(
        source_factory=(
            (lambda _worker_id: SequenceRecordSource(records, node_id=node_id))
            if shard_count == 1
            else (lambda _worker_id: MappingRecordSource(records_by_node))
        ),
        limit=limit,
        worker_count=worker_count,
        config=config,
        timeout_seconds=timeout_seconds,
        worker_delay_seconds=worker_delay_seconds,
        collect_outputs=collect_outputs,
    )


def _round_robin_records(
    records: Sequence[GuessRecord],
    node_ids: tuple[NodeId, ...],
) -> dict[NodeId, tuple[GuessRecord, ...]]:
    shards: dict[NodeId, list[GuessRecord]] = {node_id: [] for node_id in node_ids}
    for index, record in enumerate(records):
        shards[node_ids[index % len(node_ids)]].append(record)
    return {node_id: tuple(shard_records) for node_id, shard_records in shards.items()}


__all__ = [
    "SourceFactory",
    "run_distributed_protocol",
    "run_distributed_sequence_protocol",
]
