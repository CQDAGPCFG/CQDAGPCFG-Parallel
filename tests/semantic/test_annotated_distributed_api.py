from __future__ import annotations

from threading import Event, Thread

import pytest
from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.distributed import (
    cqpcfg_generator,
    cqpcfg_distributed,
    cqpcfg_tracker,
    cqpcfg_worker,
)
from cqdagpcfg_parallel.protocol import stable_digest
from cqdagpcfg_parallel.runtime.zmq_transport import _require_zmq
from cqdagpcfg_parallel.simulation import SequenceRecordSource


pytest.importorskip("zmq")


def _record(index: int) -> GuessRecord:
    return GuessRecord(
        prob=1.0 / (index + 1),
        guess=f"g{index}",
        structure_index=0,
        structure_name="A",
        ranks=(index,),
    )


def test_cqpcfg_distributed_annotation_runs_protocol() -> None:
    records = tuple(_record(index) for index in range(32))
    limit = 24

    @cqpcfg_distributed(
        limit=limit,
        worker_count=2,
        demand_window=6,
        entropy=4.0,
        worker_delay_seconds=0.001,
    )
    @cqpcfg_generator
    def source(worker_id):
        return SequenceRecordSource(records)

    result, workers = source.run()

    assert result.digest == stable_digest(records[:limit])
    assert len(result.seen_workers) == 2
    assert sum(worker.completed_records for worker in workers) >= limit


def test_cqpcfg_tracker_and_worker_annotations_run_over_zmq_context() -> None:
    zmq = _require_zmq()
    context = zmq.Context()
    address = "inproc://annotated-tracker-worker"
    records = tuple(_record(index) for index in range(20))
    limit = 12

    @cqpcfg_tracker(
        bind=address,
        limit=limit,
        expected_workers=1,
        demand_window=4,
        timeout_seconds=5.0,
    )
    def tracker_config():
        return None

    @cqpcfg_worker(connect=address, worker_id="worker-0", work_delay_seconds=0.001)
    @cqpcfg_generator
    def worker_source(worker_id):
        return SequenceRecordSource(records)

    started = Event()
    result_holder = []
    errors = []

    def run_tracker() -> None:
        try:
            result_holder.append(tracker_config.run(context=context, started_event=started))
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    tracker_thread = Thread(target=run_tracker, daemon=True)
    tracker_thread.start()
    assert started.wait(5.0)
    worker_stats = worker_source.run(context=context)
    tracker_thread.join(5.0)
    context.term()

    if errors:
        raise errors[0]
    assert result_holder
    assert result_holder[0].digest == stable_digest(records[:limit])
    assert worker_stats.completed_records >= limit
