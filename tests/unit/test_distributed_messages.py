from __future__ import annotations

import pytest
from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.distributed import (
    ControlMessageCodec,
    DistributedProtocolConfig,
    DistributedProtocolTracker,
    RuntimeFeedback,
    chunk_message,
    migrate_install_message,
    migrate_state_message,
    ready_message,
    work_message,
)
from cqdagpcfg_parallel.protocol import NodeId, WorkItem, WorkerId
from cqdagpcfg_parallel.runtime import ZmqEndpoint


def _record(index: int) -> GuessRecord:
    return GuessRecord(
        prob=1.0 / (index + 1),
        guess=f"g{index}",
        structure_index=0,
        structure_name="A",
        ranks=(index,),
    )


def test_control_message_codec_round_trips_work_item() -> None:
    item = WorkItem(
        node_id=NodeId("root"),
        start=2,
        end=5,
        worker_id=WorkerId("worker-0"),
        epoch=7,
        reclaim_before=2,
    )

    decoded = ControlMessageCodec.loads(ControlMessageCodec.dumps(work_message(item)))

    assert decoded.type == "work"
    assert decoded.work_item == item
    assert decoded.work_item.reclaim_before == 2
    assert decoded.worker_id == WorkerId("worker-0")


def test_control_message_codec_round_trips_chunk_records() -> None:
    item = WorkItem(
        node_id=NodeId("root"),
        start=0,
        end=2,
        worker_id=WorkerId("worker-0"),
        epoch=1,
    )
    records = (_record(0), _record(1))

    feedback = RuntimeFeedback(
        chunk_latency_seconds=0.01,
        records_requested=2,
        records_produced=2,
    )

    decoded = ControlMessageCodec.loads(
        ControlMessageCodec.dumps(
            chunk_message(item, records, runtime_feedback=feedback),
        )
    )

    assert decoded.type == "chunk"
    assert decoded.work_item == item
    assert decoded.runtime_feedback == feedback
    assert tuple(record.stable_string() for record in decoded.records) == tuple(
        record.stable_string() for record in records
    )


def test_control_message_codec_round_trips_model_fingerprint() -> None:
    item = WorkItem(
        node_id=NodeId("root"),
        start=0,
        end=1,
        worker_id=WorkerId("worker-0"),
        epoch=1,
    )

    decoded = ControlMessageCodec.loads(
        ControlMessageCodec.dumps(
            chunk_message(
                item,
                (_record(0),),
                model_fingerprint="sha256:model",
            )
        )
    )

    assert decoded.model_fingerprint == "sha256:model"


def test_control_message_codec_round_trips_migration_state() -> None:
    decoded = ControlMessageCodec.loads(
        ControlMessageCodec.dumps(
            migrate_state_message(
                migration_id="migration-1",
                node_id=NodeId("node-a"),
                source_worker_id=WorkerId("worker-a"),
                target_worker_id=WorkerId("worker-b"),
                source_epoch=3,
                snapshot_payload='{"format":"snapshot"}',
                snapshot_digest="abc",
                snapshot_bytes=21,
                model_fingerprint="sha256:model",
            )
        )
    )

    assert decoded.type == "migrate_state"
    assert decoded.migration_id == "migration-1"
    assert decoded.node_id == NodeId("node-a")
    assert decoded.source_worker_id == WorkerId("worker-a")
    assert decoded.target_worker_id == WorkerId("worker-b")
    assert decoded.source_epoch == 3
    assert decoded.snapshot_payload == '{"format":"snapshot"}'
    assert decoded.snapshot_digest == "abc"
    assert decoded.snapshot_bytes == 21
    assert decoded.model_fingerprint == "sha256:model"


def test_control_message_codec_round_trips_migration_install() -> None:
    decoded = ControlMessageCodec.loads(
        ControlMessageCodec.dumps(
            migrate_install_message(
                migration_id="migration-2",
                node_id=NodeId("node-a"),
                source_worker_id=WorkerId("worker-a"),
                target_worker_id=WorkerId("worker-b"),
                source_epoch=4,
                snapshot_payload="{}",
                snapshot_digest="def",
                snapshot_bytes=2,
            )
        )
    )

    assert decoded.type == "migrate_install"
    assert decoded.migration_id == "migration-2"
    assert decoded.snapshot_digest == "def"


def test_tracker_rejects_mismatched_worker_model_fingerprint() -> None:
    worker_id = WorkerId("worker-0")
    tracker = DistributedProtocolTracker(
        endpoint=ZmqEndpoint("inproc://unused", bind=True),
        config=DistributedProtocolConfig(model_fingerprint="sha256:expected"),
    )

    with pytest.raises(RuntimeError, match="model_fingerprint"):
        tracker._validate_worker_identity(
            ready_message(worker_id, model_fingerprint="sha256:other"),
            worker_id,
        )
