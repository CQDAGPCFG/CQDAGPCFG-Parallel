from __future__ import annotations

import pytest
from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.distributed import (
    cqpcfg_consumer,
    cqpcfg_generator,
    cqpcfg_tracker,
    cqpcfg_worker,
)
from cqdagpcfg_parallel.runtime import CandidateBatch, publish_record_batches
from cqdagpcfg_parallel.simulation import SequenceRecordSource


def test_tracker_annotation_exposes_bind_address() -> None:
    @cqpcfg_tracker(
        bind="cqpcfg://0.0.0.0:5555",
        limit=1,
        expected_workers=1,
    )
    def tracker_config():
        return None

    assert tracker_config.bind.address == "tcp://0.0.0.0:5555"
    assert tracker_config.bind.bind


def test_worker_annotation_exposes_connect_address() -> None:
    @cqpcfg_worker(connect="cqpcfg://127.0.0.1:5555", worker_id="worker-0")
    @cqpcfg_generator
    def worker_source():
        return SequenceRecordSource(())

    assert worker_source.connect.address == "tcp://127.0.0.1:5555"
    assert not worker_source.connect.bind


def test_annotations_keep_endpoint_as_legacy_alias() -> None:
    @cqpcfg_worker(endpoint="cqpcfg://127.0.0.1:5556", worker_id="worker-0")
    @cqpcfg_generator
    def worker_source():
        return SequenceRecordSource(())

    assert worker_source.connect.address == "tcp://127.0.0.1:5556"


def test_annotations_reject_ambiguous_bind_and_endpoint() -> None:
    with pytest.raises(ValueError, match="either bind"):
        cqpcfg_tracker(
            bind="cqpcfg://0.0.0.0:5555",
            endpoint="cqpcfg://0.0.0.0:5556",
            limit=1,
            expected_workers=1,
        )


def test_worker_annotation_requires_explicit_generator_role() -> None:
    with pytest.raises(TypeError, match="cqpcfg_generator"):

        @cqpcfg_worker(connect="cqpcfg://127.0.0.1:5555", worker_id="worker-0")
        def worker_source():
            return SequenceRecordSource(())


def test_generator_annotation_returns_local_result_source() -> None:
    @cqpcfg_generator
    def generator(worker_id):
        return SequenceRecordSource(())

    assert generator.role == "generator"
    assert isinstance(generator.source_for("worker-0"), SequenceRecordSource)


def test_consumer_annotation_implements_candidate_batch_sink() -> None:
    received: list[tuple[str, ...]] = []
    closed: list[bool] = []

    @cqpcfg_consumer(close=lambda: closed.append(True))
    def consumer(batch: CandidateBatch) -> None:
        received.append(batch.guesses)

    records = [
        GuessRecord(
            prob=1.0 / (index + 1),
            guess=f"g{index}",
            structure_index=0,
            structure_name="A",
            ranks=(index,),
        )
        for index in range(3)
    ]

    publish_record_batches(
        records,
        consumer,
        batch_size=2,
        max_batch_payload_bytes=32,
    )

    assert consumer.role == "consumer"
    assert received == [("g0", "g1"), ("g2",)]
    assert closed == [True]
