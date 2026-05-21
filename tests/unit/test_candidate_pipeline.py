from __future__ import annotations

import pytest
from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.runtime import (
    BatchRetryLedger,
    BatchState,
    CandidateBatch,
    DurableBatchCheckpoint,
    PipelineConfig,
    make_candidate_batches,
    run_candidate_pipeline,
)


def _record(index: int, guess: str) -> GuessRecord:
    return GuessRecord(
        prob=1.0 / (index + 1),
        guess=guess,
        structure_index=0,
        structure_name="A",
        ranks=(index,),
    )


def test_candidate_batches_split_by_payload_and_count() -> None:
    records = [_record(index, guess) for index, guess in enumerate(["aaaa", "bbbb", "cccc"])]

    batches = list(
        make_candidate_batches(
            records,
            batch_size=10,
            max_batch_payload_bytes=10,
        )
    )

    assert [(batch.batch_id, batch.start_rank, batch.end_rank) for batch in batches] == [
        (0, 0, 2),
        (1, 2, 3),
    ]
    assert [batch.guesses for batch in batches] == [("aaaa", "bbbb"), ("cccc",)]
    assert all(batch.payload_bytes <= 10 for batch in batches)


def test_candidate_batches_reject_single_oversized_guess() -> None:
    records = [_record(0, "this-is-too-large")]

    with pytest.raises(ValueError, match="single guess exceeds"):
        list(
            make_candidate_batches(
                records,
                batch_size=10,
                max_batch_payload_bytes=4,
            )
        )


def test_mock_pipeline_keeps_queue_under_configured_bounds() -> None:
    records = [_record(index, f"g{index:03d}") for index in range(50)]
    config = PipelineConfig(
        batch_size=5,
        max_batch_payload_bytes=64,
        max_pending_batches=2,
        max_pending_candidates=10,
        max_pending_payload_bytes=128,
        consumer_count=1,
        consumer_delay_seconds=0.001,
    )

    stats = run_candidate_pipeline(records, config)

    assert stats.produced_candidates == 50
    assert stats.consumed_candidates == 50
    assert stats.produced_batches == 10
    assert stats.consumed_batches == 10
    assert stats.duplicate_batches == 0
    assert stats.peak_pending_batches <= config.max_pending_batches
    assert stats.peak_pending_candidates <= config.max_pending_candidates
    assert stats.peak_pending_payload_bytes <= config.max_pending_payload_bytes
    assert stats.peak_inflight_batches <= config.consumer_count
    assert stats.peak_inflight_payload_bytes <= (
        config.consumer_count * config.max_batch_payload_bytes
    )
    assert stats.producer_waits > 0


def test_queue_rejects_single_batch_that_cannot_fit() -> None:
    batch = CandidateBatch.from_records(
        batch_id=0,
        start_rank=0,
        records=[_record(0, "aaaa"), _record(1, "bbbb")],
    )
    config = PipelineConfig(
        batch_size=10,
        max_batch_payload_bytes=32,
        max_pending_batches=1,
        max_pending_candidates=1,
        max_pending_payload_bytes=32,
    )

    with pytest.raises(RuntimeError, match="candidate pipeline failed"):
        run_candidate_pipeline(batch.records, config)


def test_batch_retry_ledger_tracks_failed_consumer_retry() -> None:
    batch = CandidateBatch.from_records(
        batch_id=7,
        start_rank=20,
        records=[_record(20, "g20"), _record(21, "g21")],
    )
    ledger = BatchRetryLedger()

    ledger.publish(batch)
    first = ledger.start(batch.batch_id, consumer_id="consumer-a", now=1.0)
    failed = ledger.fail(batch.batch_id, consumer_id="consumer-a", now=2.0)
    retry = ledger.start(batch.batch_id, consumer_id="consumer-b", now=3.0)
    done = ledger.complete(batch.batch_id, consumer_id="consumer-b", now=4.0)

    assert first.state == BatchState.INFLIGHT
    assert failed.state == BatchState.FAILED
    assert retry.attempts == 2
    assert done.state == BatchState.DONE
    assert ledger.retryable() == ()
    assert ledger.stats.published == 1
    assert ledger.stats.started == 2
    assert ledger.stats.failed == 1
    assert ledger.stats.completed == 1
    assert ledger.stats.retries == 1


def test_batch_retry_ledger_json_snapshot_round_trips() -> None:
    batch = CandidateBatch.from_records(
        batch_id=3,
        start_rank=10,
        records=[_record(10, "g10")],
    )
    ledger = BatchRetryLedger()
    ledger.publish(batch, now=1.0)
    ledger.start(batch.batch_id, consumer_id="consumer-a", now=2.0)

    restored = BatchRetryLedger.from_dict(ledger.to_dict())

    entry = restored.entry(batch.batch_id)
    assert entry is not None
    assert entry.state == BatchState.INFLIGHT
    assert entry.consumer_id == "consumer-a"
    assert restored.stats.published == 1
    assert restored.stats.started == 1


def test_durable_batch_checkpoint_keeps_only_pending_payloads(tmp_path) -> None:
    done_batch = CandidateBatch.from_records(
        batch_id=0,
        start_rank=0,
        records=[_record(0, "done")],
    )
    pending_batch = CandidateBatch.from_records(
        batch_id=1,
        start_rank=1,
        records=[_record(1, "pending")],
    )
    ledger = BatchRetryLedger()
    ledger.publish(done_batch)
    ledger.complete(done_batch.batch_id, consumer_id="consumer-a")
    ledger.publish(pending_batch)
    ledger.start(pending_batch.batch_id, consumer_id="consumer-b")

    checkpoint = DurableBatchCheckpoint.create(
        next_batch_id=2,
        next_start_rank=2,
        ledger=ledger,
        inflight_batches={pending_batch.batch_id: pending_batch},
    )
    path = tmp_path / "batch-checkpoint.json"
    checkpoint.write_atomic(path)

    restored = DurableBatchCheckpoint.read(path)

    assert restored.next_batch_id == 2
    assert restored.next_start_rank == 2
    assert [batch.batch_id for batch in restored.pending_batches()] == [1]
    assert restored.pending_batches()[0].guesses == ("pending",)
    assert restored.ledger.entry(0).state == BatchState.DONE  # type: ignore[union-attr]
    assert restored.ledger.entry(1).state == BatchState.INFLIGHT  # type: ignore[union-attr]
