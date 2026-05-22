from __future__ import annotations

from CQDAGPCFG import OptimizedCQDAGEnumerator
from CQDAGPCFG.cpp_backend import CppOptimizedCQDAGEnumerator, cpp_backend_available
from CQDAGPCFG.training import PCFGTrainer

from cqdagpcfg_parallel.protocol import stable_record_string
from cqdagpcfg_parallel.runtime import PipelineConfig, run_candidate_pipeline


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


def test_candidate_pipeline_preserves_cqdagpcfg_prefix_and_bounds() -> None:
    model = _toy_model()
    limit = 80
    baseline = OptimizedCQDAGEnumerator(model).run(
        limit=limit,
        sample_limit=limit,
        measure_memory=False,
    )
    generator = (
        CppOptimizedCQDAGEnumerator(model)
        if cpp_backend_available()
        else OptimizedCQDAGEnumerator(model)
    )
    config = PipelineConfig(
        batch_size=7,
        max_batch_payload_bytes=256,
        max_pending_batches=2,
        max_pending_candidates=14,
        max_pending_payload_bytes=512,
        consumer_count=1,
        collect_outputs=True,
        consumer_delay_seconds=0.001,
    )

    stats = run_candidate_pipeline(generator.iter_records(limit), config)

    assert stats.produced_candidates == limit
    assert stats.consumed_candidates == limit
    assert stats.duplicate_batches == 0
    assert stats.peak_pending_batches <= config.max_pending_batches
    assert stats.peak_pending_candidates <= config.max_pending_candidates
    assert stats.peak_pending_payload_bytes <= config.max_pending_payload_bytes
    assert stats.peak_inflight_batches <= config.consumer_count
    assert stats.peak_inflight_payload_bytes <= (
        config.consumer_count * config.max_batch_payload_bytes
    )
    assert stats.collected_stable_records == tuple(
        stable_record_string(record) for record in baseline.outputs
    )
