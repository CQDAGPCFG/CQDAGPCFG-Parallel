from __future__ import annotations

from cqdagpcfg_parallel.distributed import (
    WorkerResourceSpec,
    memory_limited_batch_limits,
    memory_limited_chunk_size,
    memory_limited_model_page_cache,
)


def test_memory_limited_chunk_size_caps_by_worker_memory() -> None:
    resources = WorkerResourceSpec(memory_bytes=512 * 1024 * 1024)

    cap = memory_limited_chunk_size(
        resources,
        configured_max_chunk_size=1_000_000,
    )

    assert 0 < cap < 1_000_000


def test_memory_limited_chunk_size_keeps_config_without_memory_budget() -> None:
    assert (
        memory_limited_chunk_size(
            WorkerResourceSpec(),
            configured_max_chunk_size=8192,
        )
        == 8192
    )


def test_memory_limited_batch_limits_use_smallest_consumer_memory() -> None:
    limits = memory_limited_batch_limits(
        (
            WorkerResourceSpec(memory_bytes=8 * 1024**3),
            WorkerResourceSpec(memory_bytes=512 * 1024**2),
        ),
        configured_batch_size=1_000_000,
        configured_max_payload_bytes=256 * 1024**2,
    )

    assert 0 < limits.batch_size < 1_000_000
    assert 0 < limits.max_payload_bytes < 256 * 1024**2


def test_memory_limited_model_page_cache_respects_explicit_worker_cap() -> None:
    resources = WorkerResourceSpec(
        memory_bytes=4 * 1024**3,
        model_json_page_cache=32,
    )

    assert memory_limited_model_page_cache(resources, 128) == 32
