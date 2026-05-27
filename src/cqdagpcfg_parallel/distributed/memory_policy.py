from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .resources import WorkerResourceSpec


MIB = 1024 * 1024
DEFAULT_RUNTIME_RESERVE_BYTES = 256 * MIB
DEFAULT_GENERATOR_WORK_FRACTION = 0.20
DEFAULT_CONSUMER_BATCH_FRACTION = 0.20
DEFAULT_MODEL_PAGE_CACHE_FRACTION = 0.10
DEFAULT_SOURCE_RECORD_BYTES = 512
DEFAULT_STREAMING_ARTIFACT_RECORD_BYTES = 16
DEFAULT_STREAMING_ARTIFACT_WORK_FRACTION = 0.50
DEFAULT_BATCH_RECORD_BYTES = 256
DEFAULT_MODEL_JSON_PAGE_BYTES = 1 * MIB
MIN_MEMORY_LIMITED_RECORDS = 1
MIN_MEMORY_LIMITED_PAYLOAD_BYTES = 4 * 1024


@dataclass(frozen=True, slots=True)
class BatchMemoryLimits:
    batch_size: int
    max_payload_bytes: int

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.max_payload_bytes <= 0:
            raise ValueError("max_payload_bytes must be positive")


def memory_limited_chunk_size(
    resources: WorkerResourceSpec | None,
    configured_max_chunk_size: int,
    *,
    runtime_reserve_bytes: int = DEFAULT_RUNTIME_RESERVE_BYTES,
    work_fraction: float = DEFAULT_GENERATOR_WORK_FRACTION,
    estimated_record_bytes: int = DEFAULT_SOURCE_RECORD_BYTES,
) -> int:
    if configured_max_chunk_size <= 0:
        raise ValueError("configured_max_chunk_size must be positive")
    cap = _record_cap_from_memory(
        resources,
        configured_max_chunk_size=configured_max_chunk_size,
        runtime_reserve_bytes=runtime_reserve_bytes,
        fraction=work_fraction,
        estimated_record_bytes=estimated_record_bytes,
    )
    return min(configured_max_chunk_size, cap)


def memory_limited_batch_limits(
    resources: Iterable[WorkerResourceSpec],
    *,
    configured_batch_size: int,
    configured_max_payload_bytes: int,
    runtime_reserve_bytes: int = DEFAULT_RUNTIME_RESERVE_BYTES,
    batch_fraction: float = DEFAULT_CONSUMER_BATCH_FRACTION,
    estimated_record_bytes: int = DEFAULT_BATCH_RECORD_BYTES,
) -> BatchMemoryLimits:
    if configured_batch_size <= 0:
        raise ValueError("configured_batch_size must be positive")
    if configured_max_payload_bytes <= 0:
        raise ValueError("configured_max_payload_bytes must be positive")
    _validate_budget_inputs(
        runtime_reserve_bytes=runtime_reserve_bytes,
        fraction=batch_fraction,
        estimated_record_bytes=estimated_record_bytes,
    )

    batch_size = configured_batch_size
    max_payload_bytes = configured_max_payload_bytes
    seen_memory_budget = False
    for resource in resources:
        if resource.memory_bytes is None:
            continue
        seen_memory_budget = True
        available = _available_memory_bytes(resource.memory_bytes, runtime_reserve_bytes)
        budget = int(available * batch_fraction)
        batch_size = min(
            batch_size,
            max(MIN_MEMORY_LIMITED_RECORDS, budget // estimated_record_bytes),
        )
        max_payload_bytes = min(
            max_payload_bytes,
            max(MIN_MEMORY_LIMITED_PAYLOAD_BYTES, budget),
        )

    if not seen_memory_budget:
        return BatchMemoryLimits(
            batch_size=configured_batch_size,
            max_payload_bytes=configured_max_payload_bytes,
        )
    return BatchMemoryLimits(
        batch_size=max(MIN_MEMORY_LIMITED_RECORDS, batch_size),
        max_payload_bytes=max(MIN_MEMORY_LIMITED_PAYLOAD_BYTES, max_payload_bytes),
    )


def memory_limited_model_page_cache(
    resources: WorkerResourceSpec | None,
    configured_page_cache: int,
    *,
    runtime_reserve_bytes: int = DEFAULT_RUNTIME_RESERVE_BYTES,
    page_cache_fraction: float = DEFAULT_MODEL_PAGE_CACHE_FRACTION,
    estimated_page_bytes: int = DEFAULT_MODEL_JSON_PAGE_BYTES,
) -> int:
    if configured_page_cache <= 0:
        raise ValueError("configured_page_cache must be positive")
    _validate_budget_inputs(
        runtime_reserve_bytes=runtime_reserve_bytes,
        fraction=page_cache_fraction,
        estimated_record_bytes=estimated_page_bytes,
    )
    if resources is None:
        return configured_page_cache
    if resources.model_json_page_cache is not None:
        configured_page_cache = min(configured_page_cache, resources.model_json_page_cache)
    if resources.memory_bytes is None:
        return configured_page_cache
    available = _available_memory_bytes(resources.memory_bytes, runtime_reserve_bytes)
    budget = int(available * page_cache_fraction)
    cap = max(1, budget // estimated_page_bytes)
    return max(1, min(configured_page_cache, cap))


def _record_cap_from_memory(
    resources: WorkerResourceSpec | None,
    *,
    configured_max_chunk_size: int,
    runtime_reserve_bytes: int,
    fraction: float,
    estimated_record_bytes: int,
) -> int:
    _validate_budget_inputs(
        runtime_reserve_bytes=runtime_reserve_bytes,
        fraction=fraction,
        estimated_record_bytes=estimated_record_bytes,
    )
    if resources is None or resources.memory_bytes is None:
        return configured_max_chunk_size
    available = _available_memory_bytes(resources.memory_bytes, runtime_reserve_bytes)
    budget = int(available * fraction)
    return max(MIN_MEMORY_LIMITED_RECORDS, budget // estimated_record_bytes)


def _available_memory_bytes(memory_bytes: int, runtime_reserve_bytes: int) -> int:
    if memory_bytes < 0:
        raise ValueError("memory_bytes cannot be negative")
    if runtime_reserve_bytes < 0:
        raise ValueError("runtime_reserve_bytes cannot be negative")
    return max(0, memory_bytes - runtime_reserve_bytes)


def _validate_budget_inputs(
    *,
    runtime_reserve_bytes: int,
    fraction: float,
    estimated_record_bytes: int,
) -> None:
    if runtime_reserve_bytes < 0:
        raise ValueError("runtime_reserve_bytes cannot be negative")
    if not 0.0 < fraction <= 1.0:
        raise ValueError("memory fraction must be in (0, 1]")
    if estimated_record_bytes <= 0:
        raise ValueError("estimated_record_bytes must be positive")


__all__ = [
    "BatchMemoryLimits",
    "DEFAULT_BATCH_RECORD_BYTES",
    "DEFAULT_CONSUMER_BATCH_FRACTION",
    "DEFAULT_GENERATOR_WORK_FRACTION",
    "DEFAULT_MODEL_JSON_PAGE_BYTES",
    "DEFAULT_MODEL_PAGE_CACHE_FRACTION",
    "DEFAULT_RUNTIME_RESERVE_BYTES",
    "DEFAULT_SOURCE_RECORD_BYTES",
    "memory_limited_batch_limits",
    "memory_limited_chunk_size",
    "memory_limited_model_page_cache",
]
