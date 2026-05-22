from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from cqdagpcfg_parallel.distributed.role_allocator import RoleAllocationInput


@dataclass(frozen=True, slots=True)
class RoleSignalConfig:
    total_nodes: int
    generator_rate: float
    consumer_rate: float
    batch_size: int
    limit: int
    model_json_page_cache: int
    pending_window_batches: int = 4
    min_observed_rate: float = 1e-9
    role_swap_cost_seconds: float = 1.0


@dataclass(frozen=True, slots=True)
class RoleSignalSnapshot:
    allocation_input: RoleAllocationInput
    metrics_by_node: Mapping[str, Mapping[str, object]]
    current_generators: int
    current_consumers: int


def build_role_signal_snapshot(
    *,
    config: RoleSignalConfig,
    agent_metrics_paths: Mapping[str, Path],
    roles: Mapping[str, object],
    tracker_metrics_path: Path,
) -> RoleSignalSnapshot | None:
    current_generators = _count_role(agent_metrics_paths, roles, "generator")
    current_consumers = _count_role(agent_metrics_paths, roles, "consumer")
    if current_generators <= 0 or current_consumers <= 0:
        return None

    tracker_metrics = _read_json(tracker_metrics_path)
    published = int(tracker_metrics.get("published_candidates", 0))
    generation_rate = float(tracker_metrics.get("candidate_rate", 0.0))
    remaining = max(0, config.limit - published)

    consumed = 0
    consumer_rate = 0.0
    consumer_idle_seconds = 0.0
    consumer_elapsed_seconds = 0.0
    generator_waits = 0
    generator_completed_items = 0
    source_cached_records = 0
    source_peak_cached_records = 0
    source_reclaimed_records = 0
    source_repository_units = 0
    source_stream_units = 0
    metrics_by_node: dict[str, Mapping[str, object]] = {}

    for node_id, path in agent_metrics_paths.items():
        payload = _read_json(path)
        if not payload:
            continue
        metrics_by_node[node_id] = payload
        consumed += int(payload.get("consumed_candidates", 0))
        role = _role_value(roles.get(node_id))
        if role == "consumer":
            consumer_rate += float(payload.get("consumer_rate", 0.0))
            consumer_idle_seconds += float(payload.get("network_poll_seconds", 0.0))
            consumer_elapsed_seconds += float(payload.get("elapsed_seconds", 0.0))
        elif role == "generator":
            generator_waits += int(payload.get("waits", 0))
            generator_completed_items += int(payload.get("completed_items", 0))
            source_cached_records += int(payload.get("source_cached_records", 0))
            source_peak_cached_records += int(payload.get("source_peak_cached_records", 0))
            source_reclaimed_records += int(payload.get("source_reclaimed_records", 0))
            source_repository_units += int(
                payload.get("source_dag_repository_active_units", 0)
            )
            source_stream_units += int(payload.get("source_dag_stream_active_units", 0))

    if published <= 0 and consumed <= 0:
        return None

    pending = max(0, published - consumed)
    generator_rate_per_node = (
        generation_rate / current_generators
        if generation_rate > 0.0
        else config.generator_rate
    )
    consumer_rate_per_node = (
        consumer_rate / current_consumers
        if consumer_rate > 0.0
        else config.consumer_rate
    )
    max_pending = (
        config.batch_size * max(1, current_consumers) * config.pending_window_batches
    )
    queue_pressure = bounded_ratio(pending, max_pending)
    generator_idle_ratio = bounded_ratio(
        generator_waits,
        generator_waits + generator_completed_items,
    )
    consumer_idle_ratio = bounded_ratio(
        consumer_idle_seconds,
        consumer_elapsed_seconds,
    )
    source_cache_pressure = bounded_ratio(
        source_cached_records,
        max(source_cached_records, source_peak_cached_records, 1),
    )
    page_locality = bounded_ratio(
        source_repository_units + source_stream_units,
        max(1, current_generators * config.model_json_page_cache),
    )
    frontier_pressure = bounded_ratio(
        (1.0 - queue_pressure) * consumer_idle_ratio
        + max(0.0, consumer_rate_per_node - generator_rate_per_node)
        / max(generator_rate_per_node + consumer_rate_per_node, config.min_observed_rate),
        1.0,
    )
    priority_pressure = 1.0 - bounded_ratio(published, max(config.limit, 1))
    reclaim_pressure = max(
        queue_pressure,
        source_cache_pressure
        * (
            1.0
            - bounded_ratio(
                source_reclaimed_records,
                source_reclaimed_records + source_cached_records,
            )
        ),
    )
    return RoleSignalSnapshot(
        allocation_input=RoleAllocationInput(
            total_nodes=config.total_nodes,
            generator_rate_per_node=max(
                generator_rate_per_node,
                config.min_observed_rate,
            ),
            consumer_rate_per_node=max(
                consumer_rate_per_node,
                config.min_observed_rate,
            ),
            current_generator_count=current_generators,
            remaining_candidates=remaining,
            pending_candidates=pending,
            max_pending_candidates=max_pending,
            generator_idle_ratio=generator_idle_ratio,
            consumer_idle_ratio=consumer_idle_ratio,
            migration_cost_per_role_swap=config.batch_size,
            role_swap_cost_seconds=config.role_swap_cost_seconds,
            cqdag_frontier_pressure=frontier_pressure,
            cqdag_priority_pressure=priority_pressure,
            cqdag_reclaim_pressure=reclaim_pressure,
            cqdag_page_locality=page_locality,
        ),
        metrics_by_node=metrics_by_node,
        current_generators=current_generators,
        current_consumers=current_consumers,
    )


def switch_candidates(
    node_ids: Iterable[str],
    roles: Mapping[str, object],
    role: str,
    *,
    metrics_by_node: Mapping[str, Mapping[str, object]] | None = None,
) -> tuple[str, ...]:
    metrics_by_node = {} if metrics_by_node is None else metrics_by_node
    selected = tuple(
        node_id for node_id in node_ids if _role_value(roles.get(node_id)) == role
    )
    if role == "generator":
        return tuple(
            sorted(
                selected,
                key=lambda node_id: (
                    int(
                        metrics_by_node.get(node_id, {}).get(
                            "source_dag_repository_active_units",
                            0,
                        )
                    )
                    + int(
                        metrics_by_node.get(node_id, {}).get(
                            "source_dag_stream_active_units",
                            0,
                        )
                    )
                    + int(
                        metrics_by_node.get(node_id, {}).get(
                            "source_cached_records",
                            0,
                        )
                    ),
                    int(metrics_by_node.get(node_id, {}).get("completed_records", 0)),
                    node_id,
                ),
            )
        )
    if role == "consumer":
        return tuple(
            sorted(
                selected,
                key=lambda node_id: (
                    -bounded_ratio(
                        float(
                            metrics_by_node.get(node_id, {}).get(
                                "network_poll_seconds",
                                0.0,
                            )
                        ),
                        float(
                            metrics_by_node.get(node_id, {}).get(
                                "elapsed_seconds",
                                0.0,
                            )
                        ),
                    ),
                    float(metrics_by_node.get(node_id, {}).get("consumer_rate", 0.0)),
                    int(metrics_by_node.get(node_id, {}).get("consumed_candidates", 0)),
                    node_id,
                ),
            )
        )
    return selected


def bounded_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return min(1.0, max(0.0, numerator / denominator))


def _count_role(
    agent_metrics_paths: Mapping[str, Path],
    roles: Mapping[str, object],
    role: str,
) -> int:
    return sum(
        1
        for node_id in agent_metrics_paths
        if _role_value(roles.get(node_id)) == role
    )


def _role_value(role: object) -> str | None:
    if role is None:
        return None
    value = getattr(role, "value", role)
    return str(value)


def _read_json(path: Path) -> Mapping[str, object]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        return {}
    return payload


__all__ = [
    "RoleSignalConfig",
    "RoleSignalSnapshot",
    "bounded_ratio",
    "build_role_signal_snapshot",
    "switch_candidates",
]
