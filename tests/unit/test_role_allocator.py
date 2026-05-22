from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_EXPERIMENT_SRC = Path(__file__).resolve().parents[2] / "experiments" / "src"
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from cqdagpcfg_parallel.distributed import (  # noqa: E402
    CqdagAwareElasticRoleAllocator,
    DistributedRole,
    DynamicRoleAllocator,
    RoleAllocationConfig,
    RoleAllocationInput,
    ThroughputOptimalRoleAllocator,
)
from shared.role_signals import (  # noqa: E402
    RoleSignalConfig,
    build_role_signal_snapshot,
    switch_candidates,
)


def test_role_allocator_balances_generator_and_consumer_rates() -> None:
    allocator = ThroughputOptimalRoleAllocator()

    plan = allocator.plan(
        RoleAllocationInput(
            total_nodes=6,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=200.0,
        )
    )

    assert plan.generator_count == 4
    assert plan.consumer_count == 2
    assert plan.reason == "throughput_optimal"
    assert plan.expected_generation_rate == 400.0
    assert plan.expected_consumption_rate == 400.0
    assert plan.expected_throughput == 400.0
    assert plan.bottleneck == "balanced"


def test_role_allocator_balances_equal_rates() -> None:
    allocator = ThroughputOptimalRoleAllocator()

    plan = allocator.plan(
        RoleAllocationInput(
            total_nodes=6,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=100.0,
        )
    )

    assert plan.generator_count == 3
    assert plan.consumer_count == 3
    assert plan.reason == "throughput_optimal"


def test_role_allocator_prefers_less_overproduction_when_integer_split_ties() -> None:
    allocator = ThroughputOptimalRoleAllocator()

    plan = allocator.plan(
        RoleAllocationInput(
            total_nodes=5,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=100.0,
        )
    )

    assert plan.generator_count == 2
    assert plan.consumer_count == 3
    assert plan.reason == "throughput_optimal"
    assert plan.expected_throughput == 200.0
    assert plan.bottleneck == "generator"


def test_role_allocator_matches_bruteforce_throughput_optimum() -> None:
    for total_nodes in range(2, 12):
        for generator_rate in (50.0, 100.0, 250.0):
            for consumer_rate in (75.0, 100.0, 300.0):
                snapshot = RoleAllocationInput(
                    total_nodes=total_nodes,
                    generator_rate_per_node=generator_rate,
                    consumer_rate_per_node=consumer_rate,
                )
                allocator = ThroughputOptimalRoleAllocator()
                plan = allocator.plan(snapshot)
                brute_force = max(
                    min(k * generator_rate, (total_nodes - k) * consumer_rate)
                    for k in range(1, total_nodes)
                )

                assert plan.expected_throughput == brute_force


def test_role_allocator_assigns_node_roles() -> None:
    allocator = ThroughputOptimalRoleAllocator()

    plan = allocator.assign(
        ("node-a", "node-b", "node-c", "node-d"),
        RoleAllocationInput(
            total_nodes=4,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=100.0,
        ),
    )

    assert [assignment.role for assignment in plan.assignments] == [
        DistributedRole.GENERATOR,
        DistributedRole.GENERATOR,
        DistributedRole.CONSUMER,
        DistributedRole.CONSUMER,
    ]


def test_role_allocator_requires_enough_nodes_for_minimum_roles() -> None:
    allocator = ThroughputOptimalRoleAllocator(
        RoleAllocationConfig(min_generators=2, min_consumers=2),
    )

    with pytest.raises(ValueError, match="minimum role counts"):
        allocator.plan(
            RoleAllocationInput(
                total_nodes=3,
                generator_rate_per_node=100.0,
                consumer_rate_per_node=100.0,
            )
        )


def test_role_allocator_uses_queue_pressure_to_add_consumers() -> None:
    allocator = ThroughputOptimalRoleAllocator()

    baseline = allocator.plan(
        RoleAllocationInput(
            total_nodes=6,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=100.0,
        )
    )
    pressured = allocator.plan(
        RoleAllocationInput(
            total_nodes=6,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=100.0,
            pending_candidates=950,
            max_pending_candidates=1000,
        )
    )

    assert baseline.generator_count == 3
    assert pressured.generator_count < baseline.generator_count
    assert pressured.consumer_count > baseline.consumer_count
    assert pressured.queue_pressure == 0.95


def test_role_allocator_avoids_switch_when_migration_cost_exceeds_gain() -> None:
    allocator = ThroughputOptimalRoleAllocator()

    plan = allocator.plan(
        RoleAllocationInput(
            total_nodes=6,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=200.0,
            current_generator_count=3,
            migration_cost_per_role_swap=200.0,
        )
    )

    assert plan.generator_count == 3
    assert plan.estimated_swap_cost == 0.0


def test_cqdag_aware_allocator_moves_toward_generators_when_frontier_is_blocked() -> None:
    allocator = CqdagAwareElasticRoleAllocator()

    baseline = allocator.plan(
        RoleAllocationInput(
            total_nodes=6,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=100.0,
        )
    )
    pressured = allocator.plan(
        RoleAllocationInput(
            total_nodes=6,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=100.0,
            cqdag_frontier_pressure=1.0,
            cqdag_priority_pressure=1.0,
        )
    )

    assert baseline.generator_count == 3
    assert pressured.generator_count > baseline.generator_count
    assert pressured.reason == "cqdag_aware_elastic"
    assert pressured.cqdag_frontier_pressure == 1.0


def test_cqdag_aware_allocator_moves_toward_consumers_under_reclaim_pressure() -> None:
    allocator = CqdagAwareElasticRoleAllocator()

    baseline = allocator.plan(
        RoleAllocationInput(
            total_nodes=6,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=100.0,
        )
    )
    pressured = allocator.plan(
        RoleAllocationInput(
            total_nodes=6,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=100.0,
            cqdag_reclaim_pressure=1.0,
        )
    )

    assert pressured.generator_count < baseline.generator_count
    assert pressured.consumer_count > baseline.consumer_count
    assert pressured.cqdag_reclaim_pressure == 1.0


def test_dynamic_role_allocator_is_cqdag_aware_default() -> None:
    allocator = DynamicRoleAllocator()

    plan = allocator.plan(
        RoleAllocationInput(
            total_nodes=4,
            generator_rate_per_node=100.0,
            consumer_rate_per_node=100.0,
            cqdag_frontier_pressure=1.0,
        )
    )

    assert plan.reason == "cqdag_aware_elastic"


def test_role_allocator_payback_blocks_short_tail_switch() -> None:
    allocator = CqdagAwareElasticRoleAllocator()
    snapshot = RoleAllocationInput(
        total_nodes=5,
        generator_rate_per_node=120.0,
        consumer_rate_per_node=1000.0,
        current_generator_count=3,
        remaining_candidates=100,
        role_swap_cost_seconds=1.0,
    )
    plan = allocator.plan(snapshot)

    payback = allocator.payback_for(snapshot, plan, current_generator_count=3)

    assert plan.generator_count == 4
    assert not payback.should_switch
    assert payback.reason == "negative_payback"


def test_role_allocator_payback_allows_long_tail_switch() -> None:
    allocator = CqdagAwareElasticRoleAllocator()
    snapshot = RoleAllocationInput(
        total_nodes=5,
        generator_rate_per_node=120.0,
        consumer_rate_per_node=1000.0,
        current_generator_count=3,
        remaining_candidates=10000,
        role_swap_cost_seconds=1.0,
    )
    plan = allocator.plan(snapshot)

    payback = allocator.payback_for(snapshot, plan, current_generator_count=3)

    assert plan.generator_count == 4
    assert payback.should_switch
    assert payback.saved_seconds > 0.0


def test_role_signals_build_cqdag_aware_allocation_input(tmp_path) -> None:
    tracker_metrics_path = tmp_path / "tracker.json"
    generator_metrics_path = tmp_path / "generator.json"
    consumer_metrics_path = tmp_path / "consumer.json"
    tracker_metrics_path.write_text(
        json.dumps({"published_candidates": 100, "candidate_rate": 200.0}),
        encoding="utf-8",
    )
    generator_metrics_path.write_text(
        json.dumps(
            {
                "waits": 1,
                "completed_items": 9,
                "source_cached_records": 12,
                "source_peak_cached_records": 24,
                "source_reclaimed_records": 6,
                "source_dag_repository_active_units": 4,
                "source_dag_stream_active_units": 2,
            }
        ),
        encoding="utf-8",
    )
    consumer_metrics_path.write_text(
        json.dumps(
            {
                "consumed_candidates": 60,
                "consumer_rate": 120.0,
                "network_poll_seconds": 1.0,
                "elapsed_seconds": 10.0,
            }
        ),
        encoding="utf-8",
    )

    snapshot = build_role_signal_snapshot(
        config=RoleSignalConfig(
            total_nodes=2,
            generator_rate=100.0,
            consumer_rate=100.0,
            batch_size=10,
            limit=1000,
            model_json_page_cache=16,
        ),
        agent_metrics_paths={
            "node-g": generator_metrics_path,
            "node-c": consumer_metrics_path,
        },
        roles={"node-g": DistributedRole.GENERATOR, "node-c": DistributedRole.CONSUMER},
        tracker_metrics_path=tracker_metrics_path,
    )

    assert snapshot is not None
    assert snapshot.current_generators == 1
    assert snapshot.current_consumers == 1
    assert snapshot.allocation_input.pending_candidates == 40
    assert snapshot.allocation_input.remaining_candidates == 900
    assert snapshot.allocation_input.generator_rate_per_node == 200.0
    assert snapshot.allocation_input.consumer_rate_per_node == 120.0
    assert snapshot.allocation_input.cqdag_reclaim_pressure > 0.0
    assert snapshot.allocation_input.cqdag_page_locality > 0.0


def test_role_signal_switch_candidates_preserve_cqdag_state() -> None:
    roles = {
        "hot-generator": "generator",
        "cold-generator": "generator",
        "idle-consumer": "consumer",
        "busy-consumer": "consumer",
    }
    metrics = {
        "hot-generator": {
            "source_dag_repository_active_units": 10,
            "source_dag_stream_active_units": 10,
            "source_cached_records": 10,
        },
        "cold-generator": {
            "source_dag_repository_active_units": 0,
            "source_dag_stream_active_units": 1,
            "source_cached_records": 1,
        },
        "idle-consumer": {
            "network_poll_seconds": 9.0,
            "elapsed_seconds": 10.0,
            "consumer_rate": 1.0,
        },
        "busy-consumer": {
            "network_poll_seconds": 1.0,
            "elapsed_seconds": 10.0,
            "consumer_rate": 100.0,
        },
    }

    generator_candidates = switch_candidates(
        roles.keys(),
        roles,
        "generator",
        metrics_by_node=metrics,
    )
    consumer_candidates = switch_candidates(
        roles.keys(),
        roles,
        "consumer",
        metrics_by_node=metrics,
    )

    assert generator_candidates[0] == "cold-generator"
    assert consumer_candidates[0] == "idle-consumer"
