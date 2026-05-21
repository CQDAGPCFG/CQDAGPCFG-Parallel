from __future__ import annotations

import pytest

from cqdagpcfg_parallel.distributed import (
    CqdagAwareElasticRoleAllocator,
    DistributedRole,
    DynamicRoleAllocator,
    RoleAllocationConfig,
    RoleAllocationInput,
    ThroughputOptimalRoleAllocator,
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
