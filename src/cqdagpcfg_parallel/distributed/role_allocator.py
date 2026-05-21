from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Sequence


class DistributedRole(str, Enum):
    GENERATOR = "generator"
    CONSUMER = "consumer"


@dataclass(frozen=True, slots=True)
class RoleAllocationConfig:
    min_generators: int = 1
    min_consumers: int = 1
    high_queue_pressure: float = 0.8
    low_queue_pressure: float = 0.2
    queue_pressure_weight: float = 1.0
    idle_ratio_weight: float = 0.25
    frontier_pressure_weight: float = 1.0
    priority_pressure_weight: float = 0.5
    reclaim_pressure_weight: float = 1.0
    page_locality_weight: float = 0.5

    def __post_init__(self) -> None:
        if self.min_generators < 0:
            raise ValueError("min_generators cannot be negative")
        if self.min_consumers < 0:
            raise ValueError("min_consumers cannot be negative")
        if not 0.0 <= self.low_queue_pressure <= self.high_queue_pressure <= 1.0:
            raise ValueError("queue pressure thresholds must be ordered in [0, 1]")
        if self.queue_pressure_weight < 0.0:
            raise ValueError("queue_pressure_weight cannot be negative")
        if self.idle_ratio_weight < 0.0:
            raise ValueError("idle_ratio_weight cannot be negative")
        if self.frontier_pressure_weight < 0.0:
            raise ValueError("frontier_pressure_weight cannot be negative")
        if self.priority_pressure_weight < 0.0:
            raise ValueError("priority_pressure_weight cannot be negative")
        if self.reclaim_pressure_weight < 0.0:
            raise ValueError("reclaim_pressure_weight cannot be negative")
        if self.page_locality_weight < 0.0:
            raise ValueError("page_locality_weight cannot be negative")


@dataclass(frozen=True, slots=True)
class RoleAllocationInput:
    total_nodes: int
    generator_rate_per_node: float
    consumer_rate_per_node: float
    current_generator_count: int | None = None
    pending_candidates: int = 0
    max_pending_candidates: int = 0
    generator_idle_ratio: float = 0.0
    consumer_idle_ratio: float = 0.0
    migration_cost_per_role_swap: float = 0.0
    cqdag_frontier_pressure: float = 0.0
    cqdag_priority_pressure: float = 0.0
    cqdag_reclaim_pressure: float = 0.0
    cqdag_page_locality: float = 0.0

    def __post_init__(self) -> None:
        if self.total_nodes <= 0:
            raise ValueError("total_nodes must be positive")
        if self.generator_rate_per_node <= 0.0:
            raise ValueError("generator_rate_per_node must be positive")
        if self.consumer_rate_per_node <= 0.0:
            raise ValueError("consumer_rate_per_node must be positive")
        if self.current_generator_count is not None and not (
            0 <= self.current_generator_count <= self.total_nodes
        ):
            raise ValueError("current_generator_count must be within total_nodes")
        if self.pending_candidates < 0:
            raise ValueError("pending_candidates cannot be negative")
        if self.max_pending_candidates < 0:
            raise ValueError("max_pending_candidates cannot be negative")
        if not 0.0 <= self.generator_idle_ratio <= 1.0:
            raise ValueError("generator_idle_ratio must be between 0 and 1")
        if not 0.0 <= self.consumer_idle_ratio <= 1.0:
            raise ValueError("consumer_idle_ratio must be between 0 and 1")
        if self.migration_cost_per_role_swap < 0.0:
            raise ValueError("migration_cost_per_role_swap cannot be negative")
        if not 0.0 <= self.cqdag_frontier_pressure <= 1.0:
            raise ValueError("cqdag_frontier_pressure must be between 0 and 1")
        if not 0.0 <= self.cqdag_priority_pressure <= 1.0:
            raise ValueError("cqdag_priority_pressure must be between 0 and 1")
        if not 0.0 <= self.cqdag_reclaim_pressure <= 1.0:
            raise ValueError("cqdag_reclaim_pressure must be between 0 and 1")
        if not 0.0 <= self.cqdag_page_locality <= 1.0:
            raise ValueError("cqdag_page_locality must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class RoleAssignment:
    node_id: str
    role: DistributedRole


@dataclass(frozen=True, slots=True)
class RoleAllocationPlan:
    generator_count: int
    consumer_count: int
    reason: str
    assignments: tuple[RoleAssignment, ...] = ()
    expected_generation_rate: float = 0.0
    expected_consumption_rate: float = 0.0
    expected_throughput: float = 0.0
    bottleneck: str = "balanced"
    queue_pressure: float = 0.0
    estimated_swap_cost: float = 0.0
    cqdag_frontier_pressure: float = 0.0
    cqdag_reclaim_pressure: float = 0.0
    cqdag_page_locality: float = 0.0


class ThroughputOptimalRoleAllocator:
    """Choose the generator/consumer ratio that maximizes steady-state throughput.

    Under the homogeneous-node model, if k nodes generate candidates and N-k nodes
    consume them, the pipeline throughput is:

        T(k) = min(k * generator_rate, (N - k) * consumer_rate)

    The allocator returns the valid integer k that maximizes T(k).
    """

    def __init__(self, config: RoleAllocationConfig | None = None) -> None:
        self.config = RoleAllocationConfig() if config is None else config
        self.reason = "throughput_optimal"

    def plan(self, snapshot: RoleAllocationInput) -> RoleAllocationPlan:
        self._require_capacity(snapshot.total_nodes)
        generator_count = self._optimal_generator_count(snapshot)
        return self._make_plan(snapshot, generator_count, assignments=())

    def assign(
        self,
        node_ids: Sequence[str],
        snapshot: RoleAllocationInput,
    ) -> RoleAllocationPlan:
        if len(node_ids) != snapshot.total_nodes:
            raise ValueError("node_ids length must match total_nodes")
        plan = self.plan(snapshot)
        assignments = tuple(
            RoleAssignment(
                node_id=node_id,
                role=(
                    DistributedRole.GENERATOR
                    if index < plan.generator_count
                    else DistributedRole.CONSUMER
                ),
            )
            for index, node_id in enumerate(node_ids)
        )
        return RoleAllocationPlan(
            generator_count=plan.generator_count,
            consumer_count=plan.consumer_count,
            reason=plan.reason,
            assignments=assignments,
            expected_generation_rate=plan.expected_generation_rate,
            expected_consumption_rate=plan.expected_consumption_rate,
            expected_throughput=plan.expected_throughput,
            bottleneck=plan.bottleneck,
            queue_pressure=plan.queue_pressure,
            estimated_swap_cost=plan.estimated_swap_cost,
            cqdag_frontier_pressure=plan.cqdag_frontier_pressure,
            cqdag_reclaim_pressure=plan.cqdag_reclaim_pressure,
            cqdag_page_locality=plan.cqdag_page_locality,
        )

    def throughput_for(self, snapshot: RoleAllocationInput, generator_count: int) -> float:
        if generator_count < 0 or generator_count > snapshot.total_nodes:
            raise ValueError("generator_count must be within total_nodes")
        consumer_count = snapshot.total_nodes - generator_count
        generation_rate = generator_count * snapshot.generator_rate_per_node
        consumption_rate = consumer_count * snapshot.consumer_rate_per_node
        return min(generation_rate, consumption_rate)

    def _optimal_generator_count(self, snapshot: RoleAllocationInput) -> int:
        min_generators = self.config.min_generators
        max_generators = snapshot.total_nodes - self.config.min_consumers
        candidates = range(min_generators, max_generators + 1)
        continuous_balance = (
            snapshot.total_nodes
            * snapshot.consumer_rate_per_node
            / (snapshot.generator_rate_per_node + snapshot.consumer_rate_per_node)
        )
        return max(
            candidates,
            key=lambda count: self._optimization_key(
                snapshot,
                generator_count=count,
                continuous_balance=continuous_balance,
            ),
        )

    def _optimization_key(
        self,
        snapshot: RoleAllocationInput,
        *,
        generator_count: int,
        continuous_balance: float,
    ) -> tuple[float, float, float, int]:
        consumer_count = snapshot.total_nodes - generator_count
        generation_rate = generator_count * snapshot.generator_rate_per_node
        consumption_rate = consumer_count * snapshot.consumer_rate_per_node
        throughput = min(generation_rate, consumption_rate)
        imbalance = abs(generation_rate - consumption_rate)
        distance_from_balance = abs(generator_count - continuous_balance)
        control_score = self._control_score(
            snapshot,
            generator_count=generator_count,
            generation_rate=generation_rate,
            consumption_rate=consumption_rate,
            throughput=throughput,
        )
        return (
            control_score,
            throughput,
            -imbalance,
            -distance_from_balance,
            -generator_count,
        )

    def _make_plan(
        self,
        snapshot: RoleAllocationInput,
        generator_count: int,
        *,
        assignments: tuple[RoleAssignment, ...],
    ) -> RoleAllocationPlan:
        consumer_count = snapshot.total_nodes - generator_count
        generation_rate = generator_count * snapshot.generator_rate_per_node
        consumption_rate = consumer_count * snapshot.consumer_rate_per_node
        throughput = min(generation_rate, consumption_rate)
        if generation_rate < consumption_rate:
            bottleneck = "generator"
        elif consumption_rate < generation_rate:
            bottleneck = "consumer"
        else:
            bottleneck = "balanced"
        return RoleAllocationPlan(
            generator_count=generator_count,
            consumer_count=consumer_count,
            reason=self.reason,
            assignments=assignments,
            expected_generation_rate=generation_rate,
            expected_consumption_rate=consumption_rate,
            expected_throughput=throughput,
            bottleneck=bottleneck,
            queue_pressure=_queue_pressure(snapshot),
            estimated_swap_cost=self._swap_cost(snapshot, generator_count),
            cqdag_frontier_pressure=snapshot.cqdag_frontier_pressure,
            cqdag_reclaim_pressure=snapshot.cqdag_reclaim_pressure,
            cqdag_page_locality=snapshot.cqdag_page_locality,
        )

    def _require_capacity(self, total_nodes: int) -> None:
        required = self.config.min_generators + self.config.min_consumers
        if total_nodes < required:
            raise ValueError("total_nodes cannot satisfy minimum role counts")

    def _control_score(
        self,
        snapshot: RoleAllocationInput,
        *,
        generator_count: int,
        generation_rate: float,
        consumption_rate: float,
        throughput: float,
    ) -> float:
        pressure = _queue_pressure(snapshot)
        score = self._elastic_throughput_score(
            snapshot,
            generation_rate=generation_rate,
            consumption_rate=consumption_rate,
        )
        overproduction = max(0.0, generation_rate - consumption_rate)
        underproduction = max(0.0, consumption_rate - generation_rate)

        if snapshot.max_pending_candidates > 0:
            if pressure >= self.config.high_queue_pressure:
                score += (
                    self.config.queue_pressure_weight
                    * pressure
                    * max(0.0, consumption_rate - generation_rate)
                )
                score -= self.config.queue_pressure_weight * pressure * overproduction
            elif pressure <= self.config.low_queue_pressure:
                score += (
                    self.config.queue_pressure_weight
                    * (1.0 - pressure)
                    * max(0.0, generation_rate - consumption_rate)
                )
                score -= self.config.queue_pressure_weight * (1.0 - pressure) * underproduction

        idle_bias = snapshot.consumer_idle_ratio - snapshot.generator_idle_ratio
        score += self.config.idle_ratio_weight * idle_bias * generation_rate
        score -= self._page_locality_swap_cost(snapshot, generator_count)
        score -= self._swap_cost(snapshot, generator_count)
        return score

    def _elastic_throughput_score(
        self,
        snapshot: RoleAllocationInput,
        *,
        generation_rate: float,
        consumption_rate: float,
    ) -> float:
        frontier_signal = (
            self.config.frontier_pressure_weight
            * snapshot.cqdag_frontier_pressure
            * (1.0 + self.config.priority_pressure_weight * snapshot.cqdag_priority_pressure)
        )
        reclaim_signal = (
            self.config.reclaim_pressure_weight * snapshot.cqdag_reclaim_pressure
        )
        adjusted_generation = generation_rate / (1.0 + frontier_signal)
        adjusted_consumption = consumption_rate / (1.0 + reclaim_signal)
        return min(adjusted_generation, adjusted_consumption)

    def _swap_cost(self, snapshot: RoleAllocationInput, generator_count: int) -> float:
        if snapshot.current_generator_count is None:
            return 0.0
        swaps = abs(generator_count - snapshot.current_generator_count)
        return swaps * snapshot.migration_cost_per_role_swap

    def _page_locality_swap_cost(
        self,
        snapshot: RoleAllocationInput,
        generator_count: int,
    ) -> float:
        if snapshot.current_generator_count is None:
            return 0.0
        removed_generators = max(0, snapshot.current_generator_count - generator_count)
        return (
            removed_generators
            * snapshot.generator_rate_per_node
            * snapshot.cqdag_page_locality
            * self.config.page_locality_weight
        )


def _queue_pressure(snapshot: RoleAllocationInput) -> float:
    if snapshot.max_pending_candidates <= 0:
        return 0.0
    return min(1.0, snapshot.pending_candidates / snapshot.max_pending_candidates)


class CqdagAwareElasticRoleAllocator(ThroughputOptimalRoleAllocator):
    """CQDAG-aware generator/consumer role allocator.

    The allocator keeps the throughput balance model, but treats CQDAG frontier
    blocking and reclaim pressure as first-class signals. A blocked high-priority
    frontier makes generation effectively scarcer, so the optimal split moves
    toward generators. Reclaim pressure makes consumption effectively scarcer, so
    the split moves toward consumers. Page locality penalizes switching warm
    generators away from generation.
    """

    def __init__(self, config: RoleAllocationConfig | None = None) -> None:
        super().__init__(config=config)
        self.reason = "cqdag_aware_elastic"


class DynamicRoleAllocator(CqdagAwareElasticRoleAllocator):
    """Backward-compatible name for the default elastic CQDAG role allocator."""


__all__ = [
    "DistributedRole",
    "CqdagAwareElasticRoleAllocator",
    "DynamicRoleAllocator",
    "RoleAllocationConfig",
    "RoleAllocationInput",
    "RoleAllocationPlan",
    "RoleAssignment",
    "ThroughputOptimalRoleAllocator",
]
