from __future__ import annotations

from dataclasses import dataclass

from .types import Demand, NodeId


MIN_MASS_DENSITY = 1e-300


@dataclass(frozen=True, slots=True)
class NodeSchedulingFeatures:
    node_id: NodeId
    entropy: float = 0.0
    priority: float = 1.0
    estimated_cost: float = 1.0
    cardinality: int | None = None

    def __post_init__(self) -> None:
        if self.entropy < 0.0:
            raise ValueError("entropy cannot be negative")
        if self.priority < 0.0:
            raise ValueError("priority cannot be negative")
        if self.estimated_cost <= 0.0:
            raise ValueError("estimated_cost must be positive")
        if self.cardinality is not None and self.cardinality < 0:
            raise ValueError("cardinality cannot be negative")


@dataclass(frozen=True, slots=True)
class NodeDependency:
    parent_id: NodeId
    child_id: NodeId
    donation_weight: float = 1.0

    def __post_init__(self) -> None:
        if self.donation_weight < 0.0:
            raise ValueError("donation_weight cannot be negative")


@dataclass(slots=True)
class NodeRuntimeState:
    node_id: NodeId
    frontier_start: int = 0
    ready_end: int = 0
    scheduled_end: int = 0
    target_end: int = 0
    entropy: float = 0.0
    urgency: float = 1.0
    priority: float = 1.0
    donated_priority: float = 0.0
    estimated_cost: float = 1.0
    child_miss_rate: float = 0.0
    chunk_latency_ewma: float = 0.0
    feedback_count: int = 0
    mass_density_ewma: float = 0.0
    mass_feedback_count: int = 0
    exhausted: bool = False
    exhausted_at: int | None = None

    def __post_init__(self) -> None:
        if self.frontier_start < 0:
            raise ValueError("frontier_start cannot be negative")
        if self.ready_end < 0:
            raise ValueError("ready_end cannot be negative")
        if self.scheduled_end < 0:
            raise ValueError("scheduled_end cannot be negative")
        if self.scheduled_end < self.ready_end:
            raise ValueError("scheduled_end cannot be smaller than ready_end")
        if self.target_end < 0:
            raise ValueError("target_end cannot be negative")
        if self.entropy < 0.0:
            raise ValueError("entropy cannot be negative")
        if self.urgency < 0.0:
            raise ValueError("urgency cannot be negative")
        if self.priority < 0.0:
            raise ValueError("priority cannot be negative")
        if self.donated_priority < 0.0:
            raise ValueError("donated_priority cannot be negative")
        if self.estimated_cost <= 0.0:
            raise ValueError("estimated_cost must be positive")
        if not 0.0 <= self.child_miss_rate <= 1.0:
            raise ValueError("child_miss_rate must be between 0 and 1")
        if self.chunk_latency_ewma < 0.0:
            raise ValueError("chunk_latency_ewma cannot be negative")
        if self.feedback_count < 0:
            raise ValueError("feedback_count cannot be negative")
        if self.mass_density_ewma < 0.0:
            raise ValueError("mass_density_ewma cannot be negative")
        if self.mass_feedback_count < 0:
            raise ValueError("mass_feedback_count cannot be negative")
        if self.exhausted_at is not None and self.exhausted_at < 0:
            raise ValueError("exhausted_at cannot be negative")

    @property
    def effective_target_end(self) -> int:
        if self.exhausted_at is None:
            return self.target_end
        return min(self.target_end, self.exhausted_at)

    @property
    def demand_gap(self) -> int:
        if self.exhausted:
            return 0
        return max(0, self.effective_target_end - self.ready_end)

    @property
    def effective_mass_density(self) -> float:
        if self.mass_feedback_count > 0 and self.mass_density_ewma > 0.0:
            return self.mass_density_ewma
        return max(self.priority, MIN_MASS_DENSITY)

    @property
    def demand_mass_gap(self) -> float:
        return self.demand_gap * self.effective_mass_density

    @property
    def frontier_ready_gap(self) -> int:
        return max(0, self.ready_end - self.frontier_start)

    @property
    def is_frontier_blocked(self) -> bool:
        return not self.exhausted and self.demand_gap > 0 and self.ready_end <= self.frontier_start

    def register_demand(self, demand: Demand) -> None:
        if demand.node_id != self.node_id:
            raise ValueError("demand node_id does not match state")
        self.target_end = max(self.target_end, demand.target_end)
        self.urgency = max(self.urgency, demand.urgency)

    def update_frontier_start(self, index: int) -> None:
        if index < self.frontier_start:
            raise ValueError("frontier_start cannot move backward")
        self.frontier_start = index

    def update_ready_end(self, ready_end: int) -> None:
        if ready_end < self.ready_end:
            raise ValueError("ready_end cannot move backward")
        self.ready_end = ready_end
        self.scheduled_end = max(self.scheduled_end, ready_end)
        if self.exhausted_at is not None and self.ready_end >= self.exhausted_at:
            self.exhausted = True

    def reserve_until(self, end: int) -> None:
        if end < self.ready_end:
            raise ValueError("scheduled_end cannot move behind ready_end")
        self.scheduled_end = max(self.scheduled_end, end)

    def reset_scheduled_end(self, end: int | None = None) -> None:
        value = self.ready_end if end is None else end
        if value < self.ready_end:
            raise ValueError("scheduled_end cannot be reset behind ready_end")
        self.scheduled_end = value

    def mark_exhausted(self, at: int | None = None) -> None:
        if at is None:
            self.exhausted = True
            self.exhausted_at = self.ready_end
            return
        if at < 0:
            raise ValueError("exhaustion index cannot be negative")
        self.exhausted_at = (
            at if self.exhausted_at is None else min(self.exhausted_at, at)
        )
        if self.ready_end >= self.exhausted_at:
            self.exhausted = True

    def update_scheduling_features(
        self,
        *,
        entropy: float | None = None,
        priority: float | None = None,
        estimated_cost: float | None = None,
    ) -> None:
        if entropy is not None:
            if entropy < 0.0:
                raise ValueError("entropy cannot be negative")
            self.entropy = max(self.entropy, entropy)
        if priority is not None:
            if priority < 0.0:
                raise ValueError("priority cannot be negative")
            self.priority = max(self.priority, priority)
        if estimated_cost is not None:
            if estimated_cost <= 0.0:
                raise ValueError("estimated_cost must be positive")
            self.estimated_cost = max(self.estimated_cost, estimated_cost)

    def donate_priority(self, amount: float) -> None:
        if amount < 0.0:
            raise ValueError("donated priority cannot be negative")
        self.donated_priority = max(self.donated_priority, amount)

    def record_runtime_feedback(
        self,
        *,
        chunk_latency_seconds: float,
        records_requested: int,
        records_produced: int,
        ewma_alpha: float,
        chunk_probability_mass: float | None = None,
    ) -> None:
        if chunk_latency_seconds < 0.0:
            raise ValueError("chunk_latency_seconds cannot be negative")
        if records_requested < 0:
            raise ValueError("records_requested cannot be negative")
        if records_produced < 0:
            raise ValueError("records_produced cannot be negative")
        if records_produced > records_requested:
            raise ValueError("records_produced cannot exceed records_requested")
        if not 0.0 < ewma_alpha <= 1.0:
            raise ValueError("ewma_alpha must be in (0, 1]")
        if chunk_probability_mass is not None and chunk_probability_mass < 0.0:
            raise ValueError("chunk_probability_mass cannot be negative")

        miss_rate = 0.0
        if records_requested:
            miss_rate = (records_requested - records_produced) / records_requested
        self.child_miss_rate = _ewma(self.child_miss_rate, miss_rate, ewma_alpha, self.feedback_count)
        self.chunk_latency_ewma = _ewma(
            self.chunk_latency_ewma,
            chunk_latency_seconds,
            ewma_alpha,
            self.feedback_count,
        )
        self.feedback_count += 1
        if chunk_probability_mass is not None and records_produced > 0:
            mass_density = chunk_probability_mass / records_produced
            self.mass_density_ewma = _ewma(
                self.mass_density_ewma,
                mass_density,
                ewma_alpha,
                self.mass_feedback_count,
            )
            self.mass_feedback_count += 1


class NodeStateTable:
    def __init__(self) -> None:
        self._states: dict[NodeId, NodeRuntimeState] = {}
        self._dependencies: dict[NodeId, dict[NodeId, float]] = {}

    def ensure_node(
        self,
        node_id: NodeId,
        *,
        entropy: float = 0.0,
        priority: float | None = None,
        estimated_cost: float | None = None,
    ) -> NodeRuntimeState:
        state = self._states.get(node_id)
        if state is None:
            state = NodeRuntimeState(
                node_id=node_id,
                entropy=entropy,
                priority=1.0 if priority is None else priority,
                estimated_cost=1.0 if estimated_cost is None else estimated_cost,
            )
            self._states[node_id] = state
        else:
            state.update_scheduling_features(
                entropy=entropy,
                priority=priority,
                estimated_cost=estimated_cost,
            )
        return state

    def update_frontier_start(self, node_id: NodeId, index: int) -> NodeRuntimeState:
        state = self.get(node_id)
        state.update_frontier_start(index)
        return state

    def get(self, node_id: NodeId) -> NodeRuntimeState:
        try:
            return self._states[node_id]
        except KeyError as exc:
            raise KeyError(f"unknown node_id: {node_id}") from exc

    def register_demand(
        self,
        node_id: NodeId,
        target_end: int,
        *,
        urgency: float = 1.0,
        entropy: float = 0.0,
        priority: float | None = None,
        estimated_cost: float | None = None,
    ) -> NodeRuntimeState:
        state = self.ensure_node(
            node_id,
            entropy=entropy,
            priority=priority,
            estimated_cost=estimated_cost,
        )
        state.register_demand(Demand(node_id=node_id, target_end=target_end, urgency=urgency))
        return state

    def update_ready_end(self, node_id: NodeId, ready_end: int) -> NodeRuntimeState:
        state = self.ensure_node(node_id)
        state.update_ready_end(ready_end)
        return state

    def active_demands(self) -> tuple[NodeRuntimeState, ...]:
        return tuple(state for state in self._states.values() if state.demand_gap > 0)

    def register_dependency(
        self,
        parent_id: NodeId,
        child_id: NodeId,
        *,
        donation_weight: float = 1.0,
    ) -> None:
        if donation_weight < 0.0:
            raise ValueError("donation_weight cannot be negative")
        self.ensure_node(parent_id)
        self.ensure_node(child_id)
        self._dependencies.setdefault(parent_id, {})[child_id] = donation_weight

    def donate_priority(self, node_id: NodeId, amount: float) -> NodeRuntimeState:
        state = self.ensure_node(node_id)
        state.donate_priority(amount)
        return state

    def apply_priority_donations(self) -> None:
        for parent_id, children in self._dependencies.items():
            parent = self._states.get(parent_id)
            if parent is None or parent.demand_gap <= 0:
                continue
            for child_id, weight in children.items():
                child = self.ensure_node(child_id)
                donation = parent.priority * parent.urgency * weight
                child.donate_priority(donation)

    def record_runtime_feedback(
        self,
        node_id: NodeId,
        *,
        chunk_latency_seconds: float,
        records_requested: int,
        records_produced: int,
        ewma_alpha: float,
        chunk_probability_mass: float | None = None,
    ) -> NodeRuntimeState:
        state = self.ensure_node(node_id)
        state.record_runtime_feedback(
            chunk_latency_seconds=chunk_latency_seconds,
            records_requested=records_requested,
            records_produced=records_produced,
            ewma_alpha=ewma_alpha,
            chunk_probability_mass=chunk_probability_mass,
        )
        return state

    def values(self) -> tuple[NodeRuntimeState, ...]:
        return tuple(self._states.values())


def _ewma(previous: float, sample: float, alpha: float, count: int) -> float:
    if count == 0:
        return sample
    return alpha * sample + (1.0 - alpha) * previous


__all__ = [
    "MIN_MASS_DENSITY",
    "NodeDependency",
    "NodeRuntimeState",
    "NodeSchedulingFeatures",
    "NodeStateTable",
]
