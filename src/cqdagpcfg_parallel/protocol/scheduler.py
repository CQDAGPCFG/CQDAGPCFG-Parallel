from __future__ import annotations

from dataclasses import dataclass
from math import ceil, sqrt

from .chunk_store import InMemoryChunkStore
from .lease_table import LeaseDeniedError, LeaseTable
from .node_state import NodeRuntimeState, NodeStateTable
from .types import ChunkSizePolicy, LeaseStrategyName, NodeId, WorkItem, WorkerId


DEFAULT_FIXED_CHUNK_SIZE = 8
DEFAULT_MIN_CHUNK_SIZE = 1
DEFAULT_MAX_CHUNK_SIZE = 64
DEFAULT_ENTROPY_LAMBDA = 0.25
DEFAULT_PRIORITY_DONATION_WEIGHT = 1.0
DEFAULT_CHILD_MISS_PENALTY = 1.0
DEFAULT_TARGET_CHUNK_LATENCY_SECONDS = 0.05
DEFAULT_LATENCY_FEEDBACK_MIN = 0.5
DEFAULT_LATENCY_FEEDBACK_MAX = 2.0
DEFAULT_FEEDBACK_EWMA_ALPHA = 0.25
DEFAULT_NODE_AFFINITY_BONUS = 0.5
DEFAULT_NODE_MIGRATION_PENALTY = 0.0
DEFAULT_MAX_PARALLEL_LEASES_PER_NODE = 2
DEFAULT_PARALLEL_LATENCY_FACTOR = 0.5
DEFAULT_TARGET_CHUNK_PROBABILITY_MASS = 0.0
DEFAULT_RANK_WINDOW_SIZE = 0
DEFAULT_RANK_WINDOW_FRONTIER_MULTIPLIER = 4.0
DEFAULT_TAIL_STEAL_MIN_GAP = 1
DEFAULT_TAIL_STEAL_PENDING_LIMIT_MULTIPLIER = 2.0
DEFAULT_TAIL_STEAL_SCORE_THRESHOLD = 0.0
MIN_EFFECTIVE_COST = 1e-12


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    policy: ChunkSizePolicy = ChunkSizePolicy.CQDAG_ADAPTIVE
    fixed_chunk_size: int = DEFAULT_FIXED_CHUNK_SIZE
    min_chunk_size: int = DEFAULT_MIN_CHUNK_SIZE
    max_chunk_size: int = DEFAULT_MAX_CHUNK_SIZE
    entropy_lambda: float = DEFAULT_ENTROPY_LAMBDA
    priority_donation_weight: float = DEFAULT_PRIORITY_DONATION_WEIGHT
    child_miss_penalty: float = DEFAULT_CHILD_MISS_PENALTY
    target_chunk_latency_seconds: float = DEFAULT_TARGET_CHUNK_LATENCY_SECONDS
    latency_feedback_min: float = DEFAULT_LATENCY_FEEDBACK_MIN
    latency_feedback_max: float = DEFAULT_LATENCY_FEEDBACK_MAX
    feedback_ewma_alpha: float = DEFAULT_FEEDBACK_EWMA_ALPHA
    runtime_feedback_enabled: bool = True
    node_affinity_enabled: bool = True
    node_affinity_bonus: float = DEFAULT_NODE_AFFINITY_BONUS
    node_migration_penalty: float = DEFAULT_NODE_MIGRATION_PENALTY
    max_parallel_leases_per_node: int = DEFAULT_MAX_PARALLEL_LEASES_PER_NODE
    parallel_latency_factor: float = DEFAULT_PARALLEL_LATENCY_FACTOR
    lease_strategy: LeaseStrategyName = LeaseStrategyName.RANGE
    target_chunk_probability_mass: float = DEFAULT_TARGET_CHUNK_PROBABILITY_MASS
    rank_window_size: int = DEFAULT_RANK_WINDOW_SIZE
    rank_window_frontier_multiplier: float = DEFAULT_RANK_WINDOW_FRONTIER_MULTIPLIER
    tail_stealing_enabled: bool = True
    tail_steal_min_gap: int = DEFAULT_TAIL_STEAL_MIN_GAP
    tail_steal_pending_limit_multiplier: float = DEFAULT_TAIL_STEAL_PENDING_LIMIT_MULTIPLIER
    tail_steal_score_threshold: float = DEFAULT_TAIL_STEAL_SCORE_THRESHOLD

    def __post_init__(self) -> None:
        object.__setattr__(self, "policy", ChunkSizePolicy(self.policy))
        object.__setattr__(self, "lease_strategy", LeaseStrategyName(self.lease_strategy))
        if self.fixed_chunk_size <= 0:
            raise ValueError("fixed_chunk_size must be positive")
        if self.min_chunk_size <= 0:
            raise ValueError("min_chunk_size must be positive")
        if self.max_chunk_size < self.min_chunk_size:
            raise ValueError("max_chunk_size cannot be smaller than min_chunk_size")
        if self.entropy_lambda < 0.0:
            raise ValueError("entropy_lambda cannot be negative")
        if self.priority_donation_weight < 0.0:
            raise ValueError("priority_donation_weight cannot be negative")
        if self.child_miss_penalty < 0.0:
            raise ValueError("child_miss_penalty cannot be negative")
        if self.target_chunk_latency_seconds <= 0.0:
            raise ValueError("target_chunk_latency_seconds must be positive")
        if self.latency_feedback_min <= 0.0:
            raise ValueError("latency_feedback_min must be positive")
        if self.latency_feedback_max < self.latency_feedback_min:
            raise ValueError("latency_feedback_max cannot be smaller than latency_feedback_min")
        if not 0.0 < self.feedback_ewma_alpha <= 1.0:
            raise ValueError("feedback_ewma_alpha must be in (0, 1]")
        if self.node_affinity_bonus < 0.0:
            raise ValueError("node_affinity_bonus cannot be negative")
        if self.node_migration_penalty < 0.0:
            raise ValueError("node_migration_penalty cannot be negative")
        if self.max_parallel_leases_per_node <= 0:
            raise ValueError("max_parallel_leases_per_node must be positive")
        if self.parallel_latency_factor <= 0.0:
            raise ValueError("parallel_latency_factor must be positive")
        if self.target_chunk_probability_mass < 0.0:
            raise ValueError("target_chunk_probability_mass cannot be negative")
        if self.rank_window_size < 0:
            raise ValueError("rank_window_size cannot be negative")
        if self.rank_window_frontier_multiplier < 0.0:
            raise ValueError("rank_window_frontier_multiplier cannot be negative")
        if self.tail_steal_min_gap <= 0:
            raise ValueError("tail_steal_min_gap must be positive")
        if self.tail_steal_pending_limit_multiplier < 0.0:
            raise ValueError("tail_steal_pending_limit_multiplier cannot be negative")
        if self.tail_steal_score_threshold < 0.0:
            raise ValueError("tail_steal_score_threshold cannot be negative")

    def chunk_size(self, *, demand_gap: int, entropy: float) -> int:
        if demand_gap <= 0:
            return 0
        if entropy < 0.0:
            raise ValueError("entropy cannot be negative")

        if self.policy == ChunkSizePolicy.CQDAG_ADAPTIVE:
            raw = _cqdag_adaptive_chunk_size(
                demand_gap=demand_gap,
                entropy=entropy,
                entropy_lambda=self.entropy_lambda,
            )
        elif self.policy == ChunkSizePolicy.FIXED:
            raw = self.fixed_chunk_size
        elif self.policy == ChunkSizePolicy.GAP_ADAPTIVE:
            raw = ceil(sqrt(demand_gap))
        elif self.policy == ChunkSizePolicy.ENTROPY_ADAPTIVE:
            raw = ceil(sqrt(demand_gap) / (1.0 + self.entropy_lambda * entropy))
        else:  # pragma: no cover - defensive for future enum expansion
            raise ValueError(f"unsupported chunk size policy: {self.policy}")

        return min(demand_gap, _clamp(raw, self.min_chunk_size, self.max_chunk_size))

    def chunk_size_for_state(self, state: NodeRuntimeState) -> int:
        base = self.chunk_size(demand_gap=state.demand_gap, entropy=state.entropy)
        if base <= 0 or not self.runtime_feedback_enabled or state.feedback_count == 0:
            return base

        adjusted = float(base)
        adjusted *= 1.0 / (1.0 + self.child_miss_penalty * state.child_miss_rate)
        if state.chunk_latency_ewma > 0.0:
            adjusted *= _clamp_float(
                self.target_chunk_latency_seconds / state.chunk_latency_ewma,
                self.latency_feedback_min,
                self.latency_feedback_max,
            )

        raw = ceil(adjusted)
        return min(state.demand_gap, _clamp(raw, self.min_chunk_size, self.max_chunk_size))


@dataclass(frozen=True, slots=True)
class ScheduleStats:
    scheduled_items: int = 0
    scheduled_records: int = 0
    lease_denials: int = 0
    affinity_hits: int = 0
    affinity_misses: int = 0
    parallel_items: int = 0
    tail_steal_attempts: int = 0
    tail_steals: int = 0
    tail_steal_denials: int = 0
    rank_window_waits: int = 0
    rank_window_forced_items: int = 0
    rank_window_peak_outstanding_records: int = 0


class RangeLeaseStrategy:
    name = LeaseStrategyName.RANGE

    def score(self, scheduler: "PriorityCostScheduler", state: NodeRuntimeState) -> float:
        return scheduler._range_score(state)

    def chunk_size(
        self,
        scheduler: "PriorityCostScheduler",
        state: NodeRuntimeState,
        gap: int,
    ) -> int:
        return scheduler._range_chunk_size_for_gap(state, gap)

    def estimated_mass(self, state: NodeRuntimeState, chunk_size: int) -> float:
        return chunk_size * state.effective_mass_density

    def mass_budget(self, scheduler: "PriorityCostScheduler", state: NodeRuntimeState) -> float:
        return 0.0


class ProbabilityMassLeaseStrategy(RangeLeaseStrategy):
    name = LeaseStrategyName.PROBABILITY_MASS

    def score(self, scheduler: "PriorityCostScheduler", state: NodeRuntimeState) -> float:
        if state.demand_gap <= 0 or state.urgency <= 0.0:
            return 0.0
        density = state.effective_mass_density
        if density <= 0.0:
            return 0.0
        donated_multiplier = 1.0
        if state.donated_priority > 0.0:
            base_priority = max(state.priority, MIN_EFFECTIVE_COST)
            donated_multiplier += (
                scheduler.config.priority_donation_weight
                * state.donated_priority
                / base_priority
            )
        return (
            state.demand_gap
            * density
            * state.urgency
            * donated_multiplier
            / scheduler._effective_cost(state)
        )

    def chunk_size(
        self,
        scheduler: "PriorityCostScheduler",
        state: NodeRuntimeState,
        gap: int,
    ) -> int:
        target_mass = scheduler.config.target_chunk_probability_mass
        density = state.effective_mass_density
        if target_mass <= 0.0 or density <= 0.0:
            return scheduler._range_chunk_size_for_gap(state, gap)
        raw = ceil(target_mass / density)
        return min(
            gap,
            _clamp(
                raw,
                scheduler.config.min_chunk_size,
                scheduler.config.max_chunk_size,
            ),
        )

    def mass_budget(self, scheduler: "PriorityCostScheduler", state: NodeRuntimeState) -> float:
        return scheduler.config.target_chunk_probability_mass


class RankWindowProbabilityMassLeaseStrategy(ProbabilityMassLeaseStrategy):
    name = LeaseStrategyName.RANK_WINDOW_PROBABILITY_MASS

    def score(self, scheduler: "PriorityCostScheduler", state: NodeRuntimeState) -> float:
        base = super().score(scheduler, state)
        if base <= 0.0:
            return 0.0
        if state.is_frontier_blocked:
            return base * (1.0 + scheduler.config.rank_window_frontier_multiplier)
        return base


def _lease_strategy_for(name: LeaseStrategyName) -> RangeLeaseStrategy:
    if name == LeaseStrategyName.RANGE:
        return RangeLeaseStrategy()
    if name == LeaseStrategyName.PROBABILITY_MASS:
        return ProbabilityMassLeaseStrategy()
    if name == LeaseStrategyName.RANK_WINDOW_PROBABILITY_MASS:
        return RankWindowProbabilityMassLeaseStrategy()
    raise ValueError(f"unsupported lease strategy: {name}")


class PriorityCostScheduler:
    def __init__(
        self,
        *,
        states: NodeStateTable,
        chunk_store: InMemoryChunkStore,
        leases: LeaseTable,
        config: SchedulerConfig | None = None,
    ) -> None:
        self.states = states
        self.chunk_store = chunk_store
        self.leases = leases
        self.config = SchedulerConfig() if config is None else config
        self._scheduled_items = 0
        self._scheduled_records = 0
        self._lease_denials = 0
        self._affinity_hits = 0
        self._affinity_misses = 0
        self._parallel_items = 0
        self._tail_steal_attempts = 0
        self._tail_steals = 0
        self._tail_steal_denials = 0
        self._rank_window_waits = 0
        self._rank_window_forced_items = 0
        self._rank_window_peak_outstanding_records = 0
        self._last_worker_by_node: dict[NodeId, WorkerId] = {}
        self.lease_strategy = _lease_strategy_for(self.config.lease_strategy)

    @property
    def stats(self) -> ScheduleStats:
        return ScheduleStats(
            scheduled_items=self._scheduled_items,
            scheduled_records=self._scheduled_records,
            lease_denials=self._lease_denials,
            affinity_hits=self._affinity_hits,
            affinity_misses=self._affinity_misses,
            parallel_items=self._parallel_items,
            tail_steal_attempts=self._tail_steal_attempts,
            tail_steals=self._tail_steals,
            tail_steal_denials=self._tail_steal_denials,
            rank_window_waits=self._rank_window_waits,
            rank_window_forced_items=self._rank_window_forced_items,
            rank_window_peak_outstanding_records=self._rank_window_peak_outstanding_records,
        )

    def schedule(
        self,
        worker_id: WorkerId,
        *,
        now: float | None = None,
        max_chunk_size: int | None = None,
    ) -> WorkItem | None:
        effective_max_chunk_size = self._effective_max_chunk_size(max_chunk_size)
        candidates = self._demand_candidates(worker_id)
        rank_window_outstanding = self._rank_window_outstanding_records()
        self._rank_window_peak_outstanding_records = max(
            self._rank_window_peak_outstanding_records,
            rank_window_outstanding,
        )
        for state in candidates:
            start = max(state.ready_end, state.scheduled_end)
            gap = max(0, state.effective_target_end - start)
            if gap <= 0:
                continue
            active_leases = self.leases.active_count(state.node_id, now=now)
            extends_active_tail = active_leases > 0 and start > state.ready_end
            if active_leases >= self.config.max_parallel_leases_per_node:
                self._lease_denials += 1
                continue
            if extends_active_tail:
                if not self.config.tail_stealing_enabled:
                    self._lease_denials += 1
                    continue
                self._tail_steal_attempts += 1
                if not self._can_tail_steal_state(
                    state,
                    gap=gap,
                    active_leases=active_leases,
                    pending_tail=start - state.ready_end,
                    max_chunk_size=effective_max_chunk_size,
                ):
                    self._lease_denials += 1
                    self._tail_steal_denials += 1
                    continue
            window_gap = self._rank_window_gap(
                state,
                gap,
                outstanding_records=rank_window_outstanding,
            )
            if window_gap <= 0:
                self._lease_denials += 1
                self._rank_window_waits += 1
                continue
            gap = min(gap, window_gap)
            chunk_size = min(
                effective_max_chunk_size,
                self._chunk_size_for_gap(state, gap),
            )
            if chunk_size <= 0:
                continue

            try:
                lease = self.leases.acquire(
                    state.node_id,
                    worker_id,
                    start=start,
                    end=start + chunk_size,
                    now=now,
                    allow_concurrent=active_leases > 0,
                )
            except LeaseDeniedError:
                self._lease_denials += 1
                continue

            item = WorkItem(
                node_id=state.node_id,
                start=start,
                end=start + chunk_size,
                worker_id=worker_id,
                epoch=lease.epoch,
                reclaim_before=state.ready_end,
                estimated_mass=self.lease_strategy.estimated_mass(state, chunk_size),
                mass_budget=self.lease_strategy.mass_budget(self, state),
            )
            state.reserve_until(item.end)
            if active_leases > 0:
                self._parallel_items += 1
            if extends_active_tail:
                self._tail_steals += 1
            self._record_affinity_assignment(state.node_id, worker_id)
            self._scheduled_items += 1
            self._scheduled_records += item.size
            self._rank_window_peak_outstanding_records = max(
                self._rank_window_peak_outstanding_records,
                rank_window_outstanding + item.size,
            )
            return item
        return None

    def _demand_candidates(self, worker_id: WorkerId) -> tuple[NodeRuntimeState, ...]:
        active_states = tuple(self.states.active_demands())
        for state in active_states:
            ready_end = self.chunk_store.ready_end(state.node_id)
            state.update_ready_end(ready_end)
            state.reset_scheduled_end(
                self.leases.contiguous_reserved_end(state.node_id, ready_end),
            )
        self.states.apply_priority_donations()
        return tuple(
            sorted(
                active_states,
                key=lambda state: (
                    -self._schedulable_score_for_worker(state, worker_id),
                    -self._schedulable_gap(state),
                    -state.demand_gap,
                    -state.urgency,
                    str(state.node_id),
                ),
            )
        )

    def _schedulable_gap(self, state: NodeRuntimeState) -> int:
        start = max(state.ready_end, state.scheduled_end)
        return max(0, state.effective_target_end - start)

    def _schedulable_score_for_worker(
        self,
        state: NodeRuntimeState,
        worker_id: WorkerId,
    ) -> float:
        schedulable_gap = self._schedulable_gap(state)
        if schedulable_gap <= 0 or state.demand_gap <= 0:
            return 0.0
        return self.score_for_worker(state, worker_id) * (
            schedulable_gap / state.demand_gap
        )

    def _rank_window_gap(
        self,
        state: NodeRuntimeState,
        gap: int,
        *,
        outstanding_records: int,
    ) -> int:
        if self.config.rank_window_size <= 0:
            return gap
        remaining = self.config.rank_window_size - outstanding_records
        if remaining > 0:
            return min(gap, remaining)
        if state.is_frontier_blocked:
            self._rank_window_forced_items += 1
            return 1
        return 0

    def _rank_window_outstanding_records(self) -> int:
        if self.config.rank_window_size <= 0:
            return 0
        outstanding = 0
        for state in self.states.values():
            frontier = state.frontier_start
            ready_end = self.chunk_store.ready_end(state.node_id)
            reserved_end = self.leases.contiguous_reserved_end(state.node_id, ready_end)
            scheduled_end = max(state.scheduled_end, reserved_end, ready_end)
            outstanding += max(0, scheduled_end - frontier)
        resident = self.chunk_store.stats().record_count
        return max(outstanding, resident)

    def score_for_worker(self, state: NodeRuntimeState, worker_id: WorkerId) -> float:
        return (
            self.score(state)
            * self._affinity_multiplier(state, worker_id)
            / self._migration_penalty_multiplier(state, worker_id)
        )

    def score(self, state: NodeRuntimeState) -> float:
        return self.lease_strategy.score(self, state)

    def _range_score(self, state: NodeRuntimeState) -> float:
        if state.demand_gap <= 0:
            return 0.0
        priority = state.priority + self.config.priority_donation_weight * state.donated_priority
        if priority <= 0.0 or state.urgency <= 0.0:
            return 0.0
        return state.demand_gap * state.urgency * priority / self._effective_cost(state)

    def _affinity_multiplier(self, state: NodeRuntimeState, worker_id: WorkerId) -> float:
        if not self.config.node_affinity_enabled:
            return 1.0
        if self._last_worker_by_node.get(state.node_id) != worker_id:
            return 1.0
        return 1.0 + self.config.node_affinity_bonus

    def _migration_penalty_multiplier(
        self,
        state: NodeRuntimeState,
        worker_id: WorkerId,
    ) -> float:
        previous_worker = self._last_worker_by_node.get(state.node_id)
        if previous_worker is None or previous_worker == worker_id:
            return 1.0
        return 1.0 + self.config.node_migration_penalty

    def _record_affinity_assignment(self, node_id: NodeId, worker_id: WorkerId) -> None:
        previous_worker = self._last_worker_by_node.get(node_id)
        if self.config.node_affinity_enabled and previous_worker is not None:
            if previous_worker == worker_id:
                self._affinity_hits += 1
            else:
                self._affinity_misses += 1
        self._last_worker_by_node[node_id] = worker_id

    def _effective_cost(self, state: NodeRuntimeState) -> float:
        cost = max(state.estimated_cost, MIN_EFFECTIVE_COST)
        if not self.config.runtime_feedback_enabled or state.feedback_count == 0:
            return cost

        cost *= 1.0 + self.config.child_miss_penalty * state.child_miss_rate
        if state.chunk_latency_ewma > self.config.target_chunk_latency_seconds:
            cost *= state.chunk_latency_ewma / self.config.target_chunk_latency_seconds
        return max(cost, MIN_EFFECTIVE_COST)

    def _chunk_size_for_gap(self, state: NodeRuntimeState, gap: int) -> int:
        return self.lease_strategy.chunk_size(self, state, gap)

    def _effective_max_chunk_size(self, max_chunk_size: int | None) -> int:
        if max_chunk_size is None:
            return self.config.max_chunk_size
        if max_chunk_size <= 0:
            raise ValueError("max_chunk_size must be positive")
        return _clamp(max_chunk_size, self.config.min_chunk_size, self.config.max_chunk_size)

    def _range_chunk_size_for_gap(self, state: NodeRuntimeState, gap: int) -> int:
        if gap <= 0:
            return 0
        base = self.config.chunk_size(demand_gap=gap, entropy=state.entropy)
        if base <= 0 or not self.config.runtime_feedback_enabled or state.feedback_count == 0:
            return base

        adjusted = float(base)
        adjusted *= 1.0 / (1.0 + self.config.child_miss_penalty * state.child_miss_rate)
        if state.chunk_latency_ewma > 0.0:
            adjusted *= _clamp_float(
                self.config.target_chunk_latency_seconds / state.chunk_latency_ewma,
                self.config.latency_feedback_min,
                self.config.latency_feedback_max,
            )
        raw = ceil(adjusted)
        return min(gap, _clamp(raw, self.config.min_chunk_size, self.config.max_chunk_size))

    def _can_parallelize_state(self, state: NodeRuntimeState) -> bool:
        if self.config.max_parallel_leases_per_node <= 1:
            return False
        if not self.config.runtime_feedback_enabled:
            return False
        return True

    def _can_tail_steal_state(
        self,
        state: NodeRuntimeState,
        *,
        gap: int,
        active_leases: int,
        pending_tail: int,
        max_chunk_size: int,
    ) -> bool:
        if not self.config.tail_stealing_enabled:
            return False
        if not self._can_parallelize_state(state):
            return False
        if gap < self.config.tail_steal_min_gap:
            return False
        pending_limit = (
            max_chunk_size * self.config.tail_steal_pending_limit_multiplier
        )
        if pending_tail > pending_limit:
            return False
        return (
            self._tail_steal_score(
                state,
                gap=gap,
                active_leases=active_leases,
                pending_tail=pending_tail,
            )
            >= self.config.tail_steal_score_threshold
        )

    def _tail_steal_score(
        self,
        state: NodeRuntimeState,
        *,
        gap: int,
        active_leases: int,
        pending_tail: int,
    ) -> float:
        benefit = gap * state.effective_mass_density * max(state.urgency, 0.0)
        if benefit <= 0.0:
            return 0.0
        pending_factor = 1.0 + pending_tail / max(1.0, float(self.config.max_chunk_size))
        lease_factor = 1.0 + active_leases
        risk = self._effective_cost(state) * pending_factor * lease_factor
        return benefit / max(risk, MIN_EFFECTIVE_COST)


class GapOnlyScheduler(PriorityCostScheduler):
    """Backward-compatible name for the priority/cost aware scheduler."""


def _clamp(value: int, lower: int, upper: int) -> int:
    return min(max(value, lower), upper)


def _clamp_float(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


def _cqdag_adaptive_chunk_size(
    *,
    demand_gap: int,
    entropy: float,
    entropy_lambda: float,
) -> int:
    base = sqrt(demand_gap)
    if entropy > 0.0:
        base /= 1.0 + entropy_lambda * entropy
    return ceil(base)


__all__ = [
    "DEFAULT_CHILD_MISS_PENALTY",
    "DEFAULT_ENTROPY_LAMBDA",
    "DEFAULT_FEEDBACK_EWMA_ALPHA",
    "DEFAULT_FIXED_CHUNK_SIZE",
    "DEFAULT_LATENCY_FEEDBACK_MAX",
    "DEFAULT_LATENCY_FEEDBACK_MIN",
    "DEFAULT_MAX_CHUNK_SIZE",
    "DEFAULT_MAX_PARALLEL_LEASES_PER_NODE",
    "DEFAULT_MIN_CHUNK_SIZE",
    "DEFAULT_NODE_AFFINITY_BONUS",
    "DEFAULT_NODE_MIGRATION_PENALTY",
    "DEFAULT_PARALLEL_LATENCY_FACTOR",
    "DEFAULT_PRIORITY_DONATION_WEIGHT",
    "DEFAULT_RANK_WINDOW_FRONTIER_MULTIPLIER",
    "DEFAULT_RANK_WINDOW_SIZE",
    "DEFAULT_TARGET_CHUNK_PROBABILITY_MASS",
    "DEFAULT_TARGET_CHUNK_LATENCY_SECONDS",
    "DEFAULT_TAIL_STEAL_MIN_GAP",
    "DEFAULT_TAIL_STEAL_PENDING_LIMIT_MULTIPLIER",
    "DEFAULT_TAIL_STEAL_SCORE_THRESHOLD",
    "GapOnlyScheduler",
    "MIN_EFFECTIVE_COST",
    "ProbabilityMassLeaseStrategy",
    "PriorityCostScheduler",
    "RankWindowProbabilityMassLeaseStrategy",
    "RangeLeaseStrategy",
    "ScheduleStats",
    "SchedulerConfig",
]
