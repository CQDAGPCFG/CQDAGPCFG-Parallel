from __future__ import annotations

from dataclasses import dataclass
from math import ceil, sqrt

from .chunk_store import InMemoryChunkStore
from .lease_table import LeaseDeniedError, LeaseTable
from .node_state import NodeRuntimeState, NodeStateTable
from .types import ChunkSizePolicy, NodeId, WorkItem, WorkerId


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

    def __post_init__(self) -> None:
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
        self._last_worker_by_node: dict[NodeId, WorkerId] = {}

    @property
    def stats(self) -> ScheduleStats:
        return ScheduleStats(
            scheduled_items=self._scheduled_items,
            scheduled_records=self._scheduled_records,
            lease_denials=self._lease_denials,
            affinity_hits=self._affinity_hits,
            affinity_misses=self._affinity_misses,
            parallel_items=self._parallel_items,
        )

    def schedule(self, worker_id: WorkerId, *, now: float | None = None) -> WorkItem | None:
        candidates = self._demand_candidates(worker_id)
        for state in candidates:
            start = max(state.ready_end, state.scheduled_end)
            gap = max(0, state.effective_target_end - start)
            if gap <= 0:
                continue
            active_leases = self.leases.active_count(state.node_id, now=now)
            extends_active_tail = active_leases > 0 and start > state.ready_end
            if extends_active_tail and not self._can_parallelize_state(state):
                self._lease_denials += 1
                continue
            if active_leases >= self.config.max_parallel_leases_per_node:
                self._lease_denials += 1
                continue
            chunk_size = self._chunk_size_for_gap(state, gap)
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
            )
            state.reserve_until(item.end)
            if active_leases > 0:
                self._parallel_items += 1
            self._record_affinity_assignment(state.node_id, worker_id)
            self._scheduled_items += 1
            self._scheduled_records += item.size
            return item
        return None

    def _demand_candidates(self, worker_id: WorkerId) -> tuple[NodeRuntimeState, ...]:
        for state in self.states.values():
            ready_end = self.chunk_store.ready_end(state.node_id)
            state.update_ready_end(ready_end)
            state.reset_scheduled_end(
                self.leases.contiguous_reserved_end(state.node_id, ready_end),
            )
        self.states.apply_priority_donations()
        return tuple(
            sorted(
                self.states.active_demands(),
                key=lambda state: (
                    -self.score_for_worker(state, worker_id),
                    -state.demand_gap,
                    -state.urgency,
                    str(state.node_id),
                ),
            )
        )

    def score_for_worker(self, state: NodeRuntimeState, worker_id: WorkerId) -> float:
        return (
            self.score(state)
            * self._affinity_multiplier(state, worker_id)
            / self._migration_penalty_multiplier(state, worker_id)
        )

    def score(self, state: NodeRuntimeState) -> float:
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
        if state.feedback_count == 0:
            return False
        return (
            state.chunk_latency_ewma
            <= self.config.target_chunk_latency_seconds * self.config.parallel_latency_factor
        )


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
    "DEFAULT_TARGET_CHUNK_LATENCY_SECONDS",
    "GapOnlyScheduler",
    "MIN_EFFECTIVE_COST",
    "PriorityCostScheduler",
    "ScheduleStats",
    "SchedulerConfig",
]
