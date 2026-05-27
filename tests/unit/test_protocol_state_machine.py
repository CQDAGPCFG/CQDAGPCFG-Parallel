from __future__ import annotations

import pytest
from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.protocol import (
    ChunkSizePolicy,
    EnumerationChunk,
    GapOnlyScheduler,
    InMemoryChunkStore,
    LeaseDeniedError,
    LeaseTable,
    LeaseStrategyName,
    NodeId,
    NodeRuntimeState,
    NodeStateTable,
    PriorityCostScheduler,
    SchedulerConfig,
    WorkerId,
    stable_digest,
)
from cqdagpcfg_parallel.simulation import GlobalMerger, RootShard, simulate_sequence_protocol


def _record(index: int, guess: str | None = None) -> GuessRecord:
    return GuessRecord(
        prob=1.0 / (index + 1),
        guess=f"g{index}" if guess is None else guess,
        structure_index=0,
        structure_name="A",
        ranks=(index,),
    )


def _chunk(start: int, records: list[GuessRecord]) -> EnumerationChunk:
    return EnumerationChunk.from_records(
        node_id=NodeId("root"),
        start=start,
        records=records,
        worker_id=WorkerId("worker"),
        epoch=1,
    )


def test_chunk_store_publishes_contiguous_prefix_and_buffers_gaps() -> None:
    store = InMemoryChunkStore()
    store.publish(_chunk(0, [_record(0), _record(1)]))

    assert store.ready_end(NodeId("root")) == 2
    assert store.read(NodeId("root"), 1).guess == "g1"
    assert store.read(NodeId("root"), 2) is None

    assert store.publish(_chunk(4, [_record(4)])) == 2
    assert store.ready_end(NodeId("root")) == 2
    assert store.publish(_chunk(2, [_record(2), _record(3)])) == 5
    assert store.ready_end(NodeId("root")) == 5
    assert store.read(NodeId("root"), 4).guess == "g4"


def test_chunk_store_allows_idempotent_duplicate_publish() -> None:
    store = InMemoryChunkStore()
    chunk = _chunk(0, [_record(0), _record(1)])

    assert store.publish(chunk) == 2
    assert store.publish(chunk) == 2
    assert store.stats().duplicate_publish_count == 1


def test_chunk_store_reclaims_consumed_prefix_without_moving_ready_end() -> None:
    store = InMemoryChunkStore()
    store.publish(_chunk(0, [_record(index) for index in range(5)]))

    assert store.reclaim_before(NodeId("root"), 3) == 3
    assert store.base_offset(NodeId("root")) == 3
    assert store.ready_end(NodeId("root")) == 5
    assert store.read(NodeId("root"), 0) is None
    assert store.read(NodeId("root"), 3).guess == "g3"
    assert [record.guess for record in store.records(NodeId("root"))] == ["g3", "g4"]
    assert store.stats().record_count == 2
    assert store.stats().peak_record_count == 5
    assert store.stats().reclaimed_record_count == 3

    assert store.publish(_chunk(5, [_record(5)])) == 6
    assert store.ready_end(NodeId("root")) == 6


def test_lease_table_enforces_single_active_writer_and_epoch() -> None:
    leases = LeaseTable(default_ttl_seconds=1.0)
    lease = leases.acquire(NodeId("root"), WorkerId("worker-a"), now=0.0)

    with pytest.raises(LeaseDeniedError):
        leases.acquire(NodeId("root"), WorkerId("worker-b"), now=0.5)

    assert leases.validate(
        node_id=NodeId("root"),
        worker_id=WorkerId("worker-a"),
        epoch=lease.epoch,
        now=0.5,
    )
    assert not leases.validate(
        node_id=NodeId("root"),
        worker_id=WorkerId("worker-a"),
        epoch=lease.epoch,
        now=1.5,
    )

    renewed = leases.acquire(NodeId("root"), WorkerId("worker-b"), now=1.5)
    assert renewed.epoch == lease.epoch + 1


def test_lease_table_can_hold_concurrent_range_leases() -> None:
    leases = LeaseTable(default_ttl_seconds=1.0)
    first = leases.acquire(
        NodeId("root"),
        WorkerId("worker-a"),
        start=0,
        end=4,
        now=0.0,
    )
    with pytest.raises(LeaseDeniedError):
        leases.acquire(
            NodeId("root"),
            WorkerId("worker-overlap"),
            start=2,
            end=6,
            now=0.05,
            allow_concurrent=True,
        )
    second = leases.acquire(
        NodeId("root"),
        WorkerId("worker-b"),
        start=4,
        end=8,
        now=0.1,
        allow_concurrent=True,
    )

    assert first.epoch != second.epoch
    assert leases.active_count(NodeId("root"), now=0.2) == 2
    assert leases.contiguous_reserved_end(NodeId("root"), 0, now=0.2) == 8
    assert leases.validate(
        node_id=NodeId("root"),
        worker_id=WorkerId("worker-a"),
        epoch=first.epoch,
        start=0,
        end=4,
        now=0.2,
    )
    assert leases.validate(
        node_id=NodeId("root"),
        worker_id=WorkerId("worker-b"),
        epoch=second.epoch,
        start=4,
        end=8,
        now=0.2,
    )


def test_scheduler_chunk_size_policies() -> None:
    assert (
        SchedulerConfig().chunk_size(
            demand_gap=64,
            entropy=0.0,
        )
        == 8
    )
    assert (
        SchedulerConfig().chunk_size(
            demand_gap=64,
            entropy=4.0,
        )
        == 4
    )
    assert (
        SchedulerConfig(policy=ChunkSizePolicy.FIXED, fixed_chunk_size=8).chunk_size(
            demand_gap=64,
            entropy=0.0,
        )
        == 8
    )
    assert (
        SchedulerConfig(policy=ChunkSizePolicy.GAP_ADAPTIVE).chunk_size(
            demand_gap=64,
            entropy=0.0,
        )
        == 8
    )
    assert (
        SchedulerConfig(policy=ChunkSizePolicy.ENTROPY_ADAPTIVE).chunk_size(
            demand_gap=64,
            entropy=4.0,
        )
        == 4
    )


def test_scheduler_prefers_high_priority_structure() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    leases = LeaseTable()
    low_priority = NodeId("low-priority")
    high_priority = NodeId("high-priority")
    states.register_demand(low_priority, 10, priority=1.0)
    states.register_demand(high_priority, 5, priority=5.0)

    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=leases,
        config=SchedulerConfig(fixed_chunk_size=2),
    )

    item = scheduler.schedule(WorkerId("worker"))

    assert item is not None
    assert item.node_id == high_priority


def test_scheduler_penalizes_expensive_node() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    leases = LeaseTable()
    cheap = NodeId("cheap")
    expensive = NodeId("expensive")
    states.register_demand(cheap, 5, estimated_cost=1.0)
    states.register_demand(expensive, 10, estimated_cost=10.0)

    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=leases,
        config=SchedulerConfig(fixed_chunk_size=2),
    )

    item = scheduler.schedule(WorkerId("worker"))

    assert item is not None
    assert item.node_id == cheap


def test_probability_mass_strategy_prefers_high_mass_node_over_larger_gap() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    low_mass = NodeId("wide-low-mass")
    high_mass = NodeId("narrow-high-mass")
    states.register_demand(low_mass, 100, priority=1.0)
    states.register_demand(high_mass, 2, priority=1.0)
    states.record_runtime_feedback(
        low_mass,
        chunk_latency_seconds=0.001,
        records_requested=100,
        records_produced=100,
        ewma_alpha=1.0,
        chunk_probability_mass=0.1,
    )
    states.record_runtime_feedback(
        high_mass,
        chunk_latency_seconds=0.001,
        records_requested=2,
        records_produced=2,
        ewma_alpha=1.0,
        chunk_probability_mass=0.8,
    )

    range_scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=LeaseTable(),
        config=SchedulerConfig(fixed_chunk_size=2),
    )
    mass_scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=LeaseTable(),
        config=SchedulerConfig(
            fixed_chunk_size=2,
            lease_strategy=LeaseStrategyName.PROBABILITY_MASS,
        ),
    )

    assert range_scheduler.schedule(WorkerId("range-worker")).node_id == low_mass
    assert mass_scheduler.schedule(WorkerId("mass-worker")).node_id == high_mass


def test_probability_mass_strategy_uses_mass_budget_for_chunk_size() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    node_id = NodeId("mass-budgeted")
    states.register_demand(node_id, 100, priority=1.0)
    states.record_runtime_feedback(
        node_id,
        chunk_latency_seconds=0.001,
        records_requested=10,
        records_produced=10,
        ewma_alpha=1.0,
        chunk_probability_mass=2.0,
    )
    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=LeaseTable(),
        config=SchedulerConfig(
            fixed_chunk_size=32,
            min_chunk_size=1,
            max_chunk_size=64,
            lease_strategy=LeaseStrategyName.PROBABILITY_MASS,
            target_chunk_probability_mass=1.0,
        ),
    )

    item = scheduler.schedule(WorkerId("worker"))

    assert item is not None
    assert item.size == 5
    assert item.mass_budget == 1.0
    assert item.estimated_mass == pytest.approx(1.0)


def test_scheduler_applies_worker_specific_chunk_cap() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    node_id = NodeId("memory-capped")
    states.register_demand(node_id, 100, priority=1.0)
    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=LeaseTable(),
        config=SchedulerConfig(
            policy=ChunkSizePolicy.FIXED,
            fixed_chunk_size=64,
            min_chunk_size=1,
            max_chunk_size=64,
        ),
    )

    item = scheduler.schedule(WorkerId("small-worker"), max_chunk_size=7)

    assert item is not None
    assert item.size == 7


def test_merger_skips_missing_shard_when_upper_bound_cannot_beat_ready_head() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    high = RootShard(
        node_id=NodeId("structure:0:high"),
        chunk_store=store,
        states=states,
        priority=0.9,
    )
    low = RootShard(
        node_id=NodeId("structure:1:low"),
        chunk_store=store,
        states=states,
        priority=0.1,
    )
    store.publish(
        EnumerationChunk.from_records(
            node_id=high.node_id,
            start=0,
            records=[
                GuessRecord(
                    prob=0.5,
                    guess="high",
                    structure_index=0,
                    structure_name="high",
                    ranks=(0,),
                ),
            ],
            worker_id=WorkerId("publisher"),
            epoch=1,
        ),
    )

    record = GlobalMerger((high, low)).next_ready()

    assert record is not None
    assert record.guess == "high"
    assert low.cursor == 0
    assert states.get(low.node_id).demand_gap == 0


def test_merger_can_disable_lazy_shard_activation() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    high = RootShard(
        node_id=NodeId("structure:0:high"),
        chunk_store=store,
        states=states,
        priority=0.9,
    )
    low = RootShard(
        node_id=NodeId("structure:1:low"),
        chunk_store=store,
        states=states,
        priority=0.1,
    )
    store.publish(
        EnumerationChunk.from_records(
            node_id=high.node_id,
            start=0,
            records=[
                GuessRecord(
                    prob=0.5,
                    guess="high",
                    structure_index=0,
                    structure_name="high",
                    ranks=(0,),
                ),
            ],
            worker_id=WorkerId("publisher"),
            epoch=1,
        ),
    )

    record = GlobalMerger((high, low), lazy_shard_activation=False).next_ready()

    assert record is None
    assert high.cursor == 0
    assert states.get(low.node_id).demand_gap == 1


def test_merger_blocks_missing_shard_when_upper_bound_can_beat_ready_head() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    ready = RootShard(
        node_id=NodeId("structure:0:ready"),
        chunk_store=store,
        states=states,
        priority=0.5,
    )
    missing = RootShard(
        node_id=NodeId("structure:1:missing"),
        chunk_store=store,
        states=states,
        priority=0.9,
    )
    store.publish(
        EnumerationChunk.from_records(
            node_id=ready.node_id,
            start=0,
            records=[
                GuessRecord(
                    prob=0.5,
                    guess="ready",
                    structure_index=0,
                    structure_name="ready",
                    ranks=(0,),
                ),
            ],
            worker_id=WorkerId("publisher"),
            epoch=1,
        ),
    )

    record = GlobalMerger((ready, missing)).next_ready()

    assert record is None
    assert ready.cursor == 0
    assert states.get(missing.node_id).demand_gap == 1


def test_rank_window_strategy_caps_scheduled_frontier_records() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    node_id = NodeId("windowed")
    states.register_demand(node_id, 100, priority=1.0)
    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=LeaseTable(),
        config=SchedulerConfig(
            policy=ChunkSizePolicy.FIXED,
            fixed_chunk_size=16,
            lease_strategy=LeaseStrategyName.RANK_WINDOW_PROBABILITY_MASS,
            rank_window_size=4,
        ),
    )

    item = scheduler.schedule(WorkerId("worker"))

    assert item is not None
    assert item.size == 4
    assert scheduler.stats.rank_window_peak_outstanding_records == 4


def test_rank_window_strategy_forces_blocking_head_when_window_is_full() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    warm = NodeId("warm")
    blocked = NodeId("blocked")
    store.publish(
        EnumerationChunk.from_records(
            node_id=warm,
            start=0,
            records=[_record(index) for index in range(4)],
            worker_id=WorkerId("publisher"),
            epoch=1,
        ),
    )
    states.update_ready_end(warm, 4)
    states.register_demand(blocked, 10, priority=10.0)
    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=LeaseTable(),
        config=SchedulerConfig(
            policy=ChunkSizePolicy.FIXED,
            fixed_chunk_size=16,
            lease_strategy=LeaseStrategyName.RANK_WINDOW_PROBABILITY_MASS,
            rank_window_size=4,
        ),
    )

    item = scheduler.schedule(WorkerId("worker"))

    assert item is not None
    assert item.node_id == blocked
    assert item.size == 1
    assert scheduler.stats.rank_window_forced_items == 1


def test_rank_window_strategy_waits_for_nonblocking_prefetch_when_window_is_full() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    node_id = NodeId("prefetch")
    store.publish(
        EnumerationChunk.from_records(
            node_id=node_id,
            start=0,
            records=[_record(index) for index in range(4)],
            worker_id=WorkerId("publisher"),
            epoch=1,
        ),
    )
    states.update_ready_end(node_id, 4)
    states.register_demand(node_id, 10, priority=1.0)
    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=LeaseTable(),
        config=SchedulerConfig(
            policy=ChunkSizePolicy.FIXED,
            fixed_chunk_size=16,
            lease_strategy=LeaseStrategyName.RANK_WINDOW_PROBABILITY_MASS,
            rank_window_size=4,
        ),
    )

    item = scheduler.schedule(WorkerId("worker"))

    assert item is None
    assert scheduler.stats.rank_window_waits == 1


def test_scheduler_node_affinity_prefers_warm_worker_when_scores_are_close() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    leases = LeaseTable()
    warm = NodeId("warm")
    slightly_bigger = NodeId("slightly-bigger")
    worker = WorkerId("worker-a")
    states.register_demand(warm, 10)

    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=leases,
        config=SchedulerConfig(fixed_chunk_size=2, node_affinity_bonus=0.5),
    )

    first = scheduler.schedule(worker)
    assert first is not None
    assert first.node_id == warm
    lease = leases.current(first.node_id)
    assert lease is not None
    leases.release(lease)

    states.register_demand(slightly_bigger, 12)
    second = scheduler.schedule(worker)

    assert second is not None
    assert second.node_id == warm
    assert scheduler.stats.affinity_hits == 1
    assert scheduler.stats.affinity_misses == 0


def test_scheduler_node_affinity_is_soft_and_can_be_disabled() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    leases = LeaseTable()
    warm = NodeId("warm")
    bigger = NodeId("bigger")
    worker = WorkerId("worker-a")
    states.register_demand(warm, 10)

    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=leases,
        config=SchedulerConfig(
            fixed_chunk_size=2,
            node_affinity_enabled=False,
            node_affinity_bonus=0.5,
        ),
    )

    first = scheduler.schedule(worker)
    assert first is not None
    assert first.node_id == warm
    lease = leases.current(first.node_id)
    assert lease is not None
    leases.release(lease)

    states.register_demand(bigger, 12)
    second = scheduler.schedule(worker)

    assert second is not None
    assert second.node_id == bigger
    assert scheduler.stats.affinity_hits == 0


def test_scheduler_migration_penalty_discourages_cold_worker_takeover() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    leases = LeaseTable()
    warm = NodeId("warm")
    bigger = NodeId("bigger")
    worker_a = WorkerId("worker-a")
    worker_b = WorkerId("worker-b")
    states.register_demand(warm, 10)

    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=leases,
        config=SchedulerConfig(
            fixed_chunk_size=2,
            node_affinity_bonus=0.0,
            node_migration_penalty=0.5,
        ),
    )

    first = scheduler.schedule(worker_a)
    assert first is not None
    assert first.node_id == warm
    lease = leases.current(first.node_id)
    assert lease is not None
    leases.release(lease)

    states.register_demand(bigger, 8)
    second = scheduler.schedule(worker_b)

    assert second is not None
    assert second.node_id == bigger


def test_node_state_keeps_explicit_scheduling_features() -> None:
    states = NodeStateTable()
    node_id = NodeId("node")

    states.ensure_node(node_id, priority=0.5, estimated_cost=0.5)
    states.update_ready_end(node_id, 0)

    state = states.get(node_id)
    assert state.priority == 0.5
    assert state.estimated_cost == 0.5


def test_node_state_delays_future_exhaustion_until_prefix_is_ready() -> None:
    state = NodeRuntimeState(
        node_id=NodeId("node"),
        ready_end=24,
        scheduled_end=24,
        target_end=40,
    )

    state.mark_exhausted(30)

    assert not state.exhausted
    assert state.demand_gap == 6

    state.update_ready_end(29)
    assert not state.exhausted
    assert state.demand_gap == 1

    state.update_ready_end(30)
    assert state.exhausted
    assert state.demand_gap == 0


def test_scheduler_priority_donation_lifts_blocked_child() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    leases = LeaseTable()
    regular = NodeId("regular-child")
    donated = NodeId("donated-child")
    states.register_demand(regular, 5)
    states.register_demand(donated, 5)
    states.donate_priority(donated, 10.0)

    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=leases,
        config=SchedulerConfig(fixed_chunk_size=2),
    )

    item = scheduler.schedule(WorkerId("worker"))

    assert item is not None
    assert item.node_id == donated


def test_scheduler_splits_hot_node_into_concurrent_ranges() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    leases = LeaseTable()
    node_id = NodeId("hot")
    states.register_demand(node_id, 20)

    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=leases,
        config=SchedulerConfig(
            policy=ChunkSizePolicy.FIXED,
            fixed_chunk_size=4,
            max_parallel_leases_per_node=2,
        ),
    )

    first = scheduler.schedule(WorkerId("worker-a"))
    states.record_runtime_feedback(
        node_id,
        chunk_latency_seconds=0.001,
        records_requested=4,
        records_produced=4,
        ewma_alpha=1.0,
    )
    second = scheduler.schedule(WorkerId("worker-b"))
    third = scheduler.schedule(WorkerId("worker-c"))

    assert first is not None
    assert second is not None
    assert third is None
    assert (first.start, first.end) == (0, 4)
    assert (second.start, second.end) == (4, 12)
    assert scheduler.stats.parallel_items == 1
    assert scheduler.stats.tail_steal_attempts == 1
    assert scheduler.stats.tail_steals == 1


def test_scheduler_denies_tail_steal_when_pending_tail_is_too_large() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    leases = LeaseTable()
    node_id = NodeId("hot")
    states.register_demand(node_id, 100)

    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=leases,
        config=SchedulerConfig(
            policy=ChunkSizePolicy.FIXED,
            fixed_chunk_size=10,
            max_chunk_size=10,
            max_parallel_leases_per_node=2,
            tail_steal_pending_limit_multiplier=0.5,
        ),
    )

    first = scheduler.schedule(WorkerId("worker-a"))
    states.record_runtime_feedback(
        node_id,
        chunk_latency_seconds=0.001,
        records_requested=10,
        records_produced=10,
        ewma_alpha=1.0,
    )
    second = scheduler.schedule(WorkerId("worker-b"))

    assert first is not None
    assert second is None
    assert scheduler.stats.tail_steal_attempts == 1
    assert scheduler.stats.tail_steal_denials == 1
    assert scheduler.stats.tail_steals == 0


def test_scheduler_denies_tail_steal_below_score_threshold() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    leases = LeaseTable()
    node_id = NodeId("low-benefit")
    states.register_demand(node_id, 20, estimated_cost=100.0)

    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=leases,
        config=SchedulerConfig(
            policy=ChunkSizePolicy.FIXED,
            fixed_chunk_size=4,
            max_parallel_leases_per_node=2,
            tail_steal_score_threshold=10.0,
        ),
    )

    first = scheduler.schedule(WorkerId("worker-a"))
    states.record_runtime_feedback(
        node_id,
        chunk_latency_seconds=0.001,
        records_requested=4,
        records_produced=4,
        ewma_alpha=1.0,
        chunk_probability_mass=0.001,
    )
    second = scheduler.schedule(WorkerId("worker-b"))

    assert first is not None
    assert second is None
    assert scheduler.stats.tail_steal_attempts == 1
    assert scheduler.stats.tail_steal_denials == 1


def test_scheduler_applies_dependency_priority_donation() -> None:
    store = InMemoryChunkStore()
    states = NodeStateTable()
    leases = LeaseTable()
    parent = NodeId("blocked-parent")
    child = NodeId("donated-child")
    competing_child = NodeId("competing-child")
    states.register_demand(parent, 10, priority=10.0)
    states.register_demand(child, 1, priority=1.0)
    states.register_demand(competing_child, 2, priority=1.0)
    states.register_dependency(parent, child, donation_weight=1.0)
    leases.acquire(parent, WorkerId("blocked-owner"))

    scheduler = PriorityCostScheduler(
        states=states,
        chunk_store=store,
        leases=leases,
        config=SchedulerConfig(fixed_chunk_size=1),
    )

    item = scheduler.schedule(WorkerId("worker"))

    assert item is not None
    assert item.node_id == child


def test_runtime_feedback_reduces_chunk_size_for_slow_or_missy_node() -> None:
    state = NodeRuntimeState(node_id=NodeId("slow"), target_end=64)
    config = SchedulerConfig(
        policy=ChunkSizePolicy.GAP_ADAPTIVE,
        target_chunk_latency_seconds=0.01,
        latency_feedback_min=0.25,
        latency_feedback_max=2.0,
        child_miss_penalty=1.0,
    )

    state.record_runtime_feedback(
        chunk_latency_seconds=0.1,
        records_requested=8,
        records_produced=4,
        ewma_alpha=1.0,
    )

    assert config.chunk_size_for_state(state) < config.chunk_size(
        demand_gap=state.demand_gap,
        entropy=state.entropy,
    )


def test_gap_only_scheduler_name_remains_compatible() -> None:
    assert issubclass(GapOnlyScheduler, PriorityCostScheduler)


@pytest.mark.parametrize("policy", list(ChunkSizePolicy))
def test_single_process_protocol_preserves_sequence_prefix(policy: ChunkSizePolicy) -> None:
    records = tuple(_record(index) for index in range(32))

    result = simulate_sequence_protocol(
        records,
        limit=20,
        policy=policy,
        demand_window=8,
        fixed_chunk_size=8,
        entropy=4.0,
    )

    assert result.outputs == records[:20]
    assert result.digest == stable_digest(records[:20])
    assert result.stats.publish_count > 0
