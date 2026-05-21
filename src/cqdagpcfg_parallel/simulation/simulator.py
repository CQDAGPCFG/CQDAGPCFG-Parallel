from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.protocol import (
    ChunkSizePolicy,
    InMemoryChunkStore,
    LeaseTable,
    NodeId,
    NodeStateTable,
    PriorityCostScheduler,
    SchedulerConfig,
    WorkerId,
    stable_digest,
)
from cqdagpcfg_parallel.runtime.worker import LocalProtocolWorker, LocalResultSource

from .merger import GlobalMerger
from .root_shard import RootShard


@dataclass(frozen=True, slots=True)
class ProtocolSimulatorConfig:
    scheduler: SchedulerConfig = SchedulerConfig()
    worker_id: WorkerId = WorkerId("worker-0")
    node_id: NodeId = NodeId("root")
    demand_window: int = 16
    entropy: float = 0.0
    priority: float = 1.0
    estimated_cost: float = 1.0

    def __post_init__(self) -> None:
        if self.demand_window <= 0:
            raise ValueError("demand_window must be positive")
        if self.entropy < 0.0:
            raise ValueError("entropy cannot be negative")
        if self.priority < 0.0:
            raise ValueError("priority cannot be negative")
        if self.estimated_cost <= 0.0:
            raise ValueError("estimated_cost must be positive")


@dataclass(frozen=True, slots=True)
class ProtocolRunStats:
    scheduled_items: int
    scheduled_records: int
    publish_count: int
    duplicate_publish_count: int
    ready_end: int
    resident_records: int = 0
    peak_resident_records: int = 0
    reclaimed_records: int = 0
    affinity_hits: int = 0
    affinity_misses: int = 0


@dataclass(frozen=True, slots=True)
class ProtocolSimulationResult:
    outputs: tuple[GuessRecord, ...]
    digest: str
    stats: ProtocolRunStats

    @property
    def stable_records(self) -> tuple[str, ...]:
        return tuple(record.stable_string() for record in self.outputs)


class SequenceRecordSource:
    def __init__(
        self,
        records: Sequence[GuessRecord],
        *,
        node_id: NodeId = NodeId("root"),
    ) -> None:
        self.records = tuple(records)
        self.node_id = node_id

    def read_range(
        self,
        node_id: NodeId,
        start: int,
        end: int,
    ) -> Sequence[GuessRecord]:
        if node_id != self.node_id:
            raise KeyError(f"unknown sequence source node: {node_id}")
        if start < 0 or end < start:
            raise ValueError("invalid source range")
        return self.records[start:end]


class MappingRecordSource:
    def __init__(self, records_by_node: dict[NodeId, Sequence[GuessRecord]]) -> None:
        if not records_by_node:
            raise ValueError("records_by_node cannot be empty")
        self.records_by_node = {
            node_id: tuple(records) for node_id, records in records_by_node.items()
        }

    def read_range(
        self,
        node_id: NodeId,
        start: int,
        end: int,
    ) -> Sequence[GuessRecord]:
        if start < 0 or end < start:
            raise ValueError("invalid source range")
        try:
            records = self.records_by_node[node_id]
        except KeyError as exc:
            raise KeyError(f"unknown mapping source node: {node_id}") from exc
        return records[start:end]


class SingleProcessProtocolSimulator:
    def __init__(
        self,
        *,
        source: LocalResultSource,
        config: ProtocolSimulatorConfig | None = None,
    ) -> None:
        self.config = ProtocolSimulatorConfig() if config is None else config
        self.chunk_store = InMemoryChunkStore()
        self.states = NodeStateTable()
        self.leases = LeaseTable()
        self.scheduler = PriorityCostScheduler(
            states=self.states,
            chunk_store=self.chunk_store,
            leases=self.leases,
            config=self.config.scheduler,
        )
        self.worker = LocalProtocolWorker(
            source=source,
            chunk_store=self.chunk_store,
            leases=self.leases,
            states=self.states,
        )
        self.shard = RootShard(
            node_id=self.config.node_id,
            chunk_store=self.chunk_store,
            states=self.states,
            demand_window=self.config.demand_window,
            entropy=self.config.entropy,
            priority=self.config.priority,
            estimated_cost=self.config.estimated_cost,
        )
        self.merger = GlobalMerger((self.shard,))

    def run(self, limit: int) -> ProtocolSimulationResult:
        if limit < 0:
            raise ValueError("limit cannot be negative")
        outputs: list[GuessRecord] = []

        while len(outputs) < limit:
            record = self.merger.next_ready()
            if record is not None:
                outputs.append(record)
                self.chunk_store.reclaim_before(self.shard.node_id, self.shard.cursor)
                continue

            item = self.scheduler.schedule(self.config.worker_id)
            if item is None:
                states = self.states.values()
                if states and all(state.exhausted for state in states):
                    break
                raise RuntimeError("no schedulable work for a missing root head")
            self.worker.run(item)

        chunk_stats = self.chunk_store.stats()
        schedule_stats = self.scheduler.stats
        return ProtocolSimulationResult(
            outputs=tuple(outputs),
            digest=stable_digest(outputs),
            stats=ProtocolRunStats(
                scheduled_items=schedule_stats.scheduled_items,
                scheduled_records=schedule_stats.scheduled_records,
                publish_count=chunk_stats.publish_count,
                duplicate_publish_count=chunk_stats.duplicate_publish_count,
                ready_end=self.chunk_store.ready_end(self.config.node_id),
                resident_records=chunk_stats.record_count,
                peak_resident_records=chunk_stats.peak_record_count,
                reclaimed_records=chunk_stats.reclaimed_record_count,
                affinity_hits=schedule_stats.affinity_hits,
                affinity_misses=schedule_stats.affinity_misses,
            ),
        )


def simulate_sequence_protocol(
    records: Sequence[GuessRecord],
    *,
    limit: int,
    policy: ChunkSizePolicy = ChunkSizePolicy.CQDAG_ADAPTIVE,
    demand_window: int = 16,
    fixed_chunk_size: int = 8,
    entropy: float = 0.0,
) -> ProtocolSimulationResult:
    config = ProtocolSimulatorConfig(
        scheduler=SchedulerConfig(policy=policy, fixed_chunk_size=fixed_chunk_size),
        demand_window=demand_window,
        entropy=entropy,
    )
    simulator = SingleProcessProtocolSimulator(
        source=SequenceRecordSource(records, node_id=config.node_id),
        config=config,
    )
    return simulator.run(limit)


__all__ = [
    "ProtocolRunStats",
    "ProtocolSimulationResult",
    "ProtocolSimulatorConfig",
    "MappingRecordSource",
    "SequenceRecordSource",
    "SingleProcessProtocolSimulator",
    "simulate_sequence_protocol",
]
