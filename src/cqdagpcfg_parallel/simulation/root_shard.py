from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.protocol import InMemoryChunkStore, NodeId, NodeStateTable


RecordOrderKey = tuple[float, int, tuple[int, ...]]


def default_record_order_key(record: GuessRecord) -> RecordOrderKey:
    return record.order_key()


@dataclass(slots=True)
class RootShard:
    node_id: NodeId
    chunk_store: InMemoryChunkStore
    states: NodeStateTable
    demand_window: int = 1
    entropy: float = 0.0
    priority: float = 1.0
    estimated_cost: float = 1.0
    cardinality: int | None = None
    order_key: Callable[[GuessRecord], RecordOrderKey] = default_record_order_key
    cursor: int = 0

    def __post_init__(self) -> None:
        if self.demand_window <= 0:
            raise ValueError("demand_window must be positive")
        if self.entropy < 0.0:
            raise ValueError("entropy cannot be negative")
        if self.priority < 0.0:
            raise ValueError("priority cannot be negative")
        if self.estimated_cost <= 0.0:
            raise ValueError("estimated_cost must be positive")
        if self.cardinality is not None and self.cardinality < 0:
            raise ValueError("cardinality cannot be negative")
        self.chunk_store.ensure_node(self.node_id)
        self.states.ensure_node(
            self.node_id,
            entropy=self.entropy,
            priority=self.priority,
            estimated_cost=self.estimated_cost,
        )

    def head(self) -> GuessRecord | None:
        record = self.peek_head()
        if record is None:
            self.register_head_demand()
        return record

    def peek_head(self) -> GuessRecord | None:
        self.states.update_frontier_start(self.node_id, self.cursor)
        return self.chunk_store.read(self.node_id, self.cursor)

    def register_head_demand(self) -> None:
        if self.is_exhausted:
            return
        self.states.register_demand(
            self.node_id,
            self.cursor + self.demand_window,
            entropy=self.entropy,
            priority=self.priority,
            estimated_cost=self.estimated_cost,
        )

    def pop_head(self) -> GuessRecord | None:
        record = self.head()
        if record is None:
            return None
        self.cursor += 1
        self.states.update_frontier_start(self.node_id, self.cursor)
        return record

    def best_possible_order_key(self) -> RecordOrderKey:
        structure_index = _structure_index_from_node_id(self.node_id)
        return (-self.priority, structure_index, ())

    @property
    def is_exhausted(self) -> bool:
        return self.states.get(self.node_id).exhausted


def _structure_index_from_node_id(node_id: NodeId) -> int:
    parts = str(node_id).split(":", 2)
    if len(parts) >= 2 and parts[0] == "structure":
        try:
            return int(parts[1])
        except ValueError:
            return 0
    return 0


__all__ = [
    "RecordOrderKey",
    "RootShard",
    "default_record_order_key",
]
