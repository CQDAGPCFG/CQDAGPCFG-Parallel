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
        self.chunk_store.ensure_node(self.node_id)
        self.states.ensure_node(
            self.node_id,
            entropy=self.entropy,
            priority=self.priority,
            estimated_cost=self.estimated_cost,
        )

    def head(self) -> GuessRecord | None:
        record = self.chunk_store.read(self.node_id, self.cursor)
        if record is None:
            if self.is_exhausted:
                return None
            self.states.register_demand(
                self.node_id,
                self.cursor + self.demand_window,
                entropy=self.entropy,
                priority=self.priority,
                estimated_cost=self.estimated_cost,
            )
        return record

    def pop_head(self) -> GuessRecord | None:
        record = self.head()
        if record is None:
            return None
        self.cursor += 1
        return record

    @property
    def is_exhausted(self) -> bool:
        return self.states.get(self.node_id).exhausted


__all__ = [
    "RecordOrderKey",
    "RootShard",
    "default_record_order_key",
]
