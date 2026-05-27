from __future__ import annotations

from dataclasses import dataclass

from CQDAGPCFG import GuessRecord

from .root_shard import RootShard


@dataclass(slots=True)
class GlobalMerger:
    shards: tuple[RootShard, ...]
    lazy_shard_activation: bool = True

    def __post_init__(self) -> None:
        if not self.shards:
            raise ValueError("global merger requires at least one root shard")

    def next_ready(self) -> GuessRecord | None:
        heads: list[tuple[tuple[float, int, tuple[int, ...]], int, GuessRecord]] = []
        missing_shards: list[RootShard] = []
        for index, shard in enumerate(self.shards):
            head = shard.peek_head()
            if head is None:
                if shard.is_exhausted:
                    continue
                missing_shards.append(shard)
                continue
            heads.append((shard.order_key(head), index, head))

        if not heads:
            self._register_initial_demands(missing_shards)
            return None

        best_key, shard_index, _ = min(heads)
        if not self.lazy_shard_activation and missing_shards:
            for shard in missing_shards:
                shard.register_head_demand()
            return None
        blocked = False
        for shard in missing_shards:
            if shard.best_possible_order_key() <= best_key:
                shard.register_head_demand()
                blocked = True
        if blocked:
            return None
        return self.shards[shard_index].pop_head()

    def _register_initial_demands(self, missing_shards: list[RootShard]) -> None:
        if not missing_shards:
            return
        if not self.lazy_shard_activation:
            for shard in missing_shards:
                shard.register_head_demand()
            return
        best_possible = min(shard.best_possible_order_key() for shard in missing_shards)
        for shard in missing_shards:
            if shard.best_possible_order_key() == best_possible:
                shard.register_head_demand()


__all__ = [
    "GlobalMerger",
]
