from __future__ import annotations

from dataclasses import dataclass

from CQDAGPCFG import GuessRecord

from .root_shard import RootShard


@dataclass(slots=True)
class GlobalMerger:
    shards: tuple[RootShard, ...]

    def __post_init__(self) -> None:
        if not self.shards:
            raise ValueError("global merger requires at least one root shard")

    def next_ready(self) -> GuessRecord | None:
        heads: list[tuple[tuple[float, int, tuple[int, ...]], int, GuessRecord]] = []
        missing = False
        for index, shard in enumerate(self.shards):
            head = shard.head()
            if head is None:
                if shard.is_exhausted:
                    continue
                missing = True
                continue
            heads.append((shard.order_key(head), index, head))

        if missing:
            return None
        if not heads:
            return None

        _, shard_index, _ = min(heads)
        return self.shards[shard_index].pop_head()


__all__ = [
    "GlobalMerger",
]
