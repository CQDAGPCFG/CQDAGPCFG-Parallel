from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ProtocolMetrics:
    scheduled_items: int = 0
    scheduled_records: int = 0
    published_chunks: int = 0
    published_records: int = 0
    read_misses: int = 0
    merger_outputs: int = 0

    def snapshot(self) -> "ProtocolMetrics":
        return ProtocolMetrics(
            scheduled_items=self.scheduled_items,
            scheduled_records=self.scheduled_records,
            published_chunks=self.published_chunks,
            published_records=self.published_records,
            read_misses=self.read_misses,
            merger_outputs=self.merger_outputs,
        )


__all__ = [
    "ProtocolMetrics",
]
