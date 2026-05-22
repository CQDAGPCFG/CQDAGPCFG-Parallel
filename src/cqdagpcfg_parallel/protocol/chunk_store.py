from __future__ import annotations

from dataclasses import dataclass

from CQDAGPCFG import GuessRecord

from .types import EnumerationChunk, NodeId, stable_record_string


class ChunkStoreError(RuntimeError):
    pass


class ChunkPublishError(ChunkStoreError):
    pass


@dataclass(frozen=True, slots=True)
class ChunkStoreStats:
    node_count: int = 0
    record_count: int = 0
    peak_record_count: int = 0
    reclaimed_record_count: int = 0
    publish_count: int = 0
    duplicate_publish_count: int = 0


class InMemoryChunkStore:
    """Contiguous prefix store for CQDAG node local result streams."""

    def __init__(self) -> None:
        self._records: dict[NodeId, list[GuessRecord]] = {}
        self._pending: dict[NodeId, dict[int, tuple[GuessRecord, ...]]] = {}
        self._base_offsets: dict[NodeId, int] = {}
        self._peak_record_count = 0
        self._reclaimed_record_count = 0
        self._publish_count = 0
        self._duplicate_publish_count = 0

    def ensure_node(self, node_id: NodeId) -> None:
        self._records.setdefault(node_id, [])
        self._pending.setdefault(node_id, {})
        self._base_offsets.setdefault(node_id, 0)

    def base_offset(self, node_id: NodeId) -> int:
        return self._base_offsets.get(node_id, 0)

    def ready_end(self, node_id: NodeId) -> int:
        return self.base_offset(node_id) + len(self._records.get(node_id, ()))

    def read(self, node_id: NodeId, index: int) -> GuessRecord | None:
        if index < 0:
            raise ValueError("read index cannot be negative")
        records = self._records.get(node_id)
        base_offset = self.base_offset(node_id)
        if records is None or index < base_offset or index >= base_offset + len(records):
            return None
        return records[index - base_offset]

    def read_range(
        self,
        node_id: NodeId,
        start: int,
        end: int,
    ) -> tuple[GuessRecord, ...] | None:
        if start < 0 or end < start:
            raise ValueError("invalid read range")
        records = self._records.get(node_id)
        base_offset = self.base_offset(node_id)
        if records is None or start < base_offset or end > base_offset + len(records):
            return None
        return tuple(records[start - base_offset : end - base_offset])

    def records(
        self,
        node_id: NodeId,
        *,
        start: int = 0,
        end: int | None = None,
    ) -> tuple[GuessRecord, ...]:
        if start < 0:
            raise ValueError("start cannot be negative")
        records = self._records.get(node_id, [])
        base_offset = self.base_offset(node_id)
        effective_start = base_offset if start == 0 and end is None else start
        stop = self.ready_end(node_id) if end is None else end
        if stop < effective_start:
            raise ValueError("end cannot be smaller than start")
        if effective_start < base_offset or stop > base_offset + len(records):
            raise ChunkStoreError("requested range is not fully materialized")
        return tuple(records[effective_start - base_offset : stop - base_offset])

    def publish(self, chunk: EnumerationChunk) -> int:
        records = self._records.setdefault(chunk.node_id, [])
        pending = self._pending.setdefault(chunk.node_id, {})
        base_offset = self._base_offsets.setdefault(chunk.node_id, 0)
        ready_end = base_offset + len(records)

        start = chunk.start
        incoming = chunk.records

        if chunk.end <= base_offset:
            self._duplicate_publish_count += 1
            return ready_end

        if start < base_offset:
            trim = base_offset - start
            incoming = incoming[trim:]
            start = base_offset

        if start > ready_end:
            self._store_pending(
                pending,
                start=start,
                incoming=incoming,
                base_offset=base_offset,
            )
            self._publish_count += 1
            self._update_peak_record_count()
            return ready_end

        end = start + len(incoming)
        overlap = max(0, min(end, ready_end) - start)
        if overlap:
            local_start = start - base_offset
            existing = records[local_start : local_start + overlap]
            incoming_overlap = incoming[:overlap]
            if [_stable(record) for record in existing] != [
                _stable(record) for record in incoming_overlap
            ]:
                raise ChunkPublishError("chunk overlaps existing records with different data")

        if end <= ready_end:
            self._duplicate_publish_count += 1
            return ready_end

        records.extend(incoming[overlap:])
        ready_end = base_offset + len(records)
        while ready_end in pending:
            buffered = pending.pop(ready_end)
            records.extend(buffered)
            ready_end = base_offset + len(records)
        self._publish_count += 1
        self._update_peak_record_count()
        return ready_end

    def reclaim_before(self, node_id: NodeId, index: int) -> int:
        """Drop materialized records before absolute ``index``.

        The store keeps absolute stream indices stable by moving the node's
        base offset forward. Future reads must use absolute indices.
        """
        if index < 0:
            raise ValueError("reclaim index cannot be negative")
        records = self._records.get(node_id)
        if records is None:
            return 0
        base_offset = self.base_offset(node_id)
        ready_end = base_offset + len(records)
        reclaim_end = min(max(index, base_offset), ready_end)
        reclaimed = reclaim_end - base_offset
        if reclaimed <= 0:
            return base_offset
        del records[:reclaimed]
        self._base_offsets[node_id] = reclaim_end
        self._reclaimed_record_count += reclaimed
        return reclaim_end

    def advance_base_offset(self, node_id: NodeId, index: int) -> int:
        """Move a node stream base forward without materialized records.

        Durable tracker recovery uses this when emitted prefixes are known from
        a checkpoint but local chunks were lost with the crashed tracker.
        """
        if index < 0:
            raise ValueError("base offset cannot be negative")
        self.ensure_node(node_id)
        base_offset = self.base_offset(node_id)
        if index < base_offset:
            raise ValueError("base offset cannot move backward")
        records = self._records[node_id]
        pending = self._pending[node_id]
        ready_end = base_offset + len(records)
        if index <= ready_end:
            return self.reclaim_before(node_id, index)
        if records:
            self._reclaimed_record_count += len(records)
            records.clear()
        if pending:
            discarded = sum(len(chunk) for start, chunk in pending.items() if start < index)
            if discarded:
                self._reclaimed_record_count += discarded
            for start in tuple(pending):
                if start < index:
                    del pending[start]
        self._base_offsets[node_id] = index
        return index

    def stats(self) -> ChunkStoreStats:
        return ChunkStoreStats(
            node_count=len(self._records),
            record_count=self._resident_record_count(),
            peak_record_count=self._peak_record_count,
            reclaimed_record_count=self._reclaimed_record_count,
            publish_count=self._publish_count,
            duplicate_publish_count=self._duplicate_publish_count,
        )

    def _update_peak_record_count(self) -> None:
        self._peak_record_count = max(
            self._peak_record_count,
            self._resident_record_count(),
        )

    def _resident_record_count(self) -> int:
        return sum(len(records) for records in self._records.values()) + sum(
            len(chunk)
            for pending_by_start in self._pending.values()
            for chunk in pending_by_start.values()
        )

    def _store_pending(
        self,
        pending: dict[int, tuple[GuessRecord, ...]],
        *,
        start: int,
        incoming: tuple[GuessRecord, ...],
        base_offset: int,
    ) -> None:
        end = start + len(incoming)
        for pending_start, existing in pending.items():
            pending_end = pending_start + len(existing)
            if end <= pending_start or start >= pending_end:
                continue
            overlap_start = max(start, pending_start)
            overlap_end = min(end, pending_end)
            incoming_slice = incoming[overlap_start - start : overlap_end - start]
            existing_slice = existing[
                overlap_start - pending_start : overlap_end - pending_start
            ]
            if [_stable(record) for record in existing_slice] != [
                _stable(record) for record in incoming_slice
            ]:
                raise ChunkPublishError("pending chunk overlaps with different data")
            if start >= pending_start and end <= pending_end:
                self._duplicate_publish_count += 1
                return
        if start < base_offset:
            raise ChunkPublishError("pending chunk starts before base offset")
        pending[start] = incoming


def _stable(record: GuessRecord) -> str:
    return stable_record_string(record)


__all__ = [
    "ChunkPublishError",
    "ChunkStoreError",
    "ChunkStoreStats",
    "InMemoryChunkStore",
]
