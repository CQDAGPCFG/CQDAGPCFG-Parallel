from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from typing import Callable, Protocol, Sequence, runtime_checkable

from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.protocol import (
    EnumerationChunk,
    InMemoryChunkStore,
    LeaseTable,
    NodeId,
    NodeStateTable,
    WorkItem,
)


class LocalResultSource(Protocol):
    def read_range(
        self,
        node_id: NodeId,
        start: int,
        end: int,
    ) -> Sequence[GuessRecord]: ...


class LazyLocalResultSource:
    """Defers expensive source construction until generation is actually needed."""

    def __init__(self, source_factory: Callable[[], LocalResultSource]) -> None:
        self.source_factory = source_factory
        self._source: LocalResultSource | None = None
        self.load_count = 0

    @property
    def loaded_once(self) -> bool:
        return self.load_count > 0

    @property
    def is_loaded(self) -> bool:
        return self._source is not None

    def read_range(
        self,
        node_id: NodeId,
        start: int,
        end: int,
    ) -> Sequence[GuessRecord]:
        return self._require_source().read_range(node_id, start, end)

    def reclaim_before(self, node_id: NodeId, index: int) -> int:
        if self._source is None:
            return 0
        reclaim_before = getattr(self._source, "reclaim_before", None)
        if not callable(reclaim_before):
            return 0
        return int(reclaim_before(node_id, index))

    def stats(self) -> SourceReclaimCounters:
        if self._source is None:
            return SourceReclaimCounters()
        return source_reclaim_counters(self._source)

    def capture_state(self, *args, **kwargs):
        capture_state = getattr(self._require_source(), "capture_state", None)
        if not callable(capture_state):
            raise RuntimeError("source does not support state migration capture")
        return capture_state(*args, **kwargs)

    def restore_state(self, *args, **kwargs) -> None:
        restore_state = getattr(self._require_source(), "restore_state", None)
        if not callable(restore_state):
            raise RuntimeError("source does not support state migration restore")
        restore_state(*args, **kwargs)

    def _require_source(self) -> LocalResultSource:
        if self._source is None:
            self._source = self.source_factory()
            self.load_count += 1
        return self._source


@runtime_checkable
class ReclaimableLocalResultSource(LocalResultSource, Protocol):
    def reclaim_before(self, node_id: NodeId, index: int) -> int: ...


@dataclass(slots=True)
class WorkerRunResult:
    work_item: WorkItem
    chunk: EnumerationChunk | None
    ready_end: int
    chunk_latency_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class SourceReclaimCounters:
    cached_records: int = 0
    peak_cached_records: int = 0
    reclaimed_records: int = 0
    dag_repository_active_units: int = 0
    dag_stream_active_units: int = 0


class LocalProtocolWorker:
    def __init__(
        self,
        *,
        source: LocalResultSource,
        chunk_store: InMemoryChunkStore,
        leases: LeaseTable,
        states: NodeStateTable | None = None,
    ) -> None:
        self.source = source
        self.chunk_store = chunk_store
        self.leases = leases
        self.states = states

    def run(self, item: WorkItem) -> WorkerRunResult:
        self.leases.require_valid(
            node_id=item.node_id,
            worker_id=item.worker_id,
            epoch=item.epoch,
            start=item.start,
            end=item.end,
        )
        _reclaim_source_before(self.source, item.node_id, item.reclaim_before)
        started_at = monotonic()
        records = tuple(self.source.read_range(item.node_id, item.start, item.end))
        chunk_latency_seconds = monotonic() - started_at
        if not records:
            if self.states is not None:
                self.states.record_runtime_feedback(
                    item.node_id,
                    chunk_latency_seconds=chunk_latency_seconds,
                    records_requested=item.size,
                    records_produced=0,
                    ewma_alpha=0.25,
                ).mark_exhausted(item.start)
            lease = self.leases.lease_for(item.node_id, item.epoch)
            if lease is not None:
                self.leases.release(lease)
            return WorkerRunResult(
                work_item=item,
                chunk=None,
                ready_end=self.chunk_store.ready_end(item.node_id),
                chunk_latency_seconds=chunk_latency_seconds,
            )

        chunk = EnumerationChunk.from_records(
            node_id=item.node_id,
            start=item.start,
            records=records,
            worker_id=item.worker_id,
            epoch=item.epoch,
        )
        ready_end = self.chunk_store.publish(chunk)
        if self.states is not None:
            self.states.update_ready_end(item.node_id, ready_end)
            self.states.record_runtime_feedback(
                item.node_id,
                chunk_latency_seconds=chunk_latency_seconds,
                records_requested=item.size,
                records_produced=len(records),
                ewma_alpha=0.25,
            )
            if len(records) < item.size:
                self.states.ensure_node(item.node_id).mark_exhausted(
                    item.start + len(records),
                )

        lease = self.leases.lease_for(item.node_id, item.epoch)
        if lease is not None:
            self.leases.release(lease)

        return WorkerRunResult(
            work_item=item,
            chunk=chunk,
            ready_end=ready_end,
            chunk_latency_seconds=chunk_latency_seconds,
        )


def _reclaim_source_before(source: LocalResultSource, node_id: NodeId, index: int) -> int | None:
    reclaim_before = getattr(source, "reclaim_before", None)
    if not callable(reclaim_before):
        return None
    return int(reclaim_before(node_id, index))


def source_reclaim_counters(source: LocalResultSource) -> SourceReclaimCounters:
    stats = getattr(source, "stats", None)
    if not callable(stats):
        return SourceReclaimCounters()
    snapshot = stats()
    return SourceReclaimCounters(
        cached_records=int(getattr(snapshot, "cached_records", 0)),
        peak_cached_records=int(getattr(snapshot, "peak_cached_records", 0)),
        reclaimed_records=int(getattr(snapshot, "reclaimed_records", 0)),
        dag_repository_active_units=int(getattr(snapshot, "dag_repository_active_units", 0)),
        dag_stream_active_units=int(getattr(snapshot, "dag_stream_active_units", 0)),
    )


__all__ = [
    "LazyLocalResultSource",
    "LocalProtocolWorker",
    "LocalResultSource",
    "ReclaimableLocalResultSource",
    "SourceReclaimCounters",
    "WorkerRunResult",
    "_reclaim_source_before",
    "source_reclaim_counters",
]
