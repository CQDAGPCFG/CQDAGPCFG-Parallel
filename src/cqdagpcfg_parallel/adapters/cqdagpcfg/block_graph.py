from __future__ import annotations

from dataclasses import dataclass
from math import log, prod
from typing import Any, Iterable, Sequence

from CQDAGPCFG import GuessRecord
from CQDAGPCFG.enumeration.optimized.blocks import (
    LeafBlock,
    LocalMergedBlock,
    MergedBlock,
    SharedBlock,
    SingleConsumerBlock,
)
from CQDAGPCFG.enumeration.optimized.builders import BlockFactoryBuilder
from CQDAGPCFG.enumeration.optimized.factory import OptimizedBlockFactory
from CQDAGPCFG.enumeration.types import flatten_rank_key

from cqdagpcfg_parallel.protocol import (
    NodeDependency,
    NodeId,
    NodeSchedulingFeatures,
    NodeStateTable,
    WorkerId,
)
from cqdagpcfg_parallel.storage import (
    BlockStateSnapshot,
    FrontierEntrySnapshot,
    GuessRecordSnapshot,
    GuessRecordWindowSnapshot,
    IndexCountSnapshot,
    LogProbRankEntry,
    MergedBlockStateSnapshot,
    NodeWatermarkSnapshot,
    ResultWindowSnapshot,
    StateMigrationSnapshot,
    StructureStreamStateSnapshot,
)

from .serial_oracle import SerialCQDAGOracle


ROOT_NODE_ID = NodeId("root")
_SOURCE_SKIP_ACK_WINDOW = 64
_SERIAL_PROBABILITY_ROUND_DIGITS = 15


@dataclass(frozen=True, slots=True)
class BlockNodeDescriptor:
    node_id: NodeId
    name: str
    structure_index: int | None = None
    symbols: tuple[str, ...] = ()
    entropy: float = 0.0
    slot_dispersion: float = 0.0
    priority: float = 1.0
    estimated_cost: float = 1.0
    base_prob: float = 1.0
    cardinality: int = 1


@dataclass(frozen=True, slots=True)
class CQDAGSourceReclaimStats:
    node_count: int = 0
    cached_records: int = 0
    peak_cached_records: int = 0
    reclaimed_records: int = 0
    dag_repository_active_units: int = 0
    dag_stream_active_units: int = 0


class CQDAGRecordSource:
    """Lazy local-result provider backed by a CQDAGPCFG serial enumerator."""

    def __init__(
        self,
        model,
        *,
        node_id: NodeId = ROOT_NODE_ID,
        max_records: int,
        prefer_cpp: bool = True,
        factory_builder: BlockFactoryBuilder | None = None,
    ) -> None:
        if max_records < 0:
            raise ValueError("max_records cannot be negative")
        self.model = model
        self.node_id = node_id
        self.max_records = max_records
        self.prefer_cpp = prefer_cpp
        self.factory_builder = factory_builder
        self.oracle = SerialCQDAGOracle(
            model,
            prefer_cpp=prefer_cpp,
            factory_builder=factory_builder,
        )
        self._iterator = iter(self.oracle.iter_records(max_records))
        self._cache: list[GuessRecord] = []
        self._cache_base = 0
        self._peak_cached_records = 0
        self._reclaimed_records = 0
        self.exhausted = False

    def read_range(
        self,
        node_id: NodeId,
        start: int,
        end: int,
    ) -> Sequence[GuessRecord]:
        if node_id != self.node_id:
            raise KeyError(f"unknown CQDAG record source node: {node_id}")
        if start < 0 or end < start:
            raise ValueError("invalid source range")
        if start < self._cache_base:
            self._restart_from_zero()
        if start > self._ready_end:
            self._skip_to(start)
        self._ensure(end)
        return tuple(self._cache[start - self._cache_base : end - self._cache_base])

    def reclaim_before(self, node_id: NodeId, index: int) -> int:
        if node_id != self.node_id:
            raise KeyError(f"unknown CQDAG record source node: {node_id}")
        if index < 0:
            raise ValueError("reclaim index cannot be negative")
        ready_end = self._cache_base + len(self._cache)
        reclaim_end = min(max(index, self._cache_base), ready_end)
        reclaimed = reclaim_end - self._cache_base
        if reclaimed <= 0:
            return self._cache_base
        del self._cache[:reclaimed]
        self._cache_base = reclaim_end
        self._reclaimed_records += reclaimed
        return self._cache_base

    def stats(self) -> CQDAGSourceReclaimStats:
        return CQDAGSourceReclaimStats(
            node_count=1,
            cached_records=len(self._cache),
            peak_cached_records=self._peak_cached_records,
            reclaimed_records=self._reclaimed_records,
        )

    @property
    def _ready_end(self) -> int:
        return self._cache_base + len(self._cache)

    def _skip_to(self, index: int) -> None:
        if index <= self._ready_end:
            return
        reclaim_end = min(index, self._ready_end)
        if reclaim_end > self._cache_base:
            reclaimed = reclaim_end - self._cache_base
            del self._cache[:reclaimed]
            self._cache_base = reclaim_end
            self._reclaimed_records += reclaimed
        while self._cache_base < index and not self.exhausted:
            try:
                next(self._iterator)
            except StopIteration:
                self.exhausted = True
                return
            self._cache_base += 1
            self._reclaimed_records += 1

    def _ensure(self, end: int) -> None:
        if end > self.max_records:
            raise RuntimeError("requested range exceeds configured source limit")
        while self._cache_base + len(self._cache) < end and not self.exhausted:
            try:
                self._cache.append(next(self._iterator))
                self._update_peak_cached_records()
            except StopIteration:
                self.exhausted = True

    def _update_peak_cached_records(self) -> None:
        self._peak_cached_records = max(self._peak_cached_records, len(self._cache))

    def _restart_from_zero(self) -> None:
        self.oracle = SerialCQDAGOracle(
            self.model,
            prefer_cpp=self.prefer_cpp,
            factory_builder=self.factory_builder,
        )
        self._iterator = iter(self.oracle.iter_records(self.max_records))
        self._cache = []
        self._cache_base = 0
        self.exhausted = False


class CQDAGStructureRecordSource:
    """Local-result provider where each CQDAGPCFG structure is a protocol node."""

    def __init__(
        self,
        model,
        *,
        max_records_per_structure: int,
        adapter: "CQDAGBlockGraphAdapter | None" = None,
    ) -> None:
        if max_records_per_structure < 0:
            raise ValueError("max_records_per_structure cannot be negative")
        self.model = model
        self.max_records_per_structure = max_records_per_structure
        self.adapter = CQDAGBlockGraphAdapter(model) if adapter is None else adapter
        self.factory = OptimizedBlockFactory(model)
        self._descriptors = {
            descriptor.node_id: descriptor for descriptor in self.adapter.structure_nodes()
        }
        self._streams: dict[NodeId, _StructureLocalStream] = {}

    def read_range(
        self,
        node_id: NodeId,
        start: int,
        end: int,
    ) -> Sequence[GuessRecord]:
        if start < 0 or end < start:
            raise ValueError("invalid source range")
        stream = self._stream_for(node_id)
        return stream.read_range(start, end)

    def reclaim_before(self, node_id: NodeId, index: int) -> int:
        if node_id not in self._descriptors:
            raise KeyError(f"unknown CQDAG structure source node: {node_id}")
        stream = self._streams.get(node_id)
        if stream is None:
            return 0
        return stream.reclaim_before(index)

    def stats(self) -> CQDAGSourceReclaimStats:
        return CQDAGSourceReclaimStats(
            node_count=len(self._descriptors),
            cached_records=sum(stream.cached_records for stream in self._streams.values()),
            peak_cached_records=sum(stream.peak_cached_records for stream in self._streams.values()),
            reclaimed_records=sum(stream.reclaimed_records for stream in self._streams.values()),
            dag_repository_active_units=self.factory.active_units(),
            dag_stream_active_units=sum(stream.active_units for stream in self._streams.values()),
        )

    def capture_state(
        self,
        *,
        model_fingerprint: str,
        source_worker_id: WorkerId,
        node_ids: Sequence[NodeId] | None = None,
        target_worker_id: WorkerId | None = None,
        reason: str = "rebalance",
    ) -> StateMigrationSnapshot:
        streams = self._selected_streams(node_ids)
        block_snapshots = _capture_reachable_blocks(
            tuple(stream.block for _, stream in streams)
        )
        return StateMigrationSnapshot.create(
            model_fingerprint=model_fingerprint,
            source_worker_id=source_worker_id,
            target_worker_id=target_worker_id,
            reason=reason,
            streams=tuple(stream.capture_state(node_id) for node_id, stream in streams),
            blocks=block_snapshots,
            watermarks=tuple(stream.watermark(node_id) for node_id, stream in streams),
        )

    def restore_state(
        self,
        snapshot: StateMigrationSnapshot,
        *,
        expected_model_fingerprint: str | None = None,
    ) -> None:
        if (
            expected_model_fingerprint is not None
            and snapshot.model_fingerprint != expected_model_fingerprint
        ):
            raise ValueError("snapshot model_fingerprint does not match target source")

        block_by_signature = dict(_iter_repository_blocks(self.factory))
        for stream in self._streams.values():
            block_by_signature.setdefault(tuple(stream.structure.symbols), stream.block)

        for block_snapshot in sorted(
            snapshot.blocks,
            key=lambda item: len(item.signature),
            reverse=True,
        ):
            block = block_by_signature.get(block_snapshot.signature)
            if block is None:
                block = self.factory.resolve_block(block_snapshot.signature, share=True)
                block_by_signature[block_snapshot.signature] = block
            _restore_block_snapshot(block, block_snapshot)
            _index_resolved_children(block, block_by_signature)

        for stream_snapshot in snapshot.streams:
            stream = self._stream_for(stream_snapshot.node_id)
            if tuple(stream.structure.symbols) != stream_snapshot.root_signature:
                raise ValueError(
                    f"snapshot root_signature does not match node: {stream_snapshot.node_id}"
                )
            stream.restore_state(stream_snapshot)

    def _selected_streams(
        self,
        node_ids: Sequence[NodeId] | None,
    ) -> tuple[tuple[NodeId, "_StructureLocalStream"], ...]:
        if node_ids is None:
            return tuple(self._streams.items())
        selected: list[tuple[NodeId, _StructureLocalStream]] = []
        for node_id in node_ids:
            selected.append((node_id, self._stream_for(node_id)))
        return tuple(selected)

    def _stream_for(self, node_id: NodeId) -> "_StructureLocalStream":
        stream = self._streams.get(node_id)
        if stream is not None:
            return stream
        try:
            descriptor = self._descriptors[node_id]
        except KeyError as exc:
            raise KeyError(f"unknown CQDAG structure source node: {node_id}") from exc
        stream = _StructureLocalStream(
            self.model,
            factory=self.factory,
            structure_index=_require_structure_index(descriptor),
            max_records=self.max_records_per_structure,
        )
        self._streams[node_id] = stream
        return stream


class CQDAGBlockGraphAdapter:
    def __init__(self, model, *, root_node_id: NodeId = ROOT_NODE_ID) -> None:
        self.model = model
        self.root_node_id = root_node_id
        self._slot_entropy_cache: dict[str, float] = {}
        self._slot_entropy_bound_cache: dict[str, float] = {}
        self._slot_cardinality_cache: dict[str, int] = {}
        self._slot_access_weight_cache: dict[str, float] = {}
        self._structure_cost_cache: dict[int, float] = {}
        self._structure_dispersion_cache: dict[int, float] = {}
        self._structure_nodes_cache: tuple[BlockNodeDescriptor, ...] | None = None
        self._scheduling_features_cache: tuple[NodeSchedulingFeatures, ...] | None = None

    @property
    def root_node(self) -> BlockNodeDescriptor:
        return BlockNodeDescriptor(
            node_id=self.root_node_id,
            name="root",
            entropy=self.model_entropy(),
            slot_dispersion=self.model_slot_dispersion(),
            priority=sum(structure.base_prob for structure in self.model.structures),
            estimated_cost=max(
                sum(self.structure_cost(index) for index in range(len(self.model.structures))),
                1.0,
            ),
            base_prob=sum(structure.base_prob for structure in self.model.structures),
            cardinality=max(self.model.total_points(), 1),
        )

    def structure_nodes(self) -> tuple[BlockNodeDescriptor, ...]:
        if self._structure_nodes_cache is not None:
            return self._structure_nodes_cache
        self._structure_nodes_cache = tuple(
            BlockNodeDescriptor(
                node_id=NodeId(f"structure:{index}:{structure.name}"),
                name=structure.name,
                structure_index=index,
                symbols=tuple(structure.symbols),
                entropy=sum(self._slot_entropy(symbol) for symbol in structure.symbols),
                slot_dispersion=self.structure_slot_dispersion(index),
                priority=structure.base_prob,
                estimated_cost=self.structure_cost(index),
                base_prob=structure.base_prob,
                cardinality=max(prod(self._slot_cardinality(symbol) for symbol in structure.symbols), 1),
            )
            for index, structure in enumerate(self.model.structures)
        )
        return self._structure_nodes_cache

    def scheduling_features(self) -> tuple[NodeSchedulingFeatures, ...]:
        if self._scheduling_features_cache is not None:
            return self._scheduling_features_cache
        self._scheduling_features_cache = tuple(
            NodeSchedulingFeatures(
                node_id=node.node_id,
                entropy=node.slot_dispersion,
                priority=node.priority,
                estimated_cost=node.estimated_cost,
            )
            for node in self.structure_nodes()
        )
        return self._scheduling_features_cache

    def dependencies(
        self,
        *,
        donation_weight: float = 1.0,
    ) -> tuple[NodeDependency, ...]:
        return tuple(
            NodeDependency(
                parent_id=self.root_node_id,
                child_id=node.node_id,
                donation_weight=donation_weight,
            )
            for node in self.structure_nodes()
        )

    def apply_scheduling_features(
        self,
        states: NodeStateTable,
        *,
        include_root: bool = True,
        include_dependencies: bool = True,
        donation_weight: float = 1.0,
    ) -> None:
        if include_root:
            root = self.root_node
            states.ensure_node(
                root.node_id,
                entropy=root.slot_dispersion,
                priority=root.priority,
                estimated_cost=root.estimated_cost,
            )
        for node in self.structure_nodes():
            states.ensure_node(
                node.node_id,
                entropy=node.slot_dispersion,
                priority=node.priority,
                estimated_cost=node.estimated_cost,
            )
            if include_dependencies:
                states.register_dependency(
                    self.root_node_id,
                    node.node_id,
                    donation_weight=donation_weight,
                )

    def serial_order_key(self, record: GuessRecord) -> tuple[float, int, tuple[int, ...]]:
        # Match CQDAGPCFG's probability ordering while absorbing tiny log/product
        # roundoff differences between structure-local streams.
        return (
            -round(record.prob, _SERIAL_PROBABILITY_ROUND_DIGITS),
            record.structure_index,
            record.ranks,
        )

    def model_entropy(self) -> float:
        return sum(self._slot_entropy(symbol) for symbol in self.model.slot_tables)

    def model_slot_dispersion(self) -> float:
        denominator = sum(self._slot_entropy_bound(symbol) for symbol in self.model.slot_tables)
        if denominator <= 0.0:
            return 0.0
        return self.model_entropy() / denominator

    def structure_cost(self, structure_index: int) -> float:
        cached = self._structure_cost_cache.get(structure_index)
        if cached is not None:
            return cached
        structure = self.model.structures[structure_index]
        cost = max(
            sum(1.0 + self._slot_access_weight(symbol) for symbol in structure.symbols),
            1.0,
        )
        self._structure_cost_cache[structure_index] = cost
        return cost

    def structure_slot_dispersion(self, structure_index: int) -> float:
        cached = self._structure_dispersion_cache.get(structure_index)
        if cached is not None:
            return cached
        structure = self.model.structures[structure_index]
        numerator = sum(self._slot_entropy(symbol) for symbol in structure.symbols)
        denominator = sum(self._slot_entropy_bound(symbol) for symbol in structure.symbols)
        if denominator <= 0.0:
            dispersion = 0.0
        else:
            dispersion = numerator / denominator
        self._structure_dispersion_cache[structure_index] = dispersion
        return dispersion

    def _slot_entropy(self, symbol: str) -> float:
        cached = self._slot_entropy_cache.get(symbol)
        if cached is not None:
            return cached
        value = slot_entropy(self.model.slot_tables[symbol])
        self._slot_entropy_cache[symbol] = value
        return value

    def _slot_entropy_bound(self, symbol: str) -> float:
        cached = self._slot_entropy_bound_cache.get(symbol)
        if cached is not None:
            return cached
        value = slot_entropy_bound(self.model.slot_tables[symbol])
        self._slot_entropy_bound_cache[symbol] = value
        return value

    def _slot_cardinality(self, symbol: str) -> int:
        cached = self._slot_cardinality_cache.get(symbol)
        if cached is not None:
            return cached
        value = self.model.slot_cardinality(symbol)
        self._slot_cardinality_cache[symbol] = value
        return value

    def _slot_access_weight(self, symbol: str) -> float:
        cached = self._slot_access_weight_cache.get(symbol)
        if cached is not None:
            return cached
        value = self.model.slot_access_weight(symbol)
        self._slot_access_weight_cache[symbol] = value
        return value


def slot_entropy(slot_table) -> float:
    return -sum(prob * log(prob) for prob in slot_table.probs if prob > 0.0)


def slot_entropy_bound(slot_table) -> float:
    size = len(slot_table)
    if size <= 1:
        return 0.0
    return log(size)


class _StructureLocalStream:
    def __init__(
        self,
        model,
        *,
        factory: OptimizedBlockFactory,
        structure_index: int,
        max_records: int,
    ) -> None:
        self.model = model
        self.factory = factory
        self.structure_index = structure_index
        self.structure = model.structures[structure_index]
        self.max_records = max_records
        self.block = factory.get_block(self.structure.symbols)
        self.consumer_id = self.block.register_consumer()
        self._cache: list[GuessRecord] = []
        self._cache_base = 0
        self._peak_cached_records = 0
        self._reclaimed_records = 0
        self.exhausted = False

    def read_range(self, start: int, end: int) -> tuple[GuessRecord, ...]:
        if start < 0 or end < start:
            raise ValueError("invalid structure source range")
        if start < self._cache_base:
            self._restart_from_zero()
        if end > self.max_records:
            raise RuntimeError("requested range exceeds configured structure source limit")
        if start > self.ready_end:
            self._skip_to(start)
        self._ensure(end)
        return tuple(self._cache[start - self._cache_base : end - self._cache_base])

    @property
    def cached_records(self) -> int:
        return len(self._cache)

    @property
    def peak_cached_records(self) -> int:
        return self._peak_cached_records

    @property
    def reclaimed_records(self) -> int:
        return self._reclaimed_records

    @property
    def active_units(self) -> int:
        return self.block.active_units()

    @property
    def ready_end(self) -> int:
        return self._cache_base + len(self._cache)

    def reclaim_before(self, index: int) -> int:
        if index < 0:
            raise ValueError("reclaim index cannot be negative")
        ready_end = self._cache_base + len(self._cache)
        reclaim_end = min(max(index, self._cache_base), ready_end)
        reclaimed = reclaim_end - self._cache_base
        if reclaimed <= 0:
            return self._cache_base
        self.block.ack(self.consumer_id, reclaim_end - 1)
        del self._cache[:reclaimed]
        self._cache_base = reclaim_end
        self._reclaimed_records += reclaimed
        return self._cache_base

    def _skip_to(self, index: int) -> None:
        if index <= self.ready_end:
            return
        self.reclaim_before(min(index, self.ready_end))
        while self._cache_base < index and not self.exhausted:
            target = min(index, self._cache_base + _SOURCE_SKIP_ACK_WINDOW)
            try:
                self.block.ensure_generated(target - 1)
            except IndexError:
                produced = max(self.block.produced_count(), self._cache_base)
                if produced > self._cache_base:
                    self.block.ack(self.consumer_id, produced - 1)
                    self._reclaimed_records += produced - self._cache_base
                    self._cache_base = produced
                self.exhausted = True
                return
            self.block.ack(self.consumer_id, target - 1)
            self._reclaimed_records += target - self._cache_base
            self._cache_base = target

    def _ensure(self, end: int) -> None:
        while self._cache_base + len(self._cache) < end and not self.exhausted:
            index = self._cache_base + len(self._cache)
            try:
                if index >= self.block.produced_count():
                    self.block.ensure_generated(index)
                local_log_prob, rank_key = self.block.get_generated(index)
            except IndexError:
                self.exhausted = True
                return
            record = self.model.record_for_log_prob(
                self.structure_index,
                flatten_rank_key(rank_key),
                self.structure.log_base_prob + local_log_prob,
            )
            self._cache.append(record)
            self._update_peak_cached_records()

    def _update_peak_cached_records(self) -> None:
        self._peak_cached_records = max(self._peak_cached_records, len(self._cache))

    def _restart_from_zero(self) -> None:
        self.factory = OptimizedBlockFactory(self.model)
        self.block = self.factory.get_block(self.structure.symbols)
        self.consumer_id = self.block.register_consumer()
        self._cache = []
        self._cache_base = 0
        self.exhausted = False

    def capture_state(self, node_id: NodeId) -> StructureStreamStateSnapshot:
        return StructureStreamStateSnapshot(
            node_id=node_id,
            structure_index=self.structure_index,
            structure_name=self.structure.name,
            symbols=tuple(self.structure.symbols),
            root_signature=tuple(self.structure.symbols),
            max_records=self.max_records,
            stream_base=self._cache_base,
            ready_end=self.ready_end,
            consumer_id=self.consumer_id,
            guess_cache=GuessRecordWindowSnapshot(
                base=self._cache_base,
                entries=tuple(
                    GuessRecordSnapshot.from_record(record) for record in self._cache
                ),
            ),
        )

    def watermark(self, node_id: NodeId) -> NodeWatermarkSnapshot:
        return NodeWatermarkSnapshot(
            node_id=node_id,
            ready_end=self.ready_end,
            reclaim_before=self._cache_base,
            target_end=None,
        )

    def restore_state(self, snapshot: StructureStreamStateSnapshot) -> None:
        if snapshot.structure_index != self.structure_index:
            raise ValueError("snapshot structure_index does not match target stream")
        if snapshot.symbols != tuple(self.structure.symbols):
            raise ValueError("snapshot symbols do not match target stream")
        if snapshot.max_records > self.max_records:
            raise ValueError("snapshot max_records exceeds target stream limit")
        self.consumer_id = snapshot.consumer_id
        if snapshot.guess_cache.entries:
            self._cache_base = snapshot.guess_cache.base
            self._cache = tuple_entry_records(snapshot.guess_cache.entries)
        else:
            self._cache_base = snapshot.ready_end
            self._cache = []
        self._peak_cached_records = max(self._peak_cached_records, len(self._cache))
        self._reclaimed_records = max(self._reclaimed_records, self._cache_base)
        self.exhausted = False


def _require_structure_index(descriptor: BlockNodeDescriptor) -> int:
    if descriptor.structure_index is None:
        raise ValueError(f"descriptor is not a structure node: {descriptor.node_id}")
    return descriptor.structure_index


def _capture_reachable_blocks(blocks: Iterable[Any]) -> tuple[BlockStateSnapshot, ...]:
    seen: set[int] = set()
    snapshots: list[BlockStateSnapshot] = []
    for block in blocks:
        _capture_block_tree(block, seen, snapshots)
    return tuple(snapshots)


def _capture_block_tree(
    block: Any,
    seen: set[int],
    snapshots: list[BlockStateSnapshot],
) -> None:
    block_id = id(block)
    if block_id in seen:
        return
    seen.add(block_id)
    snapshots.append(_capture_block_snapshot(block))
    left = getattr(block, "left", None)
    right = getattr(block, "right", None)
    if left is not None:
        _capture_block_tree(left, seen, snapshots)
    if right is not None:
        _capture_block_tree(right, seen, snapshots)


def _capture_block_snapshot(block: Any) -> BlockStateSnapshot:
    return BlockStateSnapshot(
        signature=tuple(block.signature),
        kind=_block_kind(block),
        results=_result_window_from_block(block),
        seed_result=_optional_result_entry(getattr(block, "_seed_result", None)),
        consumer_upto=_consumer_upto(block),
        expected_consumers=int(getattr(block, "_expected_consumers", 1)),
        registered_consumers=_registered_consumers(block),
        promotion_pins=int(getattr(block, "_promotion_pins", 0)),
        merged=_capture_merged_state(block) if _is_merged_block(block) else None,
    )


def _capture_merged_state(block: Any) -> MergedBlockStateSnapshot:
    return MergedBlockStateSnapshot(
        left_signature=tuple(block.left_signature),
        right_signature=tuple(block.right_signature),
        left_consumer=getattr(block, "left_consumer", None),
        right_consumer=getattr(block, "right_consumer", None),
        left_cache=_result_window_from_values(
            block.left_cache,
            int(block.left_cache_base),
            int(block.left_cache_head),
        ),
        right_cache=_result_window_from_values(
            block.right_cache,
            int(block.right_cache_base),
            int(block.right_cache_head),
        ),
        frontier=tuple(
            FrontierEntrySnapshot(
                neg_log_prob=float(entry[0]),
                rank_key=_rank_key_tuple(entry[1]),
                left_index=int(entry[2]),
            )
            for entry in block.frontier
        ),
        active_rows=tuple(
            IndexCountSnapshot(index=int(index), value=int(value))
            for index, value in sorted(block.active_rows.items())
        ),
        active_right_counts=tuple(
            IndexCountSnapshot(index=int(index), value=int(value))
            for index, value in sorted(block.active_right_counts.items())
        ),
        right_min_heap=tuple(int(value) for value in block._right_min_heap),
        min_active_left=block.min_active_left,
        min_active_right=block.min_active_right,
        max_started_row=int(block.max_started_row),
        initialized=bool(block.initialized),
        seeded_zero=bool(block.seeded_zero),
        refines_since_reclaim=int(block._refines_since_reclaim),
        reclaim_units=int(block._reclaim_units),
        next_reclaim_after=int(block._next_reclaim_after),
    )


def _restore_block_snapshot(block: Any, snapshot: BlockStateSnapshot) -> None:
    if tuple(block.signature) != snapshot.signature:
        raise ValueError("block snapshot signature does not match target block")
    if _is_merged_block(block):
        block._resolve_children()
    _restore_result_window(block, snapshot.results)
    if hasattr(block, "_seed_result"):
        block._seed_result = _result_from_entry(snapshot.seed_result)
    if hasattr(block, "_promotion_pins"):
        block._promotion_pins = snapshot.promotion_pins
    _restore_consumers(block, snapshot)
    if snapshot.merged is not None:
        _restore_merged_state(block, snapshot.merged)


def _index_resolved_children(block: Any, block_by_signature: dict[tuple[str, ...], Any]) -> None:
    left = getattr(block, "left", None)
    right = getattr(block, "right", None)
    if left is not None:
        block_by_signature.setdefault(tuple(left.signature), left)
    if right is not None:
        block_by_signature.setdefault(tuple(right.signature), right)


def _restore_merged_state(block: Any, snapshot: MergedBlockStateSnapshot) -> None:
    if tuple(block.left_signature) != snapshot.left_signature:
        raise ValueError("merged block left_signature mismatch")
    if tuple(block.right_signature) != snapshot.right_signature:
        raise ValueError("merged block right_signature mismatch")
    block.left_consumer = snapshot.left_consumer
    block.right_consumer = snapshot.right_consumer
    block.left_cache = _entries_to_results(snapshot.left_cache.entries)
    block.left_cache_base = snapshot.left_cache.base
    block.left_cache_head = 0
    block.right_cache = _entries_to_results(snapshot.right_cache.entries)
    block.right_cache_base = snapshot.right_cache.base
    block.right_cache_head = 0
    block.frontier = [
        (entry.neg_log_prob, _rank_key_tuple(entry.rank_key), entry.left_index)
        for entry in snapshot.frontier
    ]
    block.active_rows = {entry.index: entry.value for entry in snapshot.active_rows}
    block.active_right_counts = {
        entry.index: entry.value for entry in snapshot.active_right_counts
    }
    block._right_min_heap = list(snapshot.right_min_heap)
    block.min_active_left = snapshot.min_active_left
    block.min_active_right = snapshot.min_active_right
    block.max_started_row = snapshot.max_started_row
    block.initialized = snapshot.initialized
    block.seeded_zero = snapshot.seeded_zero
    block._refines_since_reclaim = snapshot.refines_since_reclaim
    block._reclaim_units = snapshot.reclaim_units
    block._next_reclaim_after = snapshot.next_reclaim_after


def _restore_result_window(block: Any, snapshot: ResultWindowSnapshot) -> None:
    if not hasattr(block, "results"):
        return
    block.results = _entries_to_results(snapshot.entries)
    block.results_base = snapshot.base
    block.results_head = 0


def _restore_consumers(block: Any, snapshot: BlockStateSnapshot) -> None:
    if isinstance(block, LeafBlock):
        return
    if isinstance(block, SharedBlock):
        block._consumer_upto = list(snapshot.consumer_upto)
        block._expected_consumers = snapshot.expected_consumers
        block._registered_consumers = snapshot.registered_consumers
        return
    if isinstance(block, SingleConsumerBlock):
        block._registered = snapshot.registered_consumers > 0
        block._consumer_upto = snapshot.consumer_upto[0] if snapshot.consumer_upto else -1


def _iter_repository_blocks(factory: OptimizedBlockFactory) -> tuple[tuple[tuple[str, ...], Any], ...]:
    raw_blocks = getattr(factory.repository, "_blocks", {})
    return tuple((tuple(signature), block) for signature, block in raw_blocks.items())


def _result_window_from_block(block: Any) -> ResultWindowSnapshot:
    if not hasattr(block, "results"):
        return ResultWindowSnapshot()
    return _result_window_from_values(
        block.results,
        int(getattr(block, "results_base", 0)),
        int(getattr(block, "results_head", 0)),
    )


def _result_window_from_values(
    values: Sequence[Any],
    base: int,
    head: int,
) -> ResultWindowSnapshot:
    return ResultWindowSnapshot(
        base=base,
        entries=tuple(_result_entry(result) for result in values[head:]),
    )


def _result_entry(result: Any) -> LogProbRankEntry:
    return LogProbRankEntry(
        log_prob=float(result[0]),
        rank_key=_rank_key_tuple(result[1]),
    )


def _optional_result_entry(result: Any | None) -> LogProbRankEntry | None:
    return None if result is None else _result_entry(result)


def _result_from_entry(entry: LogProbRankEntry | None) -> Any | None:
    if entry is None:
        return None
    return (entry.log_prob, _rank_key_tuple(entry.rank_key))


def _entries_to_results(entries: Sequence[LogProbRankEntry]) -> list[Any]:
    return [(entry.log_prob, _rank_key_tuple(entry.rank_key)) for entry in entries]


def tuple_entry_records(entries: Sequence[GuessRecordSnapshot]) -> list[GuessRecord]:
    return [entry.to_record() for entry in entries]


def _rank_key_tuple(value: Any) -> Any:
    if isinstance(value, int):
        return value
    if isinstance(value, list):
        return tuple(_rank_key_tuple(child) for child in value)
    if isinstance(value, tuple):
        return tuple(_rank_key_tuple(child) for child in value)
    raise TypeError(f"invalid rank key value: {value!r}")


def _consumer_upto(block: Any) -> tuple[int, ...]:
    value = getattr(block, "_consumer_upto", ())
    if isinstance(value, list):
        return tuple(int(item) for item in value)
    if isinstance(value, int):
        return (value,) if getattr(block, "_registered", True) else ()
    return ()


def _registered_consumers(block: Any) -> int:
    if hasattr(block, "_registered_consumers"):
        return int(block._registered_consumers)
    if hasattr(block, "_registered"):
        return 1 if block._registered else 0
    return len(_consumer_upto(block))


def _block_kind(block: Any) -> str:
    if isinstance(block, LeafBlock):
        return "leaf"
    if isinstance(block, MergedBlock):
        return "shared_merged"
    if isinstance(block, LocalMergedBlock):
        return "local_merged"
    if isinstance(block, SharedBlock):
        return "shared"
    if isinstance(block, SingleConsumerBlock):
        return "single_consumer"
    return type(block).__name__


def _is_merged_block(block: Any) -> bool:
    return isinstance(block, (MergedBlock, LocalMergedBlock))


__all__ = [
    "BlockNodeDescriptor",
    "CQDAGBlockGraphAdapter",
    "CQDAGRecordSource",
    "CQDAGSourceReclaimStats",
    "CQDAGStructureRecordSource",
    "ROOT_NODE_ID",
    "slot_entropy",
    "slot_entropy_bound",
]
