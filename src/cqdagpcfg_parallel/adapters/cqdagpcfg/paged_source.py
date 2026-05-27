from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from math import exp, fsum, log
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from CQDAGPCFG import GuessRecord
from CQDAGPCFG.enumeration.optimized.factory import OptimizedBlockFactory
from CQDAGPCFG.model.types import SlotEntry, Structure

from cqdagpcfg_parallel.protocol import NodeId, WorkerId
from cqdagpcfg_parallel.runtime import ZmqModelArtifactClient
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint
from cqdagpcfg_parallel.storage import (
    ModelJsonPage,
    PagedModelManifest,
    PagedSlotTableManifest,
    StateMigrationSnapshot,
    slot_page_id,
    structure_page_id,
)

from .block_graph import (
    CandidateRangeArtifact,
    CQDAGRecordSource,
    CQDAGSourceReclaimStats,
    ROOT_NODE_ID,
    _StructureLocalStream,
    _capture_reachable_blocks,
    _index_resolved_children,
    _iter_repository_blocks,
    _require_structure_index,
    _restore_block_snapshot,
    _write_record_artifact,
)
from .block_graph import BlockNodeDescriptor


@dataclass(frozen=True, slots=True)
class PagedModelStats:
    json_pages: int = 0
    json_page_hits: int = 0
    json_page_misses: int = 0
    json_page_evictions: int = 0


class PagedCQDAGModelClient:
    def __init__(
        self,
        *,
        endpoint: str,
        model_id: str,
        max_json_pages: int = 128,
    ) -> None:
        if max_json_pages <= 0:
            raise ValueError("max_json_pages must be positive")
        self.endpoint = endpoint
        self.model_id = model_id
        self.max_json_pages = max_json_pages
        self.client = ZmqModelArtifactClient(ZmqEndpoint.from_uri(endpoint, bind=False))
        self._manifest: PagedModelManifest | None = None
        self._pages: OrderedDict[str, ModelJsonPage] = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    @property
    def manifest(self) -> PagedModelManifest:
        if self._manifest is None:
            self._manifest = self.client.paged_manifest(self.model_id)
        return self._manifest

    @property
    def model_fingerprint(self) -> str:
        return self.manifest.model_fingerprint

    @property
    def stats(self) -> PagedModelStats:
        return PagedModelStats(
            json_pages=len(self._pages),
            json_page_hits=self._hits,
            json_page_misses=self._misses,
            json_page_evictions=self._evictions,
        )

    def page(self, page_id: str) -> ModelJsonPage:
        cached = self._pages.get(page_id)
        if cached is not None:
            self._pages.move_to_end(page_id)
            self._hits += 1
            return cached
        self._misses += 1
        page = self.client.fetch_page(self.model_fingerprint, page_id=page_id)
        if page.model_fingerprint != self.model_fingerprint:
            raise ValueError("model page fingerprint does not match manifest")
        self._pages[page_id] = page
        self._evict()
        return page

    def structures(self) -> tuple[Structure, ...]:
        values: list[Structure] = []
        for page_index in range(self.manifest.structure_page_count):
            page = self.page(structure_page_id(page_index))
            for item in page.data["structures"]:
                values.append(Structure.from_dict(dict(item)))
        return tuple(values)

    def close(self) -> None:
        self.client.close()

    def _evict(self) -> None:
        while len(self._pages) > self.max_json_pages:
            self._pages.popitem(last=False)
            self._evictions += 1


class PagedPcfgModel:
    def __init__(self, client: PagedCQDAGModelClient) -> None:
        self.client = client
        self.paged_manifest = client.manifest
        self.structures = client.structures()
        self.metadata = dict(self.paged_manifest.metadata)
        self.unknown_structure_prob = 0.0
        self.cqdag_index = None
        self.slot_tables = _PagedSlotTableMapping(self)
        self._slot_manifest_by_symbol = {
            table.symbol: table for table in self.paged_manifest.slot_tables
        }

    @property
    def stats(self) -> PagedModelStats:
        return self.client.stats

    def has_unknown_mass(self) -> bool:
        if self.unknown_structure_prob > 0.0:
            return True
        return any(table.unknown_prob > 0.0 for table in self.paged_manifest.slot_tables)

    def slot_manifest(self, symbol: str) -> PagedSlotTableManifest:
        try:
            return self._slot_manifest_by_symbol[symbol]
        except KeyError as exc:
            raise KeyError(f"unknown slot table: {symbol}") from exc

    def point_log_prob(self, structure_index: int, ranks: Sequence[int]) -> float:
        structure = self.structures[structure_index]
        terms = [structure.log_base_prob]
        for symbol, rank in zip(structure.symbols, ranks):
            terms.append(self.slot_tables[symbol].log_probs[rank])
        return fsum(terms)

    def point_prob(self, structure_index: int, ranks: Sequence[int]) -> float:
        return exp(self.point_log_prob(structure_index, ranks))

    def subsequence_log_prob(self, symbols: Sequence[str], ranks: Sequence[int]) -> float:
        return fsum(
            self.slot_tables[symbol].log_probs[rank]
            for symbol, rank in zip(symbols, ranks)
        )

    def subsequence_prob(self, symbols: Sequence[str], ranks: Sequence[int]) -> float:
        return exp(self.subsequence_log_prob(symbols, ranks))

    def guess_for(self, structure_index: int, ranks: Sequence[int]) -> str:
        structure = self.structures[structure_index]
        return "".join(
            self.slot_tables[symbol].surfaces[rank]
            for symbol, rank in zip(structure.symbols, ranks)
        )

    def record_for(self, structure_index: int, ranks: Sequence[int]) -> GuessRecord:
        return self.record_for_log_prob(
            structure_index,
            ranks,
            self.point_log_prob(structure_index, ranks),
        )

    def record_for_log_prob(
        self,
        structure_index: int,
        ranks: Sequence[int],
        log_prob: float,
    ) -> GuessRecord:
        structure = self.structures[structure_index]
        rank_tuple = tuple(int(rank) for rank in ranks)
        return GuessRecord(
            prob=self.point_prob(structure_index, rank_tuple),
            guess=self.guess_for(structure_index, rank_tuple),
            structure_index=structure_index,
            structure_name=structure.name,
            ranks=rank_tuple,
        )

    def arities_for(self, structure_index: int) -> tuple[int, ...]:
        structure = self.structures[structure_index]
        return tuple(len(self.slot_tables[symbol]) for symbol in structure.symbols)

    def slot_cardinality(self, symbol: str) -> int:
        return self.slot_manifest(symbol).cardinality

    def slot_log_weight(self, symbol: str) -> float:
        return log(max(self.slot_cardinality(symbol), 1))

    def slot_access_weight(self, symbol: str) -> float:
        return self.slot_manifest(symbol).access_weight

    def structure_slot_weights(self, structure_index: int) -> tuple[float, ...]:
        structure = self.structures[structure_index]
        return tuple(self.slot_log_weight(symbol) for symbol in structure.symbols)

    def structure_weight_prefix_sums(self, structure_index: int) -> tuple[float, ...]:
        prefix = [0.0]
        for weight in self.structure_slot_weights(structure_index):
            prefix.append(prefix[-1] + weight)
        return tuple(prefix)

    def total_points(self) -> int:
        total = 0
        for structure in self.structures:
            count = 1
            for symbol in structure.symbols:
                count *= self.slot_cardinality(symbol)
            total += count
        return total


class _PagedSlotTableMapping(Mapping[str, "PagedSlotTable"]):
    def __init__(self, model: PagedPcfgModel) -> None:
        self.model = model
        self._tables: dict[str, PagedSlotTable] = {}

    def __getitem__(self, symbol: str) -> "PagedSlotTable":
        table = self._tables.get(symbol)
        if table is None:
            table = PagedSlotTable(self.model, self.model.slot_manifest(symbol))
            self._tables[symbol] = table
        return table

    def __iter__(self) -> Iterator[str]:
        return iter(table.symbol for table in self.model.paged_manifest.slot_tables)

    def __len__(self) -> int:
        return len(self.model.paged_manifest.slot_tables)


class PagedSlotTable:
    def __init__(self, model: PagedPcfgModel, manifest: PagedSlotTableManifest) -> None:
        self.model = model
        self.manifest = manifest
        self.symbol = manifest.symbol
        self.unknown_prob = manifest.unknown_prob
        self.surfaces = _SlotValueSequence(self, "surface")
        self.probs = _SlotValueSequence(self, "prob")
        self.log_probs = _SlotValueSequence(self, "log_prob")
        self.entries = _SlotValueSequence(self, "entry")

    def __len__(self) -> int:
        return self.manifest.cardinality

    def interval_mass(self, left: int, right: int) -> float:
        if left < 0 or right < left or right >= len(self):
            raise IndexError((left, right))
        return sum(float(self.probs[index]) for index in range(left, right + 1))

    def entry(self, index: int) -> Mapping[str, Any]:
        if index < 0 or index >= len(self):
            raise IndexError(index)
        page_index = index // self.manifest.page_size
        page = self.model.client.page(slot_page_id(self.symbol, page_index))
        start = int(page.data["start"])
        return dict(page.data["entries"][index - start])


class _SlotValueSequence(Sequence[Any]):
    def __init__(self, table: PagedSlotTable, field: str) -> None:
        self.table = table
        self.field = field

    def __len__(self) -> int:
        return len(self.table)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return tuple(self[item] for item in range(*index.indices(len(self))))
        entry = self.table.entry(int(index))
        if self.field == "surface":
            return str(entry["surface"])
        if self.field == "prob":
            return float(entry["prob"])
        if self.field == "log_prob":
            return log(float(entry["prob"]))
        if self.field == "entry":
            return SlotEntry.from_dict(dict(entry))
        raise KeyError(self.field)


class PagedBlockFactoryBuilder:
    def build(self, model: PagedPcfgModel) -> OptimizedBlockFactory:
        return OptimizedBlockFactory(model, eager_materialize=False)


class PagedCQDAGRecordSource(CQDAGRecordSource):
    def __init__(
        self,
        model: PagedPcfgModel,
        *,
        node_id: NodeId = ROOT_NODE_ID,
        max_records: int,
    ) -> None:
        super().__init__(
            model,
            node_id=node_id,
            max_records=max_records,
            prefer_cpp=False,
            factory_builder=PagedBlockFactoryBuilder(),
        )

    def stats(self) -> CQDAGSourceReclaimStats:
        base = super().stats()
        paged = self.model.stats
        return CQDAGSourceReclaimStats(
            node_count=base.node_count,
            cached_records=base.cached_records,
            peak_cached_records=base.peak_cached_records,
            reclaimed_records=base.reclaimed_records,
            dag_repository_active_units=paged.json_pages,
            dag_stream_active_units=base.dag_stream_active_units,
        )

class PagedCQDAGStructureRecordSource:
    def __init__(
        self,
        model: PagedPcfgModel,
        *,
        max_records_per_structure: int,
    ) -> None:
        if max_records_per_structure < 0:
            raise ValueError("max_records_per_structure cannot be negative")
        self.model = model
        self.max_records_per_structure = max_records_per_structure
        self.factory = OptimizedBlockFactory(model, eager_materialize=False)
        self._descriptors = {
            NodeId(f"structure:{index}:{structure.name}"): BlockNodeDescriptor(
                node_id=NodeId(f"structure:{index}:{structure.name}"),
                name=structure.name,
                structure_index=index,
                symbols=tuple(structure.symbols),
                priority=structure.base_prob,
                estimated_cost=max(
                    sum(1.0 + model.slot_access_weight(symbol) for symbol in structure.symbols),
                    1.0,
                ),
                base_prob=structure.base_prob,
            )
            for index, structure in enumerate(model.structures)
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
        return self._stream_for(node_id).read_range(start, end)

    def write_range_artifact(
        self,
        node_id: NodeId,
        start: int,
        end: int,
        *,
        guess_path: str | Path,
        stable_path: str | Path | None = None,
        verify_artifact: bool = True,
        include_stable_metadata: bool = True,
    ) -> CandidateRangeArtifact:
        return _write_record_artifact(
            self.read_range(node_id, start, end),
            guess_path=guess_path,
            stable_path=stable_path,
            verify_artifact=verify_artifact,
            include_stable_metadata=include_stable_metadata,
        )

    def reclaim_before(self, node_id: NodeId, index: int) -> int:
        stream = self._streams.get(node_id)
        if stream is None:
            return 0
        return stream.reclaim_before(index)

    def stats(self) -> CQDAGSourceReclaimStats:
        paged = self.model.stats
        return CQDAGSourceReclaimStats(
            node_count=len(self._descriptors),
            cached_records=sum(stream.cached_records for stream in self._streams.values()),
            peak_cached_records=sum(stream.peak_cached_records for stream in self._streams.values()),
            reclaimed_records=sum(stream.reclaimed_records for stream in self._streams.values()),
            dag_repository_active_units=self.factory.active_units() + paged.json_pages,
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
        block_snapshots = _capture_reachable_blocks(tuple(stream.block for _, stream in streams))
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
            stream.restore_state(stream_snapshot)

    def _selected_streams(
        self,
        node_ids: Sequence[NodeId] | None,
    ) -> tuple[tuple[NodeId, _StructureLocalStream], ...]:
        if node_ids is None:
            return tuple(self._streams.items())
        return tuple((node_id, self._stream_for(node_id)) for node_id in node_ids)

    def _stream_for(self, node_id: NodeId) -> _StructureLocalStream:
        stream = self._streams.get(node_id)
        if stream is not None:
            return stream
        descriptor = self._descriptors.get(node_id)
        if descriptor is None:
            descriptor = _descriptor_from_node_id(self.model, node_id)
            self._descriptors[node_id] = descriptor
        stream = _StructureLocalStream(
            self.model,
            factory=self.factory,
            structure_index=_require_structure_index(descriptor),
            max_records=self.max_records_per_structure,
        )
        self._streams[node_id] = stream
        return stream


def build_paged_model(*, endpoint: str, model_id: str, max_json_pages: int = 128) -> PagedPcfgModel:
    client = PagedCQDAGModelClient(
        endpoint=endpoint,
        model_id=model_id,
        max_json_pages=max_json_pages,
    )
    return PagedPcfgModel(client)


def _descriptor_from_node_id(model: PagedPcfgModel, node_id: NodeId) -> BlockNodeDescriptor:
    parts = str(node_id).split(":", 2)
    if len(parts) < 2 or parts[0] != "structure":
        raise KeyError(f"unknown CQDAG structure source node: {node_id}")
    index = int(parts[1])
    structure = model.structures[index]
    return BlockNodeDescriptor(
        node_id=node_id,
        name=structure.name,
        structure_index=index,
        symbols=tuple(structure.symbols),
        priority=structure.base_prob,
        estimated_cost=max(
            sum(1.0 + model.slot_access_weight(symbol) for symbol in structure.symbols),
            1.0,
        ),
        base_prob=structure.base_prob,
    )


__all__ = [
    "PagedCQDAGModelClient",
    "PagedCQDAGRecordSource",
    "PagedCQDAGStructureRecordSource",
    "PagedModelStats",
    "PagedPcfgModel",
    "PagedSlotTable",
    "build_paged_model",
]
