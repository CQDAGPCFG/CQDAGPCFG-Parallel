from __future__ import annotations

import hashlib
import os
from collections import deque
from dataclasses import dataclass, replace
from math import fsum, log, prod
from pathlib import Path
from typing import Any, Iterable, Sequence

from CQDAGPCFG import GuessRecord
from CQDAGPCFG.cpp_backend import CppOptimizedCQDAGEnumerator
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
    STABLE_PROBABILITY_DIGITS,
    StableStreamFingerprint,
    WorkerId,
    canonical_record_bytes,
    stable_record_string,
)
from cqdagpcfg_parallel.runtime.candidate_batch import UNCHECKED_ARTIFACT_SHA256
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
_SERIAL_PROBABILITY_SIGNIFICANT_DIGITS = STABLE_PROBABILITY_DIGITS
_ARTIFACT_WRITE_BUFFER_BYTES = 1 << 20
DEFAULT_PROBABILITY_MASS_LANE_BIAS = 2.0


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
    lane_index: int | None = None
    lane_count: int = 1
    rank_start: int = 0
    rank_end: int | None = None

    @property
    def is_lane(self) -> bool:
        return self.rank_end is not None and self.rank_end > self.rank_start


@dataclass(frozen=True, slots=True)
class StructureRankLane:
    structure_index: int
    structure_name: str
    lane_index: int
    lane_count: int
    start: int
    end: int

    @property
    def node_id(self) -> NodeId:
        return structure_rank_lane_node_id(
            self.structure_index,
            self.structure_name,
            self.lane_index,
            self.lane_count,
        )

    @property
    def base_node_id(self) -> NodeId:
        return NodeId(f"structure:{self.structure_index}:{self.structure_name}")

    @property
    def length(self) -> int:
        return max(0, self.end - self.start)


@dataclass(frozen=True, slots=True)
class CQDAGSourceReclaimStats:
    node_count: int = 0
    cached_records: int = 0
    peak_cached_records: int = 0
    reclaimed_records: int = 0
    dag_repository_active_units: int = 0
    dag_stream_active_units: int = 0


@dataclass(frozen=True, slots=True)
class CandidateRangeArtifact:
    record_count: int
    payload_bytes: int
    artifact_uri: str
    artifact_sha256: str
    artifact_bytes: int
    stable_artifact_uri: str | None
    stable_artifact_sha256: str | None
    stable_artifact_bytes: int
    stable_fingerprint: str | None
    stable_fingerprint_bytes: int
    probability_mass: float


def _empty_candidate_artifact(
    *,
    guess_path: Path,
    stable_path: Path | None,
    verify_artifact: bool,
) -> CandidateRangeArtifact:
    guess_path.parent.mkdir(parents=True, exist_ok=True)
    guess_path.write_bytes(b"")
    if stable_path is not None:
        stable_path.parent.mkdir(parents=True, exist_ok=True)
        stable_path.write_bytes(b"")
    return CandidateRangeArtifact(
        record_count=0,
        payload_bytes=0,
        artifact_uri=guess_path.resolve().as_uri(),
        artifact_sha256=_artifact_sha256(guess_path, verify_artifact),
        artifact_bytes=0,
        stable_artifact_uri=(
            stable_path.resolve().as_uri() if stable_path is not None else None
        ),
        stable_artifact_sha256=(
            _file_sha256(stable_path) if stable_path is not None else None
        ),
        stable_artifact_bytes=0,
        stable_fingerprint="sfp-v1:0:0000000000000000:0000000000000000",
        stable_fingerprint_bytes=0,
        probability_mass=0.0,
    )


def structure_rank_lane_node_id(
    structure_index: int,
    structure_name: str,
    lane_index: int,
    lane_count: int,
) -> NodeId:
    if structure_index < 0:
        raise ValueError("structure_index cannot be negative")
    if lane_index < 0:
        raise ValueError("lane_index cannot be negative")
    if lane_count <= 0:
        raise ValueError("lane_count must be positive")
    if lane_index >= lane_count:
        raise ValueError("lane_index cannot be greater than or equal to lane_count")
    return NodeId(
        f"structure:{structure_index}:lane:{lane_index}:{lane_count}:{structure_name}"
    )


def parse_structure_rank_lane_node_id(node_id: NodeId) -> tuple[int, int, int, str] | None:
    parts = str(node_id).split(":", 5)
    if len(parts) != 6 or parts[0] != "structure" or parts[2] != "lane":
        return None
    try:
        structure_index = int(parts[1])
        lane_index = int(parts[3])
        lane_count = int(parts[4])
    except ValueError:
        return None
    if structure_index < 0 or lane_index < 0 or lane_count <= 0 or lane_index >= lane_count:
        return None
    return structure_index, lane_index, lane_count, parts[5]


def structure_rank_range_node_id(
    structure_index: int,
    structure_name: str,
    start: int,
    end: int,
) -> NodeId:
    if structure_index < 0:
        raise ValueError("structure_index cannot be negative")
    if start < 0:
        raise ValueError("rank range start cannot be negative")
    if end <= start:
        raise ValueError("rank range end must be greater than start")
    return NodeId(f"structure:{structure_index}:range:{start}:{end}:{structure_name}")


def parse_structure_rank_range_node_id(node_id: NodeId) -> tuple[int, int, int, str] | None:
    parts = str(node_id).split(":", 5)
    if len(parts) != 6 or parts[0] != "structure" or parts[2] != "range":
        return None
    try:
        structure_index = int(parts[1])
        start = int(parts[3])
        end = int(parts[4])
    except ValueError:
        return None
    if structure_index < 0 or start < 0 or end <= start:
        return None
    return structure_index, start, end, parts[5]


def structure_rank_lane_bounds(
    *,
    cardinality: int,
    lane_index: int,
    lane_count: int,
) -> tuple[int, int]:
    if cardinality < 0:
        raise ValueError("cardinality cannot be negative")
    if lane_count <= 0:
        raise ValueError("lane_count must be positive")
    if lane_index < 0 or lane_index >= lane_count:
        raise ValueError("lane_index is out of range")
    if cardinality == 0:
        return 0, 0
    effective_lanes = min(lane_count, cardinality)
    if lane_index >= effective_lanes:
        return cardinality, cardinality
    lane_size = (cardinality + effective_lanes - 1) // effective_lanes
    start = min(cardinality, lane_index * lane_size)
    end = min(cardinality, start + lane_size)
    return start, end


def structure_probability_mass_lane_bounds(
    *,
    cardinality: int,
    lane_index: int,
    lane_count: int,
    rank_horizon: int | None = None,
    mass_bias: float = DEFAULT_PROBABILITY_MASS_LANE_BIAS,
) -> tuple[int, int]:
    if cardinality < 0:
        raise ValueError("cardinality cannot be negative")
    if lane_count <= 0:
        raise ValueError("lane_count must be positive")
    if lane_index < 0 or lane_index >= lane_count:
        raise ValueError("lane_index is out of range")
    if mass_bias <= 0.0:
        raise ValueError("mass_bias must be positive")
    if cardinality == 0:
        return 0, 0
    horizon = cardinality if rank_horizon is None else min(cardinality, rank_horizon)
    if horizon <= 0:
        return 0, 0
    effective_lanes = min(lane_count, horizon)
    if lane_index >= effective_lanes:
        return horizon, horizon

    def boundary(index: int) -> int:
        if index <= 0:
            return 0
        if index >= effective_lanes:
            return horizon
        fraction = (index / effective_lanes) ** mass_bias
        raw = round(horizon * fraction)
        lower = index
        upper = horizon - (effective_lanes - index)
        return min(upper, max(lower, raw))

    start = boundary(lane_index)
    end = boundary(lane_index + 1)
    if end <= start:
        end = min(horizon, start + 1)
    return start, end


def _probability_mass_fraction(
    rank: int,
    *,
    horizon: int,
    mass_bias: float,
) -> float:
    if horizon <= 0:
        return 0.0
    ratio = min(1.0, max(0.0, rank / horizon))
    return ratio ** (1.0 / mass_bias)


class CQDAGRecordSource:
    """Lazy local-result provider backed by a CQDAGPCFG serial enumerator."""

    def __init__(
        self,
        model,
        *,
        node_id: NodeId = ROOT_NODE_ID,
        max_records: int,
        prefer_cpp: bool = False,
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
        self._lookahead_record: GuessRecord | None = None
        self._cache: list[GuessRecord] = []
        self._cache_base = 0
        self._raw_index = 0
        self._raw_buffer: deque[GuessRecord] = deque()
        self._lookahead_record: GuessRecord | None = None
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
        effective_end = min(end, self.max_records)
        if start >= effective_end:
            self.exhausted = True
            return ()
        if start < self._cache_base:
            self._restart_from_zero()
        if start > self._ready_end:
            self._skip_to(start)
        self._ensure(effective_end)
        if effective_end < end:
            self.exhausted = True
        return tuple(
            self._cache[start - self._cache_base : effective_end - self._cache_base]
        )

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
        if node_id != self.node_id:
            raise KeyError(f"unknown CQDAG record source node: {node_id}")
        if start < 0 or end < start:
            raise ValueError("invalid source range")
        effective_end = min(end, self.max_records)
        records = (
            ()
            if start >= effective_end
            else tuple(self.read_range(node_id, start, effective_end))
        )
        if len(records) < end - start:
            self.exhausted = True
        return _write_record_artifact(
            records,
            guess_path=guess_path,
            stable_path=stable_path,
            verify_artifact=verify_artifact,
            include_stable_metadata=include_stable_metadata,
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
            group = self._next_canonical_group()
            if not group:
                return
            skip_count = min(index - self._cache_base, len(group))
            self._cache_base += skip_count
            self._reclaimed_records += skip_count
            if skip_count < len(group):
                self._cache.extend(group[skip_count:])
                self._update_peak_cached_records()
                return

    def _ensure(self, end: int) -> None:
        if end > self.max_records:
            raise RuntimeError("requested range exceeds configured source limit")
        while self._cache_base + len(self._cache) < end and not self.exhausted:
            group = self._next_canonical_group()
            if not group:
                return
            self._cache.extend(group)
            self._update_peak_cached_records()

    def _update_peak_cached_records(self) -> None:
        self._peak_cached_records = max(self._peak_cached_records, len(self._cache))

    def _restart_from_zero(self) -> None:
        self.oracle = SerialCQDAGOracle(
            self.model,
            prefer_cpp=self.prefer_cpp,
            factory_builder=self.factory_builder,
        )
        self._iterator = iter(self.oracle.iter_records(self.max_records))
        self._lookahead_record = None
        self._cache = []
        self._cache_base = 0
        self.exhausted = False

    def _next_canonical_group(self) -> tuple[GuessRecord, ...]:
        first = self._pop_next_raw_record()
        if first is None:
            return ()
        group_key = _probability_group_key(first)
        group = [first]
        while True:
            record = self._pop_next_raw_record()
            if record is None:
                break
            if _probability_group_key(record) != group_key:
                self._lookahead_record = record
                break
            group.append(record)
        return tuple(sorted(group, key=_canonical_tie_key))

    def _pop_next_raw_record(self) -> GuessRecord | None:
        if self._lookahead_record is not None:
            record = self._lookahead_record
            self._lookahead_record = None
            return record
        try:
            return next(self._iterator)
        except StopIteration:
            self.exhausted = True
            return None


class CppFileCQDAGRecordSource:
    """Root stream backed directly by the CQDAGPCFG C++ file loader."""

    def __init__(
        self,
        model_path: str | Path,
        *,
        node_id: NodeId = ROOT_NODE_ID,
        max_records: int,
        use_promote: bool = False,
        share_nodes: bool = False,
    ) -> None:
        if max_records < 0:
            raise ValueError("max_records cannot be negative")
        self.model_path = Path(model_path)
        self.node_id = node_id
        self.max_records = max_records
        self.prefer_cpp = True
        self.use_promote = use_promote
        self.share_nodes = share_nodes
        if self.model_path.suffix == ".bin":
            self.enumerator = CppOptimizedCQDAGEnumerator.from_binary_file(
                self.model_path,
                use_promote=use_promote,
                share_nodes=share_nodes,
            )
        else:
            self.enumerator = CppOptimizedCQDAGEnumerator.from_json_file(
                self.model_path,
                use_promote=use_promote,
                share_nodes=share_nodes,
            )
        self._cache: list[GuessRecord] = []
        self._cache_base = 0
        self._raw_index = 0
        self._raw_buffer: deque[GuessRecord] = deque()
        self._lookahead_record: GuessRecord | None = None
        self._raw_fetch_size = 4096
        self._raw_exhausted = False
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
        effective_end = min(end, self.max_records)
        if start >= effective_end:
            self.exhausted = True
            return ()
        if start < self._cache_base:
            self._restart_from_zero()
        if start > self._ready_end:
            self._skip_to(start)
        self._ensure(effective_end)
        if effective_end < end:
            self.exhausted = True
        return tuple(
            self._cache[start - self._cache_base : effective_end - self._cache_base]
        )

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
        if node_id != self.node_id:
            raise KeyError(f"unknown CQDAG record source node: {node_id}")
        if start < 0 or end < start:
            raise ValueError("invalid source range")
        guess_path = Path(guess_path)
        stable_path = None if stable_path is None else Path(stable_path)
        guess_path.parent.mkdir(parents=True, exist_ok=True)
        if stable_path is not None:
            stable_path.parent.mkdir(parents=True, exist_ok=True)
        effective_end = min(end, self.max_records)
        if start >= effective_end:
            guess_path.write_bytes(b"")
            if stable_path is not None:
                stable_path.write_bytes(b"")
            self.exhausted = True
            return CandidateRangeArtifact(
                record_count=0,
                payload_bytes=0,
                artifact_uri=guess_path.resolve().as_uri(),
                artifact_sha256=_artifact_sha256(guess_path, verify_artifact),
                artifact_bytes=0,
                stable_artifact_uri=(
                    stable_path.resolve().as_uri() if stable_path is not None else None
                ),
                stable_artifact_sha256=(
                    _file_sha256(stable_path) if stable_path is not None else None
                ),
                stable_artifact_bytes=0,
                stable_fingerprint="sfp-v1:0:0000000000000000:0000000000000000",
                stable_fingerprint_bytes=0,
                probability_mass=0.0,
            )
        info = self.enumerator.write_root_artifacts(
            start,
            effective_end,
            guess_path,
            stable_path,
            include_stable_metadata=(
                include_stable_metadata or _stable_metadata_enabled(stable_path)
            ),
        )
        record_count = int(info["record_count"])
        if record_count == 0:
            self.exhausted = True
        if record_count < end - start:
            self.exhausted = True
        return CandidateRangeArtifact(
            record_count=record_count,
            payload_bytes=int(info["payload_bytes"]),
            artifact_uri=guess_path.resolve().as_uri(),
            artifact_sha256=_artifact_sha256(guess_path, verify_artifact),
            artifact_bytes=int(info["artifact_bytes"]),
            stable_artifact_uri=(
                stable_path.resolve().as_uri() if stable_path is not None else None
            ),
            stable_artifact_sha256=(
                _file_sha256(stable_path) if stable_path is not None else None
            ),
            stable_artifact_bytes=int(info["stable_bytes"]),
            stable_fingerprint=str(info.get("stable_fingerprint", "")) or None,
            stable_fingerprint_bytes=int(info.get("stable_fingerprint_bytes", 0)),
            probability_mass=float(info["probability_mass"]),
        )

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
            dag_repository_active_units=self.enumerator.active_units(),
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
            group = self._next_canonical_group()
            if not group:
                return
            skip_count = min(index - self._cache_base, len(group))
            self._cache_base += skip_count
            self._reclaimed_records += skip_count
            if skip_count < len(group):
                self._cache.extend(group[skip_count:])
                self._update_peak_cached_records()
                return

    def _ensure(self, end: int) -> None:
        if end > self.max_records:
            raise RuntimeError("requested range exceeds configured source limit")
        while self._cache_base + len(self._cache) < end and not self.exhausted:
            group = self._next_canonical_group()
            if not group:
                return
            self._cache.extend(group)
            self._update_peak_cached_records()

    def _next_canonical_group(self) -> tuple[GuessRecord, ...]:
        first = self._pop_next_raw_record()
        if first is None:
            return ()
        group_key = _probability_group_key(first)
        group = [first]
        while True:
            record = self._pop_next_raw_record()
            if record is None:
                break
            if _probability_group_key(record) != group_key:
                self._lookahead_record = record
                break
            group.append(record)
        return tuple(sorted(group, key=_canonical_tie_key))

    def _pop_next_raw_record(self) -> GuessRecord | None:
        if self._lookahead_record is not None:
            record = self._lookahead_record
            self._lookahead_record = None
            return record
        while not self._raw_buffer and not self._raw_exhausted:
            if self._raw_index >= self.max_records:
                self._raw_exhausted = True
                return None
            start = self._raw_index
            end = min(self.max_records, self._raw_index + self._raw_fetch_size)
            records = tuple(self.enumerator.iter_root_raw_records(start, end))
            self._raw_index += len(records)
            if not records:
                self._raw_exhausted = True
                return None
            self._raw_buffer.extend(records)
            if len(records) < end - start:
                self._raw_exhausted = True
        if not self._raw_buffer:
            self.exhausted = True
            return None
        return self._raw_buffer.popleft()

    def _restart_from_zero(self) -> None:
        if self.model_path.suffix == ".bin":
            self.enumerator = CppOptimizedCQDAGEnumerator.from_binary_file(self.model_path)
        else:
            self.enumerator = CppOptimizedCQDAGEnumerator.from_json_file(self.model_path)
        self._cache = []
        self._cache_base = 0
        self._raw_index = 0
        self._raw_buffer.clear()
        self._lookahead_record = None
        self._raw_exhausted = False
        self.exhausted = False

    def _update_peak_cached_records(self) -> None:
        self._peak_cached_records = max(self._peak_cached_records, len(self._cache))


def _probability_group_key(record: GuessRecord) -> str:
    return f"{record.prob:.{_SERIAL_PROBABILITY_SIGNIFICANT_DIGITS}g}"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_sha256(path: Path, verify_artifact: bool) -> str:
    if verify_artifact:
        return _file_sha256(path)
    return UNCHECKED_ARTIFACT_SHA256


def _write_record_artifact(
    records: Sequence[GuessRecord],
    *,
    guess_path: str | Path,
    stable_path: str | Path | None = None,
    verify_artifact: bool = True,
    include_stable_metadata: bool = True,
) -> CandidateRangeArtifact:
    guess_path = Path(guess_path)
    stable_path = None if stable_path is None else Path(stable_path)
    guess_path.parent.mkdir(parents=True, exist_ok=True)
    if stable_path is not None:
        stable_path.parent.mkdir(parents=True, exist_ok=True)

    guess_buffer = bytearray()
    stable_buffer = bytearray()
    payload_bytes = 0
    compute_stable_metadata = include_stable_metadata or stable_path is not None
    fingerprint = StableStreamFingerprint() if compute_stable_metadata else None
    with guess_path.open("wb") as guess_out:
        stable_out = stable_path.open("wb") if stable_path is not None else None
        try:
            for record in records:
                guess_line = record.guess.encode("utf-8")
                guess_buffer.extend(guess_line)
                guess_buffer.append(0x0A)
                payload_bytes += len(guess_line) + 1
                if fingerprint is not None:
                    fingerprint = fingerprint.update_bytes(canonical_record_bytes(record))
                if stable_path is not None:
                    stable_line = stable_record_string(record).encode("utf-8")
                    stable_buffer.extend(stable_line)
                    stable_buffer.append(0x0A)
                if len(guess_buffer) >= _ARTIFACT_WRITE_BUFFER_BYTES:
                    guess_out.write(guess_buffer)
                    guess_buffer.clear()
                if len(stable_buffer) >= _ARTIFACT_WRITE_BUFFER_BYTES and stable_out is not None:
                    stable_out.write(stable_buffer)
                    stable_buffer.clear()
            if guess_buffer:
                guess_out.write(guess_buffer)
            if stable_buffer and stable_out is not None:
                stable_out.write(stable_buffer)
        finally:
            if stable_out is not None:
                stable_out.close()

    return CandidateRangeArtifact(
        record_count=len(records),
        payload_bytes=payload_bytes,
        artifact_uri=guess_path.resolve().as_uri(),
        artifact_sha256=_artifact_sha256(guess_path, verify_artifact),
        artifact_bytes=guess_path.stat().st_size,
        stable_artifact_uri=(
            stable_path.resolve().as_uri() if stable_path is not None else None
        ),
        stable_artifact_sha256=(
            _file_sha256(stable_path) if stable_path is not None else None
        ),
        stable_artifact_bytes=stable_path.stat().st_size if stable_path is not None else 0,
        stable_fingerprint=(
            fingerprint.to_string("rfp-v1") if fingerprint is not None else None
        ),
        stable_fingerprint_bytes=(
            fingerprint.byte_length if fingerprint is not None else 0
        ),
        probability_mass=fsum(record.prob for record in records),
    )


def _probability_sort_value(record: GuessRecord) -> float:
    return float(_probability_group_key(record))


def _canonical_tie_key(record: GuessRecord) -> tuple[int, tuple[int, ...], str]:
    return (record.structure_index, record.ranks, record.guess)


class CQDAGStructureRecordSource:
    """Local-result provider where each CQDAGPCFG structure is a protocol node."""

    def __init__(
        self,
        model,
        *,
        max_records_per_structure: int,
        adapter: "CQDAGBlockGraphAdapter | None" = None,
        prefer_cpp: bool = False,
    ) -> None:
        if max_records_per_structure < 0:
            raise ValueError("max_records_per_structure cannot be negative")
        self.model = model
        self.max_records_per_structure = max_records_per_structure
        self.prefer_cpp = prefer_cpp
        self.adapter = CQDAGBlockGraphAdapter(model) if adapter is None else adapter
        self.factory = None if prefer_cpp else OptimizedBlockFactory(model)
        self.cpp_enumerator = CppOptimizedCQDAGEnumerator(model) if prefer_cpp else None
        self._descriptors = {
            descriptor.node_id: descriptor for descriptor in self.adapter.structure_nodes()
        }
        self._streams: dict[NodeId, _StructureLocalStream | _CppStructureLocalStream] = {}

    def read_range(
        self,
        node_id: NodeId,
        start: int,
        end: int,
    ) -> Sequence[GuessRecord]:
        if start < 0 or end < start:
            raise ValueError("invalid source range")
        descriptor = self._descriptor_for_node(node_id)
        lane = self._lane_for_descriptor(descriptor)
        if lane is None:
            stream = self._stream_for(node_id)
            return stream.read_range(start, end)
        effective_end = min(end, lane.length)
        if start >= effective_end:
            return ()
        stream = self._stream_for(node_id)
        return stream.read_range(lane.start + start, lane.start + effective_end)

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
        descriptor = self._descriptor_for_node(node_id)
        lane = self._lane_for_descriptor(descriptor)
        stream = self._stream_for(node_id)
        if lane is not None:
            effective_end = min(end, lane.length)
            guess_path = Path(guess_path)
            stable_path = None if stable_path is None else Path(stable_path)
            if start >= effective_end:
                return _empty_candidate_artifact(
                    guess_path=guess_path,
                    stable_path=stable_path,
                    verify_artifact=verify_artifact,
                )
            start = lane.start + start
            end = lane.start + effective_end
        writer = getattr(stream, "write_range_artifact", None)
        if callable(writer):
            return writer(
                start,
                end,
                guess_path=guess_path,
                stable_path=stable_path,
                verify_artifact=verify_artifact,
                include_stable_metadata=include_stable_metadata,
            )
        return _write_record_artifact(
            stream.read_range(start, end),
            guess_path=guess_path,
            stable_path=stable_path,
            verify_artifact=verify_artifact,
            include_stable_metadata=include_stable_metadata,
        )

    def reclaim_before(self, node_id: NodeId, index: int) -> int:
        descriptor = self._descriptor_for_node(node_id)
        lane = self._lane_for_descriptor(descriptor)
        stream = self._streams.get(node_id)
        if stream is None:
            return 0
        if lane is not None:
            before = max(0, min(stream.reclaimed_records, lane.end) - lane.start)
            stream.reclaim_before(lane.start + min(index, lane.length))
            after = max(0, min(stream.reclaimed_records, lane.end) - lane.start)
            return max(0, after - before)
        return stream.reclaim_before(index)

    def stats(self) -> CQDAGSourceReclaimStats:
        reclaimed_records = 0
        for node_id, stream in self._streams.items():
            descriptor = self._descriptor_for_node(node_id)
            lane = self._lane_for_descriptor(descriptor)
            if lane is None:
                reclaimed_records += stream.reclaimed_records
            else:
                reclaimed_records += max(
                    0,
                    min(stream.reclaimed_records, lane.end) - lane.start,
                )
        return CQDAGSourceReclaimStats(
            node_count=len(self._descriptors),
            cached_records=sum(stream.cached_records for stream in self._streams.values()),
            peak_cached_records=sum(stream.peak_cached_records for stream in self._streams.values()),
            reclaimed_records=reclaimed_records,
            dag_repository_active_units=self._repository_active_units(),
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
            tuple(stream.block for _, stream in streams if hasattr(stream, "block"))
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

        block_by_signature = (
            dict(_iter_repository_blocks(self.factory))
            if self.factory is not None
            else {}
        )
        for stream in self._streams.values():
            if hasattr(stream, "block"):
                block_by_signature.setdefault(tuple(stream.structure.symbols), stream.block)

        if self.factory is not None:
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
    ) -> tuple[tuple[NodeId, "_StructureLocalStream | _CppStructureLocalStream"], ...]:
        if node_ids is None:
            return tuple(self._streams.items())
        selected: list[tuple[NodeId, _StructureLocalStream | _CppStructureLocalStream]] = []
        for node_id in node_ids:
            selected.append((node_id, self._stream_for(node_id)))
        return tuple(selected)

    def _stream_for(self, node_id: NodeId) -> "_StructureLocalStream | _CppStructureLocalStream":
        stream = self._streams.get(node_id)
        if stream is not None:
            return stream
        descriptor = self._descriptor_for_node(node_id)
        lane = self._lane_for_descriptor(descriptor)
        structure_index = _require_structure_index(descriptor)
        max_records = (
            min(self.max_records_per_structure, lane.end)
            if lane is not None
            else self.max_records_per_structure
        )
        if self.cpp_enumerator is not None:
            stream = _CppStructureLocalStream(
                self.model,
                enumerator=self.cpp_enumerator,
                structure_index=structure_index,
                max_records=max_records,
            )
        else:
            if self.factory is None:
                raise RuntimeError("CQDAG structure source factory is not initialized")
            stream = _StructureLocalStream(
                self.model,
                factory=self.factory,
                structure_index=structure_index,
                max_records=max_records,
            )
        self._streams[node_id] = stream
        return stream

    def _descriptor_for_node(self, node_id: NodeId) -> BlockNodeDescriptor:
        descriptor = self._descriptors.get(node_id)
        if descriptor is not None:
            return descriptor
        lane_parts = parse_structure_rank_lane_node_id(node_id)
        range_parts = parse_structure_rank_range_node_id(node_id)
        if lane_parts is None and range_parts is None:
            raise KeyError(f"unknown CQDAG structure source node: {node_id}")
        if range_parts is not None:
            structure_index, start, end, structure_name = range_parts
            lane_index = None
            lane_count = 1
        else:
            assert lane_parts is not None
            structure_index, lane_index, lane_count, structure_name = lane_parts
        base_node_id = NodeId(f"structure:{structure_index}:{structure_name}")
        base = self._descriptors.get(base_node_id)
        if base is None:
            raise KeyError(f"unknown CQDAG structure source node: {node_id}")
        lanes = min(lane_count, max(1, base.cardinality))
        if range_parts is None:
            assert lane_index is not None
            start, end = structure_rank_lane_bounds(
                cardinality=base.cardinality,
                lane_index=lane_index,
                lane_count=lanes,
            )
        else:
            start = min(start, base.cardinality)
            end = min(end, base.cardinality)
        if end <= start:
            raise KeyError(f"empty CQDAG structure lane node: {node_id}")
        descriptor = replace(
            base,
            node_id=node_id,
            name=(
                f"{base.name}/rank{start}-{end}"
                if range_parts is not None
                else f"{base.name}/lane{lane_index + 1}of{lanes}"
            ),
            cardinality=end - start,
            lane_index=lane_index,
            lane_count=lanes,
            rank_start=start,
            rank_end=end,
        )
        self._descriptors[node_id] = descriptor
        return descriptor

    def _lane_for_descriptor(
        self,
        descriptor: BlockNodeDescriptor,
    ) -> StructureRankLane | None:
        if descriptor.rank_end is None:
            return None
        structure_index = _require_structure_index(descriptor)
        structure_name = self.model.structures[structure_index].name
        return StructureRankLane(
            structure_index=structure_index,
            structure_name=structure_name,
            lane_index=0 if descriptor.lane_index is None else descriptor.lane_index,
            lane_count=descriptor.lane_count,
            start=descriptor.rank_start,
            end=descriptor.rank_end,
        )

    def _repository_active_units(self) -> int:
        if self.cpp_enumerator is not None:
            return self.cpp_enumerator.active_units()
        if self.factory is None:
            return 0
        return self.factory.active_units()


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

    def structure_rank_lane_nodes(
        self,
        *,
        lane_count: int,
        rank_horizon: int | None = None,
        split_strategy: str = "equal_rank",
        mass_bias: float = DEFAULT_PROBABILITY_MASS_LANE_BIAS,
    ) -> tuple[BlockNodeDescriptor, ...]:
        if lane_count <= 0:
            raise ValueError("lane_count must be positive")
        if split_strategy not in {"equal_rank", "probability_mass"}:
            raise ValueError("split_strategy must be equal_rank or probability_mass")
        if rank_horizon is not None and rank_horizon < 0:
            raise ValueError("rank_horizon cannot be negative")
        if mass_bias <= 0.0:
            raise ValueError("mass_bias must be positive")
        nodes: list[BlockNodeDescriptor] = []
        for node in self.structure_nodes():
            horizon = (
                node.cardinality
                if rank_horizon is None
                else min(node.cardinality, rank_horizon)
            )
            lanes = min(lane_count, max(1, horizon))
            for lane_index in range(lanes):
                if split_strategy == "probability_mass":
                    start, end = structure_probability_mass_lane_bounds(
                        cardinality=node.cardinality,
                        lane_index=lane_index,
                        lane_count=lanes,
                        rank_horizon=horizon,
                        mass_bias=mass_bias,
                    )
                else:
                    start, end = structure_rank_lane_bounds(
                        cardinality=node.cardinality,
                        lane_index=lane_index,
                        lane_count=lanes,
                    )
                if end <= start:
                    continue
                if split_strategy == "probability_mass":
                    mass_start = _probability_mass_fraction(
                        start,
                        horizon=max(1, horizon),
                        mass_bias=mass_bias,
                    )
                    mass_end = _probability_mass_fraction(
                        end,
                        horizon=max(1, horizon),
                        mass_bias=mass_bias,
                    )
                    lane_mass_fraction = max(0.0, mass_end - mass_start)
                    lane_decay = 1.0 / (1.0 + lane_index)
                    priority = node.priority * lane_mass_fraction * lane_decay
                    base_prob = node.base_prob * lane_mass_fraction
                    node_id = structure_rank_range_node_id(
                        _require_structure_index(node),
                        node.name,
                        start,
                        end,
                    )
                    name = f"{node.name}/masslane{lane_index + 1}of{lanes}"
                else:
                    # Earlier rank lanes are more valuable in cracking mode.  We
                    # keep the structure probability as the dominant signal and use
                    # a mild lane decay so the scheduler prefers front lanes without
                    # starving high-mass neighboring structures.
                    lane_decay = 1.0 / (1.0 + lane_index)
                    priority = node.priority * lane_decay
                    base_prob = node.base_prob * lane_decay
                    node_id = structure_rank_lane_node_id(
                        _require_structure_index(node),
                        node.name,
                        lane_index,
                        lanes,
                    )
                    name = f"{node.name}/lane{lane_index + 1}of{lanes}"
                nodes.append(
                    replace(
                        node,
                        node_id=node_id,
                        name=name,
                        priority=priority,
                        base_prob=base_prob,
                        cardinality=end - start,
                        lane_index=lane_index,
                        lane_count=lanes,
                        rank_start=start,
                        rank_end=end,
                    )
                )
        return tuple(nodes)

    def scheduling_features(self) -> tuple[NodeSchedulingFeatures, ...]:
        if self._scheduling_features_cache is not None:
            return self._scheduling_features_cache
        self._scheduling_features_cache = tuple(
            NodeSchedulingFeatures(
                node_id=node.node_id,
                entropy=node.slot_dispersion,
                priority=node.priority,
                estimated_cost=node.estimated_cost,
                cardinality=node.cardinality,
            )
            for node in self.structure_nodes()
        )
        return self._scheduling_features_cache

    def scheduling_features_for(
        self,
        nodes: Sequence[BlockNodeDescriptor],
    ) -> tuple[NodeSchedulingFeatures, ...]:
        return tuple(
            NodeSchedulingFeatures(
                node_id=node.node_id,
                entropy=node.slot_dispersion,
                priority=node.priority,
                estimated_cost=node.estimated_cost,
                cardinality=node.cardinality,
            )
            for node in nodes
        )

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
        return (
            -_probability_sort_value(record),
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


class _CppStructureLocalStream:
    def __init__(
        self,
        model,
        *,
        enumerator: CppOptimizedCQDAGEnumerator,
        structure_index: int,
        max_records: int,
    ) -> None:
        self.model = model
        self.enumerator = enumerator
        self.structure_index = structure_index
        self.structure = model.structures[structure_index]
        self.max_records = max_records
        self._cache: list[GuessRecord] = []
        self._cache_base = 0
        self._raw_index = 0
        self._raw_buffer: deque[GuessRecord] = deque()
        self._lookahead_record: GuessRecord | None = None
        self._peak_cached_records = 0
        self._reclaimed_records = 0
        self.exhausted = False

    def read_range(self, start: int, end: int) -> tuple[GuessRecord, ...]:
        if start < 0 or end < start:
            raise ValueError("invalid structure source range")
        if start < self._cache_base:
            raise RuntimeError("requested range was already reclaimed by the C++ source")
        effective_end = min(end, self.max_records)
        if start >= effective_end:
            self.exhausted = True
            return ()
        if start > self.ready_end:
            self._skip_to(start)
        self._ensure(effective_end)
        if effective_end < end:
            self.exhausted = True
        return tuple(
            self._cache[start - self._cache_base : effective_end - self._cache_base]
        )

    def write_range_artifact(
        self,
        start: int,
        end: int,
        *,
        guess_path: str | Path,
        stable_path: str | Path | None = None,
        verify_artifact: bool = True,
        include_stable_metadata: bool = True,
    ) -> CandidateRangeArtifact:
        if start < 0 or end < start:
            raise ValueError("invalid structure source range")
        use_isolated_writer = start < self._cache_base
        guess_path = Path(guess_path)
        stable_path = None if stable_path is None else Path(stable_path)
        effective_end = min(end, self.max_records)
        if start >= effective_end:
            self.exhausted = True
            return _empty_candidate_artifact(
                guess_path=guess_path,
                stable_path=stable_path,
                verify_artifact=verify_artifact,
            )
        guess_path.parent.mkdir(parents=True, exist_ok=True)
        if stable_path is not None:
            stable_path.parent.mkdir(parents=True, exist_ok=True)
        writer = getattr(self.enumerator, "write_structure_artifacts", None)
        if not callable(writer):
            return _write_record_artifact(
                self.read_range(start, effective_end),
                guess_path=guess_path,
                stable_path=stable_path,
                verify_artifact=verify_artifact,
                include_stable_metadata=include_stable_metadata,
            )
        try:
            if use_isolated_writer:
                raise RuntimeError("reclaimed range requires isolated C++ writer")
            info = writer(
                self.structure_index,
                start,
                effective_end,
                guess_path,
                stable_path,
                include_stable_metadata=(
                    include_stable_metadata or _stable_metadata_enabled(stable_path)
                ),
            )
        except RuntimeError as exc:
            if "reclaimed" not in str(exc):
                raise
            isolated = CppOptimizedCQDAGEnumerator(self.model)
            info = isolated.write_structure_artifacts(
                self.structure_index,
                start,
                effective_end,
                guess_path,
                stable_path,
                include_stable_metadata=(
                    include_stable_metadata or _stable_metadata_enabled(stable_path)
                ),
            )
        record_count = int(info["record_count"])
        new_base = start + record_count
        if new_base >= self._cache_base:
            self._cache = []
            self._cache_base = new_base
            self._raw_index = self._cache_base
            self._raw_buffer.clear()
            self._lookahead_record = None
            self._reclaimed_records = max(self._reclaimed_records, self._cache_base)
        if effective_end < end or record_count < effective_end - start:
            self.exhausted = True
        return CandidateRangeArtifact(
            record_count=record_count,
            payload_bytes=int(info["payload_bytes"]),
            artifact_uri=guess_path.resolve().as_uri(),
            artifact_sha256=_artifact_sha256(guess_path, verify_artifact),
            artifact_bytes=int(info["artifact_bytes"]),
            stable_artifact_uri=(
                stable_path.resolve().as_uri() if stable_path is not None else None
            ),
            stable_artifact_sha256=(
                _file_sha256(stable_path) if stable_path is not None else None
            ),
            stable_artifact_bytes=int(info["stable_bytes"]),
            stable_fingerprint=str(info.get("stable_fingerprint", "")) or None,
            stable_fingerprint_bytes=int(info.get("stable_fingerprint_bytes", 0)),
            probability_mass=float(info["probability_mass"]),
        )

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
        return self.enumerator.structure_active_units(self.structure_index)

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
        del self._cache[:reclaimed]
        self._cache_base = reclaim_end
        self._reclaimed_records += reclaimed
        return self._cache_base

    def _skip_to(self, index: int) -> None:
        if index <= self.ready_end:
            return
        self.reclaim_before(min(index, self.ready_end))
        while self._cache_base < index:
            group = self._next_canonical_group()
            if not group:
                return
            skip_count = min(index - self._cache_base, len(group))
            self._cache_base += skip_count
            self._reclaimed_records += skip_count
            if skip_count < len(group):
                self._cache.extend(group[skip_count:])
                self._update_peak_cached_records()
                return

    def _ensure(self, end: int) -> None:
        while self.ready_end < end:
            group = self._next_canonical_group()
            if not group:
                self.exhausted = True
                return
            self._cache.extend(group)
            self._update_peak_cached_records()

    def _update_peak_cached_records(self) -> None:
        self._peak_cached_records = max(self._peak_cached_records, len(self._cache))

    def _next_canonical_group(self) -> tuple[GuessRecord, ...]:
        first = self._pop_next_raw_record()
        if first is None:
            return ()
        group_key = _probability_group_key(first)
        group = [first]
        while True:
            record = self._pop_next_raw_record()
            if record is None:
                break
            if _probability_group_key(record) != group_key:
                self._lookahead_record = record
                break
            group.append(record)
        return tuple(sorted(group, key=_canonical_tie_key))

    def _pop_next_raw_record(self) -> GuessRecord | None:
        if self._lookahead_record is not None:
            record = self._lookahead_record
            self._lookahead_record = None
            return record
        if self.exhausted and not self._raw_buffer:
            return None
        if not self._raw_buffer:
            self._fill_raw_buffer()
        if not self._raw_buffer:
            return None
        return self._raw_buffer.popleft()

    def _fill_raw_buffer(self) -> None:
        if self.exhausted or self._raw_index >= self.max_records:
            self.exhausted = True
            return
        raw_start = self._raw_index
        raw_end = min(self.max_records, raw_start + _SOURCE_SKIP_ACK_WINDOW)
        records = tuple(
            self.enumerator.iter_structure_records(
                self.structure_index,
                raw_start,
                raw_end,
            )
        )
        self._raw_index += len(records)
        self._raw_buffer.extend(records)
        if len(records) < raw_end - raw_start:
            self.exhausted = True

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
            consumer_id=0,
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
        if snapshot.guess_cache.entries:
            self._cache_base = snapshot.guess_cache.base
            self._cache = tuple_entry_records(snapshot.guess_cache.entries)
        else:
            self._cache_base = snapshot.ready_end
            self._cache = []
        self.enumerator.restore_structure_state(
            self.structure_index,
            self.ready_end,
            exhausted=False,
        )
        self._raw_index = self.ready_end
        self._raw_buffer.clear()
        self._lookahead_record = None
        self._peak_cached_records = max(self._peak_cached_records, len(self._cache))
        self._reclaimed_records = max(self._reclaimed_records, self._cache_base)
        self.exhausted = False


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
        self._raw_index = 0
        self._lookahead_record: GuessRecord | None = None
        self._peak_cached_records = 0
        self._reclaimed_records = 0
        self.exhausted = False

    def read_range(self, start: int, end: int) -> tuple[GuessRecord, ...]:
        if start < 0 or end < start:
            raise ValueError("invalid structure source range")
        if start < self._cache_base:
            self._restart_from_zero()
        effective_end = min(end, self.max_records)
        if start >= effective_end:
            self.exhausted = True
            return ()
        if start > self.ready_end:
            self._skip_to(start)
        self._ensure(effective_end)
        if effective_end < end:
            self.exhausted = True
        return tuple(
            self._cache[start - self._cache_base : effective_end - self._cache_base]
        )

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
        del self._cache[:reclaimed]
        self._cache_base = reclaim_end
        self._reclaimed_records += reclaimed
        return self._cache_base

    def _skip_to(self, index: int) -> None:
        if index <= self.ready_end:
            return
        self.reclaim_before(min(index, self.ready_end))
        while self._cache_base < index:
            group = self._next_canonical_group()
            if not group:
                return
            skip_count = min(index - self._cache_base, len(group))
            self._cache_base += skip_count
            self._reclaimed_records += skip_count
            if skip_count < len(group):
                self._cache.extend(group[skip_count:])
                self._update_peak_cached_records()
                return

    def _ensure(self, end: int) -> None:
        while self._cache_base + len(self._cache) < end:
            group = self._next_canonical_group()
            if not group:
                return
            self._cache.extend(group)
            self._update_peak_cached_records()

    def _update_peak_cached_records(self) -> None:
        self._peak_cached_records = max(self._peak_cached_records, len(self._cache))

    def _restart_from_zero(self) -> None:
        self.factory = OptimizedBlockFactory(self.model)
        self.block = self.factory.get_block(self.structure.symbols)
        self.consumer_id = self.block.register_consumer()
        self._cache = []
        self._cache_base = 0
        self._raw_index = 0
        self._lookahead_record = None
        self.exhausted = False

    def _next_canonical_group(self) -> tuple[GuessRecord, ...]:
        first = self._pop_next_raw_record()
        if first is None:
            return ()
        group_key = _probability_group_key(first)
        group = [first]
        while True:
            record = self._pop_next_raw_record()
            if record is None:
                break
            if _probability_group_key(record) != group_key:
                self._lookahead_record = record
                break
            group.append(record)
        return tuple(sorted(group, key=_canonical_tie_key))

    def _pop_next_raw_record(self) -> GuessRecord | None:
        if self._lookahead_record is not None:
            record = self._lookahead_record
            self._lookahead_record = None
            return record
        if self.exhausted:
            return None
        index = self._raw_index
        if index >= self.max_records:
            self.exhausted = True
            return None
        try:
            if index >= self.block.produced_count():
                self.block.ensure_generated(index)
            local_log_prob, rank_key = self.block.get_generated(index)
        except IndexError:
            self.exhausted = True
            return None
        self.block.ack(self.consumer_id, index)
        self._raw_index += 1
        return self.model.record_for_log_prob(
            self.structure_index,
            flatten_rank_key(rank_key),
            self.structure.log_base_prob + local_log_prob,
        )

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
        self._raw_index = self.ready_end
        self._lookahead_record = None
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


def _stable_metadata_enabled(stable_path: str | Path | None) -> bool:
    if stable_path is not None:
        return True
    return os.environ.get("CQPCFG_DISABLE_STABLE_METADATA", "").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }


__all__ = [
    "BlockNodeDescriptor",
    "CandidateRangeArtifact",
    "CQDAGBlockGraphAdapter",
    "CQDAGRecordSource",
    "CQDAGSourceReclaimStats",
    "CQDAGStructureRecordSource",
    "ROOT_NODE_ID",
    "StructureRankLane",
    "parse_structure_rank_lane_node_id",
    "parse_structure_rank_range_node_id",
    "slot_entropy",
    "slot_entropy_bound",
    "structure_probability_mass_lane_bounds",
    "structure_rank_lane_bounds",
    "structure_rank_lane_node_id",
    "structure_rank_range_node_id",
]
