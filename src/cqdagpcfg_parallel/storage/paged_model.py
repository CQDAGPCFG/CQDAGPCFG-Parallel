from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from math import ceil, log
from pathlib import Path
from typing import Any, Mapping

from .model_store import FileModelArtifactStore


@dataclass(frozen=True, slots=True)
class PagedSlotTableManifest:
    symbol: str
    cardinality: int
    unknown_prob: float
    access_weight: float
    page_size: int
    page_count: int


@dataclass(frozen=True, slots=True)
class PagedModelManifest:
    model_id: str
    model_fingerprint: str
    structure_count: int
    structure_page_size: int
    structure_page_count: int
    slot_tables: tuple[PagedSlotTableManifest, ...]
    metadata: Mapping[str, Any]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "PagedModelManifest":
        return cls(
            model_id=str(raw["model_id"]),
            model_fingerprint=str(raw["model_fingerprint"]),
            structure_count=int(raw["structure_count"]),
            structure_page_size=int(raw["structure_page_size"]),
            structure_page_count=int(raw["structure_page_count"]),
            slot_tables=tuple(
                PagedSlotTableManifest(
                    symbol=str(item["symbol"]),
                    cardinality=int(item["cardinality"]),
                    unknown_prob=float(item["unknown_prob"]),
                    access_weight=float(item["access_weight"]),
                    page_size=int(item["page_size"]),
                    page_count=int(item["page_count"]),
                )
                for item in raw["slot_tables"]
            ),
            metadata=dict(raw.get("metadata", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "model_fingerprint": self.model_fingerprint,
            "structure_count": self.structure_count,
            "structure_page_size": self.structure_page_size,
            "structure_page_count": self.structure_page_count,
            "slot_tables": [asdict(item) for item in self.slot_tables],
            "metadata": dict(self.metadata),
        }

    def slot(self, symbol: str) -> PagedSlotTableManifest:
        for table in self.slot_tables:
            if table.symbol == symbol:
                return table
        raise KeyError(f"unknown slot table: {symbol}")


@dataclass(frozen=True, slots=True)
class ModelJsonPage:
    model_fingerprint: str
    page_id: str
    data: Mapping[str, Any]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ModelJsonPage":
        return cls(
            model_fingerprint=str(raw["model_fingerprint"]),
            page_id=str(raw["page_id"]),
            data=dict(raw["data"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_fingerprint": self.model_fingerprint,
            "page_id": self.page_id,
            "data": dict(self.data),
        }


class FilePagedModelArtifactStore(FileModelArtifactStore):
    """File-backed artifact store plus JSON model pages.

    Tracker owns the canonical model file. Workers can fetch a small manifest,
    structure pages, and slot-table entry pages without receiving the full JSON
    artifact.
    """

    def __init__(self) -> None:
        super().__init__()
        self._paged_manifests: dict[str, PagedModelManifest] = {}
        self._pages: dict[tuple[str, str], ModelJsonPage] = {}

    @classmethod
    def from_path(
        cls,
        path: Path,
        *,
        model_id: str = "cqdagpcfg-model",
        artifact_uri: str | None = None,
        chunk_size: int = 1 << 20,
        slot_page_size: int = 1024,
        structure_page_size: int = 4096,
    ) -> "FilePagedModelArtifactStore":
        store = cls()
        store.put_file(
            path,
            model_id=model_id,
            artifact_uri=artifact_uri,
            chunk_size=chunk_size,
        )
        store.put_paged_model(
            path,
            model_id=model_id,
            slot_page_size=slot_page_size,
            structure_page_size=structure_page_size,
        )
        return store

    def put_paged_model(
        self,
        path: Path,
        *,
        model_id: str,
        slot_page_size: int = 1024,
        structure_page_size: int = 4096,
    ) -> PagedModelManifest:
        if slot_page_size <= 0:
            raise ValueError("slot_page_size must be positive")
        if structure_page_size <= 0:
            raise ValueError("structure_page_size must be positive")
        artifact_manifest = self.manifest_for_model(model_id)
        raw = json.loads(path.read_text(encoding="utf-8"))
        structures = tuple(dict(item) for item in raw["structures"])
        slot_tables_raw = {
            str(symbol): dict(table)
            for symbol, table in raw["slot_tables"].items()
        }

        slot_manifests: list[PagedSlotTableManifest] = []
        for symbol, table in sorted(slot_tables_raw.items()):
            entries = tuple(dict(entry) for entry in table["entries"])
            page_count = ceil(len(entries) / slot_page_size) if entries else 0
            slot_manifest = PagedSlotTableManifest(
                symbol=symbol,
                cardinality=len(entries),
                unknown_prob=float(table.get("unknown_prob", 0.0)),
                access_weight=_slot_access_weight(entries),
                page_size=slot_page_size,
                page_count=page_count,
            )
            slot_manifests.append(slot_manifest)
            for page_index in range(page_count):
                start = page_index * slot_page_size
                end = min(len(entries), start + slot_page_size)
                page_id = slot_page_id(symbol, page_index)
                self._pages[(artifact_manifest.model_fingerprint, page_id)] = ModelJsonPage(
                    model_fingerprint=artifact_manifest.model_fingerprint,
                    page_id=page_id,
                    data={
                        "kind": "slot_entries",
                        "symbol": symbol,
                        "start": start,
                        "entries": list(entries[start:end]),
                    },
                )

        structure_page_count = (
            ceil(len(structures) / structure_page_size) if structures else 0
        )
        for page_index in range(structure_page_count):
            start = page_index * structure_page_size
            end = min(len(structures), start + structure_page_size)
            page_id = structure_page_id(page_index)
            self._pages[(artifact_manifest.model_fingerprint, page_id)] = ModelJsonPage(
                model_fingerprint=artifact_manifest.model_fingerprint,
                page_id=page_id,
                data={
                    "kind": "structures",
                    "start": start,
                    "structures": list(structures[start:end]),
                },
            )

        manifest = PagedModelManifest(
            model_id=model_id,
            model_fingerprint=artifact_manifest.model_fingerprint,
            structure_count=len(structures),
            structure_page_size=structure_page_size,
            structure_page_count=structure_page_count,
            slot_tables=tuple(slot_manifests),
            metadata=dict(raw.get("metadata", {})),
        )
        self._paged_manifests[model_id] = manifest
        return manifest

    def paged_manifest_for_model(self, model_id: str) -> PagedModelManifest:
        try:
            return self._paged_manifests[model_id]
        except KeyError as exc:
            raise KeyError(f"unknown paged model_id: {model_id}") from exc

    def fetch_page(self, fingerprint: str, *, page_id: str) -> ModelJsonPage:
        try:
            return self._pages[(fingerprint, page_id)]
        except KeyError as exc:
            raise KeyError(f"unknown model page: {fingerprint} {page_id}") from exc


def structure_page_id(page_index: int) -> str:
    if page_index < 0:
        raise ValueError("page_index cannot be negative")
    return f"structures:{page_index}"


def slot_page_id(symbol: str, page_index: int) -> str:
    if page_index < 0:
        raise ValueError("page_index cannot be negative")
    return f"slot:{symbol}:{page_index}"


def _slot_access_weight(entries: tuple[Mapping[str, Any], ...]) -> float:
    entropy = 0.0
    for entry in entries:
        prob = float(entry["prob"])
        if prob > 0.0:
            entropy -= prob * log(prob)
    return max(entropy, 1e-12)


__all__ = [
    "FilePagedModelArtifactStore",
    "ModelJsonPage",
    "PagedModelManifest",
    "PagedSlotTableManifest",
    "slot_page_id",
    "structure_page_id",
]
