from __future__ import annotations

import json
import tempfile
from dataclasses import asdict, dataclass
from hashlib import sha256
from math import ceil, log
from pathlib import Path
from typing import Any, Mapping

from .model_store import DEFAULT_MODEL_CHUNK_SIZE, FileModelArtifactStore


DEFAULT_SLOT_PAGE_SIZE = 1024
DEFAULT_STRUCTURE_PAGE_SIZE = 4096
MIN_SLOT_ACCESS_WEIGHT = 1e-12


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

    def __init__(self, *, page_root: Path | None = None) -> None:
        super().__init__()
        self._paged_manifests: dict[str, PagedModelManifest] = {}
        self._owned_page_root = (
            tempfile.TemporaryDirectory(prefix="cqdagpcfg-model-pages-")
            if page_root is None
            else None
        )
        self.page_root = (
            Path(self._owned_page_root.name)
            if self._owned_page_root is not None
            else page_root
        )
        self.page_root.mkdir(parents=True, exist_ok=True)
        self._page_paths: dict[tuple[str, str], Path] = {}

    @classmethod
    def from_path(
        cls,
        path: Path,
        *,
        model_id: str = "cqdagpcfg-model",
        artifact_uri: str | None = None,
        chunk_size: int = DEFAULT_MODEL_CHUNK_SIZE,
        slot_page_size: int = DEFAULT_SLOT_PAGE_SIZE,
        structure_page_size: int = DEFAULT_STRUCTURE_PAGE_SIZE,
        page_root: Path | None = None,
    ) -> "FilePagedModelArtifactStore":
        store = cls(page_root=page_root)
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
        slot_page_size: int = DEFAULT_SLOT_PAGE_SIZE,
        structure_page_size: int = DEFAULT_STRUCTURE_PAGE_SIZE,
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
            entries = _normalized_slot_entries(table)
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
                self._write_page(
                    ModelJsonPage(
                        model_fingerprint=artifact_manifest.model_fingerprint,
                        page_id=page_id,
                        data={
                            "kind": "slot_entries",
                            "symbol": symbol,
                            "start": start,
                            "entries": list(entries[start:end]),
                        },
                    )
                )

        structure_page_count = (
            ceil(len(structures) / structure_page_size) if structures else 0
        )
        for page_index in range(structure_page_count):
            start = page_index * structure_page_size
            end = min(len(structures), start + structure_page_size)
            page_id = structure_page_id(page_index)
            self._write_page(
                ModelJsonPage(
                    model_fingerprint=artifact_manifest.model_fingerprint,
                    page_id=page_id,
                    data={
                        "kind": "structures",
                        "start": start,
                        "structures": list(structures[start:end]),
                    },
                )
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
            path = self._page_paths[(fingerprint, page_id)]
        except KeyError as exc:
            raise KeyError(f"unknown model page: {fingerprint} {page_id}") from exc
        return ModelJsonPage.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def close(self) -> None:
        if self._owned_page_root is not None:
            self._owned_page_root.cleanup()
            self._owned_page_root = None

    def _write_page(self, page: ModelJsonPage) -> None:
        page_dir = self.page_root / _fingerprint_dirname(page.model_fingerprint)
        page_dir.mkdir(parents=True, exist_ok=True)
        path = page_dir / f"{_page_filename(page.page_id)}.json"
        tmp_path = path.with_suffix(".json.tmp")
        tmp_path.write_text(
            json.dumps(page.to_dict(), separators=(",", ":"), ensure_ascii=False),
            encoding="utf-8",
        )
        tmp_path.replace(path)
        self._page_paths[(page.model_fingerprint, page.page_id)] = path


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
    return max(entropy, MIN_SLOT_ACCESS_WEIGHT)


def _normalized_slot_entries(
    table: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    entries = tuple(dict(entry) for entry in table["entries"])
    if not entries:
        return ()
    unknown_prob = float(table.get("unknown_prob", 0.0))
    total = sum(float(entry["prob"]) for entry in entries)
    if total <= 0.0:
        raise ValueError("slot table has non-positive total probability")
    visible_mass = 1.0 - unknown_prob
    return tuple(
        {
            **entry,
            "prob": (float(entry["prob"]) / total) * visible_mass,
        }
        for entry in entries
    )


def _fingerprint_dirname(fingerprint: str) -> str:
    return fingerprint.replace(":", "_")


def _page_filename(page_id: str) -> str:
    return sha256(page_id.encode("utf-8")).hexdigest()


__all__ = [
    "DEFAULT_SLOT_PAGE_SIZE",
    "DEFAULT_STRUCTURE_PAGE_SIZE",
    "FilePagedModelArtifactStore",
    "MIN_SLOT_ACCESS_WEIGHT",
    "ModelJsonPage",
    "PagedModelManifest",
    "PagedSlotTableManifest",
    "slot_page_id",
    "structure_page_id",
]
