from __future__ import annotations

import json
from collections import OrderedDict
from dataclasses import dataclass
from hashlib import sha256
from math import ceil
from pathlib import Path
from typing import Protocol

from .manifest import model_fingerprint


@dataclass(frozen=True, slots=True)
class ModelArtifactManifest:
    model_id: str
    model_fingerprint: str
    size_bytes: int
    chunk_size: int
    chunk_count: int
    artifact_uri: str | None = None

    def __post_init__(self) -> None:
        if not self.model_id:
            raise ValueError("model_id cannot be empty")
        if not self.model_fingerprint:
            raise ValueError("model_fingerprint cannot be empty")
        if self.size_bytes < 0:
            raise ValueError("size_bytes cannot be negative")
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self.chunk_count < 0:
            raise ValueError("chunk_count cannot be negative")


@dataclass(frozen=True, slots=True)
class ModelArtifactChunk:
    model_fingerprint: str
    offset: int
    data: bytes
    final: bool

    @property
    def end_offset(self) -> int:
        return self.offset + len(self.data)


@dataclass(frozen=True, slots=True)
class ModelPageCacheStats:
    pages: int = 0
    bytes: int = 0
    hits: int = 0
    misses: int = 0
    evictions: int = 0


class ModelArtifactStore(Protocol):
    def manifest(self, fingerprint: str) -> ModelArtifactManifest: ...

    def manifest_for_model(self, model_id: str) -> ModelArtifactManifest: ...

    def fetch_chunk(self, fingerprint: str, *, offset: int) -> ModelArtifactChunk: ...


class InMemoryModelArtifactStore:
    """Small reference implementation of protocol-level model fetch.

    Real deployments can back the same manifest/chunk contract with HTTP,
    object storage, or local shared volumes.
    """

    def __init__(self) -> None:
        self._payloads: dict[str, bytes] = {}
        self._manifests: dict[str, ModelArtifactManifest] = {}
        self._fingerprint_by_model_id: dict[str, str] = {}

    def put_model(
        self,
        payload: str | bytes,
        *,
        model_id: str = "cqdagpcfg-model",
        artifact_uri: str | None = None,
        chunk_size: int = 1 << 20,
    ) -> ModelArtifactManifest:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        data = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        fingerprint = model_fingerprint(data)
        manifest = ModelArtifactManifest(
            model_id=model_id,
            model_fingerprint=fingerprint,
            size_bytes=len(data),
            chunk_size=chunk_size,
            chunk_count=ceil(len(data) / chunk_size) if data else 0,
            artifact_uri=artifact_uri,
        )
        self._payloads[fingerprint] = data
        self._manifests[fingerprint] = manifest
        self._fingerprint_by_model_id[model_id] = fingerprint
        return manifest

    def manifest(self, fingerprint: str) -> ModelArtifactManifest:
        try:
            return self._manifests[fingerprint]
        except KeyError as exc:
            raise KeyError(f"unknown model fingerprint: {fingerprint}") from exc

    def manifest_for_model(self, model_id: str) -> ModelArtifactManifest:
        try:
            fingerprint = self._fingerprint_by_model_id[model_id]
        except KeyError as exc:
            raise KeyError(f"unknown model_id: {model_id}") from exc
        return self.manifest(fingerprint)

    def fetch_chunk(self, fingerprint: str, *, offset: int) -> ModelArtifactChunk:
        if offset < 0:
            raise ValueError("offset cannot be negative")
        manifest = self.manifest(fingerprint)
        payload = self._payloads[fingerprint]
        if offset > len(payload):
            raise ValueError("offset is beyond model payload")
        end = min(len(payload), offset + manifest.chunk_size)
        return ModelArtifactChunk(
            model_fingerprint=fingerprint,
            offset=offset,
            data=payload[offset:end],
            final=end >= len(payload),
        )

    def fetch_all(self, fingerprint: str) -> bytes:
        manifest = self.manifest(fingerprint)
        chunks: list[bytes] = []
        offset = 0
        while offset < manifest.size_bytes:
            chunk = self.fetch_chunk(fingerprint, offset=offset)
            chunks.append(chunk.data)
            offset = chunk.end_offset
        return b"".join(chunks)


class FileModelArtifactStore:
    """File-backed model artifact provider.

    This store serves the same manifest/chunk protocol as
    InMemoryModelArtifactStore without keeping the model payload in RAM.
    """

    def __init__(self) -> None:
        self._paths: dict[str, Path] = {}
        self._manifests: dict[str, ModelArtifactManifest] = {}
        self._fingerprint_by_model_id: dict[str, str] = {}

    @classmethod
    def from_path(
        cls,
        path: Path,
        *,
        model_id: str = "cqdagpcfg-model",
        artifact_uri: str | None = None,
        chunk_size: int = 1 << 20,
    ) -> "FileModelArtifactStore":
        store = cls()
        store.put_file(
            path,
            model_id=model_id,
            artifact_uri=artifact_uri,
            chunk_size=chunk_size,
        )
        return store

    def put_file(
        self,
        path: Path,
        *,
        model_id: str = "cqdagpcfg-model",
        artifact_uri: str | None = None,
        chunk_size: int = 1 << 20,
    ) -> ModelArtifactManifest:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        resolved = path.resolve()
        if not resolved.is_file():
            raise FileNotFoundError(resolved)
        size_bytes = resolved.stat().st_size
        fingerprint = file_model_fingerprint(resolved)
        manifest = ModelArtifactManifest(
            model_id=model_id,
            model_fingerprint=fingerprint,
            size_bytes=size_bytes,
            chunk_size=chunk_size,
            chunk_count=ceil(size_bytes / chunk_size) if size_bytes else 0,
            artifact_uri=artifact_uri or str(resolved),
        )
        self._paths[fingerprint] = resolved
        self._manifests[fingerprint] = manifest
        self._fingerprint_by_model_id[model_id] = fingerprint
        return manifest

    def manifest(self, fingerprint: str) -> ModelArtifactManifest:
        try:
            return self._manifests[fingerprint]
        except KeyError as exc:
            raise KeyError(f"unknown model fingerprint: {fingerprint}") from exc

    def manifest_for_model(self, model_id: str) -> ModelArtifactManifest:
        try:
            fingerprint = self._fingerprint_by_model_id[model_id]
        except KeyError as exc:
            raise KeyError(f"unknown model_id: {model_id}") from exc
        return self.manifest(fingerprint)

    def fetch_chunk(self, fingerprint: str, *, offset: int) -> ModelArtifactChunk:
        if offset < 0:
            raise ValueError("offset cannot be negative")
        manifest = self.manifest(fingerprint)
        path = self._paths[fingerprint]
        if offset > manifest.size_bytes:
            raise ValueError("offset is beyond model payload")
        with path.open("rb") as handle:
            handle.seek(offset)
            data = handle.read(manifest.chunk_size)
        return ModelArtifactChunk(
            model_fingerprint=fingerprint,
            offset=offset,
            data=data,
            final=offset + len(data) >= manifest.size_bytes,
        )


class FileModelArtifactCache:
    """Local immutable cache for fetched model artifacts."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def artifact_path(self, manifest: ModelArtifactManifest) -> Path:
        return self.root / f"{_fingerprint_filename(manifest.model_fingerprint)}.model"

    def manifest_path(self, manifest: ModelArtifactManifest) -> Path:
        return self.root / f"{_fingerprint_filename(manifest.model_fingerprint)}.manifest.json"

    def is_cached(self, manifest: ModelArtifactManifest) -> bool:
        path = self.artifact_path(manifest)
        return (
            path.is_file()
            and path.stat().st_size == manifest.size_bytes
            and file_model_fingerprint(path) == manifest.model_fingerprint
        )

    def put_bytes(self, payload: bytes, *, manifest: ModelArtifactManifest) -> Path:
        fingerprint = model_fingerprint(payload)
        if fingerprint != manifest.model_fingerprint:
            raise ValueError("model payload fingerprint does not match manifest")
        path = self.artifact_path(manifest)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_bytes(payload)
        tmp_path.replace(path)
        self._write_manifest(manifest)
        return path

    def materialize(self, client, model_id: str) -> tuple[Path, ModelArtifactManifest]:
        manifest = client.manifest(model_id)
        if self.is_cached(manifest):
            self._write_manifest(manifest)
            return self.artifact_path(manifest), manifest

        path = self.artifact_path(manifest)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        offset = 0
        with tmp_path.open("wb") as handle:
            while offset < manifest.size_bytes:
                chunk = client.fetch_chunk(manifest.model_fingerprint, offset=offset)
                if chunk.offset != offset:
                    raise ValueError("model chunk offset is not contiguous")
                if chunk.model_fingerprint != manifest.model_fingerprint:
                    raise ValueError("model chunk fingerprint does not match manifest")
                handle.write(chunk.data)
                offset = chunk.end_offset
                if chunk.final:
                    break
        if offset != manifest.size_bytes:
            tmp_path.unlink(missing_ok=True)
            raise ValueError("model fetch ended before manifest size")
        if file_model_fingerprint(tmp_path) != manifest.model_fingerprint:
            tmp_path.unlink(missing_ok=True)
            raise ValueError("fetched model fingerprint does not match manifest")
        tmp_path.replace(path)
        self._write_manifest(manifest)
        return path, manifest

    def _write_manifest(self, manifest: ModelArtifactManifest) -> None:
        payload = {
            "model_id": manifest.model_id,
            "model_fingerprint": manifest.model_fingerprint,
            "size_bytes": manifest.size_bytes,
            "chunk_size": manifest.chunk_size,
            "chunk_count": manifest.chunk_count,
            "artifact_uri": manifest.artifact_uri,
        }
        self.manifest_path(manifest).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )


class BoundedModelPageCache:
    """Bounded LRU cache for demand-fetched model artifact chunks."""

    def __init__(self, *, max_bytes: int) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self.max_bytes = max_bytes
        self._pages: OrderedDict[tuple[str, int], bytes] = OrderedDict()
        self._bytes = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    @property
    def stats(self) -> ModelPageCacheStats:
        return ModelPageCacheStats(
            pages=len(self._pages),
            bytes=self._bytes,
            hits=self._hits,
            misses=self._misses,
            evictions=self._evictions,
        )

    def get_or_fetch(self, client, manifest: ModelArtifactManifest, *, offset: int) -> ModelArtifactChunk:
        page_offset = _page_offset(manifest, offset)
        key = (manifest.model_fingerprint, page_offset)
        data = self._pages.get(key)
        if data is not None:
            self._pages.move_to_end(key)
            self._hits += 1
            return ModelArtifactChunk(
                model_fingerprint=manifest.model_fingerprint,
                offset=page_offset,
                data=data,
                final=page_offset + len(data) >= manifest.size_bytes,
            )

        self._misses += 1
        chunk = client.fetch_chunk(manifest.model_fingerprint, offset=page_offset)
        if chunk.offset != page_offset:
            raise ValueError("fetched page offset does not match request")
        if chunk.model_fingerprint != manifest.model_fingerprint:
            raise ValueError("fetched page fingerprint does not match manifest")
        self._put(key, chunk.data)
        return chunk

    def prefetch(self, client, manifest: ModelArtifactManifest, offsets: tuple[int, ...]) -> None:
        for offset in offsets:
            self.get_or_fetch(client, manifest, offset=offset)

    def reclaim_except(self, keep_offsets: tuple[int, ...], *, model_fingerprint: str) -> int:
        keep = {(model_fingerprint, offset) for offset in keep_offsets}
        reclaimed = 0
        for key in tuple(self._pages):
            if key in keep:
                continue
            data = self._pages.pop(key)
            self._bytes -= len(data)
            reclaimed += len(data)
        return reclaimed

    def _put(self, key: tuple[str, int], data: bytes) -> None:
        existing = self._pages.pop(key, None)
        if existing is not None:
            self._bytes -= len(existing)
        self._pages[key] = data
        self._bytes += len(data)
        self._evict()

    def _evict(self) -> None:
        while self._bytes > self.max_bytes and self._pages:
            _key, data = self._pages.popitem(last=False)
            self._bytes -= len(data)
            self._evictions += 1


def file_model_fingerprint(path: Path, *, block_size: int = 1 << 20) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(block_size)
            if not block:
                break
            digest.update(block)
    return f"sha256:{digest.hexdigest()}"


def _fingerprint_filename(fingerprint: str) -> str:
    return fingerprint.replace(":", "_").replace("/", "_")


def _page_offset(manifest: ModelArtifactManifest, offset: int) -> int:
    if offset < 0:
        raise ValueError("offset cannot be negative")
    if offset > manifest.size_bytes:
        raise ValueError("offset is beyond model payload")
    return (offset // manifest.chunk_size) * manifest.chunk_size


__all__ = [
    "BoundedModelPageCache",
    "FileModelArtifactCache",
    "FileModelArtifactStore",
    "InMemoryModelArtifactStore",
    "ModelArtifactChunk",
    "ModelArtifactManifest",
    "ModelArtifactStore",
    "ModelPageCacheStats",
    "file_model_fingerprint",
]
