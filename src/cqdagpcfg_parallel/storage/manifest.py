from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any, Mapping

from cqdagpcfg_parallel.protocol import EnumerationChunk, NodeId, WorkerId


@dataclass(frozen=True, slots=True)
class ModelManifest:
    model_id: str
    model_fingerprint: str
    artifact_uri: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_json_payload(
        cls,
        payload: str | bytes | Mapping[str, Any],
        *,
        model_id: str = "cqdagpcfg-model",
        artifact_uri: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> "ModelManifest":
        return cls(
            model_id=model_id,
            model_fingerprint=model_fingerprint(payload),
            artifact_uri=artifact_uri,
            metadata={} if metadata is None else dict(metadata),
        )

    def require_match(self, fingerprint: str | None) -> None:
        if fingerprint is None:
            raise ValueError("worker did not report model_fingerprint")
        if fingerprint != self.model_fingerprint:
            raise ValueError("worker model_fingerprint does not match manifest")


def model_fingerprint(payload: str | bytes | Mapping[str, Any]) -> str:
    if isinstance(payload, bytes):
        data = payload
    elif isinstance(payload, str):
        data = payload.encode("utf-8")
    else:
        data = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    return f"sha256:{sha256(data).hexdigest()}"


@dataclass(frozen=True, slots=True)
class ChunkManifestEntry:
    node_id: NodeId
    start: int
    end: int
    worker_id: WorkerId
    epoch: int
    record_count: int

    @classmethod
    def from_chunk(cls, chunk: EnumerationChunk) -> "ChunkManifestEntry":
        return cls(
            node_id=chunk.node_id,
            start=chunk.start,
            end=chunk.end,
            worker_id=chunk.worker_id,
            epoch=chunk.epoch,
            record_count=len(chunk.records),
        )


@dataclass(slots=True)
class ChunkManifest:
    entries: list[ChunkManifestEntry] = field(default_factory=list)

    def append(self, chunk: EnumerationChunk) -> ChunkManifestEntry:
        entry = ChunkManifestEntry.from_chunk(chunk)
        self.entries.append(entry)
        return entry

    def by_node(self, node_id: NodeId) -> tuple[ChunkManifestEntry, ...]:
        return tuple(entry for entry in self.entries if entry.node_id == node_id)


__all__ = [
    "ChunkManifest",
    "ChunkManifestEntry",
    "ModelManifest",
    "model_fingerprint",
]
