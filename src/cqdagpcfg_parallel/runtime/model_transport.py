from __future__ import annotations

import base64
import json
from dataclasses import asdict, dataclass
from types import TracebackType
from typing import Any, Literal

from cqdagpcfg_parallel.storage import (
    ModelJsonPage,
    ModelArtifactChunk,
    ModelArtifactManifest,
    ModelArtifactStore,
    PagedModelManifest,
)

from .zmq_transport import ZmqEndpoint, _require_zmq


ModelFetchKind = Literal["manifest", "chunk", "paged_manifest", "page"]
DEFAULT_MODEL_FETCH_TIMEOUT_MS = 30_000
DEFAULT_MODEL_FETCH_RETRIES = 1


class ModelFetchError(RuntimeError):
    """Base error raised by the model fetch protocol."""


class ModelFetchTimeoutError(ModelFetchError):
    """Raised when a model fetch request cannot complete before its timeout."""


@dataclass(frozen=True, slots=True)
class ModelFetchRequest:
    kind: ModelFetchKind
    model_id: str | None = None
    model_fingerprint: str | None = None
    offset: int = 0
    page_id: str | None = None

    def __post_init__(self) -> None:
        if self.kind in {"manifest", "paged_manifest"} and not self.model_id:
            raise ValueError("manifest fetch requires model_id")
        if self.kind == "chunk" and not self.model_fingerprint:
            raise ValueError("chunk fetch requires model_fingerprint")
        if self.kind == "page" and (not self.model_fingerprint or not self.page_id):
            raise ValueError("page fetch requires model_fingerprint and page_id")
        if self.offset < 0:
            raise ValueError("offset cannot be negative")


@dataclass(frozen=True, slots=True)
class ModelFetchResponse:
    ok: bool
    manifest: ModelArtifactManifest | None = None
    paged_manifest: PagedModelManifest | None = None
    chunk: ModelArtifactChunk | None = None
    page: ModelJsonPage | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if self.ok and self.error is not None:
            raise ValueError("successful response cannot include error")
        if not self.ok and not self.error:
            raise ValueError("failed response must include error")


class JsonModelFetchCodec:
    schema_version = 1

    @classmethod
    def dumps_request(cls, request: ModelFetchRequest) -> bytes:
        payload = {
            "schema_version": cls.schema_version,
            "type": "model_fetch_request",
            "kind": request.kind,
            "offset": request.offset,
        }
        if request.model_id is not None:
            payload["model_id"] = request.model_id
        if request.model_fingerprint is not None:
            payload["model_fingerprint"] = request.model_fingerprint
        if request.page_id is not None:
            payload["page_id"] = request.page_id
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @classmethod
    def loads_request(cls, payload: bytes) -> ModelFetchRequest:
        raw = json.loads(payload.decode("utf-8"))
        if raw.get("schema_version") != cls.schema_version:
            raise ValueError("unsupported model fetch schema version")
        if raw.get("type") != "model_fetch_request":
            raise ValueError("unsupported model fetch message type")
        return ModelFetchRequest(
            kind=raw["kind"],
            model_id=raw.get("model_id"),
            model_fingerprint=raw.get("model_fingerprint"),
            offset=int(raw.get("offset", 0)),
            page_id=raw.get("page_id"),
        )

    @classmethod
    def dumps_response(cls, response: ModelFetchResponse) -> bytes:
        payload: dict[str, object] = {
            "schema_version": cls.schema_version,
            "type": "model_fetch_response",
            "ok": response.ok,
        }
        if response.error is not None:
            payload["error"] = response.error
        if response.manifest is not None:
            payload["manifest"] = asdict(response.manifest)
        if response.paged_manifest is not None:
            payload["paged_manifest"] = response.paged_manifest.to_dict()
        if response.chunk is not None:
            payload["chunk"] = {
                "model_fingerprint": response.chunk.model_fingerprint,
                "offset": response.chunk.offset,
                "data": base64.b64encode(response.chunk.data).decode("ascii"),
                "final": response.chunk.final,
            }
        if response.page is not None:
            payload["page"] = response.page.to_dict()
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @classmethod
    def loads_response(cls, payload: bytes) -> ModelFetchResponse:
        raw = json.loads(payload.decode("utf-8"))
        if raw.get("schema_version") != cls.schema_version:
            raise ValueError("unsupported model fetch schema version")
        if raw.get("type") != "model_fetch_response":
            raise ValueError("unsupported model fetch message type")
        manifest = None
        if raw.get("manifest") is not None:
            manifest_raw = raw["manifest"]
            manifest = ModelArtifactManifest(
                model_id=str(manifest_raw["model_id"]),
                model_fingerprint=str(manifest_raw["model_fingerprint"]),
                size_bytes=int(manifest_raw["size_bytes"]),
                chunk_size=int(manifest_raw["chunk_size"]),
                chunk_count=int(manifest_raw["chunk_count"]),
                artifact_uri=manifest_raw.get("artifact_uri"),
            )
        paged_manifest = None
        if raw.get("paged_manifest") is not None:
            paged_manifest = PagedModelManifest.from_dict(raw["paged_manifest"])
        chunk = None
        if raw.get("chunk") is not None:
            chunk_raw = raw["chunk"]
            chunk = ModelArtifactChunk(
                model_fingerprint=str(chunk_raw["model_fingerprint"]),
                offset=int(chunk_raw["offset"]),
                data=base64.b64decode(str(chunk_raw["data"]).encode("ascii")),
                final=bool(chunk_raw["final"]),
            )
        page = None
        if raw.get("page") is not None:
            page = ModelJsonPage.from_dict(raw["page"])
        return ModelFetchResponse(
            ok=bool(raw["ok"]),
            manifest=manifest,
            paged_manifest=paged_manifest,
            chunk=chunk,
            page=page,
            error=raw.get("error"),
        )


class ZmqModelArtifactServer:
    def __init__(
        self,
        endpoint: ZmqEndpoint,
        store: ModelArtifactStore,
        *,
        context: Any | None = None,
        codec: type[JsonModelFetchCodec] = JsonModelFetchCodec,
    ) -> None:
        if not endpoint.bind:
            raise ValueError("model artifact server endpoint must bind")
        self.endpoint = endpoint
        self.store = store
        self.context = context
        self.codec = codec
        self._socket: Any | None = None
        self._owns_context = context is None
        self._closed = False

    def __enter__(self) -> "ZmqModelArtifactServer":
        self.open()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        self.close()

    def open(self) -> None:
        if self._socket is not None:
            return
        zmq = _require_zmq()
        if self.context is None:
            self.context = zmq.Context()
        socket = self.context.socket(zmq.REP)
        socket.setsockopt(zmq.LINGER, self.endpoint.linger_ms)
        if self.endpoint.bind:
            socket.bind(self.endpoint.address)
        else:  # pragma: no cover - guarded above
            socket.connect(self.endpoint.address)
        self._socket = socket

    def serve_once(self, *, timeout_ms: int | None = None) -> bool:
        self.open()
        if self._socket is None:
            raise RuntimeError("model artifact server socket is not open")
        zmq = _require_zmq()
        if timeout_ms is not None:
            if timeout_ms < 0:
                raise ValueError("timeout_ms cannot be negative")
            if self._socket.poll(timeout_ms, zmq.POLLIN) == 0:
                return False
        request = self.codec.loads_request(self._socket.recv())
        response = self._handle_request(request)
        self._socket.send(self.codec.dumps_response(response))
        return True

    def _handle_request(self, request: ModelFetchRequest) -> ModelFetchResponse:
        try:
            if request.kind == "manifest":
                if request.model_id is None:
                    raise ValueError("manifest request is missing model_id")
                return ModelFetchResponse(
                    ok=True,
                    manifest=self.store.manifest_for_model(request.model_id),
                )
            if request.kind == "paged_manifest":
                if request.model_id is None:
                    raise ValueError("paged_manifest request is missing model_id")
                return ModelFetchResponse(
                    ok=True,
                    paged_manifest=self.store.paged_manifest_for_model(request.model_id),
                )
            if request.kind == "page":
                if request.model_fingerprint is None or request.page_id is None:
                    raise ValueError("page request is missing model_fingerprint or page_id")
                return ModelFetchResponse(
                    ok=True,
                    page=self.store.fetch_page(
                        request.model_fingerprint,
                        page_id=request.page_id,
                    ),
                )
            if request.model_fingerprint is None:
                raise ValueError("chunk request is missing model_fingerprint")
            return ModelFetchResponse(
                ok=True,
                chunk=self.store.fetch_chunk(
                    request.model_fingerprint,
                    offset=request.offset,
                ),
            )
        except Exception as exc:
            return ModelFetchResponse(
                ok=False,
                error=f"{type(exc).__name__}: {exc}",
            )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._socket is not None:
            self._socket.close()
            self._socket = None
        if self._owns_context and self.context is not None:
            self.context.term()
            self.context = None


class ZmqModelArtifactClient:
    def __init__(
        self,
        endpoint: ZmqEndpoint,
        *,
        context: Any | None = None,
        codec: type[JsonModelFetchCodec] = JsonModelFetchCodec,
        timeout_ms: int = DEFAULT_MODEL_FETCH_TIMEOUT_MS,
        retries: int = DEFAULT_MODEL_FETCH_RETRIES,
    ) -> None:
        if endpoint.bind:
            raise ValueError("model artifact client endpoint must connect")
        if timeout_ms < 0:
            raise ValueError("timeout_ms cannot be negative")
        if retries < 0:
            raise ValueError("retries cannot be negative")
        self.endpoint = endpoint
        self.context = context
        self.codec = codec
        self.timeout_ms = timeout_ms
        self.retries = retries
        self._socket: Any | None = None
        self._owns_context = context is None
        self._closed = False

    def __enter__(self) -> "ZmqModelArtifactClient":
        self.open()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        self.close()

    def open(self) -> None:
        if self._socket is not None:
            return
        if self._closed:
            raise RuntimeError("model artifact client is closed")
        zmq = _require_zmq()
        if self.context is None:
            self.context = zmq.Context()
        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, self.endpoint.linger_ms)
        if self.timeout_ms > 0:
            socket.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
            socket.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
        socket.connect(self.endpoint.address)
        self._socket = socket

    def manifest(self, model_id: str) -> ModelArtifactManifest:
        response = self._request(ModelFetchRequest(kind="manifest", model_id=model_id))
        if response.manifest is None:
            raise RuntimeError("model server did not return a manifest")
        return response.manifest

    def paged_manifest(self, model_id: str) -> PagedModelManifest:
        response = self._request(ModelFetchRequest(kind="paged_manifest", model_id=model_id))
        if response.paged_manifest is None:
            raise RuntimeError("model server did not return a paged manifest")
        return response.paged_manifest

    def fetch_chunk(self, model_fingerprint: str, *, offset: int) -> ModelArtifactChunk:
        response = self._request(
            ModelFetchRequest(
                kind="chunk",
                model_fingerprint=model_fingerprint,
                offset=offset,
            )
        )
        if response.chunk is None:
            raise RuntimeError("model server did not return a chunk")
        return response.chunk

    def fetch_page(self, model_fingerprint: str, *, page_id: str) -> ModelJsonPage:
        response = self._request(
            ModelFetchRequest(
                kind="page",
                model_fingerprint=model_fingerprint,
                page_id=page_id,
            )
        )
        if response.page is None:
            raise RuntimeError("model server did not return a page")
        return response.page

    def fetch_all(self, model_id: str) -> bytes:
        manifest = self.manifest(model_id)
        chunks: list[bytes] = []
        offset = 0
        while offset < manifest.size_bytes:
            chunk = self.fetch_chunk(manifest.model_fingerprint, offset=offset)
            chunks.append(chunk.data)
            offset = chunk.end_offset
        return b"".join(chunks)

    def _request(self, request: ModelFetchRequest) -> ModelFetchResponse:
        zmq = _require_zmq()
        last_timeout: BaseException | None = None
        for attempt in range(self.retries + 1):
            self.open()
            if self._socket is None:
                raise RuntimeError("model artifact client socket is not open")
            try:
                self._socket.send(self.codec.dumps_request(request))
                response = self.codec.loads_response(self._socket.recv())
            except zmq.Again as exc:
                last_timeout = exc
                self._drop_socket()
                if attempt >= self.retries:
                    raise ModelFetchTimeoutError(
                        f"model fetch timed out for {request.kind}"
                    ) from exc
                continue
            if not response.ok:
                raise ModelFetchError(
                    f"model fetch failed for {request.kind}: "
                    f"{response.error or 'unknown server error'}"
                )
            return response
        raise ModelFetchTimeoutError(
            f"model fetch timed out for {request.kind}"
        ) from last_timeout

    def _drop_socket(self) -> None:
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._drop_socket()
        if self._owns_context and self.context is not None:
            self.context.term()
            self.context = None


__all__ = [
    "DEFAULT_MODEL_FETCH_RETRIES",
    "DEFAULT_MODEL_FETCH_TIMEOUT_MS",
    "JsonModelFetchCodec",
    "ModelFetchError",
    "ModelFetchRequest",
    "ModelFetchResponse",
    "ModelFetchTimeoutError",
    "ZmqModelArtifactClient",
    "ZmqModelArtifactServer",
]
