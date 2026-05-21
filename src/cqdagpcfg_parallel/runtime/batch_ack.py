from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from time import perf_counter
from types import TracebackType
from typing import Any

from .zmq_transport import ZmqBatchTransportStats, ZmqEndpoint, _require_zmq


class BatchAckStatus(str, Enum):
    DONE = "done"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class BatchAck:
    batch_id: int
    consumer_id: str
    status: BatchAckStatus
    error: str | None = None

    def __post_init__(self) -> None:
        if self.batch_id < 0:
            raise ValueError("batch_id cannot be negative")
        if not self.consumer_id:
            raise ValueError("consumer_id cannot be empty")
        if self.status == BatchAckStatus.FAILED and not self.error:
            raise ValueError("failed ack must include an error")


class JsonBatchAckCodec:
    schema_version = 1

    @classmethod
    def dumps(cls, ack: BatchAck) -> bytes:
        payload: dict[str, object] = {
            "schema_version": cls.schema_version,
            "type": "batch_ack",
            "batch_id": ack.batch_id,
            "consumer_id": ack.consumer_id,
            "status": ack.status.value,
        }
        if ack.error is not None:
            payload["error"] = ack.error
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @classmethod
    def loads(cls, payload: bytes) -> BatchAck:
        raw = json.loads(payload.decode("utf-8"))
        if raw.get("schema_version") != cls.schema_version:
            raise ValueError("unsupported BatchAck schema version")
        if raw.get("type") != "batch_ack":
            raise ValueError("unsupported BatchAck message type")
        return BatchAck(
            batch_id=int(raw["batch_id"]),
            consumer_id=str(raw["consumer_id"]),
            status=BatchAckStatus(str(raw["status"])),
            error=str(raw["error"]) if raw.get("error") is not None else None,
        )


class ZmqPushBatchAckSink:
    def __init__(
        self,
        endpoint: ZmqEndpoint,
        *,
        context: Any | None = None,
        codec: type[JsonBatchAckCodec] = JsonBatchAckCodec,
    ) -> None:
        self.endpoint = endpoint
        self.context = context
        self.codec = codec
        self._socket: Any | None = None
        self._owns_context = context is None
        self._closed = False
        self._messages = 0
        self._bytes = 0
        self._serialize_seconds = 0.0
        self._send_seconds = 0.0

    @property
    def stats(self) -> ZmqBatchTransportStats:
        return ZmqBatchTransportStats(
            messages=self._messages,
            bytes=self._bytes,
            serialize_seconds=self._serialize_seconds,
            send_seconds=self._send_seconds,
        )

    def __enter__(self) -> "ZmqPushBatchAckSink":
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def open(self) -> None:
        if self._socket is not None:
            return
        zmq = _require_zmq()
        if self.context is None:
            self.context = zmq.Context()
        socket = self.context.socket(zmq.PUSH)
        socket.setsockopt(zmq.SNDHWM, self.endpoint.high_watermark)
        socket.setsockopt(zmq.LINGER, self.endpoint.linger_ms)
        if self.endpoint.bind:
            socket.bind(self.endpoint.address)
        else:
            socket.connect(self.endpoint.address)
        self._socket = socket

    def publish(self, ack: BatchAck) -> None:
        if self._closed:
            raise RuntimeError("cannot publish to a closed ZeroMQ ack sink")
        self.open()
        assert self._socket is not None
        started_at = perf_counter()
        payload = self.codec.dumps(ack)
        self._serialize_seconds += perf_counter() - started_at
        started_at = perf_counter()
        self._socket.send(payload)
        self._send_seconds += perf_counter() - started_at
        self._messages += 1
        self._bytes += len(payload)

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


class ZmqPullBatchAckSource:
    def __init__(
        self,
        endpoint: ZmqEndpoint,
        *,
        context: Any | None = None,
        codec: type[JsonBatchAckCodec] = JsonBatchAckCodec,
    ) -> None:
        self.endpoint = endpoint
        self.context = context
        self.codec = codec
        self._socket: Any | None = None
        self._owns_context = context is None
        self._closed = False
        self._messages = 0
        self._bytes = 0
        self._deserialize_seconds = 0.0
        self._recv_seconds = 0.0
        self._poll_seconds = 0.0
        self._poll_timeouts = 0

    @property
    def stats(self) -> ZmqBatchTransportStats:
        return ZmqBatchTransportStats(
            messages=self._messages,
            bytes=self._bytes,
            deserialize_seconds=self._deserialize_seconds,
            recv_seconds=self._recv_seconds,
            poll_seconds=self._poll_seconds,
            poll_timeouts=self._poll_timeouts,
        )

    def __enter__(self) -> "ZmqPullBatchAckSource":
        self.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def open(self) -> None:
        if self._socket is not None:
            return
        zmq = _require_zmq()
        if self.context is None:
            self.context = zmq.Context()
        socket = self.context.socket(zmq.PULL)
        socket.setsockopt(zmq.RCVHWM, self.endpoint.high_watermark)
        socket.setsockopt(zmq.LINGER, self.endpoint.linger_ms)
        if self.endpoint.bind:
            socket.bind(self.endpoint.address)
        else:
            socket.connect(self.endpoint.address)
        self._socket = socket

    def receive(self, *, timeout_ms: int | None = None) -> BatchAck | None:
        if self._closed:
            raise RuntimeError("cannot receive from a closed ZeroMQ ack source")
        self.open()
        assert self._socket is not None
        zmq = _require_zmq()

        if timeout_ms is None:
            started_at = perf_counter()
            payload = self._socket.recv()
            self._recv_seconds += perf_counter() - started_at
            return self._decode_payload(payload)

        if timeout_ms < 0:
            raise ValueError("timeout_ms cannot be negative")
        started_at = perf_counter()
        poll_result = self._socket.poll(timeout_ms, zmq.POLLIN)
        self._poll_seconds += perf_counter() - started_at
        if poll_result == 0:
            self._poll_timeouts += 1
            return None
        started_at = perf_counter()
        payload = self._socket.recv()
        self._recv_seconds += perf_counter() - started_at
        return self._decode_payload(payload)

    def _decode_payload(self, payload: bytes) -> BatchAck:
        started_at = perf_counter()
        ack = self.codec.loads(payload)
        self._deserialize_seconds += perf_counter() - started_at
        self._messages += 1
        self._bytes += len(payload)
        return ack

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


__all__ = [
    "BatchAck",
    "BatchAckStatus",
    "JsonBatchAckCodec",
    "ZmqPullBatchAckSource",
    "ZmqPushBatchAckSink",
]
