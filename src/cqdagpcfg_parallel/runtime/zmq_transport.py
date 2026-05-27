from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from types import TracebackType
from typing import Any
from urllib.parse import parse_qs, urlparse

from .batch_transport import BatchEndOfStream, BinaryCandidateBatchCodec
from .candidate_batch import CandidateBatch


DEFAULT_ZMQ_HIGH_WATERMARK = 100
DEFAULT_ZMQ_LINGER_MS = 0

CONTROL_PORT_OFFSET = 0
BATCH_PORT_OFFSET = 1
ROLE_PORT_OFFSET = 2
ACK_PORT_OFFSET = 3
MODEL_PORT_OFFSET = 4


def _require_zmq() -> Any:
    try:
        import zmq  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - depends on optional dependency
        raise RuntimeError(
            "pyzmq is required for ZeroMQ transports; install cqdagpcfg-parallel[zmq]"
        ) from exc
    return zmq


@dataclass(frozen=True, slots=True)
class ZmqBatchTransportStats:
    messages: int = 0
    batch_messages: int = 0
    end_messages: int = 0
    bytes: int = 0
    serialize_seconds: float = 0.0
    deserialize_seconds: float = 0.0
    send_seconds: float = 0.0
    recv_seconds: float = 0.0
    poll_seconds: float = 0.0
    poll_timeouts: int = 0


@dataclass(frozen=True, slots=True)
class ZmqEndpoint:
    address: str
    bind: bool = False
    high_watermark: int = DEFAULT_ZMQ_HIGH_WATERMARK
    linger_ms: int = DEFAULT_ZMQ_LINGER_MS

    def __post_init__(self) -> None:
        if not self.address:
            raise ValueError("ZeroMQ endpoint address cannot be empty")
        if self.high_watermark <= 0:
            raise ValueError("ZeroMQ high_watermark must be positive")
        if self.linger_ms < 0:
            raise ValueError("ZeroMQ linger_ms cannot be negative")

    @classmethod
    def from_uri(
        cls,
        uri: str,
        *,
        bind: bool = False,
        high_watermark: int = DEFAULT_ZMQ_HIGH_WATERMARK,
        linger_ms: int = DEFAULT_ZMQ_LINGER_MS,
    ) -> "ZmqEndpoint":
        parsed = urlparse(uri)
        query = parse_qs(parsed.query)
        endpoint_bind = _query_bool(query.get("bind", []), default=bind)
        endpoint_hwm = _query_int(query.get("hwm", []), default=high_watermark)
        endpoint_linger = _query_int(query.get("linger", []), default=linger_ms)

        if parsed.scheme == "cqpcfg":
            if not parsed.hostname or parsed.port is None:
                raise ValueError("cqpcfg URI must include host and port")
            return cls(
                address=f"tcp://{parsed.hostname}:{parsed.port}",
                bind=endpoint_bind,
                high_watermark=endpoint_hwm,
                linger_ms=endpoint_linger,
            )
        if parsed.scheme in {"tcp", "ipc", "inproc"}:
            address = uri.split("?", 1)[0]
            return cls(
                address=address,
                bind=endpoint_bind,
                high_watermark=endpoint_hwm,
                linger_ms=endpoint_linger,
            )
        raise ValueError(f"unsupported endpoint URI scheme: {parsed.scheme}")


def configure_zmq_socket(
    socket: Any,
    endpoint: ZmqEndpoint,
    *,
    zmq_module: Any | None = None,
    identity: bytes | None = None,
    send: bool = False,
    recv: bool = False,
    connect: bool = False,
) -> None:
    """Apply CQPCFG transport defaults to a ZeroMQ socket.

    The protocol often runs over Docker bridges, SSH tunnels, and WAN links.
    Plain ZeroMQ defaults are fine on localhost, but they are too optimistic for
    those paths: they do not enable TCP keepalive and connecting sockets may
    queue before a usable pipe exists. These options keep local behavior intact
    while making control, batch, and ack channels less fragile off-host.
    """

    zmq = _require_zmq() if zmq_module is None else zmq_module
    if identity is not None:
        socket.setsockopt(zmq.IDENTITY, identity)
    if send:
        socket.setsockopt(zmq.SNDHWM, endpoint.high_watermark)
    if recv:
        socket.setsockopt(zmq.RCVHWM, endpoint.high_watermark)
    socket.setsockopt(zmq.LINGER, endpoint.linger_ms)
    if connect:
        _setsockopt_if_available(zmq, socket, "RECONNECT_IVL", 100)
        _setsockopt_if_available(zmq, socket, "RECONNECT_IVL_MAX", 2000)
        _setsockopt_if_available(zmq, socket, "TCP_KEEPALIVE", 1)
        _setsockopt_if_available(zmq, socket, "TCP_KEEPALIVE_IDLE", 30)
        _setsockopt_if_available(zmq, socket, "TCP_KEEPALIVE_INTVL", 10)
        _setsockopt_if_available(zmq, socket, "TCP_KEEPALIVE_CNT", 3)
        if send:
            _setsockopt_if_available(zmq, socket, "IMMEDIATE", 1)


def _setsockopt_if_available(
    zmq: Any,
    socket: Any,
    option_name: str,
    value: int,
) -> None:
    option = getattr(zmq, option_name, None)
    if option is None:
        return
    socket.setsockopt(option, value)


@dataclass(frozen=True, slots=True)
class ZmqEndpointBundle:
    """Public CQPCFG endpoint expanded into internal ZeroMQ subchannels."""

    control: str
    batch: str
    role: str
    ack: str
    model: str

    @classmethod
    def from_base_uri(
        cls,
        uri: str,
        *,
        advertise_host: str | None = None,
    ) -> "ZmqEndpointBundle":
        parsed = urlparse(uri)
        if parsed.scheme not in {"cqpcfg", "tcp"}:
            raise ValueError("endpoint bundle base URI must use cqpcfg:// or tcp://")
        if not parsed.hostname or parsed.port is None:
            raise ValueError("endpoint bundle base URI must include host and port")
        host = parsed.hostname if advertise_host is None else advertise_host
        query = f"?{parsed.query}" if parsed.query else ""
        return cls(
            control=_subchannel_uri(parsed.scheme, host, parsed.port, CONTROL_PORT_OFFSET, query),
            batch=_subchannel_uri(parsed.scheme, host, parsed.port, BATCH_PORT_OFFSET, query),
            role=_subchannel_uri(parsed.scheme, host, parsed.port, ROLE_PORT_OFFSET, query),
            ack=_subchannel_uri(parsed.scheme, host, parsed.port, ACK_PORT_OFFSET, query),
            model=_subchannel_uri(parsed.scheme, host, parsed.port, MODEL_PORT_OFFSET, query),
        )


def _subchannel_uri(
    scheme: str,
    host: str,
    base_port: int,
    offset: int,
    query: str,
) -> str:
    port = base_port + offset
    return f"{scheme}://{_format_uri_host(host)}:{port}{query}"


def _format_uri_host(host: str) -> str:
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _query_bool(values: list[str], *, default: bool) -> bool:
    if not values:
        return default
    value = values[-1].strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean URI option: {values[-1]}")


def _query_int(values: list[str], *, default: int) -> int:
    if not values:
        return default
    try:
        return int(values[-1])
    except ValueError as exc:
        raise ValueError(f"invalid integer URI option: {values[-1]}") from exc


class ZmqPushBatchSink:
    def __init__(
        self,
        endpoint: ZmqEndpoint,
        *,
        context: Any | None = None,
        codec: type[BinaryCandidateBatchCodec] = BinaryCandidateBatchCodec,
    ) -> None:
        self.endpoint = endpoint
        self.context = context
        self.codec = codec
        self._socket: Any | None = None
        self._owns_context = context is None
        self._closed = False
        self._messages = 0
        self._batch_messages = 0
        self._end_messages = 0
        self._bytes = 0
        self._serialize_seconds = 0.0
        self._send_seconds = 0.0

    @property
    def stats(self) -> ZmqBatchTransportStats:
        return ZmqBatchTransportStats(
            messages=self._messages,
            batch_messages=self._batch_messages,
            end_messages=self._end_messages,
            bytes=self._bytes,
            serialize_seconds=self._serialize_seconds,
            send_seconds=self._send_seconds,
        )

    def __enter__(self) -> "ZmqPushBatchSink":
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
        socket = self.context.socket(zmq.PUSH)
        configure_zmq_socket(
            socket,
            self.endpoint,
            zmq_module=zmq,
            send=True,
            connect=not self.endpoint.bind,
        )
        if self.endpoint.bind:
            socket.bind(self.endpoint.address)
        else:
            socket.connect(self.endpoint.address)
        self._socket = socket

    def publish(self, batch: CandidateBatch) -> None:
        if self._closed:
            raise RuntimeError("cannot publish to a closed ZeroMQ sink")
        self.open()
        assert self._socket is not None
        started_at = perf_counter()
        payload = self.codec.dumps(batch)
        self._serialize_seconds += perf_counter() - started_at
        started_at = perf_counter()
        self._socket.send(payload)
        self._send_seconds += perf_counter() - started_at
        self._messages += 1
        self._batch_messages += 1
        self._bytes += len(payload)

    def publish_end_of_stream(self, count: int = 1) -> None:
        if count <= 0:
            raise ValueError("end-of-stream count must be positive")
        if self._closed:
            raise RuntimeError("cannot publish to a closed ZeroMQ sink")
        self.open()
        assert self._socket is not None
        started_at = perf_counter()
        payload = self.codec.dumps_end()
        self._serialize_seconds += perf_counter() - started_at
        for _ in range(count):
            started_at = perf_counter()
            self._socket.send(payload)
            self._send_seconds += perf_counter() - started_at
            self._messages += 1
            self._end_messages += 1
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


class ZmqPullBatchSource:
    def __init__(
        self,
        endpoint: ZmqEndpoint,
        *,
        context: Any | None = None,
        codec: type[BinaryCandidateBatchCodec] = BinaryCandidateBatchCodec,
    ) -> None:
        self.endpoint = endpoint
        self.context = context
        self.codec = codec
        self._socket: Any | None = None
        self._owns_context = context is None
        self._closed = False
        self._messages = 0
        self._batch_messages = 0
        self._end_messages = 0
        self._bytes = 0
        self._deserialize_seconds = 0.0
        self._recv_seconds = 0.0
        self._poll_seconds = 0.0
        self._poll_timeouts = 0

    @property
    def stats(self) -> ZmqBatchTransportStats:
        return ZmqBatchTransportStats(
            messages=self._messages,
            batch_messages=self._batch_messages,
            end_messages=self._end_messages,
            bytes=self._bytes,
            deserialize_seconds=self._deserialize_seconds,
            recv_seconds=self._recv_seconds,
            poll_seconds=self._poll_seconds,
            poll_timeouts=self._poll_timeouts,
        )

    def __enter__(self) -> "ZmqPullBatchSource":
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
        socket = self.context.socket(zmq.PULL)
        configure_zmq_socket(
            socket,
            self.endpoint,
            zmq_module=zmq,
            recv=True,
            connect=not self.endpoint.bind,
        )
        if self.endpoint.bind:
            socket.bind(self.endpoint.address)
        else:
            socket.connect(self.endpoint.address)
        self._socket = socket

    def receive_envelope(
        self,
        *,
        timeout_ms: int | None = None,
    ) -> CandidateBatch | BatchEndOfStream | None:
        if self._closed:
            raise RuntimeError("cannot receive from a closed ZeroMQ source")
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

    def receive(self, *, timeout_ms: int | None = None) -> CandidateBatch | None:
        envelope = self.receive_envelope(timeout_ms=timeout_ms)
        if isinstance(envelope, BatchEndOfStream):
            raise RuntimeError("received end-of-stream while waiting for CandidateBatch")
        return envelope

    def _decode_payload(self, payload: bytes) -> CandidateBatch | BatchEndOfStream:
        started_at = perf_counter()
        envelope = self.codec.loads_envelope(payload)
        self._deserialize_seconds += perf_counter() - started_at
        self._messages += 1
        self._bytes += len(payload)
        if isinstance(envelope, BatchEndOfStream):
            self._end_messages += 1
        else:
            self._batch_messages += 1
        return envelope

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
    "DEFAULT_ZMQ_HIGH_WATERMARK",
    "DEFAULT_ZMQ_LINGER_MS",
    "ZmqBatchTransportStats",
    "ZmqEndpoint",
    "ZmqPullBatchSource",
    "ZmqPushBatchSink",
]
