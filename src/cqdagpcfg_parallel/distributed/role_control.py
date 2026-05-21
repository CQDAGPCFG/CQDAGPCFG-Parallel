from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Iterable, Mapping

from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint, _require_zmq

from .job_context import JobContext
from .resources import RoleResourcePolicy, WorkerResourceSpec


@dataclass(frozen=True, slots=True)
class RoleControlReply:
    role: str = "idle"
    stop: bool = False
    job_context: JobContext | None = None
    job_context_version: str | None = None


@dataclass(frozen=True, slots=True)
class RoleControlStats:
    messages: int = 0
    bytes: int = 0
    poll_seconds: float = 0.0
    send_seconds: float = 0.0
    recv_seconds: float = 0.0


class RoleController:
    """ZeroMQ role controller for persistent node agents."""

    def __init__(
        self,
        *,
        endpoint: ZmqEndpoint,
        roles: Mapping[str, str],
        auto_assign_roles: Iterable[str] = (),
        default_role: str = "idle",
        assign_default_role: bool = False,
        resource_policy: RoleResourcePolicy = RoleResourcePolicy(),
        job_context: JobContext | Mapping[str, Any] | None = None,
    ) -> None:
        if not endpoint.bind:
            raise ValueError("role controller endpoint must bind")
        valid_roles = {"generator", "consumer", "idle"}
        if default_role not in valid_roles:
            raise ValueError("default_role must be generator, consumer, or idle")
        queued_roles = deque(str(role) for role in auto_assign_roles)
        invalid_roles = sorted(set(queued_roles) - valid_roles)
        if invalid_roles:
            raise ValueError(f"invalid auto-assigned roles: {invalid_roles}")
        self.endpoint = endpoint
        self.roles = dict(roles)
        self.auto_assign_roles = queued_roles
        self.default_role = default_role
        self.assign_default_role = assign_default_role
        self.resource_policy = resource_policy
        self.job_context = _normalize_job_context(job_context)
        self.stop = False
        self.status_by_node: dict[str, dict] = {}
        self._messages = 0
        self._bytes = 0
        self._poll_seconds = 0.0
        self._send_seconds = 0.0
        self._recv_seconds = 0.0
        self._zmq = _require_zmq()
        self._context = self._zmq.Context()
        self._socket = self._context.socket(self._zmq.ROUTER)
        self._socket.setsockopt(self._zmq.SNDHWM, endpoint.high_watermark)
        self._socket.setsockopt(self._zmq.RCVHWM, endpoint.high_watermark)
        self._socket.setsockopt(self._zmq.LINGER, endpoint.linger_ms)
        self._socket.bind(endpoint.address)

    @property
    def stats(self) -> RoleControlStats:
        return RoleControlStats(
            messages=self._messages,
            bytes=self._bytes,
            poll_seconds=self._poll_seconds,
            send_seconds=self._send_seconds,
            recv_seconds=self._recv_seconds,
        )

    def set_roles(self, roles: Mapping[str, str]) -> None:
        self.roles = dict(roles)

    def role_count(self, role: str) -> int:
        return sum(1 for value in self.roles.values() if value == role)

    def set_stop(self, value: bool) -> None:
        self.stop = value

    def poll(self, *, timeout_ms: int = 0) -> None:
        while True:
            started_at = perf_counter()
            result = self._socket.poll(timeout_ms, self._zmq.POLLIN)
            self._poll_seconds += perf_counter() - started_at
            if result == 0:
                return

            started_at = perf_counter()
            identity, payload = self._socket.recv_multipart()
            self._recv_seconds += perf_counter() - started_at

            node_id = identity.decode("utf-8")
            try:
                status = json.loads(payload.decode("utf-8"))
                self.status_by_node[node_id] = status
            except json.JSONDecodeError:
                status = {"decode_error": True}
                self.status_by_node[node_id] = status
            resources = WorkerResourceSpec.from_dict(
                _mapping_or_none(status.get("resources")),
            )
            if node_id not in self.roles:
                self._assign_new_node(node_id, resources)
            role = self.roles.get(node_id, self.default_role)
            if not resources.fits(self.resource_policy.requirement_for(role)):
                role = "idle"

            reply_payload: dict[str, Any] = {
                "schema_version": 1,
                "role": role,
                "stop": self.stop,
            }
            if self.job_context is not None:
                reply_payload["job_context_version"] = self.job_context.version
                if status.get("job_context_version") != self.job_context.version:
                    reply_payload["job_context"] = self.job_context.to_dict()
            reply = json.dumps(
                reply_payload,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            started_at = perf_counter()
            self._socket.send_multipart([identity, reply])
            self._send_seconds += perf_counter() - started_at
            self._messages += 1
            self._bytes += len(payload) + len(reply)
            timeout_ms = 0

    def _assign_new_node(self, node_id: str, resources: WorkerResourceSpec) -> None:
        for role in tuple(self.auto_assign_roles):
            if resources.fits(self.resource_policy.requirement_for(role)):
                self.roles[node_id] = role
                self.auto_assign_roles.remove(role)
                return
        if self.assign_default_role and resources.fits(
            self.resource_policy.requirement_for(self.default_role),
        ):
            self.roles[node_id] = self.default_role

    def close(self) -> None:
        self._socket.close()
        self._context.term()


class RoleClient:
    """DEALER client used by a NodeAgent to receive its current role."""

    def __init__(
        self,
        *,
        node_id: str,
        endpoint: ZmqEndpoint,
        reply_timeout_ms: int = 0,
    ) -> None:
        if endpoint.bind:
            raise ValueError("role client endpoint must connect")
        if reply_timeout_ms < 0:
            raise ValueError("reply_timeout_ms cannot be negative")
        self.node_id = node_id
        self.endpoint = endpoint
        self.reply_timeout_ms = reply_timeout_ms
        self._messages = 0
        self._bytes = 0
        self._poll_seconds = 0.0
        self._send_seconds = 0.0
        self._recv_seconds = 0.0
        self._last_reply = RoleControlReply()
        self._zmq = _require_zmq()
        self._context = self._zmq.Context()
        self._socket = self._context.socket(self._zmq.DEALER)
        self._socket.setsockopt(self._zmq.IDENTITY, node_id.encode("utf-8"))
        self._socket.setsockopt(self._zmq.SNDHWM, endpoint.high_watermark)
        self._socket.setsockopt(self._zmq.RCVHWM, endpoint.high_watermark)
        self._socket.setsockopt(self._zmq.LINGER, endpoint.linger_ms)
        self._socket.connect(endpoint.address)

    @property
    def stats(self) -> RoleControlStats:
        return RoleControlStats(
            messages=self._messages,
            bytes=self._bytes,
            poll_seconds=self._poll_seconds,
            send_seconds=self._send_seconds,
            recv_seconds=self._recv_seconds,
        )

    def request(self, status: Mapping[str, object]) -> RoleControlReply:
        request_payload = {"schema_version": 1, "node_id": self.node_id, **dict(status)}
        if (
            self._last_reply.job_context_version is not None
            and "job_context_version" not in request_payload
        ):
            request_payload["job_context_version"] = self._last_reply.job_context_version
        payload = json.dumps(
            request_payload,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

        started_at = perf_counter()
        try:
            self._socket.send(payload, self._zmq.DONTWAIT)
        except self._zmq.Again:
            self._send_seconds += perf_counter() - started_at
            self._bytes += len(payload)
            self._messages += 1
            return self._last_reply
        self._send_seconds += perf_counter() - started_at

        started_at = perf_counter()
        has_reply = self._socket.poll(self.reply_timeout_ms, self._zmq.POLLIN)
        self._poll_seconds += perf_counter() - started_at
        if has_reply:
            started_at = perf_counter()
            reply_payload = self._socket.recv()
            self._recv_seconds += perf_counter() - started_at
            reply = json.loads(reply_payload.decode("utf-8"))
            job_context_version = reply.get("job_context_version")
            job_context = (
                JobContext.from_dict(reply["job_context"])
                if reply.get("job_context") is not None
                else self._last_reply.job_context
            )
            self._last_reply = RoleControlReply(
                role=str(reply.get("role", "idle")),
                stop=bool(reply.get("stop", False)),
                job_context=job_context,
                job_context_version=(
                    str(job_context_version)
                    if job_context_version is not None
                    else self._last_reply.job_context_version
                ),
            )
            self._bytes += len(payload) + len(reply_payload)
        else:
            self._bytes += len(payload)
        self._messages += 1
        return self._last_reply

    def close(self) -> None:
        self._socket.close()
        self._context.term()


def _normalize_job_context(
    job_context: JobContext | Mapping[str, Any] | None,
) -> JobContext | None:
    if job_context is None:
        return None
    if isinstance(job_context, JobContext):
        return job_context
    return JobContext.from_dict(job_context)


def _mapping_or_none(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


__all__ = [
    "RoleClient",
    "RoleControlReply",
    "RoleControlStats",
    "RoleController",
]
