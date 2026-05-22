from __future__ import annotations

import socket
from dataclasses import dataclass
from time import monotonic, sleep
from typing import Any

from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint, ZmqEndpointBundle

from .job_context import JobContext
from .resources import WorkerResourceSpec, parse_byte_size
from .role_control import RoleClient, RoleControlReply


@dataclass(frozen=True, slots=True)
class NodeEndpointConfig:
    control_connect: str
    batch_connect: str
    role_connect: str | None
    ack_connect: str
    model_connect: str | None


def resolve_node_id(value: str | None) -> str:
    node_id = value or socket.gethostname()
    node_id = node_id.strip()
    if not node_id:
        raise ValueError("node id cannot be empty")
    return node_id


def safe_node_filename(value: str) -> str:
    safe = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in value
    )
    return safe or "node"


def expand_node_endpoints(
    *,
    connect: str | None,
    control_connect: str = "cqpcfg://127.0.0.1:5555",
    batch_connect: str = "cqpcfg://127.0.0.1:5556",
    role_connect: str | None = None,
    ack_connect: str = "cqpcfg://127.0.0.1:5558",
    model_connect: str | None = None,
) -> NodeEndpointConfig:
    if connect is None:
        return NodeEndpointConfig(
            control_connect=control_connect,
            batch_connect=batch_connect,
            role_connect=role_connect,
            ack_connect=ack_connect,
            model_connect=model_connect,
        )
    bundle = ZmqEndpointBundle.from_base_uri(connect)
    return NodeEndpointConfig(
        control_connect=bundle.control,
        batch_connect=bundle.batch,
        role_connect=role_connect or bundle.role,
        ack_connect=bundle.ack,
        model_connect=model_connect or bundle.model,
    )


def worker_resources(
    *,
    resource_cpus: float | None = None,
    resource_memory: str | int | None = None,
    resource_gpus: int | None = None,
    resource_gpu_memory: str | int | None = None,
    model_json_page_cache: int | None = None,
) -> WorkerResourceSpec:
    return WorkerResourceSpec(
        cpu_cores=resource_cpus,
        memory_bytes=parse_byte_size(resource_memory),
        gpu_count=resource_gpus,
        gpu_memory_bytes=parse_byte_size(resource_gpu_memory),
        model_json_page_cache=model_json_page_cache,
    )


def fetch_job_context(
    *,
    node_id: str,
    role_connect: str,
    resources: WorkerResourceSpec,
    reply_timeout_ms: int = 100,
    timeout_seconds: float = 30.0,
    refresh_interval_seconds: float = 0.05,
) -> RoleControlReply:
    if timeout_seconds < 0.0:
        raise ValueError("timeout_seconds cannot be negative")
    if refresh_interval_seconds <= 0.0:
        raise ValueError("refresh_interval_seconds must be positive")
    role_client = RoleClient(
        node_id=f"{node_id}:bootstrap",
        endpoint=ZmqEndpoint.from_uri(role_connect, bind=False),
        reply_timeout_ms=reply_timeout_ms,
    )
    try:
        deadline = monotonic() + timeout_seconds
        while True:
            reply = role_client.request(
                {
                    "current_role": "bootstrap",
                    "completed_records": 0,
                    "consumed_candidates": 0,
                    "role_switches": 0,
                    "resources": resources.to_dict(),
                }
            )
            if reply.job_context is not None:
                return reply
            if monotonic() >= deadline:
                raise TimeoutError("timed out waiting for tracker JobContext")
            sleep(min(refresh_interval_seconds, 0.1))
    finally:
        role_client.close()


def job_payload_from_job_context(job_context: JobContext) -> dict[str, Any]:
    payload = dict(job_context.job_payload)
    payload.setdefault("limit", job_context.limit)
    payload.setdefault("model_fingerprint", job_context.model_fingerprint)
    return payload


def targets_from_job_context(job_context: JobContext) -> dict[str, Any]:
    return job_payload_from_job_context(job_context)


__all__ = [
    "NodeEndpointConfig",
    "expand_node_endpoints",
    "fetch_job_context",
    "job_payload_from_job_context",
    "resolve_node_id",
    "safe_node_filename",
    "targets_from_job_context",
    "worker_resources",
]
