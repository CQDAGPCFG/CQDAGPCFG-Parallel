from __future__ import annotations

from threading import Thread

import pytest

from cqdagpcfg_parallel.distributed import (
    JobContext,
    RoleClient,
    RoleController,
    RoleResourcePolicy,
    WorkerResourceSpec,
    parse_byte_size,
)
from cqdagpcfg_parallel.runtime import ZmqEndpoint


def make_job_context() -> JobContext:
    return JobContext(
        job_id="toy-job",
        limit=16,
        hash_algorithm="sha256",
        targets=({"rank": 0, "guess": "ab12!", "hash": "deadbeef"},),
        model_id="toy-model",
        model_fingerprint="sha256:abc",
        model_connect="cqpcfg://127.0.0.1:7000",
        control_connect="cqpcfg://127.0.0.1:7001",
        batch_connect="cqpcfg://127.0.0.1:7002",
        ack_connect="cqpcfg://127.0.0.1:7003",
        source_mode="root",
        demand_window=8,
    )


def test_job_context_round_trips_dict() -> None:
    context = make_job_context()

    decoded = JobContext.from_dict(context.to_dict())

    assert decoded == context


def test_role_controller_sends_job_context(tmp_path) -> None:
    pytest.importorskip("zmq")
    endpoint_uri = f"ipc://{tmp_path / 'role.sock'}"
    context = make_job_context()
    controller = RoleController(
        endpoint=ZmqEndpoint(endpoint_uri, bind=True, linger_ms=0),
        roles={"node-0": "generator"},
        job_context=context,
    )
    client = RoleClient(
        node_id="node-0",
        endpoint=ZmqEndpoint(endpoint_uri, bind=False, linger_ms=0),
        reply_timeout_ms=1000,
    )

    thread = Thread(target=lambda: controller.poll(timeout_ms=1000), daemon=True)
    thread.start()
    try:
        reply = client.request({"current_role": "bootstrap"})
    finally:
        thread.join(2.0)
        client.close()
        controller.close()

    assert not thread.is_alive()
    assert reply.role == "generator"
    assert reply.job_context == context


def test_role_controller_auto_assigns_unknown_nodes(tmp_path) -> None:
    pytest.importorskip("zmq")
    endpoint_uri = f"ipc://{tmp_path / 'auto-role.sock'}"
    controller = RoleController(
        endpoint=ZmqEndpoint(endpoint_uri, bind=True, linger_ms=0),
        roles={},
        auto_assign_roles=("generator", "consumer"),
    )
    clients = [
        RoleClient(
            node_id=f"node-{index}",
            endpoint=ZmqEndpoint(endpoint_uri, bind=False, linger_ms=0),
            reply_timeout_ms=1000,
        )
        for index in range(3)
    ]

    try:
        replies = []
        for client in clients:
            thread = Thread(target=lambda: controller.poll(timeout_ms=1000), daemon=True)
            thread.start()
            replies.append(client.request({"current_role": "bootstrap"}))
            thread.join(2.0)
            assert not thread.is_alive()
    finally:
        for client in clients:
            client.close()
        controller.close()

    assert [reply.role for reply in replies] == ["generator", "consumer", "idle"]
    assert controller.roles["node-0"] == "generator"
    assert controller.roles["node-1"] == "consumer"


def test_role_controller_persistently_assigns_default_role(tmp_path) -> None:
    pytest.importorskip("zmq")
    endpoint_uri = f"ipc://{tmp_path / 'default-role.sock'}"
    controller = RoleController(
        endpoint=ZmqEndpoint(endpoint_uri, bind=True, linger_ms=0),
        roles={},
        default_role="generator",
        assign_default_role=True,
    )
    client = RoleClient(
        node_id="late-node",
        endpoint=ZmqEndpoint(endpoint_uri, bind=False, linger_ms=0),
        reply_timeout_ms=1000,
    )

    thread = Thread(target=lambda: controller.poll(timeout_ms=1000), daemon=True)
    thread.start()
    try:
        reply = client.request({"current_role": "bootstrap"})
    finally:
        thread.join(2.0)
        client.close()
        controller.close()

    assert not thread.is_alive()
    assert reply.role == "generator"
    assert controller.role_count("generator") == 1
    assert controller.roles["late-node"] == "generator"


def test_role_controller_uses_resource_policy_for_assignment(tmp_path) -> None:
    pytest.importorskip("zmq")
    endpoint_uri = f"ipc://{tmp_path / 'resource-role.sock'}"
    controller = RoleController(
        endpoint=ZmqEndpoint(endpoint_uri, bind=True, linger_ms=0),
        roles={},
        auto_assign_roles=("consumer", "generator"),
        resource_policy=RoleResourcePolicy(
            consumer_min=WorkerResourceSpec(gpu_count=1),
        ),
    )
    client = RoleClient(
        node_id="cpu-only-node",
        endpoint=ZmqEndpoint(endpoint_uri, bind=False, linger_ms=0),
        reply_timeout_ms=1000,
    )

    thread = Thread(target=lambda: controller.poll(timeout_ms=1000), daemon=True)
    thread.start()
    try:
        reply = client.request(
            {
                "current_role": "bootstrap",
                "resources": WorkerResourceSpec(cpu_cores=4, gpu_count=0).to_dict(),
            }
        )
    finally:
        thread.join(2.0)
        client.close()
        controller.close()

    assert not thread.is_alive()
    assert reply.role == "generator"
    assert controller.roles["cpu-only-node"] == "generator"


def test_parse_byte_size_accepts_docker_style_units() -> None:
    assert parse_byte_size("512m") == 512 * 1024**2
    assert parse_byte_size("4g") == 4 * 1024**3
