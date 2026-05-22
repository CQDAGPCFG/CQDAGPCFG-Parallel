from __future__ import annotations

from threading import Thread

import pytest

from cqdagpcfg_parallel.distributed import (
    JobContext,
    NodeAgent,
    RoleClient,
    RoleControlReply,
    RoleControlStats,
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
        model_id="toy-model",
        model_fingerprint="sha256:abc",
        model_connect="cqpcfg://127.0.0.1:7000",
        control_connect="cqpcfg://127.0.0.1:7001",
        batch_connect="cqpcfg://127.0.0.1:7002",
        ack_connect="cqpcfg://127.0.0.1:7003",
        source_mode="root",
        demand_window=8,
        job_payload={
            "algorithm": "sha256",
            "limit": 16,
            "model_fingerprint": "sha256:abc",
            "targets": [{"rank": 0, "guess": "ab12!", "hash": "deadbeef"}],
        },
    )


def test_job_context_round_trips_dict() -> None:
    context = make_job_context()

    decoded = JobContext.from_dict(context.to_dict())

    assert decoded == context
    encoded = context.to_dict()
    assert "job_payload" in encoded
    assert "hash_algorithm" not in encoded
    assert "targets" not in encoded


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
        node_id="node-0:bootstrap",
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
    assert reply.role == "idle"
    assert reply.job_context == context
    assert "node-0:bootstrap" not in controller.roles
    assert "node-0:bootstrap" not in controller.status_by_node
    assert client.stats.request_seconds > 0.0
    assert client.stats.roundtrip_ewma_seconds > 0.0


def test_node_agent_role_refresh_interval_respects_overhead_budget() -> None:
    class Source:
        def read_range(self, node_id, start, end):
            return ()

    class FakeRoleClient:
        roundtrip_ewma_seconds = 0.2

        @property
        def stats(self):
            return RoleControlStats(roundtrip_ewma_seconds=self.roundtrip_ewma_seconds)

        def close(self) -> None:
            pass

    agent = NodeAgent(
        node_id="node-0",
        role_client=FakeRoleClient(),
        control_endpoint=ZmqEndpoint("tcp://127.0.0.1:7001", bind=False),
        batch_endpoint=ZmqEndpoint("tcp://127.0.0.1:7002", bind=False),
        source=Source(),
        consume_batch=lambda batch: None,
        role_refresh_interval_seconds=0.05,
        role_refresh_max_interval_seconds=1.0,
        role_control_overhead_budget=0.01,
    )

    interval = agent._optimized_role_refresh_interval(
        RoleControlReply(role="generator"),
        RoleControlReply(role="generator"),
    )
    assert interval == 1.0

    agent.role_client.roundtrip_ewma_seconds = 0.0002
    interval = agent._optimized_role_refresh_interval(
        RoleControlReply(role="generator"),
        RoleControlReply(role="generator"),
    )
    assert interval == 0.05

    interval = agent._optimized_role_refresh_interval(
        RoleControlReply(role="generator"),
        RoleControlReply(role="consumer"),
    )
    assert interval == 0.05


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
            replies.append(client.request({"current_role": "idle"}))
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
        reply = client.request({"current_role": "idle"})
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
                "current_role": "idle",
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


def test_role_controller_expires_stale_nodes_and_reuses_role(tmp_path) -> None:
    pytest.importorskip("zmq")
    endpoint_uri = f"ipc://{tmp_path / 'expire-role.sock'}"
    controller = RoleController(
        endpoint=ZmqEndpoint(endpoint_uri, bind=True, linger_ms=0),
        roles={},
        auto_assign_roles=("consumer",),
    )
    first = RoleClient(
        node_id="node-old",
        endpoint=ZmqEndpoint(endpoint_uri, bind=False, linger_ms=0),
        reply_timeout_ms=1000,
    )
    second = RoleClient(
        node_id="node-new",
        endpoint=ZmqEndpoint(endpoint_uri, bind=False, linger_ms=0),
        reply_timeout_ms=1000,
    )

    try:
        thread = Thread(target=lambda: controller.poll(timeout_ms=1000), daemon=True)
        thread.start()
        assert first.request({"current_role": "idle"}).role == "consumer"
        thread.join(2.0)
        assert not thread.is_alive()

        expired = controller.expire_stale_nodes(timeout_seconds=0.0)
        assert expired[0][0] == "node-old"
        assert "node-old" not in controller.roles
        assert "node-old" not in controller.status_by_node

        thread = Thread(target=lambda: controller.poll(timeout_ms=1000), daemon=True)
        thread.start()
        assert second.request({"current_role": "idle"}).role == "consumer"
        thread.join(2.0)
        assert not thread.is_alive()
    finally:
        first.close()
        second.close()
        controller.close()


def test_parse_byte_size_accepts_docker_style_units() -> None:
    assert parse_byte_size("512m") == 512 * 1024**2
    assert parse_byte_size("4g") == 4 * 1024**3
