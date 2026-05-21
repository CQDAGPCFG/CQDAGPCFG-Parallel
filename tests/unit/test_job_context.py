from __future__ import annotations

from threading import Thread

import pytest

from cqdagpcfg_parallel.distributed import JobContext, RoleClient, RoleController
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
