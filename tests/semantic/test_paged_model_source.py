from __future__ import annotations

from threading import Event, Thread

import pytest

from CQDAGPCFG import save_model
from CQDAGPCFG.training import PCFGTrainer

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGRecordSource,
    PagedCQDAGRecordSource,
    build_paged_model,
)
from cqdagpcfg_parallel.runtime import ZmqEndpoint, ZmqModelArtifactServer
from cqdagpcfg_parallel.storage import FilePagedModelArtifactStore


def test_paged_cqdag_source_matches_serial_prefix(tmp_path) -> None:
    pytest.importorskip("zmq")
    model = PCFGTrainer().train(
        (
            "ab12!",
            "ab12!",
            "cd12!",
            "ab34@",
            "password12",
            "hello2024!",
        )
    )
    model_path = tmp_path / "model.json"
    save_model(model, model_path)
    store = FilePagedModelArtifactStore.from_path(
        model_path,
        model_id="toy",
        slot_page_size=2,
        structure_page_size=2,
    )
    endpoint_uri = f"ipc://{tmp_path / 'model-page.sock'}"
    server = ZmqModelArtifactServer(
        ZmqEndpoint(endpoint_uri, bind=True, linger_ms=0),
        store,
    )
    stop = Event()

    def serve() -> None:
        with server:
            while not stop.is_set():
                server.serve_once(timeout_ms=50)

    thread = Thread(target=serve, daemon=True)
    thread.start()
    try:
        paged_model = build_paged_model(
            endpoint=endpoint_uri,
            model_id="toy",
            max_json_pages=8,
        )
        baseline = CQDAGRecordSource(model, max_records=32, prefer_cpp=False)
        paged = PagedCQDAGRecordSource(paged_model, max_records=32)

        baseline_records = baseline.read_range("root", 0, 16)
        paged_records = paged.read_range("root", 0, 16)
    finally:
        if "paged_model" in locals():
            paged_model.client.close()
        stop.set()
        thread.join(2.0)
        server.close()

    assert [record.stable_string() for record in paged_records] == [
        record.stable_string() for record in baseline_records
    ]
    assert paged_model.stats.json_page_misses > 0
