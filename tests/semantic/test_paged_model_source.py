from __future__ import annotations

import json
from threading import Event, Thread

import pytest

from CQDAGPCFG import load_model, save_model
from CQDAGPCFG.cpp_backend import cpp_backend_available
from CQDAGPCFG.training import PCFGTrainer

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGNodeSourceConfig,
    CQDAGRecordSource,
    PagedCQDAGRecordSource,
    build_cqdag_node_source,
    build_paged_model,
)
from cqdagpcfg_parallel.protocol import stable_digest
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


def test_cpp_node_source_fetches_model_artifact_and_preserves_prefix(tmp_path) -> None:
    pytest.importorskip("zmq")
    if not cpp_backend_available():
        pytest.skip("CQDAGPCFG C++ backend is not built")
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
    endpoint_uri = f"ipc://{tmp_path / 'cpp-model-artifact.sock'}"
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
        config = CQDAGNodeSourceConfig(
            model_path=None,
            model_connect=endpoint_uri,
            model_id="toy",
            generation_backend="cpp",
        )
        source = build_cqdag_node_source(
            config,
            model_cache_dir=tmp_path / "cache",
            limit=24,
        )
        baseline = CQDAGRecordSource(load_model(model_path), max_records=32)
        baseline_records = baseline.read_range("root", 0, 24)
        cpp_records = source.read_range("root", 0, 24)
    finally:
        stop.set()
        thread.join(2.0)
        server.close()

    assert getattr(source, "prefer_cpp") is True
    assert stable_digest(cpp_records) == stable_digest(baseline_records)


def test_paged_cqdag_source_normalizes_slot_pages_like_loaded_model(tmp_path) -> None:
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
    raw = json.loads(model_path.read_text(encoding="utf-8"))
    first_symbol = next(iter(raw["slot_tables"]))
    for entry in raw["slot_tables"][first_symbol]["entries"]:
        entry["prob"] *= 0.5
    model_path.write_text(json.dumps(raw), encoding="utf-8")

    normalized_model = load_model(model_path)
    store = FilePagedModelArtifactStore.from_path(
        model_path,
        model_id="toy",
        slot_page_size=2,
        structure_page_size=2,
    )
    endpoint_uri = f"ipc://{tmp_path / 'normalized-model-page.sock'}"
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
        baseline = CQDAGRecordSource(normalized_model, max_records=32, prefer_cpp=False)
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
