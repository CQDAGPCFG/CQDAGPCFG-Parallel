from __future__ import annotations

from threading import Thread

import pytest

from cqdagpcfg_parallel.runtime import (
    JsonModelFetchCodec,
    LazyLocalResultSource,
    ModelFetchTimeoutError,
    ModelFetchRequest,
    ZmqEndpoint,
    ZmqModelArtifactClient,
    ZmqModelArtifactServer,
)
from cqdagpcfg_parallel.storage import (
    BoundedModelPageCache,
    FileModelArtifactCache,
    FileModelArtifactStore,
    FilePagedModelArtifactStore,
    InMemoryModelArtifactStore,
    slot_page_id,
    structure_page_id,
)


def test_json_model_fetch_codec_round_trips_manifest_request() -> None:
    request = ModelFetchRequest(kind="manifest", model_id="toy")

    decoded = JsonModelFetchCodec.loads_request(JsonModelFetchCodec.dumps_request(request))

    assert decoded == request


def test_zmq_model_artifact_transport_fetches_manifest_and_chunks() -> None:
    zmq = pytest.importorskip("zmq")
    context = zmq.Context()
    address = "inproc://model-artifact-fetch"
    payload = b'{"cqdagpcfg":"model","tables":[1,2,3,4,5,6]}'
    store = InMemoryModelArtifactStore()
    manifest = store.put_model(payload, model_id="toy", chunk_size=9)
    server = ZmqModelArtifactServer(
        ZmqEndpoint(address, bind=True, linger_ms=0),
        store,
        context=context,
    )
    client = ZmqModelArtifactClient(
        ZmqEndpoint(address, bind=False, linger_ms=0),
        context=context,
    )

    def serve() -> None:
        with server:
            for _ in range(manifest.chunk_count + 2):
                assert server.serve_once(timeout_ms=1000)

    thread = Thread(target=serve, daemon=True)
    thread.start()
    try:
        with client:
            fetched_manifest = client.manifest("toy")
            fetched_payload = client.fetch_all("toy")
    finally:
        thread.join(2.0)
        client.close()
        server.close()
        context.term()

    assert not thread.is_alive()
    assert fetched_manifest == manifest
    assert fetched_payload == payload


def test_zmq_model_artifact_client_times_out_without_reply() -> None:
    zmq = pytest.importorskip("zmq")
    context = zmq.Context()
    address = "inproc://model-artifact-timeout"
    socket = context.socket(zmq.REP)
    socket.setsockopt(zmq.LINGER, 0)
    socket.bind(address)
    received = []

    def receive_without_reply() -> None:
        if socket.poll(1000, zmq.POLLIN):
            received.append(socket.recv())

    thread = Thread(target=receive_without_reply, daemon=True)
    thread.start()
    client = ZmqModelArtifactClient(
        ZmqEndpoint(address, bind=False, linger_ms=0),
        context=context,
        timeout_ms=20,
        retries=1,
    )

    try:
        with pytest.raises(ModelFetchTimeoutError):
            client.manifest("toy")
    finally:
        client.close()
        thread.join(2.0)
        socket.close()
        context.term()

    assert received


def test_file_model_artifact_cache_materializes_chunks(tmp_path) -> None:
    payload = b'{"cqdagpcfg":"large-ish","rows":["a","b","c","d"]}'
    model_path = tmp_path / "model.json"
    model_path.write_bytes(payload)
    store = FileModelArtifactStore.from_path(model_path, model_id="toy", chunk_size=7)

    class FakeClient:
        def manifest(self, model_id: str):
            return store.manifest_for_model(model_id)

        def fetch_chunk(self, model_fingerprint: str, *, offset: int):
            return store.fetch_chunk(model_fingerprint, offset=offset)

    cache = FileModelArtifactCache(tmp_path / "cache")
    path, manifest = cache.materialize(FakeClient(), "toy")

    assert path.read_bytes() == payload
    assert cache.is_cached(manifest)


def test_lazy_local_result_source_loads_only_on_read() -> None:
    loads = 0

    class ToySource:
        def read_range(self, node_id, start: int, end: int):
            return tuple(range(start, end))

    def factory():
        nonlocal loads
        loads += 1
        return ToySource()

    source = LazyLocalResultSource(factory)

    assert not source.loaded_once
    assert source.stats().cached_records == 0
    assert loads == 0
    assert source.read_range("root", 2, 5) == (2, 3, 4)
    assert source.loaded_once
    assert loads == 1


def test_bounded_model_page_cache_fetches_and_evicts() -> None:
    payload = b"abcdefghijklmnopqrstuvwxyz"
    store = InMemoryModelArtifactStore()
    manifest = store.put_model(payload, model_id="toy", chunk_size=5)

    class FakeClient:
        def fetch_chunk(self, model_fingerprint: str, *, offset: int):
            return store.fetch_chunk(model_fingerprint, offset=offset)

    cache = BoundedModelPageCache(max_bytes=10)

    first = cache.get_or_fetch(FakeClient(), manifest, offset=2)
    second = cache.get_or_fetch(FakeClient(), manifest, offset=4)
    third = cache.get_or_fetch(FakeClient(), manifest, offset=11)

    assert first.data == b"abcde"
    assert second.data == b"abcde"
    assert third.data == b"klmno"
    assert cache.stats.hits == 1
    assert cache.stats.misses == 2
    assert cache.stats.bytes <= 10


def test_file_paged_model_artifact_store_writes_pages_to_disk(tmp_path) -> None:
    model_path = tmp_path / "model.json"
    page_root = tmp_path / "pages"
    model_path.write_text(
        """
        {
          "metadata": {"source": "unit"},
          "structures": [
            {"name": "D1", "symbols": ["D1"], "base_prob": 1.0}
          ],
          "slot_tables": {
            "D1": {
              "unknown_prob": 0.0,
              "entries": [
                {"surface": "1", "prob": 0.6},
                {"surface": "2", "prob": 0.4}
              ]
            }
          }
        }
        """,
        encoding="utf-8",
    )

    store = FilePagedModelArtifactStore.from_path(
        model_path,
        model_id="toy",
        chunk_size=64,
        slot_page_size=1,
        structure_page_size=1,
        page_root=page_root,
    )
    manifest = store.paged_manifest_for_model("toy")
    slot_page = store.fetch_page(
        manifest.model_fingerprint,
        page_id=slot_page_id("D1", 0),
    )
    structure_page = store.fetch_page(
        manifest.model_fingerprint,
        page_id=structure_page_id(0),
    )

    assert list(page_root.rglob("*.json"))
    assert slot_page.data["entries"][0]["surface"] == "1"
    assert structure_page.data["structures"][0]["name"] == "D1"
