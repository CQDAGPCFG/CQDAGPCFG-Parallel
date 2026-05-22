from __future__ import annotations

import pytest
from CQDAGPCFG import GuessRecord

import cqdagpcfg_parallel.runtime.batch_transport as batch_transport
from cqdagpcfg_parallel.runtime import (
    BatchEndOfStream,
    BatchAck,
    BatchAckStatus,
    BinaryCandidateBatchCodec,
    BoundedBatchSink,
    CandidateBatch,
    JsonBatchAckCodec,
    JsonCandidateBatchCodec,
    MemoryBatchSink,
    ZmqEndpoint,
    ZmqEndpointBundle,
    ZmqPullBatchAckSource,
    ZmqPullBatchSource,
    ZmqPushBatchAckSink,
    ZmqPushBatchSink,
    publish_record_batches,
)


def _record(index: int, guess: str) -> GuessRecord:
    return GuessRecord(
        prob=1.0 / (index + 1),
        guess=guess,
        structure_index=index % 2,
        structure_name="A",
        ranks=(index,),
    )


def _batch(batch_id: int, start_rank: int, count: int = 2) -> CandidateBatch:
    return CandidateBatch.from_records(
        batch_id=batch_id,
        start_rank=start_rank,
        records=[_record(start_rank + offset, f"g{start_rank + offset}") for offset in range(count)],
    )


def test_json_candidate_batch_codec_round_trips() -> None:
    batch = _batch(3, 6, count=3)

    decoded = JsonCandidateBatchCodec.loads(JsonCandidateBatchCodec.dumps(batch))

    assert decoded.batch_id == batch.batch_id
    assert decoded.start_rank == batch.start_rank
    assert decoded.end_rank == batch.end_rank
    assert decoded.payload_bytes == batch.payload_bytes
    assert [record.stable_string() for record in decoded.records] == [
        record.stable_string() for record in batch.records
    ]


def test_json_candidate_batch_codec_round_trips_end_of_stream() -> None:
    decoded = JsonCandidateBatchCodec.loads_envelope(JsonCandidateBatchCodec.dumps_end())

    assert isinstance(decoded, BatchEndOfStream)
    assert decoded.reason == "complete"


def test_binary_candidate_batch_codec_round_trips() -> None:
    batch = _batch(4, 8, count=4)

    decoded = BinaryCandidateBatchCodec.loads(BinaryCandidateBatchCodec.dumps(batch))

    assert decoded.batch_id == batch.batch_id
    assert decoded.start_rank == batch.start_rank
    assert decoded.end_rank == batch.end_rank
    assert [record.stable_string() for record in decoded.records] == [
        record.stable_string() for record in batch.records
    ]


def test_cpp_binary_candidate_batch_serializer_matches_python(monkeypatch: pytest.MonkeyPatch) -> None:
    cpp_serializer = batch_transport.cpp_serialize_candidate_batch
    if cpp_serializer is None:
        pytest.skip("CQDAGPCFG C++ batch serializer is not available")
    batch = _batch(5, 10, count=4)

    monkeypatch.setattr(batch_transport, "cpp_serialize_candidate_batch", None)
    python_payload = BinaryCandidateBatchCodec.dumps(batch)
    monkeypatch.setattr(batch_transport, "cpp_serialize_candidate_batch", cpp_serializer)
    cpp_payload = BinaryCandidateBatchCodec.dumps(batch)

    assert cpp_payload == python_payload
    decoded = BinaryCandidateBatchCodec.loads(cpp_payload)
    assert decoded.guesses == batch.guesses


def test_binary_candidate_batch_codec_round_trips_end_of_stream() -> None:
    decoded = BinaryCandidateBatchCodec.loads_envelope(BinaryCandidateBatchCodec.dumps_end())

    assert isinstance(decoded, BatchEndOfStream)
    assert decoded.reason == "complete"


def test_bounded_batch_sink_decorates_downstream_sink() -> None:
    downstream = MemoryBatchSink(delay_seconds=0.001)
    sink = BoundedBatchSink(
        downstream,
        max_pending_batches=1,
        max_pending_candidates=2,
        max_pending_payload_bytes=64,
    )

    with sink:
        for batch_id in range(5):
            sink.publish(_batch(batch_id, batch_id * 2))

    assert [batch.batch_id for batch in downstream.batches] == [0, 1, 2, 3, 4]
    assert downstream.closed
    assert sink.stats.forwarded_batches == 5
    assert sink.stats.forwarded_candidates == 10
    assert sink.stats.peak_pending_batches <= 1
    assert sink.stats.peak_pending_candidates <= 2
    assert sink.stats.peak_pending_payload_bytes <= 64
    assert sink.stats.producer_waits > 0


def test_publish_record_batches_uses_sink_interface() -> None:
    downstream = MemoryBatchSink()
    records = [_record(index, f"g{index}") for index in range(5)]

    publish_record_batches(
        records,
        downstream,
        batch_size=2,
        max_batch_payload_bytes=64,
    )

    assert downstream.closed
    assert [batch.guesses for batch in downstream.batches] == [
        ("g0", "g1"),
        ("g2", "g3"),
        ("g4",),
    ]


def test_zmq_push_pull_batch_transport_round_trips() -> None:
    zmq = pytest.importorskip("zmq")
    context = zmq.Context()
    address = "inproc://candidate-batch-roundtrip"
    batch = _batch(1, 2, count=3)

    source = ZmqPullBatchSource(
        ZmqEndpoint(address, bind=True, high_watermark=2),
        context=context,
    )
    sink = ZmqPushBatchSink(
        ZmqEndpoint(address, bind=False, high_watermark=2),
        context=context,
    )

    try:
        with source, sink:
            sink.publish(batch)
            received = source.receive(timeout_ms=1000)
    finally:
        context.term()

    assert received is not None
    assert received.batch_id == batch.batch_id
    assert received.start_rank == batch.start_rank
    assert [record.stable_string() for record in received.records] == [
        record.stable_string() for record in batch.records
    ]
    assert sink.stats.messages == 1
    assert sink.stats.batch_messages == 1
    assert sink.stats.bytes > 0
    assert sink.stats.serialize_seconds >= 0.0
    assert sink.stats.send_seconds >= 0.0
    assert source.stats.messages == 1
    assert source.stats.batch_messages == 1
    assert source.stats.bytes == sink.stats.bytes
    assert source.stats.deserialize_seconds >= 0.0
    assert source.stats.recv_seconds >= 0.0


def test_zmq_push_pull_batch_transport_sends_end_of_stream() -> None:
    zmq = pytest.importorskip("zmq")
    context = zmq.Context()
    address = "inproc://candidate-batch-end-of-stream"

    source = ZmqPullBatchSource(
        ZmqEndpoint(address, bind=True, high_watermark=2),
        context=context,
    )
    sink = ZmqPushBatchSink(
        ZmqEndpoint(address, bind=False, high_watermark=2),
        context=context,
    )

    try:
        with source, sink:
            sink.publish_end_of_stream()
            received = source.receive_envelope(timeout_ms=1000)
    finally:
        context.term()

    assert isinstance(received, BatchEndOfStream)
    assert sink.stats.end_messages == 1
    assert source.stats.end_messages == 1


def test_json_batch_ack_codec_round_trips() -> None:
    ack = BatchAck(
        batch_id=3,
        consumer_id="consumer-a",
        status=BatchAckStatus.FAILED,
        error="temporary failure",
        outputs=({"rank": 7, "guess": "secret"},),
    )

    decoded = JsonBatchAckCodec.loads(JsonBatchAckCodec.dumps(ack))

    assert decoded == ack


def test_zmq_push_pull_batch_ack_transport_round_trips() -> None:
    zmq = pytest.importorskip("zmq")
    context = zmq.Context()
    address = "inproc://candidate-batch-ack-roundtrip"
    ack = BatchAck(
        batch_id=9,
        consumer_id="consumer-b",
        status=BatchAckStatus.DONE,
    )

    source = ZmqPullBatchAckSource(
        ZmqEndpoint(address, bind=True, high_watermark=2),
        context=context,
    )
    sink = ZmqPushBatchAckSink(
        ZmqEndpoint(address, bind=False, high_watermark=2),
        context=context,
    )

    try:
        with source, sink:
            sink.publish(ack)
            received = source.receive(timeout_ms=1000)
    finally:
        context.term()

    assert received == ack
    assert sink.stats.messages == 1
    assert source.stats.messages == 1
    assert source.stats.bytes == sink.stats.bytes


def test_cqpcfg_uri_maps_to_tcp_endpoint() -> None:
    endpoint = ZmqEndpoint.from_uri("cqpcfg://127.0.0.1:5555?hwm=3&linger=4")

    assert endpoint.address == "tcp://127.0.0.1:5555"
    assert not endpoint.bind
    assert endpoint.high_watermark == 3
    assert endpoint.linger_ms == 4


def test_endpoint_bundle_derives_protocol_subchannels() -> None:
    bundle = ZmqEndpointBundle.from_base_uri(
        "cqpcfg://0.0.0.0:5555?hwm=3",
        advertise_host="tracker.example",
    )

    assert bundle.control == "cqpcfg://tracker.example:5555?hwm=3"
    assert bundle.batch == "cqpcfg://tracker.example:5556?hwm=3"
    assert bundle.role == "cqpcfg://tracker.example:5557?hwm=3"
    assert bundle.ack == "cqpcfg://tracker.example:5558?hwm=3"
    assert bundle.model == "cqpcfg://tracker.example:5559?hwm=3"
