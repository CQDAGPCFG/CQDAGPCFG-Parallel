from __future__ import annotations

import pytest
from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.protocol import NodeId, WorkerId
from cqdagpcfg_parallel.storage import (
    BlockStateSnapshot,
    CompactDistributedTrackerCheckpointWriter,
    DistributedTrackerCheckpoint,
    FrontierEntrySnapshot,
    GuessRecordSnapshot,
    GuessRecordWindowSnapshot,
    IndexCountSnapshot,
    InMemoryModelArtifactStore,
    LogProbRankEntry,
    MergedBlockStateSnapshot,
    ModelManifest,
    NodeWatermarkSnapshot,
    ResultWindowSnapshot,
    StateMigrationSnapshot,
    StructureStreamStateSnapshot,
)


def test_state_migration_snapshot_json_round_trips() -> None:
    record = GuessRecord(
        prob=0.5,
        guess="ab12",
        structure_index=0,
        structure_name="A1D2",
        ranks=(0, 1),
    )
    stream = StructureStreamStateSnapshot(
        node_id=NodeId("structure:0:A1D2"),
        structure_index=0,
        structure_name="A1D2",
        symbols=("A1", "D2"),
        root_signature=("A1", "D2"),
        max_records=128,
        stream_base=10,
        ready_end=11,
        consumer_id=0,
        guess_cache=GuessRecordWindowSnapshot(
            base=10,
            entries=(GuessRecordSnapshot.from_record(record),),
        ),
    )
    merged = MergedBlockStateSnapshot(
        left_signature=("A1",),
        right_signature=("D2",),
        left_consumer=0,
        right_consumer=1,
        left_cache=ResultWindowSnapshot(
            base=3,
            entries=(LogProbRankEntry(log_prob=-0.2, rank_key=(3,)),),
        ),
        right_cache=ResultWindowSnapshot(
            base=4,
            entries=(LogProbRankEntry(log_prob=-0.3, rank_key=(4,)),),
        ),
        frontier=(
            FrontierEntrySnapshot(
                neg_log_prob=0.7,
                rank_key=((3,), (4,)),
                left_index=3,
            ),
        ),
        active_rows=(IndexCountSnapshot(index=3, value=4),),
        active_right_counts=(IndexCountSnapshot(index=4, value=1),),
        right_min_heap=(4,),
        min_active_left=3,
        min_active_right=4,
        max_started_row=5,
        initialized=True,
        seeded_zero=True,
        refines_since_reclaim=2,
        reclaim_units=2,
        next_reclaim_after=3,
    )
    root_block = BlockStateSnapshot(
        signature=("A1", "D2"),
        kind="local_merged",
        results=ResultWindowSnapshot(
            base=10,
            entries=(LogProbRankEntry(log_prob=-0.5, rank_key=((3,), (4,))),),
        ),
        seed_result=LogProbRankEntry(log_prob=-0.1, rank_key=((0,), (0,))),
        consumer_upto=(10,),
        expected_consumers=1,
        registered_consumers=1,
        merged=merged,
    )
    snapshot = StateMigrationSnapshot.create(
        model_fingerprint="model-sha256:abc",
        source_worker_id=WorkerId("worker-a"),
        target_worker_id=WorkerId("worker-b"),
        streams=(stream,),
        blocks=(root_block,),
        watermarks=(
            NodeWatermarkSnapshot(
                node_id=stream.node_id,
                ready_end=11,
                reclaim_before=10,
                target_end=16,
            ),
        ),
    )

    restored = StateMigrationSnapshot.from_json(snapshot.to_json())

    assert restored == snapshot
    assert restored.content_digest() == snapshot.content_digest()
    assert restored.payload_bytes() == len(snapshot.to_json())
    assert restored.block_count == 1
    assert restored.stream_count == 1
    assert restored.streams[0].guess_cache.entries[0].to_record().stable_string() == (
        record.stable_string()
    )


def test_state_migration_snapshot_rejects_duplicate_block_signatures() -> None:
    stream = StructureStreamStateSnapshot(
        node_id=NodeId("structure:0:A1"),
        structure_index=0,
        structure_name="A1",
        symbols=("A1",),
        root_signature=("A1",),
        max_records=8,
        stream_base=0,
        ready_end=0,
        consumer_id=0,
    )
    block = BlockStateSnapshot(signature=("A1",), kind="leaf")

    with pytest.raises(ValueError, match="duplicate block signatures"):
        StateMigrationSnapshot.create(
            model_fingerprint="model-sha256:abc",
            source_worker_id=WorkerId("worker-a"),
            streams=(stream,),
            blocks=(block, block),
        )


def test_state_migration_snapshot_requires_stream_root_block() -> None:
    stream = StructureStreamStateSnapshot(
        node_id=NodeId("structure:0:A1"),
        structure_index=0,
        structure_name="A1",
        symbols=("A1",),
        root_signature=("A1",),
        max_records=8,
        stream_base=0,
        ready_end=0,
        consumer_id=0,
    )
    block = BlockStateSnapshot(signature=("D1",), kind="leaf")

    with pytest.raises(ValueError, match="stream root block is missing"):
        StateMigrationSnapshot.create(
            model_fingerprint="model-sha256:abc",
            source_worker_id=WorkerId("worker-a"),
            streams=(stream,),
            blocks=(block,),
        )


def test_model_manifest_fingerprint_is_stable_for_json_payloads() -> None:
    manifest_a = ModelManifest.from_json_payload({"b": 2, "a": 1}, model_id="toy")
    manifest_b = ModelManifest.from_json_payload({"a": 1, "b": 2}, model_id="toy")

    assert manifest_a.model_fingerprint == manifest_b.model_fingerprint
    manifest_a.require_match(manifest_b.model_fingerprint)


def test_model_artifact_store_fetches_model_by_fingerprint_chunks() -> None:
    store = InMemoryModelArtifactStore()
    payload = b'{"model":"cqdag","tables":[1,2,3,4,5]}'

    manifest = store.put_model(payload, model_id="toy", chunk_size=7)
    chunks = []
    offset = 0
    while True:
        chunk = store.fetch_chunk(manifest.model_fingerprint, offset=offset)
        chunks.append(chunk.data)
        offset = chunk.end_offset
        if chunk.final:
            break

    assert manifest.model_id == "toy"
    assert manifest.size_bytes == len(payload)
    assert manifest.chunk_count > 1
    assert b"".join(chunks) == payload
    assert store.fetch_all(manifest.model_fingerprint) == payload


def test_distributed_tracker_checkpoint_json_round_trips() -> None:
    checkpoint = DistributedTrackerCheckpoint.create(
        emitted_count=2,
        shard_cursors={
            NodeId("root:0"): 1,
            NodeId("root:1"): 1,
        },
        emitted_stable_records=("record-a", "record-b"),
    )

    restored = DistributedTrackerCheckpoint.from_json(checkpoint.to_json())

    assert restored.emitted_count == 2
    assert restored.cursor_for(NodeId("root:0")) == 1
    assert restored.cursor_for(NodeId("root:missing")) == 0
    assert restored.emitted_stable_records == ("record-a", "record-b")


def test_compact_distributed_tracker_checkpoint_uses_external_log(tmp_path) -> None:
    checkpoint_path = tmp_path / "tracker.checkpoint.json"
    log_path = tmp_path / "stable-records.jsonl"
    writer = CompactDistributedTrackerCheckpointWriter(
        checkpoint_path=checkpoint_path,
        stable_log_path=log_path,
    )
    checkpoint = DistributedTrackerCheckpoint.create(
        emitted_count=2,
        shard_cursors={NodeId("root:0"): 2},
        emitted_stable_records=("record-a", "record-b"),
    )

    writer.write(checkpoint)
    restored = DistributedTrackerCheckpoint.read(checkpoint_path)

    assert restored.emitted_stable_records == ()
    assert restored.emitted_log_uri == str(log_path)
    assert restored.stable_records_for_resume() == ("record-a", "record-b")
    assert checkpoint_path.stat().st_size < log_path.stat().st_size + 256
