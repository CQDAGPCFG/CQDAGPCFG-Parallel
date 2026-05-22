from __future__ import annotations

import pytest
from CQDAGPCFG import save_model
from CQDAGPCFG.cpp_backend import CppOptimizedCQDAGEnumerator, cpp_backend_available
from CQDAGPCFG.training import PCFGTrainer

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGBlockGraphAdapter,
    CQDAGNodeSourceConfig,
    CQDAGRecordSource,
    CQDAGStructureRecordSource,
    CppFileCQDAGRecordSource,
    ROOT_NODE_ID,
    SerialCQDAGOracle,
    build_cqdag_node_source,
    resolve_generation_backend,
)
from cqdagpcfg_parallel.protocol import (
    ChunkSizePolicy,
    NodeStateTable,
    SchedulerConfig,
    WorkerId,
    stable_digest,
    stable_record_string,
)
from cqdagpcfg_parallel.storage import StateMigrationSnapshot
from cqdagpcfg_parallel.simulation import (
    ProtocolSimulatorConfig,
    SingleProcessProtocolSimulator,
)


def _toy_model():
    return PCFGTrainer().train(
        [
            "ab12!",
            "ab12!",
            "cd12!",
            "ab34@",
            "p@ssw0rd",
            "password12",
            "dragonball99",
            "moon24@",
            "star77#",
            "1990abc!",
            "asdf12!",
            "hello2024!",
            "hello2024!",
            "elite99",
        ]
    )


@pytest.mark.parametrize("policy", list(ChunkSizePolicy))
def test_protocol_loop_preserves_cqdagpcfg_serial_prefix(policy: ChunkSizePolicy) -> None:
    model = _toy_model()
    limit = 80
    demand_window = 16
    oracle = SerialCQDAGOracle(model)
    baseline = oracle.run(limit)
    entropy = CQDAGBlockGraphAdapter(model).root_node.entropy
    source = CQDAGRecordSource(
        model,
        node_id=ROOT_NODE_ID,
        max_records=limit + demand_window,
    )
    simulator = SingleProcessProtocolSimulator(
        source=source,
        config=ProtocolSimulatorConfig(
            scheduler=SchedulerConfig(
                policy=policy,
                fixed_chunk_size=8,
                min_chunk_size=1,
                max_chunk_size=32,
                entropy_lambda=0.25,
            ),
            node_id=ROOT_NODE_ID,
            demand_window=demand_window,
            entropy=entropy,
        ),
    )

    result = simulator.run(limit)

    assert result.digest == baseline.digest
    assert result.stable_records == tuple(
        stable_record_string(record) for record in baseline.outputs
    )
    assert result.stats.scheduled_items > 0


def test_cqdag_adapter_exports_structure_scheduling_features() -> None:
    model = _toy_model()
    adapter = CQDAGBlockGraphAdapter(model)
    nodes = adapter.structure_nodes()

    assert len(nodes) == len(model.structures)
    assert all(node.priority == model.structures[node.structure_index].base_prob for node in nodes)
    assert all(node.estimated_cost > 0.0 for node in nodes)
    assert all(0.0 <= node.slot_dispersion <= 1.0 for node in nodes)

    states = NodeStateTable()
    adapter.apply_scheduling_features(states)
    root = states.get(ROOT_NODE_ID)
    child = states.get(nodes[0].node_id)

    assert root.priority > 0.0
    assert child.priority == nodes[0].priority

    states.register_demand(ROOT_NODE_ID, 4, priority=root.priority)
    states.register_demand(nodes[0].node_id, 1, priority=child.priority)
    states.apply_priority_donations()

    assert states.get(nodes[0].node_id).donated_priority > 0.0


def test_cqdag_structure_source_reclaims_protocol_prefix_without_changing_stream() -> None:
    model = _toy_model()
    adapter = CQDAGBlockGraphAdapter(model)
    node = max(adapter.structure_nodes(), key=lambda descriptor: descriptor.cardinality)
    limit = min(node.cardinality, 16)
    if limit < 4:
        pytest.skip("toy model did not produce a large enough structure stream")
    mid = limit // 2

    baseline_source = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=limit,
        adapter=adapter,
    )
    baseline = baseline_source.read_range(node.node_id, 0, limit)

    reclaiming_source = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=limit,
        adapter=adapter,
    )
    first = reclaiming_source.read_range(node.node_id, 0, mid)
    before = reclaiming_source.stats()

    assert before.cached_records >= mid
    assert reclaiming_source.reclaim_before(node.node_id, mid) == mid
    after = reclaiming_source.stats()
    assert after.cached_records < before.cached_records
    assert after.reclaimed_records >= mid

    second = reclaiming_source.read_range(node.node_id, mid, limit)
    assert tuple(record.stable_string() for record in first + second) == tuple(
        record.stable_string() for record in baseline
    )

    reclaiming_source.reclaim_before(node.node_id, limit)
    assert reclaiming_source.stats().cached_records == 0


def test_cqdag_structure_source_skips_unneeded_prefix_without_caching_records() -> None:
    model = _toy_model()
    adapter = CQDAGBlockGraphAdapter(model)
    node = max(adapter.structure_nodes(), key=lambda descriptor: descriptor.cardinality)
    limit = min(node.cardinality, 16)
    if limit < 8:
        pytest.skip("toy model did not produce a large enough structure stream")
    start = limit // 2
    end = start + 3

    baseline_source = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=limit,
        adapter=adapter,
    )
    baseline = baseline_source.read_range(node.node_id, start, end)

    skipping_source = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=limit,
        adapter=adapter,
    )
    skipped = skipping_source.read_range(node.node_id, start, end)
    stats = skipping_source.stats()

    assert tuple(record.stable_string() for record in skipped) == tuple(
        record.stable_string() for record in baseline
    )
    assert stats.cached_records >= end - start
    assert stats.cached_records <= limit - start
    assert stats.peak_cached_records >= end - start
    assert stats.reclaimed_records >= start


def test_cpp_cqdag_structure_source_matches_python_stream_and_skip() -> None:
    if not cpp_backend_available():
        pytest.skip("CQDAGPCFG C++ backend is not built")
    model = _toy_model()
    adapter = CQDAGBlockGraphAdapter(model)
    node = max(adapter.structure_nodes(), key=lambda descriptor: descriptor.cardinality)
    limit = min(node.cardinality, 24)
    if limit < 8:
        pytest.skip("toy model did not produce a large enough structure stream")
    start = limit // 2
    end = start + 3

    baseline_source = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=limit,
        adapter=adapter,
        prefer_cpp=False,
    )
    baseline = baseline_source.read_range(node.node_id, 0, limit)

    cpp_source = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=limit,
        adapter=adapter,
        prefer_cpp=True,
    )
    cpp_prefix = cpp_source.read_range(node.node_id, 0, limit)

    skipping_source = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=limit,
        adapter=adapter,
        prefer_cpp=True,
    )
    skipped = skipping_source.read_range(node.node_id, start, end)
    stats = skipping_source.stats()

    assert tuple(record.stable_string() for record in cpp_prefix) == tuple(
        record.stable_string() for record in baseline
    )
    assert tuple(record.stable_string() for record in skipped) == tuple(
        record.stable_string() for record in baseline[start:end]
    )
    assert stats.cached_records >= end - start
    assert stats.cached_records <= limit - start
    assert stats.peak_cached_records >= end - start
    assert stats.reclaimed_records >= start


def test_cpp_file_cqdag_root_source_matches_serial_prefix(tmp_path) -> None:
    if not cpp_backend_available():
        pytest.skip("CQDAGPCFG C++ backend is not built")
    model = _toy_model()
    model_path = tmp_path / "model.json"
    save_model(model, model_path)
    limit = 80

    baseline = SerialCQDAGOracle(model).run(limit)
    source = CppFileCQDAGRecordSource(model_path, max_records=limit)
    records = source.read_range(ROOT_NODE_ID, 0, limit)

    assert stable_digest(records) == baseline.digest
    assert tuple(stable_record_string(record) for record in records) == tuple(
        stable_record_string(record) for record in baseline.outputs
    )


def test_cpp_structure_state_export_and_restore(tmp_path) -> None:
    if not cpp_backend_available():
        pytest.skip("CQDAGPCFG C++ backend is not built")
    model = _toy_model()
    model_path = tmp_path / "model.json"
    save_model(model, model_path)
    adapter = CQDAGBlockGraphAdapter(model)
    node = max(adapter.structure_nodes(), key=lambda descriptor: descriptor.cardinality)
    if node.cardinality < 8:
        pytest.skip("toy model did not produce a large enough structure stream")

    enumerator = CppOptimizedCQDAGEnumerator.from_json_file(model_path)
    prefix = tuple(enumerator.iter_structure_records(node.structure_index, 0, 4))
    state = enumerator.structure_state(node.structure_index)
    assert state["produced"] >= 4
    assert state["has_cursor"] is True

    restored = CppOptimizedCQDAGEnumerator.from_json_file(model_path)
    restored.restore_structure_state(node.structure_index, int(state["produced"]))
    suffix = tuple(
        restored.iter_structure_records(
            node.structure_index,
            int(state["produced"]),
            int(state["produced"]) + 4,
        )
    )
    baseline = tuple(
        CppOptimizedCQDAGEnumerator(model).iter_structure_records(
            node.structure_index,
            0,
            int(state["produced"]) + 4,
        )
    )

    assert tuple(stable_record_string(record) for record in prefix) == tuple(
        stable_record_string(record) for record in baseline[:4]
    )
    assert tuple(stable_record_string(record) for record in suffix) == tuple(
        stable_record_string(record) for record in baseline[int(state["produced"]) :]
    )


def test_node_source_auto_uses_cpp_for_structure_source(tmp_path) -> None:
    if not cpp_backend_available():
        pytest.skip("CQDAGPCFG C++ backend is not built")
    model = _toy_model()
    model_path = tmp_path / "model.json"
    save_model(model, model_path)

    config = CQDAGNodeSourceConfig.from_explicit_model(
        model_path=model_path,
        model_connect=None,
        model_id="toy",
        source_mode="structure",
        generation_backend="auto",
    )
    source = build_cqdag_node_source(config, limit=16)

    assert resolve_generation_backend(config) == "cpp"
    assert source.prefer_cpp is True


def test_node_source_auto_uses_cpp_file_source_for_root(tmp_path) -> None:
    if not cpp_backend_available():
        pytest.skip("CQDAGPCFG C++ backend is not built")
    model = _toy_model()
    model_path = tmp_path / "model.json"
    save_model(model, model_path)

    config = CQDAGNodeSourceConfig.from_explicit_model(
        model_path=model_path,
        model_connect=None,
        model_id="toy",
        source_mode="root",
        generation_backend="auto",
    )
    source = build_cqdag_node_source(config, limit=16)

    assert resolve_generation_backend(config) == "cpp"
    assert isinstance(source, CppFileCQDAGRecordSource)


def test_cqdag_structure_source_state_migration_resumes_stream_after_json_roundtrip() -> None:
    model = _toy_model()
    adapter = CQDAGBlockGraphAdapter(model)
    node = max(adapter.structure_nodes(), key=lambda descriptor: descriptor.cardinality)
    limit = min(node.cardinality, 24)
    if limit < 8:
        pytest.skip("toy model did not produce a large enough structure stream")
    cut = limit // 2

    baseline_source = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=limit,
        adapter=adapter,
    )
    baseline = baseline_source.read_range(node.node_id, 0, limit)

    source_worker = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=limit,
        adapter=adapter,
    )
    prefix = source_worker.read_range(node.node_id, 0, cut)
    source_worker.reclaim_before(node.node_id, cut)
    snapshot = source_worker.capture_state(
        model_fingerprint="toy-model",
        source_worker_id=WorkerId("source-worker"),
        target_worker_id=WorkerId("target-worker"),
        node_ids=(node.node_id,),
    )
    migrated = StateMigrationSnapshot.from_json(snapshot.to_json())

    target_worker = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=limit,
        adapter=adapter,
    )
    target_worker.restore_state(migrated, expected_model_fingerprint="toy-model")
    suffix = target_worker.read_range(node.node_id, cut, limit)

    assert tuple(record.stable_string() for record in prefix + suffix) == tuple(
        record.stable_string() for record in baseline
    )
    assert migrated.streams[0].stream_base == cut
    assert migrated.streams[0].ready_end >= cut
    assert migrated.watermarks[0].reclaim_before == cut


def test_cqdag_structure_source_state_migration_rejects_wrong_model_fingerprint() -> None:
    model = _toy_model()
    adapter = CQDAGBlockGraphAdapter(model)
    node = max(adapter.structure_nodes(), key=lambda descriptor: descriptor.cardinality)
    source = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=8,
        adapter=adapter,
    )
    source.read_range(node.node_id, 0, 1)
    snapshot = source.capture_state(
        model_fingerprint="model-a",
        source_worker_id=WorkerId("source-worker"),
        node_ids=(node.node_id,),
    )
    target = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=8,
        adapter=adapter,
    )

    with pytest.raises(ValueError, match="model_fingerprint"):
        target.restore_state(snapshot, expected_model_fingerprint="model-b")
