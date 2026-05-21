from __future__ import annotations

import pytest
from CQDAGPCFG.training import PCFGTrainer

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGBlockGraphAdapter,
    CQDAGRecordSource,
    CQDAGStructureRecordSource,
    ROOT_NODE_ID,
    SerialCQDAGOracle,
)
from cqdagpcfg_parallel.protocol import (
    ChunkSizePolicy,
    NodeStateTable,
    SchedulerConfig,
    WorkerId,
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
    assert result.stable_records == tuple(record.stable_string() for record in baseline.outputs)
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
    assert stats.cached_records == end - start
    assert stats.peak_cached_records == end - start
    assert stats.reclaimed_records >= start


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
    assert migrated.streams[0].ready_end == cut
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
