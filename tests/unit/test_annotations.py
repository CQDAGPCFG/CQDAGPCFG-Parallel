from __future__ import annotations

from pathlib import Path

import pytest
from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.distributed import (
    AnnotatedConsumer,
    AnnotatedGenerator,
    WorkerResourceSpec,
    cqpcfg_consumer,
    cqpcfg_generator,
    cqpcfg_node_agent,
    cqpcfg_tracker,
    cqpcfg_worker,
)
from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    AnnotatedCQDAGPCFGNode,
    AnnotatedCQDAGPCFGTracker,
    CQDAGCandidate,
    CQDAGPCFGTracker,
    CqdagNodeAgentServiceConfig,
    CqdagTrackerServiceConfig,
    cqdagpcfg_consumer,
    cqdagpcfg_generator,
    cqdagpcfg_node_agent,
    cqdagpcfg_remote,
    cqdagpcfg_tracker as cqdagpcfg_tracker_adapter,
)
from cqdagpcfg_parallel.adapters.cqdagpcfg.tracker_service import (
    DEFAULT_CRACKING_ROOT_ARTIFACT_TARGET_BYTES,
    apply_endpoint_bundle,
    effective_max_parallel_leases_per_node,
    _apply_tracker_optimization_profile,
)
from cqdagpcfg_parallel.adapters.cqdagpcfg.node_service import (
    _apply_node_optimization_profile,
)
from cqdagpcfg_parallel.runtime import CandidateBatch, publish_record_batches
from cqdagpcfg_parallel.simulation import SequenceRecordSource


def test_tracker_annotation_exposes_bind_address() -> None:
    @cqpcfg_tracker(
        bind="cqpcfg://0.0.0.0:5555",
        limit=1,
        expected_workers=1,
    )
    def tracker_config():
        return None

    assert tracker_config.bind.address == "tcp://0.0.0.0:5555"
    assert tracker_config.bind.bind


def test_worker_annotation_exposes_connect_address() -> None:
    @cqpcfg_worker(connect="cqpcfg://127.0.0.1:5555", worker_id="worker-0")
    @cqpcfg_generator
    def worker_source():
        return SequenceRecordSource(())

    assert worker_source.connect.address == "tcp://127.0.0.1:5555"
    assert not worker_source.connect.bind


def test_worker_annotation_exposes_worker_selected_resources() -> None:
    @cqpcfg_worker(
        connect="cqpcfg://127.0.0.1:5555",
        worker_id="worker-0",
        resource_cpus=2.5,
        resource_memory="4g",
        resource_gpus=1,
        resource_gpu_memory="8g",
        model_json_page_cache=64,
        resource_labels={"zone": "local"},
    )
    @cqpcfg_generator
    def worker_source():
        return SequenceRecordSource(())

    assert worker_source.resources.cpu_cores == 2.5
    assert worker_source.resources.memory_bytes == 4 * 1024**3
    assert worker_source.resources.gpu_count == 1
    assert worker_source.resources.gpu_memory_bytes == 8 * 1024**3
    assert worker_source.resources.model_json_page_cache == 64
    assert worker_source.resources.labels == {"zone": "local"}


def test_worker_annotation_accepts_resource_spec_object() -> None:
    resources = WorkerResourceSpec(cpu_cores=1.0, memory_bytes=1024)

    @cqpcfg_worker(
        connect="cqpcfg://127.0.0.1:5555",
        worker_id="worker-0",
        resources=resources,
    )
    @cqpcfg_generator
    def worker_source():
        return SequenceRecordSource(())

    assert worker_source.resources == resources


def test_worker_annotation_rejects_mixed_resource_styles() -> None:
    with pytest.raises(ValueError, match="either resources"):
        cqpcfg_worker(
            connect="cqpcfg://127.0.0.1:5555",
            worker_id="worker-0",
            resources=WorkerResourceSpec(cpu_cores=1.0),
            resource_cpus=2.0,
        )


def test_node_agent_class_annotation_groups_generator_and_consumer() -> None:
    consumed: list[tuple[str, ...]] = []

    @cqpcfg_node_agent(
        connect="cqpcfg://127.0.0.1:5555",
        node_id="node-0",
        resource_cpus=2.0,
        resource_memory="2g",
    )
    class WorkerNode:
        def __init__(self) -> None:
            self.records = (
                GuessRecord(
                    prob=1.0,
                    guess="g0",
                    structure_index=0,
                    structure_name="A",
                    ranks=(0,),
                ),
            )

        @cqpcfg_generator
        def source(self, worker_id):
            assert worker_id == "node-0"
            return SequenceRecordSource(self.records)

        @cqpcfg_consumer
        def consume(self, batch: CandidateBatch) -> None:
            consumed.append(batch.guesses)

    source = WorkerNode.generator.source_for("node-0")
    batch = CandidateBatch.from_records(batch_id=1, start_rank=0, records=source.read_range("root", 0, 1))
    WorkerNode.consumer.publish(batch)

    assert WorkerNode.control_connect.address == "tcp://127.0.0.1:5555"
    assert WorkerNode.batch_connect.address == "tcp://127.0.0.1:5556"
    assert WorkerNode.role_connect.address == "tcp://127.0.0.1:5557"
    assert WorkerNode.resources.cpu_cores == 2.0
    assert WorkerNode.resources.memory_bytes == 2 * 1024**3
    assert consumed == [("g0",)]


def test_cqdagpcfg_node_agent_annotation_is_user_facing() -> None:
    config = CqdagNodeAgentServiceConfig(
        connect="cqpcfg://127.0.0.1:5555",
        node_id="node-0",
        metrics_dir=Path("/tmp/metrics"),
        outputs_dir=Path("/tmp/outputs"),
    )

    @cqdagpcfg_node_agent(config)
    class ExperimentNode:
        @cqdagpcfg_generator
        def source(self, worker_id):
            return SequenceRecordSource(())

        @cqdagpcfg_consumer
        def consume(self, batch: CandidateBatch) -> None:
            assert batch.batch_id

    assert isinstance(ExperimentNode, AnnotatedCQDAGPCFGNode)
    assert ExperimentNode.config == config
    assert ExperimentNode.node_class.__name__ == "ExperimentNode"


def test_cqdagpcfg_node_agent_annotation_accepts_overrides() -> None:
    base = CqdagNodeAgentServiceConfig(
        connect="cqpcfg://127.0.0.1:5555",
        node_id="node-0",
        metrics_dir=Path("/tmp/metrics"),
        outputs_dir=Path("/tmp/outputs"),
    )

    @cqdagpcfg_node_agent(base, node_id="node-1", resource_cpus=2.0)
    class ExperimentNode:
        @cqdagpcfg_generator
        def source(self, worker_id):
            return SequenceRecordSource(())

        @cqdagpcfg_consumer
        def consume(self, batch: CandidateBatch) -> None:
            assert batch.batch_id

    assert ExperimentNode.config.node_id == "node-1"
    assert ExperimentNode.config.connect == base.connect
    assert ExperimentNode.config.resource_cpus == 2.0


def test_cqdagpcfg_tracker_can_read_advanced_options_from_env(monkeypatch) -> None:
    monkeypatch.setenv("CQPCFG_ACK_TIMEOUT_SECONDS", "12.5")
    monkeypatch.setenv("CQPCFG_BATCH_SIZE", "64")
    monkeypatch.setenv("CQPCFG_CONSUMER_MIN_GPUS", "1")
    monkeypatch.setenv("CQPCFG_DISABLE_RECLAIM", "true")

    @cqdagpcfg_tracker_adapter(
        env_prefix="CQPCFG",
        model_path=Path("/tmp/model.json"),
        job_spec_path=Path("/tmp/job-spec.json"),
        total_nodes=3,
    )
    class ExperimentTracker:
        pass

    assert isinstance(ExperimentTracker, AnnotatedCQDAGPCFGTracker)
    assert ExperimentTracker.config.model_path == Path("/tmp/model.json")
    assert ExperimentTracker.config.job_spec_path == Path("/tmp/job-spec.json")
    assert ExperimentTracker.config.total_nodes == 3
    assert ExperimentTracker.config.ack_timeout_seconds == 12.5
    assert ExperimentTracker.config.batch_size == 64
    assert ExperimentTracker.config.consumer_min_gpus == 1
    assert ExperimentTracker.config.disable_reclaim is True


def test_cqdagpcfg_tracker_rejects_config_with_env_prefix() -> None:
    config = CqdagTrackerServiceConfig(
        model_path=Path("/tmp/model.json"),
        job_spec_path=Path("/tmp/job-spec.json"),
    )

    with pytest.raises(ValueError, match="config or env_prefix"):
        cqdagpcfg_tracker_adapter(config, env_prefix="CQPCFG")


def test_cqdagpcfg_consumer_can_handle_candidates_without_batch_metadata() -> None:
    records = (
        GuessRecord(
            prob=1.0,
            guess="g0",
            structure_index=0,
            structure_name="A",
            ranks=(0,),
        ),
        GuessRecord(
            prob=0.5,
            guess="g1",
            structure_index=0,
            structure_name="A",
            ranks=(1,),
        ),
    )
    batch = CandidateBatch.from_records(batch_id=7, start_rank=10, records=records)

    @cqdagpcfg_consumer
    def consume(candidate: CQDAGCandidate):
        if candidate.guess == "g0":
            return {"hash": "digest-0"}
        return None

    assert consume.handler(batch) == [
        {
            "batch_id": 7,
            "offset": 0,
            "rank": 10,
            "guess": "g0",
            "prob": 1.0,
            "structure_index": 0,
            "structure_name": "A",
            "ranks": (0,),
            "hash": "digest-0",
        }
    ]


def test_cqdagpcfg_consumer_treats_string_result_as_generic_value() -> None:
    records = (
        GuessRecord(
            prob=1.0,
            guess="g0",
            structure_index=0,
            structure_name="A",
            ranks=(0,),
        ),
    )
    batch = CandidateBatch.from_records(batch_id=7, start_rank=10, records=records)

    @cqdagpcfg_consumer
    def consume(guess: str):
        return "digest-0" if guess == "g0" else None

    assert consume.handler(batch) == [
        {
            "batch_id": 7,
            "offset": 0,
            "rank": 10,
            "guess": "g0",
            "prob": 1.0,
            "structure_index": 0,
            "structure_name": "A",
            "ranks": (0,),
            "value": "digest-0",
        }
    ]


def test_cqdagpcfg_remote_infers_ray_style_node_methods() -> None:
    base = CqdagNodeAgentServiceConfig(
        connect="cqpcfg://127.0.0.1:5555",
        node_id="node-0",
        metrics_dir=Path("/tmp/metrics"),
        outputs_dir=Path("/tmp/outputs"),
    )

    @cqdagpcfg_remote(base, num_cpus=2.0, memory="4g", num_gpus=1)
    class RayStyleNode:
        def generate(self):
            return SequenceRecordSource(())

        def consume(self, guess: str):
            if guess == "g0":
                return {"hash": "digest-0"}
            return None

    assert isinstance(RayStyleNode, AnnotatedCQDAGPCFGNode)
    assert isinstance(RayStyleNode.node_class.__dict__["generate"], AnnotatedGenerator)
    assert isinstance(RayStyleNode.node_class.__dict__["consume"], AnnotatedConsumer)
    assert RayStyleNode.config.resource_cpus == 2.0
    assert RayStyleNode.config.resource_memory == "4g"
    assert RayStyleNode.config.resource_gpus == 1

    updated = RayStyleNode.options(num_cpus=3.0)

    assert updated.config.resource_cpus == 3.0
    assert updated.config.resource_memory == "4g"


def test_cqdagpcfg_remote_can_read_node_options_from_env(monkeypatch) -> None:
    monkeypatch.setenv("CQPCFG_NODE_ID", "node-env")
    monkeypatch.setenv("CQPCFG_CONNECT", "cqpcfg://tracker:5555")
    monkeypatch.setenv("CQPCFG_MODEL_CACHE_DIR", "/tmp/cqpcfg-model-cache")
    monkeypatch.setenv("CQPCFG_MODEL_JSON_PAGE_CACHE", "64")
    monkeypatch.setenv("CQPCFG_RESOURCE_CPUS", "2.5")
    monkeypatch.setenv("CQPCFG_RESOURCE_MEMORY", "4g")

    @cqdagpcfg_remote(env_prefix="CQPCFG", num_gpus=1)
    class EnvNode:
        def consume(self, guess: str):
            return None

    assert EnvNode.config.node_id == "node-env"
    assert EnvNode.config.connect == "cqpcfg://tracker:5555"
    assert EnvNode.config.model_cache_dir == Path("/tmp/cqpcfg-model-cache")
    assert EnvNode.config.model_json_page_cache == 64
    assert EnvNode.config.resource_cpus == 2.5
    assert EnvNode.config.resource_memory == "4g"
    assert EnvNode.config.resource_gpus == 1


def test_cqdagpcfg_remote_explicit_options_override_env(monkeypatch) -> None:
    monkeypatch.setenv("CQPCFG_CONNECT", "cqpcfg://env-tracker:5555")
    monkeypatch.setenv("CQPCFG_RESOURCE_CPUS", "1.0")
    monkeypatch.setenv("CQPCFG_RESOURCE_MEMORY", "1g")

    @cqdagpcfg_remote(
        env_prefix="CQPCFG",
        connect="cqpcfg://decorator-tracker:5555",
        num_cpus=4.0,
        memory="8g",
        model_json_page_cache=256,
    )
    class ExplicitNode:
        def consume(self, guess: str):
            return None

    assert ExplicitNode.config.connect == "cqpcfg://decorator-tracker:5555"
    assert ExplicitNode.config.resource_cpus == 4.0
    assert ExplicitNode.config.resource_memory == "8g"
    assert ExplicitNode.config.model_json_page_cache == 256


def test_cqdagpcfg_remote_rejects_config_with_env_prefix() -> None:
    config = CqdagNodeAgentServiceConfig(
        connect="cqpcfg://127.0.0.1:5555",
        node_id="node-0",
        metrics_dir=Path("/tmp/metrics"),
        outputs_dir=Path("/tmp/outputs"),
    )

    with pytest.raises(ValueError, match="config or env_prefix"):
        cqdagpcfg_remote(config, env_prefix="CQPCFG")


def test_cqdagpcfg_remote_uses_default_source_when_generate_is_omitted() -> None:
    @cqdagpcfg_remote
    class DefaultSourceNode:
        def consume(self, guess: str):
            return None

    source = SequenceRecordSource(())

    class Context:
        def build_source(self):
            return source

    instance = DefaultSourceNode.node_class()
    instance.context = Context()
    component = DefaultSourceNode.node_class.__dict__["__cqdagpcfg_default_generator__"]
    generator = AnnotatedGenerator(
        factory=component.factory.__get__(instance, type(instance)),
    )

    assert generator.source_for("node-0") is source


def test_cqdagpcfg_generate_can_wrap_default_source() -> None:
    wrapped_source = SequenceRecordSource(())

    @cqdagpcfg_remote
    class WrappedSourceNode:
        def generate(self, source):
            assert isinstance(source, SequenceRecordSource)
            return wrapped_source

        def consume(self, guess: str):
            return None

    class Context:
        def build_source(self):
            return SequenceRecordSource(())

    instance = WrappedSourceNode.node_class()
    instance.context = Context()
    component = WrappedSourceNode.node_class.__dict__["generate"]
    generator = AnnotatedGenerator(
        factory=component.factory.__get__(instance, type(instance)),
    )

    assert generator.source_for("node-0") is wrapped_source


def test_cqdagpcfg_generate_filter_preserves_output_ranges() -> None:
    records = (
        GuessRecord(1.0, "a", 0, "A", (0,)),
        GuessRecord(0.9, "abcd", 0, "A", (1,)),
        GuessRecord(0.8, "abcdef", 0, "A", (2,)),
        GuessRecord(0.7, "xy", 0, "A", (3,)),
    )

    @cqdagpcfg_remote
    class LengthFilterNode:
        def generate(self, guess: str) -> str | None:
            if 2 <= len(guess) <= 4:
                return guess
            return None

        def consume(self, guess: str):
            return None

    class Context:
        def build_source(self):
            return SequenceRecordSource(records)

    instance = LengthFilterNode.node_class()
    instance.context = Context()
    component = LengthFilterNode.node_class.__dict__["generate"]
    generator = AnnotatedGenerator(
        factory=component.factory.__get__(instance, type(instance)),
    )
    source = generator.source_for("node-0")

    assert [record.guess for record in source.read_range("root", 0, 1)] == ["abcd"]
    assert [record.guess for record in source.read_range("root", 1, 2)] == ["xy"]
    assert [record.guess for record in source.read_range("root", 0, 2)] == ["abcd", "xy"]


def test_cqdagpcfg_generate_rejects_bool_results() -> None:
    records = (GuessRecord(1.0, "abcd", 0, "A", (0,)),)

    @cqdagpcfg_remote
    class BoolNode:
        def generate(self, guess: str):
            return len(guess) > 0

        def consume(self, guess: str):
            return None

    class Context:
        def build_source(self):
            return SequenceRecordSource(records)

    instance = BoolNode.node_class()
    instance.context = Context()
    component = BoolNode.node_class.__dict__["generate"]
    generator = AnnotatedGenerator(
        factory=component.factory.__get__(instance, type(instance)),
    )
    source = generator.source_for("node-0")

    with pytest.raises(TypeError, match="not bool"):
        source.read_range("root", 0, 1)


def test_cqdagpcfg_generate_can_transform_guesses_without_breaking_ranges() -> None:
    records = (
        GuessRecord(1.0, "pass", 0, "A", (0,)),
        GuessRecord(0.9, "word", 0, "A", (1,)),
    )

    @cqdagpcfg_remote
    class TransformNode:
        def generate(self, guess: str):
            return [guess, f"{guess}2026"]

        def consume(self, guess: str):
            return None

    class Context:
        def build_source(self):
            return SequenceRecordSource(records)

    instance = TransformNode.node_class()
    instance.context = Context()
    component = TransformNode.node_class.__dict__["generate"]
    generator = AnnotatedGenerator(
        factory=component.factory.__get__(instance, type(instance)),
    )
    source = generator.source_for("node-0")

    assert [record.guess for record in source.read_range("root", 0, 4)] == [
        "pass",
        "pass2026",
        "word",
        "word2026",
    ]


def test_cqdagpcfg_remote_accepts_bare_ray_style_decoration() -> None:
    @cqdagpcfg_remote
    class BareNode:
        def consume(self, guess: str):
            return None

    assert isinstance(BareNode, AnnotatedCQDAGPCFGNode)
    assert isinstance(
        BareNode.node_class.__dict__["__cqdagpcfg_default_generator__"],
        AnnotatedGenerator,
    )
    assert isinstance(BareNode.node_class.__dict__["consume"], AnnotatedConsumer)


def test_cqdagpcfg_node_agent_requires_explicit_generator_and_consumer() -> None:
    config = CqdagNodeAgentServiceConfig(
        connect="cqpcfg://127.0.0.1:5555",
        node_id="node-0",
        metrics_dir=Path("/tmp/metrics"),
        outputs_dir=Path("/tmp/outputs"),
    )

    with pytest.raises(ValueError, match="@cqdagpcfg_generator"):

        @cqdagpcfg_node_agent(config)
        class MissingGenerator:
            @cqdagpcfg_consumer
            def consume(self, batch: CandidateBatch) -> None:
                assert batch.batch_id

    with pytest.raises(ValueError, match="@cqdagpcfg_consumer"):

        @cqdagpcfg_node_agent(config)
        class MissingConsumer:
            @cqdagpcfg_generator
            def source(self, worker_id):
                return SequenceRecordSource(())


def test_cqdagpcfg_tracker_annotation_is_user_facing() -> None:
    config = CqdagTrackerServiceConfig(
        model_path=Path("/tmp/model.json"),
        job_spec_path=Path("/tmp/job-spec.json"),
        bind="cqpcfg://0.0.0.0:5555",
    )

    @cqdagpcfg_tracker_adapter(config)
    class ExperimentTracker:
        pass

    assert isinstance(ExperimentTracker, AnnotatedCQDAGPCFGTracker)
    assert ExperimentTracker.config == config
    assert ExperimentTracker.tracker_class.__name__ == "ExperimentTracker"
    assert ExperimentTracker.build() == CQDAGPCFGTracker(
        config=config,
        tracker_class=ExperimentTracker.tracker_class,
    )


def test_cqdagpcfg_tracker_annotation_accepts_overrides() -> None:
    base = CqdagTrackerServiceConfig(
        model_path=Path("/tmp/model.json"),
        job_spec_path=Path("/tmp/job-spec.json"),
        batch_size=16,
    )

    @cqdagpcfg_tracker_adapter(base, batch_size=32, timeout_seconds=10.0)
    class ExperimentTracker:
        pass

    assert ExperimentTracker.config.model_path == base.model_path
    assert ExperimentTracker.config.batch_size == 32
    assert ExperimentTracker.config.timeout_seconds == 10.0


def test_tracker_bind_does_not_overwrite_explicit_public_endpoints() -> None:
    config = CqdagTrackerServiceConfig(
        model_path=Path("/tmp/model.json"),
        job_spec_path=Path("/tmp/job-spec.json"),
        bind="cqpcfg://127.0.0.1:5555",
        public_control_connect="cqpcfg://tracker.example.com:15556",
        public_batch_connect="cqpcfg://tracker.example.com:15557",
        public_ack_connect="cqpcfg://tracker.example.com:15559",
        public_model_connect="cqpcfg://tracker.example.com:15560",
    )

    apply_endpoint_bundle(config)

    assert config.control_bind == "cqpcfg://127.0.0.1:5555"
    assert config.batch_bind == "cqpcfg://127.0.0.1:5556"
    assert config.role_bind == "cqpcfg://127.0.0.1:5557"
    assert config.ack_bind == "cqpcfg://127.0.0.1:5558"
    assert config.model_serve_bind == "cqpcfg://127.0.0.1:5559"
    assert config.public_control_connect == "cqpcfg://tracker.example.com:15556"
    assert config.public_batch_connect == "cqpcfg://tracker.example.com:15557"
    assert config.public_ack_connect == "cqpcfg://tracker.example.com:15559"
    assert config.public_model_connect == "cqpcfg://tracker.example.com:15560"


def test_root_mode_allows_rank_space_parallel_leases() -> None:
    root = CqdagTrackerServiceConfig(
        model_path=Path("/tmp/model.json"),
        job_spec_path=Path("/tmp/job-spec.json"),
        source_mode="root",
        max_parallel_leases_per_node=8,
    )
    structure = CqdagTrackerServiceConfig(
        model_path=Path("/tmp/model.json"),
        job_spec_path=Path("/tmp/job-spec.json"),
        source_mode="structure",
        max_parallel_leases_per_node=8,
    )

    assert effective_max_parallel_leases_per_node(root) == 8
    assert effective_max_parallel_leases_per_node(structure) == 8


def test_cracking_node_profile_disables_artifact_validation_overhead() -> None:
    config = CqdagNodeAgentServiceConfig(
        connect="cqpcfg://127.0.0.1:5555",
        node_id="node-0",
        metrics_dir=Path("/tmp/metrics"),
        outputs_dir=Path("/tmp/outputs"),
        optimization_profile="cracking",
        write_stable_artifacts=True,
        verify_candidate_artifacts=True,
    )

    resolved = _apply_node_optimization_profile(config)

    assert not resolved.write_stable_artifacts
    assert not resolved.verify_candidate_artifacts


def test_cracking_tracker_profile_uses_throughput_defaults() -> None:
    config = CqdagTrackerServiceConfig(
        model_path=Path("/tmp/model.json"),
        job_spec_path=Path("/tmp/job-spec.json"),
        optimization_profile="cracking",
        validate_serial_digest=True,
        disable_reclaim=True,
        delete_candidate_blocks_on_ack=False,
    )

    _apply_tracker_optimization_profile(config)

    assert not config.validate_serial_digest
    assert not config.disable_reclaim
    assert not config.delete_candidate_blocks_on_ack
    assert config.root_artifact_target_bytes == DEFAULT_CRACKING_ROOT_ARTIFACT_TARGET_BYTES


def test_node_agent_annotation_accepts_explicit_subchannel_connects() -> None:
    @cqpcfg_node_agent(
        control_connect="cqpcfg://127.0.0.1:6000",
        batch_connect="cqpcfg://127.0.0.1:6001",
        role_connect="cqpcfg://127.0.0.1:6002",
        node_id="node-0",
    )
    class WorkerNode:
        @cqpcfg_generator
        def source(self, worker_id):
            return SequenceRecordSource(())

        @cqpcfg_consumer
        def consume(self, batch: CandidateBatch) -> None:
            assert batch.batch_id

    assert WorkerNode.control_connect.address == "tcp://127.0.0.1:6000"
    assert WorkerNode.batch_connect.address == "tcp://127.0.0.1:6001"
    assert WorkerNode.role_connect.address == "tcp://127.0.0.1:6002"
    assert WorkerNode.ack_connect is None


def test_node_agent_annotation_rejects_function_decoration() -> None:
    with pytest.raises(TypeError, match="must decorate a class"):

        @cqpcfg_node_agent(connect="cqpcfg://127.0.0.1:5555", node_id="node-0")
        @cqpcfg_generator
        def worker_source():
            return SequenceRecordSource(())


def test_node_agent_class_annotation_requires_consumer_method() -> None:
    with pytest.raises(ValueError, match="@cqpcfg_consumer"):

        @cqpcfg_node_agent(connect="cqpcfg://127.0.0.1:5555", node_id="node-0")
        class WorkerNode:
            @cqpcfg_generator
            def source(self, worker_id):
                return SequenceRecordSource(())


def test_annotations_keep_endpoint_as_legacy_alias() -> None:
    @cqpcfg_worker(endpoint="cqpcfg://127.0.0.1:5556", worker_id="worker-0")
    @cqpcfg_generator
    def worker_source():
        return SequenceRecordSource(())

    assert worker_source.connect.address == "tcp://127.0.0.1:5556"


def test_annotations_reject_ambiguous_bind_and_endpoint() -> None:
    with pytest.raises(ValueError, match="either bind"):
        cqpcfg_tracker(
            bind="cqpcfg://0.0.0.0:5555",
            endpoint="cqpcfg://0.0.0.0:5556",
            limit=1,
            expected_workers=1,
        )


def test_worker_annotation_requires_explicit_generator_role() -> None:
    with pytest.raises(TypeError, match="cqpcfg_generator"):

        @cqpcfg_worker(connect="cqpcfg://127.0.0.1:5555", worker_id="worker-0")
        def worker_source():
            return SequenceRecordSource(())


def test_generator_annotation_returns_local_result_source() -> None:
    @cqpcfg_generator
    def generator(worker_id):
        return SequenceRecordSource(())

    assert generator.role == "generator"
    assert isinstance(generator.source_for("worker-0"), SequenceRecordSource)


def test_consumer_annotation_implements_candidate_batch_sink() -> None:
    received: list[tuple[str, ...]] = []
    closed: list[bool] = []

    @cqpcfg_consumer(close=lambda: closed.append(True))
    def consumer(batch: CandidateBatch) -> None:
        received.append(batch.guesses)

    records = [
        GuessRecord(
            prob=1.0 / (index + 1),
            guess=f"g{index}",
            structure_index=0,
            structure_name="A",
            ranks=(index,),
        )
        for index in range(3)
    ]

    publish_record_batches(
        records,
        consumer,
        batch_size=2,
        max_batch_payload_bytes=32,
    )

    assert consumer.role == "consumer"
    assert received == [("g0", "g1"), ("g2",)]
    assert closed == [True]
