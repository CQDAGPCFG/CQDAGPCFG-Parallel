from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from inspect import signature
from pathlib import Path
from time import monotonic
from typing import Any, Callable

from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.distributed import (
    AnnotatedConsumer,
    AnnotatedGenerator,
    NodeAgent,
    NodeAgentStats,
    RoleClient,
    cqpcfg_consumer,
    cqpcfg_generator,
    expand_node_endpoints,
    fetch_job_context,
    job_payload_from_job_context,
    resolve_node_id,
    safe_node_filename,
    worker_resources,
)
from cqdagpcfg_parallel.framework_logging import (
    configure_framework_logging,
    log_event,
)
from cqdagpcfg_parallel.protocol import NodeId, WorkerId
from cqdagpcfg_parallel.runtime import (
    CandidateBatch,
    LazyLocalResultSource,
    LocalResultSource,
)
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint

from .node_reporting import NodeAgentJsonReporter
from .node_source import (
    CQDAGGenerationBackend,
    CQDAGNodeSourceConfig,
    build_cqdag_node_source,
    resolve_generation_backend,
)


LOGGER = logging.getLogger("cqdagpcfg.node")
_GENERATED_SOURCE_SCAN_BATCH_SIZE = 256


@dataclass(frozen=True, slots=True)
class CqdagNodeAgentServiceConfig:
    node_id: str | None = None
    connect: str | None = None
    role_connect: str | None = None
    model_path: Path | None = None
    model_connect: str | None = None
    model_id: str = "cqdagpcfg-e2e-model"
    model_cache_dir: Path | None = None
    model_json_page_cache: int = 128
    resource_cpus: float | None = None
    resource_memory: str | None = None
    resource_gpus: int | None = None
    resource_gpu_memory: str | None = None
    disable_paged_source: bool = False
    generation_backend: CQDAGGenerationBackend = "auto"
    job_spec_path: Path | None = None
    source_mode: str = "root"
    control_connect: str = "cqpcfg://127.0.0.1:5555"
    batch_connect: str = "cqpcfg://127.0.0.1:5556"
    ack_connect: str = "cqpcfg://127.0.0.1:5558"
    demand_window: int = 8
    work_delay_seconds: float = 0.0
    receive_timeout_ms: int = 100
    consumer_drain_quiet_ms: int = 200
    consumer_drain_timeout_ms: int = 2000
    idle_sleep_seconds: float = 0.01
    role_refresh_interval_seconds: float = 0.05
    role_refresh_max_interval_seconds: float = 1.0
    role_control_overhead_budget: float = 0.01
    role_reply_timeout_ms: int = 100
    job_bootstrap_timeout_seconds: float = 30.0
    metrics_flush_interval_seconds: float = 0.25
    experiment_start_monotonic: float | None = None
    metrics_path: Path | None = None
    metrics_dir: Path | None = None
    outputs_path: Path | None = None
    outputs_dir: Path | None = None

    def __post_init__(self) -> None:
        if self.model_json_page_cache <= 0:
            raise ValueError("model_json_page_cache must be positive")
        if self.work_delay_seconds < 0.0:
            raise ValueError("work_delay_seconds cannot be negative")
        if self.receive_timeout_ms < 0:
            raise ValueError("receive_timeout_ms cannot be negative")
        if self.consumer_drain_quiet_ms < 0:
            raise ValueError("consumer_drain_quiet_ms cannot be negative")
        if self.consumer_drain_timeout_ms < 0:
            raise ValueError("consumer_drain_timeout_ms cannot be negative")
        if self.idle_sleep_seconds < 0.0:
            raise ValueError("idle_sleep_seconds cannot be negative")
        if self.role_refresh_interval_seconds <= 0.0:
            raise ValueError("role_refresh_interval_seconds must be positive")
        if self.role_refresh_max_interval_seconds < self.role_refresh_interval_seconds:
            raise ValueError(
                "role_refresh_max_interval_seconds must be greater than or equal to "
                "role_refresh_interval_seconds",
            )
        if not 0.0 < self.role_control_overhead_budget <= 1.0:
            raise ValueError("role_control_overhead_budget must be in (0, 1]")
        if self.role_reply_timeout_ms < 0:
            raise ValueError("role_reply_timeout_ms cannot be negative")
        if self.job_bootstrap_timeout_seconds < 0.0:
            raise ValueError("job_bootstrap_timeout_seconds cannot be negative")
        if self.metrics_flush_interval_seconds < 0.0:
            raise ValueError("metrics_flush_interval_seconds cannot be negative")
        if self.demand_window < 0:
            raise ValueError("demand_window cannot be negative")
        if self.source_mode not in {"root", "structure"}:
            raise ValueError("source_mode must be root or structure")
        if self.generation_backend not in {"auto", "cpp", "paged", "python"}:
            raise ValueError("generation_backend must be auto, cpp, paged, or python")
        if self.metrics_path is not None and self.metrics_dir is not None:
            raise ValueError("metrics_path and metrics_dir cannot both be set")
        if self.outputs_path is not None and self.outputs_dir is not None:
            raise ValueError("outputs_path and outputs_dir cannot both be set")


@dataclass(frozen=True, slots=True)
class CQDAGCandidate:
    """Framework-level candidate view passed to CQDAGPCFG consumers."""

    batch_id: int
    offset: int
    rank: int
    guess: str
    prob: float
    structure_index: int
    structure_name: str
    ranks: tuple[int, ...]
    record: GuessRecord

    @classmethod
    def from_batch(cls, batch: CandidateBatch, offset: int) -> "CQDAGCandidate":
        record = batch.records[offset]
        return cls(
            batch_id=batch.batch_id,
            offset=offset,
            rank=batch.start_rank + offset,
            guess=record.guess,
            prob=record.prob,
            structure_index=record.structure_index,
            structure_name=record.structure_name,
            ranks=tuple(record.ranks),
            record=record,
        )

    def output_metadata(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "offset": self.offset,
            "rank": self.rank,
            "guess": self.guess,
            "prob": self.prob,
            "structure_index": self.structure_index,
            "structure_name": self.structure_name,
            "ranks": self.ranks,
        }


@dataclass(frozen=True, slots=True)
class CQDAGPCFGNodeContext:
    config: CqdagNodeAgentServiceConfig
    node_id: str
    job_payload: Mapping[str, Any]
    source_config: CQDAGNodeSourceConfig
    control_connect: str
    batch_connect: str
    role_connect: str
    ack_connect: str | None
    started_at: float
    _consumer_outputs: list[dict[str, Any]]
    reporter: NodeAgentJsonReporter

    @property
    def model_fingerprint(self) -> str | None:
        fingerprint = self.job_payload.get("model_fingerprint")
        return None if fingerprint is None else str(fingerprint)

    @property
    def output_count(self) -> int:
        return len(self._consumer_outputs)

    @property
    def consumer_outputs(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(dict(output) for output in self._consumer_outputs)

    def build_source(self) -> LocalResultSource:
        return build_cqdag_node_source(
            self.source_config,
            model_cache_dir=self.config.model_cache_dir,
            limit=int(self.job_payload["limit"]),
            expected_fingerprint=self.model_fingerprint,
        )


@dataclass(frozen=True, slots=True)
class CQDAGPCFGNode:
    """Default CQDAGPCFG node-agent runner.

    The public entrypoint should read like "start this node", not like "call a
    service helper".
    """

    config: CqdagNodeAgentServiceConfig

    def run(self) -> NodeAgentStats:
        return _run_cqdag_node_agent_service(
            self.config,
            node_class=_DefaultCQDAGPCFGNode,
        )

    def remote(self) -> NodeAgentStats:
        return self.run()

    def options(self, **overrides) -> "CQDAGPCFGNode":
        return CQDAGPCFGNode(_replace_config(self.config, overrides))


@dataclass(frozen=True, slots=True)
class AnnotatedCQDAGPCFGNode:
    """User-facing annotated CQDAGPCFG node definition."""

    node_class: type[Any]
    config: CqdagNodeAgentServiceConfig

    def run(self) -> NodeAgentStats:
        return _run_cqdag_node_agent_service(
            self.config,
            node_class=self.node_class,
        )

    def remote(self) -> NodeAgentStats:
        return self.run()

    def options(self, **overrides) -> "AnnotatedCQDAGPCFGNode":
        return AnnotatedCQDAGPCFGNode(
            node_class=self.node_class,
            config=_replace_config(self.config, overrides),
        )


def cqdagpcfg_node_agent(
    config: CqdagNodeAgentServiceConfig | None = None,
    **overrides,
) -> Callable[[type[Any]], AnnotatedCQDAGPCFGNode]:
    """Declare a CQDAGPCFG elastic node with a user-visible decorator.

    The decorated class is intentionally just the user's declaration site. The
    default CQDAGPCFG source, candidate consumer, role hot-swap, model paging,
    and metrics wiring are provided by this adapter.
    """

    if config is None:
        resolved_config = CqdagNodeAgentServiceConfig(**overrides)
    elif overrides:
        resolved_config = replace(config, **overrides)
    else:
        resolved_config = config

    def decorator(node_class: type[Any]) -> AnnotatedCQDAGPCFGNode:
        if not isinstance(node_class, type):
            raise TypeError("@cqdagpcfg_node_agent must decorate a class")
        _validate_component_declarations(node_class)
        return AnnotatedCQDAGPCFGNode(node_class=node_class, config=resolved_config)

    return decorator


def cqdagpcfg_remote(
    config: CqdagNodeAgentServiceConfig | type[Any] | None = None,
    **options,
) -> Callable[[type[Any]], AnnotatedCQDAGPCFGNode] | AnnotatedCQDAGPCFGNode:
    if isinstance(config, type):
        if options:
            raise TypeError("@cqdagpcfg.remote cannot mix bare class decoration and options")
        return _decorate_ray_style_node(config, CqdagNodeAgentServiceConfig())

    resolved_config = _resolve_config(config, options)

    def decorator(node_class: type[Any]) -> AnnotatedCQDAGPCFGNode:
        return _decorate_ray_style_node(node_class, resolved_config)

    return decorator


def _wrap_cqdagpcfg_generator(
    source_factory: Callable[..., LocalResultSource],
) -> Callable[..., LocalResultSource]:
    all_parameters = tuple(signature(source_factory).parameters.values())
    receives_instance = bool(
        all_parameters and all_parameters[0].name in {"self", "cls"}
    )
    business_parameters = [
        parameter
        for parameter in all_parameters
        if parameter.name not in {"self", "cls"}
    ]
    if len(business_parameters) > 2:
        raise TypeError(
            "@cqdagpcfg_generator accepts generate(), generate(source), "
            "generate(worker_id), or generate(source, worker_id)"
        )

    parameter_names = tuple(parameter.name for parameter in business_parameters)

    def generator_wrapper(*args, **kwargs) -> LocalResultSource:
        if kwargs:
            raise TypeError("@cqdagpcfg_generator handlers do not accept keywords")
        instance = args[0] if receives_instance and args else None
        worker_id = args[-1] if args else None
        prefix = args[:1] if receives_instance else ()

        if not parameter_names:
            return _coerce_generated_source(
                source_factory(*prefix),
                default_source=None,
            )

        if parameter_names == ("worker_id",):
            return _coerce_generated_source(
                source_factory(*prefix, worker_id),
                default_source=None,
            )

        if parameter_names == ("source",) or parameter_names == ("base_source",):
            default_source = _build_default_source(instance)
            return _coerce_generated_source(
                source_factory(*prefix, default_source),
                default_source=default_source,
            )

        if parameter_names in {("source", "worker_id"), ("base_source", "worker_id")}:
            default_source = _build_default_source(instance)
            return _coerce_generated_source(
                source_factory(*prefix, default_source, worker_id),
                default_source=default_source,
            )

        if parameter_names in {("worker_id", "source"), ("worker_id", "base_source")}:
            default_source = _build_default_source(instance)
            return _coerce_generated_source(
                source_factory(*prefix, worker_id, default_source),
                default_source=default_source,
            )

        if _is_record_generator_signature(parameter_names):
            default_source = _build_default_source(instance)
            return _RangePreservingGeneratedSource(
                default_source,
                lambda record: _call_record_generator(
                    source_factory,
                    prefix=prefix,
                    parameter_names=parameter_names,
                    record=record,
                    worker_id=worker_id,
                ),
            )

        raise TypeError(
            "@cqdagpcfg_generator parameter names must be source, base_source, "
            "worker_id, guess, record, or candidate"
        )

    return generator_wrapper


def _build_default_source(instance: object | None) -> LocalResultSource:
    context = getattr(instance, "context", None) if instance is not None else None
    if context is None or not hasattr(context, "build_source"):
        raise RuntimeError(
            "default CQDAGPCFG source is only available inside a cqdagpcfg node"
        )
    return context.build_source()


def _coerce_generated_source(
    result: object,
    *,
    default_source: LocalResultSource | None,
) -> LocalResultSource:
    if result is None:
        if default_source is None:
            raise TypeError("@cqdagpcfg_generator must return a source")
        return default_source
    if _is_local_result_source(result):
        return result
    if callable(result) and default_source is not None:
        return _RangePreservingGeneratedSource(default_source, result)
    raise TypeError(
        "@cqdagpcfg_generator must return a LocalResultSource, None, "
        "or a record transform callable"
    )


def _is_local_result_source(candidate: object) -> bool:
    return callable(getattr(candidate, "read_range", None))


def _is_record_generator_signature(parameter_names: tuple[str, ...]) -> bool:
    valid = {"guess", "record", "candidate", "worker_id"}
    if not parameter_names or any(name not in valid for name in parameter_names):
        return False
    return any(name in {"guess", "record", "candidate"} for name in parameter_names)


def _call_record_generator(
    source_factory: Callable[..., object],
    *,
    prefix: tuple[object, ...],
    parameter_names: tuple[str, ...],
    record: GuessRecord,
    worker_id: WorkerId | None,
) -> object:
    values = {
        "guess": record.guess,
        "record": record,
        "candidate": record,
        "worker_id": worker_id,
    }
    return source_factory(*prefix, *(values[name] for name in parameter_names))


class _RangePreservingGeneratedSource:
    def __init__(
        self,
        source: LocalResultSource,
        transform: Callable[[GuessRecord], object],
        *,
        scan_batch_size: int = _GENERATED_SOURCE_SCAN_BATCH_SIZE,
    ) -> None:
        if scan_batch_size <= 0:
            raise ValueError("scan_batch_size must be positive")
        self.source = source
        self.transform = transform
        self.scan_batch_size = scan_batch_size
        self._records_by_node: dict[NodeId, list[GuessRecord]] = defaultdict(list)
        self._start_by_node: dict[NodeId, int] = defaultdict(int)
        self._source_end_by_node: dict[NodeId, int] = defaultdict(int)
        self._exhausted_nodes: set[NodeId] = set()

    def read_range(
        self,
        node_id: NodeId,
        start: int,
        end: int,
    ) -> tuple[GuessRecord, ...]:
        if start < 0 or end < start:
            raise ValueError("invalid generated source range")
        if end == start:
            return ()
        self._fill_until(node_id, end)
        base = self._start_by_node[node_id]
        offset_start = max(0, start - base)
        offset_end = max(0, end - base)
        return tuple(self._records_by_node[node_id][offset_start:offset_end])

    def reclaim_before(self, node_id: NodeId, index: int) -> int:
        base = self._start_by_node[node_id]
        drop_count = min(
            len(self._records_by_node[node_id]),
            max(0, index - base),
        )
        if drop_count:
            del self._records_by_node[node_id][:drop_count]
            self._start_by_node[node_id] += drop_count

        reclaim_before = getattr(self.source, "reclaim_before", None)
        if callable(reclaim_before):
            reclaim_before(node_id, self._source_end_by_node[node_id])
        return drop_count

    def stats(self):
        stats = getattr(self.source, "stats", None)
        return stats() if callable(stats) else None

    def _fill_until(self, node_id: NodeId, target_end: int) -> None:
        while (
            self._available_end(node_id) < target_end
            and node_id not in self._exhausted_nodes
        ):
            source_start = self._source_end_by_node[node_id]
            needed = max(1, target_end - self._available_end(node_id))
            source_end = self._bounded_source_end(
                source_start,
                min(self.scan_batch_size, needed),
            )
            if source_end <= source_start:
                self._exhausted_nodes.add(node_id)
                return
            records = tuple(self.source.read_range(node_id, source_start, source_end))
            self._source_end_by_node[node_id] += len(records)
            for record in records:
                self._records_by_node[node_id].extend(
                    _normalize_generated_records(record, self.transform(record))
                )
            if len(records) < source_end - source_start:
                self._exhausted_nodes.add(node_id)

    def _available_end(self, node_id: NodeId) -> int:
        return self._start_by_node[node_id] + len(self._records_by_node[node_id])

    def _bounded_source_end(self, start: int, size: int) -> int:
        end = start + size
        source_limit = _source_record_limit(self.source)
        if source_limit is None:
            return end
        return min(end, source_limit)


def _source_record_limit(source: LocalResultSource) -> int | None:
    for attribute in ("max_records", "max_records_per_structure"):
        value = getattr(source, attribute, None)
        if value is not None:
            return int(value)
    records = getattr(source, "records", None)
    if records is not None:
        return len(records)
    return None


def _normalize_generated_records(
    base_record: GuessRecord,
    result: object,
) -> tuple[GuessRecord, ...]:
    if result is None:
        return ()
    if isinstance(result, bool):
        raise TypeError(
            "generate() must return a candidate value, not bool; "
            "return the guess to keep it or None to drop it"
        )
    if isinstance(result, GuessRecord):
        return (result,)
    if isinstance(result, str):
        return (_copy_guess_record(base_record, guess=result),)
    if isinstance(result, Mapping):
        return (_record_from_mapping(base_record, result),)
    if not isinstance(result, Iterable) or isinstance(result, (bytes, bytearray)):
        raise TypeError(
            "generate() must return None, str, GuessRecord, mapping, "
            "or an iterable of those values"
        )

    records: list[GuessRecord] = []
    for item in result:
        records.extend(_normalize_generated_records(base_record, item))
    return tuple(records)


def _record_from_mapping(
    base_record: GuessRecord,
    values: Mapping[str, Any],
) -> GuessRecord:
    return _copy_guess_record(
        base_record,
        prob=float(values.get("prob", base_record.prob)),
        guess=str(values.get("guess", base_record.guess)),
        structure_index=int(values.get("structure_index", base_record.structure_index)),
        structure_name=str(values.get("structure_name", base_record.structure_name)),
        ranks=tuple(values.get("ranks", base_record.ranks)),
    )


def _copy_guess_record(
    base_record: GuessRecord,
    *,
    prob: float | None = None,
    guess: str | None = None,
    structure_index: int | None = None,
    structure_name: str | None = None,
    ranks: tuple[int, ...] | None = None,
) -> GuessRecord:
    return GuessRecord(
        prob=base_record.prob if prob is None else prob,
        guess=base_record.guess if guess is None else guess,
        structure_index=(
            base_record.structure_index
            if structure_index is None
            else structure_index
        ),
        structure_name=(
            base_record.structure_name
            if structure_name is None
            else structure_name
        ),
        ranks=base_record.ranks if ranks is None else ranks,
    )


def cqdagpcfg_generator(source_factory):
    return cqpcfg_generator(_wrap_cqdagpcfg_generator(source_factory))


def cqdagpcfg_consumer(
    handler: Callable[..., object] | None = None,
    *,
    close: Callable[[], None] | None = None,
) -> Callable[[Callable[..., object]], AnnotatedConsumer] | AnnotatedConsumer:
    def decorator(consumer_handler: Callable[..., object]) -> AnnotatedConsumer:
        return cqpcfg_consumer(
            _wrap_cqdagpcfg_consumer(consumer_handler),
            close=close,
        )

    if handler is None:
        return decorator
    return decorator(handler)


@dataclass(frozen=True, slots=True)
class CQDAGPCFGFramework:
    """Ray-style public facade for the PCFG framework."""

    def remote(
        self,
        config: CqdagNodeAgentServiceConfig | type[Any] | None = None,
        **options,
    ) -> Callable[[type[Any]], AnnotatedCQDAGPCFGNode] | AnnotatedCQDAGPCFGNode:
        return cqdagpcfg_remote(config, **options)

    def generator(self, source_factory):
        return cqdagpcfg_generator(source_factory)

    def consumer(
        self,
        handler: Callable[..., object] | None = None,
        *,
        close: Callable[[], None] | None = None,
    ) -> Callable[[Callable[..., object]], AnnotatedConsumer] | AnnotatedConsumer:
        return cqdagpcfg_consumer(handler, close=close)


cqdagpcfg = CQDAGPCFGFramework()
pcfg = cqdagpcfg


class _DefaultCQDAGPCFGNode:
    @cqdagpcfg_generator
    def source(self, source):
        return source

    @cqpcfg_consumer
    def consume(self, batch: CandidateBatch) -> None:
        return None


def _run_cqdag_node_agent_service(
    config: CqdagNodeAgentServiceConfig,
    *,
    node_class: type[Any],
) -> NodeAgentStats:
    configure_framework_logging()
    context = _build_node_context(config)
    generator, consumer = _resolve_cqdag_node_components(node_class, context)
    consumer = _capture_consumer_results(consumer, context)
    resources = worker_resources(
        resource_cpus=config.resource_cpus,
        resource_memory=config.resource_memory,
        resource_gpus=config.resource_gpus,
        resource_gpu_memory=config.resource_gpu_memory,
        model_json_page_cache=config.model_json_page_cache,
    )
    source = LazyLocalResultSource(
        lambda: generator.source_for(WorkerId(context.node_id)),
    )
    log_event(
        LOGGER,
        logging.INFO,
        "node.start",
        node_id=context.node_id,
        control_connect=context.control_connect,
        batch_connect=context.batch_connect,
        role_connect=context.role_connect,
        ack_connect=context.ack_connect,
        model_fingerprint=context.model_fingerprint,
        generation_backend=resolve_generation_backend(context.source_config),
        cpu_cores=resources.cpu_cores,
        memory_bytes=resources.memory_bytes,
        gpu_count=resources.gpu_count,
        model_json_page_cache=resources.model_json_page_cache,
    )
    role_client = RoleClient(
        node_id=context.node_id,
        endpoint=ZmqEndpoint.from_uri(context.role_connect, bind=False),
        reply_timeout_ms=config.role_reply_timeout_ms,
    )
    agent = NodeAgent(
        node_id=context.node_id,
        role_client=role_client,
        control_endpoint=ZmqEndpoint.from_uri(context.control_connect, bind=False),
        batch_endpoint=ZmqEndpoint.from_uri(context.batch_connect, bind=False),
        ack_endpoint=(
            ZmqEndpoint.from_uri(context.ack_connect, bind=False)
            if context.ack_connect is not None
            else None
        ),
        source=source,
        consume_batch=consumer.publish,
        model_fingerprint=context.model_fingerprint,
        work_delay_seconds=config.work_delay_seconds,
        receive_timeout_ms=config.receive_timeout_ms,
        consumer_drain_quiet_ms=config.consumer_drain_quiet_ms,
        consumer_drain_timeout_ms=config.consumer_drain_timeout_ms,
        idle_sleep_seconds=config.idle_sleep_seconds,
        role_refresh_interval_seconds=config.role_refresh_interval_seconds,
        role_refresh_max_interval_seconds=config.role_refresh_max_interval_seconds,
        role_control_overhead_budget=config.role_control_overhead_budget,
        stats_flush_interval_seconds=config.metrics_flush_interval_seconds,
        stats_callback=context.reporter.write,
        resources=resources,
    )
    stats = agent.run()

    log_event(
        LOGGER,
        logging.INFO,
        "node.complete",
        node_id=stats.node_id,
        final_role=stats.current_role,
        desired_role=stats.desired_role,
        role_switches=stats.role_switches,
        generator_sessions=stats.generator_sessions,
        consumer_sessions=stats.consumer_sessions,
        completed_records=stats.completed_records,
        consumed_candidates=stats.consumed_candidates,
        consumer_outputs=context.output_count,
        source_peak_cached_records=stats.source_peak_cached_records,
        source_reclaimed_records=stats.source_reclaimed_records,
        network_bytes=stats.network_bytes,
        ack_network_bytes=stats.ack_network_bytes,
        elapsed_seconds=f"{stats.elapsed_seconds:.6f}",
    )
    return stats


def _build_node_context(config: CqdagNodeAgentServiceConfig) -> CQDAGPCFGNodeContext:
    endpoints = expand_node_endpoints(
        connect=config.connect,
        control_connect=config.control_connect,
        batch_connect=config.batch_connect,
        role_connect=config.role_connect,
        ack_connect=config.ack_connect,
        model_connect=config.model_connect,
    )
    if endpoints.role_connect is None:
        raise RuntimeError("node agent requires connect or role_connect")

    node_id = resolve_node_id(config.node_id)
    metrics_path = _resolve_output_path(
        explicit_path=config.metrics_path,
        directory=config.metrics_dir,
        node_id=node_id,
        suffix=".json",
        label="metrics",
    )
    outputs_path = _resolve_output_path(
        explicit_path=config.outputs_path,
        directory=config.outputs_dir,
        node_id=node_id,
        suffix=".json",
        label="outputs",
    )

    resources = worker_resources(
        resource_cpus=config.resource_cpus,
        resource_memory=config.resource_memory,
        resource_gpus=config.resource_gpus,
        resource_gpu_memory=config.resource_gpu_memory,
        model_json_page_cache=config.model_json_page_cache,
    )
    job_spec_path = config.job_spec_path
    if job_spec_path is None:
        bootstrap_reply = fetch_job_context(
            node_id=node_id,
            role_connect=endpoints.role_connect,
            resources=resources,
            reply_timeout_ms=config.role_reply_timeout_ms,
            timeout_seconds=config.job_bootstrap_timeout_seconds,
            refresh_interval_seconds=config.role_refresh_interval_seconds,
        )
        job_context = bootstrap_reply.job_context
        assert job_context is not None
        log_event(
            LOGGER,
            logging.INFO,
            "node.job_context_received",
            node_id=node_id,
            job_id=job_context.job_id,
            model_id=job_context.model_id,
            source_mode=job_context.source_mode,
            limit=job_context.limit,
            control_connect=job_context.control_connect,
            batch_connect=job_context.batch_connect,
            ack_connect=job_context.ack_connect,
            model_connect=job_context.model_connect,
        )
        job_payload = job_payload_from_job_context(job_context)
        source_config = CQDAGNodeSourceConfig.from_job_context(
            job_context,
            disable_paged_source=config.disable_paged_source,
            generation_backend=config.generation_backend,
            model_json_page_cache=config.model_json_page_cache,
            resources=resources,
        )
        control_connect = job_context.control_connect
        batch_connect = job_context.batch_connect
        ack_connect = job_context.ack_connect
    else:
        job_payload = _read_json(job_spec_path)
        log_event(
            LOGGER,
            logging.INFO,
            "node.explicit_job_loaded",
            node_id=node_id,
            job_spec_path=job_spec_path,
            model_path=config.model_path,
            model_connect=endpoints.model_connect,
            source_mode=config.source_mode,
        )
        source_config = CQDAGNodeSourceConfig.from_explicit_model(
            model_path=config.model_path,
            model_connect=endpoints.model_connect,
            model_id=config.model_id,
            source_mode=config.source_mode,
            demand_window=config.demand_window,
            disable_paged_source=config.disable_paged_source,
            generation_backend=config.generation_backend,
            model_json_page_cache=config.model_json_page_cache,
            resources=resources,
        )
        control_connect = endpoints.control_connect
        batch_connect = endpoints.batch_connect
        ack_connect = endpoints.ack_connect

    limit = int(job_payload["limit"])
    consumer_outputs: list[dict[str, Any]] = []
    started_at = (
        monotonic()
        if config.experiment_start_monotonic is None
        else config.experiment_start_monotonic
    )
    reporter = NodeAgentJsonReporter(
        metrics_path=metrics_path,
        outputs_path=outputs_path,
        limit=limit,
        outputs_provider=lambda: consumer_outputs,
    )
    return CQDAGPCFGNodeContext(
        config=config,
        node_id=node_id,
        job_payload=job_payload,
        source_config=source_config,
        control_connect=control_connect,
        batch_connect=batch_connect,
        role_connect=endpoints.role_connect,
        ack_connect=ack_connect,
        started_at=started_at,
        _consumer_outputs=consumer_outputs,
        reporter=reporter,
    )


def _resolve_config(
    config: CqdagNodeAgentServiceConfig | None,
    overrides: Mapping[str, Any],
) -> CqdagNodeAgentServiceConfig:
    normalized = _normalize_ray_options(overrides)
    env_prefix = normalized.pop("env_prefix", None)
    if env_prefix is not None and not isinstance(env_prefix, str):
        raise TypeError("env_prefix must be a string")

    env_config = (
        _node_agent_config_from_env(env_prefix)
        if env_prefix is not None
        else None
    )
    if config is None and env_config is None:
        return CqdagNodeAgentServiceConfig(**normalized)
    if config is None:
        assert env_config is not None
        return replace(env_config, **normalized) if normalized else env_config
    if env_config is not None:
        raise ValueError("use either config or env_prefix, not both")
    if normalized:
        return replace(config, **normalized)
    return config


def _replace_config(
    config: CqdagNodeAgentServiceConfig,
    overrides: Mapping[str, Any],
) -> CqdagNodeAgentServiceConfig:
    return replace(config, **_normalize_ray_options(overrides))


def _normalize_ray_options(options: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(options)
    aliases = {
        "num_cpus": "resource_cpus",
        "num_gpus": "resource_gpus",
        "memory": "resource_memory",
        "gpu_memory": "resource_gpu_memory",
    }
    for source, target in aliases.items():
        if source not in normalized:
            continue
        if target in normalized:
            raise ValueError(f"use either {source}=... or {target}=..., not both")
        normalized[target] = normalized.pop(source)
    return normalized


def _node_agent_config_from_env(prefix: str) -> CqdagNodeAgentServiceConfig:
    normalized_prefix = prefix.rstrip("_")
    values: dict[str, Any] = {}
    readers: dict[str, Callable[[str], Any]] = {
        "node_id": _env_str,
        "connect": _env_str,
        "role_connect": _env_str,
        "model_path": _env_path,
        "model_connect": _env_str,
        "model_id": _env_str,
        "model_cache_dir": _env_path,
        "model_json_page_cache": _env_int,
        "resource_cpus": _env_float,
        "resource_memory": _env_str,
        "resource_gpus": _env_int,
        "resource_gpu_memory": _env_str,
        "disable_paged_source": _env_bool,
        "generation_backend": _env_str,
        "job_spec_path": _env_path,
        "source_mode": _env_str,
        "control_connect": _env_str,
        "batch_connect": _env_str,
        "ack_connect": _env_str,
        "demand_window": _env_int,
        "work_delay_seconds": _env_float,
        "receive_timeout_ms": _env_int,
        "consumer_drain_quiet_ms": _env_int,
        "consumer_drain_timeout_ms": _env_int,
        "idle_sleep_seconds": _env_float,
        "role_refresh_interval_seconds": _env_float,
        "role_refresh_max_interval_seconds": _env_float,
        "role_control_overhead_budget": _env_float,
        "role_reply_timeout_ms": _env_int,
        "job_bootstrap_timeout_seconds": _env_float,
        "metrics_flush_interval_seconds": _env_float,
        "experiment_start_monotonic": _env_float,
        "metrics_path": _env_path,
        "metrics_dir": _env_path,
        "outputs_path": _env_path,
        "outputs_dir": _env_path,
    }
    for field_name, reader in readers.items():
        env_name = f"{normalized_prefix}_{field_name.upper()}"
        value = os.environ.get(env_name)
        if value is None or value == "":
            continue
        values[field_name] = reader(value)
    return CqdagNodeAgentServiceConfig(**values)


def _env_str(value: str) -> str:
    return value


def _env_path(value: str) -> Path:
    return Path(value)


def _env_int(value: str) -> int:
    return int(value)


def _env_float(value: str) -> float:
    return float(value)


def _env_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _infer_ray_style_components(node_class: type[Any]) -> None:
    values = vars(node_class)
    if not any(isinstance(value, AnnotatedGenerator) for value in values.values()):
        decorated = _decorate_first_matching_method(
            node_class,
            ("generate", "source"),
            cqdagpcfg_generator,
            role_name="generator",
        )
        if not decorated:
            setattr(
                node_class,
                "__cqdagpcfg_default_generator__",
                cqdagpcfg_generator(_default_cqdagpcfg_source),
            )
    if not any(isinstance(value, AnnotatedConsumer) for value in values.values()):
        _decorate_first_matching_method(
            node_class,
            ("consume", "verify", "check"),
            cqdagpcfg_consumer,
            role_name="consumer",
        )


def _decorate_first_matching_method(
    node_class: type[Any],
    names: tuple[str, ...],
    decorator: Callable[[Callable[..., object]], object],
    *,
    role_name: str,
) -> bool:
    for name in names:
        method = vars(node_class).get(name)
        if method is None:
            continue
        if isinstance(method, (AnnotatedGenerator, AnnotatedConsumer)):
            return True
        if not callable(method):
            raise TypeError(f"cqdagpcfg {role_name} method must be callable: {name}")
        setattr(node_class, name, decorator(method))
        return True
    return False


def _default_cqdagpcfg_source(self, source: LocalResultSource) -> LocalResultSource:
    return source


def _decorate_ray_style_node(
    node_class: type[Any],
    config: CqdagNodeAgentServiceConfig,
) -> AnnotatedCQDAGPCFGNode:
    if not isinstance(node_class, type):
        raise TypeError("@cqdagpcfg.remote must decorate a class")
    _infer_ray_style_components(node_class)
    _validate_component_declarations(node_class)
    return AnnotatedCQDAGPCFGNode(node_class=node_class, config=config)


def _resolve_cqdag_node_components(
    node_class: type[Any],
    context: CQDAGPCFGNodeContext,
) -> tuple[AnnotatedGenerator, AnnotatedConsumer]:
    if not isinstance(node_class, type):
        raise TypeError("@cqdagpcfg_node_agent must decorate a class")
    instance = _instantiate_node_class(node_class, context)
    generator = _single_component(
        _bind_class_components(instance, AnnotatedGenerator),
        component_name="generator",
        decorator_name="@cqdagpcfg_generator",
    )
    consumer = _single_component(
        _bind_class_components(instance, AnnotatedConsumer),
        component_name="consumer",
        decorator_name="@cqdagpcfg_consumer",
    )
    assert isinstance(generator, AnnotatedGenerator)
    assert isinstance(consumer, AnnotatedConsumer)
    return generator, consumer


def _capture_consumer_results(
    consumer: AnnotatedConsumer,
    context: CQDAGPCFGNodeContext,
) -> AnnotatedConsumer:
    def handler(batch: CandidateBatch):
        result = consumer.handler(batch)
        return _record_consumer_result(context, batch, result)

    return AnnotatedConsumer(
        handler=handler,
        close_handler=consumer.close_handler,
    )


def _record_consumer_result(
    context: CQDAGPCFGNodeContext,
    batch: CandidateBatch,
    result: object,
) -> tuple[dict[str, Any], ...]:
    if result is None:
        return ()
    if isinstance(result, str):
        output = _normalize_consumer_output(context, batch, {"value": result})
        context._consumer_outputs.append(output)
        return (output,)
    if isinstance(result, Mapping):
        output = _normalize_consumer_output(context, batch, result)
        context._consumer_outputs.append(output)
        return (output,)
    if not isinstance(result, Iterable) or isinstance(result, (str, bytes)):
        raise TypeError(
            "@cqdagpcfg_consumer must return None, a mapping, or an iterable of mappings"
        )
    outputs: list[dict[str, Any]] = []
    for item in result:
        if not isinstance(item, Mapping):
            raise TypeError(
                "@cqdagpcfg_consumer returned an iterable containing a non-mapping item"
            )
        output = _normalize_consumer_output(context, batch, item)
        context._consumer_outputs.append(output)
        outputs.append(output)
    return tuple(outputs)


def _normalize_consumer_output(
    context: CQDAGPCFGNodeContext,
    batch: CandidateBatch,
    output: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = dict(output)
    offset = normalized.get("offset")
    if offset is not None:
        candidate = CQDAGCandidate.from_batch(batch, int(offset))
        metadata = candidate.output_metadata()
        metadata.update(normalized)
        normalized = metadata
    else:
        normalized.setdefault("batch_id", batch.batch_id)
    normalized.setdefault("node_id", context.node_id)
    normalized.setdefault("elapsed_seconds", monotonic() - context.started_at)
    return normalized


def _wrap_cqdagpcfg_consumer(
    consumer_handler: Callable[..., object],
) -> Callable[..., object]:
    if _consumer_accepts_batch(consumer_handler):
        return consumer_handler

    first_param_name = _first_business_parameter_name(consumer_handler)

    def candidate_consumer(*args, **kwargs) -> list[dict[str, Any]]:
        if kwargs:
            raise TypeError("@cqdagpcfg_consumer candidate handlers do not accept keywords")
        if not args or not isinstance(args[-1], CandidateBatch):
            raise TypeError("@cqdagpcfg_consumer expected a CandidateBatch from the runtime")
        batch = args[-1]
        prefix = args[:-1]
        outputs: list[dict[str, Any]] = []
        for offset in range(len(batch.records)):
            candidate = CQDAGCandidate.from_batch(batch, offset)
            if first_param_name == "guess":
                result = consumer_handler(*prefix, candidate.guess)
            elif first_param_name == "record":
                result = consumer_handler(*prefix, candidate.record)
            else:
                result = consumer_handler(*prefix, candidate)
            outputs.extend(_candidate_result_to_outputs(candidate, result))
        return outputs

    return candidate_consumer


def _consumer_accepts_batch(handler: Callable[..., object]) -> bool:
    parameters = _business_parameters(handler)
    if not parameters:
        raise TypeError("@cqdagpcfg_consumer handler must accept a candidate or batch")
    first = parameters[0]
    return first.name == "batch" or first.annotation is CandidateBatch


def _first_business_parameter_name(handler: Callable[..., object]) -> str:
    parameters = _business_parameters(handler)
    if not parameters:
        raise TypeError("@cqdagpcfg_consumer handler must accept a candidate or batch")
    return parameters[0].name


def _business_parameters(handler: Callable[..., object]):
    return [
        parameter
        for parameter in signature(handler).parameters.values()
        if parameter.name not in {"self", "cls"}
    ]


def _candidate_result_to_outputs(
    candidate: CQDAGCandidate,
    result: object,
) -> list[dict[str, Any]]:
    if result is None or result is False:
        return []
    if result is True:
        return [candidate.output_metadata()]
    if isinstance(result, str):
        output = candidate.output_metadata()
        output["value"] = result
        return [output]
    if isinstance(result, Mapping):
        output = candidate.output_metadata()
        output.update(dict(result))
        return [output]
    if not isinstance(result, Iterable) or isinstance(result, (str, bytes)):
        raise TypeError(
            "@cqdagpcfg_consumer must return None, bool, str, a mapping, "
            "or an iterable of output values"
        )
    outputs = []
    for item in result:
        if item is None or item is False:
            continue
        if item is True:
            outputs.append(candidate.output_metadata())
            continue
        if isinstance(item, str):
            output = candidate.output_metadata()
            output["value"] = item
            outputs.append(output)
            continue
        if not isinstance(item, Mapping):
            raise TypeError(
                "@cqdagpcfg_consumer returned an iterable containing an unsupported item"
            )
        output = candidate.output_metadata()
        output.update(dict(item))
        outputs.append(output)
    return outputs


def _validate_component_declarations(node_class: type[Any]) -> None:
    generator_count = sum(
        isinstance(value, AnnotatedGenerator) for value in vars(node_class).values()
    )
    consumer_count = sum(
        isinstance(value, AnnotatedConsumer) for value in vars(node_class).values()
    )
    _validate_component_count(
        generator_count,
        component_name="generator",
        decorator_name="@cqdagpcfg_generator",
    )
    _validate_component_count(
        consumer_count,
        component_name="consumer",
        decorator_name="@cqdagpcfg_consumer",
    )


def _validate_component_count(
    count: int,
    *,
    component_name: str,
    decorator_name: str,
) -> None:
    if count == 1:
        return
    if count > 1:
        raise ValueError(
            f"cqdagpcfg node class must define exactly one {decorator_name} method"
        )
    raise ValueError(
        f"cqdagpcfg node class must define a {decorator_name} {component_name}"
    )


def _instantiate_node_class(
    node_class: type[Any],
    context: CQDAGPCFGNodeContext,
) -> object:
    parameters = signature(node_class).parameters
    if len(parameters) == 1:
        parameter_name = next(iter(parameters))
        if parameter_name in {"job_payload", "payload"}:
            instance = node_class(context.job_payload)
        else:
            instance = node_class(context)
        setattr(instance, "context", context)
        return instance
    if len(parameters) == 0:
        instance = node_class()
        setattr(instance, "context", context)
        return instance
    raise TypeError(
        "cqdagpcfg node class must accept zero arguments, context, or job_payload"
    )


def _bind_class_components(
    instance: object,
    component_type: type[AnnotatedGenerator] | type[AnnotatedConsumer],
) -> list[AnnotatedGenerator] | list[AnnotatedConsumer]:
    components = []
    for value in vars(type(instance)).values():
        if not isinstance(value, component_type):
            continue
        if isinstance(value, AnnotatedGenerator):
            components.append(
                AnnotatedGenerator(
                    factory=_bind_callable(value.factory, instance),
                )
            )
        else:
            components.append(
                AnnotatedConsumer(
                    handler=_bind_callable(value.handler, instance),
                    close_handler=(
                        _bind_callable(value.close_handler, instance)
                        if value.close_handler is not None
                        else None
                    ),
                )
            )
    return components


def _bind_callable(func: Callable[..., Any], instance: object) -> Callable[..., Any]:
    bind = getattr(func, "__get__", None)
    if not callable(bind):
        return func
    return bind(instance, type(instance))


def _single_component(
    components: list[AnnotatedGenerator] | list[AnnotatedConsumer],
    *,
    component_name: str,
    decorator_name: str,
) -> AnnotatedGenerator | AnnotatedConsumer:
    if len(components) == 1:
        return components[0]
    if len(components) > 1:
        raise ValueError(f"cqdagpcfg node class must define exactly one {decorator_name} method")
    raise ValueError(f"cqdagpcfg node class must define a {decorator_name} {component_name}")


def _resolve_output_path(
    *,
    explicit_path: Path | None,
    directory: Path | None,
    node_id: str,
    suffix: str,
    label: str,
) -> Path:
    if explicit_path is not None:
        return explicit_path
    if directory is None:
        raise RuntimeError(f"node agent requires {label}_path or {label}_dir")
    return directory / f"{safe_node_filename(node_id)}{suffix}"


def _read_json(path: Path) -> Mapping[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


__all__ = [
    "AnnotatedCQDAGPCFGNode",
    "CQDAGCandidate",
    "CQDAGPCFGFramework",
    "CQDAGPCFGNodeContext",
    "CQDAGPCFGNode",
    "CqdagNodeAgentServiceConfig",
    "cqdagpcfg",
    "cqdagpcfg_consumer",
    "cqdagpcfg_generator",
    "cqdagpcfg_node_agent",
    "cqdagpcfg_remote",
    "pcfg",
]
