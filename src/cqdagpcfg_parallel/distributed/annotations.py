from __future__ import annotations

from dataclasses import dataclass
from inspect import signature
from threading import Event
from typing import Any, Callable, Mapping

from cqdagpcfg_parallel.protocol import NodeId, SchedulerConfig, WorkerId
from cqdagpcfg_parallel.runtime import CandidateBatch
from cqdagpcfg_parallel.runtime.worker import LazyLocalResultSource, LocalResultSource
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint, ZmqEndpointBundle

from .node_agent import NodeAgent, NodeAgentStats, NodeAgentStatsCallback
from .resources import WorkerResourceSpec, parse_byte_size
from .role_control import RoleClient
from .runner import run_distributed_protocol
from .tracker import DistributedProtocolConfig, DistributedProtocolTracker, DistributedRunResult
from .worker import DistributedProtocolWorker, DistributedWorkerStats


ConfigFactory = Callable[[], DistributedProtocolConfig | None]
WorkerSourceFactory = Callable[..., LocalResultSource]
ConsumerHandler = Callable[[CandidateBatch], None]


@dataclass(frozen=True, slots=True)
class AnnotatedGenerator:
    factory: WorkerSourceFactory
    role: str = "generator"

    def source_for(self, worker_id: WorkerId) -> LocalResultSource:
        return _call_source_factory(self.factory, worker_id)

    def __call__(self, worker_id: WorkerId) -> LocalResultSource:
        return self.source_for(worker_id)


@dataclass(slots=True)
class AnnotatedConsumer:
    handler: ConsumerHandler
    close_handler: Callable[[], None] | None = None
    role: str = "consumer"
    closed: bool = False

    def publish(self, batch: CandidateBatch) -> None:
        if self.closed:
            raise RuntimeError("cannot publish to a closed annotated consumer")
        self.handler(batch)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self.close_handler is not None:
            self.close_handler()

    def __call__(self, batch: CandidateBatch) -> None:
        self.publish(batch)


@dataclass(frozen=True, slots=True)
class AnnotatedDistributedProtocol:
    generator: AnnotatedGenerator
    limit: int
    worker_count: int
    endpoint: ZmqEndpoint | None
    config: DistributedProtocolConfig
    timeout_seconds: float
    worker_delay_seconds: float

    def run(self) -> tuple[DistributedRunResult, tuple[DistributedWorkerStats, ...]]:
        return run_distributed_protocol(
            source_factory=lambda worker_id: self.generator.source_for(worker_id),
            limit=self.limit,
            worker_count=self.worker_count,
            endpoint=self.endpoint,
            config=self.config,
            timeout_seconds=self.timeout_seconds,
            worker_delay_seconds=self.worker_delay_seconds,
        )

    def __call__(self, worker_id: WorkerId) -> LocalResultSource:
        return self.generator.source_for(worker_id)


@dataclass(frozen=True, slots=True)
class AnnotatedTracker:
    config_factory: ConfigFactory
    bind: ZmqEndpoint
    limit: int
    expected_workers: int
    default_config: DistributedProtocolConfig
    timeout_seconds: float

    def build(self, *, context: Any | None = None) -> DistributedProtocolTracker:
        return DistributedProtocolTracker(
            endpoint=self.bind,
            config=self._config(),
            context=context,
        )

    def run(
        self,
        *,
        context: Any | None = None,
        started_event: Event | None = None,
    ) -> DistributedRunResult:
        return self.build(context=context).run(
            limit=self.limit,
            expected_workers=self.expected_workers,
            timeout_seconds=self.timeout_seconds,
            started_event=started_event,
        )

    def _config(self) -> DistributedProtocolConfig:
        configured = self.config_factory()
        return self.default_config if configured is None else configured


@dataclass(frozen=True, slots=True)
class AnnotatedWorker:
    generator: AnnotatedGenerator
    connect: ZmqEndpoint
    worker_id: WorkerId
    wait_sleep_seconds: float
    work_delay_seconds: float
    model_fingerprint: str | None = None
    resources: WorkerResourceSpec = WorkerResourceSpec()

    def build(self, *, context: Any | None = None) -> DistributedProtocolWorker:
        return DistributedProtocolWorker(
            worker_id=self.worker_id,
            endpoint=self.connect,
            source=self.generator.source_for(self.worker_id),
            context=context,
            wait_sleep_seconds=self.wait_sleep_seconds,
            work_delay_seconds=self.work_delay_seconds,
            model_fingerprint=self.model_fingerprint,
        )

    def run(self, *, context: Any | None = None) -> DistributedWorkerStats:
        return self.build(context=context).run()

    def __call__(self) -> LocalResultSource:
        return self.generator.source_for(self.worker_id)


@dataclass(frozen=True, slots=True)
class AnnotatedNodeAgent:
    generator: AnnotatedGenerator
    consumer: AnnotatedConsumer
    node_id: str
    control_connect: ZmqEndpoint
    batch_connect: ZmqEndpoint
    role_connect: ZmqEndpoint
    ack_connect: ZmqEndpoint | None
    resources: WorkerResourceSpec
    role_reply_timeout_ms: int
    model_fingerprint: str | None = None
    work_delay_seconds: float = 0.0
    receive_timeout_ms: int = 100
    consumer_drain_quiet_ms: int = 200
    consumer_drain_timeout_ms: int = 2000
    idle_sleep_seconds: float = 0.01
    role_refresh_interval_seconds: float = 0.05
    stats_flush_interval_seconds: float = 0.25
    stats_callback: NodeAgentStatsCallback | None = None

    def build(self) -> NodeAgent:
        role_client = RoleClient(
            node_id=self.node_id,
            endpoint=self.role_connect,
            reply_timeout_ms=self.role_reply_timeout_ms,
        )
        source = LazyLocalResultSource(
            lambda: self.generator.source_for(WorkerId(self.node_id))
        )
        return NodeAgent(
            node_id=self.node_id,
            role_client=role_client,
            control_endpoint=self.control_connect,
            batch_endpoint=self.batch_connect,
            ack_endpoint=self.ack_connect,
            source=source,
            consume_batch=self.consumer.publish,
            model_fingerprint=self.model_fingerprint,
            work_delay_seconds=self.work_delay_seconds,
            receive_timeout_ms=self.receive_timeout_ms,
            consumer_drain_quiet_ms=self.consumer_drain_quiet_ms,
            consumer_drain_timeout_ms=self.consumer_drain_timeout_ms,
            idle_sleep_seconds=self.idle_sleep_seconds,
            role_refresh_interval_seconds=self.role_refresh_interval_seconds,
            stats_flush_interval_seconds=self.stats_flush_interval_seconds,
            stats_callback=self.stats_callback,
            resources=self.resources,
        )

    def run(self) -> NodeAgentStats:
        return self.build().run()

    def __call__(self) -> LocalResultSource:
        return self.generator.source_for(WorkerId(self.node_id))


def cqpcfg_generator(source_factory: WorkerSourceFactory) -> AnnotatedGenerator:
    return AnnotatedGenerator(factory=source_factory)


def cqpcfg_consumer(
    handler: ConsumerHandler | None = None,
    *,
    close: Callable[[], None] | None = None,
) -> Callable[[ConsumerHandler], AnnotatedConsumer] | AnnotatedConsumer:
    def decorator(consumer_handler: ConsumerHandler) -> AnnotatedConsumer:
        return AnnotatedConsumer(handler=consumer_handler, close_handler=close)

    if handler is None:
        return decorator
    return decorator(handler)


def cqpcfg_distributed(
    *,
    limit: int,
    worker_count: int,
    endpoint: str | ZmqEndpoint | None = None,
    demand_window: int = 16,
    node_affinity_enabled: bool = True,
    node_affinity_bonus: float = 0.5,
    entropy: float = 0.0,
    node_id: NodeId = NodeId("root"),
    timeout_seconds: float = 10.0,
    worker_delay_seconds: float = 0.0,
    model_fingerprint: str | None = None,
) -> Callable[[AnnotatedGenerator], AnnotatedDistributedProtocol]:
    config = DistributedProtocolConfig(
        scheduler=SchedulerConfig(
            node_affinity_enabled=node_affinity_enabled,
            node_affinity_bonus=node_affinity_bonus,
        ),
        node_id=node_id,
        demand_window=demand_window,
        entropy=entropy,
        model_fingerprint=model_fingerprint,
    )
    parsed_endpoint = _coerce_endpoint(endpoint, bind=True) if endpoint is not None else None

    def decorator(generator: AnnotatedGenerator) -> AnnotatedDistributedProtocol:
        _require_generator(generator)
        return AnnotatedDistributedProtocol(
            generator=generator,
            limit=limit,
            worker_count=worker_count,
            endpoint=parsed_endpoint,
            config=config,
            timeout_seconds=timeout_seconds,
            worker_delay_seconds=worker_delay_seconds,
        )

    return decorator


def cqpcfg_tracker(
    *,
    bind: str | ZmqEndpoint | None = None,
    endpoint: str | ZmqEndpoint | None = None,
    limit: int,
    expected_workers: int,
    demand_window: int = 16,
    node_affinity_enabled: bool = True,
    node_affinity_bonus: float = 0.5,
    entropy: float = 0.0,
    node_id: NodeId = NodeId("root"),
    timeout_seconds: float = 10.0,
    model_fingerprint: str | None = None,
) -> Callable[[ConfigFactory], AnnotatedTracker]:
    parsed_bind = _coerce_bind_or_connect(
        primary=bind,
        legacy_endpoint=endpoint,
        bind=True,
        label="bind",
    )
    default_config = DistributedProtocolConfig(
        scheduler=SchedulerConfig(
            node_affinity_enabled=node_affinity_enabled,
            node_affinity_bonus=node_affinity_bonus,
        ),
        node_id=node_id,
        demand_window=demand_window,
        entropy=entropy,
        model_fingerprint=model_fingerprint,
    )

    def decorator(config_factory: ConfigFactory) -> AnnotatedTracker:
        return AnnotatedTracker(
            config_factory=config_factory,
            bind=parsed_bind,
            limit=limit,
            expected_workers=expected_workers,
            default_config=default_config,
            timeout_seconds=timeout_seconds,
        )

    return decorator


def cqpcfg_worker(
    *,
    connect: str | ZmqEndpoint | None = None,
    endpoint: str | ZmqEndpoint | None = None,
    worker_id: str | WorkerId,
    wait_sleep_seconds: float = 0.001,
    work_delay_seconds: float = 0.0,
    model_fingerprint: str | None = None,
    resources: WorkerResourceSpec | None = None,
    resource_cpus: float | None = None,
    resource_memory: str | int | None = None,
    resource_gpus: int | None = None,
    resource_gpu_memory: str | int | None = None,
    model_json_page_cache: int | None = None,
    resource_labels: Mapping[str, str] | None = None,
) -> Callable[[AnnotatedGenerator], AnnotatedWorker]:
    parsed_connect = _coerce_bind_or_connect(
        primary=connect,
        legacy_endpoint=endpoint,
        bind=False,
        label="connect",
    )
    parsed_worker_id = WorkerId(str(worker_id))
    parsed_resources = _coerce_worker_resources(
        resources=resources,
        resource_cpus=resource_cpus,
        resource_memory=resource_memory,
        resource_gpus=resource_gpus,
        resource_gpu_memory=resource_gpu_memory,
        model_json_page_cache=model_json_page_cache,
        resource_labels=resource_labels,
    )

    def decorator(generator: AnnotatedGenerator) -> AnnotatedWorker:
        _require_generator(generator)
        return AnnotatedWorker(
            generator=generator,
            connect=parsed_connect,
            worker_id=parsed_worker_id,
            wait_sleep_seconds=wait_sleep_seconds,
            work_delay_seconds=work_delay_seconds,
            model_fingerprint=model_fingerprint,
            resources=parsed_resources,
        )

    return decorator


def cqpcfg_node_agent(
    *,
    connect: str | None = None,
    control_connect: str | ZmqEndpoint | None = None,
    batch_connect: str | ZmqEndpoint | None = None,
    role_connect: str | ZmqEndpoint | None = None,
    ack_connect: str | ZmqEndpoint | None = None,
    node_id: str | WorkerId,
    role_reply_timeout_ms: int = 100,
    model_fingerprint: str | None = None,
    work_delay_seconds: float = 0.0,
    receive_timeout_ms: int = 100,
    consumer_drain_quiet_ms: int = 200,
    consumer_drain_timeout_ms: int = 2000,
    idle_sleep_seconds: float = 0.01,
    role_refresh_interval_seconds: float = 0.05,
    stats_flush_interval_seconds: float = 0.25,
    stats_callback: NodeAgentStatsCallback | None = None,
    resources: WorkerResourceSpec | None = None,
    resource_cpus: float | None = None,
    resource_memory: str | int | None = None,
    resource_gpus: int | None = None,
    resource_gpu_memory: str | int | None = None,
    model_json_page_cache: int | None = None,
    resource_labels: Mapping[str, str] | None = None,
) -> Callable[[type[Any]], AnnotatedNodeAgent]:
    endpoints = _coerce_node_agent_endpoints(
        connect=connect,
        control_connect=control_connect,
        batch_connect=batch_connect,
        role_connect=role_connect,
        ack_connect=ack_connect,
    )
    parsed_resources = _coerce_worker_resources(
        resources=resources,
        resource_cpus=resource_cpus,
        resource_memory=resource_memory,
        resource_gpus=resource_gpus,
        resource_gpu_memory=resource_gpu_memory,
        model_json_page_cache=model_json_page_cache,
        resource_labels=resource_labels,
    )

    def decorator(candidate: type[Any]) -> AnnotatedNodeAgent:
        generator, resolved_consumer = _resolve_node_agent_components(candidate)
        return AnnotatedNodeAgent(
            generator=generator,
            consumer=resolved_consumer,
            node_id=str(node_id),
            control_connect=endpoints.control,
            batch_connect=endpoints.batch,
            role_connect=endpoints.role,
            ack_connect=endpoints.ack,
            resources=parsed_resources,
            role_reply_timeout_ms=role_reply_timeout_ms,
            model_fingerprint=model_fingerprint,
            work_delay_seconds=work_delay_seconds,
            receive_timeout_ms=receive_timeout_ms,
            consumer_drain_quiet_ms=consumer_drain_quiet_ms,
            consumer_drain_timeout_ms=consumer_drain_timeout_ms,
            idle_sleep_seconds=idle_sleep_seconds,
            role_refresh_interval_seconds=role_refresh_interval_seconds,
            stats_flush_interval_seconds=stats_flush_interval_seconds,
            stats_callback=stats_callback,
        )

    return decorator


def _coerce_endpoint(endpoint: str | ZmqEndpoint, *, bind: bool) -> ZmqEndpoint:
    if isinstance(endpoint, ZmqEndpoint):
        return endpoint
    return ZmqEndpoint.from_uri(endpoint, bind=bind)


def _coerce_bind_or_connect(
    *,
    primary: str | ZmqEndpoint | None,
    legacy_endpoint: str | ZmqEndpoint | None,
    bind: bool,
    label: str,
) -> ZmqEndpoint:
    if primary is not None and legacy_endpoint is not None:
        raise ValueError(f"use either {label}=... or endpoint=..., not both")
    selected = primary if primary is not None else legacy_endpoint
    if selected is None:
        raise ValueError(f"{label} address is required")
    return _coerce_endpoint(selected, bind=bind)


def _coerce_worker_resources(
    *,
    resources: WorkerResourceSpec | None,
    resource_cpus: float | None,
    resource_memory: str | int | None,
    resource_gpus: int | None,
    resource_gpu_memory: str | int | None,
    model_json_page_cache: int | None,
    resource_labels: Mapping[str, str] | None,
) -> WorkerResourceSpec:
    granular_resource_args = (
        resource_cpus,
        resource_memory,
        resource_gpus,
        resource_gpu_memory,
        model_json_page_cache,
    )
    has_granular_resource_args = any(value is not None for value in granular_resource_args)
    if resources is not None and (has_granular_resource_args or resource_labels):
        raise ValueError("use either resources=... or resource_* arguments, not both")
    if resources is not None:
        return resources
    return WorkerResourceSpec(
        cpu_cores=resource_cpus,
        memory_bytes=parse_byte_size(resource_memory),
        gpu_count=resource_gpus,
        gpu_memory_bytes=parse_byte_size(resource_gpu_memory),
        model_json_page_cache=model_json_page_cache,
        labels=dict(resource_labels or {}),
    )


@dataclass(frozen=True, slots=True)
class _NodeAgentEndpoints:
    control: ZmqEndpoint
    batch: ZmqEndpoint
    role: ZmqEndpoint
    ack: ZmqEndpoint | None


def _coerce_node_agent_endpoints(
    *,
    connect: str | None,
    control_connect: str | ZmqEndpoint | None,
    batch_connect: str | ZmqEndpoint | None,
    role_connect: str | ZmqEndpoint | None,
    ack_connect: str | ZmqEndpoint | None,
) -> _NodeAgentEndpoints:
    if connect is not None:
        bundle = ZmqEndpointBundle.from_base_uri(connect)
        control_connect = control_connect or bundle.control
        batch_connect = batch_connect or bundle.batch
        role_connect = role_connect or bundle.role
        ack_connect = ack_connect or bundle.ack
    if control_connect is None:
        raise ValueError("control_connect is required when connect is not provided")
    if batch_connect is None:
        raise ValueError("batch_connect is required when connect is not provided")
    if role_connect is None:
        raise ValueError("role_connect is required when connect is not provided")
    return _NodeAgentEndpoints(
        control=_coerce_endpoint(control_connect, bind=False),
        batch=_coerce_endpoint(batch_connect, bind=False),
        role=_coerce_endpoint(role_connect, bind=False),
        ack=_coerce_endpoint(ack_connect, bind=False) if ack_connect is not None else None,
    )


def _resolve_node_agent_components(
    candidate: type[Any],
) -> tuple[AnnotatedGenerator, AnnotatedConsumer]:
    if not isinstance(candidate, type):
        raise TypeError("@cqpcfg_node_agent must decorate a class")

    instance = candidate()
    generator = _single_component(
        _bind_class_components(instance, AnnotatedGenerator),
        component_name="generator",
        decorator_name="@cqpcfg_generator",
    )
    consumer = _single_component(
        _bind_class_components(instance, AnnotatedConsumer),
        component_name="consumer",
        decorator_name="@cqpcfg_consumer",
    )
    assert isinstance(generator, AnnotatedGenerator)
    assert isinstance(consumer, AnnotatedConsumer)
    return generator, consumer


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
    required: bool = True,
) -> AnnotatedGenerator | AnnotatedConsumer | None:
    if len(components) == 1:
        return components[0]
    if len(components) > 1:
        raise ValueError(f"node agent class must define exactly one {decorator_name} method")
    if required:
        raise ValueError(f"node agent class must define a {decorator_name} {component_name}")
    return None


def _call_source_factory(
    source_factory: WorkerSourceFactory,
    worker_id: WorkerId,
) -> LocalResultSource:
    if not signature(source_factory).parameters:
        return source_factory()
    return source_factory(worker_id)


def _require_generator(candidate: object) -> None:
    if not isinstance(candidate, AnnotatedGenerator):
        raise TypeError("worker source must be decorated with @cqpcfg_generator")


__all__ = [
    "AnnotatedDistributedProtocol",
    "AnnotatedConsumer",
    "AnnotatedGenerator",
    "AnnotatedNodeAgent",
    "AnnotatedTracker",
    "AnnotatedWorker",
    "cqpcfg_consumer",
    "cqpcfg_distributed",
    "cqpcfg_generator",
    "cqpcfg_node_agent",
    "cqpcfg_tracker",
    "cqpcfg_worker",
]
