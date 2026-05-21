from __future__ import annotations

from dataclasses import dataclass
from inspect import signature
from threading import Event
from typing import Any, Callable

from cqdagpcfg_parallel.protocol import NodeId, SchedulerConfig, WorkerId
from cqdagpcfg_parallel.runtime import CandidateBatch
from cqdagpcfg_parallel.runtime.worker import LocalResultSource
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint

from .runner import SourceFactory, run_distributed_protocol
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
) -> Callable[[AnnotatedGenerator], AnnotatedWorker]:
    parsed_connect = _coerce_bind_or_connect(
        primary=connect,
        legacy_endpoint=endpoint,
        bind=False,
        label="connect",
    )
    parsed_worker_id = WorkerId(str(worker_id))

    def decorator(generator: AnnotatedGenerator) -> AnnotatedWorker:
        _require_generator(generator)
        return AnnotatedWorker(
            generator=generator,
            connect=parsed_connect,
            worker_id=parsed_worker_id,
            wait_sleep_seconds=wait_sleep_seconds,
            work_delay_seconds=work_delay_seconds,
            model_fingerprint=model_fingerprint,
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
    "AnnotatedTracker",
    "AnnotatedWorker",
    "cqpcfg_consumer",
    "cqpcfg_distributed",
    "cqpcfg_generator",
    "cqpcfg_tracker",
    "cqpcfg_worker",
]
