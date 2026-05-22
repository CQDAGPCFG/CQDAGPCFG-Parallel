from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from time import monotonic, sleep
from typing import Any, Callable

from cqdagpcfg_parallel.framework_logging import log_event
from cqdagpcfg_parallel.protocol import WorkerId
from cqdagpcfg_parallel.runtime import (
    BatchAck,
    BatchAckStatus,
    BatchEndOfStream,
    CandidateBatch,
    ZmqPushBatchAckSink,
)
from cqdagpcfg_parallel.runtime.worker import LocalResultSource, source_reclaim_counters
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint, ZmqPullBatchSource

from .role_control import RoleClient, RoleControlReply
from .resources import WorkerResourceSpec
from .worker import DistributedProtocolWorker


CandidateBatchHandler = Callable[[CandidateBatch], object]
NodeAgentStatsCallback = Callable[["NodeAgentStats"], None]
LOGGER = logging.getLogger("cqdagpcfg.node_agent")


@dataclass(frozen=True, slots=True)
class NodeAgentStats:
    node_id: str
    current_role: str
    desired_role: str
    final: bool
    model_loaded_once: bool
    resource_cpu_cores: float | None = None
    resource_memory_bytes: int | None = None
    resource_gpu_count: int | None = None
    resource_model_json_page_cache: int | None = None
    role_switches: int = 0
    generator_sessions: int = 0
    consumer_sessions: int = 0
    completed_items: int = 0
    completed_records: int = 0
    waits: int = 0
    consumed_batches: int = 0
    consumed_candidates: int = 0
    generation_rate: float = 0.0
    consumer_rate: float = 0.0
    network_messages: int = 0
    network_batch_messages: int = 0
    network_end_messages: int = 0
    network_bytes: int = 0
    network_poll_seconds: float = 0.0
    network_poll_timeouts: int = 0
    network_recv_seconds: float = 0.0
    network_deserialize_seconds: float = 0.0
    ack_network_messages: int = 0
    ack_network_bytes: int = 0
    ack_network_send_seconds: float = 0.0
    ack_network_serialize_seconds: float = 0.0
    role_control_messages: int = 0
    role_control_bytes: int = 0
    role_control_seconds: float = 0.0
    role_control_request_seconds: float = 0.0
    role_control_roundtrip_ewma_seconds: float = 0.0
    role_refresh_interval_seconds: float = 0.0
    source_cached_records: int = 0
    source_peak_cached_records: int = 0
    source_reclaimed_records: int = 0
    source_dag_repository_active_units: int = 0
    source_dag_stream_active_units: int = 0
    drained_batches: int = 0
    drained_candidates: int = 0
    drain_timeouts: int = 0
    elapsed_seconds: float = 0.0


class NodeAgent:
    """Persistent worker node that can hot-swap generator and consumer roles."""

    def __init__(
        self,
        *,
        node_id: str,
        role_client: RoleClient,
        control_endpoint: ZmqEndpoint,
        batch_endpoint: ZmqEndpoint,
        source: LocalResultSource,
        consume_batch: CandidateBatchHandler,
        ack_endpoint: ZmqEndpoint | None = None,
        model_fingerprint: str | None = None,
        work_delay_seconds: float = 0.0,
        receive_timeout_ms: int = 100,
        consumer_drain_quiet_ms: int = 200,
        consumer_drain_timeout_ms: int = 2000,
        idle_sleep_seconds: float = 0.01,
        role_refresh_interval_seconds: float = 0.05,
        role_refresh_max_interval_seconds: float = 1.0,
        role_control_overhead_budget: float = 0.01,
        stats_flush_interval_seconds: float = 0.25,
        stats_callback: NodeAgentStatsCallback | None = None,
        resources: WorkerResourceSpec = WorkerResourceSpec(),
    ) -> None:
        if control_endpoint.bind:
            raise ValueError("control_endpoint must connect")
        if batch_endpoint.bind:
            raise ValueError("batch_endpoint must connect")
        if ack_endpoint is not None and ack_endpoint.bind:
            raise ValueError("ack_endpoint must connect")
        if work_delay_seconds < 0.0:
            raise ValueError("work_delay_seconds cannot be negative")
        if receive_timeout_ms < 0:
            raise ValueError("receive_timeout_ms cannot be negative")
        if consumer_drain_quiet_ms < 0:
            raise ValueError("consumer_drain_quiet_ms cannot be negative")
        if consumer_drain_timeout_ms < 0:
            raise ValueError("consumer_drain_timeout_ms cannot be negative")
        if idle_sleep_seconds < 0.0:
            raise ValueError("idle_sleep_seconds cannot be negative")
        if role_refresh_interval_seconds <= 0.0:
            raise ValueError("role_refresh_interval_seconds must be positive")
        if role_refresh_max_interval_seconds < role_refresh_interval_seconds:
            raise ValueError(
                "role_refresh_max_interval_seconds must be greater than or equal to "
                "role_refresh_interval_seconds",
            )
        if not 0.0 < role_control_overhead_budget <= 1.0:
            raise ValueError("role_control_overhead_budget must be in (0, 1]")
        if stats_flush_interval_seconds < 0.0:
            raise ValueError("stats_flush_interval_seconds cannot be negative")
        self.node_id = node_id
        self.role_client = role_client
        self.control_endpoint = control_endpoint
        self.batch_endpoint = batch_endpoint
        self.ack_endpoint = ack_endpoint
        self.source = source
        self.consume_batch = consume_batch
        self.model_fingerprint = model_fingerprint
        self.work_delay_seconds = work_delay_seconds
        self.receive_timeout_ms = receive_timeout_ms
        self.consumer_drain_quiet_ms = consumer_drain_quiet_ms
        self.consumer_drain_timeout_ms = consumer_drain_timeout_ms
        self.idle_sleep_seconds = idle_sleep_seconds
        self.role_refresh_interval_seconds = role_refresh_interval_seconds
        self.role_refresh_max_interval_seconds = role_refresh_max_interval_seconds
        self.role_control_overhead_budget = role_control_overhead_budget
        self.stats_flush_interval_seconds = stats_flush_interval_seconds
        self.stats_callback = stats_callback
        self.resources = resources

        self.started_at = monotonic()
        self.role_switches = 0
        self.generator_sessions = 0
        self.consumer_sessions = 0
        self.completed_items = 0
        self.completed_records = 0
        self.waits = 0
        self.consumed_batches = 0
        self.consumed_candidates = 0
        self.drained_batches = 0
        self.drained_candidates = 0
        self.drain_timeouts = 0
        self.current_role = "idle"
        self._last_reply = RoleControlReply()
        self._next_role_refresh_at = 0.0
        self._current_role_refresh_interval_seconds = role_refresh_interval_seconds
        self._next_stats_flush_at = 0.0
        self._current_batch_source: ZmqPullBatchSource | None = None
        self._network_messages = 0
        self._network_batch_messages = 0
        self._network_end_messages = 0
        self._network_bytes = 0
        self._network_poll_seconds = 0.0
        self._network_poll_timeouts = 0
        self._network_recv_seconds = 0.0
        self._network_deserialize_seconds = 0.0
        self._ack_network_messages = 0
        self._ack_network_bytes = 0
        self._ack_network_send_seconds = 0.0
        self._ack_network_serialize_seconds = 0.0

    def run(self) -> NodeAgentStats:
        self.flush_stats(final=False)
        while not self.should_stop():
            desired_role = self.desired_role()
            if desired_role == "generator":
                retired = self.run_generator_session()
                if not retired:
                    break
            elif desired_role == "consumer":
                finished = self.run_consumer_session()
                if finished:
                    break
            else:
                self.current_role = "idle"
                self.flush_stats(final=False)
                sleep(self.idle_sleep_seconds)

        self.current_role = "stopped"
        stats = self.flush_stats(final=True)
        self.role_client.close()
        return stats

    def run_generator_session(self) -> bool:
        self.generator_sessions += 1
        self.current_role = "generator"
        self._role_reply(force=True)
        self.flush_stats(final=False, force=True)
        session_id = f"{self.node_id}-generator-{self.generator_sessions}"
        retire_requested = False
        log_event(
            LOGGER,
            logging.INFO,
            "node_agent.generator_session_start",
            node_id=self.node_id,
            session_id=session_id,
            session=self.generator_sessions,
        )

        def should_retire() -> bool:
            nonlocal retire_requested
            retire_requested = self.should_stop() or self.desired_role() != "generator"
            return retire_requested

        worker = DistributedProtocolWorker(
            worker_id=WorkerId(session_id),
            endpoint=self.control_endpoint,
            source=self.source,
            work_delay_seconds=self.work_delay_seconds,
            should_retire=should_retire,
            model_fingerprint=self.model_fingerprint,
        )
        stats = worker.run()
        self.completed_items += stats.completed_items
        self.completed_records += stats.completed_records
        self.waits += stats.waits
        self.flush_stats(final=False)
        log_event(
            LOGGER,
            logging.INFO,
            "node_agent.generator_session_complete",
            node_id=self.node_id,
            session_id=session_id,
            completed_items=stats.completed_items,
            completed_records=stats.completed_records,
            waits=stats.waits,
            retire_requested=retire_requested,
        )

        if retire_requested and not self.should_stop():
            self.role_switches += 1
            log_event(
                LOGGER,
                logging.INFO,
                "node_agent.role_switch",
                node_id=self.node_id,
                previous_role="generator",
                next_role=self._last_reply.role,
                role_switches=self.role_switches,
            )
            return True
        return False

    def run_consumer_session(self) -> bool:
        self.consumer_sessions += 1
        self.current_role = "consumer"
        self._role_reply(force=True)
        self.flush_stats(final=False, force=True)
        log_event(
            LOGGER,
            logging.INFO,
            "node_agent.consumer_session_start",
            node_id=self.node_id,
            session=self.consumer_sessions,
        )
        source = ZmqPullBatchSource(self.batch_endpoint)
        ack_sink = (
            ZmqPushBatchAckSink(self.ack_endpoint)
            if self.ack_endpoint is not None
            else None
        )
        self._current_batch_source = source
        try:
            with source:
                if ack_sink is not None:
                    ack_sink.open()
                while not self.should_stop():
                    if self.desired_role() != "consumer":
                        finished = self._drain_consumer_before_switch(source, ack_sink)
                        self.role_switches += 1
                        log_event(
                            LOGGER,
                            logging.INFO,
                            "node_agent.role_switch",
                            node_id=self.node_id,
                            previous_role="consumer",
                            next_role=self._last_reply.role,
                            role_switches=self.role_switches,
                            drained_batches=self.drained_batches,
                            drain_timeouts=self.drain_timeouts,
                        )
                        self.flush_stats(final=False)
                        return finished
                    message = source.receive_envelope(timeout_ms=self.receive_timeout_ms)
                    if message is None:
                        continue
                    if isinstance(message, BatchEndOfStream):
                        self.flush_stats(final=False)
                        log_event(
                            LOGGER,
                            logging.INFO,
                            "node_agent.consumer_end_of_stream",
                            node_id=self.node_id,
                            consumed_batches=self.consumed_batches,
                            consumed_candidates=self.consumed_candidates,
                        )
                        return True
                    self._consume_and_ack(message, ack_sink)
                    self.flush_stats(final=False)
        finally:
            self._add_network_stats(source)
            if ack_sink is not None:
                self._add_ack_network_stats(ack_sink)
                ack_sink.close()
            self._current_batch_source = None
        return True

    def _drain_consumer_before_switch(
        self,
        source: ZmqPullBatchSource,
        ack_sink: ZmqPushBatchAckSink | None,
    ) -> bool:
        self.current_role = "draining_consumer"
        quiet_deadline = monotonic() + self.consumer_drain_quiet_ms / 1000.0
        hard_deadline = monotonic() + self.consumer_drain_timeout_ms / 1000.0
        while not self.should_stop():
            now = monotonic()
            if now >= hard_deadline:
                self.drain_timeouts += 1
                return False
            timeout_ms = max(
                0,
                int(min(quiet_deadline - now, hard_deadline - now) * 1000),
            )
            if timeout_ms <= 0:
                return False
            message = source.receive_envelope(timeout_ms=timeout_ms)
            if message is None:
                return False
            if isinstance(message, BatchEndOfStream):
                self.flush_stats(final=False)
                return True
            self._consume_and_ack(message, ack_sink)
            self.drained_batches += 1
            self.drained_candidates += len(message.records)
            quiet_deadline = monotonic() + self.consumer_drain_quiet_ms / 1000.0
            self.flush_stats(final=False)
        return False

    def _consume_and_ack(
        self,
        message: CandidateBatch,
        ack_sink: ZmqPushBatchAckSink | None,
    ) -> None:
        try:
            outputs = _normalize_consumer_outputs(self.consume_batch(message))
            if ack_sink is not None:
                ack_sink.publish(
                    BatchAck(
                        batch_id=message.batch_id,
                        consumer_id=self.node_id,
                        status=BatchAckStatus.DONE,
                        outputs=outputs,
                    )
                )
        except BaseException as exc:
            log_event(
                LOGGER,
                logging.ERROR,
                "node_agent.consume_failed",
                node_id=self.node_id,
                batch_id=message.batch_id,
                start_rank=message.start_rank,
                end_rank=message.end_rank,
                error=exc,
            )
            if ack_sink is not None:
                ack_sink.publish(
                    BatchAck(
                        batch_id=message.batch_id,
                        consumer_id=self.node_id,
                        status=BatchAckStatus.FAILED,
                        error=str(exc),
                    )
                )
            raise
        self.consumed_batches += 1
        self.consumed_candidates += len(message.records)

    def desired_role(self) -> str:
        return self._role_reply().role

    def should_stop(self) -> bool:
        return self._role_reply().stop

    def flush_stats(self, *, final: bool, force: bool = False) -> NodeAgentStats:
        now = monotonic()
        if not final and not force and now < self._next_stats_flush_at:
            return self.stats_snapshot(final=False)
        self._next_stats_flush_at = now + self.stats_flush_interval_seconds
        stats = self.stats_snapshot(final=final)
        if self.stats_callback is not None:
            self.stats_callback(stats)
        return stats

    def stats_snapshot(self, *, final: bool) -> NodeAgentStats:
        elapsed = max(monotonic() - self.started_at, 1e-12)
        network = self._network_snapshot()
        role_stats = self.role_client.stats
        source_stats = source_reclaim_counters(self.source)
        role_control_seconds = (
            role_stats.poll_seconds + role_stats.recv_seconds + role_stats.send_seconds
        )
        model_loaded_once = bool(getattr(self.source, "loaded_once", True))
        return NodeAgentStats(
            node_id=self.node_id,
            current_role=self.current_role,
            desired_role=self._last_reply.role,
            final=final,
            model_loaded_once=model_loaded_once,
            resource_cpu_cores=self.resources.cpu_cores,
            resource_memory_bytes=self.resources.memory_bytes,
            resource_gpu_count=self.resources.gpu_count,
            resource_model_json_page_cache=self.resources.model_json_page_cache,
            role_switches=self.role_switches,
            generator_sessions=self.generator_sessions,
            consumer_sessions=self.consumer_sessions,
            completed_items=self.completed_items,
            completed_records=self.completed_records,
            waits=self.waits,
            consumed_batches=self.consumed_batches,
            consumed_candidates=self.consumed_candidates,
            generation_rate=self.completed_records / elapsed,
            consumer_rate=self.consumed_candidates / elapsed,
            network_messages=network.messages,
            network_batch_messages=network.batch_messages,
            network_end_messages=network.end_messages,
            network_bytes=network.bytes,
            network_poll_seconds=network.poll_seconds,
            network_poll_timeouts=network.poll_timeouts,
            network_recv_seconds=network.recv_seconds,
            network_deserialize_seconds=network.deserialize_seconds,
            ack_network_messages=self._ack_network_messages,
            ack_network_bytes=self._ack_network_bytes,
            ack_network_send_seconds=self._ack_network_send_seconds,
            ack_network_serialize_seconds=self._ack_network_serialize_seconds,
            role_control_messages=role_stats.messages,
            role_control_bytes=role_stats.bytes,
            role_control_seconds=role_control_seconds,
            role_control_request_seconds=role_stats.request_seconds,
            role_control_roundtrip_ewma_seconds=role_stats.roundtrip_ewma_seconds,
            role_refresh_interval_seconds=self._current_role_refresh_interval_seconds,
            source_cached_records=source_stats.cached_records,
            source_peak_cached_records=source_stats.peak_cached_records,
            source_reclaimed_records=source_stats.reclaimed_records,
            source_dag_repository_active_units=source_stats.dag_repository_active_units,
            source_dag_stream_active_units=source_stats.dag_stream_active_units,
            drained_batches=self.drained_batches,
            drained_candidates=self.drained_candidates,
            drain_timeouts=self.drain_timeouts,
            elapsed_seconds=elapsed,
        )

    def _role_reply(self, *, force: bool = False) -> RoleControlReply:
        now = monotonic()
        if not force and now < self._next_role_refresh_at:
            return self._last_reply
        previous_reply = self._last_reply
        elapsed = max(now - self.started_at, 1e-12)
        source_stats = source_reclaim_counters(self.source)
        network = self._network_snapshot()
        self._last_reply = self.role_client.request(
            {
                "current_role": self.current_role,
                "completed_records": self.completed_records,
                "consumed_candidates": self.consumed_candidates,
                "role_switches": self.role_switches,
                "generation_rate": self.completed_records / elapsed,
                "consumer_rate": self.consumed_candidates / elapsed,
                "network_poll_seconds": network.poll_seconds,
                "elapsed_seconds": elapsed,
                "waits": self.waits,
                "completed_items": self.completed_items,
                "source_cached_records": source_stats.cached_records,
                "source_peak_cached_records": source_stats.peak_cached_records,
                "source_reclaimed_records": source_stats.reclaimed_records,
                "source_dag_repository_active_units": source_stats.dag_repository_active_units,
                "source_dag_stream_active_units": source_stats.dag_stream_active_units,
                "resources": self.resources.to_dict(),
            }
        )
        self._current_role_refresh_interval_seconds = (
            self._optimized_role_refresh_interval(previous_reply, self._last_reply)
        )
        self._next_role_refresh_at = monotonic() + self._current_role_refresh_interval_seconds
        return self._last_reply

    def _optimized_role_refresh_interval(
        self,
        previous_reply: RoleControlReply,
        next_reply: RoleControlReply,
    ) -> float:
        if (
            previous_reply.role != next_reply.role
            or previous_reply.stop != next_reply.stop
            or previous_reply.job_context_version != next_reply.job_context_version
        ):
            return self.role_refresh_interval_seconds
        if next_reply.stop:
            return self.role_refresh_interval_seconds

        roundtrip = self.role_client.roundtrip_ewma_seconds
        if roundtrip <= 0.0:
            return self.role_refresh_interval_seconds
        budget_interval = roundtrip / self.role_control_overhead_budget
        return min(
            self.role_refresh_max_interval_seconds,
            max(self.role_refresh_interval_seconds, budget_interval),
        )

    def _network_snapshot(self):
        stats = _MutableNetworkStats(
            messages=self._network_messages,
            batch_messages=self._network_batch_messages,
            end_messages=self._network_end_messages,
            bytes=self._network_bytes,
            poll_seconds=self._network_poll_seconds,
            poll_timeouts=self._network_poll_timeouts,
            recv_seconds=self._network_recv_seconds,
            deserialize_seconds=self._network_deserialize_seconds,
        )
        if self._current_batch_source is not None:
            current = self._current_batch_source.stats
            stats.messages += current.messages
            stats.batch_messages += current.batch_messages
            stats.end_messages += current.end_messages
            stats.bytes += current.bytes
            stats.poll_seconds += current.poll_seconds
            stats.poll_timeouts += current.poll_timeouts
            stats.recv_seconds += current.recv_seconds
            stats.deserialize_seconds += current.deserialize_seconds
        return stats

    def _add_network_stats(self, source: ZmqPullBatchSource) -> None:
        stats = source.stats
        self._network_messages += stats.messages
        self._network_batch_messages += stats.batch_messages
        self._network_end_messages += stats.end_messages
        self._network_bytes += stats.bytes
        self._network_poll_seconds += stats.poll_seconds
        self._network_poll_timeouts += stats.poll_timeouts
        self._network_recv_seconds += stats.recv_seconds
        self._network_deserialize_seconds += stats.deserialize_seconds

    def _add_ack_network_stats(self, sink: ZmqPushBatchAckSink) -> None:
        stats = sink.stats
        self._ack_network_messages += stats.messages
        self._ack_network_bytes += stats.bytes
        self._ack_network_send_seconds += stats.send_seconds
        self._ack_network_serialize_seconds += stats.serialize_seconds


@dataclass(slots=True)
class _MutableNetworkStats:
    messages: int = 0
    batch_messages: int = 0
    end_messages: int = 0
    bytes: int = 0
    poll_seconds: float = 0.0
    poll_timeouts: int = 0
    recv_seconds: float = 0.0
    deserialize_seconds: float = 0.0


def _normalize_consumer_outputs(result: object) -> tuple[Mapping[str, Any], ...]:
    if result is None:
        return ()
    if isinstance(result, Mapping):
        return (dict(result),)
    if not isinstance(result, Iterable) or isinstance(result, (str, bytes, bytearray)):
        raise TypeError(
            "consumer batch handler must return None, a mapping, "
            "or an iterable of mappings",
        )
    outputs: list[Mapping[str, Any]] = []
    for item in result:
        if not isinstance(item, Mapping):
            raise TypeError("consumer batch handler returned a non-mapping output")
        outputs.append(dict(item))
    return tuple(outputs)


__all__ = [
    "CandidateBatchHandler",
    "NodeAgent",
    "NodeAgentStats",
    "NodeAgentStatsCallback",
]
