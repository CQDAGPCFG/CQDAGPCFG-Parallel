from __future__ import annotations

from dataclasses import dataclass
from time import monotonic, sleep
from typing import Any, Callable

from cqdagpcfg_parallel.protocol import NodeId, WorkerId
from cqdagpcfg_parallel.runtime.worker import (
    LocalResultSource,
    _reclaim_source_before,
    source_reclaim_counters,
)
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint, _require_zmq
from cqdagpcfg_parallel.storage import StateMigrationSnapshot

from .messages import (
    ControlMessage,
    ControlMessageCodec,
    RuntimeFeedback,
    chunk_message,
    exhausted_message,
    migrate_abort_message,
    migrate_ack_message,
    migrate_state_message,
    ready_message,
    retire_message,
)


@dataclass(frozen=True, slots=True)
class DistributedWorkerStats:
    worker_id: WorkerId
    completed_items: int = 0
    completed_records: int = 0
    waits: int = 0
    source_cached_records: int = 0
    source_peak_cached_records: int = 0
    source_reclaimed_records: int = 0
    source_dag_repository_active_units: int = 0
    source_dag_stream_active_units: int = 0


class DistributedProtocolWorker:
    def __init__(
        self,
        *,
        worker_id: WorkerId,
        endpoint: ZmqEndpoint,
        source: LocalResultSource,
        context: Any | None = None,
        wait_sleep_seconds: float = 0.001,
        work_delay_seconds: float = 0.0,
        should_retire: Callable[[], bool] | None = None,
        model_fingerprint: str | None = None,
    ) -> None:
        if endpoint.bind:
            raise ValueError("worker endpoint must connect, not bind")
        if wait_sleep_seconds < 0.0:
            raise ValueError("wait_sleep_seconds cannot be negative")
        if work_delay_seconds < 0.0:
            raise ValueError("work_delay_seconds cannot be negative")
        self.worker_id = worker_id
        self.endpoint = endpoint
        self.source = source
        self.context = context
        self.wait_sleep_seconds = wait_sleep_seconds
        self.work_delay_seconds = work_delay_seconds
        self.should_retire = should_retire or (lambda: False)
        self.model_fingerprint = model_fingerprint

    def run(self) -> DistributedWorkerStats:
        zmq = _require_zmq()
        owns_context = self.context is None
        context = zmq.Context() if self.context is None else self.context
        socket = context.socket(zmq.DEALER)
        socket.setsockopt(zmq.IDENTITY, str(self.worker_id).encode("utf-8"))
        socket.setsockopt(zmq.SNDHWM, self.endpoint.high_watermark)
        socket.setsockopt(zmq.RCVHWM, self.endpoint.high_watermark)
        socket.setsockopt(zmq.LINGER, self.endpoint.linger_ms)
        socket.connect(self.endpoint.address)

        completed_items = 0
        completed_records = 0
        waits = 0
        try:
            self._send(
                socket,
                ready_message(
                    self.worker_id,
                    model_fingerprint=self.model_fingerprint,
                ),
            )
            while True:
                reply = self._recv(socket)
                if reply.type == "stop":
                    return self._stats(completed_items, completed_records, waits)
                if reply.type == "wait":
                    if self.should_retire():
                        self._send(
                            socket,
                            retire_message(
                                self.worker_id,
                                model_fingerprint=self.model_fingerprint,
                            ),
                        )
                        final_reply = self._recv(socket)
                        if final_reply.type != "stop":
                            raise RuntimeError(f"unexpected retire reply: {final_reply.type}")
                        return self._stats(completed_items, completed_records, waits)
                    waits += 1
                    if self.wait_sleep_seconds:
                        sleep(self.wait_sleep_seconds)
                    self._send(
                        socket,
                        ready_message(
                            self.worker_id,
                            model_fingerprint=self.model_fingerprint,
                        ),
                    )
                    continue
                if reply.type == "error":
                    raise RuntimeError(reply.error or "tracker returned an error")
                if reply.type == "migrate_prepare":
                    self._send(socket, self._capture_migration_state(reply))
                    continue
                if reply.type == "migrate_install":
                    self._send(socket, self._install_migration_state(reply))
                    continue
                if reply.type in {"migrate_commit", "migrate_abort"}:
                    self._send(
                        socket,
                        ready_message(
                            self.worker_id,
                            model_fingerprint=self.model_fingerprint,
                        ),
                    )
                    continue
                if reply.type != "work" or reply.work_item is None:
                    raise RuntimeError(f"unexpected tracker reply: {reply.type}")

                item = reply.work_item
                _reclaim_source_before(self.source, item.node_id, item.reclaim_before)
                started_at = monotonic()
                records = tuple(self.source.read_range(item.node_id, item.start, item.end))
                if self.work_delay_seconds:
                    sleep(self.work_delay_seconds)
                latency = monotonic() - started_at
                retire_after_item = self.should_retire()
                feedback = RuntimeFeedback(
                    chunk_latency_seconds=latency,
                    records_requested=item.size,
                    records_produced=len(records),
                )
                if records:
                    completed_items += 1
                    completed_records += len(records)
                    self._send(
                        socket,
                        chunk_message(
                            item,
                            records,
                            runtime_feedback=feedback,
                            retire=retire_after_item,
                            model_fingerprint=self.model_fingerprint,
                        ),
                    )
                else:
                    self._send(
                        socket,
                        exhausted_message(
                            item,
                            runtime_feedback=feedback,
                            retire=retire_after_item,
                            model_fingerprint=self.model_fingerprint,
                        ),
                    )
        finally:
            socket.close()
            if owns_context:
                context.term()

    def _send(self, socket: Any, message: ControlMessage) -> None:
        socket.send(ControlMessageCodec.dumps(message))

    def _recv(self, socket: Any) -> ControlMessage:
        return ControlMessageCodec.loads(socket.recv())

    def _capture_migration_state(self, message: ControlMessage) -> ControlMessage:
        try:
            capture_state = getattr(self.source, "capture_state", None)
            if not callable(capture_state):
                raise RuntimeError("source does not support state migration capture")
            migration_id, node_id, target_worker_id, source_epoch = self._migration_fields(
                message,
            )
            model_fingerprint = message.model_fingerprint or self.model_fingerprint or ""
            snapshot = capture_state(
                model_fingerprint=model_fingerprint,
                source_worker_id=self.worker_id,
                target_worker_id=target_worker_id,
                node_ids=(node_id,),
                reason="rebalance",
            )
            payload = snapshot.to_json().decode("utf-8")
            return migrate_state_message(
                migration_id=migration_id,
                node_id=node_id,
                source_worker_id=self.worker_id,
                target_worker_id=target_worker_id,
                source_epoch=source_epoch,
                snapshot_payload=payload,
                snapshot_digest=snapshot.content_digest(),
                snapshot_bytes=snapshot.payload_bytes(),
                model_fingerprint=model_fingerprint,
            )
        except BaseException as exc:
            return self._migration_abort(message, str(exc))

    def _install_migration_state(self, message: ControlMessage) -> ControlMessage:
        try:
            restore_state = getattr(self.source, "restore_state", None)
            if not callable(restore_state):
                raise RuntimeError("source does not support state migration restore")
            migration_id, node_id, target_worker_id, source_epoch = self._migration_fields(
                message,
            )
            if target_worker_id != self.worker_id:
                raise RuntimeError("migration install target does not match worker")
            if not message.snapshot_payload:
                raise RuntimeError("migration install is missing snapshot_payload")
            snapshot = StateMigrationSnapshot.from_json(message.snapshot_payload)
            expected_fingerprint = message.model_fingerprint or self.model_fingerprint
            restore_state(snapshot, expected_model_fingerprint=expected_fingerprint)
            return migrate_ack_message(
                migration_id=migration_id,
                node_id=node_id,
                source_worker_id=message.source_worker_id or WorkerId(""),
                target_worker_id=self.worker_id,
                source_epoch=source_epoch,
                model_fingerprint=expected_fingerprint,
            )
        except BaseException as exc:
            return self._migration_abort(message, str(exc))

    def _migration_abort(self, message: ControlMessage, error: str) -> ControlMessage:
        migration_id = message.migration_id or ""
        node_id = message.node_id or NodeId("")
        source_worker_id = message.source_worker_id or self.worker_id
        target_worker_id = message.target_worker_id or self.worker_id
        return migrate_abort_message(
            migration_id=migration_id,
            node_id=node_id,
            source_worker_id=source_worker_id,
            target_worker_id=target_worker_id,
            source_epoch=message.source_epoch or 0,
            error=error,
            model_fingerprint=message.model_fingerprint or self.model_fingerprint,
        )

    def _migration_fields(
        self,
        message: ControlMessage,
    ) -> tuple[str, NodeId, WorkerId, int]:
        if message.migration_id is None:
            raise RuntimeError("migration message is missing migration_id")
        if message.node_id is None:
            raise RuntimeError("migration message is missing node_id")
        if message.target_worker_id is None:
            raise RuntimeError("migration message is missing target_worker_id")
        if message.source_epoch is None:
            raise RuntimeError("migration message is missing source_epoch")
        return (
            message.migration_id,
            message.node_id,
            message.target_worker_id,
            message.source_epoch,
        )

    def _stats(
        self,
        completed_items: int,
        completed_records: int,
        waits: int,
    ) -> DistributedWorkerStats:
        source = source_reclaim_counters(self.source)
        return DistributedWorkerStats(
            worker_id=self.worker_id,
            completed_items=completed_items,
            completed_records=completed_records,
            waits=waits,
            source_cached_records=source.cached_records,
            source_peak_cached_records=source.peak_cached_records,
            source_reclaimed_records=source.reclaimed_records,
            source_dag_repository_active_units=source.dag_repository_active_units,
            source_dag_stream_active_units=source.dag_stream_active_units,
        )


__all__ = [
    "DistributedProtocolWorker",
    "DistributedWorkerStats",
]
