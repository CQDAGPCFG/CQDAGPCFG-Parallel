from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Callable
from urllib.parse import unquote, urlparse

from cqdagpcfg_parallel.protocol import NodeId, WorkerId
from cqdagpcfg_parallel.runtime.worker import (
    LocalResultSource,
    _reclaim_source_before,
    source_reclaim_counters,
)
from cqdagpcfg_parallel.runtime.candidate_batch import UNCHECKED_ARTIFACT_SHA256
from cqdagpcfg_parallel.runtime.zmq_transport import (
    ZmqEndpoint,
    _require_zmq,
    configure_zmq_socket,
)
from cqdagpcfg_parallel.storage import StateMigrationSnapshot

from .messages import (
    ControlMessage,
    ControlMessageCodec,
    RuntimeFeedback,
    artifact_chunk_message,
    chunk_message,
    exhausted_message,
    migrate_abort_message,
    migrate_ack_message,
    migrate_state_message,
    ready_message,
    retire_message,
    stop_message,
)
from .resources import WorkerResourceSpec


@dataclass(frozen=True, slots=True)
class DistributedWorkerStats:
    worker_id: WorkerId
    completed_items: int = 0
    completed_records: int = 0
    waits: int = 0
    control_reply_timeouts: int = 0
    control_retries: int = 0
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
        control_reply_timeout_ms: int = 5000,
        max_control_reply_timeouts: int | None = 3,
        should_retire: Callable[[], bool] | None = None,
        model_fingerprint: str | None = None,
        write_stable_artifacts: bool = True,
        verify_candidate_artifacts: bool = True,
        resources: WorkerResourceSpec = WorkerResourceSpec(),
    ) -> None:
        if endpoint.bind:
            raise ValueError("worker endpoint must connect, not bind")
        if wait_sleep_seconds < 0.0:
            raise ValueError("wait_sleep_seconds cannot be negative")
        if work_delay_seconds < 0.0:
            raise ValueError("work_delay_seconds cannot be negative")
        if control_reply_timeout_ms <= 0:
            raise ValueError("control_reply_timeout_ms must be positive")
        if max_control_reply_timeouts is not None and max_control_reply_timeouts <= 0:
            raise ValueError("max_control_reply_timeouts must be positive")
        self.worker_id = worker_id
        self.endpoint = endpoint
        self.source = source
        self.context = context
        self.wait_sleep_seconds = wait_sleep_seconds
        self.work_delay_seconds = work_delay_seconds
        self.control_reply_timeout_ms = control_reply_timeout_ms
        self.max_control_reply_timeouts = max_control_reply_timeouts
        self.should_retire = should_retire or (lambda: False)
        self.model_fingerprint = model_fingerprint
        self.write_stable_artifacts = write_stable_artifacts
        self.verify_candidate_artifacts = verify_candidate_artifacts
        self.resources = resources
        self.artifact_dir = _worker_artifact_dir()
        self.discard_candidate_payloads = (
            self.resources.labels.get("cqpcfg.discard_candidate_payloads") == "1"
        )
        self._control_reply_timeouts = 0
        self._control_retries = 0

    def run(self) -> DistributedWorkerStats:
        zmq = _require_zmq()
        owns_context = self.context is None
        context = zmq.Context() if self.context is None else self.context
        socket = context.socket(zmq.DEALER)
        configure_zmq_socket(
            socket,
            self.endpoint,
            zmq_module=zmq,
            identity=str(self.worker_id).encode("utf-8"),
            send=True,
            recv=True,
            connect=True,
        )
        socket.connect(self.endpoint.address)

        completed_items = 0
        completed_records = 0
        waits = 0
        try:
            last_message = ready_message(
                self.worker_id,
                model_fingerprint=self.model_fingerprint,
                worker_resources=self.resources,
            )
            self._send(socket, last_message)
            while True:
                reply = self._recv(socket, last_message=last_message)
                if reply.type == "stop":
                    return self._stats(completed_items, completed_records, waits)
                if reply.type == "wait":
                    if self.should_retire():
                        last_message = retire_message(
                            self.worker_id,
                            model_fingerprint=self.model_fingerprint,
                            worker_resources=self.resources,
                        )
                        self._send(socket, last_message)
                        final_reply = self._recv(socket, last_message=last_message)
                        if final_reply.type != "stop":
                            raise RuntimeError(f"unexpected retire reply: {final_reply.type}")
                        return self._stats(completed_items, completed_records, waits)
                    waits += 1
                    if self.wait_sleep_seconds:
                        sleep(self.wait_sleep_seconds)
                    last_message = ready_message(
                        self.worker_id,
                        model_fingerprint=self.model_fingerprint,
                        worker_resources=self.resources,
                    )
                    self._send(socket, last_message)
                    continue
                if reply.type == "error":
                    raise RuntimeError(reply.error or "tracker returned an error")
                if reply.type == "migrate_prepare":
                    last_message = self._capture_migration_state(reply)
                    self._send(socket, last_message)
                    continue
                if reply.type == "migrate_install":
                    last_message = self._install_migration_state(reply)
                    self._send(socket, last_message)
                    continue
                if reply.type in {"migrate_commit", "migrate_abort"}:
                    last_message = ready_message(
                        self.worker_id,
                        model_fingerprint=self.model_fingerprint,
                        worker_resources=self.resources,
                    )
                    self._send(socket, last_message)
                    continue
                if reply.type != "work" or reply.work_item is None:
                    raise RuntimeError(f"unexpected tracker reply: {reply.type}")

                item = reply.work_item
                _reclaim_source_before(self.source, item.node_id, item.reclaim_before)
                started_at = monotonic()
                artifact = self._write_artifact_chunk(item)
                records = (
                    ()
                    if artifact is not None
                    else tuple(self.source.read_range(item.node_id, item.start, item.end))
                )
                if self.work_delay_seconds:
                    sleep(self.work_delay_seconds)
                latency = monotonic() - started_at
                retire_after_item = self.should_retire()
                produced_records = (
                    int(artifact.record_count) if artifact is not None else len(records)
                )
                feedback = RuntimeFeedback(
                    chunk_latency_seconds=latency,
                    records_requested=item.size,
                    records_produced=produced_records,
                )
                if artifact is not None and artifact.record_count > 0:
                    completed_items += 1
                    completed_records += int(artifact.record_count)
                    last_message = artifact_chunk_message(
                        item,
                        artifact_uri=artifact.artifact_uri,
                        artifact_sha256=artifact.artifact_sha256,
                        artifact_bytes=artifact.artifact_bytes,
                        artifact_record_count=artifact.record_count,
                        artifact_payload_bytes=artifact.payload_bytes,
                        artifact_probability_mass=artifact.probability_mass,
                        stable_artifact_uri=artifact.stable_artifact_uri,
                        stable_artifact_sha256=artifact.stable_artifact_sha256,
                        stable_artifact_bytes=artifact.stable_artifact_bytes,
                        stable_fingerprint=artifact.stable_fingerprint,
                        stable_fingerprint_bytes=artifact.stable_fingerprint_bytes,
                        runtime_feedback=feedback,
                        retire=retire_after_item,
                        model_fingerprint=self.model_fingerprint,
                        worker_resources=self.resources,
                        artifact_format=(
                            "count-only-v1"
                            if self.discard_candidate_payloads
                            else "guess-lines-v1"
                        ),
                    )
                    self._send(socket, last_message)
                elif records:
                    completed_items += 1
                    completed_records += len(records)
                    last_message = chunk_message(
                        item,
                        records,
                        runtime_feedback=feedback,
                        retire=retire_after_item,
                        model_fingerprint=self.model_fingerprint,
                        worker_resources=self.resources,
                    )
                    self._send(socket, last_message)
                else:
                    last_message = exhausted_message(
                        item,
                        runtime_feedback=feedback,
                        retire=retire_after_item,
                        model_fingerprint=self.model_fingerprint,
                        worker_resources=self.resources,
                    )
                    self._send(socket, last_message)
        finally:
            socket.close()
            if owns_context:
                context.term()

    def _write_artifact_chunk(self, item) -> Any | None:
        if self.resources.labels.get("cqpcfg.streaming_artifacts") != "1":
            return None
        if self.artifact_dir is None and not self.discard_candidate_payloads:
            return None
        writer = getattr(self.source, "write_range_artifact", None)
        if not callable(writer):
            return None
        stem = _artifact_stem(self.worker_id, item)
        guess_path = (
            Path(os.devnull)
            if self.discard_candidate_payloads
            else self.artifact_dir / f"{stem}.txt"
        )
        stable_path = (
            self.artifact_dir / f"{stem}.stable"
            if self.write_stable_artifacts and not self.discard_candidate_payloads
            else None
        )
        try:
            artifact = writer(
                item.node_id,
                item.start,
                item.end,
                guess_path=guess_path,
                stable_path=stable_path,
                verify_artifact=self.verify_candidate_artifacts,
                include_stable_metadata=self.verify_candidate_artifacts,
            )
            if int(getattr(artifact, "record_count", 0)) <= 0:
                _delete_local_artifact_files(artifact)
            if not self.discard_candidate_payloads:
                return artifact
            return _discarded_artifact(artifact, self.worker_id, item)
        except RuntimeError as exc:
            if "does not support range artifact" in str(exc):
                return None
            raise

    def _send(self, socket: Any, message: ControlMessage) -> None:
        socket.send(ControlMessageCodec.dumps(message))

    def _recv(self, socket: Any, *, last_message: ControlMessage) -> ControlMessage:
        zmq = _require_zmq()
        consecutive_timeouts = 0
        while True:
            if socket.poll(self.control_reply_timeout_ms, zmq.POLLIN):
                return ControlMessageCodec.loads(socket.recv())
            self._control_reply_timeouts += 1
            consecutive_timeouts += 1
            # Do not blindly resend READY/CHUNK messages here. Without message
            # sequence numbers, a resend can queue duplicate work assignments
            # and make a streaming source receive a range it has already
            # reclaimed. ZMQ/TCP preserves the original request; timeout is only
            # recorded as observability.
            if self.should_retire():
                return stop_message()
            if (
                self.max_control_reply_timeouts is not None
                and consecutive_timeouts >= self.max_control_reply_timeouts
            ):
                return stop_message()

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
            control_reply_timeouts=self._control_reply_timeouts,
            control_retries=self._control_retries,
            source_cached_records=source.cached_records,
            source_peak_cached_records=source.peak_cached_records,
            source_reclaimed_records=source.reclaimed_records,
            source_dag_repository_active_units=source.dag_repository_active_units,
            source_dag_stream_active_units=source.dag_stream_active_units,
        )


def _worker_artifact_dir() -> Path | None:
    if os.environ.get("CQPCFG_DISABLE_WORKER_ARTIFACTS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return None
    raw = (
        os.environ.get("CQPCFG_WORKER_CANDIDATE_BLOCK_DIR")
        or os.environ.get("CQPCFG_CANDIDATE_BLOCK_DIR")
    )
    if raw is None or raw == "":
        return None
    if os.environ.get("CQPCFG_ENABLE_WORKER_ARTIFACTS", "").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return None
    path = Path(raw)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _discarded_artifact(artifact: Any, worker_id: WorkerId, item) -> Any:
    return replace(
        artifact,
        artifact_uri=(
            f"count://{worker_id}/{item.node_id}/{item.start}-{item.end}"
        ),
        artifact_sha256=UNCHECKED_ARTIFACT_SHA256,
        artifact_bytes=0,
    )


def _delete_local_artifact_files(artifact: Any) -> None:
    for attr in ("artifact_uri", "stable_artifact_uri"):
        uri = getattr(artifact, attr, None)
        if uri is not None:
            _delete_local_artifact_uri(str(uri))


def _delete_local_artifact_uri(uri: str) -> None:
    parsed = urlparse(uri)
    if parsed.scheme and parsed.scheme != "file":
        return
    path = Path(unquote(parsed.path)) if parsed.scheme == "file" else Path(uri)
    try:
        if path.is_file():
            path.unlink()
    except OSError:
        pass


def _artifact_stem(worker_id: WorkerId, item) -> str:
    worker = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in str(worker_id)
    )
    node = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in str(item.node_id)
    )
    return f"worker-{worker}-{node}-{item.epoch}-{item.start}-{item.end}"


__all__ = [
    "DistributedProtocolWorker",
    "DistributedWorkerStats",
]
