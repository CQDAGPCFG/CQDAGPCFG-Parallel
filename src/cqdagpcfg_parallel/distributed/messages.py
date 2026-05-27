from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from typing import Any, Literal

from CQDAGPCFG import GuessRecord

from cqdagpcfg_parallel.protocol import NodeId, WorkItem, WorkerId
from cqdagpcfg_parallel.runtime.batch_transport import BinaryCandidateBatchCodec
from cqdagpcfg_parallel.runtime.candidate_batch import CandidateBatch

from .resources import WorkerResourceSpec


MessageType = Literal[
    "ready",
    "work",
    "chunk",
    "exhausted",
    "retire",
    "wait",
    "stop",
    "error",
    "migrate_prepare",
    "migrate_state",
    "migrate_install",
    "migrate_ack",
    "migrate_commit",
    "migrate_abort",
]


@dataclass(frozen=True, slots=True)
class RuntimeFeedback:
    chunk_latency_seconds: float
    records_requested: int
    records_produced: int

    def __post_init__(self) -> None:
        if self.chunk_latency_seconds < 0.0:
            raise ValueError("chunk_latency_seconds cannot be negative")
        if self.records_requested < 0:
            raise ValueError("records_requested cannot be negative")
        if self.records_produced < 0:
            raise ValueError("records_produced cannot be negative")
        if self.records_produced > self.records_requested:
            raise ValueError("records_produced cannot exceed records_requested")


@dataclass(frozen=True, slots=True)
class ControlMessage:
    type: MessageType
    worker_id: WorkerId | None = None
    work_item: WorkItem | None = None
    records: tuple[GuessRecord, ...] = ()
    runtime_feedback: RuntimeFeedback | None = None
    error: str | None = None
    retire: bool = False
    model_fingerprint: str | None = None
    migration_id: str | None = None
    node_id: NodeId | None = None
    source_worker_id: WorkerId | None = None
    target_worker_id: WorkerId | None = None
    source_epoch: int | None = None
    target_epoch: int | None = None
    snapshot_payload: str | None = None
    snapshot_digest: str | None = None
    snapshot_bytes: int | None = None
    artifact_uri: str | None = None
    artifact_sha256: str | None = None
    artifact_format: str | None = None
    artifact_bytes: int | None = None
    artifact_record_count: int | None = None
    artifact_payload_bytes: int | None = None
    artifact_probability_mass: float | None = None
    stable_artifact_uri: str | None = None
    stable_artifact_sha256: str | None = None
    stable_artifact_bytes: int | None = None
    stable_fingerprint: str | None = None
    stable_fingerprint_bytes: int | None = None
    worker_resources: WorkerResourceSpec | None = None


class ControlMessageCodec:
    schema_version = 1
    _binary_magic = b"CQC"
    _binary_header = struct.Struct("!3sBI")

    @classmethod
    def dumps(cls, message: ControlMessage) -> bytes:
        if message.records:
            return cls._dumps_binary_records(message)
        return cls._dumps_json(message)

    @classmethod
    def _dumps_json(cls, message: ControlMessage) -> bytes:
        payload: dict[str, Any] = {
            "schema_version": cls.schema_version,
            "type": message.type,
        }
        if message.worker_id is not None:
            payload["worker_id"] = str(message.worker_id)
        if message.work_item is not None:
            payload["work_item"] = _work_item_to_dict(message.work_item)
        if message.records:
            payload["records"] = [_record_to_dict(record) for record in message.records]
        if message.runtime_feedback is not None:
            payload["runtime_feedback"] = _runtime_feedback_to_dict(message.runtime_feedback)
        if message.error is not None:
            payload["error"] = message.error
        if message.retire:
            payload["retire"] = True
        if message.model_fingerprint is not None:
            payload["model_fingerprint"] = message.model_fingerprint
        if message.migration_id is not None:
            payload["migration_id"] = message.migration_id
        if message.node_id is not None:
            payload["node_id"] = str(message.node_id)
        if message.source_worker_id is not None:
            payload["source_worker_id"] = str(message.source_worker_id)
        if message.target_worker_id is not None:
            payload["target_worker_id"] = str(message.target_worker_id)
        if message.source_epoch is not None:
            payload["source_epoch"] = message.source_epoch
        if message.target_epoch is not None:
            payload["target_epoch"] = message.target_epoch
        if message.snapshot_payload is not None:
            payload["snapshot_payload"] = message.snapshot_payload
        if message.snapshot_digest is not None:
            payload["snapshot_digest"] = message.snapshot_digest
        if message.snapshot_bytes is not None:
            payload["snapshot_bytes"] = message.snapshot_bytes
        if message.artifact_uri is not None:
            payload["artifact_uri"] = message.artifact_uri
        if message.artifact_sha256 is not None:
            payload["artifact_sha256"] = message.artifact_sha256
        if message.artifact_format is not None:
            payload["artifact_format"] = message.artifact_format
        if message.artifact_bytes is not None:
            payload["artifact_bytes"] = message.artifact_bytes
        if message.artifact_record_count is not None:
            payload["artifact_record_count"] = message.artifact_record_count
        if message.artifact_payload_bytes is not None:
            payload["artifact_payload_bytes"] = message.artifact_payload_bytes
        if message.artifact_probability_mass is not None:
            payload["artifact_probability_mass"] = message.artifact_probability_mass
        if message.stable_artifact_uri is not None:
            payload["stable_artifact_uri"] = message.stable_artifact_uri
        if message.stable_artifact_sha256 is not None:
            payload["stable_artifact_sha256"] = message.stable_artifact_sha256
        if message.stable_artifact_bytes is not None:
            payload["stable_artifact_bytes"] = message.stable_artifact_bytes
        if message.stable_fingerprint is not None:
            payload["stable_fingerprint"] = message.stable_fingerprint
        if message.stable_fingerprint_bytes is not None:
            payload["stable_fingerprint_bytes"] = message.stable_fingerprint_bytes
        if message.worker_resources is not None:
            payload["worker_resources"] = message.worker_resources.to_dict()
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @classmethod
    def _dumps_binary_records(cls, message: ControlMessage) -> bytes:
        payload = json.loads(cls._dumps_json(_message_without_records(message)).decode("utf-8"))
        payload["records_codec"] = "candidate-batch-binary-v1"
        start_rank = message.work_item.start if message.work_item is not None else 0
        record_payload = BinaryCandidateBatchCodec.dumps(
            CandidateBatch.from_records(
                batch_id=0,
                start_rank=start_rank,
                records=message.records,
            )
        )
        metadata = json.dumps(
            payload,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return b"".join(
            (
                cls._binary_header.pack(
                    cls._binary_magic,
                    cls.schema_version,
                    len(metadata),
                ),
                metadata,
                record_payload,
            )
        )

    @classmethod
    def loads(cls, payload: bytes) -> ControlMessage:
        if payload.startswith(cls._binary_magic):
            return cls._loads_binary_records(payload)
        raw = json.loads(payload.decode("utf-8"))
        return cls._message_from_raw(raw)

    @classmethod
    def _loads_binary_records(cls, payload: bytes) -> ControlMessage:
        magic, version, metadata_length = cls._binary_header.unpack_from(payload, 0)
        if magic != cls._binary_magic:
            raise ValueError("unsupported control message binary magic")
        if version != cls.schema_version:
            raise ValueError("unsupported control message binary schema version")
        offset = cls._binary_header.size
        metadata = payload[offset : offset + metadata_length]
        record_payload = payload[offset + metadata_length :]
        raw = json.loads(metadata.decode("utf-8"))
        if raw.get("records_codec") != "candidate-batch-binary-v1":
            raise ValueError("unsupported control message records codec")
        batch = BinaryCandidateBatchCodec.loads(record_payload)
        message = cls._message_from_raw(raw)
        return ControlMessage(
            type=message.type,
            worker_id=message.worker_id,
            work_item=message.work_item,
            records=batch.records,
            runtime_feedback=message.runtime_feedback,
            error=message.error,
            retire=message.retire,
            model_fingerprint=message.model_fingerprint,
            migration_id=message.migration_id,
            node_id=message.node_id,
            source_worker_id=message.source_worker_id,
            target_worker_id=message.target_worker_id,
            source_epoch=message.source_epoch,
            target_epoch=message.target_epoch,
            snapshot_payload=message.snapshot_payload,
            snapshot_digest=message.snapshot_digest,
            snapshot_bytes=message.snapshot_bytes,
            artifact_uri=message.artifact_uri,
            artifact_sha256=message.artifact_sha256,
            artifact_format=message.artifact_format,
            artifact_bytes=message.artifact_bytes,
            artifact_record_count=message.artifact_record_count,
            artifact_payload_bytes=message.artifact_payload_bytes,
            artifact_probability_mass=message.artifact_probability_mass,
            stable_artifact_uri=message.stable_artifact_uri,
            stable_artifact_sha256=message.stable_artifact_sha256,
            stable_artifact_bytes=message.stable_artifact_bytes,
            stable_fingerprint=message.stable_fingerprint,
            stable_fingerprint_bytes=message.stable_fingerprint_bytes,
            worker_resources=message.worker_resources,
        )

    @classmethod
    def _message_from_raw(cls, raw: dict[str, Any]) -> ControlMessage:
        if raw.get("schema_version") != cls.schema_version:
            raise ValueError("unsupported control message schema version")

        work_item = None
        if "work_item" in raw:
            work_item = _work_item_from_dict(raw["work_item"])

        return ControlMessage(
            type=raw["type"],
            worker_id=WorkerId(raw["worker_id"]) if raw.get("worker_id") else None,
            work_item=work_item,
            records=tuple(_record_from_dict(record) for record in raw.get("records", ())),
            runtime_feedback=(
                _runtime_feedback_from_dict(raw["runtime_feedback"])
                if "runtime_feedback" in raw
                else None
            ),
            error=raw.get("error"),
            retire=bool(raw.get("retire", False)),
            model_fingerprint=raw.get("model_fingerprint"),
            migration_id=raw.get("migration_id"),
            node_id=NodeId(raw["node_id"]) if raw.get("node_id") else None,
            source_worker_id=(
                WorkerId(raw["source_worker_id"]) if raw.get("source_worker_id") else None
            ),
            target_worker_id=(
                WorkerId(raw["target_worker_id"]) if raw.get("target_worker_id") else None
            ),
            source_epoch=int(raw["source_epoch"]) if "source_epoch" in raw else None,
            target_epoch=int(raw["target_epoch"]) if "target_epoch" in raw else None,
            snapshot_payload=raw.get("snapshot_payload"),
            snapshot_digest=raw.get("snapshot_digest"),
            snapshot_bytes=int(raw["snapshot_bytes"]) if "snapshot_bytes" in raw else None,
            artifact_uri=raw.get("artifact_uri"),
            artifact_sha256=raw.get("artifact_sha256"),
            artifact_format=raw.get("artifact_format"),
            artifact_bytes=int(raw["artifact_bytes"]) if "artifact_bytes" in raw else None,
            artifact_record_count=(
                int(raw["artifact_record_count"])
                if "artifact_record_count" in raw
                else None
            ),
            artifact_payload_bytes=(
                int(raw["artifact_payload_bytes"])
                if "artifact_payload_bytes" in raw
                else None
            ),
            artifact_probability_mass=(
                float(raw["artifact_probability_mass"])
                if "artifact_probability_mass" in raw
                else None
            ),
            stable_artifact_uri=raw.get("stable_artifact_uri"),
            stable_artifact_sha256=raw.get("stable_artifact_sha256"),
            stable_artifact_bytes=(
                int(raw["stable_artifact_bytes"])
                if "stable_artifact_bytes" in raw
                else None
            ),
            stable_fingerprint=raw.get("stable_fingerprint"),
            stable_fingerprint_bytes=(
                int(raw["stable_fingerprint_bytes"])
                if "stable_fingerprint_bytes" in raw
                else None
            ),
            worker_resources=(
                WorkerResourceSpec.from_dict(raw["worker_resources"])
                if "worker_resources" in raw
                else None
            ),
        )


def ready_message(
    worker_id: WorkerId,
    *,
    model_fingerprint: str | None = None,
    worker_resources: WorkerResourceSpec | None = None,
) -> ControlMessage:
    return ControlMessage(
        type="ready",
        worker_id=worker_id,
        model_fingerprint=model_fingerprint,
        worker_resources=worker_resources,
    )


def work_message(item: WorkItem) -> ControlMessage:
    return ControlMessage(type="work", worker_id=item.worker_id, work_item=item)


def chunk_message(
    item: WorkItem,
    records: tuple[GuessRecord, ...],
    *,
    runtime_feedback: RuntimeFeedback | None = None,
    retire: bool = False,
    model_fingerprint: str | None = None,
    worker_resources: WorkerResourceSpec | None = None,
) -> ControlMessage:
    return ControlMessage(
        type="chunk",
        worker_id=item.worker_id,
        work_item=item,
        records=records,
        runtime_feedback=runtime_feedback,
        retire=retire,
        model_fingerprint=model_fingerprint,
        worker_resources=worker_resources,
    )


def artifact_chunk_message(
    item: WorkItem,
    *,
    artifact_uri: str,
    artifact_sha256: str,
    artifact_bytes: int,
    artifact_record_count: int,
    artifact_payload_bytes: int,
    artifact_probability_mass: float,
    stable_artifact_uri: str | None = None,
    stable_artifact_sha256: str | None = None,
    stable_artifact_bytes: int | None = None,
    stable_fingerprint: str | None = None,
    stable_fingerprint_bytes: int | None = None,
    runtime_feedback: RuntimeFeedback | None = None,
    retire: bool = False,
    model_fingerprint: str | None = None,
    worker_resources: WorkerResourceSpec | None = None,
    artifact_format: str = "guess-lines-v1",
) -> ControlMessage:
    return ControlMessage(
        type="chunk",
        worker_id=item.worker_id,
        work_item=item,
        runtime_feedback=runtime_feedback,
        retire=retire,
        model_fingerprint=model_fingerprint,
        artifact_uri=artifact_uri,
        artifact_sha256=artifact_sha256,
        artifact_format=artifact_format,
        artifact_bytes=artifact_bytes,
        artifact_record_count=artifact_record_count,
        artifact_payload_bytes=artifact_payload_bytes,
        artifact_probability_mass=artifact_probability_mass,
        stable_artifact_uri=stable_artifact_uri,
        stable_artifact_sha256=stable_artifact_sha256,
        stable_artifact_bytes=stable_artifact_bytes,
        stable_fingerprint=stable_fingerprint,
        stable_fingerprint_bytes=stable_fingerprint_bytes,
        worker_resources=worker_resources,
    )


def exhausted_message(
    item: WorkItem,
    *,
    runtime_feedback: RuntimeFeedback | None = None,
    retire: bool = False,
    model_fingerprint: str | None = None,
    worker_resources: WorkerResourceSpec | None = None,
) -> ControlMessage:
    return ControlMessage(
        type="exhausted",
        worker_id=item.worker_id,
        work_item=item,
        runtime_feedback=runtime_feedback,
        retire=retire,
        model_fingerprint=model_fingerprint,
        worker_resources=worker_resources,
    )


def wait_message() -> ControlMessage:
    return ControlMessage(type="wait")


def stop_message() -> ControlMessage:
    return ControlMessage(type="stop")


def retire_message(
    worker_id: WorkerId,
    *,
    model_fingerprint: str | None = None,
    worker_resources: WorkerResourceSpec | None = None,
) -> ControlMessage:
    return ControlMessage(
        type="retire",
        worker_id=worker_id,
        retire=True,
        model_fingerprint=model_fingerprint,
        worker_resources=worker_resources,
    )


def error_message(message: str) -> ControlMessage:
    return ControlMessage(type="error", error=message)


def migrate_prepare_message(
    *,
    migration_id: str,
    node_id: NodeId,
    source_worker_id: WorkerId,
    target_worker_id: WorkerId,
    source_epoch: int,
    model_fingerprint: str | None = None,
) -> ControlMessage:
    return ControlMessage(
        type="migrate_prepare",
        migration_id=migration_id,
        node_id=node_id,
        source_worker_id=source_worker_id,
        target_worker_id=target_worker_id,
        source_epoch=source_epoch,
        model_fingerprint=model_fingerprint,
    )


def migrate_state_message(
    *,
    migration_id: str,
    node_id: NodeId,
    source_worker_id: WorkerId,
    target_worker_id: WorkerId,
    source_epoch: int,
    snapshot_payload: str,
    snapshot_digest: str,
    snapshot_bytes: int,
    model_fingerprint: str | None = None,
) -> ControlMessage:
    return ControlMessage(
        type="migrate_state",
        migration_id=migration_id,
        node_id=node_id,
        source_worker_id=source_worker_id,
        target_worker_id=target_worker_id,
        source_epoch=source_epoch,
        snapshot_payload=snapshot_payload,
        snapshot_digest=snapshot_digest,
        snapshot_bytes=snapshot_bytes,
        model_fingerprint=model_fingerprint,
    )


def migrate_install_message(
    *,
    migration_id: str,
    node_id: NodeId,
    source_worker_id: WorkerId,
    target_worker_id: WorkerId,
    source_epoch: int,
    snapshot_payload: str,
    snapshot_digest: str,
    snapshot_bytes: int,
    model_fingerprint: str | None = None,
) -> ControlMessage:
    return ControlMessage(
        type="migrate_install",
        migration_id=migration_id,
        node_id=node_id,
        source_worker_id=source_worker_id,
        target_worker_id=target_worker_id,
        source_epoch=source_epoch,
        snapshot_payload=snapshot_payload,
        snapshot_digest=snapshot_digest,
        snapshot_bytes=snapshot_bytes,
        model_fingerprint=model_fingerprint,
    )


def migrate_ack_message(
    *,
    migration_id: str,
    node_id: NodeId,
    source_worker_id: WorkerId,
    target_worker_id: WorkerId,
    source_epoch: int,
    model_fingerprint: str | None = None,
) -> ControlMessage:
    return ControlMessage(
        type="migrate_ack",
        migration_id=migration_id,
        node_id=node_id,
        source_worker_id=source_worker_id,
        target_worker_id=target_worker_id,
        source_epoch=source_epoch,
        model_fingerprint=model_fingerprint,
    )


def migrate_commit_message(
    *,
    migration_id: str,
    node_id: NodeId,
    source_worker_id: WorkerId,
    target_worker_id: WorkerId,
    source_epoch: int,
    target_epoch: int,
    model_fingerprint: str | None = None,
) -> ControlMessage:
    return ControlMessage(
        type="migrate_commit",
        migration_id=migration_id,
        node_id=node_id,
        source_worker_id=source_worker_id,
        target_worker_id=target_worker_id,
        source_epoch=source_epoch,
        target_epoch=target_epoch,
        model_fingerprint=model_fingerprint,
    )


def migrate_abort_message(
    *,
    migration_id: str,
    node_id: NodeId,
    source_worker_id: WorkerId,
    target_worker_id: WorkerId,
    source_epoch: int,
    error: str | None = None,
    model_fingerprint: str | None = None,
) -> ControlMessage:
    return ControlMessage(
        type="migrate_abort",
        migration_id=migration_id,
        node_id=node_id,
        source_worker_id=source_worker_id,
        target_worker_id=target_worker_id,
        source_epoch=source_epoch,
        error=error,
        model_fingerprint=model_fingerprint,
    )


def _work_item_to_dict(item: WorkItem) -> dict[str, Any]:
    return {
        "node_id": str(item.node_id),
        "start": item.start,
        "end": item.end,
        "worker_id": str(item.worker_id),
        "epoch": item.epoch,
        "reclaim_before": item.reclaim_before,
        "estimated_mass": item.estimated_mass,
        "mass_budget": item.mass_budget,
    }


def _work_item_from_dict(raw: dict[str, Any]) -> WorkItem:
    return WorkItem(
        node_id=NodeId(str(raw["node_id"])),
        start=int(raw["start"]),
        end=int(raw["end"]),
        worker_id=WorkerId(str(raw["worker_id"])),
        epoch=int(raw["epoch"]),
        reclaim_before=int(raw.get("reclaim_before", 0)),
        estimated_mass=float(raw.get("estimated_mass", 0.0)),
        mass_budget=float(raw.get("mass_budget", 0.0)),
    )


def _record_to_dict(record: GuessRecord) -> dict[str, Any]:
    return {
        "prob": record.prob,
        "guess": record.guess,
        "structure_index": record.structure_index,
        "structure_name": record.structure_name,
        "ranks": list(record.ranks),
    }


def _record_from_dict(raw: dict[str, Any]) -> GuessRecord:
    return GuessRecord(
        prob=float(raw["prob"]),
        guess=str(raw["guess"]),
        structure_index=int(raw["structure_index"]),
        structure_name=str(raw["structure_name"]),
        ranks=tuple(int(rank) for rank in raw["ranks"]),
    )


def _runtime_feedback_to_dict(feedback: RuntimeFeedback) -> dict[str, Any]:
    return {
        "chunk_latency_seconds": feedback.chunk_latency_seconds,
        "records_requested": feedback.records_requested,
        "records_produced": feedback.records_produced,
    }


def _runtime_feedback_from_dict(raw: dict[str, Any]) -> RuntimeFeedback:
    return RuntimeFeedback(
        chunk_latency_seconds=float(raw["chunk_latency_seconds"]),
        records_requested=int(raw["records_requested"]),
        records_produced=int(raw["records_produced"]),
    )


def _message_without_records(message: ControlMessage) -> ControlMessage:
    return ControlMessage(
        type=message.type,
        worker_id=message.worker_id,
        work_item=message.work_item,
        runtime_feedback=message.runtime_feedback,
        error=message.error,
        retire=message.retire,
        model_fingerprint=message.model_fingerprint,
        migration_id=message.migration_id,
        node_id=message.node_id,
        source_worker_id=message.source_worker_id,
        target_worker_id=message.target_worker_id,
        source_epoch=message.source_epoch,
        target_epoch=message.target_epoch,
        snapshot_payload=message.snapshot_payload,
        snapshot_digest=message.snapshot_digest,
        snapshot_bytes=message.snapshot_bytes,
        artifact_uri=message.artifact_uri,
        artifact_sha256=message.artifact_sha256,
        artifact_format=message.artifact_format,
        artifact_bytes=message.artifact_bytes,
        artifact_record_count=message.artifact_record_count,
        artifact_payload_bytes=message.artifact_payload_bytes,
        artifact_probability_mass=message.artifact_probability_mass,
        stable_artifact_uri=message.stable_artifact_uri,
        stable_artifact_sha256=message.stable_artifact_sha256,
        stable_artifact_bytes=message.stable_artifact_bytes,
        stable_fingerprint=message.stable_fingerprint,
        stable_fingerprint_bytes=message.stable_fingerprint_bytes,
        worker_resources=message.worker_resources,
    )


__all__ = [
    "ControlMessage",
    "ControlMessageCodec",
    "RuntimeFeedback",
    "artifact_chunk_message",
    "chunk_message",
    "error_message",
    "exhausted_message",
    "migrate_abort_message",
    "migrate_ack_message",
    "migrate_commit_message",
    "migrate_install_message",
    "migrate_prepare_message",
    "migrate_state_message",
    "ready_message",
    "retire_message",
    "stop_message",
    "wait_message",
    "work_message",
]
