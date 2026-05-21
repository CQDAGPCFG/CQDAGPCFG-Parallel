from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class JobContext:
    """Runtime job description sent to resource-only node agents."""

    job_id: str
    limit: int
    hash_algorithm: str
    targets: tuple[Mapping[str, Any], ...]
    model_id: str
    model_fingerprint: str
    model_connect: str
    control_connect: str
    batch_connect: str
    ack_connect: str
    source_mode: str = "root"
    demand_window: int = 8
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.job_id:
            raise ValueError("job_id cannot be empty")
        if self.limit <= 0:
            raise ValueError("limit must be positive")
        if not self.hash_algorithm:
            raise ValueError("hash_algorithm cannot be empty")
        if not self.model_id:
            raise ValueError("model_id cannot be empty")
        if not self.model_fingerprint:
            raise ValueError("model_fingerprint cannot be empty")
        for name, value in (
            ("model_connect", self.model_connect),
            ("control_connect", self.control_connect),
            ("batch_connect", self.batch_connect),
            ("ack_connect", self.ack_connect),
        ):
            if not value:
                raise ValueError(f"{name} cannot be empty")
        if self.source_mode not in {"root", "structure"}:
            raise ValueError("source_mode must be root or structure")
        if self.demand_window < 0:
            raise ValueError("demand_window cannot be negative")

    @property
    def version(self) -> str:
        return f"{self.job_id}:{self.model_fingerprint}:{self.limit}"

    @classmethod
    def from_targets_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        job_id: str,
        model_id: str,
        model_connect: str,
        control_connect: str,
        batch_connect: str,
        ack_connect: str,
        source_mode: str = "root",
        demand_window: int = 8,
        metadata: Mapping[str, str] | None = None,
    ) -> "JobContext":
        return cls(
            job_id=job_id,
            limit=int(payload["limit"]),
            hash_algorithm=str(payload["algorithm"]),
            targets=tuple(dict(target) for target in payload.get("targets", ())),
            model_id=model_id,
            model_fingerprint=str(payload["model_fingerprint"]),
            model_connect=model_connect,
            control_connect=control_connect,
            batch_connect=batch_connect,
            ack_connect=ack_connect,
            source_mode=source_mode,
            demand_window=demand_window,
            metadata={} if metadata is None else dict(metadata),
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "JobContext":
        return cls(
            job_id=str(payload["job_id"]),
            limit=int(payload["limit"]),
            hash_algorithm=str(payload["hash_algorithm"]),
            targets=tuple(dict(target) for target in payload.get("targets", ())),
            model_id=str(payload["model_id"]),
            model_fingerprint=str(payload["model_fingerprint"]),
            model_connect=str(payload["model_connect"]),
            control_connect=str(payload["control_connect"]),
            batch_connect=str(payload["batch_connect"]),
            ack_connect=str(payload["ack_connect"]),
            source_mode=str(payload.get("source_mode", "root")),
            demand_window=int(payload.get("demand_window", 8)),
            metadata=dict(payload.get("metadata", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "limit": self.limit,
            "hash_algorithm": self.hash_algorithm,
            "targets": [dict(target) for target in self.targets],
            "model_id": self.model_id,
            "model_fingerprint": self.model_fingerprint,
            "model_connect": self.model_connect,
            "control_connect": self.control_connect,
            "batch_connect": self.batch_connect,
            "ack_connect": self.ack_connect,
            "source_mode": self.source_mode,
            "demand_window": self.demand_window,
            "metadata": dict(self.metadata),
        }


__all__ = ["JobContext"]
