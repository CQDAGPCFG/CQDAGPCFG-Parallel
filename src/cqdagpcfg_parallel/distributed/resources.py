from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class WorkerResourceSpec:
    """Resource capability or budget carried by the CQPCFG role protocol."""

    cpu_cores: float | None = None
    memory_bytes: int | None = None
    gpu_count: int | None = None
    gpu_memory_bytes: int | None = None
    model_json_page_cache: int | None = None
    labels: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.cpu_cores is not None and self.cpu_cores < 0.0:
            raise ValueError("cpu_cores cannot be negative")
        if self.memory_bytes is not None and self.memory_bytes < 0:
            raise ValueError("memory_bytes cannot be negative")
        if self.gpu_count is not None and self.gpu_count < 0:
            raise ValueError("gpu_count cannot be negative")
        if self.gpu_memory_bytes is not None and self.gpu_memory_bytes < 0:
            raise ValueError("gpu_memory_bytes cannot be negative")
        if self.model_json_page_cache is not None and self.model_json_page_cache <= 0:
            raise ValueError("model_json_page_cache must be positive")

    def fits(self, requirement: "WorkerResourceSpec") -> bool:
        return (
            _fits_float(self.cpu_cores, requirement.cpu_cores)
            and _fits_int(self.memory_bytes, requirement.memory_bytes)
            and _fits_int(self.gpu_count, requirement.gpu_count)
            and _fits_int(self.gpu_memory_bytes, requirement.gpu_memory_bytes)
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any] | None) -> "WorkerResourceSpec":
        if payload is None:
            return cls()
        return cls(
            cpu_cores=_optional_float(payload.get("cpu_cores")),
            memory_bytes=_optional_int(payload.get("memory_bytes")),
            gpu_count=_optional_int(payload.get("gpu_count")),
            gpu_memory_bytes=_optional_int(payload.get("gpu_memory_bytes")),
            model_json_page_cache=_optional_int(payload.get("model_json_page_cache")),
            labels={str(key): str(value) for key, value in dict(payload.get("labels", {})).items()},
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.cpu_cores is not None:
            payload["cpu_cores"] = self.cpu_cores
        if self.memory_bytes is not None:
            payload["memory_bytes"] = self.memory_bytes
        if self.gpu_count is not None:
            payload["gpu_count"] = self.gpu_count
        if self.gpu_memory_bytes is not None:
            payload["gpu_memory_bytes"] = self.gpu_memory_bytes
        if self.model_json_page_cache is not None:
            payload["model_json_page_cache"] = self.model_json_page_cache
        if self.labels:
            payload["labels"] = dict(self.labels)
        return payload


@dataclass(frozen=True, slots=True)
class RoleResourcePolicy:
    generator_min: WorkerResourceSpec = WorkerResourceSpec()
    consumer_min: WorkerResourceSpec = WorkerResourceSpec()

    def requirement_for(self, role: str) -> WorkerResourceSpec:
        if role == "generator":
            return self.generator_min
        if role == "consumer":
            return self.consumer_min
        return WorkerResourceSpec()


def parse_byte_size(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    raw = value.strip().lower()
    if not raw:
        return None
    suffixes = {
        "k": 1024,
        "kb": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "t": 1024**4,
        "tb": 1024**4,
    }
    for suffix, multiplier in suffixes.items():
        if raw.endswith(suffix):
            return int(float(raw[: -len(suffix)]) * multiplier)
    return int(raw)


def _fits_float(available: float | None, required: float | None) -> bool:
    return required is None or available is None or available >= required


def _fits_int(available: int | None, required: int | None) -> bool:
    return required is None or available is None or available >= required


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


__all__ = [
    "RoleResourcePolicy",
    "WorkerResourceSpec",
    "parse_byte_size",
]
