from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from cqdagpcfg_parallel.protocol import stable_digest

from .serial_oracle import SerialCQDAGOracle


@dataclass(frozen=True, slots=True)
class CQDAGJobSpec:
    """Validated CQDAGPCFG job payload consumed by the tracker and nodes."""

    limit: int
    serial_digest: str
    model_fingerprint: str | None
    payload: Mapping[str, Any]

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "CQDAGJobSpec":
        try:
            limit = int(payload["limit"])
        except KeyError as exc:
            raise ValueError("CQDAG job spec requires limit") from exc
        if limit <= 0:
            raise ValueError("CQDAG job spec limit must be positive")
        try:
            serial_digest = str(payload["serial_digest"])
        except KeyError as exc:
            raise ValueError("CQDAG job spec requires serial_digest") from exc
        if not serial_digest:
            raise ValueError("CQDAG job spec serial_digest cannot be empty")
        model_fingerprint = payload.get("model_fingerprint")
        return cls(
            limit=limit,
            serial_digest=serial_digest,
            model_fingerprint=(
                None if model_fingerprint is None else str(model_fingerprint)
            ),
            payload=dict(payload),
        )

    @classmethod
    def read(cls, path: Path) -> "CQDAGJobSpec":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("CQDAG job spec must be a JSON object")
        return cls.from_mapping(payload)

    def with_model_fingerprint(self, model_fingerprint: str) -> "CQDAGJobSpec":
        payload = dict(self.payload)
        payload["model_fingerprint"] = model_fingerprint
        return CQDAGJobSpec(
            limit=self.limit,
            serial_digest=self.serial_digest,
            model_fingerprint=model_fingerprint,
            payload=payload,
        )


def compute_serial_digest(model, *, limit: int) -> str:
    if limit <= 0:
        raise ValueError("limit must be positive")
    return stable_digest(SerialCQDAGOracle(model).iter_records(limit))


__all__ = ["CQDAGJobSpec", "compute_serial_digest"]
