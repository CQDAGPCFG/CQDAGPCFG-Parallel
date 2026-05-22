from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping

from CQDAGPCFG import GuessRecord, load_model
from CQDAGPCFG.cpp_backend import cpp_backend_available

from cqdagpcfg_parallel.protocol import stable_digest, stable_record_string
from cqdagpcfg_parallel.storage import ModelManifest

from .block_graph import CppFileCQDAGRecordSource, ROOT_NODE_ID
from .serial_oracle import SerialCQDAGOracle


DEFAULT_MODEL_ID = "cqdagpcfg-e2e-model"
DEFAULT_SERIAL_CHUNK_SIZE = 4096


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

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )


def prepare_cqdag_job_spec(
    model_path: Path,
    *,
    limit: int,
    model_id: str = DEFAULT_MODEL_ID,
    hash_algorithm: str = "sha256",
    target_ranks: Iterable[int] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> CQDAGJobSpec:
    """Build a tracker job spec without materializing the serial prefix."""

    model_path = Path(model_path)
    if limit <= 0:
        raise ValueError("limit must be positive")
    normalized_target_ranks = normalize_target_ranks(target_ranks, limit)
    manifest = ModelManifest.from_json_payload(
        model_path.read_bytes(),
        model_id=model_id,
        artifact_uri=str(model_path),
    )
    serial_digest, targets = serial_digest_and_targets(
        model_path,
        limit=limit,
        target_ranks=normalized_target_ranks,
        hash_algorithm=hash_algorithm,
        progress_callback=progress_callback,
    )
    return CQDAGJobSpec.from_mapping(
        {
            "algorithm": hash_algorithm,
            "limit": limit,
            "model_source": str(model_path),
            "model_fingerprint": manifest.model_fingerprint,
            "serial_digest": serial_digest,
            "targets": targets,
        },
    )


def serial_digest_and_targets(
    model_path: Path,
    *,
    limit: int,
    target_ranks: tuple[int, ...],
    hash_algorithm: str,
    progress_callback: Callable[[int, int], None] | None = None,
) -> tuple[str, list[dict[str, object]]]:
    digest = hashlib.sha256()
    target_rank_set = set(target_ranks)
    target_by_rank: dict[int, dict[str, object]] = {}
    for rank, record in enumerate(iter_serial_records(model_path, limit)):
        digest.update(stable_record_string(record).encode("utf-8"))
        digest.update(b"\n")
        if rank in target_rank_set:
            target_by_rank[rank] = {
                "rank": rank,
                "guess": record.guess,
                "hash": digest_guess(record.guess, algorithm=hash_algorithm),
            }
        produced = rank + 1
        if progress_callback is not None and produced % 1_000_000 == 0:
            progress_callback(produced, limit)

    missing = sorted(target_rank_set - set(target_by_rank))
    if missing:
        raise RuntimeError(f"target ranks were not produced by serial oracle: {missing}")
    return digest.hexdigest(), [target_by_rank[rank] for rank in target_ranks]


def iter_serial_records(model_path: Path, limit: int) -> Iterator[GuessRecord]:
    if cpp_backend_available():
        source = CppFileCQDAGRecordSource(model_path, max_records=limit)
        for start in range(0, limit, DEFAULT_SERIAL_CHUNK_SIZE):
            end = min(limit, start + DEFAULT_SERIAL_CHUNK_SIZE)
            records = tuple(source.read_range(ROOT_NODE_ID, start, end))
            if not records:
                return
            yield from records
            source.reclaim_before(ROOT_NODE_ID, end)
        return

    model = load_model(model_path)
    yield from SerialCQDAGOracle(model, prefer_cpp=False).iter_records(limit)


def normalize_target_ranks(
    raw_ranks: Iterable[int] | None,
    output_count: int,
) -> tuple[int, ...]:
    if output_count <= 0:
        raise ValueError("output_count must be positive")
    ranks = tuple(raw_ranks) if raw_ranks is not None else (0, min(7, output_count - 1))
    normalized = tuple(dict.fromkeys(int(rank) for rank in ranks))
    for rank in normalized:
        if rank < 0 or rank >= output_count:
            raise ValueError(f"target rank out of generated prefix: {rank}")
    return normalized


def digest_guess(guess: str, *, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    digest.update(guess.encode("utf-8"))
    return digest.hexdigest()


def compute_serial_digest(model, *, limit: int) -> str:
    if limit <= 0:
        raise ValueError("limit must be positive")
    return stable_digest(SerialCQDAGOracle(model).iter_records(limit))


__all__ = [
    "CQDAGJobSpec",
    "compute_serial_digest",
    "digest_guess",
    "iter_serial_records",
    "normalize_target_ranks",
    "prepare_cqdag_job_spec",
    "serial_digest_and_targets",
]
