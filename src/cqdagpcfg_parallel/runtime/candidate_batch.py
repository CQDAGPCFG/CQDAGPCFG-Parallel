from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence
from urllib.parse import unquote, urlparse

from CQDAGPCFG import GuessRecord


UNCHECKED_ARTIFACT_SHA256 = "unchecked"


def guess_payload_bytes(guess: str) -> int:
    return len(guess.encode("utf-8")) + 1


@dataclass(frozen=True, slots=True)
class CandidateBatch:
    batch_id: int
    start_rank: int
    records: tuple[GuessRecord, ...]
    payload_bytes: int
    artifact_uri: str | None = None
    artifact_sha256: str | None = None
    artifact_format: str | None = None
    artifact_bytes: int = 0
    artifact_record_count: int = 0

    @classmethod
    def from_records(
        cls,
        *,
        batch_id: int,
        start_rank: int,
        records: Sequence[GuessRecord],
    ) -> "CandidateBatch":
        if not records:
            raise ValueError("candidate batch cannot be empty")
        payload_bytes = sum(guess_payload_bytes(record.guess) for record in records)
        return cls(
            batch_id=batch_id,
            start_rank=start_rank,
            records=tuple(records),
            payload_bytes=payload_bytes,
        )

    @classmethod
    def from_artifact(
        cls,
        *,
        batch_id: int,
        start_rank: int,
        record_count: int,
        payload_bytes: int,
        artifact_uri: str,
        artifact_sha256: str,
        artifact_bytes: int,
        artifact_format: str = "guess-lines-v1",
    ) -> "CandidateBatch":
        if record_count <= 0:
            raise ValueError("candidate artifact batch record_count must be positive")
        if payload_bytes < 0:
            raise ValueError("candidate artifact batch payload_bytes cannot be negative")
        if artifact_bytes < 0:
            raise ValueError("candidate artifact batch artifact_bytes cannot be negative")
        if not artifact_uri:
            raise ValueError("candidate artifact batch requires artifact_uri")
        if not artifact_sha256:
            raise ValueError("candidate artifact batch requires artifact_sha256")
        return cls(
            batch_id=batch_id,
            start_rank=start_rank,
            records=(),
            payload_bytes=payload_bytes,
            artifact_uri=artifact_uri,
            artifact_sha256=artifact_sha256,
            artifact_format=artifact_format,
            artifact_bytes=artifact_bytes,
            artifact_record_count=record_count,
        )

    @property
    def end_rank(self) -> int:
        return self.start_rank + self.record_count

    @property
    def record_count(self) -> int:
        return len(self.records) if self.records else self.artifact_record_count

    @property
    def is_artifact(self) -> bool:
        return self.artifact_uri is not None

    @property
    def guesses(self) -> tuple[str, ...]:
        return tuple(self.iter_guesses())

    def iter_guesses(self) -> Iterator[str]:
        if not self.is_artifact:
            for record in self.records:
                yield record.guess
            return
        if self.artifact_format not in {None, "guess-lines-v1"}:
            raise RuntimeError(
                f"candidate artifact format is not iterable: {self.artifact_format}"
            )
        path = _artifact_path(self.artifact_uri or "")
        if (
            self.artifact_sha256 is not None
            and self.artifact_sha256 != UNCHECKED_ARTIFACT_SHA256
        ):
            actual = _file_sha256(path)
            if actual != self.artifact_sha256:
                raise RuntimeError(
                    "candidate artifact sha256 mismatch: "
                    f"{actual} != {self.artifact_sha256}"
                )
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                yield line.rstrip("\n")


def _artifact_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme in {"", "file"}:
        return Path(unquote(parsed.path if parsed.scheme else uri))
    raise RuntimeError(f"unsupported candidate artifact URI scheme: {parsed.scheme}")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


__all__ = [
    "CandidateBatch",
    "UNCHECKED_ARTIFACT_SHA256",
    "guess_payload_bytes",
]
