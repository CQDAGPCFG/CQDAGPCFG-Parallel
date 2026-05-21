from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from CQDAGPCFG import GuessRecord


def guess_payload_bytes(guess: str) -> int:
    return len(guess.encode("utf-8")) + 1


@dataclass(frozen=True, slots=True)
class CandidateBatch:
    batch_id: int
    start_rank: int
    records: tuple[GuessRecord, ...]
    payload_bytes: int

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

    @property
    def end_rank(self) -> int:
        return self.start_rank + len(self.records)

    @property
    def guesses(self) -> tuple[str, ...]:
        return tuple(record.guess for record in self.records)


__all__ = [
    "CandidateBatch",
    "guess_payload_bytes",
]
