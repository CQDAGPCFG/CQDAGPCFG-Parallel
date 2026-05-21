from __future__ import annotations

from typing import Iterable

from CQDAGPCFG import GuessRecord

from .candidate_batch import CandidateBatch, guess_payload_bytes


def make_candidate_batches(
    records: Iterable[GuessRecord],
    *,
    batch_size: int,
    max_batch_payload_bytes: int,
) -> Iterable[CandidateBatch]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_batch_payload_bytes <= 0:
        raise ValueError("max_batch_payload_bytes must be positive")

    batch_id = 0
    start_rank = 0
    current: list[GuessRecord] = []
    current_payload = 0

    for record in records:
        record_bytes = guess_payload_bytes(record.guess)
        if record_bytes > max_batch_payload_bytes:
            raise ValueError("single guess exceeds max_batch_payload_bytes")

        would_exceed_count = len(current) >= batch_size
        would_exceed_payload = current_payload + record_bytes > max_batch_payload_bytes
        if current and (would_exceed_count or would_exceed_payload):
            yield CandidateBatch.from_records(
                batch_id=batch_id,
                start_rank=start_rank,
                records=current,
            )
            batch_id += 1
            start_rank += len(current)
            current = []
            current_payload = 0

        current.append(record)
        current_payload += record_bytes

    if current:
        yield CandidateBatch.from_records(
            batch_id=batch_id,
            start_rank=start_rank,
            records=current,
        )


__all__ = [
    "make_candidate_batches",
]
