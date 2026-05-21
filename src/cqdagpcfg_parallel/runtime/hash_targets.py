from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from time import monotonic, perf_counter, sleep
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

if TYPE_CHECKING:
    from cqdagpcfg_parallel.distributed import NodeAgentStats

from cqdagpcfg_parallel.runtime import CandidateBatch


def digest_guess(guess: str, *, algorithm: str = "sha256") -> str:
    digest = hashlib.new(algorithm)
    digest.update(guess.encode("utf-8"))
    return digest.hexdigest()


class HashTargetSet:
    """Normalized target hashes provided by the tracker/job context.

    The framework owns the target hash table, while the consumer chooses how a
    guess is transformed into a digest.
    """

    def __init__(self, targets: Mapping[str, Any]) -> None:
        self.default_algorithm = str(targets["algorithm"])
        self.target_by_hash: dict[str, list[dict[str, Any]]] = {}
        for target in targets["targets"]:
            self.target_by_hash.setdefault(str(target["hash"]), []).append(dict(target))

    @property
    def algorithm(self) -> str:
        return self.default_algorithm

    def digest(self, guess: str, *, algorithm: str | None = None) -> str:
        return digest_guess(guess, algorithm=algorithm or self.default_algorithm)

    def matches_for_digest(self, digest: str) -> list[dict[str, Any]]:
        return [
            {
                "target_rank": int(target["rank"]),
                "hash": digest,
            }
            for target in self.target_by_hash.get(digest, ())
        ]

    def match(self, guess: str, *, algorithm: str | None = None) -> list[dict[str, Any]]:
        return self.matches_for_digest(self.digest(guess, algorithm=algorithm))


class HashTargetConsumer:
    """Small hash-verification consumer for protocol tests and examples."""

    def __init__(
        self,
        *,
        node_id: str,
        targets: Mapping[str, Any],
        hash_delay_seconds: float = 0.0,
        started_at: float | None = None,
    ) -> None:
        self.node_id = node_id
        self.started_at = monotonic() if started_at is None else started_at
        self.target_set = HashTargetSet(targets)
        self.algorithm = self.target_set.default_algorithm
        self.hash_delay_seconds = hash_delay_seconds
        self.hits: list[dict[str, Any]] = []

    def consume(self, batch: CandidateBatch) -> None:
        for offset, record in enumerate(batch.records):
            if self.hash_delay_seconds:
                sleep(self.hash_delay_seconds)
            for target in self.target_set.match(record.guess):
                self.hits.append(
                    {
                        "rank": batch.start_rank + offset,
                        "target_rank": int(target["target_rank"]),
                        "batch_id": batch.batch_id,
                        "guess": record.guess,
                        "hash": target["hash"],
                        "node_id": self.node_id,
                        "elapsed_seconds": monotonic() - self.started_at,
                    }
                )


class NodeAgentJsonReporter:
    """Write node metrics and hash hits in the experiment JSON format."""

    def __init__(
        self,
        *,
        metrics_path: Path,
        hits_path: Path,
        algorithm: str,
        limit: int,
        consumer: HashTargetConsumer | None = None,
        hits_provider: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
    ) -> None:
        self.metrics_path = metrics_path
        self.hits_path = hits_path
        self.algorithm = algorithm
        self.limit = limit
        if hits_provider is None and consumer is not None:
            def consumer_hits() -> Sequence[Mapping[str, Any]]:
                return consumer.hits

            hits_provider = consumer_hits
        if hits_provider is None:
            def no_hits() -> Sequence[Mapping[str, Any]]:
                return ()

            hits_provider = no_hits
        self.hits_provider = hits_provider
        self.report_write_count = 0
        self.report_write_seconds = 0.0

    def write(self, stats: "NodeAgentStats") -> None:
        hits = tuple(dict(hit) for hit in self.hits_provider())
        started_at = perf_counter()
        _write_json(
            self.metrics_path,
            {
                "role": "node_agent",
                **asdict(stats),
                "hits": len(hits),
                "role_file_reads": 0,
                "role_file_read_seconds": 0.0,
                "report_write_count": self.report_write_count,
                "report_write_seconds": self.report_write_seconds,
            },
        )
        metrics_write_seconds = perf_counter() - started_at

        started_at = perf_counter()
        _write_json(
            self.hits_path,
            {
                "consumer_id": stats.node_id,
                "algorithm": self.algorithm,
                "limit": self.limit,
                "consumed_batches": stats.consumed_batches,
                "consumed_candidates": stats.consumed_candidates,
                "hits": hits,
            },
        )
        self.report_write_seconds += metrics_write_seconds + (perf_counter() - started_at)
        self.report_write_count += 2


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


__all__ = [
    "HashTargetSet",
    "HashTargetConsumer",
    "NodeAgentJsonReporter",
    "digest_guess",
]
