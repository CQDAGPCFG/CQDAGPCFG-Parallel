from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence

if TYPE_CHECKING:
    from cqdagpcfg_parallel.distributed import NodeAgentStats


class NodeAgentJsonReporter:
    """Write framework node metrics and user consumer outputs as JSON.

    The reporter does not interpret verifier-specific semantics. It only
    persists consumer results returned by the user-defined consumer.
    """

    def __init__(
        self,
        *,
        metrics_path: Path,
        outputs_path: Path,
        limit: int,
        outputs_provider: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
    ) -> None:
        self.metrics_path = metrics_path
        self.outputs_path = outputs_path
        self.limit = limit
        self.outputs_provider = outputs_provider or _empty_outputs
        self.report_write_count = 0
        self.report_write_seconds = 0.0

    def write(self, stats: "NodeAgentStats") -> None:
        outputs = tuple(dict(output) for output in self.outputs_provider())
        started_at = perf_counter()
        _write_json(
            self.metrics_path,
            {
                "role": "node_agent",
                **asdict(stats),
                "consumer_outputs": len(outputs),
                "report_write_count": self.report_write_count,
                "report_write_seconds": self.report_write_seconds,
            },
        )
        metrics_write_seconds = perf_counter() - started_at

        started_at = perf_counter()
        _write_json(
            self.outputs_path,
            {
                "consumer_id": stats.node_id,
                "limit": self.limit,
                "consumed_batches": stats.consumed_batches,
                "consumed_candidates": stats.consumed_candidates,
                "consumer_outputs": outputs,
                "outputs": outputs,
            },
        )
        self.report_write_seconds += metrics_write_seconds + (perf_counter() - started_at)
        self.report_write_count += 2


def _empty_outputs() -> Sequence[Mapping[str, Any]]:
    return ()


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


__all__ = ["NodeAgentJsonReporter"]
