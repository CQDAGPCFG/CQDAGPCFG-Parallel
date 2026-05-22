#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
import sys
from collections import deque
from pathlib import Path

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from shared.common import ensure_project_paths

ensure_project_paths()

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGPCFGBatchRetryEvent,
    CQDAGPCFGCheckpointEvent,
    CQDAGPCFGMemorySnapshot,
    CQDAGPCFGNodeEvent,
    CQDAGPCFGRoleChangeEvent,
    CQDAGPCFGTrackerError,
    CQDAGPCFGTrackerJob,
    CQDAGPCFGTrackerSummary,
    cqdagpcfg_tracker,
)
from cqdagpcfg_parallel.framework_logging import log_event
from cqdagpcfg_parallel.runtime import CandidateBatch


LOGGER = logging.getLogger("cqdagpcfg.experiment.tracker")


def _print_tracker_help() -> None:
    print("usage: CQPCFG_MODEL_PATH=model.json CQPCFG_JOB_SPEC_PATH=job-spec.json python experiments/cqpcfg_experiment.py tracker")
    print()
    print("tracker configuration is read from CQPCFG_* environment variables.")


if len(sys.argv) > 1:
    if sys.argv[1] in {"-h", "--help"}:
        _print_tracker_help()
        raise SystemExit(0)
    raise SystemExit(
        "tracker service is configured with CQPCFG_* environment variables; "
        "pass no CLI arguments",
    )


def _env_path(name: str) -> Path | None:
    value = os.environ.get(name)
    return None if value is None or value == "" else Path(value)


def _env_required_path(name: str) -> Path:
    value = _env_path(name)
    if value is None:
        raise SystemExit(f"tracker requires {name}")
    return value


def _env_int(name: str, default: int | None = None) -> int | None:
    value = os.environ.get(name)
    return default if value is None or value == "" else int(value)


@cqdagpcfg_tracker(
    env_prefix="CQPCFG",
    model_path=_env_required_path("CQPCFG_MODEL_PATH"),
    job_spec_path=_env_required_path("CQPCFG_JOB_SPEC_PATH"),
    bind=os.environ.get("CQPCFG_BIND") or None,
    advertise_host=os.environ.get("CQPCFG_ADVERTISE_HOST", "127.0.0.1"),
    total_nodes=_env_int("CQPCFG_TOTAL_NODES"),
    initial_generators=_env_int("CQPCFG_INITIAL_GENERATORS"),
    initial_consumers=_env_int("CQPCFG_INITIAL_CONSUMERS"),
    source_mode=os.environ.get("CQPCFG_SOURCE_MODE", "root"),
)
class ExperimentTracker:
    """Default CQDAGPCFG tracker used by the experiment."""

    def __init__(self) -> None:
        self.candidate_sample_size = _env_int("CQPCFG_CANDIDATE_SAMPLE_SIZE", 0) or 0
        self.candidate_sample_max_length = _env_int("CQPCFG_CANDIDATE_SAMPLE_MAX_LENGTH", 64) or 64
        if self.candidate_sample_size < 0:
            raise ValueError("CQPCFG_CANDIDATE_SAMPLE_SIZE cannot be negative")
        if self.candidate_sample_max_length <= 0:
            raise ValueError("CQPCFG_CANDIDATE_SAMPLE_MAX_LENGTH must be positive")
        self.candidate_samples: deque[dict[str, object]] = deque(
            maxlen=max(1, self.candidate_sample_size),
        )

    def on_start(self, job: CQDAGPCFGTrackerJob) -> None:
        log_event(
            LOGGER,
            logging.INFO,
            "experiment.tracker_start",
            model=job.model_path,
            limit=job.limit,
            job_payload_items=job.job_payload_items,
            source_mode=job.source_mode,
        )

    def on_node_join(self, node: CQDAGPCFGNodeEvent) -> None:
        log_event(LOGGER, logging.INFO, "experiment.node_join", node_id=node.node_id, role=node.role)

    def on_node_leave(self, node: CQDAGPCFGNodeEvent) -> None:
        log_event(
            LOGGER,
            logging.INFO,
            "experiment.node_leave",
            node_id=node.node_id,
            reason=node.reason,
        )

    def on_role_change(self, event: CQDAGPCFGRoleChangeEvent) -> None:
        log_event(
            LOGGER,
            logging.INFO,
            "experiment.role_change",
            node_id=event.node_id,
            previous_role=event.previous_role,
            new_role=event.new_role,
        )

    def on_memory_snapshot(self, snapshot: CQDAGPCFGMemorySnapshot) -> None:
        log_event(
            LOGGER,
            logging.INFO,
            "experiment.memory_snapshot",
            resident_records=snapshot.resident_records,
            peak_resident_records=snapshot.peak_resident_records,
            reclaimed_records=snapshot.reclaimed_records,
            pending_batches=snapshot.pending_batches,
        )

    def on_candidate_batch(self, batch: CandidateBatch) -> None:
        if self.candidate_sample_size == 0:
            return
        for offset, record in enumerate(batch.records):
            self.candidate_samples.append(
                {
                    "rank": batch.start_rank + offset,
                    "batch_id": batch.batch_id,
                    "guess": self._display_guess(record.guess),
                    "prob": record.prob,
                    "structure_index": record.structure_index,
                    "structure_name": record.structure_name,
                },
            )

    def metrics_snapshot(self) -> dict[str, object]:
        if self.candidate_sample_size == 0:
            return {}
        return {"candidate_samples": tuple(self.candidate_samples)}

    def _display_guess(self, guess: str) -> str:
        if len(guess) <= self.candidate_sample_max_length:
            return guess
        if self.candidate_sample_max_length <= 3:
            return "." * self.candidate_sample_max_length
        return f"{guess[: self.candidate_sample_max_length - 3]}..."

    def on_checkpoint(self, checkpoint: CQDAGPCFGCheckpointEvent) -> None:
        log_event(
            LOGGER,
            logging.INFO,
            "experiment.checkpoint",
            emitted_count=checkpoint.emitted_count,
        )

    def on_batch_retry(self, event: CQDAGPCFGBatchRetryEvent) -> None:
        log_event(
            LOGGER,
            logging.WARNING,
            "experiment.batch_retry",
            batch_id=event.batch_id,
            reason=event.reason,
            attempts=event.attempts,
        )

    def on_error(self, error: CQDAGPCFGTrackerError) -> None:
        log_event(
            LOGGER,
            logging.ERROR,
            "experiment.tracker_error",
            stage=error.stage,
            error_type=error.error_type,
            message=error.message,
        )

    def on_complete(self, summary: CQDAGPCFGTrackerSummary) -> None:
        log_event(
            LOGGER,
            logging.INFO,
            "experiment.tracker_complete",
            digest_match=summary.digest == summary.serial_digest,
            peak_resident_records=summary.peak_resident_records,
            emitted_records=summary.emitted_records,
            reclaimed_records=summary.reclaimed_records,
        )


def main() -> None:
    ExperimentTracker.run()


if __name__ == "__main__":
    main()
