#!/usr/bin/env python3
from __future__ import annotations

import sys
import logging
from pathlib import Path

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from shared.common import ensure_project_paths
from shared.tracker_args import cqdag_tracker_config_from_args

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


LOGGER = logging.getLogger("cqdagpcfg.experiment.tracker")


@cqdagpcfg_tracker(cqdag_tracker_config_from_args())
class ExperimentTracker:
    """Default CQDAGPCFG tracker used by the experiment."""

    def on_start(self, job: CQDAGPCFGTrackerJob) -> None:
        log_event(
            LOGGER,
            logging.INFO,
            "experiment.tracker_start",
            model=job.model_path,
            limit=job.limit,
            target_hashes=job.target_count,
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
