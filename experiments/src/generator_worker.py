#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from common import ensure_project_paths, read_json, write_json

ensure_project_paths()

from CQDAGPCFG import load_model

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGBlockGraphAdapter,
    CQDAGStructureRecordSource,
)
from cqdagpcfg_parallel.distributed import DistributedProtocolWorker
from cqdagpcfg_parallel.protocol import WorkerId
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a CQDAGPCFG E2E generator worker.")
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--targets-path", type=Path, required=True)
    parser.add_argument("--control-connect", default="cqpcfg://127.0.0.1:5555")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--demand-window", type=int, default=8)
    parser.add_argument("--work-delay-seconds", type=float, default=0.0)
    parser.add_argument("--retire-file", type=Path, default=None)
    parser.add_argument("--metrics-path", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    targets = read_json(args.targets_path)
    model = load_model(args.model_path)
    adapter = CQDAGBlockGraphAdapter(model)
    source = CQDAGStructureRecordSource(
        model,
        max_records_per_structure=int(targets["limit"]) + args.demand_window,
        adapter=adapter,
    )
    worker = DistributedProtocolWorker(
        worker_id=WorkerId(args.worker_id),
        endpoint=ZmqEndpoint.from_uri(args.control_connect, bind=False),
        source=source,
        work_delay_seconds=args.work_delay_seconds,
        model_fingerprint=targets.get("model_fingerprint"),
        should_retire=(
            (lambda: is_retired(args.retire_file, args.worker_id))
            if args.retire_file is not None
            else None
        ),
    )
    stats = worker.run()

    if args.metrics_path is not None:
        write_json(
            args.metrics_path,
            {
                "role": "generator",
                "worker_id": stats.worker_id,
                "completed_items": stats.completed_items,
                "completed_records": stats.completed_records,
                "waits": stats.waits,
                "source_cached_records": stats.source_cached_records,
                "source_peak_cached_records": stats.source_peak_cached_records,
                "source_reclaimed_records": stats.source_reclaimed_records,
                "source_dag_repository_active_units": stats.source_dag_repository_active_units,
                "source_dag_stream_active_units": stats.source_dag_stream_active_units,
                "final": True,
            },
        )

    print("generator worker completed")
    print(f"  worker id        : {stats.worker_id}")
    print(f"  completed items  : {stats.completed_items}")
    print(f"  completed records: {stats.completed_records}")
    print(f"  waits            : {stats.waits}")
    print(f"  source cached    : {stats.source_cached_records}")
    print(f"  source peak cache: {stats.source_peak_cached_records}")
    print(f"  source reclaimed : {stats.source_reclaimed_records}")


def is_retired(path: Path, worker_id: str) -> bool:
    if not path.exists():
        return False
    payload = read_json(path)
    retired = payload.get("retired_generators", ())
    return worker_id in set(str(value) for value in retired)


if __name__ == "__main__":
    main()
