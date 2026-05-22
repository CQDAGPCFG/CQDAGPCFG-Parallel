#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from shared.common import ensure_project_paths

ensure_project_paths()

from cqdagpcfg_parallel.adapters.cqdagpcfg import prepare_cqdag_job_spec


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare a CQDAGPCFG job spec from an existing model.",
    )
    parser.add_argument(
        "--source-model-path",
        type=Path,
        required=True,
        help="Already trained CQDAGPCFG JSON model used by tracker and workers.",
    )
    parser.add_argument("--job-spec-path", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--hash-algorithm", choices=("sha256", "sha1", "md5"), default="sha256")
    parser.add_argument("--target-rank", type=int, action="append", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    job_spec = prepare_cqdag_job_spec(
        args.source_model_path,
        limit=args.limit,
        hash_algorithm=args.hash_algorithm,
        target_ranks=args.target_rank,
        progress_callback=print_progress,
    )
    job_spec.write(args.job_spec_path)

    print("prepared CQDAGPCFG job spec")
    print(f"  model        : {args.source_model_path}")
    print(f"  fingerprint  : {job_spec.model_fingerprint}")
    print(f"  job spec     : {args.job_spec_path}")
    print(f"  limit        : {job_spec.limit}")
    print(f"  serial digest: {job_spec.serial_digest}")
    print("  target hashes:")
    for target in job_spec.payload["targets"]:
        print(f"    rank={target['rank']} guess={target['guess']} hash={target['hash']}")


def print_progress(produced: int, limit: int) -> None:
    print(f"  prepared serial records: {produced}/{limit}", flush=True)


if __name__ == "__main__":
    main()
