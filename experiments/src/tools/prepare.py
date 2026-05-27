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

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    prepare_cqdag_cracking_job_spec,
    prepare_cqdag_job_spec,
)


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
    parser.add_argument(
        "--no-targets",
        action="store_true",
        help=(
            "Do not add default rank targets. Use with --decoy-hash for "
            "throughput-only no-hit cracking benchmarks."
        ),
    )
    parser.add_argument(
        "--decoy-hash",
        action="append",
        default=None,
        help=(
            "Hashcat-only decoy hash included in the hashlist but not required "
            "as a hit. May be repeated."
        ),
    )
    parser.add_argument(
        "--cracking-profile",
        action="store_true",
        help="Skip serial oracle digest preparation for throughput-oriented cracking runs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    target_ranks = () if args.no_targets else args.target_rank
    if args.cracking_profile:
        job_spec = prepare_cqdag_cracking_job_spec(
            args.source_model_path,
            limit=args.limit,
            hash_algorithm=args.hash_algorithm,
            target_ranks=target_ranks,
            decoy_hashes=args.decoy_hash,
        )
    else:
        job_spec = prepare_cqdag_job_spec(
            args.source_model_path,
            limit=args.limit,
            hash_algorithm=args.hash_algorithm,
            target_ranks=target_ranks,
            decoy_hashes=args.decoy_hash,
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
    if job_spec.payload.get("decoy_hashes"):
        print("  decoy hashes:")
        for digest in job_spec.payload["decoy_hashes"]:
            print(f"    {digest}")


def print_progress(produced: int, limit: int) -> None:
    print(f"  prepared serial records: {produced}/{limit}", flush=True)


if __name__ == "__main__":
    main()
