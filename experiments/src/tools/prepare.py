#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from shared.common import (
    digest_guess,
    ensure_project_paths,
    normalize_target_ranks,
    write_json,
)

ensure_project_paths()

from CQDAGPCFG import load_model

from cqdagpcfg_parallel.adapters.cqdagpcfg import SerialCQDAGOracle
from cqdagpcfg_parallel.protocol import stable_record_string
from cqdagpcfg_parallel.storage import ModelManifest


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
    if args.limit <= 0:
        raise SystemExit("--limit must be positive")

    model = load_model(args.source_model_path)
    model_manifest = ModelManifest.from_json_payload(
        args.source_model_path.read_bytes(),
        model_id="cqdagpcfg-e2e-model",
        artifact_uri=str(args.source_model_path),
    )

    target_ranks = normalize_target_ranks(args.target_rank, args.limit)
    baseline_digest, targets = serial_digest_and_targets(
        model,
        limit=args.limit,
        target_ranks=target_ranks,
        hash_algorithm=args.hash_algorithm,
    )
    write_json(
        args.job_spec_path,
        {
            "algorithm": args.hash_algorithm,
            "limit": args.limit,
            "model_source": str(args.source_model_path),
            "model_fingerprint": model_manifest.model_fingerprint,
            "serial_digest": baseline_digest,
            "targets": targets,
        },
    )

    print("prepared CQDAGPCFG job spec")
    print(f"  model        : {args.source_model_path}")
    print(f"  fingerprint  : {model_manifest.model_fingerprint}")
    print(f"  job spec     : {args.job_spec_path}")
    print(f"  limit        : {args.limit}")
    print(f"  serial digest: {baseline_digest}")
    print("  target hashes:")
    for target in targets:
        print(f"    rank={target['rank']} guess={target['guess']} hash={target['hash']}")


def serial_digest_and_targets(
    model,
    *,
    limit: int,
    target_ranks: tuple[int, ...],
    hash_algorithm: str,
) -> tuple[str, list[dict[str, object]]]:
    digest = hashlib.sha256()
    target_rank_set = set(target_ranks)
    target_by_rank = {}
    oracle = SerialCQDAGOracle(model, prefer_cpp=True)
    for rank, record in enumerate(oracle.iter_records(limit)):
        digest.update(stable_record_string(record).encode("utf-8"))
        digest.update(b"\n")
        if rank in target_rank_set:
            target_by_rank[rank] = {
                "rank": rank,
                "guess": record.guess,
                "hash": digest_guess(record.guess, algorithm=hash_algorithm),
            }
        if (rank + 1) % 1_000_000 == 0:
            print(f"  prepared serial records: {rank + 1}/{limit}", flush=True)
    missing = sorted(target_rank_set - set(target_by_rank))
    if missing:
        raise RuntimeError(
            f"target ranks were not produced by serial oracle: {missing}",
        )
    return digest.hexdigest(), [target_by_rank[rank] for rank in target_ranks]


if __name__ == "__main__":
    main()
