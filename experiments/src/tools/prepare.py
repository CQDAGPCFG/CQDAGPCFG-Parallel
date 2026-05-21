#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from shared.common import (
    digest_guess,
    ensure_project_paths,
    load_training_passwords,
    normalize_target_ranks,
    write_json,
)

ensure_project_paths()

from CQDAGPCFG import load_model, save_model
from CQDAGPCFG.training import PCFGTrainer

from cqdagpcfg_parallel.adapters.cqdagpcfg import SerialCQDAGOracle
from cqdagpcfg_parallel.storage import ModelManifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare CQDAGPCFG E2E model and target hashes.")
    parser.add_argument("--train-file", type=Path, default=None)
    parser.add_argument(
        "--source-model-path",
        type=Path,
        default=None,
        help="Use an already trained CQDAGPCFG JSON model instead of training a toy model.",
    )
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--targets-path", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--hash-algorithm", choices=("sha256", "sha1", "md5"), default="sha256")
    parser.add_argument("--target-rank", type=int, action="append", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.limit <= 0:
        raise SystemExit("--limit must be positive")

    if args.source_model_path is not None:
        if args.train_file is not None:
            raise SystemExit("--train-file cannot be used with --source-model-path")
        model = load_model(args.source_model_path)
        training_samples = None
        model_source = str(args.source_model_path)
    else:
        passwords = load_training_passwords(args.train_file)
        model = PCFGTrainer().train(passwords)
        training_samples = len(passwords)
        model_source = "trained-from-input"

    args.model_path.parent.mkdir(parents=True, exist_ok=True)
    save_model(model, args.model_path)
    runtime_model = load_model(args.model_path)
    model_manifest = ModelManifest.from_json_payload(
        args.model_path.read_bytes(),
        model_id="cqdagpcfg-e2e-model",
        artifact_uri=str(args.model_path),
    )

    baseline = SerialCQDAGOracle(runtime_model).run(args.limit)
    target_ranks = normalize_target_ranks(args.target_rank, len(baseline.outputs))
    targets = [
        {
            "rank": rank,
            "guess": baseline.outputs[rank].guess,
            "hash": digest_guess(baseline.outputs[rank].guess, algorithm=args.hash_algorithm),
        }
        for rank in target_ranks
    ]
    write_json(
        args.targets_path,
        {
            "algorithm": args.hash_algorithm,
            "limit": args.limit,
            "model_source": model_source,
            "model_fingerprint": model_manifest.model_fingerprint,
            "training_samples": training_samples,
            "serial_digest": baseline.digest,
            "targets": targets,
        },
    )

    print("prepared CQDAGPCFG E2E artifacts")
    print(f"  model        : {args.model_path}")
    print(f"  model source : {model_source}")
    print(f"  fingerprint  : {model_manifest.model_fingerprint}")
    print(f"  targets      : {args.targets_path}")
    print(f"  limit        : {args.limit}")
    print(f"  serial digest: {baseline.digest}")
    print("  targets      :")
    for target in targets:
        print(f"    rank={target['rank']} guess={target['guess']} hash={target['hash']}")


if __name__ == "__main__":
    main()
