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

from cqdagpcfg_parallel.adapters.cqdagpcfg import CQDAGModelCandidateSpace


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize one CQDAGPCFG candidate space as both a CQDAG model "
            "and an external pcfg-manager rule directory."
        ),
    )
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--name", default="cqdagpcfg-model")
    parser.add_argument(
        "--no-copy-model",
        action="store_true",
        help="Reference the source model in the manifest instead of copying it.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifacts = CQDAGModelCandidateSpace(
        args.model_path,
        name=args.name,
        copy_model=not args.no_copy_model,
    ).materialize(args.output_dir)
    print("materialized candidate space")
    print(f"  name        : {artifacts.name}")
    print(f"  model       : {artifacts.model_path}")
    print(f"  pcfg rules  : {artifacts.pcfg_rules_dir}")
    print(f"  manifest    : {artifacts.manifest_path}")
    print(f"  total points: {artifacts.total_points}")


if __name__ == "__main__":
    main()
