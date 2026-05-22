#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from shared.common import ensure_project_paths, load_training_passwords

ensure_project_paths()

from CQDAGPCFG import save_model
from CQDAGPCFG.training import PCFGTrainer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a small CQDAGPCFG model for local experiment scenarios.",
    )
    parser.add_argument("--train-file", type=Path, default=None)
    parser.add_argument("--model-path", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    passwords = load_training_passwords(args.train_file)
    model = PCFGTrainer().train(passwords)
    args.model_path.parent.mkdir(parents=True, exist_ok=True)
    save_model(model, args.model_path)

    print("trained CQDAGPCFG toy model")
    print(f"  model   : {args.model_path}")
    print(f"  samples : {len(passwords)}")


if __name__ == "__main__":
    main()
