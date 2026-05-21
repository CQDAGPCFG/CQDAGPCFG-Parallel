#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path
from time import sleep

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from shared.common import digest_guess, ensure_project_paths

ensure_project_paths()

from cqdagpcfg_parallel import cqdagpcfg


@cqdagpcfg.remote(
    env_prefix="CQPCFG",
    connect=os.environ.get("CQPCFG_CONNECT", "cqpcfg://127.0.0.1:5555"),
    num_cpus=float(os.environ.get("CQPCFG_RESOURCE_CPUS", "1")),
    memory=os.environ.get("CQPCFG_RESOURCE_MEMORY", "2g"),
    num_gpus=int(os.environ.get("CQPCFG_RESOURCE_GPUS", "0")),
    model_json_page_cache=int(os.environ.get("CQPCFG_MODEL_JSON_PAGE_CACHE", "128")),
)
class ExperimentNode:
    """Default CQDAGPCFG generator/consumer node used by the experiment."""

    def __init__(self) -> None:
        self.hash_algorithm = "sha256"
        self.hash_delay_seconds = float(os.environ.get("CQPCFG_HASH_DELAY_SECONDS", "0.0"))
        self.min_password_length = int(os.environ.get("CQPCFG_MIN_PASSWORD_LENGTH", "0"))
        self.max_password_length = int(os.environ.get("CQPCFG_MAX_PASSWORD_LENGTH", "0"))

    def generate(self, guess: str) -> str | None:
        if self.min_password_length and len(guess) < self.min_password_length:
            return None
        if self.max_password_length and len(guess) > self.max_password_length:
            return None
        return guess

    def consume(self, guess: str):
        if self.hash_delay_seconds:
            sleep(self.hash_delay_seconds)
        return digest_guess(guess, algorithm=self.hash_algorithm)


def main() -> None:
    ExperimentNode.remote()


if __name__ == "__main__":
    main()
