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
from shared.hash_targets import ExperimentHashTargets

ensure_project_paths()

from cqdagpcfg_parallel import cqdagpcfg


def _env_float(name: str, default: float | None = None) -> float | None:
    value = os.environ.get(name)
    return default if value is None or value == "" else float(value)


def _env_int(name: str, default: int | None = None) -> int | None:
    value = os.environ.get(name)
    return default if value is None or value == "" else int(value)


@cqdagpcfg.remote(
    env_prefix="CQPCFG",
    connect=os.environ.get("CQPCFG_CONNECT") or None,
    node_id=os.environ.get("CQPCFG_NODE_ID") or None,
    num_cpus=_env_float("CQPCFG_RESOURCE_CPUS", 1.0),
    memory=os.environ.get("CQPCFG_RESOURCE_MEMORY") or "2g",
    num_gpus=_env_int("CQPCFG_RESOURCE_GPUS", 0),
)
class ExperimentNode:
    """Default CQDAGPCFG generator/consumer node used by the experiment."""

    def __init__(self, job_payload) -> None:
        self.targets = ExperimentHashTargets(job_payload)
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
        digest = digest_guess(guess, algorithm=self.hash_algorithm)
        return self.targets.match_digest(digest)


def main() -> None:
    ExperimentNode.remote()


if __name__ == "__main__":
    main()
