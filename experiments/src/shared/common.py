from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Iterable


DEFAULT_TRAINING_PASSWORDS = (
    "ab12!",
    "ab12!",
    "cd12!",
    "ab34@",
    "p@ssw0rd",
    "password12",
    "dragonball99",
    "moon24@",
    "star77#",
    "1990abc!",
    "asdf12!",
    "hello2024!",
    "hello2024!",
    "elite99",
)


def repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists() and (parent / "src" / "cqdagpcfg_parallel").exists():
            return parent
    raise RuntimeError(f"could not find CQDAGPCFG-Parallel repo root from {current}")


def workspace_root() -> Path:
    return repo_root().parent


def experiment_src_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def ensure_project_paths() -> None:
    root = repo_root()
    for path in (experiment_src_dir(), root / "src", workspace_root()):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)


def load_training_passwords(path: Path | None) -> tuple[str, ...]:
    if path is None:
        return DEFAULT_TRAINING_PASSWORDS
    values = tuple(
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )
    if not values:
        raise ValueError(f"training file has no non-empty passwords: {path}")
    return values


def normalize_target_ranks(raw_ranks: Iterable[int] | None, output_count: int) -> tuple[int, ...]:
    ranks = tuple(raw_ranks) if raw_ranks is not None else (0, min(7, output_count - 1))
    normalized = tuple(dict.fromkeys(ranks))
    for rank in normalized:
        if rank < 0 or rank >= output_count:
            raise ValueError(f"target rank out of generated prefix: {rank}")
    return normalized


def digest_guess(guess: str, *, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    digest.update(guess.encode("utf-8"))
    return digest.hexdigest()


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))
