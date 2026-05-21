#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from common import ensure_project_paths, read_json

ensure_project_paths()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify combined hash consumer hit reports.")
    parser.add_argument("--targets-path", type=Path, required=True)
    parser.add_argument("--hits-path", type=Path, action="append", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    targets = read_json(args.targets_path)
    expected_guesses = {str(target["guess"]) for target in targets["targets"]}
    hits = []
    for path in args.hits_path:
        if not path.exists():
            raise SystemExit(f"missing hit report: {path}")
        hits.extend(read_json(path)["hits"])

    found_guesses = {str(hit["guess"]) for hit in hits}
    missing = sorted(expected_guesses - found_guesses)
    if missing:
        raise SystemExit(f"hash consumers missed target guesses: {missing}")

    print("combined hash consumer reports verified")
    print(f"  consumers: {len(args.hits_path)}")
    print(f"  hits     : {len(hits)}")


if __name__ == "__main__":
    main()
