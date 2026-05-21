#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"

COMMANDS = {
    "prepare": SRC / "tools" / "prepare.py",
    "tracker": SRC / "services" / "tracker.py",
    "worker": SRC / "services" / "node_agent.py",
    "metrics": SRC / "services" / "metrics_exporter.py",
    "verify": SRC / "tools" / "verify_hits.py",
    "local": SRC / "scenarios" / "run_local.py",
    "validate": SRC / "scenarios" / "protocol_validation.py",
    "fault": SRC / "scenarios" / "fault_injection.py",
    "churn": SRC / "scenarios" / "worker_churn.py",
}

DESCRIPTIONS = {
    "prepare": "prepare a model and target hashes",
    "tracker": "run the tracker service",
    "worker": "run an elastic node-agent worker",
    "metrics": "export JSON metrics for Prometheus",
    "verify": "verify hit reports",
    "local": "run a local end-to-end pipeline",
    "validate": "run protocol validation scenarios",
    "fault": "run tracker-crash recovery scenario",
    "churn": "run worker join/leave scenario",
}


def main() -> None:
    launcher = Path(sys.argv[0]).name
    args = sys.argv[1:]
    if not args or args[0] in {"-h", "--help", "help"}:
        print_help()
        return

    command = args[0]
    target = COMMANDS.get(command)
    if target is None:
        valid = ", ".join(sorted(COMMANDS))
        raise SystemExit(f"unknown command {command!r}; valid commands: {valid}")
    if command == "worker" and len(args) > 1 and args[1] in {"-h", "--help"}:
        print_worker_help()
        return

    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    sys.argv = [f"{launcher} {command}", *args[1:]]
    code = compile(target.read_text(), str(target), "exec")
    exec(
        code,
        {
            "__name__": "__main__",
            "__file__": str(target),
            "__package__": None,
            "__cached__": None,
        },
    )


def print_help() -> None:
    print("usage: python experiments/cqpcfg_experiment.py <command> [args...]")
    print()
    print("commands:")
    width = max(len(command) for command in COMMANDS)
    for command in sorted(COMMANDS):
        print(f"  {command:<{width}}  {DESCRIPTIONS[command]}")
    print()
    print("examples:")
    print("  python experiments/cqpcfg_experiment.py local --limit 1000")
    print("  python experiments/cqpcfg_experiment.py tracker --model-path model.json --targets-path targets.json")
    print("  CQPCFG_CONNECT=cqpcfg://tracker:5555 python experiments/cqpcfg_experiment.py worker")


def print_worker_help() -> None:
    print("usage: CQPCFG_CONNECT=cqpcfg://tracker:5555 python experiments/cqpcfg_experiment.py worker")
    print()
    print("worker configuration is read from environment variables:")
    print("  CQPCFG_NODE_ID")
    print("  CQPCFG_CONNECT")
    print("  CQPCFG_MODEL_CACHE_DIR")
    print("  CQPCFG_METRICS_PATH or CQPCFG_METRICS_DIR")
    print("  CQPCFG_HITS_PATH or CQPCFG_HITS_DIR")
    print("  CQPCFG_MODEL_JSON_PAGE_CACHE")
    print("  CQPCFG_RESOURCE_CPUS / CQPCFG_RESOURCE_MEMORY / CQPCFG_RESOURCE_GPUS")


if __name__ == "__main__":
    main()
