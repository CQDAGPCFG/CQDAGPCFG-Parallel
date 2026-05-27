#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
from pathlib import Path

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from shared.benchmark_backends import (
    BenchmarkSpec,
    CandidateSpaceManifest,
    FitcrackBackend,
    OursProtocolBackend,
    PcfgManagerBackend,
    command_with_monitor,
)
from shared.common import ensure_project_paths

ensure_project_paths()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one backend against a materialized candidate space.",
    )
    parser.add_argument(
        "--backend",
        choices=("ours", "pcfg-manager", "fitcrack"),
        required=True,
    )
    parser.add_argument("--candidate-space", type=Path, required=True)
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--limit", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=14_400.0)
    parser.add_argument("--hash", action="append", default=[])
    parser.add_argument("--hash-algorithm", default="md5")
    parser.add_argument("--total-nodes", type=int, default=2)
    parser.add_argument(
        "--source-mode",
        choices=("root", "structure", "shard"),
        default="root",
        help=(
            "CQDAGPCFG protocol source mode for the ours backend. "
            "root uses rank-space paging; structure uses CQDAG structure shards; "
            "shard emits structure-local cracking artifacts directly."
        ),
    )
    parser.add_argument("--monitor-interval-seconds", type=float, default=2.0)
    parser.add_argument("--max-rss-mib", type=float, default=None)
    parser.add_argument(
        "--pcfg-manager",
        type=Path,
        default=Path("experiments/external/pcfg-manager/pcfg-manager"),
    )
    parser.add_argument("--pcfg-manager-port", type=int, default=50151)
    parser.add_argument("--pcfg-manager-chunk-duration", default="30s")
    parser.add_argument("--pcfg-manager-chunk-start-size", type=int, default=10_000)
    parser.add_argument("--pcfg-manager-client-count", type=int, default=1)
    parser.add_argument("--fitcrack-cookie-path", type=Path, default=None)
    parser.add_argument("--fitcrack-grammar-id", type=int, default=None)
    parser.add_argument("--fitcrack-grammar-name", default=None)
    parser.add_argument("--fitcrack-grammar-keyspace", type=int, default=None)
    parser.add_argument("--fitcrack-poll-seconds", type=float, default=10.0)
    parser.add_argument("--fitcrack-max-docker-memory-mib", type=float, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result_dir = args.result_dir.resolve()
    result_dir.mkdir(parents=True, exist_ok=True)
    manifest = CandidateSpaceManifest.load(args.candidate_space)
    hashes = tuple(args.hash) or ("ba066b54c0a283266046638c7431dd51", "0" * 32)
    spec = BenchmarkSpec(
        candidate_space=manifest,
        result_dir=result_dir,
        limit=args.limit,
        hashes=hashes,
        timeout_seconds=args.timeout_seconds,
        hash_algorithm=args.hash_algorithm,
        total_nodes=args.total_nodes,
        source_mode=args.source_mode,
        monitor_interval_seconds=args.monitor_interval_seconds,
        max_rss_mib=args.max_rss_mib,
    )
    backend = build_backend(args, manifest)
    command = backend.build(spec)
    runnable, env = command_with_monitor(
        command=command,
        result_dir=result_dir,
        interval_seconds=args.monitor_interval_seconds,
        python=spec.python,
        max_rss_mib=None if args.backend == "pcfg-manager" else args.max_rss_mib,
    )
    (result_dir / "benchmark-command.json").write_text(
        json.dumps(
            {
                "backend": backend.name,
                "candidate_space": str(manifest.path),
                "limit": args.limit,
                "source_mode": args.source_mode,
                "command": runnable,
                "extra_env": dict(command.env),
                "monitor_process_tree": command.monitor_process_tree,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    process = subprocess.Popen(runnable, env=env, text=True)

    def forward_stop(signum, _frame) -> None:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        raise SystemExit(128 + signum)

    old_term = signal.signal(signal.SIGTERM, forward_stop)
    old_int = signal.signal(signal.SIGINT, forward_stop)
    try:
        code = process.wait()
    finally:
        signal.signal(signal.SIGTERM, old_term)
        signal.signal(signal.SIGINT, old_int)
    raise SystemExit(code)


def build_backend(args: argparse.Namespace, manifest: CandidateSpaceManifest):
    if args.backend == "ours":
        return OursProtocolBackend()
    if args.backend == "pcfg-manager":
        return PcfgManagerBackend(
            pcfg_manager=args.pcfg_manager,
            port=args.pcfg_manager_port,
            chunk_duration=args.pcfg_manager_chunk_duration,
            chunk_start_size=args.pcfg_manager_chunk_start_size,
            client_count=args.pcfg_manager_client_count,
            max_rss_mib=args.max_rss_mib,
        )
    if args.fitcrack_cookie_path is None:
        raise SystemExit("--fitcrack-cookie-path is required for fitcrack backend")
    if args.fitcrack_grammar_id is None:
        raise SystemExit("--fitcrack-grammar-id is required for fitcrack backend")
    grammar_name = args.fitcrack_grammar_name or manifest.name
    grammar_keyspace = (
        args.fitcrack_grammar_keyspace
        if args.fitcrack_grammar_keyspace is not None
        else manifest.total_points
    )
    return FitcrackBackend(
        cookie_path=args.fitcrack_cookie_path,
        grammar_id=args.fitcrack_grammar_id,
        grammar_name=grammar_name,
        grammar_keyspace=grammar_keyspace,
        poll_seconds=args.fitcrack_poll_seconds,
        max_docker_memory_mib=args.fitcrack_max_docker_memory_mib,
    )


if __name__ == "__main__":
    main()
