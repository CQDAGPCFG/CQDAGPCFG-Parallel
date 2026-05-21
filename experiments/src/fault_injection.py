#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from time import monotonic, sleep

from common import ensure_project_paths, experiment_src_dir, read_json, repo_root
from run_local import run_checked, verify_hash_hits

ensure_project_paths()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a fault-injection CQDAGPCFG pipeline: kill the tracker after a "
            "durable checkpoint, restart it, and add a late generator worker."
        ),
    )
    parser.add_argument("--train-file", type=Path, default=None)
    parser.add_argument("--source-model-path", type=Path, default=None)
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=400)
    parser.add_argument("--target-rank", type=int, action="append", default=None)
    parser.add_argument("--hash-algorithm", choices=("sha256", "sha1", "md5"), default="sha256")
    parser.add_argument("--consumer-count", type=int, default=2)
    parser.add_argument("--control-bind", default="cqpcfg://127.0.0.1:5655")
    parser.add_argument("--control-connect", default="cqpcfg://127.0.0.1:5655")
    parser.add_argument("--batch-bind", default="cqpcfg://127.0.0.1:5656")
    parser.add_argument("--batch-connect", default="cqpcfg://127.0.0.1:5656")
    parser.add_argument("--ack-bind", default="cqpcfg://127.0.0.1:5658")
    parser.add_argument("--ack-connect", default="cqpcfg://127.0.0.1:5658")
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-batch-payload-bytes", type=int, default=4096)
    parser.add_argument("--demand-window", type=int, default=8)
    parser.add_argument("--max-chunk-size", type=int, default=16)
    parser.add_argument("--worker-delay-seconds", type=float, default=0.002)
    parser.add_argument("--hash-delay-seconds", type=float, default=0.0)
    parser.add_argument("--crash-after-seconds", type=float, default=0.5)
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.limit <= 0:
        raise SystemExit("--limit must be positive")
    if args.consumer_count <= 0:
        raise SystemExit("--consumer-count must be positive")

    root = repo_root()
    scripts_dir = experiment_src_dir()
    work_dir_context = (
        tempfile.TemporaryDirectory(prefix="cqdagpcfg-fault-")
        if args.work_dir is None
        else None
    )
    work_dir = Path(work_dir_context.name) if work_dir_context is not None else args.work_dir
    assert work_dir is not None
    work_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["PYTHONPATH"] = (
        f"{scripts_dir}:{root / 'src'}:{root.parent}:{env.get('PYTHONPATH', '')}"
    )
    python = sys.executable

    model_path = work_dir / "model.json"
    targets_path = work_dir / "targets.json"
    checkpoint_path = work_dir / "tracker.checkpoint.json"
    stable_log_path = work_dir / "stable-records.jsonl"
    batch_checkpoint_path = work_dir / "batch.checkpoint.json"
    metrics_path = work_dir / "tracker.metrics.json"

    prepare_cmd = [
        python,
        str(scripts_dir / "prepare.py"),
        "--model-path",
        str(model_path),
        "--targets-path",
        str(targets_path),
        "--limit",
        str(args.limit),
        "--hash-algorithm",
        args.hash_algorithm,
    ]
    if args.train_file is not None:
        prepare_cmd.extend(["--train-file", str(args.train_file)])
    if args.source_model_path is not None:
        prepare_cmd.extend(["--source-model-path", str(args.source_model_path)])
    for rank in args.target_rank or []:
        prepare_cmd.extend(["--target-rank", str(rank)])
    run_checked(prepare_cmd, env=env)

    hit_paths = tuple(work_dir / f"hits-{index}.json" for index in range(args.consumer_count))
    consumers = [
        subprocess.Popen(
            [
                python,
                str(scripts_dir / "hash_consumer.py"),
                "--targets-path",
                str(targets_path),
                "--batch-connect",
                args.batch_connect,
                "--ack-connect",
                args.ack_connect,
                "--consumer-id",
                f"consumer-{index}",
                "--hits-path",
                str(hit_paths[index]),
                "--overall-timeout-seconds",
                str(args.timeout_seconds),
                "--hash-delay-seconds",
                str(args.hash_delay_seconds),
            ],
            env=env,
            text=True,
        )
        for index in range(args.consumer_count)
    ]
    sleep(0.2)

    tracker = start_tracker(
        args=args,
        python=python,
        scripts_dir=scripts_dir,
        model_path=model_path,
        targets_path=targets_path,
        checkpoint_path=checkpoint_path,
        stable_log_path=stable_log_path,
        batch_checkpoint_path=batch_checkpoint_path,
        metrics_path=metrics_path,
        env=env,
        resume=False,
    )
    workers = [
        start_worker(
            args=args,
            python=python,
            scripts_dir=scripts_dir,
            model_path=model_path,
            targets_path=targets_path,
            worker_id="worker-0",
            env=env,
        )
    ]

    try:
        wait_for_checkpoint(checkpoint_path, timeout_seconds=args.timeout_seconds)
        crash_deadline = monotonic() + args.crash_after_seconds
        while monotonic() < crash_deadline and tracker.poll() is None:
            sleep(0.05)
        if tracker.poll() is not None:
            raise SystemExit("tracker completed before fault could be injected")
        tracker.kill()
        tracker.wait(timeout=5)
        for worker in workers:
            if worker.poll() is None:
                worker.terminate()
            try:
                worker.wait(timeout=3)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=3)
        workers = []
        print("fault injected: tracker process killed after durable checkpoint")

        sleep(0.5)
        tracker = start_tracker(
            args=args,
            python=python,
            scripts_dir=scripts_dir,
            model_path=model_path,
            targets_path=targets_path,
            checkpoint_path=checkpoint_path,
            stable_log_path=stable_log_path,
            batch_checkpoint_path=batch_checkpoint_path,
            metrics_path=metrics_path,
            env=env,
            resume=True,
        )
        workers.append(
            start_worker(
                args=args,
                python=python,
                scripts_dir=scripts_dir,
                model_path=model_path,
                targets_path=targets_path,
                worker_id="worker-late",
                env=env,
            )
        )
        print("fault recovery: tracker restarted and late worker joined")

        failures = wait_all([tracker, *workers, *consumers], timeout_seconds=args.timeout_seconds)
        if failures:
            for command, code in failures:
                print(f"process failed with code {code}: {command}", file=sys.stderr)
            raise SystemExit(1)
        verify_hash_hits(targets_path, hit_paths)
        print("fault-injection run completed")
        print(f"  work dir          : {work_dir}")
        print(f"  checkpoint        : {checkpoint_path}")
        print(f"  stable log        : {stable_log_path}")
        print(f"  batch checkpoint  : {batch_checkpoint_path}")
        print(f"  final emitted     : {read_json(checkpoint_path)['emitted_count']}")
    finally:
        for process in [tracker, *workers, *consumers]:
            if process.poll() is None:
                process.terminate()
        if work_dir_context is not None:
            work_dir_context.cleanup()


def start_tracker(
    *,
    args: argparse.Namespace,
    python: str,
    scripts_dir: Path,
    model_path: Path,
    targets_path: Path,
    checkpoint_path: Path,
    stable_log_path: Path,
    batch_checkpoint_path: Path,
    metrics_path: Path,
    env: dict[str, str],
    resume: bool,
) -> subprocess.Popen[str]:
    command = [
        python,
        str(scripts_dir / "tracker.py"),
        "--model-path",
        str(model_path),
        "--targets-path",
        str(targets_path),
        "--control-bind",
        args.control_bind,
        "--batch-bind",
        args.batch_bind,
        "--ack-bind",
        args.ack_bind,
        "--consumer-count",
        str(args.consumer_count),
        "--demand-window",
        str(args.demand_window),
        "--max-chunk-size",
        str(args.max_chunk_size),
        "--batch-size",
        str(args.batch_size),
        "--max-batch-payload-bytes",
        str(args.max_batch_payload_bytes),
        "--checkpoint-path",
        str(checkpoint_path),
        "--checkpoint-stable-log-path",
        str(stable_log_path),
        "--batch-checkpoint-path",
        str(batch_checkpoint_path),
        "--metrics-path",
        str(metrics_path),
        "--timeout-seconds",
        str(args.timeout_seconds),
    ]
    if resume:
        command.extend(
            [
                "--resume-checkpoint-path",
                str(checkpoint_path),
                "--resume-batch-checkpoint-path",
                str(batch_checkpoint_path),
            ]
        )
    return subprocess.Popen(command, env=env, text=True)


def start_worker(
    *,
    args: argparse.Namespace,
    python: str,
    scripts_dir: Path,
    model_path: Path,
    targets_path: Path,
    worker_id: str,
    env: dict[str, str],
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            python,
            str(scripts_dir / "generator_worker.py"),
            "--model-path",
            str(model_path),
            "--targets-path",
            str(targets_path),
            "--control-connect",
            args.control_connect,
            "--worker-id",
            worker_id,
            "--demand-window",
            str(args.demand_window),
            "--work-delay-seconds",
            str(args.worker_delay_seconds),
        ],
        env=env,
        text=True,
    )


def wait_for_checkpoint(path: Path, *, timeout_seconds: float) -> None:
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        if path.exists():
            try:
                payload = read_json(path)
            except Exception:
                sleep(0.05)
                continue
            if int(payload.get("emitted_count", 0)) > 0:
                return
        sleep(0.05)
    raise TimeoutError(f"timed out waiting for checkpoint: {path}")


def wait_all(
    processes: list[subprocess.Popen[str]],
    *,
    timeout_seconds: float,
) -> list[tuple[list[str] | str, int]]:
    deadline = monotonic() + timeout_seconds
    pending = set(processes)
    failures: list[tuple[list[str] | str, int]] = []
    while pending and monotonic() < deadline:
        for process in list(pending):
            code = process.poll()
            if code is None:
                continue
            pending.remove(process)
            if code != 0:
                failures.append((process.args, code))
        sleep(0.05)
    for process in pending:
        process.terminate()
    for process in list(pending):
        try:
            code = process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            code = process.wait(timeout=3)
        if code != 0:
            failures.append((process.args, code))
    return failures


if __name__ == "__main__":
    main()
