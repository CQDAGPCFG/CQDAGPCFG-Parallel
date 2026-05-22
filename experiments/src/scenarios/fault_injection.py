#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from threading import Event, Thread
from time import monotonic, sleep

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from scenarios.run_local import run_checked, verify_hash_hits
from shared.common import ensure_project_paths, experiment_src_dir, read_json, repo_root

ensure_project_paths()

from cqdagpcfg_parallel.distributed import RoleController
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a fault-injection CQDAGPCFG pipeline: kill the tracker after a "
            "durable checkpoint, restart it, and add a late elastic node agent."
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
    parser.add_argument("--role-bind", default="cqpcfg://127.0.0.1:5657")
    parser.add_argument("--role-connect", default="cqpcfg://127.0.0.1:5657")
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

    model_path = args.source_model_path or work_dir / "model.json"
    job_spec_path = work_dir / "job-spec.json"
    checkpoint_path = work_dir / "tracker.checkpoint.json"
    stable_log_path = work_dir / "stable-records.jsonl"
    batch_checkpoint_path = work_dir / "batch.checkpoint.json"
    metrics_path = work_dir / "tracker.metrics.json"

    if args.source_model_path is None:
        train_cmd = [
            python,
            str(scripts_dir / "tools" / "train_model.py"),
            "--model-path",
            str(model_path),
        ]
        if args.train_file is not None:
            train_cmd.extend(["--train-file", str(args.train_file)])
        run_checked(train_cmd, env=env)
    elif args.train_file is not None:
        raise SystemExit("--train-file cannot be used with --source-model-path")

    prepare_cmd = [
        python,
        str(scripts_dir / "tools" / "prepare.py"),
        "--source-model-path",
        str(model_path),
        "--job-spec-path",
        str(job_spec_path),
        "--limit",
        str(args.limit),
        "--hash-algorithm",
        args.hash_algorithm,
    ]
    for rank in args.target_rank or []:
        prepare_cmd.extend(["--target-rank", str(rank)])
    run_checked(prepare_cmd, env=env)

    consumer_ids = tuple(f"consumer-{index}" for index in range(args.consumer_count))
    roles = {node_id: "consumer" for node_id in consumer_ids}
    roles["worker-0"] = "generator"
    role_controller, stop_roles, role_thread = start_role_controller(args, roles)
    agents = [
        start_agent(
            args=args,
            python=python,
            scripts_dir=scripts_dir,
            model_path=model_path,
            job_spec_path=job_spec_path,
            work_dir=work_dir,
            node_id=node_id,
            env=env,
        )
        for node_id in (*consumer_ids, "worker-0")
    ]
    output_paths = [work_dir / f"outputs-{node_id}.json" for node_id in (*consumer_ids, "worker-0")]

    tracker = start_tracker(
        args=args,
        python=python,
        scripts_dir=scripts_dir,
        model_path=model_path,
        job_spec_path=job_spec_path,
        checkpoint_path=checkpoint_path,
        stable_log_path=stable_log_path,
        batch_checkpoint_path=batch_checkpoint_path,
        metrics_path=metrics_path,
        env=env,
        resume=False,
    )

    try:
        wait_for_checkpoint(checkpoint_path, timeout_seconds=args.timeout_seconds)
        crash_deadline = monotonic() + args.crash_after_seconds
        while monotonic() < crash_deadline and tracker.poll() is None:
            sleep(0.05)
        if tracker.poll() is not None:
            raise SystemExit("tracker completed before fault could be injected")
        tracker.kill()
        tracker.wait(timeout=5)
        terminate_agent(agents[-1])
        agents = agents[:-1]
        roles.pop("worker-0", None)
        role_controller.set_roles(roles)
        print("fault injected: tracker process killed after durable checkpoint")

        sleep(0.5)
        tracker = start_tracker(
            args=args,
            python=python,
            scripts_dir=scripts_dir,
            model_path=model_path,
            job_spec_path=job_spec_path,
            checkpoint_path=checkpoint_path,
            stable_log_path=stable_log_path,
            batch_checkpoint_path=batch_checkpoint_path,
            metrics_path=metrics_path,
            env=env,
            resume=True,
        )
        roles["worker-late"] = "generator"
        role_controller.set_roles(roles)
        late_agent = start_agent(
            args=args,
            python=python,
            scripts_dir=scripts_dir,
            model_path=model_path,
            job_spec_path=job_spec_path,
            work_dir=work_dir,
            node_id="worker-late",
            env=env,
        )
        agents.append(late_agent)
        output_paths.append(work_dir / "outputs-worker-late.json")
        print("fault recovery: tracker restarted and late worker joined")

        failures = wait_all([tracker, *agents], timeout_seconds=args.timeout_seconds)
        if failures:
            for command, code in failures:
                print(f"process failed with code {code}: {command}", file=sys.stderr)
            raise SystemExit(1)
        verify_hash_hits(job_spec_path, tuple(output_paths))
        print("fault-injection run completed")
        print(f"  work dir          : {work_dir}")
        print(f"  checkpoint        : {checkpoint_path}")
        print(f"  stable log        : {stable_log_path}")
        print(f"  batch checkpoint  : {batch_checkpoint_path}")
        print(f"  final emitted     : {read_json(checkpoint_path)['emitted_count']}")
    finally:
        for process in [tracker, *agents]:
            if process.poll() is None:
                process.terminate()
        stop_roles.set()
        role_thread.join(timeout=2.0)
        role_controller.close()
        if work_dir_context is not None:
            work_dir_context.cleanup()


def start_tracker(
    *,
    args: argparse.Namespace,
    python: str,
    scripts_dir: Path,
    model_path: Path,
    job_spec_path: Path,
    checkpoint_path: Path,
    stable_log_path: Path,
    batch_checkpoint_path: Path,
    metrics_path: Path,
    env: dict[str, str],
    resume: bool,
) -> subprocess.Popen[str]:
    tracker_env = {
        **env,
        "CQPCFG_MODEL_PATH": str(model_path),
        "CQPCFG_JOB_SPEC_PATH": str(job_spec_path),
        "CQPCFG_CONTROL_BIND": args.control_bind,
        "CQPCFG_BATCH_BIND": args.batch_bind,
        "CQPCFG_ACK_BIND": args.ack_bind,
        "CQPCFG_CONSUMER_COUNT": str(args.consumer_count),
        "CQPCFG_DEMAND_WINDOW": str(args.demand_window),
        "CQPCFG_MAX_CHUNK_SIZE": str(args.max_chunk_size),
        "CQPCFG_BATCH_SIZE": str(args.batch_size),
        "CQPCFG_MAX_BATCH_PAYLOAD_BYTES": str(args.max_batch_payload_bytes),
        "CQPCFG_CHECKPOINT_PATH": str(checkpoint_path),
        "CQPCFG_CHECKPOINT_STABLE_LOG_PATH": str(stable_log_path),
        "CQPCFG_BATCH_CHECKPOINT_PATH": str(batch_checkpoint_path),
        "CQPCFG_METRICS_PATH": str(metrics_path),
        "CQPCFG_TIMEOUT_SECONDS": str(args.timeout_seconds),
    }
    if resume:
        tracker_env.update(
            {
                "CQPCFG_RESUME_CHECKPOINT_PATH": str(checkpoint_path),
                "CQPCFG_RESUME_BATCH_CHECKPOINT_PATH": str(batch_checkpoint_path),
            },
        )
    return subprocess.Popen(
        [python, str(scripts_dir / "services" / "tracker.py")],
        env=tracker_env,
        text=True,
    )


def start_role_controller(
    args: argparse.Namespace,
    roles: dict[str, str],
) -> tuple[RoleController, Event, Thread]:
    controller = RoleController(
        endpoint=ZmqEndpoint.from_uri(args.role_bind, bind=True),
        roles=roles,
    )
    stop_event = Event()

    def serve() -> None:
        while not stop_event.is_set():
            controller.poll(timeout_ms=100)

    thread = Thread(target=serve, name="cqdagpcfg-fault-role-controller", daemon=True)
    thread.start()
    return controller, stop_event, thread


def start_agent(
    *,
    args: argparse.Namespace,
    python: str,
    scripts_dir: Path,
    model_path: Path,
    job_spec_path: Path,
    work_dir: Path,
    node_id: str,
    env: dict[str, str],
) -> subprocess.Popen[str]:
    metrics_dir = work_dir / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    agent_env = {
        **env,
        "CQPCFG_NODE_ID": node_id,
        "CQPCFG_ROLE_CONNECT": args.role_connect,
        "CQPCFG_MODEL_PATH": str(model_path),
        "CQPCFG_JOB_SPEC_PATH": str(job_spec_path),
        "CQPCFG_SOURCE_MODE": "root",
        "CQPCFG_CONTROL_CONNECT": args.control_connect,
        "CQPCFG_BATCH_CONNECT": args.batch_connect,
        "CQPCFG_ACK_CONNECT": args.ack_connect,
        "CQPCFG_METRICS_PATH": str(metrics_dir / f"{node_id}.json"),
        "CQPCFG_OUTPUTS_PATH": str(work_dir / f"outputs-{node_id}.json"),
        "CQPCFG_HASH_DELAY_SECONDS": str(args.hash_delay_seconds),
        "CQPCFG_WORK_DELAY_SECONDS": str(args.worker_delay_seconds),
        "CQPCFG_DEMAND_WINDOW": str(args.demand_window),
    }
    return subprocess.Popen(
        [
            python,
            str(scripts_dir / "services" / "node_agent.py"),
        ],
        env=agent_env,
        text=True,
    )


def terminate_agent(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3)


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
