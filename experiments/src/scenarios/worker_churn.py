#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, sleep

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from scenarios.run_local import print_overhead_summary, run_checked, verify_hash_hits
from shared.common import ensure_project_paths, experiment_src_dir, read_json, repo_root, write_json

ensure_project_paths()

from cqdagpcfg_parallel.distributed import JobContext, RoleController
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint


@dataclass(frozen=True, slots=True)
class ScheduledRoleChange:
    at_seconds: float
    node_id: str
    role: str


@dataclass(slots=True)
class AgentProcess:
    node_id: str
    process: subprocess.Popen[str]
    metrics_path: Path
    hits_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a large CQDAGPCFG pipeline while adding/removing worker roles.",
    )
    parser.add_argument("--source-model-path", type=Path, default=None)
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=1_000_000)
    parser.add_argument("--target-rank", type=int, action="append", default=None)
    parser.add_argument("--hash-algorithm", choices=("sha256", "sha1", "md5"), default="sha256")
    parser.add_argument("--total-nodes", type=int, default=6)
    parser.add_argument("--initial-generators", type=int, default=2)
    parser.add_argument("--initial-consumers", type=int, default=1)
    parser.add_argument("--change", action="append", default=None)
    parser.add_argument("--no-default-changes", action="store_true")
    parser.add_argument(
        "--prestart-idle-agents",
        action="store_true",
        help="Start idle NodeAgent processes at launch. By default idle nodes cold-join when assigned a non-idle role.",
    )
    parser.add_argument("--control-bind", default="cqpcfg://127.0.0.1:6750")
    parser.add_argument("--control-connect", default="cqpcfg://127.0.0.1:6750")
    parser.add_argument("--batch-bind", default="cqpcfg://127.0.0.1:6751")
    parser.add_argument("--batch-connect", default="cqpcfg://127.0.0.1:6751")
    parser.add_argument("--role-bind", default="cqpcfg://127.0.0.1:6752")
    parser.add_argument("--role-connect", default="cqpcfg://127.0.0.1:6752")
    parser.add_argument("--ack-bind", default="cqpcfg://127.0.0.1:6753")
    parser.add_argument("--ack-connect", default="cqpcfg://127.0.0.1:6753")
    parser.add_argument("--model-id", default="cqdagpcfg-e2e-model")
    parser.add_argument("--model-serve-bind", default=None)
    parser.add_argument("--model-connect", default=None)
    parser.add_argument("--model-cache-dir", type=Path, default=None)
    parser.add_argument("--model-chunk-size", type=int, default=1 << 20)
    parser.add_argument("--model-slot-page-size", type=int, default=1024)
    parser.add_argument("--model-structure-page-size", type=int, default=4096)
    parser.add_argument("--model-json-page-cache", type=int, default=128)
    parser.add_argument("--source-mode", choices=("root", "structure"), default="root")
    parser.add_argument("--demand-window", type=int, default=64)
    parser.add_argument("--max-chunk-size", type=int, default=256)
    parser.add_argument("--max-parallel-leases-per-node", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--max-batch-payload-bytes", type=int, default=65536)
    parser.add_argument("--worker-delay-seconds", type=float, default=0.0)
    parser.add_argument("--hash-delay-seconds", type=float, default=0.0)
    parser.add_argument("--consumer-drain-quiet-ms", type=int, default=200)
    parser.add_argument("--consumer-drain-timeout-ms", type=int, default=2000)
    parser.add_argument("--role-reply-timeout-ms", type=int, default=100)
    parser.add_argument("--metrics-flush-interval-seconds", type=float, default=1.0)
    parser.add_argument("--ack-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--ack-retry-interval-seconds", type=float, default=10.0)
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.limit <= 0:
        raise SystemExit("--limit must be positive")
    if args.total_nodes < 2:
        raise SystemExit("--total-nodes must be at least 2")
    if args.initial_generators < 0 or args.initial_consumers < 0:
        raise SystemExit("initial role counts cannot be negative")
    if args.initial_generators + args.initial_consumers > args.total_nodes:
        raise SystemExit("initial role counts exceed total nodes")

    root = repo_root()
    scripts_dir = experiment_src_dir()
    work_dir_context = (
        tempfile.TemporaryDirectory(prefix="cqdagpcfg-churn-")
        if args.work_dir is None
        else None
    )
    work_dir = Path(work_dir_context.name) if work_dir_context is not None else args.work_dir
    assert work_dir is not None
    work_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir = work_dir / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["PYTHONPATH"] = (
        f"{scripts_dir}:{root / 'src'}:{root.parent}:{env.get('PYTHONPATH', '')}"
    )
    python = sys.executable
    model_path = work_dir / "model.json"
    targets_path = work_dir / "targets.json"
    tracker_metrics_path = metrics_dir / "tracker.json"

    prepare_cmd = [
        python,
        str(scripts_dir / "tools" / "prepare.py"),
        "--model-path",
        str(model_path),
        "--targets-path",
        str(targets_path),
        "--limit",
        str(args.limit),
        "--hash-algorithm",
        args.hash_algorithm,
    ]
    if args.source_model_path is not None:
        prepare_cmd.extend(["--source-model-path", str(args.source_model_path)])
    for rank in args.target_rank or []:
        prepare_cmd.extend(["--target-rank", str(rank)])
    run_checked(prepare_cmd, env=env)

    node_ids = tuple(f"node-{index}" for index in range(args.total_nodes))
    roles = initial_roles(
        node_ids,
        generator_count=args.initial_generators,
        consumer_count=args.initial_consumers,
    )
    changes = parse_changes(args.change, node_ids)
    if not changes and not args.no_default_changes:
        changes = default_changes(args.total_nodes)
    job_context = build_job_context(args, targets_path) if use_job_context(args) else None

    experiment_started_at = monotonic()
    agents: list[AgentProcess] = []
    agent_by_node: dict[str, AgentProcess] = {}

    def ensure_agent(node_id: str) -> AgentProcess:
        existing = agent_by_node.get(node_id)
        if existing is not None and existing.process.poll() is None:
            return existing
        if existing is not None:
            if existing in agents:
                agents.remove(existing)
            del agent_by_node[node_id]
        agent = start_agent(
            args=args,
            python=python,
            scripts_dir=scripts_dir,
            model_path=model_path,
            targets_path=targets_path,
            work_dir=work_dir,
            metrics_dir=metrics_dir,
            node_id=node_id,
            experiment_started_at=experiment_started_at,
            env=env,
        )
        agents.append(agent)
        agent_by_node[node_id] = agent
        print(f"agent started: {node_id}", flush=True)
        return agent

    for node_id, role in roles.items():
        if role != "idle" or args.prestart_idle_agents:
            ensure_agent(node_id)

    tracker = start_tracker(
        args=args,
        python=python,
        scripts_dir=scripts_dir,
        model_path=model_path,
        targets_path=targets_path,
        metrics_path=tracker_metrics_path,
        env=env,
    )
    role_controller = RoleController(
        endpoint=ZmqEndpoint.from_uri(args.role_bind, bind=True),
        roles=roles,
        job_context=job_context,
    )

    started_at = monotonic()
    applied: set[int] = set()
    failures: list[tuple[list[str] | str, int]] = []
    next_role_report_at = 1.0
    try:
        while tracker.poll() is None:
            role_controller.poll(timeout_ms=50)
            now = monotonic() - started_at
            if now >= next_role_report_at:
                stats = role_controller.stats
                print(
                    "role controller "
                    f"t={now:.1f}s messages={stats.messages} "
                    f"nodes={sorted(role_controller.status_by_node)}",
                    flush=True,
                )
                next_role_report_at = now + 5.0
            for index, change in enumerate(changes):
                if index in applied or now < change.at_seconds:
                    continue
                roles[change.node_id] = change.role
                role_controller.set_roles(roles)
                if change.role != "idle":
                    ensure_agent(change.node_id)
                applied.add(index)
                print(
                    f"role change at {now:.3f}s: {change.node_id} -> {change.role}",
                    flush=True,
                )
            failures.extend(reap_finished(agents, agent_by_node))
            sleep(0.05)

        tracker_code = tracker.wait()
        if tracker_code != 0:
            failures.append((tracker.args, tracker_code))

        role_controller.set_stop(True)
        deadline = monotonic() + args.timeout_seconds
        while agents and monotonic() < deadline:
            role_controller.poll(timeout_ms=50)
            failures.extend(reap_finished(agents, agent_by_node))
            sleep(0.05)
        if agents:
            sleep(0.2)
        for agent in agents:
            if agent.process.poll() is None:
                agent.process.terminate()
        for agent in agents:
            code = agent.process.wait(timeout=5)
            if code != 0:
                failures.append((agent.process.args, code))

        if failures:
            for command, code in failures:
                print(f"process failed with code {code}: {command}", file=sys.stderr)
            raise SystemExit(1)

        hit_paths = ensure_hit_reports(work_dir, node_ids)
        verify_hash_hits(targets_path, hit_paths)
        summary = summarize_run(work_dir, tracker_metrics_path, hit_paths)
        write_json(work_dir / "churn_summary.json", summary)
        print_summary(summary)
        print_overhead_summary(
            tracker_metrics_path=tracker_metrics_path,
            node_metrics_paths=tuple(metrics_dir.glob("node-*.json")),
            role_controller=role_controller,
        )
    finally:
        if tracker.poll() is None:
            tracker.terminate()
        role_controller.set_stop(True)
        role_controller.poll(timeout_ms=50)
        for agent in agents:
            if agent.process.poll() is None:
                agent.process.terminate()
        role_controller.close()
        if work_dir_context is not None:
            work_dir_context.cleanup()


def initial_roles(
    node_ids: tuple[str, ...],
    *,
    generator_count: int,
    consumer_count: int,
) -> dict[str, str]:
    roles = {node_id: "idle" for node_id in node_ids}
    for node_id in node_ids[:generator_count]:
        roles[node_id] = "generator"
    for node_id in node_ids[generator_count : generator_count + consumer_count]:
        roles[node_id] = "consumer"
    return roles


def default_changes(total_nodes: int) -> list[ScheduledRoleChange]:
    changes = [
        ScheduledRoleChange(10.0, "node-3", "generator"),
        ScheduledRoleChange(20.0, "node-4", "consumer"),
        ScheduledRoleChange(35.0, "node-0", "idle"),
        ScheduledRoleChange(50.0, "node-5", "generator"),
        ScheduledRoleChange(65.0, "node-2", "idle"),
        ScheduledRoleChange(80.0, "node-0", "consumer"),
    ]
    return [change for change in changes if int(change.node_id.split("-")[-1]) < total_nodes]


def parse_changes(raw_changes: list[str] | None, node_ids: tuple[str, ...]) -> list[ScheduledRoleChange]:
    if not raw_changes:
        return []
    valid_nodes = set(node_ids)
    valid_roles = {"generator", "consumer", "idle"}
    parsed: list[ScheduledRoleChange] = []
    for raw in raw_changes:
        parts = raw.split(":")
        if len(parts) != 3:
            raise SystemExit("--change must be formatted as seconds:node-id:role")
        at_seconds = float(parts[0])
        node_id = parts[1]
        role = parts[2]
        if node_id not in valid_nodes:
            raise SystemExit(f"unknown node in --change: {node_id}")
        if role not in valid_roles:
            raise SystemExit(f"invalid role in --change: {role}")
        parsed.append(ScheduledRoleChange(at_seconds, node_id, role))
    return sorted(parsed, key=lambda change: change.at_seconds)


def start_agent(
    *,
    args: argparse.Namespace,
    python: str,
    scripts_dir: Path,
    model_path: Path,
    targets_path: Path,
    work_dir: Path,
    metrics_dir: Path,
    node_id: str,
    experiment_started_at: float,
    env: dict[str, str],
) -> AgentProcess:
    metrics_path = metrics_dir / f"{node_id}.json"
    hits_path = work_dir / f"hits-{node_id}.json"
    agent_env = {
        **env,
        "CQPCFG_NODE_ID": node_id,
        "CQPCFG_ROLE_CONNECT": args.role_connect,
        "CQPCFG_WORK_DELAY_SECONDS": str(args.worker_delay_seconds),
        "CQPCFG_HASH_DELAY_SECONDS": str(args.hash_delay_seconds),
        "CQPCFG_CONSUMER_DRAIN_QUIET_MS": str(args.consumer_drain_quiet_ms),
        "CQPCFG_CONSUMER_DRAIN_TIMEOUT_MS": str(args.consumer_drain_timeout_ms),
        "CQPCFG_ROLE_REPLY_TIMEOUT_MS": str(args.role_reply_timeout_ms),
        "CQPCFG_METRICS_FLUSH_INTERVAL_SECONDS": str(args.metrics_flush_interval_seconds),
        "CQPCFG_EXPERIMENT_START_MONOTONIC": str(experiment_started_at),
        "CQPCFG_METRICS_PATH": str(metrics_path),
        "CQPCFG_HITS_PATH": str(hits_path),
        "CQPCFG_MODEL_JSON_PAGE_CACHE": str(args.model_json_page_cache),
    }
    if args.model_cache_dir is not None:
        agent_env["CQPCFG_MODEL_CACHE_DIR"] = str(args.model_cache_dir)
    if not use_job_context(args):
        agent_env.update(
            {
                "CQPCFG_TARGETS_PATH": str(targets_path),
                "CQPCFG_SOURCE_MODE": args.source_mode,
                "CQPCFG_CONTROL_CONNECT": args.control_connect,
                "CQPCFG_BATCH_CONNECT": args.batch_connect,
                "CQPCFG_ACK_CONNECT": args.ack_connect,
                "CQPCFG_DEMAND_WINDOW": str(args.demand_window),
            }
        )
        if args.model_connect is not None:
            agent_env["CQPCFG_MODEL_CONNECT"] = args.model_connect
            agent_env["CQPCFG_MODEL_ID"] = args.model_id
        else:
            agent_env["CQPCFG_MODEL_PATH"] = str(model_path)
    command = [
        python,
        str(scripts_dir / "services" / "node_agent.py"),
    ]
    return AgentProcess(
        node_id=node_id,
        process=subprocess.Popen(command, env=agent_env, text=True),
        metrics_path=metrics_path,
        hits_path=hits_path,
    )


def start_tracker(
    *,
    args: argparse.Namespace,
    python: str,
    scripts_dir: Path,
    model_path: Path,
    targets_path: Path,
    metrics_path: Path,
    env: dict[str, str],
) -> subprocess.Popen[str]:
    command = [
        python,
        str(scripts_dir / "services" / "tracker.py"),
        "--model-path",
        str(model_path),
        "--targets-path",
        str(targets_path),
        "--model-id",
        args.model_id,
        "--source-mode",
        args.source_mode,
        "--control-bind",
        args.control_bind,
        "--batch-bind",
        args.batch_bind,
        "--ack-bind",
        args.ack_bind,
        "--consumer-count",
        str(args.total_nodes),
        "--demand-window",
        str(args.demand_window),
        "--max-chunk-size",
        str(args.max_chunk_size),
        "--max-parallel-leases-per-node",
        str(args.max_parallel_leases_per_node),
        "--batch-size",
        str(args.batch_size),
        "--max-batch-payload-bytes",
        str(args.max_batch_payload_bytes),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--ack-timeout-seconds",
        str(args.ack_timeout_seconds),
        "--ack-retry-interval-seconds",
        str(args.ack_retry_interval_seconds),
        "--metrics-path",
        str(metrics_path),
        "--metrics-flush-interval-seconds",
        str(args.metrics_flush_interval_seconds),
    ]
    if args.model_serve_bind is not None:
        command.extend(
            [
                "--model-serve-bind",
                args.model_serve_bind,
                "--model-chunk-size",
                str(args.model_chunk_size),
                "--model-slot-page-size",
                str(args.model_slot_page_size),
                "--model-structure-page-size",
                str(args.model_structure_page_size),
            ]
        )
    return subprocess.Popen(command, env=env, text=True)


def use_job_context(args: argparse.Namespace) -> bool:
    return args.model_connect is not None or args.model_serve_bind is not None


def build_job_context(args: argparse.Namespace, targets_path: Path) -> JobContext:
    model_connect = args.model_connect
    if model_connect is None:
        if args.model_serve_bind is None:
            raise RuntimeError("model serve endpoint is required for JobContext")
        model_connect = connect_uri_from_bind(args.model_serve_bind)
    return JobContext.from_targets_payload(
        read_json(targets_path),
        job_id="worker-churn",
        model_id=args.model_id,
        model_connect=model_connect,
        control_connect=args.control_connect,
        batch_connect=args.batch_connect,
        ack_connect=args.ack_connect,
        source_mode=args.source_mode,
        demand_window=args.demand_window,
    )


def connect_uri_from_bind(uri: str) -> str:
    if uri.startswith("cqpcfg://0.0.0.0:"):
        return uri.replace("cqpcfg://0.0.0.0:", "cqpcfg://127.0.0.1:", 1)
    return uri


def reap_finished(
    agents: list[AgentProcess],
    agent_by_node: dict[str, AgentProcess] | None = None,
) -> list[tuple[list[str] | str, int]]:
    failures: list[tuple[list[str] | str, int]] = []
    for agent in tuple(agents):
        code = agent.process.poll()
        if code is None:
            continue
        if code != 0:
            failures.append((agent.process.args, code))
        agents.remove(agent)
        if agent_by_node is not None and agent_by_node.get(agent.node_id) is agent:
            del agent_by_node[agent.node_id]
    return failures


def ensure_hit_reports(work_dir: Path, node_ids: tuple[str, ...]) -> tuple[Path, ...]:
    hit_paths = tuple(work_dir / f"hits-{node_id}.json" for node_id in node_ids)
    for path in hit_paths:
        if not path.exists():
            write_json(path, {"hits": []})
    return hit_paths


def summarize_run(
    work_dir: Path,
    tracker_metrics_path: Path,
    hit_paths: tuple[Path, ...],
) -> dict:
    targets = read_json(work_dir / "targets.json")
    tracker = read_json(tracker_metrics_path)
    hits = []
    for path in hit_paths:
        if path.exists():
            hits.extend(read_json(path).get("hits", []))
    first_hit_by_target: dict[str, dict] = {}
    for hit in hits:
        key = str(hit["target_rank"])
        current = first_hit_by_target.get(key)
        if current is None or float(hit.get("elapsed_seconds", 0.0)) < float(
            current.get("elapsed_seconds", 0.0)
        ):
            first_hit_by_target[key] = hit
    nodes = [
        read_json(path)
        for path in sorted((work_dir / "metrics").glob("node-*.json"))
        if path.exists()
    ]
    return {
        "limit": targets["limit"],
        "serial_digest": targets["serial_digest"],
        "tracker_final": tracker.get("final"),
        "candidate_rate": tracker.get("candidate_rate"),
        "elapsed_seconds": tracker.get("elapsed_seconds"),
        "emitted_records": tracker.get("emitted_records"),
        "ack_pending_batches": tracker.get("ack_pending_batches"),
        "ack_republished_batches": tracker.get("ack_republished_batches"),
        "chunkstore_peak_resident_records": tracker.get("chunkstore_peak_resident_records"),
        "chunkstore_reclaimed_records": tracker.get("chunkstore_reclaimed_records"),
        "role_switches": sum(int(node.get("role_switches", 0)) for node in nodes),
        "drained_batches": sum(int(node.get("drained_batches", 0)) for node in nodes),
        "drained_candidates": sum(int(node.get("drained_candidates", 0)) for node in nodes),
        "consumed_candidates": sum(int(node.get("consumed_candidates", 0)) for node in nodes),
        "first_hit_by_target": first_hit_by_target,
    }


def print_summary(summary: dict) -> None:
    print("worker churn summary")
    print(f"  limit             : {summary['limit']}")
    print(f"  emitted records   : {summary['emitted_records']}")
    print(f"  candidate rate    : {summary['candidate_rate']:.3f}")
    print(f"  elapsed seconds   : {summary['elapsed_seconds']:.3f}")
    print(f"  ack pending       : {summary['ack_pending_batches']}")
    print(f"  republished       : {summary['ack_republished_batches']}")
    print(f"  role switches     : {summary['role_switches']}")
    print(f"  drained batches   : {summary['drained_batches']}")
    print("  first hits        :")
    for target_rank, hit in sorted(
        summary["first_hit_by_target"].items(),
        key=lambda item: int(item[0]),
    ):
        print(
            f"    target_rank={target_rank} rank={hit['rank']} "
            f"elapsed={float(hit.get('elapsed_seconds', 0.0)):.6f}s "
            f"node={hit.get('node_id', hit.get('consumer_id', ''))}"
        )


if __name__ == "__main__":
    main()
