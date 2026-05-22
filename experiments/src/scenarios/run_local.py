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

from shared.common import ensure_project_paths, experiment_src_dir, read_json, repo_root

ensure_project_paths()

from cqdagpcfg_parallel.distributed import (
    CqdagAwareElasticRoleAllocator,
    JobContext,
    RoleAllocationInput,
    RoleController,
)
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint
from shared.role_signals import (
    RoleSignalConfig,
    build_role_signal_snapshot,
    switch_candidates,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the local CQDAGPCFG tracker plus elastic node-agent pipeline.",
    )
    parser.add_argument("--train-file", type=Path, default=None)
    parser.add_argument("--source-model-path", type=Path, default=None)
    parser.add_argument("--work-dir", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--target-rank", type=int, action="append", default=None)
    parser.add_argument("--hash-algorithm", choices=("sha256", "sha1", "md5"), default="sha256")
    parser.add_argument("--total-nodes", type=int, default=5)
    parser.add_argument("--generator-rate", type=float, default=120_000.0)
    parser.add_argument("--consumer-rate", type=float, default=180_000.0)
    parser.add_argument("--control-bind", default="cqpcfg://127.0.0.1:5555")
    parser.add_argument("--control-connect", default="cqpcfg://127.0.0.1:5555")
    parser.add_argument("--batch-bind", default="cqpcfg://127.0.0.1:5556")
    parser.add_argument("--batch-connect", default="cqpcfg://127.0.0.1:5556")
    parser.add_argument("--ack-bind", default="cqpcfg://127.0.0.1:5558")
    parser.add_argument("--ack-connect", default="cqpcfg://127.0.0.1:5558")
    parser.add_argument("--role-bind", default="cqpcfg://127.0.0.1:5557")
    parser.add_argument("--role-connect", default="cqpcfg://127.0.0.1:5557")
    parser.add_argument("--model-id", default="cqdagpcfg-e2e-model")
    parser.add_argument("--model-serve-bind", default=None)
    parser.add_argument("--model-connect", default=None)
    parser.add_argument("--model-cache-dir", type=Path, default=None)
    parser.add_argument("--model-chunk-size", type=int, default=1 << 20)
    parser.add_argument("--model-slot-page-size", type=int, default=1024)
    parser.add_argument("--model-structure-page-size", type=int, default=4096)
    parser.add_argument("--model-json-page-cache", type=int, default=128)
    parser.add_argument("--source-mode", choices=("root", "structure"), default="root")
    parser.add_argument("--demand-window", type=int, default=8)
    parser.add_argument("--max-chunk-size", type=int, default=32)
    parser.add_argument("--max-parallel-leases-per-node", type=int, default=2)
    parser.add_argument("--disable-node-affinity", action="store_true")
    parser.add_argument("--node-affinity-bonus", type=float, default=0.5)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--max-batch-payload-bytes", type=int, default=4096)
    parser.add_argument("--ack-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--ack-retry-interval-seconds", type=float, default=5.0)
    parser.add_argument("--worker-delay-seconds", type=float, default=0.0)
    parser.add_argument("--hash-delay-seconds", type=float, default=0.0)
    parser.add_argument("--consumer-drain-quiet-ms", type=int, default=200)
    parser.add_argument("--consumer-drain-timeout-ms", type=int, default=2000)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--disable-reclaim", action="store_true")
    parser.add_argument("--dynamic-rebalance", action="store_true")
    parser.add_argument("--rebalance-interval-seconds", type=float, default=0.25)
    parser.add_argument("--role-switch-cooldown-seconds", type=float, default=3.0)
    parser.add_argument("--role-switch-min-improvement", type=float, default=0.25)
    parser.add_argument("--metrics-flush-interval-seconds", type=float, default=0.25)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = repo_root()
    scripts_dir = experiment_src_dir()
    work_dir_context = (
        tempfile.TemporaryDirectory(prefix="cqdagpcfg-e2e-")
        if args.work_dir is None
        else None
    )
    work_dir = Path(work_dir_context.name) if work_dir_context is not None else args.work_dir
    assert work_dir is not None
    work_dir.mkdir(parents=True, exist_ok=True)
    model_path = args.source_model_path or work_dir / "model.json"
    job_spec_path = work_dir / "job-spec.json"

    env = os.environ.copy()
    env["PYTHONPATH"] = (
        f"{scripts_dir}:{root / 'src'}:{root.parent}:{env.get('PYTHONPATH', '')}"
    )
    python = sys.executable

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

    role_plan = CqdagAwareElasticRoleAllocator().plan(
        RoleAllocationInput(
            total_nodes=args.total_nodes,
            generator_rate_per_node=args.generator_rate,
            consumer_rate_per_node=args.consumer_rate,
        )
    )
    print("local E2E role plan")
    print(f"  generators: {role_plan.generator_count}")
    print(f"  consumers : {role_plan.consumer_count}")
    print(f"  throughput: {role_plan.expected_throughput:.3f}", flush=True)

    run_node_agent_pipeline(
        args=args,
        env=env,
        python=python,
        scripts_dir=scripts_dir,
        model_path=model_path,
        job_spec_path=job_spec_path,
        work_dir=work_dir,
        initial_plan=role_plan,
        enable_rebalance=args.dynamic_rebalance,
    )
    if work_dir_context is not None:
        work_dir_context.cleanup()


def run_checked(command: list[str], *, env: dict[str, str]) -> None:
    subprocess.run(command, env=env, check=True, text=True)


def verify_hash_hits(job_spec_path: Path, output_paths: tuple[Path, ...]) -> None:
    targets = read_json(job_spec_path)
    expected_guesses = {str(target["guess"]) for target in targets["targets"]}
    all_outputs = []
    for path in output_paths:
        if not path.exists():
            raise SystemExit(f"missing consumer output report: {path}")
        all_outputs.extend(read_json(path)["consumer_outputs"])
    found_guesses = {str(output["guess"]) for output in all_outputs}
    missing = sorted(expected_guesses - found_guesses)
    if missing:
        raise SystemExit(f"hash consumers missed target guesses: {missing}")
    print("local node hit reports verified")
    print(f"  nodes: {len(output_paths)}")
    print(f"  outputs: {len(all_outputs)}")


@dataclass(slots=True)
class AgentProcess:
    node_id: str
    process: subprocess.Popen[str]
    metrics_path: Path
    outputs_path: Path


@dataclass(slots=True)
class RoleStabilityState:
    last_global_switch_at: float
    last_switch_by_node: dict[str, float]


def run_node_agent_pipeline(
    *,
    args: argparse.Namespace,
    env: dict[str, str],
    python: str,
    scripts_dir: Path,
    model_path: Path,
    job_spec_path: Path,
    work_dir: Path,
    initial_plan,
    enable_rebalance: bool,
) -> None:
    if args.rebalance_interval_seconds <= 0.0:
        raise SystemExit("--rebalance-interval-seconds must be positive")
    if args.role_switch_cooldown_seconds < 0.0:
        raise SystemExit("--role-switch-cooldown-seconds cannot be negative")
    if args.role_switch_min_improvement < 0.0:
        raise SystemExit("--role-switch-min-improvement cannot be negative")

    metrics_dir = work_dir / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    tracker_metrics_path = metrics_dir / "tracker.json"

    node_ids = tuple(f"node-{index}" for index in range(args.total_nodes))
    roles = {
        node_id: ("generator" if index < initial_plan.generator_count else "consumer")
        for index, node_id in enumerate(node_ids)
    }
    role_controller = RoleController(
        endpoint=ZmqEndpoint.from_uri(args.role_bind, bind=True),
        roles=roles,
        job_context=build_job_context(args, job_spec_path) if use_job_context(args) else None,
    )

    agents: dict[str, AgentProcess] = {}
    all_output_paths: list[Path] = []
    all_metrics_paths: list[Path] = []
    allocator = CqdagAwareElasticRoleAllocator()
    role_stability = RoleStabilityState(
        last_global_switch_at=-args.role_switch_cooldown_seconds,
        last_switch_by_node={
            node_id: -args.role_switch_cooldown_seconds for node_id in node_ids
        },
    )

    def start_agent(node_id: str) -> None:
        metrics_path = metrics_dir / f"{node_id}.json"
        outputs_path = work_dir / f"outputs-{node_id}.json"
        all_metrics_paths.append(metrics_path)
        all_output_paths.append(outputs_path)
        agent_env = {
            **env,
            "CQPCFG_NODE_ID": node_id,
            "CQPCFG_ROLE_CONNECT": args.role_connect,
            "CQPCFG_WORK_DELAY_SECONDS": str(args.worker_delay_seconds),
            "CQPCFG_HASH_DELAY_SECONDS": str(args.hash_delay_seconds),
            "CQPCFG_CONSUMER_DRAIN_QUIET_MS": str(args.consumer_drain_quiet_ms),
            "CQPCFG_CONSUMER_DRAIN_TIMEOUT_MS": str(args.consumer_drain_timeout_ms),
            "CQPCFG_METRICS_FLUSH_INTERVAL_SECONDS": str(args.metrics_flush_interval_seconds),
            "CQPCFG_METRICS_PATH": str(metrics_path),
            "CQPCFG_OUTPUTS_PATH": str(outputs_path),
            "CQPCFG_MODEL_JSON_PAGE_CACHE": str(args.model_json_page_cache),
        }
        if args.model_cache_dir is not None:
            agent_env["CQPCFG_MODEL_CACHE_DIR"] = str(args.model_cache_dir)
        if not use_job_context(args):
            agent_env.update(
                {
                    "CQPCFG_JOB_SPEC_PATH": str(job_spec_path),
                    "CQPCFG_CONTROL_CONNECT": args.control_connect,
                    "CQPCFG_BATCH_CONNECT": args.batch_connect,
                    "CQPCFG_ACK_CONNECT": args.ack_connect,
                    "CQPCFG_DEMAND_WINDOW": str(args.demand_window),
                    "CQPCFG_SOURCE_MODE": args.source_mode,
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
        agents[node_id] = AgentProcess(
            node_id=node_id,
            process=subprocess.Popen(command, env=agent_env, text=True),
            metrics_path=metrics_path,
            outputs_path=outputs_path,
        )

    def request_switch(node_id: str, new_role: str) -> bool:
        old_role = roles.get(node_id)
        if old_role == new_role:
            return False
        now = monotonic()
        if (
            now - role_stability.last_global_switch_at
            < args.role_switch_cooldown_seconds
        ):
            return False
        if (
            now - role_stability.last_switch_by_node.get(node_id, 0.0)
            < args.role_switch_cooldown_seconds
        ):
            return False
        roles[node_id] = new_role
        role_stability.last_global_switch_at = now
        role_stability.last_switch_by_node[node_id] = now
        role_controller.set_roles(roles)
        print(f"in-process role switch requested: {node_id} {old_role} -> {new_role}")
        return True

    def reap_finished() -> list[tuple[list[str] | str, int]]:
        failures = []
        for node_id, agent in list(agents.items()):
            code = agent.process.poll()
            if code is None:
                continue
            if code != 0:
                failures.append((agent.process.args, code))
            del agents[node_id]
        return failures

    for node_id in node_ids:
        start_agent(node_id)

    tracker_env = {
        **env,
        "CQPCFG_MODEL_PATH": str(model_path),
        "CQPCFG_JOB_SPEC_PATH": str(job_spec_path),
        "CQPCFG_MODEL_ID": args.model_id,
        "CQPCFG_CONTROL_BIND": args.control_bind,
        "CQPCFG_BATCH_BIND": args.batch_bind,
        "CQPCFG_ACK_BIND": args.ack_bind,
        "CQPCFG_CONSUMER_COUNT": str(args.total_nodes if enable_rebalance else initial_plan.consumer_count),
        "CQPCFG_SOURCE_MODE": args.source_mode,
        "CQPCFG_DEMAND_WINDOW": str(args.demand_window),
        "CQPCFG_MAX_CHUNK_SIZE": str(args.max_chunk_size),
        "CQPCFG_MAX_PARALLEL_LEASES_PER_NODE": str(args.max_parallel_leases_per_node),
        "CQPCFG_NODE_AFFINITY_BONUS": str(args.node_affinity_bonus),
        "CQPCFG_BATCH_SIZE": str(args.batch_size),
        "CQPCFG_MAX_BATCH_PAYLOAD_BYTES": str(args.max_batch_payload_bytes),
        "CQPCFG_TIMEOUT_SECONDS": str(args.timeout_seconds),
        "CQPCFG_ACK_TIMEOUT_SECONDS": str(args.ack_timeout_seconds),
        "CQPCFG_ACK_RETRY_INTERVAL_SECONDS": str(args.ack_retry_interval_seconds),
        "CQPCFG_METRICS_PATH": str(tracker_metrics_path),
        "CQPCFG_METRICS_FLUSH_INTERVAL_SECONDS": str(args.metrics_flush_interval_seconds),
    }
    if args.model_serve_bind is not None:
        tracker_env.update(
            {
                "CQPCFG_MODEL_SERVE_BIND": args.model_serve_bind,
                "CQPCFG_MODEL_CHUNK_SIZE": str(args.model_chunk_size),
                "CQPCFG_MODEL_SLOT_PAGE_SIZE": str(args.model_slot_page_size),
                "CQPCFG_MODEL_STRUCTURE_PAGE_SIZE": str(args.model_structure_page_size),
            },
        )
    if args.disable_reclaim:
        tracker_env["CQPCFG_DISABLE_RECLAIM"] = "1"
    if args.disable_node_affinity:
        tracker_env["CQPCFG_DISABLE_NODE_AFFINITY"] = "1"
    sleep(0.2)
    tracker_process = subprocess.Popen(
        [python, str(scripts_dir / "services" / "tracker.py")],
        env=tracker_env,
        text=True,
    )

    failures: list[tuple[list[str] | str, int]] = []
    next_rebalance_at = monotonic() + args.rebalance_interval_seconds
    try:
        while tracker_process.poll() is None:
            role_controller.poll()
            failures.extend(reap_finished())
            now = monotonic()
            if enable_rebalance and now >= next_rebalance_at:
                maybe_rebalance_roles(
                    args=args,
                    allocator=allocator,
                    agents=agents,
                    roles=roles,
                    request_switch=request_switch,
                    tracker_metrics_path=tracker_metrics_path,
                )
                next_rebalance_at = now + args.rebalance_interval_seconds
            sleep(0.05)

        tracker_code = tracker_process.wait()
        if tracker_code != 0:
            failures.append((tracker_process.args, tracker_code))

        role_controller.set_stop(True)
        role_controller.poll(timeout_ms=50)
        deadline = monotonic() + args.timeout_seconds
        while agents and monotonic() < deadline:
            role_controller.poll(timeout_ms=50)
            failures.extend(reap_finished())
            sleep(0.05)
        if agents:
            sleep(0.2)
        for agent in agents.values():
            if agent.process.poll() is None:
                agent.process.terminate()
        for agent in agents.values():
            code = agent.process.wait(timeout=2)
            if code != 0:
                failures.append((agent.process.args, code))
        if failures:
            for command, code in failures:
                print(f"process failed with code {code}: {command}", file=sys.stderr)
            raise SystemExit(1)
        verify_hash_hits(job_spec_path, tuple(all_output_paths))
        print_overhead_summary(
            tracker_metrics_path=tracker_metrics_path,
            node_metrics_paths=tuple(all_metrics_paths),
            role_controller=role_controller,
        )
    finally:
        if tracker_process.poll() is None:
            tracker_process.terminate()
        role_controller.set_stop(True)
        role_controller.poll(timeout_ms=50)
        for agent in agents.values():
            if agent.process.poll() is None:
                agent.process.terminate()
        role_controller.close()


def maybe_rebalance_roles(
    *,
    args: argparse.Namespace,
    allocator: CqdagAwareElasticRoleAllocator,
    agents: dict[str, AgentProcess],
    roles: dict[str, str],
    request_switch,
    tracker_metrics_path: Path,
) -> None:
    role_signals = build_role_signal_snapshot(
        config=RoleSignalConfig(
            total_nodes=args.total_nodes,
            generator_rate=args.generator_rate,
            consumer_rate=args.consumer_rate,
            batch_size=args.batch_size,
            limit=args.limit,
            model_json_page_cache=args.model_json_page_cache,
            role_swap_cost_seconds=max(
                args.role_switch_cooldown_seconds,
                args.rebalance_interval_seconds
                + args.consumer_drain_quiet_ms / 1000.0,
            ),
        ),
        agent_metrics_paths={
            node_id: agent.metrics_path for node_id, agent in agents.items()
        },
        roles=roles,
        tracker_metrics_path=tracker_metrics_path,
    )
    if role_signals is None:
        return

    current_generators = role_signals.current_generators
    snapshot = role_signals.allocation_input
    plan = allocator.plan(snapshot)
    desired_generators = plan.generator_count
    if desired_generators < 1:
        desired_generators = 1
    if desired_generators > args.total_nodes - 1:
        desired_generators = args.total_nodes - 1
    if desired_generators == current_generators:
        return

    current_throughput = allocator.throughput_for(snapshot, current_generators)
    improvement = (
        (plan.expected_throughput - current_throughput) / max(current_throughput, 1e-9)
    )
    desired_delta = desired_generators - current_generators
    pressure_extreme = (
        desired_delta < 0
        and (plan.queue_pressure >= 0.80 or plan.cqdag_reclaim_pressure >= 0.80)
    ) or (
        desired_delta > 0
        and (
            plan.cqdag_frontier_pressure >= 0.60
            or (
                plan.queue_pressure <= 0.05
                and snapshot.consumer_idle_ratio >= 0.50
                and snapshot.generator_idle_ratio <= 0.25
            )
        )
    )
    if improvement < args.role_switch_min_improvement and not pressure_extreme:
        return
    payback = allocator.payback_for(snapshot, plan, current_generators)
    if not payback.should_switch:
        return

    if desired_generators < current_generators:
        for node_id in switch_candidates(
            agents.keys(),
            roles,
            "generator",
            metrics_by_node=role_signals.metrics_by_node,
        ):
            if roles.get(node_id) == "generator":
                if request_switch(node_id, "consumer"):
                    break
    elif desired_generators > current_generators:
        for node_id in switch_candidates(
            agents.keys(),
            roles,
            "consumer",
            metrics_by_node=role_signals.metrics_by_node,
        ):
            if roles.get(node_id) == "consumer":
                if request_switch(node_id, "generator"):
                    break


def use_job_context(args: argparse.Namespace) -> bool:
    return args.model_connect is not None or args.model_serve_bind is not None


def build_job_context(args: argparse.Namespace, job_spec_path: Path) -> JobContext:
    model_connect = args.model_connect
    if model_connect is None:
        if args.model_serve_bind is None:
            raise RuntimeError("model serve endpoint is required for JobContext")
        model_connect = connect_uri_from_bind(args.model_serve_bind)
    return JobContext.from_job_payload(
        read_json(job_spec_path),
        job_id="local-dynamic",
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


def print_overhead_summary(
    *,
    tracker_metrics_path: Path,
    node_metrics_paths: tuple[Path, ...],
    role_controller: RoleController,
) -> None:
    tracker = read_json(tracker_metrics_path) if tracker_metrics_path.exists() else {}
    node_metrics = [read_json(path) for path in node_metrics_paths if path.exists()]

    sent_bytes = int(tracker.get("network_bytes", 0))
    sent_messages = int(tracker.get("network_messages", 0))
    sent_batches = int(tracker.get("network_batch_messages", 0))
    republished_batches = int(tracker.get("ack_republished_batches", 0))
    serialize_seconds = float(tracker.get("network_serialize_seconds", 0.0))
    send_seconds = float(tracker.get("network_send_seconds", 0.0))

    received_bytes = sum(int(item.get("network_bytes", 0)) for item in node_metrics)
    received_messages = sum(int(item.get("network_messages", 0)) for item in node_metrics)
    received_batches = sum(int(item.get("network_batch_messages", 0)) for item in node_metrics)
    recv_seconds = sum(float(item.get("network_recv_seconds", 0.0)) for item in node_metrics)
    deserialize_seconds = sum(
        float(item.get("network_deserialize_seconds", 0.0)) for item in node_metrics
    )
    poll_seconds = sum(float(item.get("network_poll_seconds", 0.0)) for item in node_metrics)
    poll_timeouts = sum(int(item.get("network_poll_timeouts", 0)) for item in node_metrics)
    ack_messages = int(tracker.get("ack_network_messages", 0)) + sum(
        int(item.get("ack_network_messages", 0)) for item in node_metrics
    )
    ack_bytes = int(tracker.get("ack_network_bytes", 0)) + sum(
        int(item.get("ack_network_bytes", 0)) for item in node_metrics
    )
    ack_seconds = (
        float(tracker.get("ack_network_recv_seconds", 0.0))
        + float(tracker.get("ack_network_deserialize_seconds", 0.0))
        + sum(float(item.get("ack_network_send_seconds", 0.0)) for item in node_metrics)
        + sum(float(item.get("ack_network_serialize_seconds", 0.0)) for item in node_metrics)
    )

    role_file_reads = sum(int(item.get("role_file_reads", 0)) for item in node_metrics)
    role_file_read_seconds = sum(
        float(item.get("role_file_read_seconds", 0.0)) for item in node_metrics
    )
    role_control = role_controller.stats
    role_control_messages = role_control.messages + sum(
        int(item.get("role_control_messages", 0)) for item in node_metrics
    )
    role_control_bytes = role_control.bytes + sum(
        int(item.get("role_control_bytes", 0)) for item in node_metrics
    )
    role_control_seconds = (
        role_control.poll_seconds
        + role_control.recv_seconds
        + role_control.send_seconds
        + sum(float(item.get("role_control_seconds", 0.0)) for item in node_metrics)
    )
    role_control_request_seconds = sum(
        float(item.get("role_control_request_seconds", 0.0)) for item in node_metrics
    )
    report_write_count = int(tracker.get("metrics_write_count", 0)) + sum(
        int(item.get("report_write_count", 0)) for item in node_metrics
    )
    report_write_seconds = float(tracker.get("metrics_write_seconds", 0.0)) + sum(
        float(item.get("report_write_seconds", 0.0)) for item in node_metrics
    )

    published_candidates = int(tracker.get("published_candidates", 0))
    bytes_per_candidate = sent_bytes / published_candidates if published_candidates else 0.0
    resident_records = int(tracker.get("chunkstore_resident_records", 0))
    peak_resident_records = int(tracker.get("chunkstore_peak_resident_records", 0))
    reclaimed_records = int(tracker.get("chunkstore_reclaimed_records", 0))
    affinity_hits = int(tracker.get("scheduler_affinity_hits", 0))
    affinity_misses = int(tracker.get("scheduler_affinity_misses", 0))
    source_cached_records = sum(int(item.get("source_cached_records", 0)) for item in node_metrics)
    source_peak_cached_records = sum(
        int(item.get("source_peak_cached_records", 0)) for item in node_metrics
    )
    source_reclaimed_records = sum(
        int(item.get("source_reclaimed_records", 0)) for item in node_metrics
    )
    source_dag_repository_active_units = sum(
        int(item.get("source_dag_repository_active_units", 0)) for item in node_metrics
    )
    source_dag_stream_active_units = sum(
        int(item.get("source_dag_stream_active_units", 0)) for item in node_metrics
    )
    drained_batches = sum(int(item.get("drained_batches", 0)) for item in node_metrics)
    drained_candidates = sum(
        int(item.get("drained_candidates", 0)) for item in node_metrics
    )
    drain_timeouts = sum(int(item.get("drain_timeouts", 0)) for item in node_metrics)
    model_loaded_nodes = sum(1 for item in node_metrics if item.get("model_loaded_once"))

    print("network/io overhead summary")
    print(f"  model-loaded nodes       : {model_loaded_nodes}/{len(node_metrics)}")
    print(f"  chunk resident records  : {resident_records}")
    print(f"  chunk peak resident     : {peak_resident_records}")
    print(f"  chunk reclaimed records : {reclaimed_records}")
    print(f"  affinity hits           : {affinity_hits}")
    print(f"  affinity misses         : {affinity_misses}")
    print(f"  source cached records   : {source_cached_records}")
    print(f"  source peak cache       : {source_peak_cached_records}")
    print(f"  source reclaimed records: {source_reclaimed_records}")
    print(f"  source repo active units: {source_dag_repository_active_units}")
    print(f"  source root active units: {source_dag_stream_active_units}")
    print(f"  drained batches          : {drained_batches}")
    print(f"  drained candidates       : {drained_candidates}")
    print(f"  drain timeouts           : {drain_timeouts}")
    print(f"  data sent messages       : {sent_messages} ({sent_batches} batches)")
    print(f"  republished batches      : {republished_batches}")
    print(f"  data sent bytes          : {sent_bytes}")
    print(f"  data bytes/candidate     : {bytes_per_candidate:.3f}")
    print(f"  send serialize seconds   : {serialize_seconds:.6f}")
    print(f"  socket send seconds      : {send_seconds:.6f}")
    print(f"  data recv messages       : {received_messages} ({received_batches} batches)")
    print(f"  data recv bytes          : {received_bytes}")
    print(f"  socket recv seconds      : {recv_seconds:.6f}")
    print(f"  recv deserialize seconds : {deserialize_seconds:.6f}")
    print(f"  recv poll seconds        : {poll_seconds:.6f}")
    print(f"  recv poll timeouts       : {poll_timeouts}")
    print(f"  ack messages             : {ack_messages}")
    print(f"  ack bytes                : {ack_bytes}")
    print(f"  ack seconds              : {ack_seconds:.6f}")
    print(f"  role file reads          : {role_file_reads}")
    print(f"  role file read seconds   : {role_file_read_seconds:.6f}")
    print(f"  role control messages    : {role_control_messages}")
    print(f"  role control bytes       : {role_control_bytes}")
    print(f"  role control seconds     : {role_control_seconds:.6f}")
    print(f"  role request seconds     : {role_control_request_seconds:.6f}")
    print(f"  report writes            : {report_write_count}")
    print(f"  report write seconds     : {report_write_seconds:.6f}")


if __name__ == "__main__":
    main()
