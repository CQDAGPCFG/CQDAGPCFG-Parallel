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

from common import ensure_project_paths, experiment_src_dir, read_json, repo_root, write_json

ensure_project_paths()

from cqdagpcfg_parallel.distributed import (
    CqdagAwareElasticRoleAllocator,
    JobContext,
    RoleAllocationInput,
    RoleController,
)
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the split-process CQDAGPCFG E2E pipeline locally.",
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
    model_path = work_dir / "model.json"
    targets_path = work_dir / "targets.json"

    env = os.environ.copy()
    env["PYTHONPATH"] = (
        f"{scripts_dir}:{root / 'src'}:{root.parent}:{env.get('PYTHONPATH', '')}"
    )
    python = sys.executable

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
    print(f"  throughput: {role_plan.expected_throughput:.3f}")

    if args.dynamic_rebalance:
        run_dynamic_rebalanced(
            args=args,
            env=env,
            python=python,
            scripts_dir=scripts_dir,
            model_path=model_path,
            targets_path=targets_path,
            work_dir=work_dir,
            initial_plan=role_plan,
        )
        if work_dir_context is not None:
            work_dir_context.cleanup()
        return

    hit_paths = tuple(work_dir / f"hits-{index}.json" for index in range(role_plan.consumer_count))
    consumer_cmds = [
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
        ]
        for index in range(role_plan.consumer_count)
    ]
    tracker_cmd = [
        python,
        str(scripts_dir / "tracker.py"),
        "--model-path",
        str(model_path),
        "--targets-path",
        str(targets_path),
        "--model-id",
        args.model_id,
        "--control-bind",
        args.control_bind,
        "--batch-bind",
        args.batch_bind,
        "--ack-bind",
        args.ack_bind,
        "--consumer-count",
        str(role_plan.consumer_count),
        "--expected-workers",
        str(role_plan.generator_count),
        "--demand-window",
        str(args.demand_window),
        "--max-chunk-size",
        str(args.max_chunk_size),
        "--max-parallel-leases-per-node",
        str(args.max_parallel_leases_per_node),
        "--node-affinity-bonus",
        str(args.node_affinity_bonus),
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
    ]
    if args.model_serve_bind is not None:
        tracker_cmd.extend(
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
    if args.disable_reclaim:
        tracker_cmd.append("--disable-reclaim")
    if args.disable_node_affinity:
        tracker_cmd.append("--disable-node-affinity")
    worker_cmds = [
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
            f"worker-{index}",
            "--demand-window",
            str(args.demand_window),
            "--work-delay-seconds",
            str(args.worker_delay_seconds),
        ]
        for index in range(role_plan.generator_count)
    ]

    processes: list[subprocess.Popen[str]] = []
    try:
        for command in consumer_cmds:
            processes.append(subprocess.Popen(command, env=env, text=True))
        sleep(0.2)
        processes.append(subprocess.Popen(tracker_cmd, env=env, text=True))
        for command in worker_cmds:
            processes.append(subprocess.Popen(command, env=env, text=True))
        failures = []
        for process in processes:
            code = process.wait()
            if code != 0:
                failures.append((process.args, code))
        if failures:
            for command, code in failures:
                print(f"process failed with code {code}: {command}", file=sys.stderr)
            raise SystemExit(1)
        verify_hash_hits(targets_path, hit_paths)
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        if work_dir_context is not None:
            work_dir_context.cleanup()


def run_checked(command: list[str], *, env: dict[str, str]) -> None:
    subprocess.run(command, env=env, check=True, text=True)


def verify_hash_hits(targets_path: Path, hit_paths: tuple[Path, ...]) -> None:
    targets = read_json(targets_path)
    expected_guesses = {str(target["guess"]) for target in targets["targets"]}
    all_hits = []
    for path in hit_paths:
        if not path.exists():
            raise SystemExit(f"missing consumer hit report: {path}")
        all_hits.extend(read_json(path)["hits"])
    found_guesses = {str(hit["guess"]) for hit in all_hits}
    missing = sorted(expected_guesses - found_guesses)
    if missing:
        raise SystemExit(f"hash consumers missed target guesses: {missing}")
    print("local hash consumers verified")
    print(f"  consumers: {len(hit_paths)}")
    print(f"  hits     : {len(all_hits)}")


@dataclass(slots=True)
class AgentProcess:
    node_id: str
    process: subprocess.Popen[str]
    metrics_path: Path
    hits_path: Path


@dataclass(slots=True)
class RoleStabilityState:
    last_global_switch_at: float
    last_switch_by_node: dict[str, float]


def run_dynamic_rebalanced(
    *,
    args: argparse.Namespace,
    env: dict[str, str],
    python: str,
    scripts_dir: Path,
    model_path: Path,
    targets_path: Path,
    work_dir: Path,
    initial_plan,
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
        job_context=build_job_context(args, targets_path) if use_job_context(args) else None,
    )

    agents: dict[str, AgentProcess] = {}
    all_hit_paths: list[Path] = []
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
        hits_path = work_dir / f"hits-{node_id}.json"
        all_metrics_paths.append(metrics_path)
        all_hit_paths.append(hits_path)
        command = [
            python,
            str(scripts_dir / "node_agent.py"),
            "--node-id",
            node_id,
            "--role-connect",
            args.role_connect,
            "--work-delay-seconds",
            str(args.worker_delay_seconds),
            "--hash-delay-seconds",
            str(args.hash_delay_seconds),
            "--consumer-drain-quiet-ms",
            str(args.consumer_drain_quiet_ms),
            "--consumer-drain-timeout-ms",
            str(args.consumer_drain_timeout_ms),
            "--metrics-flush-interval-seconds",
            str(args.metrics_flush_interval_seconds),
            "--metrics-path",
            str(metrics_path),
            "--hits-path",
            str(hits_path),
        ]
        command.extend(["--model-json-page-cache", str(args.model_json_page_cache)])
        if use_job_context(args):
            if args.model_cache_dir is not None:
                command.extend(["--model-cache-dir", str(args.model_cache_dir)])
        elif args.model_connect is not None:
            command.extend(
                [
                    "--targets-path",
                    str(targets_path),
                    "--control-connect",
                    args.control_connect,
                    "--batch-connect",
                    args.batch_connect,
                    "--ack-connect",
                    args.ack_connect,
                    "--demand-window",
                    str(args.demand_window),
                    "--model-connect",
                    args.model_connect,
                    "--model-id",
                    args.model_id,
                ]
            )
            if args.model_cache_dir is not None:
                command.extend(["--model-cache-dir", str(args.model_cache_dir)])
        else:
            command.extend(
                [
                    "--targets-path",
                    str(targets_path),
                    "--control-connect",
                    args.control_connect,
                    "--batch-connect",
                    args.batch_connect,
                    "--ack-connect",
                    args.ack_connect,
                    "--demand-window",
                    str(args.demand_window),
                    "--model-path",
                    str(model_path),
                ]
            )
        agents[node_id] = AgentProcess(
            node_id=node_id,
            process=subprocess.Popen(command, env=env, text=True),
            metrics_path=metrics_path,
            hits_path=hits_path,
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

    tracker_cmd = [
        python,
        str(scripts_dir / "tracker.py"),
        "--model-path",
        str(model_path),
        "--targets-path",
        str(targets_path),
        "--model-id",
        args.model_id,
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
        "--node-affinity-bonus",
        str(args.node_affinity_bonus),
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
        str(tracker_metrics_path),
        "--metrics-flush-interval-seconds",
        str(args.metrics_flush_interval_seconds),
    ]
    if args.model_serve_bind is not None:
        tracker_cmd.extend(
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
    if args.disable_reclaim:
        tracker_cmd.append("--disable-reclaim")
    if args.disable_node_affinity:
        tracker_cmd.append("--disable-node-affinity")
    sleep(0.2)
    tracker_process = subprocess.Popen(tracker_cmd, env=env, text=True)

    failures: list[tuple[list[str] | str, int]] = []
    next_rebalance_at = monotonic() + args.rebalance_interval_seconds
    try:
        while tracker_process.poll() is None:
            role_controller.poll()
            failures.extend(reap_finished())
            now = monotonic()
            if now >= next_rebalance_at:
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
        verify_hash_hits(targets_path, tuple(all_hit_paths))
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
    current_generators = sum(
        1 for node_id in agents if roles.get(node_id) == "generator"
    )
    current_consumers = sum(
        1 for node_id in agents if roles.get(node_id) == "consumer"
    )
    if current_generators <= 0 or current_consumers <= 0:
        return

    tracker_metrics = read_json(tracker_metrics_path) if tracker_metrics_path.exists() else {}
    published = int(tracker_metrics.get("published_candidates", 0))
    generation_rate = float(tracker_metrics.get("candidate_rate", 0.0))
    consumed = 0
    consumer_rate = 0.0
    consumer_idle_seconds = 0.0
    consumer_elapsed_seconds = 0.0
    generator_waits = 0
    generator_completed_items = 0
    source_cached_records = 0
    source_peak_cached_records = 0
    source_reclaimed_records = 0
    source_repository_units = 0
    source_stream_units = 0
    metrics_by_node: dict[str, dict] = {}
    for node_id, agent in agents.items():
        path = agent.metrics_path
        if not path.exists():
            continue
        payload = read_json(path)
        metrics_by_node[node_id] = payload
        consumed += int(payload.get("consumed_candidates", 0))
        if roles.get(node_id) == "consumer":
            consumer_rate += float(payload.get("consumer_rate", 0.0))
            consumer_idle_seconds += float(payload.get("network_poll_seconds", 0.0))
            consumer_elapsed_seconds += float(payload.get("elapsed_seconds", 0.0))
        elif roles.get(node_id) == "generator":
            generator_waits += int(payload.get("waits", 0))
            generator_completed_items += int(payload.get("completed_items", 0))
            source_cached_records += int(payload.get("source_cached_records", 0))
            source_peak_cached_records += int(payload.get("source_peak_cached_records", 0))
            source_reclaimed_records += int(payload.get("source_reclaimed_records", 0))
            source_repository_units += int(payload.get("source_dag_repository_active_units", 0))
            source_stream_units += int(payload.get("source_dag_stream_active_units", 0))

    if published <= 0 and consumed <= 0:
        return

    pending = max(0, published - consumed)
    generator_rate_per_node = (
        generation_rate / current_generators if generation_rate > 0.0 else args.generator_rate
    )
    consumer_rate_per_node = (
        consumer_rate / current_consumers if consumer_rate > 0.0 else args.consumer_rate
    )
    max_pending = args.batch_size * max(1, current_consumers) * 4
    queue_pressure = _bounded_ratio(pending, max_pending)
    generator_idle_ratio = _bounded_ratio(
        generator_waits,
        generator_waits + generator_completed_items,
    )
    consumer_idle_ratio = _bounded_ratio(
        consumer_idle_seconds,
        consumer_elapsed_seconds,
    )
    source_cache_pressure = _bounded_ratio(
        source_cached_records,
        max(source_cached_records, source_peak_cached_records, 1),
    )
    page_locality = _bounded_ratio(
        source_repository_units + source_stream_units,
        max(1, current_generators * args.model_json_page_cache),
    )
    frontier_pressure = _bounded_ratio(
        (1.0 - queue_pressure) * consumer_idle_ratio
        + max(0.0, consumer_rate_per_node - generator_rate_per_node)
        / max(generator_rate_per_node + consumer_rate_per_node, 1e-9),
        1.0,
    )
    priority_pressure = 1.0 - _bounded_ratio(published, max(args.limit, 1))
    reclaim_pressure = max(
        queue_pressure,
        source_cache_pressure
        * (1.0 - _bounded_ratio(source_reclaimed_records, source_reclaimed_records + source_cached_records)),
    )
    snapshot = RoleAllocationInput(
        total_nodes=args.total_nodes,
        generator_rate_per_node=max(generator_rate_per_node, 1e-9),
        consumer_rate_per_node=max(consumer_rate_per_node, 1e-9),
        current_generator_count=current_generators,
        pending_candidates=pending,
        max_pending_candidates=max_pending,
        generator_idle_ratio=generator_idle_ratio,
        consumer_idle_ratio=consumer_idle_ratio,
        migration_cost_per_role_swap=args.batch_size,
        cqdag_frontier_pressure=frontier_pressure,
        cqdag_priority_pressure=priority_pressure,
        cqdag_reclaim_pressure=reclaim_pressure,
        cqdag_page_locality=page_locality,
    )
    plan = allocator.plan(
        snapshot
    )
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

    if desired_generators < current_generators:
        for node_id in _switch_candidates(
            agents,
            roles,
            "generator",
            metrics_by_node=metrics_by_node,
        ):
            if roles.get(node_id) == "generator":
                if request_switch(node_id, "consumer"):
                    break
    elif desired_generators > current_generators:
        for node_id in _switch_candidates(
            agents,
            roles,
            "consumer",
            metrics_by_node=metrics_by_node,
        ):
            if roles.get(node_id) == "consumer":
                if request_switch(node_id, "generator"):
                    break


def _switch_candidates(
    agents: dict[str, AgentProcess],
    roles: dict[str, str],
    role: str,
    *,
    metrics_by_node: dict[str, dict] | None = None,
) -> tuple[str, ...]:
    metrics_by_node = {} if metrics_by_node is None else metrics_by_node
    node_ids = tuple(node_id for node_id in agents if roles.get(node_id) == role)
    if role == "generator":
        return tuple(
            sorted(
                node_ids,
                key=lambda node_id: (
                    int(
                        metrics_by_node.get(node_id, {}).get(
                            "source_dag_repository_active_units",
                            0,
                        )
                    )
                    + int(
                        metrics_by_node.get(node_id, {}).get(
                            "source_dag_stream_active_units",
                            0,
                        )
                    )
                    + int(
                        metrics_by_node.get(node_id, {}).get(
                            "source_cached_records",
                            0,
                        )
                    ),
                    int(metrics_by_node.get(node_id, {}).get("completed_records", 0)),
                    node_id,
                ),
            )
        )
    if role == "consumer":
        return tuple(
            sorted(
                node_ids,
                key=lambda node_id: (
                    -_bounded_ratio(
                        float(
                            metrics_by_node.get(node_id, {}).get(
                                "network_poll_seconds",
                                0.0,
                            )
                        ),
                        float(metrics_by_node.get(node_id, {}).get("elapsed_seconds", 0.0)),
                    ),
                    float(metrics_by_node.get(node_id, {}).get("consumer_rate", 0.0)),
                    int(metrics_by_node.get(node_id, {}).get("consumed_candidates", 0)),
                    node_id,
                ),
            )
        )
    return node_ids


def _bounded_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return min(1.0, max(0.0, numerator / denominator))


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
        job_id="local-dynamic",
        model_id=args.model_id,
        model_connect=model_connect,
        control_connect=args.control_connect,
        batch_connect=args.batch_connect,
        ack_connect=args.ack_connect,
        source_mode="root",
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
    print(f"  report writes            : {report_write_count}")
    print(f"  report write seconds     : {report_write_seconds:.6f}")


if __name__ == "__main__":
    main()
