from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from .common import ensure_project_paths


def cqdag_tracker_config_from_args(argv: Sequence[str] | None = None):
    """Build the experiment tracker config from CLI arguments.

    CLI parsing stays in the experiment layer. The library receives an explicit
    config object and does not know whether it came from CLI, env, a file, or an
    orchestrator.
    """

    ensure_project_paths()

    from cqdagpcfg_parallel.adapters.cqdagpcfg import CqdagTrackerServiceConfig

    args = _parse_args(argv)
    return CqdagTrackerServiceConfig(
        model_path=args.model_path,
        targets_path=args.targets_path,
        model_id=args.model_id,
        model_serve_bind=args.model_serve_bind,
        model_chunk_size=args.model_chunk_size,
        model_slot_page_size=args.model_slot_page_size,
        model_structure_page_size=args.model_structure_page_size,
        bind=args.bind,
        advertise_host=args.advertise_host,
        control_bind=args.control_bind,
        public_control_connect=args.public_control_connect,
        batch_bind=args.batch_bind,
        batch_connect=args.batch_connect,
        public_batch_connect=args.public_batch_connect,
        ack_bind=args.ack_bind,
        public_ack_connect=args.public_ack_connect,
        public_model_connect=args.public_model_connect,
        role_bind=args.role_bind,
        total_nodes=args.total_nodes,
        min_generators=args.min_generators,
        min_consumers=args.min_consumers,
        initial_generators=args.initial_generators,
        initial_consumers=args.initial_consumers,
        late_worker_role=args.late_worker_role,
        generator_min_cpus=args.generator_min_cpus,
        generator_min_memory=args.generator_min_memory,
        generator_min_gpus=args.generator_min_gpus,
        consumer_min_cpus=args.consumer_min_cpus,
        consumer_min_memory=args.consumer_min_memory,
        consumer_min_gpus=args.consumer_min_gpus,
        consumer_count=args.consumer_count,
        ack_timeout_seconds=args.ack_timeout_seconds,
        ack_retry_interval_seconds=args.ack_retry_interval_seconds,
        batch_startup_grace_seconds=args.batch_startup_grace_seconds,
        expected_workers=args.expected_workers,
        shutdown_grace_seconds=args.shutdown_grace_seconds,
        metrics_path=args.metrics_path,
        metrics_flush_interval_seconds=args.metrics_flush_interval_seconds,
        checkpoint_path=args.checkpoint_path,
        resume_checkpoint_path=args.resume_checkpoint_path,
        checkpoint_stable_log_path=args.checkpoint_stable_log_path,
        checkpoint_interval_records=args.checkpoint_interval_records,
        batch_checkpoint_path=args.batch_checkpoint_path,
        resume_batch_checkpoint_path=args.resume_batch_checkpoint_path,
        source_mode=args.source_mode,
        demand_window=args.demand_window,
        max_chunk_size=args.max_chunk_size,
        max_parallel_leases_per_node=args.max_parallel_leases_per_node,
        disable_node_affinity=args.disable_node_affinity,
        node_affinity_bonus=args.node_affinity_bonus,
        batch_size=args.batch_size,
        max_batch_payload_bytes=args.max_batch_payload_bytes,
        timeout_seconds=args.timeout_seconds,
        disable_reclaim=args.disable_reclaim,
    )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the CQDAGPCFG E2E tracker and publish CandidateBatch data.",
    )
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--model-id", default="cqdagpcfg-e2e-model")
    parser.add_argument("--model-serve-bind", default=None)
    parser.add_argument("--model-chunk-size", type=int, default=1 << 20)
    parser.add_argument("--model-slot-page-size", type=int, default=1024)
    parser.add_argument("--model-structure-page-size", type=int, default=4096)
    parser.add_argument("--targets-path", type=Path, required=True)
    parser.add_argument(
        "--bind",
        default=None,
        help=(
            "Base protocol endpoint. Expands to control, batch, role, ack, and "
            "model subchannels on consecutive ports."
        ),
    )
    parser.add_argument("--advertise-host", default="127.0.0.1")
    parser.add_argument("--control-bind", default="cqpcfg://0.0.0.0:5555")
    parser.add_argument("--public-control-connect", default=None)
    parser.add_argument("--batch-bind", default=None)
    parser.add_argument("--batch-connect", default="cqpcfg://127.0.0.1:5556")
    parser.add_argument("--public-batch-connect", default=None)
    parser.add_argument("--ack-bind", default="cqpcfg://0.0.0.0:5558")
    parser.add_argument("--public-ack-connect", default=None)
    parser.add_argument("--public-model-connect", default=None)
    parser.add_argument("--role-bind", default=None)
    parser.add_argument("--total-nodes", type=int, default=None)
    parser.add_argument("--min-generators", type=int, default=1)
    parser.add_argument("--min-consumers", type=int, default=1)
    parser.add_argument("--initial-generators", type=int, default=None)
    parser.add_argument("--initial-consumers", type=int, default=None)
    parser.add_argument(
        "--late-worker-role",
        choices=("generator", "consumer", "idle"),
        default="generator",
    )
    parser.add_argument("--generator-min-cpus", type=float, default=None)
    parser.add_argument("--generator-min-memory", default=None)
    parser.add_argument("--generator-min-gpus", type=int, default=None)
    parser.add_argument("--consumer-min-cpus", type=float, default=None)
    parser.add_argument("--consumer-min-memory", default=None)
    parser.add_argument("--consumer-min-gpus", type=int, default=None)
    parser.add_argument("--consumer-count", type=int, default=None)
    parser.add_argument("--ack-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--ack-retry-interval-seconds", type=float, default=5.0)
    parser.add_argument("--batch-startup-grace-seconds", type=float, default=0.2)
    parser.add_argument("--expected-workers", type=int, default=None)
    parser.add_argument("--shutdown-grace-seconds", type=float, default=0.5)
    parser.add_argument("--metrics-path", type=Path, default=None)
    parser.add_argument("--metrics-flush-interval-seconds", type=float, default=0.25)
    parser.add_argument("--checkpoint-path", type=Path, default=None)
    parser.add_argument("--resume-checkpoint-path", type=Path, default=None)
    parser.add_argument("--checkpoint-stable-log-path", type=Path, default=None)
    parser.add_argument("--checkpoint-interval-records", type=int, default=1)
    parser.add_argument("--batch-checkpoint-path", type=Path, default=None)
    parser.add_argument("--resume-batch-checkpoint-path", type=Path, default=None)
    parser.add_argument("--source-mode", choices=("root", "structure"), default="root")
    parser.add_argument("--demand-window", type=int, default=8)
    parser.add_argument("--max-chunk-size", type=int, default=32)
    parser.add_argument("--max-parallel-leases-per-node", type=int, default=2)
    parser.add_argument("--disable-node-affinity", action="store_true")
    parser.add_argument("--node-affinity-bonus", type=float, default=0.5)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--max-batch-payload-bytes", type=int, default=4096)
    parser.add_argument("--timeout-seconds", type=float, default=3600.0)
    parser.add_argument("--disable-reclaim", action="store_true")
    return parser.parse_args(argv)
