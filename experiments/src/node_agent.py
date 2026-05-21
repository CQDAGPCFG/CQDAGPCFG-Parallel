#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path
from time import monotonic, perf_counter, sleep
from typing import Any

from common import digest_guess, ensure_project_paths, read_json, write_json

ensure_project_paths()

from CQDAGPCFG import load_model

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGBlockGraphAdapter,
    CQDAGRecordSource,
    CQDAGStructureRecordSource,
    PagedCQDAGRecordSource,
    PagedCQDAGStructureRecordSource,
    build_paged_model,
)
from cqdagpcfg_parallel.distributed import JobContext, NodeAgent, NodeAgentStats, RoleClient
from cqdagpcfg_parallel.runtime import (
    CandidateBatch,
    LazyLocalResultSource,
    ZmqModelArtifactClient,
)
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint
from cqdagpcfg_parallel.storage import FileModelArtifactCache, file_model_fingerprint


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a persistent CQDAGPCFG node agent with in-process role switching.",
    )
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--role-connect", required=True)
    parser.add_argument("--model-path", type=Path, default=None)
    parser.add_argument("--model-connect", default=None)
    parser.add_argument("--model-id", default="cqdagpcfg-e2e-model")
    parser.add_argument("--model-cache-dir", type=Path, default=None)
    parser.add_argument("--model-json-page-cache", type=int, default=128)
    parser.add_argument("--disable-paged-source", action="store_true")
    parser.add_argument("--targets-path", type=Path, default=None)
    parser.add_argument("--source-mode", choices=("root", "structure"), default="root")
    parser.add_argument("--control-connect", default="cqpcfg://127.0.0.1:5555")
    parser.add_argument("--batch-connect", default="cqpcfg://127.0.0.1:5556")
    parser.add_argument("--ack-connect", default="cqpcfg://127.0.0.1:5558")
    parser.add_argument("--demand-window", type=int, default=8)
    parser.add_argument("--work-delay-seconds", type=float, default=0.0)
    parser.add_argument("--hash-delay-seconds", type=float, default=0.0)
    parser.add_argument("--receive-timeout-ms", type=int, default=100)
    parser.add_argument("--consumer-drain-quiet-ms", type=int, default=200)
    parser.add_argument("--consumer-drain-timeout-ms", type=int, default=2000)
    parser.add_argument("--idle-sleep-seconds", type=float, default=0.01)
    parser.add_argument("--role-refresh-interval-seconds", type=float, default=0.05)
    parser.add_argument("--role-reply-timeout-ms", type=int, default=100)
    parser.add_argument("--job-bootstrap-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--metrics-flush-interval-seconds", type=float, default=0.25)
    parser.add_argument("--experiment-start-monotonic", type=float, default=None)
    parser.add_argument("--metrics-path", type=Path, required=True)
    parser.add_argument("--hits-path", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.hash_delay_seconds < 0.0:
        raise SystemExit("--hash-delay-seconds cannot be negative")
    if args.model_json_page_cache <= 0:
        raise SystemExit("--model-json-page-cache must be positive")

    role_client = RoleClient(
        node_id=args.node_id,
        endpoint=ZmqEndpoint.from_uri(args.role_connect, bind=False),
        reply_timeout_ms=args.role_reply_timeout_ms,
    )
    job_context = None
    if args.targets_path is None:
        job_context = wait_for_job_context(args=args, role_client=role_client)
        targets = targets_from_job_context(job_context)
    else:
        targets = read_json(args.targets_path)
    limit = int(targets["limit"])
    job_settings = resolve_job_settings(args=args, job_context=job_context)
    source = LazyLocalResultSource(
        lambda: build_cqdag_source(
            settings=job_settings,
            model_cache_dir=args.model_cache_dir,
            limit=limit,
            expected_fingerprint=targets.get("model_fingerprint"),
        )
    )

    consumer = HashBatchConsumer(
        node_id=args.node_id,
        targets=targets,
        hash_delay_seconds=args.hash_delay_seconds,
        started_at=args.experiment_start_monotonic,
    )
    reporter = NodeAgentReporter(
        metrics_path=args.metrics_path,
        hits_path=args.hits_path,
        consumer=consumer,
        algorithm=str(targets["algorithm"]),
        limit=limit,
    )

    agent = NodeAgent(
        node_id=args.node_id,
        role_client=role_client,
        control_endpoint=ZmqEndpoint.from_uri(job_settings.control_connect, bind=False),
        batch_endpoint=ZmqEndpoint.from_uri(job_settings.batch_connect, bind=False),
        ack_endpoint=ZmqEndpoint.from_uri(job_settings.ack_connect, bind=False),
        source=source,
        consume_batch=consumer.consume,
        model_fingerprint=targets.get("model_fingerprint"),
        work_delay_seconds=args.work_delay_seconds,
        receive_timeout_ms=args.receive_timeout_ms,
        consumer_drain_quiet_ms=args.consumer_drain_quiet_ms,
        consumer_drain_timeout_ms=args.consumer_drain_timeout_ms,
        idle_sleep_seconds=args.idle_sleep_seconds,
        role_refresh_interval_seconds=args.role_refresh_interval_seconds,
        stats_flush_interval_seconds=args.metrics_flush_interval_seconds,
        stats_callback=reporter.write,
    )
    stats = agent.run()

    print("node agent completed")
    print(f"  node id            : {stats.node_id}")
    print(f"  role switches      : {stats.role_switches}")
    print(f"  completed records  : {stats.completed_records}")
    print(f"  consumed candidates: {stats.consumed_candidates}")
    print(f"  hits               : {len(consumer.hits)}")


def build_cqdag_source(
    *,
    settings: "NodeJobSettings",
    model_cache_dir: Path | None,
    limit: int,
    expected_fingerprint: str | None,
):
    if settings.model_path is None and not settings.disable_paged_source:
        model = build_paged_model(
            endpoint=settings.model_connect,
            model_id=settings.model_id,
            max_json_pages=settings.model_json_page_cache,
        )
        if (
            expected_fingerprint is not None
            and model.paged_manifest.model_fingerprint != expected_fingerprint
        ):
            raise RuntimeError("paged model fingerprint does not match targets")
        if settings.source_mode == "structure":
            return PagedCQDAGStructureRecordSource(
                model,
                max_records_per_structure=limit + settings.demand_window,
            )
        return PagedCQDAGRecordSource(
            model,
            max_records=limit + settings.demand_window,
        )

    model_path = resolve_model_path(
        settings,
        model_cache_dir=model_cache_dir,
        expected_fingerprint=expected_fingerprint,
    )
    model = load_model(model_path)
    adapter = CQDAGBlockGraphAdapter(model)
    if settings.source_mode == "structure":
        return CQDAGStructureRecordSource(
            model,
            max_records_per_structure=limit + settings.demand_window,
            adapter=adapter,
        )
    return CQDAGRecordSource(
        model,
        max_records=limit + settings.demand_window,
    )


def resolve_model_path(
    settings: "NodeJobSettings",
    *,
    model_cache_dir: Path | None,
    expected_fingerprint: str | None,
) -> Path:
    if settings.model_path is not None:
        if (
            expected_fingerprint is not None
            and file_model_fingerprint(settings.model_path) != expected_fingerprint
        ):
            raise RuntimeError("local model fingerprint does not match targets")
        return settings.model_path
    cache_dir = model_cache_dir
    if cache_dir is None:
        cache_dir = Path.home() / ".cache" / "cqdagpcfg_parallel" / "models"
    cache = FileModelArtifactCache(cache_dir)
    with ZmqModelArtifactClient(
        ZmqEndpoint.from_uri(settings.model_connect, bind=False)
    ) as client:
        model_path, manifest = cache.materialize(client, settings.model_id)
    if (
        expected_fingerprint is not None
        and manifest.model_fingerprint != expected_fingerprint
    ):
        raise RuntimeError("fetched model fingerprint does not match targets")
    return model_path


class NodeJobSettings:
    def __init__(
        self,
        *,
        model_path: Path | None,
        model_connect: str,
        model_id: str,
        source_mode: str,
        demand_window: int,
        control_connect: str,
        batch_connect: str,
        ack_connect: str,
        disable_paged_source: bool,
        model_json_page_cache: int,
    ) -> None:
        self.model_path = model_path
        self.model_connect = model_connect
        self.model_id = model_id
        self.source_mode = source_mode
        self.demand_window = demand_window
        self.control_connect = control_connect
        self.batch_connect = batch_connect
        self.ack_connect = ack_connect
        self.disable_paged_source = disable_paged_source
        self.model_json_page_cache = model_json_page_cache


def resolve_job_settings(
    *,
    args: argparse.Namespace,
    job_context: JobContext | None,
) -> NodeJobSettings:
    if job_context is not None:
        return NodeJobSettings(
            model_path=None,
            model_connect=job_context.model_connect,
            model_id=job_context.model_id,
            source_mode=job_context.source_mode,
            demand_window=job_context.demand_window,
            control_connect=job_context.control_connect,
            batch_connect=job_context.batch_connect,
            ack_connect=job_context.ack_connect,
            disable_paged_source=args.disable_paged_source,
            model_json_page_cache=args.model_json_page_cache,
        )
    if args.model_path is None and args.model_connect is None:
        raise RuntimeError("node agent requires --model-path, --model-connect, or JobContext")
    if args.model_connect is None:
        model_connect = "cqpcfg://127.0.0.1:0"
    else:
        model_connect = args.model_connect
    return NodeJobSettings(
        model_path=args.model_path,
        model_connect=model_connect,
        model_id=args.model_id,
        source_mode=args.source_mode,
        demand_window=args.demand_window,
        control_connect=args.control_connect,
        batch_connect=args.batch_connect,
        ack_connect=args.ack_connect,
        disable_paged_source=args.disable_paged_source,
        model_json_page_cache=args.model_json_page_cache,
    )


def wait_for_job_context(
    *,
    args: argparse.Namespace,
    role_client: RoleClient,
) -> JobContext:
    if args.job_bootstrap_timeout_seconds < 0.0:
        raise RuntimeError("--job-bootstrap-timeout-seconds cannot be negative")
    deadline = monotonic() + args.job_bootstrap_timeout_seconds
    while True:
        reply = role_client.request(
            {
                "current_role": "bootstrap",
                "completed_records": 0,
                "consumed_candidates": 0,
                "role_switches": 0,
            }
        )
        if reply.job_context is not None:
            return reply.job_context
        if monotonic() >= deadline:
            raise RuntimeError("timed out waiting for tracker JobContext")
        sleep(min(args.role_refresh_interval_seconds, 0.1))


def targets_from_job_context(job_context: JobContext) -> dict[str, Any]:
    return {
        "algorithm": job_context.hash_algorithm,
        "limit": job_context.limit,
        "model_fingerprint": job_context.model_fingerprint,
        "targets": [dict(target) for target in job_context.targets],
    }


class HashBatchConsumer:
    def __init__(
        self,
        *,
        node_id: str,
        targets: dict,
        hash_delay_seconds: float,
        started_at: float | None,
    ) -> None:
        self.node_id = node_id
        self.started_at = monotonic() if started_at is None else started_at
        self.algorithm = str(targets["algorithm"])
        self.hash_delay_seconds = hash_delay_seconds
        self.target_by_hash: dict[str, list[dict]] = {}
        for target in targets["targets"]:
            self.target_by_hash.setdefault(str(target["hash"]), []).append(dict(target))
        self.hits: list[dict] = []

    def consume(self, batch: CandidateBatch) -> None:
        for offset, record in enumerate(batch.records):
            if self.hash_delay_seconds:
                sleep(self.hash_delay_seconds)
            digest = digest_guess(record.guess, algorithm=self.algorithm)
            for target in self.target_by_hash.get(digest, ()):
                self.hits.append(
                    {
                        "rank": batch.start_rank + offset,
                        "target_rank": int(target["rank"]),
                        "batch_id": batch.batch_id,
                        "guess": record.guess,
                        "hash": digest,
                        "node_id": self.node_id,
                        "elapsed_seconds": monotonic() - self.started_at,
                    }
                )


class NodeAgentReporter:
    def __init__(
        self,
        *,
        metrics_path: Path,
        hits_path: Path,
        consumer: HashBatchConsumer,
        algorithm: str,
        limit: int,
    ) -> None:
        self.metrics_path = metrics_path
        self.hits_path = hits_path
        self.consumer = consumer
        self.algorithm = algorithm
        self.limit = limit
        self.report_write_count = 0
        self.report_write_seconds = 0.0

    def write(self, stats: NodeAgentStats) -> None:
        started_at = perf_counter()
        metrics = {
            "role": "node_agent",
            **asdict(stats),
            "hits": len(self.consumer.hits),
            "role_file_reads": 0,
            "role_file_read_seconds": 0.0,
            "report_write_count": self.report_write_count,
            "report_write_seconds": self.report_write_seconds,
        }
        write_json(self.metrics_path, metrics)
        metrics_write_seconds = perf_counter() - started_at

        started_at = perf_counter()
        write_json(
            self.hits_path,
            {
                "consumer_id": stats.node_id,
                "algorithm": self.algorithm,
                "limit": self.limit,
                "consumed_batches": stats.consumed_batches,
                "consumed_candidates": stats.consumed_candidates,
                "hits": self.consumer.hits,
            },
        )
        self.report_write_seconds += metrics_write_seconds + (perf_counter() - started_at)
        self.report_write_count += 2


if __name__ == "__main__":
    main()
