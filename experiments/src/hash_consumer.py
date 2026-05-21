#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from time import monotonic, sleep

from common import digest_guess, ensure_project_paths, read_json, write_json

ensure_project_paths()

from cqdagpcfg_parallel.runtime import BatchAck, BatchAckStatus, BatchEndOfStream
from cqdagpcfg_parallel.runtime import ZmqPushBatchAckSink
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint, ZmqPullBatchSource


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the CQDAGPCFG E2E hash consumer.")
    parser.add_argument("--targets-path", type=Path, required=True)
    parser.add_argument("--batch-bind", default=None)
    parser.add_argument("--batch-connect", default=None)
    parser.add_argument("--ack-connect", default="cqpcfg://127.0.0.1:5558")
    parser.add_argument("--consumer-id", default="consumer-0")
    parser.add_argument("--hits-path", type=Path, default=None)
    parser.add_argument("--metrics-path", type=Path, default=None)
    parser.add_argument("--retire-file", type=Path, default=None)
    parser.add_argument("--require-all-targets", action="store_true")
    parser.add_argument("--receive-timeout-ms", type=int, default=1000)
    parser.add_argument("--overall-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--hash-delay-seconds", type=float, default=0.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.hash_delay_seconds < 0.0:
        raise SystemExit("--hash-delay-seconds cannot be negative")
    targets = read_json(args.targets_path)
    algorithm = str(targets["algorithm"])
    limit = int(targets["limit"])
    target_by_hash: dict[str, list[dict]] = {}
    for target in targets["targets"]:
        target_by_hash.setdefault(str(target["hash"]), []).append(dict(target))

    consumed_batches = 0
    consumed_candidates = 0
    hits: list[dict] = []
    started_at = monotonic()
    if args.batch_bind is not None and args.batch_connect is not None:
        raise SystemExit("--batch-bind and --batch-connect cannot be used together")
    endpoint_uri = args.batch_connect or args.batch_bind or "cqpcfg://0.0.0.0:5556"
    endpoint = ZmqEndpoint.from_uri(endpoint_uri, bind=args.batch_connect is None)
    ack_endpoint = ZmqEndpoint.from_uri(args.ack_connect, bind=False)

    with ZmqPullBatchSource(endpoint) as source, ZmqPushBatchAckSink(ack_endpoint) as ack_sink:
        while True:
            if monotonic() - started_at > args.overall_timeout_seconds:
                raise SystemExit("hash consumer timed out waiting for candidate batches")
            message = source.receive_envelope(timeout_ms=args.receive_timeout_ms)
            if message is None:
                if args.retire_file is not None and is_retired(args.retire_file, args.consumer_id):
                    break
                continue
            if isinstance(message, BatchEndOfStream):
                break
            batch = message
            try:
                consumed_batches += 1
                consumed_candidates += len(batch.records)
                for offset, record in enumerate(batch.records):
                    if args.hash_delay_seconds:
                        sleep(args.hash_delay_seconds)
                    digest = digest_guess(record.guess, algorithm=algorithm)
                    for target in target_by_hash.get(digest, ()):
                        hits.append(
                            {
                                "rank": batch.start_rank + offset,
                                "target_rank": int(target["rank"]),
                                "batch_id": batch.batch_id,
                                "guess": record.guess,
                                "hash": digest,
                                "elapsed_seconds": monotonic() - started_at,
                            }
                        )
                ack_sink.publish(
                    BatchAck(
                        batch_id=batch.batch_id,
                        consumer_id=args.consumer_id,
                        status=BatchAckStatus.DONE,
                    )
                )
            except BaseException as exc:
                ack_sink.publish(
                    BatchAck(
                        batch_id=batch.batch_id,
                        consumer_id=args.consumer_id,
                        status=BatchAckStatus.FAILED,
                        error=str(exc),
                    )
                )
                raise
            write_metrics(
                args.metrics_path,
                consumer_id=args.consumer_id,
                algorithm=algorithm,
                consumed_batches=consumed_batches,
                consumed_candidates=consumed_candidates,
                hits=hits,
                started_at=started_at,
                final=False,
            )
            if args.retire_file is not None and is_retired(args.retire_file, args.consumer_id):
                break

    expected_guesses = {str(target["guess"]) for target in targets["targets"]}
    found_guesses = {str(hit["guess"]) for hit in hits}
    if args.require_all_targets and not expected_guesses.issubset(found_guesses):
        missing = sorted(expected_guesses - found_guesses)
        raise SystemExit(f"hash consumer missed target guesses: {missing}")

    if args.hits_path is not None:
        write_json(
            args.hits_path,
            {
                "consumer_id": args.consumer_id,
                "algorithm": algorithm,
                "limit": limit,
                "consumed_batches": consumed_batches,
                "consumed_candidates": consumed_candidates,
                "hits": hits,
            },
        )
    write_metrics(
        args.metrics_path,
        consumer_id=args.consumer_id,
        algorithm=algorithm,
        consumed_batches=consumed_batches,
        consumed_candidates=consumed_candidates,
        hits=hits,
        started_at=started_at,
        final=True,
    )

    print("hash consumer completed")
    print(f"  consumer id        : {args.consumer_id}")
    print(f"  algorithm          : {algorithm}")
    print(f"  consumed batches   : {consumed_batches}")
    print(f"  consumed candidates: {consumed_candidates}")
    print(f"  hits               : {len(hits)}")
    for hit in sorted(hits, key=lambda item: (item["rank"], item["batch_id"])):
        print(
            f"    rank={hit['rank']} target_rank={hit['target_rank']} "
            f"batch={hit['batch_id']} guess={hit['guess']} hash={hit['hash']}"
        )


def is_retired(path: Path, consumer_id: str) -> bool:
    if not path.exists():
        return False
    payload = read_json(path)
    retired = payload.get("retired_consumers", ())
    return consumer_id in set(str(value) for value in retired)


def write_metrics(
    path: Path | None,
    *,
    consumer_id: str,
    algorithm: str,
    consumed_batches: int,
    consumed_candidates: int,
    hits: list[dict],
    started_at: float,
    final: bool,
) -> None:
    if path is None:
        return
    elapsed = max(monotonic() - started_at, 1e-12)
    write_json(
        path,
        {
            "role": "consumer",
            "consumer_id": consumer_id,
            "algorithm": algorithm,
            "consumed_batches": consumed_batches,
            "consumed_candidates": consumed_candidates,
            "candidate_rate": consumed_candidates / elapsed,
            "hits": len(hits),
            "elapsed_seconds": elapsed,
            "final": final,
        },
    )


if __name__ == "__main__":
    main()
