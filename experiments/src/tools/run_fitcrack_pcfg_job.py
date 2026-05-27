#!/usr/bin/env python3
from __future__ import annotations

import argparse
import http.cookiejar
import json
import re
import subprocess
import time
import urllib.request
from pathlib import Path


FITCRACK_BASE = "http://127.0.0.1:15000"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run and monitor a Fitcrack PCFG job.")
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--cookie-path", type=Path, required=True)
    parser.add_argument("--limit", type=int, required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=3600.0)
    parser.add_argument("--grammar-id", type=int, default=1)
    parser.add_argument("--grammar-name", default="john")
    parser.add_argument("--grammar-keyspace", type=int, default=1_321_431_161)
    parser.add_argument(
        "--max-docker-memory-mib",
        type=float,
        default=None,
        help="Stop the job if summed Fitcrack container memory exceeds this value.",
    )
    parser.add_argument(
        "--hash",
        action="append",
        default=[],
        help="MD5 hash to include in the job. May be repeated.",
    )
    args = parser.parse_args()
    if args.limit <= 0:
        raise SystemExit("--limit must be positive")
    if args.poll_seconds <= 0.0:
        raise SystemExit("--poll-seconds must be positive")
    return args


def opener(cookie_path: Path):
    cookie_jar = http.cookiejar.MozillaCookieJar(str(cookie_path))
    cookie_jar.load(ignore_discard=True, ignore_expires=True)
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))


def request_json(client, method: str, path: str, payload: dict | None = None) -> dict:
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        FITCRACK_BASE + path,
        data=body,
        headers=headers,
        method=method,
    )
    with client.open(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def mem_to_mib(value: str) -> float:
    first = value.split("/")[0].strip()
    match = re.match(r"([0-9.]+)\s*([A-Za-z]+)", first)
    if not match:
        return 0.0
    number = float(match.group(1))
    unit = match.group(2).lower()
    if unit == "b":
        return number / (1024 * 1024)
    if unit in {"kib", "kb"}:
        return number / 1024
    if unit in {"mib", "mb"}:
        return number
    if unit in {"gib", "gb"}:
        return number * 1024
    return number


def docker_memory() -> tuple[dict[str, float], float]:
    output = subprocess.check_output(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{json .}}",
            "cqpcfg-fitcrack-baseline",
            "cqpcfg-fitcrack-boinc",
        ],
        text=True,
    )
    values: dict[str, float] = {}
    total = 0.0
    for line in output.splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        value = round(mem_to_mib(item.get("MemUsage", "0B / 0B")), 3)
        values[item.get("Name", "?")] = value
        total += value
    return values, round(total, 3)


def build_payload(args: argparse.Namespace) -> dict:
    hashes = args.hash or ["00000000000000000000000000000000"]
    return {
        "name": args.name,
        "comment": "Fitcrack native PCFG 100M fair process-tree comparison",
        "hosts_ids": [1],
        "seconds_per_job": 600,
        "time_start": "",
        "time_end": "",
        "attack_settings": {
            "attack_mode": 9,
            "attack_name": "pcfg",
            "attack_submode": 0,
            "distribution_mode": 0,
            "pcfg_grammar": {
                "id": args.grammar_id,
                "name": args.grammar_name,
                "keyspace": args.grammar_keyspace,
            },
            "keyspace_limit": args.limit,
            "rules": None,
            "rule_left": "",
            "rule_right": "",
            "optimized": True,
        },
        "hash_settings": {
            "hash_type": "0",
            "hash_list": [{"hash": value} for value in hashes],
            "valid_only": False,
        },
    }


def main() -> None:
    args = parse_args()
    result_dir = args.result_dir
    result_dir.mkdir(parents=True, exist_ok=True)
    client = opener(args.cookie_path)
    payload = build_payload(args)
    (result_dir / "payload.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    create_response = request_json(client, "POST", "/job", payload)
    (result_dir / "create_response.json").write_text(
        json.dumps(create_response, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    job_id = str(create_response["job_id"])
    start_response = request_json(client, "GET", f"/job/{job_id}/action?operation=start")
    (result_dir / "start_response.json").write_text(
        json.dumps(start_response, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    samples = []
    peak_by_container: dict[str, float] = {}
    peak_total = 0.0
    stop_reason = None
    started_at = time.perf_counter()
    while True:
        elapsed = time.perf_counter() - started_at
        job = request_json(client, "GET", f"/job/{job_id}")
        by_container, total = docker_memory()
        peak_total = max(peak_total, total)
        for name, value in by_container.items():
            peak_by_container[name] = max(peak_by_container.get(name, 0.0), value)
        sample = {
            "elapsed_seconds": elapsed,
            "status": job.get("status"),
            "status_text": job.get("status_text"),
            "progress": job.get("progress"),
            "current_index": job.get("current_index"),
            "keyspace": job.get("keyspace"),
            "hc_keyspace": job.get("hc_keyspace"),
            "cracked_hashes_str": job.get("cracked_hashes_str"),
            "total_time": job.get("total_time"),
            "workunit_sum_time": job.get("workunit_sum_time"),
            "docker_mib_by_container": by_container,
            "docker_total_mib": total,
        }
        samples.append(sample)
        (result_dir / "monitor_samples.json").write_text(
            json.dumps(samples, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        status_text = str(job.get("status_text", "")).lower()
        if status_text in {"finished", "exhausted", "timeout", "malformed"}:
            break
        if str(job.get("status")) in {"1", "2", "3", "4"}:
            break
        if args.max_docker_memory_mib is not None and total > args.max_docker_memory_mib:
            stop_reason = (
                f"memory_limit:{total:.1f}>{args.max_docker_memory_mib:.1f}MiB"
            )
            request_json(client, "GET", f"/job/{job_id}/action?operation=stop")
            break
        if elapsed > args.timeout_seconds:
            stop_reason = "timeout"
            request_json(client, "GET", f"/job/{job_id}/action?operation=stop")
            break
        time.sleep(args.poll_seconds)

    final = request_json(client, "GET", f"/job/{job_id}")
    (result_dir / "job_final.json").write_text(
        json.dumps(final, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary = {
        "job_id": job_id,
        "observed_done_seconds": samples[-1]["elapsed_seconds"] if samples else None,
        "monitor_wall_seconds": time.perf_counter() - started_at,
        "status": final.get("status"),
        "status_text": final.get("status_text"),
        "progress": final.get("progress"),
        "cracked_hashes_str": final.get("cracked_hashes_str"),
        "total_time": final.get("total_time"),
        "workunit_sum_time": final.get("workunit_sum_time"),
        "keyspace": final.get("keyspace"),
        "hc_keyspace": final.get("hc_keyspace"),
        "current_index": final.get("current_index"),
        "docker_peak_mib_by_container": peak_by_container,
        "docker_peak_total_mib": peak_total,
        "stop_reason": stop_reason,
    }
    (result_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
