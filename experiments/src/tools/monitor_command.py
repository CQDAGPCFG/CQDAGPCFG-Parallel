#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import signal
import json
import subprocess
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a command and sample summed process-tree RSS.",
    )
    parser.add_argument("--samples-path", type=Path, required=True)
    parser.add_argument("--summary-path", type=Path, required=True)
    parser.add_argument("--interval-seconds", type=float, default=0.05)
    parser.add_argument("--max-rss-mib", type=float, default=None)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.interval_seconds <= 0.0:
        raise SystemExit("--interval-seconds must be positive")
    if args.max_rss_mib is not None and args.max_rss_mib <= 0.0:
        raise SystemExit("--max-rss-mib must be positive")
    if not args.command:
        raise SystemExit("missing command")
    if args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        raise SystemExit("missing command after --")
    return args


def process_table() -> dict[int, list[int]]:
    table: dict[int, list[int]] = {}
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            stat = (proc / "stat").read_text(errors="replace")
            tail = stat.rsplit(")", 1)[1].split()
            ppid = int(tail[1])
            pid = int(proc.name)
        except Exception:
            continue
        table.setdefault(ppid, []).append(pid)
    return table


def descendants(root_pid: int) -> list[int]:
    table = process_table()
    seen = {root_pid}
    stack = [root_pid]
    while stack:
        pid = stack.pop()
        for child in table.get(pid, ()):
            if child in seen:
                continue
            seen.add(child)
            stack.append(child)
    return sorted(seen)


def rss_kib(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text(errors="replace").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except Exception:
        return 0
    return 0


def sample(root_pid: int, started_at: float) -> dict:
    pids = descendants(root_pid)
    by_pid = {pid: rss_kib(pid) for pid in pids}
    total = sum(by_pid.values())
    return {
        "elapsed_seconds": time.perf_counter() - started_at,
        "rss_kib": total,
        "rss_mib": total / 1024,
        "pids": pids,
        "rss_by_pid_kib": by_pid,
    }


def main() -> None:
    args = parse_args()
    args.samples_path.parent.mkdir(parents=True, exist_ok=True)
    args.summary_path.parent.mkdir(parents=True, exist_ok=True)

    started_at = time.perf_counter()
    process = subprocess.Popen(args.command, start_new_session=True)
    samples: list[dict] = []
    peak = {"rss_kib": 0, "rss_mib": 0.0, "pids": [], "rss_by_pid_kib": {}}
    stop_reason = None
    try:
        while process.poll() is None:
            current = sample(process.pid, started_at)
            samples.append(current)
            if current["rss_kib"] > peak["rss_kib"]:
                peak = current
            if (
                args.max_rss_mib is not None
                and current["rss_mib"] > args.max_rss_mib
            ):
                stop_reason = (
                    f"memory_limit:{current['rss_mib']:.1f}>{args.max_rss_mib:.1f}MiB"
                )
                terminate_group(process)
                break
            time.sleep(args.interval_seconds)
        current = sample(process.pid, started_at)
        samples.append(current)
        if current["rss_kib"] > peak["rss_kib"]:
            peak = current
    finally:
        return_code = process.wait()
        ended_at = time.perf_counter()

    summary = {
        "command": args.command,
        "return_code": return_code,
        "wall_seconds": ended_at - started_at,
        "sample_count": len(samples),
        "peak_rss_kib": peak["rss_kib"],
        "peak_rss_mib": peak["rss_mib"],
        "peak_sample": peak,
        "stop_reason": stop_reason,
    }
    args.samples_path.write_text(
        json.dumps(samples, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    args.summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    raise SystemExit(return_code)


def terminate_group(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


if __name__ == "__main__":
    main()
