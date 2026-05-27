#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize candidate-space benchmark result directories.",
    )
    parser.add_argument("result_dirs", type=Path, nargs="+")
    return parser.parse_args()


def main() -> None:
    rows = [summarize(path) for path in parse_args().result_dirs]
    print("| Backend | Limit | Wall Time (s) | Throughput (c/s) | Peak RSS (MiB) | Status |")
    print("|---|---:|---:|---:|---:|---|")
    for row in rows:
        print(
            "| {backend} | {limit} | {wall} | {throughput} | "
            "{rss} | {status} |".format(
                backend=row["backend"],
                limit=row["limit"],
                wall=_format_float(row["wall"]),
                throughput=_format_rate(row["throughput"]),
                rss=_format_float(row["rss"], digits=1),
                status=row["status"],
            ),
        )


def summarize(path: Path) -> dict:
    command = _read_json(path / "benchmark-command.json")
    backend = command.get("backend", path.name)
    if backend == "fitcrack":
        return _summarize_fitcrack(path, backend)
    return _summarize_process_tree(path, backend)


def _summarize_process_tree(path: Path, backend: str) -> dict:
    command = _read_json(path / "benchmark-command.json")
    monitor = _read_json(path / "process_tree_summary.json")
    limit = int(command["limit"])
    wall = float(monitor["wall_seconds"])
    status = "ok" if int(monitor.get("return_code", 1)) == 0 else "failed"
    backend_status_path = path / "backend" / "status.json"
    if backend_status_path.exists():
        backend_status = _read_json(backend_status_path)
        client_code = backend_status.get("client_return_code")
        server_code = backend_status.get("server_return_code")
        if client_code != 0 or server_code != 0:
            status = f"failed client={client_code} server={server_code}"
    ok = status == "ok"
    return {
        "backend": backend,
        "limit": limit,
        "wall": wall,
        "throughput": limit / wall if ok and wall else None,
        "rss": float(monitor.get("peak_rss_mib", 0.0)),
        "status": status,
    }


def _summarize_fitcrack(path: Path, backend: str) -> dict:
    command = _read_json(path / "benchmark-command.json")
    summary = _read_json(path / "backend" / "summary.json")
    limit = int(command["limit"])
    wall = float(summary.get("observed_done_seconds") or summary.get("monitor_wall_seconds") or 0.0)
    status = str(summary.get("status_text") or summary.get("status") or "unknown")
    if summary.get("stop_reason"):
        status = f"stopped {summary['stop_reason']}"
    ok = status == "finished"
    return {
        "backend": backend,
        "limit": limit,
        "wall": wall,
        "throughput": limit / wall if ok and wall else None,
        "rss": float(summary.get("docker_peak_total_mib", 0.0)),
        "status": status,
    }


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _format_float(value: float, *, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def _format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.0f}"


if __name__ == "__main__":
    main()
