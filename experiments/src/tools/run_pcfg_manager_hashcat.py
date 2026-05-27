#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import shutil
import subprocess
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run pcfg-manager server+client with Hashcat.",
    )
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--pcfg-manager", type=Path, required=True)
    parser.add_argument("--rules", type=Path, required=True)
    parser.add_argument("--limit", type=int, required=True)
    parser.add_argument("--port", default="50151")
    parser.add_argument(
        "--hash",
        action="append",
        default=[],
        help="Hash to include. May be repeated.",
    )
    parser.add_argument("--hashcat", default="/usr/bin/hashcat")
    parser.add_argument("--chunk-duration", default="30s")
    parser.add_argument("--chunk-start-size", type=int, default=10000)
    parser.add_argument("--client-count", type=int, default=1)
    parser.add_argument("--max-rss-mib", type=float, default=None)
    parser.add_argument("--monitor-interval-seconds", type=float, default=0.5)
    args = parser.parse_args()
    if args.limit <= 0:
        raise SystemExit("--limit must be positive")
    if args.client_count <= 0:
        raise SystemExit("--client-count must be positive")
    if args.max_rss_mib is not None and args.max_rss_mib <= 0.0:
        raise SystemExit("--max-rss-mib must be positive")
    if args.monitor_interval_seconds <= 0.0:
        raise SystemExit("--monitor-interval-seconds must be positive")
    return args


def write_hashcat_wrapper(path: Path, hashcat: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/bin/sh\n"
        "touch results.txt\n"
        f"exec {hashcat} --potfile-disable --quiet --force -D 1 \"$@\"\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def terminate(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


def main() -> None:
    args = parse_args()
    args.pcfg_manager = args.pcfg_manager.resolve()
    args.rules = args.rules.resolve()
    result_dir = args.result_dir.resolve()
    result_dir.mkdir(parents=True, exist_ok=True)
    hashcat_dirs = tuple(result_dir / f"hashcat-{index}" for index in range(args.client_count))
    for hashcat_dir in hashcat_dirs:
        write_hashcat_wrapper(hashcat_dir / "hashcat64.bin", args.hashcat)
    hash_path = result_dir / "target.hash"
    hashes = args.hash or ["00000000000000000000000000000000"]
    hash_path.write_text("\n".join(hashes) + "\n", encoding="utf-8")

    server_stdout = (result_dir / "server.stdout").open("w", encoding="utf-8")
    server_stderr = (result_dir / "server.stderr").open("w", encoding="utf-8")
    client_stdout = [
        (result_dir / f"client-{index}.stdout").open("w", encoding="utf-8")
        for index in range(args.client_count)
    ]
    client_stderr = [
        (result_dir / f"client-{index}.stderr").open("w", encoding="utf-8")
        for index in range(args.client_count)
    ]

    server_command = [
        str(args.pcfg_manager),
        "server",
        "-r",
        str(args.rules),
        "-p",
        args.port,
        "--hashcat-mode",
        "0",
        "--hashlist",
        str(hash_path),
        "--max-guesses",
        str(args.limit),
        "--chunk-duration",
        args.chunk_duration,
        "--chunk-start-size",
        str(args.chunk_start_size),
    ]
    client_commands = [
        [
            str(args.pcfg_manager),
            "client",
            "-s",
            f"127.0.0.1:{args.port}",
            "--hashcat-folder",
            str(hashcat_dir),
            "--stats",
        ]
        for hashcat_dir in hashcat_dirs
    ]
    (result_dir / "commands.json").write_text(
        json.dumps(
            {"server": server_command, "clients": client_commands},
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    started_at = time.perf_counter()
    server = subprocess.Popen(
        server_command,
        stdout=server_stdout,
        stderr=server_stderr,
        text=True,
        cwd=result_dir,
        start_new_session=True,
    )
    (result_dir / "server.pid").write_text(str(server.pid), encoding="utf-8")
    time.sleep(1.0)
    clients = [
        subprocess.Popen(
            client_command,
            stdout=client_stdout[index],
            stderr=client_stderr[index],
            text=True,
            cwd=result_dir,
            start_new_session=True,
        )
        for index, client_command in enumerate(client_commands)
    ]
    (result_dir / "client.pids").write_text(
        "\n".join(str(client.pid) for client in clients),
        encoding="utf-8",
    )

    def handle_stop(signum, _frame) -> None:
        for client in clients:
            terminate(client)
        terminate(server)
        raise SystemExit(128 + signum)

    old_term = signal.signal(signal.SIGTERM, handle_stop)
    old_int = signal.signal(signal.SIGINT, handle_stop)

    client_codes: list[int | None] = []
    server_code = None
    peak_rss_mib = 0.0
    stop_reason = None
    try:
        while True:
            rss_mib = process_tree_rss_mib((server, *clients))
            peak_rss_mib = max(peak_rss_mib, rss_mib)
            if args.max_rss_mib is not None and rss_mib > args.max_rss_mib:
                stop_reason = f"memory_limit:{rss_mib:.1f}>{args.max_rss_mib:.1f}MiB"
                for client in clients:
                    terminate(client)
                terminate(server)
                break
            if all(client.poll() is not None for client in clients):
                break
            if server.poll() is not None:
                break
            time.sleep(args.monitor_interval_seconds)
        for client in clients:
            client_codes.append(client.wait())
        server_code = server.wait(timeout=30)
    except subprocess.TimeoutExpired:
        terminate(server)
        server_code = server.returncode
    finally:
        for client in clients:
            terminate(client)
        terminate(server)
        for handle in (server_stdout, server_stderr, *client_stdout, *client_stderr):
            handle.close()
        signal.signal(signal.SIGTERM, old_term)
        signal.signal(signal.SIGINT, old_int)

    ended_at = time.perf_counter()
    summary = {
        "limit": args.limit,
        "hashes": hashes,
        "client_count": args.client_count,
        "client_return_codes": client_codes,
        "client_return_code": client_codes[0] if client_codes else None,
        "server_return_code": server_code,
        "wall_seconds": ended_at - started_at,
        "peak_rss_mib": peak_rss_mib,
        "stop_reason": stop_reason,
    }
    (result_dir / "status.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    # pcfg-manager's server is a long-lived coordinator.  In this runner the
    # client is the completion signal; after clients exit cleanly we terminate
    # the server process, which reports SIGTERM as -15.
    accepted_server_codes = {0, -signal.SIGTERM, None}
    if any(code not in {0, None} for code in client_codes) or server_code not in accepted_server_codes:
        raise SystemExit(1)


def process_tree_rss_mib(processes: tuple[subprocess.Popen, ...]) -> float:
    pids = set()
    for process in processes:
        if process.poll() is None:
            pids.update(descendants(process.pid))
    return sum(rss_kib(pid) for pid in pids) / 1024


def descendants(root_pid: int) -> set[int]:
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
    return seen


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


def rss_kib(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text(errors="replace").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except Exception:
        return 0
    return 0


if __name__ == "__main__":
    main()
