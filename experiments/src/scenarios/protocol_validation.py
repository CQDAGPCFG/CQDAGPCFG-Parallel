#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import monotonic

_EXPERIMENT_SRC = Path(__file__).resolve().parents[1]
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from shared.common import ensure_project_paths, experiment_src_dir, repo_root, write_json

ensure_project_paths()


@dataclass(frozen=True, slots=True)
class ValidationRun:
    name: str
    stage: str
    command: list[str]
    work_dir: Path
    elapsed_seconds: float
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run CQDAGPCFG protocol validation, fault, bottleneck, and ablation experiments.",
    )
    parser.add_argument(
        "--source-model-path",
        type=Path,
        default=Path("../CQDAGPCFG/examples/artifacts/rockyou_train/model.json"),
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=None,
        help="Directory where validation logs, JSON, and Markdown summaries are written.",
    )
    parser.add_argument("--limits", type=int, nargs="+", default=(1000, 20_000))
    parser.add_argument("--large-limit", type=int, default=100_000)
    parser.add_argument("--ablation-limit", type=int, default=5000)
    parser.add_argument("--fault-limit", type=int, default=1000)
    parser.add_argument("--bottleneck-limit", type=int, default=5000)
    parser.add_argument("--total-nodes", type=int, default=5)
    parser.add_argument("--max-parallel-leases-per-node", type=int, default=2)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--base-port", type=int, default=6100)
    parser.add_argument("--skip-large", action="store_true")
    parser.add_argument("--skip-fault", action="store_true")
    parser.add_argument("--skip-bottleneck", action="store_true")
    parser.add_argument("--skip-ablation", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = repo_root()
    scripts_dir = experiment_src_dir()
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    work_dir = (
        root / "experiments" / "results" / "protocol_validation" / run_id
        if args.work_dir is None
        else args.work_dir
    )
    work_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["PYTHONPATH"] = (
        f"{scripts_dir}:{root / 'src'}:{root.parent}:{env.get('PYTHONPATH', '')}"
    )
    python = sys.executable

    runs: list[ValidationRun] = []
    scenario_index = 0

    limits = list(dict.fromkeys(int(limit) for limit in args.limits))
    if not args.skip_large and args.large_limit not in limits:
        limits.append(args.large_limit)
    for limit in limits:
        scenario_index += 1
        runs.append(
            run_command(
                name=f"scale-{limit}",
                stage="large-limit",
                command=run_local_command(
                    python=python,
                    scripts_dir=scripts_dir,
                    args=args,
                    limit=limit,
                    scenario_dir=work_dir / f"scale-{limit}",
                    port_base=args.base_port + scenario_index * 10,
                    extra=(),
                ),
                env=env,
                work_dir=work_dir,
                timeout_seconds=args.timeout_seconds,
            )
        )

    if not args.skip_fault:
        scenario_index += 1
        runs.append(
            run_command(
                name="tracker-crash-late-worker",
                stage="fault",
                command=fault_command(
                    python=python,
                    scripts_dir=scripts_dir,
                    args=args,
                    scenario_dir=work_dir / "fault",
                    port_base=args.base_port + scenario_index * 10,
                ),
                env=env,
                work_dir=work_dir,
                timeout_seconds=args.timeout_seconds,
            )
        )

    if not args.skip_bottleneck:
        scenario_index += 1
        runs.append(
            run_command(
                name="dynamic-bottleneck",
                stage="bottleneck",
                command=run_local_command(
                    python=python,
                    scripts_dir=scripts_dir,
                    args=args,
                    limit=args.bottleneck_limit,
                    scenario_dir=work_dir / "bottleneck",
                    port_base=args.base_port + scenario_index * 10,
                    extra=(
                        "--dynamic-rebalance",
                        "--hash-delay-seconds",
                        "0.00005",
                    ),
                ),
                env=env,
                work_dir=work_dir,
                timeout_seconds=args.timeout_seconds,
            )
        )

    if not args.skip_ablation:
        ablations = (
            ("default", ()),
            ("single-range-lease", ("--max-parallel-leases-per-node", "1")),
            ("no-reclaim", ("--disable-reclaim",)),
            ("no-affinity", ("--disable-node-affinity",)),
        )
        for name, extra in ablations:
            scenario_index += 1
            runs.append(
                run_command(
                    name=f"ablation-{name}",
                    stage="ablation",
                    command=run_local_command(
                        python=python,
                        scripts_dir=scripts_dir,
                        args=args,
                        limit=args.ablation_limit,
                        scenario_dir=work_dir / f"ablation-{name}",
                        port_base=args.base_port + scenario_index * 10,
                        extra=extra,
                    ),
                    env=env,
                    work_dir=work_dir,
                    timeout_seconds=args.timeout_seconds,
                )
            )

    summary = {
        "work_dir": str(work_dir),
        "source_model_path": str(args.source_model_path),
        "all_ok": all(run.ok for run in runs),
        "runs": [run_summary(run) for run in runs],
    }
    write_json(work_dir / "protocol_validation_summary.json", summary)
    write_markdown(work_dir / "protocol_validation_summary.md", summary)
    print(f"protocol validation summary: {work_dir / 'protocol_validation_summary.md'}")

    if not summary["all_ok"]:
        raise SystemExit(1)


def run_local_command(
    *,
    python: str,
    scripts_dir: Path,
    args: argparse.Namespace,
    limit: int,
    scenario_dir: Path,
    port_base: int,
    extra: tuple[str, ...],
) -> list[str]:
    return [
        python,
        str(scripts_dir / "scenarios" / "run_local.py"),
        "--source-model-path",
        str(args.source_model_path),
        "--work-dir",
        str(scenario_dir),
        "--limit",
        str(limit),
        "--total-nodes",
        str(args.total_nodes),
        "--max-parallel-leases-per-node",
        str(args.max_parallel_leases_per_node),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--control-bind",
        f"cqpcfg://127.0.0.1:{port_base}",
        "--control-connect",
        f"cqpcfg://127.0.0.1:{port_base}",
        "--batch-bind",
        f"cqpcfg://127.0.0.1:{port_base + 1}",
        "--batch-connect",
        f"cqpcfg://127.0.0.1:{port_base + 1}",
        "--role-bind",
        f"cqpcfg://127.0.0.1:{port_base + 2}",
        "--role-connect",
        f"cqpcfg://127.0.0.1:{port_base + 2}",
        "--ack-bind",
        f"cqpcfg://127.0.0.1:{port_base + 3}",
        "--ack-connect",
        f"cqpcfg://127.0.0.1:{port_base + 3}",
        *extra,
    ]


def fault_command(
    *,
    python: str,
    scripts_dir: Path,
    args: argparse.Namespace,
    scenario_dir: Path,
    port_base: int,
) -> list[str]:
    return [
        python,
        str(scripts_dir / "scenarios" / "fault_injection.py"),
        "--source-model-path",
        str(args.source_model_path),
        "--work-dir",
        str(scenario_dir),
        "--limit",
        str(args.fault_limit),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--crash-after-seconds",
        "0.2",
        "--control-bind",
        f"cqpcfg://127.0.0.1:{port_base}",
        "--control-connect",
        f"cqpcfg://127.0.0.1:{port_base}",
        "--batch-bind",
        f"cqpcfg://127.0.0.1:{port_base + 1}",
        "--batch-connect",
        f"cqpcfg://127.0.0.1:{port_base + 1}",
        "--ack-bind",
        f"cqpcfg://127.0.0.1:{port_base + 3}",
        "--ack-connect",
        f"cqpcfg://127.0.0.1:{port_base + 3}",
    ]


def run_command(
    *,
    name: str,
    stage: str,
    command: list[str],
    env: dict[str, str],
    work_dir: Path,
    timeout_seconds: float,
) -> ValidationRun:
    print(f"[{stage}] {name}")
    started_at = monotonic()
    completed = subprocess.run(
        command,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout_seconds + 10.0,
    )
    elapsed_seconds = monotonic() - started_at
    run = ValidationRun(
        name=name,
        stage=stage,
        command=command,
        work_dir=work_dir,
        elapsed_seconds=elapsed_seconds,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )
    log_dir = work_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / f"{name}.stdout.log").write_text(completed.stdout, encoding="utf-8")
    (log_dir / f"{name}.stderr.log").write_text(completed.stderr, encoding="utf-8")
    print(f"  returncode: {completed.returncode}")
    print(f"  elapsed   : {elapsed_seconds:.3f}s")
    return run


def run_summary(run: ValidationRun) -> dict[str, object]:
    parsed = parse_stdout(run.stdout)
    return {
        "name": run.name,
        "stage": run.stage,
        "ok": run.ok,
        "elapsed_seconds": run.elapsed_seconds,
        "returncode": run.returncode,
        "command": run.command,
        **parsed,
    }


def parse_stdout(stdout: str) -> dict[str, object]:
    values: dict[str, object] = {}
    for key, label in {
        "limit": "limit",
        "digest": "digest",
        "emitted_records": "emitted records",
        "resident_records": "resident records",
        "peak_resident_records": "peak resident",
        "reclaimed_records": "reclaimed records",
        "elapsed_protocol_seconds": "elapsed seconds",
        "data_sent_bytes": "data sent bytes",
        "data_bytes_per_candidate": "data bytes/candidate",
        "recv_poll_seconds": "recv poll seconds",
        "role_control_messages": "role control messages",
        "role_control_seconds": "role control seconds",
        "source_peak_cache": "source peak cache",
        "source_reclaimed_records": "source reclaimed records",
    }.items():
        match = re.search(rf"^\s*{re.escape(label)}\s*:\s*(.+)$", stdout, re.MULTILINE)
        if match:
            values[key] = coerce_value(match.group(1).strip())
    values["hash_verified"] = "local hash consumers verified" in stdout
    values["fault_recovered"] = "fault-injection run completed" in stdout
    values["role_switch_observed"] = "in-process role switch requested" in stdout
    return values


def coerce_value(value: str) -> object:
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    try:
        return float(value)
    except ValueError:
        return value


def write_markdown(path: Path, summary: dict[str, object]) -> None:
    runs = summary["runs"]
    assert isinstance(runs, list)
    lines = [
        "# CQDAGPCFG Protocol Validation Summary",
        "",
        f"- Source model: `{summary['source_model_path']}`",
        f"- Result: {'PASS' if summary['all_ok'] else 'FAIL'}",
        "",
        "## Runs",
        "",
        "| Stage | Name | OK | Limit | Digest | Peak resident | Reclaimed | Hash | Fault | Role switch |",
        "|---|---|---:|---:|---|---:|---:|---:|---:|---:|",
    ]
    for item in runs:
        assert isinstance(item, dict)
        lines.append(
            "| {stage} | {name} | {ok} | {limit} | `{digest}` | {peak} | {reclaimed} | {hash_ok} | {fault} | {role} |".format(
                stage=item.get("stage", ""),
                name=item.get("name", ""),
                ok="Y" if item.get("ok") else "N",
                limit=item.get("limit", ""),
                digest=str(item.get("digest", ""))[:12],
                peak=item.get("peak_resident_records", ""),
                reclaimed=item.get("reclaimed_records", ""),
                hash_ok="Y" if item.get("hash_verified") else "",
                fault="Y" if item.get("fault_recovered") else "",
                role="Y" if item.get("role_switch_observed") else "",
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Large-limit runs check serial CQDAGPCFG digest equality under range leases.",
            "- Fault run checks tracker crash recovery and late worker join.",
            "- Bottleneck run records network, ACK, role-control, and reclaim overhead.",
            "- Ablation runs compare default protocol against single-range lease, disabled reclaim, and disabled affinity.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
