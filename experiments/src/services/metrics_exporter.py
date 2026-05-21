#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Mapping


METRIC_PREFIX = "cqdagpcfg"
DEFAULT_EXPORTER_PORT = 9108
_METRIC_NAME_RE = re.compile(r"[^a-zA-Z0-9_:]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Expose CQDAGPCFG JSON metrics as Prometheus text metrics.",
    )
    parser.add_argument("--metrics-dir", type=Path, required=True)
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=DEFAULT_EXPORTER_PORT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.port <= 0:
        raise SystemExit("--port must be positive")
    args.metrics_dir.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(
        (args.bind, args.port),
        _handler_for(args.metrics_dir),
    )
    print(f"metrics exporter listening on http://{args.bind}:{args.port}/metrics")
    server.serve_forever()


def _handler_for(metrics_dir: Path):
    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/healthz":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok\n")
                return
            if self.path != "/metrics":
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"not found\n")
                return
            payload = render_prometheus_metrics(metrics_dir).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args) -> None:  # noqa: A002
            return

    return MetricsHandler


def render_prometheus_metrics(metrics_dir: Path) -> str:
    lines = [
        "# HELP cqdagpcfg_metric_source_up Whether a JSON metrics file was readable.",
        "# TYPE cqdagpcfg_metric_source_up gauge",
    ]
    emitted_types: set[str] = set()
    for path in sorted(metrics_dir.glob("*.json")):
        labels = _labels_for_path(path)
        try:
            payload = _read_metrics_file(path)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            lines.append(f"cqdagpcfg_metric_source_up{{{_format_labels(labels)}}} 0")
            continue
        labels = _labels_for_payload(path, payload)
        lines.append(f"cqdagpcfg_metric_source_up{{{_format_labels(labels)}}} 1")
        for key, value in sorted(payload.items()):
            sample = _numeric_sample(value)
            if sample is None:
                continue
            metric_name = _metric_name(key)
            if metric_name not in emitted_types:
                lines.append(f"# TYPE {metric_name} gauge")
                emitted_types.add(metric_name)
            lines.append(f"{metric_name}{{{_format_labels(labels)}}} {sample}")
    lines.append("")
    return "\n".join(lines)


def _read_metrics_file(path: Path) -> Mapping[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("metrics payload must be a JSON object")
    return payload


def _labels_for_path(path: Path) -> dict[str, str]:
    return {
        "source_file": path.stem,
        "role": "unknown",
        "instance": path.stem,
    }


def _labels_for_payload(path: Path, payload: Mapping[str, object]) -> dict[str, str]:
    role = str(payload.get("role", "unknown"))
    instance = str(
        payload.get("node_id")
        or payload.get("worker_id")
        or payload.get("consumer_id")
        or path.stem
    )
    labels = _labels_for_path(path)
    labels.update({"role": role, "instance": instance})
    return labels


def _metric_name(key: str) -> str:
    normalized = _METRIC_NAME_RE.sub("_", key.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        normalized = "unnamed"
    return f"{METRIC_PREFIX}_{normalized}"


def _numeric_sample(value: object) -> str | None:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return repr(float(value))
    return None


def _format_labels(labels: Mapping[str, str]) -> str:
    return ",".join(
        f'{_metric_name_fragment(key)}="{_escape_label_value(value)}"'
        for key, value in sorted(labels.items())
    )


def _metric_name_fragment(value: str) -> str:
    normalized = _METRIC_NAME_RE.sub("_", value.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "label"


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


if __name__ == "__main__":
    main()
