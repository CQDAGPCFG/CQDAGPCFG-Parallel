from __future__ import annotations

import json
import sys
from pathlib import Path


_EXPERIMENT_SRC = Path(__file__).resolve().parents[2] / "experiments" / "src"
if str(_EXPERIMENT_SRC) not in sys.path:
    sys.path.insert(0, str(_EXPERIMENT_SRC))

from services.metrics_exporter import render_prometheus_metrics  # noqa: E402


def test_render_prometheus_metrics_exposes_recent_candidate_samples(tmp_path: Path) -> None:
    metrics_path = tmp_path / "tracker.json"
    metrics_path.write_text(
        json.dumps(
            {
                "role": "tracker",
                "published_candidates": 2,
                "candidate_samples": [
                    {
                        "rank": 0,
                        "batch_id": 0,
                        "guess": "password1",
                        "prob": 0.4,
                        "structure_index": 3,
                        "structure_name": "W8D1",
                    },
                    {
                        "rank": 1,
                        "batch_id": 0,
                        "guess": "admin123",
                        "prob": 0.3,
                        "structure_index": 4,
                        "structure_name": "W5D3",
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    rendered = render_prometheus_metrics(tmp_path)

    assert 'cqdagpcfg_published_candidates{instance="tracker",role="tracker",source_file="tracker"} 2.0' in rendered
    assert "# HELP cqdagpcfg_candidate_sample_info Recent candidate samples emitted by the tracker." in rendered
    assert 'guess="password1"' in rendered
    assert 'rank="0"' in rendered
    assert 'structure_name="W8D1"' in rendered
    assert 'guess="admin123"' in rendered


def test_render_prometheus_metrics_exposes_node_role_info(tmp_path: Path) -> None:
    metrics_path = tmp_path / "node-0.json"
    metrics_path.write_text(
        json.dumps(
            {
                "role": "node_agent",
                "node_id": "node-0",
                "current_role": "generator",
                "desired_role": "consumer",
                "completed_records": 10,
            },
        ),
        encoding="utf-8",
    )

    rendered = render_prometheus_metrics(tmp_path)

    assert "# HELP cqdagpcfg_node_role_info Node agent role assignment and current execution role." in rendered
    assert 'cqdagpcfg_node_role_info{current_role="generator",desired_role="consumer",instance="node-0",node_id="node-0",role="generator",source_file="node-0"} 1' in rendered
    assert 'cqdagpcfg_completed_records{instance="node-0",role="generator",source_file="node-0"} 10.0' in rendered


def test_render_prometheus_metrics_uses_desired_role_for_stopped_node(tmp_path: Path) -> None:
    metrics_path = tmp_path / "node-1.json"
    metrics_path.write_text(
        json.dumps(
            {
                "role": "node_agent",
                "node_id": "node-1",
                "current_role": "stopped",
                "desired_role": "consumer",
                "consumed_candidates": 80,
            },
        ),
        encoding="utf-8",
    )

    rendered = render_prometheus_metrics(tmp_path)

    assert 'cqdagpcfg_node_role_info{current_role="stopped",desired_role="consumer",instance="node-1",node_id="node-1",role="consumer",source_file="node-1"} 1' in rendered
    assert 'cqdagpcfg_consumed_candidates{instance="node-1",role="consumer",source_file="node-1"} 80.0' in rendered
