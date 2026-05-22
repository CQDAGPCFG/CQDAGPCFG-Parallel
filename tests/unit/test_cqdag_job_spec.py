from __future__ import annotations

from CQDAGPCFG import save_model
from CQDAGPCFG.training import PCFGTrainer

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGRecordSource,
    ROOT_NODE_ID,
    normalize_target_ranks,
    prepare_cqdag_job_spec,
)
from cqdagpcfg_parallel.protocol import stable_digest


def test_prepare_cqdag_job_spec_streams_serial_digest(tmp_path) -> None:
    model = PCFGTrainer().train(
        (
            "ab12!",
            "ab12!",
            "cd12!",
            "ab34@",
            "password12",
            "hello2024!",
        )
    )
    model_path = tmp_path / "model.json"
    save_model(model, model_path)

    spec = prepare_cqdag_job_spec(
        model_path,
        limit=12,
        target_ranks=(0, 7),
    )
    baseline = CQDAGRecordSource(model, max_records=12)

    assert spec.limit == 12
    assert spec.serial_digest == stable_digest(
        baseline.read_range(ROOT_NODE_ID, 0, 12),
    )
    assert spec.model_fingerprint is not None
    assert spec.payload["targets"][0]["rank"] == 0
    assert spec.payload["targets"][1]["rank"] == 7


def test_normalize_target_ranks_validates_prefix_bounds() -> None:
    assert normalize_target_ranks(None, 3) == (0, 2)
    assert normalize_target_ranks((2, 2, 0), 3) == (2, 0)
