from __future__ import annotations

from CQDAGPCFG import save_model
from CQDAGPCFG.training import PCFGTrainer

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGRecordSource,
    ROOT_NODE_ID,
    normalize_decoy_hashes,
    normalize_target_ranks,
    prepare_cqdag_cracking_job_spec,
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


def test_normalize_decoy_hashes_deduplicates_and_drops_empty_values() -> None:
    assert normalize_decoy_hashes((" ABC ", "", "abc", "def")) == ["abc", "def"]


def test_prepare_cracking_job_spec_keeps_target_hashes_without_serial_digest(tmp_path) -> None:
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

    spec = prepare_cqdag_cracking_job_spec(
        model_path,
        limit=1_000_000,
        target_ranks=(0, 7),
        decoy_hashes=("0" * 32,),
    )

    assert spec.serial_digest == "unchecked-cracking-profile"
    assert spec.payload["serial_stream_fingerprint"] is None
    assert [target["rank"] for target in spec.payload["targets"]] == [0, 7]
    assert all(target["hash"] for target in spec.payload["targets"])
    assert spec.payload["decoy_hashes"] == ["0" * 32]
