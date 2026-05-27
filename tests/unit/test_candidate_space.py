from __future__ import annotations

import json

from CQDAGPCFG import save_model
from CQDAGPCFG.training import PCFGTrainer

from cqdagpcfg_parallel.adapters.cqdagpcfg import (
    CQDAGModelCandidateSpace,
    PcfgManagerRuleExporter,
)


def test_cqdag_candidate_space_exports_pcfg_manager_rules(tmp_path) -> None:
    model = PCFGTrainer().train(
        (
            "ab12!",
            "ab12!",
            "cd12!",
            "password12",
            "hello2024!",
        ),
    )
    model_path = tmp_path / "source-model.json"
    save_model(model, model_path)

    artifacts = CQDAGModelCandidateSpace(
        model_path,
        name="unit-space",
    ).materialize(tmp_path / "space")

    manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema"] == "cqpcfg-candidate-space-v1"
    assert artifacts.model_path.exists()
    assert artifacts.pcfg_rules_dir.joinpath("config.ini").exists()
    assert artifacts.pcfg_rules_dir.joinpath("Grammar", "Grammar.txt").exists()
    assert artifacts.total_points == model.total_points()

    config = artifacts.pcfg_rules_dir.joinpath("config.ini").read_text(encoding="utf-8")
    for transition_id in {symbol[0] for symbol in model.slot_tables}:
        assert f"[BASE_{transition_id}]" in config


def test_pcfg_manager_export_rejects_non_pcfg_manager_symbols(tmp_path) -> None:
    model = PCFGTrainer().train(("ab12!", "cd12!"))
    raw = model.to_dict()
    table = raw["slot_tables"].pop(next(iter(raw["slot_tables"])))
    table["symbol"] = "WORD"
    raw["slot_tables"]["WORD"] = table
    raw["structures"][0]["symbols"] = ["WORD"]
    bad_model = type(model).from_dict(raw)

    exporter = PcfgManagerRuleExporter(bad_model)

    try:
        exporter.export(tmp_path / "bad")
    except ValueError as exc:
        assert "one-letter slot symbols" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("export should reject unsupported slot names")
