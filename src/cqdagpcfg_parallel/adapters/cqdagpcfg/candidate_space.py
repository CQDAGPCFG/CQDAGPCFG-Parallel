from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Protocol

from CQDAGPCFG import PcfgModel, load_model


@dataclass(frozen=True, slots=True)
class CandidateSpaceArtifacts:
    """Materialized views of one logical candidate space."""

    name: str
    model_path: Path
    pcfg_rules_dir: Path
    manifest_path: Path
    total_points: int
    metadata: Mapping[str, object]


class CandidateSpace(Protocol):
    """A swappable source of candidates for internal and external runners."""

    name: str

    def materialize(self, output_dir: Path) -> CandidateSpaceArtifacts:
        """Write all backend-specific views of the same candidate space."""


@dataclass(frozen=True, slots=True)
class CQDAGModelCandidateSpace:
    """Candidate space backed by an existing CQDAGPCFG model.

    The CQDAGPCFG model is the source of truth.  The PCFG rule directory is an
    export view for external tools such as pcfg-manager, so benchmark runners do
    not accidentally compare different grammars.
    """

    model_path: Path
    name: str = "cqdagpcfg-model"
    copy_model: bool = True

    def materialize(self, output_dir: Path) -> CandidateSpaceArtifacts:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        source_model_path = self.model_path.resolve()
        model_path = output_dir / "model.json" if self.copy_model else source_model_path
        if self.copy_model:
            shutil.copy2(source_model_path, model_path)
        model = load_model(model_path)

        pcfg_rules_dir = output_dir / "pcfg_rules"
        exporter = PcfgManagerRuleExporter(model)
        export = exporter.export(pcfg_rules_dir)
        manifest_path = output_dir / "candidate-space.json"
        metadata = {
            "name": self.name,
            "source_model_path": str(source_model_path),
            "model_path": str(model_path),
            "pcfg_rules_dir": str(pcfg_rules_dir),
            **export.metadata,
        }
        manifest = {
            "schema": "cqpcfg-candidate-space-v1",
            "name": self.name,
            "model_path": str(model_path),
            "pcfg_rules_dir": str(pcfg_rules_dir),
            "total_points": export.total_points,
            "metadata": metadata,
        }
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return CandidateSpaceArtifacts(
            name=self.name,
            model_path=model_path,
            pcfg_rules_dir=pcfg_rules_dir,
            manifest_path=manifest_path,
            total_points=export.total_points,
            metadata=metadata,
        )


@dataclass(frozen=True, slots=True)
class PcfgManagerExportResult:
    rules_dir: Path
    total_points: int
    metadata: Mapping[str, object]


class PcfgManagerRuleExporter:
    """Export a CQDAGPCFG model as a pcfg-manager-compatible rule directory."""

    _DIRECTORY_NAMES = {
        "A": "Alpha",
        "D": "Digits",
        "K": "Keyboard",
        "L": "Leet",
        "S": "Symbols",
        "W": "Words",
        "Y": "Years",
    }

    def __init__(self, model: PcfgModel) -> None:
        self.model = model

    def export(self, output_dir: Path) -> PcfgManagerExportResult:
        output_dir = Path(output_dir)
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)

        symbols = tuple(sorted(self.model.slot_tables))
        groups = _group_symbols_by_transition(symbols)
        for transition_id, group_symbols in groups.items():
            directory = output_dir / self._directory_for(transition_id)
            directory.mkdir(parents=True, exist_ok=True)
            for symbol in group_symbols:
                _write_probability_file(
                    directory / f"{_symbol_length(symbol)}.txt",
                    (
                        (entry.surface, float(entry.prob))
                        for entry in self.model.slot_tables[symbol].entries
                    ),
                )

        grammar_dir = output_dir / "Grammar"
        grammar_dir.mkdir(parents=True, exist_ok=True)
        _write_probability_file(
            grammar_dir / "Grammar.txt",
            (
                ("".join(structure.symbols), float(structure.base_prob))
                for structure in self.model.structures
            ),
        )
        (output_dir / "config.ini").write_text(
            self._config_ini(groups),
            encoding="utf-8",
        )
        total_points = int(self.model.total_points())
        return PcfgManagerExportResult(
            rules_dir=output_dir,
            total_points=total_points,
            metadata={
                "pcfg_manager_export": {
                    "transition_ids": sorted(groups),
                    "slot_count": len(symbols),
                    "structure_count": len(self.model.structures),
                    "total_points": total_points,
                    "format": "copy-terminal-cqdagpcfg-v1",
                },
            },
        )

    def _config_ini(self, groups: Mapping[str, tuple[str, ...]]) -> str:
        replacements = [
            {"Transition_id": transition_id, "Config_id": f"BASE_{transition_id}"}
            for transition_id in sorted(groups)
        ]
        lines = [
            "[TRAINING_PROGRAM_DETAILS]",
            "program = cqdagpcfg_parallel",
            "version = candidate-space-v1",
            "",
            "[TRAINING_DATASET_DETAILS]",
            "comments = exported from CQDAGPCFG model",
            "",
            "[START]",
            "name = Base Structure",
            "function = Transparent",
            "directory = Grammar",
            "file_type = Flat",
            "inject_type = Wordlist",
            "is_terminal = False",
            f"replacements = {json.dumps(replacements, separators=(',', ':'))}",
            'filenames = ["Grammar.txt"]',
            "",
        ]
        for transition_id in sorted(groups):
            filenames = [
                f"{_symbol_length(symbol)}.txt"
                for symbol in sorted(groups[transition_id], key=_symbol_length)
            ]
            directory = self._directory_for(transition_id)
            lines.extend(
                [
                    f"[BASE_{transition_id}]",
                    f"name = {transition_id}",
                    "function = Copy",
                    f"directory = {directory}",
                    "file_type = Length",
                    "inject_type = Copy",
                    "is_terminal = True",
                    f"filenames = {json.dumps(filenames)}",
                    "",
                ],
            )
        return "\n".join(lines)

    def _directory_for(self, transition_id: str) -> str:
        return self._DIRECTORY_NAMES.get(transition_id, f"Slot_{transition_id}")


def _group_symbols_by_transition(symbols: Iterable[str]) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for symbol in symbols:
        transition_id = _transition_id(symbol)
        grouped.setdefault(transition_id, []).append(symbol)
    return {key: tuple(value) for key, value in grouped.items()}


def _transition_id(symbol: str) -> str:
    if len(symbol) < 2 or not symbol[0].isupper() or not symbol[1:].isdigit():
        raise ValueError(
            "pcfg-manager export requires one-letter slot symbols with numeric "
            f"length suffixes, got {symbol!r}",
        )
    return symbol[0]


def _symbol_length(symbol: str) -> int:
    _transition_id(symbol)
    return int(symbol[1:])


def _write_probability_file(
    path: Path,
    rows: Iterable[tuple[str, float]],
) -> None:
    previous_prob = float("inf")
    with path.open("w", encoding="utf-8") as handle:
        wrote = False
        for surface, probability in rows:
            if "\t" in surface or "\n" in surface or "\r" in surface:
                raise ValueError(
                    f"pcfg-manager export cannot encode tab/newline surface {surface!r}",
                )
            if probability <= 0.0:
                raise ValueError(f"probability must be positive for {surface!r}")
            if probability > previous_prob:
                raise ValueError(
                    f"probability rows must be descending in {path}: "
                    f"{probability} > {previous_prob}",
                )
            handle.write(f"{surface}\t{probability:.17g}\n")
            previous_prob = probability
            wrote = True
    if not wrote:
        raise ValueError(f"cannot write empty probability file: {path}")


__all__ = [
    "CandidateSpace",
    "CandidateSpaceArtifacts",
    "CQDAGModelCandidateSpace",
    "PcfgManagerExportResult",
    "PcfgManagerRuleExporter",
]
