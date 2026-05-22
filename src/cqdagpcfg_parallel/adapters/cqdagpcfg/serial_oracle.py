from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from CQDAGPCFG import GuessRecord, OptimizedCQDAGEnumerator
from CQDAGPCFG.enumeration.optimized.builders import BlockFactoryBuilder
from CQDAGPCFG.cpp_backend import CppOptimizedCQDAGEnumerator, cpp_backend_available

from cqdagpcfg_parallel.protocol import STABLE_PROBABILITY_DIGITS, stable_digest


@dataclass(frozen=True, slots=True)
class SerialOracleResult:
    outputs: tuple[GuessRecord, ...]
    digest: str


class SerialCQDAGOracle:
    def __init__(
        self,
        model,
        *,
        prefer_cpp: bool = False,
        factory_builder: BlockFactoryBuilder | None = None,
    ) -> None:
        self.model = model
        self.prefer_cpp = prefer_cpp
        self.factory_builder = factory_builder

    def enumerator(self):
        if self.factory_builder is None and self.prefer_cpp and cpp_backend_available():
            return CppOptimizedCQDAGEnumerator(self.model)
        return OptimizedCQDAGEnumerator(
            self.model,
            factory_builder=self.factory_builder,
        )

    def iter_records(self, limit: int) -> Iterable[GuessRecord]:
        if limit < 0:
            raise ValueError("limit cannot be negative")
        return _canonicalize_tie_groups(self.enumerator().iter_records(limit))

    def records(self, limit: int) -> tuple[GuessRecord, ...]:
        return tuple(self.iter_records(limit))

    def run(self, limit: int) -> SerialOracleResult:
        outputs = self.records(limit)
        return SerialOracleResult(outputs=outputs, digest=stable_digest(outputs))


__all__ = [
    "SerialCQDAGOracle",
    "SerialOracleResult",
]


def _canonicalize_tie_groups(records: Iterable[GuessRecord]) -> Iterable[GuessRecord]:
    group: list[GuessRecord] = []
    group_key: str | None = None
    for record in records:
        key = _probability_group_key(record)
        if group and key != group_key:
            yield from sorted(group, key=_canonical_tie_key)
            group = []
        group_key = key
        group.append(record)
    if group:
        yield from sorted(group, key=_canonical_tie_key)


def _probability_group_key(record: GuessRecord) -> str:
    return f"{record.prob:.{STABLE_PROBABILITY_DIGITS}g}"


def _canonical_tie_key(record: GuessRecord) -> tuple[int, tuple[int, ...], str]:
    return (record.structure_index, record.ranks, record.guess)
