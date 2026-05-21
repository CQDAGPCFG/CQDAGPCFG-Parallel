from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from CQDAGPCFG import GuessRecord, OptimizedCQDAGEnumerator
from CQDAGPCFG.cpp_backend import CppOptimizedCQDAGEnumerator, cpp_backend_available

from cqdagpcfg_parallel.protocol import stable_digest


@dataclass(frozen=True, slots=True)
class SerialOracleResult:
    outputs: tuple[GuessRecord, ...]
    digest: str


class SerialCQDAGOracle:
    def __init__(self, model, *, prefer_cpp: bool = True) -> None:
        self.model = model
        self.prefer_cpp = prefer_cpp

    def enumerator(self):
        if self.prefer_cpp and cpp_backend_available():
            return CppOptimizedCQDAGEnumerator(self.model)
        return OptimizedCQDAGEnumerator(self.model)

    def iter_records(self, limit: int) -> Iterable[GuessRecord]:
        if limit < 0:
            raise ValueError("limit cannot be negative")
        return self.enumerator().iter_records(limit)

    def records(self, limit: int) -> tuple[GuessRecord, ...]:
        return tuple(self.iter_records(limit))

    def run(self, limit: int) -> SerialOracleResult:
        outputs = self.records(limit)
        return SerialOracleResult(outputs=outputs, digest=stable_digest(outputs))


__all__ = [
    "SerialCQDAGOracle",
    "SerialOracleResult",
]
