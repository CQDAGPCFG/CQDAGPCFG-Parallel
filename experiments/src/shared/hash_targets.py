from __future__ import annotations

from typing import Any, Mapping


class ExperimentHashTargets:
    """Experiment-side target hash table.

    The framework streams password candidates. The experiment decides how to
    digest candidates and which digests count as hits.
    """

    def __init__(self, targets: Mapping[str, Any]) -> None:
        self.by_hash: dict[str, list[dict[str, Any]]] = {}
        for target in targets["targets"]:
            self.by_hash.setdefault(str(target["hash"]), []).append(dict(target))

    def match_digest(self, digest: str) -> list[dict[str, Any]]:
        return [
            {
                "target_rank": int(target["rank"]),
                "hash": digest,
            }
            for target in self.by_hash.get(digest, ())
        ]


__all__ = ["ExperimentHashTargets"]
