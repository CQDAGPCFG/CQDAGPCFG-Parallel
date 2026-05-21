from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from CQDAGPCFG import load_model

from cqdagpcfg_parallel.distributed import JobContext, WorkerResourceSpec
from cqdagpcfg_parallel.runtime import ZmqModelArtifactClient
from cqdagpcfg_parallel.runtime.zmq_transport import ZmqEndpoint
from cqdagpcfg_parallel.storage import FileModelArtifactCache, file_model_fingerprint

from .block_graph import (
    CQDAGBlockGraphAdapter,
    CQDAGRecordSource,
    CQDAGStructureRecordSource,
)
from .paged_source import (
    PagedCQDAGRecordSource,
    PagedCQDAGStructureRecordSource,
    build_paged_model,
)


@dataclass(frozen=True, slots=True)
class CQDAGNodeSourceConfig:
    model_path: Path | None
    model_connect: str
    model_id: str
    source_mode: str = "root"
    demand_window: int = 8
    disable_paged_source: bool = False
    model_json_page_cache: int = 128

    def __post_init__(self) -> None:
        if self.source_mode not in {"root", "structure"}:
            raise ValueError("source_mode must be root or structure")
        if self.demand_window < 0:
            raise ValueError("demand_window cannot be negative")
        if self.model_json_page_cache <= 0:
            raise ValueError("model_json_page_cache must be positive")

    @classmethod
    def from_job_context(
        cls,
        job_context: JobContext,
        *,
        disable_paged_source: bool = False,
        model_json_page_cache: int = 128,
        resources: WorkerResourceSpec | None = None,
    ) -> "CQDAGNodeSourceConfig":
        return cls(
            model_path=None,
            model_connect=job_context.model_connect,
            model_id=job_context.model_id,
            source_mode=job_context.source_mode,
            demand_window=job_context.demand_window,
            disable_paged_source=disable_paged_source,
            model_json_page_cache=_resource_page_cache(model_json_page_cache, resources),
        )

    @classmethod
    def from_explicit_model(
        cls,
        *,
        model_path: Path | None,
        model_connect: str | None,
        model_id: str,
        source_mode: str = "root",
        demand_window: int = 8,
        disable_paged_source: bool = False,
        model_json_page_cache: int = 128,
        resources: WorkerResourceSpec | None = None,
    ) -> "CQDAGNodeSourceConfig":
        if model_path is None and model_connect is None:
            raise ValueError("model_path, model_connect, or JobContext is required")
        return cls(
            model_path=model_path,
            model_connect=model_connect or "cqpcfg://127.0.0.1:0",
            model_id=model_id,
            source_mode=source_mode,
            demand_window=demand_window,
            disable_paged_source=disable_paged_source,
            model_json_page_cache=_resource_page_cache(model_json_page_cache, resources),
        )


def build_cqdag_node_source(
    config: CQDAGNodeSourceConfig,
    *,
    model_cache_dir: Path | None = None,
    limit: int,
    expected_fingerprint: str | None = None,
):
    if config.model_path is None and not config.disable_paged_source:
        model = build_paged_model(
            endpoint=config.model_connect,
            model_id=config.model_id,
            max_json_pages=config.model_json_page_cache,
        )
        if (
            expected_fingerprint is not None
            and model.paged_manifest.model_fingerprint != expected_fingerprint
        ):
            raise RuntimeError("paged model fingerprint does not match targets")
        if config.source_mode == "structure":
            return PagedCQDAGStructureRecordSource(
                model,
                max_records_per_structure=limit + config.demand_window,
            )
        return PagedCQDAGRecordSource(
            model,
            max_records=limit + config.demand_window,
        )

    model_path = resolve_cqdag_model_path(
        config,
        model_cache_dir=model_cache_dir,
        expected_fingerprint=expected_fingerprint,
    )
    model = load_model(model_path)
    adapter = CQDAGBlockGraphAdapter(model)
    if config.source_mode == "structure":
        return CQDAGStructureRecordSource(
            model,
            max_records_per_structure=limit + config.demand_window,
            adapter=adapter,
        )
    return CQDAGRecordSource(
        model,
        max_records=limit + config.demand_window,
    )


def resolve_cqdag_model_path(
    config: CQDAGNodeSourceConfig,
    *,
    model_cache_dir: Path | None,
    expected_fingerprint: str | None,
) -> Path:
    if config.model_path is not None:
        if (
            expected_fingerprint is not None
            and file_model_fingerprint(config.model_path) != expected_fingerprint
        ):
            raise RuntimeError("local model fingerprint does not match targets")
        return config.model_path
    cache_dir = (
        model_cache_dir
        if model_cache_dir is not None
        else Path.home() / ".cache" / "cqdagpcfg_parallel" / "models"
    )
    cache = FileModelArtifactCache(cache_dir)
    with ZmqModelArtifactClient(
        ZmqEndpoint.from_uri(config.model_connect, bind=False)
    ) as client:
        model_path, manifest = cache.materialize(client, config.model_id)
    if (
        expected_fingerprint is not None
        and manifest.model_fingerprint != expected_fingerprint
    ):
        raise RuntimeError("fetched model fingerprint does not match targets")
    return model_path


def _resource_page_cache(
    default_value: int,
    resources: WorkerResourceSpec | None,
) -> int:
    if resources is None or resources.model_json_page_cache is None:
        return default_value
    return resources.model_json_page_cache


__all__ = [
    "CQDAGNodeSourceConfig",
    "build_cqdag_node_source",
    "resolve_cqdag_model_path",
]
