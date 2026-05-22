from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from CQDAGPCFG import load_model
from CQDAGPCFG.cpp_backend import cpp_backend_available

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


CQDAGGenerationBackend = Literal["auto", "cpp", "paged", "python"]


@dataclass(frozen=True, slots=True)
class CQDAGNodeSourceConfig:
    model_path: Path | None
    model_connect: str
    model_id: str
    source_mode: str = "root"
    demand_window: int = 8
    disable_paged_source: bool = False
    model_json_page_cache: int = 128
    generation_backend: CQDAGGenerationBackend = "auto"

    def __post_init__(self) -> None:
        if self.source_mode not in {"root", "structure"}:
            raise ValueError("source_mode must be root or structure")
        if self.demand_window < 0:
            raise ValueError("demand_window cannot be negative")
        if self.model_json_page_cache <= 0:
            raise ValueError("model_json_page_cache must be positive")
        if self.generation_backend not in {"auto", "cpp", "paged", "python"}:
            raise ValueError("generation_backend must be auto, cpp, paged, or python")

    @classmethod
    def from_job_context(
        cls,
        job_context: JobContext,
        *,
        disable_paged_source: bool = False,
        model_json_page_cache: int = 128,
        generation_backend: CQDAGGenerationBackend = "auto",
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
            generation_backend=generation_backend,
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
        generation_backend: CQDAGGenerationBackend = "auto",
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
            generation_backend=generation_backend,
        )


def build_cqdag_node_source(
    config: CQDAGNodeSourceConfig,
    *,
    model_cache_dir: Path | None = None,
    limit: int,
    expected_fingerprint: str | None = None,
):
    backend = resolve_generation_backend(config)
    if backend == "paged":
        model = build_paged_model(
            endpoint=config.model_connect,
            model_id=config.model_id,
            max_json_pages=config.model_json_page_cache,
        )
        if (
            expected_fingerprint is not None
            and model.paged_manifest.model_fingerprint != expected_fingerprint
        ):
            raise RuntimeError("paged model fingerprint does not match job spec")
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
            prefer_cpp=backend == "cpp",
        )
    return CQDAGRecordSource(
        model,
        max_records=limit + config.demand_window,
        prefer_cpp=backend == "cpp",
    )


def resolve_generation_backend(
    config: CQDAGNodeSourceConfig,
) -> CQDAGGenerationBackend:
    if config.disable_paged_source and config.generation_backend == "paged":
        raise ValueError("disable_paged_source cannot be used with generation_backend=paged")
    if config.generation_backend == "python":
        return "python"
    if config.generation_backend == "paged":
        return "paged"
    if config.generation_backend == "cpp":
        if not cpp_backend_available():
            raise RuntimeError(
                "generation_backend=cpp requires the CQDAGPCFG C++ backend to be built"
            )
        return "cpp"
    if config.disable_paged_source:
        return "cpp" if cpp_backend_available() else "python"
    if cpp_backend_available():
        return "cpp"
    return "paged"


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
            raise RuntimeError("local model fingerprint does not match job spec")
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
        raise RuntimeError("fetched model fingerprint does not match job spec")
    return model_path


def _resource_page_cache(
    default_value: int,
    resources: WorkerResourceSpec | None,
) -> int:
    if resources is None or resources.model_json_page_cache is None:
        return default_value
    return resources.model_json_page_cache


__all__ = [
    "CQDAGGenerationBackend",
    "CQDAGNodeSourceConfig",
    "build_cqdag_node_source",
    "resolve_generation_backend",
    "resolve_cqdag_model_path",
]
