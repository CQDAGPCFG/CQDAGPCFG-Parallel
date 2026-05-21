"""Memory-bounded distributed protocol runtime for CQDAGPCFG."""

from .adapters.cqdagpcfg import CQDAGCandidate, cqdagpcfg, pcfg
from .distributed import NodeAgent, RoleClient, RoleController, WorkerResourceSpec
from .framework_logging import configure_framework_logging
from .runtime import CandidateBatch, CandidateBatchSink, CandidateBatchSource

__version__ = "0.1.0"

__all__ = [
    "CandidateBatch",
    "CandidateBatchSink",
    "CandidateBatchSource",
    "CQDAGCandidate",
    "NodeAgent",
    "pcfg",
    "cqdagpcfg",
    "RoleClient",
    "RoleController",
    "WorkerResourceSpec",
    "configure_framework_logging",
    "__version__",
]
