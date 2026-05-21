"""Memory-bounded distributed protocol runtime for CQDAGPCFG."""

from .distributed import NodeAgent, RoleClient, RoleController
from .runtime import CandidateBatch, CandidateBatchSink, CandidateBatchSource

__version__ = "0.1.0"

__all__ = [
    "CandidateBatch",
    "CandidateBatchSink",
    "CandidateBatchSource",
    "NodeAgent",
    "RoleClient",
    "RoleController",
    "__version__",
]
