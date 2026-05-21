"""Boundary layer for using CQDAGPCFG as a serial oracle and block provider."""

from .block_graph import (
    ROOT_NODE_ID,
    BlockNodeDescriptor,
    CQDAGBlockGraphAdapter,
    CQDAGRecordSource,
    CQDAGSourceReclaimStats,
    CQDAGStructureRecordSource,
    slot_entropy,
    slot_entropy_bound,
)
from .paged_source import (
    PagedCQDAGModelClient,
    PagedCQDAGRecordSource,
    PagedCQDAGStructureRecordSource,
    PagedModelStats,
    PagedPcfgModel,
    PagedSlotTable,
    build_paged_model,
)
from .serial_oracle import SerialCQDAGOracle, SerialOracleResult

__all__ = [
    "BlockNodeDescriptor",
    "CQDAGBlockGraphAdapter",
    "CQDAGRecordSource",
    "CQDAGSourceReclaimStats",
    "CQDAGStructureRecordSource",
    "PagedCQDAGModelClient",
    "PagedCQDAGRecordSource",
    "PagedCQDAGStructureRecordSource",
    "PagedModelStats",
    "PagedPcfgModel",
    "PagedSlotTable",
    "ROOT_NODE_ID",
    "SerialCQDAGOracle",
    "SerialOracleResult",
    "build_paged_model",
    "slot_entropy",
    "slot_entropy_bound",
]
