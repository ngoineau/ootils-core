"""
MPS (Master Production Schedule) module for Ootils Core.

Consolidates demand for finished goods before propagation to MRP.
"""

from .models import (
    MPSNode,
    MPSStatus,
    MPSPlannedForEdge,
    MPSSuppliesEdge,
)
from .engine import AggregateDemandEngine, AggregateDemandRequest, AggregateDemandResult, MPSNodeSummary, PromoteToMRPResult
from .capacity_engine import (
    CapacityCheckEngine,
    CapacityViolation,
    AdjustmentSuggestion,
    CapacityCheckResult,
)
from .api import router

__all__ = [
    "MPSNode",
    "MPSStatus",
    "MPSPlannedForEdge",
    "MPSSuppliesEdge",
    "AggregateDemandEngine",
    "AggregateDemandRequest",
    "AggregateDemandResult",
    "MPSNodeSummary",
    "PromoteToMRPResult",
    "CapacityCheckEngine",
    "CapacityViolation",
    "AdjustmentSuggestion",
    "CapacityCheckResult",
    "router",
]
