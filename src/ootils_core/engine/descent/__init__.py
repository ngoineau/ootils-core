"""
Demand-descent engine (DESC-1) — national planning -> per-DC execution.

shares: pure, DB-free, deterministic split-share maths (item x state
    history -> item x distribution-center pct), the primitive the descent
    run (PR-B) applies to materialize per-DC demand nodes. See
    ``shares`` module docstring for the full business model.
"""
from ootils_core.engine.descent.shares import (
    DEFAULT_CONFIDENCE_SATURATION_QTY,
    METHOD_EQUAL_SPLIT,
    METHOD_HISTORY,
    DcEligibility,
    EqualSplitResult,
    HistorySplitResult,
    ShareComputationError,
    SplitComputation,
    SplitShare,
    StateDcRoute,
    StateDemandObservation,
    UnroutedState,
    compute_split_computation,
    compute_split_shares,
    equal_split_shares,
)

__all__ = [
    "DEFAULT_CONFIDENCE_SATURATION_QTY",
    "METHOD_EQUAL_SPLIT",
    "METHOD_HISTORY",
    "DcEligibility",
    "EqualSplitResult",
    "HistorySplitResult",
    "ShareComputationError",
    "SplitComputation",
    "SplitShare",
    "StateDcRoute",
    "StateDemandObservation",
    "UnroutedState",
    "compute_split_computation",
    "compute_split_shares",
    "equal_split_shares",
]
