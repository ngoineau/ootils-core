"""
Demand-descent engine (DESC-1) — national planning -> per-DC execution.

shares: pure, DB-free, deterministic split-share maths (item x state
    history -> item x distribution-center pct), the primitive a future
    calibration job applies to populate ``demand_split_pct``. See
    ``shares`` module docstring for the full business model.
run: the descent RUN itself (PR-B, ADR-043 §1) — reads national demand +
    the scenario-resolved ``demand_split_pct``/``item_dc_eligibility`` and
    materializes per-DC demand nodes. See ``run`` module docstring.
"""
from ootils_core.engine.descent.run import (
    DescentError,
    DescentResult,
    execute_descent,
)
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
    "DescentError",
    "DescentResult",
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
    "execute_descent",
]
