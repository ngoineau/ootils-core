"""
DRP (Distribution Requirements Planning) engine — the distribution echelon of
the single netting cascade (ADR-020 §Unité de planification / ADR-028).

core  : pure, DB-free, deterministic transfer-signal maths, keyed by
        (item, location) — the per-site echelon (mrp/core is the item-level
        make/buy echelon).
loader: SELECT-only, scenario-parameterized load into DRPData (safety stock
        overlay-resolved via #347, so the plan is forkable).
"""
from ootils_core.engine.drp.core import (
    TransferLink,
    TransferSignal,
    excess_by_location,
    projected_deficits,
    transfer_signals,
)
from ootils_core.engine.drp.loader import DRPData, load_drp_data

__all__ = [
    "DRPData",
    "TransferLink",
    "TransferSignal",
    "excess_by_location",
    "load_drp_data",
    "projected_deficits",
    "transfer_signals",
]
