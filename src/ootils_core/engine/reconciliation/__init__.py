"""
Heuristic reconciliation of inbound ERP purchase orders against exported
recommendations (ADR-042 decision 4, PR-5b; migration 086).

matcher: the OBSERVATION-only matcher that pairs an inbound ``PurchaseOrderSupply``
    node with an already-exported recommendation on business attributes (item,
    qty ± tolerance, date ± window) and stamps ``recommendations.fulfilled_at``
    / ``fulfilled_erp_id`` for the unambiguous pairs — never touching
    ``recommendations.status`` or the state machine. ``match_candidates`` is the
    pure, DB-free, deterministic core; ``run_reconciliation`` is the sole writer
    (load → match → stamp → one ``reconciliation_runs`` row → one
    ``reconciliation_completed`` event). See ``matcher.py``'s module docstring
    for the two KNOWN GAPS (the PO node carries no supplier; non-TRANSFER recos
    carry no location).
"""
from ootils_core.engine.reconciliation.matcher import (
    DATE_TOLERANCE_DAYS,
    QTY_TOLERANCE_PCT,
    InboundPO,
    MatchResult,
    RecoCandidate,
    ReconciliationRunResult,
    match_candidates,
    run_reconciliation,
)

__all__ = [
    "DATE_TOLERANCE_DAYS",
    "QTY_TOLERANCE_PCT",
    "InboundPO",
    "MatchResult",
    "RecoCandidate",
    "ReconciliationRunResult",
    "match_candidates",
    "run_reconciliation",
]
