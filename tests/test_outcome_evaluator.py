"""
Golden-master for the recommendation-outcome classifier (chantier #393 A3-PR2,
ADR-030): ``engine/outcome/evaluator.py::evaluate_outcome`` — the PURE, DB-free,
clock-free 5-way verdict + NULL-honest $ figures.

Same discipline as tests/test_drp_core_golden.py: every expected value below is
derived STEP BY STEP by hand from the documented semantics of evaluator.py
(module + function docstrings + the three threshold constants) BEFORE running —
none is copied back from an execution. If the evaluator ever disagrees with a
derivation here, the derivation is the contract and the divergence is a bug to
investigate, not a golden to "fix".

The five deterministic branches, with the exact knobs (evaluator.py:100-102):
    AVOIDED_EPS_RATIO        = 0.05   (observed <= predicted*0.05 -> effectively gone)
    AVOIDED_EPS_ABS          = 1      (absolute sub-unit floor, whichever is larger)
    MATERIALIZED_FLOOR_RATIO = 0.90   (observed >= predicted*0.90 -> not reduced)

The avoided-$ basis (evaluator.py:157-199):
    predicted_$ = predicted_deficit_qty * unit_cost
    unit_cost precedence: evidence['unit_cost'] (if > 0) -> estimated_cost /
    recommended_qty (if qty > 0) -> None (NULL-honest, no masked 0).

evaluate_outcome is pure (plain dict inputs, in-memory dataclasses), so this runs
with NO database — just the classifier math.
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import FrozenInstanceError
from decimal import Decimal
from uuid import UUID

import pytest

from ootils_core.engine.outcome.evaluator import (
    AVOIDED_EPS_ABS,
    AVOIDED_EPS_RATIO,
    MATERIALIZED_FLOOR_RATIO,
    VALID_STATUSES,
    ObservedShortage,
    _avoided_ceiling,
    _predicted_unit_cost,
    evaluate_outcome,
)

# Stable coordinates reused across cases (values are irrelevant to the math — the
# classifier keys nothing on the actual UUIDs, it only echoes recommendation_id /
# snapshot_id back onto the verdict).
RECO_ID = UUID("11111111-1111-1111-1111-111111111111")
ITEM_ID = UUID("22222222-2222-2222-2222-222222222222")
LOC_ID = UUID("33333333-3333-3333-3333-333333333333")
SNAP_ID = UUID("44444444-4444-4444-4444-444444444444")
AS_OF = _dt.date(2026, 7, 1)
SHORT_DATE = _dt.date(2026, 8, 15)


# ---------------------------------------------------------------------------
# Builders — a reco dict (a `recommendations` row projection) and the observed
# shortage / snapshot the pure evaluator is fed.
# ---------------------------------------------------------------------------


def _reco(
    *,
    status: str,
    deficit_qty=Decimal("100"),
    shortage_date=SHORT_DATE,
    evidence: dict | None = None,
    estimated_cost=None,
    recommended_qty=None,
) -> dict:
    """A minimal `recommendations` row as the evaluator reads it. deficit_qty and
    shortage_date are the frozen PREDICTION; status decides whether the reco
    acted; evidence / estimated_cost / recommended_qty give the $ basis."""
    return {
        "recommendation_id": RECO_ID,
        "status": status,
        "shortage_date": shortage_date,
        "deficit_qty": deficit_qty,
        "estimated_cost": estimated_cost,
        "recommended_qty": recommended_qty,
        "evidence": evidence,
    }


def _observed(deficit_qty, *, severity_usd=Decimal("0")) -> ObservedShortage:
    return ObservedShortage(
        item_id=ITEM_ID,
        location_id=LOC_ID,
        shortage_date=SHORT_DATE,
        deficit_qty=Decimal(str(deficit_qty)),
        severity_usd=Decimal(str(severity_usd)),
    )


def _snapshot() -> dict:
    return {"snapshot_id": SNAP_ID, "item_id": ITEM_ID, "location_id": LOC_ID}


# A per-unit cost of 3.0 makes predicted_$ = 100 * 3 = 300 for the default reco.
UNIT_COST_EVIDENCE = {"unit_cost": 3.0}


# ===========================================================================
# 1. AVOIDED — acted reco, snapshot present, observed deficit effectively zero
# ===========================================================================


def test_avoided_observed_zero_credits_full_predicted_dollars():
    """Case 1 — AVOIDED (the headline win).
    Reco APPROVED (acted), predicted deficit 100, unit_cost 3.0 (evidence) ->
    predicted_$ = 100 * 3 = 300. Snapshot present. Observed deficit 0.
      avoided_ceiling = max(100*0.05, 1) = max(5.0, 1) = 5.0.
      observed 0 <= ceiling 5.0 -> AVOIDED.
    Verdict: observed_deficit_qty = 0 (Decimal(0), a HARD zero the branch sets),
    avoided_severity_usd = predicted_$ = 300. snapshot_id echoed.
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.observed_deficit_qty == Decimal(0)
    assert out.avoided_severity_usd == Decimal("300")
    assert out.predicted_deficit_qty == Decimal("100")
    assert out.predicted_shortage_date == SHORT_DATE
    assert out.snapshot_id == SNAP_ID
    assert out.recommendation_id == RECO_ID
    assert out.evaluated_as_of == AS_OF


def test_avoided_small_residual_under_ceiling_still_avoided():
    """Case 1b — a residual strictly UNDER the ceiling is still AVOIDED.
    Predicted 100 -> ceiling 5.0. Observed 4 (< 5.0) -> AVOIDED.
    observed_deficit_qty is forced to Decimal(0) by the branch (the residual is
    treated as noise, NOT carried through as 4). avoided_$ = full 300.
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(4),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.observed_deficit_qty == Decimal(0), "sub-ceiling residual is zeroed, not carried"
    assert out.avoided_severity_usd == Decimal("300")


def test_avoided_applied_status_also_acts():
    """APPLIED is the post-approval executed state and ALSO counts as acted
    (ACTED_STATUSES = {APPROVED, APPLIED}). Same AVOIDED verdict as APPROVED."""
    out = evaluate_outcome(
        _reco(status="APPLIED", evidence=UNIT_COST_EVIDENCE),
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.avoided_severity_usd == Decimal("300")


def test_avoided_no_observed_shortage_row_is_zero_deficit():
    """observed_shortage=None means "no active shortage observed at the
    coordinate" -> observed_qty defaults to Decimal(0) (evaluator.py:261-263) ->
    0 <= ceiling -> AVOIDED. The snapshot presence is what keeps it out of
    INDETERMINATE; None-observed + snapshot-present is the canonical avoided
    signal."""
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        None,
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.observed_deficit_qty == Decimal(0)
    assert out.avoided_severity_usd == Decimal("300")


# ===========================================================================
# 2. PARTIAL — reduced but not eliminated (between the two ratios)
# ===========================================================================


def test_partial_credits_avoided_fraction_of_predicted_dollars():
    """Case 2 — PARTIAL.
    APPROVED, predicted 100, unit_cost 3.0 -> predicted_$ = 300. Snapshot
    present. Observed 40:
      ceiling = 5.0 ; 40 > 5.0 (not AVOIDED).
      ratio = 40 / 100 = 0.40 ; 0.40 < 0.90 floor (not MATERIALIZED).
      -> PARTIAL, observed_deficit_qty carried through as 40.
      avoided_$ = predicted_$ * (1 - ratio) = 300 * (1 - 0.40) = 300 * 0.60 = 180.
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(40),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "PARTIAL"
    assert out.observed_deficit_qty == Decimal("40")
    assert out.avoided_severity_usd == Decimal("180")
    assert out.predicted_deficit_qty == Decimal("100")


def test_partial_just_below_materialized_floor():
    """Case 2b — observed strictly just under the floor is still PARTIAL, not
    MATERIALIZED. Predicted 100, observed 89:
      ratio = 89/100 = 0.89 < 0.90 -> PARTIAL (the < vs >= boundary lives at 90).
      avoided_$ = 300 * (1 - 0.89) = 300 * 0.11 = 33.00.
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(89),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "PARTIAL"
    # 300 * 0.11 — assert exactly (Decimal, no float drift): 300 * (1 - 89/100).
    assert out.avoided_severity_usd == Decimal("300") * (Decimal(1) - Decimal("89") / Decimal("100"))
    assert out.avoided_severity_usd == pytest.approx(33.0)


def test_partial_just_above_avoided_ceiling():
    """Case 2c — observed strictly just ABOVE the ceiling is PARTIAL, not
    AVOIDED. Predicted 100 -> ceiling 5.0. Observed 6 (> 5.0):
      ratio = 6/100 = 0.06 ; 0.05 (ceiling ratio) < 0.06 < 0.90 -> PARTIAL.
      avoided_$ = 300 * (1 - 0.06) = 300 * 0.94 = 282.00.
    Note the ceiling is the ABS/ratio MAX (5.0), NOT the raw ratio boundary — 6
    clears 5.0 so it is graded, not zeroed.
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(6),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "PARTIAL"
    assert out.observed_deficit_qty == Decimal("6")
    assert out.avoided_severity_usd == Decimal("300") * (Decimal(1) - Decimal("6") / Decimal("100"))


# ===========================================================================
# 3. MATERIALIZED — happened essentially as predicted (avoided = 0, NOT None)
# ===========================================================================


def test_materialized_observed_near_prediction_credits_zero_not_none():
    """Case 3 — MATERIALIZED.
    APPROVED, predicted 100, unit_cost 3.0. Snapshot present. Observed 95:
      ceiling 5.0 ; 95 > 5.0 (not AVOIDED).
      ratio = 95/100 = 0.95 >= 0.90 floor -> MATERIALIZED.
      avoided_severity_usd = Decimal(0) — a GENUINE zero (nothing avoided),
      DISTINCT from None ("not computable"). observed carried through as 95.
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(95),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "MATERIALIZED"
    assert out.observed_deficit_qty == Decimal("95")
    assert out.avoided_severity_usd == Decimal(0)
    assert out.avoided_severity_usd is not None, "MATERIALIZED credits a hard 0, never None"


def test_materialized_observed_exactly_predicted():
    """Observed == predicted (ratio 1.0 >= 0.90) -> MATERIALIZED, avoided 0."""
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(100),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "MATERIALIZED"
    assert out.avoided_severity_usd == Decimal(0)


def test_materialized_observed_worse_than_predicted():
    """Observed 150 > predicted 100 (ratio 1.5 >= 0.90) -> MATERIALIZED. The
    engine never credits a negative avoided (it is a hard 0, not 300*(1-1.5))."""
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(150),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "MATERIALIZED"
    assert out.observed_deficit_qty == Decimal("150")
    assert out.avoided_severity_usd == Decimal(0)


# ===========================================================================
# 4. NOT_APPLICABLE — the reco never acted (counter-factual, no credit)
# ===========================================================================


@pytest.mark.parametrize("status", ["DRAFT", "REVIEWED", "REJECTED", "EXPIRED"])
def test_not_applicable_when_reco_never_acted(status):
    """Case 4 — NOT_APPLICABLE.
    A reco NOT in ACTED_STATUSES ({APPROVED, APPLIED}) never influenced reality.
    Observed 100 (the shortage DID materialise). The branch fires BEFORE the
    snapshot check, so a snapshot is present but irrelevant:
      evaluation_status = NOT_APPLICABLE.
      observed_deficit_qty = the observed deficit (100) — the cost-of-inaction
        signal, recorded so KPI 5 can value it.
      avoided_severity_usd = None — NO credit (the reco did not act); crucially
        None, NOT 0.
    """
    out = evaluate_outcome(
        _reco(status=status, evidence=UNIT_COST_EVIDENCE),
        _observed(100),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "NOT_APPLICABLE"
    assert out.observed_deficit_qty == Decimal("100"), "cost-of-inaction: observed deficit recorded"
    assert out.avoided_severity_usd is None, "never-acted reco gets NO avoided credit (None, not 0)"
    assert out.predicted_deficit_qty == Decimal("100")


def test_not_applicable_no_shortage_records_zero_not_none():
    """A never-acted reco whose predicted shortage did NOT occur (observed None):
    observed_deficit_qty defaults to Decimal(0) (the cost-of-inaction signal is a
    genuine 0 here — nothing materialised), avoided still None (no credit)."""
    out = evaluate_outcome(
        _reco(status="DRAFT", evidence=UNIT_COST_EVIDENCE),
        None,
        None,
        AS_OF,
    )
    assert out.evaluation_status == "NOT_APPLICABLE"
    assert out.observed_deficit_qty == Decimal(0)
    assert out.avoided_severity_usd is None
    # No snapshot passed -> snapshot_id is None on the verdict.
    assert out.snapshot_id is None


def test_not_applicable_carries_snapshot_id_when_present():
    """The NOT_APPLICABLE branch echoes snapshot_id when a snapshot was supplied
    (it is computed before the branch, evaluator.py:253-255)."""
    out = evaluate_outcome(
        _reco(status="REJECTED"),
        _observed(50),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "NOT_APPLICABLE"
    assert out.snapshot_id == SNAP_ID


# ===========================================================================
# 5. INDETERMINATE — acted reco with NO observation snapshot
# ===========================================================================


def test_indeterminate_acted_but_no_snapshot():
    """Case 5 — INDETERMINATE.
    APPROVED (acted) but snapshot_row=None: without a point-in-time observation
    we cannot honestly assert the deficit was avoided. Everything observed/avoided
    is None (honest); predicted_* still reported; snapshot_id forced to None.
    Even though observed_shortage says 0, the ABSENCE of a snapshot dominates.
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(0),
        None,
        AS_OF,
    )
    assert out.evaluation_status == "INDETERMINATE"
    assert out.observed_deficit_qty is None
    assert out.avoided_severity_usd is None
    assert out.snapshot_id is None
    assert out.predicted_deficit_qty == Decimal("100"), "prediction still reported"
    assert out.predicted_shortage_date == SHORT_DATE


def test_indeterminate_acted_no_snapshot_even_with_observed_shortage():
    """APPLIED (acted), a real observed shortage (60), but NO snapshot -> still
    INDETERMINATE (the snapshot gate is checked before the deficit comparison)."""
    out = evaluate_outcome(
        _reco(status="APPLIED", evidence=UNIT_COST_EVIDENCE),
        _observed(60),
        None,
        AS_OF,
    )
    assert out.evaluation_status == "INDETERMINATE"
    assert out.observed_deficit_qty is None
    assert out.avoided_severity_usd is None


# ===========================================================================
# 6. Threshold boundaries + edge cases (the load-bearing arithmetic)
# ===========================================================================


def test_boundary_observed_exactly_at_avoided_ceiling_is_avoided():
    """Observed EXACTLY at the ceiling (5.0 for predicted 100) is AVOIDED — the
    comparison is `observed_qty <= avoided_ceiling` (inclusive at the ceiling).
    avoided_$ = full 300; observed zeroed."""
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(5),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.observed_deficit_qty == Decimal(0)
    assert out.avoided_severity_usd == Decimal("300")


def test_boundary_observed_exactly_at_materialized_floor():
    """Observed EXACTLY at the floor (90 for predicted 100) is MATERIALIZED — the
    comparison is `ratio >= MATERIALIZED_FLOOR_RATIO` (inclusive at the floor).
    ratio = 90/100 = 0.90 -> MATERIALIZED, avoided a hard 0."""
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(90),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "MATERIALIZED"
    assert out.observed_deficit_qty == Decimal("90")
    assert out.avoided_severity_usd == Decimal(0)


def test_boundary_tiny_predicted_uses_absolute_floor():
    """The absolute floor (AVOIDED_EPS_ABS=1) dominates for a tiny prediction.
    Predicted 2 -> ceiling = max(2*0.05, 1) = max(0.1, 1) = 1.0 (the ABS floor
    wins, so a tiny deficit is not held to an impossible 0.1 bar).
    Observed 1 <= ceiling 1.0 -> AVOIDED (unit_cost 3 -> predicted_$ = 2*3 = 6).
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", deficit_qty=Decimal("2"), evidence=UNIT_COST_EVIDENCE),
        _observed(1),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.avoided_severity_usd == Decimal("6")


def test_predicted_qty_none_with_shortage_is_materialized_prudent():
    """predicted_qty=None (reco carried no deficit figure) + a real observed
    shortage: documents what the code does. observed 50 > ceiling (ABS 1, since
    predicted None -> _avoided_ceiling returns AVOIDED_EPS_ABS=1). Then the
    `predicted_qty is None or <= 0` guard fires -> MATERIALIZED is the PRUDENT
    honest call (a shortage happened; nothing proven avoided). avoided = 0.
    observed carried through as 50; predicted_deficit_qty stays None.
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", deficit_qty=None, evidence=UNIT_COST_EVIDENCE),
        _observed(50),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "MATERIALIZED"
    assert out.predicted_deficit_qty is None
    assert out.observed_deficit_qty == Decimal("50")
    assert out.avoided_severity_usd == Decimal(0)


def test_predicted_qty_none_no_shortage_is_avoided_with_null_dollars():
    """predicted_qty=None + observed 0: the AVOIDED branch fires FIRST (0 <=
    ceiling 1). But predicted_severity is None (no deficit qty to value) ->
    avoided_severity_usd = None even though the verdict is AVOIDED. This is the
    NULL-honest $ discipline: an AVOIDED with no cost basis credits None, not 0.
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", deficit_qty=None, evidence=UNIT_COST_EVIDENCE),
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.observed_deficit_qty == Decimal(0)
    assert out.avoided_severity_usd is None, "AVOIDED with no predicted qty -> NULL $, not 0"


def test_predicted_qty_zero_with_shortage_is_materialized():
    """predicted_qty = 0 (not None): _avoided_ceiling returns ABS 1 (the
    `<= 0` guard), observed 10 > 1 -> not AVOIDED, then `predicted_qty <= 0` ->
    MATERIALIZED, avoided 0."""
    out = evaluate_outcome(
        _reco(status="APPROVED", deficit_qty=Decimal("0"), evidence=UNIT_COST_EVIDENCE),
        _observed(10),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "MATERIALIZED"
    assert out.avoided_severity_usd == Decimal(0)


def test_avoided_with_no_unit_cost_credits_null_dollars():
    """NULL-honest $: an AVOIDED verdict with NO derivable unit cost (no evidence
    unit_cost, no estimated_cost/recommended_qty) credits avoided_severity_usd =
    None, NEVER a masked 0 — even though the classification IS AVOIDED.
    Predicted 100, observed 0 -> AVOIDED; unit_cost None -> predicted_$ None.
    """
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=None),  # no evidence, no est/qty
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.observed_deficit_qty == Decimal(0)
    assert out.avoided_severity_usd is None, "no cost basis -> NULL avoided, distinct from 0"


def test_partial_with_no_unit_cost_credits_null_dollars():
    """PARTIAL with no cost basis -> avoided fraction is None (NULL-honest),
    while the observed_deficit_qty (40) and the verdict are still recorded."""
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=None),
        _observed(40),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "PARTIAL"
    assert out.observed_deficit_qty == Decimal("40")
    assert out.avoided_severity_usd is None


def test_unit_cost_fallback_estimated_cost_over_recommended_qty():
    """The $ basis fallback: no evidence['unit_cost'], but estimated_cost=600 and
    recommended_qty=120 -> unit_cost = 600/120 = 5.0. Predicted 100, observed 0
    -> AVOIDED, avoided_$ = 100 * 5.0 = 500.
    """
    out = evaluate_outcome(
        _reco(
            status="APPROVED",
            evidence=None,
            estimated_cost=Decimal("600"),
            recommended_qty=Decimal("120"),
        ),
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.avoided_severity_usd == Decimal("500")


def test_evidence_unit_cost_wins_over_estimated_cost_fallback():
    """Precedence: evidence['unit_cost']=3.0 is used even when estimated_cost/
    recommended_qty would give a DIFFERENT per-unit (600/120 = 5.0). Predicted
    100, observed 0 -> AVOIDED, avoided_$ = 100 * 3.0 = 300 (evidence wins)."""
    out = evaluate_outcome(
        _reco(
            status="APPROVED",
            evidence={"unit_cost": 3.0},
            estimated_cost=Decimal("600"),
            recommended_qty=Decimal("120"),
        ),
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.avoided_severity_usd == Decimal("300")


def test_evidence_unit_cost_non_positive_ignored_falls_back():
    """A non-positive evidence unit_cost (0) is ignored (`uc > 0` guard) and the
    estimated_cost/recommended_qty fallback (600/120 = 5.0) takes over.
    Predicted 100, observed 0 -> AVOIDED, avoided_$ = 100 * 5.0 = 500."""
    out = evaluate_outcome(
        _reco(
            status="APPROVED",
            evidence={"unit_cost": 0},
            estimated_cost=Decimal("600"),
            recommended_qty=Decimal("120"),
        ),
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.avoided_severity_usd == Decimal("500")


def test_unit_cost_none_when_recommended_qty_zero():
    """estimated_cost present but recommended_qty = 0 -> the `qty > 0` guard
    blocks the division -> unit_cost None -> NULL-honest avoided even on AVOIDED.
    """
    out = evaluate_outcome(
        _reco(
            status="APPROVED",
            evidence=None,
            estimated_cost=Decimal("600"),
            recommended_qty=Decimal("0"),
        ),
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.avoided_severity_usd is None


def test_evidence_unit_cost_from_string_is_decimal_exact():
    """evidence['unit_cost'] as a STRING ('2.5') is coerced via Decimal(str(...))
    without float drift. Predicted 100, observed 0 -> AVOIDED, avoided_$ =
    100 * 2.5 = 250.00 exactly."""
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence={"unit_cost": "2.5"}),
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"
    assert out.avoided_severity_usd == Decimal("250.0")


def test_string_and_uuid_reco_id_coerced():
    """recommendation_id / snapshot_id supplied as strings (as a dict_row may
    carry them) are coerced to UUID on the verdict (_coerce_uuid)."""
    reco = _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE)
    reco["recommendation_id"] = str(RECO_ID)
    snap = {"snapshot_id": str(SNAP_ID)}
    out = evaluate_outcome(reco, _observed(0), snap, AS_OF)
    assert out.recommendation_id == RECO_ID
    assert isinstance(out.recommendation_id, UUID)
    assert out.snapshot_id == SNAP_ID
    assert isinstance(out.snapshot_id, UUID)


def test_status_case_insensitive_lowercase_approved_acts():
    """status is upper-cased before the ACTED check (`str(...).upper()`), so a
    lowercase 'approved' still counts as acted -> AVOIDED, not NOT_APPLICABLE."""
    out = evaluate_outcome(
        _reco(status="approved", evidence=UNIT_COST_EVIDENCE),
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    assert out.evaluation_status == "AVOIDED"


# ===========================================================================
# 7. Helper-level golden checks (the ceiling + unit-cost precedence in isolation)
# ===========================================================================


def test_avoided_ceiling_helper_arithmetic():
    """_avoided_ceiling: max(qty*0.05, 1), ABS floor when qty is None/<=0."""
    assert _avoided_ceiling(Decimal("100")) == Decimal("5.00")  # 100*0.05 wins
    assert _avoided_ceiling(Decimal("2")) == Decimal("1")  # ABS floor wins (0.1 < 1)
    assert _avoided_ceiling(Decimal("20")) == Decimal("1.00")  # 20*0.05 = 1.0 == ABS
    assert _avoided_ceiling(None) == AVOIDED_EPS_ABS  # no prediction -> ABS alone
    assert _avoided_ceiling(Decimal("0")) == AVOIDED_EPS_ABS
    assert _avoided_ceiling(Decimal("-5")) == AVOIDED_EPS_ABS


def test_predicted_unit_cost_precedence_helper():
    """_predicted_unit_cost: evidence['unit_cost'] (>0) first, else
    estimated_cost/recommended_qty (qty>0), else None."""
    assert _predicted_unit_cost({"evidence": {"unit_cost": 4.0}}) == Decimal("4.0")
    # evidence wins over est/qty
    assert _predicted_unit_cost(
        {"evidence": {"unit_cost": 4.0}, "estimated_cost": 600, "recommended_qty": 120}
    ) == Decimal("4.0")
    # est/qty fallback when no evidence
    assert _predicted_unit_cost({"estimated_cost": 600, "recommended_qty": 120}) == Decimal("5")
    # non-positive evidence ignored -> falls back
    assert _predicted_unit_cost(
        {"evidence": {"unit_cost": 0}, "estimated_cost": 600, "recommended_qty": 120}
    ) == Decimal("5")
    # nothing derivable -> None
    assert _predicted_unit_cost({}) is None
    assert _predicted_unit_cost({"evidence": None}) is None
    assert _predicted_unit_cost({"estimated_cost": 600, "recommended_qty": 0}) is None


# ===========================================================================
# 8. Contract guards — thresholds, frozen dataclasses, VALID_STATUSES == CHECK
# ===========================================================================


def test_threshold_constants_are_exact():
    """The three documented knobs, pinned (a change here is a deliberate contract
    change — the golden derivations above depend on these exact values)."""
    assert AVOIDED_EPS_RATIO == Decimal("0.05")
    assert AVOIDED_EPS_ABS == Decimal("1")
    assert MATERIALIZED_FLOOR_RATIO == Decimal("0.90")


def test_outcome_row_is_frozen():
    out = evaluate_outcome(
        _reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE),
        _observed(0),
        _snapshot(),
        AS_OF,
    )
    with pytest.raises(FrozenInstanceError):
        out.evaluation_status = "MATERIALIZED"  # type: ignore[misc]


def test_observed_shortage_is_frozen():
    obs = _observed(10)
    assert isinstance(obs, ObservedShortage)
    with pytest.raises(FrozenInstanceError):
        obs.deficit_qty = Decimal("999")  # type: ignore[misc]


def test_valid_statuses_matches_migration_069_check():
    """VALID_STATUSES is the Python half of the migration-069 evaluation_status
    CHECK. This guard reads the CHECK's five literals straight from the migration
    SQL and asserts set equality — if either side drifts, this fails.
    """
    import pathlib
    import re

    migration = (
        pathlib.Path(__file__).resolve().parents[1]
        / "src" / "ootils_core" / "db" / "migrations"
        / "069_recommendation_outcomes.sql"
    )
    sql = migration.read_text(encoding="utf-8")
    # The CHECK block: evaluation_status ... CHECK (evaluation_status IN ( 'A', ... ))
    m = re.search(
        r"evaluation_status\s+TEXT\s+NOT NULL\s+CHECK\s*\(\s*evaluation_status\s+IN\s*\((?P<body>.*?)\)\s*\)",
        sql,
        re.DOTALL,
    )
    assert m is not None, "could not locate the evaluation_status CHECK in migration 069"
    literals = set(re.findall(r"'([A-Z_]+)'", m.group("body")))
    assert literals == set(VALID_STATUSES), (
        f"migration CHECK {literals} != Python VALID_STATUSES {set(VALID_STATUSES)}"
    )
    # And the evaluator only ever emits values inside this set (spot-check the
    # five branches produce members of VALID_STATUSES).
    assert set(VALID_STATUSES) == {
        "AVOIDED",
        "MATERIALIZED",
        "PARTIAL",
        "NOT_APPLICABLE",
        "INDETERMINATE",
    }


def test_every_branch_status_is_a_valid_status():
    """Belt: each of the five verdicts the evaluator can return is in
    VALID_STATUSES (so nothing it writes can violate the migration CHECK)."""
    snap = _snapshot()
    cases = [
        (_reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE), _observed(0), snap),  # AVOIDED
        (_reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE), _observed(40), snap),  # PARTIAL
        (_reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE), _observed(95), snap),  # MATERIALIZED
        (_reco(status="DRAFT", evidence=UNIT_COST_EVIDENCE), _observed(100), snap),  # NOT_APPLICABLE
        (_reco(status="APPROVED", evidence=UNIT_COST_EVIDENCE), _observed(0), None),  # INDETERMINATE
    ]
    seen = set()
    for reco, obs, sn in cases:
        v = evaluate_outcome(reco, obs, sn, AS_OF)
        assert v.evaluation_status in VALID_STATUSES
        seen.add(v.evaluation_status)
    assert seen == set(VALID_STATUSES), "the five cases must exercise all five verdicts"
