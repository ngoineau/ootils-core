"""
Unit tests for the canonical receipt-vs-need reschedule comparison (#346 PR-A).

reschedule_signals lives in the DB-free MRP math core (engine/mrp/core.py), so
these run with no database — PlanningData is built in memory exactly like the
core golden-master (test_mrp_core_golden.py).

The most important test here is STABILITY: regenerating a plan on unchanged data
must emit ZERO signals. A receipt already on its need date (or within dampening)
produces no message. An MRP that spits reschedule messages on a stable plan is
unusable.

Need-date convention (mirrors the projection): the need date of a receipt is the
START of the weekly bucket where the CENTRE OF GRAVITY of that receipt's own
consumption sits — the bucket at which its cumulative allocated quantity first
reaches 50% of the receipt qty (its median unit), NOT the first requirement it
marginally touches (see core._need_bucket_for_receipts). Bucket t start =
HS + t weeks.
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict

from ootils_core.engine.mrp import core  # noqa: E402

HS = dt.date(2026, 1, 5)  # fixed Monday horizon start (same as the golden-master)


def _ro(node_id, item, date, qty, is_firm=False, node_type="PurchaseOrderSupply"):
    return core.ReceiptOrder(
        node_id=node_id, item_id=item, receipt_date=date, qty=qty,
        is_firm=is_firm, node_type=node_type)


def build_pd(**kw) -> core.PlanningData:
    d = core.PlanningData(
        horizon_start=kw.pop("horizon_start", HS),
        n_buckets=kw.pop("n_buckets", 12))
    for k, v in kw.items():
        setattr(d, k, v)
    involved = set()
    for m in (d.llc, d.is_make, d.on_hand, d.safety, d.co_b, d.fc_b, d.sched_b):
        involved.update(m.keys())
    d.involved = involved
    d.max_llc = max((d.llc.get(i, 0) for i in involved), default=0)
    by_level: defaultdict[int, list] = defaultdict(list)
    for i in involved:
        by_level[d.llc.get(i, 0)].append(i)
    d.by_level = by_level
    return d


def _bucket_date(t: int) -> dt.date:
    return HS + dt.timedelta(weeks=t)


# ───────────────────────── (a) stability — THE central invariant ─────────────

def test_stability_unchanged_plan_emits_zero_signals():
    """Demand 100 @wk2, one receipt of 100 arriving IN wk2. The receipt is on its
    need bucket => not a single message. This is the whole point of #346:
    regenerating a stable plan must be silent."""
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(2), 100.0)]},
    )
    gross = {"X": {2: 100.0}}
    assert core.reschedule_signals(d, gross) == []


def test_stability_intra_bucket_weekday_still_silent():
    """A receipt landing mid-need-bucket (Thursday, not the bucket-start Monday)
    is still on time — need is resolved at bucket granularity, so no message."""
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(2) + dt.timedelta(days=3), 100.0)]},
    )
    gross = {"X": {2: 100.0}}
    assert core.reschedule_signals(d, gross) == []


# ───────────────────────── (b) receipt too late → RESCHEDULE_IN ──────────────

def test_receipt_too_late_emits_reschedule_in():
    """Demand 100 @wk2 (need = wk2 start), receipt arrives wk6 — far too late.
    Pull it in: RESCHEDULE_IN, proposed = wk2 start."""
    receipt_date = _bucket_date(6)
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", receipt_date, 100.0)]},
    )
    gross = {"X": {2: 100.0}}
    sigs = core.reschedule_signals(d, gross)
    assert len(sigs) == 1
    s = sigs[0]
    assert s.action == core.RESCHEDULE_IN
    assert s.node_id == "po1"
    assert s.current_receipt_date == receipt_date
    assert s.proposed_date == _bucket_date(2)
    assert s.qty == 100.0


# ───────────────────────── (c) receipt too early → RESCHEDULE_OUT ────────────

def test_receipt_too_early_emits_reschedule_out():
    """Demand 100 @wk6 (need = wk6 start), receipt arrives wk0 — far too early.
    Push it out: RESCHEDULE_OUT, proposed = wk6 start."""
    receipt_date = _bucket_date(0)
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", receipt_date, 100.0)]},
    )
    gross = {"X": {6: 100.0}}
    sigs = core.reschedule_signals(d, gross)
    assert len(sigs) == 1
    s = sigs[0]
    assert s.action == core.RESCHEDULE_OUT
    assert s.current_receipt_date == receipt_date
    assert s.proposed_date == _bucket_date(6)


# ───────────────────────── (d) demand disappeared → CANCEL ───────────────────

def test_no_matching_need_emits_cancel():
    """A receipt of 100 with NO demand anywhere on the horizon is pure surplus:
    CANCEL, proposed_date None."""
    receipt_date = _bucket_date(2)
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", receipt_date, 100.0)]},
    )
    gross: dict = {}  # demand vanished
    sigs = core.reschedule_signals(d, gross)
    assert len(sigs) == 1
    s = sigs[0]
    assert s.action == core.CANCEL
    assert s.proposed_date is None
    assert s.current_receipt_date == receipt_date
    assert s.qty == 100.0


def test_receipt_covered_by_on_hand_is_cancel():
    """On-hand already covers all demand — the incoming receipt has no need."""
    d = build_pd(
        on_hand={"X": 200},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(2), 100.0)]},
    )
    gross = {"X": {2: 100.0}}  # fully covered by on-hand 200
    sigs = core.reschedule_signals(d, gross)
    assert [s.action for s in sigs] == [core.CANCEL]


# ───────────────────────── (e) date dampening ───────────────────────────────

def test_date_delta_below_min_days_is_dampened():
    """Receipt one bucket (7 days) off its need bucket, but reschedule_min_days is
    30 → the 7-day nudge is below threshold: NO message (dampening)."""
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(3), 100.0)]},
        resched_min_days={"X": 30},  # very tolerant threshold
    )
    gross = {"X": {2: 100.0}}  # need = wk2, receipt wk3 → 7-day delta < 30
    assert core.reschedule_signals(d, gross) == []


def test_date_delta_at_min_days_boundary_emits():
    """Threshold is inclusive: |delta| >= min_days emits. 7-day delta, min_days 7
    → RESCHEDULE_IN fires."""
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(3), 100.0)]},
        resched_min_days={"X": 7},
    )
    gross = {"X": {2: 100.0}}
    sigs = core.reschedule_signals(d, gross)
    assert [s.action for s in sigs] == [core.RESCHEDULE_IN]


# ───────────────────────── (f) qty tolerance is a V1 no-op for CANCEL ────────

def test_qty_tolerance_does_not_suppress_cancel_in_v1():
    """V1 semantics (Défaut 3 fix): reschedule_qty_tolerance_pct is NOT applied
    to CANCEL. A receipt reaching the CANCEL branch is 100% surplus by
    construction; a graded tolerance is meaningless without splitting the
    receipt (reserved for V2). So even a 100% tolerance does NOT suppress the
    CANCEL of a fully-surplus receipt sitting inside the horizon."""
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(2), 100.0)]},
        resched_qty_tol_pct={"X": 100.0},  # ignored in V1
    )
    assert [s.action for s in core.reschedule_signals(d, {})] == [core.CANCEL]


def test_full_surplus_cancels():
    """A 100%-surplus receipt well inside the horizon → CANCEL fires."""
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(2), 100.0)]},
    )
    assert [s.action for s in core.reschedule_signals(d, {})] == [core.CANCEL]


# ───────────────────────── determinism & multi-receipt ──────────────────────

def test_multiple_receipts_fifo_need_allocation():
    """Two receipts, demand pulls the earlier one to its need bucket and leaves
    the later one surplus. FIFO by receipt date decides which is needed."""
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [
            _ro("po_early", "X", _bucket_date(2), 100.0),
            _ro("po_late", "X", _bucket_date(2), 100.0),
        ]},
    )
    gross = {"X": {2: 100.0}}  # only 100 needed at wk2; one receipt is surplus
    sigs = core.reschedule_signals(d, gross)
    # The earlier receipt covers the need (on its bucket → silent); the later is
    # surplus → CANCEL. Result sorted by (item, node_id).
    assert len(sigs) == 1
    assert sigs[0].action == core.CANCEL
    assert sigs[0].node_id == "po_late"


def test_deterministic_output_is_sorted():
    """Signals come back sorted by (item_id, node_id) regardless of input order."""
    d = build_pd(
        on_hand={"A": 0, "B": 0},
        sched_orders={
            "B": [_ro("b_po", "B", _bucket_date(0), 50.0)],
            "A": [_ro("a_po", "A", _bucket_date(0), 50.0)],
        },
    )
    # Both receipts are pure surplus (no demand) → two CANCELs, A before B.
    sigs = core.reschedule_signals(d, {})
    assert [(s.item_id, s.node_id) for s in sigs] == [("A", "a_po"), ("B", "b_po")]


def test_safety_stock_pulls_need_earlier():
    """Safety stock is a floor: with ss=20, demand 100 @wk4 makes the balance
    breach safety at wk4 (need = wk4). A receipt on wk4 is on time."""
    d = build_pd(
        on_hand={"X": 20},
        safety={"X": 20},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(4), 100.0)]},
    )
    gross = {"X": {4: 100.0}}
    assert core.reschedule_signals(d, gross) == []


# ───────────────────────── (g) Défaut 2 — median / centre-of-gravity need ────

def test_over_pull_median_no_reschedule_in_to_first_touch():
    """A receipt of 200 covering 10 @wk1 + 190 @wk10. The OLD first-touch
    convention set need=wk1 and proposed pulling all 200 to wk1 (56 days early
    for 190 units — the over-pull bug). The MEDIAN convention (Défaut 2 fix)
    sets need=wk10 (its median unit is consumed at wk10). A receipt at wk9 is
    then only ~1 week early → at most a small 1-week nudge, NEVER a RESCHEDULE_IN
    toward wk1."""
    d = build_pd(
        n_buckets=14,
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(9), 200.0)]},
    )
    gross = {"X": {1: 10.0, 10: 190.0}}
    sigs = core.reschedule_signals(d, gross)
    # The over-pull is gone: no pull-IN toward the marginal first touch (wk1).
    assert [s for s in sigs if s.action == core.RESCHEDULE_IN] == []
    # The receipt sits 1 week early vs its centre of gravity (wk10) → a modest
    # push-out to wk10, not a 56-day pull-in.
    assert len(sigs) == 1
    assert sigs[0].action == core.RESCHEDULE_OUT
    assert sigs[0].proposed_date == _bucket_date(10)


def test_median_receipt_on_gravity_center_is_silent():
    """Same 10 @wk1 / 190 @wk10 split, receipt landing exactly on its median
    bucket wk10 → on time, no message. Confirms the median convention makes the
    stability invariant hold for a lumpy consumption profile."""
    d = build_pd(
        n_buckets=14,
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(10), 200.0)]},
    )
    gross = {"X": {1: 10.0, 10: 190.0}}
    assert core.reschedule_signals(d, gross) == []


def test_median_need_bucket_is_gravity_center():
    """Same 10 @wk1 / 190 @wk10 split, receipt far too late at wk13. The proposed
    date must be the median bucket wk10 (centre of gravity), NOT the first-touch
    wk1."""
    d = build_pd(
        n_buckets=16,
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(13), 200.0)]},
    )
    gross = {"X": {1: 10.0, 10: 190.0}}
    sigs = core.reschedule_signals(d, gross)
    assert len(sigs) == 1
    assert sigs[0].action == core.RESCHEDULE_IN
    assert sigs[0].proposed_date == _bucket_date(10)


# ───────────────────────── (h) Défaut 4 — horizon-edge CANCEL guard ──────────

def test_no_cancel_on_horizon_edge_receipt():
    """A fully-surplus receipt landing in the LAST bucket of the loaded horizon
    must NOT be cancelled: the demand justifying it may sit just beyond the
    window (visibility, not surplus)."""
    d = build_pd(
        n_buckets=12,
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(11), 100.0)]},  # last bucket
    )
    gross: dict = {}  # no visible need inside the window
    assert core.reschedule_signals(d, gross) == []


def test_demand_just_beyond_horizon_does_not_cancel_edge_receipt():
    """Demand exists but at a bucket >= n_buckets (out of the loaded window).
    _need_bucket_for_receipts sees no requirement, but the receipt sits on the
    horizon edge → the guard suppresses the phantom CANCEL."""
    d = build_pd(
        n_buckets=12,
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(11), 100.0)]},
    )
    gross = {"X": {13: 100.0}}  # bucket 13 >= n_buckets=12 → invisible to the walk
    assert core.reschedule_signals(d, gross) == []


def test_surplus_well_inside_horizon_still_cancels():
    """The edge guard is narrow: a fully-surplus receipt comfortably inside the
    horizon (not the last bucket) still CANCELs."""
    d = build_pd(
        n_buckets=12,
        on_hand={"X": 0},
        sched_orders={"X": [_ro("po1", "X", _bucket_date(2), 100.0)]},
    )
    assert [s.action for s in core.reschedule_signals(d, {})] == [core.CANCEL]


# ───────────────────────── (i) Défaut 1 — firm PlannedSupply is re-datable ───

def test_firm_planned_supply_is_reschedulable():
    """A firm PlannedSupply (FPO) mis-dated far from its need emits a reschedule
    just like a committed PO — that is the headline case of #346. Here need=wk2,
    the FPO lands at wk6 → RESCHEDULE_IN."""
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [
            _ro("fpo1", "X", _bucket_date(6), 100.0,
                is_firm=True, node_type="PlannedSupply"),
        ]},
    )
    gross = {"X": {2: 100.0}}
    sigs = core.reschedule_signals(d, gross)
    assert len(sigs) == 1
    assert sigs[0].action == core.RESCHEDULE_IN
    assert sigs[0].node_type == "PlannedSupply"
    assert sigs[0].is_firm is True


# ───────────────────────── (j) bonus — node_type + is_firm carried through ───

def test_signal_carries_node_type_and_is_firm():
    """RescheduleSignal must carry node_type/is_firm verbatim from the source
    ReceiptOrder (PR-B attribution needs them without a re-fetch)."""
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [
            _ro("wo1", "X", _bucket_date(6), 100.0,
                is_firm=True, node_type="WorkOrderSupply"),
        ]},
    )
    gross = {"X": {2: 100.0}}
    s = core.reschedule_signals(d, gross)[0]
    assert s.node_type == "WorkOrderSupply"
    assert s.is_firm is True


def test_cancel_signal_carries_attribution():
    """A CANCEL signal also carries node_type/is_firm from its source order."""
    d = build_pd(
        on_hand={"X": 0},
        sched_orders={"X": [
            _ro("fpo1", "X", _bucket_date(2), 100.0,
                is_firm=True, node_type="PlannedSupply"),
        ]},
    )
    s = core.reschedule_signals(d, {})[0]
    assert s.action == core.CANCEL
    assert s.node_type == "PlannedSupply"
    assert s.is_firm is True
