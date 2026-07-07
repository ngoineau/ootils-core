"""
Golden-master for the MRP engine (scripts/mrp_core.py).

The engine's correctness has so far been validated by eye on the pilote. Twice
this validation caught fundamental bugs (multi-location forecast loss, the
safety-vs-stockout threshold) that each moved headline numbers ~3x. This test
locks the math: a tiny hand-computed dataset whose expected outputs are derived
in the comments, so any future change that deviates from the documented arithmetic
fails CI instead of silently shifting a plan.

mrp_core's planning functions are pure (they operate on a PlanningData object, not
the DB), so this runs with no database — just the engine math.
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict

import pytest

# Canonical home of the MRP math (ADR-020 PAS 3): the packaged, DB-free core.
# scripts/mrp_core.py is now a re-export shim — see test_mrp_shim_compat.py.
from ootils_core.engine.mrp import core  # noqa: E402

HS = dt.date(2026, 1, 5)  # fixed Monday horizon start


def build_pd(**kw) -> core.PlanningData:
    """Construct a PlanningData from explicit maps, deriving involved/by_level/
    max_llc exactly as load_planning_data does."""
    d = core.PlanningData(horizon_start=kw.pop("horizon_start", HS), n_buckets=kw.pop("n_buckets", 12))
    for k, v in kw.items():
        setattr(d, k, v)
    involved = set()
    for m in (d.llc, d.is_make, d.on_hand, d.safety, d.co_b, d.fc_b, d.sched_b):
        involved.update(m.keys())
    for parent, comps in d.bom.items():
        involved.add(parent)
        for c, _, _ in comps:
            involved.add(c)
    d.involved = involved
    d.max_llc = max((d.llc.get(i, 0) for i in involved), default=0)
    by_level = defaultdict(list)
    for i in involved:
        by_level[d.llc.get(i, 0)].append(i)
    d.by_level = by_level
    return d


# ───────────────────────── lot sizing ─────────────────────────

def test_lot_size_floor_and_multiple():
    assert core.lot_size(100, 0, 0) == 100          # no constraint
    assert core.lot_size(100, 150, 0) == 150        # MOQ floor
    assert core.lot_size(100, 0, 40) == 120         # ceil(100/40)*40
    assert core.lot_size(100, 150, 40) == 160       # floor 150, then ceil(150/40)*40


def test_apply_lot_rule_variants():
    nb = 12
    assert core.apply_lot_rule("LOTFORLOT", 100, 0, 0, {}, 0, 4, 0, 0, 0, 0, nb) == 100
    assert core.apply_lot_rule("LOTFORLOT", 100, 0, 0, {}, 0, 4, 0, 0, 150, 0, nb) == 150
    assert core.apply_lot_rule("EOQ", 30, 0, 0, {}, 0, 4, 100, 0, 0, 0, nb) == 100        # max(eoq, shortfall)
    assert core.apply_lot_rule("FIXED_QTY", 100, 0, 0, {}, 0, 4, 0, 0, 30, 0, nb) == 120  # ceil(100/30)*30
    # MIN_MAX: qty = (ss + maxoq) - pa = (10 + 100) - 20 = 90
    assert core.apply_lot_rule("MIN_MAX", 0, 20, 10, {}, 0, 4, 0, 100, 0, 0, nb) == 90
    # POQ: shortfall 10 + window of netreq over (t+1 .. t+P-1) = 20 + 30 = 50  -> 60
    assert core.apply_lot_rule("POQ", 10, 0, 0, {1: 20, 2: 30}, 0, 3, 0, 0, 0, 0, nb) == 60


def test_apply_lot_rule_max_order_qty_cap():
    """max_order_qty caps a single order for every rule EXCEPT MIN_MAX (which uses
    maxoq as a target stock level, not a per-order ceiling)."""
    nb = 12
    # LOTFORLOT shortfall 5000 capped at max_order_qty 1000
    assert core.apply_lot_rule("LOTFORLOT", 5000, 0, 0, {}, 0, 4, 0, 1000, 0, 0, nb) == 1000
    # MIN_MAX NOT capped: (ss + maxoq) - pa = (0 + 1000) - (-100) = 1100
    assert core.apply_lot_rule("MIN_MAX", 0, -100, 0, {}, 0, 4, 0, 1000, 0, 0, nb) == 1100


# ───────────────────────── forecast proration ─────────────────────────

def test_spread_period_conserves_mass_inside_horizon():
    """A monthly lump of 400 over 4 weeks, fully inside the horizon, spreads to
    100/week across buckets 0-3 and conserves total mass."""
    out = defaultdict(float)
    core._spread_period(400.0, HS, HS + dt.timedelta(days=28),
                        HS, HS + dt.timedelta(days=540), 12, out)
    assert sum(out.values()) == pytest.approx(400.0)
    for b in (0, 1, 2, 3):
        assert out[b] == pytest.approx(100.0)


def test_spread_period_clips_pre_horizon_fraction():
    """A period straddling the horizon start drops the elapsed (pre-horizon)
    fraction — we never plan the past. 400 over 28 days, only the last 14 in
    horizon -> 200 allocated."""
    out = defaultdict(float)
    core._spread_period(400.0, HS - dt.timedelta(days=14), HS + dt.timedelta(days=14),
                        HS, HS + dt.timedelta(days=540), 12, out)
    assert sum(out.values()) == pytest.approx(200.0)


# ───────────────────────── forecast consumption ─────────────────────────

def test_consume_demand_max_only_and_demand_time_fence():
    d = build_pd(
        co_b={"X1": {2: 100}, "X2": {0: 10, 3: 10}},
        fc_b={"X1": {2: 60, 4: 80}, "X2": {0: 50, 3: 50}},
        frozen_d={"X2": 14},   # 2-week demand time fence
    )
    g = core.consume_demand(d)
    # X1: bucket 2 = max(100, 60) = 100 (never the sum); bucket 4 = forecast 80
    assert g["X1"] == {2: 100.0, 4: 80.0}
    # X2: bucket 0 < DTF(2 wk) -> orders only = 10; bucket 3 >= DTF -> max(10,50) = 50
    assert g["X2"] == {0: 10.0, 3: 50.0}


def test_consume_demand_window_zero_is_per_bucket_max_golden_invariant():
    """GOLDEN INVARIANT (#349): with no consumption window seeded (consume_window
    empty => 0 per item) the primitive MUST be byte-identical to per-bucket
    max(orders, forecast) — the pre-window semantics the golden dataset relies
    on. This locks that the window is a strict extension, inert at window=0."""
    d = build_pd(
        co_b={"X1": {2: 100}, "X2": {0: 10, 3: 10}},
        fc_b={"X1": {2: 60, 4: 80}, "X2": {0: 50, 3: 50}},
        frozen_d={"X2": 14},
        # consume_window intentionally left empty => window 0 for every item.
    )
    g = core.consume_demand(d)
    assert g["X1"] == {2: 100.0, 4: 80.0}
    assert g["X2"] == {0: 10.0, 3: 50.0}


def test_consume_demand_window_closes_early_buy_displaced_double_count():
    """The window fix (#349), locked in the golden file: a firm booking 100 in
    bucket 3 and its forecast 120 in the neighbouring bucket 4 net to 120 with a
    covering window, NOT 220. window=0 on the SAME data double-counts to 220 —
    proving the window is what closes the Early-Buy trap, not a golden change."""
    o = {3: 100.0}
    f = {4: 120.0}
    d_win = build_pd(co_b={"Y": o}, fc_b={"Y": f}, consume_window={"Y": 2})
    d_zero = build_pd(co_b={"Y": o}, fc_b={"Y": f})
    assert sum(core.consume_demand(d_win)["Y"].values()) == pytest.approx(120.0)
    assert sum(core.consume_demand(d_zero)["Y"].values()) == pytest.approx(220.0)


# ───────────────────────── time-phased cascade ─────────────────────────

def test_run_timephased_bom_cascade_leadtime_lotsize():
    """FG (make) demand 50 @wk4, BOM FG->2x COMP (buy, MOQ 150).
    FG: order 50 @need wk4, release wk4 - LT(1wk) = wk3 (WO). Explodes 50*2 = 100
        of COMP at the parent release bucket (wk3).
    COMP: need 100 @wk3, LFL 100 floored to MOQ 150; release wk3 - LT(2wk) = wk1 (PO).
    """
    d = build_pd(
        llc={"COMP": 1},
        is_make={"FG": True},
        make_lt={"FG": 7},
        buy_lt={"COMP": 14},
        moq={"COMP": 150},
        bom={"FG": [("COMP", 2.0, 0.0)]},
    )
    gross = {"FG": {4: 50.0}}
    r = core.run_timephased(d, gross)
    planned = set(r["planned"])
    assert ("FG", 50.0, 3, 4, "WO", False) in planned
    assert ("COMP", 150.0, 1, 3, "PO", False) in planned
    assert r["n_wo"] == 1 and r["n_po"] == 1
    assert r["past_due"] == 0


def test_run_timephased_past_due_release():
    """Demand @wk0 with a 2-week lead time -> release wk0-2 = -2 -> past due,
    release clamped to 0."""
    d = build_pd(is_make={"FG2": True}, make_lt={"FG2": 14})
    r = core.run_timephased(d, {"FG2": {0: 10.0}})
    assert ("FG2", 10.0, 0, 0, "WO", True) in set(r["planned"])
    assert r["past_due"] == 1


# ───────────────────────── shortage at safety (review fix #1) ─────────────────────────

def test_first_shortage_triggers_at_safety_not_stockout():
    """on_hand 30, safety 20, demand 15 @wk2 then 20 @wk4.
    Projected on-hand: wk2 -> 15, which is BELOW safety (20) though still > 0.
    So the shortage fires at wk2 with deficit = ss - pa = 20 - 15 = 5
    (NOT at a later stockout)."""
    d = build_pd(on_hand={"S1": 30}, safety={"S1": 20})
    gross = {"S1": {2: 15.0, 4: 20.0}}
    out = core.first_shortage(d, gross)
    assert out["S1"]["bucket"] == 2
    assert out["S1"]["deficit"] == pytest.approx(5.0)
    assert out["S1"]["balance"] == pytest.approx(15.0)


def test_first_shortage_zero_safety_is_stockout():
    """ss=0 reduces to the first negative bucket."""
    d = build_pd(on_hand={"S2": 10}, safety={})
    out = core.first_shortage(d, {"S2": {1: 4.0, 2: 9.0}})
    # wk1 -> 6 (>=0), wk2 -> -3 -> shortage, deficit = 0 - (-3) = 3
    assert out["S2"]["bucket"] == 2
    assert out["S2"]["deficit"] == pytest.approx(3.0)


# ───────────────────────── excess & obsolete ─────────────────────────

def test_excess_obsolete_classification():
    """EXC: on_hand 1000, annual demand 10 -> coverage 1200 mo -> EXCESS,
            excess = 1000 - 12*(10/12) = 990.
       OBS: on_hand 200, no demand -> OBSOLETE, excess = 200.
       OK : on_hand 100, annual 200 -> coverage 6 mo -> not E&O (excluded)."""
    # n_buckets=53 ⇒ a full-year window (weeks_win=52, annualization factor 1.0),
    # so the summed demand IS the annual figure.
    d = build_pd(n_buckets=53, on_hand={"EXC": 1000, "OBS": 200, "OK": 100})
    gross = {"EXC": {1: 5.0, 10: 5.0}, "OK": {1: 200.0}}
    eo = core.excess_obsolete(d, gross, months=12.0)
    assert eo["EXC"]["class"] == "EXCESS"
    assert eo["EXC"]["excess_units"] == pytest.approx(990.0)
    assert eo["OBS"]["class"] == "OBSOLETE"
    assert eo["OBS"]["excess_units"] == pytest.approx(200.0)
    assert "OK" not in eo


def test_excess_obsolete_annualizes_to_short_horizon():
    """With a horizon shorter than a year, demand in the window is scaled up to an
    annual rate (so coverage isn't over-stated and healthy stock isn't mis-flagged).
    n_buckets=26 ⇒ weeks_win=26, factor 52/26=2. Demand 10 → annual 20, monthly
    1.667, coverage = 100/1.667 = 60 mo > 12 → EXCESS; excess = 100 - 12*1.667 = 80."""
    d = build_pd(n_buckets=26, on_hand={"E": 100})
    eo = core.excess_obsolete(d, {"E": {0: 10.0}}, months=12.0)
    assert eo["E"]["class"] == "EXCESS"
    assert eo["E"]["excess_units"] == pytest.approx(80.0)


# ═════════════════════════════════════════════════════════════════════════
# #423 PR1 — CANONICAL SEMANTICS LOCK
# These five goldens freeze the behaviours of the math core (engine/mrp/core.py)
# that PR2 will align the APICS write-path engine onto. Each locks a specific
# semantic the ADR-020 MRP consolidation exposed; the expected value is derived
# by hand in the comment, never pasted from a run. Frozen-fence policy = pilot
# option (a): the core keeps its demand-time-fence (forecast ignored in the
# zone, firm orders only) AND still EMITS the planned orders that fall in the
# zone — never a silent suppression.
# ═════════════════════════════════════════════════════════════════════════


def test_golden_independent_demand_of_llc_component_adds_to_dependent():
    """GOLDEN 1 — LLC>0 component with its OWN independent demand.

    Locks the spare-parts case behind the ADR-020 −11 % material écart (the 2/3
    gap): a component that is BOTH exploded from a parent BOM *and* carries its
    own forecast/orders must have the two demands ADD, not replace (core.py:375
    for netreq, core.py:380 for the projected-on-hand walk both do `g + dep`).
    Dropping the independent leg understates the component's true burn.

    Dataset (all DB-free):
      FG   : make, forecast 10 @wk4, make_lt 7d (1 wk).
      BOM  : FG -> 2x COMP.
      COMP : buy (LLC 1), buy_lt 14d (2 wk), on_hand 4, OWN forecast 5 @wk3.

    Derivation, level by level:
      Level 0 (FG): gross {4: 10}. pa walks 0..3 = 0, then wk4: 0-10 = -10 < ss0
        -> shortfall 10, LFL qty 10. release 4-1 = wk3 (WO). Explode to COMP at
        the PARENT RELEASE bucket wk3: dependent[COMP][3] = 10 * 2 = 20.
      Level 1 (COMP): independent gross {3: 5}, dependent {3: 20} — BOTH land in
        wk3. pa starts at on_hand 4; walks 0..2 = 4, then wk3:
        pa = 4 - g(5) - dep(20) = 4 - 25 = -21 < ss0.
        shortfall = 0 - (-21) = 21  == 20 (dep) + 5 (indep) - 4 (stock).
        LFL qty 21, release 3-2 = wk1 (PO).
    So COMP's single net requirement is 21 — the dependent 20 and the
    independent 5 summed, minus on-hand 4. If the independent leg were dropped
    it would be 16; if it replaced the dependent it would be 1. Only 21 is right.
    """
    d = build_pd(
        llc={"COMP": 1},
        is_make={"FG": True},
        make_lt={"FG": 7},
        buy_lt={"COMP": 14},
        on_hand={"COMP": 4},
        bom={"FG": [("COMP", 2.0, 0.0)]},
        fc_b={"FG": {4: 10.0}, "COMP": {3: 5.0}},
    )
    gross = core.consume_demand(d)
    r = core.run_timephased(d, gross)
    planned = set(r["planned"])
    assert ("FG", 10.0, 3, 4, "WO", False) in planned
    # COMP: 20 (dependent) + 5 (own forecast) - 4 (on hand) = 21, ONE order.
    assert ("COMP", 21.0, 1, 3, "PO", False) in planned
    assert r["dependent"]["COMP"] == {3: 20.0}
    assert r["n_wo"] == 1 and r["n_po"] == 1
    assert r["past_due"] == 0


def test_golden_lotforlot_with_order_multiple():
    """GOLDEN 2 — LOT-FOR-LOT + order_multiple.

    Locks the ADR-020 (and ADR-028 DRP-consistent) rule that the order multiple
    is a FINAL guard applied to EVERY lot rule, LOTFORLOT included (core.py:34-40
    lot_size, called at core.py:91). LFL sizes the order to the shortfall, then
    the multiple rounds UP: no order may violate the pack/multiple constraint.

    Derivation: rule LOTFORLOT, shortfall 20, moq 0, mult 12.
      LFL branch (core.py:89-90): qty = shortfall = 20.
      lot_size(20, moq=0, mult=12): no MOQ floor; ceil(20/12) * 12 = 2 * 12 = 24.
      max_order_qty 0 -> no cap.
    => 24 (never 20; the raw shortfall is rounded up to the next multiple).
    """
    nb = 12
    assert core.apply_lot_rule(
        "LOTFORLOT", 20, 0, 0, {}, 0, 4, 0, 0, 0, 12, nb
    ) == 24


def test_golden_late_receipt_clamped_to_bucket_zero():
    """GOLDEN 3 — a receipt dated BEFORE the horizon start clamps to bucket 0.

    Locks core.py:196-197 `bucket() = max(0, (d - horizon_start).days // 7)`:
    a past-due / already-arrived receipt is bucketed to "today" (bucket 0), never
    dropped to a negative bucket and never silently ignored. This is what lets
    stock that landed just before the plan date still count as available supply
    (the ADR-020 lesson: never lose a receipt to bucketisation).

    Derivation (horizon_start = HS, a fixed Monday):
      due 10 days BEFORE HS: (HS-10 - HS).days = -10; -10 // 7 = -2; max(0,-2)=0.
      due exactly on HS     : 0 // 7 = 0.
      due 8 days AFTER HS    : 8 // 7 = 1  (control: a real future bucket).
    """
    d = build_pd()
    assert d.bucket(HS - dt.timedelta(days=10)) == 0   # late receipt -> today
    assert d.bucket(HS) == 0                            # on the horizon start
    assert d.bucket(HS + dt.timedelta(days=8)) == 1     # genuine future bucket


def test_golden_min_max_not_capped_by_max_order_qty():
    """GOLDEN 4 — MIN_MAX refills to (ss + max) and is NOT capped by max_oq.

    Locks core.py:85-86 (MIN_MAX target = (ss + maxoq) - pa) together with the
    core.py:95 cap carve-out (`rule != "MIN_MAX"`): for a MIN_MAX item the single
    `max_oq` field is the max STOCK LEVEL (a target to refill to), NOT a
    per-order ceiling — so the computed order legitimately EXCEEDS max_oq when the
    projected balance is below it, whereas the SAME max_oq value caps any other
    rule at that number. Re-capping MIN_MAX with its own target field would
    forbid it from ever reaching max, defeating the rule.

    Derivation, cascade (item MM, rule MIN_MAX):
      ss 10, max_oq 100 (the max stock level), buy_lt 7d (1 wk), on_hand 20,
      forecast 15 @wk2.
      pa starts 20; walks 0..1 = 20; wk2: pa = 20 - 15 = 5, and 5 < ss 10
        -> shortfall = 10 - 5 = 5.
      MIN_MAX qty = (ss + maxoq) - pa = (10 + 100) - 5 = 105.
      lot_size(105, moq 0, mult 0) = 105; cap SKIPPED because rule == MIN_MAX.
      => order 105  (ABOVE the max_oq of 100). release 2-1 = wk1 (PO).
    Contrast at the unit level: fed to LOTFORLOT the same max_oq 100 caps the
    order to 100; MIN_MAX with the same field yields 105. Same field, opposite
    role — that is the semantic being frozen.
    """
    d = build_pd(
        on_hand={"MM": 20},
        safety={"MM": 10},
        max_oq={"MM": 100},
        buy_lt={"MM": 7},
        lot_rule={"MM": "MIN_MAX"},
        fc_b={"MM": {2: 15.0}},
    )
    gross = core.consume_demand(d)
    r = core.run_timephased(d, gross)
    # 105 > max_oq 100: MIN_MAX target is a stock level, not a per-order cap.
    assert ("MM", 105.0, 1, 2, "PO", False) in set(r["planned"])
    assert r["n_po"] == 1 and r["past_due"] == 0
    # Same max_oq=100 as a per-order ceiling caps LOTFORLOT at 100 (the contrast).
    assert core.apply_lot_rule(
        "LOTFORLOT", 105, 5, 10, {}, 2, 4, 0, 100, 0, 0, 12
    ) == 100.0
    # while MIN_MAX with the same field refills to (ss+max)-pa = 105, uncapped.
    assert core.apply_lot_rule(
        "MIN_MAX", 5, 5, 10, {}, 2, 4, 0, 100, 0, 0, 12
    ) == 105


def test_golden_frozen_fence_option_a_ignores_forecast_but_emits_order():
    """GOLDEN 5 — frozen (demand-time-)fence, pilot option (a).

    Locks the two halves of option (a) at once:
      (i)  INSIDE the fence the forecast is IGNORED and only firm customer orders
           count as demand (core.py:318 dtf_weeks = ceil(frozen_d/7);
           core.py:331 `if t < dtf_weeks: v = o`). The forecast is not trusted so
           close to now.
      (ii) The net requirement that remains in the fenced zone STILL produces a
           planned order — run_timephased makes no distinction for fenced buckets
           (core.py:379-394): a real order is emitted, NEVER silently suppressed.

    Derivation (item FGF, make):
      frozen_d 7d -> dtf_weeks = ceil(7/7) = 1  => bucket 0 is fenced.
      forecast 50 @wk0 (inside fence), firm order 30 @wk0 (inside fence).
      consume_demand wk0: t(0) < dtf(1) -> v = orders only = 30 (forecast 50
        DROPPED). gross = {FGF: {0: 30}}.
      run_timephased: pa starts 0; wk0: pa = 0 - 30 = -30 < ss0
        -> shortfall 30, LFL qty 30. release 0 - make_lt(1wk) = -1 -> past due,
        clamped to wk0 (WO, past_due).
      => exactly ONE order of 30 in the fenced zone; forecast 50 ignored; no
         order suppressed.
    """
    d = build_pd(
        is_make={"FGF": True},
        make_lt={"FGF": 7},
        frozen_d={"FGF": 7},          # 1-week demand time fence -> bucket 0 fenced
        co_b={"FGF": {0: 30.0}},      # firm order inside the fence
        fc_b={"FGF": {0: 50.0}},      # forecast inside the fence -> IGNORED
    )
    gross = core.consume_demand(d)
    # (i) forecast 50 dropped inside the fence; only the firm order 30 survives.
    assert gross["FGF"] == {0: 30.0}
    r = core.run_timephased(d, gross)
    # (ii) the fenced net requirement STILL emits a planned order (never dropped).
    assert ("FGF", 30.0, 0, 0, "WO", True) in set(r["planned"])
    assert r["n_wo"] == 1
    assert r["past_due"] == 1
