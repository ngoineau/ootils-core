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
import os
import sys
from collections import defaultdict

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import mrp_core as core  # noqa: E402

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
    d = build_pd(on_hand={"EXC": 1000, "OBS": 200, "OK": 100})
    gross = {"EXC": {1: 5.0, 10: 5.0}, "OK": {1: 200.0}}
    eo = core.excess_obsolete(d, gross, months=12.0)
    assert eo["EXC"]["class"] == "EXCESS"
    assert eo["EXC"]["excess_units"] == pytest.approx(990.0)
    assert eo["OBS"]["class"] == "OBSOLETE"
    assert eo["OBS"]["excess_units"] == pytest.approx(200.0)
    assert "OK" not in eo
