"""
Unit coverage for the forecast-consumption WINDOW (#349) in the single
consumption primitive `ootils_core.engine.mrp.core.consume_demand`.

The window is the fix for the Early-Buy double-count: a firm booking dated by
its delivery due-date and its forecast land in NEIGHBOURING weekly buckets
(bucketisation noise — monthly forecast prorated to weeks vs bookings on exact
dates). Pure per-bucket max(orders, forecast) cannot net two quantities that
sit one bucket apart, so it double-counts. A positive per-item window lets a
bucket's orders consume forecast in neighbouring buckets (APICS standard:
backward first, then forward), netting the pair once.

These tests are PURE (PlanningData built in memory, no DB). The invariant they
lock is CHIFFRE: net gross demand must never be the raw sum of orders and
forecast — it is bounded by the per-bucket max sum, and the window closes the
cross-bucket gap that a bare per-bucket max leaves open.
"""
from __future__ import annotations

import datetime as dt

import pytest

from ootils_core.engine.mrp import core

HS = dt.date(2026, 1, 5)  # fixed Monday horizon start
ITEM = "W1"


def build_pd(**kw) -> core.PlanningData:
    d = core.PlanningData(
        horizon_start=kw.pop("horizon_start", HS),
        n_buckets=kw.pop("n_buckets", 12),
    )
    for k, v in kw.items():
        setattr(d, k, v)
    return d


def _max_sum(orders: dict, forecast: dict) -> float:
    """sum over buckets of max(orders[t], forecast[t]) — the per-bucket-max
    ceiling the windowed result must never exceed."""
    total = 0.0
    for t in set(orders) | set(forecast):
        total += max(orders.get(t, 0.0), forecast.get(t, 0.0))
    return total


# ───────────────────── Early Buy, same bucket ─────────────────────


def test_early_buy_same_bucket_nets_to_max_with_window():
    """Booking 100 and forecast 120 in the SAME bucket 4, window covering:
    net = max(100, 120) = 120, never 220. Per-bucket max already handles this,
    but the window must NOT break it."""
    o = {4: 100.0}
    f = {4: 120.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: 2})
    g = core.consume_demand(d)
    assert sum(g[ITEM].values()) == pytest.approx(120.0)
    assert g[ITEM][4] == pytest.approx(120.0)


def test_early_buy_same_bucket_window_zero_still_max():
    """The SAME same-bucket case with window=0: still 120 (per-bucket max)."""
    o = {4: 100.0}
    f = {4: 120.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: 0})
    g = core.consume_demand(d)
    assert sum(g[ITEM].values()) == pytest.approx(120.0)


# ───────────────────── The real trap: displaced by one bucket ─────────────────────


def test_early_buy_displaced_bucket_window_nets_once():
    """THE Early-Buy trap: booking 100 in bucket 3, forecast 120 in bucket 4
    (one bucket apart from bucketisation noise). With window_buckets=2 the
    booking consumes the neighbouring forecast, so the total nets to the
    forecast 120 — NEVER 220."""
    o = {3: 100.0}
    f = {4: 120.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: 2})
    g = core.consume_demand(d)
    assert sum(g[ITEM].values()) == pytest.approx(120.0)


def test_early_buy_displaced_bucket_window_zero_double_counts():
    """Proof the window IS the fix: the SAME displaced case with window=0
    double-counts to 220 (booking bucket 3 + forecast bucket 4, no
    cross-bucket netting possible with bare per-bucket max)."""
    o = {3: 100.0}
    f = {4: 120.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: 0})
    g = core.consume_demand(d)
    assert sum(g[ITEM].values()) == pytest.approx(220.0)


def test_displaced_backward_before_forward():
    """APICS backward-before-forward ordering: a booking in bucket 4 with
    forecast in both bucket 3 (backward) and bucket 5 (forward), window 1.
    The booking must exhaust the BACKWARD forecast first."""
    o = {4: 50.0}
    f = {3: 50.0, 5: 50.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: 1})
    trace: list = []
    g = core.consume_demand(d, trace=trace)
    # Backward bucket 3 forecast fully consumed by the booking; forward bucket 5
    # forecast is untouched and stands. Net: wk4 = 50 (concrete booking),
    # wk5 = 50 (untouched forecast), wk3 = 0. Total = 100 (the wk3/wk4 pair
    # netted once; wk5 forecast is genuinely separate demand).
    assert sum(g[ITEM].values()) == pytest.approx(100.0)
    assert g[ITEM].get(4) == pytest.approx(50.0)
    assert g[ITEM].get(5) == pytest.approx(50.0)
    assert 3 not in g[ITEM]  # backward forecast fully consumed
    # trace records the backward move first (offset -1 before +1).
    assert (ITEM, 4, 3, 50.0) in trace


def test_window_too_small_does_not_reach():
    """Window must be honoured as a HARD limit: booking bucket 2, forecast
    bucket 5 (3 apart), window=2 cannot reach it => no netting, both stand."""
    o = {2: 100.0}
    f = {5: 120.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: 2})
    g = core.consume_demand(d)
    assert sum(g[ITEM].values()) == pytest.approx(220.0)


# ───────────────────── Conservation of mass (the invariant) ─────────────────────


@pytest.mark.parametrize("window", [0, 1, 2, 5])
def test_mass_never_exceeds_per_bucket_max_sum(window):
    """Central invariant: whatever the window, total net demand is bounded by
    sum(max(orders[t], forecast[t])) — NEVER the raw sum. A larger window can
    only ever net MORE (reduce the total), never inflate it."""
    o = {1: 40.0, 3: 100.0, 6: 20.0}
    f = {2: 60.0, 3: 120.0, 4: 30.0, 7: 200.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: window})
    g = core.consume_demand(d)
    ceiling = _max_sum(o, f)
    assert sum(g[ITEM].values()) <= ceiling + 1e-9


def test_larger_window_never_increases_total():
    """A monotonicity check: growing the window can only lower or hold the
    total net demand (more cross-consumption), never raise it."""
    o = {2: 100.0, 5: 100.0}
    f = {3: 120.0, 6: 120.0}
    totals = []
    for w in (0, 1, 2, 3):
        d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: w})
        totals.append(sum(core.consume_demand(d)[ITEM].values()))
    assert totals == sorted(totals, reverse=True)


# ───────────────────── window=0 == golden per-bucket max ─────────────────────


def test_window_zero_equals_per_bucket_max():
    """window=0 (default) reduces EXACTLY to per-bucket max(orders, forecast)
    for a range of overlapping/non-overlapping buckets — the golden semantics."""
    o = {0: 10.0, 2: 100.0, 3: 5.0}
    f = {2: 60.0, 3: 50.0, 4: 80.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f})  # no consume_window => 0
    g = core.consume_demand(d)
    assert g[ITEM] == {0: 10.0, 2: 100.0, 3: 50.0, 4: 80.0}


def test_missing_consume_window_defaults_to_zero():
    """An item absent from d.consume_window behaves as window=0 (per-bucket
    max), so existing callers that never populate the dict are unaffected."""
    o = {3: 100.0}
    f = {4: 120.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f})
    g = core.consume_demand(d)
    assert sum(g[ITEM].values()) == pytest.approx(220.0)  # no netting w/o window


# ───────────────────── strategies that ignore the window ─────────────────────


def test_forecast_only_ignores_window():
    """forecast_only => net = forecast per bucket, window has no effect."""
    o = {3: 100.0}
    f = {3: 60.0, 4: 120.0}
    d = build_pd(
        co_b={ITEM: o}, fc_b={ITEM: f},
        consume_window={ITEM: 3}, strat={ITEM: "forecast_only"},
    )
    g = core.consume_demand(d)
    assert g[ITEM] == {3: 60.0, 4: 120.0}


def test_orders_only_ignores_window():
    """orders_only => net = orders per bucket, window has no effect."""
    o = {3: 100.0, 4: 40.0}
    f = {3: 60.0, 4: 120.0}
    d = build_pd(
        co_b={ITEM: o}, fc_b={ITEM: f},
        consume_window={ITEM: 3}, strat={ITEM: "orders_only"},
    )
    g = core.consume_demand(d)
    assert g[ITEM] == {3: 100.0, 4: 40.0}


# ───────────────────── demand time fence interaction ─────────────────────


def test_window_respects_demand_time_fence():
    """Inside the demand time fence (frozen_d) a bucket carries ORDERS ONLY and
    is excluded from window mechanics: its forecast is not offered for
    consumption and its orders don't reach out. frozen_d=14 (2 wk fence).
    Booking 30 @wk0 (fenced) => orders only, 30. Booking 100 @wk3 with forecast
    120 @wk4 (both beyond fence), window 2 => nets to 120."""
    o = {0: 30.0, 3: 100.0}
    f = {0: 200.0, 4: 120.0}
    d = build_pd(
        co_b={ITEM: o}, fc_b={ITEM: f},
        consume_window={ITEM: 2}, frozen_d={ITEM: 14},
    )
    g = core.consume_demand(d)
    assert g[ITEM][0] == pytest.approx(30.0)  # fenced: orders only, forecast ignored
    assert sum(v for t, v in g[ITEM].items() if t >= 2) == pytest.approx(120.0)


# ───────────────────── partial consumption ─────────────────────


def test_partial_cross_consumption_leaves_forecast_remainder():
    """Booking 40 @wk3 consumes 40 of the 120 forecast @wk4; the concrete
    booking (40) stays at wk3, the remaining 80 forecast stands at wk4. Net:
    wk3 = 40, wk4 = 80 => total 120 (the pair netted once — 40 of the forecast
    was replaced by the concrete booking, never double-counted as 40 + 120)."""
    o = {3: 40.0}
    f = {4: 120.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: 2})
    g = core.consume_demand(d)
    assert sum(g[ITEM].values()) == pytest.approx(120.0)
    assert g[ITEM].get(3) == pytest.approx(40.0)
    assert g[ITEM].get(4) == pytest.approx(80.0)


def test_orders_exceeding_forecast_keep_remainder():
    """Booking 200 @wk3, forecast 120 @wk4, window 2: the booking consumes all
    120 forecast (wk4 -> 0); the concrete 200 stands at wk3. Net = 200 total,
    never 320 — the forecast is fully replaced by the larger concrete demand."""
    o = {3: 200.0}
    f = {4: 120.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: 2})
    g = core.consume_demand(d)
    assert sum(g[ITEM].values()) == pytest.approx(200.0)
    assert g[ITEM].get(3) == pytest.approx(200.0)
    assert 4 not in g[ITEM]


def test_two_bookings_do_not_double_consume_same_forecast():
    """Two bookings 60 @wk3 and 80 @wk5 both within window of forecast 120
    @wk4: the forecast is consumed ONCE (remaining_fc tracks it globally).
    wk3 booking consumes 60 of the forecast; wk5 booking consumes the other 60.
    Both concrete bookings stand at their own buckets, no forecast remains.
    Net: wk3 = 60, wk5 = 80, wk4 = 0 => total 140. The forecast 120 is netted
    against both bookings once, never counted as 60 + 80 + 120 = 260."""
    o = {3: 60.0, 5: 80.0}
    f = {4: 120.0}
    d = build_pd(co_b={ITEM: o}, fc_b={ITEM: f}, consume_window={ITEM: 2})
    g = core.consume_demand(d)
    total = sum(g[ITEM].values())
    assert total == pytest.approx(140.0)
    assert g[ITEM].get(3) == pytest.approx(60.0)
    assert g[ITEM].get(5) == pytest.approx(80.0)
    assert 4 not in g[ITEM]
    assert total <= _max_sum(o, f) + 1e-9
