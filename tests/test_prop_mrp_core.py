"""Property-based exactness net for the MRP math core (engine/mrp/core.py) —
moteur-c1 C1.

Covers three primitives:

* consume_demand — forecast consumption + demand-time-fence. Invariants:
  non-negative net, NO demand created (Σ net <= Σ orders + Σ forecast), the
  exact golden semantics under max_only/window0/no-fence (net == max(orders,
  forecast), orders never lost), monotonicity, and order-independence.
* lot_size / apply_lot_rule — lot sizing. Invariants: an order covers at least
  the net requirement (uncapped), ships whole multiples, respects the max cap
  for every rule EXCEPT the documented MIN_MAX, and is monotonic in the
  shortage.

All tests are pure (no DB). The PlanningData built here carries only the fields
each primitive reads; every other field keeps its dataclass default.
"""
from __future__ import annotations

import datetime as dt

from hypothesis import given
from hypothesis import strategies as st

from ootils_core.engine.mrp.core import (
    PlanningData,
    apply_lot_rule,
    consume_demand,
    lot_size,
)

TOL = 1e-6
_HORIZON_START = dt.date(2026, 1, 5)
_N_BUCKETS = 8

_qty = st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False)
_ITEMS = ["I0", "I1", "I2"]

# Every lot rule EXCEPT MIN_MAX: MIN_MAX uses max_order_qty as a target stock
# LEVEL (ss + maxoq - pa), not a per-order cap or a shortfall cover, so the
# "covers the shortfall" and "respects the cap" invariants deliberately exclude
# it (documented carve-out in apply_lot_rule).
_RULES_NON_MINMAX = ["LOTFORLOT", "POQ", "EOQ", "FIXED_QTY", "MULTIPLE"]


def _is_whole_multiple(value: float, mult: float) -> bool:
    ratio = value / mult
    return abs(ratio - round(ratio)) <= 1e-9 * max(1.0, abs(ratio))


@st.composite
def _consume_data(
    draw: st.DrawFn,
    *,
    force_max_only: bool = False,
    no_fence: bool = False,
    no_window: bool = False,
) -> PlanningData:
    items = draw(st.lists(st.sampled_from(_ITEMS), min_size=1, max_size=3, unique=True))
    co_b: dict[str, dict[int, float]] = {}
    fc_b: dict[str, dict[int, float]] = {}
    strat: dict[str, str] = {}
    frozen_d: dict[str, int] = {}
    consume_window: dict[str, int] = {}
    for item in items:
        orders: dict[int, float] = {}
        forecast: dict[int, float] = {}
        for bucket in range(_N_BUCKETS):
            if draw(st.booleans()):
                orders[bucket] = draw(_qty)
            if draw(st.booleans()):
                forecast[bucket] = draw(_qty)
        co_b[item] = orders
        fc_b[item] = forecast
        strat[item] = (
            "max_only"
            if force_max_only
            else draw(st.sampled_from(["max_only", "forecast_only", "orders_only"]))
        )
        frozen_d[item] = 0 if no_fence else draw(st.sampled_from([0, 7, 14, 21]))
        consume_window[item] = 0 if no_window else draw(st.integers(min_value=0, max_value=3))
    return PlanningData(
        horizon_start=_HORIZON_START,
        n_buckets=_N_BUCKETS,
        co_b=co_b,
        fc_b=fc_b,
        strat=strat,
        frozen_d=frozen_d,
        consume_window=consume_window,
    )


@st.composite
def _lot_rule_args(draw: st.DrawFn, *, rules: list[str], capped: bool) -> tuple:
    rule = draw(st.sampled_from(rules))
    shortfall = draw(st.floats(min_value=0.01, max_value=100_000.0, allow_nan=False, allow_infinity=False))
    ss = draw(st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False))
    pa = draw(st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False))
    t = draw(st.integers(min_value=0, max_value=_N_BUCKETS - 1))
    netreq = {
        k: draw(st.floats(min_value=0.0, max_value=500.0, allow_nan=False, allow_infinity=False))
        for k in range(_N_BUCKETS)
        if draw(st.booleans())
    }
    P = draw(st.integers(min_value=1, max_value=6))
    eoq = draw(st.floats(min_value=0.0, max_value=10_000.0, allow_nan=False, allow_infinity=False))
    maxoq = (
        draw(st.floats(min_value=10.0, max_value=5000.0, allow_nan=False, allow_infinity=False))
        if capped
        else 0.0
    )
    moq = draw(st.sampled_from([0.0, 10.0, 50.0]))
    mult = draw(st.sampled_from([0.0, 1.0, 5.0, 25.0]))
    return rule, shortfall, pa, ss, netreq, t, P, eoq, maxoq, moq, mult


@given(_consume_data())
def test_consume_demand_non_negative(data: PlanningData) -> None:
    """CATCHES: a consumption path that lets net demand go negative (e.g. a
    subtract where a max belongs). Every emitted net value is >= 0 for ANY
    strategy/fence/window mix — a space a per-bucket golden never enumerates."""
    result = consume_demand(data)
    for buckets in result.values():
        for value in buckets.values():
            assert value >= 0.0


@given(_consume_data())
def test_consume_demand_creates_no_demand(data: PlanningData) -> None:
    """CATCHES: netting/consumption that INFLATES demand instead of reducing it
    (e.g. orders + forecast summed instead of maxed/consumed). Σ net <= Σ orders
    + Σ forecast per item — the mass bound that holds across ALL strategies and
    any forecast-consumption window. A per-bucket golden cannot express it
    because it never sums a random cross-bucket window."""
    result = consume_demand(data)
    for item in set(data.co_b) | set(data.fc_b):
        gross = sum(data.co_b.get(item, {}).values()) + sum(data.fc_b.get(item, {}).values())
        net = sum(result.get(item, {}).values())
        assert net <= gross + TOL


@given(_consume_data(force_max_only=True, no_fence=True, no_window=True))
def test_consume_demand_max_only_is_bucket_max(data: PlanningData) -> None:
    """CATCHES: the golden max_only semantics drifting. With window == 0, no
    fence, strat max_only, each bucket's net is EXACTLY max(orders, forecast) —
    orders (concrete) are never lost and forecast is netted, never summed on
    top. Randomized so every ordering of o<f, o>f, o==f, and the empty-bucket
    case is hit; the exactness (not just a bound) is what kills a `max`->`+` or
    `max`->`min` mutant."""
    result = consume_demand(data)
    for item in set(data.co_b) | set(data.fc_b):
        orders = data.co_b.get(item, {})
        forecast = data.fc_b.get(item, {})
        for bucket in set(orders) | set(forecast):
            o = orders.get(bucket, 0.0)
            f = forecast.get(bucket, 0.0)
            got = result.get(item, {}).get(bucket, 0.0)
            assert got == max(o, f)
            assert got >= o - TOL


@given(
    _consume_data(force_max_only=True, no_fence=True, no_window=True),
    st.sampled_from(_ITEMS),
    st.integers(min_value=0, max_value=_N_BUCKETS - 1),
    st.floats(min_value=0.0, max_value=500.0, allow_nan=False, allow_infinity=False),
)
def test_consume_demand_monotone_in_gross(
    data: PlanningData, item: str, bucket: int, bump: float
) -> None:
    """CATCHES: a mutant where MORE gross demand yields LESS net — the
    monotonicity a golden cannot assert without enumerating pairs. Bump one
    forecast bucket up by delta >= 0; total net for that item must not
    decrease."""
    before = sum(consume_demand(data).get(item, {}).values())
    forecast = dict(data.fc_b.get(item, {}))
    forecast[bucket] = forecast.get(bucket, 0.0) + bump
    data.fc_b[item] = forecast
    after = sum(consume_demand(data).get(item, {}).values())
    assert after >= before - TOL


@given(_consume_data())
def test_consume_demand_order_independent(data: PlanningData) -> None:
    """CATCHES: any reliance on dict iteration order (a single-order golden
    never reveals it). Rebuilding every input dict in reversed insertion order
    yields byte-identical net demand."""
    baseline = {k: dict(v) for k, v in consume_demand(data).items()}
    reversed_data = PlanningData(
        horizon_start=data.horizon_start,
        n_buckets=data.n_buckets,
        co_b={k: dict(reversed(list(v.items()))) for k, v in reversed(list(data.co_b.items()))},
        fc_b={k: dict(reversed(list(v.items()))) for k, v in reversed(list(data.fc_b.items()))},
        strat=data.strat,
        frozen_d=data.frozen_d,
        consume_window=data.consume_window,
    )
    reordered = {k: dict(v) for k, v in consume_demand(reversed_data).items()}
    assert baseline == reordered


@given(
    st.floats(min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
    st.sampled_from([0.0, 10.0, 50.0]),
    st.sampled_from([0.0, 1.0, 2.0, 5.0, 10.0, 25.0]),
)
def test_lot_size_only_ever_raises(qty: float, moq: float, mult: float) -> None:
    """CATCHES: a floor/ceil or >=/> slip in lot sizing that UNDER-orders and
    silently reopens a shortage. lot_size only ever raises: result >= the raw
    qty, >= moq when set, and is an exact multiple of mult when set. A golden's
    single value won't probe the moq/mult boundary interaction."""
    result = lot_size(qty, moq, mult)
    assert result >= qty - TOL
    if moq and moq > 0:
        assert result >= moq - TOL
    if mult and mult > 0:
        assert _is_whole_multiple(result, mult)


@given(
    st.floats(min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
    st.floats(min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
    st.sampled_from([0.0, 10.0, 50.0]),
    st.sampled_from([0.0, 1.0, 5.0, 25.0]),
)
def test_lot_size_monotone(qty_a: float, qty_b: float, moq: float, mult: float) -> None:
    """CATCHES: a comparison flip that shrinks the lot as demand grows. lot_size
    is monotonic non-decreasing in the raw qty — a point golden can't see it."""
    lo, hi = sorted((qty_a, qty_b))
    assert lot_size(hi, moq, mult) >= lot_size(lo, moq, mult) - TOL


@given(_lot_rule_args(rules=_RULES_NON_MINMAX, capped=False))
def test_apply_lot_rule_covers_shortfall_uncapped(args: tuple) -> None:
    """CATCHES: an under-order in ANY lot rule (POQ's `+ window` flipped to
    `- window`, EOQ's max flipped to min, a lost ceil). With NO max cap
    (maxoq = 0) every rule orders at least the net requirement (shortfall) and
    ships a whole multiple. Checked across every rule at once — no single-rule
    golden does that."""
    rule, shortfall, pa, ss, netreq, t, P, eoq, maxoq, moq, mult = args
    result = apply_lot_rule(rule, shortfall, pa, ss, netreq, t, P, eoq, maxoq, moq, mult, _N_BUCKETS)
    assert result >= shortfall - TOL
    if mult and mult > 0:
        assert _is_whole_multiple(result, mult)


@given(_lot_rule_args(rules=_RULES_NON_MINMAX, capped=True))
def test_apply_lot_rule_respects_max_cap(args: tuple) -> None:
    """CATCHES: a dropped max-order cap, or a dropped `rule != 'MIN_MAX'` guard.
    For every rule EXCEPT the documented MIN_MAX, the order never exceeds
    max_order_qty — regardless of which rule produced the raw qty. Random eoq /
    shortfall magnitudes routinely push the raw qty above the cap, exercising
    the clamp a golden rarely triggers."""
    rule, shortfall, pa, ss, netreq, t, P, eoq, maxoq, moq, mult = args
    result = apply_lot_rule(rule, shortfall, pa, ss, netreq, t, P, eoq, maxoq, moq, mult, _N_BUCKETS)
    assert result <= maxoq + TOL


@given(
    _lot_rule_args(rules=_RULES_NON_MINMAX, capped=False),
    st.floats(min_value=0.0, max_value=5000.0, allow_nan=False, allow_infinity=False),
)
def test_apply_lot_rule_monotone_in_shortfall(args: tuple, extra: float) -> None:
    """CATCHES: a rule whose order SHRINKS as the shortage grows (uncapped).
    Bigger net requirement never yields a smaller order — the 'plus de demande
    brute => jamais moins de besoin net' direction, which a golden can't assert
    without pairs."""
    rule, shortfall, pa, ss, netreq, t, P, eoq, maxoq, moq, mult = args
    low = apply_lot_rule(rule, shortfall, pa, ss, netreq, t, P, eoq, maxoq, moq, mult, _N_BUCKETS)
    high = apply_lot_rule(
        rule, shortfall + extra, pa, ss, netreq, t, P, eoq, maxoq, moq, mult, _N_BUCKETS
    )
    assert high >= low - TOL
