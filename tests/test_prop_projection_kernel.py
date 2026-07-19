"""Property-based exactness net for the projected-inventory kernel
(engine/kernel/calc/projection.py) — moteur-c1 C1.

The kernel's whole job is one accounting identity — closing = opening +
Σ(in-bucket supply) - Σ(in-bucket demand) — computed on Decimal so it is EXACT,
plus a shortage flag keyed off a documented epsilon. Goldens check the identity
on a few fixed buckets; the properties below assert it (exactly, in Decimal)
across every mix of in-window and out-of-window events, verify bucket-to-bucket
carry over a chain, prove event-order independence, and pin the shortage-flag
boundary the SQL-parity depends on.

All tests are pure (no DB). Quantities are clean 2-dp Decimals so `Decimal(str
(q))` (the kernel's own coercion) round-trips exactly.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from ootils_core.engine.kernel.calc.projection import ProjectionKernel
from ootils_core.engine.kernel.shortage.detector import SHORTAGE_EPSILON

# Clean 2-dp decimals: event quantities are magnitudes (>= 0); opening stock may
# be negative (a projection can open in backorder). str(Decimal('12.34')) ==
# '12.34', so the kernel's Decimal(str(q)) coercion is loss-free and the balance
# identity holds with EXACT Decimal equality (no tolerance).
_qty = st.decimals(min_value=Decimal("0"), max_value=Decimal("100000"), places=2, allow_nan=False, allow_infinity=False)
_opening = st.decimals(
    min_value=Decimal("-100000"), max_value=Decimal("100000"), places=2, allow_nan=False, allow_infinity=False
)
# A window wide enough to straddle every bucket generated below, so events fall
# both inside and outside the bucket(s) and the point_in_bucket filter is
# actually exercised.
_event_date = st.dates(min_value=date(2025, 12, 25), max_value=date(2026, 4, 1))
_events = st.lists(st.tuples(_event_date, _qty), max_size=8)


@st.composite
def _single_bucket(
    draw: st.DrawFn,
) -> tuple[Decimal, list[tuple[date, Decimal]], list[tuple[date, Decimal]], date, date]:
    bucket_start = date(2026, 1, 1)
    bucket_end = bucket_start + timedelta(days=draw(st.integers(min_value=1, max_value=30)))
    return draw(_opening), draw(_events), draw(_events), bucket_start, bucket_end


@st.composite
def _chained_buckets(
    draw: st.DrawFn,
) -> tuple[Decimal, list[date], list[tuple[date, Decimal]], list[tuple[date, Decimal]]]:
    start = date(2026, 1, 1)
    n = draw(st.integers(min_value=2, max_value=5))
    span = draw(st.integers(min_value=1, max_value=14))
    boundaries = [start + timedelta(days=span * i) for i in range(n + 1)]
    return draw(_opening), boundaries, draw(_events), draw(_events)


def _sum_in_window(events: list[tuple[date, Decimal]], start: date, end: date) -> Decimal:
    return sum((Decimal(str(q)) for d, q in events if start <= d < end), Decimal("0"))


@given(_single_bucket())
def test_projection_exact_balance(
    scenario: tuple[Decimal, list[tuple[date, Decimal]], list[tuple[date, Decimal]], date, date],
) -> None:
    """CATCHES: a sign flip in the core identity (`+ inflows` -> `- inflows`, or
    inflows/outflows swapped) instantly. closing == opening + Σ(in-bucket
    supply) - Σ(in-bucket demand), with EXACT Decimal equality — inflows/
    outflows counting ONLY events with bucket_start <= date < bucket_end (end
    EXCLUSIVE). A golden checks one bucket; this checks the identity for every
    mix of in-window and out-of-window events."""
    opening, supply, demand, bs, be = scenario
    result = ProjectionKernel().compute_pi_node(opening, supply, demand, bs, be)
    expected_in = _sum_in_window(supply, bs, be)
    expected_out = _sum_in_window(demand, bs, be)
    assert result["inflows"] == expected_in
    assert result["outflows"] == expected_out
    assert result["closing_stock"] == opening + expected_in - expected_out
    assert result["opening_stock"] == opening


@given(_single_bucket(), st.data())
def test_projection_permutation_invariant(
    scenario: tuple[Decimal, list[tuple[date, Decimal]], list[tuple[date, Decimal]], date, date],
    data: st.DataObject,
) -> None:
    """CATCHES: any order-dependence in the accumulation (e.g. a switch to a
    lossy float accumulator where event order would matter). Shuffling the
    supply and demand event lists yields a byte-identical result dict —
    Decimal addition is exact and commutative, so the invariant must hold
    perfectly, not approximately."""
    opening, supply, demand, bs, be = scenario
    kernel = ProjectionKernel()
    baseline = kernel.compute_pi_node(opening, supply, demand, bs, be)
    shuffled = kernel.compute_pi_node(
        opening,
        list(data.draw(st.permutations(supply))),
        list(data.draw(st.permutations(demand))),
        bs,
        be,
    )
    assert baseline == shuffled


@given(_chained_buckets())
def test_projection_chaining_carries_closing(
    scenario: tuple[Decimal, list[date], list[tuple[date, Decimal]], list[tuple[date, Decimal]]],
) -> None:
    """CATCHES: a bucket-to-bucket carry bug (a dropped or duplicated closing)
    that a single-bucket golden structurally cannot see. Feeding closing(b) as
    opening(b+1) across a chain of CONSECUTIVE buckets, each result's opening
    equals the previous closing, and the final closing telescopes to opening +
    Σ(all in-horizon supply) - Σ(all in-horizon demand) — computed here from an
    INDEPENDENT window sum, not from the kernel's own per-bucket figures."""
    opening, boundaries, supply, demand = scenario
    kernel = ProjectionKernel()
    running = opening
    for i in range(len(boundaries) - 1):
        bs, be = boundaries[i], boundaries[i + 1]
        result = kernel.compute_pi_node(running, supply, demand, bs, be)
        assert result["opening_stock"] == running
        running = result["closing_stock"]
    horizon_in = _sum_in_window(supply, boundaries[0], boundaries[-1])
    horizon_out = _sum_in_window(demand, boundaries[0], boundaries[-1])
    assert running == opening + horizon_in - horizon_out


@given(_single_bucket())
def test_projection_shortage_flag_consistent(
    scenario: tuple[Decimal, list[tuple[date, Decimal]], list[tuple[date, Decimal]], date, date],
) -> None:
    """CATCHES: a flipped shortage comparison or a wrong epsilon sign at the ~0
    boundary the SQL parity depends on. has_shortage IFF closing <
    -SHORTAGE_EPSILON, and shortage_qty is |closing| when short else exactly 0.
    Random scenarios routinely drive closing negative (demand > opening +
    supply), exercising both sides of the flag."""
    opening, supply, demand, bs, be = scenario
    result = ProjectionKernel().compute_pi_node(opening, supply, demand, bs, be)
    closing = result["closing_stock"]
    assert result["has_shortage"] == (closing < -SHORTAGE_EPSILON)
    if result["has_shortage"]:
        assert result["shortage_qty"] == abs(closing)
    else:
        assert result["shortage_qty"] == Decimal("0")
