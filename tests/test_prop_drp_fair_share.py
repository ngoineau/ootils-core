"""Property-based exactness net for the DRP distribution core
(engine/drp/core.py) — moteur-c1 C1.

Two levels are checked:

* _fair_share_round — the single logistics down-rounding primitive. Its
  invariants (never over-ship the bounded need / the source / the cap; whole
  multiples only; monotonic in available stock) are asserted across the whole
  input box, including the boundaries where a floor/ceil confusion strips the
  source.
* transfer_signals — the aggregate plan. Conservation is the property no fixed
  golden can parametrize: total shipped OUT of a source never exceeds its
  distributable excess (the 'down-round remnant is never lost / double-spent'
  guarantee, including the remnant-sweep pass), total shipped INTO a
  destination never exceeds its deficit, and every emitted qty is a positive
  whole multiple.

All tests are pure (no DB). Float comparisons use a small tolerance; the values
are bounded so the absolute tolerance below is comfortably above the rounding
error of a single floor()*mult.
"""
from __future__ import annotations

from collections import defaultdict

from hypothesis import given
from hypothesis import strategies as st

from ootils_core.engine.drp.core import (
    TransferLink,
    _fair_share_round,
    excess_by_location,
    projected_deficits,
    transfer_signals,
)

# Absolute float tolerance for conservation/multiple checks. floor(x)*mult on
# values <= ~1e6 with mult >= 0.5 carries at most a few ULPs of error; 1e-6 is
# far above that yet far below any real over-ship a mutant would introduce.
TOL = 1e-6

_mult = st.sampled_from([0.5, 1.0, 2.0, 5.0, 10.0, 12.0, 25.0, 100.0])
_nonneg = st.floats(min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False)
_small_nonneg = st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False)
_max_qty = st.one_of(
    st.none(),
    st.floats(min_value=0.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
)

_LOCS = ["L0", "L1", "L2", "L3"]
_ITEMS = ["A", "B"]


def _is_whole_multiple(qty: float, mult: float) -> bool:
    ratio = qty / mult
    return abs(ratio - round(ratio)) <= 1e-9 * max(1.0, abs(ratio))


@st.composite
def _drp_scenario(
    draw: st.DrawFn,
) -> tuple[
    dict[tuple[str, str], dict[int, float]],
    dict[tuple[str, str], float],
    dict[tuple[str, str], float],
    list[TransferLink],
    int,
    float,
]:
    horizon = draw(st.integers(min_value=2, max_value=8))
    mult = draw(st.sampled_from([1.0, 2.0, 5.0, 10.0]))
    coords = draw(
        st.lists(
            st.tuples(st.sampled_from(_ITEMS), st.sampled_from(_LOCS)),
            min_size=2,
            max_size=8,
            unique=True,
        )
    )
    demand_by_loc: dict[tuple[str, str], dict[int, float]] = {}
    on_hand_by_loc: dict[tuple[str, str], float] = {}
    safety_by_loc: dict[tuple[str, str], float] = {}
    for coord in coords:
        demand: dict[int, float] = {}
        for _ in range(draw(st.integers(min_value=0, max_value=3))):
            bucket = draw(st.integers(min_value=0, max_value=horizon - 1))
            demand[bucket] = demand.get(bucket, 0.0) + draw(
                st.floats(min_value=0.0, max_value=500.0, allow_nan=False, allow_infinity=False)
            )
        demand_by_loc[coord] = demand
        on_hand_by_loc[coord] = draw(
            st.floats(min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False)
        )
        safety_by_loc[coord] = draw(
            st.floats(min_value=0.0, max_value=200.0, allow_nan=False, allow_infinity=False)
        )

    links: list[TransferLink] = []
    for _ in range(draw(st.integers(min_value=1, max_value=6))):
        src = draw(st.sampled_from(_LOCS))
        dst = draw(st.sampled_from(_LOCS))
        if src == dst:
            continue
        links.append(
            TransferLink(
                source_location=src,
                dest_location=dst,
                lead_buckets=draw(st.integers(min_value=0, max_value=4)),
                min_qty=draw(st.sampled_from([0.0, mult, 2.0 * mult])),
                max_qty=draw(
                    st.one_of(
                        st.none(),
                        st.floats(
                            min_value=mult, max_value=2000.0, allow_nan=False, allow_infinity=False
                        ),
                    )
                ),
                priority=draw(st.integers(min_value=1, max_value=3)),
                item=draw(st.one_of(st.none(), st.sampled_from(_ITEMS))),
                transfer_multiple=mult,
            )
        )
    return demand_by_loc, on_hand_by_loc, safety_by_loc, links, horizon, mult


@given(_nonneg, _mult, _small_nonneg, _max_qty, _nonneg)
def test_fair_share_round_conserves(
    raw_part: float, mult: float, min_qty: float, max_qty: float | None, avail: float
) -> None:
    """CATCHES: a lot-sizing sign/rounding flip (floor -> ceil, or a dropped
    min()) that a fixed golden misses. The returned qty must NEVER exceed the
    bounded need (raw_part), the source's remaining excess (avail), or the lane
    cap (max_qty), and is never negative. One golden checks one (raw, mult,
    avail) point; this sweeps the whole box — including the boundaries where a
    ceil confusion over-ships and strips the source."""
    qty = _fair_share_round(raw_part, mult, min_qty, max_qty, avail)
    assert qty >= 0.0
    assert qty <= avail + TOL
    assert qty <= raw_part + TOL
    if max_qty is not None:
        assert qty <= max_qty + TOL


@given(_nonneg, _mult, _small_nonneg, _max_qty, _nonneg)
def test_fair_share_round_is_whole_multiple(
    raw_part: float, mult: float, min_qty: float, max_qty: float | None, avail: float
) -> None:
    """CATCHES: a transfer that ships a fractional case/pallet. The result is 0
    or an EXACT integer multiple of the lane multiple. Goldens check a couple
    of clean divisions; this hits ratios where float floor-division sits one
    ULP shy of the integer — exactly where a naive `qty % mult == 0` check
    breaks."""
    qty = _fair_share_round(raw_part, mult, min_qty, max_qty, avail)
    if qty > 0.0:
        assert _is_whole_multiple(qty, mult)


@given(_nonneg, _mult, _small_nonneg, _max_qty, _nonneg)
def test_fair_share_round_respects_minimum(
    raw_part: float, mult: float, min_qty: float, max_qty: float | None, avail: float
) -> None:
    """CATCHES: a sub-minimum micro-transfer. The result is either 0 or
    >= min_qty — the lane physically cannot ship below its minimum. Off-by-one
    goldens don't probe the min_qty boundary; the randomized min_qty here does."""
    qty = _fair_share_round(raw_part, mult, min_qty, max_qty, avail)
    assert qty == 0.0 or qty >= min_qty - TOL


@given(_nonneg, _mult, _small_nonneg, _max_qty, _nonneg, _nonneg)
def test_fair_share_round_monotonic_in_avail(
    raw_part: float,
    mult: float,
    min_qty: float,
    max_qty: float | None,
    avail_a: float,
    avail_b: float,
) -> None:
    """CATCHES: a flipped comparison that makes allocation DECREASE as more
    stock becomes available — the anti-starvation direction. Fix the lane and
    the need; raising the source's remaining excess can only keep or increase
    the shipped qty. A golden pins single points; this asserts the ordering
    across the whole avail axis."""
    lo, hi = sorted((avail_a, avail_b))
    q_lo = _fair_share_round(raw_part, mult, min_qty, max_qty, lo)
    q_hi = _fair_share_round(raw_part, mult, min_qty, max_qty, hi)
    assert q_hi >= q_lo - TOL


@given(_drp_scenario())
def test_transfer_signals_never_overship_a_source(
    scenario: tuple[
        dict[tuple[str, str], dict[int, float]],
        dict[tuple[str, str], float],
        dict[tuple[str, str], float],
        list[TransferLink],
        int,
        float,
    ],
) -> None:
    """CATCHES: the remnant-sweep re-offering capacity the proportional pass
    already spent (the 'reliquat jamais perdu / jamais double-tire' bug). Total
    shipped OUT of any (item, source) coordinate must never exceed that
    coordinate's distributable excess. The reference excess is recomputed from
    the UN-mutated excess_by_location, so a decrement-sign mutation inside
    transfer_signals shows up as an over-ship here. No golden can enumerate the
    multi-destination scarcity cases that stress this."""
    demand, on_hand, safety, links, horizon, _mult_unused = scenario
    signals = transfer_signals(demand, on_hand, safety, links, horizon)
    excess = excess_by_location(demand, on_hand, safety, horizon)
    shipped_out: dict[tuple[str, str], float] = defaultdict(float)
    for sig in signals:
        shipped_out[(sig.item, sig.source_location)] += sig.qty
    for coord, total in shipped_out.items():
        assert total <= excess.get(coord, 0.0) + TOL


@given(_drp_scenario())
def test_transfer_signals_never_overserve_a_deficit(
    scenario: tuple[
        dict[tuple[str, str], dict[int, float]],
        dict[tuple[str, str], float],
        dict[tuple[str, str], float],
        list[TransferLink],
        int,
        float,
    ],
) -> None:
    """CATCHES: a fair-share pass or sweep that over-serves a destination. Total
    shipped INTO any (item, dest) coordinate must never exceed its projected
    deficit. Random scenarios routinely produce the ties and residuals a fixed
    golden never lines up."""
    demand, on_hand, safety, links, horizon, _mult_unused = scenario
    signals = transfer_signals(demand, on_hand, safety, links, horizon)
    deficits = projected_deficits(demand, on_hand, safety, horizon)
    shipped_in: dict[tuple[str, str], float] = defaultdict(float)
    for sig in signals:
        shipped_in[(sig.item, sig.dest_location)] += sig.qty
    for coord, total in shipped_in.items():
        deficit_qty = deficits.get(coord, (0, 0.0))[1]
        assert total <= deficit_qty + TOL


@given(_drp_scenario())
def test_transfer_signals_ship_positive_whole_multiples(
    scenario: tuple[
        dict[tuple[str, str], dict[int, float]],
        dict[tuple[str, str], float],
        dict[tuple[str, str], float],
        list[TransferLink],
        int,
        float,
    ],
) -> None:
    """CATCHES: a zero or fractional-multiple transfer leaking into the plan.
    Every emitted signal ships a STRICTLY positive whole multiple of the lane
    multiple (all lanes share one multiple per scenario). A golden proves this
    for one hand-built plan; this proves it for every generated one."""
    demand, on_hand, safety, links, horizon, mult = scenario
    signals = transfer_signals(demand, on_hand, safety, links, horizon)
    for sig in signals:
        assert sig.qty > 0.0
        assert _is_whole_multiple(sig.qty, mult)
