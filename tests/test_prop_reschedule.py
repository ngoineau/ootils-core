"""Property-based exactness net for the reschedule-message dampening
(engine/mrp/core.py:reschedule_signals) — moteur-c1 C1.

The dampening rule and the CANCEL horizon-edge guard are exactly the kind of
boundary logic a hand-picked golden sits comfortably inside and never probes.
These properties construct controlled single-item plans where the receipt's
need bucket is FIXED by construction (a single demand spike), which decouples
the need date from the receipt date so the boundary can be walked precisely:

* a RESCHEDULE fires IFF the receipt is in a different bucket than its need AND
  the day-delta to the (Monday-aligned) proposed date is >= min_days (strict
  `< min_days` dampening);
* a CANCEL fires for a fully-surplus receipt IFF it sits strictly before the
  last loaded bucket (horizon-edge guard);
* an emitted RESCHEDULE always proposes a date different from the current one;
* a receipt already on its need bucket is silent for ANY min_days (the central
  stability invariant).

All tests are pure (no DB). MONDAY anchors the weekly bucket grid so
`proposed` (always a bucket-start Monday) has a predictable weekday.
"""
from __future__ import annotations

import datetime as dt

from hypothesis import given
from hypothesis import strategies as st

from ootils_core.engine.mrp.core import (
    CANCEL,
    RESCHEDULE_IN,
    RESCHEDULE_OUT,
    PlanningData,
    ReceiptOrder,
    reschedule_signals,
)

# 2026-01-05 is a Monday — the horizon anchor, so bucket b starts on the Monday
# MONDAY + b weeks and `proposed` (a bucket start) is always a Monday.
MONDAY = dt.date(2026, 1, 5)
_ITEM = "IT"
_N_BUCKETS = 12
_qty = st.floats(min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False)


def _single_receipt_plan(
    *, receipt_date: dt.date, qty: float, need_bucket: int, min_days: int
) -> tuple[PlanningData, dict[str, dict[int, float]]]:
    """One item, one receipt, one demand spike at ``need_bucket`` (on-hand 0,
    safety 0). The lone requirement is fully covered by the lone receipt, so the
    receipt's need bucket is exactly ``need_bucket`` REGARDLESS of its own date."""
    order = ReceiptOrder(
        node_id="r1",
        item_id=_ITEM,
        receipt_date=receipt_date,
        qty=qty,
        is_firm=False,
        node_type="PurchaseOrderSupply",
    )
    data = PlanningData(
        horizon_start=MONDAY,
        n_buckets=_N_BUCKETS,
        on_hand={_ITEM: 0.0},
        safety={_ITEM: 0.0},
        sched_orders={_ITEM: [order]},
        resched_min_days={_ITEM: min_days},
    )
    gross = {_ITEM: {need_bucket: qty}}
    return data, gross


@st.composite
def _single_receipt_scenario(
    draw: st.DrawFn,
) -> tuple[PlanningData, dict[str, dict[int, float]], int, dt.date, dt.date, int]:
    need_bucket = draw(st.integers(min_value=2, max_value=8))
    qty = draw(_qty)
    min_days = draw(st.integers(min_value=1, max_value=10))
    offset_days = draw(st.integers(min_value=-40, max_value=40))
    proposed = MONDAY + dt.timedelta(weeks=need_bucket)
    receipt_date = proposed + dt.timedelta(days=offset_days)
    data, gross = _single_receipt_plan(
        receipt_date=receipt_date, qty=qty, need_bucket=need_bucket, min_days=min_days
    )
    return data, gross, need_bucket, proposed, receipt_date, min_days


@st.composite
def _surplus_receipt_scenario(
    draw: st.DrawFn,
) -> tuple[PlanningData, dict[str, dict[int, float]], int, dt.date]:
    n_buckets = draw(st.integers(min_value=3, max_value=12))
    bucket = draw(st.integers(min_value=0, max_value=n_buckets + 2))
    qty = draw(_qty)
    receipt_date = MONDAY + dt.timedelta(weeks=bucket)
    order = ReceiptOrder(
        node_id="r1",
        item_id=_ITEM,
        receipt_date=receipt_date,
        qty=qty,
        is_firm=False,
        node_type="PurchaseOrderSupply",
    )
    data = PlanningData(
        horizon_start=MONDAY,
        n_buckets=n_buckets,
        on_hand={_ITEM: 0.0},
        safety={_ITEM: 0.0},
        sched_orders={_ITEM: [order]},
    )
    # No demand at all => no requirement => the receipt is entirely surplus.
    gross: dict[str, dict[int, float]] = {}
    return data, gross, n_buckets, receipt_date


@st.composite
def _multi_receipt_scenario(
    draw: st.DrawFn,
) -> tuple[PlanningData, dict[str, dict[int, float]]]:
    orders: list[ReceiptOrder] = []
    for i in range(draw(st.integers(min_value=1, max_value=4))):
        bucket = draw(st.integers(min_value=0, max_value=_N_BUCKETS - 1))
        weekday = draw(st.integers(min_value=0, max_value=6))
        orders.append(
            ReceiptOrder(
                node_id=f"r{i}",
                item_id=_ITEM,
                receipt_date=MONDAY + dt.timedelta(weeks=bucket, days=weekday),
                qty=draw(_qty),
                is_firm=draw(st.booleans()),
                node_type="PurchaseOrderSupply",
            )
        )
    demand: dict[int, float] = {}
    for _ in range(draw(st.integers(min_value=0, max_value=4))):
        bucket = draw(st.integers(min_value=0, max_value=_N_BUCKETS - 1))
        demand[bucket] = demand.get(bucket, 0.0) + draw(
            st.floats(min_value=0.0, max_value=500.0, allow_nan=False, allow_infinity=False)
        )
    data = PlanningData(
        horizon_start=MONDAY,
        n_buckets=_N_BUCKETS,
        on_hand={_ITEM: draw(st.floats(min_value=0.0, max_value=500.0, allow_nan=False, allow_infinity=False))},
        safety={_ITEM: draw(st.floats(min_value=0.0, max_value=200.0, allow_nan=False, allow_infinity=False))},
        sched_orders={_ITEM: orders},
    )
    gross = {_ITEM: demand} if demand else {}
    return data, gross


@given(_single_receipt_scenario())
def test_reschedule_dampening_characterization(
    scenario: tuple[PlanningData, dict[str, dict[int, float]], int, dt.date, dt.date, int],
) -> None:
    """CATCHES: an off-by-one in the strict `< min_days` dampening threshold AND
    a broken bucket-equality short-circuit — the exact spots a hand-picked
    example never lands on. With the need bucket fixed, a signal fires IFF the
    receipt is in a DIFFERENT bucket than its need AND |delta_days| >= min_days;
    otherwise NO signal. When it fires, the action direction and the proposed
    date are pinned too."""
    data, gross, need_bucket, proposed, receipt_date, min_days = scenario
    signals = reschedule_signals(data, gross)
    current_bucket = data.bucket(receipt_date)
    delta_days = (proposed - receipt_date).days
    if current_bucket == need_bucket:
        assert signals == []
    elif abs(delta_days) < min_days:
        assert signals == []
    else:
        assert len(signals) == 1
        sig = signals[0]
        assert sig.proposed_date == proposed
        assert sig.current_receipt_date == receipt_date
        assert sig.proposed_date != sig.current_receipt_date
        assert sig.action == (RESCHEDULE_IN if delta_days < 0 else RESCHEDULE_OUT)


@given(
    st.integers(min_value=2, max_value=8),
    _qty,
    st.integers(min_value=1, max_value=10),
)
def test_reschedule_boundary_exact_min_days(need_bucket: int, qty: float, min_days: int) -> None:
    """CATCHES: the exact dampening boundary a golden almost never lands on. At
    |delta| == min_days a signal MUST fire; at |delta| == min_days - 1 it must
    NOT. The receipt is placed at precisely those two offsets before the
    Monday-aligned proposed date (an earlier bucket, so buckets differ for any
    offset >= 1)."""
    proposed = MONDAY + dt.timedelta(weeks=need_bucket)
    for offset, expect_signal in ((min_days, True), (min_days - 1, False)):
        receipt_date = proposed - dt.timedelta(days=offset)
        data, gross = _single_receipt_plan(
            receipt_date=receipt_date, qty=qty, need_bucket=need_bucket, min_days=min_days
        )
        signals = reschedule_signals(data, gross)
        assert (len(signals) == 1) is expect_signal


@given(_surplus_receipt_scenario())
def test_reschedule_cancel_horizon_edge_guard(
    scenario: tuple[PlanningData, dict[str, dict[int, float]], int, dt.date],
) -> None:
    """CATCHES: a wrong horizon-edge CANCEL boundary. A fully-surplus receipt is
    CANCELled IFF it sits STRICTLY before the last loaded bucket; on or past the
    edge it is spared (its justifying demand may sit just beyond the loaded
    window). Randomizing the bucket around n_buckets-1 pins the `<` boundary a
    golden won't straddle."""
    data, gross, n_buckets, receipt_date = scenario
    signals = reschedule_signals(data, gross)
    current_bucket = data.bucket(receipt_date)
    if current_bucket < n_buckets - 1:
        assert len(signals) == 1
        assert signals[0].action == CANCEL
        assert signals[0].proposed_date is None
    else:
        assert signals == []


@given(
    st.integers(min_value=2, max_value=8),
    _qty,
    st.integers(min_value=0, max_value=20),
)
def test_reschedule_stable_when_receipt_on_need(
    need_bucket: int, qty: float, min_days: int
) -> None:
    """CATCHES: a spurious zero-delta message. THE central stability invariant
    (module docstring): a receipt already sitting in its need bucket produces
    ZERO signals for ANY min_days — re-running on unchanged data is silent. A
    golden checks one such case; this checks the whole family."""
    receipt_date = MONDAY + dt.timedelta(weeks=need_bucket)
    data, gross = _single_receipt_plan(
        receipt_date=receipt_date, qty=qty, need_bucket=need_bucket, min_days=min_days
    )
    assert reschedule_signals(data, gross) == []


@given(_multi_receipt_scenario())
def test_reschedule_emitted_signal_moves_the_date(
    scenario: tuple[PlanningData, dict[str, dict[int, float]]],
) -> None:
    """CATCHES: a no-op message (proposed date == current date) leaking into the
    plan. Every emitted RESCHEDULE proposes a date STRICTLY different from the
    receipt's current date; CANCEL proposes no date. Holds on arbitrary
    multi-receipt plans, not just the controlled single-receipt fixtures."""
    data, gross = scenario
    for sig in reschedule_signals(data, gross):
        if sig.action == CANCEL:
            assert sig.proposed_date is None
        else:
            assert sig.proposed_date is not None
            assert sig.proposed_date != sig.current_receipt_date
