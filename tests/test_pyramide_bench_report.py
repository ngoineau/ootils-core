"""
Pure unit tests of the reconciliation-bench report layer
(``pyramide/hierarchy/bench.py``) — no database.

Covers: BenchRow scoring via the accuracy module (hand-calculated golden,
two methods, expected verdict), deterministic report ordering, None-safe
verdicts (undefined WAPE never wins; all-None block -> None verdict),
tie-breaking, the strategy gate ('two_stage' raises
NotImplementedError BEFORE any DB access — provable with db=None), and
the grain layer (golden bucketing of a known daily series, today-snapping
to complete buckets, unknown grain -> ValueError, day == historical
behaviour).
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from ootils_core.pyramide.hierarchy.bench import (
    GRAIN_DAY,
    GRAIN_MONTH,
    GRAIN_WEEK,
    LEVEL_LEAF,
    LEVEL_ROOT,
    METHOD_BASE,
    BenchRow,
    _bench_cutoff,
    _bucket_daily,
    _dense_curve,
    _shift_buckets,
    build_bench_report,
    compute_bench_row,
    run_reconciliation_bench,
)

CUTOFF = date(2026, 6, 1)


def _report(rows, **overrides):
    kwargs = dict(
        domain="test-domain",
        cutoff=CUTOFF,
        horizon=2,
        holdout_days=2,
        lookback_days=30,
        warnings=(),
    )
    kwargs.update(overrides)
    return build_bench_report(rows, **kwargs)


def _row(
    block="B1",
    level=LEVEL_LEAF,
    method="middleout",
    wape="0.4",
    mase=None,
    bias="0",
    n_series=2,
    n_obs=4,
):
    return BenchRow(
        block=block,
        level=level,
        method=method,
        wape=None if wape is None else Decimal(wape),
        mase=None if mase is None else Decimal(mase),
        bias=None if bias is None else Decimal(bias),
        n_series=n_series,
        n_obs=n_obs,
    )


# ---------------------------------------------------------------------------
# Golden: hand-calculated scoring of two methods, expected verdict
# ---------------------------------------------------------------------------


def test_golden_two_methods_leaf_scoring_and_verdict():
    # Two leaves, horizon 2. Actuals: L1 = [10, 0], L2 = [0, 10].
    actuals = [
        (Decimal(10), Decimal(0)),
        (Decimal(0), Decimal(10)),
    ]
    # Insamples: L1 constant (MASE denominator 0 -> None, excluded),
    # L2 empty (too short -> None, excluded) => row MASE is None.
    insamples = [(Decimal(10), Decimal(10), Decimal(10)), ()]

    # middleout: L1 = [8, 2], L2 = [2, 8].
    #   pooled |e| = 2+2+2+2 = 8 ; sum|a| = 20     -> WAPE = 0.4
    #   bias = ((8-10)+(2-0)+(2-0)+(8-10)) / 4     -> 0
    mo = compute_bench_row(
        block="B1", level=LEVEL_LEAF, method="middleout",
        actual_curves=actuals,
        forecast_curves=[
            (Decimal(8), Decimal(2)),
            (Decimal(2), Decimal(8)),
        ],
        insamples=insamples,
    )
    assert mo.wape == Decimal("0.4")
    assert mo.bias == Decimal("0")
    assert mo.mase is None  # None-safe per-series exclusion
    assert (mo.n_series, mo.n_obs) == (2, 4)

    # base (uniform): both leaves [5, 5].
    #   pooled |e| = 5+5+5+5 = 20 ; sum|a| = 20    -> WAPE = 1.0
    base = compute_bench_row(
        block="B1", level=LEVEL_LEAF, method=METHOD_BASE,
        actual_curves=actuals,
        forecast_curves=[
            (Decimal(5), Decimal(5)),
            (Decimal(5), Decimal(5)),
        ],
        insamples=insamples,
    )
    assert base.wape == Decimal("1")
    assert base.bias == Decimal("0")

    report = _report([base, mo])
    assert report.verdicts() == {"B1": "middleout"}


def test_golden_mase_averages_defined_series_only():
    # L1: insample [0, 10, 0] -> naive MAE = (10+10)/2 = 10;
    #     |e| = |10-8| + |0-2| = 4 over 2 obs -> MASE = 2/10 = 0.2
    # L2: constant insample -> None (excluded from the mean).
    row = compute_bench_row(
        block="B1", level=LEVEL_LEAF, method="middleout",
        actual_curves=[
            (Decimal(10), Decimal(0)),
            (Decimal(0), Decimal(10)),
        ],
        forecast_curves=[
            (Decimal(8), Decimal(2)),
            (Decimal(2), Decimal(8)),
        ],
        insamples=[
            (Decimal(0), Decimal(10), Decimal(0)),
            (Decimal(5), Decimal(5), Decimal(5)),
        ],
    )
    assert row.mase == Decimal("0.2")


def test_compute_bench_row_validates_alignment():
    with pytest.raises(ValueError):
        compute_bench_row(
            block="B1", level=LEVEL_LEAF, method="m",
            actual_curves=[], forecast_curves=[], insamples=[],
        )
    with pytest.raises(ValueError):
        compute_bench_row(
            block="B1", level=LEVEL_LEAF, method="m",
            actual_curves=[(Decimal(1),)],
            forecast_curves=[(Decimal(1),), (Decimal(2),)],
            insamples=[()],
        )
    with pytest.raises(ValueError):
        compute_bench_row(
            block="B1", level=LEVEL_LEAF, method="m",
            actual_curves=[(Decimal(1), Decimal(2))],
            forecast_curves=[(Decimal(1),)],  # per-series length mismatch
            insamples=[()],
        )


# ---------------------------------------------------------------------------
# Report construction: deterministic ordering, to_rows
# ---------------------------------------------------------------------------


def test_rows_sorted_block_then_level_rank_then_method():
    shuffled = [
        _row(block="B2", level=LEVEL_LEAF, method="middleout"),
        _row(block="B1", level=LEVEL_LEAF, method="middleout"),
        _row(block="B1", level=LEVEL_ROOT, method="middleout"),
        _row(block="B1", level="family", method="middleout"),
        _row(block="B1", level=LEVEL_LEAF, method=METHOD_BASE, wape="1.0"),
    ]
    report = _report(shuffled)
    key = [(r.block, r.level, r.method) for r in report.rows]
    assert key == [
        ("B1", LEVEL_ROOT, "middleout"),   # root first
        ("B1", "family", "middleout"),     # intermediate (recon) level
        ("B1", LEVEL_LEAF, METHOD_BASE),   # leaves last, methods sorted
        ("B1", LEVEL_LEAF, "middleout"),
        ("B2", LEVEL_LEAF, "middleout"),
    ]
    # Same inputs in any order -> byte-identical report (determinism).
    assert _report(list(reversed(shuffled))) == report


def test_to_rows_mirrors_sorted_rows():
    report = _report([_row(method="middleout"), _row(method=METHOD_BASE, wape="1.0")])
    assert report.to_rows() == [
        ("B1", LEVEL_LEAF, METHOD_BASE, Decimal("1.0"), None, Decimal("0"), 2, 4),
        ("B1", LEVEL_LEAF, "middleout", Decimal("0.4"), None, Decimal("0"), 2, 4),
    ]


def test_report_is_frozen():
    report = _report([_row()])
    with pytest.raises(AttributeError):
        report.horizon = 99  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Verdicts: None-safety, ties, non-leaf rows ignored
# ---------------------------------------------------------------------------


def test_verdict_excludes_none_wape():
    report = _report([
        _row(method="middleout", wape=None),       # undefined -> never wins
        _row(method=METHOD_BASE, wape="1.5"),
    ])
    assert report.verdicts() == {"B1": METHOD_BASE}


def test_verdict_none_when_no_leaf_wape_defined():
    report = _report([
        _row(method="middleout", wape=None),
        _row(method=METHOD_BASE, wape=None),
        _row(block="B2", method="middleout", wape="0.3"),
    ])
    assert report.verdicts() == {"B1": None, "B2": "middleout"}


def test_verdict_ignores_non_leaf_levels():
    report = _report([
        _row(level=LEVEL_ROOT, method="middleout", wape="0.01"),
        _row(level="family", method="middleout", wape="0.02"),
        _row(level=LEVEL_LEAF, method="middleout", wape=None),
    ])
    # Excellent root WAPE cannot crown a method: only leaves decide.
    assert report.verdicts() == {"B1": None}


def test_verdict_tie_breaks_on_method_name():
    report = _report([
        _row(method="zeta", wape="0.4"),
        _row(method="alpha", wape="0.4"),
    ])
    assert report.verdicts() == {"B1": "alpha"}


# ---------------------------------------------------------------------------
# Strategy / parameter gate — validated BEFORE any DB access (db=None)
# ---------------------------------------------------------------------------


def test_two_stage_raises_not_implemented_without_db():
    with pytest.raises(NotImplementedError, match="two_stage.*geo hierarchy"):
        run_reconciliation_bench(None, domain="d", strategy="two_stage")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"strategy": "nonsense"},
        {"horizon": 0},
        {"holdout_days": 0},
        {"lookback_days": 0},
        {"methods": ()},
        {"methods": ("middleout", "definitely-not-a-method")},
        # 'base' is the implicit comparator, not a requestable method.
        {"methods": (METHOD_BASE,)},
        # unknown grain — rejected before any DB access.
        {"grain": "quarter"},
        {"grain": "daily"},  # engine vocabulary, not a bench grain
    ],
)
def test_invalid_params_raise_before_db_access(kwargs):
    with pytest.raises(ValueError):
        run_reconciliation_bench(None, domain="d", **kwargs)


# ---------------------------------------------------------------------------
# Grain: bucketing goldens, today-snapping, day == historical behaviour
# ---------------------------------------------------------------------------

# 2026 calendar facts used below: 2026-01-26, 2026-02-09, 2026-03-02 and
# 2026-05-25 are Mondays; 2026-05-26 is a Tuesday.
_DAILY = {
    "A": {
        date(2026, 1, 30): Decimal(3),   # Fri, week of Mon 2026-01-26
        date(2026, 1, 31): Decimal(4),   # Sat, same week
        date(2026, 2, 1): Decimal(5),    # Sun, SAME week — but February
        date(2026, 2, 14): Decimal(2),   # Sat, week of Mon 2026-02-09
        date(2026, 3, 2): Decimal(7),    # Mon, its own week start
    }
}


def test_golden_monthly_bucketing_hand_calculated():
    assert _bucket_daily(_DAILY, GRAIN_MONTH) == {
        "A": {
            date(2026, 1, 1): Decimal(7),   # 3 + 4
            date(2026, 2, 1): Decimal(7),   # 5 + 2
            date(2026, 3, 1): Decimal(7),
        }
    }


def test_golden_weekly_bucketing_iso_monday_keys():
    # Feb 1 (Sunday) belongs to the week STARTING Mon Jan 26 — the ISO
    # week wins over the month boundary at grain='week'.
    assert _bucket_daily(_DAILY, GRAIN_WEEK) == {
        "A": {
            date(2026, 1, 26): Decimal(12),  # 3 + 4 + 5
            date(2026, 2, 9): Decimal(2),
            date(2026, 3, 2): Decimal(7),
        }
    }


def test_today_snapping_month_mid_month_excludes_partial_month():
    # Data up to 26 May: May is partial -> excluded. holdout=1 -> the
    # last complete evaluated month is April (docstring example).
    assert _bench_cutoff(date(2026, 5, 26), GRAIN_MONTH, 1) == date(2026, 4, 1)
    assert _bench_cutoff(date(2026, 5, 26), GRAIN_MONTH, 2) == date(2026, 3, 1)
    # Year boundary in month arithmetic.
    assert _bench_cutoff(date(2026, 1, 15), GRAIN_MONTH, 12) == date(2025, 1, 1)
    assert _shift_buckets(date(2026, 1, 1), -1, GRAIN_MONTH) == date(2025, 12, 1)
    assert _shift_buckets(date(2025, 12, 1), 1, GRAIN_MONTH) == date(2026, 1, 1)


def test_today_snapping_week_mid_week_excludes_partial_week():
    # Tue 2026-05-26 snaps to Mon 2026-05-25 (partial week excluded).
    assert _bench_cutoff(date(2026, 5, 26), GRAIN_WEEK, 4) == date(2026, 4, 27)
    # A Monday is already a bucket start — snapping is a no-op.
    assert _bench_cutoff(date(2026, 5, 25), GRAIN_WEEK, 4) == date(2026, 4, 27)


def test_dense_curve_fills_empty_buckets_with_zero():
    monthly = {date(2026, 4, 1): Decimal(7)}
    assert _dense_curve(monthly, date(2026, 2, 1), 3, GRAIN_MONTH) == (
        Decimal(0), Decimal(0), Decimal(7),
    )


def test_grain_day_is_the_historical_behaviour():
    # Bucketing is the identity (the very same object — zero rewrite).
    assert _bucket_daily(_DAILY, GRAIN_DAY) is _DAILY
    # cutoff = today - holdout days, exactly the historical formula.
    today = date(2026, 5, 26)
    assert _bench_cutoff(today, GRAIN_DAY, 28) == today - timedelta(days=28)
    # Bucket stepping is day stepping.
    assert _shift_buckets(today, -3, GRAIN_DAY) == today - timedelta(days=3)
    # Dense eval curve: default grain == explicit day == daily stepping.
    by_date = {today: Decimal(1), today + timedelta(days=2): Decimal(5)}
    expected = (Decimal(1), Decimal(0), Decimal(5))
    assert _dense_curve(by_date, today, 3) == expected
    assert _dense_curve(by_date, today, 3, GRAIN_DAY) == expected
    # Reports built without a grain carry the day default.
    assert _report([_row()]).grain == GRAIN_DAY


def test_report_carries_grain():
    report = _report([_row()], grain=GRAIN_MONTH)
    assert report.grain == GRAIN_MONTH
