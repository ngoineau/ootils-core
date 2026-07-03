"""
Pure unit tests of the reconciliation-bench report layer
(``pyramide/hierarchy/bench.py``) — no database.

Covers: BenchRow scoring via the accuracy module (hand-calculated golden,
two methods, expected verdict), deterministic report ordering, None-safe
verdicts (undefined WAPE never wins; all-None block -> None verdict),
tie-breaking, and the strategy gate ('two_stage' raises
NotImplementedError BEFORE any DB access — provable with db=None).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ootils_core.pyramide.hierarchy.bench import (
    LEVEL_LEAF,
    LEVEL_ROOT,
    METHOD_BASE,
    BenchRow,
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
    ],
)
def test_invalid_params_raise_before_db_access(kwargs):
    with pytest.raises(ValueError):
        run_reconciliation_bench(None, domain="d", **kwargs)
