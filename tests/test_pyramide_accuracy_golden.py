"""
Golden-master for the Pyramide accuracy core (src/ootils_core/pyramide/accuracy.py).

Same philosophy as test_mrp_core_golden.py: every expected value is derived BY
HAND in the docstring/comments of the test that asserts it, so any change to
the metric math fails CI with a visible arithmetic disagreement instead of
silently re-ranking forecast models or shifting confidence intervals.

The module is pure (Decimal sequences in, Decimal out, forecast model injected
as a callable) so this runs with no database and no model dependency.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from ootils_core.pyramide.accuracy import (
    AccuracyReport,
    bias,
    conformal_intervals,
    evaluate_rolling_origin,
    interval_coverage,
    mase,
    smape,
    wape,
)

D = Decimal


def dseq(*values) -> list[Decimal]:
    """Build a Decimal list from ints/strings (never raw floats)."""
    return [D(str(v)) for v in values]


def naive_last(train, horizon):
    """Deterministic injected model: repeat the last training value."""
    return [train[-1]] * horizon


# ───────────────────────── MASE ─────────────────────────


def test_mase_m1_golden():
    """Hand calculation (the spec's canonical example):

    insample = [10, 12, 14, 16]
    naive m=1 errors: |12-10|, |14-12|, |16-14| = 2, 2, 2  -> denominator MAE = 2
    actuals  = [20, 21], forecasts = [19, 18]
    errors: |20-19|=1, |21-18|=3                            -> numerator MAE = 2
    MASE = 2 / 2 = 1.0
    """
    result = mase(dseq(20, 21), dseq(19, 18), insample=dseq(10, 12, 14, 16), m=1)
    assert result == D(1)


def test_mase_seasonal_m2_golden():
    """Seasonal scaling, m=2:

    insample = [10, 20, 12, 22, 14, 24]
    naive m=2 errors: |12-10|, |22-20|, |14-12|, |24-22| = 2,2,2,2 -> MAE = 2
    actuals  = [16, 26], forecasts = [15, 22]
    errors: 1, 4 -> numerator MAE = 2.5
    MASE = 2.5 / 2 = 1.25
    """
    result = mase(dseq(16, 26), dseq(15, 22), insample=dseq(10, 20, 12, 22, 14, 24), m=2)
    assert result == D("1.25")


def test_mase_constant_insample_is_none():
    """Constant history: naive errors all 0 -> denominator 0 -> sentinel None,
    never a raised division (documented degenerate case)."""
    assert mase(dseq(5, 6), dseq(5, 5), insample=dseq(5, 5, 5, 5), m=1) is None


def test_mase_insample_too_short_is_none():
    """len(insample) <= m leaves no naive error to average -> None."""
    assert mase(dseq(5), dseq(5), insample=dseq(7), m=1) is None
    assert mase(dseq(5), dseq(5), insample=dseq(7, 8), m=2) is None


def test_mase_structural_errors_raise():
    with pytest.raises(ValueError):
        mase([], [], insample=dseq(1, 2))
    with pytest.raises(ValueError):
        mase(dseq(1, 2), dseq(1), insample=dseq(1, 2))
    with pytest.raises(ValueError):
        mase(dseq(1), dseq(1), insample=dseq(1, 2), m=0)


# ───────────────────────── WAPE / sMAPE / bias ─────────────────────────


def test_wape_golden():
    """actuals [10, 0, 5], forecasts [12, 1, 4]:
    errors 2, 1, 1 -> sum 4 ; sum|a| = 15 ; WAPE = 4/15."""
    assert wape(dseq(10, 0, 5), dseq(12, 1, 4)) == D(4) / D(15)


def test_wape_zero_total_demand_is_none():
    """All-zero actuals: WAPE undefined -> None (documented; NOT max(|a|,1))."""
    assert wape(dseq(0, 0, 0), dseq(3, 0, 1)) is None


def test_smape_golden():
    """actuals [10, 0, 0], forecasts [10, 5, 0]:
    pair 1: |a|+|f| = 20, error 0        -> term 0
    pair 2: 2*5 / (0+5)                  -> term 2   (maximal disagreement)
    pair 3: both zero -> perfect zero    -> term 0   (documented convention)
    sMAPE = (0 + 2 + 0) / 3 = 2/3."""
    assert smape(dseq(10, 0, 0), dseq(10, 5, 0)) == D(2) / D(3)


def test_bias_signs_golden():
    """bias = mean(forecast - actual); POSITIVE = over-forecast (stock-critical).

    over : actuals [10, 10], forecasts [12, 14] -> (2 + 4)/2 = +3
    under: actuals [10, 10], forecasts [8, 9]   -> (-2 - 1)/2 = -1.5
    """
    assert bias(dseq(10, 10), dseq(12, 14)) == D(3)
    assert bias(dseq(10, 10), dseq(8, 9)) == D("-1.5")


def test_pairwise_structural_errors_raise():
    for fn in (wape, smape, bias):
        with pytest.raises(ValueError):
            fn([], [])
        with pytest.raises(ValueError):
            fn(dseq(1, 2), dseq(1))


# ───────────────────────── rolling-origin backtest ─────────────────────────


def test_evaluate_rolling_origin_golden():
    """Fully hand-derived report.

    series = [10, 12, 14, 16, 18, 20], forecast_fn = naive last value,
    horizon = 2, min_train = 3, step = 1, m = 1.

    origin 3: train [10,12,14]        window [16,18] curve [14,14]
              residuals h1: 16-14=2, h2: 18-14=4
              denom = MAE(|12-10|,|14-12|) = 2 ; scaled errors 2/2=1, 4/2=2
    origin 4: train [10,12,14,16]     window [18,20] curve [16,16]
              residuals h1: 2, h2: 4 ; denom = 2 ; scaled 1, 2
    origin 5: train [10,12,14,16,18]  window [20] (PARTIAL) curve [18]
              residual h1: 2 ; denom = 2 ; scaled 1

    Pooled pairs: actuals [16,18,18,20,20], forecasts [14,14,16,16,18]
      n_cutoffs = 3, n_observations = 5
      MASE  = mean(1,2,1,2,1) = 7/5 = 1.4
      WAPE  = (2+4+2+4+2) / (16+18+18+20+20) = 14/92
      bias  = mean(-2,-4,-2,-4,-2) = -14/5 = -2.8   (under-forecast: negative)
      sMAPE = (4/30 + 8/32 + 4/34 + 8/36 + 4/38) / 5
      residuals: h1 -> (2,2,2), h2 -> (4,4) ; coverage = None (point forecasts)
    """
    report = evaluate_rolling_origin(
        dseq(10, 12, 14, 16, 18, 20), naive_last, horizon=2, min_train=3, step=1, m=1
    )
    assert isinstance(report, AccuracyReport)
    assert report.n_cutoffs == 3
    assert report.n_observations == 5
    assert report.mase == D("1.4")
    assert report.wape == D(14) / D(92)
    assert report.bias == D("-2.8")
    assert report.smape == (D(4) / D(30) + D(8) / D(32) + D(4) / D(34) + D(8) / D(36) + D(4) / D(38)) / D(5)
    assert report.coverage is None
    assert report.per_horizon_residuals == {1: (D(2), D(2), D(2)), 2: (D(4), D(4))}


def test_evaluate_rolling_origin_constant_train_excluded_from_mase_only():
    """series [5, 5, 5, 8], min_train=2, horizon=1, naive model.

    origin 2: train [5,5]   -> naive MAE 0 -> cutoff excluded from MASE pool
              window [5], curve [5], residual 0
    origin 3: train [5,5,5] -> naive MAE 0 -> excluded too
              window [8], curve [5], residual 3

    No cutoff qualifies -> mase None ; the other metrics still use both pairs:
      WAPE = (0+3)/(5+8) = 3/13 ; bias = (0 + (5-8))/2 = -1.5.
    """
    report = evaluate_rolling_origin(dseq(5, 5, 5, 8), naive_last, horizon=1, min_train=2)
    assert report.mase is None
    assert report.wape == D(3) / D(13)
    assert report.bias == D("-1.5")
    assert report.n_cutoffs == 2
    assert report.per_horizon_residuals == {1: (D(0), D(3))}


def test_evaluate_rolling_origin_structural_errors():
    series = dseq(1, 2, 3)
    with pytest.raises(ValueError):
        evaluate_rolling_origin(series, naive_last, horizon=0, min_train=1)
    with pytest.raises(ValueError):
        evaluate_rolling_origin(series, naive_last, horizon=1, min_train=0)
    with pytest.raises(ValueError):
        evaluate_rolling_origin(series, naive_last, horizon=1, min_train=1, step=0)
    with pytest.raises(ValueError):
        evaluate_rolling_origin(series, naive_last, horizon=1, min_train=1, m=0)
    with pytest.raises(ValueError):  # min_train >= len(series): no cutoff
        evaluate_rolling_origin(series, naive_last, horizon=1, min_train=3)
    with pytest.raises(ValueError):  # model returning a short curve is a bug
        evaluate_rolling_origin(series, lambda train, h: [], horizon=1, min_train=1)


# ───────────────────────── conformal intervals ─────────────────────────


def test_conformal_intervals_20_residuals_golden():
    """20 known residuals, alpha = 0.1, documented rank method:

    residuals = -10, -9, ..., 9  (already sorted, n = 20)
    k_lo = max(1, floor(21 * 0.05))  = max(1, floor(1.05))  = 1  -> sorted[0]  = -10
    k_hi = min(20, ceil(21 * 0.95))  = min(20, ceil(19.95)) = 20 -> sorted[19] = 9
    """
    residuals = dseq(*range(-10, 10))
    lower, upper = conformal_intervals({1: residuals}, alpha=D("0.1"))
    assert lower == {1: D(-10)}
    assert upper == {1: D(9)}

    # Coverage check on the golden data itself: with a point forecast f = 0,
    # the interval is [-10, 9]; every calibration residual -10..9 lies inside
    # (bounds inclusive) -> empirical coverage = 20/20 = 1 >= 1 - alpha = 0.9.
    zeros = dseq(*([0] * len(residuals)))
    lowers = [z + lower[1] for z in zeros]
    uppers = [z + upper[1] for z in zeros]
    assert interval_coverage(residuals, lowers, uppers) == D(1)


def test_conformal_intervals_interior_ranks_golden():
    """Interior (non-extreme) order statistics, n = 39, alpha = 0.1:

    residuals = 1..39 sorted
    k_lo = max(1, floor(40 * 0.05))  = 2  -> sorted[1]  = 2
    k_hi = min(39, ceil(40 * 0.95))  = 38 -> sorted[37] = 38

    Coverage on the calibration data: residuals 2..38 inside [2, 38]
    inclusive -> 37/39 >= 0.9 (finite-sample guarantee holds: n >= 19).
    """
    residuals = dseq(*range(1, 40))
    lower, upper = conformal_intervals({1: residuals}, alpha=D("0.1"))
    assert lower == {1: D(2)}
    assert upper == {1: D(38)}
    covered = interval_coverage(
        residuals, [lower[1]] * 39, [upper[1]] * 39
    )
    assert covered == D(37) / D(39)
    assert covered >= D("0.9")


def test_conformal_intervals_multi_horizon_and_empty_horizon_skipped():
    """Horizons are calibrated independently; an empty horizon is absent from
    the output (no residuals = no calibration, never an invented bound).

    h2: residuals (4, 4), alpha 0.5 -> k_lo = floor(3*0.25) = 0 -> clamped to 1
        k_hi = ceil(3*0.75) = 3 -> clamped to min(2, 3) = 2 -> both bounds = 4.
    """
    lower, upper = conformal_intervals({1: [], 2: dseq(4, 4)}, alpha=D("0.5"))
    assert 1 not in lower and 1 not in upper
    assert lower == {2: D(4)}
    assert upper == {2: D(4)}


def test_conformal_intervals_alpha_normalization_and_validation():
    """Float alpha is normalized via Decimal(str(alpha)) -> identical ranks to
    Decimal('0.1'); out-of-range alpha raises."""
    residuals = {1: dseq(*range(-10, 10))}
    assert conformal_intervals(residuals, alpha=0.1) == conformal_intervals(
        residuals, alpha=D("0.1")
    )
    for bad in (D(0), D(1), D("-0.2"), D("1.5")):
        with pytest.raises(ValueError):
            conformal_intervals(residuals, alpha=bad)


def test_interval_coverage_golden():
    """actuals [1, 5, 10] vs intervals [0,2], [5,6], [11,12]:
    covered: 1 yes (0<=1<=2), 5 yes (bound inclusive), 10 no -> 2/3."""
    assert interval_coverage(
        dseq(1, 5, 10), dseq(0, 5, 11), dseq(2, 6, 12)
    ) == D(2) / D(3)
    with pytest.raises(ValueError):
        interval_coverage([], [], [])
    with pytest.raises(ValueError):
        interval_coverage(dseq(1), dseq(1, 2), dseq(1))


# ───────────────────────── determinism ─────────────────────────


def test_determinism_two_calls_identical():
    """Two identical invocations produce field-for-field identical results —
    the reproducibility contract agents rely on (ADR-003 discipline)."""
    series = dseq(10, 12, 14, 16, 18, 20)
    r1 = evaluate_rolling_origin(series, naive_last, horizon=2, min_train=3)
    r2 = evaluate_rolling_origin(series, naive_last, horizon=2, min_train=3)
    assert r1 == r2
    c1 = conformal_intervals(r1.per_horizon_residuals, alpha=D("0.5"))
    c2 = conformal_intervals(r2.per_horizon_residuals, alpha=D("0.5"))
    assert c1 == c2
