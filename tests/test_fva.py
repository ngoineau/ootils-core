"""
Golden unit tests for the PURE Forecast Value Added core (pyramide/fva.py,
#393 A3-PR3, ADR-030). DB-free: in-memory Decimal series in, ``FvaResult``
out. The real persistence / round-trip / migration-068 wiring lives in
tests/integration/test_fva_integration.py.

The load-bearing invariants under test:

- ``seasonal_naive_forecast`` = the deliberately trivial baseline
  ``y_hat[t] = y[t - season]`` (wrap by season, clamp <0, ``[]`` below one
  season, ValueError on structural misuse).
- FVA = naive - stat, POSITIVE = the stat model beats the trivial baseline.
  A NEGATIVE FVA is legitimate and NEVER clamped (proven with a strict
  ``< 0`` assertion).
- **None-honest, STRICT.** ``None`` = "not computable on this data"; ``0.0``
  = "stat == naive" (a real, comparable result). The two are asserted
  distinct — the single most important property of this module.
- Methodological consistency: the naive is backtested on the SAME
  rolling-origin cutoffs as the stat report (``naive.n_cutoffs ==
  stat.n_cutoffs`` witness).

Every expected number below is hand-derived; ``test_naive_backtest_exact_by_hand``
pins a fully hand-worked case to literals so the naive math is proven
independently of the implementation.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from ootils_core.pyramide.accuracy import (
    AccuracyReport,
    evaluate_rolling_origin,
)
from ootils_core.pyramide.fva import (
    FvaResult,
    compute_fva,
    resolve_season_length,
    seasonal_naive_forecast,
)

D = Decimal


def _stat_report(
    *,
    wape: Decimal | None,
    mase: Decimal | None,
    n_cutoffs: int,
    horizon: int,
) -> AccuracyReport:
    """A stat AccuracyReport shaped like a real rolling-origin backtest:
    ``n_cutoffs`` origins, per-horizon residual buckets 1..horizon (their
    VALUES are irrelevant to compute_fva — only the KEYS size the naive
    window and n_cutoffs is the alignment witness). wape/mase are the stat
    operands compute_fva subtracts from the naive."""
    return AccuracyReport(
        mase=mase,
        wape=wape,
        smape=D("0.1"),
        bias=D("0"),
        coverage=None,
        per_horizon_residuals={
            h: tuple(D("0") for _ in range(n_cutoffs)) for h in range(1, horizon + 1)
        },
        n_cutoffs=n_cutoffs,
        n_observations=n_cutoffs * horizon,
    )


# ---------------------------------------------------------------------------
# 1. seasonal_naive_forecast — the trivial baseline curve
# ---------------------------------------------------------------------------


def test_seasonal_naive_repeats_last_season_exactly():
    # Two full seasons; the forecast for one horizon-season is the last
    # observed season verbatim: y_hat[i] = history[len - season + i].
    history = [D(1), D(2), D(3), D(4), D(5), D(6)]
    assert seasonal_naive_forecast(history, 3, 3) == [D(4), D(5), D(6)]


def test_seasonal_naive_wraps_modulo_season_beyond_one_cycle():
    # Horizon longer than a season wraps: [4,5,6, 4,5,6, 4] for horizon 7.
    history = [D(1), D(2), D(3), D(4), D(5), D(6)]
    assert seasonal_naive_forecast(history, 3, 7) == [
        D(4), D(5), D(6), D(4), D(5), D(6), D(4),
    ]


def test_seasonal_naive_clamps_negative_to_zero():
    # A negative demand forecast does not exist: last season [-5, 2, -1]
    # clamps to [0, 2, 0]. The clamp is per-step, not the whole curve.
    history = [D(-5), D(2), D(-1)]
    assert seasonal_naive_forecast(history, 3, 3) == [D(0), D(2), D(0)]


def test_seasonal_naive_below_one_season_returns_empty():
    # < one full season of history => no value "one season ago" => []
    # (the caller reads this as "baseline undefined", None-honest — never a
    # zero forecast).
    assert seasonal_naive_forecast([D(1), D(2)], 3, 4) == []


def test_seasonal_naive_rejects_structural_misuse():
    history = [D(1), D(2), D(3)]
    with pytest.raises(ValueError):
        seasonal_naive_forecast(history, 0, 3)  # season < 1
    with pytest.raises(ValueError):
        seasonal_naive_forecast(history, 2, 0)  # horizon < 1


# ---------------------------------------------------------------------------
# 2. compute_fva — the sign of the value added (positive/negative, no clamp)
# ---------------------------------------------------------------------------


def test_fva_positive_when_stat_beats_naive_on_a_trend():
    # Linear-trend series: the seasonal-naive carries a big error (it repeats
    # a stale season), so a stat model with a much lower WAPE ADDS value:
    # fva_wape = naive_wape - stat_wape > 0.
    series = [D(x) for x in (10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32)]
    stat = _stat_report(wape=D("0.01"), mase=D("0.1"), n_cutoffs=4, horizon=2)

    result = compute_fva(series, 3, stat_report=stat)

    assert result.naive_wape is not None
    assert result.fva_wape is not None
    assert result.fva_wape > 0  # stat removed error => value added
    assert result.fva_wape == result.naive_wape - D("0.01")
    assert result.fva_mase == result.naive_mase - D("0.1")


def test_fva_strictly_negative_when_naive_wins_never_clamped():
    # Same series, but a DELIBERATELY worse stat: wape/mase above the naive.
    # The FVA must be STRICTLY < 0 — proving the module never clamps a
    # negative value added to 0 (the load-bearing "honest negative" rule).
    series = [D(x) for x in (10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32)]
    baseline = compute_fva(
        series, 3, stat_report=_stat_report(
            wape=D("0"), mase=D("0"), n_cutoffs=4, horizon=2,
        ),
    )
    assert baseline.naive_wape is not None and baseline.naive_mase is not None

    worse = _stat_report(
        wape=baseline.naive_wape + D("0.5"),
        mase=baseline.naive_mase + D("1"),
        n_cutoffs=4,
        horizon=2,
    )
    result = compute_fva(series, 3, stat_report=worse)

    assert result.fva_wape == D("-0.5")
    assert result.fva_mase == D("-1")
    assert result.fva_wape < 0  # not clamped to 0
    assert result.fva_mase < 0


# ---------------------------------------------------------------------------
# 3. compute_fva — None-honest, STRICT (None vs 0.0). The core contract.
# ---------------------------------------------------------------------------


def test_fva_none_when_history_shorter_than_one_season_at_first_origin():
    # season=5 but the stat's first shared origin (min_train = L - n_cutoffs
    # = 8 - 5 = 3) holds < 1 full season: no value one season ago there, so
    # the naive is undefined => naive AND fva None (not a fabricated 0).
    series = [D(x) for x in (3, 1, 4, 1, 5, 9, 2, 6)]
    stat = _stat_report(wape=D("0.2"), mase=D("1.0"), n_cutoffs=5, horizon=2)

    result = compute_fva(series, 5, stat_report=stat)

    assert result.naive_wape is None
    assert result.naive_mase is None
    assert result.fva_wape is None
    assert result.fva_mase is None


def test_fva_none_when_stat_operand_none_but_naive_stays_computable():
    # The distinctive None-honest case: the stat WAPE is None (migration 055
    # — e.g. an all-zero pooled window), yet the naive baseline IS computable.
    # fva_wape must be None (one operand missing) WITHOUT poisoning the naive
    # side: naive_wape is a real number, and the MASE axis (stat mase given)
    # still yields a real fva_mase. None and "missing operand" stay local.
    series = [D(x) for x in range(10, 34, 2)]  # 12 points, 10..32
    stat = _stat_report(wape=None, mase=D("0.5"), n_cutoffs=4, horizon=2)

    result = compute_fva(series, 3, stat_report=stat)

    assert result.naive_wape is not None  # baseline computed regardless
    assert result.fva_wape is None  # but no comparison without a stat wape
    assert result.naive_mase is not None
    assert result.fva_mase is not None  # the mase axis is unaffected
    assert result.fva_mase == result.naive_mase - D("0.5")


def test_fva_zero_is_distinct_from_none_when_stat_equals_naive():
    # stat == naive exactly => fva == 0.0 (a real, comparable "no value
    # added"), which must be DISTINCT from None ("not comparable"). This is
    # the pair the whole module refuses to conflate.
    series = [D(x) for x in range(10, 34, 2)]

    def naive_fn(train, periods):
        return seasonal_naive_forecast(train, 3, periods)

    # Recover the exact naive metrics on the stat's cutoffs (min_train =
    # 12 - 4 = 8) and hand them back AS the stat metrics.
    naive_ref = evaluate_rolling_origin(
        series=series, forecast_fn=naive_fn, horizon=2, min_train=8, step=1, m=1,
    )
    stat = _stat_report(
        wape=naive_ref.wape, mase=naive_ref.mase, n_cutoffs=4, horizon=2,
    )

    result = compute_fva(series, 3, stat_report=stat)

    assert result.fva_wape == 0  # stat == naive, a real 0
    assert result.fva_wape is not None  # explicitly NOT None
    assert result.fva_mase == 0
    assert result.fva_mase is not None


def test_fva_none_when_report_carries_no_rolling_origin_structure():
    # A report with no per-horizon residuals / no cutoffs (external backend,
    # blend, too-short history) has nothing for the naive to mirror: naive
    # and fva collapse to None rather than guessing a window.
    series = [D(x) for x in range(20)]
    empty = AccuracyReport(
        mase=D("1"), wape=D("0.2"), smape=D("0.1"), bias=D("0"), coverage=None,
        per_horizon_residuals={}, n_cutoffs=0, n_observations=0,
    )

    result = compute_fva(series, 3, stat_report=empty)

    assert result == FvaResult(
        naive_wape=None, naive_mase=None, fva_wape=None, fva_mase=None,
    )


def test_compute_fva_rejects_bad_season_length():
    stat = _stat_report(wape=D("0.1"), mase=D("0.1"), n_cutoffs=1, horizon=1)
    with pytest.raises(ValueError):
        compute_fva([D(1), D(2)], 0, stat_report=stat)


# ---------------------------------------------------------------------------
# 3b. A fully hand-worked golden: the naive backtest math pinned to literals
# ---------------------------------------------------------------------------


def test_naive_backtest_exact_by_hand():
    # season=2, series=[10,20,12,24,14,28], stat n_cutoffs=2 => min_train=4,
    # horizon=1. Naive backtest, step=1:
    #   origin 4: train=[10,20,12,24] -> snf=train[2]=12 ; actual=14 ; |14-12|=2
    #   origin 5: train=[10,20,12,24,14] -> snf=train[3]=24 ; actual=28 ; |28-24|=4
    # naive_wape = (2+4) / (14+28) = 6/42 = 1/7.
    # MASE (m=1, per-cutoff 1-step naive MAE):
    #   origin 4 |20-10|,|12-20|,|24-12| = 10,8,12 -> mean 10 -> scaled 2/10=0.2
    #   origin 5 add |14-24|=10 -> mean 40/4=10 -> scaled 4/10=0.4
    #   naive_mase = (0.2+0.4)/2 = 0.3
    series = [D(10), D(20), D(12), D(24), D(14), D(28)]
    stat = _stat_report(wape=D("0.05"), mase=D("0.1"), n_cutoffs=2, horizon=1)

    result = compute_fva(series, 2, stat_report=stat)

    assert result.naive_wape == D(6) / D(42)
    assert result.naive_mase == D("0.3")
    assert result.fva_wape == D(6) / D(42) - D("0.05")
    assert result.fva_mase == D("0.3") - D("0.1")


# ---------------------------------------------------------------------------
# 4. Methodological alignment — naive on the SAME cutoffs as the stat
# ---------------------------------------------------------------------------


def test_naive_aligned_to_stat_cutoffs_on_long_series():
    # A long series (>= 52 + season) with a real stat backtest: compute_fva
    # recovers the stat's exact origin (min_train = L - n_cutoffs) and scores
    # the naive there. The apples-to-apples witness — identical n_cutoffs —
    # is verified against an independent naive backtest at the same origin,
    # and fva_wape reduces exactly to naive_wape - stat_wape.
    season = 7
    length = 52 + season + 10
    pattern = (20, 22, 25, 30, 28, 15, 12)
    series = [D(pattern[i % 7] + i // 7) for i in range(length)]

    def stat_fn(train, periods):
        return [train[-1]] * periods  # plain last-value naive as the "stat"

    stat_report = evaluate_rolling_origin(
        series=series, forecast_fn=stat_fn, horizon=3, min_train=30, step=1, m=1,
    )

    result = compute_fva(series, season, stat_report=stat_report)

    assert result.naive_wape is not None
    assert result.naive_mase is not None
    assert result.fva_wape is not None
    assert result.fva_mase is not None

    # Independent witness: re-backtest the naive at the same recovered origin.
    def naive_fn(train, periods):
        return seasonal_naive_forecast(train, season, periods)

    naive_ref = evaluate_rolling_origin(
        series=series, forecast_fn=naive_fn, horizon=3,
        min_train=length - stat_report.n_cutoffs, step=1, m=1,
    )
    assert naive_ref.n_cutoffs == stat_report.n_cutoffs  # aligned by construction
    assert result.naive_wape == naive_ref.wape
    assert result.fva_wape == naive_ref.wape - stat_report.wape
    assert result.fva_mase == naive_ref.mase - stat_report.mase


# ---------------------------------------------------------------------------
# 5. resolve_season_length — granularity defaults + tolerant override
# ---------------------------------------------------------------------------


def test_resolve_season_length_defaults_per_granularity():
    assert resolve_season_length("daily", {}) == 7
    assert resolve_season_length("weekly", {}) == 52
    assert resolve_season_length("monthly", {}) == 12


def test_resolve_season_length_honours_valid_override():
    assert resolve_season_length("daily", {"season_length": 4}) == 4
    assert resolve_season_length("weekly", {"season_length": "9"}) == 9  # int-coercible


def test_resolve_season_length_tolerant_on_illegible_override():
    # Illegible / out-of-range overrides fall back to the granularity
    # default WITHOUT raising — a bad param must never crash a run.
    assert resolve_season_length("monthly", {"season_length": "oops"}) == 12
    assert resolve_season_length("daily", {"season_length": None}) == 7
    assert resolve_season_length("weekly", {"season_length": 0}) == 52
    assert resolve_season_length("daily", {"season_length": -3}) == 7


# ---------------------------------------------------------------------------
# 6. FvaResult is a frozen value object
# ---------------------------------------------------------------------------


def test_fva_result_is_frozen():
    result = FvaResult(
        naive_wape=D("0.2"), naive_mase=D("1"),
        fva_wape=D("0.05"), fva_mase=D("0.1"),
    )
    with pytest.raises(Exception):
        result.fva_wape = D("0")  # type: ignore[misc]
