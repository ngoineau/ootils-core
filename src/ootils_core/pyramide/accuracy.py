"""
Pyramide axis D — pure forecast-accuracy metrics and conformal intervals.

This module is the measurement core of the Pyramide governance loop
(docs/DESIGN-pyramide-forecasting.md §2.D): rolling-origin backtesting,
scale-free error metrics (MASE / WAPE / sMAPE / bias) and split-conformal
prediction intervals. It follows the same discipline as
``engine/mrp/core.py``:

- **DB-free and model-free.** Every function operates on in-memory sequences.
  ``evaluate_rolling_origin`` receives the forecast model as an injected
  callable — this module knows no engine, no repository, no network.
- **Deterministic.** No randomness, no float quantile interpolation, no
  wall-clock reads. Same inputs → same outputs, always (ADR-003 discipline,
  applied to the stochastic boundary's *measurement*).
- **Decimal end-to-end.** Series values in the Pyramide layer are
  ``decimal.Decimal`` (see ``pyramide/engines.py``); metrics are computed with
  Decimal arithmetic (default context, 28 significant digits) so results are
  reproducible across platforms and hand-checkable in golden tests.
- **Fail loudly on misuse, sentinel on degenerate data.** Structural errors
  (empty inputs, length mismatch, bad parameters) raise ``ValueError``.
  Mathematically degenerate data (zero denominator) returns ``None`` — never
  a masked division such as ``max(denominator, 1)``, which silently turns a
  scale-free metric into a meaningless absolute one.

Why MASE and not MAPE
---------------------
The Pyramide model catalogue includes Croston, i.e. intermittent demand with
many zero periods is a first-class citizen. MAPE divides each error by the
actual: a single zero actual makes it undefined (or forces the ``max(|a|,1)``
hack, which corrupts the metric exactly on the series where intermittent
models matter). MASE (Hyndman & Koehler 2006) instead scales errors by the
in-sample MAE of the (seasonal) naive method — defined whenever the history
is not perfectly constant, symmetric in over/under-forecast, scale-free, and
directly comparable across items. Degenerate case (constant history → naive
MAE = 0) is reported as ``None``, not an arbitrary number.

The metrics feed model selection, the per-forecast confidence score and —
via :func:`conformal_intervals` — the ``confidence_interval_lower/upper``
columns of the forecast artifact. Wiring into the engines is a later PR;
this module stays pure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Callable, Mapping, Sequence


__all__ = [
    "AccuracyReport",
    "bias",
    "conformal_intervals",
    "evaluate_rolling_origin",
    "interval_coverage",
    "mase",
    "smape",
    "wape",
]


ForecastFn = Callable[[Sequence[Decimal], int], Sequence[Decimal]]
"""Injected forecast model: ``forecast_fn(train, horizon) -> curve``.

Receives the training slice of the series and the number of steps to
forecast; must return a sequence of at least ``horizon`` values. The
accuracy module never imports a model — determinism and testability come
from this inversion."""


@dataclass(frozen=True)
class AccuracyReport:
    """Aggregated result of a rolling-origin backtest.

    Attributes:
        mase: Mean absolute scaled error pooled across cutoffs (each error is
            scaled by *its own cutoff's* in-sample naive MAE, so no future
            data leaks into the denominator). ``None`` when no cutoff had a
            usable denominator (e.g. constant training history).
        wape: sum|actual - forecast| / sum|actual| over all evaluated
            (cutoff, horizon) pairs. ``None`` when total actual demand is
            zero (see :func:`wape`).
        smape: Symmetric MAPE in [0, 2] over all evaluated pairs (see
            :func:`smape` for the both-zero convention).
        bias: Mean signed error ``forecast - actual``. **Positive means
            over-forecast** — the stock-critical direction: a persistently
            positive bias inflates safety stock and purchase plans.
        coverage: Share of actuals falling inside prediction intervals.
            Always ``None`` here: rolling-origin evaluation produces point
            forecasts; coverage only exists once intervals have been
            evaluated (see :func:`interval_coverage`). Kept in the report so
            interval-aware backtests fill the same structure.
        per_horizon_residuals: ``{h: (residuals...)}`` with ``h`` starting at
            1; residual = ``actual - forecast`` (note the sign: residuals are
            actual-minus-forecast per the conformal convention, while
            ``bias`` is forecast-minus-actual so that positive = over-
            forecast). Order within each horizon follows cutoff order —
            deterministic. This is the direct input of
            :func:`conformal_intervals`.
        n_cutoffs: Number of forecast origins evaluated.
        n_observations: Total number of (actual, forecast) pairs evaluated
            across all cutoffs and horizons.
    """

    mase: Decimal | None
    wape: Decimal | None
    smape: Decimal | None
    bias: Decimal
    coverage: Decimal | None
    per_horizon_residuals: dict[int, tuple[Decimal, ...]] = field(default_factory=dict)
    n_cutoffs: int = 0
    n_observations: int = 0


def _check_pair(actuals: Sequence[Decimal], forecasts: Sequence[Decimal]) -> None:
    """Structural validation shared by all pairwise metrics: fail loudly."""
    if len(actuals) == 0:
        raise ValueError("actuals must not be empty")
    if len(actuals) != len(forecasts):
        raise ValueError(
            f"actuals and forecasts must have the same length ({len(actuals)} != {len(forecasts)})"
        )


def mase(
    actuals: Sequence[Decimal],
    forecasts: Sequence[Decimal],
    insample: Sequence[Decimal],
    m: int = 1,
) -> Decimal | None:
    """Mean Absolute Scaled Error (Hyndman & Koehler 2006).

    Definition::

        numerator   = MAE(actuals, forecasts)
        denominator = MAE of the m-step naive forecast on the insample
                    = mean(|insample[t] - insample[t-m]|) for t = m .. len-1
        MASE        = numerator / denominator

    ``m`` is the seasonal period of the scaling naive: ``m=1`` scales by the
    plain "repeat last value" naive; pass the known ``season_length`` (7
    daily, 52 weekly, 12 monthly — cf. ``engines._default_season_length``)
    to scale by the seasonal naive, which is the fair yardstick for a
    seasonal series.

    Interpretation: MASE < 1 → better than the naive method on this history;
    MASE = 1 → as good as naive; > 1 → worse.

    Degenerate cases (documented sentinel, never a raising division):
    - the insample is constant at lag ``m`` (denominator = 0) → ``None``.
      A metric cannot rank models on a series the naive predicts perfectly;
      callers must treat ``None`` as "not comparable", not as 0.
    - the insample is too short to produce a naive error
      (``len(insample) <= m``) → ``None``.

    Raises:
        ValueError: empty ``actuals``, length mismatch, or ``m < 1``.
    """
    _check_pair(actuals, forecasts)
    if m < 1:
        raise ValueError(f"m must be >= 1 (got {m})")
    if len(insample) <= m:
        return None

    naive_errors = [abs(insample[t] - insample[t - m]) for t in range(m, len(insample))]
    denominator = sum(naive_errors, Decimal(0)) / Decimal(len(naive_errors))
    if denominator == 0:
        return None

    numerator = sum(
        (abs(a - f) for a, f in zip(actuals, forecasts)), Decimal(0)
    ) / Decimal(len(actuals))
    return numerator / denominator


def wape(actuals: Sequence[Decimal], forecasts: Sequence[Decimal]) -> Decimal | None:
    """Weighted Absolute Percentage Error: ``sum|a - f| / sum|a|``.

    Volume-weighted, so large-demand periods dominate — the business-facing
    "% of demand missed" number (convertible to currency via ASP outside
    this module).

    Degenerate case: total absolute actual demand of zero → ``None``.
    Deliberately NOT the ``max(sum|a|, 1)`` trap: on an all-zero window that
    hack would report the raw forecast volume as a percentage, which is
    meaningless and scale-dependent. ``None`` = "WAPE is undefined here".

    Raises:
        ValueError: empty ``actuals`` or length mismatch.
    """
    _check_pair(actuals, forecasts)
    total_actual = sum((abs(a) for a in actuals), Decimal(0))
    if total_actual == 0:
        return None
    total_error = sum((abs(a - f) for a, f in zip(actuals, forecasts)), Decimal(0))
    return total_error / total_actual


def smape(actuals: Sequence[Decimal], forecasts: Sequence[Decimal]) -> Decimal:
    """Symmetric MAPE, as a ratio in [0, 2]:

        mean( 2*|a - f| / (|a| + |f|) )

    Convention for the ``|a| + |f| = 0`` pair (both actual and forecast are
    zero): the term contributes **0** — the forecast matched the actual
    exactly, which is a perfect prediction, not an undefined one. This keeps
    sMAPE total (always defined for non-empty input) and rewards models that
    correctly predict zero-demand periods, instead of silently dropping
    those periods from the mean.

    Note the scale: 0 = perfect, 2 = maximal disagreement (one side zero,
    the other non-zero). Multiply by 100 for a percentage display; this
    module returns the raw ratio.

    Raises:
        ValueError: empty ``actuals`` or length mismatch.
    """
    _check_pair(actuals, forecasts)
    total = Decimal(0)
    for a, f in zip(actuals, forecasts):
        denominator = abs(a) + abs(f)
        if denominator == 0:
            continue  # contributes 0 by convention
        total += 2 * abs(a - f) / denominator
    return total / Decimal(len(actuals))


def bias(actuals: Sequence[Decimal], forecasts: Sequence[Decimal]) -> Decimal:
    """Mean signed error: ``mean(forecast - actual)``.

    **Positive = over-forecast** — the stock-critical direction: persistent
    positive bias silently inflates safety stock, purchase plans and working
    capital; persistent negative bias starves them. Unlike the absolute
    metrics, bias is always defined for non-empty input (no denominator).

    Sign convention note: this is the *opposite* sign of the residuals
    stored in :attr:`AccuracyReport.per_horizon_residuals` (which are
    ``actual - forecast``, the standard conformal-calibration convention).

    Raises:
        ValueError: empty ``actuals`` or length mismatch.
    """
    _check_pair(actuals, forecasts)
    return sum(
        ((f - a) for a, f in zip(actuals, forecasts)), Decimal(0)
    ) / Decimal(len(actuals))


def interval_coverage(
    actuals: Sequence[Decimal],
    lowers: Sequence[Decimal],
    uppers: Sequence[Decimal],
) -> Decimal:
    """Empirical coverage: share of actuals with ``lower <= actual <= upper``.

    Bounds are inclusive (an actual exactly on the bound is covered) —
    consistent with :func:`conformal_intervals`, whose offsets are order
    statistics of observed residuals: the calibration residual that produced
    the bound must count as covered.

    Returns a ratio in [0, 1]. The nominal target for intervals built with
    ``conformal_intervals(..., alpha)`` is ``1 - alpha``.

    Raises:
        ValueError: empty ``actuals`` or length mismatch across the three
            sequences.
    """
    if len(actuals) == 0:
        raise ValueError("actuals must not be empty")
    if not (len(actuals) == len(lowers) == len(uppers)):
        raise ValueError(
            "actuals, lowers and uppers must have the same length "
            f"({len(actuals)}, {len(lowers)}, {len(uppers)})"
        )
    covered = sum(1 for a, lo, hi in zip(actuals, lowers, uppers) if lo <= a <= hi)
    return Decimal(covered) / Decimal(len(actuals))


def evaluate_rolling_origin(
    series: Sequence[Decimal],
    forecast_fn: ForecastFn,
    horizon: int,
    min_train: int,
    step: int = 1,
    m: int = 1,
) -> AccuracyReport:
    """Rolling-origin backtest: multi-cutoff, multi-horizon, model-agnostic.

    For each origin ``o`` in ``min_train, min_train + step, ...`` (while
    ``o < len(series)``):

    1. ``train = series[:o]`` — strictly past data, no leakage.
    2. ``window = series[o : o + horizon]`` — the actuals to predict.
       Near the end of the series the window is **partial** (fewer than
       ``horizon`` actuals remain): the cutoff is still evaluated on the
       available steps. This maximizes data usage on short series and
       matches the neighbouring backtest in ``engines._backtest_score``.
    3. ``curve = forecast_fn(train, len(window))`` — the injected model
       produces one value per step; the curve must be at least as long as
       the window (``ValueError`` otherwise — a model returning a short
       curve is a bug, not a data condition).
    4. Each pair yields a residual ``actual - forecast`` recorded under its
       horizon step ``h`` (1-based), in cutoff order.

    Aggregation (all deterministic, all documented at the metric functions):

    - ``wape`` / ``smape`` / ``bias`` are computed on the **pooled** pairs
      across every cutoff and horizon.
    - ``mase`` uses per-cutoff scaling to avoid leakage: each absolute error
      of a cutoff is divided by the m-step naive MAE of **that cutoff's own
      training slice**, then all scaled errors are pooled and averaged
      (Hyndman's scaled-error formulation applied per origin). Cutoffs whose
      training slice yields no usable denominator (constant at lag ``m``, or
      shorter than ``m + 1``) are excluded from the MASE pool only — they
      still count for the other metrics. If no cutoff qualifies,
      ``mase`` is ``None``.
    - ``coverage`` is ``None``: point forecasts carry no intervals.

    Purity contract: ``forecast_fn`` is the only external code invoked; if
    it is deterministic, the whole evaluation is deterministic.

    Raises:
        ValueError: ``horizon < 1``, ``min_train < 1``, ``step < 1``,
            ``m < 1``, or ``min_train >= len(series)`` (no cutoff can be
            formed — an empty report would silently hide a misconfigured
            backtest).
    """
    if horizon < 1:
        raise ValueError(f"horizon must be >= 1 (got {horizon})")
    if min_train < 1:
        raise ValueError(f"min_train must be >= 1 (got {min_train})")
    if step < 1:
        raise ValueError(f"step must be >= 1 (got {step})")
    if m < 1:
        raise ValueError(f"m must be >= 1 (got {m})")
    if min_train >= len(series):
        raise ValueError(
            f"min_train ({min_train}) leaves no cutoff in a series of length {len(series)}"
        )

    pooled_actuals: list[Decimal] = []
    pooled_forecasts: list[Decimal] = []
    scaled_errors: list[Decimal] = []
    residuals_by_horizon: dict[int, list[Decimal]] = {}
    n_cutoffs = 0

    for origin in range(min_train, len(series), step):
        train = series[:origin]
        window = series[origin : origin + horizon]
        curve = forecast_fn(train, len(window))
        if len(curve) < len(window):
            raise ValueError(
                f"forecast_fn returned {len(curve)} values for a {len(window)}-step window "
                f"at origin {origin}"
            )
        n_cutoffs += 1

        # Per-cutoff MASE denominator: m-step naive MAE on this train slice.
        denominator: Decimal | None = None
        if len(train) > m:
            naive_errors = [abs(train[t] - train[t - m]) for t in range(m, len(train))]
            candidate = sum(naive_errors, Decimal(0)) / Decimal(len(naive_errors))
            if candidate != 0:
                denominator = candidate

        for h, (actual, forecast) in enumerate(zip(window, curve), start=1):
            pooled_actuals.append(actual)
            pooled_forecasts.append(forecast)
            residuals_by_horizon.setdefault(h, []).append(actual - forecast)
            if denominator is not None:
                scaled_errors.append(abs(actual - forecast) / denominator)

    mase_value: Decimal | None = None
    if scaled_errors:
        mase_value = sum(scaled_errors, Decimal(0)) / Decimal(len(scaled_errors))

    return AccuracyReport(
        mase=mase_value,
        wape=wape(pooled_actuals, pooled_forecasts),
        smape=smape(pooled_actuals, pooled_forecasts),
        bias=bias(pooled_actuals, pooled_forecasts),
        coverage=None,
        per_horizon_residuals={
            h: tuple(residuals) for h, residuals in sorted(residuals_by_horizon.items())
        },
        n_cutoffs=n_cutoffs,
        n_observations=len(pooled_actuals),
    )


def conformal_intervals(
    per_horizon_residuals: Mapping[int, Sequence[Decimal]],
    alpha: Decimal | str | float,
) -> tuple[dict[int, Decimal], dict[int, Decimal]]:
    """Split-conformal prediction offsets per horizon, from backtest residuals.

    Given calibration residuals ``e = actual - forecast`` for each horizon
    (typically :attr:`AccuracyReport.per_horizon_residuals`), returns
    ``(lower_offsets, upper_offsets)`` such that the interval for a future
    forecast ``f`` at horizon ``h`` is::

        [f + lower_offsets[h],  f + upper_offsets[h]]

    with nominal marginal coverage ``1 - alpha`` (asymmetric two-sided
    split conformal: ``alpha/2`` in each tail, so systematic bias widens
    the correct side instead of both). These offsets are what will feed
    ``confidence_interval_lower/upper`` (migration 026) in a later PR.

    Quantile method — exact and documented
    --------------------------------------
    Pure order statistics ("inverted ECDF"), **no linear interpolation**,
    with the split-conformal finite-sample rank correction on ``n + 1``.
    WARNING for future refactors: this is NOT ``np.quantile`` with
    ``method='lower'``/``'higher'`` — numpy ranks on ``(n - 1)`` while this
    module ranks on ``(n + 1)`` (e.g. n=41, alpha=0.1: k_lo=2 here vs 3
    there). Any numpy rewrite must reimplement the explicit k_lo/k_hi
    formula below, with exact (non-float) rank arithmetic::

        sorted_e = sorted(residuals)          # n values
        k_lo = max(1, floor((n + 1) * alpha / 2))        -> lower = sorted_e[k_lo - 1]
        k_hi = min(n, ceil((n + 1) * (1 - alpha / 2)))   -> upper = sorted_e[k_hi - 1]

    Why this method and not interpolation:

    - **Coverage guarantee.** The ``(n + 1)``-rank with ceiling on the upper
      tail is the standard split-conformal quantile: under exchangeability
      of calibration and future residuals it gives finite-sample marginal
      coverage >= 1 - alpha. Interpolated quantiles carry no such guarantee.
    - **Determinism and exactness.** Order statistics are exact members of
      the input set: no float interpolation, no platform-dependent rounding,
      results stay ``Decimal``. The rank arithmetic itself runs in Decimal
      (``alpha`` is normalized via ``Decimal(str(alpha))``) so binary-float
      artefacts like ``20 * 0.9 == 18.000000000000004`` can never shift a
      rank.
    - **Explainability.** Each bound IS an observed residual — an agent or
      planner can point at the exact historical error that set the bound
      (ADR-004 discipline).

    Small-sample clamp: when ``n < 2/alpha - 1`` the theoretical rank falls
    outside ``1..n``; the ranks are clamped to the extreme order statistics
    (min/max residual) and the finite-sample guarantee degrades to
    best-effort. Callers needing the strict guarantee must supply at least
    ``ceil(2/alpha) - 1`` residuals per horizon (e.g. 19 for alpha = 0.1).

    Horizons with an empty residual sequence are skipped (absent from both
    output dicts) — no residuals means no calibration, and inventing a bound
    would be a silent wrong answer.

    Args:
        per_horizon_residuals: ``{h: residuals}``; residuals are
            ``actual - forecast``.
        alpha: Miscoverage rate in (0, 1), e.g. ``Decimal("0.1")`` for 90 %
            nominal coverage. Floats/strings are normalized with
            ``Decimal(str(alpha))`` for deterministic rank arithmetic.

    Raises:
        ValueError: ``alpha`` outside (0, 1).
    """
    alpha_d = Decimal(str(alpha))
    if not (Decimal(0) < alpha_d < Decimal(1)):
        raise ValueError(f"alpha must be in (0, 1) (got {alpha})")

    lower_offsets: dict[int, Decimal] = {}
    upper_offsets: dict[int, Decimal] = {}
    half = alpha_d / 2
    for h in sorted(per_horizon_residuals):
        residuals = sorted(per_horizon_residuals[h])
        n = len(residuals)
        if n == 0:
            continue
        rank_lo = ((n + 1) * half).to_integral_value(rounding=ROUND_FLOOR)
        rank_hi = ((n + 1) * (1 - half)).to_integral_value(rounding=ROUND_CEILING)
        k_lo = max(1, int(rank_lo))
        k_hi = min(n, int(rank_hi))
        lower_offsets[h] = residuals[k_lo - 1]
        upper_offsets[h] = residuals[k_hi - 1]
    return lower_offsets, upper_offsets
