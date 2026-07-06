"""
Pyramide axis A3 — Forecast Value Added (FVA) over a trivial seasonal-naive.

FVA answers the governance question "is the stat forecast worth its complexity
versus a benchmark nobody would defend deploying?" (#393, ADR-030). The
benchmark is the **seasonal-naive** forecast — repeat the value one season ago,
``y_hat[t] = y[t - season_length]`` — deliberately the most trivial model that
still respects seasonality. If the stat pipeline cannot beat "same period last
season", its complexity is not paying for itself.

    FVA = naive - stat  (POSITIVE = the stat model beats the naive baseline)

Lower WAPE/MASE is better, so removing error yields a positive FVA. A NEGATIVE
FVA is a legitimate, honest result (the stat model lost to the trivial
baseline) and is NEVER clamped — consumers (Decision Ladder) decide what a
non-positive FVA means.

This module follows the same discipline as ``pyramide/accuracy.py`` and
``engine/mrp/core.py``:

- **DB-free, model-free, deterministic.** In-memory Decimal sequences in,
  ``FvaResult`` out. No randomness, no wall-clock, no I/O — golden-testable in
  isolation.
- **None-honest (STRICT).** ``None`` means "not computable on this data",
  NEVER a masked 0. A FVA of ``0.0`` means "stat == naive" (a real, comparable
  result); ``None`` means "not comparable" — the two must never be conflated.

Methodological consistency — the load-bearing invariant
-------------------------------------------------------
FVA is only honest if the naive baseline is scored on the EXACT SAME
rolling-origin backtest as the stat model: same series, same cutoffs, same
horizon, same error normalisation. Comparing a naive backtested on different
windows against the stat metrics would compare apples and oranges.

We guarantee this by REUSING ``accuracy.evaluate_rolling_origin`` — the very
function that produced the stat report — over the SAME series with the SAME
``step=1``/``m=1``, and by RECOVERING the stat's exact origin set from the
report instead of guessing it: with ``step=1`` the stat cutoffs are
``range(min_train_stat, len(series))``, so
``min_train_stat = len(series) - stat_report.n_cutoffs`` (the migration-055
52-origin tail is already folded into that count). Backtesting the naive from
that same origin makes the two span identical cutoffs by construction; a final
``n_cutoffs`` equality check is the safety net, and ``compute_fva`` returns
``None`` (never a mismatched comparison) whenever the seasonal-naive cannot be
aligned there — e.g. the first shared origin holds less than one full season
of training, so there is no value one season ago.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .accuracy import AccuracyReport, evaluate_rolling_origin


logger = logging.getLogger(__name__)


__all__ = [
    "FvaResult",
    "compute_fva",
    "resolve_season_length",
    "seasonal_naive_forecast",
]


# Default seasonal cycle per granularity — the SAME table as
# engines._default_season_length (7 daily / 52 weekly / 12 monthly). Kept in
# sync here (rather than importing a private) so fva.py stays the single FVA
# surface; the run's season is method_params["season_length"] when set, else
# this default — exactly the resolution engines.forecast() applies.
_DEFAULT_SEASON_LENGTH: Mapping[str, int] = {"daily": 7, "weekly": 52, "monthly": 12}


def resolve_season_length(granularity: str, method_params: Mapping[str, Any]) -> int:
    """Seasonal cycle length a Pyramide run actually used: the caller-supplied
    ``method_params["season_length"]`` when present and readable, else the
    granularity default (7/52/12) — the identical resolution
    ``engines.forecast()`` performs via ``params.setdefault(...)``.

    Tolerant on the override (like ``engines._candidate_specs``): an illegible
    value falls back to the granularity default with a debug trace, never an
    exception — the FVA baseline must not crash a run over a bad param.
    """
    raw = method_params.get("season_length")
    if raw is not None:
        try:
            season = int(raw)
        except (TypeError, ValueError):
            logger.debug(
                "season_length illisible (%r); défaut par granularité", raw
            )
        else:
            if season >= 1:
                return season
            logger.debug("season_length < 1 (%r); défaut par granularité", raw)
    return _DEFAULT_SEASON_LENGTH[granularity]


@dataclass(frozen=True)
class FvaResult:
    """Forecast Value Added of the stat model over the seasonal-naive baseline.

    Every field is ``Optional`` and None-honest (migration 068, inherited from
    055): ``None`` = "not computable on this data", never a masked 0.

    - ``naive_wape`` / ``naive_mase``: the seasonal-naive baseline error,
      scored on the SAME rolling-origin backtest as the stat metrics. ``None``
      when the baseline is undefined (history < 1 full season, so no value one
      season ago) OR when the naive backtest could not be aligned to the stat's
      cutoffs (see ``compute_fva``).
    - ``fva_wape`` = ``naive_wape - wape``; ``fva_mase`` = ``naive_mase - mase``
      (POSITIVE = stat beats naive). ``None`` whenever EITHER operand is None
      (naive missing, or the stat wape/mase is itself None per migration 055).
    """

    naive_wape: Decimal | None
    naive_mase: Decimal | None
    fva_wape: Decimal | None
    fva_mase: Decimal | None


def seasonal_naive_forecast(
    history: Sequence[Decimal],
    season_length: int,
    horizon: int,
) -> list[Decimal]:
    """Seasonal-naive forecast: ``y_hat[t] = y[t - season_length]``.

    Repeats the last observed season forward: horizon step ``i`` (0-based)
    takes the value ``season_length`` positions before the corresponding
    future index, i.e. ``history[len(history) - season_length + (i mod
    season_length)]``. This is the textbook seasonal-naive (S-naive), the
    deliberately trivial FVA benchmark — NOT the full SeasonalForecaster
    (level x seasonal indices), which would inflate the "value added".

    Deterministic and clamped to 0 (a negative demand forecast does not
    exist), consistent with every served curve in the Pyramide layer.

    Raises:
        ValueError: ``season_length < 1`` or ``horizon < 1`` (structural
            misuse — fail loudly, like the accuracy module).

    Returns an empty list when the baseline is NOT computable — history
    shorter than one full season, so there is no value "one season ago". The
    caller treats an empty curve as "baseline undefined" (None-honest),
    never as a zero forecast.
    """
    if season_length < 1:
        raise ValueError(f"season_length must be >= 1 (got {season_length})")
    if horizon < 1:
        raise ValueError(f"horizon must be >= 1 (got {horizon})")
    if len(history) < season_length:
        return []

    last_season_start = len(history) - season_length
    return [
        max(history[last_season_start + (step % season_length)], Decimal("0"))
        for step in range(horizon)
    ]


def compute_fva(
    history: Sequence[Decimal],
    season_length: int,
    *,
    stat_report: AccuracyReport,
) -> FvaResult:
    """Forecast Value Added of the stat model over the seasonal-naive baseline.

    ``stat_report`` is the rolling-origin backtest report of the model that
    PRODUCED the run's values (``PyramideRunResult.accuracy_report``). Its
    ``wape`` / ``mase`` are the stat operands (already computed by the run —
    never recomputed here), and its ``n_cutoffs`` is the alignment witness.

    Methodological consistency (the critical point) — the naive is backtested
    by the SAME ``accuracy.evaluate_rolling_origin`` used for the stat report,
    with the SAME ``series`` (``history``), ``horizon`` (derived from the
    report's per-horizon residuals), ``step=1`` and ``m=1``. The stat's exact
    origin set is RECOVERED from the report rather than guessed: with
    ``step=1`` the stat cutoffs are ``range(min_train_stat, len(series))``, so
    ``min_train_stat = len(series) - stat_report.n_cutoffs``. Backtesting the
    naive from that same origin makes the two span identical cutoffs by
    construction. A final ``n_cutoffs`` equality check is kept as a safety net
    (a naive that cannot run on the first shared origin — history shorter than
    one season there — collapses to ``None`` rather than a mismatched
    comparison). The naive metrics (and thus the FVA) are ``None`` whenever
    that alignment does not hold.

    None-honest, all fields:
    - naive metrics ``None`` when the baseline is undefined (history < 1 full
      season, or the report carries no horizon to size the window) or the
      cutoffs cannot be aligned to the stat report;
    - ``fva_*`` ``None`` whenever either operand is ``None`` (naive missing, or
      the stat ``wape``/``mase`` already ``None`` per migration 055 — a FVA is
      "not comparable", never "no value added").

    Determinism: no randomness, no wall-clock; same inputs -> same output.
    """
    if season_length < 1:
        raise ValueError(f"season_length must be >= 1 (got {season_length})")

    naive_wape, naive_mase = _backtest_seasonal_naive(
        history, season_length, stat_report
    )
    return FvaResult(
        naive_wape=naive_wape,
        naive_mase=naive_mase,
        fva_wape=_fva(naive_wape, stat_report.wape),
        fva_mase=_fva(naive_mase, stat_report.mase),
    )


def _backtest_seasonal_naive(
    history: Sequence[Decimal],
    season_length: int,
    stat_report: AccuracyReport,
) -> tuple[Decimal | None, Decimal | None]:
    """Score the seasonal-naive on the stat report's own cutoffs.

    Returns ``(naive_wape, naive_mase)``, either ``None`` when the baseline is
    not computable or its backtest cannot be aligned to the stat cutoffs. MASE
    uses ``m=1`` uniformly, exactly like ``engines._backtest_report`` — the
    scaling naive stays identical across the compared models, so ``naive_mase``
    and ``mase`` are directly subtractable.
    """
    # Horizon of the stat backtest = deepest per-horizon residual bucket. No
    # residuals (or no cutoffs) means the stat report has no rolling-origin
    # structure to mirror (external backend, blend, too-short history) —
    # nothing to align.
    if not stat_report.per_horizon_residuals or stat_report.n_cutoffs < 1:
        return None, None
    horizon = max(stat_report.per_horizon_residuals)

    # Recover the stat's EXACT first origin: with step=1 the stat cutoffs are
    # range(min_train_stat, len), i.e. len - min_train_stat = n_cutoffs. The
    # 52-origin tail of the stat harness (migration 055) is already baked into
    # that count, so the naive inherits the same window without re-deriving it.
    min_train = len(history) - stat_report.n_cutoffs
    if min_train < season_length or min_train >= len(history):
        # The first shared origin has less than one full season of training,
        # so the seasonal-naive has no value one season ago there: the
        # baseline is not computable on the stat's cutoffs.
        return None, None

    def forecast_fn(train: Sequence[Decimal], periods: int) -> Sequence[Decimal]:
        return seasonal_naive_forecast(train, season_length, periods)

    try:
        naive_report = evaluate_rolling_origin(
            series=list(history),
            forecast_fn=forecast_fn,
            horizon=horizon,
            min_train=min_train,
            step=1,
            m=1,
        )
    except ValueError:
        # A structural mismatch (e.g. a train slice shorter than one season
        # yielding an empty curve) means the naive cannot be scored on these
        # cutoffs — refuse rather than emit an unaligned baseline.
        return None, None

    # Apples-to-apples witness: with step=1 on a fixed series the origin set
    # is range(min_train, len). Identical n_cutoffs <=> identical cutoffs.
    # Any divergence would compare the naive on different windows than the
    # stat — None, never a mismatched FVA.
    if naive_report.n_cutoffs != stat_report.n_cutoffs:
        return None, None
    return naive_report.wape, naive_report.mase


def _fva(naive: Decimal | None, stat: Decimal | None) -> Decimal | None:
    """``naive - stat`` (positive = stat beats naive). ``None`` if EITHER
    operand is ``None`` — a FVA is "not comparable", never "no value added".
    The negative result is legitimate and returned as-is (never clamped)."""
    if naive is None or stat is None:
        return None
    return naive - stat
