"""
Pyramide axis B — PR-B1: head/tail series router (spec §5).

Routing = choosing a forecast **method** AND a forecast **level** for one
series. A tail series is usually best forecast at an AGGREGATE node +
MinT/middle-out disaggregation, NOT with a foundation model at the leaf —
FM stays reserved (cold-start + valued tail without signal), which is the
scale wall of docs/DESIGN-pyramide-forecasting.md §5.

Discipline (same contract as ``pyramide/accuracy.py``):

- **DB-free and engine-free.** The router only sees :class:`SeriesFeatures`
  computed by the caller (the annual value units x ASP, the lifecycle tag,
  the twin availability all come from outside — the router knows no
  repository, no network, no engine).
- **Deterministic.** Same features + same thresholds + same metrics ->
  same :class:`RoutingDecision`, always. No randomness, no wall clock.
- **Parameterized, nothing business-coded.** Every classification cutoff
  lives in :class:`RoutingThresholds` (documented defaults, all
  overridable). The ABC *value* cutoffs are explicitly calibration
  parameters: prefer passing ``abc_class`` computed by the caller from a
  Pareto rank over the whole portfolio, which the router cannot see.
- **Data-driven beats hard-coded.** ``metrics_lookup`` (optional) feeds
  aggregated axis-D backtest scores per series CLASS: when it knows a
  winner among the branch's candidate methods, the router prefers it over
  the static default. Without it, the static §5 tree decides.
- **Explainable.** Every decision carries a short auditable ``reason``
  with the numbers that triggered the branch (ADR-004 discipline) — this
  string is what migration 058 persists as ``routing_reason``.

Decision tree implemented (spec §5, top-down, first match wins)::

    cold-start (or lifecycle 'launch')
        with twin              -> TWIN, leaf        (V1: named, wired in B2)
        aggregate has signal   -> AUTO_SELECT, aggregate
        otherwise              -> FM_CHRONOS, leaf  (FM direct leaf)
    intermittent (zero_ratio)  -> CROSTON, leaf
    end_of_life                -> MA short window, leaf (bounded decay
                                  proxy; NEVER extrapolates seasonality)
    head (deep history + A-class + strong season)
                               -> SEASONAL, leaf   (LGBM candidate via metrics)
    tail (C-class / unknown class / sparse history)
        aggregate has signal   -> AUTO_SELECT, aggregate (NOT FM — scale wall)
        otherwise              -> FM_CHRONOS, leaf
    mid (everything left: B-class, or A-class without stable season)
                               -> AUTO_SELECT, leaf

V1 mapping choices (documented, revisit in B2+):

- **TWIN** is a routing-vocabulary method (:data:`METHOD_TWIN`), not yet an
  executable engine method: PR-B1 routes and persists provenance only
  (opt-in), PR-B2 wires execution. Executors must map or reject it.
- **end_of_life**: no 'declining' method exists in the catalogue, so the
  router picks a short-window Moving Average — it tracks the decaying
  recent level, is bounded by construction (average of observed values,
  demand clamped >= 0 downstream) and cannot extrapolate a seasonal peak
  into a dying item. The reason string says so explicitly.
- **intermittent (spec §5 'Croston / TSB')**: only CROSTON exists in the
  catalogue — TSB (Teunter-Syntetos-Babai) is implemented nowhere in the
  codebase, so the branch offers CROSTON alone. Add TSB to the candidates
  here the day it lands in forecasting/algorithms.py (review PR-B1).
- **tail trigger (spec §5 'sparse / C-class / signal faible')**: classify()
  routes to the tail on sparse OR C-class/unknown-class only. A deep-history
  B-class series with a merely-weak signal goes to mid/AUTO_SELECT at leaf
  level — deliberate V1 reading: 'signal faible' without sparsity or
  C-class is left to the backtest (metrics_lookup) rather than a hard
  threshold, so the data decides. Revisit if D3 metrics show B-class leaf
  forecasts losing systematically to aggregate ones (review PR-B1).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Callable, List, Mapping, Sequence, Union

from ootils_core.forecasting import ForecastMethod
from ootils_core.forecasting.algorithms import ForecastingError, SeasonalForecaster

from .models import METHOD_AUTO_SELECT, METHOD_FM_CHRONOS, METHOD_ML_LGBM

__all__ = [
    "CLASS_COLD_START",
    "CLASS_END_OF_LIFE",
    "CLASS_HEAD",
    "CLASS_INTERMITTENT",
    "CLASS_MID",
    "CLASS_TAIL",
    "LEVEL_AGGREGATE",
    "LEVEL_LEAF",
    "LIFECYCLES",
    "METHOD_TWIN",
    "MetricsLookup",
    "RoutingDecision",
    "RoutingError",
    "RoutingThresholds",
    "SeriesFeatures",
    "classify",
    "route",
    "seasonal_strength",
]


LEVEL_LEAF = "leaf"
LEVEL_AGGREGATE = "aggregate"
_LEVELS = frozenset({LEVEL_LEAF, LEVEL_AGGREGATE})

# Routing vocabulary only (see module docstring): the twin-transfer engine
# does not exist yet — B2 wires it. Deliberately NOT in models.SUPPORTED_METHODS
# so an executor that blindly runs a routed method fails loudly, never silently.
METHOD_TWIN = "TWIN"

LIFECYCLE_LAUNCH = "launch"
LIFECYCLE_MATURE = "mature"
LIFECYCLE_END_OF_LIFE = "end_of_life"
LIFECYCLES = frozenset({LIFECYCLE_LAUNCH, LIFECYCLE_MATURE, LIFECYCLE_END_OF_LIFE})

# Series classes — the keys ``metrics_lookup`` is called with. A class is
# the router's own taxonomy (spec §5 axes), NOT a method.
CLASS_COLD_START = "cold_start"
CLASS_INTERMITTENT = "intermittent"
CLASS_END_OF_LIFE = "end_of_life"
CLASS_HEAD = "head"
CLASS_MID = "mid"
CLASS_TAIL = "tail"

_ABC_CLASSES = frozenset({"A", "B", "C"})

MetricsLookup = Callable[[str], "Mapping[str, Any] | None"]
"""Optional axis-D feedback: ``metrics_lookup(series_class)`` returns
``{method: aggregated_backtest_score}`` for that class (e.g. pooled WAPE
from ``pyramide_accuracy_metrics``, LOWER IS BETTER) or ``None``/empty when
no backtest evidence exists. Scores may be Decimal/int/float/str — they are
normalized through ``Decimal(str(score))`` so comparisons stay exact. Only
methods among the branch's candidates are considered (a backtest can never
push a seasonal model onto an end-of-life series, for instance)."""


class RoutingError(ValueError):
    """Invalid routing input (features, thresholds or decision fields)."""


@dataclass(frozen=True)
class RoutingThresholds:
    """Classification cutoffs of the §5 tree — ALL parameterizable.

    Defaults are documented starting points, not business truths; they are
    expected to be calibrated per dataset (and, later, tuned from axis-D
    backtests). Nothing here is hard-coded into :func:`route`.

    Attributes:
        cold_start_days: History strictly shorter than this = cold-start.
            Default 60 (< 2 monthly cycles: nothing stable to fit).
        intermittent_zero_ratio: Share of zero-demand periods strictly
            above this = intermittent (Croston territory). Default 0.6 —
            the value used in the spec's own routing example.
        sparse_history_days: History strictly shorter than this (but past
            cold-start) = sparse -> tail routing. Default 180 (~ half a
            year: too short to separate season from noise at the leaf).
        head_min_history_days: Minimum history for head routing. Default
            365 (a full yearly cycle observed at least once).
        seasonal_strength_min: Minimum :func:`seasonal_strength` (mean
            absolute deviation of the seasonal indices around 1.0) for the
            season to be considered real. Default 0.15 = the average
            period deviates >= 15 % from the base level.
        abc_a_annual_value: ``annual_value`` (units x ASP, computed by the
            caller) at or above which a series is A-class. Default 100000.
        abc_b_annual_value: ``annual_value`` at or above which a series is
            B-class (below = C). Default 10000. Both ABC cutoffs are pure
            calibration knobs — when the caller can rank the whole
            portfolio (Pareto), pass ``abc_class`` directly instead.
        eol_ma_window: Window (periods) of the short Moving Average used
            as the V1 bounded-decay proxy for end-of-life series.
            Default 4: short enough to follow the decline of the recent
            level, long enough not to chase single-period noise.
    """

    cold_start_days: int = 60
    intermittent_zero_ratio: Decimal = Decimal("0.6")
    sparse_history_days: int = 180
    head_min_history_days: int = 365
    seasonal_strength_min: Decimal = Decimal("0.15")
    abc_a_annual_value: Decimal = Decimal("100000")
    abc_b_annual_value: Decimal = Decimal("10000")
    eol_ma_window: int = 4

    def __post_init__(self) -> None:
        if self.cold_start_days < 1:
            raise RoutingError(f"cold_start_days must be >= 1 (got {self.cold_start_days})")
        if self.sparse_history_days < self.cold_start_days:
            raise RoutingError(
                f"sparse_history_days ({self.sparse_history_days}) must be >= "
                f"cold_start_days ({self.cold_start_days})"
            )
        if self.head_min_history_days < self.sparse_history_days:
            raise RoutingError(
                f"head_min_history_days ({self.head_min_history_days}) must be >= "
                f"sparse_history_days ({self.sparse_history_days})"
            )
        if not (Decimal(0) <= self.intermittent_zero_ratio <= Decimal(1)):
            raise RoutingError(
                f"intermittent_zero_ratio must be in [0, 1] (got {self.intermittent_zero_ratio})"
            )
        if self.seasonal_strength_min < 0:
            raise RoutingError(
                f"seasonal_strength_min must be >= 0 (got {self.seasonal_strength_min})"
            )
        if self.abc_b_annual_value > self.abc_a_annual_value:
            raise RoutingError(
                f"abc_b_annual_value ({self.abc_b_annual_value}) must be <= "
                f"abc_a_annual_value ({self.abc_a_annual_value})"
            )
        if self.eol_ma_window < 1:
            raise RoutingError(f"eol_ma_window must be >= 1 (got {self.eol_ma_window})")


@dataclass(frozen=True)
class SeriesFeatures:
    """Classification features of ONE series — computed by the CALLER.

    The router is DB-free: everything below is an input, nothing is looked
    up. In particular ``annual_value`` (units x ASP), ``lifecycle``,
    ``has_twin`` and ``aggregate_signal_ok`` encode knowledge only the
    caller has (pricing, item master, hierarchy registry).

    Attributes:
        history_depth_days: Days spanned by the demand history.
        zero_ratio: Share of zero-demand periods in the history, in [0, 1]
            (intermittence axis).
        abc_class: 'A' | 'B' | 'C' when the caller ranked the portfolio
            (preferred), else None and ``annual_value`` is used against the
            thresholds' cutoffs. Both None = class unknown -> the router
            stays conservative and routes tail (cheap aggregate path).
        annual_value: Annualized value (units x ASP) computed by the
            caller. Only consulted when ``abc_class`` is None.
        seasonal_strength: Output of :func:`seasonal_strength` (or an
            equivalent caller-side measure): mean absolute deviation of
            the seasonal indices around 1.0. None = unknown/not computable
            (e.g. < 2 full cycles) — treated as "no proven season".
        lifecycle: 'launch' | 'mature' | 'end_of_life' | None (unknown).
            Provided by the caller (item master); 'launch' routes like
            cold-start even when some history exists (e.g. recoded item).
        has_twin: A twin/predecessor item with usable history exists.
        aggregate_signal_ok: The series' parent aggregate node has enough
            history/signal to be forecast instead of the leaf.
    """

    history_depth_days: int
    zero_ratio: Decimal
    abc_class: str | None = None
    annual_value: Decimal | None = None
    seasonal_strength: Decimal | None = None
    lifecycle: str | None = None
    has_twin: bool = False
    aggregate_signal_ok: bool = False

    def __post_init__(self) -> None:
        if self.history_depth_days < 0:
            raise RoutingError(
                f"history_depth_days must be >= 0 (got {self.history_depth_days})"
            )
        if not (Decimal(0) <= self.zero_ratio <= Decimal(1)):
            raise RoutingError(f"zero_ratio must be in [0, 1] (got {self.zero_ratio})")
        if self.abc_class is not None and self.abc_class not in _ABC_CLASSES:
            raise RoutingError(
                f"abc_class must be one of {sorted(_ABC_CLASSES)} or None (got {self.abc_class!r})"
            )
        if self.annual_value is not None and self.annual_value < 0:
            raise RoutingError(f"annual_value must be >= 0 (got {self.annual_value})")
        if self.seasonal_strength is not None and self.seasonal_strength < 0:
            raise RoutingError(
                f"seasonal_strength must be >= 0 (got {self.seasonal_strength})"
            )
        if self.lifecycle is not None and self.lifecycle not in LIFECYCLES:
            raise RoutingError(
                f"lifecycle must be one of {sorted(LIFECYCLES)} or None (got {self.lifecycle!r})"
            )


@dataclass(frozen=True)
class RoutingDecision:
    """The router's verdict for one series: method + level + why.

    ``reason`` is the short auditable sentence persisted as
    ``pyramide_runs.routing_reason`` (migration 058); ``features_used``
    keeps the feature values the branch actually consulted (in-memory
    explainability — not persisted, the reason string carries the numbers
    that matter).
    """

    method: str
    level: str
    reason: str
    features_used: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.method:
            raise RoutingError("method must be a non-empty string")
        if self.level not in _LEVELS:
            raise RoutingError(
                f"level must be one of {sorted(_LEVELS)} (got {self.level!r})"
            )
        if not self.reason:
            raise RoutingError("reason must be a non-empty string")
        object.__setattr__(self, "features_used", MappingProxyType(dict(self.features_used)))


def seasonal_strength(
    history: Sequence[Union[Decimal, float, int]], season_length: int
) -> Decimal | None:
    """Seasonal signal strength, REUSING the SeasonalForecaster's indices.

    Definition: mean absolute deviation of the fitted seasonal indices
    around 1.0 (indices average 1.0 by construction, see
    ``SeasonalForecaster._fit``). 0 = perfectly flat profile; 0.15 = the
    average period deviates 15 % from the base level. This is the value
    :attr:`SeriesFeatures.seasonal_strength` expects and that
    :attr:`RoutingThresholds.seasonal_strength_min` is compared against.

    Deliberately a thin helper over ``SeasonalForecaster.seasonal_indices``
    (forecasting/algorithms.py) — no duplicated decomposition code: the
    strength the router sees is derived from exactly the model the head
    branch would run.

    Returns None (unknown, treated as "no proven season") when the history
    covers fewer than 2 full cycles — with a single cycle, indices and
    noise are indistinguishable (same rule as the forecaster itself).

    Raises:
        ValueError: season_length < 2 (propagated from SeasonalForecaster).
    """
    try:
        indices = SeasonalForecaster(season_length).seasonal_indices(list(history))
    except ForecastingError:
        return None
    total = sum((abs(index - Decimal(1)) for index in indices), Decimal(0))
    return total / Decimal(len(indices))


def _abc_class(features: SeriesFeatures, thresholds: RoutingThresholds) -> str | None:
    """Resolve the ABC class: explicit class > annual-value cutoffs > None."""
    if features.abc_class is not None:
        return features.abc_class
    if features.annual_value is None:
        return None
    if features.annual_value >= thresholds.abc_a_annual_value:
        return "A"
    if features.annual_value >= thresholds.abc_b_annual_value:
        return "B"
    return "C"


def classify(
    features: SeriesFeatures, thresholds: RoutingThresholds | None = None
) -> str:
    """Series class per the §5 axes (top-down, first match wins).

    This is the key ``metrics_lookup`` is called with — exposed so callers
    can pre-aggregate axis-D metrics by the exact same taxonomy.
    """
    thresholds = thresholds or RoutingThresholds()
    if (
        features.history_depth_days < thresholds.cold_start_days
        or features.lifecycle == LIFECYCLE_LAUNCH
    ):
        return CLASS_COLD_START
    if features.zero_ratio > thresholds.intermittent_zero_ratio:
        return CLASS_INTERMITTENT
    if features.lifecycle == LIFECYCLE_END_OF_LIFE:
        return CLASS_END_OF_LIFE
    abc = _abc_class(features, thresholds)
    if (
        abc == "A"
        and features.history_depth_days >= thresholds.head_min_history_days
        and features.seasonal_strength is not None
        and features.seasonal_strength >= thresholds.seasonal_strength_min
    ):
        return CLASS_HEAD
    if (
        abc in (None, "C")
        or features.history_depth_days < thresholds.sparse_history_days
    ):
        return CLASS_TAIL
    return CLASS_MID


def route(
    features: SeriesFeatures,
    *,
    thresholds: RoutingThresholds | None = None,
    metrics_lookup: MetricsLookup | None = None,
) -> RoutingDecision:
    """Route ONE series to a (method, level) with an auditable reason.

    Pure function of its arguments — deterministic by construction (the
    only external code invoked is ``metrics_lookup``; if it is
    deterministic, the routing is).

    ``metrics_lookup(series_class)`` may override the static method with
    the class's backtest winner, but ONLY among the branch's candidate
    methods and NEVER the level: the level choice (leaf vs aggregate) is
    the scale/coherence decision of the tree, not a per-method score.
    """
    thresholds = thresholds or RoutingThresholds()
    series_class = classify(features, thresholds)
    abc = _abc_class(features, thresholds)

    candidates: tuple[str, ...]
    if series_class == CLASS_COLD_START:
        trigger = (
            "lifecycle 'launch'"
            if features.lifecycle == LIFECYCLE_LAUNCH
            else (
                f"history {features.history_depth_days}d < "
                f"{thresholds.cold_start_days}d"
            )
        )
        if features.has_twin:
            candidates = (METHOD_TWIN, METHOD_FM_CHRONOS)
            level = LEVEL_LEAF
            reason = (
                f"cold-start series ({trigger}) with twin available: "
                "twin-transfer at leaf + MinT reconciliation"
            )
        elif features.aggregate_signal_ok:
            candidates = (METHOD_AUTO_SELECT,)
            level = LEVEL_AGGREGATE
            reason = (
                f"cold-start series ({trigger}), no twin, aggregate has "
                "signal: forecast at aggregate + MinT disaggregation"
            )
        else:
            candidates = (METHOD_FM_CHRONOS,)
            level = LEVEL_LEAF
            reason = (
                f"cold-start series ({trigger}), no twin, no aggregate "
                "signal: zero-shot FM direct at leaf"
            )
        features_used = {
            "history_depth_days": features.history_depth_days,
            "lifecycle": features.lifecycle,
            "has_twin": features.has_twin,
            "aggregate_signal_ok": features.aggregate_signal_ok,
        }
    elif series_class == CLASS_INTERMITTENT:
        candidates = (ForecastMethod.CROSTON,)
        level = LEVEL_LEAF
        reason = (
            f"intermittent demand (zero_ratio {features.zero_ratio} > "
            f"{thresholds.intermittent_zero_ratio}): Croston at leaf"
        )
        features_used = {"zero_ratio": features.zero_ratio}
    elif series_class == CLASS_END_OF_LIFE:
        # V1 documented choice: no 'declining' method exists in the
        # catalogue, so the bounded-decay proxy is a SHORT-window moving
        # average — it follows the falling recent level and cannot
        # extrapolate a seasonal peak into a dying item. Candidates
        # deliberately exclude every seasonal-capable method.
        candidates = (ForecastMethod.MA, ForecastMethod.EXP_SMOOTHING)
        level = LEVEL_LEAF
        reason = (
            "end-of-life series: bounded decay via short-window MA "
            f"(window {thresholds.eol_ma_window}), never seasonal "
            "extrapolation (V1: no dedicated 'declining' method)"
        )
        features_used = {
            "lifecycle": features.lifecycle,
            "seasonal_strength": features.seasonal_strength,
        }
    elif series_class == CLASS_HEAD:
        # Spec: "stat saisonnier ou LGBM+exogène" — the LGBM alternative is
        # reachable only through backtest evidence (metrics_lookup).
        candidates = (ForecastMethod.SEASONAL, METHOD_ML_LGBM)
        level = LEVEL_LEAF
        reason = (
            f"head series (history {features.history_depth_days}d >= "
            f"{thresholds.head_min_history_days}d, {abc}-class, "
            f"seasonal_strength {features.seasonal_strength} >= "
            f"{thresholds.seasonal_strength_min}): seasonal stat at leaf + MinT"
        )
        features_used = {
            "history_depth_days": features.history_depth_days,
            "abc_class": abc,
            "seasonal_strength": features.seasonal_strength,
        }
    elif series_class == CLASS_TAIL:
        if abc in (None, "C"):
            trigger = f"{abc or 'unknown'}-class"
            if features.zero_ratio > 0:
                trigger += f" (zero_ratio {features.zero_ratio})"
        else:
            trigger = (
                f"sparse history {features.history_depth_days}d < "
                f"{thresholds.sparse_history_days}d ({abc}-class)"
            )
        if features.aggregate_signal_ok:
            # The scale wall: tail series are NOT sent to a leaf FM when a
            # signal-bearing aggregate exists — FM stays reserved.
            candidates = (METHOD_AUTO_SELECT,)
            level = LEVEL_AGGREGATE
            reason = (
                f"tail series ({trigger}): forecast at aggregate + MinT "
                "disaggregation"
            )
        else:
            candidates = (METHOD_FM_CHRONOS,)
            level = LEVEL_LEAF
            reason = (
                f"tail series ({trigger}), aggregate lacks signal: "
                "zero-shot FM direct at leaf"
            )
        features_used = {
            "history_depth_days": features.history_depth_days,
            "abc_class": abc,
            "zero_ratio": features.zero_ratio,
            "aggregate_signal_ok": features.aggregate_signal_ok,
        }
    else:  # CLASS_MID
        candidates = (METHOD_AUTO_SELECT,)
        level = LEVEL_LEAF
        if abc == "A":
            qualifier = (
                "A-class without proven seasonal signal "
                f"(seasonal_strength {features.seasonal_strength} < "
                f"{thresholds.seasonal_strength_min} or unknown, or history "
                f"{features.history_depth_days}d < {thresholds.head_min_history_days}d)"
            )
        else:
            qualifier = f"{abc}-class"
        reason = f"mid series ({qualifier}): AUTO_SELECT stat at leaf + MinT"
        features_used = {
            "history_depth_days": features.history_depth_days,
            "abc_class": abc,
            "seasonal_strength": features.seasonal_strength,
        }

    method = candidates[0]
    if metrics_lookup is not None:
        winner = _backtest_winner(metrics_lookup, series_class, candidates)
        if winner is not None and winner[0] != method:
            method = winner[0]
            reason += (
                f"; backtest override: {method} wins class '{series_class}' "
                f"(score {winner[1]})"
            )

    features_used["series_class"] = series_class
    return RoutingDecision(
        method=method, level=level, reason=reason, features_used=features_used
    )


def _backtest_winner(
    metrics_lookup: MetricsLookup,
    series_class: str,
    candidates: Sequence[str],
) -> tuple[str, Decimal] | None:
    """Best candidate per aggregated axis-D scores (lower is better).

    None when the lookup has no usable score for any candidate — the
    static tree default then stands. Ties break on the method name
    (alphabetical) so the choice stays deterministic.
    """
    metrics = metrics_lookup(series_class)
    if not metrics:
        return None
    scored: List[tuple[Decimal, str]] = []
    for candidate in candidates:
        score = metrics.get(candidate)
        if score is None:
            continue
        scored.append((Decimal(str(score)), candidate))
    if not scored:
        return None
    best_score, best_method = min(scored)
    return best_method, best_score
