"""
Golden tests for the Pyramide head/tail router (axis B, PR-B1).

Pure, DB-free (routing.py contract): every branch of the §5 decision
tree (docs/DESIGN-pyramide-forecasting.md) is exercised with hand-written
features and a hand-written expected decision — including the scale-wall
case (tail series -> AGGREGATE, never leaf FM, when the aggregate has
signal). Also covered: parameterizable thresholds, the metrics_lookup
backtest override (data-driven beats hard-coded, but never outside the
branch's candidates and never the level), determinism, the
seasonal_strength helper reusing SeasonalForecaster, and input
validation.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from ootils_core.forecasting import ForecastMethod
from ootils_core.pyramide.models import (
    METHOD_AUTO_SELECT,
    METHOD_FM_CHRONOS,
    METHOD_ML_LGBM,
)
from ootils_core.pyramide.routing import (
    CLASS_COLD_START,
    CLASS_END_OF_LIFE,
    CLASS_HEAD,
    CLASS_INTERMITTENT,
    CLASS_MID,
    CLASS_TAIL,
    LEVEL_AGGREGATE,
    LEVEL_LEAF,
    METHOD_TWIN,
    RoutingDecision,
    RoutingError,
    RoutingThresholds,
    SeriesFeatures,
    classify,
    route,
    seasonal_strength,
)


D = Decimal


def _features(**overrides) -> SeriesFeatures:
    """A mature, mid-of-the-road B-class series; tests override one axis."""
    base = dict(
        history_depth_days=400,
        zero_ratio=D("0.1"),
        abc_class="B",
        annual_value=None,
        seasonal_strength=D("0.05"),
        lifecycle="mature",
        has_twin=False,
        aggregate_signal_ok=True,
    )
    base.update(overrides)
    return SeriesFeatures(**base)


# ---------------------------------------------------------------------------
# §5 tree — one golden decision per branch
# ---------------------------------------------------------------------------


class TestColdStartBranch:
    def test_cold_start_with_twin_routes_twin_at_leaf(self):
        decision = route(_features(history_depth_days=20, has_twin=True))
        assert decision.method == METHOD_TWIN
        assert decision.level == LEVEL_LEAF
        assert "cold-start" in decision.reason
        assert "twin" in decision.reason
        assert decision.features_used["series_class"] == CLASS_COLD_START

    def test_cold_start_no_twin_with_aggregate_signal_routes_aggregate(self):
        decision = route(
            _features(history_depth_days=20, has_twin=False, aggregate_signal_ok=True)
        )
        assert decision.method == METHOD_AUTO_SELECT
        assert decision.level == LEVEL_AGGREGATE
        assert "aggregate" in decision.reason

    def test_cold_start_no_twin_no_aggregate_routes_fm_leaf(self):
        # The ONLY cold-start path that spends FM: new series, no twin,
        # no usable aggregate (spec §5 insight).
        decision = route(
            _features(history_depth_days=20, has_twin=False, aggregate_signal_ok=False)
        )
        assert decision.method == METHOD_FM_CHRONOS
        assert decision.level == LEVEL_LEAF
        assert "FM" in decision.reason

    def test_launch_lifecycle_routes_cold_start_despite_history(self):
        decision = route(
            _features(history_depth_days=400, lifecycle="launch", has_twin=True)
        )
        assert decision.method == METHOD_TWIN
        assert decision.features_used["series_class"] == CLASS_COLD_START
        assert "launch" in decision.reason

    def test_reason_carries_the_numbers(self):
        decision = route(
            _features(history_depth_days=20, has_twin=False, aggregate_signal_ok=True)
        )
        assert "20d" in decision.reason
        assert "60d" in decision.reason  # default cold_start_days


class TestIntermittentBranch:
    def test_intermittent_routes_croston_at_leaf(self):
        decision = route(_features(zero_ratio=D("0.84")))
        assert decision.method == ForecastMethod.CROSTON
        assert decision.level == LEVEL_LEAF
        assert "0.84" in decision.reason
        assert "0.6" in decision.reason  # the threshold, auditable
        assert decision.features_used["series_class"] == CLASS_INTERMITTENT

    def test_zero_ratio_at_threshold_is_not_intermittent(self):
        # Strict inequality: exactly at the cutoff stays on the ABC path.
        decision = route(_features(zero_ratio=D("0.6")))
        assert decision.features_used["series_class"] != CLASS_INTERMITTENT

    def test_intermittent_wins_over_end_of_life(self):
        # Croston is flat by design — it already does not extrapolate a
        # season, and it is the right model for sporadic demand.
        decision = route(_features(zero_ratio=D("0.9"), lifecycle="end_of_life"))
        assert decision.method == ForecastMethod.CROSTON


class TestEndOfLifeBranch:
    def test_end_of_life_routes_short_ma_never_seasonal(self):
        # Strong measured seasonality must NOT be extrapolated on a dying
        # item (§5: "décroissance bornée, NE PAS extrapoler la saison").
        decision = route(
            _features(lifecycle="end_of_life", abc_class="A", seasonal_strength=D("0.9"))
        )
        assert decision.method == ForecastMethod.MA
        assert decision.level == LEVEL_LEAF
        assert decision.features_used["series_class"] == CLASS_END_OF_LIFE
        # The V1 mapping (no dedicated 'declining' method) is explicit.
        assert "declining" in decision.reason
        assert "seasonal" in decision.reason.lower()

    def test_end_of_life_reason_documents_window(self):
        decision = route(_features(lifecycle="end_of_life"))
        assert "window 4" in decision.reason  # default eol_ma_window


class TestHeadBranch:
    def test_head_rich_a_class_seasonal_routes_seasonal_leaf(self):
        decision = route(
            _features(
                history_depth_days=730,
                abc_class="A",
                seasonal_strength=D("0.4"),
            )
        )
        assert decision.method == ForecastMethod.SEASONAL
        assert decision.level == LEVEL_LEAF
        assert decision.features_used["series_class"] == CLASS_HEAD
        assert "A-class" in decision.reason

    def test_a_class_weak_season_falls_to_mid(self):
        decision = route(
            _features(history_depth_days=730, abc_class="A", seasonal_strength=D("0.02"))
        )
        assert decision.method == METHOD_AUTO_SELECT
        assert decision.level == LEVEL_LEAF
        assert decision.features_used["series_class"] == CLASS_MID

    def test_a_class_unknown_season_falls_to_mid(self):
        decision = route(
            _features(history_depth_days=730, abc_class="A", seasonal_strength=None)
        )
        assert decision.features_used["series_class"] == CLASS_MID

    def test_a_class_history_below_head_min_is_not_head(self):
        decision = route(
            _features(history_depth_days=200, abc_class="A", seasonal_strength=D("0.4"))
        )
        assert decision.features_used["series_class"] != CLASS_HEAD


class TestMidBranch:
    def test_b_class_routes_auto_select_leaf(self):
        decision = route(_features(abc_class="B"))
        assert decision.method == METHOD_AUTO_SELECT
        assert decision.level == LEVEL_LEAF
        assert decision.features_used["series_class"] == CLASS_MID
        assert "B-class" in decision.reason


class TestTailBranch:
    def test_c_class_routes_aggregate_not_fm(self):
        # THE scale wall: a tail series with a signal-bearing aggregate is
        # forecast at the AGGREGATE — never sent to a leaf FM.
        decision = route(
            _features(abc_class="C", zero_ratio=D("0.5"), aggregate_signal_ok=True)
        )
        assert decision.level == LEVEL_AGGREGATE
        assert decision.method == METHOD_AUTO_SELECT
        assert decision.method != METHOD_FM_CHRONOS
        assert "aggregate" in decision.reason
        assert "disaggregation" in decision.reason
        assert decision.features_used["series_class"] == CLASS_TAIL

    def test_c_class_without_aggregate_signal_routes_fm_leaf(self):
        # "FM si l'agrégat manque de signal" — the reserved FM spend.
        decision = route(_features(abc_class="C", aggregate_signal_ok=False))
        assert decision.method == METHOD_FM_CHRONOS
        assert decision.level == LEVEL_LEAF
        assert "lacks signal" in decision.reason

    def test_sparse_history_b_class_routes_tail(self):
        # tail = sparse OR C-class OR weak signal (spec: an OR, not an AND).
        decision = route(_features(history_depth_days=100, abc_class="B"))
        assert decision.level == LEVEL_AGGREGATE
        assert decision.features_used["series_class"] == CLASS_TAIL
        assert "100d" in decision.reason

    def test_unknown_class_routes_tail_conservatively(self):
        decision = route(_features(abc_class=None, annual_value=None))
        assert decision.features_used["series_class"] == CLASS_TAIL
        assert "unknown-class" in decision.reason


# ---------------------------------------------------------------------------
# ABC via annual_value (units x ASP computed by the CALLER)
# ---------------------------------------------------------------------------


class TestAbcFromAnnualValue:
    def test_annual_value_above_a_cutoff_is_a_class(self):
        decision = route(
            _features(
                abc_class=None,
                annual_value=D("250000"),
                history_depth_days=730,
                seasonal_strength=D("0.4"),
            )
        )
        assert decision.features_used["series_class"] == CLASS_HEAD
        assert decision.features_used["abc_class"] == "A"

    def test_annual_value_between_cutoffs_is_b_class(self):
        decision = route(_features(abc_class=None, annual_value=D("50000")))
        assert decision.features_used["series_class"] == CLASS_MID

    def test_annual_value_below_b_cutoff_is_c_class(self):
        decision = route(_features(abc_class=None, annual_value=D("500")))
        assert decision.features_used["series_class"] == CLASS_TAIL

    def test_explicit_abc_class_wins_over_annual_value(self):
        decision = route(_features(abc_class="C", annual_value=D("999999")))
        assert decision.features_used["series_class"] == CLASS_TAIL


# ---------------------------------------------------------------------------
# Parameterizable thresholds — nothing business-coded
# ---------------------------------------------------------------------------


class TestThresholds:
    def test_custom_intermittent_cutoff_flips_the_decision(self):
        features = _features(zero_ratio=D("0.4"))
        assert route(features).method == METHOD_AUTO_SELECT  # default 0.6
        strict = RoutingThresholds(intermittent_zero_ratio=D("0.3"))
        assert route(features, thresholds=strict).method == ForecastMethod.CROSTON

    def test_custom_cold_start_cutoff_flips_the_decision(self):
        features = _features(history_depth_days=70, has_twin=True)
        assert route(features).features_used["series_class"] == CLASS_TAIL  # sparse
        wide = RoutingThresholds(cold_start_days=90, sparse_history_days=90)
        assert (
            route(features, thresholds=wide).features_used["series_class"]
            == CLASS_COLD_START
        )

    def test_custom_abc_cutoffs_flip_the_class(self):
        features = _features(abc_class=None, annual_value=D("50000"))
        assert route(features).features_used["series_class"] == CLASS_MID
        elitist = RoutingThresholds(
            abc_a_annual_value=D("40000"), abc_b_annual_value=D("20000")
        )
        # A-class under the new cutoffs, but weak season -> mid, not head.
        decision = route(features, thresholds=elitist)
        assert decision.features_used["abc_class"] == "A"
        assert decision.features_used["series_class"] == CLASS_MID

    def test_invalid_thresholds_fail_loudly(self):
        with pytest.raises(RoutingError):
            RoutingThresholds(cold_start_days=0)
        with pytest.raises(RoutingError):
            RoutingThresholds(sparse_history_days=10, cold_start_days=60)
        with pytest.raises(RoutingError):
            RoutingThresholds(intermittent_zero_ratio=D("1.5"))
        with pytest.raises(RoutingError):
            RoutingThresholds(abc_a_annual_value=D("10"), abc_b_annual_value=D("20"))
        with pytest.raises(RoutingError):
            RoutingThresholds(eol_ma_window=0)


# ---------------------------------------------------------------------------
# metrics_lookup — data-driven beats hard-coded (within candidates)
# ---------------------------------------------------------------------------


class TestMetricsLookup:
    HEAD_FEATURES = dict(
        history_depth_days=730, abc_class="A", seasonal_strength=D("0.4")
    )

    def test_lookup_prefers_the_backtest_winner(self):
        # Axis-D says LGBM beats the seasonal stat on head series.
        def lookup(series_class):
            assert series_class == CLASS_HEAD
            return {
                ForecastMethod.SEASONAL: D("0.35"),
                METHOD_ML_LGBM: D("0.21"),
            }

        decision = route(_features(**self.HEAD_FEATURES), metrics_lookup=lookup)
        assert decision.method == METHOD_ML_LGBM
        assert "backtest override" in decision.reason
        assert "0.21" in decision.reason

    def test_lookup_keeps_static_default_when_it_wins(self):
        decision = route(
            _features(**self.HEAD_FEATURES),
            metrics_lookup=lambda _: {
                ForecastMethod.SEASONAL: D("0.20"),
                METHOD_ML_LGBM: D("0.30"),
            },
        )
        assert decision.method == ForecastMethod.SEASONAL
        assert "backtest override" not in decision.reason

    def test_lookup_never_overrides_outside_candidates(self):
        # A (bogus) great seasonal score cannot push seasonality onto an
        # end-of-life series: candidates exclude it by construction.
        decision = route(
            _features(lifecycle="end_of_life"),
            metrics_lookup=lambda _: {ForecastMethod.SEASONAL: D("0.01")},
        )
        assert decision.method == ForecastMethod.MA

    def test_lookup_never_changes_the_level(self):
        decision = route(
            _features(abc_class="C", aggregate_signal_ok=True),
            metrics_lookup=lambda _: {METHOD_AUTO_SELECT: D("0.5")},
        )
        assert decision.level == LEVEL_AGGREGATE

    def test_lookup_returning_none_or_empty_falls_back_to_static(self):
        for lookup in (lambda _: None, lambda _: {}):
            decision = route(_features(**self.HEAD_FEATURES), metrics_lookup=lookup)
            assert decision.method == ForecastMethod.SEASONAL

    def test_lookup_tie_breaks_deterministically_on_method_name(self):
        decision = route(
            _features(**self.HEAD_FEATURES),
            metrics_lookup=lambda _: {
                ForecastMethod.SEASONAL: D("0.25"),
                METHOD_ML_LGBM: D("0.25"),
            },
        )
        # 'ML_LGBM' < 'SEASONAL' alphabetically — stable, documented.
        assert decision.method == METHOD_ML_LGBM


# ---------------------------------------------------------------------------
# Determinism + validation
# ---------------------------------------------------------------------------


class TestDeterminismAndValidation:
    def test_same_features_same_decision(self):
        features = _features(abc_class="C", zero_ratio=D("0.5"))
        first = route(features)
        for _ in range(5):
            again = route(features)
            assert again.method == first.method
            assert again.level == first.level
            assert again.reason == first.reason
            assert dict(again.features_used) == dict(first.features_used)

    def test_classify_matches_route_series_class(self):
        for features in (
            _features(history_depth_days=10),
            _features(zero_ratio=D("0.9")),
            _features(lifecycle="end_of_life"),
            _features(history_depth_days=730, abc_class="A", seasonal_strength=D("0.4")),
            _features(abc_class="B"),
            _features(abc_class="C"),
        ):
            assert classify(features) == route(features).features_used["series_class"]

    def test_invalid_features_fail_loudly(self):
        with pytest.raises(RoutingError):
            _features(zero_ratio=D("1.2"))
        with pytest.raises(RoutingError):
            _features(history_depth_days=-1)
        with pytest.raises(RoutingError):
            _features(abc_class="D")
        with pytest.raises(RoutingError):
            _features(lifecycle="dead")
        with pytest.raises(RoutingError):
            _features(annual_value=D("-5"))

    def test_routing_decision_validates_level_and_reason(self):
        with pytest.raises(RoutingError):
            RoutingDecision(method="MA", level="middle", reason="x")
        with pytest.raises(RoutingError):
            RoutingDecision(method="MA", level="leaf", reason="")
        with pytest.raises(RoutingError):
            RoutingDecision(method="", level="leaf", reason="x")

    def test_features_used_is_read_only(self):
        decision = route(_features())
        with pytest.raises(TypeError):
            decision.features_used["series_class"] = "hacked"


# ---------------------------------------------------------------------------
# seasonal_strength — reuses SeasonalForecaster, no duplicated decomposition
# ---------------------------------------------------------------------------


class TestSeasonalStrength:
    def test_flat_series_has_zero_strength(self):
        assert seasonal_strength([D("100")] * 28, season_length=7) == D("0")

    def test_seasonal_series_has_positive_strength(self):
        # Weekly pattern: one strong day per cycle.
        cycle = [D("200"), D("50"), D("100"), D("50")]
        strength = seasonal_strength(cycle * 4, season_length=4)
        assert strength is not None
        assert strength > D("0.4")  # indices 2.0/0.5/1.0/0.5 -> MAD 0.5

    def test_exact_golden_value(self):
        # indices = [2.0, 0.5, 1.0, 0.5]; mean |index - 1| = (1+0.5+0+0.5)/4
        cycle = [D("200"), D("50"), D("100"), D("50")]
        assert seasonal_strength(cycle * 2, season_length=4) == D("0.5")

    def test_short_history_returns_none(self):
        # < 2 full cycles: unknown, never an invented strength.
        assert seasonal_strength([D("10")] * 10, season_length=7) is None

    def test_invalid_season_length_raises(self):
        with pytest.raises(ValueError):
            seasonal_strength([D("1")] * 10, season_length=1)
