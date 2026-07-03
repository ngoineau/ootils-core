"""
Golden-master for the SEASONAL forecasting path (Pyramide V1 axe A — PR1).

The default forecast path used to emit a REPEATED VALUE; SEASONAL now emits a
CURVE (level x seasonal indices). This file locks the math on tiny
hand-computed series so any future change that deviates from the documented
arithmetic fails CI instead of silently reshaping a demand plan
(pattern: tests/test_mrp_core_golden.py).

Reference dataset (season_length=4, two full cycles):

    history          = [200, 50, 100, 50, 200, 50, 100, 50]

    grand mean       = 800 / 8 = 100
    position means   = [200, 50, 100, 50]
    seasonal indices = [2.0, 0.5, 1.0, 0.5]   (mean exactly 1.0)
    deseasonalized   = [100] * 8              (perfectly periodic)
    level            = 100
    next period      = cycle position 0 -> 100 * 2.0 = 200
    6-period curve   = [200, 50, 100, 50, 200, 50]

All algorithms are pure Python and deterministic — no DB, no mocks.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from ootils_core.forecasting import (
    ForecastingEngine,
    ForecastingError,
    ForecastMethod,
    SeasonalForecaster,
    create_forecaster,
)


HISTORY = [200, 50, 100, 50, 200, 50, 100, 50]
INDICES = [Decimal("2"), Decimal("0.5"), Decimal("1"), Decimal("0.5")]
CURVE_6 = [Decimal("200"), Decimal("50"), Decimal("100"), Decimal("50"), Decimal("200"), Decimal("50")]


# ───────────────────────── SeasonalForecaster (pure algorithm) ─────────────────────────


def test_indices_recovered_exactly():
    """Indices on the reference dataset are exactly [2.0, 0.5, 1.0, 0.5]."""
    forecaster = SeasonalForecaster(season_length=4)
    assert forecaster.seasonal_indices(HISTORY) == INDICES


def test_forecast_curve_is_level_times_indices():
    """Projected curve = level (100) x index of each future position."""
    forecaster = SeasonalForecaster(season_length=4)
    assert forecaster.forecast_curve(HISTORY, periods=6) == CURVE_6
    # forecast() = first point of the curve
    assert forecaster.forecast(HISTORY) == Decimal("200")


def test_noisy_cycles_average_out():
    """Two noisy cycles whose position means still land on the known indices.

    history        = [190, 55, 95, 60, 210, 45, 105, 40]
    grand mean     = 800 / 8 = 100
    position means = [(190+210)/2, (55+45)/2, (95+105)/2, (60+40)/2]
                   = [200, 50, 100, 50] -> indices [2.0, 0.5, 1.0, 0.5]
    level          = mean of LAST cycle deseasonalized
                   = mean([210/2.0, 45/0.5, 105/1.0, 40/0.5])
                   = mean([105, 90, 105, 80]) = 95
    curve(4)       = [95*2.0, 95*0.5, 95*1.0, 95*0.5] = [190, 47.5, 95, 47.5]
    """
    history = [190, 55, 95, 60, 210, 45, 105, 40]
    forecaster = SeasonalForecaster(season_length=4)

    assert forecaster.seasonal_indices(history) == INDICES
    assert forecaster.forecast_curve(history, periods=4) == [
        Decimal("190"),
        Decimal("47.5"),
        Decimal("95"),
        Decimal("47.5"),
    ]


def test_end_alignment_with_partial_leading_cycle():
    """A partial leading cycle is dropped: only the last k complete cycles
    (aligned to the END of the history) drive indices and phase — the 9-point
    series [50] + reference history forecasts exactly like the reference."""
    history = [50, *HISTORY]
    forecaster = SeasonalForecaster(season_length=4)

    assert forecaster.seasonal_indices(history) == INDICES
    assert forecaster.forecast_curve(history, periods=6) == CURVE_6


def test_constant_series_yields_unit_indices():
    """Constant series -> all indices exactly 1.0, forecast = the constant."""
    forecaster = SeasonalForecaster(season_length=4)
    history = [100] * 8

    assert forecaster.seasonal_indices(history) == [Decimal("1")] * 4
    assert forecaster.forecast_curve(history, periods=5) == [Decimal("100")] * 5


def test_all_zero_series_forecasts_zero():
    """All-zero series: neutral indices (no exploitable profile), level 0."""
    forecaster = SeasonalForecaster(season_length=4)
    history = [0] * 8

    assert forecaster.seasonal_indices(history) == [Decimal("1")] * 4
    assert forecaster.forecast_curve(history, periods=3) == [Decimal("0")] * 3


def test_less_than_two_cycles_raises():
    """< 2 complete cycles: indices would be noise — the algorithm refuses
    (the flat fallback is the ENGINE's responsibility, with provenance)."""
    forecaster = SeasonalForecaster(season_length=4)

    with pytest.raises(ForecastingError):
        forecaster.forecast(HISTORY[:7])


def test_season_length_validation():
    with pytest.raises(ValueError):
        SeasonalForecaster(season_length=1)
    with pytest.raises(ValueError):
        SeasonalForecaster(season_length=0)


def test_factory_creates_seasonal_forecaster():
    forecaster = create_forecaster("seasonal", season_length=12)
    assert isinstance(forecaster, SeasonalForecaster)
    assert forecaster.season_length == 12

    # No default cycle length: the caller must choose one.
    with pytest.raises(ValueError):
        create_forecaster("seasonal")


# ───────────────────────── ForecastingEngine SEASONAL path ─────────────────────────


def test_engine_seasonal_generate_carries_provenance():
    """generate(SEASONAL) succeeds with >= 2 cycles and stamps the provenance
    (season_length + seasonal_applied) in the existing parameters field."""
    engine = ForecastingEngine()
    result = engine.generate(
        item_history=HISTORY,
        method=ForecastMethod.SEASONAL,
        params={"season_length": 4},
    )

    assert result.forecast_value == Decimal("200")
    assert result.parameters["season_length"] == 4
    assert result.parameters["seasonal_applied"] is True
    assert result.warnings == []


def test_engine_forecast_series_returns_the_curve():
    """forecast_series(SEASONAL) returns the CURVE, not a repeated value."""
    engine = ForecastingEngine()
    series = engine.forecast_series(
        item_history=HISTORY,
        method=ForecastMethod.SEASONAL,
        params={"season_length": 4},
        periods=6,
    )

    assert series == CURVE_6


def test_engine_seasonal_fallback_is_flat_and_documented():
    """< 2 complete cycles: documented flat fallback, never silent.

    history = [10, 20, 30, 40, 50, 60] with season_length=4 (needs 8 points)
    -> flat MA(window=min(6, 4)=4) on the last 4 points = (30+40+50+60)/4 = 45
    """
    engine = ForecastingEngine()
    history = [10, 20, 30, 40, 50, 60]

    result = engine.generate(
        item_history=history,
        method=ForecastMethod.SEASONAL,
        params={"season_length": 4},
    )

    assert result.forecast_value == Decimal("45")
    assert result.parameters["seasonal_applied"] is False
    assert any("fallback" in warning for warning in result.warnings)

    # The series stays flat on the fallback path
    series = engine.forecast_series(
        item_history=history,
        method=ForecastMethod.SEASONAL,
        params={"season_length": 4},
        periods=3,
    )
    assert series == [Decimal("45")] * 3


def test_engine_seasonal_requires_season_length():
    """season_length is mandatory (no business-specific default)."""
    engine = ForecastingEngine()

    with pytest.raises(ForecastingError):
        engine.generate(item_history=HISTORY, method=ForecastMethod.SEASONAL)
    with pytest.raises(ForecastingError):
        engine.generate(
            item_history=HISTORY,
            method=ForecastMethod.SEASONAL,
            params={"season_length": 1},
        )


def test_engine_seasonal_rejects_non_integer_season_length():
    """Strictly an int — no coercion of 7.9 / "52" / bool (aligned with the
    strict API validator in routers/forecasting.py)."""
    engine = ForecastingEngine()

    for bad in (7.9, "52", True, None, [4]):
        with pytest.raises(ForecastingError):
            engine.generate(
                item_history=HISTORY,
                method=ForecastMethod.SEASONAL,
                params={"season_length": bad},
            )


def test_croston_series_stays_flat_on_intermittent_demand():
    """Croston remains FLAT by design: on intermittent demand, projecting
    seasonal indices would extrapolate the random position of transactions,
    not a season."""
    engine = ForecastingEngine()
    history = [0, 0, 100, 0, 0, 150, 0, 0, 0, 200]

    series = engine.forecast_series(
        item_history=history,
        method=ForecastMethod.CROSTON,
        periods=6,
    )

    assert len(series) == 6
    assert all(value == series[0] for value in series)
