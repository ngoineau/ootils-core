"""
Unit tests for forecasting algorithms (FORECAST-002).

Tests cover:
- Moving Average (MA) forecaster
- Exponential Smoothing (ES) forecaster
- Croston forecaster (intermittent demand)
- Seasonal forecaster (seasonal index decomposition — golden battery in
  tests/test_seasonal_forecaster_golden.py)
- ForecastingEngine service
- Accuracy metrics (MAPE, Bias, Tracking Signal)
- Auto-calibration of parameters
"""

from decimal import Decimal

import pytest

from ootils_core.forecasting import (
    CrostonForecaster,
    ExponentialSmoothingForecaster,
    ForecastingEngine,
    ForecastMethod,
    ForecastingError,
    MovingAverageForecaster,
    SeasonalForecaster,
    create_forecaster,
)


# ─────────────────────────────────────────────────────────────
# Moving Average Tests
# ─────────────────────────────────────────────────────────────

class TestMovingAverageForecaster:
    """Tests for Moving Average algorithm."""

    def test_simple_ma_with_window_3(self):
        """MA with window=3 on simple data."""
        forecaster = MovingAverageForecaster(window_size=3)
        historical = [100, 120, 110, 130, 125]
        
        result = forecaster.forecast(historical)
        
        # Last 3 values: 110, 130, 125 → mean = 365/3 = 121.67
        expected = Decimal("121.6666666666666666666666666667")
        assert abs(result - expected) < Decimal("0.0001")

    def test_ma_with_window_2(self):
        """MA with window=2."""
        forecaster = MovingAverageForecaster(window_size=2)
        historical = [100, 150, 200]
        
        result = forecaster.forecast(historical)
        
        # Last 2 values: 150, 200 → mean = 175
        assert result == Decimal("175")

    def test_ma_with_decimal_data(self):
        """MA with Decimal input values."""
        forecaster = MovingAverageForecaster(window_size=2)
        historical = [Decimal("100.5"), Decimal("199.5")]
        
        result = forecaster.forecast(historical)
        
        assert result == Decimal("150")

    def test_ma_insufficient_data(self):
        """MA should raise error with insufficient data."""
        forecaster = MovingAverageForecaster(window_size=5)
        historical = [100, 120, 110]  # Only 3 values, need 5
        
        with pytest.raises(ForecastingError):
            forecaster.forecast(historical)

    def test_ma_window_size_validation_valid(self):
        """MA should reject invalid window_size."""
        with pytest.raises(ValueError):
            MovingAverageForecaster(window_size=0)
        
        with pytest.raises(ValueError):
            MovingAverageForecaster(window_size=-1)

    def test_ma_repr(self):
        """MA string representation."""
        forecaster = MovingAverageForecaster(window_size=4)
        assert "window_size=4" in repr(forecaster)


# ─────────────────────────────────────────────────────────────
# Exponential Smoothing Tests
# ─────────────────────────────────────────────────────────────

class TestExponentialSmoothingForecaster:
    """Tests for Exponential Smoothing algorithm."""

    def test_es_with_alpha_0_3(self):
        """ES with alpha=0.3 on simple data."""
        forecaster = ExponentialSmoothingForecaster(alpha=0.3)
        historical = [100, 120, 110]
        
        result = forecaster.forecast(historical)
        
        # F1 = 100 (init)
        # F2 = 0.3*120 + 0.7*100 = 36 + 70 = 106
        # F3 = 0.3*110 + 0.7*106 = 33 + 74.2 = 107.2
        assert abs(result - Decimal("107.2")) < Decimal("0.0001")

    def test_es_with_alpha_0_5(self):
        """ES with alpha=0.5 (more reactive)."""
        forecaster = ExponentialSmoothingForecaster(alpha=0.5)
        historical = [100, 200]
        
        result = forecaster.forecast(historical)
        
        # F1 = 100
        # F2 = 0.5*200 + 0.5*100 = 150
        assert result == Decimal("150")

    def test_es_with_alpha_1_0(self):
        """ES with alpha=1.0 (naive forecast = last value)."""
        forecaster = ExponentialSmoothingForecaster(alpha=1.0)
        historical = [100, 150, 200]
        
        result = forecaster.forecast(historical)
        
        # With alpha=1, forecast = last value
        assert result == Decimal("200")

    def test_es_invalid_alpha(self):
        """ES should reject invalid alpha."""
        with pytest.raises(ValueError):
            ExponentialSmoothingForecaster(alpha=0)
        
        with pytest.raises(ValueError):
            ExponentialSmoothingForecaster(alpha=1.5)
        
        with pytest.raises(ValueError):
            ExponentialSmoothingForecaster(alpha=-0.1)

    def test_es_repr(self):
        """ES string representation."""
        forecaster = ExponentialSmoothingForecaster(alpha=0.7)
        assert "alpha=0.7" in repr(forecaster)


# ─────────────────────────────────────────────────────────────
# Croston Tests (Intermittent Demand)
# ─────────────────────────────────────────────────────────────

class TestCrostonForecaster:
    """Tests for Croston algorithm (intermittent demand)."""

    def test_croston_typical_intermittent(self):
        """Croston on typical intermittent demand pattern."""
        forecaster = CrostonForecaster()
        # Demand at periods 2, 5, 9 (0-indexed)
        historical = [0, 0, 100, 0, 0, 150, 0, 0, 0, 200]
        
        result = forecaster.forecast(historical)
        
        # Z = (100+150+200)/3 = 150
        # P = ((5-2)+(9-5))/2 = (3+4)/2 = 3.5
        # Forecast = 150/3.5 = 42.857...
        assert abs(result - Decimal("42.857")) < Decimal("0.01")

    def test_croston_no_demand(self):
        """Croston with no positive demand should return 0."""
        forecaster = CrostonForecaster()
        historical = [0, 0, 0, 0, 0]
        
        result = forecaster.forecast(historical)
        
        assert result == Decimal("0")

    def test_croston_single_demand(self):
        """Croston with single positive demand."""
        forecaster = CrostonForecaster()
        historical = [0, 0, 100, 0, 0]
        
        result = forecaster.forecast(historical)
        
        # Only one demand, return that value
        assert result == Decimal("100")

    def test_croston_with_threshold(self):
        """Croston with custom demand threshold."""
        forecaster = CrostonForecaster(min_demand_threshold=50)
        historical = [10, 20, 100, 30, 150]
        
        result = forecaster.forecast(historical)
        
        # Only 100 and 150 exceed threshold=50
        # Z = (100+150)/2 = 125
        # P = (4-2) = 2 (indices 2 and 4)
        # Forecast = 125/2 = 62.5
        assert abs(result - Decimal("62.5")) < Decimal("0.01")

    def test_croston_repr(self):
        """Croston string representation."""
        forecaster = CrostonForecaster(min_demand_threshold=0.5)
        assert "min_demand_threshold=0.5" in repr(forecaster)


# ─────────────────────────────────────────────────────────────
# Seasonal Tests (seasonal index decomposition)
# ─────────────────────────────────────────────────────────────
# The full hand-computed golden battery lives in
# tests/test_seasonal_forecaster_golden.py — these are the API-shape basics.


class TestSeasonalForecaster:
    """Tests for Seasonal decomposition algorithm."""

    def test_seasonal_recovers_known_indices(self):
        """Seasonal indices on a 2-position cycle.

        historical = [150, 50, 150, 50], season_length = 2
        grand mean = 100 → indices = [1.5, 0.5]
        level = mean([150/1.5, 50/0.5]) = 100 → next forecast = 100 * 1.5 = 150
        """
        forecaster = SeasonalForecaster(season_length=2)
        historical = [150, 50, 150, 50]

        assert forecaster.seasonal_indices(historical) == [Decimal("1.5"), Decimal("0.5")]
        assert forecaster.forecast(historical) == Decimal("150")

    def test_seasonal_curve_repeats_profile(self):
        """forecast_curve projects level x index over the horizon."""
        forecaster = SeasonalForecaster(season_length=2)
        historical = [150, 50, 150, 50]

        curve = forecaster.forecast_curve(historical, periods=4)

        assert curve == [Decimal("150"), Decimal("50"), Decimal("150"), Decimal("50")]

    def test_seasonal_insufficient_history(self):
        """Fewer than 2 complete cycles → ForecastingError (no silent guess)."""
        forecaster = SeasonalForecaster(season_length=4)

        with pytest.raises(ForecastingError):
            forecaster.forecast([100, 120, 110])

    def test_seasonal_invalid_season_length(self):
        """Seasonal should reject season_length < 2."""
        with pytest.raises(ValueError):
            SeasonalForecaster(season_length=1)

        with pytest.raises(ValueError):
            SeasonalForecaster(season_length=-3)

    def test_seasonal_invalid_periods(self):
        """forecast_curve should reject periods < 1."""
        forecaster = SeasonalForecaster(season_length=2)

        with pytest.raises(ValueError):
            forecaster.forecast_curve([150, 50, 150, 50], periods=0)

    def test_seasonal_repr(self):
        """Seasonal string representation."""
        forecaster = SeasonalForecaster(season_length=12)
        assert "season_length=12" in repr(forecaster)


# ─────────────────────────────────────────────────────────────
# Factory Function Tests
# ─────────────────────────────────────────────────────────────

class TestCreateForecaster:
    """Tests for factory function."""

    def test_create_ma_forecaster(self):
        """Create MA forecaster via factory."""
        forecaster = create_forecaster('moving_average', window_size=4)
        
        assert isinstance(forecaster, MovingAverageForecaster)
        assert forecaster.window_size == 4

    def test_create_es_forecaster(self):
        """Create ES forecaster via factory."""
        forecaster = create_forecaster('exponential_smoothing', alpha=0.6)
        
        assert isinstance(forecaster, ExponentialSmoothingForecaster)
        assert forecaster.alpha == 0.6

    def test_create_croston_forecaster(self):
        """Create Croston forecaster via factory."""
        forecaster = create_forecaster('croston', min_demand_threshold=1.0)
        
        assert isinstance(forecaster, CrostonForecaster)
        assert forecaster.min_demand_threshold == 1.0

    def test_create_seasonal_forecaster(self):
        """Create Seasonal forecaster via factory."""
        forecaster = create_forecaster('seasonal', season_length=52)

        assert isinstance(forecaster, SeasonalForecaster)
        assert forecaster.season_length == 52

    def test_create_seasonal_requires_season_length(self):
        """Seasonal factory has no default cycle length."""
        with pytest.raises(ValueError):
            create_forecaster('seasonal')

    def test_create_unknown_method(self):
        """Factory should reject unknown method."""
        with pytest.raises(ValueError):
            create_forecaster('unknown_method')


# ─────────────────────────────────────────────────────────────
# ForecastingEngine Service Tests
# ─────────────────────────────────────────────────────────────

class TestForecastingEngine:
    """Tests for ForecastingEngine service."""

    def test_engine_generate_ma(self):
        """Engine generate with MA method."""
        engine = ForecastingEngine()
        historical = [100, 120, 110, 130, 125]
        
        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.MA,
            params={"window": 3}
        )
        
        assert result.method == ForecastMethod.MA
        assert abs(result.forecast_value - Decimal("121.67")) < Decimal("0.01")
        assert result.parameters == {"window": 3}
        assert result.historical_count == 5

    def test_engine_generate_es(self):
        """Engine generate with ES method."""
        engine = ForecastingEngine()
        historical = [100, 120, 110]
        
        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.EXP_SMOOTHING,
            params={"alpha": 0.3}
        )
        
        assert result.method == ForecastMethod.EXP_SMOOTHING
        assert abs(result.forecast_value - Decimal("107.2")) < Decimal("0.01")

    def test_engine_generate_croston(self):
        """Engine generate with Croston method."""
        engine = ForecastingEngine()
        historical = [0, 0, 100, 0, 0, 150, 0, 0, 0, 200]
        
        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.CROSTON
        )
        
        assert result.method == ForecastMethod.CROSTON
        assert abs(result.forecast_value - Decimal("42.86")) < Decimal("0.01")

    def test_engine_unknown_method(self):
        """Engine should reject unknown method."""
        engine = ForecastingEngine()
        
        with pytest.raises(ForecastingError):
            engine.generate(
                item_history=[100, 120, 110],
                method="UNKNOWN"
            )

    def test_engine_empty_history(self):
        """Engine should reject empty history."""
        engine = ForecastingEngine()
        
        with pytest.raises(ForecastingError):
            engine.generate(
                item_history=[],
                method=ForecastMethod.MA
            )

    def test_engine_with_accuracy_metrics(self):
        """Engine generate with actuals for accuracy metrics."""
        engine = ForecastingEngine()
        historical = [100, 120, 110, 130, 125]
        actuals = [128, 130]  # Actual values for comparison
        
        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.MA,
            params={"window": 3},
            actuals=actuals
        )
        
        assert "mape" in result.metrics
        assert "bias" in result.metrics
        assert "tracking_signal" in result.metrics
        assert "mad" in result.metrics
        assert "mse" in result.metrics

    def test_engine_auto_calibrate_alpha(self):
        """Engine with auto-calibration for alpha."""
        engine = ForecastingEngine(auto_calibrate=True)
        historical = [100, 150, 100, 150, 100, 150]  # Oscillating pattern
        
        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.EXP_SMOOTHING
        )
        
        assert "alpha" in result.parameters
        assert 0.1 <= result.parameters["alpha"] <= 0.9
        assert len(result.warnings) > 0  # Should have calibration warning

    def test_engine_auto_calibrate_window(self):
        """Engine with auto-calibration for window."""
        engine = ForecastingEngine(auto_calibrate=True)
        historical = [100, 110, 105, 108, 112, 110, 115, 113]
        
        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.MA
        )
        
        assert "window" in result.parameters
        assert 2 <= result.parameters["window"] <= 9

    def test_engine_forecast_series(self):
        """Engine forecast_series for multiple periods."""
        engine = ForecastingEngine()
        historical = [100, 120, 110, 130, 125]
        
        series = engine.forecast_series(
            item_history=historical,
            method=ForecastMethod.MA,
            params={"window": 3},
            periods=5
        )
        
        assert len(series) == 5
        # All periods should have same forecast (flat forecast)
        assert all(v == series[0] for v in series)

    def test_engine_generate_seasonal(self):
        """Engine generate with SEASONAL method (>= 2 complete cycles)."""
        engine = ForecastingEngine()
        historical = [150, 50, 150, 50]

        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.SEASONAL,
            params={"season_length": 2}
        )

        assert result.method == ForecastMethod.SEASONAL
        assert result.forecast_value == Decimal("150")
        assert result.parameters["season_length"] == 2
        assert result.parameters["seasonal_applied"] is True

    def test_engine_seasonal_requires_season_length(self):
        """SEASONAL without season_length → ForecastingError (no default)."""
        engine = ForecastingEngine()

        with pytest.raises(ForecastingError):
            engine.generate(
                item_history=[150, 50, 150, 50],
                method=ForecastMethod.SEASONAL
            )

    def test_engine_forecast_series_seasonal_curve(self):
        """forecast_series with SEASONAL returns a curve, not a repeated value."""
        engine = ForecastingEngine()
        historical = [150, 50, 150, 50]

        series = engine.forecast_series(
            item_history=historical,
            method=ForecastMethod.SEASONAL,
            params={"season_length": 2},
            periods=4
        )

        assert series == [Decimal("150"), Decimal("50"), Decimal("150"), Decimal("50")]

    def test_engine_seasonal_fallback_below_two_cycles(self):
        """SEASONAL on < 2 complete cycles falls back flat, with provenance."""
        engine = ForecastingEngine()
        historical = [150, 50, 150]  # 3 points < 2 * season_length

        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.SEASONAL,
            params={"season_length": 2}
        )

        assert result.parameters["seasonal_applied"] is False
        assert any("fallback" in w for w in result.warnings)
        # Flat level = MA(window=min(3, 2)=2) on last 2 points = (50+150)/2
        assert result.forecast_value == Decimal("100")


# ─────────────────────────────────────────────────────────────
# Accuracy Metrics Tests
# ─────────────────────────────────────────────────────────────

class TestAccuracyMetrics:
    """Tests for accuracy metrics calculation."""

    def test_mape_calculation(self):
        """MAPE calculation on simple data."""
        engine = ForecastingEngine()
        forecasts = [100, 120, 110]
        actuals = [105, 115, 120]
        
        metrics = engine.calculate_accuracy_metrics(forecasts, actuals)
        
        # Errors: 5, -5, 10
        # Abs % errors: 5/105*100=4.76, 5/115*100=4.35, 10/120*100=8.33
        # MAPE = (4.76+4.35+8.33)/3 = 5.81
        assert metrics.mape is not None
        assert abs(float(metrics.mape) - 5.81) < 0.1

    def test_bias_positive(self):
        """Positive bias (under-forecasting)."""
        engine = ForecastingEngine()
        forecasts = [100, 100, 100]
        actuals = [110, 120, 130]  # All higher than forecast
        
        metrics = engine.calculate_accuracy_metrics(forecasts, actuals)
        
        # Errors: 10, 20, 30 → Bias = 60 (positive = under-forecast)
        assert metrics.bias == Decimal("60")

    def test_bias_negative(self):
        """Negative bias (over-forecasting)."""
        engine = ForecastingEngine()
        forecasts = [100, 100, 100]
        actuals = [90, 80, 70]  # All lower than forecast
        
        metrics = engine.calculate_accuracy_metrics(forecasts, actuals)
        
        # Errors: -10, -20, -30 → Bias = -60 (negative = over-forecast)
        assert metrics.bias == Decimal("-60")

    def test_tracking_signal(self):
        """Tracking signal calculation."""
        engine = ForecastingEngine()
        forecasts = [100, 100, 100]
        actuals = [110, 120, 130]
        
        metrics = engine.calculate_accuracy_metrics(forecasts, actuals)
        
        # Bias = 60
        # MAD = (10+20+30)/3 = 20
        # Tracking Signal = 60/20 = 3
        assert metrics.tracking_signal is not None
        assert abs(float(metrics.tracking_signal) - 3.0) < 0.01

    def test_mad_calculation(self):
        """MAD (Mean Absolute Deviation) calculation."""
        engine = ForecastingEngine()
        forecasts = [100, 100, 100]
        actuals = [110, 90, 100]
        
        metrics = engine.calculate_accuracy_metrics(forecasts, actuals)
        
        # Abs errors: 10, 10, 0 → MAD = 20/3 = 6.67
        assert metrics.mad is not None
        assert abs(float(metrics.mad) - 6.67) < 0.01

    def test_mse_calculation(self):
        """MSE (Mean Squared Error) calculation."""
        engine = ForecastingEngine()
        forecasts = [100, 100, 100]
        actuals = [110, 90, 100]
        
        metrics = engine.calculate_accuracy_metrics(forecasts, actuals)
        
        # Squared errors: 100, 100, 0 → MSE = 200/3 = 66.67
        assert metrics.mse is not None
        assert abs(float(metrics.mse) - 66.67) < 0.01

    def test_accuracy_mismatched_lengths(self):
        """Accuracy metrics should reject mismatched lengths."""
        engine = ForecastingEngine()
        
        with pytest.raises(ForecastingError):
            engine.calculate_accuracy_metrics(
                forecasts=[100, 120],
                actuals=[105]
            )

    def test_accuracy_empty_data(self):
        """Accuracy metrics should reject empty data."""
        engine = ForecastingEngine()
        
        with pytest.raises(ForecastingError):
            engine.calculate_accuracy_metrics(
                forecasts=[],
                actuals=[]
            )


# ─────────────────────────────────────────────────────────────
# Integration Tests with Real Data Patterns
# ─────────────────────────────────────────────────────────────

class TestRealDataPatterns:
    """Tests with realistic demand patterns."""

    def test_stable_demand_pattern(self):
        """Forecasting on stable demand pattern."""
        engine = ForecastingEngine()
        # Stable demand around 100 units
        historical = [98, 102, 99, 101, 100, 103, 97, 100]
        
        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.MA,
            params={"window": 4}
        )
        
        # Should forecast around 100
        assert abs(result.forecast_value - Decimal("100")) < Decimal("5")

    def test_trending_demand_pattern(self):
        """Forecasting on trending demand pattern."""
        engine = ForecastingEngine()
        # Upward trend: 100, 110, 120, 130, 140
        historical = [100, 110, 120, 130, 140]
        
        # ES with high alpha should capture trend better
        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.EXP_SMOOTHING,
            params={"alpha": 0.5}
        )
        
        # ES produces weighted average, not trend extrapolation
        # F1=100, F2=0.5*110+0.5*100=105, F3=0.5*120+0.5*105=112.5, F4=0.5*130+0.5*112.5=121.25, F5=0.5*140+0.5*121.25=130.625
        # Forecast is between min and max of recent values (smoothed)
        assert Decimal("100") < result.forecast_value < Decimal("140")
        # Should be closer to recent values due to high alpha
        assert result.forecast_value > Decimal("120")

    def test_intermittent_demand_spare_parts(self):
        """Forecasting spare parts (classic intermittent demand)."""
        engine = ForecastingEngine()
        # Spare part: demand every 2-3 months, 50-100 units
        historical = [0, 0, 75, 0, 0, 0, 50, 0, 0, 80, 0, 0]
        
        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.CROSTON
        )
        
        # Should give reasonable forecast (demand/interval)
        assert result.forecast_value > Decimal("0")
        assert result.forecast_value < Decimal("50")  # Less than avg demand

    def test_seasonal_pattern_approximation(self):
        """MA can approximate seasonal pattern with right window."""
        engine = ForecastingEngine()
        # Simple seasonal: 100, 200, 100, 200, 100, 200
        historical = [100, 200, 100, 200, 100, 200]
        
        # MA with window=2 captures the oscillation
        result = engine.generate(
            item_history=historical,
            method=ForecastMethod.MA,
            params={"window": 2}
        )
        
        # Forecast = (100+200)/2 = 150 (average of oscillation)
        assert abs(result.forecast_value - Decimal("150")) < Decimal("0.01")
