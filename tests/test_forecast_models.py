"""
Unit tests for forecast data models (FORECAST-001).
Tests cover Forecast, ForecastValue, and ForecastAdjustment models.
"""
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from ootils_core.models import (
    Forecast,
    ForecastAdjustment,
    ForecastMethod,
    ForecastValue,
)


class TestForecastMethod:
    """Test ForecastMethod enum constants."""

    def test_method_constants(self):
        """Verify all forecast method constants are defined."""
        assert ForecastMethod.MA == "MA"
        assert ForecastMethod.EXP_SMOOTHING == "EXP_SMOOTHING"
        assert ForecastMethod.CROSTON == "CROSTON"
        assert ForecastMethod.SEASONAL == "SEASONAL"

    def test_method_values_are_strings(self):
        """All method values should be strings."""
        assert isinstance(ForecastMethod.MA, str)
        assert isinstance(ForecastMethod.EXP_SMOOTHING, str)
        assert isinstance(ForecastMethod.CROSTON, str)
        assert isinstance(ForecastMethod.SEASONAL, str)


class TestForecast:
    """Test Forecast model."""

    def test_create_minimal_forecast(self):
        """Create a forecast with required fields only."""
        forecast = Forecast(
            forecast_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            horizon_start=date(2026, 1, 1),
            horizon_end=date(2026, 3, 31),
            granularity="daily",
            method=ForecastMethod.MA,
        )

        assert forecast.granularity == "daily"
        assert forecast.method == "MA"
        assert forecast.horizon_start == date(2026, 1, 1)
        assert forecast.horizon_end == date(2026, 3, 31)

    def test_create_forecast_with_all_granularities(self):
        """Test all supported granularity values."""
        for granularity in ["daily", "weekly", "monthly"]:
            forecast = Forecast(
                forecast_id=uuid4(),
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                horizon_start=date(2026, 1, 1),
                horizon_end=date(2026, 12, 31),
                granularity=granularity,
                method=ForecastMethod.SEASONAL,
            )
            assert forecast.granularity == granularity

    def test_create_forecast_with_all_methods(self):
        """Test all supported forecasting methods."""
        methods = [
            ForecastMethod.MA,
            ForecastMethod.EXP_SMOOTHING,
            ForecastMethod.CROSTON,
            ForecastMethod.SEASONAL,
        ]
        for method in methods:
            forecast = Forecast(
                forecast_id=uuid4(),
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                horizon_start=date(2026, 1, 1),
                horizon_end=date(2026, 12, 31),
                granularity="weekly",
                method=method,
            )
            assert forecast.method == method

    def test_forecast_timestamps_auto_generated(self):
        """Verify created_at and updated_at are auto-generated."""
        before = datetime.now(timezone.utc)
        forecast = Forecast(
            forecast_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            horizon_start=date(2026, 1, 1),
            horizon_end=date(2026, 12, 31),
            granularity="weekly",
            method=ForecastMethod.MA,
        )
        after = datetime.now(timezone.utc)

        assert before <= forecast.created_at <= after
        assert before <= forecast.updated_at <= after

    def test_forecast_uuid_fields(self):
        """Verify UUID fields are properly set."""
        forecast_id = uuid4()
        item_id = uuid4()
        location_id = uuid4()
        scenario_id = uuid4()

        forecast = Forecast(
            forecast_id=forecast_id,
            item_id=item_id,
            location_id=location_id,
            scenario_id=scenario_id,
            horizon_start=date(2026, 1, 1),
            horizon_end=date(2026, 12, 31),
            granularity="monthly",
            method=ForecastMethod.MA,
        )

        assert forecast.forecast_id == forecast_id
        assert forecast.item_id == item_id
        assert forecast.location_id == location_id
        assert forecast.scenario_id == scenario_id


class TestForecastValue:
    """Test ForecastValue model."""

    def test_create_minimal_value(self):
        """Create a forecast value with required fields only."""
        value = ForecastValue(
            value_id=uuid4(),
            forecast_id=uuid4(),
            date=date(2026, 1, 15),
            quantity=Decimal("100.50"),
            method=ForecastMethod.MA,
        )

        assert value.date == date(2026, 1, 15)
        assert value.quantity == Decimal("100.50")
        assert value.method == "MA"
        assert value.confidence_interval_lower is None
        assert value.confidence_interval_upper is None

    def test_create_value_with_confidence_intervals(self):
        """Create a forecast value with confidence intervals."""
        value = ForecastValue(
            value_id=uuid4(),
            forecast_id=uuid4(),
            date=date(2026, 2, 1),
            quantity=Decimal("250.00"),
            method=ForecastMethod.EXP_SMOOTHING,
            confidence_interval_lower=Decimal("200.00"),
            confidence_interval_upper=Decimal("300.00"),
        )

        assert value.confidence_interval_lower == Decimal("200.00")
        assert value.confidence_interval_upper == Decimal("300.00")

    def test_create_value_with_decimal_precision(self):
        """Test decimal precision for quantities."""
        value = ForecastValue(
            value_id=uuid4(),
            forecast_id=uuid4(),
            date=date(2026, 3, 15),
            quantity=Decimal("123.456789"),
            method=ForecastMethod.CROSTON,
        )

        assert value.quantity == Decimal("123.456789")

    def test_value_timestamps_auto_generated(self):
        """Verify created_at is auto-generated."""
        before = datetime.now(timezone.utc)
        value = ForecastValue(
            value_id=uuid4(),
            forecast_id=uuid4(),
            date=date(2026, 1, 1),
            quantity=Decimal("100"),
            method=ForecastMethod.MA,
        )
        after = datetime.now(timezone.utc)

        assert before <= value.created_at <= after

    def test_multiple_values_for_forecast(self):
        """Test creating multiple values for the same forecast."""
        forecast_id = uuid4()
        values = []

        for day in range(1, 31):
            value = ForecastValue(
                value_id=uuid4(),
                forecast_id=forecast_id,
                date=date(2026, 1, day),
                quantity=Decimal(str(100 + day)),
                method=ForecastMethod.MA,
            )
            values.append(value)

        assert len(values) == 30
        assert all(v.forecast_id == forecast_id for v in values)
        assert values[0].date == date(2026, 1, 1)
        assert values[-1].date == date(2026, 1, 30)


class TestForecastAdjustment:
    """Test ForecastAdjustment model."""

    def test_create_manual_adjustment(self):
        """Create a manual adjustment with delta."""
        adjustment = ForecastAdjustment(
            adjustment_id=uuid4(),
            forecast_id=uuid4(),
            value_id=uuid4(),
            adjustment_type="manual",
            delta=Decimal("50.00"),
            reason="Customer requested increase",
            user_id="user_123",
        )

        assert adjustment.adjustment_type == "manual"
        assert adjustment.delta == Decimal("50.00")
        assert adjustment.delta_percent is None
        assert adjustment.reason == "Customer requested increase"
        assert adjustment.user_id == "user_123"
        assert adjustment.value_id is not None

    def test_create_promotion_adjustment(self):
        """Create a promotion adjustment with percentage."""
        adjustment = ForecastAdjustment(
            adjustment_id=uuid4(),
            forecast_id=uuid4(),
            value_id=None,  # Applies to entire forecast
            adjustment_type="promotion",
            delta=Decimal("100.00"),
            delta_percent=Decimal("15.00"),
            reason="Q2 promotion campaign",
        )

        assert adjustment.adjustment_type == "promotion"
        assert adjustment.value_id is None
        assert adjustment.delta_percent == Decimal("15.00")

    def test_create_seasonality_adjustment(self):
        """Create a seasonality adjustment."""
        adjustment = ForecastAdjustment(
            adjustment_id=uuid4(),
            forecast_id=uuid4(),
            adjustment_type="seasonality",
            delta=Decimal("-25.00"),  # Negative adjustment
            reason="Holiday season reduction",
        )

        assert adjustment.adjustment_type == "seasonality"
        assert adjustment.delta == Decimal("-25.00")

    def test_create_event_adjustment(self):
        """Create an event-based adjustment."""
        adjustment = ForecastAdjustment(
            adjustment_id=uuid4(),
            forecast_id=uuid4(),
            adjustment_type="event",
            delta=Decimal("200.00"),
            reason="Special event: Olympic Games",
            user_id="planner_456",
        )

        assert adjustment.adjustment_type == "event"
        assert adjustment.delta == Decimal("200.00")

    def test_adjustment_timestamps_auto_generated(self):
        """Verify applied_at and created_at are auto-generated."""
        before = datetime.now(timezone.utc)
        adjustment = ForecastAdjustment(
            adjustment_id=uuid4(),
            forecast_id=uuid4(),
            adjustment_type="manual",
            delta=Decimal("10.00"),
        )
        after = datetime.now(timezone.utc)

        assert before <= adjustment.applied_at <= after
        assert before <= adjustment.created_at <= after

    def test_adjustment_with_only_delta(self):
        """Test adjustment with only delta (no percent)."""
        adjustment = ForecastAdjustment(
            adjustment_id=uuid4(),
            forecast_id=uuid4(),
            adjustment_type="manual",
            delta=Decimal("75.50"),
        )

        assert adjustment.delta == Decimal("75.50")
        assert adjustment.delta_percent is None

    def test_adjustment_with_only_percent(self):
        """Test adjustment with only percent (no delta) - NOT SUPPORTED, delta is required."""
        # Per spec, delta is required. delta_percent is optional.
        # This test verifies that delta must always be provided.
        with pytest.raises(TypeError, match="missing 1 required positional argument: 'delta'"):
            ForecastAdjustment(
                adjustment_id=uuid4(),
                forecast_id=uuid4(),
                adjustment_type="manual",
                delta_percent=Decimal("10.50"),
            )

    def test_adjustment_with_both_delta_and_percent(self):
        """Test adjustment with both delta and percent."""
        adjustment = ForecastAdjustment(
            adjustment_id=uuid4(),
            forecast_id=uuid4(),
            adjustment_type="promotion",
            delta=Decimal("50.00"),
            delta_percent=Decimal("12.00"),
        )

        assert adjustment.delta == Decimal("50.00")
        assert adjustment.delta_percent == Decimal("12.00")


class TestForecastIntegration:
    """Integration tests for forecast models working together."""

    def test_forecast_with_values_and_adjustments(self):
        """Create a complete forecast with values and adjustments."""
        forecast_id = uuid4()

        # Create forecast
        forecast = Forecast(
            forecast_id=forecast_id,
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            horizon_start=date(2026, 1, 1),
            horizon_end=date(2026, 1, 31),
            granularity="daily",
            method=ForecastMethod.MA,
        )

        # Create values for each day
        values = []
        for day in range(1, 11):
            value = ForecastValue(
                value_id=uuid4(),
                forecast_id=forecast_id,
                date=date(2026, 1, day),
                quantity=Decimal("100.00"),
                method=ForecastMethod.MA,
            )
            values.append(value)

        # Apply adjustment to specific value
        adjustment = ForecastAdjustment(
            adjustment_id=uuid4(),
            forecast_id=forecast_id,
            value_id=values[0].value_id,  # Adjust first day
            adjustment_type="manual",
            delta=Decimal("25.00"),
            reason="Rush order",
            user_id="planner_001",
        )

        assert forecast.forecast_id == forecast_id
        assert len(values) == 10
        assert adjustment.value_id == values[0].value_id

    def test_forecast_with_multiple_adjustment_types(self):
        """Test forecast with different adjustment types."""
        forecast_id = uuid4()

        adjustments = [
            ForecastAdjustment(
                adjustment_id=uuid4(),
                forecast_id=forecast_id,
                adjustment_type="manual",
                delta=Decimal("10.00"),
            ),
            ForecastAdjustment(
                adjustment_id=uuid4(),
                forecast_id=forecast_id,
                adjustment_type="promotion",
                delta=Decimal("15.00"),
                delta_percent=Decimal("15.00"),
            ),
            ForecastAdjustment(
                adjustment_id=uuid4(),
                forecast_id=forecast_id,
                adjustment_type="seasonality",
                delta=Decimal("-20.00"),
            ),
        ]

        assert len(adjustments) == 3
        assert adjustments[0].adjustment_type == "manual"
        assert adjustments[1].adjustment_type == "promotion"
        assert adjustments[2].adjustment_type == "seasonality"

    def test_weekly_granularity_forecast(self):
        """Test weekly granularity forecast structure."""
        forecast = Forecast(
            forecast_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            horizon_start=date(2026, 1, 1),
            horizon_end=date(2026, 12, 31),
            granularity="weekly",
            method=ForecastMethod.SEASONAL,
        )

        # Create weekly values (use proper date arithmetic)
        from datetime import timedelta
        values = []
        start_date = date(2026, 1, 1)
        for week in range(52):
            value_date = start_date + timedelta(weeks=week)
            value = ForecastValue(
                value_id=uuid4(),
                forecast_id=forecast.forecast_id,
                date=value_date,
                quantity=Decimal("500.00"),
                method=ForecastMethod.SEASONAL,
                confidence_interval_lower=Decimal("450.00"),
                confidence_interval_upper=Decimal("550.00"),
            )
            values.append(value)

        assert forecast.granularity == "weekly"
        assert len(values) == 52
        assert all(v.confidence_interval_lower is not None for v in values)

    def test_monthly_granularity_forecast(self):
        """Test monthly granularity forecast structure."""
        forecast = Forecast(
            forecast_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            horizon_start=date(2026, 1, 1),
            horizon_end=date(2026, 12, 31),
            granularity="monthly",
            method=ForecastMethod.EXP_SMOOTHING,
        )

        # Create monthly values
        months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        values = []
        for month in months:
            value = ForecastValue(
                value_id=uuid4(),
                forecast_id=forecast.forecast_id,
                date=date(2026, month, 1),
                quantity=Decimal("1000.00"),
                method=ForecastMethod.EXP_SMOOTHING,
            )
            values.append(value)

        assert forecast.granularity == "monthly"
        assert len(values) == 12
