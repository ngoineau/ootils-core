"""
Unit tests for ForecastConsumerCore — APICS CPIM Part 2 compliant.

Tests cover:
  U-FC-001 to U-FC-007: Forecast consumption strategies
  U-FR-001 to U-FR-004: Forecast rules (within/beyond fence)
  U-DC-001 to U-DC-006: Double counting prevention
  A-APICS-006: Critical regression test (forecast + orders ≠ sum)

Architecture: Tests hit ForecastConsumerCore (pure logic, no DB).
The DB-backed ForecastConsumer is a thin wrapper tested via integration tests.
"""

from datetime import date, timedelta
from decimal import Decimal

from ootils_core.engine.mrp.forecast_consumer import (
    ForecastConsumerCore,
    ConsumptionStrategy,
)

# ─── Fixtures ──────────────────────────────────────────────────────────────────

TODAY = date(2026, 4, 20)  # Monday


def week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


# ─── U-FC: Forecast Consumption Strategies ─────────────────────────────────────

class TestForecastConsumptionStrategies:
    """U-FC series: test each consumption strategy."""

    def test_u_fc_001_max_strategy_basic(self):
        """U-FC-001: MAX strategy — net_demand = max(forecast, orders) within fence."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("100"))],
            customer_orders=[(TODAY, Decimal("80"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.net_demand == Decimal("100")
        assert bucket.strategy == "MAX"
        assert bucket.is_within_fence is True

    def test_u_fc_002_max_orders_exceed_forecast(self):
        """U-FC-002: MAX — when orders > forecast, net_demand = orders."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("50"))],
            customer_orders=[(TODAY, Decimal("80"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0 or b.customer_orders > 0][0]
        assert bucket.net_demand == Decimal("80")

    def test_u_fc_003_forecast_only_strategy(self):
        """U-FC-003: FORECAST_ONLY — orders are ignored entirely."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("100"))],
            customer_orders=[(TODAY, Decimal("200"))],
            strategy=ConsumptionStrategy.FORECAST_ONLY,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.net_demand == Decimal("100")

    def test_u_fc_004_orders_only_strategy(self):
        """U-FC-004: ORDERS_ONLY — forecast is ignored within fence."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("200"))],
            customer_orders=[(TODAY, Decimal("80"))],
            strategy=ConsumptionStrategy.ORDERS_ONLY,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.customer_orders > 0][0]
        assert bucket.net_demand == Decimal("80")

    def test_u_fc_005_priority_strategy_consumption_tracking(self):
        """U-FC-005: PRIORITY — tracks consumed_forecast and remaining_forecast."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("100"))],
            customer_orders=[(TODAY, Decimal("80"))],
            strategy=ConsumptionStrategy.PRIORITY,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.net_demand == Decimal("100")  # Same as MAX
        assert bucket.consumed_forecast == Decimal("80")
        assert bucket.remaining_forecast == Decimal("20")
        assert bucket.over_consumption == Decimal("0")

    def test_u_fc_006_priority_over_consumption(self):
        """U-FC-006: PRIORITY — orders exceed forecast → over_consumption tracked."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("50"))],
            customer_orders=[(TODAY, Decimal("80"))],
            strategy=ConsumptionStrategy.PRIORITY,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0 or b.customer_orders > 0][0]
        assert bucket.net_demand == Decimal("80")
        assert bucket.consumed_forecast == Decimal("50")
        assert bucket.remaining_forecast == Decimal("0")
        assert bucket.over_consumption == Decimal("30")

    def test_u_fc_007_demand_fence_zero_weeks(self):
        """U-FC-007: With demand_fence_weeks=0, only forecast counts everywhere."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("100"))],
            customer_orders=[(TODAY, Decimal("200"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=0,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        # Fence end = week_start(TODAY) + 0 weeks = TODAY, so TODAY is NOT < TODAY
        # → beyond fence → only forecast
        assert bucket.net_demand == Decimal("100")


# ─── U-FR: Forecast Rules ──────────────────────────────────────────────────────

class TestForecastRules:
    """U-FR series: demand fence and beyond-fence behavior."""

    def test_u_fr_001_within_fence_max(self):
        """U-FR-001: Within fence, MAX uses max(forecast, orders)."""
        w1 = TODAY
        result = ForecastConsumerCore.consume(
            forecasts=[(w1, Decimal("100"))],
            customer_orders=[(w1, Decimal("120"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.period_start == week_start(w1)][0]
        assert bucket.net_demand == Decimal("120")
        assert bucket.is_within_fence is True

    def test_u_fr_002_beyond_fence_forecast_only(self):
        """U-FR-002: Beyond fence, only forecast counts (orders excluded)."""
        beyond = TODAY + timedelta(weeks=10)
        result = ForecastConsumerCore.consume(
            forecasts=[(beyond, Decimal("100"))],
            customer_orders=[(beyond, Decimal("200"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=4,
            horizon_weeks=26,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.net_demand == Decimal("100")
        assert bucket.is_within_fence is False

    def test_u_fr_003_fence_boundary_exact(self):
        """U-FR-003: Period exactly at fence boundary is beyond fence."""
        # fence_end = TODAY + 4 weeks = 2026-05-18
        # A period starting at 2026-05-18 is NOT < fence_end → beyond fence
        at_boundary = TODAY + timedelta(weeks=4)  # 2026-05-18
        result = ForecastConsumerCore.consume(
            forecasts=[(at_boundary, Decimal("100"))],
            customer_orders=[(at_boundary, Decimal("200"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=4,
            horizon_weeks=8,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.is_within_fence is False
        assert bucket.net_demand == Decimal("100")  # forecast only

    def test_u_fr_004_orders_only_beyond_fence_zero(self):
        """U-FR-004: ORDERS_ONLY beyond fence → net_demand = 0."""
        beyond = TODAY + timedelta(weeks=10)
        result = ForecastConsumerCore.consume(
            forecasts=[(beyond, Decimal("100"))],
            customer_orders=[(beyond, Decimal("200"))],
            strategy=ConsumptionStrategy.ORDERS_ONLY,
            demand_fence_weeks=4,
            horizon_weeks=26,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.net_demand == Decimal("0")


# ─── U-DC: Double Counting Prevention ─────────────────────────────────────────

class TestDoubleCountingPrevention:
    """U-DC series: ensure forecast + orders are NEVER summed."""

    def test_u_dc_001_max_no_double_counting(self):
        """U-DC-001: MAX strategy never sums forecast + orders (critical)."""
        # This is A-APICS-006 — the most critical test
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("100"))],
            customer_orders=[(TODAY, Decimal("80"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.net_demand == Decimal("100"), \
            f"DOUBLE COUNTING BUG: net_demand={bucket.net_demand}, expected 100, NOT 180"

    def test_u_dc_002_forecast_only_no_double_counting(self):
        """U-DC-002: FORECAST_ONLY never adds orders."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("100"))],
            customer_orders=[(TODAY, Decimal("200"))],
            strategy=ConsumptionStrategy.FORECAST_ONLY,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.net_demand == Decimal("100"), \
            f"FORECAST_ONLY added orders: net_demand={bucket.net_demand}"

    def test_u_dc_003_orders_only_no_double_counting(self):
        """U-DC-003: ORDERS_ONLY never adds forecast."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("200"))],
            customer_orders=[(TODAY, Decimal("80"))],
            strategy=ConsumptionStrategy.ORDERS_ONLY,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.customer_orders > 0][0]
        assert bucket.net_demand == Decimal("80"), \
            f"ORDERS_ONLY added forecast: net_demand={bucket.net_demand}"

    def test_u_dc_004_priority_no_double_counting(self):
        """U-DC-004: PRIORITY strategy never sums forecast + orders."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("100"))],
            customer_orders=[(TODAY, Decimal("80"))],
            strategy=ConsumptionStrategy.PRIORITY,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.net_demand == Decimal("100"), \
            f"PRIORITY double counting: net_demand={bucket.net_demand}"

    def test_u_dc_005_beyond_fence_no_double_counting(self):
        """U-DC-005: Beyond fence, orders are never added to forecast."""
        beyond = TODAY + timedelta(weeks=10)
        result = ForecastConsumerCore.consume(
            forecasts=[(beyond, Decimal("100"))],
            customer_orders=[(beyond, Decimal("200"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=4,
            horizon_weeks=26,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.net_demand == Decimal("100"), \
            f"Beyond fence double counting: net_demand={bucket.net_demand}"

    def test_u_dc_006_zero_forecast_no_orders_inflation(self):
        """U-DC-006: Zero forecast + nonzero orders → net = orders (within fence)."""
        result = ForecastConsumerCore.consume(
            forecasts=[(TODAY, Decimal("0"))],
            customer_orders=[(TODAY, Decimal("80"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=4,
            horizon_weeks=4,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.customer_orders > 0][0]
        assert bucket.net_demand == Decimal("80")


# ─── APICS Conformity Tests ────────────────────────────────────────────────────

class TestAPICSConformity:
    """A-APICS series: validate against APICS CPIM reference scenarios."""

    def test_a_apics_006_double_counting_prevention(self):
        """
        A-APICS-006: CRITICAL — Forecast + orders in same period must NOT be summed.

        Forecast = 100, Orders = 80
        Within demand fence → net_demand = max(100, 80) = 100
        NOT 180 (which would be the double counting bug).
        """
        # Scenario 006 from apics_scenarios.py
        forecasts = [(date(2026, 5, 1), Decimal("100"))]
        customer_orders = [(date(2026, 5, 1), Decimal("80"))]

        result = ForecastConsumerCore.consume(
            forecasts=forecasts,
            customer_orders=customer_orders,
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=4,
            horizon_weeks=8,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0][0]
        assert bucket.net_demand == Decimal("100"), \
            f"A-APICS-006 FAIL: net_demand={bucket.net_demand}, expected 100 NOT 180"

    def test_a_apics_007_over_consumption(self):
        """A-APICS-007: Orders exceed forecast within fence."""
        # Scenario 007: fc=50, co=80 → net=80
        result = ForecastConsumerCore.consume(
            forecasts=[(date(2026, 5, 1), Decimal("50"))],
            customer_orders=[(date(2026, 5, 1), Decimal("80"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=4,
            horizon_weeks=8,
            start_date=TODAY,
        )
        bucket = [b for b in result if b.original_forecast > 0 or b.customer_orders > 0][0]
        assert bucket.net_demand == Decimal("80")
        assert bucket.over_consumption == Decimal("30")

    def test_a_apics_004_within_and_beyond_fence(self):
        """A-APICS-004: Within fence = max(fc, co), beyond = forecast only."""
        w1 = TODAY  # within 2-week fence
        w3 = TODAY + timedelta(weeks=3)  # beyond 2-week fence
        result = ForecastConsumerCore.consume(
            forecasts=[(w1, Decimal("100")), (w3, Decimal("100"))],
            customer_orders=[(w1, Decimal("80")), (w3, Decimal("120"))],
            strategy=ConsumptionStrategy.MAX,
            demand_fence_weeks=2,
            horizon_weeks=8,
            start_date=TODAY,
        )
        # Within fence: max(100, 80) = 100
        b1 = [b for b in result if b.period_start == week_start(w1)][0]
        assert b1.net_demand == Decimal("100")
        assert b1.is_within_fence is True

        # Beyond fence: forecast only = 100 (not 120)
        b3 = [b for b in result if b.period_start == week_start(w3)][0]
        assert b3.net_demand == Decimal("100")
        assert b3.is_within_fence is False

    def test_strategy_enum_backward_compat(self):
        """Backward compatibility: old DB enum values map to new strategies."""
        assert ConsumptionStrategy("max_only") == ConsumptionStrategy.MAX
        assert ConsumptionStrategy("consume_forward") == ConsumptionStrategy.PRIORITY
        assert ConsumptionStrategy("consume_backward") == ConsumptionStrategy.PRIORITY
        assert ConsumptionStrategy("consume_both") == ConsumptionStrategy.PRIORITY

    def test_all_strategies_produce_valid_buckets(self):
        """All strategies produce ConsumedBucket with required fields."""
        for strat in ConsumptionStrategy:
            result = ForecastConsumerCore.consume(
                forecasts=[(TODAY, Decimal("100"))],
                customer_orders=[(TODAY, Decimal("80"))],
                strategy=strat,
                demand_fence_weeks=4,
                horizon_weeks=4,
                start_date=TODAY,
            )
            data = [b for b in result if b.original_forecast > 0][0]
            assert data.net_demand >= Decimal("0")
            assert data.original_forecast == Decimal("100")
            assert data.customer_orders == Decimal("80")
            assert data.strategy == strat.value