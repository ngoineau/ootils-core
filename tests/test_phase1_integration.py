"""
Phase 1 unit tests — DB-free portion.

This file holds only the tests that exercise pure Python logic (forecast
algorithms, MPS dataclass workflow, edge-case error paths). Every test
that previously used a mocked database connection has been either:

- DROPPED as pure mock plumbing (assertions only verified the mocked
  cursor returned what the mock was told to return); or
- considered DUPLICATE of tests/integration/test_phase1_e2e.py, which
  exercises the full Forecast -> MPS -> CRP -> ATP chain through the
  real FastAPI routers on a migrated PostgreSQL database.

The canonical end-to-end coverage now lives in:
- tests/integration/test_phase1_e2e.py  (REST e2e over real DB)
- tests/integration/test_atp_api_integration.py  (ATP REST happy/edge)
- tests/integration/test_allocation_engine_integration.py  (engine over DB)
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from ootils_core.forecasting.engine import ForecastingEngine, ForecastMethod, ForecastResult
from ootils_core.mps.models import MPSNode, MPSStatus

from tests.fixtures.forecast_data import ForecastDatasets


# ─────────────────────────────────────────────────────────────
# Forecast generation — pure algorithmic tests, no DB.
# ─────────────────────────────────────────────────────────────

class TestForecastGeneration:
    """Test forecast generation with various demand patterns."""

    def test_stable_demand_moving_average(self):
        """Test MA forecasting on stable demand pattern."""
        fixture = ForecastDatasets.stable_demand()
        engine = ForecastingEngine()

        result = engine.generate(
            item_history=[float(d) for d in fixture.historical_demand],
            method=ForecastMethod.MA,
            params={"window": 3},
        )

        assert isinstance(result, ForecastResult)
        assert result.method == ForecastMethod.MA
        assert result.forecast_value > Decimal("0")
        assert fixture.expected_value_range[0] <= result.forecast_value <= fixture.expected_value_range[1]

    def test_trending_demand_exponential_smoothing(self):
        """Test exponential smoothing on trending demand."""
        fixture = ForecastDatasets.trending_demand()
        engine = ForecastingEngine()

        result = engine.generate(
            item_history=[float(d) for d in fixture.historical_demand],
            method=ForecastMethod.EXP_SMOOTHING,
            params={"alpha": 0.3},
        )

        assert isinstance(result, ForecastResult)
        assert result.method == ForecastMethod.EXP_SMOOTHING
        assert result.forecast_value > Decimal("150")  # Should be weighted toward recent values

    def test_intermittent_demand_croston(self):
        """Test Croston's method on intermittent demand."""
        fixture = ForecastDatasets.intermittent_demand()
        engine = ForecastingEngine()

        result = engine.generate(
            item_history=[float(d) for d in fixture.historical_demand],
            method=ForecastMethod.CROSTON,
        )

        assert isinstance(result, ForecastResult)
        assert result.method == ForecastMethod.CROSTON
        # Croston should handle zeros gracefully
        assert result.forecast_value >= Decimal("0")

    def test_forecast_engine_auto_method_selection(self):
        """Test that engine can handle different methods."""
        engine = ForecastingEngine()
        history = [100, 105, 98, 102, 101, 99, 103, 100, 102, 98]

        methods = [
            ForecastMethod.MA,
            ForecastMethod.EXP_SMOOTHING,
            ForecastMethod.CROSTON,
        ]

        for method in methods:
            result = engine.generate(
                item_history=history,
                method=method,
            )
            assert isinstance(result, ForecastResult)
            assert result.forecast_value > Decimal("0")


# ─────────────────────────────────────────────────────────────
# MPS dataclass workflow — pure, no DB.
# ─────────────────────────────────────────────────────────────

class TestMPSNodeWorkflow:
    """Test the MPSNode dataclass status transitions and demand math."""

    def test_mps_node_status_workflow(self):
        """Test MPS node status transitions and demand computation."""
        mps_node = MPSNode(
            mps_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            time_bucket="2026-W15",
            time_bucket_start=date(2026, 4, 6),
            time_bucket_end=date(2026, 4, 12),
            forecast_quantity=Decimal("100"),
            sales_orders_quantity=Decimal("50"),
        )

        # Initial state
        assert mps_node.status == MPSStatus.DRAFT
        assert mps_node.total_demand == Decimal("0")

        # Compute demand
        total = mps_node.compute_total_demand()
        assert total == Decimal("150")

        # Status transitions
        success, error = mps_node.transition_to(MPSStatus.REVIEWED, "user1")
        assert success is True
        assert mps_node.status == MPSStatus.REVIEWED
        assert mps_node.reviewed_by == "user1"

        success, error = mps_node.transition_to(MPSStatus.APPROVED, "user2")
        assert success is True
        assert mps_node.status == MPSStatus.APPROVED

        success, error = mps_node.transition_to(MPSStatus.RELEASED, "user3")
        assert success is True
        assert mps_node.status == MPSStatus.RELEASED

        # Cannot transition from RELEASED
        success, error = mps_node.transition_to(MPSStatus.DRAFT, "user1")
        assert success is False


# ─────────────────────────────────────────────────────────────
# Forecast -> MPS handoff at the dataclass level (no DB).
# The full Forecast -> MPS persisted chain is covered by
# tests/integration/test_phase1_e2e.py.
# ─────────────────────────────────────────────────────────────

class TestForecastToMPSHandoff:
    """Wire a forecast result into an MPSNode without touching the DB."""

    def test_forecast_to_mps_flow(self):
        """Generated forecast feeds into MPSNode demand math."""
        forecast_engine = ForecastingEngine()
        history = [100, 105, 98, 102, 101, 99, 103, 100, 102, 98]

        forecast_result = forecast_engine.generate(
            item_history=history,
            method=ForecastMethod.MA,
            params={"window": 3},
        )

        assert forecast_result.forecast_value > Decimal("0")

        mps_node = MPSNode(
            mps_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            time_bucket="2026-W15",
            time_bucket_start=date(2026, 4, 6),
            time_bucket_end=date(2026, 4, 12),
            forecast_quantity=forecast_result.forecast_value,
            sales_orders_quantity=Decimal("50"),
        )

        total_demand = mps_node.compute_total_demand()
        assert total_demand > forecast_result.forecast_value

        success, _ = mps_node.transition_to(MPSStatus.REVIEWED, "planner")
        assert success is True

        success, _ = mps_node.transition_to(MPSStatus.APPROVED, "manager")
        assert success is True


# ─────────────────────────────────────────────────────────────
# Edge cases / error handling — pure validation paths.
# ─────────────────────────────────────────────────────────────

class TestEdgeCases:
    """Test edge cases and error handling on pure logic."""

    def test_forecast_with_empty_history(self):
        """Test forecasting with empty history raises error."""
        from ootils_core.forecasting.algorithms import ForecastingError

        engine = ForecastingEngine()

        with pytest.raises(ForecastingError, match="vide"):
            engine.generate(
                item_history=[],
                method=ForecastMethod.MA,
            )

    def test_forecast_with_insufficient_history(self):
        """Test forecasting with insufficient history raises error."""
        from ootils_core.forecasting.algorithms import ForecastingError

        engine = ForecastingEngine()

        # MA with window=5 but only 2 data points - should raise error
        with pytest.raises(ForecastingError, match="insuffisantes"):
            engine.generate(
                item_history=[100, 50],
                method=ForecastMethod.MA,
                params={"window": 5},
            )

    def test_mps_invalid_status_transition(self):
        """Test MPS node rejects invalid status transitions."""
        mps_node = MPSNode(
            mps_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            time_bucket="2026-W15",
            time_bucket_start=date(2026, 4, 6),
            time_bucket_end=date(2026, 4, 12),
            status=MPSStatus.DRAFT,
        )

        # Cannot go from DRAFT to APPROVED (must go through REVIEWED)
        success, error = mps_node.transition_to(MPSStatus.APPROVED, "user")
        assert success is False
        assert "Cannot transition" in error


# ─────────────────────────────────────────────────────────────
# Skipped (covered by integration tests):
#
# - Forecast -> MPS aggregate (DB-write):
#       tests/integration/test_phase1_e2e.py::test_phase1_forecast_mps_crp_atp_rest_e2e
# - CRP capacity check / overload detection:
#       tests/integration/test_phase1_e2e.py (CRP step)
#       tests/integration/test_rccp.py
# - ATP availability / shortage:
#       tests/integration/test_atp_api_integration.py
#       tests/integration/test_phase1_e2e.py (ATP step)
# ─────────────────────────────────────────────────────────────
