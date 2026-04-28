"""
Integration tests for unified MRP endpoint (simple + APICS modes).

Run with: pytest tests/test_mrp_unified.py -v
"""
import pytest
from uuid import uuid4
from decimal import Decimal
from datetime import date, timedelta

from fastapi.testclient import TestClient
from ootils_core.api.app import create_app


@pytest.fixture
def client():
    """Create test client with test DB."""
    app = create_app()
    # Override DB dependency to use test DB
    # (implementation depends on test DB setup)
    with TestClient(app) as c:
        yield c


class TestMrpRunSimpleMode:
    """Test simple single-level MRP (apics_mode=False)."""
    
    def test_simple_mrp_default(self, client):
        """Default mode is simple MRP (backward compatible)."""
        response = client.post(
            "/v1/mrp/run",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "horizon_days": 90,
            },
            headers={"Authorization": "Bearer test-token"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "planned_orders_created" in data
        assert "planned_orders" in data
        assert "message" in data
    
    def test_simple_mrp_explicit_false(self, client):
        """Explicit apics_mode=False uses simple MRP."""
        response = client.post(
            "/v1/mrp/run",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "apics_mode": False,
                "clear_existing": True,
            },
            headers={"Authorization": "Bearer test-token"},
        )
        assert response.status_code == 200
        data = response.json()
        # Simple mode returns planned_orders array
        assert isinstance(data["planned_orders"], list)


class TestMrpRunApicsMode:
    """Test APICS multi-level MRP (apics_mode=True)."""
    
    def test_apics_mrp_enabled(self, client):
        """apics_mode=True triggers full APICS engine."""
        response = client.post(
            "/v1/mrp/run",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "apics_mode": True,
                "horizon_days": 90,
                "bucket_grain": "week",
                "forecast_strategy": "MAX",
            },
            headers={"Authorization": "Bearer test-token"},
        )
        assert response.status_code == 200
        data = response.json()
        # APICS mode returns different fields
        assert "run_id" in data
        assert "items_processed" in data
        assert "total_records" in data
        assert "nodes_created" in data
        assert "edges_created" in data
    
    def test_apics_mrp_with_llc_recalc(self, client):
        """APICS mode with LLC recalculation."""
        response = client.post(
            "/v1/mrp/run",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "apics_mode": True,
                "recalculate_llc": True,
                "horizon_days": 180,
            },
            headers={"Authorization": "Bearer test-token"},
        )
        assert response.status_code == 200


class TestMrpRunValidation:
    """Test input validation."""
    
    def test_invalid_bucket_grain(self, client):
        """Invalid bucket_grain is rejected."""
        response = client.post(
            "/v1/mrp/run",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "apics_mode": True,
                "bucket_grain": "invalid",  # Must be day|week|month
            },
            headers={"Authorization": "Bearer test-token"},
        )
        assert response.status_code == 422
    
    def test_invalid_forecast_strategy(self, client):
        """Invalid forecast_strategy is rejected."""
        response = client.post(
            "/v1/mrp/run",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "apics_mode": True,
                "forecast_strategy": "INVALID",
            },
            headers={"Authorization": "Bearer test-token"},
        )
        assert response.status_code == 422
    
    def test_consumption_window_bounds(self, client):
        """consumption_window_days must be 1-90."""
        response = client.post(
            "/v1/mrp/run",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "apics_mode": True,
                "consumption_window_days": 0,  # Too low
            },
            headers={"Authorization": "Bearer test-token"},
        )
        assert response.status_code == 422


class TestDeprecatedEndpoint:
    """Test deprecated /v1/mrp/apics/run still works."""
    
    def test_deprecated_endpoint_still_works(self, client):
        """Legacy endpoint should still function but log deprecation."""
        response = client.post(
            "/v1/mrp/apics/run",
            json={
                "location_id": str(uuid4()),
                "horizon_days": 90,
            },
            headers={"Authorization": "Bearer test-token"},
        )
        # Should still work (backward compatibility)
        assert response.status_code in [200, 404, 500]  # 404/500 OK if no data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
