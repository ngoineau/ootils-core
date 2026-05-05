# ruff: noqa: F401,F811
"""Integration tests for unified MRP endpoint (simple + APICS modes)."""
from __future__ import annotations

import pytest

from .conftest import requires_db
from .test_mrp_api import api_client, auth, seeded_db, test_item_location  # noqa: F401


@requires_db
class TestMrpRunSimpleMode:
    """Test simple single-level MRP (apics_mode=False)."""

    def test_simple_mrp_default(self, api_client, auth, test_item_location):
        """Default mode is simple MRP (backward compatible)."""
        response = api_client.post(
            "/v1/mrp/run",
            json={
                "item_id": test_item_location["item_id"],
                "location_id": test_item_location["location_id"],
                "horizon_days": 90,
            },
            headers=auth,
        )
        assert response.status_code == 200, response.text
        data = response.json()
        assert "planned_orders_created" in data
        assert "planned_orders" in data
        assert "message" in data

    def test_simple_mrp_explicit_false(self, api_client, auth, test_item_location):
        """Explicit apics_mode=False uses simple MRP."""
        response = api_client.post(
            "/v1/mrp/run",
            json={
                "item_id": test_item_location["item_id"],
                "location_id": test_item_location["location_id"],
                "apics_mode": False,
                "clear_existing": True,
            },
            headers=auth,
        )
        assert response.status_code == 200, response.text
        data = response.json()
        assert isinstance(data["planned_orders"], list)


@requires_db
class TestMrpRunApicsMode:
    """Test APICS multi-level MRP (apics_mode=True)."""

    def test_apics_mrp_enabled(self, api_client, auth, test_item_location):
        """apics_mode=True triggers full APICS engine."""
        response = api_client.post(
            "/v1/mrp/run",
            json={
                "item_id": test_item_location["item_id"],
                "location_id": test_item_location["location_id"],
                "apics_mode": True,
                "horizon_days": 90,
                "bucket_grain": "week",
                "forecast_strategy": "MAX",
            },
            headers=auth,
        )
        assert response.status_code == 200, response.text
        data = response.json()
        assert "run_id" in data
        assert "items_processed" in data
        assert "total_records" in data
        assert "nodes_created" in data
        assert "edges_created" in data

    def test_apics_mrp_with_llc_recalc(self, api_client, auth, test_item_location):
        """APICS mode with LLC recalculation."""
        response = api_client.post(
            "/v1/mrp/run",
            json={
                "item_id": test_item_location["item_id"],
                "location_id": test_item_location["location_id"],
                "apics_mode": True,
                "recalculate_llc": True,
                "horizon_days": 180,
            },
            headers=auth,
        )
        assert response.status_code == 200, response.text


@requires_db
class TestMrpRunValidation:
    """Test input validation on unified MRP endpoint."""

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("bucket_grain", "invalid"),
            ("forecast_strategy", "INVALID"),
            ("consumption_window_days", 0),
        ],
    )
    def test_invalid_apics_options(self, api_client, auth, test_item_location, field, value):
        payload = {
            "item_id": test_item_location["item_id"],
            "location_id": test_item_location["location_id"],
            "apics_mode": True,
            field: value,
        }
        response = api_client.post("/v1/mrp/run", json=payload, headers=auth)
        assert response.status_code == 422, response.text


@requires_db
def test_deprecated_endpoint_still_routes(api_client, auth, test_item_location):
    """Legacy endpoint should still route and authenticate."""
    response = api_client.post(
        "/v1/mrp/apics/run",
        json={
            "location_id": test_item_location["location_id"],
            "horizon_days": 90,
        },
        headers=auth,
    )
    assert response.status_code != 401
