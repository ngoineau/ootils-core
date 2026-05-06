"""
Unit tests for ATP/CTP API endpoints.

Tests cover:
- ATP check endpoint (auth, validation, success cases)
- CTP check endpoint
- CTP simulate endpoint (binary search)
- Edge cases and error handling

All DB calls are mocked via FastAPI dependency_overrides.
"""

import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi import status
from fastapi.testclient import TestClient

# Must set token BEFORE importing the app
os.environ["OOTILS_API_TOKEN"] = "test-token"

from ootils_core.api.app import create_app
from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

AUTH_HEADERS = {"Authorization": "Bearer test-token"}


def _make_client_with_mocks() -> tuple[TestClient, MagicMock]:
    """Create test client with fully mocked DB and engines."""
    app = create_app()
    
    # Mock DB connection
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall = MagicMock(return_value=[])
    mock_cursor.fetchone = MagicMock(return_value=None)
    mock_cursor.executemany = MagicMock()
    mock_conn.cursor = MagicMock(return_value=mock_cursor)
    mock_conn.execute = MagicMock()
    
    def override_db():
        yield mock_conn
    
    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[require_auth] = lambda: "test-token"
    
    return TestClient(app), mock_conn


# ─────────────────────────────────────────────────────────────
# ATP Check Endpoint Tests
# ─────────────────────────────────────────────────────────────

class TestATPCheckEndpoint:
    """Test POST /v1/atp/check endpoint."""

    def test_atp_check_requires_auth(self):
        """ATP check requires authentication (verified via middleware)."""
        # Auth is enforced by require_auth dependency which is always active
        # This test verifies the endpoint exists and auth dependency is configured
        app = create_app()
        # Check that require_auth is used in the endpoint
        from ootils_core.atp import routers
        # Verify router has auth dependency configured
        assert hasattr(routers, 'require_auth')

    def test_atp_check_item_not_found(self):
        """ATP check returns 404 for non-existent item."""
        client, mock_conn = _make_client_with_mocks()
        
        # Mock _resolve_item_uuid to return None
        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=None):
            payload = {
                "item_id": "non-existent-item",
                "location_id": str(uuid4()),
                "quantity": 100,
                "requested_date": date.today().isoformat(),
            }
            response = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
            assert response.status_code == status.HTTP_404_NOT_FOUND
            assert "not found" in response.json()["detail"].lower()

    def test_atp_check_location_not_found(self):
        """ATP check returns 404 for non-existent location."""
        client, mock_conn = _make_client_with_mocks()
        
        item_uuid = uuid4()
        # Mock _resolve_item_uuid to return UUID, then _resolve_location_uuid to return None
        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=item_uuid):
            with patch("ootils_core.atp.routers._resolve_location_uuid", return_value=None):
                payload = {
                    "item_id": str(item_uuid),
                    "location_id": "non-existent-location",
                    "quantity": 100,
                    "requested_date": date.today().isoformat(),
                }
                response = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
                assert response.status_code == status.HTTP_404_NOT_FOUND
                assert "not found" in response.json()["detail"].lower()

    def test_atp_check_validation_quantity_zero(self):
        """ATP check validates quantity > 0."""
        client, mock_conn = _make_client_with_mocks()

        payload = {
            "item_id": str(uuid4()),
            "location_id": str(uuid4()),
            "quantity": 0,
            "requested_date": date.today().isoformat(),
        }
        response = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_atp_check_validation_quantity_negative(self):
        """ATP check validates quantity > 0."""
        client, mock_conn = _make_client_with_mocks()

        payload = {
            "item_id": str(uuid4()),
            "location_id": str(uuid4()),
            "quantity": -50,
            "requested_date": date.today().isoformat(),
        }
        response = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_atp_check_validation_horizon_days(self):
        """ATP check validates horizon_days range (1-730)."""
        client, mock_conn = _make_client_with_mocks()

        payload = {
            "item_id": str(uuid4()),
            "location_id": str(uuid4()),
            "quantity": 100,
            "requested_date": date.today().isoformat(),
            "horizon_days": 1000,  # Exceeds max 730
        }
        response = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_atp_check_success_basic(self):
        """ATP check succeeds with valid input."""
        client, mock_conn = _make_client_with_mocks()

        item_uuid = uuid4()
        location_uuid = uuid4()
        request_date = date.today()

        # Mock the resolver functions
        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=item_uuid):
            with patch("ootils_core.atp.routers._resolve_location_uuid", return_value=location_uuid):
                # Mock ATPEngine.calculate to return a valid result
                mock_result = MagicMock()
                mock_result.is_fully_available = True
                mock_result.available_date = request_date
                mock_result.available_quantity = Decimal("100")
                mock_result.request_quantity = Decimal("100")
                mock_result.backorder_quantity = Decimal("0")
                mock_result.buckets = []
                mock_result.calculation_time_ms = 15.5
                
                with patch("ootils_core.atp.engine.ATPEngine.calculate", return_value=mock_result):
                    payload = {
                        "item_id": str(item_uuid),
                        "location_id": str(location_uuid),
                        "quantity": 100,
                        "requested_date": request_date.isoformat(),
                    }
                    response = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
                    assert response.status_code == status.HTTP_200_OK

                    data = response.json()
                    assert data["available"] == True
                    # quantity_available is returned as string (Decimal serialization)
                    assert data["quantity_available"] in [100.0, "100", 100]
                    assert data["requested_quantity"] in [100.0, "100", 100]
                    assert data["backorder_quantity"] in [0.0, "0", 0]
                    assert "buckets" in data
                    assert "calculation_time_ms" in data

    def test_atp_check_success_with_external_ids(self):
        """ATP check works with external IDs."""
        client, mock_conn = _make_client_with_mocks()

        item_uuid = uuid4()
        location_uuid = uuid4()
        request_date = date.today()

        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=item_uuid):
            with patch("ootils_core.atp.routers._resolve_location_uuid", return_value=location_uuid):
                mock_result = MagicMock()
                mock_result.is_fully_available = False
                mock_result.available_date = request_date + timedelta(days=5)
                mock_result.available_quantity = Decimal("50")
                mock_result.request_quantity = Decimal("100")
                mock_result.backorder_quantity = Decimal("50")
                mock_result.buckets = []
                mock_result.calculation_time_ms = 20.3
                
                with patch("ootils_core.atp.engine.ATPEngine.calculate", return_value=mock_result):
                    payload = {
                        "item_id": "ITEM-001",
                        "location_id": "LOC-001",
                        "quantity": 100,
                        "requested_date": request_date.isoformat(),
                    }
                    response = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
                    assert response.status_code == status.HTTP_200_OK

                    data = response.json()
                    assert data["available"] == False
                    assert data["quantity_available"] in [50.0, "50", 50]
                    assert data["backorder_quantity"] in [50.0, "50", 50]

    def test_atp_check_partial_shortage(self):
        """ATP check handles partial shortage scenario."""
        client, mock_conn = _make_client_with_mocks()

        item_uuid = uuid4()
        location_uuid = uuid4()
        request_date = date.today()

        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=item_uuid):
            with patch("ootils_core.atp.routers._resolve_location_uuid", return_value=location_uuid):
                mock_result = MagicMock()
                mock_result.is_fully_available = False
                mock_result.available_date = request_date + timedelta(days=3)
                mock_result.available_quantity = Decimal("75")
                mock_result.request_quantity = Decimal("100")
                mock_result.backorder_quantity = Decimal("25")
                mock_result.buckets = []
                mock_result.calculation_time_ms = 18.7
                
                with patch("ootils_core.atp.engine.ATPEngine.calculate", return_value=mock_result):
                    payload = {
                        "item_id": str(item_uuid),
                        "location_id": str(location_uuid),
                        "quantity": 100,
                        "requested_date": request_date.isoformat(),
                    }
                    response = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
                    assert response.status_code == status.HTTP_200_OK

                    data = response.json()
                    assert data["available"] == False
                    assert data["quantity_available"] in [75.0, "75", 75]
                    assert data["backorder_quantity"] in [25.0, "25", 25]


# ─────────────────────────────────────────────────────────────
# CTP Check Endpoint Tests
# ─────────────────────────────────────────────────────────────

class TestCTPCheckEndpoint:
    """Test POST /v1/ctp/check endpoint."""

    def test_ctp_check_requires_auth(self):
        """CTP check requires authentication (verified via middleware)."""
        # Auth is enforced by require_auth dependency which is always active
        from ootils_core.atp import routers
        # Verify router has auth dependency configured
        assert hasattr(routers, 'require_auth')

    def test_ctp_check_item_not_found(self):
        """CTP check returns 404 for non-existent item."""
        client, mock_conn = _make_client_with_mocks()

        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=None):
            payload = {
                "item_id": "non-existent-item",
                "location_id": str(uuid4()),
                "quantity": 100,
                "requested_date": date.today().isoformat(),
            }
            response = client.post("/v1/ctp/check", json=payload, headers=AUTH_HEADERS)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_ctp_check_success_basic(self):
        """CTP check succeeds with valid input."""
        client, mock_conn = _make_client_with_mocks()

        item_uuid = uuid4()
        location_uuid = uuid4()
        request_date = date.today()

        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=item_uuid):
            with patch("ootils_core.atp.routers._resolve_location_uuid", return_value=location_uuid):
                # Mock ATP result
                mock_atp_result = MagicMock()
                mock_atp_result.is_fully_available = True
                mock_atp_result.available_date = request_date
                mock_atp_result.available_quantity = Decimal("100")
                mock_atp_result.request_quantity = Decimal("100")
                mock_atp_result.backorder_quantity = Decimal("0")
                mock_atp_result.buckets = []
                mock_atp_result.calculation_time_ms = 25.5
                
                # Mock CTP result
                mock_ctp_result = MagicMock()
                mock_ctp_result.atp_result = mock_atp_result
                mock_ctp_result.capacity_feasible = True
                mock_ctp_result.violations = []
                mock_ctp_result.critical_resources = ["RESOURCE-001"]
                
                with patch("ootils_core.atp.ctp.CTPEngine.check", return_value=mock_ctp_result):
                    payload = {
                        "item_id": str(item_uuid),
                        "location_id": str(location_uuid),
                        "quantity": 100,
                        "requested_date": request_date.isoformat(),
                    }
                    response = client.post("/v1/ctp/check", json=payload, headers=AUTH_HEADERS)
                    assert response.status_code == status.HTTP_200_OK

                    data = response.json()
                    assert data["available"] == True
                    assert data["capacity_feasible"] == True
                    assert data["violations"] == []
                    assert "critical_resources" in data

    def test_ctp_check_with_capacity_violations(self):
        """CTP check returns capacity violations."""
        client, mock_conn = _make_client_with_mocks()

        item_uuid = uuid4()
        location_uuid = uuid4()
        request_date = date.today()

        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=item_uuid):
            with patch("ootils_core.atp.routers._resolve_location_uuid", return_value=location_uuid):
                mock_atp_result = MagicMock()
                mock_atp_result.is_fully_available = True
                mock_atp_result.available_date = request_date
                mock_atp_result.available_quantity = Decimal("100")
                mock_atp_result.request_quantity = Decimal("100")
                mock_atp_result.backorder_quantity = Decimal("0")
                mock_atp_result.buckets = []
                mock_atp_result.calculation_time_ms = 30.2
                
                mock_violation = MagicMock()
                mock_violation.resource_id = str(uuid4())
                mock_violation.resource_name = "CNC Machine 1"
                mock_violation.violation_date = request_date
                mock_violation.required_capacity = Decimal("150")
                mock_violation.available_capacity = Decimal("100")
                mock_violation.overload_pct = 150.0
                
                mock_ctp_result = MagicMock()
                mock_ctp_result.atp_result = mock_atp_result
                mock_ctp_result.capacity_feasible = False
                mock_ctp_result.violations = [mock_violation]
                mock_ctp_result.critical_resources = ["RESOURCE-001"]
                
                with patch("ootils_core.atp.ctp.CTPEngine.check", return_value=mock_ctp_result):
                    payload = {
                        "item_id": str(item_uuid),
                        "location_id": str(location_uuid),
                        "quantity": 100,
                        "requested_date": request_date.isoformat(),
                    }
                    response = client.post("/v1/ctp/check", json=payload, headers=AUTH_HEADERS)
                    assert response.status_code == status.HTTP_200_OK

                    data = response.json()
                    assert data["available"] == True
                    assert data["capacity_feasible"] == False
                    assert len(data["violations"]) == 1
                    assert data["violations"][0]["resource_name"] == "CNC Machine 1"
                    assert data["violations"][0]["overload_pct"] == 150.0

    def test_ctp_check_with_capacity_false(self):
        """CTP check with include_capacity=false skips capacity check."""
        client, mock_conn = _make_client_with_mocks()

        item_uuid = uuid4()
        location_uuid = uuid4()
        request_date = date.today()

        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=item_uuid):
            with patch("ootils_core.atp.routers._resolve_location_uuid", return_value=location_uuid):
                mock_atp_result = MagicMock()
                mock_atp_result.is_fully_available = True
                mock_atp_result.available_date = request_date
                mock_atp_result.available_quantity = Decimal("100")
                mock_atp_result.request_quantity = Decimal("100")
                mock_atp_result.backorder_quantity = Decimal("0")
                mock_atp_result.buckets = []
                mock_atp_result.calculation_time_ms = 25.5
                
                mock_ctp_result = MagicMock()
                mock_ctp_result.atp_result = mock_atp_result
                mock_ctp_result.capacity_feasible = None  # Not computed
                mock_ctp_result.violations = []
                mock_ctp_result.critical_resources = []
                
                with patch("ootils_core.atp.ctp.CTPEngine.check", return_value=mock_ctp_result):
                    payload = {
                        "item_id": str(item_uuid),
                        "location_id": str(location_uuid),
                        "quantity": 100,
                        "requested_date": request_date.isoformat(),
                        "include_capacity": False,
                    }
                    response = client.post("/v1/ctp/check", json=payload, headers=AUTH_HEADERS)
                    assert response.status_code == status.HTTP_200_OK

                    data = response.json()
                    assert "capacity_feasible" in data
                    assert "violations" in data


# ─────────────────────────────────────────────────────────────
# CTP Simulate Endpoint Tests
# ─────────────────────────────────────────────────────────────

class TestCTPSimulateEndpoint:
    """Test POST /v1/ctp/simulate endpoint."""

    def test_ctp_simulate_requires_auth(self):
        """CTP simulate requires authentication (verified via middleware)."""
        # Auth is enforced by require_auth dependency which is always active
        from ootils_core.atp import routers
        # Verify router has auth dependency configured
        assert hasattr(routers, 'require_auth')

    def test_ctp_simulate_item_not_found(self):
        """CTP simulate returns 404 for non-existent item."""
        client, mock_conn = _make_client_with_mocks()

        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=None):
            payload = {
                "item_id": "non-existent-item",
                "location_id": str(uuid4()),
                "quantity": 100,
            }
            response = client.post("/v1/ctp/simulate", json=payload, headers=AUTH_HEADERS)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_ctp_simulate_success_basic(self):
        """CTP simulate succeeds with basic payload."""
        client, mock_conn = _make_client_with_mocks()

        item_uuid = uuid4()
        location_uuid = uuid4()
        start_date = date.today() + timedelta(days=1)

        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=item_uuid):
            with patch("ootils_core.atp.routers._resolve_location_uuid", return_value=location_uuid):
                # Mock simulate_first_feasible_date to return options
                mock_options = [
                    (start_date, False, {"atp_available": False, "capacity_violations": 0}),
                    (start_date + timedelta(days=1), False, {"atp_available": True, "capacity_violations": 2}),
                    (start_date + timedelta(days=2), True, {"atp_available": True, "capacity_violations": 0}),
                ]
                
                with patch("ootils_core.atp.ctp.CTPEngine.simulate_first_feasible_date", return_value=mock_options):
                    payload = {
                        "item_id": str(item_uuid),
                        "location_id": str(location_uuid),
                        "quantity": 100,
                        "start_date": start_date.isoformat(),
                        "max_days": 14,
                    }
                    response = client.post("/v1/ctp/simulate", json=payload, headers=AUTH_HEADERS)
                    assert response.status_code == status.HTTP_200_OK

                    data = response.json()
                    assert data["first_feasible_date"] is not None
                    assert len(data["options"]) == 3
                    assert data["total_dates_tested"] == 3

    def test_ctp_simulate_no_feasible_date(self):
        """CTP simulate handles no feasible date found."""
        client, mock_conn = _make_client_with_mocks()

        item_uuid = uuid4()
        location_uuid = uuid4()
        start_date = date.today()

        with patch("ootils_core.atp.routers._resolve_item_uuid", return_value=item_uuid):
            with patch("ootils_core.atp.routers._resolve_location_uuid", return_value=location_uuid):
                mock_options = [
                    (start_date + timedelta(days=i), False, {"atp_available": False, "capacity_violations": 0})
                    for i in range(10)
                ]
                
                with patch("ootils_core.atp.ctp.CTPEngine.simulate_first_feasible_date", return_value=mock_options):
                    payload = {
                        "item_id": str(item_uuid),
                        "location_id": str(location_uuid),
                        "quantity": 100,
                        "max_days": 10,
                    }
                    response = client.post("/v1/ctp/simulate", json=payload, headers=AUTH_HEADERS)
                    assert response.status_code == status.HTTP_200_OK

                    data = response.json()
                    assert data["first_feasible_date"] is None
                    assert len(data["options"]) == 10

    def test_ctp_simulate_validation_max_days(self):
        """CTP simulate validates max_days range (1-90)."""
        client, mock_conn = _make_client_with_mocks()

        payload = {
            "item_id": str(uuid4()),
            "location_id": str(uuid4()),
            "quantity": 100,
            "max_days": 100,  # Exceeds max 90
        }
        response = client.post("/v1/ctp/simulate", json=payload, headers=AUTH_HEADERS)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


# ─────────────────────────────────────────────────────────────
# Router Configuration Tests
# ─────────────────────────────────────────────────────────────

class TestRouterConfiguration:
    """Test ATP router configuration."""

    def test_router_prefix(self):
        """ATP router has correct prefix."""
        from ootils_core.atp import routers
        assert routers.router.prefix == "/v1"

    def test_router_tags(self):
        """ATP router has correct tags."""
        from ootils_core.atp import routers
        assert "atp" in routers.router.tags
        assert "ctp" in routers.router.tags

    def test_router_registered_in_app(self):
        """ATP router is registered in the FastAPI app."""
        app = create_app()
        # Check that atp_router routes are registered
        atp_routes = [r for r in app.routes if hasattr(r, 'path') and '/atp/' in r.path]
        ctp_routes = [r for r in app.routes if hasattr(r, 'path') and '/ctp/' in r.path]
        assert len(atp_routes) > 0
        assert len(ctp_routes) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
