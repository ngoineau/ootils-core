"""
Unit tests for forecasting API endpoints (FORECAST-003).

Tests cover:
- POST /v1/demand/forecast/generate
- GET /v1/demand/forecast/{forecast_id}
- GET /v1/demand/forecast (list with filters)
- POST /v1/demand/forecast/{id}/adjust
- DELETE /v1/demand/forecast/{id}
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from uuid import uuid4

import pytest
from fastapi import status
from fastapi.testclient import TestClient
from unittest.mock import MagicMock

# Set env token before importing app
os.environ["OOTILS_API_TOKEN"] = "test-token"

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import get_db

# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

def _mock_db() -> MagicMock:
    """Return a mock psycopg3 Connection."""
    conn = MagicMock()
    conn.execute.return_value = MagicMock(rowcount=1)
    return conn


@pytest.fixture
def app():
    """Create test app instance."""
    application = create_app()
    return application


@pytest.fixture
def auth_headers():
    """Provide valid auth headers."""
    return {"Authorization": "Bearer test-token"}


# ─────────────────────────────────────────────────────────────
# POST /v1/demand/forecast/generate
# ─────────────────────────────────────────────────────────────

class TestGenerateForecast:
    """Tests for forecast generation endpoint."""

    def test_generate_forecast_horizon_exceeded(self, app, auth_headers):
        """Test forecast generation with horizon > 365 days."""
        mock_conn = _mock_db()
        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                "/v1/demand/forecast/generate",
                json={
                    "item_id": "ITEM-001",
                    "location_id": "LOC-001",
                    "horizon_days": 400,
                },
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_generate_forecast_invalid_granularity(self, app, auth_headers):
        """Test forecast generation with invalid granularity."""
        mock_conn = _mock_db()
        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                "/v1/demand/forecast/generate",
                json={
                    "item_id": "ITEM-001",
                    "location_id": "LOC-001",
                    "granularity": "hourly",
                },
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_generate_forecast_invalid_method(self, app, auth_headers):
        """Test forecast generation with invalid method."""
        mock_conn = _mock_db()
        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                "/v1/demand/forecast/generate",
                json={
                    "item_id": "ITEM-001",
                    "location_id": "LOC-001",
                    "method": "INVALID_METHOD",
                },
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_generate_forecast_valid_methods(self, app, auth_headers):
        """Test forecast generation with all valid methods."""
        methods = ["MA", "EXP_SMOOTHING", "CROSTON", "SEASONAL"]
        for method in methods:
            mock_conn = _mock_db()
            def override_db():
                yield mock_conn
            app.dependency_overrides[get_db] = override_db

            with TestClient(app) as c:
                response = c.post(
                    "/v1/demand/forecast/generate",
                    json={
                        "item_id": "ITEM-001",
                        "location_id": "LOC-001",
                        "method": method,
                    },
                    headers=auth_headers,
                )
            app.dependency_overrides.clear()
            # Validation passes; actual execution depends on DB state
            assert response.status_code in [
                status.HTTP_200_OK,
                status.HTTP_404_NOT_FOUND,
                status.HTTP_500_INTERNAL_SERVER_ERROR,
            ]

    def test_generate_forecast_valid_granularities(self, app, auth_headers):
        """Test forecast generation with all valid granularities."""
        granularities = ["daily", "weekly", "monthly"]
        for granularity in granularities:
            mock_conn = _mock_db()
            def override_db():
                yield mock_conn
            app.dependency_overrides[get_db] = override_db

            with TestClient(app) as c:
                response = c.post(
                    "/v1/demand/forecast/generate",
                    json={
                        "item_id": "ITEM-001",
                        "location_id": "LOC-001",
                        "granularity": granularity,
                    },
                    headers=auth_headers,
                )
            app.dependency_overrides.clear()
            assert response.status_code in [
                status.HTTP_200_OK,
                status.HTTP_404_NOT_FOUND,
                status.HTTP_500_INTERNAL_SERVER_ERROR,
            ]

    def test_generate_forecast_with_method_params(self, app, auth_headers):
        """Test forecast generation with method parameters."""
        mock_conn = _mock_db()
        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                "/v1/demand/forecast/generate",
                json={
                    "item_id": "ITEM-001",
                    "location_id": "LOC-001",
                    "method": "MA",
                    "method_params": {"window": 14},
                },
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code in [
            status.HTTP_200_OK,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
        ]

    def test_generate_forecast_item_not_found(self, app, auth_headers):
        """Test forecast generation when item doesn't exist."""
        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchone.return_value = None  # Item not found

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                "/v1/demand/forecast/generate",
                json={
                    "item_id": "NONEXISTENT",
                    "location_id": "LOC-001",
                },
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "not found" in response.json()["detail"].lower()

    def test_generate_forecast_location_not_found(self, app, auth_headers):
        """Test forecast generation when location doesn't exist."""
        mock_conn = _mock_db()
        # Item exists, location doesn't
        mock_conn.execute.return_value.fetchone.side_effect = [
            {"item_id": uuid4()},
            None,
        ]

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                "/v1/demand/forecast/generate",
                json={
                    "item_id": "ITEM-001",
                    "location_id": "NONEXISTENT",
                },
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_404_NOT_FOUND


# ─────────────────────────────────────────────────────────────
# GET /v1/demand/forecast/{forecast_id}
# ─────────────────────────────────────────────────────────────

class TestGetForecast:
    """Tests for get forecast endpoint."""

    def test_get_forecast_not_found(self, app, auth_headers):
        """Test getting non-existent forecast."""
        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchone.return_value = None

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.get(
                f"/v1/demand/forecast/{uuid4()}",
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_get_forecast_success(self, app, auth_headers):
        """Test successful forecast retrieval."""
        from datetime import datetime
        
        forecast_id = uuid4()
        item_id = uuid4()
        location_id = uuid4()
        scenario_id = uuid4()

        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchone.side_effect = [
            {
                "forecast_id": forecast_id,
                "item_id": item_id,
                "location_id": location_id,
                "scenario_id": scenario_id,
                "horizon_start": date.today(),
                "horizon_end": date.today() + timedelta(days=89),
                "granularity": "daily",
                "method": "MA",
                "created_at": datetime.now(),  # Use datetime, not date
            },
        ]
        mock_conn.execute.return_value.fetchall.return_value = []

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.get(
                f"/v1/demand/forecast/{forecast_id}",
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["forecast_id"] == str(forecast_id)
        assert "values" in data
        assert "metadata" in data


# ─────────────────────────────────────────────────────────────
# GET /v1/demand/forecast (list)
# ─────────────────────────────────────────────────────────────

class TestListForecasts:
    """Tests for list forecasts endpoint."""

    def test_list_forecasts_empty(self, app, auth_headers):
        """Test list with no forecasts."""
        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchall.return_value = []

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.get(
                "/v1/demand/forecast",
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "forecasts" in data
        assert data["total_count"] == 0

    def test_list_forecasts_with_filters(self, app, auth_headers):
        """Test list with various filters."""
        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchall.return_value = []

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            filters = [
                ("item_id", "ITEM-001"),
                ("location_id", "LOC-001"),
                ("granularity", "daily"),
                ("method", "MA"),
            ]
            for param, value in filters:
                response = c.get(
                    f"/v1/demand/forecast?{param}={value}",
                    headers=auth_headers,
                )
                assert response.status_code == status.HTTP_200_OK
        app.dependency_overrides.clear()

    def test_list_forecasts_invalid_limit(self, app, auth_headers):
        """Test list with invalid limit."""
        mock_conn = _mock_db()
        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.get(
                "/v1/demand/forecast?limit=1000",
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


# ─────────────────────────────────────────────────────────────
# POST /v1/demand/forecast/{id}/adjust
# ─────────────────────────────────────────────────────────────

class TestAdjustForecast:
    """Tests for forecast adjustment endpoint."""

    def test_adjust_forecast_not_found(self, app, auth_headers):
        """Test adjusting non-existent forecast."""
        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchone.return_value = None

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                f"/v1/demand/forecast/{uuid4()}/adjust",
                json={"delta": 10},
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_adjust_forecast_no_delta(self, app, auth_headers):
        """Test adjustment without delta or delta_percent."""
        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchone.return_value = {
            "forecast_id": uuid4(),
            "item_id": uuid4(),
            "location_id": uuid4(),
        }

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                f"/v1/demand/forecast/{uuid4()}/adjust",
                json={},
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_adjust_forecast_delta_only(self, app, auth_headers):
        """Test adjustment with delta only."""
        forecast_id = uuid4()
        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchone.side_effect = [
            {"forecast_id": forecast_id, "item_id": uuid4(), "location_id": uuid4()},
            {"total_qty": 100},
        ]

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                f"/v1/demand/forecast/{forecast_id}/adjust",
                json={"delta": 50},
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "adjustment_id" in data

    def test_adjust_forecast_percent_only(self, app, auth_headers):
        """Test adjustment with delta_percent only."""
        forecast_id = uuid4()
        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchone.side_effect = [
            {"forecast_id": forecast_id, "item_id": uuid4(), "location_id": uuid4()},
            {"total_qty": 100},
        ]

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                f"/v1/demand/forecast/{forecast_id}/adjust",
                json={"delta_percent": 10},
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_200_OK

    def test_adjust_forecast_all_types(self, app, auth_headers):
        """Test adjustments with all valid types."""
        forecast_id = uuid4()
        adjustment_types = ["manual", "promotion", "seasonality", "event"]

        for adj_type in adjustment_types:
            mock_conn = _mock_db()
            mock_conn.execute.return_value.fetchone.side_effect = [
                {"forecast_id": forecast_id, "item_id": uuid4(), "location_id": uuid4()},
                {"total_qty": 100},
            ]

            def override_db():
                yield mock_conn
            app.dependency_overrides[get_db] = override_db

            with TestClient(app) as c:
                response = c.post(
                    f"/v1/demand/forecast/{forecast_id}/adjust",
                    json={"adjustment_type": adj_type, "delta": 10},
                    headers=auth_headers,
                )
                assert response.status_code == status.HTTP_200_OK
            app.dependency_overrides.clear()

    def test_adjust_forecast_invalid_type(self, app, auth_headers):
        """Test adjustment with invalid type."""
        mock_conn = _mock_db()
        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.post(
                f"/v1/demand/forecast/{uuid4()}/adjust",
                json={"adjustment_type": "invalid", "delta": 10},
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


# ─────────────────────────────────────────────────────────────
# DELETE /v1/demand/forecast/{id}
# ─────────────────────────────────────────────────────────────

class TestDeleteForecast:
    """Tests for forecast deletion endpoint."""

    def test_delete_forecast_not_found(self, app, auth_headers):
        """Test deleting non-existent forecast."""
        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchone.return_value = None

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.delete(
                f"/v1/demand/forecast/{uuid4()}",
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_delete_forecast_success(self, app, auth_headers):
        """Test successful forecast deletion."""
        forecast_id = uuid4()
        mock_conn = _mock_db()
        mock_conn.execute.return_value.fetchone.return_value = {
            "forecast_id": forecast_id,
        }

        def override_db():
            yield mock_conn
        app.dependency_overrides[get_db] = override_db

        with TestClient(app) as c:
            response = c.delete(
                f"/v1/demand/forecast/{forecast_id}",
                headers=auth_headers,
            )
        app.dependency_overrides.clear()
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["status"] == "deleted"


# ─────────────────────────────────────────────────────────────
# Integration / Workflow tests
# ─────────────────────────────────────────────────────────────

class TestForecastingWorkflow:
    """End-to-end workflow tests (require DB)."""

    @pytest.mark.skip(reason="Requires database connection")
    def test_full_forecast_lifecycle(self, app, auth_headers):
        """Test complete forecast lifecycle: generate → get → adjust → delete."""
        pytest.skip("Requires database connection")
