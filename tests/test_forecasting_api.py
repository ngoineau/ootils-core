"""
Slim tests for the forecasting FastAPI router.

Keeps ONLY the tests that legitimately do not need a database:
  - Pydantic-validation 422 boundary tests (horizon_days range, granularity
    enum, method enum, list limit range, adjustment_type enum). These fire
    before any DB call.
  - Router-introspection tests (prefix, tags, registered in app).

Every test that used to mock the DB / ForecastingEngine was ported to
tests/integration/test_forecasting_api_integration.py against a real
PostgreSQL database, per the "no mocks" rule (CLAUDE.md).

The TestClient here overrides ``get_db`` with a sentinel that raises if
touched — proving the 422 fires *before* any DB access happens.
"""
from __future__ import annotations

import os
from uuid import uuid4

import pytest
from fastapi import status
from fastapi.testclient import TestClient

# Must set token BEFORE importing the app
os.environ["OOTILS_API_TOKEN"] = "test-token"

from ootils_core.api.app import create_app
from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db


AUTH_HEADERS = {"Authorization": "Bearer test-token"}


class _DBAccessForbidden(AssertionError):
    """Raised if a test that should fail validation tries to use the DB."""


class _ForbiddenDB:
    """Sentinel: any attribute access raises. Proves 422 fires before DB use."""

    def __getattr__(self, name):
        raise _DBAccessForbidden(
            f"Validation test reached DB (accessed {name!r}) — 422 should have fired first"
        )


def _make_client_no_db() -> TestClient:
    """TestClient where any DB use crashes the test."""
    app = create_app()

    def override_db():
        yield _ForbiddenDB()

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[require_auth] = lambda: "test-token"
    return TestClient(app)


# ─────────────────────────────────────────────────────────────
# POST /v1/demand/forecast/generate — validation
# ─────────────────────────────────────────────────────────────


class TestGenerateForecastValidation:
    """422 boundary tests for POST /v1/demand/forecast/generate."""

    def test_generate_forecast_horizon_exceeded(self):
        """horizon_days > 365 → 422 before any DB call."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/demand/forecast/generate",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "horizon_days": 400,
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_generate_forecast_horizon_zero(self):
        """horizon_days < 1 → 422."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/demand/forecast/generate",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "horizon_days": 0,
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_generate_forecast_invalid_granularity(self):
        """Granularity not in {daily, weekly, monthly} → 422."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/demand/forecast/generate",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "granularity": "hourly",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_generate_forecast_invalid_method(self):
        """Method not in allowed set → 422."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/demand/forecast/generate",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "method": "INVALID_METHOD",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_generate_forecast_seasonal_missing_season_length(self):
        """SEASONAL without method_params.season_length → 422 before any DB call."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/demand/forecast/generate",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "method": "SEASONAL",
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_generate_forecast_seasonal_invalid_season_length(self):
        """SEASONAL with season_length < 2 → 422 before any DB call."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/demand/forecast/generate",
            json={
                "item_id": "ITEM-001",
                "location_id": "LOC-001",
                "method": "SEASONAL",
                "method_params": {"season_length": 1},
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────────────────────────────────────────
# GET /v1/demand/forecast/{forecast_id} — validation
# ─────────────────────────────────────────────────────────────


class TestGetForecastValidation:
    """422 boundary tests for GET /v1/demand/forecast/{forecast_id}."""

    def test_get_forecast_invalid_uuid(self):
        """Path param that is not a UUID → 422 before any DB call."""
        client = _make_client_no_db()
        resp = client.get(
            "/v1/demand/forecast/not-a-uuid",
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────────────────────────────────────────
# GET /v1/demand/forecast — validation
# ─────────────────────────────────────────────────────────────


class TestListForecastsValidation:
    """422 boundary tests for GET /v1/demand/forecast."""

    def test_list_forecasts_invalid_limit_too_high(self):
        """limit > 500 → 422 before any DB call."""
        client = _make_client_no_db()
        resp = client.get(
            "/v1/demand/forecast?limit=1000",
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_list_forecasts_invalid_limit_zero(self):
        """limit < 1 → 422."""
        client = _make_client_no_db()
        resp = client.get(
            "/v1/demand/forecast?limit=0",
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_list_forecasts_invalid_granularity(self):
        """Granularity not in enum → 422."""
        client = _make_client_no_db()
        resp = client.get(
            "/v1/demand/forecast?granularity=hourly",
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_list_forecasts_invalid_method(self):
        """Method not in enum → 422."""
        client = _make_client_no_db()
        resp = client.get(
            "/v1/demand/forecast?method=BOGUS",
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────────────────────────────────────────
# POST /v1/demand/forecast/{id}/adjust — validation
# ─────────────────────────────────────────────────────────────


class TestAdjustForecastValidation:
    """422 boundary tests for POST /v1/demand/forecast/{forecast_id}/adjust."""

    def test_adjust_forecast_invalid_uuid(self):
        """Path param that is not a UUID → 422 before any DB call."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/demand/forecast/not-a-uuid/adjust",
            json={"delta": 10},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_adjust_forecast_invalid_type(self):
        """adjustment_type not in {manual, promotion, seasonality, event} → 422."""
        client = _make_client_no_db()
        resp = client.post(
            f"/v1/demand/forecast/{uuid4()}/adjust",
            json={"adjustment_type": "invalid", "delta": 10},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────────────────────────────────────────
# DELETE /v1/demand/forecast/{id} — validation
# ─────────────────────────────────────────────────────────────


class TestDeleteForecastValidation:
    """422 boundary tests for DELETE /v1/demand/forecast/{forecast_id}."""

    def test_delete_forecast_invalid_uuid(self):
        """Path param that is not a UUID → 422 before any DB call."""
        client = _make_client_no_db()
        resp = client.delete(
            "/v1/demand/forecast/not-a-uuid",
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────────────────────────────────────────
# Router introspection
# ─────────────────────────────────────────────────────────────


class TestRouterConfiguration:
    """Sanity checks on router wiring."""

    def test_router_prefix(self):
        from ootils_core.api.routers import forecasting
        assert forecasting.router.prefix == "/v1/demand/forecast"

    def test_router_tags(self):
        from ootils_core.api.routers import forecasting
        assert "forecasting" in forecasting.router.tags

    def test_router_registered_in_app(self):
        app = create_app()
        forecast_routes = [
            r for r in app.routes
            if hasattr(r, "path") and "/v1/demand/forecast" in r.path
        ]
        assert len(forecast_routes) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
