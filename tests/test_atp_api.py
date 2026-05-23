"""
Slim tests for the ATP/CTP FastAPI routers.

Keeps ONLY the tests that legitimately do not need a database:
  - Pydantic-validation 422 boundary tests (quantity > 0, horizon_days range,
    max_days range). These fire before any DB call.
  - Router-introspection tests (prefix, tags, registered in app).
  - "Auth dependency is configured" sanity checks.

Every test that used to mock _resolve_item_uuid / _resolve_location_uuid /
ATPEngine.calculate / CTPEngine.check / CTPEngine.simulate_first_feasible_date
was ported to tests/integration/test_atp_api_integration.py against a real
PostgreSQL database, per the "no mocks" rule (CLAUDE.md).

The TestClient here overrides ``get_db`` with a sentinel that raises if
touched — proving the 422 fires *before* any DB access happens.
"""

import os

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
# ATP Check — validation-only tests
# ─────────────────────────────────────────────────────────────


class TestATPCheckValidation:
    """422 boundary tests for POST /v1/atp/check."""

    def test_atp_check_requires_auth_dependency_configured(self):
        """ATP routers module exposes require_auth — used as endpoint dependency."""
        from ootils_core.atp import routers
        assert hasattr(routers, "require_auth")

    def test_atp_check_validation_quantity_zero(self):
        client = _make_client_no_db()
        payload = {
            "item_id": "00000000-0000-0000-0000-000000000001",
            "location_id": "00000000-0000-0000-0000-000000000002",
            "quantity": 0,
            "requested_date": "2026-05-23",
        }
        resp = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_atp_check_validation_quantity_negative(self):
        client = _make_client_no_db()
        payload = {
            "item_id": "00000000-0000-0000-0000-000000000001",
            "location_id": "00000000-0000-0000-0000-000000000002",
            "quantity": -50,
            "requested_date": "2026-05-23",
        }
        resp = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_atp_check_validation_horizon_days_too_high(self):
        client = _make_client_no_db()
        payload = {
            "item_id": "00000000-0000-0000-0000-000000000001",
            "location_id": "00000000-0000-0000-0000-000000000002",
            "quantity": 100,
            "requested_date": "2026-05-23",
            "horizon_days": 1000,  # > max 730
        }
        resp = client.post("/v1/atp/check", json=payload, headers=AUTH_HEADERS)
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────────────────────────────────────────
# CTP Check — auth-config sanity only
# ─────────────────────────────────────────────────────────────


class TestCTPCheckValidation:
    def test_ctp_check_requires_auth_dependency_configured(self):
        from ootils_core.atp import routers
        assert hasattr(routers, "require_auth")


# ─────────────────────────────────────────────────────────────
# CTP Simulate — validation-only tests
# ─────────────────────────────────────────────────────────────


class TestCTPSimulateValidation:
    def test_ctp_simulate_requires_auth_dependency_configured(self):
        from ootils_core.atp import routers
        assert hasattr(routers, "require_auth")

    def test_ctp_simulate_validation_max_days_too_high(self):
        client = _make_client_no_db()
        payload = {
            "item_id": "00000000-0000-0000-0000-000000000001",
            "location_id": "00000000-0000-0000-0000-000000000002",
            "quantity": 100,
            "max_days": 100,  # > max 90
        }
        resp = client.post("/v1/ctp/simulate", json=payload, headers=AUTH_HEADERS)
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────────────────────────────────────────
# Router introspection
# ─────────────────────────────────────────────────────────────


class TestRouterConfiguration:
    def test_router_prefix(self):
        from ootils_core.atp import routers
        assert routers.router.prefix == "/v1"

    def test_router_tags(self):
        from ootils_core.atp import routers
        assert "atp" in routers.router.tags
        assert "ctp" in routers.router.tags

    def test_router_registered_in_app(self):
        app = create_app()
        atp_routes = [r for r in app.routes if hasattr(r, "path") and "/atp/" in r.path]
        ctp_routes = [r for r in app.routes if hasattr(r, "path") and "/ctp/" in r.path]
        assert len(atp_routes) > 0
        assert len(ctp_routes) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
