"""
Slim tests for the BOM FastAPI router (src/ootils_core/api/routers/bom.py).

Keeps ONLY the tests that legitimately do not need a database:
  - Pydantic-validation 422 boundary tests (quantity_per > 0, levels range,
    missing required fields). These fire before any DB call.
  - 401 auth tests (the auth dependency rejects missing tokens before any
    DB use).
  - Router-introspection tests (prefix, tags, registered in app).

Every test that used to mock `_resolve_item_id` / `_get_active_bom` /
`_get_bom_lines` / `_detect_cycle` / `_recalculate_llc` / `_get_on_hand_qty`
was ported to tests/integration/test_router_bom_integration.py against a
real PostgreSQL database, per the "no mocks" rule (CLAUDE.md).

The TestClient here overrides ``get_db`` with a sentinel that raises if
touched — proving validation / auth fires *before* any DB access happens.
"""
from __future__ import annotations

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
    """Sentinel: any attribute access raises. Proves 422 / 401 fires before DB use."""

    def __getattr__(self, name):
        raise _DBAccessForbidden(
            f"Validation test reached DB (accessed {name!r}) — 422/401 should have fired first"
        )


def _make_client_no_db() -> TestClient:
    """TestClient where any DB use crashes the test."""
    app = create_app()

    def override_db():
        yield _ForbiddenDB()

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[require_auth] = lambda: "test-token"
    return TestClient(app)


def _make_client_no_auth_override() -> TestClient:
    """TestClient with DB forbidden AND auth NOT overridden (used for 401 tests)."""
    app = create_app()

    def override_db():
        yield _ForbiddenDB()

    app.dependency_overrides[get_db] = override_db
    return TestClient(app)


# ─────────────────────────────────────────────────────────────
# Auth (401) — no token → rejected before DB access
# ─────────────────────────────────────────────────────────────


class TestBOMRouterAuth:
    """401 tests for each BOM endpoint (require_auth fires before get_db)."""

    def test_ingest_bom_requires_auth(self):
        client = _make_client_no_auth_override()
        resp = client.post(
            "/v1/ingest/bom",
            json={"parent_external_id": "P1", "components": []},
        )
        assert resp.status_code == 401

    def test_get_bom_requires_auth(self):
        client = _make_client_no_auth_override()
        resp = client.get("/v1/bom/P1")
        assert resp.status_code == 401

    def test_explode_bom_requires_auth(self):
        client = _make_client_no_auth_override()
        resp = client.post(
            "/v1/bom/explode",
            json={"item_external_id": "P1", "quantity": 10},
        )
        assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# Pydantic validation (422) — fires before any DB access
# ─────────────────────────────────────────────────────────────


class TestBOMIngestValidation:
    """422 boundary tests for POST /v1/ingest/bom."""

    def test_ingest_bom_validation_empty_body(self):
        client = _make_client_no_db()
        resp = client.post("/v1/ingest/bom", json={}, headers=AUTH_HEADERS)
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_ingest_bom_validation_negative_quantity(self):
        """quantity_per <= 0 → Field(..., gt=0) blocks before DB."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/bom",
            json={
                "parent_external_id": "P1",
                "components": [
                    {"component_external_id": "C1", "quantity_per": -1.0},
                ],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_ingest_bom_validation_zero_quantity(self):
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/bom",
            json={
                "parent_external_id": "P1",
                "components": [
                    {"component_external_id": "C1", "quantity_per": 0.0},
                ],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_ingest_bom_validation_scrap_factor_out_of_range(self):
        """scrap_factor must be in [0.0, 1.0)."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/bom",
            json={
                "parent_external_id": "P1",
                "components": [
                    {"component_external_id": "C1", "quantity_per": 1.0, "scrap_factor": 1.5},
                ],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


class TestBOMExplodeValidation:
    """422 boundary tests for POST /v1/bom/explode."""

    def test_explode_bom_validation_zero_quantity(self):
        client = _make_client_no_db()
        resp = client.post(
            "/v1/bom/explode",
            json={"item_external_id": "P1", "quantity": 0},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_explode_bom_validation_negative_quantity(self):
        client = _make_client_no_db()
        resp = client.post(
            "/v1/bom/explode",
            json={"item_external_id": "P1", "quantity": -5},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_explode_bom_validation_levels_zero(self):
        """levels must be >= 1."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/bom/explode",
            json={"item_external_id": "P1", "quantity": 1, "levels": 0},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_explode_bom_validation_levels_too_high(self):
        """levels must be <= 20."""
        client = _make_client_no_db()
        resp = client.post(
            "/v1/bom/explode",
            json={"item_external_id": "P1", "quantity": 1, "levels": 100},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────────────────────────────────────────
# Router introspection
# ─────────────────────────────────────────────────────────────


class TestBOMRouterConfiguration:
    def test_router_prefix(self):
        from ootils_core.api.routers import bom
        assert bom.router.prefix == "/v1"

    def test_router_tags(self):
        from ootils_core.api.routers import bom
        assert "bom" in bom.router.tags

    def test_router_registered_in_app(self):
        app = create_app()
        bom_routes = [r for r in app.routes if hasattr(r, "path") and "/bom" in r.path]
        ingest_routes = [
            r for r in app.routes if hasattr(r, "path") and r.path == "/v1/ingest/bom"
        ]
        assert len(bom_routes) > 0
        assert len(ingest_routes) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
