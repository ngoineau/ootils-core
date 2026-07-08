"""
Slim tests for the Sprint M6 FastAPI routers
(/v1/events, /v1/projection, /v1/issues, /v1/explain, /v1/simulate, /v1/graph).

Keeps ONLY the tests that legitimately do not need a database:
  - Pydantic-validation 422 boundary tests (unknown event_type, invalid
    UUID query params, disallowed override field_name, non-regression #48).
  - Auth boundary tests (401 without / with wrong token — these short-circuit
    before any DB call via the auth dependency).
  - OpenAPI / docs gating (no DB use).
  - Router introspection sanity checks.

Every test that previously patched GraphStore / ShortageDetector /
ExplanationBuilder / ScenarioManager / _build_propagation_engine was
ported to tests/integration/test_m6_api_integration.py against a real
PostgreSQL database, per the "no mocks" rule (CLAUDE.md).

The TestClient here overrides ``get_db`` with a sentinel that raises if
touched — proving the 422 / 401 fires *before* any DB access happens.

Non-regression #48 (test_post_simulate_invalid_node_returns_422):
  Override field_name 'shortage_qty' is not in the allowed override
  whitelist (_ALLOWED_FIELDS). The OverrideIn field_validator rejects it
  during Pydantic validation, before the DB is touched — so this still
  belongs in the slim file. Assertions check status 422 + sanitised
  error structure only, no str(exc) content checks (per chantier 2 of
  audit 2026-05-23).
"""
from __future__ import annotations

import os

import pytest
from fastapi import status
from fastapi.testclient import TestClient

# Set env token before importing app
os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import get_db

AUTH_HEADERS = {"Authorization": "Bearer test-token"}


class _DBAccessForbidden(AssertionError):
    """Raised if a test that should fail validation/auth tries to use the DB."""


class _ForbiddenDB:
    """Sentinel: any attribute access raises. Proves 422/401 fires before DB use."""

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
    return TestClient(app)


@pytest.fixture
def client() -> TestClient:
    return _make_client_no_db()


# ─────────────────────────── Health (no auth, no DB) ───────────────────────────


def test_health_no_auth(client):
    """Health endpoint requires no auth and never hits the DB."""
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["version"] == "1.0.0"


# ─────────────────────────── Auth boundary ───────────────────────────


def test_events_401_without_token(client):
    """Protected endpoint returns 401 without token, before DB access."""
    resp = client.post("/v1/events", json={"event_type": "supply_date_changed"})
    assert resp.status_code == 401


def test_events_401_wrong_token(client):
    resp = client.post(
        "/v1/events",
        json={"event_type": "supply_date_changed"},
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp.status_code == 401


def test_projection_401_without_token(client):
    resp = client.get("/v1/projection?item_id=abc&location_id=xyz")
    assert resp.status_code == 401


def test_issues_401_without_token(client):
    resp = client.get("/v1/issues")
    assert resp.status_code == 401


def test_explain_401_without_token(client):
    resp = client.get("/v1/explain?node_id=abc")
    assert resp.status_code == 401


def test_simulate_401_without_token(client):
    resp = client.post("/v1/simulate", json={"scenario_name": "x"})
    assert resp.status_code == 401


def test_graph_401_without_token(client):
    resp = client.get("/v1/graph?item_id=a&location_id=b")
    assert resp.status_code == 401


# ─────────────────────────── POST /v1/events — Pydantic validation ───────────────────────────


def test_post_event_invalid_type_returns_422(client):
    """Unknown event_type → 422 before any DB write."""
    resp = client.post(
        "/v1/events",
        json={"event_type": "invalid_type_xyz"},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────── GET /v1/explain — UUID parse ───────────────────────────


def test_get_explain_invalid_uuid_returns_422(client):
    """Non-UUID node_id → 422 (raised explicitly before builder is called)."""
    resp = client.get("/v1/explain?node_id=not-a-uuid", headers=AUTH_HEADERS)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────── POST /v1/simulate — non-regression #48 ───────────────────────────


def test_post_simulate_disallowed_field_name_returns_422(client):
    """
    POST /simulate with a field_name outside the OverrideIn whitelist must
    fail Pydantic validation with 422, before any DB / ScenarioManager call.
    Uses a clearly bogus field name ('totally_not_a_field') — never present
    in ScenarioManager._ALLOWED_FIELDS, so the field_validator rejects it
    at body-parse time.
    """
    resp = client.post(
        "/v1/simulate",
        json={
            "scenario_name": "bad-field-sim",
            "overrides": [
                {
                    "node_id": "00000000-0000-0000-0000-000000000000",
                    "field_name": "totally_not_a_field",  # not whitelisted
                    "new_value": "0",
                }
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT, resp.text
    # FastAPI's default 422 envelope: {"detail": [{"loc": [...], "msg": ..., ...}]}
    body = resp.json()
    assert "detail" in body
    detail = body["detail"]
    assert isinstance(detail, list) and len(detail) >= 1
    # The offending field path should be echoed (loc tuple includes "field_name")
    assert any("field_name" in str(d.get("loc", [])) for d in detail)


def test_post_simulate_invalid_node_uuid_returns_422(client):
    """An override with a non-UUID node_id → 422 before DB."""
    resp = client.post(
        "/v1/simulate",
        json={
            "scenario_name": "bad-node-uuid",
            "overrides": [
                {
                    "node_id": "not-a-uuid",
                    "field_name": "quantity",
                    "new_value": "10",
                }
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────── OpenAPI / docs gating ───────────────────────────


def test_openapi_disabled_by_default(client):
    """OpenAPI schema is disabled unless OOTILS_ENABLE_API_DOCS is set."""
    resp = client.get("/openapi.json")
    assert resp.status_code == 404


def test_openapi_enabled_when_docs_flag_set(monkeypatch):
    monkeypatch.setenv("OOTILS_ENABLE_API_DOCS", "1")
    application = create_app()

    def override_db():
        yield _ForbiddenDB()

    application.dependency_overrides[get_db] = override_db

    with TestClient(application) as c:
        resp = c.get("/openapi.json")
        assert resp.status_code == 200
        schema = resp.json()
        assert schema["info"]["title"] == "Ootils Core API"
        assert schema["info"]["version"] == "1.0.0"

        docs_resp = c.get("/docs")
        assert docs_resp.status_code == 200
        assert "Swagger UI" in docs_resp.text

    application.dependency_overrides.clear()


# ─────────────────────────── Router introspection ───────────────────────────


class TestRouterConfiguration:
    """Sanity: M6 routers are registered and use require_auth."""

    def test_events_router_registered(self):
        app = create_app()
        events_routes = [
            r for r in app.routes if hasattr(r, "path") and r.path == "/v1/events"
        ]
        assert len(events_routes) > 0

    def test_projection_router_registered(self):
        app = create_app()
        proj_routes = [
            r for r in app.routes if hasattr(r, "path") and r.path.startswith("/v1/projection")
        ]
        assert len(proj_routes) > 0

    def test_issues_router_registered(self):
        app = create_app()
        issues_routes = [
            r for r in app.routes if hasattr(r, "path") and r.path == "/v1/issues"
        ]
        assert len(issues_routes) > 0

    def test_explain_router_registered(self):
        app = create_app()
        explain_routes = [
            r for r in app.routes if hasattr(r, "path") and r.path == "/v1/explain"
        ]
        assert len(explain_routes) > 0

    def test_simulate_router_registered(self):
        app = create_app()
        sim_routes = [
            r for r in app.routes if hasattr(r, "path") and r.path == "/v1/simulate"
        ]
        assert len(sim_routes) > 0

    def test_graph_router_registered(self):
        app = create_app()
        graph_routes = [
            r for r in app.routes if hasattr(r, "path") and r.path == "/v1/graph"
        ]
        assert len(graph_routes) > 0

    def test_simulate_router_uses_auth(self):
        from ootils_core.api.routers import simulate
        assert hasattr(simulate, "require_scope")

    def test_events_router_uses_auth(self):
        from ootils_core.api.routers import events
        assert hasattr(events, "require_scope")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
