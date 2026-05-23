"""
Slim tests for the Ghosts FastAPI router (src/ootils_core/api/routers/ghosts.py).

Keeps ONLY the tests that legitimately do not need a database:
  - Pydantic-validation 422 boundary tests (ghost_type, status, role,
    transition_curve allowed values + non-empty name + weight ranges).
  - Membership-constraint 422 tests for _validate_membership — these run
    on parsed Pydantic input, *before* any DB call.
  - 401 auth tests.
  - Router-introspection tests.

Every test that used to mock DB execute() / patch run_ghost was ported to
tests/integration/test_router_ghosts_integration.py against a real
PostgreSQL database, per the "no mocks" rule (CLAUDE.md).

The TestClient here overrides ``get_db`` with a sentinel that raises if
touched — proving validation / auth fires *before* any DB access happens.
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
    """TestClient with DB forbidden AND auth NOT overridden (401 tests)."""
    app = create_app()

    def override_db():
        yield _ForbiddenDB()

    app.dependency_overrides[get_db] = override_db
    return TestClient(app)


# ─────────────────────────────────────────────────────────────
# Auth (401)
# ─────────────────────────────────────────────────────────────


class TestGhostsRouterAuth:
    def test_ingest_ghost_requires_auth(self):
        client = _make_client_no_auth_override()
        resp = client.post(
            "/v1/ingest/ghosts",
            json={"name": "g1", "ghost_type": "phase_transition", "members": []},
        )
        assert resp.status_code == 401

    def test_list_ghosts_requires_auth(self):
        client = _make_client_no_auth_override()
        resp = client.get("/v1/ghosts")
        assert resp.status_code == 401

    def test_get_ghost_requires_auth(self):
        client = _make_client_no_auth_override()
        resp = client.get(f"/v1/ghosts/{uuid4()}")
        assert resp.status_code == 401

    def test_run_ghost_requires_auth(self):
        client = _make_client_no_auth_override()
        resp = client.post(
            f"/v1/ghosts/{uuid4()}/run",
            json={
                "scenario_id": str(uuid4()),
                "from_date": "2026-01-01",
                "to_date": "2026-01-31",
            },
        )
        assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# Pydantic field validation (422) — fires before DB access
# ─────────────────────────────────────────────────────────────


class TestGhostIngestPydanticValidation:
    def test_empty_name_rejected(self):
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/ghosts",
            json={"name": "   ", "ghost_type": "phase_transition", "members": []},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_invalid_ghost_type(self):
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/ghosts",
            json={"name": "g1", "ghost_type": "bogus", "members": []},
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_invalid_status(self):
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/ghosts",
            json={
                "name": "g1",
                "ghost_type": "phase_transition",
                "status": "bogus",
                "members": [],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_member_invalid_role(self):
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/ghosts",
            json={
                "name": "g1",
                "ghost_type": "phase_transition",
                "members": [{"item_id": str(uuid4()), "role": "bogus"}],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    def test_member_invalid_curve(self):
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/ghosts",
            json={
                "name": "g1",
                "ghost_type": "phase_transition",
                "members": [
                    {
                        "item_id": str(uuid4()),
                        "role": "incoming",
                        "transition_curve": "bogus",
                    }
                ],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


# ─────────────────────────────────────────────────────────────
# Membership-constraint validation (422) — pure Python on parsed input,
# fires before DB queries (only if body.members is non-empty).
# ─────────────────────────────────────────────────────────────


class TestGhostMembershipConstraints:
    def test_phase_transition_missing_outgoing(self):
        item_a = uuid4()
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/ghosts",
            json={
                "name": "g1",
                "ghost_type": "phase_transition",
                "members": [{"item_id": str(item_a), "role": "incoming"}],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert any("outgoing" in str(d) for d in detail)

    def test_phase_transition_extra_member_role(self):
        a, b, c = uuid4(), uuid4(), uuid4()
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/ghosts",
            json={
                "name": "g1",
                "ghost_type": "phase_transition",
                "members": [
                    {"item_id": str(a), "role": "outgoing"},
                    {"item_id": str(b), "role": "incoming"},
                    {"item_id": str(c), "role": "member"},
                ],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert any("member" in str(d) for d in detail)

    def test_phase_transition_two_outgoing(self):
        """Hit the incoming_count != 1 branch separately from outgoing branch."""
        a, b = uuid4(), uuid4()
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/ghosts",
            json={
                "name": "g1",
                "ghost_type": "phase_transition",
                "members": [
                    {"item_id": str(a), "role": "outgoing"},
                    {"item_id": str(b), "role": "outgoing"},
                ],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert any("incoming" in str(d) for d in detail)

    def test_capacity_aggregate_no_members_with_wrong_role(self):
        """capacity_aggregate requires >=1 member with role='member'.
        A single 'incoming' member triggers both 'at least 1 member' and
        the 'cannot have incoming/outgoing' branches.
        """
        a = uuid4()
        client = _make_client_no_db()
        resp = client.post(
            "/v1/ingest/ghosts",
            json={
                "name": "g1",
                "ghost_type": "capacity_aggregate",
                "members": [{"item_id": str(a), "role": "incoming"}],
            },
            headers=AUTH_HEADERS,
        )
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert any("at least 1 member" in str(d) for d in detail)
        assert any("incoming" in str(d) for d in detail)


# ─────────────────────────────────────────────────────────────
# Router introspection
# ─────────────────────────────────────────────────────────────


class TestGhostsRouterConfiguration:
    def test_router_tags(self):
        from ootils_core.api.routers import ghosts
        assert "ghosts" in ghosts.router.tags

    def test_router_registered_in_app(self):
        app = create_app()
        ghost_routes = [
            r for r in app.routes if hasattr(r, "path") and "/ghosts" in r.path
        ]
        ingest_routes = [
            r for r in app.routes if hasattr(r, "path") and r.path == "/v1/ingest/ghosts"
        ]
        assert len(ghost_routes) > 0
        assert len(ingest_routes) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
