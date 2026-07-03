from __future__ import annotations

import os
from uuid import uuid4

from fastapi import status
from fastapi.testclient import TestClient

os.environ["OOTILS_API_TOKEN"] = "test-token"

from ootils_core.api.app import create_app
from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db


AUTH_HEADERS = {"Authorization": "Bearer test-token"}


class _DBAccessForbidden(AssertionError):
    pass


class _ForbiddenDB:
    def __getattr__(self, name):
        raise _DBAccessForbidden(f"Validation test reached DB through {name!r}")


def _make_client_no_db() -> TestClient:
    app = create_app()

    def override_db():
        yield _ForbiddenDB()

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[require_auth] = lambda: "test-token"
    return TestClient(app)


def test_create_pyramide_run_rejects_unsupported_method_before_db():
    # SEASONAL is a supported method now — use a genuinely unknown name.
    client = _make_client_no_db()
    response = client.post(
        "/v1/forecast/runs",
        json={
            "item_id": "ITEM-001",
            "location_id": "LOC-001",
            "method": "NOT_A_METHOD",
        },
        headers=AUTH_HEADERS,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


def test_create_pyramide_run_rejects_bad_granularity_before_db():
    client = _make_client_no_db()
    response = client.post(
        "/v1/forecast/runs",
        json={
            "item_id": "ITEM-001",
            "location_id": "LOC-001",
            "granularity": "hourly",
        },
        headers=AUTH_HEADERS,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


def test_get_pyramide_run_rejects_invalid_uuid_before_db():
    client = _make_client_no_db()
    response = client.get("/v1/forecast/runs/not-a-uuid", headers=AUTH_HEADERS)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


def test_snapshot_diff_requires_compare_to_before_db():
    client = _make_client_no_db()
    response = client.get(f"/v1/forecast/snapshots/{uuid4()}/diff", headers=AUTH_HEADERS)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


def test_pyramide_router_registered_in_app():
    app = create_app()
    pyramide_routes = [
        route for route in app.routes
        if hasattr(route, "path") and route.path.startswith("/v1/forecast")
    ]

    assert len(pyramide_routes) >= 6
