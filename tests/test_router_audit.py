"""
tests/test_router_audit.py — Unit tests for src/ootils_core/api/routers/audit.py
(PROD-QW: GET /v1/audit, the admin-scoped read of the api_request_log trail).

Pure unit tests: the DB is a MagicMock (get_db override), so nothing here needs
PostgreSQL. Because get_db is overridden, _should_audit_request() returns False
and the request-log middleware never fires — the router logic is exercised in
isolation. The DB-backed pagination/filter behaviour is covered blind in
tests/integration/test_prod_qw_integration.py.

What this file pins:
  * ADMIN scope required — the legacy token (admin superset) passes; a non-admin
    principal (resolve_principal override) is 403; no token is 401.
  * 422 on an unknown actor_kind (before any DB access).
  * limit is bounded by FastAPI validation (le=200, ge=1) → 422, not a clamp.
  * The response entries carry token_prefix / token_id but NEVER a raw token or
    a token_hash (the table stores neither — the shape must not invent them).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi.testclient import TestClient

# auth.py validates OOTILS_API_TOKEN at import time — set before importing app.
os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.app import create_app
from ootils_core.api.auth import Principal, resolve_principal
from ootils_core.api.dependencies import get_db

AUTH = {"Authorization": "Bearer test-token"}


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────


def _make_db_mock() -> MagicMock:
    conn = MagicMock(name="psycopg_conn")
    conn.execute.return_value = MagicMock()
    return conn


def _make_client(db_mock: MagicMock) -> TestClient:
    app = create_app()

    def override_db():
        yield db_mock

    app.dependency_overrides[get_db] = override_db
    return TestClient(app)


def _setup_count_then_rows(db_mock: MagicMock, total: int, rows: list) -> None:
    """Wire two sequential execute() calls: the COUNT(*), then the page SELECT."""
    count_cur = MagicMock()
    count_cur.fetchone.return_value = {"total": total}
    rows_cur = MagicMock()
    rows_cur.fetchall.return_value = rows
    db_mock.execute.side_effect = [count_cur, rows_cur]


def _audit_row(**over) -> dict:
    row = {
        "request_id": uuid4(),
        "correlation_id": "req_abc",
        "token_prefix": "ootk_ABCDEFG",
        "token_id": uuid4(),
        "actor_kind": "agent",
        "method": "GET",
        "path": "/v1/recommendations",
        "status_code": 200,
        "latency_ms": 12,
        "client_ip": "127.0.0.1",
        "created_at": datetime.now(timezone.utc),
    }
    row.update(over)
    return row


# ─────────────────────────────────────────────────────────────
# Auth / scope floor
# ─────────────────────────────────────────────────────────────


def test_audit_requires_auth():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/audit")
    assert resp.status_code == 401


def test_audit_non_admin_principal_is_403():
    """A resolved principal WITHOUT the admin scope is denied. require_scope
    ('admin') depends on resolve_principal, so overriding it with a read-only
    agent principal exercises the real 403 path."""
    db = _make_db_mock()
    app = create_app()

    def override_db():
        yield db

    def override_principal():
        return Principal(
            token_id=uuid4(),
            name="watcher",
            actor_kind="agent",
            scopes=frozenset({"read"}),  # lacks admin
            is_legacy=False,
        )

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[resolve_principal] = override_principal
    client = TestClient(app)

    resp = client.get("/v1/audit", headers=AUTH)
    assert resp.status_code == 403, resp.text
    assert resp.json()["detail"] == "missing scope 'admin'"


def test_audit_legacy_admin_token_passes():
    db = _make_db_mock()
    _setup_count_then_rows(db, total=0, rows=[])
    client = _make_client(db)
    resp = client.get("/v1/audit", headers=AUTH)
    assert resp.status_code == 200, resp.text


# ─────────────────────────────────────────────────────────────
# Validation: actor_kind + limit bounds
# ─────────────────────────────────────────────────────────────


def test_audit_invalid_actor_kind_is_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/audit?actor_kind=robot", headers=AUTH)
    assert resp.status_code == 422, resp.text
    # Raised before any DB access.
    db.execute.assert_not_called()


def test_audit_valid_actor_kind_filter_passes():
    db = _make_db_mock()
    _setup_count_then_rows(db, total=1, rows=[_audit_row(actor_kind="human")])
    client = _make_client(db)
    resp = client.get("/v1/audit?actor_kind=human", headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["total"] == 1


def test_audit_limit_over_max_is_422():
    """limit is bounded by FastAPI validation (le=200) → 422, NOT clamped."""
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/audit?limit=201", headers=AUTH)
    assert resp.status_code == 422, resp.text


def test_audit_limit_zero_is_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/audit?limit=0", headers=AUTH)
    assert resp.status_code == 422, resp.text


def test_audit_limit_at_max_is_accepted():
    db = _make_db_mock()
    _setup_count_then_rows(db, total=0, rows=[])
    client = _make_client(db)
    resp = client.get("/v1/audit?limit=200", headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["limit"] == 200


def test_audit_negative_status_code_filter_is_422():
    db = _make_db_mock()
    client = _make_client(db)
    # status_code Query is ge=100/le=599.
    resp = client.get("/v1/audit?status_code=42", headers=AUTH)
    assert resp.status_code == 422, resp.text


# ─────────────────────────────────────────────────────────────
# Response shape: never a token / token_hash
# ─────────────────────────────────────────────────────────────


def test_audit_response_never_exposes_token_or_hash():
    db = _make_db_mock()
    _setup_count_then_rows(db, total=1, rows=[_audit_row()])
    client = _make_client(db)
    resp = client.get("/v1/audit", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 1
    assert len(body["entries"]) == 1
    entry = body["entries"][0]
    # The non-secret correlation fields ARE present …
    assert "token_prefix" in entry
    assert "token_id" in entry
    # … but no raw token or token hash is ever surfaced (the table stores none).
    assert "token" not in entry
    assert "token_hash" not in entry
    for key in entry.keys():
        assert "hash" not in key.lower()
        assert key.lower() != "token"


def test_audit_pagination_echoes_limit_and_offset():
    db = _make_db_mock()
    _setup_count_then_rows(db, total=5, rows=[_audit_row(), _audit_row()])
    client = _make_client(db)
    resp = client.get("/v1/audit?limit=2&offset=2", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 5
    assert body["limit"] == 2
    assert body["offset"] == 2
    assert len(body["entries"]) == 2


def test_audit_count_row_none_defaults_total_zero():
    """Defensive branch: a None count row → total 0."""
    db = _make_db_mock()
    count_cur = MagicMock()
    count_cur.fetchone.return_value = None
    rows_cur = MagicMock()
    rows_cur.fetchall.return_value = []
    db.execute.side_effect = [count_cur, rows_cur]

    client = _make_client(db)
    resp = client.get("/v1/audit", headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["total"] == 0


def test_audit_row_with_null_token_id_serialises():
    """A legacy-token audit row (token_id NULL) must serialise cleanly."""
    db = _make_db_mock()
    _setup_count_then_rows(
        db,
        total=1,
        rows=[_audit_row(token_id=None, token_prefix="global_token", actor_kind=None)],
    )
    client = _make_client(db)
    resp = client.get("/v1/audit", headers=AUTH)
    assert resp.status_code == 200, resp.text
    entry = resp.json()["entries"][0]
    assert entry["token_id"] is None
    assert entry["token_prefix"] == "global_token"
    assert entry["actor_kind"] is None
