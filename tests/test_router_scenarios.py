"""
test_router_scenarios.py — Unit tests for src/ootils_core/api/routers/scenarios.py.

Covers:
  - GET    /v1/scenarios            (list, status filter, pagination)
  - GET    /v1/scenarios/{id}       (get, 404)
  - DELETE /v1/scenarios/{id}       (archive, 400 for baseline, 404)
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

AUTH = {"Authorization": "Bearer test-token"}


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


def _scenario_row(*, scenario_id=None, name="Sc-1", status="active",
                  is_baseline=False, parent=None, created_at=None):
    return {
        "scenario_id": scenario_id or uuid4(),
        "name": name,
        "status": status,
        "is_baseline": is_baseline,
        "parent_scenario_id": parent,
        "created_at": created_at or datetime.now(timezone.utc),
        "updated_at": created_at or datetime.now(timezone.utc),
    }


def _setup_executes(db_mock: MagicMock, results: list):
    cursors = []
    for r in results:
        cur = MagicMock()
        if isinstance(r, list):
            cur.fetchall.return_value = r
            cur.fetchone.return_value = r[0] if r else None
        else:
            cur.fetchone.return_value = r
            cur.fetchall.return_value = [r] if r is not None else []
        cursors.append(cur)
    db_mock.execute.side_effect = cursors


# ─────────────────────────────────────────────────────────────
# GET /v1/scenarios
# ─────────────────────────────────────────────────────────────

def test_list_scenarios_no_filter():
    db = _make_db_mock()
    rows = [_scenario_row(), _scenario_row(is_baseline=True)]
    _setup_executes(db, [{"cnt": 2}, rows])

    client = _make_client(db)
    resp = client.get("/v1/scenarios", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 2
    assert len(body["scenarios"]) == 2


def test_list_scenarios_with_status_filter():
    db = _make_db_mock()
    rows = [_scenario_row(status="archived")]
    _setup_executes(db, [{"cnt": 1}, rows])

    client = _make_client(db)
    resp = client.get("/v1/scenarios?status=archived", headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["total"] == 1


def test_list_scenarios_total_row_none_branch():
    """count_row=None → total falls back to 0."""
    db = _make_db_mock()
    _setup_executes(db, [None, []])
    client = _make_client(db)
    resp = client.get("/v1/scenarios", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


def test_list_scenarios_with_parent():
    """Scenario with parent_scenario_id set — exercises optional UUID branch."""
    db = _make_db_mock()
    parent_id = uuid4()
    rows = [_scenario_row(parent=parent_id)]
    _setup_executes(db, [{"cnt": 1}, rows])

    client = _make_client(db)
    resp = client.get("/v1/scenarios", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["scenarios"][0]["parent_scenario_id"] == str(parent_id)


def test_list_scenarios_created_at_as_string():
    """Row created_at lacks isoformat → str() fallback path."""
    db = _make_db_mock()
    rows = [_scenario_row(created_at="2026-04-08T00:00:00")]
    _setup_executes(db, [{"cnt": 1}, rows])

    client = _make_client(db)
    resp = client.get("/v1/scenarios", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["scenarios"][0]["created_at"] == "2026-04-08T00:00:00"


def test_list_scenarios_invalid_limit_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/scenarios?limit=99999", headers=AUTH)
    assert resp.status_code == 422


def test_list_scenarios_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/scenarios")
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# GET /v1/scenarios/{id}
# ─────────────────────────────────────────────────────────────

def test_get_scenario_success():
    db = _make_db_mock()
    sid = uuid4()
    cur = MagicMock()
    cur.fetchone.return_value = _scenario_row(scenario_id=sid)
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get(f"/v1/scenarios/{sid}", headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["scenario_id"] == str(sid)


def test_get_scenario_404():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get(f"/v1/scenarios/{uuid4()}", headers=AUTH)
    assert resp.status_code == 404


def test_get_scenario_invalid_uuid_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/scenarios/not-a-uuid", headers=AUTH)
    assert resp.status_code == 422


# ─────────────────────────────────────────────────────────────
# DELETE /v1/scenarios/{id}
# ─────────────────────────────────────────────────────────────

def test_delete_scenario_success():
    db = _make_db_mock()
    sid = uuid4()
    cursors = [
        MagicMock(fetchone=MagicMock(return_value={"scenario_id": sid, "is_baseline": False})),
        MagicMock(),  # UPDATE
    ]
    db.execute.side_effect = cursors

    client = _make_client(db)
    resp = client.delete(f"/v1/scenarios/{sid}", headers=AUTH)
    assert resp.status_code == 204


def test_delete_scenario_baseline_id_400():
    """Cannot delete the sentinel baseline scenario."""
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.delete(f"/v1/scenarios/{BASELINE_SCENARIO_ID}", headers=AUTH)
    assert resp.status_code == 400
    assert "baseline" in resp.json()["detail"].lower()


def test_delete_scenario_404_unknown():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    db.execute.return_value = cur
    client = _make_client(db)
    resp = client.delete(f"/v1/scenarios/{uuid4()}", headers=AUTH)
    assert resp.status_code == 404


def test_delete_scenario_is_baseline_400():
    """Found row but it's flagged as baseline → 400."""
    db = _make_db_mock()
    sid = uuid4()
    cur = MagicMock()
    cur.fetchone.return_value = {"scenario_id": sid, "is_baseline": True}
    db.execute.return_value = cur
    client = _make_client(db)
    resp = client.delete(f"/v1/scenarios/{sid}", headers=AUTH)
    assert resp.status_code == 400


def test_delete_scenario_invalid_uuid_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.delete("/v1/scenarios/not-a-uuid", headers=AUTH)
    assert resp.status_code == 422
