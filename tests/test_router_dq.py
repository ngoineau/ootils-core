"""
test_router_dq.py — Unit tests for src/ootils_core/api/routers/dq.py.

All DB and engine calls are mocked via FastAPI dependency overrides + patches.
Covers: POST /run/{batch_id}, GET /{batch_id}, GET /issues,
POST /agent/run/{batch_id}, GET /agent/report/{batch_id}, GET /agent/runs.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Generator
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import get_db


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


AUTH = {"Authorization": "Bearer test-token"}


def _setup_executes(db_mock: MagicMock, results: list):
    """Make db.execute() return MagicMocks whose fetchone/fetchall return successive results."""
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
# POST /v1/dq/run/{batch_id}
# ─────────────────────────────────────────────────────────────

def test_run_dq_batch_success():
    db = _make_db_mock()
    batch_id = uuid4()

    # 1st execute: SELECT batch — returns row
    cur = MagicMock()
    cur.fetchone.return_value = {"batch_id": batch_id}
    db.execute.return_value = cur

    fake_result = MagicMock()
    fake_result.batch_id = batch_id
    fake_result.total_rows = 10
    fake_result.passed_rows = 8
    fake_result.failed_rows = 1
    fake_result.warning_rows = 1
    fake_result.issues = []
    fake_result.batch_dq_status = "ok"

    with patch("ootils_core.api.routers.dq.run_dq", return_value=fake_result):
        client = _make_client(db)
        resp = client.post(f"/v1/dq/run/{batch_id}", headers=AUTH)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "completed"
    assert body["total_rows"] == 10
    assert body["batch_dq_status"] == "ok"


def test_run_dq_batch_404_when_batch_missing():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.post(f"/v1/dq/run/{uuid4()}", headers=AUTH)
    assert resp.status_code == 404


def test_run_dq_batch_500_when_engine_raises():
    db = _make_db_mock()
    batch_id = uuid4()
    cur = MagicMock()
    cur.fetchone.return_value = {"batch_id": batch_id}
    db.execute.return_value = cur

    with patch("ootils_core.api.routers.dq.run_dq", side_effect=RuntimeError("dq exploded")):
        client = _make_client(db)
        resp = client.post(f"/v1/dq/run/{batch_id}", headers=AUTH)

    assert resp.status_code == 500
    assert "DQ run failed" in resp.json()["detail"]


def test_run_dq_batch_invalid_uuid_returns_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post("/v1/dq/run/not-a-uuid", headers=AUTH)
    assert resp.status_code == 422


def test_run_dq_batch_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(f"/v1/dq/run/{uuid4()}")
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# GET /v1/dq/issues
# ─────────────────────────────────────────────────────────────

def _issue_row():
    return {
        "issue_id": uuid4(),
        "batch_id": uuid4(),
        "row_id": uuid4(),
        "row_number": 1,
        "dq_level": 1,
        "rule_code": "L1_REQUIRED",
        "severity": "error",
        "field_name": "name",
        "raw_value": None,
        "message": "missing",
        "auto_corrected": False,
        "resolved": False,
        "created_at": datetime.now(timezone.utc),
    }


def test_list_issues_no_filters():
    db = _make_db_mock()
    _setup_executes(db, [{"cnt": 2}, [_issue_row(), _issue_row()]])

    client = _make_client(db)
    resp = client.get("/v1/dq/issues", headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["total"] == 2
    assert len(resp.json()["issues"]) == 2


def test_list_issues_all_filters():
    db = _make_db_mock()
    _setup_executes(db, [{"cnt": 1}, [_issue_row()]])

    client = _make_client(db)
    resp = client.get(
        "/v1/dq/issues?severity=error&dq_level=1&entity_type=items&limit=10&offset=0",
        headers=AUTH,
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 1


def test_list_issues_invalid_limit_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/dq/issues?limit=99999", headers=AUTH)
    assert resp.status_code == 422


def test_list_issues_invalid_offset_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/dq/issues?offset=-1", headers=AUTH)
    assert resp.status_code == 422


# ─────────────────────────────────────────────────────────────
# GET /v1/dq/{batch_id}
# ─────────────────────────────────────────────────────────────

def test_get_batch_dq_success():
    db = _make_db_mock()
    batch_id = uuid4()
    batch_row = {
        "batch_id": batch_id,
        "entity_type": "items",
        "dq_status": "ok",
        "total_rows": 5,
    }
    _setup_executes(db, [batch_row, [_issue_row()]])

    client = _make_client(db)
    resp = client.get(f"/v1/dq/{batch_id}", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["entity_type"] == "items"
    assert body["dq_status"] == "ok"
    assert body["total_rows"] == 5
    assert len(body["issues"]) == 1


def test_get_batch_dq_total_rows_none_defaults_to_zero():
    db = _make_db_mock()
    batch_id = uuid4()
    batch_row = {
        "batch_id": batch_id,
        "entity_type": "items",
        "dq_status": None,
        "total_rows": None,
    }
    _setup_executes(db, [batch_row, []])

    client = _make_client(db)
    resp = client.get(f"/v1/dq/{batch_id}", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total_rows"] == 0
    assert resp.json()["dq_status"] is None


def test_get_batch_dq_404():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get(f"/v1/dq/{uuid4()}", headers=AUTH)
    assert resp.status_code == 404


# ─────────────────────────────────────────────────────────────
# POST /v1/dq/agent/run/{batch_id}
# ─────────────────────────────────────────────────────────────

def test_run_agent_batch_success():
    db = _make_db_mock()
    batch_id = uuid4()
    run_id = uuid4()
    cur = MagicMock()
    cur.fetchone.return_value = {"batch_id": batch_id}
    db.execute.return_value = cur

    fake_run = MagicMock()
    fake_run.run_id = run_id
    fake_run.batch_id = batch_id
    fake_run.status = "completed"

    with patch("ootils_core.engine.dq.agent.run_dq_agent", return_value=fake_run):
        client = _make_client(db)
        resp = client.post(f"/v1/dq/agent/run/{batch_id}", headers=AUTH)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["run_id"] == str(run_id)
    assert body["status"] == "completed"


def test_run_agent_batch_404_no_batch():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    db.execute.return_value = cur
    client = _make_client(db)
    resp = client.post(f"/v1/dq/agent/run/{uuid4()}", headers=AUTH)
    assert resp.status_code == 404


def test_run_agent_batch_500_on_engine_error():
    db = _make_db_mock()
    batch_id = uuid4()
    cur = MagicMock()
    cur.fetchone.return_value = {"batch_id": batch_id}
    db.execute.return_value = cur

    with patch(
        "ootils_core.engine.dq.agent.run_dq_agent", side_effect=RuntimeError("agent boom")
    ):
        client = _make_client(db)
        resp = client.post(f"/v1/dq/agent/run/{batch_id}", headers=AUTH)
    assert resp.status_code == 500
    assert "DQ Agent run failed" in resp.json()["detail"]


# ─────────────────────────────────────────────────────────────
# GET /v1/dq/agent/report/{batch_id}
# ─────────────────────────────────────────────────────────────

def _agent_issue_row():
    return {
        "issue_id": uuid4(),
        "batch_id": uuid4(),
        "row_id": uuid4(),
        "row_number": 2,
        "dq_level": 2,
        "rule_code": "L2_REF",
        "severity": "warning",
        "field_name": "supplier",
        "raw_value": "x",
        "message": "warn",
        "impact_score": 0.85,
        "agent_run_id": uuid4(),
        "llm_explanation": "exp",
        "llm_suggestion": "fix",
    }


def test_get_agent_report_success_with_run_and_summary_dict():
    db = _make_db_mock()
    batch_id = uuid4()
    run_id = uuid4()

    batch_row = {"batch_id": batch_id, "entity_type": "items"}
    agent_run_row = {
        "run_id": run_id,
        "status": "completed",
        "completed_at": datetime.now(timezone.utc),
        "summary": {"priority_actions": ["fix-1", "fix-2"]},
        "llm_narrative": "narrative-text",
    }
    issue_rows = [_agent_issue_row(), _agent_issue_row()]
    _setup_executes(db, [batch_row, agent_run_row, issue_rows])

    client = _make_client(db)
    resp = client.get(f"/v1/dq/agent/report/{batch_id}", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["entity_type"] == "items"
    assert body["status"] == "completed"
    assert body["narrative"] == "narrative-text"
    assert body["priority_actions"] == ["fix-1", "fix-2"]
    assert len(body["issues"]) == 2


def test_get_agent_report_summary_as_json_string():
    db = _make_db_mock()
    batch_id = uuid4()
    run_id = uuid4()

    batch_row = {"batch_id": batch_id, "entity_type": "items"}
    agent_run_row = {
        "run_id": run_id,
        "status": "completed",
        "completed_at": datetime.now(timezone.utc),
        "summary": '{"priority_actions": ["a"]}',
        "llm_narrative": None,
    }
    _setup_executes(db, [batch_row, agent_run_row, []])

    client = _make_client(db)
    resp = client.get(f"/v1/dq/agent/report/{batch_id}", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["priority_actions"] == ["a"]


def test_get_agent_report_summary_non_dict_returns_empty_actions():
    """summary present but not a dict → priority_actions stays []."""
    db = _make_db_mock()
    batch_id = uuid4()
    batch_row = {"batch_id": batch_id, "entity_type": "items"}
    agent_run_row = {
        "run_id": uuid4(),
        "status": "completed",
        "completed_at": datetime.now(timezone.utc),
        "summary": ["not-a-dict"],
        "llm_narrative": None,
    }
    _setup_executes(db, [batch_row, agent_run_row, []])

    client = _make_client(db)
    resp = client.get(f"/v1/dq/agent/report/{batch_id}", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["priority_actions"] == []


def test_get_agent_report_no_agent_run():
    """No prior agent run → narrative/summary/run_id all null."""
    db = _make_db_mock()
    batch_id = uuid4()
    batch_row = {"batch_id": batch_id, "entity_type": "items"}
    _setup_executes(db, [batch_row, None, []])

    client = _make_client(db)
    resp = client.get(f"/v1/dq/agent/report/{batch_id}", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] is None
    assert body["narrative"] is None
    assert body["priority_actions"] == []


def test_get_agent_report_404_no_batch():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get(f"/v1/dq/agent/report/{uuid4()}", headers=AUTH)
    assert resp.status_code == 404


def test_get_agent_report_issue_with_null_impact_score():
    """impact_score=None branch — no float() coercion."""
    db = _make_db_mock()
    batch_id = uuid4()
    issue = _agent_issue_row()
    issue["impact_score"] = None
    _setup_executes(
        db,
        [{"batch_id": batch_id, "entity_type": "items"}, None, [issue]],
    )

    client = _make_client(db)
    resp = client.get(f"/v1/dq/agent/report/{batch_id}", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["issues"][0]["impact_score"] is None


# ─────────────────────────────────────────────────────────────
# GET /v1/dq/agent/runs
# ─────────────────────────────────────────────────────────────

def _agent_run_record():
    return {
        "run_id": uuid4(),
        "batch_id": uuid4(),
        "status": "completed",
        "model_used": "claude-sonnet",
        "started_at": datetime.now(timezone.utc),
        "completed_at": datetime.now(timezone.utc),
        "summary": {"k": "v"},
        "created_at": datetime.now(timezone.utc),
    }


def test_list_agent_runs_success():
    db = _make_db_mock()
    rows = [_agent_run_record(), _agent_run_record()]
    _setup_executes(db, [{"cnt": 2}, rows])

    client = _make_client(db)
    resp = client.get("/v1/dq/agent/runs", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 2
    assert len(body["runs"]) == 2


def test_list_agent_runs_empty():
    db = _make_db_mock()
    _setup_executes(db, [{"cnt": 0}, []])
    client = _make_client(db)
    resp = client.get("/v1/dq/agent/runs", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total"] == 0
    assert resp.json()["runs"] == []


def test_list_agent_runs_invalid_limit_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/dq/agent/runs?limit=99999", headers=AUTH)
    assert resp.status_code == 422
