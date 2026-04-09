"""
test_router_events.py — Unit tests for src/ootils_core/api/routers/events.py.

Covers POST /v1/events (create event) and GET /v1/events (list events),
plus the _build_propagation_engine helper.
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
from ootils_core.api.routers import events as events_module


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
# _build_propagation_engine
# ─────────────────────────────────────────────────────────────

def test_build_propagation_engine_returns_engine_with_wired_components():
    db = _make_db_mock()
    engine = events_module._build_propagation_engine(db)
    assert engine is not None
    # Sanity: it should have a process_event method
    assert hasattr(engine, "process_event")


# ─────────────────────────────────────────────────────────────
# POST /v1/events
# ─────────────────────────────────────────────────────────────

def test_create_event_minimal_no_trigger_returns_queued():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(
        "/v1/events",
        json={"event_type": "onhand_updated"},
        headers=AUTH,
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "queued"
    assert body["affected_nodes_estimate"] == 0
    assert db.execute.called


def test_create_event_invalid_event_type_returns_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(
        "/v1/events",
        json={"event_type": "totally_made_up"},
        headers=AUTH,
    )
    assert resp.status_code == 422


def test_create_event_with_trigger_node_propagation_processed():
    db = _make_db_mock()
    trig_node = uuid4()

    fake_calc_run = MagicMock()
    fake_calc_run.nodes_recalculated = 7

    fake_engine = MagicMock()
    fake_engine.process_event.return_value = fake_calc_run

    with patch(
        "ootils_core.api.routers.events._build_propagation_engine",
        return_value=fake_engine,
    ):
        client = _make_client(db)
        resp = client.post(
            "/v1/events",
            json={
                "event_type": "supply_date_changed",
                "trigger_node_id": str(trig_node),
                "field_changed": "due_date",
                "new_date": "2026-04-18",
            },
            headers=AUTH,
        )

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "processed"
    assert body["affected_nodes_estimate"] == 7
    fake_engine.process_event.assert_called_once()


def test_create_event_with_trigger_node_propagation_returns_none_calc_run():
    """Engine returns None → status stays 'queued', affected=0."""
    db = _make_db_mock()
    fake_engine = MagicMock()
    fake_engine.process_event.return_value = None

    with patch(
        "ootils_core.api.routers.events._build_propagation_engine",
        return_value=fake_engine,
    ):
        client = _make_client(db)
        resp = client.post(
            "/v1/events",
            json={
                "event_type": "supply_qty_changed",
                "trigger_node_id": str(uuid4()),
            },
            headers=AUTH,
        )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "queued"
    assert body["affected_nodes_estimate"] == 0


def test_create_event_propagation_failure_swallowed():
    """If propagation raises, request still returns 202 with status='queued'."""
    db = _make_db_mock()
    fake_engine = MagicMock()
    fake_engine.process_event.side_effect = RuntimeError("propagator down")

    with patch(
        "ootils_core.api.routers.events._build_propagation_engine",
        return_value=fake_engine,
    ):
        client = _make_client(db)
        resp = client.post(
            "/v1/events",
            json={
                "event_type": "supply_date_changed",
                "trigger_node_id": str(uuid4()),
            },
            headers=AUTH,
        )
    assert resp.status_code == 202
    assert resp.json()["status"] == "queued"


def test_create_event_with_baseline_keyword_in_body():
    """body.scenario_id='baseline' → falls through to dependency-resolved scenario."""
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(
        "/v1/events",
        json={"event_type": "onhand_updated", "scenario_id": "baseline"},
        headers=AUTH,
    )
    assert resp.status_code == 202


def test_create_event_body_scenario_id_valid_uuid_overrides():
    """body.scenario_id with valid UUID overrides dependency-resolved scenario."""
    db = _make_db_mock()
    body_sc = uuid4()
    client = _make_client(db)
    resp = client.post(
        "/v1/events",
        json={"event_type": "onhand_updated", "scenario_id": str(body_sc)},
        headers=AUTH,
    )
    assert resp.status_code == 202
    assert resp.json()["scenario_id"] == str(body_sc)


def test_create_event_body_scenario_id_invalid_uuid_falls_back():
    """body.scenario_id is not valid UUID → falls back to dependency value (baseline)."""
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post(
        "/v1/events",
        json={"event_type": "onhand_updated", "scenario_id": "not-a-uuid"},
        headers=AUTH,
    )
    assert resp.status_code == 202
    # Should NOT be the garbage value — should be baseline
    assert resp.json()["scenario_id"] == "00000000-0000-0000-0000-000000000001"


def test_create_event_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.post("/v1/events", json={"event_type": "onhand_updated"})
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# GET /v1/events
# ─────────────────────────────────────────────────────────────

def _event_row():
    return {
        "event_id": uuid4(),
        "event_type": "supply_date_changed",
        "scenario_id": uuid4(),
        "trigger_node_id": uuid4(),
        "field_changed": "due_date",
        "old_date": None,
        "new_date": None,
        "old_quantity": None,
        "new_quantity": None,
        "processed": True,
        "source": "api",
        "created_at": datetime.now(timezone.utc),
    }


def test_list_events_no_filters():
    db = _make_db_mock()
    rows = [_event_row(), _event_row()]
    _setup_executes(db, [{"total": 2}, rows])

    client = _make_client(db)
    resp = client.get("/v1/events", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 2
    assert len(body["events"]) == 2
    assert body["limit"] == 50
    assert body["offset"] == 0


def test_list_events_count_row_none():
    """count_row is None → total=0 (defensive branch)."""
    db = _make_db_mock()
    _setup_executes(db, [None, []])

    client = _make_client(db)
    resp = client.get("/v1/events", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


def test_list_events_count_row_tuple():
    """count_row not dict but supports indexing → uses row[0]."""
    db = _make_db_mock()
    cursor1 = MagicMock()
    cursor1.fetchone.return_value = (5,)  # tuple, not a dict
    cursor2 = MagicMock()
    cursor2.fetchall.return_value = []
    db.execute.side_effect = [cursor1, cursor2]

    client = _make_client(db)
    resp = client.get("/v1/events", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total"] == 5


def test_list_events_with_event_type_filter():
    db = _make_db_mock()
    _setup_executes(db, [{"total": 1}, [_event_row()]])
    client = _make_client(db)
    resp = client.get("/v1/events?event_type=supply_date_changed", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total"] == 1


def test_list_events_invalid_event_type_filter_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/events?event_type=garbage_type", headers=AUTH)
    assert resp.status_code == 422


def test_list_events_with_scenario_id_filter():
    db = _make_db_mock()
    sid = uuid4()
    _setup_executes(db, [{"total": 0}, []])
    client = _make_client(db)
    resp = client.get(f"/v1/events?scenario_id={sid}", headers=AUTH)
    assert resp.status_code == 200


def test_list_events_invalid_scenario_id_filter_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/events?scenario_id=not-a-uuid", headers=AUTH)
    assert resp.status_code == 422


def test_list_events_with_processed_filter():
    db = _make_db_mock()
    _setup_executes(db, [{"total": 1}, [_event_row()]])
    client = _make_client(db)
    resp = client.get("/v1/events?processed=true", headers=AUTH)
    assert resp.status_code == 200


def test_list_events_invalid_limit_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/events?limit=99999", headers=AUTH)
    assert resp.status_code == 422


def test_list_events_row_as_tuple():
    """rows returned as tuples (not dicts) — exercises tuple-fallback branch in _make_record."""
    db = _make_db_mock()
    cur1 = MagicMock()
    cur1.fetchone.return_value = {"total": 1}
    cur2 = MagicMock()
    tup = (
        uuid4(),                      # 0  event_id
        "supply_date_changed",        # 1  event_type
        uuid4(),                      # 2  scenario_id
        uuid4(),                      # 3  trigger_node_id
        "due_date",                   # 4  field_changed
        None,                         # 5  old_date
        None,                         # 6  new_date
        None,                         # 7  old_quantity
        None,                         # 8  new_quantity
        True,                         # 9  processed
        None,                         # 10 source -> None → defaults to 'api'
        datetime.now(timezone.utc),   # 11 created_at
    )
    cur2.fetchall.return_value = [tup]
    db.execute.side_effect = [cur1, cur2]

    client = _make_client(db)
    resp = client.get("/v1/events", headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["events"]) == 1
    assert resp.json()["events"][0]["source"] == "api"


def test_list_events_dict_row_with_string_created_at():
    """row dict where created_at lacks isoformat — exercises str() fallback."""
    db = _make_db_mock()
    cur1 = MagicMock()
    cur1.fetchone.return_value = {"total": 1}
    row = _event_row()
    row["created_at"] = "2026-04-08T00:00:00"  # plain str, no isoformat
    cur2 = MagicMock()
    cur2.fetchall.return_value = [row]
    db.execute.side_effect = [cur1, cur2]

    client = _make_client(db)
    resp = client.get("/v1/events", headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["events"][0]["created_at"] == "2026-04-08T00:00:00"


def test_list_events_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/events")
    assert resp.status_code == 401
