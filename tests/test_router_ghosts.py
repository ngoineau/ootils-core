"""
test_router_ghosts.py — unit tests for src/ootils_core/api/routers/ghosts.py.

Target: 100% coverage.

All DB calls are mocked via FastAPI dependency_overrides.
The mock builds `execute(...)` responses as dicts matching the real dict_row
output: {column: value}.
"""
from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

# Must set token BEFORE importing the app
os.environ["OOTILS_API_TOKEN"] = "test-token"

from ootils_core.api.app import create_app
from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

AUTH_HEADERS = {"Authorization": "Bearer test-token"}


def _make_execute_mock(responses: list[Any]) -> MagicMock:
    """
    Build a MagicMock for conn.execute() where each call pops the next response.
    Each response is either:
      - a MagicMock (returned directly),
      - a list of dict (treated as fetchall result),
      - a dict (treated as a single fetchone result),
      - None (treated as fetchone → None / fetchall → []),
      - "noop" (a generic empty result for write statements).
    """
    responses = list(responses)

    def execute_side_effect(*args, **kwargs):
        if not responses:
            result = MagicMock()
            result.fetchone.return_value = None
            result.fetchall.return_value = []
            result.rowcount = 0
            return result
        item = responses.pop(0)
        if isinstance(item, MagicMock):
            return item
        result = MagicMock()
        if isinstance(item, list):
            result.fetchall.return_value = item
            result.fetchone.return_value = item[0] if item else None
        elif isinstance(item, dict):
            result.fetchone.return_value = item
            result.fetchall.return_value = [item]
        elif item is None or item == "noop":
            result.fetchone.return_value = None
            result.fetchall.return_value = []
        else:
            raise TypeError(f"Unexpected response type: {type(item)}")
        result.rowcount = 1
        return result

    return MagicMock(side_effect=execute_side_effect)


def _mock_db(responses: list[Any] | None = None) -> MagicMock:
    """Return a mock psycopg3 connection with scripted execute() responses."""
    conn = MagicMock()
    if responses is None:
        responses = []
    conn.execute = _make_execute_mock(responses)
    cursor_ctx = MagicMock()
    cursor_ctx.__enter__ = MagicMock(return_value=cursor_ctx)
    cursor_ctx.__exit__ = MagicMock(return_value=False)
    cursor_ctx.executemany = MagicMock()
    conn.cursor = MagicMock(return_value=cursor_ctx)
    return conn


def _make_client(mock_conn: MagicMock) -> TestClient:
    app = create_app()

    def override_db():
        yield mock_conn

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[require_auth] = lambda: "test-token"
    return TestClient(app)


# ─────────────────────────────────────────────────────────────
# Auth tests
# ─────────────────────────────────────────────────────────────


def test_ingest_ghost_requires_auth():
    """POST /v1/ingest/ghosts rejects missing auth."""
    app = create_app()
    conn = _mock_db([])
    app.dependency_overrides[get_db] = lambda: conn
    with TestClient(app) as c:
        resp = c.post(
            "/v1/ingest/ghosts",
            json={
                "name": "g1",
                "ghost_type": "phase_transition",
                "members": [],
            },
        )
    assert resp.status_code == 401


def test_list_ghosts_requires_auth():
    app = create_app()
    conn = _mock_db([])
    app.dependency_overrides[get_db] = lambda: conn
    with TestClient(app) as c:
        resp = c.get("/v1/ghosts")
    assert resp.status_code == 401


def test_get_ghost_requires_auth():
    app = create_app()
    conn = _mock_db([])
    app.dependency_overrides[get_db] = lambda: conn
    with TestClient(app) as c:
        resp = c.get(f"/v1/ghosts/{uuid4()}")
    assert resp.status_code == 401


def test_run_ghost_requires_auth():
    app = create_app()
    conn = _mock_db([])
    app.dependency_overrides[get_db] = lambda: conn
    with TestClient(app) as c:
        resp = c.post(
            f"/v1/ghosts/{uuid4()}/run",
            json={
                "scenario_id": str(uuid4()),
                "from_date": "2025-01-01",
                "to_date": "2025-01-31",
            },
        )
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# Pydantic validation
# ─────────────────────────────────────────────────────────────


def test_ingest_ghost_empty_name_rejected():
    conn = _mock_db([])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={"name": "   ", "ghost_type": "phase_transition", "members": []},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_ghost_invalid_ghost_type():
    conn = _mock_db([])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={"name": "g1", "ghost_type": "bogus", "members": []},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_ghost_invalid_status():
    conn = _mock_db([])
    client = _make_client(conn)
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
    assert resp.status_code == 422


def test_ingest_ghost_member_invalid_role():
    conn = _mock_db([])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "g1",
            "ghost_type": "phase_transition",
            "members": [
                {"item_id": str(uuid4()), "role": "bogus"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_ghost_member_invalid_curve():
    conn = _mock_db([])
    client = _make_client(conn)
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
                },
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


# ─────────────────────────────────────────────────────────────
# Membership constraint validation
# ─────────────────────────────────────────────────────────────


def test_ingest_ghost_phase_transition_missing_outgoing():
    item_a = uuid4()
    conn = _mock_db([])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "g1",
            "ghost_type": "phase_transition",
            "members": [
                {"item_id": str(item_a), "role": "incoming"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert any("outgoing" in str(d) for d in detail)


def test_ingest_ghost_phase_transition_extra_member_role():
    """phase_transition cannot have role='member'."""
    a, b, c = uuid4(), uuid4(), uuid4()
    conn = _mock_db([])
    client = _make_client(conn)
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


def test_ingest_ghost_capacity_aggregate_no_members():
    """capacity_aggregate requires at least 1 member with role='member'."""
    a = uuid4()
    conn = _mock_db([])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "g1",
            "ghost_type": "capacity_aggregate",
            "members": [
                {"item_id": str(a), "role": "incoming"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    # both errors should be present (no members + bad role)
    assert any("at least 1 member" in str(d) for d in detail)
    assert any("incoming" in str(d) for d in detail)


# ─────────────────────────────────────────────────────────────
# Item validation
# ─────────────────────────────────────────────────────────────


def test_ingest_ghost_item_not_found():
    a = uuid4()
    conn = _mock_db([
        [],  # SELECT items where item_id = ANY → empty
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "g1",
            "ghost_type": "capacity_aggregate",
            "members": [
                {"item_id": str(a), "role": "member"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert any("not found" in str(d) for d in detail)


def test_ingest_ghost_resource_not_found():
    """resource_id provided but not in DB → 422."""
    a = uuid4()
    rid = uuid4()
    conn = _mock_db([
        [{"item_id": a}],  # items found
        None,              # resources lookup → None
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "g1",
            "ghost_type": "capacity_aggregate",
            "resource_id": str(rid),
            "members": [
                {"item_id": str(a), "role": "member"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
    assert any("resource_id not found" in str(d) for d in resp.json()["detail"])


# ─────────────────────────────────────────────────────────────
# Successful insert / update flows
# ─────────────────────────────────────────────────────────────


def test_ingest_ghost_insert_no_members():
    """Insert path: no members, no scenario_id, no resource_id."""
    conn = _mock_db([
        # no member-validation queries (members empty)
        # no resource lookup
        None,    # existing_ghost lookup → None
        "noop",  # INSERT nodes
        "noop",  # INSERT ghost_nodes
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "g_new",
            "ghost_type": "phase_transition",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["action"] == "inserted"
    assert body["member_count"] == 0
    assert body["node_id"] is not None


def test_ingest_ghost_insert_with_members_and_item_node_found():
    """Insert flow: members, scenario_id, items + item_nodes all found."""
    item_a = uuid4()
    item_b = uuid4()
    item_a_node = uuid4()
    sid = uuid4()
    conn = _mock_db([
        # 1) item validation
        [{"item_id": item_a}, {"item_id": item_b}],
        # 2) (no resource_id provided)
        # 3) existing_ghost lookup → None
        None,
        # 4) INSERT nodes
        "noop",
        # 5) INSERT ghost_nodes
        "noop",
        # 6) DELETE ghost_members
        "noop",
        # 7) DELETE edges
        "noop",
        # member 1: INSERT ghost_members, then SELECT item node, then INSERT edge
        "noop",
        {"node_id": item_a_node},
        "noop",
        # member 2: INSERT ghost_members, then SELECT item node → None, no edge
        "noop",
        None,
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "g_new",
            "ghost_type": "phase_transition",
            "scenario_id": str(sid),
            "description": "desc",
            "members": [
                {"item_id": str(item_a), "role": "outgoing"},
                {"item_id": str(item_b), "role": "incoming"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["action"] == "inserted"
    assert body["member_count"] == 2
    assert body["node_id"] is not None


def test_ingest_ghost_update_existing():
    """Update flow: existing ghost found, members reset."""
    existing_gid = uuid4()
    existing_nid = uuid4()
    item_a = uuid4()
    item_b = uuid4()
    item_a_node = uuid4()
    item_b_node = uuid4()
    rid = uuid4()
    conn = _mock_db([
        # 1) item validation
        [{"item_id": item_a}, {"item_id": item_b}],
        # 2) resource lookup
        {"resource_id": rid},
        # 3) existing_ghost lookup → found
        {"ghost_id": existing_gid, "node_id": existing_nid},
        # 4) UPDATE ghost_nodes
        "noop",
        # 5) DELETE ghost_members
        "noop",
        # 6) DELETE edges
        "noop",
        # member 1: insert + node lookup + edge insert
        "noop",
        {"node_id": item_a_node},
        "noop",
        # member 2: insert + node lookup + edge insert
        "noop",
        {"node_id": item_b_node},
        "noop",
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "g_existing",
            "ghost_type": "phase_transition",
            "resource_id": str(rid),
            "description": "updated",
            "members": [
                {"item_id": str(item_a), "role": "outgoing"},
                {"item_id": str(item_b), "role": "incoming"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["action"] == "updated"
    assert body["ghost_id"] == str(existing_gid)
    assert body["node_id"] == str(existing_nid)
    assert body["member_count"] == 2


def test_ingest_ghost_capacity_aggregate_happy_path():
    item_a = uuid4()
    item_b = uuid4()
    conn = _mock_db([
        [{"item_id": item_a}, {"item_id": item_b}],
        None,    # existing_ghost lookup → None
        "noop",  # INSERT nodes
        "noop",  # INSERT ghost_nodes
        "noop",  # DELETE ghost_members
        "noop",  # DELETE edges
        # member 1
        "noop",
        None,    # item node not found
        # member 2
        "noop",
        None,
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "g_cap",
            "ghost_type": "capacity_aggregate",
            "members": [
                {"item_id": str(item_a), "role": "member"},
                {"item_id": str(item_b), "role": "member"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["action"] == "inserted"
    assert body["member_count"] == 2


# ─────────────────────────────────────────────────────────────
# GET /v1/ghosts (list)
# ─────────────────────────────────────────────────────────────


def test_list_ghosts_empty():
    conn = _mock_db([
        [],  # ghost_rows
    ])
    client = _make_client(conn)
    resp = client.get("/v1/ghosts", headers=AUTH_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 0
    assert body["ghosts"] == []


def test_list_ghosts_with_filters_and_results():
    gid = uuid4()
    sid = uuid4()
    rid = uuid4()
    nid = uuid4()
    member_id = uuid4()
    item_id = uuid4()
    created = datetime(2025, 1, 1, 12, 0, 0)
    updated = datetime(2025, 1, 2, 12, 0, 0)
    conn = _mock_db([
        # ghost_rows
        [{
            "ghost_id": gid,
            "name": "g1",
            "ghost_type": "phase_transition",
            "scenario_id": sid,
            "resource_id": rid,
            "node_id": nid,
            "status": "active",
            "description": "desc",
            "created_at": created,
            "updated_at": updated,
        }],
        # members for that ghost
        [{
            "member_id": member_id,
            "item_id": item_id,
            "role": "incoming",
            "transition_start_date": date(2025, 1, 1),
            "transition_end_date": date(2025, 2, 1),
            "transition_curve": "linear",
            "weight_at_start": 1.0,
            "weight_at_end": 0.0,
        }],
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/ghosts",
        params={
            "ghost_type": "phase_transition",
            "scenario_id": str(sid),
            "ghost_status": "active",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    g = body["ghosts"][0]
    assert g["ghost_id"] == str(gid)
    assert g["scenario_id"] == str(sid)
    assert g["resource_id"] == str(rid)
    assert g["node_id"] == str(nid)
    assert g["created_at"].startswith("2025-01-01")
    assert g["updated_at"].startswith("2025-01-02")
    assert len(g["members"]) == 1
    m = g["members"][0]
    assert m["transition_start_date"] == "2025-01-01"
    assert m["transition_end_date"] == "2025-02-01"


def test_list_ghosts_with_null_optionals():
    """Cover the None branches in serialization for scenario/resource/node/dates."""
    gid = uuid4()
    member_id = uuid4()
    item_id = uuid4()
    conn = _mock_db([
        [{
            "ghost_id": gid,
            "name": "g_no_opts",
            "ghost_type": "capacity_aggregate",
            "scenario_id": None,
            "resource_id": None,
            "node_id": None,
            "status": "active",
            "description": None,
            "created_at": None,
            "updated_at": None,
        }],
        [{
            "member_id": member_id,
            "item_id": item_id,
            "role": "member",
            "transition_start_date": None,
            "transition_end_date": None,
            "transition_curve": "linear",
            "weight_at_start": 0.5,
            "weight_at_end": 0.5,
        }],
    ])
    client = _make_client(conn)
    resp = client.get("/v1/ghosts", headers=AUTH_HEADERS)
    assert resp.status_code == 200
    g = resp.json()["ghosts"][0]
    assert g["scenario_id"] is None
    assert g["resource_id"] is None
    assert g["node_id"] is None
    assert g["created_at"] is None
    assert g["updated_at"] is None
    assert g["members"][0]["transition_start_date"] is None
    assert g["members"][0]["transition_end_date"] is None


# ─────────────────────────────────────────────────────────────
# GET /v1/ghosts/{ghost_id} (detail)
# ─────────────────────────────────────────────────────────────


def test_get_ghost_not_found():
    conn = _mock_db([
        None,  # ghost lookup → None
    ])
    client = _make_client(conn)
    gid = uuid4()
    resp = client.get(f"/v1/ghosts/{gid}", headers=AUTH_HEADERS)
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


def test_get_ghost_with_graph_node_and_edges():
    gid = uuid4()
    nid = uuid4()
    sid = uuid4()
    rid = uuid4()
    member_id = uuid4()
    item_id = uuid4()
    edge_id = uuid4()
    to_node_id = uuid4()
    created = datetime(2025, 1, 1, 12, 0, 0)
    updated = datetime(2025, 1, 2, 12, 0, 0)
    conn = _mock_db([
        # ghost row
        {
            "ghost_id": gid,
            "name": "g1",
            "ghost_type": "phase_transition",
            "scenario_id": sid,
            "resource_id": rid,
            "node_id": nid,
            "status": "active",
            "description": "desc",
            "created_at": created,
            "updated_at": updated,
        },
        # members
        [{
            "member_id": member_id,
            "item_id": item_id,
            "role": "incoming",
            "transition_start_date": date(2025, 1, 1),
            "transition_end_date": date(2025, 2, 1),
            "transition_curve": "linear",
            "weight_at_start": 1.0,
            "weight_at_end": 0.0,
        }],
        # graph node lookup
        {
            "node_id": nid,
            "node_type": "Ghost",
            "scenario_id": sid,
            "active": True,
        },
        # edges
        [
            {
                "edge_id": edge_id,
                "to_node_id": to_node_id,
                "edge_type": "ghost_member",
                "weight_ratio": 1.0,
            },
            {
                "edge_id": uuid4(),
                "to_node_id": uuid4(),
                "edge_type": "ghost_member",
                "weight_ratio": None,
            },
        ],
    ])
    client = _make_client(conn)
    resp = client.get(f"/v1/ghosts/{gid}", headers=AUTH_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ghost_id"] == str(gid)
    assert body["graph_node"] is not None
    assert body["graph_node"]["node_id"] == str(nid)
    assert len(body["graph_node"]["edges"]) == 2
    assert body["graph_node"]["edges"][0]["weight_ratio"] == 1.0
    assert body["graph_node"]["edges"][1]["weight_ratio"] is None


def test_get_ghost_no_node_id():
    """Ghost with no node_id → graph_node is None."""
    gid = uuid4()
    conn = _mock_db([
        # ghost row
        {
            "ghost_id": gid,
            "name": "g1",
            "ghost_type": "capacity_aggregate",
            "scenario_id": None,
            "resource_id": None,
            "node_id": None,
            "status": "active",
            "description": None,
            "created_at": None,
            "updated_at": None,
        },
        # members (none)
        [],
    ])
    client = _make_client(conn)
    resp = client.get(f"/v1/ghosts/{gid}", headers=AUTH_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["graph_node"] is None


def test_get_ghost_node_id_present_but_node_missing():
    """node_id is set in ghost row, but the nodes table no longer has it."""
    gid = uuid4()
    nid = uuid4()
    conn = _mock_db([
        # ghost row
        {
            "ghost_id": gid,
            "name": "g1",
            "ghost_type": "capacity_aggregate",
            "scenario_id": None,
            "resource_id": None,
            "node_id": nid,
            "status": "active",
            "description": None,
            "created_at": None,
            "updated_at": None,
        },
        # members
        [],
        # graph node lookup → missing
        None,
    ])
    client = _make_client(conn)
    resp = client.get(f"/v1/ghosts/{gid}", headers=AUTH_HEADERS)
    assert resp.status_code == 200
    assert resp.json()["graph_node"] is None


# ─────────────────────────────────────────────────────────────
# POST /v1/ghosts/{ghost_id}/run
# ─────────────────────────────────────────────────────────────


def test_run_ghost_endpoint_success():
    conn = _mock_db([])
    client = _make_client(conn)
    gid = uuid4()
    sid = uuid4()
    fake_result = {
        "ghost_id": str(gid),
        "ghost_type": "phase_transition",
        "alerts": [],
        "summary": {"phase_count": 0},
    }
    with patch(
        "ootils_core.api.routers.ghosts.run_ghost",
        return_value=fake_result,
    ) as run_mock:
        resp = client.post(
            f"/v1/ghosts/{gid}/run",
            json={
                "scenario_id": str(sid),
                "from_date": "2025-01-01",
                "to_date": "2025-01-31",
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json() == fake_result
    run_mock.assert_called_once()


def test_ingest_ghost_phase_transition_outgoing_ok_incoming_wrong():
    """Hit the incoming_count != 1 branch separately from outgoing branch."""
    a, b = uuid4(), uuid4()
    conn = _mock_db([])
    client = _make_client(conn)
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


def test_ingest_ghost_explicit_valid_status_and_curve():
    """Explicitly pass status='active' and transition_curve='step' to exercise validator
    'return v' branches in IngestGhostRequest.validate_status / GhostMemberInput.validate_curve.
    """
    item_a = uuid4()
    conn = _mock_db([
        [{"item_id": item_a}],
        None,    # existing_ghost lookup → None
        "noop",  # INSERT nodes
        "noop",  # INSERT ghost_nodes
        "noop",  # DELETE ghost_members
        "noop",  # DELETE edges
        "noop",  # INSERT ghost_members
        None,    # SELECT item node → None
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "g_explicit",
            "ghost_type": "capacity_aggregate",
            "status": "active",
            "members": [
                {
                    "item_id": str(item_a),
                    "role": "member",
                    "transition_curve": "step",
                },
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201


def test_run_ghost_endpoint_value_error_404():
    conn = _mock_db([])
    client = _make_client(conn)
    gid = uuid4()
    sid = uuid4()
    with patch(
        "ootils_core.api.routers.ghosts.run_ghost",
        side_effect=ValueError("Ghost xxx not found"),
    ):
        resp = client.post(
            f"/v1/ghosts/{gid}/run",
            json={
                "scenario_id": str(sid),
                "from_date": "2025-01-01",
                "to_date": "2025-01-31",
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]
