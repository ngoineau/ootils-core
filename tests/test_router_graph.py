"""
test_router_graph.py — Unit tests for src/ootils_core/api/routers/graph.py.

Covers GET /v1/graph and GET /v1/nodes (the nodes_router).
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import get_db
from ootils_core.models import Edge, Node


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


def _make_node(node_id=None, item_id=None, location_id=None, scenario_id=None,
               time_ref=None, time_span_start=None) -> Node:
    return Node(
        node_id=node_id or uuid4(),
        node_type="ProjectedInventory",
        scenario_id=scenario_id or UUID("00000000-0000-0000-0000-000000000001"),
        item_id=item_id or uuid4(),
        location_id=location_id or uuid4(),
        quantity=Decimal("100"),
        time_ref=time_ref,
        time_span_start=time_span_start,
        time_span_end=None,
        time_grain="day",
        has_shortage=False,
        shortage_qty=Decimal("0"),
        closing_stock=Decimal("80"),
    )


def _make_edge(from_id, to_id) -> Edge:
    return Edge(
        edge_id=uuid4(),
        edge_type="supply",
        from_node_id=from_id,
        to_node_id=to_id,
        scenario_id=UUID("00000000-0000-0000-0000-000000000001"),
        priority=1,
        weight_ratio=Decimal("1.0"),
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/graph
# ─────────────────────────────────────────────────────────────

def test_get_graph_with_uuid_inputs():
    db = _make_db_mock()
    item_id = uuid4()
    location_id = uuid4()
    n1 = _make_node(item_id=item_id, location_id=location_id, time_ref=date(2026, 4, 1))
    n2 = _make_node(item_id=item_id, location_id=location_id, time_ref=date(2026, 4, 2))

    # 1: SELECT * FROM nodes (the only db.execute in this happy path)
    cur1 = MagicMock()
    cur1.fetchall.return_value = [
        {"node_id": n1.node_id},
        {"node_id": n2.node_id},
    ]
    db.execute.return_value = cur1

    edges = [_make_edge(n1.node_id, n2.node_id)]

    with patch(
        "ootils_core.engine.kernel.graph.store._row_to_node",
        side_effect=[n1, n2],
    ), patch(
        "ootils_core.api.routers.graph.GraphStore.get_all_edges",
        return_value=edges,
    ):
        client = _make_client(db)
        resp = client.get(
            f"/v1/graph?item_id={item_id}&location_id={location_id}",
            headers=AUTH,
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["nodes"]) == 2
    assert len(body["edges"]) == 1
    assert body["depth"] == 2


def test_get_graph_resolves_item_and_location_by_name():
    db = _make_db_mock()
    item_uuid = uuid4()
    location_uuid = uuid4()
    n = _make_node(item_id=item_uuid, location_id=location_uuid)

    # Order of execute calls:
    # 1) SELECT item_id FROM items WHERE name = %s
    # 2) SELECT location_id FROM locations WHERE name = %s
    # 3) SELECT * FROM nodes ...
    _setup_executes(
        db,
        [
            {"item_id": item_uuid},
            {"location_id": location_uuid},
            [{"node_id": n.node_id}],
        ],
    )

    with patch(
        "ootils_core.engine.kernel.graph.store._row_to_node", return_value=n
    ), patch(
        "ootils_core.api.routers.graph.GraphStore.get_all_edges", return_value=[]
    ):
        client = _make_client(db)
        resp = client.get("/v1/graph?item_id=widget&location_id=warehouse-1", headers=AUTH)
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["nodes"]) == 1


def test_get_graph_item_not_found_returns_404():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get("/v1/graph?item_id=missing&location_id=warehouse-1", headers=AUTH)
    assert resp.status_code == 404
    assert "Item" in resp.json()["detail"]


def test_get_graph_location_not_found_returns_404():
    db = _make_db_mock()
    item_uuid = uuid4()
    # 1st call resolves item OK; 2nd call returns None for location
    _setup_executes(
        db,
        [{"item_id": item_uuid}, None],
    )

    client = _make_client(db)
    resp = client.get("/v1/graph?item_id=widget&location_id=missing-loc", headers=AUTH)
    assert resp.status_code == 404
    assert "Location" in resp.json()["detail"]


def test_get_graph_with_date_window_filters_nodes():
    db = _make_db_mock()
    item_id = uuid4()
    location_id = uuid4()
    n1 = _make_node(item_id=item_id, location_id=location_id, time_span_start=date(2026, 1, 5))
    n2 = _make_node(item_id=item_id, location_id=location_id, time_span_start=date(2026, 6, 1))
    n3 = _make_node(item_id=item_id, location_id=location_id, time_span_start=date(2026, 12, 1))
    # Node with no time info — should always be kept
    n_undated = _make_node(item_id=item_id, location_id=location_id)

    cur = MagicMock()
    cur.fetchall.return_value = [
        {"node_id": n1.node_id},
        {"node_id": n2.node_id},
        {"node_id": n3.node_id},
        {"node_id": n_undated.node_id},
    ]
    db.execute.return_value = cur

    with patch(
        "ootils_core.engine.kernel.graph.store._row_to_node",
        side_effect=[n1, n2, n3, n_undated],
    ), patch(
        "ootils_core.api.routers.graph.GraphStore.get_all_edges",
        return_value=[],
    ):
        client = _make_client(db)
        resp = client.get(
            f"/v1/graph?item_id={item_id}&location_id={location_id}"
            "&from=2026-03-01&to=2026-09-30",
            headers=AUTH,
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Expect only n2 (June) + n_undated to survive the window
    assert len(body["nodes"]) == 2


def test_get_graph_invalid_depth_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get(
        f"/v1/graph?item_id={uuid4()}&location_id={uuid4()}&depth=99",
        headers=AUTH,
    )
    assert resp.status_code == 422


def test_get_graph_missing_required_query_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/graph?item_id=widget", headers=AUTH)
    assert resp.status_code == 422


def test_get_graph_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get(f"/v1/graph?item_id={uuid4()}&location_id={uuid4()}")
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# GET /v1/nodes (nodes_router)
# ─────────────────────────────────────────────────────────────

def _node_list_row(item_id=None, location_id=None):
    return {
        "node_id": uuid4(),
        "node_type": "ProjectedInventory",
        "item_id": item_id or uuid4(),
        "location_id": location_id or uuid4(),
        "scenario_id": uuid4(),
        "time_ref": date(2026, 4, 1),
        "qty": Decimal("100"),
        "item_code": "ITEM-1",
        "location_code": "LOC-1",
    }


def test_list_nodes_no_filters():
    db = _make_db_mock()
    rows = [_node_list_row(), _node_list_row()]
    _setup_executes(db, [{"cnt": 2}, rows])

    client = _make_client(db)
    resp = client.get("/v1/nodes", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 2
    assert len(body["nodes"]) == 2


def test_list_nodes_with_all_filters():
    db = _make_db_mock()
    item_id = uuid4()
    location_id = uuid4()
    sid = uuid4()
    _setup_executes(db, [{"cnt": 1}, [_node_list_row(item_id=item_id, location_id=location_id)]])

    client = _make_client(db)
    resp = client.get(
        f"/v1/nodes?item_id={item_id}&location_id={location_id}"
        f"&node_type=ProjectedInventory&scenario_id={sid}&limit=10",
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["total"] == 1


def test_list_nodes_count_row_none():
    db = _make_db_mock()
    cur1 = MagicMock()
    cur1.fetchone.return_value = None  # forces total=0 fallback
    cur2 = MagicMock()
    cur2.fetchall.return_value = []
    db.execute.side_effect = [cur1, cur2]

    client = _make_client(db)
    resp = client.get("/v1/nodes", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


def test_list_nodes_row_with_null_item_and_location():
    """Row where item_id and location_id are NULL — exercises None branches in mapper."""
    db = _make_db_mock()
    row = _node_list_row()
    row["item_id"] = None
    row["location_id"] = None
    _setup_executes(db, [{"cnt": 1}, [row]])

    client = _make_client(db)
    resp = client.get("/v1/nodes", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["nodes"][0]["item_id"] is None
    assert body["nodes"][0]["location_id"] is None


def test_list_nodes_invalid_limit_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/nodes?limit=99999", headers=AUTH)
    assert resp.status_code == 422


def test_list_nodes_invalid_uuid_filter_422():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/nodes?item_id=not-a-uuid", headers=AUTH)
    assert resp.status_code == 422


def test_list_nodes_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/nodes")
    assert resp.status_code == 401
