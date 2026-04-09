"""
test_router_bom.py — unit tests for src/ootils_core/api/routers/bom.py.

Target: 100% coverage.

All DB calls are mocked via FastAPI dependency_overrides.
The mock builds `execute(...)` responses as dicts matching the real dict_row
output: {column: value}.
"""
from __future__ import annotations

import os
from datetime import date
from typing import Any, Generator
from unittest.mock import MagicMock
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
      - None (treated as fetchone → None).
    """
    responses = list(responses)

    def execute_side_effect(*args, **kwargs):
        if not responses:
            # Default: empty result object
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
        elif item is None:
            result.fetchone.return_value = None
            result.fetchall.return_value = []
        else:
            raise TypeError(f"Unexpected response type: {type(item)}")
        result.rowcount = 1
        return result

    execute = MagicMock(side_effect=execute_side_effect)
    return execute


def _mock_db(responses: list[Any] | None = None) -> MagicMock:
    """Return a mock psycopg3 connection with scripted execute() responses."""
    conn = MagicMock()
    if responses is None:
        responses = []
    conn.execute = _make_execute_mock(responses)
    # cursor() is a context manager used by _recalculate_llc batch updates
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
# Auth tests (no override_auth)
# ─────────────────────────────────────────────────────────────


def test_ingest_bom_requires_auth():
    """POST /v1/ingest/bom rejects missing auth."""
    app = create_app()
    conn = _mock_db([])
    app.dependency_overrides[get_db] = lambda: conn
    with TestClient(app) as c:
        resp = c.post(
            "/v1/ingest/bom",
            json={"parent_external_id": "P1", "components": []},
        )
    assert resp.status_code == 401


def test_get_bom_requires_auth():
    app = create_app()
    conn = _mock_db([])
    app.dependency_overrides[get_db] = lambda: conn
    with TestClient(app) as c:
        resp = c.get("/v1/bom/P1")
    assert resp.status_code == 401


def test_explode_bom_requires_auth():
    app = create_app()
    conn = _mock_db([])
    app.dependency_overrides[get_db] = lambda: conn
    with TestClient(app) as c:
        resp = c.post(
            "/v1/bom/explode",
            json={"item_external_id": "P1", "quantity": 10},
        )
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# POST /v1/ingest/bom
# ─────────────────────────────────────────────────────────────


def test_ingest_bom_parent_not_found():
    """422 when parent external_id does not resolve."""
    conn = _mock_db([
        None,  # _resolve_item_id(parent) → None
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": "MISSING",
            "components": [
                {"component_external_id": "C1", "quantity_per": 1.0},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert any("MISSING" in str(d) for d in detail)


def test_ingest_bom_component_not_found():
    """422 when a component cannot be resolved."""
    parent_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},     # parent found
        None,                       # component 1 missing
        {"item_id": uuid4()},       # component 2 OK
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": "P1",
            "components": [
                {"component_external_id": "C1", "quantity_per": 1.0},
                {"component_external_id": "C2", "quantity_per": 2.0},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
    assert any("C1" in str(d) for d in resp.json()["detail"])


def test_ingest_bom_cycle_detected():
    """422 when BOM cycle is detected."""
    parent_id = uuid4()
    comp_id = uuid4()
    # Edges: comp → parent (pre-existing). Now adding parent → comp creates cycle.
    conn = _mock_db([
        {"item_id": parent_id},   # _resolve_item_id parent
        {"item_id": comp_id},     # _resolve_item_id component
        # _detect_cycle edges: the component already points back to parent
        [{"parent_item_id": comp_id, "component_item_id": parent_id}],
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": "P1",
            "components": [
                {"component_external_id": "C1", "quantity_per": 1.0},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
    assert any("cycle" in str(d).lower() for d in resp.json()["detail"])


def test_ingest_bom_dry_run():
    """dry_run=True returns without DB upsert."""
    parent_id = uuid4()
    comp_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},          # resolve parent
        {"item_id": comp_id},            # resolve component
        [],                              # _detect_cycle edges (empty)
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": "P1",
            "components": [
                {"component_external_id": "C1", "quantity_per": 1.0},
            ],
            "dry_run": True,
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "dry_run"
    assert body["components_imported"] == 1
    assert body["llc_updated"] == 0


def test_ingest_bom_new_header_success():
    """Happy path: new BOM header + lines + LLC recalculation."""
    parent_id = uuid4()
    comp_id = uuid4()
    line_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},                      # resolve parent
        {"item_id": comp_id},                        # resolve component
        [],                                          # _detect_cycle edges
        None,                                        # existing_header lookup → None (new)
        MagicMock(),                                 # INSERT bom_headers
        MagicMock(),                                 # INSERT bom_lines (one line)
        MagicMock(),                                 # soft-delete removed lines
        [                                            # _recalculate_llc edges
            {
                "parent_item_id": parent_id,
                "component_item_id": comp_id,
                "line_id": line_id,
            }
        ],
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": "P1",
            "bom_version": "1.0",
            "effective_from": "2026-01-01",
            "components": [
                {
                    "component_external_id": "C1",
                    "quantity_per": 2.0,
                    "uom": "EA",
                    "scrap_factor": 0.05,
                },
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ok"
    assert body["components_imported"] == 1
    assert body["llc_updated"] == 1
    assert body["parent_item_id"] == str(parent_id)


def test_ingest_bom_existing_header_update():
    """Covers the UPDATE bom_headers branch for an existing version."""
    parent_id = uuid4()
    comp_id = uuid4()
    existing_bom_id = uuid4()
    line_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},                                 # resolve parent
        {"item_id": comp_id},                                   # resolve component
        [],                                                     # _detect_cycle edges
        {"bom_id": existing_bom_id},                            # existing header found
        MagicMock(),                                            # UPDATE bom_headers
        MagicMock(),                                            # INSERT bom_lines
        MagicMock(),                                            # soft-delete
        [                                                       # _recalculate_llc edges
            {
                "parent_item_id": parent_id,
                "component_item_id": comp_id,
                "line_id": line_id,
            }
        ],
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": "P1",
            "components": [
                {"component_external_id": "C1", "quantity_per": 1.0},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["bom_id"] == str(existing_bom_id)


def test_ingest_bom_empty_components_deactivates_all():
    """component_ids=[] → branch that deactivates all lines of the existing BOM."""
    parent_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},              # resolve parent
        [],                                  # _detect_cycle edges (no components to iterate)
        None,                                # existing_header lookup → None
        MagicMock(),                         # INSERT bom_headers
        MagicMock(),                         # deactivate all lines branch (empty components)
        [],                                  # _recalculate_llc edges (empty)
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": "P1",
            "components": [],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ok"
    assert body["components_imported"] == 0
    assert body["llc_updated"] == 0


def test_ingest_bom_validation_error_422_missing_fields():
    """422 from pydantic — empty body."""
    conn = _mock_db([])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/bom",
        json={},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_bom_validation_error_negative_quantity():
    """422 when quantity_per <= 0 (Field(..., gt=0))."""
    conn = _mock_db([])
    client = _make_client(conn)
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
    assert resp.status_code == 422


# ─────────────────────────────────────────────────────────────
# GET /v1/bom/{parent_external_id}
# ─────────────────────────────────────────────────────────────


def test_get_bom_item_not_found():
    conn = _mock_db([None])  # resolve_item_id → None
    client = _make_client(conn)
    resp = client.get("/v1/bom/MISSING", headers=AUTH_HEADERS)
    assert resp.status_code == 404
    assert "MISSING" in resp.json()["detail"]


def test_get_bom_no_active_header():
    parent_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},   # resolve parent
        None,                     # _get_active_bom → None
    ])
    client = _make_client(conn)
    resp = client.get("/v1/bom/P1", headers=AUTH_HEADERS)
    assert resp.status_code == 404


def test_get_bom_success():
    parent_id = uuid4()
    bom_id = uuid4()
    comp_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},
        {                                              # _get_active_bom header
            "bom_id": bom_id,
            "bom_version": "1.0",
            "effective_from": date(2026, 1, 1),
        },
        [                                              # _get_bom_lines
            {
                "line_id": uuid4(),
                "component_item_id": comp_id,
                "quantity_per": 2.5,
                "uom": "EA",
                "scrap_factor": 0.05,
                "llc": 1,
                "component_external_id": "C1",
            }
        ],
    ])
    client = _make_client(conn)
    resp = client.get("/v1/bom/P1", headers=AUTH_HEADERS)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["parent_external_id"] == "P1"
    assert body["bom_version"] == "1.0"
    assert body["effective_from"] == "2026-01-01"
    assert len(body["components"]) == 1
    c = body["components"][0]
    assert c["component_external_id"] == "C1"
    assert c["quantity_per"] == 2.5
    assert c["uom"] == "EA"
    assert c["scrap_factor"] == 0.05
    assert c["llc"] == 1


# ─────────────────────────────────────────────────────────────
# POST /v1/bom/explode
# ─────────────────────────────────────────────────────────────


def test_explode_bom_item_not_found():
    conn = _mock_db([None])
    client = _make_client(conn)
    resp = client.post(
        "/v1/bom/explode",
        json={"item_external_id": "MISSING", "quantity": 10},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 404


def test_explode_bom_location_not_found():
    parent_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},   # resolve parent
        None,                      # location lookup → None
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/bom/explode",
        json={
            "item_external_id": "P1",
            "quantity": 5,
            "location_external_id": "NOWHERE",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_explode_bom_leaf_item_no_bom():
    """Parent has no active BOM → explosion returns empty."""
    parent_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},   # resolve parent
        None,                      # _get_active_bom → None (leaf)
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/bom/explode",
        json={"item_external_id": "P1", "quantity": 10},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_components"] == 0
    assert body["components_with_shortage"] == 0
    assert body["explosion"] == []


def test_explode_bom_with_location_and_shortage():
    """
    Happy path: single level explosion with location, one component has shortage,
    sub-component has no BOM. Covers net_req > 0 recurse and leaf termination.
    """
    parent_id = uuid4()
    loc_id = uuid4()
    comp_id = uuid4()
    bom_id = uuid4()

    conn = _mock_db([
        {"item_id": parent_id},              # resolve parent
        {"location_id": loc_id},             # resolve location
        {                                    # _get_active_bom (level 1)
            "bom_id": bom_id,
            "bom_version": "1.0",
            "effective_from": date(2026, 1, 1),
        },
        [                                    # _get_bom_lines (level 1)
            {
                "line_id": uuid4(),
                "component_item_id": comp_id,
                "quantity_per": 2.0,
                "uom": "EA",
                "scrap_factor": 0.0,
                "llc": 0,
                "component_external_id": "C1",
            }
        ],
        {"qty": 5},                          # on-hand (location path) — shortage 20-5 = 15
        None,                                # sub-component has no BOM → leaf
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/bom/explode",
        json={
            "item_external_id": "P1",
            "quantity": 10,
            "location_external_id": "WH1",
            "explosion_date": "2026-01-01",
            "levels": 5,
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_components"] == 1
    assert body["components_with_shortage"] == 1
    line = body["explosion"][0]
    assert line["level"] == 1
    assert line["gross_requirement"] == 20.0
    assert line["on_hand_qty"] == 5.0
    assert line["net_requirement"] == 15.0
    assert line["has_shortage"] is True


def test_explode_bom_no_shortage_recurses_with_gross():
    """net_req == 0 → recurse with gross_req branch is exercised."""
    parent_id = uuid4()
    comp_id = uuid4()
    bom_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},              # resolve parent (no location)
        {                                    # _get_active_bom (level 1)
            "bom_id": bom_id,
            "bom_version": "1.0",
            "effective_from": date(2026, 1, 1),
        },
        [                                    # _get_bom_lines (level 1)
            {
                "line_id": uuid4(),
                "component_item_id": comp_id,
                "quantity_per": 1.0,
                "uom": "EA",
                "scrap_factor": 0.0,
                "llc": 0,
                "component_external_id": "C1",
            }
        ],
        {"qty": 1000},                       # on-hand no-location path (massive stock)
        None,                                # sub-component has no BOM
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/bom/explode",
        json={"item_external_id": "P1", "quantity": 10},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["components_with_shortage"] == 0
    assert body["explosion"][0]["has_shortage"] is False


def test_explode_bom_levels_cap():
    """Cover level > levels early return."""
    parent_id = uuid4()
    conn = _mock_db([
        {"item_id": parent_id},
        None,  # leaf → no BOM
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/bom/explode",
        json={"item_external_id": "P1", "quantity": 1, "levels": 1},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200


def test_explode_bom_validation_422_zero_quantity():
    conn = _mock_db([])
    client = _make_client(conn)
    resp = client.post(
        "/v1/bom/explode",
        json={"item_external_id": "P1", "quantity": 0},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_explode_bom_validation_422_levels_out_of_range():
    conn = _mock_db([])
    client = _make_client(conn)
    resp = client.post(
        "/v1/bom/explode",
        json={"item_external_id": "P1", "quantity": 1, "levels": 0},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


# ─────────────────────────────────────────────────────────────
# Helper coverage — _detect_cycle, _recalculate_llc via ingest
# ─────────────────────────────────────────────────────────────


def test_detect_cycle_deep_graph_no_cycle():
    """
    Build a non-cyclic graph for cycle detection, exercising the full DFS.

    parent=P → components=[C1]
    existing edges: C1 → X, X → Y (no loop to P).
    """
    parent_id = uuid4()
    c1 = uuid4()
    x = uuid4()
    y = uuid4()
    line_id = uuid4()

    conn = _mock_db([
        {"item_id": parent_id},      # resolve parent
        {"item_id": c1},             # resolve C1
        [                            # _detect_cycle: full edge graph
            {"parent_item_id": c1, "component_item_id": x},
            {"parent_item_id": x, "component_item_id": y},
        ],
        None,                        # existing_header → None
        MagicMock(),                 # INSERT bom_headers
        MagicMock(),                 # INSERT bom_lines
        MagicMock(),                 # soft-delete
        [                            # _recalculate_llc edges: the full graph now
            {"parent_item_id": parent_id, "component_item_id": c1, "line_id": line_id},
            {"parent_item_id": c1, "component_item_id": x, "line_id": uuid4()},
            {"parent_item_id": x, "component_item_id": y, "line_id": uuid4()},
        ],
    ])
    client = _make_client(conn)
    resp = client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": "P",
            "components": [
                {"component_external_id": "C1", "quantity_per": 1.0},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["llc_updated"] == 3
