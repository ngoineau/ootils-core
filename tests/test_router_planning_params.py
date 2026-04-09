"""
test_router_planning_params.py — Unit tests for src/ootils_core/api/routers/planning_params.py.

Covers GET /v1/items/planning-params with all filter branches and value mappings.
"""
from __future__ import annotations

import os
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import get_db


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


def _params_row(
    *,
    item_id=None,
    location_id=None,
    safety_stock_qty=Decimal("10"),
    safety_stock_days=Decimal("3"),
    reorder_point=Decimal("20"),
    lot_size=Decimal("50"),
    lead_time_days=Decimal("7"),
):
    return {
        "item_id": item_id or uuid4(),
        "location_id": location_id,
        "safety_stock_qty": safety_stock_qty,
        "safety_stock_days": safety_stock_days,
        "reorder_point": reorder_point,
        "lot_size": lot_size,
        "lead_time_days": lead_time_days,
        "effective_from": "2026-01-01",
    }


# ─────────────────────────────────────────────────────────────
# GET /v1/items/planning-params
# ─────────────────────────────────────────────────────────────

def test_planning_params_no_filters():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchall.return_value = [
        _params_row(location_id=uuid4()),
        _params_row(location_id=None),
    ]
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get("/v1/items/planning-params", headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 2
    assert len(body["params"]) == 2
    # 2nd row should have location_id=None
    assert body["params"][1]["location_id"] is None


def test_planning_params_with_item_filter():
    db = _make_db_mock()
    item_id = uuid4()
    cur = MagicMock()
    cur.fetchall.return_value = [_params_row(item_id=item_id)]
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get(f"/v1/items/planning-params?item_id={item_id}", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total"] == 1


def test_planning_params_with_location_filter():
    db = _make_db_mock()
    loc_id = uuid4()
    cur = MagicMock()
    cur.fetchall.return_value = [_params_row(location_id=loc_id)]
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get(f"/v1/items/planning-params?location_id={loc_id}", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total"] == 1


def test_planning_params_with_both_filters():
    db = _make_db_mock()
    item_id = uuid4()
    loc_id = uuid4()
    cur = MagicMock()
    cur.fetchall.return_value = [_params_row(item_id=item_id, location_id=loc_id)]
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get(
        f"/v1/items/planning-params?item_id={item_id}&location_id={loc_id}",
        headers=AUTH,
    )
    assert resp.status_code == 200


def test_planning_params_invalid_item_id_silently_ignored():
    """Garbage item_id is caught by ValueError → ignored, NOT 422."""
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchall.return_value = []
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get("/v1/items/planning-params?item_id=garbage", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


def test_planning_params_invalid_location_id_silently_ignored():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchall.return_value = []
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get("/v1/items/planning-params?location_id=garbage", headers=AUTH)
    assert resp.status_code == 200


def test_planning_params_all_null_decimal_fields():
    """Row where every Decimal field is None — exercises None branches in mapper."""
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchall.return_value = [
        _params_row(
            safety_stock_qty=None,
            safety_stock_days=None,
            reorder_point=None,
            lot_size=None,
            lead_time_days=None,
        )
    ]
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get("/v1/items/planning-params", headers=AUTH)
    assert resp.status_code == 200
    p = resp.json()["params"][0]
    assert p["safety_stock_qty"] is None
    assert p["safety_stock_days"] is None
    assert p["reorder_point"] is None
    assert p["lot_size"] is None
    assert p["lead_time_days"] is None


def test_planning_params_unauthenticated():
    db = _make_db_mock()
    client = _make_client(db)
    resp = client.get("/v1/items/planning-params")
    assert resp.status_code == 401


def test_planning_params_empty_result():
    db = _make_db_mock()
    cur = MagicMock()
    cur.fetchall.return_value = []
    db.execute.return_value = cur

    client = _make_client(db)
    resp = client.get("/v1/items/planning-params", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json()["total"] == 0
    assert resp.json()["params"] == []
