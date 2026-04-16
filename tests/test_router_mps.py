"""
test_router_mps.py — Unit tests for src/ootils_core/api/routers/mps.py.

Covers POST /v1/mps/create.
"""
from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
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


# ─────────────────────────────────────────────────────────────
# POST /v1/mps/create
# ─────────────────────────────────────────────────────────────

def test_mps_create_success():
    db = _make_db_mock()
    # Mock database responses
    db.execute.return_value.fetchone.side_effect = [
        {"item_id": UUID("12345678-1234-1234-1234-123456789abc")},
        {"location_id": UUID("22345678-1234-1234-1234-123456789abc")},
        None,  # dependencies lookup returns None (no rows)
    ]
    client = _make_client(db)

    payload = {
        "item_id": "ITEM-001",
        "location_id": "LOC-001",
        "planned_date": "2025-01-15",
        "quantity": "100.0",
        "uom": "EA",
        "status": "planned",
        "priority": 1,
        "lot_number": "LOT123",
        "work_center": "WC1",
        "dependencies": [],
        "scenario_id": "baseline",
    }

    response = client.post("/v1/mps/create", json=payload, headers=AUTH)
    assert response.status_code == 200
    data = response.json()
    assert "mps_node_id" in data
    assert data["item_id"] == "12345678-1234-1234-1234-123456789abc"
    assert data["location_id"] == "22345678-1234-1234-1234-123456789abc"
    assert data["planned_date"] == "2025-01-15"
    assert data["quantity"] == "100.0"
    assert data["status"] == "planned"
    # Verify that db.execute was called with INSERT
    assert db.execute.call_count >= 3


def test_mps_create_item_not_found():
    db = _make_db_mock()
    db.execute.return_value.fetchone.return_value = None  # item not found
    client = _make_client(db)

    payload = {
        "item_id": "NONEXISTENT",
        "location_id": "LOC-001",
        "planned_date": "2025-01-15",
        "quantity": "100.0",
    }

    response = client.post("/v1/mps/create", json=payload, headers=AUTH)
    assert response.status_code == 404
    assert "Item" in response.json()["detail"]


def test_mps_create_location_not_found():
    db = _make_db_mock()
    db.execute.return_value.fetchone.side_effect = [
        {"item_id": UUID("12345678-1234-1234-1234-123456789abc")},
        None,  # location not found
    ]
    client = _make_client(db)

    payload = {
        "item_id": "ITEM-001",
        "location_id": "NONEXISTENT",
        "planned_date": "2025-01-15",
        "quantity": "100.0",
    }

    response = client.post("/v1/mps/create", json=payload, headers=AUTH)
    assert response.status_code == 404
    assert "Location" in response.json()["detail"]