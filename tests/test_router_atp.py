"""
test_router_atp.py — Unit tests for src/ootils_core/api/routers/atp.py.

Covers POST /v1/atp/check.
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
# POST /v1/atp/check
# ─────────────────────────────────────────────────────────────

def test_atp_check_available():
    db = _make_db_mock()
    db.execute.return_value.fetchone.side_effect = [
        {"item_id": UUID("12345678-1234-1234-1234-123456789abc")},
        {"location_id": UUID("22345678-1234-1234-1234-123456789abc")},
        {"closing_stock": Decimal("200.0"), "time_span_start": date(2025, 1, 1), "node_id": UUID("33345678-1234-1234-1234-123456789abc")},
    ]
    # Mock supply rows empty
    db.execute.return_value.fetchall.return_value = []
    client = _make_client(db)

    payload = {
        "item_id": "ITEM-001",
        "location_id": "LOC-001",
        "requested_date": "2025-01-01",
        "requested_quantity": "100.0",
        "scenario_id": "baseline",
    }

    response = client.post("/v1/atp/check", json=payload, headers=AUTH)
    assert response.status_code == 200
    data = response.json()
    assert "atp_check_id" in data
    assert data["status"] == "available"
    assert Decimal(data["available_quantity"]) >= Decimal("100.0")
    assert data["shortage_quantity"] == "0.0"


def test_atp_check_partial():
    db = _make_db_mock()
    db.execute.return_value.fetchone.side_effect = [
        {"item_id": UUID("12345678-1234-1234-1234-123456789abc")},
        {"location_id": UUID("22345678-1234-1234-1234-123456789abc")},
        {"closing_stock": Decimal("50.0"), "time_span_start": date(2025, 1, 1), "node_id": UUID("33345678-1234-1234-1234-123456789abc")},
    ]
    # Mock supply rows with some supply
    db.execute.return_value.fetchall.return_value = [
        {"node_id": UUID("44445678-1234-1234-1234-123456789abc"), "quantity": Decimal("30.0"), "time_ref": date(2025, 1, 1)},
    ]
    client = _make_client(db)

    payload = {
        "item_id": "ITEM-001",
        "location_id": "LOC-001",
        "requested_date": "2025-01-01",
        "requested_quantity": "100.0",
        "scenario_id": "baseline",
    }

    response = client.post("/v1/atp/check", json=payload, headers=AUTH)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "partial"
    assert Decimal(data["available_quantity"]) < Decimal("100.0")
    assert Decimal(data["shortage_quantity"]) > Decimal("0.0")


def test_atp_check_unavailable():
    db = _make_db_mock()
    db.execute.return_value.fetchone.side_effect = [
        {"item_id": UUID("12345678-1234-1234-1234-123456789abc")},
        {"location_id": UUID("22345678-1234-1234-1234-123456789abc")},
        {"closing_stock": Decimal("0.0"), "time_span_start": date(2025, 1, 1), "node_id": UUID("33345678-1234-1234-1234-123456789abc")},
    ]
    db.execute.return_value.fetchall.return_value = []  # no supplies
    client = _make_client(db)

    payload = {
        "item_id": "ITEM-001",
        "location_id": "LOC-001",
        "requested_date": "2025-01-01",
        "requested_quantity": "100.0",
        "scenario_id": "baseline",
    }

    response = client.post("/v1/atp/check", json=payload, headers=AUTH)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "unavailable"
    assert Decimal(data["available_quantity"]) == Decimal("0.0")
    assert Decimal(data["shortage_quantity"]) == Decimal("100.0")


def test_atp_check_item_not_found():
    db = _make_db_mock()
    db.execute.return_value.fetchone.return_value = None  # item not found
    client = _make_client(db)

    payload = {
        "item_id": "NONEXISTENT",
        "location_id": "LOC-001",
        "requested_date": "2025-01-01",
        "requested_quantity": "100.0",
    }

    response = client.post("/v1/atp/check", json=payload, headers=AUTH)
    assert response.status_code == 404
    assert "Item" in response.json()["detail"]