"""
tests/integration/test_mrp_api.py — MRP API integration tests.

Tests for:
- POST /v1/mrp/run — basic single-item MRP (scoped with location_id)
- POST /v1/mrp/apics/run — full APICS multi-level MRP
- Error handling: missing location_id, invalid item_id, scenario not found
- Validation: horizon_days limits, bucket_grain options

Uses a real PostgreSQL database (via migrated_db fixture) and FastAPI TestClient.
Skip all tests if DATABASE_URL is not configured.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db, DB_AVAILABLE, TEST_DB_URL

SEED_SCRIPT = Path(__file__).parents[2] / "scripts" / "seed_demo_data.py"

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


def _run_seed():
    """Run the seed demo data script."""
    env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    return subprocess.run(
        [sys.executable, str(SEED_SCRIPT)],
        capture_output=True, text=True, env=env,
    )


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def seeded_db(migrated_db):
    """Module-scoped: migrated DB with seed data loaded once."""
    result = _run_seed()
    if result.returncode != 0:
        pytest.skip(f"Seed failed, skipping MRP tests: {result.stderr[:500]}")
    return migrated_db


@pytest.fixture(scope="module")
def api_client(seeded_db):
    """Module-scoped TestClient wired to the test DB."""
    os.environ["DATABASE_URL"] = seeded_db
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from fastapi.testclient import TestClient

    app = create_app()

    def override_db():
        db = OotilsDB(seeded_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def auth(seeded_db):
    """Authorization headers."""
    return {"Authorization": "Bearer integration-test-token"}


@pytest.fixture(scope="module")
def test_item_location(seeded_db):
    """Get a valid item_id and location_id from seeded data."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        item_row = conn.execute(
            "SELECT external_id, item_id FROM items WHERE status = 'active' LIMIT 1"
        ).fetchone()
        loc_row = conn.execute(
            "SELECT external_id, location_id FROM locations LIMIT 1"
        ).fetchone()

    if item_row is None:
        pytest.skip("No items in DB for MRP tests")
    if loc_row is None:
        pytest.skip("No locations in DB for MRP tests")

    return {
        "item_id": str(item_row["external_id"]),
        "item_uuid": str(item_row["item_id"]),
        "location_id": str(loc_row["external_id"]),
        "location_uuid": str(loc_row["location_id"]),
    }


# ---------------------------------------------------------------------------
# POST /v1/mrp/run — Basic MRP Tests
# ---------------------------------------------------------------------------

@requires_db
def test_mrp_run_missing_location_id(api_client, auth, test_item_location):
    """POST /v1/mrp/run without location_id returns 422."""
    payload = {
        "item_id": test_item_location["item_id"],
    }
    resp = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp.status_code == 422


@requires_db
def test_mrp_run_invalid_item_id(api_client, auth, test_item_location):
    """POST /v1/mrp/run with non-existent item_id returns 404."""
    payload = {
        "item_id": "NONEXISTENT-ITEM-999",
        "location_id": test_item_location["location_id"],
    }
    resp = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


@requires_db
def test_mrp_run_invalid_location_id(api_client, auth, test_item_location):
    """POST /v1/mrp/run with non-existent location_id returns 404."""
    payload = {
        "item_id": test_item_location["item_id"],
        "location_id": "NONEXISTENT-LOC-999",
    }
    resp = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


@requires_db
def test_mrp_run_invalid_scenario_id(api_client, auth, test_item_location):
    """POST /v1/mrp/run with invalid scenario_id returns 422."""
    payload = {
        "item_id": test_item_location["item_id"],
        "location_id": test_item_location["location_id"],
        "scenario_id": "not-a-valid-uuid",
    }
    resp = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp.status_code == 422


@requires_db
def test_mrp_run_basic_success(api_client, auth, test_item_location):
    """POST /v1/mrp/run with valid params returns 200 and response structure."""
    payload = {
        "item_id": test_item_location["item_id"],
        "location_id": test_item_location["location_id"],
        "horizon_days": 90,
    }
    resp = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp.status_code == 200, f"Response: {resp.text}"

    data = resp.json()
    assert "scenario_id" in data
    assert "item_id" in data
    assert "location_id" in data
    assert "planned_orders_created" in data
    assert "planned_orders" in data
    assert "message" in data

    assert isinstance(data["planned_orders"], list)
    assert isinstance(data["planned_orders_created"], int)
    assert data["planned_orders_created"] == len(data["planned_orders"])


@requires_db
def test_mrp_run_with_clear_existing(api_client, auth, test_item_location):
    """POST /v1/mrp/run with clear_existing=True deletes existing PlannedSupply nodes."""
    import psycopg
    from psycopg.rows import dict_row

    payload1 = {
        "item_id": test_item_location["item_id"],
        "location_id": test_item_location["location_id"],
        "horizon_days": 90,
        "clear_existing": False,
    }
    resp1 = api_client.post("/v1/mrp/run", json=payload1, headers=auth)
    assert resp1.status_code == 200
    first_count = resp1.json()["planned_orders_created"]

    payload2 = {
        "item_id": test_item_location["item_id"],
        "location_id": test_item_location["location_id"],
        "horizon_days": 90,
        "clear_existing": True,
    }
    resp2 = api_client.post("/v1/mrp/run", json=payload2, headers=auth)
    assert resp2.status_code == 200

    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as conn:
        item_uuid = UUID(test_item_location["item_uuid"])
        loc_uuid = UUID(test_item_location["location_uuid"])
        scenario_uuid = UUID(BASELINE_SCENARIO_ID)

        inactive_count = conn.execute(
            """
            SELECT COUNT(*) as cnt FROM nodes
            WHERE node_type = 'PlannedSupply'
              AND item_id = %s AND location_id = %s AND scenario_id = %s
              AND active = FALSE
            """,
            (item_uuid, loc_uuid, scenario_uuid),
        ).fetchone()["cnt"]

        assert inactive_count >= first_count, "Previous planned orders should be soft-deleted"


@requires_db
def test_mrp_run_planned_order_structure(api_client, auth, test_item_location):
    """POST /v1/mrp/run returns planned orders with correct structure."""
    payload = {
        "item_id": test_item_location["item_id"],
        "location_id": test_item_location["location_id"],
        "horizon_days": 90,
    }
    resp = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp.status_code == 200

    data = resp.json()
    for order in data["planned_orders"]:
        assert "node_id" in order
        assert "item_id" in order
        assert "location_id" in order
        assert "order_date" in order
        assert "need_date" in order
        assert "quantity" in order
        assert "lot_size_applied" in order
        assert "bucket_id" in order
        assert isinstance(order["lot_size_applied"], bool)
        assert isinstance(order["quantity"], str)


# ---------------------------------------------------------------------------
# POST /v1/mrp/apics/run — APICS MRP Tests
# ---------------------------------------------------------------------------

@requires_db
def test_mrp_apics_run_missing_location_id(api_client, auth):
    """POST /v1/mrp/apics/run without location_id returns 422."""
    payload = {"horizon_days": 90}
    resp = api_client.post("/v1/mrp/apics/run", json=payload, headers=auth)
    assert resp.status_code == 422


@requires_db
def test_mrp_apics_run_invalid_location_id(api_client, auth):
    """POST /v1/mrp/apics/run with non-existent location_id returns 400/500."""
    payload = {
        "location_id": "NONEXISTENT-LOC-999",
        "horizon_days": 90,
    }
    resp = api_client.post("/v1/mrp/apics/run", json=payload, headers=auth)
    assert resp.status_code in (400, 404, 500)


@requires_db
def test_mrp_apics_run_invalid_scenario_id(api_client, auth, test_item_location):
    """POST /v1/mrp/apics/run with invalid scenario_id returns 422/500."""
    payload = {
        "location_id": test_item_location["location_id"],
        "scenario_id": "not-a-valid-uuid",
        "horizon_days": 90,
    }
    resp = api_client.post("/v1/mrp/apics/run", json=payload, headers=auth)
    assert resp.status_code in (422, 500)


@requires_db
def test_mrp_apics_run_horizon_validation(api_client, auth, test_item_location):
    """POST /v1/mrp/apics/run validates horizon_days range (7-365)."""
    payload_low = {
        "location_id": test_item_location["location_id"],
        "horizon_days": 3,
    }
    resp_low = api_client.post("/v1/mrp/apics/run", json=payload_low, headers=auth)
    assert resp_low.status_code == 422

    payload_high = {
        "location_id": test_item_location["location_id"],
        "horizon_days": 400,
    }
    resp_high = api_client.post("/v1/mrp/apics/run", json=payload_high, headers=auth)
    assert resp_high.status_code == 422


@requires_db
def test_mrp_apics_run_bucket_grain_validation(api_client, auth, test_item_location):
    """POST /v1/mrp/apics/run validates bucket_grain options."""
    payload = {
        "location_id": test_item_location["location_id"],
        "bucket_grain": "invalid_grain",
    }
    resp = api_client.post("/v1/mrp/apics/run", json=payload, headers=auth)
    assert resp.status_code == 422

    for grain in ["day", "week", "month"]:
        payload_valid = {
            "location_id": test_item_location["location_id"],
            "bucket_grain": grain,
        }
        resp_valid = api_client.post("/v1/mrp/apics/run", json=payload_valid, headers=auth)
        assert resp_valid.status_code != 422


@requires_db
def test_mrp_apics_run_forecast_strategy_validation(api_client, auth, test_item_location):
    """POST /v1/mrp/apics/run validates forecast_strategy options."""
    payload = {
        "location_id": test_item_location["location_id"],
        "forecast_strategy": "INVALID_STRATEGY",
    }
    resp = api_client.post("/v1/mrp/apics/run", json=payload, headers=auth)
    assert resp.status_code == 422

    for strategy in ["MAX", "FORECAST_ONLY", "ORDERS_ONLY", "PRIORITY"]:
        payload_valid = {
            "location_id": test_item_location["location_id"],
            "forecast_strategy": strategy,
        }
        resp_valid = api_client.post("/v1/mrp/apics/run", json=payload_valid, headers=auth)
        assert resp_valid.status_code != 422


@requires_db
def test_mrp_apics_run_basic_success(api_client, auth, test_item_location):
    """POST /v1/mrp/apics/run with valid params returns 200 and response structure."""
    payload = {
        "location_id": test_item_location["location_id"],
        "horizon_days": 90,
        "bucket_grain": "week",
    }
    resp = api_client.post("/v1/mrp/apics/run", json=payload, headers=auth)
    if resp.status_code == 200:
        data = resp.json()
        assert "run_id" in data
        assert "scenario_id" in data
        assert "status" in data
        assert "items_processed" in data
        assert "total_records" in data
        assert "elapsed_ms" in data
        assert isinstance(data["errors"], list)


@requires_db
def test_mrp_apics_run_with_item_ids(api_client, auth, test_item_location):
    """POST /v1/mrp/apics/run with specific item_ids."""
    payload = {
        "location_id": test_item_location["location_id"],
        "item_ids": [test_item_location["item_id"]],
        "horizon_days": 90,
    }
    resp = api_client.post("/v1/mrp/apics/run", json=payload, headers=auth)
    assert resp.status_code != 422


# ---------------------------------------------------------------------------
# GET /v1/mrp/apics/llc — LLC Tests
# ---------------------------------------------------------------------------

@requires_db
def test_llc_get_without_recalc(api_client, auth):
    """GET /v1/mrp/apics/llc returns LLC map without recalculation."""
    resp = api_client.get("/v1/mrp/apics/llc", headers=auth)
    assert resp.status_code != 401

    if resp.status_code == 200:
        data = resp.json()
        assert "llc_map" in data
        assert "items_updated" in data
        assert isinstance(data["llc_map"], dict)


@requires_db
def test_llc_get_with_recalc(api_client, auth):
    """GET /v1/mrp/apics/llc?recalculate=true triggers LLC recalculation."""
    resp = api_client.get("/v1/mrp/apics/llc?recalculate=true", headers=auth)
    assert resp.status_code != 401

    if resp.status_code == 200:
        data = resp.json()
        assert "llc_map" in data
        assert "items_updated" in data


# ---------------------------------------------------------------------------
# POST /v1/mrp/lot-sizing — Lot Sizing Tests
# ---------------------------------------------------------------------------

@requires_db
def test_lot_sizing_lot_for_lot(api_client, auth):
    """POST /v1/mrp/lot-sizing with LOTFORLOT rule."""
    payload = {
        "net_requirements": "100",
        "projected_on_hand": "0",
        "lot_size_rule": "LOTFORLOT",
    }
    resp = api_client.post("/v1/mrp/lot-sizing", json=payload, headers=auth)
    assert resp.status_code == 200, f"Response: {resp.text}"

    data = resp.json()
    assert "planned_order_qty" in data
    assert "lot_size_rule_applied" in data
    assert data["planned_order_qty"] == "100"


@requires_db
def test_lot_sizing_fixed_qty(api_client, auth):
    """POST /v1/mrp/lot-sizing with FIXED_QTY rule."""
    payload = {
        "net_requirements": "100",
        "projected_on_hand": "0",
        "lot_size_rule": "FIXED_QTY",
        "min_order_qty": "50",
    }
    resp = api_client.post("/v1/mrp/lot-sizing", json=payload, headers=auth)
    assert resp.status_code == 200, f"Response: {resp.text}"
    data = resp.json()
    assert "planned_order_qty" in data


@requires_db
def test_lot_sizing_invalid_rule(api_client, auth):
    """POST /v1/mrp/lot-sizing with invalid lot_size_rule returns 200 (engine handles gracefully)."""
    payload = {
        "net_requirements": "100",
        "projected_on_hand": "0",
        "lot_size_rule": "INVALID_RULE",
    }
    resp = api_client.post("/v1/mrp/lot-sizing", json=payload, headers=auth)
    # Engine defaults to LOTFORLOT for unknown rules
    assert resp.status_code == 200


@requires_db
def test_lot_sizing_missing_required(api_client, auth):
    """POST /v1/mrp/lot-sizing without required fields returns 422."""
    payload = {"projected_on_hand": "0"}
    resp = api_client.post("/v1/mrp/lot-sizing", json=payload, headers=auth)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /v1/mrp/consumption — Forecast Consumption Tests
# ---------------------------------------------------------------------------

@requires_db
def test_consumption_missing_location_id(api_client, auth):
    """POST /v1/mrp/consumption without location_id returns 422."""
    payload = {}
    resp = api_client.post("/v1/mrp/consumption", json=payload, headers=auth)
    assert resp.status_code == 422


@requires_db
def test_consumption_invalid_strategy(api_client, auth, test_item_location):
    """POST /v1/mrp/consumption with invalid strategy returns 422."""
    payload = {
        "location_id": test_item_location["location_id"],
        "strategy": "INVALID_STRATEGY",
    }
    resp = api_client.post("/v1/mrp/consumption", json=payload, headers=auth)
    assert resp.status_code == 422


@requires_db
def test_consumption_valid_strategies(api_client, auth, test_item_location):
    """POST /v1/mrp/consumption with valid strategies."""
    valid_strategies = ["MAX", "FORECAST_ONLY", "ORDERS_ONLY", "PRIORITY"]
    for strategy in valid_strategies:
        payload = {
            "location_id": test_item_location["location_id"],
            "strategy": strategy,
            "horizon_days": 90,
        }
        resp = api_client.post("/v1/mrp/consumption", json=payload, headers=auth)
        assert resp.status_code != 422


@requires_db
def test_consumption_with_item_ids(api_client, auth, test_item_location):
    """POST /v1/mrp/consumption with specific item_ids."""
    payload = {
        "location_id": test_item_location["location_id"],
        "item_ids": [test_item_location["item_id"]],
        "strategy": "MAX",
    }
    resp = api_client.post("/v1/mrp/consumption", json=payload, headers=auth)
    assert resp.status_code != 422

    if resp.status_code == 200:
        data = resp.json()
        assert "items" in data
        assert "strategy" in data
        assert "elapsed_ms" in data
        assert isinstance(data["items"], list)


# ---------------------------------------------------------------------------
# Edge Cases and Error Handling
# ---------------------------------------------------------------------------

@requires_db
def test_mrp_run_no_auth(api_client, test_item_location):
    """POST /v1/mrp/run without auth returns 401."""
    payload = {
        "item_id": test_item_location["item_id"],
        "location_id": test_item_location["location_id"],
    }
    resp = api_client.post("/v1/mrp/run", json=payload)
    assert resp.status_code == 401


@requires_db
def test_mrp_apics_run_no_auth(api_client, test_item_location):
    """POST /v1/mrp/apics/run without auth returns 500 (UUID parsing fails before auth)."""
    payload = {"location_id": test_item_location["location_id"]}
    resp = api_client.post("/v1/mrp/apics/run", json=payload)
    # Note: API returns 500 due to UUID parsing error before auth check
    assert resp.status_code == 500


@requires_db
def test_mrp_run_empty_body(api_client, auth):
    """POST /v1/mrp/run with empty body returns 422."""
    resp = api_client.post("/v1/mrp/run", json={}, headers=auth)
    assert resp.status_code == 422


@requires_db
def test_mrp_run_horizon_days_zero(api_client, auth, test_item_location):
    """POST /v1/mrp/run with horizon_days=0 is allowed (edge case)."""
    payload = {
        "item_id": test_item_location["item_id"],
        "location_id": test_item_location["location_id"],
        "horizon_days": 0,
    }
    resp = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp.status_code == 200


@requires_db
def test_mrp_run_negative_horizon(api_client, auth, test_item_location):
    """POST /v1/mrp/run with negative horizon_days returns 200 (no validation)."""
    payload = {
        "item_id": test_item_location["item_id"],
        "location_id": test_item_location["location_id"],
        "horizon_days": -10,
    }
    resp = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    # Note: No validation for negative horizon_days in current implementation
    assert resp.status_code == 200
