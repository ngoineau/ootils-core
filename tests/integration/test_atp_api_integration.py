"""
Integration tests for the ATP/CTP FastAPI routers against a real
PostgreSQL database (no mocks).

Ported from tests/test_atp_api.py — every test that previously patched
``_resolve_item_uuid`` / ``_resolve_location_uuid`` / ``ATPEngine.calculate``
/ ``CTPEngine.check`` / ``CTPEngine.simulate_first_feasible_date`` is
re-implemented here using the seeded test database from
``tests/integration/test_api_db.py`` (PUMP-01 / VALVE-02 items at
DC-ATL / DC-LAX locations).

Because we use real engines and real seeded data, assertions are
written against the response *structure* rather than against mock
return values. Specific numeric outcomes are only asserted when the
seed data unambiguously produces them.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

SEED_SCRIPT = Path(__file__).parents[2] / "scripts" / "seed_demo_data.py"
AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}


def _run_seed():
    env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    return subprocess.run(
        [sys.executable, str(SEED_SCRIPT)],
        capture_output=True, text=True, env=env,
    )


# ---------------------------------------------------------------------------
# Shared module-scoped fixtures (mirror tests/integration/test_api_db.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def seeded_db(migrated_db):
    """Module-scoped: migrated DB with seed data loaded once."""
    result = _run_seed()
    if result.returncode != 0:
        pytest.skip(f"Seed failed: {result.stderr[:500]}")
    return migrated_db


@pytest.fixture(scope="module")
def api_client(seeded_db):
    """Module-scoped FastAPI TestClient bound to the real test DB."""
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
def auth():
    return AUTH_HEADERS


# ---------------------------------------------------------------------------
# ATP Check — DB-backed tests
# ---------------------------------------------------------------------------


class TestATPCheckEndpoint:
    """POST /v1/atp/check against a real DB."""

    def test_atp_check_item_not_found(self, api_client, auth):
        """Non-UUID, unknown external_id → 404."""
        payload = {
            "item_id": "non-existent-item-xyz",  # not a UUID, not an external_id in seed
            "location_id": str(uuid4()),
            "quantity": 100,
            "requested_date": date.today().isoformat(),
        }
        resp = api_client.post("/v1/atp/check", json=payload, headers=auth)
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    def test_atp_check_location_not_found(self, api_client, auth):
        """Valid item external_id + unknown location external_id → 404 on location."""
        payload = {
            "item_id": "PUMP-01",  # seeded
            "location_id": "non-existent-loc-xyz",
            "quantity": 100,
            "requested_date": date.today().isoformat(),
        }
        resp = api_client.post("/v1/atp/check", json=payload, headers=auth)
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    def test_atp_check_success_with_external_ids(self, api_client, auth):
        """Real ATP run against seeded PUMP-01 @ DC-ATL."""
        payload = {
            "item_id": "PUMP-01",
            "location_id": "DC-ATL",
            "quantity": 10,
            "requested_date": date.today().isoformat(),
            "horizon_days": 30,
        }
        resp = api_client.post("/v1/atp/check", json=payload, headers=auth)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "available" in data
        assert isinstance(data["available"], bool)
        assert "quantity_available" in data
        assert "requested_quantity" in data
        assert "backorder_quantity" in data
        assert "buckets" in data
        assert isinstance(data["buckets"], list)
        assert "calculation_time_ms" in data
        # backorder + available should reflect requested qty consistently
        assert data["requested_quantity"] in ("10", 10, 10.0)

    def test_atp_check_success_with_uuids(self, api_client, auth, seeded_db):
        """Real ATP run with item/location passed as UUIDs (resolved via UUID() short-circuit)."""
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            item_row = conn.execute(
                "SELECT item_id FROM items WHERE external_id = 'PUMP-01'"
            ).fetchone()
            loc_row = conn.execute(
                "SELECT location_id FROM locations WHERE external_id = 'DC-ATL'"
            ).fetchone()

        assert item_row and loc_row, "Seed missing PUMP-01 / DC-ATL"

        payload = {
            "item_id": str(item_row["item_id"]),
            "location_id": str(loc_row["location_id"]),
            "quantity": 5,
            "requested_date": date.today().isoformat(),
            "horizon_days": 30,
        }
        resp = api_client.post("/v1/atp/check", json=payload, headers=auth)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "available" in data
        assert "buckets" in data

    def test_atp_check_large_quantity_partial_shortage(self, api_client, auth):
        """Asking for far more than seeded stock yields backorder > 0 (partial/zero availability)."""
        payload = {
            "item_id": "PUMP-01",
            "location_id": "DC-ATL",
            "quantity": 10_000_000,  # vastly exceeds any seed value
            "requested_date": date.today().isoformat(),
            "horizon_days": 7,
        }
        resp = api_client.post("/v1/atp/check", json=payload, headers=auth)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["available"] is False
        # backorder should be > 0 since stock can't cover 10M units in 7 days
        from decimal import Decimal
        assert Decimal(str(data["backorder_quantity"])) > 0


# ---------------------------------------------------------------------------
# CTP Check — DB-backed tests
# ---------------------------------------------------------------------------


class TestCTPCheckEndpoint:
    """POST /v1/ctp/check against a real DB."""

    def test_ctp_check_item_not_found(self, api_client, auth):
        payload = {
            "item_id": "non-existent-item-xyz",
            "location_id": str(uuid4()),
            "quantity": 100,
            "requested_date": date.today().isoformat(),
        }
        resp = api_client.post("/v1/ctp/check", json=payload, headers=auth)
        assert resp.status_code == 404

    def test_ctp_check_success_basic(self, api_client, auth):
        """Real CTP run on seeded data — verify response shape."""
        payload = {
            "item_id": "PUMP-01",
            "location_id": "DC-ATL",
            "quantity": 5,
            "requested_date": date.today().isoformat(),
            "horizon_days": 30,
        }
        resp = api_client.post("/v1/ctp/check", json=payload, headers=auth)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "available" in data
        assert "capacity_feasible" in data
        assert "violations" in data
        assert isinstance(data["violations"], list)
        assert "critical_resources" in data
        assert isinstance(data["critical_resources"], list)
        assert "buckets" in data

    def test_ctp_check_with_capacity_false(self, api_client, auth):
        """include_capacity=false skips capacity step — violations empty."""
        payload = {
            "item_id": "PUMP-01",
            "location_id": "DC-ATL",
            "quantity": 5,
            "requested_date": date.today().isoformat(),
            "include_capacity": False,
        }
        resp = api_client.post("/v1/ctp/check", json=payload, headers=auth)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        # With include_capacity=false, violations list is empty regardless
        assert data["violations"] == []


# ---------------------------------------------------------------------------
# CTP Simulate — DB-backed tests
# ---------------------------------------------------------------------------


class TestCTPSimulateEndpoint:
    """POST /v1/ctp/simulate against a real DB."""

    def test_ctp_simulate_item_not_found(self, api_client, auth):
        payload = {
            "item_id": "non-existent-item-xyz",
            "location_id": str(uuid4()),
            "quantity": 100,
        }
        resp = api_client.post("/v1/ctp/simulate", json=payload, headers=auth)
        assert resp.status_code == 404

    def test_ctp_simulate_success_basic(self, api_client, auth):
        """Real CTP simulation — verify response shape and option count."""
        start_date = date.today() + timedelta(days=1)
        payload = {
            "item_id": "PUMP-01",
            "location_id": "DC-ATL",
            "quantity": 5,
            "start_date": start_date.isoformat(),
            "max_days": 14,
        }
        resp = api_client.post("/v1/ctp/simulate", json=payload, headers=auth)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "first_feasible_date" in data  # may be None
        assert "options" in data
        assert isinstance(data["options"], list)
        assert "total_dates_tested" in data
        assert data["total_dates_tested"] == len(data["options"])

    def test_ctp_simulate_huge_quantity_no_feasible(self, api_client, auth):
        """Quantity far exceeding any seed → no feasible date in horizon."""
        payload = {
            "item_id": "PUMP-01",
            "location_id": "DC-ATL",
            "quantity": 10_000_000,
            "max_days": 10,
        }
        resp = api_client.post("/v1/ctp/simulate", json=payload, headers=auth)
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["first_feasible_date"] is None
        # Options should still have been tested
        assert isinstance(data["options"], list)
