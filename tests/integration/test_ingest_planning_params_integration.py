"""
Integration tests for POST /v1/ingest/planning-params (ADR-014 D3).

Exercises the SCD2-transparent endpoint against real Postgres:
  - CREATED: first push, no prior history
  - NOOP: re-push identical values → idempotent
  - ROTATED: cross-day change → close active row, insert new
  - UPDATED_INPLACE: same-day change → UPDATE active row in place
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from uuid import UUID, uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def _conn():
    return psycopg.connect(TEST_DB_URL, row_factory=psycopg.rows.dict_row)


def _seed_item_location() -> tuple[str, str, UUID, UUID]:
    """Create a unique item + location and return their external_ids + UUIDs."""
    item_ext = f"PP-ITEM-{uuid4().hex[:8]}"
    loc_ext = f"PP-LOC-{uuid4().hex[:8]}"
    item_id = uuid4()
    location_id = uuid4()
    with _conn() as c:
        c.execute(
            "INSERT INTO items (item_id, name, item_type, uom, external_id) "
            "VALUES (%s, %s, 'finished_good', 'EA', %s)",
            (item_id, f"PP test item {item_id}", item_ext),
        )
        c.execute(
            "INSERT INTO locations (location_id, name, location_type, external_id) "
            "VALUES (%s, %s, 'dc', %s)",
            (location_id, f"PP test loc {location_id}", loc_ext),
        )
        c.commit()
    return item_ext, loc_ext, item_id, location_id


def _cleanup(item_id: UUID, location_id: UUID):
    with _conn() as c:
        c.execute(
            "DELETE FROM item_planning_params WHERE item_id = %s AND location_id = %s",
            (item_id, location_id),
        )
        c.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))
        c.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
        c.commit()


def _count_active(item_id: UUID, location_id: UUID) -> int:
    with _conn() as c:
        row = c.execute(
            "SELECT COUNT(*) AS cnt FROM item_planning_params "
            "WHERE item_id = %s AND location_id = %s AND effective_to IS NULL",
            (item_id, location_id),
        ).fetchone()
        return row["cnt"]


def _count_history(item_id: UUID, location_id: UUID) -> int:
    with _conn() as c:
        row = c.execute(
            "SELECT COUNT(*) AS cnt FROM item_planning_params "
            "WHERE item_id = %s AND location_id = %s",
            (item_id, location_id),
        ).fetchone()
        return row["cnt"]


class TestPlanningParamsScd2:
    """End-to-end SCD2 transparent behaviour against real DB."""

    def test_first_push_creates_new_active_row(self, api_client):
        item_ext, loc_ext, item_id, location_id = _seed_item_location()
        try:
            resp = api_client.post(
                "/v1/ingest/planning-params",
                json={
                    "params": [{
                        "item_external_id": item_ext,
                        "location_external_id": loc_ext,
                        "lead_time_sourcing_days": 5,
                        "safety_stock_qty": 50,
                        "lot_size_rule": "LOTFORLOT",
                    }],
                    "dry_run": False,
                },
                headers=AUTH_HEADERS,
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["summary"]["inserted"] == 1
            assert body["summary"]["updated"] == 0
            assert body["results"][0]["action"] == "created"
            # One active row, total history = 1
            assert _count_active(item_id, location_id) == 1
            assert _count_history(item_id, location_id) == 1
        finally:
            _cleanup(item_id, location_id)

    def test_identical_repush_is_noop(self, api_client):
        item_ext, loc_ext, item_id, location_id = _seed_item_location()
        try:
            payload = {
                "params": [{
                    "item_external_id": item_ext,
                    "location_external_id": loc_ext,
                    "lead_time_sourcing_days": 5,
                    "safety_stock_qty": 50,
                }],
                "dry_run": False,
            }
            r1 = api_client.post("/v1/ingest/planning-params", json=payload, headers=AUTH_HEADERS)
            assert r1.status_code == 200
            assert r1.json()["results"][0]["action"] == "created"

            # Repush the EXACT same values — must be no-op
            r2 = api_client.post(
                "/v1/ingest/planning-params",
                json={**payload, "params": payload["params"]},  # different payload object, same content
                headers=AUTH_HEADERS | {"Idempotency-Key": "force-no-replay"},
            )
            assert r2.status_code == 200, r2.text
            assert r2.json()["summary"]["inserted"] == 0
            assert r2.json()["summary"]["updated"] == 0
            assert r2.json()["results"][0]["action"] == "noop"
            assert _count_active(item_id, location_id) == 1
            assert _count_history(item_id, location_id) == 1
        finally:
            _cleanup(item_id, location_id)

    def test_same_day_change_updates_in_place(self, api_client):
        item_ext, loc_ext, item_id, location_id = _seed_item_location()
        try:
            # First push — CREATED on today
            api_client.post(
                "/v1/ingest/planning-params",
                json={
                    "params": [{
                        "item_external_id": item_ext,
                        "location_external_id": loc_ext,
                        "lead_time_sourcing_days": 5,
                    }],
                    "dry_run": False,
                },
                headers=AUTH_HEADERS,
            )
            # Second push, same day, different value — must UPDATE in place
            r2 = api_client.post(
                "/v1/ingest/planning-params",
                json={
                    "params": [{
                        "item_external_id": item_ext,
                        "location_external_id": loc_ext,
                        "lead_time_sourcing_days": 8,
                    }],
                    "dry_run": False,
                },
                headers=AUTH_HEADERS | {"Idempotency-Key": "no-replay-2"},
            )
            assert r2.status_code == 200, r2.text
            body = r2.json()
            assert body["results"][0]["action"] == "updated_inplace"
            assert "lead_time_sourcing_days" in body["results"][0]["changed_fields"]
            # Still only one row total
            assert _count_history(item_id, location_id) == 1
            # And the value is the new one
            with _conn() as c:
                row = c.execute(
                    "SELECT lead_time_sourcing_days FROM item_planning_params "
                    "WHERE item_id = %s AND location_id = %s AND effective_to IS NULL",
                    (item_id, location_id),
                ).fetchone()
                assert row["lead_time_sourcing_days"] == 8
        finally:
            _cleanup(item_id, location_id)

    def test_cross_day_change_rotates(self, api_client):
        """Simulate a cross-day change by inserting an active row with
        effective_from = yesterday, then push a new value today."""
        item_ext, loc_ext, item_id, location_id = _seed_item_location()
        try:
            # Seed an active row with effective_from = yesterday
            yesterday = date.today() - timedelta(days=1)
            with _conn() as c:
                c.execute(
                    "INSERT INTO item_planning_params "
                    "(item_id, location_id, effective_from, effective_to, "
                    "lead_time_sourcing_days, lot_size_rule, planning_horizon_days, is_make) "
                    "VALUES (%s, %s, %s, NULL, %s, 'LOTFORLOT', 90, false)",
                    (item_id, location_id, yesterday, 5),
                )
                c.commit()

            # Now push a different value — must rotate
            r = api_client.post(
                "/v1/ingest/planning-params",
                json={
                    "params": [{
                        "item_external_id": item_ext,
                        "location_external_id": loc_ext,
                        "lead_time_sourcing_days": 10,
                    }],
                    "dry_run": False,
                },
                headers=AUTH_HEADERS,
            )
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["results"][0]["action"] == "rotated"

            # 2 rows total now: the old one (effective_to = yesterday) + new one (active)
            assert _count_history(item_id, location_id) == 2
            assert _count_active(item_id, location_id) == 1

            # The currently active row must hold the new value AND carry the old lot_size_rule
            with _conn() as c:
                row = c.execute(
                    "SELECT lead_time_sourcing_days, lot_size_rule FROM item_planning_params "
                    "WHERE item_id = %s AND location_id = %s AND effective_to IS NULL",
                    (item_id, location_id),
                ).fetchone()
                assert row["lead_time_sourcing_days"] == 10
                assert row["lot_size_rule"] == "LOTFORLOT"  # carried over from the closed row
        finally:
            _cleanup(item_id, location_id)

    def test_dry_run_does_not_persist(self, api_client):
        item_ext, loc_ext, item_id, location_id = _seed_item_location()
        try:
            r = api_client.post(
                "/v1/ingest/planning-params",
                json={
                    "params": [{
                        "item_external_id": item_ext,
                        "location_external_id": loc_ext,
                        "lead_time_sourcing_days": 5,
                    }],
                    "dry_run": True,
                },
                headers=AUTH_HEADERS,
            )
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["status"] == "dry_run"
            assert body["results"][0]["action"] == "created"
            # Nothing persisted
            assert _count_history(item_id, location_id) == 0
        finally:
            _cleanup(item_id, location_id)

    def test_unknown_item_returns_422(self, api_client):
        r = api_client.post(
            "/v1/ingest/planning-params",
            json={
                "params": [{
                    "item_external_id": "DOES-NOT-EXIST",
                    "location_external_id": "DOES-NOT-EXIST-EITHER",
                    "lead_time_sourcing_days": 5,
                }],
                "dry_run": False,
            },
            headers=AUTH_HEADERS,
        )
        assert r.status_code == 422
