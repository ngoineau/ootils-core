"""
Integration tests for POST /v1/ingest/routings (ADR-014 D2 — Phase F).

Covers:
  - CREATED: new routing inserted with operations
  - REPLACED: existing routing dropped + re-created (full-reload)
  - Hour normalization: time_unit='hour' → 'minute' with x60 scaling
  - Unit cohérence: op time_unit must match resource capacity_unit (422)
  - Missing FK (item / resource): 422
  - Duplicate operation sequence in payload: 422 (Pydantic)
  - dry_run does not persist
"""
from __future__ import annotations

import os
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


def _seed(capacity_unit: str = "minute") -> tuple[str, str, UUID, UUID]:
    """Create unique item + resource, return their externals + UUIDs."""
    item_ext = f"RT-ITEM-{uuid4().hex[:8]}"
    res_ext = f"RT-RES-{uuid4().hex[:8]}"
    item_id = uuid4()
    res_id = uuid4()
    with _conn() as c:
        c.execute(
            "INSERT INTO items (item_id, name, item_type, uom, external_id) "
            "VALUES (%s, %s, 'finished_good', 'EA', %s)",
            (item_id, f"Routing test {item_id}", item_ext),
        )
        c.execute(
            "INSERT INTO resources (resource_id, external_id, name, resource_type, "
            "capacity_per_day, capacity_unit, active) "
            "VALUES (%s, %s, 'Test WC', 'work_center', 480.0, %s, TRUE)",
            (res_id, res_ext, capacity_unit),
        )
        c.commit()
    return item_ext, res_ext, item_id, res_id


def _cleanup(item_id: UUID, res_id: UUID):
    with _conn() as c:
        c.execute("DELETE FROM routings WHERE item_id = %s", (item_id,))
        c.execute("DELETE FROM resources WHERE resource_id = %s", (res_id,))
        c.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
        c.commit()


class TestRoutingsIngest:
    def test_creates_routing_with_operations(self, api_client):
        item_ext, res_ext, item_id, res_id = _seed(capacity_unit="minute")
        try:
            r = api_client.post(
                "/v1/ingest/routings",
                json={
                    "routings": [{
                        "item_external_id": item_ext,
                        "sequence": 1,
                        "description": "Std routing",
                        "operations": [
                            {"sequence": 1, "resource_external_id": res_ext,
                             "setup_time": 30, "run_time_per_unit": 1.5,
                             "time_unit": "minute"},
                            {"sequence": 2, "resource_external_id": res_ext,
                             "setup_time": 0, "run_time_per_unit": 0.5,
                             "time_unit": "minute"},
                        ],
                    }],
                    "dry_run": False,
                },
                headers=AUTH_HEADERS,
            )
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["summary"]["inserted"] == 1
            assert body["results"][0]["action"] == "created"
            assert body["results"][0]["operations_count"] == 2
            # Verify in DB
            with _conn() as c:
                row = c.execute(
                    "SELECT COUNT(*) AS cnt FROM routing_operations WHERE routing_id = %s",
                    (UUID(body["results"][0]["routing_id"]),),
                ).fetchone()
                assert row["cnt"] == 2
        finally:
            _cleanup(item_id, res_id)

    def test_replace_existing_routing(self, api_client):
        item_ext, res_ext, item_id, res_id = _seed(capacity_unit="minute")
        try:
            # First push: 2 ops
            api_client.post("/v1/ingest/routings", json={
                "routings": [{
                    "item_external_id": item_ext,
                    "sequence": 1,
                    "operations": [
                        {"sequence": 1, "resource_external_id": res_ext, "run_time_per_unit": 1.0, "time_unit": "minute"},
                        {"sequence": 2, "resource_external_id": res_ext, "run_time_per_unit": 2.0, "time_unit": "minute"},
                    ],
                }],
                "dry_run": False,
            }, headers=AUTH_HEADERS)
            # Second push: 1 op only — must replace, not coexist
            r2 = api_client.post("/v1/ingest/routings", json={
                "routings": [{
                    "item_external_id": item_ext,
                    "sequence": 1,
                    "operations": [
                        {"sequence": 1, "resource_external_id": res_ext, "run_time_per_unit": 5.0, "time_unit": "minute"},
                    ],
                }],
                "dry_run": False,
            }, headers=AUTH_HEADERS | {"Idempotency-Key": "force-no-replay"})
            assert r2.status_code == 200, r2.text
            assert r2.json()["results"][0]["action"] == "replaced"
            # DB has exactly 1 active routing for this item, with 1 op
            with _conn() as c:
                routings = c.execute(
                    "SELECT routing_id FROM routings WHERE item_id = %s AND active = TRUE",
                    (item_id,),
                ).fetchall()
                assert len(routings) == 1
                ops = c.execute(
                    "SELECT run_time_per_unit FROM routing_operations WHERE routing_id = %s",
                    (routings[0]["routing_id"],),
                ).fetchall()
                assert len(ops) == 1
                assert float(ops[0]["run_time_per_unit"]) == 5.0
        finally:
            _cleanup(item_id, res_id)

    def test_hour_normalized_to_minute(self, api_client):
        item_ext, res_ext, item_id, res_id = _seed(capacity_unit="minute")
        try:
            r = api_client.post("/v1/ingest/routings", json={
                "routings": [{
                    "item_external_id": item_ext,
                    "sequence": 1,
                    "operations": [
                        {"sequence": 1, "resource_external_id": res_ext,
                         "setup_time": 0.5, "run_time_per_unit": 2,
                         "time_unit": "hour"},
                    ],
                }],
                "dry_run": False,
            }, headers=AUTH_HEADERS)
            assert r.status_code == 200, r.text
            with _conn() as c:
                op = c.execute(
                    "SELECT setup_time, run_time_per_unit, time_unit "
                    "FROM routing_operations WHERE resource_id = %s ORDER BY sequence",
                    (res_id,),
                ).fetchone()
                # 0.5 hour * 60 = 30 minute, 2 hour * 60 = 120 minute
                assert float(op["setup_time"]) == 30.0
                assert float(op["run_time_per_unit"]) == 120.0
                assert op["time_unit"] == "minute"
        finally:
            _cleanup(item_id, res_id)

    def test_unit_mismatch_minute_op_on_unit_resource_returns_422(self, api_client):
        """ADR-014 D2: op time_unit must match resource capacity_unit."""
        item_ext, res_ext, item_id, res_id = _seed(capacity_unit="unit")
        try:
            r = api_client.post("/v1/ingest/routings", json={
                "routings": [{
                    "item_external_id": item_ext,
                    "sequence": 1,
                    "operations": [
                        {"sequence": 1, "resource_external_id": res_ext,
                         "run_time_per_unit": 1.5,
                         "time_unit": "minute"},  # resource is in 'unit' world
                    ],
                }],
                "dry_run": False,
            }, headers=AUTH_HEADERS)
            assert r.status_code == 422, r.text
            assert "does not match resource" in r.text
        finally:
            _cleanup(item_id, res_id)

    def test_hour_op_on_unit_resource_returns_422(self, api_client):
        """hour normalizes to minute, then must still match resource."""
        item_ext, res_ext, item_id, res_id = _seed(capacity_unit="unit")
        try:
            r = api_client.post("/v1/ingest/routings", json={
                "routings": [{
                    "item_external_id": item_ext,
                    "sequence": 1,
                    "operations": [
                        {"sequence": 1, "resource_external_id": res_ext,
                         "run_time_per_unit": 0.5,
                         "time_unit": "hour"},  # normalized to minute → still mismatches 'unit'
                    ],
                }],
                "dry_run": False,
            }, headers=AUTH_HEADERS)
            assert r.status_code == 422, r.text
        finally:
            _cleanup(item_id, res_id)

    def test_unit_op_on_unit_resource_succeeds(self, api_client):
        item_ext, res_ext, item_id, res_id = _seed(capacity_unit="unit")
        try:
            r = api_client.post("/v1/ingest/routings", json={
                "routings": [{
                    "item_external_id": item_ext,
                    "sequence": 1,
                    "operations": [
                        {"sequence": 1, "resource_external_id": res_ext,
                         "run_time_per_unit": 1.0,
                         "time_unit": "unit"},
                    ],
                }],
                "dry_run": False,
            }, headers=AUTH_HEADERS)
            assert r.status_code == 200, r.text
        finally:
            _cleanup(item_id, res_id)

    def test_unknown_item_returns_422(self, api_client):
        item_ext, res_ext, item_id, res_id = _seed(capacity_unit="minute")
        try:
            r = api_client.post("/v1/ingest/routings", json={
                "routings": [{
                    "item_external_id": "DOES-NOT-EXIST",
                    "sequence": 1,
                    "operations": [
                        {"sequence": 1, "resource_external_id": res_ext,
                         "run_time_per_unit": 1.0, "time_unit": "minute"},
                    ],
                }],
                "dry_run": False,
            }, headers=AUTH_HEADERS)
            assert r.status_code == 422
        finally:
            _cleanup(item_id, res_id)

    def test_unknown_resource_returns_422(self, api_client):
        item_ext, res_ext, item_id, res_id = _seed(capacity_unit="minute")
        try:
            r = api_client.post("/v1/ingest/routings", json={
                "routings": [{
                    "item_external_id": item_ext,
                    "sequence": 1,
                    "operations": [
                        {"sequence": 1, "resource_external_id": "UNKNOWN-RES",
                         "run_time_per_unit": 1.0, "time_unit": "minute"},
                    ],
                }],
                "dry_run": False,
            }, headers=AUTH_HEADERS)
            assert r.status_code == 422
        finally:
            _cleanup(item_id, res_id)

    def test_duplicate_operation_sequence_returns_422(self, api_client):
        item_ext, res_ext, item_id, res_id = _seed(capacity_unit="minute")
        try:
            r = api_client.post("/v1/ingest/routings", json={
                "routings": [{
                    "item_external_id": item_ext,
                    "sequence": 1,
                    "operations": [
                        {"sequence": 1, "resource_external_id": res_ext, "run_time_per_unit": 1.0, "time_unit": "minute"},
                        {"sequence": 1, "resource_external_id": res_ext, "run_time_per_unit": 2.0, "time_unit": "minute"},
                    ],
                }],
                "dry_run": False,
            }, headers=AUTH_HEADERS)
            assert r.status_code == 422  # Pydantic validator
        finally:
            _cleanup(item_id, res_id)

    def test_dry_run_does_not_persist(self, api_client):
        item_ext, res_ext, item_id, res_id = _seed(capacity_unit="minute")
        try:
            r = api_client.post("/v1/ingest/routings", json={
                "routings": [{
                    "item_external_id": item_ext,
                    "sequence": 1,
                    "operations": [
                        {"sequence": 1, "resource_external_id": res_ext,
                         "run_time_per_unit": 1.0, "time_unit": "minute"},
                    ],
                }],
                "dry_run": True,
            }, headers=AUTH_HEADERS)
            assert r.status_code == 200
            assert r.json()["status"] == "dry_run"
            with _conn() as c:
                cnt = c.execute("SELECT COUNT(*) AS cnt FROM routings WHERE item_id = %s", (item_id,)).fetchone()
                assert cnt["cnt"] == 0
        finally:
            _cleanup(item_id, res_id)
