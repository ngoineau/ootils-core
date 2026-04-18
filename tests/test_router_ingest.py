"""
test_router_ingest.py — Comprehensive unit tests for the ingest router.

Mocks the psycopg.Connection via FastAPI dependency overrides so we can drive
every branch of src/ootils_core/api/routers/ingest.py without a live database.

Coverage targets:
  - All 8 POST endpoints (happy / dry_run / 422 / FK errors / empty payload)
  - All helpers: _create_ingest_batch, _trigger_dq, _batch_existing,
    _wire_node_to_pi, _ensure_projection_series, _emit_ingestion_event,
    _ok, _dry_run_response, _raise_422
  - DQ failure path (logged, never raised)
  - Edge wiring for replenishes / consumes / unsupported node types
  - Projection series new vs. existing branch
"""
from __future__ import annotations

import os
from datetime import date
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from psycopg import sql as psy_sql

# Auth token must be set BEFORE the app is imported
os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.app import create_app
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db
from ootils_core.api.routers import ingest as ingest_module
from ootils_core.api.routers.ingest import (
    _batch_existing,
    _create_ingest_batch,
    _dry_run_response,
    _emit_ingestion_event,
    _ensure_projection_series,
    _ok,
    _request_hash,
    _raise_422,
    _trigger_dq,
    _wire_node_to_pi,
    IngestItemsRequest,
    ItemRow,
)


AUTH_HEADERS = {"Authorization": "Bearer test-token"}


# ─────────────────────────────────────────────────────────────
# Mock infrastructure
# ─────────────────────────────────────────────────────────────


class FakeCursor:
    """Imitates the psycopg cursor returned by Connection.execute().

    `fetchone_value` may be a single dict (returned for every fetchone) or a
    list of dicts/None which are returned in order. `fetchall_value` is a list
    of dicts (defaults to empty).
    """

    def __init__(
        self,
        fetchone_value: Any | list[Any] | None = None,
        fetchall_value: list[Any] | None = None,
    ):
        self._fetchone_value = fetchone_value
        self._fetchall_value = fetchall_value if fetchall_value is not None else []
        self._fetchone_idx = 0
        self.rowcount = 1

    def fetchone(self):
        if isinstance(self._fetchone_value, list):
            if self._fetchone_idx >= len(self._fetchone_value):
                return None
            v = self._fetchone_value[self._fetchone_idx]
            self._fetchone_idx += 1
            return v
        return self._fetchone_value

    def fetchall(self):
        return self._fetchall_value


class FakeDB:
    """Mock psycopg.Connection that returns scripted cursors per call.

    Handlers receive `(sql, params)` and return a FakeCursor (or None for
    a default empty cursor). Use `set_handler` to attach a router.
    """

    def __init__(self, handler=None):
        self.handler = handler or (lambda sql, params: FakeCursor())
        self.calls: list[tuple[str, tuple]] = []
        self.transaction_calls = 0

    class _TransactionCM:
        def __init__(self, owner):
            self.owner = owner

        def __enter__(self):
            self.owner.transaction_calls += 1
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def transaction(self):
        return self._TransactionCM(self)

    def execute(self, sql, params=None):
        rendered_sql = sql
        if not isinstance(sql, str):
            rendered_sql = psy_sql.as_string(sql)
            rendered_sql = rendered_sql.replace('"', '')
            rendered_sql = ' '.join(rendered_sql.split())
        self.calls.append((rendered_sql, params if params is not None else ()))
        result = self.handler(rendered_sql, params if params is not None else ())
        return result if result is not None else FakeCursor()


def make_client(db: FakeDB) -> TestClient:
    """Build a TestClient whose `get_db` yields the provided FakeDB."""
    app = create_app()

    def override_db():
        yield db

    app.dependency_overrides[get_db] = override_db
    return TestClient(app)


# A reusable handler that always returns DQ "passed" for run_dq.
class FakeDQResult:
    def __init__(self, status_str="passed"):
        self.batch_dq_status = status_str


# ─────────────────────────────────────────────────────────────
# Helper functions: pure unit tests
# ─────────────────────────────────────────────────────────────


def test_ok_returns_proper_response():
    resp = _ok(2, 1, 3, [{"a": 1}], batch_id=None, dq_status="passed")
    assert resp.status == "ok"
    assert resp.summary.total == 3
    assert resp.summary.inserted == 2
    assert resp.summary.updated == 1
    assert resp.summary.errors == 0
    assert resp.dq_status == "passed"


def test_dry_run_response_uses_label_and_count():
    items = [ItemRow(external_id="A1", name="A"), ItemRow(external_id="B2", name="B")]
    resp = _dry_run_response(items, label="external_id")
    assert resp.status == "dry_run"
    assert resp.summary.total == 2
    assert resp.results[0]["external_id"] == "A1"
    assert resp.results[0]["action"] == "dry_run"


def test_dry_run_response_missing_attribute_uses_question_mark():
    class Stub:
        pass

    resp = _dry_run_response([Stub()], label="external_id")
    assert resp.results[0]["external_id"] == "?"


def test_raise_422_raises_http_exception():
    with pytest.raises(HTTPException) as exc:
        _raise_422([{"row": 0, "errors": ["bad"]}])
    assert exc.value.status_code == 422
    assert exc.value.detail == [{"row": 0, "errors": ["bad"]}]


def test_create_ingest_batch_inserts_batch_and_rows():
    db = FakeDB()
    rows = [{"external_id": "A"}, ItemRow(external_id="B", name="B")]
    batch_id = _create_ingest_batch(db, "items", rows, source_system="src-x")
    assert isinstance(batch_id, UUID)
    # 1 batch insert + 2 row inserts = 3 calls
    assert len(db.calls) == 3
    assert "INSERT INTO ingest_batches" in db.calls[0][0]
    assert "INSERT INTO ingest_rows" in db.calls[1][0]
    # Pydantic model row should be model_dump'd to JSON, dict row used as-is
    assert "INSERT INTO ingest_rows" in db.calls[2][0]


def test_trigger_dq_returns_batch_status():
    db = FakeDB()
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult("passed")):
        assert _trigger_dq(db, uuid4()) == "passed"
    assert db.transaction_calls == 1


def test_trigger_dq_swallows_exceptions_and_returns_unknown():
    db = FakeDB()
    with patch.object(ingest_module, "run_dq", side_effect=RuntimeError("boom")):
        assert _trigger_dq(db, uuid4()) == "unknown"
    assert db.transaction_calls == 1


def test_batch_existing_empty_list_short_circuit():
    db = FakeDB()
    result = _batch_existing(db, "items", "external_id", "item_id", [])
    assert result == {}
    assert db.calls == []


def test_batch_existing_returns_mapping():
    item_a = uuid4()
    item_b = uuid4()
    rows = [
        {"external_id": "A", "item_id": item_a},
        {"external_id": "B", "item_id": item_b},
    ]
    db = FakeDB(handler=lambda sql, params: FakeCursor(fetchall_value=rows))
    result = _batch_existing(db, "items", "external_id", "item_id", ["A", "B"])
    assert result == {"A": item_a, "B": item_b}


def test_emit_ingestion_event_executes_insert():
    db = FakeDB()
    _emit_ingestion_event(db, BASELINE_SCENARIO_ID, uuid4())
    assert len(db.calls) == 1
    assert "INSERT INTO events" in db.calls[0][0]


def test_ensure_projection_series_returns_false_when_existing():
    series_id = uuid4()
    db = FakeDB(handler=lambda sql, params: FakeCursor(fetchone_value={"series_id": series_id}))
    created = _ensure_projection_series(db, uuid4(), uuid4(), BASELINE_SCENARIO_ID)
    assert created is False
    # Only the SELECT should have run
    assert len(db.calls) == 1
    assert "SELECT series_id FROM projection_series" in db.calls[0][0]


def test_ensure_projection_series_creates_when_missing():
    series_id = uuid4()
    state = {"select_calls": 0}

    def handler(sql, params):
        if "SELECT series_id FROM projection_series" in sql:
            state["select_calls"] += 1
            if state["select_calls"] == 1:
                return FakeCursor(fetchone_value=None)
            return FakeCursor(fetchone_value={"series_id": series_id})
        return FakeCursor()

    db = FakeDB(handler=handler)
    created = _ensure_projection_series(db, uuid4(), uuid4(), BASELINE_SCENARIO_ID)
    assert created is True
    # 2 selects + 1 insert series + 90 bucket inserts = 93 calls
    assert len(db.calls) == 93


def test_ensure_projection_series_falls_back_to_local_uuid_when_select_returns_none():
    """Branch where the post-INSERT SELECT returns no row — series_id falls
    back to the locally-generated UUID."""
    state = {"select_calls": 0}

    def handler(sql, params):
        if "SELECT series_id FROM projection_series" in sql:
            state["select_calls"] += 1
            return FakeCursor(fetchone_value=None)
        return FakeCursor()

    db = FakeDB(handler=handler)
    created = _ensure_projection_series(db, uuid4(), uuid4(), BASELINE_SCENARIO_ID)
    assert created is True


def test_wire_node_to_pi_replenishes_supply():
    pi_node_id = uuid4()
    db = FakeDB(handler=lambda sql, params: FakeCursor(fetchone_value={"node_id": pi_node_id}))
    n = _wire_node_to_pi(
        db, uuid4(), "PurchaseOrderSupply",
        uuid4(), uuid4(), BASELINE_SCENARIO_ID, date.today(),
    )
    assert n == 1
    # Last call should be the edge insert with edge_type='replenishes'
    last_sql, last_params = db.calls[-1]
    assert "INSERT INTO edges" in last_sql
    assert "replenishes" in last_params


def test_wire_node_to_pi_consumes_demand():
    pi_node_id = uuid4()
    db = FakeDB(handler=lambda sql, params: FakeCursor(fetchone_value={"node_id": pi_node_id}))
    n = _wire_node_to_pi(
        db, uuid4(), "ForecastDemand",
        uuid4(), uuid4(), BASELINE_SCENARIO_ID, date.today(),
    )
    assert n == 1
    last_sql, last_params = db.calls[-1]
    assert "consumes" in last_params


def test_wire_node_to_pi_unsupported_node_type_returns_zero():
    db = FakeDB()
    n = _wire_node_to_pi(
        db, uuid4(), "ProjectedInventory",
        uuid4(), uuid4(), BASELINE_SCENARIO_ID, date.today(),
    )
    assert n == 0
    assert db.calls == []


def test_wire_node_to_pi_no_pi_bucket_found():
    db = FakeDB(handler=lambda sql, params: FakeCursor(fetchone_value=None))
    n = _wire_node_to_pi(
        db, uuid4(), "OnHandSupply",
        uuid4(), uuid4(), BASELINE_SCENARIO_ID, date.today(),
    )
    assert n == 0


def test_wire_node_to_pi_other_supply_types():
    """Cover the other supply branches (WorkOrderSupply, PlannedSupply, TransferSupply, OnHandSupply)."""
    pi_node_id = uuid4()
    for nt in ("WorkOrderSupply", "PlannedSupply", "TransferSupply", "OnHandSupply", "CustomerOrderDemand"):
        db = FakeDB(handler=lambda sql, params: FakeCursor(fetchone_value={"node_id": pi_node_id}))
        n = _wire_node_to_pi(
            db, uuid4(), nt, uuid4(), uuid4(), BASELINE_SCENARIO_ID, date.today(),
        )
        assert n == 1


# ─────────────────────────────────────────────────────────────
# Auth tests
# ─────────────────────────────────────────────────────────────


def test_ingest_items_requires_auth():
    db = FakeDB()
    client = make_client(db)
    resp = client.post("/v1/ingest/items", json={"items": []})
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# 1. POST /v1/ingest/items
# ─────────────────────────────────────────────────────────────


def _passing_dq_db(extra_handler=None):
    """Build a FakeDB that returns 'passed' DQ for any new batch."""

    def handler(sql, params):
        if extra_handler:
            forced = extra_handler(sql, params)
            if forced is not None:
                return forced
        return FakeCursor()

    return FakeDB(handler=handler)


def test_ingest_items_happy_insert():
    db = _passing_dq_db()
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/items",
            json={
                "items": [
                    {"external_id": "SKU-001", "name": "Pump", "item_type": "finished_good", "uom": "EA", "status": "active"},
                    {"external_id": "SKU-002", "name": "Gasket"},
                ]
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ok"
    assert body["summary"]["inserted"] == 2
    assert body["summary"]["updated"] == 0


def test_ingest_items_happy_update():
    existing_id = uuid4()

    def handler(sql, params):
        if "SELECT external_id, item_id FROM items" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-001", "item_id": existing_id}])
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/items",
            json={"items": [{"external_id": "SKU-001", "name": "Pump"}]},
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["summary"]["updated"] == 1
    assert body["summary"]["inserted"] == 0


def test_ingest_items_invalid_item_type_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/items",
        json={"items": [{"external_id": "SKU-1", "name": "X", "item_type": "weird", "status": "active"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_items_invalid_status_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/items",
        json={"items": [{"external_id": "SKU-1", "name": "X", "status": "broken"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_items_dry_run_skips_writes():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/items",
        json={"dry_run": True, "items": [{"external_id": "SKU-1", "name": "X"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "dry_run"
    assert db.calls == []


def test_ingest_items_empty_payload_calls_dq_with_empty_batch():
    db = FakeDB()
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/items",
            json={"items": []},
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["summary"]["total"] == 0


def test_ingest_items_dq_failure_logged_not_raised():
    db = FakeDB()
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", side_effect=RuntimeError("dq fail")):
        resp = client.post(
            "/v1/ingest/items",
            json={"items": [{"external_id": "SKU-1", "name": "X"}]},
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["dq_status"] == "unknown"


def test_health_includes_correlation_and_version_headers():
    client = make_client(FakeDB())
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.headers["X-API-Version"] == "1.0.0"
    assert resp.headers["X-Correlation-ID"].startswith("req_")


def test_ingest_items_echoes_correlation_id_header():
    db = _passing_dq_db()
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/items",
            json={"items": [{"external_id": "SKU-001", "name": "Pump"}]},
            headers={**AUTH_HEADERS, "X-Correlation-ID": "corr-123"},
        )
    assert resp.status_code == 200
    assert resp.headers["X-Correlation-ID"] == "corr-123"


def test_ingest_items_idempotent_replay_returns_stored_response():
    body = IngestItemsRequest(items=[ItemRow(external_id="SKU-001", name="Pump")])
    stored = _ok(
        1,
        0,
        1,
        [{"external_id": "SKU-001", "item_id": str(uuid4()), "action": "inserted"}],
        batch_id=uuid4(),
        dq_status="passed",
    )

    def handler(sql, params):
        if "SELECT entity_type, request_hash, response_json" in sql:
            return FakeCursor(
                fetchone_value={
                    "entity_type": "items",
                    "request_hash": _request_hash(body),
                    "response_json": stored.model_dump_json(),
                }
            )
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/items",
        json={"items": [{"external_id": "SKU-001", "name": "Pump"}]},
        headers={**AUTH_HEADERS, "Idempotency-Key": "idem-1"},
    )
    assert resp.status_code == 200
    assert resp.headers["X-Idempotent-Replay"] == "true"
    assert resp.json()["batch_id"] == str(stored.batch_id)
    assert not any("INSERT INTO items" in sql for sql, _ in db.calls)


def test_ingest_items_idempotency_conflict_on_different_payload():
    original_body = IngestItemsRequest(items=[ItemRow(external_id="SKU-001", name="Pump")])

    def handler(sql, params):
        if "SELECT entity_type, request_hash, response_json" in sql:
            return FakeCursor(
                fetchone_value={
                    "entity_type": "items",
                    "request_hash": _request_hash(original_body),
                    "response_json": None,
                }
            )
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/items",
        json={"items": [{"external_id": "SKU-001", "name": "Pump v2"}]},
        headers={**AUTH_HEADERS, "Idempotency-Key": "idem-1"},
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "idempotency.conflict"


# ─────────────────────────────────────────────────────────────
# 2. POST /v1/ingest/locations
# ─────────────────────────────────────────────────────────────


def test_ingest_locations_happy_insert():
    db = _passing_dq_db()
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/locations",
            json={"locations": [{"external_id": "DC-ATL", "name": "Atlanta DC", "location_type": "dc"}]},
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["summary"]["inserted"] == 1


def test_ingest_locations_invalid_type_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/locations",
        json={"locations": [{"external_id": "DC-ATL", "name": "X", "location_type": "moon"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_locations_parent_not_found_422():
    """parent_external_id missing from payload AND DB → 422."""

    def handler(sql, params):
        if "SELECT 1 FROM locations" in sql:
            return FakeCursor(fetchone_value=None)
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/locations",
        json={"locations": [{"external_id": "DC-ATL", "name": "X", "parent_external_id": "DC-NA"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_locations_parent_found_in_db_passes():
    def handler(sql, params):
        if "SELECT 1 FROM locations" in sql:
            return FakeCursor(fetchone_value={"?column?": 1})
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/locations",
            json={"locations": [{"external_id": "DC-ATL", "name": "X", "parent_external_id": "DC-NA"}]},
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200


def test_ingest_locations_parent_in_payload_passes():
    db = _passing_dq_db()
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/locations",
            json={
                "locations": [
                    {"external_id": "DC-NA", "name": "NA"},
                    {"external_id": "DC-ATL", "name": "Atlanta", "parent_external_id": "DC-NA"},
                ]
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200


def test_ingest_locations_dry_run():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/locations",
        json={"dry_run": True, "locations": [{"external_id": "DC-ATL", "name": "X"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "dry_run"


def test_ingest_locations_update_existing():
    existing_id = uuid4()

    def handler(sql, params):
        if "SELECT external_id, location_id FROM locations" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-ATL", "location_id": existing_id}])
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/locations",
            json={"locations": [{"external_id": "DC-ATL", "name": "X"}]},
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["summary"]["updated"] == 1


# ─────────────────────────────────────────────────────────────
# 3. POST /v1/ingest/suppliers
# ─────────────────────────────────────────────────────────────


def test_ingest_suppliers_happy_insert():
    db = _passing_dq_db()
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/suppliers",
            json={"suppliers": [{"external_id": "SUP-1", "name": "Acme", "lead_time_days": 7, "reliability_score": 0.95}]},
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["summary"]["inserted"] == 1


def test_ingest_suppliers_invalid_status_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/suppliers",
        json={"suppliers": [{"external_id": "SUP-1", "name": "Acme", "status": "weird"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_suppliers_reliability_out_of_range_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/suppliers",
        json={"suppliers": [{"external_id": "SUP-1", "name": "Acme", "reliability_score": 1.5}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_suppliers_lead_time_zero_pydantic_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/suppliers",
        json={"suppliers": [{"external_id": "SUP-1", "name": "Acme", "lead_time_days": 0}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_suppliers_dry_run():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/suppliers",
        json={"dry_run": True, "suppliers": [{"external_id": "SUP-1", "name": "Acme"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "dry_run"


def test_ingest_suppliers_update_existing():
    existing_id = uuid4()

    def handler(sql, params):
        if "SELECT external_id, supplier_id FROM suppliers" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SUP-1", "supplier_id": existing_id}])
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/suppliers",
            json={"suppliers": [{"external_id": "SUP-1", "name": "Acme"}]},
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["summary"]["updated"] == 1


# ─────────────────────────────────────────────────────────────
# 4. POST /v1/ingest/supplier-items
# ─────────────────────────────────────────────────────────────


def test_ingest_supplier_items_happy_insert():
    sup_id = uuid4()
    item_id = uuid4()

    def handler(sql, params):
        if "FROM suppliers" in sql and "external_id" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SUP-1", "supplier_id": sup_id}])
        if "FROM items" in sql and "external_id" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "SELECT supplier_item_id FROM supplier_items" in sql:
            return FakeCursor(fetchone_value=None)
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/supplier-items",
            json={
                "supplier_items": [
                    {"supplier_external_id": "SUP-1", "item_external_id": "SKU-1", "lead_time_days": 5}
                ]
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["summary"]["inserted"] == 1


def test_ingest_supplier_items_update_existing():
    sup_id = uuid4()
    item_id = uuid4()
    si_id = uuid4()

    def handler(sql, params):
        if "FROM suppliers" in sql and "external_id" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SUP-1", "supplier_id": sup_id}])
        if "FROM items" in sql and "external_id" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "SELECT supplier_item_id FROM supplier_items" in sql:
            return FakeCursor(fetchone_value={"supplier_item_id": si_id})
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/supplier-items",
            json={
                "supplier_items": [
                    {"supplier_external_id": "SUP-1", "item_external_id": "SKU-1", "lead_time_days": 5}
                ]
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["summary"]["updated"] == 1


def test_ingest_supplier_items_fk_errors_422():
    """Both supplier and item missing in DB → 422."""
    db = FakeDB(handler=lambda sql, params: FakeCursor(fetchall_value=[]))
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/supplier-items",
        json={
            "supplier_items": [
                {"supplier_external_id": "SUP-MISS", "item_external_id": "SKU-MISS", "lead_time_days": 5}
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert any("supplier_external_id" in e["errors"][0] for e in detail)


def test_ingest_supplier_items_pydantic_422_zero_lead_time():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/supplier-items",
        json={"supplier_items": [{"supplier_external_id": "S", "item_external_id": "I", "lead_time_days": 0}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_supplier_items_dry_run():
    sup_id = uuid4()
    item_id = uuid4()

    def handler(sql, params):
        if "FROM suppliers" in sql and "external_id" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SUP-1", "supplier_id": sup_id}])
        if "FROM items" in sql and "external_id" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/supplier-items",
        json={
            "dry_run": True,
            "supplier_items": [
                {"supplier_external_id": "SUP-1", "item_external_id": "SKU-1", "lead_time_days": 5}
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "dry_run"


# ─────────────────────────────────────────────────────────────
# 5. POST /v1/ingest/on-hand
# ─────────────────────────────────────────────────────────────


def test_ingest_on_hand_happy_insert():
    item_id = uuid4()
    loc_id = uuid4()
    series_id = uuid4()

    def handler(sql, params):
        if "FROM items" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "FROM locations" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-1", "location_id": loc_id}])
        if "SELECT series_id FROM projection_series" in sql:
            return FakeCursor(fetchone_value={"series_id": series_id})
        if "SELECT node_id FROM nodes" in sql and "OnHandSupply" in sql:
            return FakeCursor(fetchone_value=None)
        if "SELECT node_id FROM nodes" in sql and "ProjectedInventory" in sql:
            return FakeCursor(fetchone_value=None)
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/on-hand",
            json={
                "on_hand": [
                    {
                        "item_external_id": "SKU-1",
                        "location_external_id": "DC-1",
                        "quantity": 100,
                        "as_of_date": "2026-04-01",
                    }
                ]
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200, resp.text
    assert resp.json()["summary"]["inserted"] == 1


def test_ingest_on_hand_update_existing():
    item_id = uuid4()
    loc_id = uuid4()
    series_id = uuid4()
    existing_node_id = uuid4()

    def handler(sql, params):
        if "FROM items" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "FROM locations" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-1", "location_id": loc_id}])
        if "SELECT series_id FROM projection_series" in sql:
            return FakeCursor(fetchone_value={"series_id": series_id})
        if "SELECT node_id FROM nodes" in sql and "OnHandSupply" in sql:
            return FakeCursor(fetchone_value={"node_id": existing_node_id})
        if "SELECT node_id FROM nodes" in sql and "ProjectedInventory" in sql:
            return FakeCursor(fetchone_value=None)
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/on-hand",
            json={
                "on_hand": [
                    {
                        "item_external_id": "SKU-1",
                        "location_external_id": "DC-1",
                        "quantity": 50,
                        "as_of_date": "2026-04-01",
                    }
                ]
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["summary"]["updated"] == 1


def test_ingest_on_hand_fk_error_422():
    db = FakeDB(handler=lambda sql, params: FakeCursor(fetchall_value=[]))
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/on-hand",
        json={
            "on_hand": [
                {
                    "item_external_id": "MISS",
                    "location_external_id": "MISS",
                    "quantity": 1,
                    "as_of_date": "2026-04-01",
                }
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_on_hand_negative_qty_pydantic_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/on-hand",
        json={
            "on_hand": [
                {
                    "item_external_id": "X",
                    "location_external_id": "Y",
                    "quantity": -1,
                    "as_of_date": "2026-04-01",
                }
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_on_hand_dry_run():
    item_id = uuid4()
    loc_id = uuid4()

    def handler(sql, params):
        if "FROM items" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "FROM locations" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-1", "location_id": loc_id}])
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/on-hand",
        json={
            "dry_run": True,
            "on_hand": [
                {
                    "item_external_id": "SKU-1",
                    "location_external_id": "DC-1",
                    "quantity": 10,
                    "as_of_date": "2026-04-01",
                }
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "dry_run"


# ─────────────────────────────────────────────────────────────
# 6. POST /v1/ingest/purchase-orders
# ─────────────────────────────────────────────────────────────


def test_ingest_purchase_orders_happy_insert():
    item_id = uuid4()
    loc_id = uuid4()
    sup_id = uuid4()
    series_id = uuid4()

    def handler(sql, params):
        if "FROM items" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "FROM locations" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-1", "location_id": loc_id}])
        if "FROM suppliers" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SUP-1", "supplier_id": sup_id}])
        if "FROM external_references" in sql:
            return FakeCursor(fetchall_value=[])
        if "SELECT series_id FROM projection_series" in sql:
            return FakeCursor(fetchone_value={"series_id": series_id})
        if "SELECT node_id FROM nodes" in sql and "ProjectedInventory" in sql:
            return FakeCursor(fetchone_value=None)
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/purchase-orders",
            json={
                "purchase_orders": [
                    {
                        "external_id": "PO-1",
                        "item_external_id": "SKU-1",
                        "location_external_id": "DC-1",
                        "supplier_external_id": "SUP-1",
                        "quantity": 100,
                        "expected_delivery_date": "2026-05-01",
                        "status": "confirmed",
                    }
                ]
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200, resp.text
    assert resp.json()["summary"]["inserted"] == 1


def test_ingest_purchase_orders_update_existing():
    item_id = uuid4()
    loc_id = uuid4()
    sup_id = uuid4()
    existing_node_id = uuid4()

    def handler(sql, params):
        if "FROM items" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "FROM locations" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-1", "location_id": loc_id}])
        if "FROM suppliers" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SUP-1", "supplier_id": sup_id}])
        if "FROM external_references" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "PO-1", "internal_id": existing_node_id}])
        if "SELECT series_id FROM projection_series" in sql:
            return FakeCursor(fetchone_value={"series_id": uuid4()})
        if "SELECT node_id FROM nodes" in sql and "ProjectedInventory" in sql:
            return FakeCursor(fetchone_value=None)
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/purchase-orders",
            json={
                "purchase_orders": [
                    {
                        "external_id": "PO-1",
                        "item_external_id": "SKU-1",
                        "location_external_id": "DC-1",
                        "supplier_external_id": "SUP-1",
                        "quantity": 100,
                        "expected_delivery_date": "2026-05-01",
                        "status": "cancelled",
                    }
                ]
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["summary"]["updated"] == 1


def test_ingest_purchase_orders_fk_errors_422():
    db = FakeDB(handler=lambda sql, params: FakeCursor(fetchall_value=[]))
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/purchase-orders",
        json={
            "purchase_orders": [
                {
                    "external_id": "PO-1",
                    "item_external_id": "MISS",
                    "location_external_id": "MISS",
                    "supplier_external_id": "MISS",
                    "quantity": 5,
                    "expected_delivery_date": "2026-05-01",
                }
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_purchase_orders_zero_qty_pydantic_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/purchase-orders",
        json={
            "purchase_orders": [
                {
                    "external_id": "PO-1",
                    "item_external_id": "X",
                    "location_external_id": "Y",
                    "supplier_external_id": "Z",
                    "quantity": 0,
                    "expected_delivery_date": "2026-05-01",
                }
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_purchase_orders_dry_run():
    item_id = uuid4()
    loc_id = uuid4()
    sup_id = uuid4()

    def handler(sql, params):
        if "FROM items" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "FROM locations" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-1", "location_id": loc_id}])
        if "FROM suppliers" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SUP-1", "supplier_id": sup_id}])
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/purchase-orders",
        json={
            "dry_run": True,
            "purchase_orders": [
                {
                    "external_id": "PO-1",
                    "item_external_id": "SKU-1",
                    "location_external_id": "DC-1",
                    "supplier_external_id": "SUP-1",
                    "quantity": 1,
                    "expected_delivery_date": "2026-05-01",
                }
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "dry_run"


# ─────────────────────────────────────────────────────────────
# 7. POST /v1/ingest/forecast-demand
# ─────────────────────────────────────────────────────────────


def test_ingest_forecast_demand_happy_insert():
    item_id = uuid4()
    loc_id = uuid4()
    series_id = uuid4()

    def handler(sql, params):
        if "FROM items" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "FROM locations" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-1", "location_id": loc_id}])
        if "SELECT series_id FROM projection_series" in sql:
            return FakeCursor(fetchone_value={"series_id": series_id})
        if "SELECT node_id FROM nodes" in sql and "ForecastDemand" in sql:
            return FakeCursor(fetchone_value=None)
        if "SELECT node_id FROM nodes" in sql and "ProjectedInventory" in sql:
            return FakeCursor(fetchone_value=None)
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/forecast-demand",
            json={
                "forecasts": [
                    {
                        "item_external_id": "SKU-1",
                        "location_external_id": "DC-1",
                        "quantity": 50,
                        "bucket_date": "2026-04-15",
                        "time_grain": "week",
                        "source": "statistical",
                    }
                ]
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200, resp.text
    assert resp.json()["summary"]["inserted"] == 1


def test_ingest_forecast_demand_update_existing():
    item_id = uuid4()
    loc_id = uuid4()
    existing_node_id = uuid4()

    def handler(sql, params):
        if "FROM items" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "FROM locations" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-1", "location_id": loc_id}])
        if "SELECT series_id FROM projection_series" in sql:
            return FakeCursor(fetchone_value={"series_id": uuid4()})
        if "SELECT node_id FROM nodes" in sql and "ForecastDemand" in sql:
            return FakeCursor(fetchone_value={"node_id": existing_node_id})
        if "SELECT node_id FROM nodes" in sql and "ProjectedInventory" in sql:
            return FakeCursor(fetchone_value=None)
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    with patch.object(ingest_module, "run_dq", return_value=FakeDQResult()):
        resp = client.post(
            "/v1/ingest/forecast-demand",
            json={
                "forecasts": [
                    {
                        "item_external_id": "SKU-1",
                        "location_external_id": "DC-1",
                        "quantity": 75,
                        "bucket_date": "2026-04-15",
                        "time_grain": "week",
                    }
                ]
            },
            headers=AUTH_HEADERS,
        )
    assert resp.status_code == 200
    assert resp.json()["summary"]["updated"] == 1


def test_ingest_forecast_demand_invalid_time_grain_422():
    """time_grain not in VALID_TIME_GRAINS but accepted by pydantic field — caught by manual validation."""
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/forecast-demand",
        json={
            "forecasts": [
                {
                    "item_external_id": "X",
                    "location_external_id": "Y",
                    "quantity": 10,
                    "bucket_date": "2026-04-15",
                    "time_grain": "year",
                }
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_forecast_demand_invalid_source_pydantic_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/forecast-demand",
        json={
            "forecasts": [
                {
                    "item_external_id": "X",
                    "location_external_id": "Y",
                    "quantity": 10,
                    "bucket_date": "2026-04-15",
                    "source": "wizardry",
                }
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_forecast_demand_fk_errors_422():
    db = FakeDB(handler=lambda sql, params: FakeCursor(fetchall_value=[]))
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/forecast-demand",
        json={
            "forecasts": [
                {
                    "item_external_id": "MISS",
                    "location_external_id": "MISS",
                    "quantity": 10,
                    "bucket_date": "2026-04-15",
                }
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_forecast_demand_dry_run():
    item_id = uuid4()
    loc_id = uuid4()

    def handler(sql, params):
        if "FROM items" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "SKU-1", "item_id": item_id}])
        if "FROM locations" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-1", "location_id": loc_id}])
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/forecast-demand",
        json={
            "dry_run": True,
            "forecasts": [
                {
                    "item_external_id": "SKU-1",
                    "location_external_id": "DC-1",
                    "quantity": 10,
                    "bucket_date": "2026-04-15",
                }
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "dry_run"


# ─────────────────────────────────────────────────────────────
# 8. POST /v1/ingest/resources
# ─────────────────────────────────────────────────────────────


def test_ingest_resources_happy_insert_no_location():
    db = _passing_dq_db()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/resources",
        json={
            "resources": [
                {"external_id": "MACH-1", "name": "CNC", "resource_type": "machine", "capacity_per_day": 8}
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["summary"]["inserted"] == 1


def test_ingest_resources_happy_insert_with_location():
    loc_id = uuid4()

    def handler(sql, params):
        if "FROM locations" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "DC-1", "location_id": loc_id}])
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/resources",
        json={
            "resources": [
                {
                    "external_id": "MACH-1",
                    "name": "CNC",
                    "resource_type": "machine",
                    "location_external_id": "DC-1",
                    "capacity_per_day": 8,
                }
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["summary"]["inserted"] == 1


def test_ingest_resources_update_existing():
    res_id = uuid4()

    def handler(sql, params):
        if "FROM resources" in sql and "external_id" in sql and "ANY" in sql:
            return FakeCursor(fetchall_value=[{"external_id": "MACH-1", "resource_id": res_id}])
        return FakeCursor()

    db = FakeDB(handler=handler)
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/resources",
        json={
            "resources": [
                {"external_id": "MACH-1", "name": "CNC v2", "resource_type": "machine"}
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["summary"]["updated"] == 1


def test_ingest_resources_invalid_type_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/resources",
        json={"resources": [{"external_id": "X", "name": "Y", "resource_type": "spaceship"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_resources_missing_location_fk_422():
    db = FakeDB(handler=lambda sql, params: FakeCursor(fetchall_value=[]))
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/resources",
        json={
            "resources": [
                {
                    "external_id": "MACH-1",
                    "name": "CNC",
                    "resource_type": "machine",
                    "location_external_id": "MISS",
                }
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_resources_zero_capacity_pydantic_422():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/resources",
        json={
            "resources": [
                {"external_id": "X", "name": "Y", "resource_type": "machine", "capacity_per_day": 0}
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_ingest_resources_dry_run():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/resources",
        json={
            "dry_run": True,
            "resources": [
                {"external_id": "MACH-1", "name": "CNC", "resource_type": "machine"}
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "dry_run"


# ─────────────────────────────────────────────────────────────
# Pydantic non-empty string validators
# ─────────────────────────────────────────────────────────────


def test_item_row_empty_name_rejected():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/items",
        json={"items": [{"external_id": "X", "name": "   "}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_location_row_empty_external_id_rejected():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/locations",
        json={"locations": [{"external_id": "", "name": "X"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_supplier_row_empty_name_rejected():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/suppliers",
        json={"suppliers": [{"external_id": "S1", "name": " "}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_purchase_order_row_empty_external_id_rejected():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/purchase-orders",
        json={
            "purchase_orders": [
                {
                    "external_id": "  ",
                    "item_external_id": "X",
                    "location_external_id": "Y",
                    "supplier_external_id": "Z",
                    "quantity": 1,
                    "expected_delivery_date": "2026-05-01",
                }
            ]
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422


def test_resource_row_empty_external_id_rejected():
    db = FakeDB()
    client = make_client(db)
    resp = client.post(
        "/v1/ingest/resources",
        json={"resources": [{"external_id": "  ", "name": "X", "resource_type": "machine"}]},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
