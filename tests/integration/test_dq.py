"""
tests/integration/test_dq.py — Integration tests for DQ Engine V1.

Tests:
  - L1: missing mandatory field → issue generated
  - L1: invalid type (date, numeric) → issue generated
  - L1: string too long → issue generated
  - L2: unknown reference → issue generated
  - L2: valid reference → no issue
  - batch dq_status: passed when no errors
  - batch dq_status: rejected when errors present
  - GET /v1/dq/{batch_id} returns issues
  - POST /v1/dq/run/{batch_id} works
  - GET /v1/dq/issues filters by severity
  - GET /v1/dq/issues filters by entity_type
  - run_dq on empty batch → validated, 0 issues

Requires a running PostgreSQL instance with migrations applied.
Set DATABASE_URL before running.
"""
from __future__ import annotations

import json
import os
from uuid import uuid4

import pytest
import psycopg
from psycopg.rows import dict_row

from .conftest import requires_db, DB_AVAILABLE, TEST_DB_URL

# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def dq_client(migrated_db):
    """Module-scoped TestClient with migrated DB."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "test-token"

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from fastapi.testclient import TestClient

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def auth():
    return {"Authorization": "Bearer test-token"}


@pytest.fixture(scope="module")
def db_conn(migrated_db):
    """Direct psycopg connection for DB assertions."""
    conn = psycopg.connect(migrated_db, row_factory=dict_row)
    yield conn
    conn.close()


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

PREFIX = str(uuid4())[:8]


def uid(base: str) -> str:
    return f"{PREFIX}-{base}"


def _create_batch_with_rows(
    db: psycopg.Connection,
    entity_type: str,
    rows: list[dict],
) -> str:
    """Insert an ingest_batch + ingest_rows directly and return batch_id."""
    batch_id = str(uuid4())
    db.execute(
        """
        INSERT INTO ingest_batches
            (batch_id, entity_type, source_system, status, total_rows, submitted_by)
        VALUES (%s, %s, 'test', 'processing', %s, 'pytest')
        """,
        (batch_id, entity_type, len(rows)),
    )
    for i, row in enumerate(rows):
        db.execute(
            """
            INSERT INTO ingest_rows (row_id, batch_id, row_number, raw_content)
            VALUES (%s, %s, %s, %s)
            """,
            (str(uuid4()), batch_id, i + 1, json.dumps(row)),
        )
    db.commit()
    return batch_id


def _insert_item(db: psycopg.Connection, external_id: str) -> str:
    """Insert a test item, return item_id."""
    item_id = str(uuid4())
    db.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        VALUES (%s, %s, 'Test Item', 'component', 'EA', 'active')
        ON CONFLICT (external_id) DO NOTHING
        """,
        (item_id, external_id),
    )
    db.commit()
    return item_id


def _insert_location(db: psycopg.Connection, external_id: str) -> str:
    """Insert a test location, return location_id."""
    loc_id = str(uuid4())
    db.execute(
        """
        INSERT INTO locations (location_id, external_id, name, location_type)
        VALUES (%s, %s, 'Test Location', 'warehouse')
        ON CONFLICT (external_id) DO NOTHING
        """,
        (loc_id, external_id),
    )
    db.commit()
    return loc_id


def _insert_supplier(db: psycopg.Connection, external_id: str) -> str:
    """Insert a test supplier, return supplier_id."""
    sup_id = str(uuid4())
    db.execute(
        """
        INSERT INTO suppliers (supplier_id, external_id, name, status)
        VALUES (%s, %s, 'Test Supplier', 'active')
        ON CONFLICT (external_id) DO NOTHING
        """,
        (sup_id, external_id),
    )
    db.commit()
    return sup_id


# ─────────────────────────────────────────────────────────────
# Test 1: L1 — missing mandatory field generates issue
# ─────────────────────────────────────────────────────────────

@requires_db
def test_l1_missing_field_generates_issue(db_conn, migrated_db):
    """Missing 'name' field in items batch → L1_MISSING_FIELD issue."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    batch_id = _create_batch_with_rows(
        db_conn,
        "items",
        [{"external_id": uid("item-missing-name"), "item_type": "component", "uom": "EA", "status": "active"}],
        # 'name' is missing
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq(conn, batch_id)
        conn.commit()

    assert result.total_rows == 1
    assert result.failed_rows == 1
    assert any(i.rule_code == "L1_MISSING_FIELD" and i.field_name == "name" for i in result.issues)


# ─────────────────────────────────────────────────────────────
# Test 2: L1 — invalid date type generates issue
# ─────────────────────────────────────────────────────────────

@requires_db
def test_l1_invalid_date_generates_issue(db_conn, migrated_db):
    """Invalid date format in purchase_orders → L1_INVALID_TYPE issue."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    batch_id = _create_batch_with_rows(
        db_conn,
        "purchase_orders",
        [{
            "external_id": uid("po-bad-date"),
            "item_external_id": uid("item-x"),
            "location_external_id": uid("loc-x"),
            "supplier_external_id": uid("sup-x"),
            "quantity": 10,
            "uom": "EA",
            "expected_delivery_date": "not-a-date",  # bad format
            "status": "confirmed",
        }],
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq(conn, batch_id)
        conn.commit()

    assert any(i.rule_code == "L1_INVALID_TYPE" and i.field_name == "expected_delivery_date" for i in result.issues)


# ─────────────────────────────────────────────────────────────
# Test 3: L1 — non-positive numeric generates issue
# ─────────────────────────────────────────────────────────────

@requires_db
def test_l1_invalid_numeric_generates_issue(db_conn, migrated_db):
    """Negative quantity in on_hand → L1_INVALID_TYPE issue."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    batch_id = _create_batch_with_rows(
        db_conn,
        "on_hand",
        [{
            "item_external_id": uid("item-y"),
            "location_external_id": uid("loc-y"),
            "quantity": -5,  # invalid
            "uom": "EA",
            "as_of_date": "2026-04-01",
        }],
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq(conn, batch_id)
        conn.commit()

    assert any(i.rule_code == "L1_INVALID_TYPE" and i.field_name == "quantity" for i in result.issues)


# ─────────────────────────────────────────────────────────────
# Test 4: L1 — string too long generates issue
# ─────────────────────────────────────────────────────────────

@requires_db
def test_l1_string_too_long_generates_issue(db_conn, migrated_db):
    """external_id > 255 chars in items → L1_INVALID_FORMAT issue."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    long_id = "X" * 300

    batch_id = _create_batch_with_rows(
        db_conn,
        "items",
        [{
            "external_id": long_id,
            "name": "Test",
            "item_type": "component",
            "uom": "EA",
            "status": "active",
        }],
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq(conn, batch_id)
        conn.commit()

    assert any(i.rule_code == "L1_INVALID_FORMAT" and i.field_name == "external_id" for i in result.issues)


# ─────────────────────────────────────────────────────────────
# Test 5: L2 — unknown reference generates issue
# ─────────────────────────────────────────────────────────────

@requires_db
def test_l2_unknown_ref_generates_issue(db_conn, migrated_db):
    """Valid L1 but non-existent item_external_id → L2_UNKNOWN_REF issue."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    nonexistent_item = uid("ghost-item")

    batch_id = _create_batch_with_rows(
        db_conn,
        "on_hand",
        [{
            "item_external_id": nonexistent_item,
            "location_external_id": uid("loc-also-ghost"),
            "quantity": 100,
            "uom": "EA",
            "as_of_date": "2026-04-01",
        }],
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq(conn, batch_id)
        conn.commit()

    l2_issues = [i for i in result.issues if i.rule_code == "L2_UNKNOWN_REF"]
    assert len(l2_issues) >= 1
    assert any(i.field_name == "item_external_id" for i in l2_issues)


# ─────────────────────────────────────────────────────────────
# Test 6: L2 — valid reference → no L2 issue
# ─────────────────────────────────────────────────────────────

@requires_db
def test_l2_valid_ref_no_issue(db_conn, migrated_db):
    """Valid item_external_id and location_external_id → no L2 issues."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    item_ext = uid("item-valid-ref")
    loc_ext = uid("loc-valid-ref")
    _insert_item(db_conn, item_ext)
    _insert_location(db_conn, loc_ext)

    batch_id = _create_batch_with_rows(
        db_conn,
        "on_hand",
        [{
            "item_external_id": item_ext,
            "location_external_id": loc_ext,
            "quantity": 50,
            "uom": "EA",
            "as_of_date": "2026-04-01",
        }],
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq(conn, batch_id)
        conn.commit()

    l2_issues = [i for i in result.issues if i.rule_code == "L2_UNKNOWN_REF"]
    assert len(l2_issues) == 0
    assert result.failed_rows == 0


# ─────────────────────────────────────────────────────────────
# Test 7: batch dq_status = validated when no errors
# ─────────────────────────────────────────────────────────────

@requires_db
def test_batch_status_validated_when_no_errors(db_conn, migrated_db):
    """Clean batch → dq_status = validated in ingest_batches."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    item_ext = uid("item-clean")
    loc_ext = uid("loc-clean")
    _insert_item(db_conn, item_ext)
    _insert_location(db_conn, loc_ext)

    batch_id = _create_batch_with_rows(
        db_conn,
        "on_hand",
        [{
            "item_external_id": item_ext,
            "location_external_id": loc_ext,
            "quantity": 10,
            "uom": "EA",
            "as_of_date": "2026-04-08",
        }],
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq(conn, batch_id)
        conn.commit()

    assert result.batch_dq_status == "validated"

    batch = db_conn.execute(
        "SELECT dq_status FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert batch["dq_status"] == "validated"


# ─────────────────────────────────────────────────────────────
# Test 8: batch dq_status = rejected when errors present
# ─────────────────────────────────────────────────────────────

@requires_db
def test_batch_status_rejected_when_errors(db_conn, migrated_db):
    """Batch with L1 errors → dq_status = rejected in ingest_batches."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    batch_id = _create_batch_with_rows(
        db_conn,
        "items",
        [{"external_id": uid("err-item"), "item_type": "component", "uom": "EA", "status": "active"}],
        # missing 'name'
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq(conn, batch_id)
        conn.commit()

    assert result.batch_dq_status == "rejected"

    batch = db_conn.execute(
        "SELECT dq_status FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert batch["dq_status"] == "rejected"


# ─────────────────────────────────────────────────────────────
# Test 9: GET /v1/dq/{batch_id} returns issues
# ─────────────────────────────────────────────────────────────

@requires_db
def test_get_batch_dq_returns_issues(dq_client, auth, db_conn, migrated_db):
    """GET /v1/dq/{batch_id} returns the issues for a batch that has errors."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    batch_id = _create_batch_with_rows(
        db_conn,
        "items",
        [{"external_id": uid("get-test-item"), "item_type": "component", "uom": "EA", "status": "active"}],
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        run_dq(conn, batch_id)
        conn.commit()

    resp = dq_client.get(f"/v1/dq/{batch_id}", headers=auth)
    assert resp.status_code == 200
    data = resp.json()
    assert data["batch_id"] == batch_id
    assert isinstance(data["issues"], list)
    assert len(data["issues"]) > 0
    assert data["issues"][0]["rule_code"] == "L1_MISSING_FIELD"


# ─────────────────────────────────────────────────────────────
# Test 10: POST /v1/dq/run/{batch_id} works
# ─────────────────────────────────────────────────────────────

@requires_db
def test_post_run_dq_endpoint(dq_client, auth, db_conn):
    """POST /v1/dq/run/{batch_id} triggers DQ and returns result."""
    item_ext = uid("item-run-test")
    loc_ext = uid("loc-run-test")
    _insert_item(db_conn, item_ext)
    _insert_location(db_conn, loc_ext)

    batch_id = _create_batch_with_rows(
        db_conn,
        "on_hand",
        [{
            "item_external_id": item_ext,
            "location_external_id": loc_ext,
            "quantity": 99,
            "uom": "EA",
            "as_of_date": "2026-04-08",
        }],
    )

    resp = dq_client.post(f"/v1/dq/run/{batch_id}", headers=auth)
    assert resp.status_code == 200
    data = resp.json()
    assert data["batch_id"] == batch_id
    assert data["status"] == "completed"
    assert data["total_rows"] == 1
    assert data["batch_dq_status"] == "validated"


# ─────────────────────────────────────────────────────────────
# Test 11: GET /v1/dq/issues filters by severity
# ─────────────────────────────────────────────────────────────

@requires_db
def test_get_issues_filter_by_severity(dq_client, auth, db_conn, migrated_db):
    """GET /v1/dq/issues?severity=error returns only error-level issues."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    # Create a batch with errors
    batch_id = _create_batch_with_rows(
        db_conn,
        "items",
        [{"external_id": uid("severity-filter-item"), "item_type": "component", "uom": "EA", "status": "active"}],
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        run_dq(conn, batch_id)
        conn.commit()

    resp = dq_client.get("/v1/dq/issues?severity=error", headers=auth)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] >= 1
    # All returned issues must be errors
    for issue in data["issues"]:
        assert issue["severity"] == "error"


# ─────────────────────────────────────────────────────────────
# Test 12: GET /v1/dq/issues filters by entity_type
# ─────────────────────────────────────────────────────────────

@requires_db
def test_get_issues_filter_by_entity_type(dq_client, auth, db_conn, migrated_db):
    """GET /v1/dq/issues?entity_type=on_hand returns only on_hand issues."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    # Create an on_hand batch with a missing field
    batch_id = _create_batch_with_rows(
        db_conn,
        "on_hand",
        [{
            "item_external_id": uid("oh-filter-item"),
            "location_external_id": uid("oh-filter-loc"),
            "quantity": -1,  # invalid → error
            "uom": "EA",
            "as_of_date": "2026-04-08",
        }],
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        run_dq(conn, batch_id)
        conn.commit()

    resp = dq_client.get("/v1/dq/issues?entity_type=on_hand", headers=auth)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] >= 1


# ─────────────────────────────────────────────────────────────
# Test 13: run_dq on empty batch → validated, 0 issues
# ─────────────────────────────────────────────────────────────

@requires_db
def test_run_dq_empty_batch(db_conn, migrated_db):
    """Batch with 0 rows → validated status, no issues generated."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    batch_id = str(uuid4())
    db_conn.execute(
        """
        INSERT INTO ingest_batches
            (batch_id, entity_type, source_system, status, total_rows, submitted_by)
        VALUES (%s, 'items', 'test', 'processing', 0, 'pytest')
        """,
        (batch_id,),
    )
    db_conn.commit()

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq(conn, batch_id)
        conn.commit()

    assert result.total_rows == 0
    assert result.batch_dq_status == "validated"
    assert len(result.issues) == 0


# ─────────────────────────────────────────────────────────────
# Test 14: L2 purchase_orders — all 3 refs checked
# ─────────────────────────────────────────────────────────────

@requires_db
def test_l2_purchase_order_all_refs_checked(db_conn, migrated_db):
    """PO with valid L1 but all 3 refs missing → 3 L2_UNKNOWN_REF issues."""
    from ootils_core.engine.dq.engine import run_dq
    import psycopg as _psycopg

    batch_id = _create_batch_with_rows(
        db_conn,
        "purchase_orders",
        [{
            "external_id": uid("po-l2-test"),
            "item_external_id": uid("ghost-item-po"),
            "location_external_id": uid("ghost-loc-po"),
            "supplier_external_id": uid("ghost-sup-po"),
            "quantity": 100,
            "uom": "EA",
            "expected_delivery_date": "2026-05-01",
            "status": "confirmed",
        }],
    )

    with _psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        result = run_dq(conn, batch_id)
        conn.commit()

    l2_issues = [i for i in result.issues if i.rule_code == "L2_UNKNOWN_REF"]
    # Should have 3 L2 issues: item, location, supplier
    assert len(l2_issues) == 3
    fields = {i.field_name for i in l2_issues}
    assert "item_external_id" in fields
    assert "location_external_id" in fields
    assert "supplier_external_id" in fields
