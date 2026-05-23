"""Integration tests for the DB-touching L4 rules.

These need a real Postgres because the rules query ingest_rows /
ingest_batches / suppliers / items / item_planning_params.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.engine.dq.l4_rules import check_l4

from .conftest import requires_db


@pytest.fixture
def conn(migrated_db: str):
    """Per-test connection, rolled back at teardown."""
    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        yield c
        c.rollback()


def _codes(issues):
    return [i.rule_code for i in issues]


def _new_batch(conn, entity_type: str, source_system: str, status: str = "pending") -> UUID:
    """Insert a minimal ingest_batches row and return its id."""
    batch_id = uuid4()
    conn.execute(
        """
        INSERT INTO ingest_batches (batch_id, entity_type, source_system, status, total_rows)
        VALUES (%s, %s, %s, %s, 0)
        """,
        (batch_id, entity_type, source_system, status),
    )
    return batch_id


def _insert_ingest_row(conn, batch_id: UUID, row_number: int, external_id: str) -> None:
    """Insert one ingest_rows row with col_01 = external_id."""
    conn.execute(
        """
        INSERT INTO ingest_rows (batch_id, row_number, raw_content, col_01)
        VALUES (%s, %s, %s, %s)
        """,
        (batch_id, row_number, "{}", external_id),
    )


# ---------------------------------------------------------------------------
# L4_INTER_BATCH_COLLISION
# ---------------------------------------------------------------------------


@requires_db
def test_inter_batch_collision_fires(conn) -> None:
    """An item external_id present in both this batch and another
    still-open batch must produce a warning."""
    other_batch = _new_batch(conn, "items", "OTHER-SOURCE", status="validated")
    _insert_ingest_row(conn, other_batch, 1, "SHARED-001")

    this_batch = _new_batch(conn, "items", "THIS-SOURCE")
    rows = [
        (uuid4(), 1, {"external_id": "SHARED-001"}),
        (uuid4(), 2, {"external_id": "UNIQUE-002"}),
    ]
    issues = check_l4(rows, "items", this_batch, conn)
    collisions = [i for i in issues if i.rule_code == "L4_INTER_BATCH_COLLISION"]
    assert len(collisions) == 1
    assert collisions[0].row_number == 1
    assert collisions[0].severity == "warning"
    assert collisions[0].field_name == "external_id"


@requires_db
def test_inter_batch_collision_ignores_imported_batches(conn) -> None:
    """A previously-imported batch is canonical now — don't flag against it."""
    old = _new_batch(conn, "items", "OLD-SOURCE", status="imported")
    _insert_ingest_row(conn, old, 1, "OLD-001")

    this_batch = _new_batch(conn, "items", "THIS-SOURCE")
    rows = [(uuid4(), 1, {"external_id": "OLD-001"})]
    issues = check_l4(rows, "items", this_batch, conn)
    assert "L4_INTER_BATCH_COLLISION" not in _codes(issues)


@requires_db
def test_inter_batch_collision_ignores_rejected_batches(conn) -> None:
    """A rejected batch isn't going anywhere — no need to warn against it."""
    rej = _new_batch(conn, "items", "REJ-SOURCE", status="rejected")
    _insert_ingest_row(conn, rej, 1, "REJ-001")

    this_batch = _new_batch(conn, "items", "THIS-SOURCE")
    rows = [(uuid4(), 1, {"external_id": "REJ-001"})]
    issues = check_l4(rows, "items", this_batch, conn)
    assert "L4_INTER_BATCH_COLLISION" not in _codes(issues)


# ---------------------------------------------------------------------------
# L4_SUPPLIER_INACTIVE
# ---------------------------------------------------------------------------


@requires_db
def test_supplier_inactive_fires(conn) -> None:
    """A supplier_items row pointing at a blocked supplier is rejected."""
    # Seed a blocked supplier
    conn.execute(
        """
        INSERT INTO suppliers (supplier_id, external_id, name, country, status)
        VALUES (gen_random_uuid(), 'SUP-BLOCKED', 'Bad Co', 'XX', 'blocked')
        """
    )
    conn.execute(
        """
        INSERT INTO suppliers (supplier_id, external_id, name, country, status)
        VALUES (gen_random_uuid(), 'SUP-OK', 'Good Co', 'XX', 'active')
        """
    )

    batch_id = _new_batch(conn, "supplier_items", "TEST")
    rows = [
        (uuid4(), 1, {"supplier_external_id": "SUP-BLOCKED", "item_external_id": "I1"}),
        (uuid4(), 2, {"supplier_external_id": "SUP-OK", "item_external_id": "I2"}),
    ]
    issues = check_l4(rows, "supplier_items", batch_id, conn)
    inact = [i for i in issues if i.rule_code == "L4_SUPPLIER_INACTIVE"]
    assert len(inact) == 1
    assert inact[0].row_number == 1
    assert inact[0].severity == "error"
    assert "blocked" in inact[0].message


@requires_db
def test_supplier_inactive_ignores_unknown_supplier(conn) -> None:
    """An unknown supplier_external_id is L2's job, not L4's.
    L4 only flags suppliers that EXIST but are non-active."""
    batch_id = _new_batch(conn, "supplier_items", "TEST")
    rows = [(uuid4(), 1, {"supplier_external_id": "SUP-DOES-NOT-EXIST"})]
    issues = check_l4(rows, "supplier_items", batch_id, conn)
    assert "L4_SUPPLIER_INACTIVE" not in _codes(issues)


# ---------------------------------------------------------------------------
# L4_ORPHAN_ITEM_NO_PLANNING
# ---------------------------------------------------------------------------


@requires_db
def test_orphan_item_no_planning_fires(conn) -> None:
    """An item already in the DB without item_planning_params should warn."""
    conn.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        VALUES (gen_random_uuid(), 'ITEM-ORPHAN', 'Orphan', 'component', 'EA', 'active')
        """
    )
    batch_id = _new_batch(conn, "items", "TEST")
    rows = [(uuid4(), 1, {"external_id": "ITEM-ORPHAN"})]
    issues = check_l4(rows, "items", batch_id, conn)
    orph = [i for i in issues if i.rule_code == "L4_ORPHAN_ITEM_NO_PLANNING"]
    assert len(orph) == 1
    assert orph[0].severity == "warning"
    assert "ITEM-ORPHAN" in orph[0].message


@requires_db
def test_orphan_item_with_planning_is_clean(conn) -> None:
    """An item with at least one item_planning_params row should NOT
    trigger the orphan warning."""
    # Seed item + location + planning row
    item_id = uuid4()
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        VALUES (%s, 'ITEM-PLANNED', 'Planned', 'component', 'EA', 'active')
        """,
        (item_id,),
    )
    conn.execute(
        """
        INSERT INTO locations (location_id, external_id, name, location_type, country, timezone)
        VALUES (%s, 'LOC-1', 'L1', 'plant', 'DE', 'Europe/Berlin')
        """,
        (loc_id,),
    )
    conn.execute(
        """
        INSERT INTO item_planning_params
            (item_id, location_id, lot_size_rule, planning_horizon_days)
        VALUES (%s, %s, 'LOTFORLOT', 90)
        """,
        (item_id, loc_id),
    )

    batch_id = _new_batch(conn, "items", "TEST")
    rows = [(uuid4(), 1, {"external_id": "ITEM-PLANNED"})]
    issues = check_l4(rows, "items", batch_id, conn)
    assert "L4_ORPHAN_ITEM_NO_PLANNING" not in _codes(issues)
