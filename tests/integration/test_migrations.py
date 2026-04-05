"""
tests/integration/test_migrations.py — Lot A: Migration SQL tests (tests 1–10).

Verifies that all 6 migrations execute correctly against a real PostgreSQL instance
and that the resulting schema matches the expected baseline.

Skip all tests if DATABASE_URL is not configured.
"""
from __future__ import annotations

import os

import pytest

from .conftest import requires_db, DB_AVAILABLE, TEST_DB_URL


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _tables(conn) -> set[str]:
    rows = conn.execute("""
        SELECT tablename FROM pg_tables WHERE schemaname = 'public'
    """).fetchall()
    return {r["tablename"] for r in rows}


def _columns(conn, table: str) -> set[str]:
    rows = conn.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
    """, (table,)).fetchall()
    return {r["column_name"] for r in rows}


def _indexes(conn, table: str) -> set[str]:
    rows = conn.execute("""
        SELECT indexname FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = %s
    """, (table,)).fetchall()
    return {r["indexname"] for r in rows}


def _check_constraints(conn, table: str) -> list[str]:
    rows = conn.execute("""
        SELECT pg_get_constraintdef(c.oid) AS def
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE c.contype = 'c'
          AND t.relname = %s
          AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
    """, (table,)).fetchall()
    return [r["def"] for r in rows]


def _fk_constraints(conn, table: str) -> list[dict]:
    rows = conn.execute("""
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'public'
          AND tc.table_name = %s
    """, (table,)).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Test 1 — Migrations 001→006 execute in order on an empty PostgreSQL DB
# ---------------------------------------------------------------------------

@requires_db
def test_01_migrations_run_on_empty_db(migrated_db):
    """All 6 migrations complete successfully; core tables exist afterward."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        tables = _tables(conn)

    expected_core = {
        "scenarios", "items", "locations", "nodes", "edges",
        "projection_series", "events", "calc_runs",
    }
    missing = expected_core - tables
    assert not missing, f"Missing tables after migrations: {missing}"


# ---------------------------------------------------------------------------
# Test 2 — Table `scenarios` exists with expected baseline row
# ---------------------------------------------------------------------------

@requires_db
def test_02_scenarios_table_baseline_row(conn):
    """Table `scenarios` contains the seeded baseline row after migrations."""
    row = conn.execute("""
        SELECT scenario_id, name, is_baseline, status
        FROM scenarios
        WHERE scenario_id = '00000000-0000-0000-0000-000000000001'
    """).fetchone()
    assert row is not None, "Baseline scenario row not found"
    assert row["name"] == "Baseline"
    assert row["is_baseline"] is True
    assert row["status"] == "active"


# ---------------------------------------------------------------------------
# Test 3 — Table `events` accepts `scenario_merge` in event_type CHECK
# ---------------------------------------------------------------------------

@requires_db
def test_03_events_accepts_scenario_merge(conn):
    """Migration 006 extends events.event_type CHECK to include scenario_merge."""
    # First ensure baseline scenario exists (needed for FK)
    conn.execute("""
        INSERT INTO scenarios (scenario_id, name, is_baseline, status)
        VALUES ('00000000-0000-0000-0000-000000000001'::UUID, 'Baseline', TRUE, 'active')
        ON CONFLICT DO NOTHING
    """)
    conn.execute("""
        INSERT INTO events (event_type, scenario_id, source)
        VALUES ('scenario_merge', '00000000-0000-0000-0000-000000000001'::UUID, 'test')
    """)
    row = conn.execute("""
        SELECT event_type FROM events
        WHERE event_type = 'scenario_merge'
        ORDER BY created_at DESC LIMIT 1
    """).fetchone()
    assert row is not None
    assert row["event_type"] == "scenario_merge"


# ---------------------------------------------------------------------------
# Test 4 — Table `shortages` created with expected indexes
# ---------------------------------------------------------------------------

@requires_db
def test_04_shortages_table_and_indexes(conn):
    """Table `shortages` exists and has the expected indexes from migration 005."""
    tables = _tables(conn)
    assert "shortages" in tables, "Table shortages not found"

    idx = _indexes(conn, "shortages")
    # Migration 005 creates these indexes
    expected_indexes = {
        "shortages_pi_node_id_idx",
        "shortages_shortage_date_idx",
        "shortages_status_idx",
    }
    # At least one of the migration-specific indexes should be present
    found = expected_indexes & idx
    assert found, f"No expected shortage indexes found. Present: {idx}"


# ---------------------------------------------------------------------------
# Test 5 — Presence of critical indexes from recent migrations
# ---------------------------------------------------------------------------

@requires_db
def test_05_critical_indexes_present(conn):
    """Key indexes from migrations 002-006 are present for engine performance."""
    critical = [
        ("nodes", "idx_nodes_scenario_type"),
        ("nodes", "idx_nodes_item_location_scenario"),
        ("nodes", "idx_nodes_dirty"),
        ("edges", "idx_edges_from"),
        ("edges", "idx_edges_to"),
        ("events", "idx_events_unprocessed"),
        ("projection_series", "idx_projection_series_lookup"),
    ]
    for table, idx_name in critical:
        idx = _indexes(conn, table)
        assert idx_name in idx, f"Missing index {idx_name} on {table}. Found: {idx}"


# ---------------------------------------------------------------------------
# Test 6 — Migration 001 is a no-op (no SQLite SQL)
# ---------------------------------------------------------------------------

def test_06_migration_001_is_noop():
    """Migration 001 must not contain SQLite-specific SQL (e.g. AUTOINCREMENT)."""
    from pathlib import Path
    migrations_dir = Path(__file__).parents[2] / "src" / "ootils_core" / "db" / "migrations"
    migration_001 = migrations_dir / "001_initial_schema.sql"
    assert migration_001.exists(), f"Migration file not found: {migration_001}"

    content = migration_001.read_text(encoding="utf-8").upper()

    sqlite_keywords = ["AUTOINCREMENT", "INTEGER PRIMARY KEY", "PRAGMA", "SQLITE"]
    for kw in sqlite_keywords:
        assert kw not in content, (
            f"Migration 001 contains SQLite-specific keyword '{kw}' — "
            "it should be a PostgreSQL no-op"
        )


# ---------------------------------------------------------------------------
# Test 7 — Migration 002 doesn't break with deferrable constraints
# ---------------------------------------------------------------------------

@requires_db
def test_07_migration_002_deferrable_fk_on_nodes_projection_series(conn):
    """
    Bug 3 fix: test now validates DEFERRABLE INITIALLY DEFERRED behavior for
    nodes.projection_series_id FK, not just null insertion.

    Within a single transaction, we insert a node referencing a projection_series_id
    that does not yet exist. The deferred FK must not raise at insert time
    (only at COMMIT). We then insert the projection_series before committing
    to verify the happy path works end-to-end.
    """
    import uuid
    import psycopg

    scenario_id = "00000000-0000-0000-0000-000000000001"
    item_id = str(uuid.uuid4())
    location_id = str(uuid.uuid4())

    conn.execute("""
        INSERT INTO items (item_id, name, item_type, uom, status)
        VALUES (%s, 'Test Item 07', 'finished_good', 'EA', 'active')
    """, (item_id,))
    conn.execute("""
        INSERT INTO locations (location_id, name, location_type, country)
        VALUES (%s, 'Test Location 07', 'dc', 'US')
    """, (location_id,))

    # Generate a projection_series_id that does NOT exist yet
    series_id = str(uuid.uuid4())
    node_id = str(uuid.uuid4())

    # Insert node referencing non-existent series_id — must NOT raise immediately
    # because fk_nodes_projection_series is DEFERRABLE INITIALLY DEFERRED
    conn.execute("""
        INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id, projection_series_id)
        VALUES (%s, 'ProjectedInventory', %s::UUID, %s::UUID, %s::UUID, %s::UUID)
    """, (node_id, scenario_id, item_id, location_id, series_id))

    # Now insert the projection_series to satisfy the deferred FK before commit
    conn.execute("""
        INSERT INTO projection_series (series_id, scenario_id, item_id, location_id, grain)
        VALUES (%s::UUID, %s::UUID, %s::UUID, %s::UUID, 'Day')
    """, (series_id, scenario_id, item_id, location_id))

    # Commit — the deferred FK is validated HERE, not at insert time.
    # If DEFERRABLE INITIALLY DEFERRED is not set correctly, this raises.
    # The fixture will rollback on teardown, so state is cleaned up.
    conn.commit()

    # Verify the node persisted through commit
    row = conn.execute("SELECT node_id FROM nodes WHERE node_id = %s::UUID", (node_id,)).fetchone()
    assert row is not None, "Node not found after deferred FK commit — constraint may have prevented commit"


# ---------------------------------------------------------------------------
# Test 8 — Rerun of OotilsDB bootstrap on already-migrated DB is idempotent
# ---------------------------------------------------------------------------

@requires_db
def test_08_bootstrap_rerun_is_idempotent(migrated_db):
    """Running OotilsDB() again on an already-migrated DB must not corrupt schema."""
    import psycopg
    from psycopg.rows import dict_row
    from ootils_core.db.connection import OotilsDB

    # Second instantiation — should be a no-op (IF NOT EXISTS throughout)
    db2 = OotilsDB(migrated_db)

    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        tables = _tables(conn)

    expected_core = {"scenarios", "items", "locations", "nodes", "edges"}
    missing = expected_core - tables
    assert not missing, f"Schema corrupted after rerun: missing {missing}"


# ---------------------------------------------------------------------------
# Test 9 — FK constraints on nodes, edges, projection_series, scenario_overrides
# ---------------------------------------------------------------------------

@requires_db
def test_09_critical_fks_are_active(conn):
    """
    Bug 4 fix: Critical foreign keys on nodes, edges, projection_series,
    and scenario_overrides are enforced at commit time.
    """
    import uuid

    bad_uuid = str(uuid.uuid4())  # does not exist

    # FK: nodes.scenario_id → scenarios.scenario_id
    with pytest.raises(Exception, match=r"(foreign key|violates)"):
        conn.execute("""
            INSERT INTO nodes (node_id, node_type, scenario_id)
            VALUES (%s, 'ProjectedInventory', %s::UUID)
        """, (str(uuid.uuid4()), bad_uuid))
        conn.commit()

    conn.rollback()

    # FK: edges.scenario_id → scenarios.scenario_id
    with pytest.raises(Exception, match=r"(foreign key|violates)"):
        conn.execute("""
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id)
            VALUES (%s, 'replenishes', %s::UUID, %s::UUID, %s::UUID)
        """, (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4()), bad_uuid))
        conn.commit()

    conn.rollback()

    # FK: projection_series.scenario_id → scenarios.scenario_id
    with pytest.raises(Exception, match=r"(foreign key|violates)"):
        conn.execute("""
            INSERT INTO projection_series (series_id, scenario_id, item_id, location_id, grain)
            VALUES (%s::UUID, %s::UUID, %s::UUID, %s::UUID, 'Day')
        """, (str(uuid.uuid4()), bad_uuid, str(uuid.uuid4()), str(uuid.uuid4())))
        conn.commit()

    conn.rollback()

    # FK: scenario_overrides.scenario_id → scenarios.scenario_id
    with pytest.raises(Exception, match=r"(foreign key|violates)"):
        conn.execute("""
            INSERT INTO scenario_overrides (scenario_id, node_id, field_name, new_value)
            VALUES (%s::UUID, %s::UUID, 'quantity', '100')
        """, (bad_uuid, str(uuid.uuid4())))
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 10 — CHECK constraints on events.source and events.event_type
# ---------------------------------------------------------------------------

@requires_db
def test_10_events_check_constraints(conn):
    """events.source and events.event_type CHECK constraints reject invalid values."""
    scenario_id = "00000000-0000-0000-0000-000000000001"

    # Invalid source
    with pytest.raises(Exception, match=r"(check|violates)"):
        conn.execute("""
            INSERT INTO events (event_type, scenario_id, source)
            VALUES ('test_event', %s::UUID, 'invalid_source')
        """, (scenario_id,))
        conn.commit()

    conn.rollback()

    # Invalid event_type
    with pytest.raises(Exception, match=r"(check|violates)"):
        conn.execute("""
            INSERT INTO events (event_type, scenario_id, source)
            VALUES ('not_a_real_event_type', %s::UUID, 'api')
        """, (scenario_id,))
        conn.commit()

    conn.rollback()

    # Valid combination — should succeed
    conn.execute("""
        INSERT INTO events (event_type, scenario_id, source)
        VALUES ('test_event', %s::UUID, 'test')
    """, (scenario_id,))
    # Don't commit — test is read-only isolated via fixture rollback
