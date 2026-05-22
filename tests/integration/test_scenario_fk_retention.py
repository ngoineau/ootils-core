"""
tests/integration/test_scenario_fk_retention.py — Lot Ootils Review R1: FK retention policy.

Verifies the schema-level guarantees introduced by migration 032:
  1. Every FK pointing at scenarios(scenario_id) declares ON DELETE RESTRICT.
  2. mrp_runs.scenario_id now has a FK to scenarios (was missing).
  3. DELETE FROM scenarios fails with ForeignKeyViolation when the scenario
     is referenced — soft-delete via status='archived' remains the only path.

See: docs/ADR-011-scenario-retention.md, docs/REVIEW-2026-05.md (R1).
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from .conftest import requires_db


# ---------------------------------------------------------------------------
# Schema-level guarantees
# ---------------------------------------------------------------------------


@requires_db
def test_all_scenario_fks_are_restrict(conn):
    """Every FK referencing scenarios(scenario_id) declares ON DELETE RESTRICT."""
    rows = conn.execute(
        """
        SELECT
            c.conrelid::regclass::text AS table_name,
            c.conname                  AS constraint_name,
            c.confdeltype              AS delete_action
        FROM pg_constraint c
        WHERE c.contype  = 'f'
          AND c.confrelid = 'scenarios'::regclass
        ORDER BY table_name, constraint_name
        """
    ).fetchall()

    assert rows, "no FKs pointing at scenarios found — migration did not run"

    offenders = [r for r in rows if r["delete_action"] != "r"]
    assert offenders == [], (
        f"FKs without ON DELETE RESTRICT remain: {offenders}"
    )


@requires_db
def test_mrp_runs_scenario_id_has_fk(conn):
    """mrp_runs.scenario_id now has the FK that migration 021 forgot."""
    row = conn.execute(
        """
        SELECT c.conname, c.confdeltype
        FROM pg_constraint c
        JOIN pg_attribute  a
          ON a.attrelid = c.conrelid
         AND a.attnum   = ANY (c.conkey)
        WHERE c.contype  = 'f'
          AND c.conrelid = 'mrp_runs'::regclass
          AND c.confrelid = 'scenarios'::regclass
          AND a.attname  = 'scenario_id'
        """
    ).fetchone()

    assert row is not None, "mrp_runs.scenario_id is still missing its FK"
    assert row["confdeltype"] == "r", (
        f"mrp_runs.scenario_id FK has delete action {row['confdeltype']!r}, expected 'r' (RESTRICT)"
    )


# ---------------------------------------------------------------------------
# Behavioral guarantee: hard-delete is blocked when scenario is referenced
# ---------------------------------------------------------------------------


@requires_db
def test_delete_scenario_with_node_raises_fk_violation(conn):
    """DELETE on a scenario referenced by a node fails with ForeignKeyViolation."""
    import psycopg

    scenario_id = uuid4()
    item_id = uuid4()
    location_id = uuid4()

    conn.execute(
        "INSERT INTO scenarios (scenario_id, name, status) VALUES (%s, %s, 'active')",
        (scenario_id, f"test-fk-retention-{scenario_id}"),
    )
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, 'fk-test-item')",
        (item_id,),
    )
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, 'fk-test-loc')",
        (location_id,),
    )
    conn.execute(
        """
        INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id)
        VALUES (%s, 'Item', %s, %s, %s)
        """,
        (uuid4(), scenario_id, item_id, location_id),
    )

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))


@requires_db
def test_delete_scenario_with_mrp_run_raises_fk_violation(conn):
    """The newly-added FK on mrp_runs.scenario_id blocks scenario hard-delete."""
    import psycopg

    scenario_id = uuid4()

    conn.execute(
        "INSERT INTO scenarios (scenario_id, name, status) VALUES (%s, %s, 'active')",
        (scenario_id, f"test-mrp-fk-{scenario_id}"),
    )
    conn.execute(
        """
        INSERT INTO mrp_runs (run_id, scenario_id, run_type, status, horizon_days, bucket_type)
        VALUES (%s, %s, 'APICS_FULL', 'completed', 90, 'WEEK')
        """,
        (uuid4(), scenario_id),
    )

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))


@requires_db
def test_delete_unreferenced_scenario_succeeds(conn):
    """A scenario with no children can still be hard-deleted (sanity check)."""
    scenario_id = uuid4()

    conn.execute(
        "INSERT INTO scenarios (scenario_id, name, status) VALUES (%s, %s, 'active')",
        (scenario_id, f"test-orphan-{scenario_id}"),
    )

    result = conn.execute(
        "DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,)
    )
    assert result.rowcount == 1
