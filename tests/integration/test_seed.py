"""
tests/integration/test_seed.py — Lot B: Seed and bootstrap tests (tests 11–16).

Verifies that scripts/seed_demo_data.py executes correctly against a real
PostgreSQL instance and produces the expected data.

Skip all tests if DATABASE_URL is not configured.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from .conftest import requires_db, DB_AVAILABLE, TEST_DB_URL

SCRIPTS_DIR = Path(__file__).parents[2] / "scripts"
SEED_SCRIPT = SCRIPTS_DIR / "seed_demo_data.py"

# Allowed values for events.source CHECK constraint
ALLOWED_SOURCES = {"api", "ingestion", "engine", "user", "test"}


def _run_seed(env: dict | None = None) -> subprocess.CompletedProcess:
    """Run the seed script as a subprocess with the test DATABASE_URL."""
    base_env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    if env:
        base_env.update(env)
    return subprocess.run(
        [sys.executable, str(SEED_SCRIPT)],
        capture_output=True,
        text=True,
        env=base_env,
    )


# ---------------------------------------------------------------------------
# Test 11 — seed_demo_data.py executes successfully on a freshly migrated DB
# ---------------------------------------------------------------------------

@requires_db
def test_11_seed_runs_successfully(migrated_db):
    """scripts/seed_demo_data.py exits 0 on a freshly migrated DB."""
    result = _run_seed()
    assert result.returncode == 0, (
        f"Seed script failed (rc={result.returncode}):\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
    assert "Seed complete" in result.stdout or "✅" in result.stdout


# ---------------------------------------------------------------------------
# Test 12 — Minimum row counts after seed
# ---------------------------------------------------------------------------

@requires_db
def test_12_seed_minimum_row_counts(migrated_db):
    """After seed, items, locations, nodes, and events have minimum expected counts."""
    import psycopg
    from psycopg.rows import dict_row

    # Ensure seed has run
    _run_seed()

    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        items_count = conn.execute("SELECT COUNT(*) AS n FROM items").fetchone()["n"]
        locations_count = conn.execute("SELECT COUNT(*) AS n FROM locations").fetchone()["n"]
        nodes_count = conn.execute("SELECT COUNT(*) AS n FROM nodes").fetchone()["n"]
        events_count = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"]

    assert items_count >= 2, f"Expected >= 2 items, got {items_count}"
    assert locations_count >= 2, f"Expected >= 2 locations, got {locations_count}"
    assert nodes_count >= 90, f"Expected >= 90 nodes (daily PI buckets), got {nodes_count}"
    assert events_count >= 1, f"Expected >= 1 events, got {events_count}"


# ---------------------------------------------------------------------------
# Test 13 — Seed does not insert forbidden values in events.source
# ---------------------------------------------------------------------------

@requires_db
def test_13_seed_no_forbidden_event_sources(migrated_db):
    """All events inserted by seed use an allowed source value."""
    import psycopg
    from psycopg.rows import dict_row

    _run_seed()

    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        rows = conn.execute("SELECT DISTINCT source FROM events").fetchall()

    sources = {r["source"] for r in rows}
    forbidden = sources - ALLOWED_SOURCES
    assert not forbidden, (
        f"Seed inserted events with forbidden source values: {forbidden}. "
        f"Allowed: {ALLOWED_SOURCES}"
    )


# ---------------------------------------------------------------------------
# Test 14 — Seed uses psycopg (psycopg3), compatible with production runtime
# ---------------------------------------------------------------------------

def test_14_seed_uses_psycopg3():
    """
    The seed script imports psycopg (psycopg3), not psycopg2.
    This ensures runtime compatibility with the Docker image.
    """
    content = SEED_SCRIPT.read_text(encoding="utf-8")
    # Must import psycopg (v3), not psycopg2
    assert "import psycopg" in content, "Seed script must import psycopg (v3)"
    assert "psycopg2" not in content, (
        "Seed script must not import psycopg2 — Docker image uses psycopg v3"
    )


# ---------------------------------------------------------------------------
# Test 15 — Seed is idempotent: second run succeeds or fails cleanly (no corruption)
# ---------------------------------------------------------------------------

@requires_db
def test_15_seed_idempotent(migrated_db):
    """
    Running seed twice either succeeds (idempotent via ON CONFLICT DO NOTHING)
    or fails with a clear error — in either case the DB is not corrupted.
    """
    import psycopg
    from psycopg.rows import dict_row

    # First run
    result1 = _run_seed()
    assert result1.returncode == 0, f"First seed run failed: {result1.stderr}"

    # Count rows before second run
    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        count_before = conn.execute("SELECT COUNT(*) AS n FROM nodes").fetchone()["n"]

    # Second run
    result2 = _run_seed()

    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        count_after = conn.execute("SELECT COUNT(*) AS n FROM nodes").fetchone()["n"]

    if result2.returncode == 0:
        # Idempotent: row count must be stable
        assert count_after == count_before, (
            f"Second seed run changed row count from {count_before} to {count_after} "
            "(expected idempotent ON CONFLICT DO NOTHING)"
        )
    else:
        # Acceptable failure: open a fresh connection to verify DB integrity
        # (the previous `with psycopg.connect(...)` block is already closed)
        with psycopg.connect(migrated_db, row_factory=dict_row) as check_conn:
            tables_query = check_conn.execute("""
                SELECT tablename FROM pg_tables WHERE schemaname = 'public'
            """).fetchall()
        tables = {r["tablename"] for r in tables_query}
        assert "nodes" in tables, "DB corrupted after failed second seed run"


# ---------------------------------------------------------------------------
# Test 16 — After seed, GET /health is OK and GET /v1/issues returns payload
# ---------------------------------------------------------------------------

@requires_db
def test_16_after_seed_api_health_and_issues(migrated_db):
    """
    After seed, the FastAPI app (TestClient) returns 200 on /health
    and a valid issues payload on GET /v1/issues with auth.
    """
    import os
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "test-token-seed"

    # Ensure seed data is present
    _run_seed()

    # Re-import to pick up env changes
    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from httpx import ASGITransport, Client

    app = create_app()

    # Override get_db to use the test DB
    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db

    from fastapi.testclient import TestClient
    with TestClient(app) as client:
        # Health check
        resp = client.get("/health")
        assert resp.status_code == 200, f"Health failed: {resp.text}"
        assert resp.json()["status"] == "ok"

        # Issues with auth
        resp = client.get(
            "/v1/issues",
            headers={"Authorization": "Bearer test-token-seed"},
        )
        assert resp.status_code == 200, f"Issues failed: {resp.text}"
        data = resp.json()
        assert "issues" in data
        assert "total" in data
        assert isinstance(data["issues"], list)

    app.dependency_overrides.clear()
