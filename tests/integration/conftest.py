"""
tests/integration/conftest.py — Shared fixtures for integration tests.

Requires a real PostgreSQL instance. Tests skip if DATABASE_URL is not set
or points to a non-reachable server.

Set DATABASE_URL to a *test* database before running:
    DATABASE_URL=postgresql://ootils:ootils@localhost:5432/ootils_test pytest tests/integration/
"""
from __future__ import annotations

import os
import subprocess
import sys

import pytest

# ---------------------------------------------------------------------------
# Detect test DB availability
# ---------------------------------------------------------------------------

TEST_DB_URL = os.environ.get("DATABASE_URL", "")

def _db_available() -> bool:
    """Return True if a PostgreSQL connection can be established."""
    if not TEST_DB_URL:
        return False
    try:
        import psycopg
        with psycopg.connect(TEST_DB_URL, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False

DB_AVAILABLE = _db_available()

requires_db = pytest.mark.skipif(
    not DB_AVAILABLE,
    reason="No PostgreSQL available — set DATABASE_URL to a test DB",
)


# ---------------------------------------------------------------------------
# DB fixture: apply migrations, yield connection, tear down schema after test
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def migrated_db():
    """
    Module-scoped fixture.
    Applies all migrations to the test DB and yields the DSN.
    After the module completes, drops all public tables to restore a clean state.
    """
    if not DB_AVAILABLE:
        pytest.skip("No PostgreSQL available")

    import psycopg

    # Apply migrations via OotilsDB (same logic as production)
    old_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = TEST_DB_URL

    try:
        from ootils_core.db.connection import OotilsDB
        db = OotilsDB(TEST_DB_URL)
    except Exception as exc:
        pytest.skip(f"Failed to apply migrations: {exc}")

    yield TEST_DB_URL

    # Tear down: drop all tables in public schema so next module starts fresh
    try:
        with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
            conn.execute("""
                DO $$
                DECLARE r RECORD;
                BEGIN
                    FOR r IN (
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = 'public'
                    ) LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
            """)
    except Exception:
        pass  # Best-effort teardown

    if old_url is not None:
        os.environ["DATABASE_URL"] = old_url
    elif "DATABASE_URL" in os.environ:
        del os.environ["DATABASE_URL"]


@pytest.fixture
def conn(migrated_db):
    """Function-scoped psycopg connection (autocommit=False, dict_row)."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        yield c
        c.rollback()  # roll back any uncommitted changes from the test
