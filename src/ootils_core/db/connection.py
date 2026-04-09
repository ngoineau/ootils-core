"""
connection.py — PostgreSQL connection management for Ootils Core
Sprint 1: Migrated from SQLite to PostgreSQL via psycopg3.

Responsibilities:
- Provide connection context managers (sync, psycopg3)
- Apply migrations in order (idempotent)
- Provide a lightweight health check
- Never expose raw cursors — always use context managers

Environment:
    DATABASE_URL — PostgreSQL DSN, e.g.:
        postgresql://user:pass@host:5432/dbname
        postgresql:///dbname (Unix socket, current user)
    Default: postgresql:///ootils_dev (Unix socket)
"""
from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import psycopg
from psycopg.rows import dict_row

# Migrations are applied in filename order.
MIGRATIONS_DIR = Path(__file__).parent / "migrations"

# Default: Unix socket connection to local postgres, database 'ootils_dev'
DEFAULT_DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql:///ootils_dev"
)


class OotilsDB:
    """
    PostgreSQL connection wrapper using psycopg3.

    Usage:
        db = OotilsDB()                              # uses DATABASE_URL env var
        db = OotilsDB("postgresql:///ootils_test")   # custom DSN

        with db.conn() as conn:
            conn.execute("SELECT 1")
    """

    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or DEFAULT_DATABASE_URL
        self._apply_migrations()

    @contextmanager
    def conn(self) -> Generator[psycopg.Connection, None, None]:
        """
        Yield a configured psycopg3 Connection with dict_row factory.
        Commits on success, rolls back on exception, always closes.
        """
        with psycopg.connect(self.database_url, row_factory=dict_row) as connection:
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def _apply_migrations(self) -> None:
        """Apply pending .sql migration files in order.

        Uses a schema_migrations tracking table so each migration runs
        exactly once.  An advisory lock prevents concurrent migration
        attempts from racing.
        """
        import sys

        migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
        if not migration_files:
            return

        # Use advisory lock (key = hash of 'ootils_migrations') to prevent
        # concurrent migration attempts from multiple app instances.
        _LOCK_KEY = 8_037_421_901  # arbitrary fixed int64

        with psycopg.connect(self.database_url, autocommit=True) as conn:
            # Acquire advisory lock (blocks until available)
            conn.execute("SELECT pg_advisory_lock(%s)", (_LOCK_KEY,))
            try:
                # Ensure tracking table exists
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version  TEXT PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """)

                # Load already-applied migrations
                applied = {
                    row["version"]
                    for row in conn.execute(
                        "SELECT version FROM schema_migrations"
                    ).fetchall()
                }

                for migration_path in migration_files:
                    version = migration_path.name
                    if version in applied:
                        continue

                    migration_sql = migration_path.read_text(encoding="utf-8")
                    try:
                        with conn.cursor() as cur:
                            cur.execute(migration_sql)
                        # Record successful migration
                        conn.execute(
                            "INSERT INTO schema_migrations (version) VALUES (%s)",
                            (version,),
                        )
                    except Exception as e:
                        err_msg = str(e).lower()
                        # Tolerate "already exists" only for initial bootstrap
                        # where the tracking table didn't exist yet.
                        if any(phrase in err_msg for phrase in [
                            "already exists",
                            "duplicate key",
                            "relation already exists",
                            "column already exists",
                            "constraint already exists",
                        ]):
                            # Still record it so we don't retry
                            try:
                                conn.execute(
                                    "INSERT INTO schema_migrations (version) VALUES (%s) ON CONFLICT DO NOTHING",
                                    (version,),
                                )
                            except Exception:
                                pass
                            continue
                        print(
                            f"[MIGRATION ERROR] {migration_path.name}: {e}",
                            file=sys.stderr,
                        )
                        raise
            finally:
                conn.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_KEY,))

    def health_check(self) -> dict:
        """
        Verify database integrity:
        1. Can connect and execute basic query
        2. No orphaned edges (referencing inactive nodes)
        3. No calc_runs stuck in 'running' > 60 seconds
        Returns {"ok": True} or {"ok": False, "issues": [...]}
        """
        issues = []

        try:
            with self.conn() as conn:
                # 1. Basic connectivity
                conn.execute("SELECT 1")

                # 2. Orphaned edges
                result = conn.execute("""
                    SELECT COUNT(*) AS cnt FROM edges e
                    WHERE e.active = TRUE
                      AND (
                          NOT EXISTS (SELECT 1 FROM nodes n WHERE n.node_id = e.from_node_id AND n.active = TRUE)
                       OR NOT EXISTS (SELECT 1 FROM nodes n WHERE n.node_id = e.to_node_id   AND n.active = TRUE)
                      )
                """).fetchone()
                if result and result["cnt"] > 0:
                    issues.append(f"Orphaned edges: {result['cnt']}")

                # 3. Stuck calc_runs
                result = conn.execute("""
                    SELECT COUNT(*) AS cnt FROM calc_runs
                    WHERE status = 'running'
                      AND started_at < now() - INTERVAL '60 seconds'
                """).fetchone()
                if result and result["cnt"] > 0:
                    issues.append(f"Stuck calc_runs: {result['cnt']}")

        except Exception as e:
            issues.append(f"Connection error: {e}")

        return {"ok": len(issues) == 0, "issues": issues}


def new_id() -> uuid.UUID:
    """Generate a new UUID v4. Used for all PKs."""
    return uuid.uuid4()
