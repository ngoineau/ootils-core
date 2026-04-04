"""
connection.py — SQLite connection management for Ootils Core
ADR-005: Storage Layer and Data Model

Responsibilities:
- Open and configure SQLite connections (WAL, FK enforcement)
- Apply migrations in order
- Provide a lightweight health check
- Never expose raw cursors — always use context managers
"""
from __future__ import annotations

import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

# Migrations are applied in filename order.
MIGRATIONS_DIR = Path(__file__).parent / "migrations"

# Default database location (override via env or constructor arg)
DEFAULT_DB_PATH = Path.home() / ".ootils" / "ootils.db"


class OotilsDB:
    """
    Thin connection wrapper. Single instance per process.

    Usage:
        db = OotilsDB()               # uses ~/.ootils/ootils.db
        db = OotilsDB(":memory:")     # for tests — keeps a persistent in-memory connection
        db = OotilsDB("/tmp/test.db") # custom path

        with db.conn() as conn:
            conn.execute("SELECT 1")
    """

    def __init__(self, db_path: str | Path | None = None) -> None:
        self._in_memory = (db_path == ":memory:")
        if self._in_memory:
            self.db_path = ":memory:"
            # In-memory: keep one persistent connection alive for the lifetime of this instance.
            self._mem_conn: sqlite3.Connection | None = sqlite3.connect(
                ":memory:", check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES
            )
            self._mem_conn.row_factory = sqlite3.Row
            self._configure(self._mem_conn)
        else:
            self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._mem_conn = None
        self._apply_migrations()

    @staticmethod
    def _configure(connection: sqlite3.Connection) -> None:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=NORMAL")

    @contextmanager
    def conn(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Yield a configured SQLite connection. Commits on success, rolls back on exception.
        For :memory: databases, yields the persistent shared connection (no open/close).
        """
        if self._in_memory:
            assert self._mem_conn is not None
            try:
                yield self._mem_conn
                self._mem_conn.commit()
            except Exception:
                self._mem_conn.rollback()
                raise
        else:
            connection = sqlite3.connect(
                str(self.db_path), detect_types=sqlite3.PARSE_DECLTYPES
            )
            connection.row_factory = sqlite3.Row
            try:
                self._configure(connection)
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    def _apply_migrations(self) -> None:
        """Apply all .sql migration files in order. Idempotent (IF NOT EXISTS throughout)."""
        migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
        if not migration_files:
            return

        if self._in_memory:
            # executescript() on the persistent connection directly
            assert self._mem_conn is not None
            for migration_path in migration_files:
                sql = migration_path.read_text(encoding="utf-8")
                self._mem_conn.executescript(sql)
        else:
            # For file DBs, open a short-lived connection for migration
            connection = sqlite3.connect(str(self.db_path), detect_types=sqlite3.PARSE_DECLTYPES)
            try:
                for migration_path in migration_files:
                    sql = migration_path.read_text(encoding="utf-8")
                    connection.executescript(sql)
            finally:
                connection.close()

    def health_check(self) -> dict:
        """
        Verify database integrity:
        1. All foreign keys resolve
        2. No orphaned edges
        3. No calc_runs stuck in 'running' > 60 seconds
        Returns {"ok": True} or {"ok": False, "issues": [...]}
        """
        issues = []

        with self.conn() as c:
            # 1. FK integrity
            fk_violations = c.execute("PRAGMA foreign_key_check").fetchall()
            if fk_violations:
                issues.append(f"FK violations: {[dict(r) for r in fk_violations[:5]]}")

            # 2. Orphaned edges (from/to nodes inactive)
            orphaned = c.execute("""
                SELECT COUNT(*) as cnt FROM edges e
                WHERE e.active = TRUE
                  AND (
                      NOT EXISTS (SELECT 1 FROM nodes n WHERE n.node_id = e.from_node_id AND n.active = TRUE)
                   OR NOT EXISTS (SELECT 1 FROM nodes n WHERE n.node_id = e.to_node_id   AND n.active = TRUE)
                  )
            """).fetchone()
            if orphaned and orphaned["cnt"] > 0:
                issues.append(f"Orphaned edges: {orphaned['cnt']}")

            # 3. Stuck calc_runs
            stuck = c.execute("""
                SELECT COUNT(*) as cnt FROM calc_runs
                WHERE status = 'running'
                  AND started_at < datetime('now', '-60 seconds')
            """).fetchone()
            if stuck and stuck["cnt"] > 0:
                issues.append(f"Stuck calc_runs: {stuck['cnt']}")

        return {"ok": len(issues) == 0, "issues": issues}


def new_id() -> str:
    """Generate a new UUID v4 string. Used for all PKs."""
    return str(uuid.uuid4())
