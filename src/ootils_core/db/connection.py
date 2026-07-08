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

import logging
import os
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import psycopg
from psycopg.rows import dict_row

from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

# Migrations are applied in filename order.
MIGRATIONS_DIR = Path(__file__).parent / "migrations"

# Default: Unix socket connection to local postgres, database 'ootils_dev'
DEFAULT_DATABASE_URL = (
    os.environ.get("DATABASE_URL")
    or os.environ.get("OOTILS_DSN")
    or "postgresql:///ootils_dev"
)

def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


# Server-side session guards applied to POOL connections ONLY (the API hot
# path). Direct psycopg.connect() calls made by scripts/watchers (the heavy
# offline MRP/calc runs) and by the migration runner deliberately do NOT
# inherit these — they open their own connections outside the pool.
#
#   statement_timeout — kill a runaway query. TENSION with the calc-run path:
#   an HTTP calc run (POST /v1/events, POST /v1/calc/...) borrows a POOL
#   connection, so its statement_timeout is THIS value. The longest legitimate
#   pilot calc run measured was 464 s; the ceiling is set to 900 s (15 min) so
#   the worst observed run plus generous margin cannot be tripped, while a
#   genuinely hung query still eventually dies. This is a stop-gap: the real
#   fix is moving long calc runs off the synchronous request path onto async
#   workers (#193) — once landed, the API pool can drop back to a tight ~5 min
#   web timeout. The heavy offline runs invoked from scripts/*.py are NOT
#   affected (they use their own psycopg.connect), so this ceiling never caps a
#   batch/CLI run.
#
#   idle_in_transaction_session_timeout — a request that opens a transaction
#   and then stalls (client gone, handler wedged) must not pin a pooled
#   connection and its locks indefinitely. 60 s is well above any normal
#   request-scoped transaction and reclaims a leaked one quickly.
_DEFAULT_STATEMENT_TIMEOUT_MS = 900_000
_DEFAULT_IDLE_IN_TXN_TIMEOUT_MS = 60_000


def _pool_session_options() -> str:
    """libpq ``options`` string of the per-connection session guards for the
    pool. Both values are env-overridable (0 disables a guard, matching the
    Postgres convention) but default to the documented ceilings above."""
    statement_timeout = _env_int(
        "OOTILS_DB_STATEMENT_TIMEOUT_MS", _DEFAULT_STATEMENT_TIMEOUT_MS
    )
    idle_in_txn_timeout = _env_int(
        "OOTILS_DB_IDLE_IN_TXN_TIMEOUT_MS", _DEFAULT_IDLE_IN_TXN_TIMEOUT_MS
    )
    return (
        f"-c statement_timeout={statement_timeout} "
        f"-c idle_in_transaction_session_timeout={idle_in_txn_timeout}"
    )


def _make_connection_pool(database_url: str):
    try:
        from psycopg_pool import ConnectionPool
    except ImportError:
        return None

    min_size = _env_int("OOTILS_DB_POOL_MIN_SIZE", 1)
    max_size = _env_int("OOTILS_DB_POOL_MAX_SIZE", 10)
    timeout = _env_float("OOTILS_DB_POOL_TIMEOUT_SECONDS", 10.0)
    # Recycle long-lived / long-idle connections so a stale server-side
    # connection (network blip, server restart, PgBouncer churn) is retired
    # rather than handed to a request. max_lifetime caps total age; max_idle
    # caps idle time in the pool.
    max_lifetime = _env_float("OOTILS_DB_POOL_MAX_LIFETIME_SECONDS", 1800.0)
    max_idle = _env_float("OOTILS_DB_POOL_MAX_IDLE_SECONDS", 600.0)
    if min_size < 1:
        raise ValueError("OOTILS_DB_POOL_MIN_SIZE must be >= 1")
    if max_size < min_size:
        raise ValueError("OOTILS_DB_POOL_MAX_SIZE must be >= OOTILS_DB_POOL_MIN_SIZE")
    if timeout <= 0:
        raise ValueError("OOTILS_DB_POOL_TIMEOUT_SECONDS must be > 0")
    if max_lifetime <= 0:
        raise ValueError("OOTILS_DB_POOL_MAX_LIFETIME_SECONDS must be > 0")
    if max_idle <= 0:
        raise ValueError("OOTILS_DB_POOL_MAX_IDLE_SECONDS must be > 0")

    return ConnectionPool(
        conninfo=database_url,
        min_size=min_size,
        max_size=max_size,
        timeout=timeout,
        # check=check_connection: run a lightweight liveness probe on every
        # connection borrowed from the pool, so a connection the server closed
        # under us is detected and replaced BEFORE a handler tries to use it
        # (turns a mid-request OperationalError into a transparent reconnect).
        check=ConnectionPool.check_connection,
        max_lifetime=max_lifetime,
        max_idle=max_idle,
        open=True,
        # options: server-side session guards (statement_timeout,
        # idle_in_transaction_session_timeout) — POOL connections only, see
        # _pool_session_options.
        kwargs={"row_factory": dict_row, "options": _pool_session_options()},
    )


def _sqlstate_of(exc: Exception) -> str | None:
    """Return PostgreSQL SQLSTATE when available."""
    return getattr(exc, "sqlstate", None) or getattr(getattr(exc, "diag", None), "sqlstate", None)


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
        self._pool = None
        self._pool_checked = False
        self._apply_migrations()

    def _get_pool(self):
        if not self._pool_checked:
            self._pool = _make_connection_pool(self.database_url)
            self._pool_checked = True
        return self._pool

    def close(self) -> None:
        pool = self._get_pool()
        if pool is not None:
            pool.close()

    @contextmanager
    def conn(self) -> Generator[DictRowConnection, None, None]:
        """
        Yield a configured psycopg3 Connection with dict_row factory.
        Commits on success, rolls back on exception, always closes.
        """
        pool = self._get_pool()
        connection_cm = pool.connection() if pool is not None else psycopg.connect(self.database_url, row_factory=dict_row)

        with connection_cm as connection:
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def _apply_migrations(self) -> None:
        """Apply pending .sql migration files in order.

        Uses a schema_migrations tracking table so each migration runs
        exactly once. An advisory lock prevents concurrent migration
        attempts from racing. Each migration runs inside its own database
        transaction so partial multi-statement application cannot be marked
        as successful.
        """
        migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
        if not migration_files:
            return

        # Use advisory lock (key = hash of 'ootils_migrations') to prevent
        # concurrent migration attempts from multiple app instances.
        _LOCK_KEY = 8_037_421_901  # arbitrary fixed int64

        with psycopg.connect(
            self.database_url,
            autocommit=True,
            row_factory=psycopg.rows.dict_row,
        ) as conn:
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
                        with conn.transaction():
                            with conn.cursor() as cur:
                                cur.execute(migration_sql)
                            conn.execute(
                                "INSERT INTO schema_migrations (version) VALUES (%s) ON CONFLICT DO NOTHING",
                                (version,),
                            )
                        applied.add(version)
                    except Exception as e:
                        sqlstate = _sqlstate_of(e)
                        sqlstate_suffix = f" [sqlstate={sqlstate}]" if sqlstate else ""
                        logger.error(
                            "Migration failed: %s%s — %s",
                            migration_path.name, sqlstate_suffix, e,
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
                # Apply a 5-second statement timeout for all health-check queries
                # to prevent monitoring hangs on large tables (fix for #160).
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout = '5000'")

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
