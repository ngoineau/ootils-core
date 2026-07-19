"""
tests/integration/conftest.py — Shared fixtures for integration tests.

Requires a real PostgreSQL instance. Tests skip if DATABASE_URL is not set
or points to a non-reachable server.

Set DATABASE_URL to a *test* database before running:
    DATABASE_URL=postgresql://ootils:ootils@localhost:5432/ootils_test pytest tests/integration/
"""
from __future__ import annotations

import logging
import os
import warnings

import pytest

logger = logging.getLogger(__name__)

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
        OotilsDB(TEST_DB_URL)
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


# ---------------------------------------------------------------------------
# Runtime-exactitude net (chantier « moteur d'exception », CHANTIER 1)
# ---------------------------------------------------------------------------
# The migration-087 view `invariant_violations` states the engine's business
# laws ONCE, declaratively (projection balance, feeds_forward chain continuity,
# stockout-flag coherence, demand-split Σ=1, demand-descent mass conservation).
# It MUST be empty over the live, COMMITTED data. This autouse module-scoped
# fixture is the tripwire: after every integration module's tests (and AFTER
# that module's own deactivating finalizers, because it tears down before
# them in dependency order — it depends on `migrated_db`, everything else
# depends on it), it SELECTs the view on a FRESH connection (committed state
# only) and asserts emptiness.
#
# EXPLICIT, DATED EXEMPTION — never a silent weakening: a test that
# DELIBERATELY commits a pre-fix / invariant-violating state (e.g. the
# migration-083 idempotence proof commits a lone partial demand_split_pct
# row that can never form a Σ=1 split; the migration-080 backfill proof seeds
# a ProjectedInventory series with no feeds_forward edges) carries
# @pytest.mark.invariants_exempt(reason="..."). When ANY test in a module is
# so marked, a non-empty view at that module's teardown is DOWNGRADED from a
# hard failure to a loud, logged warning that prints both the violations and
# the exemption reasons. The exemption is module-granular by construction (the
# net is one check per module) — the marker is a visible, reviewable, dated
# signal at the seeding test itself, exactly the "assert once vs true
# continuously" doctrine's escape hatch.
_INVARIANT_VIOLATIONS_SQL = (
    "SELECT invariant, node_id, detail, scenario_id, severity "
    "FROM invariant_violations ORDER BY invariant, node_id"
)


def pytest_configure(config):
    """Register the exemption marker here too (belt-and-braces with the
    pyproject.toml [tool.pytest.ini_options] markers list) so the marker is
    known even when this conftest is collected in isolation."""
    config.addinivalue_line(
        "markers",
        "invariants_exempt(reason): the test deliberately commits a pre-fix / "
        "invariant-violating state; the invariant_violations net downgrades "
        "its module's teardown assertion to a dated, logged exemption "
        "(chantier moteur-d'exception CHANTIER 1).",
    )


def _module_invariant_exemptions(request) -> list[str]:
    """Reasons from every @pytest.mark.invariants_exempt in THIS module (a
    module-scoped `request.node` is the Module; filter session items by their
    file-path nodeid prefix)."""
    module_nodeid = request.node.nodeid
    reasons: list[str] = []
    for item in request.session.items:
        if item.nodeid.split("::", 1)[0] != module_nodeid:
            continue
        marker = item.get_closest_marker("invariants_exempt")
        if marker is None:
            continue
        reason = marker.kwargs.get("reason") or (marker.args[0] if marker.args else "")
        reasons.append(f"{item.nodeid} :: {reason or '<no reason given>'}")
    return reasons


@pytest.fixture(scope="module", autouse=True)
def _assert_no_invariant_violations(request, migrated_db):
    """Autouse tripwire over the migration-087 `invariant_violations` view.

    Depends on `migrated_db` for two reasons: (1) teardown ordering — this
    check must run while the schema still exists, i.e. BEFORE `migrated_db`
    drops every table, which reverse-order finalization guarantees; (2) DB
    availability — when no Postgres is reachable `migrated_db` skips, and so
    does this net (no false red on a DB-less run)."""
    yield  # run the module's tests first

    if not DB_AVAILABLE:
        return

    import psycopg
    from psycopg.rows import dict_row

    try:
        with psycopg.connect(migrated_db, row_factory=dict_row) as check_conn:
            rows = check_conn.execute(_INVARIANT_VIOLATIONS_SQL).fetchall()
    except Exception as exc:  # noqa: BLE001 — surface loudly, never swallow
        pytest.fail(
            f"invariant_violations net could not run at teardown of "
            f"{request.node.nodeid}: {exc!r}",
            pytrace=False,
        )

    if not rows:
        return

    detail_lines = "\n".join(
        f"  - [{r['severity']}] {r['invariant']} "
        f"node={r['node_id']} scenario={r['scenario_id']}: {r['detail']}"
        for r in rows
    )
    exemptions = _module_invariant_exemptions(request)

    if exemptions:
        message = (
            f"invariant_violations net: {len(rows)} committed violation(s) "
            f"TOLERATED in {request.node.nodeid} under an explicit, dated "
            f"@pytest.mark.invariants_exempt (chantier moteur-d'exception "
            f"CHANTIER 1):\n{detail_lines}\nExemptions:\n"
            + "\n".join(f"  * {e}" for e in exemptions)
        )
        logger.warning(message)
        warnings.warn(message, stacklevel=1)
        return

    pytest.fail(
        f"invariant_violations net TRIPPED at teardown of "
        f"{request.node.nodeid}: {len(rows)} committed state(s) violate a "
        f"business invariant (migration 087). Either the code under test "
        f"regressed, or a test deliberately seeded a pre-fix state and must "
        f"carry @pytest.mark.invariants_exempt(reason=...):\n{detail_lines}",
        pytrace=False,
    )
