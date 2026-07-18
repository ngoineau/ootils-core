"""
tests/integration/test_demand_descent_schema_integration.py — DB-backed tests
of migration 083 (DESC-1 PR-A, ADR-043: demand-descent schema — the four
tables demand_split_pct / state_dc_routing / item_dc_eligibility /
demand_descent_lines) against a real PostgreSQL — no mocks (CLAUDE.md).

Covers:

  1. CHECK constraints as the DB-level line of defense: pct out of (0, 1]
     rejected (0, negative, > 1) with the boundaries accepted (1, one
     quantum); unknown `method` rejected, the three sanctioned values
     accepted; state_code must match ^[A-Z]{2}$ (lowercase, digits, 3
     letters, empty all rejected); confidence outside [0, 1] rejected
     (NUMERIC(4,3) alone would admit up to 9.999 — the CHECK closes the gap,
     per the migration header).
  2. The NULLS NOT DISTINCT natural key: two baseline rows (scenario_id
     NULL, season_bucket NULL) for the same (item, dc) are REJECTED — the
     exact V1 common case a plain UNIQUE (or a scenario-only partial index)
     would silently under-protect, per the migration's UNIQUENESS note —
     while a scenario override row coexists with the baseline row and a
     different season_bucket opens a distinct slot.
  3. FK to scenarios is ON DELETE RESTRICT and EFFECTIVE: deleting a
     scenario referenced by a split row raises ForeignKeyViolation (plus the
     confdeltype='r' catalog assertion, mirroring test_scenario_fk_retention).
  4. Migration 083 idempotence: triple execution overall (#1 = the
     migrated_db boot; #2 and #3 re-run the file verbatim), the pattern of
     the 078/080/081 tests — like 078/081 the file carries its own
     BEGIN/COMMIT, so the re-runs go through a fresh autocommit connection.
     Committed rows survive the re-runs un-reset; the four tables, the
     NULLS NOT DISTINCT constraint, the named indexes, the FK surface
     (explicit confdeltype per the header's FK POLICY), the zero-JSONB
     promise and the table COMMENTs all hold after the third run; a second
     OotilsDB() boot is a tracked no-op.

ISOLATION (the committed-seed lesson, cf. test_is_stocking_integration):
every test-scoped write uses PREFIX-named rows ('MIG083-…'). The CHECK /
UNIQUE / FK tests never commit — each violation attempt runs inside a
SAVEPOINT (`conn.transaction()` nested in the fixture's open transaction) so
a failed statement never poisons the test's seeds, and the function-scoped
`conn` fixture rolls everything back. The idempotence test MUST commit (the
re-executed file runs on separate connections and must see the rows); its
residue is neutralized by a finalizer registered BEFORE the first commit —
DEACTIVATION only (state_dc_routing.active=FALSE, eligibility eligible=FALSE,
items.status='obsolete'), NEVER a DELETE — no cascade can take innocent rows
with it; the split row itself has no soft-delete flag and is left in place,
inert (keyed to the obsoleted PREFIX item). The module-scoped migrated_db
teardown drops the schema afterwards as the backstop.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import errors
from psycopg.rows import dict_row

from .conftest import requires_db

pytestmark = requires_db

PREFIX = "MIG083"

_REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_083 = (
    _REPO_ROOT / "src" / "ootils_core" / "db" / "migrations"
    / "083_demand_descent.sql"
)

DESC_TABLES = (
    "demand_split_pct",
    "state_dc_routing",
    "item_dc_eligibility",
    "demand_descent_lines",
)


# ---------------------------------------------------------------------------
# Seed helpers (rollback-only tests never commit these)
# ---------------------------------------------------------------------------


def _seed_item(conn) -> object:
    item_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"{PREFIX}-ITEM-{uuid4()}"),
    )
    return item_id


def _seed_location(conn) -> object:
    location_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"{PREFIX}-DC-{uuid4()}"),
    )
    return location_id


def _seed_scenario(conn) -> object:
    scenario_id = uuid4()
    conn.execute(
        "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
        (scenario_id, f"{PREFIX}-SCEN-{uuid4()}"),
    )
    return scenario_id


def _insert_split(
    conn,
    *,
    item_id,
    dc_location_id,
    scenario_id=None,
    season_bucket=None,
    pct="0.5",
    method="history",
    confidence=None,
):
    conn.execute(
        """
        INSERT INTO demand_split_pct
            (scenario_id, item_id, dc_location_id, season_bucket,
             pct, method, confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (scenario_id, item_id, dc_location_id, season_bucket,
         Decimal(pct), method, confidence),
    )


def _insert_routing(conn, *, state_code, dc_location_id):
    conn.execute(
        "INSERT INTO state_dc_routing (state_code, dc_location_id) "
        "VALUES (%s, %s)",
        (state_code, dc_location_id),
    )


def _rejected(conn, exc, fn, /, **kwargs):
    """Run one violating INSERT inside a SAVEPOINT so the failure never
    poisons the fixture's open transaction (seeds stay usable)."""
    with pytest.raises(exc):
        with conn.transaction():
            fn(conn, **kwargs)


# ===========================================================================
# 1. CHECK constraints
# ===========================================================================


class TestDemandSplitPctChecks:
    def test_pct_out_of_bounds_rejected_boundaries_accepted(self, conn):
        """CHECK (pct > 0 AND pct <= 1): zero, negative and >1 all rejected;
        the two boundaries of the legal interval (one quantum above 0, and
        exactly 1) are accepted."""
        item_id = _seed_item(conn)
        for bad in ("0", "-0.25", "1.00000001", "2"):
            _rejected(
                conn, errors.CheckViolation, _insert_split,
                item_id=item_id, dc_location_id=_seed_location(conn), pct=bad,
            )
        # Boundary witnesses — a fresh DC each time (natural-key slots).
        _insert_split(
            conn, item_id=item_id, dc_location_id=_seed_location(conn),
            pct="0.00000001",
        )
        _insert_split(
            conn, item_id=item_id, dc_location_id=_seed_location(conn), pct="1",
        )

    def test_method_unknown_rejected_sanctioned_values_accepted(self, conn):
        item_id = _seed_item(conn)
        for bad in ("vibes", "HISTORY", ""):
            _rejected(
                conn, errors.CheckViolation, _insert_split,
                item_id=item_id, dc_location_id=_seed_location(conn),
                method=bad,
            )
        for good in ("history", "equal_split", "manual"):
            _insert_split(
                conn, item_id=item_id, dc_location_id=_seed_location(conn),
                method=good,
            )

    def test_confidence_out_of_bounds_rejected(self, conn):
        """NUMERIC(4,3) alone would admit up to 9.999 — the CHECK closes the
        gap to [0, 1]; NULL stays legal (cold-start rows carry none)."""
        item_id = _seed_item(conn)
        for bad in (Decimal("1.5"), Decimal("-0.001")):
            _rejected(
                conn, errors.CheckViolation, _insert_split,
                item_id=item_id, dc_location_id=_seed_location(conn),
                confidence=bad,
            )
        for good in (None, Decimal("0"), Decimal("1"), Decimal("0.850")):
            _insert_split(
                conn, item_id=item_id, dc_location_id=_seed_location(conn),
                confidence=good,
            )


class TestStateDcRoutingChecks:
    def test_state_code_format_enforced(self, conn):
        """CHECK (state_code ~ '^[A-Z]{2}$'): lowercase is the headline
        rejection; digits, 3 letters and empty likewise; 'CA' passes."""
        dc = _seed_location(conn)
        for bad in ("ca", "Ca", "C1", "CAL", "C", ""):
            _rejected(
                conn, errors.CheckViolation, _insert_routing,
                state_code=bad, dc_location_id=dc,
            )
        _insert_routing(conn, state_code="CA", dc_location_id=dc)


# ===========================================================================
# 2. Natural key — UNIQUE NULLS NOT DISTINCT
# ===========================================================================


class TestDemandSplitPctNaturalKey:
    def test_two_baseline_rows_same_keys_rejected(self, conn):
        """THE case the constraint exists for (migration header, UNIQUENESS
        note): scenario_id NULL + season_bucket NULL twice for the same
        (item, dc) — under a plain UNIQUE every NULL is distinct and the
        duplicate would slip through; NULLS NOT DISTINCT must reject it."""
        item_id, dc = _seed_item(conn), _seed_location(conn)
        _insert_split(conn, item_id=item_id, dc_location_id=dc, pct="0.6")
        _rejected(
            conn, errors.UniqueViolation, _insert_split,
            item_id=item_id, dc_location_id=dc, pct="0.4",
        )

    def test_scenario_override_coexists_with_baseline_but_not_twice(self, conn):
        """A fork's override row (scenario_id set) lives alongside the
        baseline (NULL) row for the same (item, dc); a SECOND row for that
        same scenario is rejected (season_bucket NULL is still one key
        value under NULLS NOT DISTINCT, whatever scenario_id is)."""
        item_id, dc = _seed_item(conn), _seed_location(conn)
        scenario_id = _seed_scenario(conn)
        _insert_split(conn, item_id=item_id, dc_location_id=dc, pct="0.6")
        _insert_split(
            conn, item_id=item_id, dc_location_id=dc,
            scenario_id=scenario_id, pct="0.3",
        )
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM demand_split_pct "
            "WHERE item_id = %s AND dc_location_id = %s",
            (item_id, dc),
        ).fetchone()["n"]
        assert n == 2
        _rejected(
            conn, errors.UniqueViolation, _insert_split,
            item_id=item_id, dc_location_id=dc,
            scenario_id=scenario_id, pct="0.7",
        )

    def test_distinct_season_bucket_opens_a_distinct_slot(self, conn):
        """season_bucket is part of the key: the V2-ready seasonal rows do
        not collide with the V1 annual (NULL) row."""
        item_id, dc = _seed_item(conn), _seed_location(conn)
        _insert_split(conn, item_id=item_id, dc_location_id=dc)
        _insert_split(
            conn, item_id=item_id, dc_location_id=dc, season_bucket="SPRING",
        )
        _rejected(
            conn, errors.UniqueViolation, _insert_split,
            item_id=item_id, dc_location_id=dc, season_bucket="SPRING",
        )


# ===========================================================================
# 3. FK scenarios — ON DELETE RESTRICT, effective
# ===========================================================================


class TestScenarioFkRestrict:
    def test_delete_scenario_with_split_row_is_restricted(self, conn):
        """Behavioral proof, not just catalog: DELETE FROM scenarios while a
        demand_split_pct row references it raises ForeignKeyViolation. The
        archived-not-deleted doctrine (ADR-011/ADR-039) relies on this."""
        item_id, dc = _seed_item(conn), _seed_location(conn)
        scenario_id = _seed_scenario(conn)
        _insert_split(
            conn, item_id=item_id, dc_location_id=dc, scenario_id=scenario_id,
        )
        _rejected(
            conn, errors.ForeignKeyViolation,
            lambda c: c.execute(
                "DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,)
            ),
        )
        # The scenario and its override row are both still there.
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM scenarios WHERE scenario_id = %s",
            (scenario_id,),
        ).fetchone()["n"] == 1

    def test_scenarios_fk_is_explicit_restrict_in_catalog(self, conn):
        """confdeltype 'r' — the repo-wide convention the guard
        test_scenario_fk_retention asserts on EVERY scenarios FK; pinned
        here too so this file fails close to the migration if it drifts."""
        rows = conn.execute(
            "SELECT confdeltype FROM pg_constraint "
            "WHERE contype = 'f' "
            "  AND conrelid = 'demand_split_pct'::regclass "
            "  AND confrelid = 'scenarios'::regclass"
        ).fetchall()
        assert [r["confdeltype"] for r in rows] == ["r"]


# ===========================================================================
# 4. Migration 083 idempotence — triple execution (pattern of 078/080/081)
# ===========================================================================


class TestMigration083Idempotent:
    def test_triple_execution_preserves_schema_and_values(
        self, migrated_db, conn, request
    ):
        """Defensive-idempotence contract (migration 063 header; the runner
        in db/connection.py does NOT swallow 'already exists'): triple
        execution overall — #1 was the migrated_db boot, #2 and #3 re-run
        the file verbatim below on autocommit connections (like 078/081 the
        file carries its own BEGIN/COMMIT).

        This test COMMITS (the re-executed file must see the rows), so the
        residue is neutralized by a finalizer registered BEFORE the commit —
        deactivation only (routing active=FALSE, eligibility eligible=FALSE,
        item obsoleted), never a DELETE; the split row itself has no
        soft-delete flag and is left inert on the obsoleted PREFIX item."""
        item_id, dc_id = uuid4(), uuid4()
        routing_id, eligibility_id = uuid4(), uuid4()

        def _sweep():
            try:
                with psycopg.connect(migrated_db, autocommit=True) as c:
                    c.execute(
                        "UPDATE state_dc_routing SET active = FALSE "
                        "WHERE routing_id = %s",
                        (routing_id,),
                    )
                    c.execute(
                        "UPDATE item_dc_eligibility SET eligible = FALSE "
                        "WHERE eligibility_id = %s",
                        (eligibility_id,),
                    )
                    c.execute(
                        "UPDATE items SET status = 'obsolete' WHERE item_id = %s",
                        (item_id,),
                    )
            except Exception:
                pass  # best-effort — migrated_db teardown is the backstop

        request.addfinalizer(_sweep)

        # Committed witnesses with NON-DEFAULT values: a botched re-run that
        # recreated a table or re-applied a DEFAULT would reset them.
        with psycopg.connect(migrated_db, autocommit=True) as raw:
            raw.execute(
                "INSERT INTO items (item_id, name) VALUES (%s, %s)",
                (item_id, f"{PREFIX}-IDEM-ITEM-{uuid4()}"),
            )
            raw.execute(
                "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
                (dc_id, f"{PREFIX}-IDEM-DC-{uuid4()}"),
            )
            raw.execute(
                "INSERT INTO state_dc_routing "
                "    (routing_id, state_code, dc_location_id, effective_from) "
                "VALUES (%s, 'ZZ', %s, %s)",
                (routing_id, dc_id, date(2026, 1, 1)),
            )
            raw.execute(
                "INSERT INTO item_dc_eligibility "
                "    (eligibility_id, item_id, dc_location_id, source) "
                "VALUES (%s, %s, %s, 'manual')",
                (eligibility_id, item_id, dc_id),
            )
            raw.execute(
                "INSERT INTO demand_split_pct "
                "    (item_id, dc_location_id, pct, method, confidence) "
                "VALUES (%s, %s, %s, 'history', %s)",
                (item_id, dc_id, Decimal("0.62500000"), Decimal("0.850")),
            )

        sql_text = MIGRATION_083.read_text(encoding="utf-8")
        with psycopg.connect(migrated_db, autocommit=True) as raw:
            raw.execute(sql_text)  # execution #2
            raw.execute(sql_text)  # execution #3 — still a clean no-op

        # -- Committed values survived both re-runs, un-reset. --------------
        routing = conn.execute(
            "SELECT state_code, active, effective_from FROM state_dc_routing "
            "WHERE routing_id = %s",
            (routing_id,),
        ).fetchone()
        assert routing is not None
        assert routing["state_code"] == "ZZ"
        assert routing["active"] is True
        assert routing["effective_from"] == date(2026, 1, 1)

        elig = conn.execute(
            "SELECT eligible, source FROM item_dc_eligibility "
            "WHERE eligibility_id = %s",
            (eligibility_id,),
        ).fetchone()
        assert elig["eligible"] is True
        assert elig["source"] == "manual"  # non-default 'derived' survived

        split = conn.execute(
            "SELECT scenario_id, season_bucket, pct, method, confidence, "
            "       manual_override "
            "FROM demand_split_pct "
            "WHERE item_id = %s AND dc_location_id = %s",
            (item_id, dc_id),
        ).fetchall()
        assert len(split) == 1
        assert split[0]["scenario_id"] is None
        assert split[0]["season_bucket"] is None
        assert split[0]["pct"] == Decimal("0.62500000")
        assert split[0]["method"] == "history"
        assert split[0]["confidence"] == Decimal("0.850")
        assert split[0]["manual_override"] is False  # DEFAULT still applies

        # -- Schema shape after the third run. ------------------------------
        for table in DESC_TABLES:
            n = conn.execute(
                "SELECT COUNT(*) AS n FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            ).fetchone()["n"]
            assert n == 1, f"table {table} missing or duplicated"

        # Zero JSONB anywhere in the four tables (migration header promise).
        jsonb = conn.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND data_type = 'jsonb' "
            "  AND table_name = ANY(%s)",
            (list(DESC_TABLES),),
        ).fetchall()
        assert jsonb == []

        # The NULLS NOT DISTINCT natural key: present exactly once, and
        # actually NULLS NOT DISTINCT (not silently degraded to plain UNIQUE).
        nk = conn.execute(
            "SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint "
            "WHERE conname = 'demand_split_pct_natural_key' "
            "  AND conrelid = 'demand_split_pct'::regclass"
        ).fetchall()
        assert len(nk) == 1
        assert "NULLS NOT DISTINCT" in nk[0]["def"]

        # The eight named indexes exist exactly once each.
        for index_name in (
            "idx_demand_split_pct_dc",
            "idx_demand_split_pct_source_run",
            "idx_state_dc_routing_dc",
            "idx_item_dc_eligibility_dc",
            "idx_demand_descent_lines_scenario",
            "idx_demand_descent_lines_run",
            "idx_demand_descent_lines_source_node",
            "idx_demand_descent_lines_derived_node",
        ):
            n = conn.execute(
                "SELECT COUNT(*) AS n FROM pg_indexes "
                "WHERE schemaname = 'public' AND indexname = %s",
                (index_name,),
            ).fetchone()["n"]
            assert n == 1, f"index {index_name} missing or duplicated"

        # FK surface — every delete action EXPLICIT per the header's FK
        # POLICY ('r' = RESTRICT, 'n' = SET NULL; never the NO ACTION
        # default 'a').
        def _fk_surface(table: str) -> list[tuple[str, str]]:
            rows = conn.execute(
                "SELECT confrelid::regclass::text AS target, confdeltype "
                "FROM pg_constraint "
                "WHERE contype = 'f' AND conrelid = %s::regclass "
                "ORDER BY confrelid::regclass::text, confdeltype",
                (table,),
            ).fetchall()
            return [(r["target"], r["confdeltype"]) for r in rows]

        assert _fk_surface("demand_split_pct") == [
            ("calc_runs", "n"),
            ("items", "r"),
            ("locations", "r"),
            ("scenarios", "r"),
        ]
        assert _fk_surface("state_dc_routing") == [("locations", "r")]
        assert _fk_surface("item_dc_eligibility") == [
            ("items", "r"),
            ("locations", "r"),
        ]
        assert _fk_surface("demand_descent_lines") == [
            ("calc_runs", "r"),
            ("items", "r"),
            ("locations", "r"),
            ("nodes", "r"),
            ("nodes", "r"),
            # Explicit sweepable scenario scope (PR-A review): a fork's
            # ledger must purge BEFORE nodes (PURGE_WHITELIST) — RESTRICT
            # like every scenarios FK (guard test_scenario_fk_retention).
            ("scenarios", "r"),
        ]

        # COMMENTs survived the re-runs (COMMENT ON replaces, never errors).
        comment = conn.execute(
            "SELECT obj_description('demand_split_pct'::regclass) AS c"
        ).fetchone()["c"]
        assert comment is not None
        assert "NULLS NOT DISTINCT" in comment

    def test_bootstrap_rerun_is_idempotent(self, migrated_db):
        """A second OotilsDB() on an already-migrated DB (the exact boot
        path) is a no-op — 083 is tracked in schema_migrations, applied
        exactly once, and the four tables are intact."""
        from ootils_core.db.connection import OotilsDB

        OotilsDB(migrated_db)

        with psycopg.connect(migrated_db, row_factory=dict_row) as c:
            applied = c.execute(
                "SELECT COUNT(*) AS n FROM schema_migrations "
                "WHERE version LIKE '083%'"
            ).fetchone()["n"]
            assert applied == 1
            for table in DESC_TABLES:
                n = c.execute(
                    "SELECT COUNT(*) AS n FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = %s",
                    (table,),
                ).fetchone()["n"]
                assert n == 1
