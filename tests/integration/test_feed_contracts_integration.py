"""
tests/integration/test_feed_contracts_integration.py — feed-contract registry
(INT-1 PR1, ADR-037) against a real PostgreSQL database (no mocks).

Covers the DB half of src/ootils_core/interfaces/contracts.py + migration 073:

  1. Schema guarantees of migration 073: exact column set (zero JSONB —
     bounded-shape config, the carve-out does not apply), the
     UNIQUE(feed_key, version) constraint, the at-most-one-active-per-feed_key
     partial UNIQUE index, every CHECK (entity_type, format, criticality,
     version >= 1, non-empty arrays, non-blank feed_key, positive arrival
     window) — including load_mode's DB-level refusal of 'delta', the second
     line of defense behind the Pydantic Literal.
  2. Loader versioning: first upsert -> version 1 active; identical reload ->
     traced no-op (still version 1, INFO log, nothing written); changed
     content -> version 2, version 1 kept intact (append-only per version,
     only its `active`/`updated_at` bookkeeping flips); get_active_contract
     returns the latest ACTIVE version, None for unknown/retired feeds (no
     fallback to the latest inactive version); upsert never commits (caller
     owns the transaction).
  3. The CLI end-to-end (scripts/load_feed_contracts.py main(), in-process
     like the watcher smokes): first run registers the 3 committed seeds at
     version 1, an identical rerun is a full no-op; --dry-run writes nothing.
  4. Migration 073 idempotence: re-executing the file verbatim on an
     already-migrated DB (rows present) is a clean no-op that preserves data
     and schema; a second OotilsDB() bootstrap likewise (defensive-idempotence
     contract, canonical pattern of migration 063's header).

feed_contracts has NO OUTBOUND foreign key (no scenarios FK: global
ingest-time config, not scenario-scoped). Since migration 078 (ADR-042 PR-2,
which absorbed ADR-037's INT-1 PR2 earlier than the "PR3" this file
originally predicted) it has exactly ONE inbound FK —
daily_runs.feed_contract_id, ON DELETE RESTRICT — asserted below. The
pre-test cleanup therefore truncates daily_runs alongside feed_contracts:
Postgres refuses to TRUNCATE a referenced table alone, even when the
referencing table is empty.
"""
from __future__ import annotations

import logging
import sys
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from psycopg import errors
from psycopg.rows import dict_row

from ootils_core.interfaces.contracts import (
    FeedContractSpec,
    get_active_contract,
    upsert_contract,
)

from .conftest import requires_db

_REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_073 = (
    _REPO_ROOT / "src" / "ootils_core" / "db" / "migrations"
    / "073_feed_contracts.sql"
)
CONFIG_DIR = _REPO_ROOT / "config" / "feed-contracts"

# Import seam: the loader CLI lives under scripts/ (outside the package),
# exactly as the sibling watcher integration tests do.
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import load_feed_contracts  # noqa: E402

pytestmark = requires_db

_CONTRACTS_LOGGER = "ootils_core.interfaces.contracts"
SEED_KEYS = ["on-hand", "open-purchase-orders", "open-work-orders"]


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_feed_contracts(migrated_db):
    """Pre-test TRUNCATE on its own autocommit connection. daily_runs
    (migration 078, ADR-042 PR-2) references feed_contracts, and Postgres
    refuses to truncate a referenced table alone even when the referencing
    table is empty — both go in ONE statement, child listed with its parent
    (asserted by test_fk_surface_no_outbound_inbound_daily_runs_only).
    Pre-test only — a post-yield TRUNCATE could block on the
    function-scoped `conn` fixture's still-open transaction; committed
    leftovers (the CLI tests commit) are swept by the NEXT test's pre-clean
    and by the module teardown's DROP of all public tables."""
    with psycopg.connect(migrated_db, autocommit=True) as c:
        c.execute("TRUNCATE daily_runs, feed_contracts")
    yield


def _payload(**over) -> dict:
    base = {
        "feed_key": "it-feed",
        "entity_type": "on_hand",
        "source_system": "WMS-IT",
        "format": "csv",
        "key_columns": ["item_external_id", "location_external_id"],
        "mandatory_columns": [
            "item_external_id", "location_external_id", "quantity",
        ],
        "load_mode": "full",
        "cadence": "0 6 * * *",
        "arrival_window_minutes": 90,
        "owner": "it-ops",
        "criticality": "blocking",
        "volume_guard_min_rows": 100,
        "volume_guard_max_pct_delta": Decimal("0.20"),
        "depends_on": [],
    }
    base.update(over)
    return base


def _spec(**over) -> FeedContractSpec:
    return FeedContractSpec.model_validate(_payload(**over))


def _rows(conn, feed_key: str) -> list[dict]:
    return conn.execute(
        "SELECT * FROM feed_contracts WHERE feed_key = %s ORDER BY version",
        (feed_key,),
    ).fetchall()


def _count(conn) -> int:
    return conn.execute(
        "SELECT COUNT(*) AS n FROM feed_contracts"
    ).fetchone()["n"]


def _raw_insert(conn, **over):
    """Direct INSERT bypassing the Pydantic layer — used to prove the DB is
    its own (second) line of defense. Arrays cast explicitly so an empty
    list reaches the cardinality CHECK, not a type-inference error."""
    row = {
        "feed_key": "raw-feed",
        "version": 1,
        "entity_type": "on_hand",
        "source_system": "RAW-TEST",
        "format": "csv",
        "key_columns": ["a"],
        "mandatory_columns": ["a", "b"],
        "load_mode": "full",
        "cadence": "0 6 * * *",
        "arrival_window_minutes": 60,
        "owner": "raw-ops",
        "criticality": "blocking",
        "active": True,
    }
    row.update(over)
    conn.execute(
        "INSERT INTO feed_contracts ("
        "    feed_key, version, entity_type, source_system, format, "
        "    key_columns, mandatory_columns, load_mode, cadence, "
        "    arrival_window_minutes, owner, criticality, active"
        ") VALUES ("
        "    %(feed_key)s, %(version)s, %(entity_type)s, %(source_system)s, "
        "    %(format)s, %(key_columns)s::text[], "
        "    %(mandatory_columns)s::text[], %(load_mode)s, %(cadence)s, "
        "    %(arrival_window_minutes)s, %(owner)s, %(criticality)s, "
        "    %(active)s"
        ")",
        row,
    )


# ---------------------------------------------------------------------------
# 1. Migration 073 — schema guarantees
# ---------------------------------------------------------------------------


class TestMigration073Schema:
    def test_table_columns_exact_and_zero_jsonb(self, conn):
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'feed_contracts'"
        ).fetchall()
        columns = {r["column_name"] for r in rows}
        assert columns == {
            "feed_contract_id", "feed_key", "version", "entity_type",
            "source_system", "format", "key_columns", "mandatory_columns",
            "load_mode", "cadence", "arrival_window_minutes", "owner",
            "criticality", "volume_guard_min_rows",
            "volume_guard_max_pct_delta", "depends_on", "active",
            "created_at", "updated_at",
        }
        # Bounded-shape config: every column typed, the JSONB carve-out does
        # not apply here.
        jsonb = [r["column_name"] for r in rows if r["data_type"] == "jsonb"]
        assert jsonb == []

    def test_indexes_present_and_active_index_is_partial_unique(self, conn):
        rows = conn.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = 'feed_contracts'"
        ).fetchall()
        by_name = {r["indexname"]: r["indexdef"] for r in rows}
        assert "idx_feed_contracts_feed_key_version" in by_name
        assert "uq_feed_contracts_active_per_feed" in by_name
        active_def = by_name["uq_feed_contracts_active_per_feed"]
        assert "UNIQUE" in active_def
        assert "WHERE active" in active_def

    def test_fk_surface_no_outbound_inbound_daily_runs_only(self, conn):
        """No OUTBOUND FK (no scenarios FK: global ingest-time config, not
        scenario-scoped). Exactly ONE inbound FK since migration 078
        (ADR-042 PR-2, which absorbed the "PR3" this test originally
        predicted): daily_runs.feed_contract_id, ON DELETE RESTRICT
        (confdeltype 'r' — the explicit-RESTRICT repo convention, never the
        Postgres-default NO ACTION)."""
        outbound = conn.execute(
            "SELECT COUNT(*) AS n FROM pg_constraint "
            "WHERE contype = 'f' AND conrelid = 'feed_contracts'::regclass"
        ).fetchone()["n"]
        assert outbound == 0
        inbound = conn.execute(
            "SELECT conrelid::regclass::text AS source, confdeltype "
            "FROM pg_constraint "
            "WHERE contype = 'f' AND confrelid = 'feed_contracts'::regclass "
            "ORDER BY conrelid::regclass::text"
        ).fetchall()
        assert [(r["source"], r["confdeltype"]) for r in inbound] == [
            ("daily_runs", "r")
        ]

    def test_feed_key_version_unique(self, conn):
        _raw_insert(conn, feed_key="uq-feed", version=1, active=True)
        with pytest.raises(errors.UniqueViolation):
            _raw_insert(conn, feed_key="uq-feed", version=1, active=False)
        conn.rollback()

    def test_at_most_one_active_version_per_feed_key(self, conn):
        """The partial UNIQUE index is a DB guarantee, not just an app-level
        invariant: two active rows for one feed_key must be impossible."""
        _raw_insert(conn, feed_key="act-feed", version=1, active=True)
        with pytest.raises(errors.UniqueViolation):
            _raw_insert(conn, feed_key="act-feed", version=2, active=True)
        conn.rollback()

        # ... while inactive history rows coexist freely with one active.
        _raw_insert(conn, feed_key="act-feed", version=1, active=False)
        _raw_insert(conn, feed_key="act-feed", version=2, active=False)
        _raw_insert(conn, feed_key="act-feed", version=3, active=True)
        assert len(_rows(conn, "act-feed")) == 3

    def test_load_mode_check_admits_only_full(self, conn):
        """The DB-level second line of defense: even a contract that slipped
        past Python is refused ('delta' arrives in a V2 migration widening
        this CHECK, fail-loudly until then)."""
        with pytest.raises(errors.CheckViolation):
            _raw_insert(conn, load_mode="delta")
        conn.rollback()

    def test_entity_type_check_rejects_unknown_value(self, conn):
        with pytest.raises(errors.CheckViolation):
            _raw_insert(conn, entity_type="widgets")
        conn.rollback()

    def test_criticality_check_rejects_unknown_value(self, conn):
        with pytest.raises(errors.CheckViolation):
            _raw_insert(conn, criticality="critical")
        conn.rollback()

    def test_format_check_rejects_unknown_value(self, conn):
        with pytest.raises(errors.CheckViolation):
            _raw_insert(conn, format="xml")
        conn.rollback()

    def test_version_must_be_at_least_1(self, conn):
        with pytest.raises(errors.CheckViolation):
            _raw_insert(conn, version=0)
        conn.rollback()

    def test_empty_key_columns_rejected(self, conn):
        with pytest.raises(errors.CheckViolation):
            _raw_insert(conn, key_columns=[])
        conn.rollback()

    def test_empty_mandatory_columns_rejected(self, conn):
        with pytest.raises(errors.CheckViolation):
            _raw_insert(conn, mandatory_columns=[])
        conn.rollback()

    def test_blank_or_padded_feed_key_rejected(self, conn):
        with pytest.raises(errors.CheckViolation):
            _raw_insert(conn, feed_key="")
        conn.rollback()
        with pytest.raises(errors.CheckViolation):
            _raw_insert(conn, feed_key=" padded ")
        conn.rollback()

    def test_arrival_window_must_be_positive(self, conn):
        with pytest.raises(errors.CheckViolation):
            _raw_insert(conn, arrival_window_minutes=0)
        conn.rollback()


# ---------------------------------------------------------------------------
# 2. Loader versioning — upsert_contract / get_active_contract
# ---------------------------------------------------------------------------


class TestLoaderVersioning:
    def test_first_load_creates_version_1(self, conn):
        out = upsert_contract(conn, _spec())
        assert out.action == "created"
        assert out.version == 1

        active = get_active_contract(conn, "it-feed")
        assert active is not None
        assert active.version == 1
        assert active.active is True
        assert active.feed_contract_id == out.feed_contract_id
        assert active.entity_type == "on_hand"
        assert active.key_columns == [
            "item_external_id", "location_external_id",
        ]
        assert active.volume_guard_min_rows == 100
        assert active.volume_guard_max_pct_delta == Decimal("0.20")
        assert active.depends_on == []

    def test_identical_reload_is_traced_noop(self, conn, caplog):
        first = upsert_contract(conn, _spec())
        with caplog.at_level(logging.INFO, logger=_CONTRACTS_LOGGER):
            second = upsert_contract(conn, _spec())

        assert second.action == "no_op"
        assert second.version == 1
        assert second.feed_contract_id == first.feed_contract_id
        # Nothing written: still exactly one row.
        assert len(_rows(conn, "it-feed")) == 1
        # Traced: the no-op leaves an INFO line naming the feed.
        assert any(
            "no-op" in rec.getMessage() and "it-feed" in rec.getMessage()
            for rec in caplog.records
        ), f"no traced no-op log found in {[r.getMessage() for r in caplog.records]}"

    def test_changed_content_appends_version_2_keeps_version_1(self, conn):
        """Append-only per version: v2 supersedes v1, but v1's CONTENT is
        never rewritten — only its `active` bookkeeping flips."""
        v1 = upsert_contract(conn, _spec())
        conn.commit()  # separate transactions => distinct now() timestamps
        v2 = upsert_contract(conn, _spec(arrival_window_minutes=120))
        conn.commit()

        assert v2.action == "created"
        assert v2.version == 2
        assert v2.feed_contract_id != v1.feed_contract_id

        rows = _rows(conn, "it-feed")
        assert [r["version"] for r in rows] == [1, 2]
        # v1: superseded — inactive, content INTACT, updated_at bumped by
        # the bookkeeping flip.
        assert rows[0]["active"] is False
        assert rows[0]["arrival_window_minutes"] == 90
        assert rows[0]["updated_at"] > rows[0]["created_at"]
        # v2: the new active truth.
        assert rows[1]["active"] is True
        assert rows[1]["arrival_window_minutes"] == 120

        active = get_active_contract(conn, "it-feed")
        assert active is not None and active.version == 2

    def test_reload_after_supersede_is_noop_at_version_2(self, conn):
        upsert_contract(conn, _spec())
        upsert_contract(conn, _spec(arrival_window_minutes=120))
        third = upsert_contract(conn, _spec(arrival_window_minutes=120))
        assert third.action == "no_op"
        assert third.version == 2
        assert len(_rows(conn, "it-feed")) == 2

    def test_reverting_to_old_content_mints_version_3(self, conn):
        """Rolling back to v1's content is a NEW fact -> version 3; v1 is
        history, never resurrected (append-only, the audit trail keeps the
        full trajectory 90 -> 120 -> 90)."""
        upsert_contract(conn, _spec())
        upsert_contract(conn, _spec(arrival_window_minutes=120))
        revert = upsert_contract(conn, _spec())
        assert revert.action == "created"
        assert revert.version == 3
        assert [r["version"] for r in _rows(conn, "it-feed")] == [1, 2, 3]
        active = get_active_contract(conn, "it-feed")
        assert active is not None
        assert active.version == 3
        assert active.arrival_window_minutes == 90

    def test_get_active_contract_unknown_feed_returns_none(self, conn):
        assert get_active_contract(conn, "never-registered") is None

    def test_retired_feed_returns_none_no_fallback(self, conn):
        """None-honest: zero active rows means disabled/retired — the reader
        never falls back to the latest inactive version."""
        upsert_contract(conn, _spec())
        conn.execute(
            "UPDATE feed_contracts SET active = FALSE, updated_at = now() "
            "WHERE feed_key = %s AND active",
            ("it-feed",),
        )
        assert get_active_contract(conn, "it-feed") is None

    def test_reregistering_retired_feed_mints_next_version(self, conn):
        """After a retire (zero active rows) the same content is re-registered
        as version max+1 — the version counter never rewinds or reuses."""
        upsert_contract(conn, _spec())
        conn.execute(
            "UPDATE feed_contracts SET active = FALSE, updated_at = now() "
            "WHERE feed_key = %s AND active",
            ("it-feed",),
        )
        again = upsert_contract(conn, _spec())
        assert again.action == "created"
        assert again.version == 2
        active = get_active_contract(conn, "it-feed")
        assert active is not None and active.version == 2

    def test_upsert_never_commits_caller_owns_transaction(self, conn):
        """Same convention as ScenarioManager: rollback after an un-committed
        upsert leaves nothing behind."""
        upsert_contract(conn, _spec())
        conn.rollback()
        assert _count(conn) == 0

    def test_none_guards_round_trip_and_noop_detection(self, conn):
        """NULL guards read back as None AND compare as unchanged on reload
        (the no-op diff runs in Python where None == None, unlike SQL
        NULL)."""
        advisory = _spec(
            feed_key="it-advisory",
            criticality="advisory",
            volume_guard_min_rows=None,
            volume_guard_max_pct_delta=None,
        )
        upsert_contract(conn, advisory)
        active = get_active_contract(conn, "it-advisory")
        assert active is not None
        assert active.volume_guard_min_rows is None
        assert active.volume_guard_max_pct_delta is None

        again = upsert_contract(conn, advisory)
        assert again.action == "no_op"

    def test_depends_on_round_trips_as_list(self, conn):
        spec = _spec(feed_key="it-child", depends_on=["it-parent"])
        upsert_contract(conn, spec)
        active = get_active_contract(conn, "it-child")
        assert active is not None
        assert active.depends_on == ["it-parent"]


# ---------------------------------------------------------------------------
# 3. CLI end-to-end — scripts/load_feed_contracts.py (in-process main())
# ---------------------------------------------------------------------------


class TestLoadFeedContractsCli:
    def _all_rows(self, dsn):
        with psycopg.connect(dsn, row_factory=dict_row) as c:
            return c.execute(
                "SELECT feed_key, version, active, criticality "
                "FROM feed_contracts ORDER BY feed_key, version"
            ).fetchall()

    def test_first_run_registers_seeds_then_identical_rerun_noop(
        self, migrated_db
    ):
        """The PR1 acceptance path: first CLI run -> the 3 committed seeds at
        version 1; identical rerun -> full no-op (still 3 rows, all version
        1, no version minted)."""
        rc = load_feed_contracts.main(["--dsn", migrated_db, "--allow-dev"])
        assert rc == 0

        rows = self._all_rows(migrated_db)
        assert [(r["feed_key"], r["version"], r["active"]) for r in rows] == [
            (k, 1, True) for k in SEED_KEYS
        ]
        by_key = {r["feed_key"]: r for r in rows}
        assert by_key["on-hand"]["criticality"] == "blocking"
        assert by_key["open-purchase-orders"]["criticality"] == "blocking"
        assert by_key["open-work-orders"]["criticality"] == "advisory"

        rc2 = load_feed_contracts.main(["--dsn", migrated_db, "--allow-dev"])
        assert rc2 == 0
        assert self._all_rows(migrated_db) == rows

    def test_dry_run_writes_nothing(self, migrated_db):
        rc = load_feed_contracts.main(
            ["--dsn", migrated_db, "--allow-dev", "--dry-run"]
        )
        assert rc == 0
        with psycopg.connect(migrated_db, row_factory=dict_row) as c:
            assert _count(c) == 0


# ---------------------------------------------------------------------------
# 4. Migration 073 idempotence at re-run
# ---------------------------------------------------------------------------


class TestMigration073Idempotent:
    def test_reexecuting_073_sql_is_noop_preserving_rows(
        self, migrated_db, conn
    ):
        """Defensive-idempotence contract (migration 063 header, runner in
        db/connection.py does NOT swallow 'already exists'): re-running the
        file verbatim on an already-migrated DB — twice — must neither raise
        nor touch existing rows/schema. The file carries its own
        BEGIN/COMMIT, so it runs on a fresh autocommit connection (mirrors
        test_reexecuting_070_sql_is_noop)."""
        with psycopg.connect(migrated_db, autocommit=True) as raw:
            _raw_insert(raw, feed_key="idem-feed")

        sql_text = MIGRATION_073.read_text(encoding="utf-8")
        with psycopg.connect(migrated_db, autocommit=True) as raw:
            raw.execute(sql_text)  # 2nd application overall
            raw.execute(sql_text)  # and a 3rd — still a clean no-op

        # Data survived the re-runs.
        rows = _rows(conn, "idem-feed")
        assert len(rows) == 1
        assert rows[0]["active"] is True

        # Schema intact: table + both indexes still present.
        assert conn.execute(
            "SELECT to_regclass('public.feed_contracts') AS t"
        ).fetchone()["t"] is not None
        names = {
            r["indexname"]
            for r in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname='public' "
                "AND tablename='feed_contracts'"
            ).fetchall()
        }
        assert "idx_feed_contracts_feed_key_version" in names
        assert "uq_feed_contracts_active_per_feed" in names

    def test_bootstrap_rerun_is_idempotent(self, migrated_db):
        """A second OotilsDB() on an already-migrated DB (the exact boot
        path) is a no-op — 073 is tracked in schema_migrations and skipped."""
        from ootils_core.db.connection import OotilsDB

        OotilsDB(migrated_db)

        with psycopg.connect(migrated_db, row_factory=dict_row) as c:
            assert c.execute(
                "SELECT to_regclass('public.feed_contracts') AS t"
            ).fetchone()["t"] is not None
            # schema_migrations.version holds the migration FILENAME
            # (db/connection.py:_apply_migrations). No params passed, so
            # psycopg leaves the % wildcard verbatim.
            applied = c.execute(
                "SELECT COUNT(*) AS n FROM schema_migrations "
                "WHERE version LIKE '073%'"
            ).fetchone()["n"]
            assert applied == 1
