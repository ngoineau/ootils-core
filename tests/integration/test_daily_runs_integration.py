"""
tests/integration/test_daily_runs_integration.py — daily-run guard evaluations
(ADR-042 PR-2, absorbing ADR-037's INT-1 PR2) against a real PostgreSQL — no
mocks (CLAUDE.md). The pure guard logic lives in tests/test_daily_run_guards.py.

Covers the DB half of src/ootils_core/interfaces/daily_run.py + migration 078:

  1. Schema guarantees of migration 078: exact daily_runs column set (zero
     JSONB, no scenario_id — baseline-by-nature per the migration header),
     the FK to feed_contracts confirmed ON DELETE RESTRICT and enforced in
     both directions (unknown contract refused; deleting a referenced
     contract refused), every CHECK (per-guard status vocabulary, the
     deliberately narrower overall_status, criticality, non-negative
     counts), NULL counts accepted (None-honest: a missing file is not a
     zero-row file), and — the cadrage's tranché — NO UNIQUE(feed_key,
     run_date): the table is an append-only audit trail (one row per
     evaluation ATTEMPT, same philosophy as calc_runs). Plus the bundled
     recommendations.exported_at column + its partial pending-export index
     (ADR-042 decision 4, schema-only in this PR).
  2. record_daily_run — the sole writer: persists ONE complete row whose
     per-guard verdict columns mirror the returned FeedGuardEvaluation
     exactly; runs against the ACTIVE contract version; never commits
     (caller owns the transaction).
  3. The prior-day baseline lookup (plan_daily_run_guard_check): the
     volume-delta baseline is the latest STRICTLY-PRIOR-day row (a same-day
     re-evaluation never becomes its own baseline), the latest attempt of
     that prior day wins (observed_at tiebreak), and a prior day whose file
     never arrived (row_count NULL) yields a None-honest NOT_EVALUATED
     delta, never a fabricated comparison.
  4. Re-run on the same run_date = append-only: a second attempt INSERTs a
     second row (no unique violation, distinct daily_run_id), and the
     "current" verdict is simply the most recent row by observed_at — the
     documented re-evaluated-once-the-file-lands lifecycle.
  5. A FAILED verdict on a blocking feed reads correctly through the
     headline "what failed today" query (run_date + overall_status), with
     criticality frozen at evaluation time on the row itself.
  6. Migration 078 idempotence: re-executing the file verbatim on an
     already-migrated DB (rows present) is a clean no-op preserving data and
     schema; a second OotilsDB() bootstrap likewise (defensive-idempotence
     contract, canonical pattern of migration 063's header).

ISOLATION (the committed-seed lesson, cf. test_purge_integration.py's
_cleanup_baseline_seed): the only test that COMMITS is the migration-078
idempotence rerun (autocommit connection, required so the re-executed SQL
file sees the rows). Its residue is neutralized by a request.addfinalizer
registered BEFORE the commit — plain child-first DELETEs on the test's own
uniquely-named feed_key (daily_runs first, then feed_contracts), which can
violate nothing (daily_runs has no children); NEVER a delete-cascade. Belt
and braces: the autouse pre-test TRUNCATE (daily_runs, feed_contracts in ONE
statement — 078's FK means feed_contracts can no longer be truncated alone)
sweeps any leftovers, and the module teardown drops all public tables.
Every other test stays on the rollback-teardown ``conn`` fixture and commits
nothing.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg import errors
from psycopg.rows import dict_row

from ootils_core.interfaces.contracts import (
    FeedContract,
    FeedContractSpec,
    get_active_contract,
    upsert_contract,
)
from ootils_core.interfaces.daily_run import (
    DailyRunGuardError,
    DailyRunObservation,
    plan_daily_run_guard_check,
    record_daily_run,
)
from ootils_core.interfaces.guards import GuardStatus

from .conftest import requires_db

pytestmark = requires_db

_REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_078 = (
    _REPO_ROOT / "src" / "ootils_core" / "db" / "migrations" / "078_daily_runs.sql"
)

# Shared timeline: cadence "0 6 * * *" (daily 06:00 UTC) + a 90-minute window
# puts the arrival deadline at 07:30 UTC on each run date.
D1 = date(2026, 7, 13)
D2 = date(2026, 7, 14)

DAILY_RUNS_COLUMNS = {
    "daily_run_id", "feed_contract_id", "feed_key", "run_date", "observed_at",
    "file_arrived_at", "row_count", "previous_row_count", "deleted_count",
    "criticality", "arrival_status", "volume_floor_status",
    "volume_delta_status", "deletion_ratio_status", "overall_status",
    "created_at",
}


def _utc(d: date, hour: int, minute: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hour, minute, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_daily_runs(migrated_db):
    """Pre-test TRUNCATE on its own autocommit connection. daily_runs
    references feed_contracts (078's FK), so Postgres refuses to truncate
    feed_contracts alone even when daily_runs is empty — both go in ONE
    statement, child listed with its parent. Pre-test only — a post-yield
    TRUNCATE could block on the function-scoped ``conn`` fixture's still-open
    transaction; committed leftovers are swept by the NEXT test's pre-clean,
    the idempotence test's own finalizer, and the module teardown's DROP of
    all public tables (same convention as the feed-contracts sibling)."""
    with psycopg.connect(migrated_db, autocommit=True) as c:
        c.execute("TRUNCATE daily_runs, feed_contracts")
    yield


def _spec(**over) -> FeedContractSpec:
    base = {
        "feed_key": "dr-feed",
        "entity_type": "on_hand",
        "source_system": "WMS-DR",
        "format": "tsv",
        "key_columns": ["item_external_id", "location_external_id"],
        "mandatory_columns": [
            "item_external_id", "location_external_id", "quantity",
        ],
        "load_mode": "full",
        "cadence": "0 6 * * *",
        "arrival_window_minutes": 90,
        "owner": "dr-ops",
        "criticality": "blocking",
        "volume_guard_min_rows": 100,
        "volume_guard_max_pct_delta": Decimal("0.20"),
        "depends_on": [],
    }
    base.update(over)
    return FeedContractSpec.model_validate(base)


def _register(conn, **over) -> FeedContract:
    """Register a contract through the REAL loader path (upsert_contract) and
    return the active row — never a hand-rolled INSERT for the happy paths."""
    spec = _spec(**over)
    upsert_contract(conn, spec)
    active = get_active_contract(conn, spec.feed_key)
    assert active is not None
    return active


def _daily_rows(conn, feed_key: str) -> list[dict]:
    return conn.execute(
        "SELECT * FROM daily_runs WHERE feed_key = %s "
        "ORDER BY run_date, observed_at",
        (feed_key,),
    ).fetchall()


def _raw_daily_insert(conn, feed_contract_id: UUID, **over) -> None:
    """Direct INSERT bypassing the Python layer — used to probe the DB's own
    CHECK/FK defenses (mirrors the sibling's _raw_insert)."""
    row = {
        "feed_contract_id": feed_contract_id,
        "feed_key": "raw-dr-feed",
        "run_date": D1,
        "observed_at": _utc(D1, 8),
        "file_arrived_at": _utc(D1, 6, 30),
        "row_count": 150,
        "previous_row_count": None,
        "deleted_count": None,
        "criticality": "blocking",
        "arrival_status": "ok",
        "volume_floor_status": "ok",
        "volume_delta_status": "not_evaluated",
        "deletion_ratio_status": "not_evaluated",
        "overall_status": "ok",
    }
    row.update(over)
    conn.execute(
        """
        INSERT INTO daily_runs (
            feed_contract_id, feed_key, run_date, observed_at,
            file_arrived_at, row_count, previous_row_count, deleted_count,
            criticality, arrival_status, volume_floor_status,
            volume_delta_status, deletion_ratio_status, overall_status
        ) VALUES (
            %(feed_contract_id)s, %(feed_key)s, %(run_date)s, %(observed_at)s,
            %(file_arrived_at)s, %(row_count)s, %(previous_row_count)s,
            %(deleted_count)s, %(criticality)s, %(arrival_status)s,
            %(volume_floor_status)s, %(volume_delta_status)s,
            %(deletion_ratio_status)s, %(overall_status)s
        )
        """,
        row,
    )


# ---------------------------------------------------------------------------
# 1. Migration 078 — schema guarantees
# ---------------------------------------------------------------------------


class TestMigration078Schema:
    def test_daily_runs_columns_exact_and_zero_jsonb(self, conn):
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'daily_runs'"
        ).fetchall()
        columns = {r["column_name"] for r in rows}
        assert columns == DAILY_RUNS_COLUMNS
        # Baseline-by-nature (migration header, ADR-030 rationale): an
        # observed ERP feed evaluation is a fact, never a fork's state.
        assert "scenario_id" not in columns
        # Every column typed and business-queryable — the JSONB carve-out
        # does not apply here.
        jsonb = [r["column_name"] for r in rows if r["data_type"] == "jsonb"]
        assert jsonb == []

    def test_indexes_present(self, conn):
        rows = conn.execute(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = 'daily_runs'"
        ).fetchall()
        by_name = {r["indexname"]: r["indexdef"] for r in rows}
        assert "idx_daily_runs_feed_key_run_date" in by_name
        assert "idx_daily_runs_run_date_overall_status" in by_name

    def test_no_unique_on_feed_key_run_date_append_only(self, conn):
        """The cadrage's tranché, enforced structurally: NO UNIQUE(feed_key,
        run_date) — a (feed_key, run_date) can accumulate several evaluation
        ATTEMPTS (append-only audit trail, migration header). The only
        unique index on the table is the PK."""
        unique_defs = [
            r["indexname"]
            for r in conn.execute(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = 'public' AND tablename = 'daily_runs' "
                "AND indexdef LIKE '%UNIQUE%'"
            ).fetchall()
        ]
        assert unique_defs == ["daily_runs_pkey"]

    def test_fk_to_feed_contracts_is_on_delete_restrict(self, conn):
        """EXPLICIT confdeltype='r' (RESTRICT), not the Postgres-default
        NO ACTION — the FK convention the repo enforces (cf. ADR-030's
        scenarios-FK rule, same discipline here)."""
        fks = conn.execute(
            "SELECT confrelid::regclass::text AS target, confdeltype "
            "FROM pg_constraint "
            "WHERE contype = 'f' AND conrelid = 'daily_runs'::regclass"
        ).fetchall()
        assert len(fks) == 1
        assert fks[0]["target"] == "feed_contracts"
        assert fks[0]["confdeltype"] == "r"

    def test_fk_rejects_unknown_contract(self, conn):
        with pytest.raises(errors.ForeignKeyViolation):
            _raw_daily_insert(conn, uuid4())
        conn.rollback()

    def test_fk_restrict_blocks_referenced_contract_delete(self, conn):
        """A real contract inserted via the REAL loader, referenced by a
        daily_runs row, cannot be hard-deleted — the RESTRICT safety net
        (feed_contracts is append-only per version and never hard-deleted in
        normal operation; this proves the net actually catches)."""
        contract = _register(conn, feed_key="dr-restrict")
        _raw_daily_insert(
            conn, contract.feed_contract_id, feed_key="dr-restrict"
        )
        with pytest.raises(errors.ForeignKeyViolation):
            conn.execute(
                "DELETE FROM feed_contracts WHERE feed_contract_id = %s",
                (contract.feed_contract_id,),
            )
        conn.rollback()

    @pytest.mark.parametrize(
        "column",
        [
            "arrival_status", "volume_floor_status",
            "volume_delta_status", "deletion_ratio_status",
        ],
    )
    def test_per_guard_status_checks_reject_unknown_value(self, conn, column):
        contract = _register(conn)
        with pytest.raises(errors.CheckViolation):
            _raw_daily_insert(
                conn, contract.feed_contract_id,
                feed_key="dr-feed", **{column: "weird"},
            )
        conn.rollback()

    def test_per_guard_columns_accept_not_evaluated(self, conn):
        """'not_evaluated' is a legal per-guard verdict (the None-honest
        state), while overall_status stays a real ok/failed call."""
        contract = _register(conn)
        _raw_daily_insert(
            conn, contract.feed_contract_id, feed_key="dr-feed",
            file_arrived_at=None, row_count=None,
            arrival_status="not_evaluated",
            volume_floor_status="not_evaluated",
            volume_delta_status="not_evaluated",
            deletion_ratio_status="not_evaluated",
            overall_status="ok",
        )
        assert len(_daily_rows(conn, "dr-feed")) == 1

    def test_overall_status_check_rejects_not_evaluated(self, conn):
        """overall_status's CHECK is deliberately NARROWER than the per-guard
        vocabulary: FeedGuardEvaluation.overall_status never returns
        NOT_EVALUATED, and the DB refuses it too."""
        contract = _register(conn)
        with pytest.raises(errors.CheckViolation):
            _raw_daily_insert(
                conn, contract.feed_contract_id,
                feed_key="dr-feed", overall_status="not_evaluated",
            )
        conn.rollback()

    def test_criticality_check_rejects_unknown_value(self, conn):
        contract = _register(conn)
        with pytest.raises(errors.CheckViolation):
            _raw_daily_insert(
                conn, contract.feed_contract_id,
                feed_key="dr-feed", criticality="critical",
            )
        conn.rollback()

    @pytest.mark.parametrize(
        "column", ["row_count", "previous_row_count", "deleted_count"]
    )
    def test_negative_counts_rejected(self, conn, column):
        contract = _register(conn)
        with pytest.raises(errors.CheckViolation):
            _raw_daily_insert(
                conn, contract.feed_contract_id,
                feed_key="dr-feed", **{column: -1},
            )
        conn.rollback()

    def test_null_counts_accepted_none_honest(self, conn):
        """NULL row_count is legal and distinct from 0: a file that never
        arrived is not an empty file (migration header)."""
        contract = _register(conn)
        _raw_daily_insert(
            conn, contract.feed_contract_id, feed_key="dr-feed",
            file_arrived_at=None, row_count=None,
            arrival_status="failed", volume_floor_status="not_evaluated",
            overall_status="failed",
        )
        row = _daily_rows(conn, "dr-feed")[0]
        assert row["row_count"] is None
        assert row["file_arrived_at"] is None

    def test_recommendations_exported_at_column_present(self, conn):
        """ADR-042 decision 4, bundled into 078: nullable TIMESTAMPTZ,
        schema-only in this PR (nothing stamps it until PR-5)."""
        row = conn.execute(
            "SELECT data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'recommendations' "
            "AND column_name = 'exported_at'"
        ).fetchone()
        assert row is not None
        assert row["data_type"] == "timestamp with time zone"
        assert row["is_nullable"] == "YES"

    def test_reco_pending_export_index_is_partial_on_null(self, conn):
        indexdef = conn.execute(
            "SELECT indexdef FROM pg_indexes "
            "WHERE schemaname = 'public' AND indexname = 'ix_reco_pending_export'"
        ).fetchone()
        assert indexdef is not None
        assert "WHERE (exported_at IS NULL)" in indexdef["indexdef"]


# ---------------------------------------------------------------------------
# 2. record_daily_run — the sole writer
# ---------------------------------------------------------------------------


class TestRecordDailyRun:
    def test_green_run_persists_complete_row(self, conn):
        """First evaluation ever: arrival + floor evaluated (green), delta +
        deletion honestly NOT_EVALUATED (no baseline yet). The persisted row
        mirrors the returned FeedGuardEvaluation column for column."""
        contract = _register(conn)
        observed_at = _utc(D1, 8)
        record = record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=150),
            now=observed_at,
        )

        assert isinstance(record.daily_run_id, UUID)
        assert record.feed_key == "dr-feed"
        assert record.run_date == D1
        ev = record.evaluation
        assert ev.criticality == "blocking"
        assert ev.by_name("arrival_window").status == GuardStatus.OK
        assert ev.by_name("volume_floor").status == GuardStatus.OK
        assert ev.by_name("volume_delta").status == GuardStatus.NOT_EVALUATED
        assert ev.by_name("deletion_ratio").status == GuardStatus.NOT_EVALUATED
        assert ev.overall_status == GuardStatus.OK

        rows = _daily_rows(conn, "dr-feed")
        assert len(rows) == 1
        row = rows[0]
        assert row["daily_run_id"] == record.daily_run_id
        assert row["feed_contract_id"] == contract.feed_contract_id
        assert row["feed_key"] == "dr-feed"
        assert row["run_date"] == D1
        assert row["observed_at"] == observed_at
        assert row["file_arrived_at"] == _utc(D1, 6, 30)
        assert row["row_count"] == 150
        assert row["previous_row_count"] is None
        assert row["deleted_count"] is None
        assert row["criticality"] == "blocking"
        assert row["arrival_status"] == "ok"
        assert row["volume_floor_status"] == "ok"
        assert row["volume_delta_status"] == "not_evaluated"
        assert row["deletion_ratio_status"] == "not_evaluated"
        assert row["overall_status"] == "ok"

    def test_evaluation_runs_against_active_contract_version(self, conn):
        """v2 supersedes v1 -> the recorded feed_contract_id is v2's (the
        version actually in effect), never the retired v1's."""
        v1 = _register(conn)
        upsert_contract(conn, _spec(arrival_window_minutes=120))
        v2 = get_active_contract(conn, "dr-feed")
        assert v2 is not None and v2.version == 2

        record = record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=150),
            now=_utc(D1, 8),
        )
        row = _daily_rows(conn, "dr-feed")[0]
        assert row["feed_contract_id"] == v2.feed_contract_id
        assert row["feed_contract_id"] != v1.feed_contract_id
        assert record.evaluation.overall_status == GuardStatus.OK

    def test_day_two_uses_prior_day_baseline_for_volume_delta(self, conn):
        """Day 1: 150 rows. Day 2: 100 rows -> a 33% swing over the 20%
        tolerance: volume_delta FAILED, previous_row_count frozen on the row
        (self-contained audit record), overall FAILED."""
        _register(conn)
        record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=150),
            now=_utc(D1, 8),
        )
        record = record_daily_run(
            conn, "dr-feed", D2,
            DailyRunObservation(file_arrived_at=_utc(D2, 6, 30), row_count=100),
            now=_utc(D2, 8),
        )

        assert record.evaluation.by_name("volume_delta").status == GuardStatus.FAILED
        assert record.evaluation.overall_status == GuardStatus.FAILED
        day2_row = _daily_rows(conn, "dr-feed")[1]
        assert day2_row["previous_row_count"] == 150
        assert day2_row["volume_delta_status"] == "failed"
        assert day2_row["overall_status"] == "failed"

    def test_prior_day_latest_attempt_is_the_baseline(self, conn):
        """Two attempts on day 1 (100 rows @08:00, then 200 rows @12:00 after
        a corrected re-drop): day 2's baseline is the LATEST prior-day
        attempt (ORDER BY run_date DESC, observed_at DESC)."""
        _register(conn)
        record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=100),
            now=_utc(D1, 8),
        )
        record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=200),
            now=_utc(D1, 12),
        )

        plan = plan_daily_run_guard_check(conn, "dr-feed", D2)
        assert plan.previous_row_count == 200

        record = record_daily_run(
            conn, "dr-feed", D2,
            DailyRunObservation(file_arrived_at=_utc(D2, 6, 30), row_count=200),
            now=_utc(D2, 8),
        )
        # 200 -> 200: 0% swing vs the latest attempt — OK. (Against the
        # 08:00 attempt's 100 it would be a 100% swing and FAILED.)
        assert record.evaluation.by_name("volume_delta").status == GuardStatus.OK

    def test_same_day_attempt_never_its_own_baseline(self, conn):
        """Day 1: 100 rows. Day 2, attempt #1: 500 rows (400% swing, FAILED).
        Day 2, attempt #2 (same 500 rows): the baseline is STILL day 1's 100
        — never the same-day attempt #1 (which would make the swing 0% and
        silently launder the anomaly into an OK)."""
        _register(conn)
        record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=100),
            now=_utc(D1, 8),
        )
        first = record_daily_run(
            conn, "dr-feed", D2,
            DailyRunObservation(file_arrived_at=_utc(D2, 6, 30), row_count=500),
            now=_utc(D2, 8),
        )
        second = record_daily_run(
            conn, "dr-feed", D2,
            DailyRunObservation(file_arrived_at=_utc(D2, 6, 30), row_count=500),
            now=_utc(D2, 9),
        )

        assert first.evaluation.by_name("volume_delta").status == GuardStatus.FAILED
        assert second.evaluation.by_name("volume_delta").status == GuardStatus.FAILED
        rows = _daily_rows(conn, "dr-feed")
        assert rows[1]["previous_row_count"] == 100
        assert rows[2]["previous_row_count"] == 100  # not attempt #1's 500

    def test_prior_day_missing_file_yields_none_honest_delta(self, conn):
        """Day 1's file never arrived (row_count NULL): day 2 has no honest
        baseline -> volume_delta NOT_EVALUATED, never a fabricated
        comparison against NULL-coerced garbage."""
        _register(conn)
        d1 = record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=None, row_count=None),
            now=_utc(D1, 9),  # past the 07:30 deadline
        )
        assert d1.evaluation.by_name("arrival_window").status == GuardStatus.FAILED
        assert d1.evaluation.overall_status == GuardStatus.FAILED

        d2 = record_daily_run(
            conn, "dr-feed", D2,
            DailyRunObservation(file_arrived_at=_utc(D2, 6, 30), row_count=150),
            now=_utc(D2, 8),
        )
        assert d2.evaluation.by_name("volume_delta").status == GuardStatus.NOT_EVALUATED
        assert _daily_rows(conn, "dr-feed")[1]["previous_row_count"] is None

    def test_deletion_ratio_observation_flows_through(self, conn):
        """Caller-supplied deletion observation (the file-diff service is
        PR-3/4): 30 of 100 previously-active rows gone = 30% > the repo-wide
        20% threshold -> deletion_ratio FAILED. Only the numerator
        (deleted_count) is persisted — 078 has no previous_active_count
        column; the denominator lives in the guard's detail message."""
        _register(conn)
        record = record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(
                file_arrived_at=_utc(D1, 6, 30), row_count=150,
                deleted_count=30, previous_active_count=100,
            ),
            now=_utc(D1, 8),
        )
        assert record.evaluation.by_name("deletion_ratio").status == GuardStatus.FAILED
        assert record.evaluation.overall_status == GuardStatus.FAILED
        row = _daily_rows(conn, "dr-feed")[0]
        assert row["deleted_count"] == 30
        assert row["deletion_ratio_status"] == "failed"

    def test_unknown_feed_raises_daily_run_guard_error(self, conn):
        with pytest.raises(DailyRunGuardError, match="never-registered"):
            plan_daily_run_guard_check(conn, "never-registered", D1)
        with pytest.raises(DailyRunGuardError, match="never-registered"):
            record_daily_run(
                conn, "never-registered", D1,
                DailyRunObservation(file_arrived_at=None, row_count=None),
                now=_utc(D1, 8),
            )

    def test_retired_feed_raises_fresh_data_defense(self, conn):
        """record_daily_run re-plans on FRESH data: a feed retired AFTER
        registration (zero active rows) is refused at write time — never
        evaluated against a stale notion of the contract."""
        _register(conn)
        conn.execute(
            "UPDATE feed_contracts SET active = FALSE, updated_at = now() "
            "WHERE feed_key = %s AND active",
            ("dr-feed",),
        )
        with pytest.raises(DailyRunGuardError, match="dr-feed"):
            record_daily_run(
                conn, "dr-feed", D1,
                DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=150),
                now=_utc(D1, 8),
            )

    def test_record_never_commits_caller_owns_transaction(self, conn):
        """Same convention as upsert_contract/ScenarioManager: rollback after
        an un-committed record leaves nothing behind."""
        _register(conn)
        record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=150),
            now=_utc(D1, 8),
        )
        conn.rollback()
        n = conn.execute("SELECT COUNT(*) AS n FROM daily_runs").fetchone()["n"]
        assert n == 0


# ---------------------------------------------------------------------------
# 3. Re-run on the same run_date — append-only, tranché by the cadrage
# ---------------------------------------------------------------------------


class TestSameDateRerunAppendOnly:
    def test_rerun_same_date_appends_second_row(self, conn):
        """The documented lifecycle: checked mid-window (nothing arrived yet,
        NOT_EVALUATED, overall ok) then re-evaluated once the file lands
        (arrival ok). TWO honest rows — no unique violation, no overwrite,
        distinct daily_run_ids — and the 'current' verdict is simply the
        most recent row by observed_at."""
        _register(conn)
        early = record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=None, row_count=None),
            now=_utc(D1, 7),  # 07:00 < the 07:30 deadline
        )
        late = record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=_utc(D1, 6, 50), row_count=150),
            now=_utc(D1, 9),
        )

        assert early.daily_run_id != late.daily_run_id
        assert (
            early.evaluation.by_name("arrival_window").status
            == GuardStatus.NOT_EVALUATED
        )
        assert early.evaluation.overall_status == GuardStatus.OK
        assert late.evaluation.by_name("arrival_window").status == GuardStatus.OK

        rows = _daily_rows(conn, "dr-feed")
        assert len(rows) == 2
        assert all(r["run_date"] == D1 for r in rows)

        current = conn.execute(
            "SELECT daily_run_id FROM daily_runs "
            "WHERE feed_key = %s AND run_date = %s "
            "ORDER BY observed_at DESC LIMIT 1",
            ("dr-feed", D1),
        ).fetchone()
        assert current["daily_run_id"] == late.daily_run_id

    def test_identical_rerun_same_date_also_appends(self, conn):
        """Even a byte-identical re-evaluation is a new attempt row (attempt
        semantics, NOT idempotent-upsert semantics — the deliberate contrast
        with the deterministic-uuid5 recommendation tables)."""
        _register(conn)
        obs = DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=150)
        a = record_daily_run(conn, "dr-feed", D1, obs, now=_utc(D1, 8))
        b = record_daily_run(conn, "dr-feed", D1, obs, now=_utc(D1, 8))
        assert a.daily_run_id != b.daily_run_id
        assert len(_daily_rows(conn, "dr-feed")) == 2
        # Same inputs -> same VERDICT (deterministic core), distinct rows.
        assert a.evaluation == b.evaluation


# ---------------------------------------------------------------------------
# 4. A FAILED verdict on a blocking feed reads correctly
# ---------------------------------------------------------------------------


class TestBlockingFeedFailureReads:
    def test_blocking_feed_floor_breach_reads_through_headline_query(self, conn):
        """The 'extraction partielle silencieuse' case on a BLOCKING feed:
        file on time but 40 rows under the 100-row floor. The headline
        'what failed today' query (the run_date + overall_status index)
        surfaces the row with criticality frozen at evaluation time — the
        exact tuple PR-3's escalate-vs-advisory decision will consume."""
        _register(conn)  # criticality='blocking'
        record = record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=40),
            now=_utc(D1, 8),
        )
        assert record.evaluation.by_name("volume_floor").status == GuardStatus.FAILED
        assert record.evaluation.criticality == "blocking"
        # The detail names observed value + threshold (exploitable evidence).
        detail = record.evaluation.by_name("volume_floor").detail
        assert "40" in detail and "100" in detail

        failed_today = conn.execute(
            "SELECT feed_key, criticality, arrival_status, volume_floor_status, "
            "       overall_status "
            "FROM daily_runs WHERE run_date = %s AND overall_status = 'failed'",
            (D1,),
        ).fetchall()
        assert len(failed_today) == 1
        row = failed_today[0]
        assert row["feed_key"] == "dr-feed"
        assert row["criticality"] == "blocking"
        assert row["arrival_status"] == "ok"        # arrival was fine
        assert row["volume_floor_status"] == "failed"  # the actual breach
        assert row["overall_status"] == "failed"

    def test_advisory_feed_failure_reads_as_advisory(self, conn):
        """Same failure on an ADVISORY feed: overall_status is just as
        'failed' (the guard verdict does not soften), but the frozen
        criticality lets PR-3 route it to the report instead of the L3
        webhook — both columns must read back exactly."""
        _register(
            conn, feed_key="dr-advisory", criticality="advisory",
            volume_guard_min_rows=100,
        )
        record = record_daily_run(
            conn, "dr-advisory", D1,
            DailyRunObservation(file_arrived_at=_utc(D1, 6, 30), row_count=40),
            now=_utc(D1, 8),
        )
        assert record.evaluation.overall_status == GuardStatus.FAILED
        row = _daily_rows(conn, "dr-advisory")[0]
        assert row["criticality"] == "advisory"
        assert row["overall_status"] == "failed"

    def test_blocking_feed_missing_file_reads_failed_arrival(self, conn):
        """The 'flux totalement absent' case: no file by the deadline on a
        blocking feed — arrival FAILED, volume guards honestly
        NOT_EVALUATED, overall FAILED, all readable from the row."""
        _register(conn)
        record_daily_run(
            conn, "dr-feed", D1,
            DailyRunObservation(file_arrived_at=None, row_count=None),
            now=_utc(D1, 9),
        )
        row = _daily_rows(conn, "dr-feed")[0]
        assert row["arrival_status"] == "failed"
        assert row["volume_floor_status"] == "not_evaluated"
        assert row["volume_delta_status"] == "not_evaluated"
        assert row["deletion_ratio_status"] == "not_evaluated"
        assert row["overall_status"] == "failed"
        assert row["criticality"] == "blocking"
        assert row["file_arrived_at"] is None
        assert row["row_count"] is None


# ---------------------------------------------------------------------------
# 5. Migration 078 idempotence at re-run
# ---------------------------------------------------------------------------


class TestMigration078Idempotent:
    def test_reexecuting_078_sql_is_noop_preserving_rows(
        self, migrated_db, conn, request
    ):
        """Defensive-idempotence contract (migration 063 header, runner in
        db/connection.py does NOT swallow 'already exists'): re-running the
        file verbatim on an already-migrated DB — twice — must neither raise
        nor touch existing rows/schema. The file carries its own
        BEGIN/COMMIT, so it runs on a fresh autocommit connection (mirrors
        test_reexecuting_073_sql_is_noop).

        This test COMMITS (the re-executed SQL must see the rows), so the
        residue is neutralized by a finalizer registered BEFORE the commit:
        child-first DELETEs on this test's own feed_key — safe by
        construction (daily_runs has no children), never a cascade."""

        def _sweep():
            with psycopg.connect(migrated_db, autocommit=True) as c:
                c.execute(
                    "DELETE FROM daily_runs WHERE feed_key = %s", ("idem-dr",)
                )
                c.execute(
                    "DELETE FROM feed_contracts WHERE feed_key = %s", ("idem-dr",)
                )

        request.addfinalizer(_sweep)

        with psycopg.connect(
            migrated_db, autocommit=True, row_factory=dict_row
        ) as raw:
            contract = _register(raw, feed_key="idem-dr")
            _raw_daily_insert(
                raw, contract.feed_contract_id, feed_key="idem-dr"
            )

        sql_text = MIGRATION_078.read_text(encoding="utf-8")
        with psycopg.connect(migrated_db, autocommit=True) as raw:
            raw.execute(sql_text)  # 2nd application overall
            raw.execute(sql_text)  # and a 3rd — still a clean no-op

        # Data survived the re-runs.
        rows = _daily_rows(conn, "idem-dr")
        assert len(rows) == 1
        assert rows[0]["overall_status"] == "ok"

        # Schema intact: table, both indexes, the exported_at column + its
        # partial index.
        assert conn.execute(
            "SELECT to_regclass('public.daily_runs') AS t"
        ).fetchone()["t"] is not None
        names = {
            r["indexname"]
            for r in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname='public' "
                "AND tablename IN ('daily_runs', 'recommendations')"
            ).fetchall()
        }
        assert "idx_daily_runs_feed_key_run_date" in names
        assert "idx_daily_runs_run_date_overall_status" in names
        assert "ix_reco_pending_export" in names
        n_exported_at = conn.execute(
            "SELECT COUNT(*) AS n FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='recommendations' "
            "AND column_name='exported_at'"
        ).fetchone()["n"]
        assert n_exported_at == 1  # ADD COLUMN IF NOT EXISTS did not duplicate

    def test_bootstrap_rerun_is_idempotent(self, migrated_db):
        """A second OotilsDB() on an already-migrated DB (the exact boot
        path) is a no-op — 078 is tracked in schema_migrations and skipped."""
        from ootils_core.db.connection import OotilsDB

        OotilsDB(migrated_db)

        with psycopg.connect(migrated_db, row_factory=dict_row) as c:
            assert c.execute(
                "SELECT to_regclass('public.daily_runs') AS t"
            ).fetchone()["t"] is not None
            applied = c.execute(
                "SELECT COUNT(*) AS n FROM schema_migrations "
                "WHERE version LIKE '078%'"
            ).fetchone()["n"]
            assert applied == 1
