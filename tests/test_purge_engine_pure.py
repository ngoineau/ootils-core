"""
test_purge_engine_pure.py — DB-free unit tests for the PURGE-1 engine's pure
surface (``engine/maintenance/purge.py``): the absolute guard checker, the
fail-fast input validation that runs BEFORE any DB access, the plan/candidate
sum properties, and the per-table SQL composition invariants.

The DB-touching lifecycle (plan/apply against real Postgres) lives in
``tests/integration/test_purge_integration.py``; the schema-completeness
guard (every scenario-scoped table whitelisted or exempted) in
``tests/test_purge_whitelist_guard.py``.
"""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pytest

from ootils_core.engine.maintenance import (
    PURGE_WHITELIST,
    PurgeCandidate,
    PurgeGuardError,
    PurgePlan,
    ShortageRetentionCandidate,
    ShortageRetentionPlan,
    apply_fork_purge,
    apply_shortage_retention,
    plan_fork_purge,
    plan_shortage_retention,
)
from ootils_core.engine.maintenance.purge import (
    _TABLE_QUERIES,
    _verify_purge_guards,
)

NOW = dt.datetime(2026, 7, 12, 12, 0, 0, tzinfo=dt.timezone.utc)
SCENARIO = uuid4()


def _guards(**overrides):
    """Call _verify_purge_guards with a fully-eligible default (archived 30
    days ago, non-baseline, TTL 7) plus per-test overrides."""
    kwargs = dict(
        scenario_id=SCENARIO,
        status="archived",
        is_baseline=False,
        archived_at=NOW - dt.timedelta(days=30),
        ttl_days=7,
        now=NOW,
    )
    kwargs.update(overrides)
    return _verify_purge_guards(**kwargs)


# ---------------------------------------------------------------------------
# _verify_purge_guards — the pure absolute-guard checker
# ---------------------------------------------------------------------------


class TestVerifyPurgeGuards:
    def test_fully_eligible_passes(self):
        assert _guards() is None  # no exception

    def test_baseline_refused_first_regardless_of_other_fields(self):
        """is_baseline wins even when every other guard would also fail —
        the baseline can never be purged, whatever its row looks like."""
        with pytest.raises(PurgeGuardError, match="baseline"):
            _guards(is_baseline=True, status="active", archived_at=None)

    @pytest.mark.parametrize("status", ["active", "merged", "draft", ""])
    def test_non_archived_status_refused(self, status):
        with pytest.raises(PurgeGuardError, match="archived"):
            _guards(status=status)

    def test_archived_at_none_refused(self):
        with pytest.raises(PurgeGuardError, match="archived_at is NULL"):
            _guards(archived_at=None)

    def test_archived_inside_ttl_window_refused(self):
        with pytest.raises(PurgeGuardError, match="TTL"):
            _guards(archived_at=NOW - dt.timedelta(days=6), ttl_days=7)

    def test_archived_exactly_at_cutoff_refused(self):
        """archived_at == cutoff is INSIDE the window (strict <): a fork
        archived exactly ttl_days ago has not yet cleared its TTL."""
        with pytest.raises(PurgeGuardError, match="TTL"):
            _guards(archived_at=NOW - dt.timedelta(days=7), ttl_days=7)

    def test_archived_just_past_cutoff_passes(self):
        assert _guards(
            archived_at=NOW - dt.timedelta(days=7, seconds=1), ttl_days=7
        ) is None

    def test_ttl_zero_means_any_past_archive_is_eligible(self):
        assert _guards(archived_at=NOW - dt.timedelta(seconds=1), ttl_days=0) is None


# ---------------------------------------------------------------------------
# Fail-fast input validation — raises BEFORE any DB access (conn=None proves it)
# ---------------------------------------------------------------------------


def _plan(candidates=()):
    return PurgePlan(ttl_days=7, generated_at=NOW, candidates=tuple(candidates))


def _retention_plan(candidates=()):
    return ShortageRetentionPlan(
        retention_days=30, generated_at=NOW, candidates=tuple(candidates)
    )


class TestFailFastValidation:
    def test_plan_fork_purge_rejects_negative_ttl_before_touching_db(self):
        with pytest.raises(ValueError, match="ttl_days"):
            plan_fork_purge(None, ttl_days=-1)  # conn never touched

    def test_plan_shortage_retention_rejects_negative_retention(self):
        with pytest.raises(ValueError, match="retention_days"):
            plan_shortage_retention(None, retention_days=-1)

    @pytest.mark.parametrize("executed_by", ["", "   "])
    def test_apply_fork_purge_requires_executed_by(self, executed_by):
        with pytest.raises(ValueError, match="executed_by"):
            apply_fork_purge(None, _plan(), executed_by=executed_by)

    @pytest.mark.parametrize("executed_by", ["", "   "])
    def test_apply_shortage_retention_requires_executed_by(self, executed_by):
        with pytest.raises(ValueError, match="executed_by"):
            apply_shortage_retention(None, _retention_plan(), executed_by=executed_by)


# ---------------------------------------------------------------------------
# Sum properties on the frozen plan dataclasses
# ---------------------------------------------------------------------------


class TestPlanSums:
    def test_candidate_rows_total_sums_per_table_counts(self):
        candidate = PurgeCandidate(
            scenario_id=SCENARIO,
            name="c",
            archived_at=NOW,
            per_table_counts={"nodes": 3, "shortages": 2, "ghost_members": 1},
        )
        assert candidate.rows_total == 6

    def test_purge_plan_rows_total_sums_candidates(self):
        c1 = PurgeCandidate(SCENARIO, "a", NOW, {"nodes": 1})
        c2 = PurgeCandidate(uuid4(), "b", NOW, {"nodes": 2, "events": 3})
        assert _plan([c1, c2]).rows_total == 6
        assert _plan([]).rows_total == 0

    def test_retention_plan_rows_total_sums_candidates(self):
        c1 = ShortageRetentionCandidate(scenario_id=SCENARIO, rows_to_delete=4)
        c2 = ShortageRetentionCandidate(scenario_id=uuid4(), rows_to_delete=1)
        assert _retention_plan([c1, c2]).rows_total == 5
        assert _retention_plan([]).rows_total == 0


# ---------------------------------------------------------------------------
# Per-table SQL composition — one COUNT/DELETE pair per whitelist entry,
# each parameterized by scenario_id exactly once.
# ---------------------------------------------------------------------------


class TestTableQueryComposition:
    def test_every_whitelist_table_has_a_query_pair(self):
        assert set(_TABLE_QUERIES) == set(PURGE_WHITELIST)

    def test_each_query_is_parameterized_by_scenario_id_exactly_once(self):
        for table, (count_sql, delete_sql) in _TABLE_QUERIES.items():
            assert count_sql.count("%s") == 1, table
            assert delete_sql.count("%s") == 1, table
            assert count_sql.startswith("SELECT COUNT(*)"), table
            assert delete_sql.startswith(f"DELETE FROM {table}"), table

    def test_indirectly_scoped_tables_scope_through_calc_runs(self):
        """explanations/causal_steps carry no scenario_id column (ADR-004);
        their scoping MUST go through calc_runs — a bare scenario_id equality
        here would be a silent always-empty predicate."""
        for table in ("explanations", "causal_steps"):
            count_sql, delete_sql = _TABLE_QUERIES[table]
            for sql in (count_sql, delete_sql):
                assert "calc_runs" in sql, table
                assert "scenario_id = %s" in sql, table
