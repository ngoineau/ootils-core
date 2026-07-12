"""
tests/test_fork_replica_role.py — ADR-040 fork FK-trigger derogation (unit).

The scenario-fork bulk copy (ScenarioManager._copy_nodes) disables row-level
FK trigger validation around its two INSERT…SELECT statements
(`SET LOCAL session_replication_role = 'replica'`) when the connection's
role permits it, and falls back to the ordinary triggers-on copy when it
does not (psycopg.errors.InsufficientPrivilege). These tests pin that
contract with a scripted mock connection — no real DB required:

  - nominal path: SAVEPOINT → SET replica → RELEASE → the two INSERTs →
    SET origin, in that exact order, with the compensatory set-based
    checks running AFTER the role is restored;
  - fallback path: InsufficientPrivilege on the SET → ROLLBACK TO
    SAVEPOINT + RELEASE SAVEPOINT + exactly one warning + the copy still
    completes on the slow path (compensatory checks included);
  - any OTHER exception on the SET propagates (no blanket except);
  - an INSERT failure on the fast path propagates as ITSELF — the finally
    block's role restore must not mask it with InFailedSqlTransaction.

Parity of the two paths against a real Postgres (plus the proof that the
compensatory check actually fires) lives in
tests/integration/test_fork_replica_parity_integration.py.
"""
from __future__ import annotations

import logging
from uuid import UUID

import psycopg
import pytest

from ootils_core.engine.scenario.manager import ScenarioManager

SRC = UUID("00000000-0000-0000-0000-000000000001")
DST = UUID("00000000-0000-0000-0000-0000000000aa")

REPLICA_SET = "SET LOCAL session_replication_role = 'replica'"
ORIGIN_SET = "SET LOCAL session_replication_role = 'origin'"
SAVEPOINT = "SAVEPOINT scenario_fork_replica_role"
ROLLBACK_TO = "ROLLBACK TO SAVEPOINT scenario_fork_replica_role"
RELEASE = "RELEASE SAVEPOINT scenario_fork_replica_role"


# ---------------------------------------------------------------------------
# Scripted connection double
# ---------------------------------------------------------------------------


class _Cursor:
    """Minimal cursor stand-in: fixed rowcount / fetch results."""

    def __init__(self, rowcount: int = 0, one=None, many=None):
        self.rowcount = rowcount
        self._one = one
        self._many = many if many is not None else []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class ScriptedConn:
    """
    Stand-in for a psycopg3 dict_row connection that records every executed
    statement (whitespace-normalized) in order, and mimics the one Postgres
    behaviour this code path depends on: after a statement raises, the
    transaction is ABORTED — every further statement fails with
    InFailedSqlTransaction until a ROLLBACK TO SAVEPOINT (or a full
    rollback) clears the state. This makes the mock order-sensitive the
    same way a real server is (e.g. a RELEASE attempted before the
    ROLLBACK TO would blow up here too).
    """

    def __init__(self, raise_on_replica_set=None, raise_on_insert_nodes=None):
        self.statements: list[str] = []
        self._raise_on_replica_set = raise_on_replica_set
        self._raise_on_insert_nodes = raise_on_insert_nodes
        self._aborted = False

    def execute(self, sql: str, params=None) -> _Cursor:
        stmt = " ".join(sql.split())
        self.statements.append(stmt)

        if stmt.startswith(ROLLBACK_TO):
            self._aborted = False
            return _Cursor()
        if self._aborted:
            raise psycopg.errors.InFailedSqlTransaction(
                "current transaction is aborted, commands ignored until "
                "end of transaction block"
            )

        if stmt == REPLICA_SET and self._raise_on_replica_set is not None:
            self._aborted = True
            raise self._raise_on_replica_set
        if stmt.startswith("INSERT INTO nodes") and self._raise_on_insert_nodes is not None:
            self._aborted = True
            raise self._raise_on_insert_nodes

        if stmt.startswith("SELECT COUNT(*)"):
            return _Cursor(one={"cnt": 0})
        if stmt.startswith("INSERT INTO nodes"):
            return _Cursor(rowcount=4)
        if stmt.startswith("INSERT INTO edges"):
            return _Cursor(rowcount=3)
        return _Cursor(one=None, many=[])

    def rollback(self) -> None:
        self._aborted = False
        self.statements.append("<connection.rollback()>")

    # -- assertion helpers --------------------------------------------------

    def index_of(self, prefix: str) -> int:
        """Index of the single statement starting with `prefix` (fails if
        absent or ambiguous — order assertions need uniqueness)."""
        hits = [i for i, s in enumerate(self.statements) if s.startswith(prefix)]
        assert len(hits) == 1, (
            f"expected exactly one statement starting with {prefix!r}, "
            f"got {len(hits)}: {self.statements}"
        )
        return hits[0]

    def has(self, prefix: str) -> bool:
        return any(s.startswith(prefix) for s in self.statements)


# ---------------------------------------------------------------------------
# (a) Nominal path — replica role wraps the two bulk INSERTs
# ---------------------------------------------------------------------------


class TestNominalFastPath:
    def test_replica_then_origin_wrap_the_two_inserts_in_order(self):
        db = ScriptedConn()
        count = ScenarioManager()._copy_nodes(SRC, DST, db)

        i_savepoint = db.index_of(SAVEPOINT)
        i_replica = db.index_of(REPLICA_SET)
        i_release = db.index_of(RELEASE)
        i_nodes = db.index_of("INSERT INTO nodes")
        i_edges = db.index_of("INSERT INTO edges")
        i_origin = db.index_of(ORIGIN_SET)

        # SAVEPOINT → SET replica → RELEASE → INSERT nodes → INSERT edges
        # → SET origin, strictly in that order.
        assert (
            i_savepoint < i_replica < i_release < i_nodes < i_edges < i_origin
        ), f"unexpected statement order: {db.statements}"

        # Nominal path never touches the fallback branch.
        assert not db.has(ROLLBACK_TO)
        # The copy result flows through unchanged.
        assert count == 4

    def test_compensatory_checks_run_after_role_is_restored(self):
        db = ScriptedConn()
        ScenarioManager()._copy_nodes(SRC, DST, db)

        i_origin = db.index_of(ORIGIN_SET)
        check_idx = [
            i for i, s in enumerate(db.statements) if s.startswith("SELECT COUNT(*)")
        ]
        # Both set-based checks (node FKs + orphan edges) run, and only
        # AFTER trigger validation has been re-enabled.
        assert len(check_idx) == 2, db.statements
        assert all(i > i_origin for i in check_idx), db.statements

    def test_node_fk_check_covers_items_locations_and_series(self):
        """Hardened compensatory check (review point): item_id, location_id
        AND projection_series_id (scoped to the NEW scenario) in ONE
        set-based query; scenario_id deliberately unchecked (the scenarios
        row was inserted in the same transaction — a tautology)."""
        db = ScriptedConn()
        ScenarioManager()._copy_nodes(SRC, DST, db)

        fk_check = db.statements[db.index_of("SELECT COUNT(*) AS cnt FROM nodes")]
        assert "FROM items" in fk_check
        assert "FROM locations" in fk_check
        assert "FROM projection_series" in fk_check
        # Scenario-scoped series check — stricter than the FK on purpose.
        assert "ps.scenario_id = %s" in fk_check
        # No tautological scenario_id existence check.
        assert "FROM scenarios" not in fk_check


# ---------------------------------------------------------------------------
# (b) Fallback path — InsufficientPrivilege on the SET
# ---------------------------------------------------------------------------


class TestInsufficientPrivilegeFallback:
    def test_falls_back_to_slow_path_and_still_copies(self, caplog):
        db = ScriptedConn(
            raise_on_replica_set=psycopg.errors.InsufficientPrivilege(
                'permission denied to set parameter "session_replication_role"'
            )
        )
        with caplog.at_level(
            logging.WARNING, logger="ootils_core.engine.scenario.manager"
        ):
            count = ScenarioManager()._copy_nodes(SRC, DST, db)

        # Savepoint dance: SAVEPOINT → failed SET → ROLLBACK TO → RELEASE.
        i_savepoint = db.index_of(SAVEPOINT)
        i_replica = db.index_of(REPLICA_SET)
        i_rollback_to = db.index_of(ROLLBACK_TO)
        i_release = db.index_of(RELEASE)
        i_nodes = db.index_of("INSERT INTO nodes")
        i_edges = db.index_of("INSERT INTO edges")
        assert (
            i_savepoint < i_replica < i_rollback_to < i_release < i_nodes < i_edges
        ), f"unexpected statement order: {db.statements}"

        # The copy completed on the slow path — nothing to restore.
        assert not db.has(ORIGIN_SET)
        assert count == 4

        # The compensatory checks still run (they are unconditional).
        check_count = sum(
            1 for s in db.statements if s.startswith("SELECT COUNT(*)")
        )
        assert check_count == 2

    def test_logs_exactly_one_warning(self, caplog):
        db = ScriptedConn(
            raise_on_replica_set=psycopg.errors.InsufficientPrivilege(
                'permission denied to set parameter "session_replication_role"'
            )
        )
        with caplog.at_level(
            logging.WARNING, logger="ootils_core.engine.scenario.manager"
        ):
            ScenarioManager()._copy_nodes(SRC, DST, db)

        denied = [
            r
            for r in caplog.records
            if "fork_fast_path_denied" in r.getMessage()
        ]
        assert len(denied) == 1
        assert denied[0].levelno == logging.WARNING
        # The warning must tell the operator how to grant the fast path.
        assert "GRANT SET ON PARAMETER" in denied[0].getMessage()


# ---------------------------------------------------------------------------
# (c) Any other exception on the SET propagates — no blanket except
# ---------------------------------------------------------------------------


class TestOtherSetErrorsPropagate:
    @pytest.mark.parametrize(
        "error",
        [
            psycopg.OperationalError("server closed the connection unexpectedly"),
            psycopg.errors.QueryCanceled("canceling statement due to user request"),
            RuntimeError("unexpected driver-level failure"),
        ],
        ids=["operational-error", "query-canceled", "runtime-error"],
    )
    def test_non_privilege_error_on_set_propagates(self, error):
        db = ScriptedConn(raise_on_replica_set=error)

        with pytest.raises(type(error)) as exc_info:
            ScenarioManager()._copy_nodes(SRC, DST, db)

        # The ORIGINAL exception object, not a re-wrap.
        assert exc_info.value is error
        # No fallback handling was attempted, and the copy never started.
        assert not db.has(ROLLBACK_TO)
        assert not db.has("INSERT INTO nodes")
        assert not db.has("INSERT INTO edges")


# ---------------------------------------------------------------------------
# Fast-path INSERT failure — original exception is never masked
# ---------------------------------------------------------------------------


class TestInsertFailureNotMasked:
    def test_insert_error_propagates_despite_failed_role_restore(self):
        """With replica mode active, a failing bulk INSERT leaves the
        transaction aborted, so the finally-block's `SET LOCAL ... 'origin'`
        raises InFailedSqlTransaction — which must be swallowed so the
        caller sees the ORIGINAL insert error, not the follow-up noise."""
        boom = psycopg.errors.UniqueViolation(
            "duplicate key value violates unique constraint"
        )
        db = ScriptedConn(raise_on_insert_nodes=boom)

        with pytest.raises(psycopg.errors.UniqueViolation) as exc_info:
            ScenarioManager()._copy_nodes(SRC, DST, db)

        assert exc_info.value is boom
        # The restore WAS attempted (and failed on the aborted transaction);
        # its InFailedSqlTransaction never surfaced.
        assert db.has(ORIGIN_SET)
