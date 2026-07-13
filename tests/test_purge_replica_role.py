"""
tests/test_purge_replica_role.py — ADR-040 extension (2026-07-12): the
fork-purge FK-trigger derogation on the ``PURGE_WHITELIST`` DELETE loop
(unit).

``purge.py``'s ``_delete_whitelist_for_scenario`` disables row-level FK
trigger validation around its 13-table DELETE loop (`SET LOCAL
session_replication_role = 'replica'`) when the connection's role permits
it, and falls back to the ordinary triggers-on deletes when it does not
(``psycopg.errors.InsufficientPrivilege``) — the same derogation
``ScenarioManager._copy_nodes`` already applies to the fork COPY (#460),
now shared via ``ootils_core.db.replica_role`` and applied here to the
purge DELETE. These tests pin that contract with a scripted mock
connection — no real DB required, mirroring ``tests/test_fork_replica_
role.py``'s pattern:

  - nominal path: SAVEPOINT -> SET replica -> RELEASE -> the 13 DELETEs
    (PURGE_WHITELIST order) -> SET origin, in that exact order;
  - fallback path: InsufficientPrivilege on the SET -> ROLLBACK TO
    SAVEPOINT + RELEASE SAVEPOINT + exactly one warning + the delete loop
    still completes on the slow path;
  - any OTHER exception on the SET propagates (no blanket except);
  - a DELETE failure on the fast path propagates as ITSELF — the finally
    block's role restore must not mask it with InFailedSqlTransaction.

``_verify_whitelist_emptied`` (the compensatory set-based check that runs
right after, unconditionally on both paths) is also pinned here at the pure
level: raises RuntimeError on any residual total, is a silent no-op at
zero. Its real-DB proof (an actual residual row makes it fire against
Postgres) lives in tests/integration/test_purge_integration.py, alongside
the full purge lifecycle re-run on the forced-fallback path.
"""
from __future__ import annotations

import logging
from uuid import UUID

import psycopg
import pytest

from ootils_core.engine.maintenance.purge import (
    PURGE_WHITELIST,
    _delete_whitelist_for_scenario,
    _PURGE_DELETE_SAVEPOINT_NAME,
    _verify_whitelist_emptied,
)

SCENARIO = UUID("00000000-0000-0000-0000-0000000000bb")

REPLICA_SET = "SET LOCAL session_replication_role = 'replica'"
ORIGIN_SET = "SET LOCAL session_replication_role = 'origin'"
SAVEPOINT = f"SAVEPOINT {_PURGE_DELETE_SAVEPOINT_NAME}"
ROLLBACK_TO = f"ROLLBACK TO SAVEPOINT {_PURGE_DELETE_SAVEPOINT_NAME}"
RELEASE = f"RELEASE SAVEPOINT {_PURGE_DELETE_SAVEPOINT_NAME}"


# ---------------------------------------------------------------------------
# Scripted connection double — same shape as test_fork_replica_role.py's
# ScriptedConn, adapted for the purge DELETE loop (no INSERT statements) and
# for _verify_whitelist_emptied's cursor()-based SELECT.
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
    rollback) clears the state.
    """

    def __init__(
        self,
        raise_on_replica_set=None,
        raise_on_delete_table: str | None = None,
        raise_on_delete_error: Exception | None = None,
        verify_total: int = 0,
    ):
        self.statements: list[str] = []
        self._raise_on_replica_set = raise_on_replica_set
        self._raise_on_delete_table = raise_on_delete_table
        self._raise_on_delete_error = raise_on_delete_error
        self._aborted = False
        self._verify_total = verify_total

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

        if self._raise_on_delete_table is not None and stmt.startswith(
            f"DELETE FROM {self._raise_on_delete_table}"
        ):
            self._aborted = True
            raise self._raise_on_delete_error

        if stmt.startswith("DELETE FROM"):
            return _Cursor(rowcount=1)

        if stmt.startswith("SELECT COALESCE(SUM(n)"):
            return _Cursor(one={"total": self._verify_total})

        return _Cursor(one=None, many=[])

    def cursor(self, row_factory=None) -> "ScriptedConn":
        # _verify_whitelist_emptied does conn.cursor(row_factory=dict_row)
        # .execute(...).fetchone() — reusing self keeps the statement log
        # unified and needs no extra bookkeeping.
        return self

    def rollback(self) -> None:
        self._aborted = False
        self.statements.append("<connection.rollback()>")

    # -- assertion helpers --------------------------------------------------

    def index_of(self, prefix: str) -> int:
        hits = [i for i, s in enumerate(self.statements) if s.startswith(prefix)]
        assert len(hits) == 1, (
            f"expected exactly one statement starting with {prefix!r}, "
            f"got {len(hits)}: {self.statements}"
        )
        return hits[0]

    def has(self, prefix: str) -> bool:
        return any(s.startswith(prefix) for s in self.statements)

    def count(self, prefix: str) -> int:
        return sum(1 for s in self.statements if s.startswith(prefix))


# ---------------------------------------------------------------------------
# (a) Nominal path — replica role wraps the 13-table DELETE loop
# ---------------------------------------------------------------------------


class TestNominalFastPath:
    def test_replica_then_origin_wrap_the_whole_delete_loop_in_order(self):
        db = ScriptedConn()
        counts = _delete_whitelist_for_scenario(db, SCENARIO)

        i_savepoint = db.index_of(SAVEPOINT)
        i_replica = db.index_of(REPLICA_SET)
        i_release = db.index_of(RELEASE)
        i_origin = db.index_of(ORIGIN_SET)

        # SAVEPOINT -> SET replica -> RELEASE -> every DELETE, in
        # PURGE_WHITELIST order -> SET origin.
        assert i_savepoint < i_replica < i_release
        delete_indices = [db.index_of(f"DELETE FROM {t}") for t in PURGE_WHITELIST]
        assert delete_indices == sorted(delete_indices), (
            "DELETEs must run in PURGE_WHITELIST order"
        )
        assert i_release < delete_indices[0]
        assert delete_indices[-1] < i_origin

        # Nominal path never touches the fallback branch.
        assert not db.has(ROLLBACK_TO)
        # Every whitelist table's rowcount flows through unchanged.
        assert counts == {t: 1 for t in PURGE_WHITELIST}

    def test_covers_every_whitelist_table_exactly_once(self):
        db = ScriptedConn()
        _delete_whitelist_for_scenario(db, SCENARIO)

        for table in PURGE_WHITELIST:
            assert db.count(f"DELETE FROM {table}") == 1, table


# ---------------------------------------------------------------------------
# (b) Fallback path — InsufficientPrivilege on the SET
# ---------------------------------------------------------------------------


class TestInsufficientPrivilegeFallback:
    def test_falls_back_to_slow_path_and_still_deletes_everything(self, caplog):
        db = ScriptedConn(
            raise_on_replica_set=psycopg.errors.InsufficientPrivilege(
                'permission denied to set parameter "session_replication_role"'
            )
        )
        with caplog.at_level(
            logging.WARNING, logger="ootils_core.db.replica_role"
        ):
            counts = _delete_whitelist_for_scenario(db, SCENARIO)

        i_savepoint = db.index_of(SAVEPOINT)
        i_replica = db.index_of(REPLICA_SET)
        i_rollback_to = db.index_of(ROLLBACK_TO)
        i_release = db.index_of(RELEASE)
        assert i_savepoint < i_replica < i_rollback_to < i_release

        delete_indices = [db.index_of(f"DELETE FROM {t}") for t in PURGE_WHITELIST]
        assert all(i > i_release for i in delete_indices)
        assert delete_indices == sorted(delete_indices)

        # The delete loop completed on the slow path — nothing to restore.
        assert not db.has(ORIGIN_SET)
        assert counts == {t: 1 for t in PURGE_WHITELIST}

    def test_logs_exactly_one_warning(self, caplog):
        db = ScriptedConn(
            raise_on_replica_set=psycopg.errors.InsufficientPrivilege(
                'permission denied to set parameter "session_replication_role"'
            )
        )
        with caplog.at_level(
            logging.WARNING, logger="ootils_core.db.replica_role"
        ):
            _delete_whitelist_for_scenario(db, SCENARIO)

        denied = [
            r
            for r in caplog.records
            if "purge.delete_fast_path_denied" in r.getMessage()
        ]
        assert len(denied) == 1
        assert denied[0].levelno == logging.WARNING
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
            _delete_whitelist_for_scenario(db, SCENARIO)

        assert exc_info.value is error
        assert not db.has(ROLLBACK_TO)
        assert not db.has("DELETE FROM")


# ---------------------------------------------------------------------------
# DELETE failure — original exception is never masked by the role restore
# ---------------------------------------------------------------------------


class TestDeleteFailureNotMasked:
    def test_delete_error_propagates_despite_failed_role_restore(self):
        """With replica mode active, a failing DELETE leaves the
        transaction aborted, so the finally-block's `SET LOCAL ... 'origin'`
        raises InFailedSqlTransaction — which must be swallowed so the
        caller sees the ORIGINAL delete error, not the follow-up noise."""
        boom = psycopg.errors.ForeignKeyViolation(
            "update or delete on table violates foreign key constraint"
        )
        db = ScriptedConn(raise_on_delete_table="shortages", raise_on_delete_error=boom)

        with pytest.raises(psycopg.errors.ForeignKeyViolation) as exc_info:
            _delete_whitelist_for_scenario(db, SCENARIO)

        assert exc_info.value is boom
        # The restore WAS attempted (and failed on the aborted transaction);
        # its InFailedSqlTransaction never surfaced.
        assert db.has(ORIGIN_SET)


# ---------------------------------------------------------------------------
# _verify_whitelist_emptied — the compensatory set-based check (pure level)
# ---------------------------------------------------------------------------


class TestVerifyWhitelistEmptied:
    def test_zero_residual_is_a_silent_noop(self):
        db = ScriptedConn(verify_total=0)
        assert _verify_whitelist_emptied(db, SCENARIO) is None

    def test_positive_residual_raises_runtime_error(self):
        db = ScriptedConn(verify_total=3)
        with pytest.raises(RuntimeError, match="residual row"):
            _verify_whitelist_emptied(db, SCENARIO)

    def test_verify_sql_binds_one_parameter_per_whitelist_table(self):
        """One %s per PURGE_WHITELIST table's own COUNT branch inside the
        UNION ALL — never drifting from PURGE_WHITELIST's length."""
        from ootils_core.engine.maintenance.purge import _VERIFY_WHITELIST_EMPTY_SQL

        assert _VERIFY_WHITELIST_EMPTY_SQL.count("%s") == len(PURGE_WHITELIST)
