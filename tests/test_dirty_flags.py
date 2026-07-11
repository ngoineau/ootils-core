"""
test_dirty_flags.py — Comprehensive unit tests for DirtyFlagManager.

Covers every method and branch in dirty.py:
  - mark_dirty (new key vs existing key)
  - clear_dirty (key exists vs key missing)
  - get_dirty_nodes (in-memory hit vs Postgres fallback)
  - is_dirty (node present vs absent, key missing)
  - flush_to_postgres (with nodes vs empty set; both INSERT branches — native
    db.executemany vs the psycopg3 cursor fallback — and the post-INSERT
    `ANALYZE dirty_nodes` that keeps PROPAGATE_SQL's planner estimates honest)
  - load_from_postgres
"""
from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4


from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_db():
    db = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    db.execute.return_value = cursor
    return db


# ===========================================================================
# mark_dirty
# ===========================================================================


class TestMarkDirty:
    def test_new_key_creates_set(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        n1, n2 = uuid4(), uuid4()

        mgr.mark_dirty({n1, n2}, scenario, run, db)

        key = (scenario, run)
        assert key in mgr._dirty
        assert mgr._dirty[key] == {n1, n2}

    def test_existing_key_adds_to_set(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        n1, n2, n3 = uuid4(), uuid4(), uuid4()

        mgr.mark_dirty({n1}, scenario, run, db)
        mgr.mark_dirty({n2, n3}, scenario, run, db)

        assert mgr._dirty[(scenario, run)] == {n1, n2, n3}

    def test_empty_set_still_creates_key(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()

        mgr.mark_dirty(set(), scenario, run, db)
        assert (scenario, run) in mgr._dirty
        assert mgr._dirty[(scenario, run)] == set()


# ===========================================================================
# clear_dirty
# ===========================================================================


class TestClearDirty:
    def test_removes_node_from_memory_and_calls_db(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        n1, n2 = uuid4(), uuid4()

        mgr.mark_dirty({n1, n2}, scenario, run, db)
        mgr.clear_dirty(n1, scenario, run, db)

        assert n1 not in mgr._dirty[(scenario, run)]
        assert n2 in mgr._dirty[(scenario, run)]

        # DB DELETE was called
        db.execute.assert_called_once()
        sql = db.execute.call_args[0][0]
        assert "DELETE FROM dirty_nodes" in sql

    def test_key_not_in_memory_still_calls_db(self):
        """If no in-memory set exists, the DB DELETE is still issued."""
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()

        mgr.clear_dirty(uuid4(), scenario, run, db)
        db.execute.assert_called_once()

    def test_discard_node_not_present_is_noop(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        n1 = uuid4()
        missing_node = uuid4()

        mgr.mark_dirty({n1}, scenario, run, db)
        mgr.clear_dirty(missing_node, scenario, run, db)

        # n1 still present
        assert n1 in mgr._dirty[(scenario, run)]


# ===========================================================================
# get_dirty_nodes
# ===========================================================================


class TestGetDirtyNodes:
    def test_returns_from_memory(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        n1 = uuid4()

        mgr.mark_dirty({n1}, scenario, run, db)
        result = mgr.get_dirty_nodes(run, scenario, db)

        assert result == {n1}
        # Should NOT call db.execute (memory hit)
        db.execute.assert_not_called()

    def test_returns_copy_not_reference(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        n1 = uuid4()

        mgr.mark_dirty({n1}, scenario, run, db)
        result = mgr.get_dirty_nodes(run, scenario, db)
        result.add(uuid4())  # Mutate the returned set
        # Internal set should be unchanged
        assert len(mgr._dirty[(scenario, run)]) == 1

    def test_falls_back_to_postgres(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        n1 = uuid4()

        # No in-memory state — configure DB to return rows
        cursor = MagicMock()
        cursor.fetchall.return_value = [{"node_id": str(n1)}]
        db.execute.return_value = cursor

        result = mgr.get_dirty_nodes(run, scenario, db)
        assert result == {n1}
        db.execute.assert_called_once()

    def test_postgres_fallback_empty(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()

        cursor = MagicMock()
        cursor.fetchall.return_value = []
        db.execute.return_value = cursor

        result = mgr.get_dirty_nodes(run, scenario, db)
        assert result == set()


# ===========================================================================
# is_dirty
# ===========================================================================


class TestIsDirty:
    def test_node_is_dirty(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        n1 = uuid4()

        mgr.mark_dirty({n1}, scenario, run, db)
        assert mgr.is_dirty(n1, scenario, run) is True

    def test_node_not_dirty(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()

        mgr.mark_dirty({uuid4()}, scenario, run, db)
        assert mgr.is_dirty(uuid4(), scenario, run) is False

    def test_key_missing_returns_false(self):
        mgr = DirtyFlagManager()
        assert mgr.is_dirty(uuid4(), uuid4(), uuid4()) is False


# ===========================================================================
# flush_to_postgres
# ===========================================================================


class TestFlushToPostgres:
    def test_batch_inserts(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        n1, n2 = uuid4(), uuid4()

        mgr.mark_dirty({n1, n2}, scenario, run, db)
        mgr.flush_to_postgres(run, scenario, db)

        db.executemany.assert_called_once()
        sql = db.executemany.call_args[0][0]
        assert "INSERT INTO dirty_nodes" in sql
        assert "ON CONFLICT" in sql
        rows = db.executemany.call_args[0][1]
        assert len(rows) == 2

    def test_empty_set_skips_db(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()

        # key not even in _dirty
        mgr.flush_to_postgres(run, scenario, db)
        db.executemany.assert_not_called()
        # Early return preserved: no INSERT means no ANALYZE either.
        db.execute.assert_not_called()

    def test_empty_set_after_mark(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()

        mgr.mark_dirty(set(), scenario, run, db)
        mgr.flush_to_postgres(run, scenario, db)
        db.executemany.assert_not_called()
        # Early return preserved: no INSERT means no ANALYZE either.
        db.execute.assert_not_called()

    # ------------------------------------------------------------------
    # PERF2 guard: the just-inserted dirty set is read straight back by
    # PROPAGATE_SQL / SHORTAGES_SQL in the same transaction. Without fresh
    # stats the planner estimates rows=1 and re-runs the inflows/outflows
    # GroupAggregate per row (measured 200x, 43 nodes/s on a 2000-node set).
    # flush_to_postgres must therefore issue a static, parameterless
    # `ANALYZE dirty_nodes` AFTER the INSERT — on BOTH insert branches —
    # and never on an empty set (the early return above).
    # ------------------------------------------------------------------

    def test_nonempty_flush_analyzes_after_insert_native_branch(self):
        """Native branch (db exposes a callable executemany): the INSERT goes
        through db.executemany and is followed — in that order — by exactly
        one db.execute("ANALYZE dirty_nodes")."""
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()

        order: list[str] = []
        db.executemany.side_effect = lambda *a, **k: order.append("insert")
        db.execute.side_effect = lambda *a, **k: order.append("analyze")

        mgr.mark_dirty({uuid4(), uuid4()}, scenario, run, db)
        mgr.flush_to_postgres(run, scenario, db)

        db.executemany.assert_called_once()
        assert "INSERT INTO dirty_nodes" in db.executemany.call_args[0][0]
        # Exactly one ANALYZE, static SQL, no bound parameters.
        db.execute.assert_called_once_with("ANALYZE dirty_nodes")
        # And strictly AFTER the INSERT (stats must cover the new rows).
        assert order == ["insert", "analyze"]

    def test_nonempty_flush_analyzes_after_insert_cursor_fallback(self):
        """Fallback branch (psycopg3 connections have no executemany, so the
        INSERT goes through cursor().executemany): ANALYZE must run on this
        path too, after the INSERT. The pre-existing tests never exercise this
        branch because MagicMock always exposes a callable executemany."""
        mgr = DirtyFlagManager()
        db = _mock_db()
        # Forces getattr(db, "executemany", None) -> None, like a real
        # psycopg3 Connection (attribute absent).
        del db.executemany
        cursor = db.cursor.return_value.__enter__.return_value
        scenario = uuid4()
        run = uuid4()

        order: list[str] = []
        cursor.executemany.side_effect = lambda *a, **k: order.append("insert")
        db.execute.side_effect = lambda *a, **k: order.append("analyze")

        mgr.mark_dirty({uuid4()}, scenario, run, db)
        mgr.flush_to_postgres(run, scenario, db)

        cursor.executemany.assert_called_once()
        assert "INSERT INTO dirty_nodes" in cursor.executemany.call_args[0][0]
        db.execute.assert_called_once_with("ANALYZE dirty_nodes")
        assert order == ["insert", "analyze"]

    def test_empty_set_no_analyze_cursor_fallback(self):
        """Early return on an empty set also short-circuits the fallback
        branch: no cursor is ever opened, no ANALYZE is ever issued."""
        mgr = DirtyFlagManager()
        db = _mock_db()
        del db.executemany
        scenario = uuid4()
        run = uuid4()

        mgr.flush_to_postgres(run, scenario, db)

        db.cursor.assert_not_called()
        db.execute.assert_not_called()


# ===========================================================================
# load_from_postgres
# ===========================================================================


class TestLoadFromPostgres:
    def test_populates_memory_from_db(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        n1, n2 = uuid4(), uuid4()

        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {"node_id": str(n1)},
            {"node_id": str(n2)},
        ]
        db.execute.return_value = cursor

        mgr.load_from_postgres(run, scenario, db)

        key = (scenario, run)
        assert key in mgr._dirty
        assert mgr._dirty[key] == {n1, n2}

    def test_empty_db_creates_empty_set(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()

        cursor = MagicMock()
        cursor.fetchall.return_value = []
        db.execute.return_value = cursor

        mgr.load_from_postgres(run, scenario, db)
        assert mgr._dirty[(scenario, run)] == set()

    def test_overwrites_existing_memory(self):
        mgr = DirtyFlagManager()
        db = _mock_db()
        scenario = uuid4()
        run = uuid4()
        old_node = uuid4()
        new_node = uuid4()

        mgr.mark_dirty({old_node}, scenario, run, db)

        cursor = MagicMock()
        cursor.fetchall.return_value = [{"node_id": str(new_node)}]
        db.execute.return_value = cursor

        mgr.load_from_postgres(run, scenario, db)
        # Old node gone, new node present
        assert mgr._dirty[(scenario, run)] == {new_node}
