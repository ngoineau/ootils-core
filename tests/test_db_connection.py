"""
Tests for ootils_core.db.connection — OotilsDB and helpers.

Covers:
- OotilsDB.__init__
- conn() context manager (commit and rollback paths)
- _apply_migrations (advisory lock dance, schema_migrations creation,
  reading migration files, per-migration transaction, raising on errors)
- health_check (ok, orphaned edges, stuck calc_runs, connection error)
- new_id() returns a UUID
"""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest
from psycopg import errors as pg_errors

from ootils_core.db import connection as conn_module
from ootils_core.db.connection import OotilsDB, new_id


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeCursorCM:
    """Context manager wrapping a MagicMock cursor."""
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class _FakeTransactionCM:
    """Context manager wrapping a fake transaction."""

    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


def _make_fake_connection(execute_results=None, execute_side_effect=None):
    """
    Build a MagicMock connection that supports:
        with psycopg.connect(...) as conn: ...
        with conn.cursor() as cur: cur.execute(sql)
        conn.execute(sql).fetchone()
        conn.commit()
        conn.rollback()
    """
    conn = MagicMock(name="connection")

    cursor = MagicMock(name="cursor")
    if execute_side_effect is not None:
        cursor.execute.side_effect = execute_side_effect
    conn.cursor.return_value = _FakeCursorCM(cursor)
    conn.transaction.return_value = _FakeTransactionCM()

    if execute_results is not None:
        # execute_results: list of dicts (or None) returned by .fetchone()
        results_iter = iter(execute_results)

        def _exec(*args, **kwargs):
            r = next(results_iter, None)
            res = MagicMock()
            res.fetchone.return_value = r
            res.fetchall.return_value = [r] if r else []
            return res

        conn.execute.side_effect = _exec
    else:
        result = MagicMock()
        result.fetchone.return_value = {"cnt": 0}
        result.fetchall.return_value = []
        conn.execute.return_value = result

    # Make the connection work as a context manager
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn, cursor


@pytest.fixture
def fake_migrations_dir(tmp_path, monkeypatch):
    """Create a fake migrations dir with one .sql file and patch MIGRATIONS_DIR."""
    mdir = tmp_path / "migrations"
    mdir.mkdir()
    (mdir / "001_init.sql").write_text("CREATE TABLE foo (id INT);", encoding="utf-8")
    monkeypatch.setattr(conn_module, "MIGRATIONS_DIR", mdir)
    return mdir


@pytest.fixture
def empty_migrations_dir(tmp_path, monkeypatch):
    """Empty migrations dir — _apply_migrations should early-return."""
    mdir = tmp_path / "empty_migrations"
    mdir.mkdir()
    monkeypatch.setattr(conn_module, "MIGRATIONS_DIR", mdir)
    return mdir


# ---------------------------------------------------------------------------
# new_id
# ---------------------------------------------------------------------------

def test_new_id_returns_uuid():
    val = new_id()
    assert isinstance(val, uuid.UUID)
    assert val.version == 4
    # Two consecutive calls produce different IDs
    assert new_id() != val


# ---------------------------------------------------------------------------
# OotilsDB.__init__ — calls _apply_migrations
# ---------------------------------------------------------------------------

def test_init_invokes_apply_migrations(empty_migrations_dir):
    with patch.object(OotilsDB, "_apply_migrations") as mock_apply:
        db = OotilsDB("postgresql:///fake")
        assert db.database_url == "postgresql:///fake"
        mock_apply.assert_called_once()


def test_init_uses_default_database_url(empty_migrations_dir):
    with patch.object(OotilsDB, "_apply_migrations"):
        db = OotilsDB()
        assert db.database_url == conn_module.DEFAULT_DATABASE_URL


# ---------------------------------------------------------------------------
# conn() context manager — commit on success, rollback on exception
# ---------------------------------------------------------------------------

def test_conn_commits_on_success(empty_migrations_dir):
    fake_conn, _ = _make_fake_connection()
    with patch.object(OotilsDB, "_apply_migrations"), \
            patch("ootils_core.db.connection.psycopg.connect", return_value=fake_conn) as mock_connect:
        db = OotilsDB("postgresql:///x")
        with db.conn() as c:
            assert c is fake_conn
        fake_conn.commit.assert_called_once()
        fake_conn.rollback.assert_not_called()
        mock_connect.assert_called_once()


def test_conn_rolls_back_on_exception(empty_migrations_dir):
    fake_conn, _ = _make_fake_connection()
    with patch.object(OotilsDB, "_apply_migrations"), \
            patch("ootils_core.db.connection.psycopg.connect", return_value=fake_conn):
        db = OotilsDB("postgresql:///x")
        with pytest.raises(RuntimeError, match="boom"):
            with db.conn():
                raise RuntimeError("boom")
        fake_conn.rollback.assert_called_once()
        fake_conn.commit.assert_not_called()


# ---------------------------------------------------------------------------
# _apply_migrations
# ---------------------------------------------------------------------------

def test_apply_migrations_no_files_early_return(empty_migrations_dir):
    """When migrations dir is empty we should not even open a connection."""
    with patch("ootils_core.db.connection.psycopg.connect") as mock_connect:
        OotilsDB("postgresql:///x")  # __init__ calls _apply_migrations
        mock_connect.assert_not_called()


def test_apply_migrations_runs_pending_files(fake_migrations_dir):
    fake_conn, cursor = _make_fake_connection()
    with patch("ootils_core.db.connection.psycopg.connect", return_value=fake_conn) as mock_connect:
        OotilsDB("postgresql:///x")
        # connect was called with autocommit=True for migrations
        assert mock_connect.called
        kwargs = mock_connect.call_args.kwargs
        assert kwargs.get("autocommit") is True
        # cursor.execute called with the SQL from our fake file
        fake_conn.transaction.assert_called_once()
        cursor.execute.assert_called_once()
        sql_arg = cursor.execute.call_args.args[0]
        assert "CREATE TABLE foo" in sql_arg
        schema_migration_inserts = [
            call for call in fake_conn.execute.call_args_list
            if "INSERT INTO schema_migrations" in call.args[0]
        ]
        assert len(schema_migration_inserts) == 1
        assert "ON CONFLICT DO NOTHING" in schema_migration_inserts[0].args[0]


@pytest.mark.parametrize(
    ("exc", "sqlstate"),
    [
        (pg_errors.DuplicateTable("relation already exists"), "42P07"),
        (pg_errors.DuplicateColumn("column already exists"), "42701"),
        (pg_errors.DuplicateObject("constraint already exists"), "42710"),
    ],
)
def test_apply_migrations_raises_on_duplicate_sqlstate(fake_migrations_dir, exc, sqlstate, capsys):
    fake_conn, _ = _make_fake_connection(execute_side_effect=exc)
    with patch("ootils_core.db.connection.psycopg.connect", return_value=fake_conn):
        with pytest.raises(type(exc)):
            OotilsDB("postgresql:///x")

    captured = capsys.readouterr()
    assert f"[sqlstate={sqlstate}]" in captured.err
    schema_migration_inserts = [
        call for call in fake_conn.execute.call_args_list
        if "INSERT INTO schema_migrations" in call.args[0]
    ]
    assert schema_migration_inserts == []


def test_apply_migrations_raises_on_real_error(fake_migrations_dir, capsys):
    """A non-idempotent error should propagate and be logged to stderr."""
    fake_conn, _ = _make_fake_connection(
        execute_side_effect=Exception("syntax error at or near FOO")
    )
    with patch("ootils_core.db.connection.psycopg.connect", return_value=fake_conn):
        with pytest.raises(Exception, match="syntax error"):
            OotilsDB("postgresql:///x")
    captured = capsys.readouterr()
    assert "[MIGRATION ERROR]" in captured.err
    assert "001_init.sql" in captured.err


def test_apply_migrations_runs_files_in_sorted_order(tmp_path, monkeypatch):
    """Multiple migration files should be executed in filename-sorted order."""
    mdir = tmp_path / "migrations"
    mdir.mkdir()
    (mdir / "002_second.sql").write_text("SELECT 2;", encoding="utf-8")
    (mdir / "001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    monkeypatch.setattr(conn_module, "MIGRATIONS_DIR", mdir)

    fake_conn, cursor = _make_fake_connection()
    with patch("ootils_core.db.connection.psycopg.connect", return_value=fake_conn):
        OotilsDB("postgresql:///x")

    executed_sql = [c.args[0] for c in cursor.execute.call_args_list]
    assert executed_sql == ["SELECT 1;", "SELECT 2;"]
    assert fake_conn.transaction.call_count == 2


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

def test_health_check_ok(empty_migrations_dir):
    """All counts zero -> ok=True, issues=[]."""
    fake_conn, _ = _make_fake_connection(
        execute_results=[
            None,            # SELECT 1 (return value not inspected)
            {"cnt": 0},      # orphaned edges
            {"cnt": 0},      # stuck calc_runs
        ],
    )
    with patch.object(OotilsDB, "_apply_migrations"), \
            patch("ootils_core.db.connection.psycopg.connect", return_value=fake_conn):
        db = OotilsDB("postgresql:///x")
        result = db.health_check()
        assert result == {"ok": True, "issues": []}


def test_health_check_orphaned_edges(empty_migrations_dir):
    fake_conn, _ = _make_fake_connection(
        execute_results=[
            None,
            {"cnt": 5},
            {"cnt": 0},
        ],
    )
    with patch.object(OotilsDB, "_apply_migrations"), \
            patch("ootils_core.db.connection.psycopg.connect", return_value=fake_conn):
        db = OotilsDB("postgresql:///x")
        result = db.health_check()
        assert result["ok"] is False
        assert any("Orphaned edges: 5" in i for i in result["issues"])


def test_health_check_stuck_calc_runs(empty_migrations_dir):
    fake_conn, _ = _make_fake_connection(
        execute_results=[
            None,
            {"cnt": 0},
            {"cnt": 3},
        ],
    )
    with patch.object(OotilsDB, "_apply_migrations"), \
            patch("ootils_core.db.connection.psycopg.connect", return_value=fake_conn):
        db = OotilsDB("postgresql:///x")
        result = db.health_check()
        assert result["ok"] is False
        assert any("Stuck calc_runs: 3" in i for i in result["issues"])


def test_health_check_both_issues(empty_migrations_dir):
    fake_conn, _ = _make_fake_connection(
        execute_results=[
            None,
            {"cnt": 2},
            {"cnt": 7},
        ],
    )
    with patch.object(OotilsDB, "_apply_migrations"), \
            patch("ootils_core.db.connection.psycopg.connect", return_value=fake_conn):
        db = OotilsDB("postgresql:///x")
        result = db.health_check()
        assert result["ok"] is False
        assert len(result["issues"]) == 2


def test_health_check_connection_error(empty_migrations_dir):
    """Any exception inside the conn block should be reported as a connection error."""
    with patch.object(OotilsDB, "_apply_migrations"):
        db = OotilsDB("postgresql:///x")

    # Now patch psycopg.connect inside conn() to blow up
    with patch(
        "ootils_core.db.connection.psycopg.connect",
        side_effect=Exception("could not connect"),
    ):
        result = db.health_check()
    assert result["ok"] is False
    assert any("Connection error" in i for i in result["issues"])
    assert any("could not connect" in i for i in result["issues"])
