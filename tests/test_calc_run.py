"""
test_calc_run.py — Comprehensive unit tests for CalcRunManager and _row_to_calc_run.

Covers every method and branch:
  - start_calc_run: lock acquired, lock NOT acquired (row missing, row locked=False)
  - complete_calc_run: completed vs completed_stale, with/without triggered_by_event_ids
  - fail_calc_run: normal path, db.execute raises on UPDATE, db.execute raises on unlock
  - recover_pending_runs: returns pending rows, empty result
  - _row_to_calc_run: full row, minimal row with defaults
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.orchestration.calc_run import CalcRunManager, _row_to_calc_run
from ootils_core.models import CalcRun, Scenario


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_db():
    db = MagicMock()
    return db


def _setup_execute_sequence(db, returns):
    """Configure db.execute to return different cursors on successive calls."""
    cursors = []
    for ret in returns:
        cursor = MagicMock()
        if isinstance(ret, dict):
            cursor.fetchone.return_value = ret
            cursor.fetchall.return_value = [ret] if ret else []
        elif isinstance(ret, list):
            cursor.fetchall.return_value = ret
            cursor.fetchone.return_value = ret[0] if ret else None
        else:
            cursor.fetchone.return_value = ret
            cursor.fetchall.return_value = []
        cursors.append(cursor)
    db.execute.side_effect = cursors
    return cursors


def _full_calc_run_row(**overrides) -> dict:
    now = datetime.now(timezone.utc)
    row = {
        "calc_run_id": str(uuid4()),
        "scenario_id": str(uuid4()),
        "triggered_by_event_ids": [str(uuid4())],
        "is_full_recompute": False,
        "dirty_node_count": 5,
        "nodes_recalculated": 3,
        "nodes_unchanged": 2,
        "status": "pending",
        "started_at": now,
        "completed_at": None,
        "error_message": None,
        "created_at": now,
    }
    row.update(overrides)
    return row


# ===========================================================================
# _row_to_calc_run
# ===========================================================================


class TestRowToCalcRun:
    def test_full_row(self):
        row = _full_calc_run_row()
        run = _row_to_calc_run(row)
        assert isinstance(run, CalcRun)
        assert run.status == "pending"
        assert run.nodes_recalculated == 3
        assert run.nodes_unchanged == 2
        assert run.dirty_node_count == 5
        assert len(run.triggered_by_event_ids) == 1

    def test_minimal_row_defaults(self):
        row = {
            "calc_run_id": str(uuid4()),
            "scenario_id": str(uuid4()),
        }
        run = _row_to_calc_run(row)
        assert run.triggered_by_event_ids == []
        assert run.is_full_recompute is False
        assert run.dirty_node_count is None
        assert run.nodes_recalculated == 0
        assert run.nodes_unchanged == 0
        assert run.status == "pending"
        assert run.created_at is not None  # falls back to datetime.now

    def test_triggered_by_event_ids_none(self):
        row = _full_calc_run_row(triggered_by_event_ids=None)
        run = _row_to_calc_run(row)
        assert run.triggered_by_event_ids == []


# ===========================================================================
# start_calc_run
# ===========================================================================


class TestStartCalcRun:
    def test_lock_acquired_returns_calc_run(self):
        db = _mock_db()
        scenario_id = uuid4()
        event_id = uuid4()
        pending_event = uuid4()

        _setup_execute_sequence(db, [
            # pg_try_advisory_lock -> locked=True
            {"locked": True},
            # SELECT pending events
            [{"event_id": str(pending_event)}],
            # INSERT calc_run
            None,
        ])

        mgr = CalcRunManager()
        run = mgr.start_calc_run(scenario_id, [event_id], db)

        assert run is not None
        assert run.scenario_id == scenario_id
        assert run.status == "running"
        assert run.is_full_recompute is False
        # event_ids merged: pending + provided
        assert event_id in [UUID(str(e)) for e in run.triggered_by_event_ids]
        assert pending_event in [UUID(str(e)) for e in run.triggered_by_event_ids]

    def test_lock_not_acquired_returns_none(self):
        db = _mock_db()
        _setup_execute_sequence(db, [{"locked": False}])

        mgr = CalcRunManager()
        run = mgr.start_calc_run(uuid4(), [uuid4()], db)
        assert run is None

    def test_lock_row_none_returns_none(self):
        db = _mock_db()
        _setup_execute_sequence(db, [None])

        mgr = CalcRunManager()
        run = mgr.start_calc_run(uuid4(), [uuid4()], db)
        assert run is None

    def test_no_pending_events(self):
        db = _mock_db()
        scenario_id = uuid4()
        event_id = uuid4()

        _setup_execute_sequence(db, [
            {"locked": True},
            [],  # No pending events
            None,  # INSERT
        ])

        mgr = CalcRunManager()
        run = mgr.start_calc_run(scenario_id, [event_id], db)
        assert run is not None
        assert event_id in [UUID(str(e)) for e in run.triggered_by_event_ids]


# ===========================================================================
# complete_calc_run
# ===========================================================================


class TestCompleteCalcRun:
    def test_completed_no_baseline(self):
        db = _mock_db()
        run = CalcRun(
            calc_run_id=uuid4(),
            scenario_id=uuid4(),
            triggered_by_event_ids=[uuid4()],
            status="running",
            nodes_recalculated=5,
            nodes_unchanged=3,
        )
        scenario = Scenario(
            scenario_id=run.scenario_id,
            name="Test",
            baseline_snapshot_id=None,
        )

        mgr = CalcRunManager()
        mgr.complete_calc_run(run, scenario, db)

        assert run.status == "completed"
        assert run.completed_at is not None
        # 3 DB calls: UPDATE calc_runs, UPDATE events, advisory_unlock
        assert db.execute.call_count == 3

    def test_completed_stale_with_baseline(self):
        db = _mock_db()
        run = CalcRun(
            calc_run_id=uuid4(),
            scenario_id=uuid4(),
            triggered_by_event_ids=[uuid4()],
            status="running",
        )
        scenario = Scenario(
            scenario_id=run.scenario_id,
            name="Test",
            baseline_snapshot_id=uuid4(),  # Has baseline -> completed_stale
        )

        mgr = CalcRunManager()
        mgr.complete_calc_run(run, scenario, db)

        assert run.status == "completed_stale"

    def test_no_event_ids_skips_event_update(self):
        db = _mock_db()
        run = CalcRun(
            calc_run_id=uuid4(),
            scenario_id=uuid4(),
            triggered_by_event_ids=[],
            status="running",
        )
        scenario = Scenario(scenario_id=run.scenario_id, name="Test")

        mgr = CalcRunManager()
        mgr.complete_calc_run(run, scenario, db)

        # Only 2 calls: UPDATE calc_runs + advisory_unlock (no event update)
        assert db.execute.call_count == 2


# ===========================================================================
# fail_calc_run
# ===========================================================================


class TestFailCalcRun:
    def test_normal_failure(self):
        db = _mock_db()
        run = CalcRun(
            calc_run_id=uuid4(),
            scenario_id=uuid4(),
            status="running",
        )

        mgr = CalcRunManager()
        mgr.fail_calc_run(run, "something broke", db)

        assert run.status == "failed"
        assert run.completed_at is not None
        assert run.error_message == "something broke"
        # 2 calls: UPDATE calc_runs + advisory_unlock
        assert db.execute.call_count == 2

    def test_db_update_raises_still_tries_unlock(self):
        db = _mock_db()
        run = CalcRun(
            calc_run_id=uuid4(),
            scenario_id=uuid4(),
            status="running",
        )

        # First execute (UPDATE) raises, second (unlock) succeeds
        db.execute.side_effect = [Exception("DB down"), MagicMock()]

        mgr = CalcRunManager()
        mgr.fail_calc_run(run, "err", db)

        # Should have attempted both calls
        assert db.execute.call_count == 2

    def test_both_db_calls_raise(self):
        db = _mock_db()
        run = CalcRun(
            calc_run_id=uuid4(),
            scenario_id=uuid4(),
            status="running",
        )

        db.execute.side_effect = [Exception("DB down"), Exception("unlock fail")]

        mgr = CalcRunManager()
        # Should NOT raise — both exceptions are caught
        mgr.fail_calc_run(run, "err", db)
        assert run.status == "failed"


# ===========================================================================
# recover_pending_runs
# ===========================================================================


class TestRecoverPendingRuns:
    def test_returns_pending_runs(self):
        db = _mock_db()
        pending_row = _full_calc_run_row(status="pending")

        # First execute: UPDATE running -> failed (no return needed)
        # Second execute: SELECT pending rows
        cursor_update = MagicMock()
        cursor_select = MagicMock()
        cursor_select.fetchall.return_value = [pending_row]
        db.execute.side_effect = [cursor_update, cursor_select]

        mgr = CalcRunManager()
        runs = mgr.recover_pending_runs(db)

        assert len(runs) == 1
        assert runs[0].status == "pending"

    def test_no_pending_runs(self):
        db = _mock_db()
        cursor_update = MagicMock()
        cursor_select = MagicMock()
        cursor_select.fetchall.return_value = []
        db.execute.side_effect = [cursor_update, cursor_select]

        mgr = CalcRunManager()
        runs = mgr.recover_pending_runs(db)
        assert runs == []

    def test_marks_running_as_failed(self):
        db = _mock_db()
        cursor_update = MagicMock()
        cursor_select = MagicMock()
        cursor_select.fetchall.return_value = []
        db.execute.side_effect = [cursor_update, cursor_select]

        mgr = CalcRunManager()
        mgr.recover_pending_runs(db)

        # First call should be the UPDATE running -> failed
        first_call_sql = db.execute.call_args_list[0][0][0]
        assert "status = 'failed'" in first_call_sql
        assert "WHERE status = 'running'" in first_call_sql
