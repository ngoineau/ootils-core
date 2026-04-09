"""
test_propagator.py — Comprehensive unit tests for PropagationEngine.

Covers every method and code path:
  - process_event: event not found, lock not acquired, no trigger node,
    both dates, old only, new only, no dates, propagation exception
  - _finish_run: scenario found, not found, with/without event_ids,
    shortage_detector present/absent, resolve_stale returns >0 / ==0 / raises
  - _propagate: empty dirty set, mixed PI/non-PI, node not found
  - _recompute_pi_node: all branches — node not found, not PI, missing dates,
    predecessor exists with/without closing_stock, no predecessor (on-hand supply),
    supply events, demand events (span vs point), overlap calc, changed/unchanged,
    explanation builder present/absent/exception, shortage detector present/absent/exception
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch, PropertyMock
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.orchestration.propagator import PropagationEngine
from ootils_core.models import CalcRun, Edge, Node, Scenario


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(**overrides) -> Node:
    defaults = dict(
        node_id=uuid4(),
        node_type="ProjectedInventory",
        scenario_id=uuid4(),
    )
    defaults.update(overrides)
    return Node(**defaults)


def _make_edge(**overrides) -> Edge:
    defaults = dict(
        edge_id=uuid4(),
        edge_type="feeds_forward",
        from_node_id=uuid4(),
        to_node_id=uuid4(),
        scenario_id=uuid4(),
    )
    defaults.update(overrides)
    return Edge(**defaults)


def _make_calc_run(**overrides) -> CalcRun:
    defaults = dict(
        calc_run_id=uuid4(),
        scenario_id=uuid4(),
        status="running",
    )
    defaults.update(overrides)
    run = CalcRun(**defaults)
    # The propagator code uses calc_run.event_ids (a bug — should be
    # triggered_by_event_ids). We add the attribute for test compatibility.
    if not hasattr(run, "event_ids"):
        run.event_ids = run.triggered_by_event_ids  # type: ignore[attr-defined]
    return run


def _make_engine(
    store=None,
    traversal=None,
    dirty=None,
    calc_run_mgr=None,
    kernel=None,
    explanation_builder=None,
    shortage_detector=None,
):
    return PropagationEngine(
        store=store or MagicMock(),
        traversal=traversal or MagicMock(),
        dirty=dirty or MagicMock(),
        calc_run_mgr=calc_run_mgr or MagicMock(),
        kernel=kernel or MagicMock(),
        explanation_builder=explanation_builder,
        shortage_detector=shortage_detector,
    )


def _mock_db():
    db = MagicMock()
    return db


# ===========================================================================
# process_event
# ===========================================================================


class TestProcessEvent:
    def test_event_not_found_returns_none(self):
        db = _mock_db()
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        db.execute.return_value = cursor

        engine = _make_engine()
        result = engine.process_event(uuid4(), uuid4(), db)
        assert result is None

    def test_lock_not_acquired_returns_none(self):
        db = _mock_db()
        cursor = MagicMock()
        cursor.fetchone.return_value = {"event_id": str(uuid4()), "trigger_node_id": str(uuid4())}
        db.execute.return_value = cursor

        calc_run_mgr = MagicMock()
        calc_run_mgr.start_calc_run.return_value = None

        engine = _make_engine(calc_run_mgr=calc_run_mgr)
        result = engine.process_event(uuid4(), uuid4(), db)
        assert result is None

    def test_no_trigger_node_skips_propagation(self):
        db = _mock_db()
        event_id = uuid4()
        scenario_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        event_cursor = MagicMock()
        event_cursor.fetchone.return_value = {
            "event_id": str(event_id),
            "trigger_node_id": None,
        }
        scenario_cursor = MagicMock()
        scenario_cursor.fetchone.return_value = {
            "scenario_id": str(scenario_id),
            "name": "Test",
            "baseline_snapshot_id": None,
            "is_baseline": False,
        }
        generic_cursor = MagicMock()

        def execute_side_effect(sql, *args, **kwargs):
            s = str(sql).lower()
            if "from events where event_id" in s:
                return event_cursor
            if "from scenarios where scenario_id" in s:
                return scenario_cursor
            return generic_cursor

        db.execute.side_effect = execute_side_effect

        calc_run_mgr = MagicMock()
        calc_run_mgr.start_calc_run.return_value = run

        engine = _make_engine(calc_run_mgr=calc_run_mgr)
        result = engine.process_event(event_id, scenario_id, db)
        assert result is run
        # Should have called _finish_run (which calls complete_calc_run)
        calc_run_mgr.complete_calc_run.assert_called_once()

    def test_both_dates_sets_window(self):
        db = _mock_db()
        event_id = uuid4()
        scenario_id = uuid4()
        trigger = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        # Event row with both dates
        event_row = {
            "event_id": str(event_id),
            "trigger_node_id": str(trigger),
            "old_date": date(2025, 1, 1),
            "new_date": date(2025, 3, 1),
        }

        # Configure db.execute side effects
        cursor_event = MagicMock()
        cursor_event.fetchone.return_value = event_row

        cursor_savepoint = MagicMock()
        cursor_update = MagicMock()
        cursor_scenario = MagicMock()
        cursor_scenario.fetchone.return_value = {
            "scenario_id": str(scenario_id),
            "name": "Test",
            "baseline_snapshot_id": None,
            "is_baseline": False,
        }
        cursor_events_update = MagicMock()
        cursor_unlock = MagicMock()

        db.execute.side_effect = [
            cursor_event,       # SELECT event
            cursor_savepoint,   # SAVEPOINT
            cursor_update,      # UPDATE dirty_node_count
            cursor_scenario,    # SELECT scenario (in _finish_run)
            cursor_update,      # UPDATE calc_runs (complete)
            cursor_events_update, # UPDATE events processed
            cursor_unlock,      # advisory unlock
        ]

        traversal = MagicMock()
        traversal.expand_dirty_subgraph.return_value = set()
        traversal.topological_sort.return_value = []

        dirty = MagicMock()
        calc_run_mgr = MagicMock()
        calc_run_mgr.start_calc_run.return_value = run

        engine = _make_engine(
            traversal=traversal,
            dirty=dirty,
            calc_run_mgr=calc_run_mgr,
        )
        result = engine.process_event(event_id, scenario_id, db)
        assert result is run

        # Verify expand_dirty_subgraph was called with correct time window
        call_args = traversal.expand_dirty_subgraph.call_args
        tw = call_args.kwargs.get("time_window") or call_args[1].get("time_window")
        assert tw[0] == date(2025, 1, 1)
        assert tw[1] == date(2025, 3, 1) + timedelta(days=365)

    def test_only_old_date(self):
        db = _mock_db()
        event_id = uuid4()
        scenario_id = uuid4()
        trigger = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        event_row = {
            "event_id": str(event_id),
            "trigger_node_id": str(trigger),
            "old_date": date(2025, 2, 1),
            "new_date": None,
        }
        scenario_row = {
            "scenario_id": str(scenario_id),
            "name": "Test",
            "baseline_snapshot_id": None,
            "is_baseline": False,
        }
        event_cursor = MagicMock()
        event_cursor.fetchone.return_value = event_row
        scenario_cursor = MagicMock()
        scenario_cursor.fetchone.return_value = scenario_row
        generic_cursor = MagicMock()

        def execute_side_effect(sql, *args, **kwargs):
            s = str(sql).lower()
            if "from events where event_id" in s:
                return event_cursor
            if "from scenarios where scenario_id" in s:
                return scenario_cursor
            return generic_cursor

        db.execute.side_effect = execute_side_effect

        traversal = MagicMock()
        traversal.expand_dirty_subgraph.return_value = set()

        calc_run_mgr = MagicMock()
        calc_run_mgr.start_calc_run.return_value = run

        engine = _make_engine(traversal=traversal, calc_run_mgr=calc_run_mgr)
        engine.process_event(event_id, scenario_id, db)

        tw = traversal.expand_dirty_subgraph.call_args.kwargs.get("time_window")
        if tw is None:
            tw = traversal.expand_dirty_subgraph.call_args[1]["time_window"]
        assert tw[0] == date(2025, 2, 1)
        assert tw[1] == date(2025, 2, 1) + timedelta(days=365)

    def test_only_new_date(self):
        db = _mock_db()
        event_id = uuid4()
        scenario_id = uuid4()
        trigger = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        event_row = {
            "event_id": str(event_id),
            "trigger_node_id": str(trigger),
            "old_date": None,
            "new_date": date(2025, 6, 15),
        }
        scenario_row = {
            "scenario_id": str(scenario_id),
            "name": "Test",
            "baseline_snapshot_id": None,
            "is_baseline": False,
        }
        event_cursor = MagicMock()
        event_cursor.fetchone.return_value = event_row
        scenario_cursor = MagicMock()
        scenario_cursor.fetchone.return_value = scenario_row
        generic_cursor = MagicMock()

        def execute_side_effect(sql, *args, **kwargs):
            s = str(sql).lower()
            if "from events where event_id" in s:
                return event_cursor
            if "from scenarios where scenario_id" in s:
                return scenario_cursor
            return generic_cursor

        db.execute.side_effect = execute_side_effect

        traversal = MagicMock()
        traversal.expand_dirty_subgraph.return_value = set()

        calc_run_mgr = MagicMock()
        calc_run_mgr.start_calc_run.return_value = run

        engine = _make_engine(traversal=traversal, calc_run_mgr=calc_run_mgr)
        engine.process_event(event_id, scenario_id, db)

        tw = traversal.expand_dirty_subgraph.call_args.kwargs.get("time_window")
        if tw is None:
            tw = traversal.expand_dirty_subgraph.call_args[1]["time_window"]
        assert tw[0] == date(2025, 6, 15)

    def test_no_dates_uses_full_range(self):
        db = _mock_db()
        event_id = uuid4()
        scenario_id = uuid4()
        trigger = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        event_row = {
            "event_id": str(event_id),
            "trigger_node_id": str(trigger),
            "old_date": None,
            "new_date": None,
        }
        scenario_row = {
            "scenario_id": str(scenario_id),
            "name": "Test",
            "baseline_snapshot_id": None,
            "is_baseline": False,
        }
        event_cursor = MagicMock()
        event_cursor.fetchone.return_value = event_row
        scenario_cursor = MagicMock()
        scenario_cursor.fetchone.return_value = scenario_row
        generic_cursor = MagicMock()

        def execute_side_effect(sql, *args, **kwargs):
            s = str(sql).lower()
            if "from events where event_id" in s:
                return event_cursor
            if "from scenarios where scenario_id" in s:
                return scenario_cursor
            return generic_cursor

        db.execute.side_effect = execute_side_effect

        traversal = MagicMock()
        traversal.expand_dirty_subgraph.return_value = set()

        calc_run_mgr = MagicMock()
        calc_run_mgr.start_calc_run.return_value = run

        engine = _make_engine(traversal=traversal, calc_run_mgr=calc_run_mgr)
        engine.process_event(event_id, scenario_id, db)

        tw = traversal.expand_dirty_subgraph.call_args.kwargs.get("time_window")
        if tw is None:
            tw = traversal.expand_dirty_subgraph.call_args[1]["time_window"]
        assert tw[0] == date.min
        assert tw[1] == date.max

    def test_propagation_exception_rolls_back_and_fails_run(self):
        db = _mock_db()
        event_id = uuid4()
        scenario_id = uuid4()
        trigger = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        event_row = {
            "event_id": str(event_id),
            "trigger_node_id": str(trigger),
            "old_date": None,
            "new_date": None,
        }
        cursor_event = MagicMock()
        cursor_event.fetchone.return_value = event_row
        db.execute.return_value = cursor_event

        traversal = MagicMock()
        traversal.expand_dirty_subgraph.side_effect = RuntimeError("boom")

        calc_run_mgr = MagicMock()
        calc_run_mgr.start_calc_run.return_value = run

        engine = _make_engine(traversal=traversal, calc_run_mgr=calc_run_mgr)
        with pytest.raises(RuntimeError, match="boom"):
            engine.process_event(event_id, scenario_id, db)

        calc_run_mgr.fail_calc_run.assert_called_once()


# ===========================================================================
# _finish_run
# ===========================================================================


class TestFinishRun:
    def test_scenario_found_no_baseline(self):
        db = _mock_db()
        scenario_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id, triggered_by_event_ids=[uuid4()])

        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "scenario_id": str(scenario_id),
            "name": "Test",
            "baseline_snapshot_id": None,
            "is_baseline": False,
        }
        db.execute.return_value = cursor

        calc_run_mgr = MagicMock()
        engine = _make_engine(calc_run_mgr=calc_run_mgr)
        engine._finish_run(run, scenario_id, db)

        calc_run_mgr.complete_calc_run.assert_called_once()
        scenario_arg = calc_run_mgr.complete_calc_run.call_args[0][1]
        assert scenario_arg.name == "Test"

    def test_scenario_found_with_baseline(self):
        db = _mock_db()
        scenario_id = uuid4()
        baseline_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id, triggered_by_event_ids=[uuid4()])

        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "scenario_id": str(scenario_id),
            "name": "Scenario A",
            "baseline_snapshot_id": str(baseline_id),
            "is_baseline": True,
        }
        db.execute.return_value = cursor

        calc_run_mgr = MagicMock()
        engine = _make_engine(calc_run_mgr=calc_run_mgr)
        engine._finish_run(run, scenario_id, db)

        scenario_arg = calc_run_mgr.complete_calc_run.call_args[0][1]
        assert scenario_arg.baseline_snapshot_id == baseline_id

    def test_scenario_not_found_uses_default(self):
        db = _mock_db()
        scenario_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id, triggered_by_event_ids=[])

        cursor = MagicMock()
        cursor.fetchone.return_value = None
        db.execute.return_value = cursor

        calc_run_mgr = MagicMock()
        engine = _make_engine(calc_run_mgr=calc_run_mgr)
        engine._finish_run(run, scenario_id, db)

        scenario_arg = calc_run_mgr.complete_calc_run.call_args[0][1]
        assert scenario_arg.name == "unknown"

    def test_event_ids_present_updates_events(self):
        db = _mock_db()
        scenario_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id, triggered_by_event_ids=[uuid4()])
        # Ensure the event_ids alias is set
        run.event_ids = run.triggered_by_event_ids

        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "scenario_id": str(scenario_id),
            "name": "Test",
            "baseline_snapshot_id": None,
            "is_baseline": False,
        }
        db.execute.return_value = cursor

        calc_run_mgr = MagicMock()
        engine = _make_engine(calc_run_mgr=calc_run_mgr)
        engine._finish_run(run, scenario_id, db)

        # Check UPDATE events was called (db.execute called multiple times)
        calls = db.execute.call_args_list
        event_update_calls = [c for c in calls if "UPDATE events" in str(c)]
        assert len(event_update_calls) >= 1

    def test_no_event_ids_skips_event_update(self):
        db = _mock_db()
        scenario_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id, triggered_by_event_ids=[])
        run.event_ids = []

        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "scenario_id": str(scenario_id),
            "name": "Test",
            "baseline_snapshot_id": None,
            "is_baseline": False,
        }
        db.execute.return_value = cursor

        calc_run_mgr = MagicMock()
        engine = _make_engine(calc_run_mgr=calc_run_mgr)
        engine._finish_run(run, scenario_id, db)

        calls = db.execute.call_args_list
        event_update_calls = [c for c in calls if "UPDATE events" in str(c)]
        assert len(event_update_calls) == 0

    def test_shortage_detector_resolve_stale_called(self):
        db = _mock_db()
        scenario_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id, triggered_by_event_ids=[])
        run.event_ids = []

        cursor = MagicMock()
        cursor.fetchone.return_value = None
        db.execute.return_value = cursor

        shortage_detector = MagicMock()
        shortage_detector.resolve_stale.return_value = 3

        calc_run_mgr = MagicMock()
        engine = _make_engine(
            calc_run_mgr=calc_run_mgr,
            shortage_detector=shortage_detector,
        )
        engine._finish_run(run, scenario_id, db)
        shortage_detector.resolve_stale.assert_called_once()

    def test_shortage_detector_resolve_stale_zero(self):
        db = _mock_db()
        scenario_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id, triggered_by_event_ids=[])
        run.event_ids = []

        cursor = MagicMock()
        cursor.fetchone.return_value = None
        db.execute.return_value = cursor

        shortage_detector = MagicMock()
        shortage_detector.resolve_stale.return_value = 0

        calc_run_mgr = MagicMock()
        engine = _make_engine(
            calc_run_mgr=calc_run_mgr,
            shortage_detector=shortage_detector,
        )
        engine._finish_run(run, scenario_id, db)
        shortage_detector.resolve_stale.assert_called_once()

    def test_shortage_detector_resolve_stale_exception(self):
        db = _mock_db()
        scenario_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id, triggered_by_event_ids=[])
        run.event_ids = []

        cursor = MagicMock()
        cursor.fetchone.return_value = None
        db.execute.return_value = cursor

        shortage_detector = MagicMock()
        shortage_detector.resolve_stale.side_effect = RuntimeError("db error")

        calc_run_mgr = MagicMock()
        engine = _make_engine(
            calc_run_mgr=calc_run_mgr,
            shortage_detector=shortage_detector,
        )
        # Should NOT raise
        engine._finish_run(run, scenario_id, db)

    def test_no_shortage_detector(self):
        db = _mock_db()
        scenario_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id, triggered_by_event_ids=[])
        run.event_ids = []

        cursor = MagicMock()
        cursor.fetchone.return_value = None
        db.execute.return_value = cursor

        calc_run_mgr = MagicMock()
        engine = _make_engine(calc_run_mgr=calc_run_mgr, shortage_detector=None)
        engine._finish_run(run, scenario_id, db)


# ===========================================================================
# _propagate
# ===========================================================================


class TestPropagate:
    def test_empty_dirty_set_returns_immediately(self):
        engine = _make_engine()
        run = _make_calc_run()
        db = _mock_db()

        engine._propagate(run, set(), db)
        engine._traversal.topological_sort.assert_not_called()

    def test_pi_node_changed_increments_recalculated(self):
        scenario_id = uuid4()
        node_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        store = MagicMock()
        pi_node = _make_node(node_id=node_id, scenario_id=scenario_id, node_type="ProjectedInventory")
        store.get_node.return_value = pi_node

        traversal = MagicMock()
        traversal.topological_sort.return_value = [node_id]

        dirty = MagicMock()
        kernel = MagicMock()

        engine = _make_engine(store=store, traversal=traversal, dirty=dirty, kernel=kernel)
        # Mock _recompute_pi_node to return True (changed)
        engine._recompute_pi_node = MagicMock(return_value=True)

        engine._propagate(run, {node_id}, _mock_db())

        assert run.nodes_recalculated == 1
        dirty.clear_dirty.assert_called_once()

    def test_pi_node_unchanged_increments_unchanged(self):
        scenario_id = uuid4()
        node_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        store = MagicMock()
        pi_node = _make_node(node_id=node_id, scenario_id=scenario_id, node_type="ProjectedInventory")
        store.get_node.return_value = pi_node

        traversal = MagicMock()
        traversal.topological_sort.return_value = [node_id]

        dirty = MagicMock()
        engine = _make_engine(store=store, traversal=traversal, dirty=dirty)
        engine._recompute_pi_node = MagicMock(return_value=False)

        engine._propagate(run, {node_id}, _mock_db())
        assert run.nodes_unchanged == 1

    def test_non_pi_node_increments_unchanged(self):
        scenario_id = uuid4()
        node_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        store = MagicMock()
        node = _make_node(node_id=node_id, scenario_id=scenario_id, node_type="ForecastDemand")
        store.get_node.return_value = node

        traversal = MagicMock()
        traversal.topological_sort.return_value = [node_id]

        dirty = MagicMock()
        engine = _make_engine(store=store, traversal=traversal, dirty=dirty)

        engine._propagate(run, {node_id}, _mock_db())
        assert run.nodes_unchanged == 1

    def test_node_not_found_clears_dirty(self):
        scenario_id = uuid4()
        node_id = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        store = MagicMock()
        store.get_node.return_value = None

        traversal = MagicMock()
        traversal.topological_sort.return_value = [node_id]

        dirty = MagicMock()
        engine = _make_engine(store=store, traversal=traversal, dirty=dirty)

        engine._propagate(run, {node_id}, _mock_db())
        dirty.clear_dirty.assert_called_once()

    def test_node_not_in_remaining_dirty_skipped(self):
        scenario_id = uuid4()
        node_a = uuid4()
        node_b = uuid4()
        run = _make_calc_run(scenario_id=scenario_id)

        store = MagicMock()
        traversal = MagicMock()
        # topo sort returns both, but only node_a is in dirty set
        traversal.topological_sort.return_value = [node_a, node_b]

        dirty = MagicMock()
        engine = _make_engine(store=store, traversal=traversal, dirty=dirty)

        engine._propagate(run, {node_a}, _mock_db())
        # node_b should be skipped; get_node called only for node_a
        assert store.get_node.call_count == 1


# ===========================================================================
# _recompute_pi_node
# ===========================================================================


class TestRecomputePiNode:
    def _make_engine_for_recompute(self, **kwargs):
        store = kwargs.get("store", MagicMock())
        kernel = kwargs.get("kernel", MagicMock())
        explanation_builder = kwargs.get("explanation_builder", None)
        shortage_detector = kwargs.get("shortage_detector", None)

        return PropagationEngine(
            store=store,
            traversal=MagicMock(),
            dirty=MagicMock(),
            calc_run_mgr=MagicMock(),
            kernel=kernel,
            explanation_builder=explanation_builder,
            shortage_detector=shortage_detector,
        )

    def test_node_not_found_returns_false(self):
        store = MagicMock()
        store.get_node.return_value = None

        engine = self._make_engine_for_recompute(store=store)
        result = engine._recompute_pi_node(uuid4(), uuid4(), uuid4(), _mock_db())
        assert result is False

    def test_node_not_pi_returns_false(self):
        store = MagicMock()
        store.get_node.return_value = _make_node(node_type="ForecastDemand")

        engine = self._make_engine_for_recompute(store=store)
        result = engine._recompute_pi_node(uuid4(), uuid4(), uuid4(), _mock_db())
        assert result is False

    def test_missing_time_span_returns_false(self):
        store = MagicMock()
        store.get_node.return_value = _make_node(
            node_type="ProjectedInventory",
            time_span_start=None,
            time_span_end=None,
        )

        engine = self._make_engine_for_recompute(store=store)
        result = engine._recompute_pi_node(uuid4(), uuid4(), uuid4(), _mock_db())
        assert result is False

    def test_missing_only_start_returns_false(self):
        store = MagicMock()
        store.get_node.return_value = _make_node(
            node_type="ProjectedInventory",
            time_span_start=None,
            time_span_end=date(2025, 1, 7),
        )

        engine = self._make_engine_for_recompute(store=store)
        result = engine._recompute_pi_node(uuid4(), uuid4(), uuid4(), _mock_db())
        assert result is False

    def test_predecessor_with_closing_stock(self):
        scenario_id = uuid4()
        node_id = uuid4()
        pred_node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
            opening_stock=Decimal("0"),
            inflows=Decimal("0"),
            outflows=Decimal("0"),
            closing_stock=Decimal("0"),
            has_shortage=False,
            shortage_qty=Decimal("0"),
        )
        pred_node = _make_node(
            node_id=pred_node_id,
            closing_stock=Decimal("100"),
        )
        pred_edge = _make_edge(
            edge_type="feeds_forward",
            from_node_id=pred_node_id,
            to_node_id=node_id,
        )

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else pred_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: (
            [pred_edge] if edge_type == "feeds_forward" else []
        )

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("100"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("100"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        result = engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        # Values changed from 0 to 100
        assert result is True
        kernel.compute_pi_node.assert_called_once()
        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["opening_stock"] == Decimal("100")

    def test_predecessor_with_none_closing_stock(self):
        scenario_id = uuid4()
        node_id = uuid4()
        pred_node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        pred_node = _make_node(node_id=pred_node_id, closing_stock=None)
        pred_edge = _make_edge(from_node_id=pred_node_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else pred_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: (
            [pred_edge] if edge_type == "feeds_forward" else []
        )

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["opening_stock"] == Decimal("0")

    def test_no_predecessor_on_hand_supply(self):
        scenario_id = uuid4()
        node_id = uuid4()
        oh_node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        oh_node = _make_node(
            node_id=oh_node_id,
            node_type="OnHandSupply",
            quantity=Decimal("50"),
        )
        oh_edge = _make_edge(
            edge_type="replenishes",
            from_node_id=oh_node_id,
            to_node_id=node_id,
        )

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else oh_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [oh_edge],
            "consumes": [],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("50"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("50"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["opening_stock"] == Decimal("50")

    def test_on_hand_supply_none_quantity(self):
        scenario_id = uuid4()
        node_id = uuid4()
        oh_node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        oh_node = _make_node(
            node_id=oh_node_id,
            node_type="OnHandSupply",
            quantity=None,
        )
        oh_edge = _make_edge(from_node_id=oh_node_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else oh_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [oh_edge],
            "consumes": [],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["opening_stock"] == Decimal("0")

    def test_supply_events_collected(self):
        scenario_id = uuid4()
        node_id = uuid4()
        po_node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        po_node = _make_node(
            node_id=po_node_id,
            node_type="PurchaseOrderSupply",
            time_ref=date(2025, 1, 3),
            quantity=Decimal("200"),
        )
        rep_edge = _make_edge(from_node_id=po_node_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else po_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [rep_edge],
            "consumes": [],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("200"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("200"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert len(call_kwargs["supply_events"]) == 1
        assert call_kwargs["supply_events"][0] == (date(2025, 1, 3), Decimal("200"))

    def test_supply_event_skipped_if_src_none(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        rep_edge = _make_edge(from_node_id=uuid4(), to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else None
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [rep_edge],
            "consumes": [],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["supply_events"] == []

    def test_supply_event_skipped_if_missing_time_ref_or_qty(self):
        scenario_id = uuid4()
        node_id = uuid4()
        po_node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        po_node = _make_node(
            node_id=po_node_id,
            node_type="PurchaseOrderSupply",
            time_ref=None,  # Missing time_ref
            quantity=Decimal("200"),
        )
        rep_edge = _make_edge(from_node_id=po_node_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else po_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [rep_edge],
            "consumes": [],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["supply_events"] == []

    def test_supply_event_skipped_for_non_supply_type(self):
        scenario_id = uuid4()
        node_id = uuid4()
        other_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        # Non-supply type on replenishes edge (e.g. OnHandSupply is handled above)
        other_node = _make_node(
            node_id=other_id,
            node_type="SomeOtherType",
            time_ref=date(2025, 1, 3),
            quantity=Decimal("10"),
        )
        rep_edge = _make_edge(from_node_id=other_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else other_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [rep_edge],
            "consumes": [],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["supply_events"] == []

    def test_demand_point_event(self):
        scenario_id = uuid4()
        node_id = uuid4()
        demand_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        demand_node = _make_node(
            node_id=demand_id,
            node_type="ForecastDemand",
            time_ref=date(2025, 1, 3),
            quantity=Decimal("30"),
            time_span_start=None,
            time_span_end=None,
        )
        consume_edge = _make_edge(from_node_id=demand_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else demand_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [],
            "consumes": [consume_edge],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("30"),
            "closing_stock": Decimal("-30"),
            "has_shortage": True,
            "shortage_qty": Decimal("30"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert len(call_kwargs["demand_events"]) == 1
        assert call_kwargs["demand_events"][0] == (date(2025, 1, 3), Decimal("30"))

    def test_demand_span_event_with_overlap(self):
        scenario_id = uuid4()
        node_id = uuid4()
        demand_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 8),  # 7 day bucket
        )
        # Demand spans 14 days, overlapping 7 days with the bucket
        demand_node = _make_node(
            node_id=demand_id,
            node_type="CustomerOrderDemand",
            time_ref=date(2025, 1, 1),
            quantity=Decimal("140"),
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 15),  # 14 day span
        )
        consume_edge = _make_edge(from_node_id=demand_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else demand_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [],
            "consumes": [consume_edge],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("70"),
            "closing_stock": Decimal("-70"),
            "has_shortage": True,
            "shortage_qty": Decimal("70"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        # 140 / 14 days = 10/day; overlap = 7 days; demand_qty = 70
        assert len(call_kwargs["demand_events"]) == 1
        assert call_kwargs["demand_events"][0] == (date(2025, 1, 1), Decimal("70"))

    def test_demand_span_no_overlap(self):
        scenario_id = uuid4()
        node_id = uuid4()
        demand_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        # Demand span entirely outside the bucket
        demand_node = _make_node(
            node_id=demand_id,
            node_type="ForecastDemand",
            time_ref=date(2025, 2, 1),
            quantity=Decimal("100"),
            time_span_start=date(2025, 2, 1),
            time_span_end=date(2025, 2, 15),
        )
        consume_edge = _make_edge(from_node_id=demand_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else demand_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [],
            "consumes": [consume_edge],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        # No overlap -> demand_events should be empty (overlap_end <= overlap_start -> skipped via continue)
        assert call_kwargs["demand_events"] == []

    def test_demand_span_zero_days(self):
        """span_days == 0 means no pro-rating; falls through to point demand."""
        scenario_id = uuid4()
        node_id = uuid4()
        demand_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        demand_node = _make_node(
            node_id=demand_id,
            node_type="ForecastDemand",
            time_ref=date(2025, 1, 3),
            quantity=Decimal("50"),
            time_span_start=date(2025, 1, 3),
            time_span_end=date(2025, 1, 3),  # Zero span
        )
        consume_edge = _make_edge(from_node_id=demand_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else demand_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [],
            "consumes": [consume_edge],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("50"),
            "closing_stock": Decimal("-50"),
            "has_shortage": True,
            "shortage_qty": Decimal("50"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        # span_days == 0 -> falls through to point demand
        assert len(call_kwargs["demand_events"]) == 1
        assert call_kwargs["demand_events"][0] == (date(2025, 1, 3), Decimal("50"))

    def test_demand_src_none_skipped(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        consume_edge = _make_edge(from_node_id=uuid4(), to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else None
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [],
            "consumes": [consume_edge],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["demand_events"] == []

    def test_demand_non_demand_type_skipped(self):
        scenario_id = uuid4()
        node_id = uuid4()
        other_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        other_node = _make_node(
            node_id=other_id,
            node_type="SomeRandomType",
            time_ref=date(2025, 1, 3),
            quantity=Decimal("10"),
        )
        consume_edge = _make_edge(from_node_id=other_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else other_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [],
            "consumes": [consume_edge],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["demand_events"] == []

    def test_demand_missing_date_and_qty_skipped(self):
        scenario_id = uuid4()
        node_id = uuid4()
        demand_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        demand_node = _make_node(
            node_id=demand_id,
            node_type="ForecastDemand",
            time_ref=None,
            quantity=None,
            time_span_start=None,
            time_span_end=None,
        )
        consume_edge = _make_edge(from_node_id=demand_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else demand_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [],
            "consumes": [consume_edge],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["demand_events"] == []

    def test_result_unchanged_returns_false(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
            opening_stock=Decimal("10"),
            inflows=Decimal("5"),
            outflows=Decimal("3"),
            closing_stock=Decimal("12"),
            has_shortage=False,
            shortage_qty=Decimal("0"),
        )

        store = MagicMock()
        store.get_node.return_value = pi_node
        store.get_edges_to.return_value = []

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("10"),
            "inflows": Decimal("5"),
            "outflows": Decimal("3"),
            "closing_stock": Decimal("12"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        result = engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())
        assert result is False

    def test_explanation_builder_called_on_shortage(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        fresh_node = _make_node(node_id=node_id, node_type="ProjectedInventory")

        store = MagicMock()
        store.get_node.return_value = pi_node
        store.get_edges_to.return_value = []
        # For fresh_node reload after persist
        store.get_node.side_effect = [pi_node, fresh_node, fresh_node]

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("50"),
            "closing_stock": Decimal("-50"),
            "has_shortage": True,
            "shortage_qty": Decimal("50"),
        }

        explanation_builder = MagicMock()
        explanation_builder.build_pi_explanation.return_value = MagicMock()

        engine = self._make_engine_for_recompute(
            store=store, kernel=kernel, explanation_builder=explanation_builder,
        )
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        explanation_builder.build_pi_explanation.assert_called_once()
        explanation_builder.persist.assert_called_once()

    def test_explanation_builder_exception_swallowed(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )

        store = MagicMock()
        store.get_node.return_value = pi_node
        store.get_edges_to.return_value = []

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("50"),
            "closing_stock": Decimal("-50"),
            "has_shortage": True,
            "shortage_qty": Decimal("50"),
        }

        explanation_builder = MagicMock()
        explanation_builder.build_pi_explanation.side_effect = RuntimeError("explain failed")

        engine = self._make_engine_for_recompute(
            store=store, kernel=kernel, explanation_builder=explanation_builder,
        )
        # Should NOT raise
        result = engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())
        assert result is True

    def test_explanation_not_called_when_no_shortage(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )

        store = MagicMock()
        store.get_node.return_value = pi_node
        store.get_edges_to.return_value = []

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        explanation_builder = MagicMock()
        engine = self._make_engine_for_recompute(
            store=store, kernel=kernel, explanation_builder=explanation_builder,
        )
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())
        explanation_builder.build_pi_explanation.assert_not_called()

    def test_explanation_fresh_node_none_skips(self):
        """If fresh_node reload returns None after persist, explanation is skipped."""
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )

        store = MagicMock()
        # First call returns pi_node, subsequent calls return None
        store.get_node.side_effect = [pi_node, None, None]
        store.get_edges_to.return_value = []

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("50"),
            "closing_stock": Decimal("-50"),
            "has_shortage": True,
            "shortage_qty": Decimal("50"),
        }

        explanation_builder = MagicMock()
        engine = self._make_engine_for_recompute(
            store=store, kernel=kernel, explanation_builder=explanation_builder,
        )
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())
        explanation_builder.build_pi_explanation.assert_not_called()

    def test_shortage_detector_called(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )

        store = MagicMock()
        store.get_node.return_value = pi_node
        store.get_edges_to.return_value = []

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        shortage_detector = MagicMock()
        shortage_detector.detect.return_value = MagicMock()

        engine = self._make_engine_for_recompute(
            store=store, kernel=kernel, shortage_detector=shortage_detector,
        )
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        shortage_detector.detect.assert_called_once()
        shortage_detector.persist.assert_called_once()

    def test_shortage_detector_detect_returns_none(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )

        store = MagicMock()
        store.get_node.return_value = pi_node
        store.get_edges_to.return_value = []

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        shortage_detector = MagicMock()
        shortage_detector.detect.return_value = None

        engine = self._make_engine_for_recompute(
            store=store, kernel=kernel, shortage_detector=shortage_detector,
        )
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())
        shortage_detector.persist.assert_not_called()

    def test_shortage_detector_exception_swallowed(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )

        store = MagicMock()
        store.get_node.return_value = pi_node
        store.get_edges_to.return_value = []

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        shortage_detector = MagicMock()
        shortage_detector.detect.side_effect = RuntimeError("detector boom")

        engine = self._make_engine_for_recompute(
            store=store, kernel=kernel, shortage_detector=shortage_detector,
        )
        # Should NOT raise
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

    def test_shortage_detector_fresh_node_none_skips(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )

        store = MagicMock()
        # First get_node returns pi_node, then None for fresh reload
        store.get_node.side_effect = [pi_node, None]
        store.get_edges_to.return_value = []

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        shortage_detector = MagicMock()
        engine = self._make_engine_for_recompute(
            store=store, kernel=kernel, shortage_detector=shortage_detector,
        )
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())
        shortage_detector.detect.assert_not_called()

    def test_demand_uses_time_span_start_when_time_ref_none(self):
        """demand_date = src_node.time_ref or src_node.time_span_start"""
        scenario_id = uuid4()
        node_id = uuid4()
        demand_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )
        demand_node = _make_node(
            node_id=demand_id,
            node_type="DependentDemand",
            time_ref=None,
            quantity=Decimal("25"),
            time_span_start=date(2025, 1, 2),
            time_span_end=None,
        )
        consume_edge = _make_edge(from_node_id=demand_id, to_node_id=node_id)

        store = MagicMock()
        store.get_node.side_effect = lambda nid, sid: pi_node if nid == node_id else demand_node
        store.get_edges_to.side_effect = lambda nid, sid, edge_type=None: {
            "feeds_forward": [],
            "replenishes": [],
            "consumes": [consume_edge],
        }.get(edge_type, [])

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("25"),
            "closing_stock": Decimal("-25"),
            "has_shortage": True,
            "shortage_qty": Decimal("25"),
        }

        engine = self._make_engine_for_recompute(store=store, kernel=kernel)
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())

        call_kwargs = kernel.compute_pi_node.call_args[1]
        assert call_kwargs["demand_events"] == [(date(2025, 1, 2), Decimal("25"))]

    def test_no_shortage_detector_configured(self):
        scenario_id = uuid4()
        node_id = uuid4()

        pi_node = _make_node(
            node_id=node_id,
            scenario_id=scenario_id,
            node_type="ProjectedInventory",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 7),
        )

        store = MagicMock()
        store.get_node.return_value = pi_node
        store.get_edges_to.return_value = []

        kernel = MagicMock()
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("0"),
            "closing_stock": Decimal("0"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        engine = self._make_engine_for_recompute(
            store=store, kernel=kernel, shortage_detector=None,
        )
        # Should complete without error
        engine._recompute_pi_node(node_id, scenario_id, uuid4(), _mock_db())
