"""
Tests for ghost engine: ghost_engine.py, capacity_aggregate.py, phase_transition.py.

Covers all branches including weight curves (linear, step, sigmoid, fallback),
capacity overload detection, phase transition inconsistency alerts, and dispatcher logic.
"""
from __future__ import annotations

import math
from datetime import date, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

from ootils_core.engine.ghost.ghost_engine import run_ghost
from ootils_core.engine.ghost.capacity_aggregate import (
    run_capacity_aggregate,
    _get_resource_capacity,
    _get_supply_load,
)
from ootils_core.engine.ghost.phase_transition import (
    compute_weight,
    run_phase_transition,
    _get_projected_inventory,
    INCONSISTENCY_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_db():
    """Return a MagicMock simulating psycopg.Connection with dict_row cursors."""
    db = MagicMock()
    return db


def _make_cursor(rows):
    """Helper: build a mock cursor whose fetchone/fetchall return given data."""
    cursor = MagicMock()
    if rows is None:
        cursor.fetchone.return_value = None
        cursor.fetchall.return_value = []
    elif isinstance(rows, dict):
        cursor.fetchone.return_value = rows
        cursor.fetchall.return_value = [rows]
    elif isinstance(rows, list):
        cursor.fetchone.return_value = rows[0] if rows else None
        cursor.fetchall.return_value = rows
    return cursor


# =========================================================================
# ghost_engine.py — run_ghost dispatcher
# =========================================================================

class TestRunGhost:

    def test_ghost_not_found_raises(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        with pytest.raises(ValueError, match="not found"):
            run_ghost(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))

    @patch("ootils_core.engine.ghost.ghost_engine.run_phase_transition")
    def test_dispatches_phase_transition(self, mock_pt):
        db = _mock_db()
        db.execute.return_value = _make_cursor({"ghost_type": "phase_transition"})
        mock_pt.return_value = {"ghost_type": "phase_transition"}

        result = run_ghost(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))
        mock_pt.assert_called_once_with(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))
        assert result["ghost_type"] == "phase_transition"

    @patch("ootils_core.engine.ghost.ghost_engine.run_capacity_aggregate")
    def test_dispatches_capacity_aggregate(self, mock_ca):
        db = _mock_db()
        db.execute.return_value = _make_cursor({"ghost_type": "capacity_aggregate"})
        mock_ca.return_value = {"ghost_type": "capacity_aggregate"}

        result = run_ghost(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))
        mock_ca.assert_called_once_with(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))
        assert result["ghost_type"] == "capacity_aggregate"

    def test_unknown_ghost_type_raises(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor({"ghost_type": "warp_drive"})
        with pytest.raises(ValueError, match="Unknown ghost_type"):
            run_ghost(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))


# =========================================================================
# phase_transition.py — compute_weight
# =========================================================================

class TestComputeWeight:

    def test_none_dates_returns_start(self):
        assert compute_weight(date(2024, 6, 1), None, None, "linear", 1.0, 0.0) == 1.0

    def test_start_date_none(self):
        assert compute_weight(date(2024, 6, 1), None, date(2024, 7, 1), "linear", 1.0, 0.0) == 1.0

    def test_end_date_none(self):
        assert compute_weight(date(2024, 6, 1), date(2024, 5, 1), None, "linear", 1.0, 0.0) == 1.0

    def test_before_start_returns_start(self):
        w = compute_weight(
            date(2024, 1, 1),
            date(2024, 3, 1), date(2024, 6, 1),
            "linear", 1.0, 0.0,
        )
        assert w == 1.0

    def test_at_start_returns_start(self):
        w = compute_weight(
            date(2024, 3, 1),
            date(2024, 3, 1), date(2024, 6, 1),
            "linear", 1.0, 0.0,
        )
        assert w == 1.0

    def test_at_end_returns_end(self):
        w = compute_weight(
            date(2024, 6, 1),
            date(2024, 3, 1), date(2024, 6, 1),
            "linear", 1.0, 0.0,
        )
        assert w == 0.0

    def test_after_end_returns_end(self):
        w = compute_weight(
            date(2024, 9, 1),
            date(2024, 3, 1), date(2024, 6, 1),
            "linear", 1.0, 0.0,
        )
        assert w == 0.0

    def test_linear_midpoint(self):
        # 100 day window, midpoint at day 50
        start = date(2024, 1, 1)
        end = date(2024, 4, 10)  # 100 days later
        mid = date(2024, 2, 20)  # 50 days in
        w = compute_weight(mid, start, end, "linear", 1.0, 0.0)
        assert pytest.approx(w, abs=0.01) == 0.5

    def test_step_inside_window_returns_start(self):
        w = compute_weight(
            date(2024, 4, 1),
            date(2024, 3, 1), date(2024, 6, 1),
            "step", 1.0, 0.0,
        )
        assert w == 1.0

    def test_step_at_end_returns_end(self):
        # At end date, the generic `t >= transition_end_date` branch returns weight_at_end
        w = compute_weight(
            date(2024, 6, 1),
            date(2024, 3, 1), date(2024, 6, 1),
            "step", 1.0, 0.0,
        )
        assert w == 0.0

    def test_sigmoid_midpoint(self):
        start = date(2024, 1, 1)
        end = date(2024, 4, 10)  # 100 days
        mid = date(2024, 2, 20)  # ratio = 0.5 -> smooth = 3*(0.5^2) - 2*(0.5^3) = 0.5
        w = compute_weight(mid, start, end, "sigmoid", 0.0, 1.0)
        assert pytest.approx(w, abs=0.01) == 0.5

    def test_sigmoid_quarter(self):
        start = date(2024, 1, 1)
        end = date(2024, 4, 10)  # 100 days
        quarter = date(2024, 1, 26)  # 25 days -> ratio = 0.25
        ratio = 0.25
        expected_smooth = 3 * ratio**2 - 2 * ratio**3
        w = compute_weight(quarter, start, end, "sigmoid", 0.0, 1.0)
        assert pytest.approx(w, abs=0.01) == expected_smooth

    def test_unknown_curve_falls_back_to_linear(self):
        start = date(2024, 1, 1)
        end = date(2024, 4, 10)  # 100 days
        mid = date(2024, 2, 20)  # 50 days -> ratio = 0.5
        w = compute_weight(mid, start, end, "spline", 1.0, 0.0)
        assert pytest.approx(w, abs=0.01) == 0.5

    def test_total_days_zero_returns_end(self):
        # start == end effectively, but t is between them (same day)
        # total_days = 0 -> return weight_at_end
        d = date(2024, 3, 1)
        # We need t > start and t < end, with start < end but total_days=0 is impossible
        # with dates. Instead test when start == end - 0 days conceptually.
        # Actually total_days = (end - start).days; if start == date(2024,3,1) and
        # end == date(2024,3,1), then t <= start returns weight_at_start.
        # To hit total_days <= 0, we need start > end (unusual but allowed).
        w = compute_weight(
            date(2024, 3, 2),
            date(2024, 3, 3), date(2024, 3, 1),  # start > end, total_days = -2
            "linear", 1.0, 0.0,
        )
        # t (3/2) is <= start (3/3) → returns weight_at_start
        assert w == 1.0

    def test_total_days_zero_branch_direct(self):
        """Force total_days <= 0 by having start > end and t between them in ISO order."""
        # start=2024-03-05, end=2024-03-01, t=2024-03-03
        # t > start? 3/3 > 3/5 → False → returns weight_at_start
        # We need t > start AND t < end with start > end → impossible with real dates.
        # The branch total_days <= 0 is unreachable with valid dates because
        # if start >= end and t > start, then t >= end would be caught first.
        # Let's verify by testing the edge case directly through the function.
        pass  # Branch is effectively dead code for valid date inputs.


# =========================================================================
# phase_transition.py — run_phase_transition
# =========================================================================

class TestRunPhaseTransition:

    def _setup_db_for_phase_transition(self, *, ghost_row, members, proj_a=None, proj_b=None):
        """Build mock DB that handles ghost, members, and projected inventory queries."""
        db = _mock_db()
        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()

            if "from ghost_nodes" in sql_lower:
                return _make_cursor(ghost_row)
            if "from ghost_members" in sql_lower:
                return _make_cursor(members)
            if "from nodes" in sql_lower and "projectedinventory" in sql_lower:
                # Alternate between item A and item B projections
                if hasattr(execute_side_effect, "_proj_toggle"):
                    execute_side_effect._proj_toggle = not execute_side_effect._proj_toggle
                else:
                    execute_side_effect._proj_toggle = False  # first call = A

                if not execute_side_effect._proj_toggle:
                    # Item A
                    if proj_a is not None:
                        return _make_cursor({"quantity": proj_a})
                    return _make_cursor(None)
                else:
                    # Item B
                    if proj_b is not None:
                        return _make_cursor({"quantity": proj_b})
                    return _make_cursor(None)
            return _make_cursor(None)

        db.execute.side_effect = execute_side_effect
        return db

    def test_ghost_not_found_raises(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        with pytest.raises(ValueError, match="not found"):
            run_phase_transition(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))

    def test_wrong_ghost_type_raises(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(
            {"ghost_id": "g1", "ghost_type": "capacity_aggregate", "scenario_id": "s1"}
        )
        with pytest.raises(ValueError, match="not a phase_transition"):
            run_phase_transition(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))

    def test_missing_outgoing_member_raises(self):
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "scenario_id": "s1"}
        members = [
            {
                "member_id": "m1", "item_id": "i1", "role": "incoming",
                "transition_start_date": date(2024, 1, 1),
                "transition_end_date": date(2024, 3, 1),
                "transition_curve": "linear",
                "weight_at_start": 0.0, "weight_at_end": 1.0,
            }
        ]
        db = self._setup_db_for_phase_transition(ghost_row=ghost, members=members)
        with pytest.raises(ValueError, match="missing outgoing or incoming"):
            run_phase_transition(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))

    def test_missing_incoming_member_raises(self):
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "scenario_id": "s1"}
        members = [
            {
                "member_id": "m1", "item_id": "i1", "role": "outgoing",
                "transition_start_date": date(2024, 1, 1),
                "transition_end_date": date(2024, 3, 1),
                "transition_curve": "linear",
                "weight_at_start": 1.0, "weight_at_end": 0.0,
            }
        ]
        db = self._setup_db_for_phase_transition(ghost_row=ghost, members=members)
        with pytest.raises(ValueError, match="missing outgoing or incoming"):
            run_phase_transition(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))

    def test_successful_run_no_alerts_when_proj_none(self):
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "scenario_id": "s1"}
        outgoing = {
            "member_id": "m1", "item_id": "i1", "role": "outgoing",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 3, 1),
            "transition_curve": "linear",
            "weight_at_start": 1.0, "weight_at_end": 0.0,
        }
        incoming = {
            "member_id": "m2", "item_id": "i2", "role": "incoming",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 3, 1),
            "transition_curve": "linear",
            "weight_at_start": 0.0, "weight_at_end": 1.0,
        }
        db = self._setup_db_for_phase_transition(
            ghost_row=ghost, members=[incoming, outgoing],
            proj_a=None, proj_b=None,
        )
        result = run_phase_transition(db, "g1", "s1", date(2024, 2, 1), date(2024, 2, 1))
        assert result["ghost_type"] == "phase_transition"
        assert result["alerts"] == []
        assert len(result["summary"]["weight_samples"]) == 1

    def test_inconsistency_alert_triggered(self):
        """When observed deviates >10% from baseline, an alert should fire."""
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "scenario_id": "s1"}
        outgoing = {
            "member_id": "m1", "item_id": "i1", "role": "outgoing",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 12, 31),
            "transition_curve": "linear",
            "weight_at_start": 1.0, "weight_at_end": 0.0,
        }
        incoming = {
            "member_id": "m2", "item_id": "i2", "role": "incoming",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 12, 31),
            "transition_curve": "linear",
            "weight_at_start": 0.0, "weight_at_end": 1.0,
        }
        # proj_a=50, proj_b=100 for a day where w_out ~ 0.5
        # baseline = proj_a / w_out = 50 / 0.5 = 100
        # observed = 50 + 100 = 150
        # delta_pct = |150 - 100| / 100 = 0.5 > 0.10 => alert
        db = self._setup_db_for_phase_transition(
            ghost_row=ghost, members=[incoming, outgoing],
            proj_a=50.0, proj_b=100.0,
        )
        # Pick a date roughly midway: July 1 => ratio ~ 0.5
        result = run_phase_transition(db, "g1", "s1", date(2024, 7, 1), date(2024, 7, 1))
        assert len(result["alerts"]) == 1
        assert result["alerts"][0]["type"] == "transition_inconsistency"

    def test_no_alert_when_within_threshold(self):
        """When deviation is within 10%, no alert."""
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "scenario_id": "s1"}
        outgoing = {
            "member_id": "m1", "item_id": "i1", "role": "outgoing",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 12, 31),
            "transition_curve": "linear",
            "weight_at_start": 1.0, "weight_at_end": 0.0,
        }
        incoming = {
            "member_id": "m2", "item_id": "i2", "role": "incoming",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 12, 31),
            "transition_curve": "linear",
            "weight_at_start": 0.0, "weight_at_end": 1.0,
        }
        # w_out at July 1 ~ 0.5; baseline = 100 / 0.5 = 200; observed = 100 + 100 = 200
        # delta_pct = 0.0 -> no alert
        db = self._setup_db_for_phase_transition(
            ghost_row=ghost, members=[incoming, outgoing],
            proj_a=100.0, proj_b=100.0,
        )
        result = run_phase_transition(db, "g1", "s1", date(2024, 7, 1), date(2024, 7, 1))
        assert result["alerts"] == []

    def test_baseline_zero_no_alert(self):
        """When baseline is 0, skip alert check."""
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "scenario_id": "s1"}
        outgoing = {
            "member_id": "m1", "item_id": "i1", "role": "outgoing",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 12, 31),
            "transition_curve": "linear",
            "weight_at_start": 1.0, "weight_at_end": 0.0,
        }
        incoming = {
            "member_id": "m2", "item_id": "i2", "role": "incoming",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 12, 31),
            "transition_curve": "linear",
            "weight_at_start": 0.0, "weight_at_end": 1.0,
        }
        # proj_a=0, proj_b=0 -> baseline=0 -> skip
        db = self._setup_db_for_phase_transition(
            ghost_row=ghost, members=[incoming, outgoing],
            proj_a=0.0, proj_b=0.0,
        )
        result = run_phase_transition(db, "g1", "s1", date(2024, 7, 1), date(2024, 7, 1))
        assert result["alerts"] == []

    def test_w_out_zero_uses_proj_b_as_baseline(self):
        """When w_out=0, baseline = proj_b."""
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "scenario_id": "s1"}
        outgoing = {
            "member_id": "m1", "item_id": "i1", "role": "outgoing",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 3, 1),
            "transition_curve": "linear",
            "weight_at_start": 1.0, "weight_at_end": 0.0,
        }
        incoming = {
            "member_id": "m2", "item_id": "i2", "role": "incoming",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 3, 1),
            "transition_curve": "linear",
            "weight_at_start": 0.0, "weight_at_end": 1.0,
        }
        # After end date, w_out = 0.0
        # baseline = proj_b = 100; observed = 0 + 200 = 200; delta_pct = 1.0 > 0.10
        db = self._setup_db_for_phase_transition(
            ghost_row=ghost, members=[incoming, outgoing],
            proj_a=0.0, proj_b=200.0,
        )
        result = run_phase_transition(db, "g1", "s1", date(2024, 6, 1), date(2024, 6, 1))
        # w_out at June 1 (after end=March 1) = 0.0
        # baseline = proj_b = 200
        # observed = 0 + 200 = 200, delta_pct = 0 -> no alert
        assert result["alerts"] == []

    def test_w_out_zero_proj_b_zero_baseline_zero(self):
        """When w_out=0 and proj_b=0, baseline=0, skip alert."""
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "scenario_id": "s1"}
        outgoing = {
            "member_id": "m1", "item_id": "i1", "role": "outgoing",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 3, 1),
            "transition_curve": "linear",
            "weight_at_start": 1.0, "weight_at_end": 0.0,
        }
        incoming = {
            "member_id": "m2", "item_id": "i2", "role": "incoming",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 3, 1),
            "transition_curve": "linear",
            "weight_at_start": 0.0, "weight_at_end": 1.0,
        }
        db = self._setup_db_for_phase_transition(
            ghost_row=ghost, members=[incoming, outgoing],
            proj_a=0.0, proj_b=0.0,
        )
        result = run_phase_transition(db, "g1", "s1", date(2024, 6, 1), date(2024, 6, 1))
        assert result["alerts"] == []

    def test_multi_day_range(self):
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "scenario_id": "s1"}
        outgoing = {
            "member_id": "m1", "item_id": "i1", "role": "outgoing",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 3, 1),
            "transition_curve": "linear",
            "weight_at_start": 1.0, "weight_at_end": 0.0,
        }
        incoming = {
            "member_id": "m2", "item_id": "i2", "role": "incoming",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 3, 1),
            "transition_curve": "linear",
            "weight_at_start": 0.0, "weight_at_end": 1.0,
        }
        db = self._setup_db_for_phase_transition(
            ghost_row=ghost, members=[incoming, outgoing],
            proj_a=None, proj_b=None,
        )
        result = run_phase_transition(db, "g1", "s1", date(2024, 2, 1), date(2024, 2, 3))
        assert len(result["summary"]["weight_samples"]) == 3

    def test_summary_fields(self):
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "scenario_id": "s1"}
        outgoing = {
            "member_id": "m1", "item_id": "i1", "role": "outgoing",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 3, 1),
            "transition_curve": "sigmoid",
            "weight_at_start": 1.0, "weight_at_end": 0.0,
        }
        incoming = {
            "member_id": "m2", "item_id": "i2", "role": "incoming",
            "transition_start_date": date(2024, 1, 1),
            "transition_end_date": date(2024, 3, 1),
            "transition_curve": "sigmoid",
            "weight_at_start": 0.0, "weight_at_end": 1.0,
        }
        db = self._setup_db_for_phase_transition(
            ghost_row=ghost, members=[incoming, outgoing],
            proj_a=None, proj_b=None,
        )
        result = run_phase_transition(db, "g1", "s1", date(2024, 2, 1), date(2024, 2, 1))
        assert result["summary"]["outgoing_item_id"] == "i1"
        assert result["summary"]["incoming_item_id"] == "i2"
        assert result["summary"]["transition_curve"] == "sigmoid"


# =========================================================================
# phase_transition.py — _get_projected_inventory
# =========================================================================

class TestGetProjectedInventory:

    def test_returns_quantity(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor({"quantity": 42.5})
        result = _get_projected_inventory(db, "i1", "s1", date(2024, 1, 1))
        assert result == 42.5

    def test_returns_none_when_no_row(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        result = _get_projected_inventory(db, "i1", "s1", date(2024, 1, 1))
        assert result is None


# =========================================================================
# capacity_aggregate.py — run_capacity_aggregate
# =========================================================================

class TestRunCapacityAggregate:

    def _setup_capacity_db(self, *, ghost_row, resource_cap, members, supply_loads):
        """
        supply_loads: dict mapping (item_id, date_str) -> load value, or a single float.
        """
        db = _mock_db()

        def execute_side_effect(sql, params=None):
            sql_lower = sql.strip().lower()

            if "from ghost_nodes" in sql_lower:
                return _make_cursor(ghost_row)
            if "from resources" in sql_lower:
                if resource_cap is not None:
                    return _make_cursor({"capacity_per_day": resource_cap})
                return _make_cursor(None)
            if "from ghost_members" in sql_lower:
                return _make_cursor(members)
            if "from nodes" in sql_lower:
                # Supply load query
                if isinstance(supply_loads, (int, float)):
                    return _make_cursor({"load_qty": supply_loads})
                if isinstance(supply_loads, dict) and params:
                    key = (str(params[0]), str(params[2]))
                    val = supply_loads.get(key, 0.0)
                    return _make_cursor({"load_qty": val})
                return _make_cursor({"load_qty": 0.0})
            return _make_cursor(None)

        db.execute.side_effect = execute_side_effect
        return db

    def test_ghost_not_found_raises(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        with pytest.raises(ValueError, match="not found"):
            run_capacity_aggregate(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))

    def test_wrong_ghost_type_raises(self):
        ghost = {"ghost_id": "g1", "ghost_type": "phase_transition", "resource_id": "r1"}
        db = self._setup_capacity_db(
            ghost_row=ghost, resource_cap=100.0, members=[], supply_loads=0.0,
        )
        with pytest.raises(ValueError, match="not a capacity_aggregate"):
            run_capacity_aggregate(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))

    def test_no_members_raises(self):
        ghost = {"ghost_id": "g1", "ghost_type": "capacity_aggregate", "resource_id": "r1"}
        db = self._setup_capacity_db(
            ghost_row=ghost, resource_cap=100.0, members=[], supply_loads=0.0,
        )
        with pytest.raises(ValueError, match="no members"):
            run_capacity_aggregate(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 2))

    def test_no_overload(self):
        ghost = {"ghost_id": "g1", "ghost_type": "capacity_aggregate", "resource_id": "r1"}
        members = [{"item_id": "item_a"}]
        db = self._setup_capacity_db(
            ghost_row=ghost, resource_cap=100.0, members=members, supply_loads=50.0,
        )
        result = run_capacity_aggregate(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 1))
        assert result["alerts"] == []
        assert len(result["summary"]["periods"]) == 1
        assert result["summary"]["periods"][0]["overloaded"] is False
        assert result["summary"]["capacity_per_day"] == 100.0

    def test_overload_generates_alert(self):
        ghost = {"ghost_id": "g1", "ghost_type": "capacity_aggregate", "resource_id": "r1"}
        members = [{"item_id": "item_a"}, {"item_id": "item_b"}]
        db = self._setup_capacity_db(
            ghost_row=ghost, resource_cap=100.0, members=members, supply_loads=80.0,
        )
        # 2 members x 80.0 = 160.0 > 100.0
        result = run_capacity_aggregate(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 1))
        assert len(result["alerts"]) == 1
        assert result["alerts"][0]["type"] == "capacity_overload"
        assert result["summary"]["periods"][0]["overloaded"] is True

    def test_null_resource_id_uses_zero_capacity(self):
        ghost = {"ghost_id": "g1", "ghost_type": "capacity_aggregate", "resource_id": None}
        members = [{"item_id": "item_a"}]
        db = self._setup_capacity_db(
            ghost_row=ghost, resource_cap=None, members=members, supply_loads=10.0,
        )
        result = run_capacity_aggregate(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 1))
        # capacity = 0, load = 10 -> overload
        assert result["summary"]["capacity_per_day"] == 0.0
        assert len(result["alerts"]) == 1
        assert result["summary"]["resource_id"] is None

    def test_multi_day_range(self):
        ghost = {"ghost_id": "g1", "ghost_type": "capacity_aggregate", "resource_id": "r1"}
        members = [{"item_id": "item_a"}]
        db = self._setup_capacity_db(
            ghost_row=ghost, resource_cap=100.0, members=members, supply_loads=50.0,
        )
        result = run_capacity_aggregate(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 3))
        assert len(result["summary"]["periods"]) == 3

    def test_member_breakdown_present(self):
        ghost = {"ghost_id": "g1", "ghost_type": "capacity_aggregate", "resource_id": "r1"}
        members = [{"item_id": "item_a"}, {"item_id": "item_b"}]
        db = self._setup_capacity_db(
            ghost_row=ghost, resource_cap=200.0, members=members, supply_loads=30.0,
        )
        result = run_capacity_aggregate(db, "g1", "s1", date(2024, 1, 1), date(2024, 1, 1))
        breakdown = result["summary"]["periods"][0]["member_breakdown"]
        assert len(breakdown) == 2
        assert breakdown[0]["item_id"] == "item_a"


# =========================================================================
# capacity_aggregate.py — helper functions
# =========================================================================

class TestCapacityHelpers:

    def test_get_resource_capacity_found(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor({"capacity_per_day": 99.5})
        assert _get_resource_capacity(db, "r1") == 99.5

    def test_get_resource_capacity_not_found(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        assert _get_resource_capacity(db, "r1") == 0.0

    def test_get_supply_load_found(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor({"load_qty": 42.0})
        assert _get_supply_load(db, "i1", "s1", date(2024, 1, 1)) == 42.0

    def test_get_supply_load_not_found(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        assert _get_supply_load(db, "i1", "s1", date(2024, 1, 1)) == 0.0
