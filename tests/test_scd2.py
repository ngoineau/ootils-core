"""
Unit tests for ootils_core.scd2 — pure SCD2 helpers (ADR-014 D3).

No DB needed. Drives the decision logic with synthetic active-row /
incoming-row dicts.
"""
from __future__ import annotations

from datetime import date

import pytest

from ootils_core.scd2 import (
    Scd2Action,
    decide_action,
    diff_tracked_fields,
)


# ---------------------------------------------------------------------------
# diff_tracked_fields
# ---------------------------------------------------------------------------


class TestDiffTrackedFields:
    TRACKED = ["lead_time_days", "safety_stock_qty", "reorder_point"]

    def test_no_active_row_returns_all_present_fields(self):
        incoming = {"lead_time_days": 7, "safety_stock_qty": 50}
        out = diff_tracked_fields(None, incoming, self.TRACKED)
        assert out == {"lead_time_days": 7, "safety_stock_qty": 50}

    def test_active_row_identical_returns_empty(self):
        active = {"lead_time_days": 7, "safety_stock_qty": 50, "reorder_point": 100}
        incoming = {"lead_time_days": 7, "safety_stock_qty": 50, "reorder_point": 100}
        assert diff_tracked_fields(active, incoming, self.TRACKED) == {}

    def test_partial_push_does_not_clear_unspecified_fields(self):
        active = {"lead_time_days": 7, "safety_stock_qty": 50, "reorder_point": 100}
        incoming = {"lead_time_days": 10}  # didn't push safety/reorder
        out = diff_tracked_fields(active, incoming, self.TRACKED)
        # Only the diff for what was pushed
        assert out == {"lead_time_days": 10}

    def test_value_changed_returns_only_diff(self):
        active = {"lead_time_days": 7, "safety_stock_qty": 50}
        incoming = {"lead_time_days": 7, "safety_stock_qty": 80}
        out = diff_tracked_fields(active, incoming, self.TRACKED)
        assert out == {"safety_stock_qty": 80}

    def test_untracked_field_ignored(self):
        active = {"lead_time_days": 7, "weird_field": "a"}
        incoming = {"lead_time_days": 7, "weird_field": "b"}
        out = diff_tracked_fields(active, incoming, self.TRACKED)
        assert out == {}

    def test_none_value_explicitly_pushed_is_a_change(self):
        """Client clearing a field by passing None ≠ omission."""
        active = {"safety_stock_qty": 50, "lead_time_days": 7}
        incoming = {"safety_stock_qty": None, "lead_time_days": 7}
        out = diff_tracked_fields(active, incoming, self.TRACKED)
        assert out == {"safety_stock_qty": None}


# ---------------------------------------------------------------------------
# decide_action
# ---------------------------------------------------------------------------


class TestDecideAction:
    TRACKED = ["lead_time_days", "safety_stock_qty"]
    TODAY = date(2026, 5, 24)

    def test_no_active_row_creates(self):
        d = decide_action(
            active_row=None,
            incoming={"lead_time_days": 7},
            tracked=self.TRACKED,
            today=self.TODAY,
        )
        assert d.action == Scd2Action.CREATED
        assert d.changed_fields == {"lead_time_days": 7}

    def test_identical_active_row_is_noop(self):
        active = {"effective_from": date(2026, 5, 1), "lead_time_days": 7, "safety_stock_qty": 50}
        incoming = {"lead_time_days": 7, "safety_stock_qty": 50}
        d = decide_action(active, incoming, self.TRACKED, self.TODAY)
        assert d.action == Scd2Action.NOOP
        assert d.changed_fields == {}

    def test_cross_day_change_rotates(self):
        active = {"effective_from": date(2026, 5, 1), "lead_time_days": 7}
        incoming = {"lead_time_days": 10}
        d = decide_action(active, incoming, self.TRACKED, self.TODAY)
        assert d.action == Scd2Action.ROTATED
        assert d.changed_fields == {"lead_time_days": 10}

    def test_same_day_change_updates_in_place(self):
        active = {"effective_from": self.TODAY, "lead_time_days": 7}
        incoming = {"lead_time_days": 10}
        d = decide_action(active, incoming, self.TRACKED, self.TODAY)
        assert d.action == Scd2Action.UPDATED_INPLACE
        assert d.changed_fields == {"lead_time_days": 10}

    def test_partial_push_with_only_unchanged_field_is_noop(self):
        active = {"effective_from": date(2026, 5, 1), "lead_time_days": 7, "safety_stock_qty": 50}
        incoming = {"lead_time_days": 7}  # only re-stated the existing value
        d = decide_action(active, incoming, self.TRACKED, self.TODAY)
        assert d.action == Scd2Action.NOOP
        assert d.changed_fields == {}

    def test_partial_push_with_one_changed_field_rotates(self):
        active = {"effective_from": date(2026, 5, 1), "lead_time_days": 7, "safety_stock_qty": 50}
        incoming = {"safety_stock_qty": 80}  # only push the changed field
        d = decide_action(active, incoming, self.TRACKED, self.TODAY)
        assert d.action == Scd2Action.ROTATED
        assert d.changed_fields == {"safety_stock_qty": 80}

    @pytest.mark.parametrize("active_from_offset_days", [1, 7, 365])
    def test_any_day_in_the_past_rotates_not_updates_in_place(self, active_from_offset_days):
        from datetime import timedelta
        active = {
            "effective_from": self.TODAY - timedelta(days=active_from_offset_days),
            "lead_time_days": 7,
        }
        incoming = {"lead_time_days": 10}
        d = decide_action(active, incoming, self.TRACKED, self.TODAY)
        assert d.action == Scd2Action.ROTATED
