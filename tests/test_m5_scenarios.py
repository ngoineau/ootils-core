"""
tests/test_m5_scenarios.py — Sprint M5 Scenario management tests.

Covers:
  - create_scenario creates a scenario with the correct parent
  - apply_override creates the override and the policy_changed event
  - diff returns the correct differences
  - diff returns empty list when there are no differences
  - Override with same field → upsert (no duplicate)
  - promote archives the scenario and creates a scenario_merge event

All tests use mocks — no real DB required.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from unittest.mock import MagicMock, call, patch
from uuid import UUID, uuid4

import pytest

from ootils_core.models import (
    Scenario,
    ScenarioDiff,
    ScenarioOverride,
)
from ootils_core.engine.scenario.manager import ScenarioManager


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

BASELINE_ID = UUID("00000000-0000-0000-0000-000000000001")


def _uuid(n: int) -> UUID:
    """Deterministic UUID from int for test readability."""
    return UUID(f"00000000-0000-0000-0000-{n:012d}")


def make_mock_db() -> MagicMock:
    """Return a mock psycopg3 Connection."""
    db = MagicMock()
    # Default: execute(...).fetchone() → None, fetchall() → []
    db.execute.return_value.fetchone.return_value = None
    db.execute.return_value.fetchall.return_value = []
    return db


def make_node_row(
    node_id: UUID,
    scenario_id: UUID,
    node_type: str = "ProjectedInventory",
    closing_stock: Optional[str] = None,
    opening_stock: Optional[str] = None,
    inflows: Optional[str] = None,
    outflows: Optional[str] = None,
    has_shortage: bool = False,
    shortage_qty: str = "0",
    item_id: Optional[UUID] = None,
    location_id: Optional[UUID] = None,
    time_span_start: Optional[date] = None,
    bucket_sequence: Optional[int] = None,
) -> dict:
    """Build a fake DB node row dict."""
    return {
        "node_id": node_id,
        "scenario_id": scenario_id,
        "node_type": node_type,
        "item_id": item_id,
        "location_id": location_id,
        "quantity": None,
        "qty_uom": None,
        "time_grain": "day",
        "time_ref": None,
        "time_span_start": time_span_start,
        "time_span_end": None,
        "is_dirty": False,
        "last_calc_run_id": None,
        "active": True,
        "projection_series_id": None,
        "bucket_sequence": bucket_sequence,
        "opening_stock": opening_stock,
        "inflows": inflows,
        "outflows": outflows,
        "closing_stock": closing_stock,
        "has_shortage": has_shortage,
        "shortage_qty": shortage_qty,
        "has_exact_date_inputs": False,
        "has_week_inputs": False,
        "has_month_inputs": False,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }


# ---------------------------------------------------------------------------
# Test: create_scenario
# ---------------------------------------------------------------------------


class TestCreateScenario:
    def test_creates_scenario_with_correct_parent(self):
        """create_scenario should insert a new scenario and return it."""
        db = make_mock_db()
        # No nodes to copy (fetchall → [])
        db.execute.return_value.fetchall.return_value = []

        manager = ScenarioManager()
        scenario = manager.create_scenario(
            name="Demand Surge",
            parent_scenario_id=BASELINE_ID,
            db=db,
        )

        assert isinstance(scenario, Scenario)
        assert scenario.name == "Demand Surge"
        assert scenario.parent_scenario_id == BASELINE_ID
        assert scenario.is_baseline is False
        assert scenario.status == "active"
        assert isinstance(scenario.scenario_id, UUID)

        # Verify INSERT into scenarios was called
        insert_calls = [
            c for c in db.execute.call_args_list
            if "INSERT INTO scenarios" in str(c)
        ]
        assert len(insert_calls) >= 1

    def test_copies_parent_nodes_to_new_scenario(self):
        """create_scenario should deep-copy parent nodes (and edges) with new IDs."""
        db = make_mock_db()
        parent_id = BASELINE_ID
        parent_node = make_node_row(
            node_id=_uuid(1),
            scenario_id=parent_id,
            closing_stock="100",
        )
        # fetchall call order:
        #   1. SELECT * FROM projection_series  → [] (none to copy)
        #   2. SELECT * FROM nodes              → [parent_node]
        #   3. SELECT * FROM edges              → [] (no edges in this test)
        db.execute.return_value.fetchall.side_effect = [
            [],             # SELECT * FROM projection_series
            [parent_node],  # SELECT * FROM nodes
            [],             # SELECT * FROM edges
        ]

        manager = ScenarioManager()
        scenario = manager.create_scenario(
            name="S1",
            parent_scenario_id=parent_id,
            db=db,
        )

        # At least one INSERT INTO nodes should have been called
        insert_node_calls = [
            c for c in db.execute.call_args_list if "INSERT INTO nodes" in str(c)
        ]
        assert len(insert_node_calls) >= 1

    def test_new_scenario_id_differs_from_parent(self):
        """Resulting scenario_id must be fresh (not the parent's)."""
        db = make_mock_db()
        db.execute.return_value.fetchall.return_value = []

        manager = ScenarioManager()
        scenario = manager.create_scenario("S2", BASELINE_ID, db)

        assert scenario.scenario_id != BASELINE_ID


# ---------------------------------------------------------------------------
# Test: apply_override
# ---------------------------------------------------------------------------


class TestApplyOverride:
    def _make_db_for_override(
        self,
        old_value: Optional[str] = "50",
        override_id: Optional[UUID] = None,
    ) -> MagicMock:
        """
        Build a mock DB that returns expected rows for apply_override:
          - first fetchone → node row with current field value
          - second fetchone (RETURNING override_id) → override row
        """
        db = MagicMock()
        oid = override_id or uuid4()

        node_row = MagicMock()
        node_row.__getitem__ = lambda self, k: old_value if k == "quantity" else None

        override_row = MagicMock()
        override_row.__getitem__ = lambda self, k: oid if k == "override_id" else None

        # Sequence: 1st execute → node SELECT, 2nd → INSERT override RETURNING,
        #           3rd → SELECT override_id, 4th → UPDATE nodes, 5th → INSERT event
        db.execute.return_value.fetchone.side_effect = [
            node_row,
            override_row,
            override_row,
        ]
        return db

    def test_returns_scenario_override(self):
        node_id = _uuid(10)
        scenario_id = _uuid(20)
        db = self._make_db_for_override()

        manager = ScenarioManager()
        result = manager.apply_override(
            scenario_id=scenario_id,
            node_id=node_id,
            field_name="quantity",
            new_value="75",
            applied_by="test-agent",
            db=db,
        )

        assert isinstance(result, ScenarioOverride)
        assert result.scenario_id == scenario_id
        assert result.node_id == node_id
        assert result.field_name == "quantity"
        assert result.new_value == "75"
        assert result.applied_by == "test-agent"

    def test_creates_policy_changed_event(self):
        """apply_override must insert a policy_changed event."""
        node_id = _uuid(10)
        scenario_id = _uuid(20)
        db = self._make_db_for_override()

        manager = ScenarioManager()
        manager.apply_override(
            scenario_id=scenario_id,
            node_id=node_id,
            field_name="quantity",
            new_value="75",
            applied_by=None,
            db=db,
        )

        event_inserts = [
            c for c in db.execute.call_args_list
            if "policy_changed" in str(c)
        ]
        assert len(event_inserts) >= 1

    def test_upserts_override_no_duplicate(self):
        """Applying an override on same (scenario, node, field) should upsert."""
        node_id = _uuid(10)
        scenario_id = _uuid(20)
        db = self._make_db_for_override()

        manager = ScenarioManager()
        manager.apply_override(
            scenario_id=scenario_id,
            node_id=node_id,
            field_name="quantity",
            new_value="75",
            applied_by=None,
            db=db,
        )

        # The SQL must contain ON CONFLICT ... DO UPDATE
        upsert_calls = [
            c for c in db.execute.call_args_list
            if "ON CONFLICT" in str(c) and "scenario_overrides" in str(c)
        ]
        assert len(upsert_calls) >= 1

    def test_rejects_invalid_field_name(self):
        """apply_override should raise ValueError for disallowed field names."""
        db = make_mock_db()
        manager = ScenarioManager()

        with pytest.raises(ValueError, match="not in the allowed override field list"):
            manager.apply_override(
                scenario_id=_uuid(1),
                node_id=_uuid(2),
                field_name="DROP TABLE nodes; --",
                new_value="evil",
                applied_by=None,
                db=db,
            )

    def test_old_value_is_captured(self):
        """old_value should reflect the node's current field value."""
        node_id = _uuid(10)
        scenario_id = _uuid(20)
        db = self._make_db_for_override(old_value="50")

        manager = ScenarioManager()
        result = manager.apply_override(
            scenario_id=scenario_id,
            node_id=node_id,
            field_name="quantity",
            new_value="99",
            applied_by=None,
            db=db,
        )

        assert result.old_value == "50"


# ---------------------------------------------------------------------------
# Test: diff
# ---------------------------------------------------------------------------


class TestDiff:
    SCENARIO_ID = _uuid(100)
    BASELINE_ID_LOCAL = BASELINE_ID
    CALC_RUN_B = _uuid(200)
    CALC_RUN_S = _uuid(201)
    ITEM_ID = _uuid(300)
    LOC_ID = _uuid(301)
    DATE = date(2026, 4, 1)

    def _make_db(
        self,
        baseline_nodes: list[dict],
        scenario_nodes: list[dict],
    ) -> MagicMock:
        """
        Mock DB for diff() called with explicit calc_run IDs (no _latest_calc_run needed).
        fetchall sequence: 1st → baseline nodes, 2nd → scenario nodes.
        """
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = None
        db.execute.return_value.fetchall.side_effect = [
            baseline_nodes,
            scenario_nodes,
        ]
        return db

    def test_returns_diffs_for_changed_fields(self):
        """diff() should return ScenarioDiff entries for changed closing_stock."""
        node_b = make_node_row(
            node_id=_uuid(1),
            scenario_id=self.BASELINE_ID_LOCAL,
            closing_stock="100",
            item_id=self.ITEM_ID,
            location_id=self.LOC_ID,
            time_span_start=self.DATE,
            bucket_sequence=0,
        )
        node_s = make_node_row(
            node_id=_uuid(2),
            scenario_id=self.SCENARIO_ID,
            closing_stock="80",
            item_id=self.ITEM_ID,
            location_id=self.LOC_ID,
            time_span_start=self.DATE,
            bucket_sequence=0,
        )

        db = self._make_db([node_b], [node_s])
        manager = ScenarioManager()
        diffs = manager.diff(
            scenario_id=self.SCENARIO_ID,
            baseline_id=self.BASELINE_ID_LOCAL,
            db=db,
            baseline_calc_run_id=self.CALC_RUN_B,
            scenario_calc_run_id=self.CALC_RUN_S,
        )

        assert len(diffs) >= 1
        fields = [d.field_name for d in diffs]
        assert "closing_stock" in fields

        closing_diff = next(d for d in diffs if d.field_name == "closing_stock")
        assert closing_diff.baseline_value == "100"
        assert closing_diff.scenario_value == "80"
        assert closing_diff.scenario_id == self.SCENARIO_ID

    def test_returns_empty_list_when_no_diff(self):
        """diff() should return [] when all compared fields are identical."""
        node_b = make_node_row(
            node_id=_uuid(1),
            scenario_id=self.BASELINE_ID_LOCAL,
            closing_stock="100",
            opening_stock="120",
            inflows="50",
            outflows="70",
            has_shortage=False,
            shortage_qty="0",
            item_id=self.ITEM_ID,
            location_id=self.LOC_ID,
            time_span_start=self.DATE,
            bucket_sequence=0,
        )
        node_s = make_node_row(
            node_id=_uuid(2),
            scenario_id=self.SCENARIO_ID,
            closing_stock="100",
            opening_stock="120",
            inflows="50",
            outflows="70",
            has_shortage=False,
            shortage_qty="0",
            item_id=self.ITEM_ID,
            location_id=self.LOC_ID,
            time_span_start=self.DATE,
            bucket_sequence=0,
        )

        db = self._make_db([node_b], [node_s])
        manager = ScenarioManager()
        diffs = manager.diff(
            scenario_id=self.SCENARIO_ID,
            baseline_id=self.BASELINE_ID_LOCAL,
            db=db,
            baseline_calc_run_id=self.CALC_RUN_B,
            scenario_calc_run_id=self.CALC_RUN_S,
        )

        assert diffs == []

    def test_diff_returns_scenario_diff_instances(self):
        """All entries returned by diff() should be ScenarioDiff instances."""
        node_b = make_node_row(
            node_id=_uuid(1),
            scenario_id=self.BASELINE_ID_LOCAL,
            closing_stock="100",
            has_shortage=False,
            shortage_qty="0",
            item_id=self.ITEM_ID,
            location_id=self.LOC_ID,
            time_span_start=self.DATE,
            bucket_sequence=0,
        )
        node_s = make_node_row(
            node_id=_uuid(2),
            scenario_id=self.SCENARIO_ID,
            closing_stock="0",
            has_shortage=True,
            shortage_qty="100",
            item_id=self.ITEM_ID,
            location_id=self.LOC_ID,
            time_span_start=self.DATE,
            bucket_sequence=0,
        )

        db = self._make_db([node_b], [node_s])
        manager = ScenarioManager()
        diffs = manager.diff(
            scenario_id=self.SCENARIO_ID,
            baseline_id=self.BASELINE_ID_LOCAL,
            db=db,
            baseline_calc_run_id=self.CALC_RUN_B,
            scenario_calc_run_id=self.CALC_RUN_S,
        )

        for d in diffs:
            assert isinstance(d, ScenarioDiff)


# ---------------------------------------------------------------------------
# Test: promote
# ---------------------------------------------------------------------------


class TestPromote:
    SCENARIO_ID = _uuid(500)
    ITEM_ID = _uuid(600)
    LOC_ID = _uuid(601)
    DATE = date(2026, 4, 1)

    def _make_db_for_promote(
        self,
        override_rows: list[dict],
        scenario_nodes: list[dict],
    ) -> MagicMock:
        db = MagicMock()

        # fetchall sequence:
        #   1st → override rows, 2nd → scenario nodes,
        #   3rd onward → baseline node matches (one per override)
        baseline_node_row = MagicMock()
        baseline_node_row.__getitem__ = lambda self, k: _uuid(999) if k == "node_id" else None

        fetchall_side_effects = (
            [override_rows, scenario_nodes]
            + [[baseline_node_row]] * len(override_rows)
        )
        db.execute.return_value.fetchall.side_effect = fetchall_side_effects
        db.execute.return_value.fetchone.return_value = None

        return db

    def test_archives_scenario_on_promote(self):
        """promote() should UPDATE scenarios.status = 'archived'."""
        scenario_node = make_node_row(
            node_id=_uuid(50),
            scenario_id=self.SCENARIO_ID,
            item_id=self.ITEM_ID,
            location_id=self.LOC_ID,
            time_span_start=self.DATE,
            bucket_sequence=0,
        )
        override = {
            "node_id": _uuid(50),
            "field_name": "quantity",
            "new_value": "200",
        }

        db = self._make_db_for_promote([override], [scenario_node])
        manager = ScenarioManager()
        manager.promote(scenario_id=self.SCENARIO_ID, db=db)

        archive_calls = [
            c for c in db.execute.call_args_list
            if "archived" in str(c) and "UPDATE scenarios" in str(c)
        ]
        assert len(archive_calls) >= 1

    def test_creates_scenario_merge_event(self):
        """promote() should insert a scenario_merge event."""
        db = self._make_db_for_promote([], [])
        manager = ScenarioManager()
        manager.promote(scenario_id=self.SCENARIO_ID, db=db)

        merge_events = [
            c for c in db.execute.call_args_list
            if "scenario_merge" in str(c)
        ]
        assert len(merge_events) >= 1

    def test_promote_with_no_overrides(self):
        """promote() with zero overrides should still archive and create event."""
        db = self._make_db_for_promote([], [])
        manager = ScenarioManager()
        manager.promote(scenario_id=self.SCENARIO_ID, db=db)

        archive_calls = [
            c for c in db.execute.call_args_list
            if "archived" in str(c)
        ]
        assert len(archive_calls) >= 1
