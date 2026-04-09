"""
Tests for impact_scorer.py.

Covers: score_issues, _get_item_ids_for_issue (batch-level and row-level),
_get_active_shortages_for_items, _get_finished_goods_via_bom.
All branches including empty inputs, JSON parse errors, BOM traversal.
"""
from __future__ import annotations

import json
import math
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.dq.agent.impact_scorer import (
    score_issues,
    _get_item_ids_for_issue,
    _get_active_shortages_for_items,
    _get_finished_goods_via_bom,
    SEVERITY_WEIGHTS,
    IssueImpact,
)
from ootils_core.engine.dq.agent.stat_rules import AgentIssue


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_db():
    return MagicMock()


def _make_cursor(rows):
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


def _make_issue(**overrides):
    defaults = dict(
        issue_id=uuid4(),
        batch_id=uuid4(),
        row_id=uuid4(),
        row_number=1,
        dq_level=3,
        rule_code="STAT_TEST",
        severity="error",
        field_name="qty",
        raw_value="42",
        message="test",
    )
    defaults.update(overrides)
    return AgentIssue(**defaults)


# =========================================================================
# _get_item_ids_for_issue
# =========================================================================

class TestGetItemIdsForIssue:

    def test_row_level_with_item_external_id(self):
        db = _mock_db()
        row_id = uuid4()
        batch_id = uuid4()
        db.execute.return_value = _make_cursor(
            {"raw_content": json.dumps({"item_external_id": "ITEM-A", "qty": 10})}
        )
        result = _get_item_ids_for_issue(db, batch_id, row_id)
        assert result == ["ITEM-A"]

    def test_row_level_with_external_id_fallback(self):
        db = _mock_db()
        row_id = uuid4()
        batch_id = uuid4()
        db.execute.return_value = _make_cursor(
            {"raw_content": json.dumps({"external_id": "ITEM-B", "qty": 10})}
        )
        result = _get_item_ids_for_issue(db, batch_id, row_id)
        assert result == ["ITEM-B"]

    def test_row_level_no_item_field(self):
        db = _mock_db()
        row_id = uuid4()
        batch_id = uuid4()
        db.execute.return_value = _make_cursor(
            {"raw_content": json.dumps({"qty": 10})}
        )
        result = _get_item_ids_for_issue(db, batch_id, row_id)
        assert result == []

    def test_row_level_row_not_found(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        result = _get_item_ids_for_issue(db, uuid4(), uuid4())
        assert result == []

    def test_row_level_invalid_json(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor({"raw_content": "NOT JSON{{"})
        result = _get_item_ids_for_issue(db, uuid4(), uuid4())
        assert result == []

    def test_row_level_content_is_dict(self):
        """raw_content is already a dict (not a string)."""
        db = _mock_db()
        db.execute.return_value = _make_cursor(
            {"raw_content": {"item_external_id": "ITEM-C"}}
        )
        result = _get_item_ids_for_issue(db, uuid4(), uuid4())
        assert result == ["ITEM-C"]

    def test_batch_level_when_row_id_none(self):
        db = _mock_db()
        batch_id = uuid4()
        db.execute.return_value = _make_cursor([
            {"raw_content": json.dumps({"item_external_id": "ITEM-1"})},
            {"raw_content": json.dumps({"external_id": "ITEM-2"})},
            {"raw_content": json.dumps({"qty": 5})},  # no item field
        ])
        result = _get_item_ids_for_issue(db, batch_id, None)
        assert set(result) == {"ITEM-1", "ITEM-2"}

    def test_batch_level_deduplicates(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": json.dumps({"item_external_id": "ITEM-X"})},
            {"raw_content": json.dumps({"item_external_id": "ITEM-X"})},
        ])
        result = _get_item_ids_for_issue(db, uuid4(), None)
        assert result == ["ITEM-X"]

    def test_batch_level_caps_at_20(self):
        db = _mock_db()
        rows = [
            {"raw_content": json.dumps({"item_external_id": f"ITEM-{i}"})}
            for i in range(30)
        ]
        db.execute.return_value = _make_cursor(rows)
        result = _get_item_ids_for_issue(db, uuid4(), None)
        assert len(result) <= 20

    def test_batch_level_invalid_json_skipped(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": "bad json"},
            {"raw_content": json.dumps({"item_external_id": "OK"})},
        ])
        result = _get_item_ids_for_issue(db, uuid4(), None)
        assert result == ["OK"]

    def test_batch_level_dict_content(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": {"item_external_id": "DICT-ITEM"}},
        ])
        result = _get_item_ids_for_issue(db, uuid4(), None)
        assert result == ["DICT-ITEM"]


# =========================================================================
# _get_active_shortages_for_items
# =========================================================================

class TestGetActiveShortages:

    def test_empty_list_returns_zero(self):
        db = _mock_db()
        count, affected = _get_active_shortages_for_items(db, [])
        assert count == 0
        assert affected == []

    def test_counts_shortages(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"external_id": "ITEM-1", "shortage_count": 3},
            {"external_id": "ITEM-2", "shortage_count": 0},
        ])
        count, affected = _get_active_shortages_for_items(db, ["ITEM-1", "ITEM-2"])
        assert count == 3
        assert affected == ["ITEM-1"]

    def test_no_shortages(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([])
        count, affected = _get_active_shortages_for_items(db, ["ITEM-X"])
        assert count == 0
        assert affected == []


# =========================================================================
# _get_finished_goods_via_bom
# =========================================================================

class TestGetFinishedGoodsViaBom:

    def test_empty_list_returns_empty(self):
        db = _mock_db()
        result = _get_finished_goods_via_bom(db, [])
        assert result == []

    def test_no_items_found_returns_empty(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([])
        result = _get_finished_goods_via_bom(db, ["NONEXISTENT"])
        assert result == []

    def test_traverses_bom_one_level(self):
        db = _mock_db()
        comp_id = uuid4()
        parent_id = uuid4()

        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from items where external_id" in sql_lower:
                return _make_cursor([{"item_id": str(comp_id), "external_id": "COMP-1"}])
            if "from bom_components" in sql_lower:
                if call_count["n"] == 2:
                    # First BOM query: component -> parent
                    return _make_cursor([{
                        "parent_item_id": str(parent_id),
                        "external_id": "FINISHED-1",
                    }])
                else:
                    # Second BOM query for parent: no further parents
                    return _make_cursor([])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        result = _get_finished_goods_via_bom(db, ["COMP-1"])
        assert "FINISHED-1" in result

    def test_handles_large_batch_of_components(self):
        """Test that queue is processed in batches of 50."""
        db = _mock_db()
        # Create 60 component IDs
        comp_ids = [uuid4() for _ in range(60)]
        items_rows = [
            {"item_id": str(cid), "external_id": f"COMP-{i}"}
            for i, cid in enumerate(comp_ids)
        ]

        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from items where external_id" in sql_lower:
                return _make_cursor(items_rows)
            if "from bom_components" in sql_lower:
                return _make_cursor([])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        ext_ids = [f"COMP-{i}" for i in range(60)]
        result = _get_finished_goods_via_bom(db, ext_ids)
        # No parents found, but we verify no crash with >50 items
        assert result == []


# =========================================================================
# score_issues
# =========================================================================

class TestScoreIssues:

    def test_scores_with_no_shortages(self):
        db = _mock_db()
        batch_id = uuid4()
        issue = _make_issue(batch_id=batch_id, severity="error")

        def execute_side_effect(sql, params=None):
            sql_lower = sql.strip().lower()
            if "from ingest_rows" in sql_lower:
                return _make_cursor(
                    {"raw_content": json.dumps({"item_external_id": "ITEM-1"})}
                )
            if "from items" in sql_lower and "join shortages" in sql_lower:
                return _make_cursor([])
            if "from items where external_id" in sql_lower:
                return _make_cursor([])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect

        result = score_issues(db, batch_id, [issue])
        assert len(result) == 1
        # severity_weight=3.0, shortages=0 => 3.0 * (1 + log(1)) = 3.0
        expected = 3.0 * (1.0 + math.log(1.0))
        assert issue.impact_score == round(expected, 4)
        assert issue.active_shortages_count == 0

    def test_scores_with_shortages(self):
        db = _mock_db()
        batch_id = uuid4()
        issue = _make_issue(batch_id=batch_id, severity="warning")

        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_rows" in sql_lower and "row_id" in sql_lower:
                return _make_cursor(
                    {"raw_content": json.dumps({"item_external_id": "ITEM-1"})}
                )
            if "from items" in sql_lower and "join shortages" in sql_lower:
                return _make_cursor([
                    {"external_id": "ITEM-1", "shortage_count": 5}
                ])
            if "from items where external_id" in sql_lower:
                return _make_cursor([])  # no BOM items
            if "from bom_components" in sql_lower:
                return _make_cursor([])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect

        score_issues(db, batch_id, [issue])
        # severity_weight=1.5, shortages=5 => 1.5 * (1 + log(6))
        expected = 1.5 * (1.0 + math.log(6.0))
        assert issue.impact_score == round(expected, 4)
        assert issue.active_shortages_count == 5
        assert "ITEM-1" in issue.affected_items

    def test_scores_with_fg_items(self):
        """When BOM traversal finds finished goods with shortages, they add to the count."""
        db = _mock_db()
        batch_id = uuid4()
        comp_id = uuid4()
        parent_id = uuid4()
        issue = _make_issue(batch_id=batch_id, severity="info")

        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_rows" in sql_lower and "row_id" in sql_lower:
                return _make_cursor(
                    {"raw_content": json.dumps({"item_external_id": "COMP-1"})}
                )
            if "from items" in sql_lower and "join shortages" in sql_lower:
                # Called twice: once for direct items, once for FG items
                if call_count["n"] <= 3:
                    return _make_cursor([
                        {"external_id": "COMP-1", "shortage_count": 2}
                    ])
                else:
                    return _make_cursor([
                        {"external_id": "FG-1", "shortage_count": 3}
                    ])
            if "from items where external_id" in sql_lower:
                return _make_cursor([
                    {"item_id": str(comp_id), "external_id": "COMP-1"}
                ])
            if "from bom_components" in sql_lower:
                if call_count["n"] <= 5:
                    return _make_cursor([{
                        "parent_item_id": str(parent_id),
                        "external_id": "FG-1",
                    }])
                return _make_cursor([])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect

        score_issues(db, batch_id, [issue])
        # Total shortages = 2 (direct) + 3 (FG) = 5
        assert issue.active_shortages_count == 5
        assert "COMP-1" in issue.affected_items or "FG-1" in issue.affected_items

    def test_unknown_severity_uses_default_weight(self):
        db = _mock_db()
        batch_id = uuid4()
        issue = _make_issue(batch_id=batch_id, severity="debug")

        def execute_side_effect(sql, params=None):
            sql_lower = sql.strip().lower()
            if "from ingest_rows" in sql_lower:
                return _make_cursor({"raw_content": json.dumps({"item_external_id": "X"})})
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect

        score_issues(db, batch_id, [issue])
        # default weight = 1.0
        expected = 1.0 * (1.0 + math.log(1.0))
        assert issue.impact_score == round(expected, 4)

    def test_empty_issues_list(self):
        db = _mock_db()
        result = score_issues(db, uuid4(), [])
        assert result == []

    def test_severity_weights_mapping(self):
        assert SEVERITY_WEIGHTS["error"] == 3.0
        assert SEVERITY_WEIGHTS["warning"] == 1.5
        assert SEVERITY_WEIGHTS["info"] == 0.5
