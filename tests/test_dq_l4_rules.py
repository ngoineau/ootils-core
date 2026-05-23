"""Unit tests for the DB-free L4 rules.

Only `_l4_duplicate_external_id` is unit-testable without DB. The
remaining L4 rules (inter-batch collision, supplier inactive, orphan
items) need real Postgres and are covered by
`tests/integration/test_dq_l4.py`.
"""
from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

from ootils_core.engine.dq.l4_rules import check_l4


def _row(ext_id, row_number=None):
    return (uuid4(), row_number or 1, {"external_id": ext_id})


def _codes(issues):
    return [i.rule_code for i in issues]


# ---------------------------------------------------------------------------
# L4_DUPLICATE_EXTERNAL_ID  (intra-batch, DB-free)
# ---------------------------------------------------------------------------


def test_no_duplicates_no_issues() -> None:
    rows = [_row("A", 1), _row("B", 2), _row("C", 3)]
    issues = check_l4(rows, "items", uuid4(), db=MagicMock())
    # Could fire from other rules but no dup-related issues
    assert "L4_DUPLICATE_EXTERNAL_ID" not in _codes(issues)


def test_duplicate_external_id_in_batch() -> None:
    rows = [_row("DUP", 1), _row("OTHER", 2), _row("DUP", 5)]
    # Use a mock db with empty results for the cross-batch + orphan rules
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = []
    issues = check_l4(rows, "items", uuid4(), db=db)
    dup_issues = [i for i in issues if i.rule_code == "L4_DUPLICATE_EXTERNAL_ID"]
    # Two issues: one per row that bears the dup external_id
    assert len(dup_issues) == 2
    assert {i.row_number for i in dup_issues} == {1, 5}
    assert all(i.severity == "error" for i in dup_issues)
    assert all(i.field_name == "external_id" for i in dup_issues)
    assert all("DUP" in i.raw_value for i in dup_issues)


def test_three_way_duplicate() -> None:
    rows = [_row("X", 1), _row("X", 2), _row("X", 3), _row("Y", 4)]
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = []
    issues = check_l4(rows, "items", uuid4(), db=db)
    dup_issues = [i for i in issues if i.rule_code == "L4_DUPLICATE_EXTERNAL_ID"]
    assert len(dup_issues) == 3
    # Message should mention the count
    for issue in dup_issues:
        assert "3 occurrences" in issue.message


def test_empty_external_id_not_counted_as_duplicate() -> None:
    """Rows missing external_id (caught by L1 already) shouldn't be
    silently grouped together as 'duplicates of nothing'."""
    rows = [
        (uuid4(), 1, {"external_id": ""}),
        (uuid4(), 2, {"external_id": None}),
        (uuid4(), 3, {}),
        (uuid4(), 4, {"external_id": "REAL"}),
    ]
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = []
    issues = check_l4(rows, "items", uuid4(), db=db)
    assert "L4_DUPLICATE_EXTERNAL_ID" not in _codes(issues)


def test_unknown_entity_type_no_issues() -> None:
    rows = [_row("A", 1), _row("A", 2)]
    db = MagicMock()
    issues = check_l4(rows, "weather_data", uuid4(), db=db)
    assert issues == []


def test_entity_with_no_l4_rules_returns_empty() -> None:
    """forecasts and on_hand have no L4 rules in the registry."""
    rows = [_row("A", 1), _row("A", 2)]
    db = MagicMock()
    issues = check_l4(rows, "forecasts", uuid4(), db=db)
    assert issues == []
    issues = check_l4(rows, "on_hand", uuid4(), db=db)
    assert issues == []
