"""
Tests for DQ Agent dispatcher (agent.py).

Covers: run_dq_agent happy path, failure path, _load_existing_issues,
_get_entity_type, _persist_agent_run, _persist_new_issues, _enrich_existing_issues.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.dq.agent.agent import (
    AgentResult,
    run_dq_agent,
    _load_existing_issues,
    _get_entity_type,
    _persist_agent_run,
    _persist_new_issues,
    _enrich_existing_issues,
)
from ootils_core.engine.dq.agent.stat_rules import AgentIssue
from ootils_core.engine.dq.agent.llm_reporter import LLMReport


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_db():
    db = MagicMock()
    return db


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
        message="test issue",
        impact_score=2.5,
        affected_items=["ITEM-1"],
        active_shortages_count=1,
    )
    defaults.update(overrides)
    return AgentIssue(**defaults)


# =========================================================================
# _load_existing_issues
# =========================================================================

class TestLoadExistingIssues:

    def test_loads_issues_from_db(self):
        batch_id = uuid4()
        row_id = uuid4()
        issue_id = uuid4()
        db = _mock_db()
        db.execute.return_value = _make_cursor([{
            "issue_id": str(issue_id),
            "batch_id": str(batch_id),
            "row_id": str(row_id),
            "row_number": 5,
            "dq_level": 1,
            "rule_code": "L1_NULL",
            "severity": "error",
            "field_name": "qty",
            "raw_value": None,
            "message": "field is null",
        }])
        issues = _load_existing_issues(db, batch_id)
        assert len(issues) == 1
        assert issues[0].issue_id == issue_id
        assert issues[0].row_id == row_id
        assert issues[0].row_number == 5

    def test_loads_issue_with_no_row_id(self):
        batch_id = uuid4()
        issue_id = uuid4()
        db = _mock_db()
        db.execute.return_value = _make_cursor([{
            "issue_id": str(issue_id),
            "batch_id": str(batch_id),
            "row_id": None,
            "row_number": None,
            "dq_level": 2,
            "rule_code": "L2_CROSS",
            "severity": "warning",
            "field_name": None,
            "raw_value": None,
            "message": "cross check failed",
        }])
        issues = _load_existing_issues(db, batch_id)
        assert len(issues) == 1
        assert issues[0].row_id is None

    def test_empty_batch(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([])
        issues = _load_existing_issues(db, uuid4())
        assert issues == []


# =========================================================================
# _get_entity_type
# =========================================================================

class TestGetEntityType:

    def test_returns_entity_type_and_total_rows(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor({"entity_type": "purchase_orders", "total_rows": 100})
        result = _get_entity_type(db, uuid4())
        assert result == ("purchase_orders", 100)

    def test_returns_empty_when_not_found(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        result = _get_entity_type(db, uuid4())
        assert result == ("", 0)


# =========================================================================
# _persist_agent_run
# =========================================================================

class TestPersistAgentRun:

    def test_inserts_run_record(self):
        db = _mock_db()
        run_id = uuid4()
        batch_id = uuid4()
        now = datetime.now(timezone.utc)
        _persist_agent_run(
            db,
            run_id=run_id,
            batch_id=batch_id,
            status="completed",
            started_at=now,
            completed_at=now,
            summary={"issues_count": 5},
            narrative="All good",
            model_used="gpt-4.1-mini",
        )
        db.execute.assert_called_once()
        args = db.execute.call_args
        assert run_id in args[0][1]
        assert batch_id in args[0][1]


# =========================================================================
# _persist_new_issues
# =========================================================================

class TestPersistNewIssues:

    def test_inserts_each_issue(self):
        db = _mock_db()
        batch_id = uuid4()
        run_id = uuid4()
        issues = [_make_issue(batch_id=batch_id), _make_issue(batch_id=batch_id)]
        _persist_new_issues(db, batch_id, run_id, issues)
        assert db.execute.call_count == 2

    def test_empty_list_no_inserts(self):
        db = _mock_db()
        _persist_new_issues(db, uuid4(), uuid4(), [])
        db.execute.assert_not_called()


# =========================================================================
# _enrich_existing_issues
# =========================================================================

class TestEnrichExistingIssues:

    def test_updates_each_issue(self):
        db = _mock_db()
        run_id = uuid4()
        issues = [_make_issue(), _make_issue()]
        _enrich_existing_issues(db, run_id, issues)
        assert db.execute.call_count == 2

    def test_empty_list_no_updates(self):
        db = _mock_db()
        _enrich_existing_issues(db, uuid4(), [])
        db.execute.assert_not_called()


# =========================================================================
# run_dq_agent — happy path
# =========================================================================

class TestRunDqAgent:

    @patch("ootils_core.engine.dq.agent.agent.generate_llm_report")
    @patch("ootils_core.engine.dq.agent.agent.score_issues")
    @patch("ootils_core.engine.dq.agent.agent.run_temporal_rules")
    @patch("ootils_core.engine.dq.agent.agent.run_stat_rules")
    def test_happy_path(self, mock_stat, mock_temp, mock_score, mock_llm):
        db = _mock_db()
        batch_id = uuid4()

        # Setup db.execute to handle various queries
        existing_issue = _make_issue(batch_id=batch_id, severity="error")
        stat_issue = _make_issue(batch_id=batch_id, severity="warning")
        temp_issue = _make_issue(batch_id=batch_id, severity="warning")

        def db_execute(sql, params=None):
            sql_lower = sql.strip().lower()
            if "insert into dq_agent_runs" in sql_lower and "running" in sql_lower:
                return _make_cursor(None)
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"entity_type": "purchase_orders", "total_rows": 50})
            if "from data_quality_issues" in sql_lower:
                return _make_cursor([{
                    "issue_id": str(existing_issue.issue_id),
                    "batch_id": str(batch_id),
                    "row_id": str(existing_issue.row_id),
                    "row_number": 1,
                    "dq_level": 1,
                    "rule_code": "L1_NULL",
                    "severity": "error",
                    "field_name": "qty",
                    "raw_value": None,
                    "message": "null field",
                }])
            if "insert into data_quality_issues" in sql_lower:
                return _make_cursor(None)
            if "update data_quality_issues" in sql_lower:
                return _make_cursor(None)
            if "update dq_agent_runs" in sql_lower:
                return _make_cursor(None)
            if "insert into dq_agent_runs" in sql_lower:
                return _make_cursor(None)
            return _make_cursor(None)

        db.execute.side_effect = db_execute

        mock_stat.return_value = [stat_issue]
        mock_temp.return_value = [temp_issue]
        mock_score.return_value = None  # modifies in place
        mock_llm.return_value = LLMReport(
            narrative="All clear",
            priority_actions=["Fix nulls"],
            issue_explanations={},
            model_used="gpt-4.1-mini",
            llm_available=True,
        )

        result = run_dq_agent(db, batch_id)

        assert isinstance(result, AgentResult)
        assert result.status == "completed"
        assert result.narrative == "All clear"
        assert result.priority_actions == ["Fix nulls"]
        assert result.model_used == "gpt-4.1-mini"
        assert len(result.issues) == 3  # 1 existing + 1 stat + 1 temporal
        db.commit.assert_called()

    @patch("ootils_core.engine.dq.agent.agent.generate_llm_report")
    @patch("ootils_core.engine.dq.agent.agent.score_issues")
    @patch("ootils_core.engine.dq.agent.agent.run_temporal_rules")
    @patch("ootils_core.engine.dq.agent.agent.run_stat_rules")
    def test_failure_path_marks_run_as_failed(self, mock_stat, mock_temp, mock_score, mock_llm):
        db = _mock_db()
        batch_id = uuid4()

        def db_execute(sql, params=None):
            sql_lower = sql.strip().lower()
            if "insert into dq_agent_runs" in sql_lower:
                return _make_cursor(None)
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"entity_type": "items", "total_rows": 10})
            if "from data_quality_issues" in sql_lower:
                return _make_cursor([])
            if "update dq_agent_runs" in sql_lower:
                return _make_cursor(None)
            return _make_cursor(None)

        db.execute.side_effect = db_execute

        mock_stat.side_effect = RuntimeError("stat boom")

        with pytest.raises(RuntimeError, match="stat boom"):
            run_dq_agent(db, batch_id)

        # Should have committed for the failed status update
        assert db.commit.call_count >= 2

    @patch("ootils_core.engine.dq.agent.agent.generate_llm_report")
    @patch("ootils_core.engine.dq.agent.agent.score_issues")
    @patch("ootils_core.engine.dq.agent.agent.run_temporal_rules")
    @patch("ootils_core.engine.dq.agent.agent.run_stat_rules")
    def test_summary_counts(self, mock_stat, mock_temp, mock_score, mock_llm):
        db = _mock_db()
        batch_id = uuid4()

        def db_execute(sql, params=None):
            sql_lower = sql.strip().lower()
            if "insert into dq_agent_runs" in sql_lower:
                return _make_cursor(None)
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"entity_type": "items", "total_rows": 20})
            if "from data_quality_issues" in sql_lower:
                return _make_cursor([])
            if "insert into data_quality_issues" in sql_lower:
                return _make_cursor(None)
            if "update dq_agent_runs" in sql_lower:
                return _make_cursor(None)
            return _make_cursor(None)

        db.execute.side_effect = db_execute

        error_issue = _make_issue(
            batch_id=batch_id, severity="error",
            affected_items=["A"], active_shortages_count=3,
        )
        warning_issue = _make_issue(
            batch_id=batch_id, severity="warning",
            affected_items=["B"], active_shortages_count=0,
        )

        mock_stat.return_value = [error_issue]
        mock_temp.return_value = [warning_issue]
        mock_score.return_value = None
        mock_llm.return_value = LLMReport(
            narrative="report",
            priority_actions=[],
            issue_explanations={},
            model_used=None,
            llm_available=False,
        )

        result = run_dq_agent(db, batch_id)
        summary = result.summary

        assert summary["total_rows"] == 20
        assert summary["issues_count"] == 2
        assert summary["critical_count"] == 1
        assert summary["warning_count"] == 1
        assert summary["stat_issues"] == 1
        assert summary["temporal_issues"] == 1
        assert summary["active_shortages_impacted"] == 3
