"""
Tests for llm_reporter.py.

Covers: generate_llm_report (OpenAI path, fallback, import error, API error),
_build_issues_context, _fallback_report (all branches).
"""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch, PropertyMock
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.dq.agent.llm_reporter import (
    generate_llm_report,
    _build_issues_context,
    _fallback_report,
    LLMReport,
    OPENAI_MODEL,
)
from ootils_core.engine.dq.agent.stat_rules import AgentIssue


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
        impact_score=5.0,
        affected_items=["ITEM-1", "ITEM-2", "ITEM-3", "ITEM-4", "ITEM-5", "ITEM-6"],
        active_shortages_count=2,
    )
    defaults.update(overrides)
    return AgentIssue(**defaults)


# =========================================================================
# _build_issues_context
# =========================================================================

class TestBuildIssuesContext:

    def test_builds_json_string(self):
        issues = [_make_issue(impact_score=10.0), _make_issue(impact_score=5.0)]
        result = _build_issues_context(issues)
        data = json.loads(result)
        assert len(data) == 2
        # Sorted by impact_score descending
        assert data[0]["impact_score"] == 10.0
        assert data[1]["impact_score"] == 5.0

    def test_caps_at_max_issues(self):
        issues = [_make_issue(impact_score=float(i)) for i in range(20)]
        result = _build_issues_context(issues, max_issues=5)
        data = json.loads(result)
        assert len(data) == 5

    def test_caps_affected_items_at_5(self):
        issue = _make_issue(affected_items=[f"ITEM-{i}" for i in range(10)])
        result = _build_issues_context([issue])
        data = json.loads(result)
        assert len(data[0]["affected_items"]) == 5

    def test_empty_issues(self):
        result = _build_issues_context([])
        data = json.loads(result)
        assert data == []

    def test_none_impact_score_sorts_as_zero(self):
        i1 = _make_issue(impact_score=None)
        i2 = _make_issue(impact_score=3.0)
        result = _build_issues_context([i1, i2])
        data = json.loads(result)
        assert data[0]["impact_score"] == 3.0


# =========================================================================
# _fallback_report
# =========================================================================

class TestFallbackReport:

    def test_basic_report(self):
        issues = [
            _make_issue(severity="error", rule_code="R1", message="bad data"),
            _make_issue(severity="warning", rule_code="R2", message="suspicious"),
        ]
        report = _fallback_report(issues, "purchase_orders")
        assert isinstance(report, LLMReport)
        assert report.llm_available is False
        assert report.model_used is None
        assert "2 issues" in report.narrative
        assert "1 erreurs" in report.narrative
        assert "R1" in report.narrative

    def test_no_critical_issues(self):
        issues = [_make_issue(severity="warning", rule_code="R1")]
        report = _fallback_report(issues, "items")
        assert "Aucune erreur critique" in report.narrative

    def test_no_issues_at_all(self):
        report = _fallback_report([], "items")
        assert "0 issues" in report.narrative
        assert "Aucune issue" in report.narrative
        assert "Aucune erreur critique" in report.narrative
        assert report.priority_actions == []

    def test_priority_actions_with_critical(self):
        issues = [_make_issue(severity="error", rule_code="R1")]
        report = _fallback_report(issues, "items")
        assert any("critique" in a for a in report.priority_actions)

    def test_priority_actions_with_shortages(self):
        issue = _make_issue(
            severity="warning",
            rule_code="STAT_SPIKE",
            field_name="lead_time",
            active_shortages_count=3,
            impact_score=10.0,
        )
        report = _fallback_report([issue], "supplier_items")
        assert any("shortage" in a for a in report.priority_actions)

    def test_no_priority_actions_when_no_shortages(self):
        issue = _make_issue(
            severity="warning",
            rule_code="R1",
            active_shortages_count=0,
            impact_score=1.0,
        )
        report = _fallback_report([issue], "items")
        # No critical, no shortages -> no priority actions
        assert report.priority_actions == []

    def test_rules_summary(self):
        issues = [
            _make_issue(rule_code="A"),
            _make_issue(rule_code="A"),
            _make_issue(rule_code="B"),
        ]
        report = _fallback_report(issues, "items")
        assert "A: 2" in report.narrative
        assert "B: 1" in report.narrative

    def test_critical_issues_capped_at_5(self):
        issues = [_make_issue(severity="error", rule_code=f"R{i}") for i in range(10)]
        report = _fallback_report(issues, "items")
        # Count bullet points in narrative
        critical_lines = [l for l in report.narrative.split("\n") if l.startswith("- **")]
        assert len(critical_lines) == 5


# =========================================================================
# generate_llm_report
# =========================================================================

class TestGenerateLlmReport:

    def test_no_api_key_uses_fallback(self):
        with patch.dict(os.environ, {}, clear=True):
            # Ensure OPENAI_API_KEY is not set
            os.environ.pop("OPENAI_API_KEY", None)
            issues = [_make_issue()]
            report = generate_llm_report(issues, "items", uuid4(), 10)
            assert report.llm_available is False
            assert report.model_used is None

    @patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test"})
    def test_import_error_uses_fallback(self):
        with patch("builtins.__import__", side_effect=_import_mock_raising_import_error):
            issues = [_make_issue()]
            report = generate_llm_report(issues, "items", uuid4(), 10)
            assert report.llm_available is False

    @patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test"})
    @patch("openai.OpenAI", create=True)
    def test_successful_llm_call(self, MockOpenAI=None):
        """Test the full OpenAI success path."""
        # We need to mock the import and the client
        issue = _make_issue()
        issue_id_str = str(issue.issue_id)

        response_data = {
            "narrative": "# DQ Report\nAll fine.",
            "priority_actions": ["Fix nulls", "Review spikes"],
            "issue_explanations": {
                issue_id_str: {
                    "explanation": "This matters because...",
                    "suggestion": "Fix by..."
                }
            },
        }

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps(response_data)
        mock_client.chat.completions.create.return_value = mock_response

        with patch("openai.OpenAI", return_value=mock_client):
            report = generate_llm_report([issue], "purchase_orders", uuid4(), 50)

        assert report.llm_available is True
        assert report.model_used == OPENAI_MODEL
        assert report.narrative == "# DQ Report\nAll fine."
        assert report.priority_actions == ["Fix nulls", "Review spikes"]
        assert issue.llm_explanation == "This matters because..."
        assert issue.llm_suggestion == "Fix by..."

    @patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test"})
    def test_api_error_uses_fallback(self):
        """Test that API exceptions fall back gracefully."""
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = RuntimeError("API timeout")

        with patch("openai.OpenAI", return_value=mock_client):
            issues = [_make_issue()]
            report = generate_llm_report(issues, "items", uuid4(), 10)

        assert report.llm_available is False
        assert report.model_used is None

    @patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test"})
    def test_llm_response_without_optional_fields(self):
        """Test when LLM response is missing optional fields."""
        response_data = {}  # Empty response

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps(response_data)
        mock_client.chat.completions.create.return_value = mock_response

        with patch("openai.OpenAI", return_value=mock_client):
            issues = [_make_issue()]
            report = generate_llm_report(issues, "items", uuid4(), 10)

        assert report.narrative == ""
        assert report.priority_actions == []
        assert report.issue_explanations == {}

    @patch.dict(os.environ, {"OPENAI_API_KEY": "sk-test"})
    def test_issue_without_matching_explanation(self):
        """Issue IDs not in explanations should not get llm fields set."""
        response_data = {
            "narrative": "report",
            "priority_actions": [],
            "issue_explanations": {
                "nonexistent-id": {"explanation": "x", "suggestion": "y"}
            },
        }

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = json.dumps(response_data)
        mock_client.chat.completions.create.return_value = mock_response

        with patch("openai.OpenAI", return_value=mock_client):
            issue = _make_issue()
            report = generate_llm_report([issue], "items", uuid4(), 10)

        assert issue.llm_explanation is None
        assert issue.llm_suggestion is None


def _import_mock_raising_import_error(name, *args, **kwargs):
    """Mock __import__ that raises ImportError for openai."""
    if name == "openai":
        raise ImportError("No module named 'openai'")
    return __builtins__.__import__(name, *args, **kwargs) if hasattr(__builtins__, '__import__') else None
