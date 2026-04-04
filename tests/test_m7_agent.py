"""
test_m7_agent.py — Sprint M7 AI Agent Demo tests.

Tests for OotilsAgent.run() using httpx.MockTransport.
All API calls are intercepted in-memory — no server, no DB required.

Coverage:
  - OotilsAgent.run() returns a valid AgentReport
  - action_type = 'expedite_supply' when causal_path has supply delayed
  - action_type = 'escalate' when no supply node identifiable
  - confidence = 'high' when simulation eliminates the shortage
  - confidence = 'medium' when simulation doesn't eliminate it
  - AgentReport.summary is non-empty
  - Pipeline continues even if explain or simulate return 404
  - AgentReport dataclass fields are correctly populated
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import patch
from uuid import UUID, uuid4

import httpx
import pytest

from ootils_core.agent.demo_agent import OotilsAgent, _find_supply_root_cause, _contains_delay
from ootils_core.models import AgentReport, AgentRecommendation

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

TOKEN = "test-bearer-token"
BASE_URL = "http://test.ootils.local"

SHORTAGE_DELAYED_PO = UUID("cccccccc-0000-0000-0000-000000000001")
SHORTAGE_DELAYED_WO = UUID("cccccccc-0000-0000-0000-000000000002")
SHORTAGE_NO_SUPPLY  = UUID("cccccccc-0000-0000-0000-000000000003")
SHORTAGE_404_EXPLAIN = UUID("cccccccc-0000-0000-0000-000000000004")

PO_NODE = UUID("dddddddd-0000-0000-0000-000000000001")
WO_NODE = UUID("dddddddd-0000-0000-0000-000000000002")

SIM_SCENARIO_RESOLVES = UUID("eeeeeeee-0000-0000-0000-000000000001")
SIM_SCENARIO_PARTIAL  = UUID("eeeeeeee-0000-0000-0000-000000000002")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _json_resp(data: Any, status: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code=status,
        headers={"content-type": "application/json"},
        content=json.dumps(data).encode(),
    )


def _issue(node_id: UUID, shortage_qty: int = 100, severity: str = "high") -> Dict:
    return {
        "node_id": str(node_id),
        "item_id": str(uuid4()),
        "location_id": str(uuid4()),
        "shortage_qty": str(shortage_qty),
        "severity_score": str(shortage_qty * 10),
        "severity": severity,
        "shortage_date": "2026-04-08",
        "explanation_id": str(uuid4()),
        "explanation_url": f"/v1/explain?node_id={node_id}",
        "summary": f"Test issue for node {node_id}",
    }


def _explain_delayed_po(node_id: UUID) -> Dict:
    return {
        "explanation_id": str(uuid4()),
        "target_node_id": str(node_id),
        "target_type": "Shortage",
        "summary": "PO delayed 8 days, demand unmet.",
        "root_cause_node_id": str(PO_NODE),
        "causal_path": [
            {
                "step": 1,
                "node_id": str(uuid4()),
                "node_type": "CustomerOrderDemand",
                "edge_type": "consumes",
                "fact": "Order requires 100u",
            },
            {
                "step": 2,
                "node_id": str(PO_NODE),
                "node_type": "PurchaseOrderSupply",
                "edge_type": "depends_on",
                "fact": "PO-991 delayed from Apr 10 to Apr 18 (8-day gap)",
            },
        ],
    }


def _explain_delayed_wo(node_id: UUID) -> Dict:
    return {
        "explanation_id": str(uuid4()),
        "target_node_id": str(node_id),
        "target_type": "Shortage",
        "summary": "Work order delayed 5 days.",
        "root_cause_node_id": str(WO_NODE),
        "causal_path": [
            {
                "step": 1,
                "node_id": str(uuid4()),
                "node_type": "ForecastDemand",
                "edge_type": "consumes",
                "fact": "Forecast demand of 50u",
            },
            {
                "step": 2,
                "node_id": str(WO_NODE),
                "node_type": "WorkOrderSupply",
                "edge_type": "depends_on",
                "fact": "WO-112 delayed — missed production window",
            },
        ],
    }


def _explain_no_supply(node_id: UUID) -> Dict:
    return {
        "explanation_id": str(uuid4()),
        "target_node_id": str(node_id),
        "target_type": "Shortage",
        "summary": "Demand spike with no supply coverage.",
        "root_cause_node_id": None,
        "causal_path": [
            {
                "step": 1,
                "node_id": str(uuid4()),
                "node_type": "ForecastDemand",
                "edge_type": "consumes",
                "fact": "Spike demand — no active supply node",
            },
            {
                "step": 2,
                "node_id": None,
                "node_type": "PolicyCheck",
                "edge_type": "governed_by",
                "fact": "No substitution rule active",
            },
        ],
    }


def _sim_resolves(shortage_node_id: UUID) -> Dict:
    return {
        "scenario_id": str(SIM_SCENARIO_RESOLVES),
        "scenario_name": "agent-expedite-test",
        "status": "created",
        "override_count": 1,
        "base_scenario_id": "00000000-0000-0000-0000-000000000001",
        "delta": {
            "resolved_shortages": [
                {
                    "node_id": str(shortage_node_id),
                    "before": {"qty": 100, "date": "2026-04-08"},
                    "after": None,
                    "resolution": "shortage eliminated",
                }
            ],
            "new_shortages": [],
        },
    }


def _sim_partial() -> Dict:
    return {
        "scenario_id": str(SIM_SCENARIO_PARTIAL),
        "scenario_name": "agent-expedite-test-2",
        "status": "created",
        "override_count": 1,
        "base_scenario_id": "00000000-0000-0000-0000-000000000001",
        "delta": {
            "resolved_shortages": [],  # didn't help
            "new_shortages": [],
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Mock transport factory
# ─────────────────────────────────────────────────────────────────────────────

class _MockTransport(httpx.BaseTransport):
    """Routes requests based on path+params to pre-built payloads."""

    def __init__(
        self,
        issues: List[Dict],
        explain_map: Dict[str, Dict],
        simulate_responses: List[Dict],
        explain_404_ids: List[str] = None,
    ) -> None:
        self._issues = issues
        self._explain_map = explain_map  # node_id str → payload
        self._simulate_responses = list(simulate_responses)
        self._sim_idx = 0
        self._explain_404_ids = set(explain_404_ids or [])

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = dict(request.url.params)

        if path == "/v1/issues":
            return _json_resp({"issues": self._issues, "total": len(self._issues), "as_of": "now"})

        if path == "/v1/explain":
            node_id = params.get("node_id", "")
            if node_id in self._explain_404_ids:
                return _json_resp({"detail": "not found"}, status=404)
            payload = self._explain_map.get(node_id)
            if payload is None:
                return _json_resp({"detail": "not found"}, status=404)
            return _json_resp(payload)

        if path == "/v1/simulate":
            if self._sim_idx < len(self._simulate_responses):
                resp = self._simulate_responses[self._sim_idx]
                self._sim_idx += 1
                return _json_resp(resp, status=201)
            return _json_resp({"detail": "no more simulate responses"}, status=500)

        return _json_resp({"detail": "not found"}, status=404)


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAgentRunReturnsValidReport:
    """OotilsAgent.run() must always return a well-formed AgentReport."""

    def test_returns_agent_report_instance(self):
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_DELAYED_PO)],
            explain_map={str(SHORTAGE_DELAYED_PO): _explain_delayed_po(SHORTAGE_DELAYED_PO)},
            simulate_responses=[_sim_resolves(SHORTAGE_DELAYED_PO)],
        )
        agent = OotilsAgent()
        report = agent.run(BASE_URL, TOKEN, transport=transport)
        assert isinstance(report, AgentReport)

    def test_report_fields_populated(self):
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_DELAYED_PO)],
            explain_map={str(SHORTAGE_DELAYED_PO): _explain_delayed_po(SHORTAGE_DELAYED_PO)},
            simulate_responses=[_sim_resolves(SHORTAGE_DELAYED_PO)],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)

        assert report.issues_found == 1
        assert report.issues_analyzed == 1
        assert isinstance(report.run_at, datetime)
        assert isinstance(report.recommendations, list)
        assert len(report.recommendations) == 1

    def test_summary_is_non_empty(self):
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_DELAYED_PO)],
            explain_map={str(SHORTAGE_DELAYED_PO): _explain_delayed_po(SHORTAGE_DELAYED_PO)},
            simulate_responses=[_sim_resolves(SHORTAGE_DELAYED_PO)],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        assert report.summary
        assert len(report.summary) > 10

    def test_empty_issues_returns_empty_report(self):
        transport = _MockTransport(issues=[], explain_map={}, simulate_responses=[])
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)

        assert isinstance(report, AgentReport)
        assert report.issues_found == 0
        assert report.issues_analyzed == 0
        assert report.simulations_run == 0
        assert report.recommendations == []
        assert report.summary  # still non-empty


class TestDecisionLogic:
    """Verify the three core decision rules."""

    def test_action_expedite_supply_when_po_delayed(self):
        """causal_path has PurchaseOrderSupply + 'delayed' → expedite_supply."""
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_DELAYED_PO)],
            explain_map={str(SHORTAGE_DELAYED_PO): _explain_delayed_po(SHORTAGE_DELAYED_PO)},
            simulate_responses=[_sim_resolves(SHORTAGE_DELAYED_PO)],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        rec = report.recommendations[0]
        assert rec.action_type == "expedite_supply"

    def test_action_expedite_supply_when_wo_delayed(self):
        """causal_path has WorkOrderSupply + 'delayed' → expedite_supply."""
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_DELAYED_WO)],
            explain_map={str(SHORTAGE_DELAYED_WO): _explain_delayed_wo(SHORTAGE_DELAYED_WO)},
            simulate_responses=[_sim_partial()],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        rec = report.recommendations[0]
        assert rec.action_type == "expedite_supply"

    def test_action_escalate_when_no_supply_node(self):
        """shortage_qty > 0 and no supply node → escalate."""
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_NO_SUPPLY)],
            explain_map={str(SHORTAGE_NO_SUPPLY): _explain_no_supply(SHORTAGE_NO_SUPPLY)},
            simulate_responses=[],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        rec = report.recommendations[0]
        assert rec.action_type == "escalate"

    def test_action_escalate_when_explain_returns_404(self):
        """If explain returns 404, agent should still produce a recommendation (escalate/low)."""
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_404_EXPLAIN)],
            explain_map={},
            simulate_responses=[],
            explain_404_ids=[str(SHORTAGE_404_EXPLAIN)],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        assert len(report.recommendations) == 1
        rec = report.recommendations[0]
        assert rec.action_type == "escalate"
        assert rec.confidence == "low"


class TestConfidenceLevels:
    """Confidence is high when simulation eliminates the shortage, medium otherwise."""

    def test_confidence_high_when_simulation_eliminates_shortage(self):
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_DELAYED_PO)],
            explain_map={str(SHORTAGE_DELAYED_PO): _explain_delayed_po(SHORTAGE_DELAYED_PO)},
            simulate_responses=[_sim_resolves(SHORTAGE_DELAYED_PO)],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        rec = report.recommendations[0]
        assert rec.confidence == "high"

    def test_confidence_medium_when_simulation_does_not_resolve(self):
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_DELAYED_WO)],
            explain_map={str(SHORTAGE_DELAYED_WO): _explain_delayed_wo(SHORTAGE_DELAYED_WO)},
            simulate_responses=[_sim_partial()],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        rec = report.recommendations[0]
        assert rec.confidence == "medium"

    def test_simulation_scenario_id_populated_on_high_confidence(self):
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_DELAYED_PO)],
            explain_map={str(SHORTAGE_DELAYED_PO): _explain_delayed_po(SHORTAGE_DELAYED_PO)},
            simulate_responses=[_sim_resolves(SHORTAGE_DELAYED_PO)],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        rec = report.recommendations[0]
        assert rec.simulation_scenario_id is not None
        assert isinstance(rec.simulation_scenario_id, UUID)

    def test_simulation_count_incremented(self):
        transport = _MockTransport(
            issues=[_issue(SHORTAGE_DELAYED_PO)],
            explain_map={str(SHORTAGE_DELAYED_PO): _explain_delayed_po(SHORTAGE_DELAYED_PO)},
            simulate_responses=[_sim_resolves(SHORTAGE_DELAYED_PO)],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        assert report.simulations_run == 1


class TestPipelineResilience:
    """Agent must not crash when some endpoints return errors."""

    def test_pipeline_continues_when_explain_returns_404(self):
        """Multiple issues: first explain 404, rest succeed. All analyzed."""
        transport = _MockTransport(
            issues=[
                _issue(SHORTAGE_404_EXPLAIN),
                _issue(SHORTAGE_DELAYED_PO),
            ],
            explain_map={
                str(SHORTAGE_DELAYED_PO): _explain_delayed_po(SHORTAGE_DELAYED_PO),
            },
            simulate_responses=[_sim_resolves(SHORTAGE_DELAYED_PO)],
            explain_404_ids=[str(SHORTAGE_404_EXPLAIN)],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        assert report.issues_found == 2
        assert report.issues_analyzed == 2  # both processed, even if one had 404
        assert len(report.recommendations) == 2

    def test_pipeline_continues_when_simulate_returns_404(self):
        """Simulate returns 404 — agent still produces recommendation (medium confidence)."""

        class _Simulate404Transport(httpx.BaseTransport):
            def handle_request(self, request: httpx.Request) -> httpx.Response:
                path = request.url.path
                params = dict(request.url.params)
                if path == "/v1/issues":
                    return _json_resp({
                        "issues": [_issue(SHORTAGE_DELAYED_PO)],
                        "total": 1,
                        "as_of": "now",
                    })
                if path == "/v1/explain":
                    nid = params.get("node_id", "")
                    if nid == str(SHORTAGE_DELAYED_PO):
                        return _json_resp(_explain_delayed_po(SHORTAGE_DELAYED_PO))
                if path == "/v1/simulate":
                    return _json_resp({"detail": "not found"}, status=404)
                return _json_resp({"detail": "not found"}, status=404)

        report = OotilsAgent().run(BASE_URL, TOKEN, transport=_Simulate404Transport())
        assert len(report.recommendations) == 1
        rec = report.recommendations[0]
        # Still recommends expedite (supply delayed), but no sim scenario → medium
        assert rec.action_type == "expedite_supply"
        assert rec.confidence == "medium"
        assert rec.simulation_scenario_id is None

    def test_zero_issues_does_not_crash(self):
        transport = _MockTransport(issues=[], explain_map={}, simulate_responses=[])
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        assert report.issues_found == 0

    def test_full_pipeline_with_mixed_issues(self):
        """
        3 issues:
          1. PO delayed → expedite, simulation resolves → high
          2. WO delayed → expedite, simulation partial → medium
          3. No supply → escalate
        """
        transport = _MockTransport(
            issues=[
                _issue(SHORTAGE_DELAYED_PO),
                _issue(SHORTAGE_DELAYED_WO, shortage_qty=50),
                _issue(SHORTAGE_NO_SUPPLY, shortage_qty=80),
            ],
            explain_map={
                str(SHORTAGE_DELAYED_PO): _explain_delayed_po(SHORTAGE_DELAYED_PO),
                str(SHORTAGE_DELAYED_WO): _explain_delayed_wo(SHORTAGE_DELAYED_WO),
                str(SHORTAGE_NO_SUPPLY): _explain_no_supply(SHORTAGE_NO_SUPPLY),
            },
            simulate_responses=[
                _sim_resolves(SHORTAGE_DELAYED_PO),
                _sim_partial(),
            ],
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)

        assert report.issues_found == 3
        assert report.issues_analyzed == 3
        assert report.simulations_run == 2
        assert len(report.recommendations) == 3

        by_node = {str(r.issue_node_id): r for r in report.recommendations}

        r1 = by_node[str(SHORTAGE_DELAYED_PO)]
        assert r1.action_type == "expedite_supply"
        assert r1.confidence == "high"

        r2 = by_node[str(SHORTAGE_DELAYED_WO)]
        assert r2.action_type == "expedite_supply"
        assert r2.confidence == "medium"

        r3 = by_node[str(SHORTAGE_NO_SUPPLY)]
        assert r3.action_type == "escalate"


class TestRecommendationFields:
    """AgentRecommendation fields must be correctly populated."""

    def _run_single(self, issue_id: UUID, explain: Dict, sim_resp: List[Dict]) -> AgentRecommendation:
        transport = _MockTransport(
            issues=[_issue(issue_id)],
            explain_map={str(issue_id): explain},
            simulate_responses=sim_resp,
        )
        report = OotilsAgent().run(BASE_URL, TOKEN, transport=transport)
        return report.recommendations[0]

    def test_issue_node_id_matches(self):
        rec = self._run_single(
            SHORTAGE_DELAYED_PO,
            _explain_delayed_po(SHORTAGE_DELAYED_PO),
            [_sim_resolves(SHORTAGE_DELAYED_PO)],
        )
        assert rec.issue_node_id == SHORTAGE_DELAYED_PO

    def test_root_cause_summary_non_empty(self):
        rec = self._run_single(
            SHORTAGE_DELAYED_PO,
            _explain_delayed_po(SHORTAGE_DELAYED_PO),
            [_sim_resolves(SHORTAGE_DELAYED_PO)],
        )
        assert rec.root_cause_summary

    def test_action_detail_non_empty(self):
        rec = self._run_single(
            SHORTAGE_DELAYED_PO,
            _explain_delayed_po(SHORTAGE_DELAYED_PO),
            [_sim_resolves(SHORTAGE_DELAYED_PO)],
        )
        assert rec.action_detail


class TestHelperFunctions:
    """Unit tests for pure helper functions."""

    def test_contains_delay_true(self):
        assert _contains_delay("PO-991 delayed from Apr 10 to Apr 18")

    def test_contains_delay_false(self):
        assert not _contains_delay("PO-991 on track for Apr 10")

    def test_contains_delay_case_insensitive(self):
        assert _contains_delay("Supply DELAYED by 5 days")

    def test_find_supply_root_cause_po(self):
        path = [
            {"step": 1, "node_id": str(uuid4()), "node_type": "CustomerOrderDemand", "fact": "Order needs 100u"},
            {"step": 2, "node_id": str(PO_NODE), "node_type": "PurchaseOrderSupply", "fact": "PO delayed 8 days"},
        ]
        result = _find_supply_root_cause(path)
        assert result is not None
        assert result["node_type"] == "PurchaseOrderSupply"

    def test_find_supply_root_cause_wo(self):
        path = [
            {"step": 1, "node_id": str(uuid4()), "node_type": "ForecastDemand", "fact": "Demand 50u"},
            {"step": 2, "node_id": str(WO_NODE), "node_type": "WorkOrderSupply", "fact": "WO-112 delayed"},
        ]
        result = _find_supply_root_cause(path)
        assert result is not None
        assert result["node_type"] == "WorkOrderSupply"

    def test_find_supply_root_cause_returns_none_when_no_delay(self):
        path = [
            {"step": 1, "node_id": str(uuid4()), "node_type": "CustomerOrderDemand", "fact": "Order needs 100u"},
            {"step": 2, "node_id": str(PO_NODE), "node_type": "PurchaseOrderSupply", "fact": "PO on time"},
        ]
        result = _find_supply_root_cause(path)
        assert result is None

    def test_find_supply_root_cause_returns_none_when_no_supply_nodes(self):
        path = [
            {"step": 1, "node_id": str(uuid4()), "node_type": "ForecastDemand", "fact": "Demand spike"},
            {"step": 2, "node_id": None, "node_type": "PolicyCheck", "fact": "No substitution"},
        ]
        result = _find_supply_root_cause(path)
        assert result is None
