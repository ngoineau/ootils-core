#!/usr/bin/env python3
"""
Ootils M7 Agent Demo — runs the autonomous planning agent against the API.

Usage:
    DATABASE_URL=postgresql:///ootils_dev OOTILS_API_TOKEN=dev-token python scripts/run_agent_demo.py

Starts the API in-process via FastAPI TestClient, seeds synthetic test data
(nodes with shortages + causal explanations), runs OotilsAgent, and prints
the AgentReport in a human-readable format.

No LLM required. All decisions are deterministic and rule-based.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("ootils.demo")

# ── Ensure the src tree is importable when running as a script ───────────────
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_repo_root, "src"))

# ── Set token before importing the app ───────────────────────────────────────
os.environ.setdefault("OOTILS_API_TOKEN", "demo-token")

import httpx
from fastapi.testclient import TestClient

from ootils_core.api.app import create_app
from ootils_core.agent.demo_agent import OotilsAgent
from ootils_core.models import AgentReport, AgentRecommendation


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic dataset (in-memory, no DB required)
# ─────────────────────────────────────────────────────────────────────────────

# Stable IDs for the demo dataset
ITEM_ID = UUID("aaaaaaaa-0000-0000-0000-000000000001")
LOC_ID = UUID("bbbbbbbb-0000-0000-0000-000000000001")
SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")  # baseline

# Shortage nodes
SHORTAGE_1 = UUID("cccccccc-0000-0000-0000-000000000001")
SHORTAGE_2 = UUID("cccccccc-0000-0000-0000-000000000002")
SHORTAGE_3 = UUID("cccccccc-0000-0000-0000-000000000003")

# Supply nodes referenced in causal paths
PO_NODE_1 = UUID("dddddddd-0000-0000-0000-000000000001")
WO_NODE_2 = UUID("dddddddd-0000-0000-0000-000000000002")

# Simulation scenario
SIM_SCENARIO_1 = UUID("eeeeeeee-0000-0000-0000-000000000001")


def _build_mock_responses() -> Dict[str, Any]:
    """
    Build the synthetic API response payloads the mock transport will serve.

    Dataset: 3 shortages
      1. SHORTAGE_1 — PO delayed → expedite_supply (simulation eliminates it → high confidence)
      2. SHORTAGE_2 — WorkOrder delayed → expedite_supply (simulation created, shortage stays → medium confidence)
      3. SHORTAGE_3 — no supply node in causal path → escalate
    """
    issues_payload = {
        "issues": [
            {
                "node_id": str(SHORTAGE_1),
                "item_id": str(ITEM_ID),
                "location_id": str(LOC_ID),
                "shortage_qty": "130",
                "severity_score": "1300",
                "severity": "high",
                "shortage_date": "2026-04-08",
                "explanation_id": str(uuid4()),
                "explanation_url": f"/v1/explain?node_id={SHORTAGE_1}",
                "summary": "PO-991 delayed 8 days. Order CO-778 at risk.",
            },
            {
                "node_id": str(SHORTAGE_2),
                "item_id": str(ITEM_ID),
                "location_id": str(LOC_ID),
                "shortage_qty": "50",
                "severity_score": "500",
                "severity": "high",
                "shortage_date": "2026-04-12",
                "explanation_id": str(uuid4()),
                "explanation_url": f"/v1/explain?node_id={SHORTAGE_2}",
                "summary": "Work order WO-112 delayed 5 days.",
            },
            {
                "node_id": str(SHORTAGE_3),
                "item_id": str(ITEM_ID),
                "location_id": str(LOC_ID),
                "shortage_qty": "80",
                "severity_score": "800",
                "severity": "high",
                "shortage_date": "2026-04-15",
                "explanation_id": None,
                "explanation_url": None,
                "summary": "Demand spike with no supply coverage.",
            },
        ],
        "total": 3,
        "as_of": datetime.now(timezone.utc).isoformat(),
    }

    explain_1_payload = {
        "explanation_id": str(uuid4()),
        "target_node_id": str(SHORTAGE_1),
        "target_type": "Shortage",
        "summary": "Order CO-778 (130u) exhausts stock. PO-991 delayed 8 days.",
        "root_cause_node_id": str(PO_NODE_1),
        "causal_path": [
            {
                "step": 1,
                "node_id": str(uuid4()),
                "node_type": "CustomerOrderDemand",
                "edge_type": "consumes",
                "fact": "Order CO-778 requires 150u due April 8",
            },
            {
                "step": 2,
                "node_id": str(uuid4()),
                "node_type": "OnHandSupply",
                "edge_type": "consumes",
                "fact": "OnHand: 20u — exhausted",
            },
            {
                "step": 3,
                "node_id": str(PO_NODE_1),
                "node_type": "PurchaseOrderSupply",
                "edge_type": "depends_on",
                "fact": "PO-991 delayed from Apr 10 to Apr 18 (8-day gap)",
            },
        ],
    }

    explain_2_payload = {
        "explanation_id": str(uuid4()),
        "target_node_id": str(SHORTAGE_2),
        "target_type": "Shortage",
        "summary": "Work order WO-112 delayed 5 days, leaving 50u unmet.",
        "root_cause_node_id": str(WO_NODE_2),
        "causal_path": [
            {
                "step": 1,
                "node_id": str(uuid4()),
                "node_type": "ForecastDemand",
                "edge_type": "consumes",
                "fact": "Forecast demand of 50u due April 12",
            },
            {
                "step": 2,
                "node_id": str(WO_NODE_2),
                "node_type": "WorkOrderSupply",
                "edge_type": "depends_on",
                "fact": "WO-112 delayed from Apr 7 to Apr 12 — missed deadline",
            },
        ],
    }

    explain_3_payload = {
        "explanation_id": str(uuid4()),
        "target_node_id": str(SHORTAGE_3),
        "target_type": "Shortage",
        "summary": "Demand spike with no supply coverage — no purchase order active.",
        "root_cause_node_id": None,
        "causal_path": [
            {
                "step": 1,
                "node_id": str(uuid4()),
                "node_type": "ForecastDemand",
                "edge_type": "consumes",
                "fact": "Spike demand of 80u due April 15 — no active supply",
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

    # Simulation 1: resolves the shortage (high confidence)
    simulate_1_payload = {
        "scenario_id": str(SIM_SCENARIO_1),
        "scenario_name": f"agent-expedite-{str(PO_NODE_1)[:8]}",
        "status": "created",
        "override_count": 1,
        "base_scenario_id": str(SCENARIO_ID),
        "delta": {
            "resolved_shortages": [
                {
                    "node_id": str(SHORTAGE_1),
                    "before": {"qty": 130, "date": "2026-04-08"},
                    "after": None,
                    "resolution": "shortage eliminated — PO arrives before demand date",
                }
            ],
            "new_shortages": [],
        },
    }

    # Simulation 2: does NOT resolve the shortage (medium confidence)
    simulate_2_payload = {
        "scenario_id": str(uuid4()),
        "scenario_name": f"agent-expedite-{str(WO_NODE_2)[:8]}",
        "status": "created",
        "override_count": 1,
        "base_scenario_id": str(SCENARIO_ID),
        "delta": {
            "resolved_shortages": [],  # shortage remains → medium confidence
            "new_shortages": [],
        },
    }

    return {
        "issues": issues_payload,
        "explain_1": explain_1_payload,
        "explain_2": explain_2_payload,
        "explain_3": explain_3_payload,
        "simulate_1": simulate_1_payload,
        "simulate_2": simulate_2_payload,
    }


class _DemoTransport(httpx.BaseTransport):
    """
    In-memory mock transport for the demo agent.
    Routes requests to synthetic JSON payloads — no real server needed.
    """

    def __init__(self, responses: Dict[str, Any]) -> None:
        self._responses = responses
        self._sim_call = 0  # track simulate call count

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = dict(request.url.params)

        if path == "/v1/issues":
            return _json_response(self._responses["issues"])

        if path == "/v1/explain":
            node_id = params.get("node_id", "")
            if node_id == str(SHORTAGE_1):
                return _json_response(self._responses["explain_1"])
            elif node_id == str(SHORTAGE_2):
                return _json_response(self._responses["explain_2"])
            elif node_id == str(SHORTAGE_3):
                return _json_response(self._responses["explain_3"])
            else:
                return _json_response({"detail": "not found"}, status_code=404)

        if path == "/v1/simulate":
            self._sim_call += 1
            if self._sim_call == 1:
                return _json_response(self._responses["simulate_1"], status_code=201)
            else:
                return _json_response(self._responses["simulate_2"], status_code=201)

        return _json_response({"detail": "not found"}, status_code=404)


def _json_response(data: Any, status_code: int = 200) -> httpx.Response:
    content = json.dumps(data).encode()
    return httpx.Response(
        status_code=status_code,
        headers={"content-type": "application/json"},
        content=content,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Report formatter
# ─────────────────────────────────────────────────────────────────────────────

_ACTION_EMOJI = {
    "expedite_supply": "🚀",
    "reduce_demand": "📉",
    "escalate": "🔴",
    "no_action": "✅",
}

_CONFIDENCE_LABEL = {
    "high": "HIGH ✓",
    "medium": "MEDIUM",
    "low": "LOW ⚠",
}


def print_report(report: AgentReport) -> None:
    """Print AgentReport in a readable format."""
    sep = "─" * 72

    print()
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║           OOTILS M7 AGENT DEMO — AUTONOMOUS PLANNING REPORT         ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print(f"  Run at  : {report.run_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()
    print(f"  Issues detected    : {report.issues_found}")
    print(f"  Issues analyzed    : {report.issues_analyzed}")
    print(f"  Simulations run    : {report.simulations_run}")
    print(f"  Recommendations    : {len(report.recommendations)}")
    print()
    print(sep)
    print("  EXECUTIVE SUMMARY")
    print(sep)
    print()

    # Word-wrap summary
    words = report.summary.split()
    line, lines = [], []
    for word in words:
        if len(" ".join(line + [word])) > 68:
            lines.append("  " + " ".join(line))
            line = [word]
        else:
            line.append(word)
    if line:
        lines.append("  " + " ".join(line))
    print("\n".join(lines))
    print()

    print(sep)
    print("  RECOMMENDATIONS")
    print(sep)

    for i, rec in enumerate(report.recommendations, 1):
        emoji = _ACTION_EMOJI.get(rec.action_type, "❓")
        confidence = _CONFIDENCE_LABEL.get(rec.confidence, rec.confidence)
        print()
        print(f"  [{i}] {emoji}  {rec.action_type.upper().replace('_', ' ')}")
        print(f"       Node       : {rec.issue_node_id}")
        print(f"       Confidence : {confidence}")
        print(f"       Root Cause : {rec.root_cause_summary}")
        print(f"       Action     : {rec.action_detail}")
        if rec.simulation_scenario_id:
            print(f"       Simulation : {rec.simulation_scenario_id}")

    print()
    print(sep)
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    print("\n🔧  Initializing Ootils M7 Agent Demo...", file=sys.stderr)

    # Build synthetic dataset
    mock_data = _build_mock_responses()
    transport = _DemoTransport(mock_data)

    # Run the agent
    print("🤖  Running autonomous agent pipeline...\n", file=sys.stderr)
    agent = OotilsAgent(timeout=30.0)
    token = os.environ.get("OOTILS_API_TOKEN", "demo-token")

    report = agent.run(
        base_url="http://demo.ootils.local",
        token=token,
        transport=transport,
    )

    # Print the report
    print_report(report)

    # Return exit code based on whether issues were found
    return 0 if report.issues_found >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
