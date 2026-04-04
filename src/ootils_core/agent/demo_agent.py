"""
demo_agent.py — OotilsAgent: autonomous supply chain planning agent.

Sprint M7 — AI Agent Demo.

The agent operates entirely through the REST API (no direct DB access).
All decisions are deterministic and rule-based (no LLM required).

Decision logic:
  - causal_path step with node_type in SUPPLY_NODE_TYPES and "delayed" in fact
    → action_type = 'expedite_supply'
  - shortage_qty > 0 and no identifiable supply root cause
    → action_type = 'escalate'
  - simulation resolves the shortage (shortage_qty → 0 in resolved list)
    → confidence = 'high'
  - otherwise
    → confidence = 'medium'
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx

from ootils_core.models import AgentRecommendation, AgentReport

logger = logging.getLogger(__name__)

# Node types that represent a supply that could be expedited
SUPPLY_NODE_TYPES = {
    "PurchaseOrderSupply",
    "WorkOrderSupply",
    "TransferOrderSupply",
}

# Keywords that indicate a delay in the fact string
DELAY_KEYWORDS = {"delayed", "delay", "postponed", "late", "gap"}


def _contains_delay(fact: str) -> bool:
    """Return True if the causal fact string mentions a delay."""
    lower = fact.lower()
    return any(kw in lower for kw in DELAY_KEYWORDS)


def _find_supply_root_cause(causal_path: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Scan the causal path and return the first supply step that indicates a delay.
    Returns None if no actionable supply root cause is found.
    """
    for step in causal_path:
        node_type = step.get("node_type") or ""
        fact = step.get("fact") or ""
        if node_type in SUPPLY_NODE_TYPES and _contains_delay(fact):
            return step
    return None


def _has_any_supply_node(causal_path: List[Dict[str, Any]]) -> bool:
    """Return True if the causal path contains any supply-type node."""
    return any(
        (step.get("node_type") or "") in SUPPLY_NODE_TYPES
        for step in causal_path
    )


class OotilsAgent:
    """
    Autonomous planning agent for ootils-core.

    Uses only the REST API — no direct database access.
    Decisions are fully deterministic and explainable.
    """

    def __init__(self, timeout: float = 30.0) -> None:
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run(
        self,
        base_url: str,
        token: str,
        *,
        severity: str = "high",
        horizon_days: int = 14,
        transport: Optional[httpx.BaseTransport] = None,
    ) -> AgentReport:
        """
        Execute the full agent pipeline and return a structured report.

        Args:
            base_url: Base URL of the Ootils API (e.g. "http://localhost:8000")
            token:    Bearer token for authentication
            severity: Issue severity filter (default: "high")
            horizon_days: Planning horizon in days (default: 14)
            transport: Optional httpx transport override (for testing)

        Returns:
            AgentReport with all findings, simulations, and recommendations
        """
        run_at = datetime.now(timezone.utc)
        headers = {"Authorization": f"Bearer {token}"}

        client_kwargs: Dict[str, Any] = {
            "base_url": base_url.rstrip("/"),
            "headers": headers,
            "timeout": self._timeout,
        }
        if transport is not None:
            client_kwargs["transport"] = transport

        with httpx.Client(**client_kwargs) as client:
            # ── Step 1: Query issues ──────────────────────────────────
            issues = self._query_issues(client, severity, horizon_days)
            issues_found = len(issues)
            logger.info("agent.issues_found count=%d", issues_found)

            # ── Steps 2–4: Explain → Simulate → Recommend ─────────────
            recommendations: List[AgentRecommendation] = []
            issues_analyzed = 0
            simulations_run = 0

            for issue in issues:
                node_id = str(issue.get("node_id", ""))
                shortage_qty = float(issue.get("shortage_qty", 0))

                logger.info("agent.analyzing node=%s shortage_qty=%s", node_id, shortage_qty)

                # Step 2: Explain
                explanation = self._explain(client, node_id)
                if explanation is None:
                    # 404 or error — still make a recommendation based on shortage alone
                    logger.warning("agent.explain_failed node=%s", node_id)
                    recommendations.append(
                        AgentRecommendation(
                            issue_node_id=_safe_uuid(node_id),
                            root_cause_summary="Explanation unavailable",
                            action_type="escalate",
                            action_detail=(
                                f"No explanation available for node {node_id}. "
                                "Manual investigation required."
                            ),
                            simulation_scenario_id=None,
                            confidence="low",
                        )
                    )
                    issues_analyzed += 1
                    continue

                causal_path: List[Dict[str, Any]] = explanation.get("causal_path", [])
                summary_text: str = explanation.get("summary", "")

                # Step 3: Simulate (only if we have an identifiable supply root cause)
                supply_step = _find_supply_root_cause(causal_path)
                sim_scenario_id: Optional[UUID] = None
                shortage_eliminated = False

                if supply_step is not None:
                    sim_result = self._simulate(
                        client=client,
                        issue_node_id=node_id,
                        supply_node_id=str(supply_step.get("node_id", "")),
                    )
                    simulations_run += 1

                    if sim_result is not None:
                        raw_id = sim_result.get("scenario_id")
                        sim_scenario_id = _safe_uuid(str(raw_id)) if raw_id else None

                        # Check if shortage was eliminated by the simulation
                        # The simulate endpoint returns a delta with resolved_shortages
                        delta = sim_result.get("delta") or {}
                        resolved = delta.get("resolved_shortages", [])
                        for res in resolved:
                            if str(res.get("node_id", "")) == node_id:
                                shortage_eliminated = True
                                break

                # Step 4: Recommend
                rec = self._build_recommendation(
                    issue_node_id=node_id,
                    shortage_qty=shortage_qty,
                    causal_path=causal_path,
                    summary_text=summary_text,
                    supply_step=supply_step,
                    sim_scenario_id=sim_scenario_id,
                    shortage_eliminated=shortage_eliminated,
                )
                recommendations.append(rec)
                issues_analyzed += 1

        # ── Build report ───────────────────────────────────────────────
        summary = self._build_summary(
            issues_found=issues_found,
            issues_analyzed=issues_analyzed,
            simulations_run=simulations_run,
            recommendations=recommendations,
        )

        report = AgentReport(
            issues_found=issues_found,
            issues_analyzed=issues_analyzed,
            simulations_run=simulations_run,
            recommendations=recommendations,
            run_at=run_at,
            summary=summary,
        )
        logger.info(
            "agent.run_complete issues_found=%d issues_analyzed=%d simulations=%d recs=%d",
            issues_found,
            issues_analyzed,
            simulations_run,
            len(recommendations),
        )
        return report

    # ------------------------------------------------------------------
    # Private pipeline steps
    # ------------------------------------------------------------------

    def _query_issues(
        self, client: httpx.Client, severity: str, horizon_days: int
    ) -> List[Dict[str, Any]]:
        """GET /v1/issues — return list of issue dicts."""
        try:
            resp = client.get(
                "/v1/issues",
                params={"severity": severity, "horizon_days": horizon_days},
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("issues", [])
            else:
                logger.warning(
                    "agent.issues_error status=%d body=%s",
                    resp.status_code,
                    resp.text[:200],
                )
                return []
        except Exception as exc:
            logger.error("agent.issues_exception exc=%s", exc)
            return []

    def _explain(
        self, client: httpx.Client, node_id: str
    ) -> Optional[Dict[str, Any]]:
        """GET /v1/explain — return explanation dict or None on failure."""
        try:
            resp = client.get("/v1/explain", params={"node_id": node_id})
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                logger.info("agent.explain_404 node=%s", node_id)
                return None
            else:
                logger.warning(
                    "agent.explain_error node=%s status=%d",
                    node_id,
                    resp.status_code,
                )
                return None
        except Exception as exc:
            logger.error("agent.explain_exception node=%s exc=%s", node_id, exc)
            return None

    def _simulate(
        self,
        client: httpx.Client,
        issue_node_id: str,
        supply_node_id: str,
    ) -> Optional[Dict[str, Any]]:
        """POST /v1/simulate — return simulation result dict or None on failure."""
        if not supply_node_id or supply_node_id == "None":
            return None

        scenario_name = f"agent-expedite-{supply_node_id[:8]}"
        payload = {
            "scenario_name": scenario_name,
            "base_scenario_id": "baseline",
            "overrides": [
                {
                    "node_id": supply_node_id,
                    "field_name": "due_date",
                    # Simulate bringing the supply in 7 days earlier
                    "new_value": "expedite",
                }
            ],
        }
        try:
            resp = client.post("/v1/simulate", json=payload)
            if resp.status_code in (200, 201):
                return resp.json()
            elif resp.status_code == 404:
                logger.info("agent.simulate_404 supply_node=%s", supply_node_id)
                return None
            else:
                logger.warning(
                    "agent.simulate_error supply_node=%s status=%d body=%s",
                    supply_node_id,
                    resp.status_code,
                    resp.text[:200],
                )
                return None
        except Exception as exc:
            logger.error("agent.simulate_exception exc=%s", exc)
            return None

    def _build_recommendation(
        self,
        *,
        issue_node_id: str,
        shortage_qty: float,
        causal_path: List[Dict[str, Any]],
        summary_text: str,
        supply_step: Optional[Dict[str, Any]],
        sim_scenario_id: Optional[UUID],
        shortage_eliminated: bool,
    ) -> AgentRecommendation:
        """Apply the decision rules and produce an AgentRecommendation."""

        # Decision rule 1: supply delayed → expedite
        if supply_step is not None:
            action_type = "expedite_supply"
            supply_node_id = supply_step.get("node_id", "unknown")
            supply_fact = supply_step.get("fact", "")
            action_detail = (
                f"Expedite supply node {supply_node_id}. "
                f"Root cause: {supply_fact}. "
                f"Shortage qty: {shortage_qty:.0f} units."
            )
            root_cause_summary = supply_fact or summary_text

            # Confidence rule
            if shortage_eliminated:
                confidence = "high"
            else:
                confidence = "medium"

        elif shortage_qty > 0 and not _has_any_supply_node(causal_path):
            # Decision rule 2: shortage with no identifiable supply → escalate
            action_type = "escalate"
            action_detail = (
                f"Shortage of {shortage_qty:.0f} units with no identifiable supply source. "
                "Escalate to supply chain manager for manual investigation."
            )
            root_cause_summary = summary_text or "No supply source found in causal path."
            confidence = "medium"

        else:
            # Fallback: no clear action
            action_type = "no_action"
            action_detail = "No actionable root cause identified. Monitor situation."
            root_cause_summary = summary_text or "No root cause identified."
            confidence = "low"

        return AgentRecommendation(
            issue_node_id=_safe_uuid(issue_node_id),
            root_cause_summary=root_cause_summary,
            action_type=action_type,
            action_detail=action_detail,
            simulation_scenario_id=sim_scenario_id,
            confidence=confidence,
        )

    def _build_summary(
        self,
        *,
        issues_found: int,
        issues_analyzed: int,
        simulations_run: int,
        recommendations: List[AgentRecommendation],
    ) -> str:
        """Generate a 1-paragraph plain English summary of the agent run."""
        expedite_count = sum(
            1 for r in recommendations if r.action_type == "expedite_supply"
        )
        escalate_count = sum(
            1 for r in recommendations if r.action_type == "escalate"
        )
        high_confidence = sum(
            1 for r in recommendations if r.confidence == "high"
        )

        parts = [
            f"The agent detected {issues_found} high-severity supply chain issue(s) "
            f"within the 14-day planning horizon.",
        ]

        if issues_analyzed > 0:
            parts.append(
                f"After analyzing {issues_analyzed} issue(s) through the causal explanation "
                f"API, {simulations_run} simulation(s) were executed to validate potential fixes."
            )

        if expedite_count > 0:
            parts.append(
                f"{expedite_count} issue(s) were traced to delayed supply nodes "
                f"and an expedite action is recommended"
                + (f" ({high_confidence} with high confidence based on simulation results)." if high_confidence else ".")
            )

        if escalate_count > 0:
            parts.append(
                f"{escalate_count} issue(s) could not be resolved automatically "
                f"and require human escalation."
            )

        if not recommendations:
            parts.append("No actionable recommendations were generated.")

        return " ".join(parts)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _safe_uuid(value: str) -> UUID:
    """Parse a UUID string, returning a nil UUID on failure."""
    try:
        return UUID(value)
    except (ValueError, AttributeError):
        return UUID("00000000-0000-0000-0000-000000000000")
