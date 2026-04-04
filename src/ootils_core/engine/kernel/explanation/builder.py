"""
builder.py — ExplanationBuilder for Sprint M3 Explainability.

Constructs CausalStep chains and Explanation objects inline during the
calculation pass, then persists them to the `explanations` / `causal_steps`
tables.

This module is the exclusive owner of those two tables; it may use direct SQL
on them.  All other graph data access goes through GraphStore.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

from ootils_core.models import CausalStep, Explanation, Node
from ootils_core.engine.kernel.graph.store import GraphStore

logger = logging.getLogger(__name__)


class ExplanationBuilder:
    """
    Builds and persists causal explanations for planning result nodes.

    Usage
    -----
    builder = ExplanationBuilder()

    # During calculation, when a shortage is detected:
    explanation = builder.build_pi_explanation(pi_node, calc_run_id, store, db)
    builder.persist(explanation, db)

    # Later, for the API:
    explanation = builder.get_explanation(target_node_id, db)
    """

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build_pi_explanation(
        self,
        pi_node: Node,
        calc_run_id: UUID,
        store: GraphStore,
        db,
    ) -> Explanation:
        """
        Construct a causal explanation for a recomputed PI node that has a shortage.

        Causal chain:
          Step 1 — Primary demand (consumes → pi_node)
          Step 2 — Primary supply  (replenishes → pi_node)
          Step 3 — Root cause      (supply missing / delayed)

        Returns an Explanation (not yet persisted).
        """
        scenario_id = pi_node.scenario_id
        node_id = pi_node.node_id
        causal_path: list[CausalStep] = []
        root_cause_node_id: Optional[UUID] = None

        # ------------------------------------------------------------------
        # Step 1: primary demand — highest-priority 'consumes' edge into pi_node
        # ------------------------------------------------------------------
        consume_edges = store.get_edges_to(node_id, scenario_id, edge_type="consumes")
        consume_edges_sorted = sorted(consume_edges, key=lambda e: (e.priority, e.edge_id))

        if consume_edges_sorted:
            primary_demand_edge = consume_edges_sorted[0]
            demand_node = store.get_node(primary_demand_edge.from_node_id, scenario_id)
            if demand_node:
                qty = demand_node.quantity or Decimal("0")
                date_str = _date_str(demand_node)
                fact = (
                    f"{demand_node.node_type} {demand_node.node_id} requires "
                    f"{qty} units{date_str}"
                )
                causal_path.append(CausalStep(
                    step=1,
                    node_id=demand_node.node_id,
                    node_type=demand_node.node_type,
                    edge_type="consumes",
                    fact=fact,
                ))
            else:
                causal_path.append(CausalStep(
                    step=1,
                    node_id=primary_demand_edge.from_node_id,
                    node_type=None,
                    edge_type="consumes",
                    fact="Demand node not found — may have been deleted",
                ))
        else:
            causal_path.append(CausalStep(
                step=1,
                node_id=None,
                node_type=None,
                edge_type="consumes",
                fact="No demand edges found for this PI node",
            ))

        # ------------------------------------------------------------------
        # Step 2: primary supply — 'replenishes' edge into pi_node
        # (exclude OnHandSupply — that feeds opening stock, not inflows)
        # ------------------------------------------------------------------
        replenish_edges = store.get_edges_to(node_id, scenario_id, edge_type="replenishes")
        supply_nodes = []
        for edge in replenish_edges:
            src = store.get_node(edge.from_node_id, scenario_id)
            if src and src.node_type not in ("OnHandSupply",):
                supply_nodes.append((edge, src))

        supply_nodes_sorted = sorted(supply_nodes, key=lambda t: (t[0].priority, t[0].edge_id))

        if supply_nodes_sorted:
            primary_supply_edge, primary_supply = supply_nodes_sorted[0]
            qty = primary_supply.quantity or Decimal("0")
            date_str = _date_str(primary_supply)
            fact = (
                f"{primary_supply.node_type} {primary_supply.node_id} "
                f"provides {qty} units{date_str}"
            )
            causal_path.append(CausalStep(
                step=2,
                node_id=primary_supply.node_id,
                node_type=primary_supply.node_type,
                edge_type="replenishes",
                fact=fact,
            ))

            # ------------------------------------------------------------------
            # Step 3: root cause — why is supply insufficient?
            # Heuristic: if supply qty < shortage_qty, supply is missing/insufficient
            # If supply has a time_ref beyond pi_node.time_span_end, it's delayed
            # ------------------------------------------------------------------
            shortage_qty = pi_node.shortage_qty or Decimal("0")
            supply_qty = primary_supply.quantity or Decimal("0")

            is_delayed = (
                primary_supply.time_ref is not None
                and pi_node.time_span_end is not None
                and primary_supply.time_ref > pi_node.time_span_end
            )
            is_insufficient = supply_qty < shortage_qty

            if is_delayed:
                root_cause_node_id = primary_supply.node_id
                delay_days = (primary_supply.time_ref - pi_node.time_span_end).days
                fact = (
                    f"{primary_supply.node_type} {primary_supply.node_id} "
                    f"is delayed {delay_days} day(s) beyond bucket end "
                    f"({primary_supply.time_ref} > {pi_node.time_span_end})"
                )
                causal_path.append(CausalStep(
                    step=3,
                    node_id=primary_supply.node_id,
                    node_type=primary_supply.node_type,
                    edge_type="depends_on",
                    fact=fact,
                ))
            elif is_insufficient:
                root_cause_node_id = primary_supply.node_id
                gap = shortage_qty - supply_qty
                fact = (
                    f"{primary_supply.node_type} {primary_supply.node_id} "
                    f"provides {supply_qty} units — insufficient by {gap} units "
                    f"(shortage: {shortage_qty})"
                )
                causal_path.append(CausalStep(
                    step=3,
                    node_id=primary_supply.node_id,
                    node_type=primary_supply.node_type,
                    edge_type="depends_on",
                    fact=fact,
                ))
            else:
                # Supply exists and is on time, but demand exceeds it
                causal_path.append(CausalStep(
                    step=3,
                    node_id=None,
                    node_type=None,
                    edge_type="governed_by",
                    fact=(
                        f"Total demand exceeds available supply "
                        f"(shortage: {shortage_qty} units) — no substitute or alternate source active"
                    ),
                ))
        else:
            # No supply at all
            causal_path.append(CausalStep(
                step=2,
                node_id=None,
                node_type=None,
                edge_type="replenishes",
                fact="No supply replenishing this PI node",
            ))
            causal_path.append(CausalStep(
                step=3,
                node_id=None,
                node_type=None,
                edge_type="governed_by",
                fact=(
                    f"Supply is entirely missing — shortage of "
                    f"{pi_node.shortage_qty or Decimal('0')} units is uncovered"
                ),
            ))

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        summary = _build_summary(pi_node, causal_path)

        explanation = Explanation(
            explanation_id=uuid4(),
            calc_run_id=calc_run_id,
            target_node_id=node_id,
            target_type="Shortage",
            root_cause_node_id=root_cause_node_id,
            causal_path=causal_path,
            summary=summary,
        )

        logger.debug(
            "Built explanation %s for PI node %s (shortage %s)",
            explanation.explanation_id,
            node_id,
            pi_node.shortage_qty,
        )
        return explanation

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------

    def persist(self, explanation: Explanation, db) -> None:
        """
        Insert the explanation and its causal steps into the DB.

        Direct SQL is acceptable here — this module owns these tables.
        Callers own transaction scope (commit/rollback).
        """
        db.execute(
            """
            INSERT INTO explanations (
                explanation_id, calc_run_id, target_node_id, target_type,
                root_cause_node_id, summary, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                explanation.explanation_id,
                explanation.calc_run_id,
                explanation.target_node_id,
                explanation.target_type,
                explanation.root_cause_node_id,
                explanation.summary,
                explanation.created_at,
            ),
        )

        for step in explanation.causal_path:
            db.execute(
                """
                INSERT INTO causal_steps (
                    step_id, explanation_id, step,
                    node_id, node_type, edge_type, fact, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid4(),
                    explanation.explanation_id,
                    step.step,
                    step.node_id,
                    step.node_type,
                    step.edge_type,
                    step.fact,
                    datetime.now(timezone.utc),
                ),
            )

        logger.info(
            "Persisted explanation %s (%d steps) for node %s",
            explanation.explanation_id,
            len(explanation.causal_path),
            explanation.target_node_id,
        )

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_explanation(
        self,
        target_node_id: UUID,
        db,
    ) -> Optional[Explanation]:
        """
        Load the most recent explanation for a given target node from the DB.

        Returns None if no explanation exists.
        """
        row = db.execute(
            """
            SELECT explanation_id, calc_run_id, target_node_id, target_type,
                   root_cause_node_id, summary, created_at
            FROM explanations
            WHERE target_node_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (target_node_id,),
        ).fetchone()

        if row is None:
            return None

        explanation_id = UUID(str(row["explanation_id"]))

        # Load steps
        step_rows = db.execute(
            """
            SELECT step, node_id, node_type, edge_type, fact
            FROM causal_steps
            WHERE explanation_id = %s
            ORDER BY step ASC
            """,
            (explanation_id,),
        ).fetchall()

        causal_path = [
            CausalStep(
                step=sr["step"],
                node_id=UUID(str(sr["node_id"])) if sr.get("node_id") else None,
                node_type=sr.get("node_type"),
                edge_type=sr.get("edge_type"),
                fact=sr["fact"],
            )
            for sr in step_rows
        ]

        return Explanation(
            explanation_id=explanation_id,
            calc_run_id=UUID(str(row["calc_run_id"])),
            target_node_id=UUID(str(row["target_node_id"])),
            target_type=row["target_type"],
            root_cause_node_id=(
                UUID(str(row["root_cause_node_id"])) if row.get("root_cause_node_id") else None
            ),
            causal_path=causal_path,
            summary=row["summary"],
            created_at=row["created_at"],
        )


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _date_str(node: Node) -> str:
    """Return a human-readable date suffix for a node, or empty string."""
    if node.time_ref:
        return f" due {node.time_ref}"
    if node.time_span_start and node.time_span_end:
        return f" spanning {node.time_span_start} to {node.time_span_end}"
    return ""


def _build_summary(pi_node: Node, causal_path: list[CausalStep]) -> str:
    """
    Build a 1-line plain-English summary from the causal path.
    Falls back to a generic message if the path is sparse.
    """
    parts: list[str] = []

    # Extract demand fact (step 1)
    demand_steps = [s for s in causal_path if s.step == 1]
    if demand_steps:
        parts.append(demand_steps[0].fact)

    # Extract root cause fact (step 3)
    root_steps = [s for s in causal_path if s.step == 3]
    if root_steps:
        parts.append(root_steps[0].fact)

    if parts:
        summary = ". ".join(parts) + "."
    else:
        shortage_qty = pi_node.shortage_qty or Decimal("0")
        summary = (
            f"Shortage of {shortage_qty} units on PI node {pi_node.node_id} "
            f"— causal chain could not be fully resolved."
        )

    # Truncate at 500 chars to keep it readable
    if len(summary) > 500:
        summary = summary[:497] + "..."

    return summary
