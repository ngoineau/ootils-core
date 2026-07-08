"""
GET /v1/explain — Get causal explanation for a planning node.

Lazy generation strategy (since 2026-05-24): if no explanation exists
for the requested node, but the node IS a Projected Inventory with an
active shortage, generate the causal chain on the fly, persist it, and
return it. Avoids the eager bulk-generation cost on every propagation.
"""
from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.kernel.explanation.builder import ExplanationBuilder
from ootils_core.engine.kernel.graph.store import GraphStore

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/explain", tags=["explain"])


class CausalStepOut(BaseModel):
    step: int
    node_id: Optional[UUID]
    node_type: Optional[str]
    edge_type: Optional[str]
    fact: str


class ExplainResponse(BaseModel):
    explanation_id: UUID
    target_node_id: UUID
    target_type: str
    summary: str
    causal_path: list[CausalStepOut]
    root_cause_node_id: Optional[UUID]


@router.get("", response_model=ExplainResponse)
def get_explanation(
    node_id: str = Query(..., description="Target node UUID to explain"),
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("read")),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> ExplainResponse:
    """Return the causal explanation for a planning node."""
    try:
        node_uuid = UUID(node_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"node_id '{node_id}' is not a valid UUID",
        )

    builder = ExplanationBuilder()
    explanation = builder.get_explanation(node_uuid, db, scenario_id=scenario_id)

    # Lazy generation : if no explanation in DB but the node has an
    # active shortage, build it on the fly. Honors M3 contract without
    # paying the eager bulk cost on every propagation.
    if explanation is None:
        store = GraphStore(db)
        pi_node = store.get_node(node_uuid, scenario_id)
        if pi_node is None or not pi_node.has_shortage:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No explanation available for node '{node_id}' (no active shortage)",
            )

        # Find the calc_run_id of the currently-active shortage on this node
        shortage_row = db.execute(
            "SELECT calc_run_id, shortage_id FROM shortages "
            "WHERE pi_node_id = %s AND scenario_id = %s AND status = 'active' "
            "ORDER BY updated_at DESC LIMIT 1",
            (node_uuid, scenario_id),
        ).fetchone()
        if shortage_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No active shortage row for node '{node_id}'",
            )

        calc_run_uuid = UUID(str(shortage_row["calc_run_id"]))
        shortage_uuid = UUID(str(shortage_row["shortage_id"]))
        try:
            explanation = builder.build_pi_explanation(
                pi_node=pi_node,
                calc_run_id=calc_run_uuid,
                store=store,
                db=db,
            )
            builder.persist(explanation, db)
            db.execute(
                "UPDATE shortages SET explanation_id = %s, updated_at = now() "
                "WHERE shortage_id = %s",
                (explanation.explanation_id, shortage_uuid),
            )
            db.commit()
            logger.info(
                "explain.lazy-generated node=%s explanation=%s",
                node_id, explanation.explanation_id,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "explain.lazy-generation failed for node %s: %s", node_id, exc,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Explanation generation failed",
            ) from exc
    else:
        logger.info(
            "explain.fetched node=%s explanation=%s",
            node_id, explanation.explanation_id,
        )

    return ExplainResponse(
        explanation_id=explanation.explanation_id,
        target_node_id=explanation.target_node_id,
        target_type=explanation.target_type,
        summary=explanation.summary,
        causal_path=[
            CausalStepOut(
                step=s.step,
                node_id=s.node_id,
                node_type=s.node_type,
                edge_type=s.edge_type,
                fact=s.fact,
            )
            for s in explanation.causal_path
        ],
        root_cause_node_id=explanation.root_cause_node_id,
    )
