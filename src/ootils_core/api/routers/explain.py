"""
GET /v1/explain — Get causal explanation for a planning node.
"""
from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.engine.kernel.explanation.builder import ExplanationBuilder

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
async def get_explanation(
    node_id: str = Query(..., description="Target node UUID to explain"),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> ExplainResponse:
    """Return the causal explanation for a planning node."""
    try:
        node_uuid = UUID(node_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"node_id '{node_id}' is not a valid UUID",
        )

    builder = ExplanationBuilder()
    explanation = builder.get_explanation(node_uuid, db, scenario_id=scenario_id)

    if explanation is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No explanation found for node '{node_id}'",
        )

    logger.info(
        "explain.fetched node=%s explanation=%s", node_id, explanation.explanation_id
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
