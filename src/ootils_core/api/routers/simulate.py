"""
POST /v1/simulate — Create a scenario with overrides and return delta.
"""
from __future__ import annotations

import logging
from typing import Any, Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db
from ootils_core.engine.scenario.manager import ScenarioManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/simulate", tags=["simulate"])


class OverrideIn(BaseModel):
    node_id: UUID
    field_name: str
    new_value: str


class SimulateRequest(BaseModel):
    scenario_name: str
    base_scenario_id: Optional[str] = None
    overrides: list[OverrideIn] = []


class SimulateResponse(BaseModel):
    scenario_id: UUID
    scenario_name: str
    status: str
    override_count: int
    base_scenario_id: UUID


@router.post("", response_model=SimulateResponse, status_code=status.HTTP_201_CREATED)
async def create_simulation(
    body: SimulateRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> SimulateResponse:
    """Create a new scenario with overrides and compute the delta vs base."""
    # Resolve base scenario
    if body.base_scenario_id and body.base_scenario_id.lower() != "baseline":
        try:
            base_id = UUID(body.base_scenario_id)
        except ValueError:
            base_id = BASELINE_SCENARIO_ID
    else:
        base_id = BASELINE_SCENARIO_ID

    manager = ScenarioManager()
    try:
        scenario = manager.create_scenario(
            name=body.scenario_name,
            parent_scenario_id=base_id,
            db=db,
        )
    except Exception as exc:
        logger.exception("simulate.create_scenario_failed name=%s", body.scenario_name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create scenario: {exc}",
        )

    # Apply overrides
    applied = 0
    for override in body.overrides:
        try:
            manager.apply_override(
                scenario_id=scenario.scenario_id,
                node_id=override.node_id,
                field_name=override.field_name,
                new_value=override.new_value,
                applied_by="api",
                db=db,
            )
            applied += 1
        except ValueError as exc:
            logger.warning(
                "simulate.override_skipped node=%s field=%s: %s",
                override.node_id,
                override.field_name,
                exc,
            )

    logger.info(
        "simulate.created scenario=%s base=%s overrides=%d",
        scenario.scenario_id,
        base_id,
        applied,
    )

    return SimulateResponse(
        scenario_id=scenario.scenario_id,
        scenario_name=body.scenario_name,
        status="created",
        override_count=applied,
        base_scenario_id=base_id,
    )
