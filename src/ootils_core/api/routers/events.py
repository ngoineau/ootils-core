"""
POST /v1/events — Submit a supply chain planning event.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db, resolve_scenario_id

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/events", tags=["events"])

# Must stay in sync with events.event_type CHECK constraint in migrations 002 + 006.
# Any new event type requires both a DB migration (ALTER TABLE ... ADD CONSTRAINT)
# and an addition here.
VALID_EVENT_TYPES = {
    # From migration 002 CHECK constraint
    "supply_date_changed",
    "supply_qty_changed",
    "demand_qty_changed",
    "onhand_updated",
    "policy_changed",
    "structure_changed",
    "scenario_created",
    "calc_triggered",
    "ingestion_complete",
    "po_date_changed",
    "test_event",
    # From migration 006 CHECK constraint extension
    "scenario_merge",
}


class EventRequest(BaseModel):
    event_type: str
    trigger_node_id: Optional[UUID] = None
    scenario_id: Optional[str] = None
    field_changed: Optional[str] = None
    old_date: Optional[date] = None
    new_date: Optional[date] = None
    old_quantity: Optional[Decimal] = None
    new_quantity: Optional[Decimal] = None
    source: str = "api"


class EventResponse(BaseModel):
    event_id: UUID
    status: str
    scenario_id: UUID
    affected_nodes_estimate: int


@router.post("", response_model=EventResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_event(
    body: EventRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> EventResponse:
    """Submit a planning event that triggers recalculation."""
    if body.event_type not in VALID_EVENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown event_type '{body.event_type}'. Valid: {sorted(VALID_EVENT_TYPES)}",
        )

    # Override scenario from body if provided
    if body.scenario_id and body.scenario_id.lower() != "baseline":
        try:
            effective_scenario_id = UUID(body.scenario_id)
        except ValueError:
            effective_scenario_id = scenario_id
    else:
        effective_scenario_id = scenario_id

    from datetime import datetime, timezone
    from uuid import uuid4
    event_id = uuid4()
    now = datetime.now(timezone.utc)

    db.execute(
        """
        INSERT INTO events (
            event_id, event_type, scenario_id, trigger_node_id,
            field_changed, old_date, new_date, old_quantity, new_quantity,
            processed, source, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, %s, %s)
        """,
        (
            event_id,
            body.event_type,
            effective_scenario_id,
            body.trigger_node_id,
            body.field_changed,
            body.old_date,
            body.new_date,
            body.old_quantity,
            body.new_quantity,
            body.source,
            now,
        ),
    )

    logger.info(
        "event.created event_id=%s type=%s scenario=%s",
        event_id,
        body.event_type,
        effective_scenario_id,
    )

    return EventResponse(
        event_id=event_id,
        status="queued",
        scenario_id=effective_scenario_id,
        affected_nodes_estimate=0,
    )
