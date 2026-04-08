"""
Events router — POST /v1/events (submit event) + GET /v1/events (read event log).
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
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


class EventRecord(BaseModel):
    event_id: UUID
    event_type: str
    scenario_id: Optional[UUID] = None
    trigger_node_id: Optional[UUID] = None
    field_changed: Optional[str] = None
    old_date: Optional[date] = None
    new_date: Optional[date] = None
    old_quantity: Optional[Decimal] = None
    new_quantity: Optional[Decimal] = None
    processed: bool
    source: str
    created_at: str


class EventListResponse(BaseModel):
    events: List[EventRecord]
    total: int
    limit: int
    offset: int


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


@router.get("", response_model=EventListResponse)
async def list_events(
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
    limit: int = Query(default=50, ge=1, le=500, description="Max events to return"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    event_type: Optional[str] = Query(default=None, description="Filter by event type"),
    scenario_id: Optional[str] = Query(default=None, description="Filter by scenario UUID"),
    processed: Optional[bool] = Query(default=None, description="Filter by processed flag"),
) -> EventListResponse:
    """Return the event log, ordered by created_at DESC."""

    # Validate event_type filter if provided
    if event_type is not None and event_type not in VALID_EVENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown event_type '{event_type}'. Valid: {sorted(VALID_EVENT_TYPES)}",
        )

    # Validate scenario_id filter if provided
    effective_scenario_id: Optional[UUID] = None
    if scenario_id is not None:
        try:
            effective_scenario_id = UUID(scenario_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid scenario_id UUID: '{scenario_id}'",
            )

    # Build WHERE clause dynamically
    conditions = []
    params: list = []

    if event_type is not None:
        conditions.append("event_type = %s")
        params.append(event_type)

    if effective_scenario_id is not None:
        conditions.append("scenario_id = %s")
        params.append(effective_scenario_id)

    if processed is not None:
        conditions.append("processed = %s")
        params.append(processed)

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # Count total matching rows
    count_row = db.execute(
        f"SELECT COUNT(*) AS total FROM events {where_clause}",
        params,
    ).fetchone()
    total = count_row[0] if count_row else 0

    # Fetch paginated rows
    rows = db.execute(
        f"""
        SELECT
            event_id, event_type, scenario_id, trigger_node_id,
            field_changed, old_date, new_date, old_quantity, new_quantity,
            processed, source, created_at
        FROM events
        {where_clause}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """,
        params + [limit, offset],
    ).fetchall()

    events = [
        EventRecord(
            event_id=row[0],
            event_type=row[1],
            scenario_id=row[2],
            trigger_node_id=row[3],
            field_changed=row[4],
            old_date=row[5],
            new_date=row[6],
            old_quantity=row[7],
            new_quantity=row[8],
            processed=bool(row[9]),
            source=row[10] or "api",
            created_at=row[11].isoformat() if row[11] else "",
        )
        for row in rows
    ]

    logger.debug("events.list total=%d limit=%d offset=%d", total, limit, offset)

    return EventListResponse(
        events=events,
        total=total,
        limit=limit,
        offset=offset,
    )
