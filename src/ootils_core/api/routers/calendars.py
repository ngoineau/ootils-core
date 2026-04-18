"""
Operational calendar router.

Endpoints:
  POST /v1/ingest/calendars                   — Upsert non-working days for a location
  GET  /v1/calendars/{location_external_id}   — List calendar entries
  POST /v1/calendars/working-days             — Compute delivery date in working days
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db

logger = logging.getLogger(__name__)

router = APIRouter(tags=["calendars"])

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _resolve_location(db: psycopg.Connection, external_id: str) -> Optional[UUID]:
    """Return location_id for external_id, or None if not found."""
    row = db.execute(
        "SELECT location_id FROM locations WHERE external_id = %s",
        (external_id,),
    ).fetchone()
    return row["location_id"] if row else None


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

class CalendarEntryInput(BaseModel):
    calendar_date: date = Field(..., description="Calendar date (YYYY-MM-DD).")
    is_working_day: bool = Field(False, description="False = non-working day (holiday, closure). Default = False.")
    shift_count: Optional[int] = Field(default=None, ge=0, le=3, description="Number of shifts on this day [0–3]. Optional.")
    capacity_factor: Optional[float] = Field(default=None, ge=0.0, le=2.0, description="Capacity factor [0.0–2.0]. 1.0 = normal capacity. Optional.")
    notes: Optional[str] = Field(None, description="Notes (e.g. 'Christmas', 'Planned maintenance'). Optional.")

    @field_validator("capacity_factor")
    @classmethod
    def validate_capacity_factor(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (0.0 <= v <= 2.0):
            raise ValueError("capacity_factor must be in [0, 2]")
        return v


class IngestCalendarsRequest(BaseModel):
    location_external_id: str = Field(..., description="External_id of the target location.")
    entries: list[CalendarEntryInput] = Field(..., description="Calendar entries to import (upsert per location × date).")
    dry_run: bool = Field(False, description="If true, validation only — no DB writes.")

    @field_validator("location_external_id")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("location_external_id must not be empty")
        return v


class CalendarIngestSummary(BaseModel):
    total: int
    inserted: int
    updated: int
    errors: int


class IngestCalendarsResponse(BaseModel):
    status: str
    location_external_id: str
    location_id: Optional[str]
    summary: CalendarIngestSummary


class CalendarEntryOutput(BaseModel):
    calendar_date: date
    is_working_day: bool
    shift_count: Optional[int]
    capacity_factor: Optional[float]
    notes: Optional[str]


class GetCalendarsResponse(BaseModel):
    location_external_id: str
    location_id: str
    entries: list[CalendarEntryOutput]
    total: int


class WorkingDaysRequest(BaseModel):
    location_external_id: str = Field(..., description="Location external_id.")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD).")
    add_working_days: int = Field(..., ge=0, le=1000, description="Number of working days to add (>= 0).")


class WorkingDaysResponse(BaseModel):
    location_external_id: str
    start_date: date
    add_working_days: int
    result_date: date
    working_days_found: int
    non_working_days_skipped: int


# ─────────────────────────────────────────────────────────────
# POST /v1/ingest/calendars
# ─────────────────────────────────────────────────────────────

@router.post("/v1/ingest/calendars", response_model=IngestCalendarsResponse, summary="Import calendar", description="Import non-working days for a location. Upsert per (location × date). Missing entry = working day by default.")
async def ingest_calendars(
    body: IngestCalendarsRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> IngestCalendarsResponse:
    """
    Upsert non-working days (and other calendar entries) for a location.

    - Resolves location_external_id → location_id (422 if unknown)
    - Validates capacity_factor ∈ [0, 2]
    - ON CONFLICT (location_id, calendar_date) DO UPDATE
    - Supports dry_run mode (validation only, no DB writes)
    """
    # Resolve location
    location_id = _resolve_location(db, body.location_external_id)
    if location_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{
                "field": "location_external_id",
                "error": f"Location '{body.location_external_id}' not found in DB",
            }],
        )

    if body.dry_run:
        return IngestCalendarsResponse(
            status="dry_run",
            location_external_id=body.location_external_id,
            location_id=str(location_id),
            summary=CalendarIngestSummary(
                total=len(body.entries), inserted=0, updated=0, errors=0
            ),
        )

    inserted = 0
    updated = 0

    for entry in body.entries:
        # Check if record exists (to track inserted vs updated)
        existing = db.execute(
            """
            SELECT calendar_id FROM operational_calendars
            WHERE location_id = %s AND calendar_date = %s
            """,
            (location_id, entry.calendar_date),
        ).fetchone()

        # Build effective values
        capacity_factor = entry.capacity_factor if entry.capacity_factor is not None else 1.0
        shift_count = entry.shift_count if entry.shift_count is not None else 1

        db.execute(
            """
            INSERT INTO operational_calendars
                (location_id, calendar_date, is_working_day, shift_count, capacity_factor, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (location_id, calendar_date) DO UPDATE
                SET is_working_day  = EXCLUDED.is_working_day,
                    shift_count     = EXCLUDED.shift_count,
                    capacity_factor = EXCLUDED.capacity_factor,
                    notes           = EXCLUDED.notes
            """,
            (
                location_id,
                entry.calendar_date,
                entry.is_working_day,
                shift_count,
                capacity_factor,
                entry.notes,
            ),
        )

        if existing:
            updated += 1
        else:
            inserted += 1

    logger.info(
        "ingest.calendars location=%s total=%d inserted=%d updated=%d",
        body.location_external_id, len(body.entries), inserted, updated,
    )

    return IngestCalendarsResponse(
        status="ok",
        location_external_id=body.location_external_id,
        location_id=str(location_id),
        summary=CalendarIngestSummary(
            total=len(body.entries),
            inserted=inserted,
            updated=updated,
            errors=0,
        ),
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/calendars/{location_external_id}
# ─────────────────────────────────────────────────────────────

@router.get("/v1/calendars/{location_external_id}", response_model=GetCalendarsResponse, summary="Get calendar", description="List calendar entries for a location (optional: filter by date range).")
async def get_calendars(
    location_external_id: str,
    from_date: Optional[date] = Query(default=None),
    to_date: Optional[date] = Query(default=None),
    working_only: bool = Query(default=False),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> GetCalendarsResponse:
    """
    Return calendar entries for a location.

    Query params:
    - from_date  ISO date, inclusive lower bound
    - to_date    ISO date, inclusive upper bound
    - working_only  if true, return only is_working_day=true entries
    """
    location_id = _resolve_location(db, location_external_id)
    if location_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Location '{location_external_id}' not found",
        )

    # Build dynamic query
    filters = ["location_id = %s"]
    params: list = [location_id]

    if from_date is not None:
        filters.append("calendar_date >= %s")
        params.append(from_date)
    if to_date is not None:
        filters.append("calendar_date <= %s")
        params.append(to_date)
    if working_only:
        filters.append("is_working_day = TRUE")

    where_clause = " AND ".join(filters)
    rows = db.execute(
        f"""
        SELECT calendar_date, is_working_day, shift_count, capacity_factor, notes
        FROM operational_calendars
        WHERE {where_clause}
        ORDER BY calendar_date
        """,
        params,
    ).fetchall()

    entries = [
        CalendarEntryOutput(
            calendar_date=row["calendar_date"],
            is_working_day=row["is_working_day"],
            shift_count=row["shift_count"],
            capacity_factor=float(row["capacity_factor"]) if row["capacity_factor"] is not None else None,
            notes=row["notes"],
        )
        for row in rows
    ]

    return GetCalendarsResponse(
        location_external_id=location_external_id,
        location_id=str(location_id),
        entries=entries,
        total=len(entries),
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/calendars/working-days
# ─────────────────────────────────────────────────────────────

@router.post("/v1/calendars/working-days", response_model=WorkingDaysResponse, summary="Compute working days", description="Return the date resulting from adding N working days to a start date, respecting the location calendar.")
async def compute_working_days(
    body: WorkingDaysRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> WorkingDaysResponse:
    """
    Compute a delivery date by adding N working days to a start date.

    Algorithm:
    1. Start from start_date (not counted)
    2. Advance day by day, up to 365 iterations max
    3. Skip days where is_working_day = FALSE in operational_calendars
    4. ADR-009: absent date = working day (safe-by-default)
    5. Count until add_working_days working days have been found
    """
    location_id = _resolve_location(db, body.location_external_id)
    if location_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{
                "field": "location_external_id",
                "error": f"Location '{body.location_external_id}' not found",
            }],
        )

    n = body.add_working_days
    start = body.start_date
    max_iter = 365

    # Fetch non-working days in window
    window_start = start + timedelta(days=1)
    window_end = start + timedelta(days=max_iter)

    rows = db.execute(
        """
        SELECT calendar_date
        FROM operational_calendars
        WHERE location_id = %s
          AND calendar_date BETWEEN %s AND %s
          AND is_working_day = FALSE
        """,
        (location_id, window_start, window_end),
    ).fetchall()
    non_working: set[date] = {row["calendar_date"] for row in rows}

    current = start
    days_counted = 0
    skipped = 0
    iterations = 0

    while days_counted < n and iterations < max_iter:
        current += timedelta(days=1)
        iterations += 1
        if current in non_working:
            skipped += 1
        else:
            days_counted += 1

    logger.info(
        "calendars.working_days location=%s start=%s n=%d result=%s skipped=%d",
        body.location_external_id, start, n, current, skipped,
    )

    return WorkingDaysResponse(
        location_external_id=body.location_external_id,
        start_date=start,
        add_working_days=n,
        result_date=current,
        working_days_found=days_counted,
        non_working_days_skipped=skipped,
    )
