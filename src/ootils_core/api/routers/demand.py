"""
POST /v1/demand/forecast — Generate statistical forecast for an item/location.

Generates a demand forecast series using statistical algorithms.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db
from ootils_core.models import ForecastSeries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/demand", tags=["demand"])


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

class ForecastRequest(BaseModel):
    item_id: str
    location_id: str
    horizon_days: int = 90
    forecast_method: str = "statistical"  # statistical | consensus | machine_learning
    confidence_level: Decimal = Decimal("0.8")
    scenario_id: Optional[str] = None  # defaults to baseline


class ForecastBucket(BaseModel):
    bucket_start: date
    bucket_end: date
    forecast_quantity: Decimal
    confidence_interval_lower: Decimal
    confidence_interval_upper: Decimal


class ForecastResponse(BaseModel):
    series_id: UUID
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    horizon_start: date
    horizon_end: date
    forecast_method: str
    confidence_level: Decimal
    buckets: list[ForecastBucket]
    created_at: str


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _resolve_item_uuid(db: psycopg.Connection, external_id: str) -> UUID | None:
    """Resolve item external_id → item_id UUID."""
    row = db.execute(
        "SELECT item_id FROM items WHERE external_id = %s AND status != 'obsolete'",
        (external_id,),
    ).fetchone()
    return row["item_id"] if row else None


def _resolve_location_uuid(db: psycopg.Connection, external_id: str) -> UUID | None:
    """Resolve location external_id → location_id UUID."""
    row = db.execute(
        "SELECT location_id FROM locations WHERE external_id = %s",
        (external_id,),
    ).fetchone()
    return row["location_id"] if row else None


def _resolve_scenario_uuid(db: psycopg.Connection, scenario_id_str: str | None) -> UUID:
    """Resolve scenario_id string → UUID, defaulting to baseline."""
    if scenario_id_str is None or scenario_id_str.lower() == "baseline":
        return BASELINE_SCENARIO_ID
    try:
        return UUID(scenario_id_str)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid scenario_id '{scenario_id_str}' — must be a valid UUID or 'baseline'",
        )


def _generate_statistical_forecast(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID,
    horizon_start: date,
    horizon_end: date,
) -> list[ForecastBucket]:
    """
    Generate a simple statistical forecast based on historical sales.
    Placeholder implementation: returns flat forecast.
    """
    # In a real implementation, you would query historical sales data
    # and apply time series forecasting (e.g., moving average, exponential smoothing).
    # For now, we return dummy data.
    buckets = []
    current = horizon_start
    while current <= horizon_end:
        next_date = current + timedelta(days=7)  # weekly buckets
        if next_date > horizon_end:
            next_date = horizon_end
        # Dummy forecast quantity
        forecast_qty = Decimal("100.0")
        lower = forecast_qty * Decimal("0.8")
        upper = forecast_qty * Decimal("1.2")
        buckets.append(ForecastBucket(
            bucket_start=current,
            bucket_end=next_date,
            forecast_quantity=forecast_qty,
            confidence_interval_lower=lower,
            confidence_interval_upper=upper,
        ))
        current = next_date + timedelta(days=1)
    return buckets


# ─────────────────────────────────────────────────────────────
# POST /v1/demand/forecast
# ─────────────────────────────────────────────────────────────

@router.post(
    "/forecast",
    response_model=ForecastResponse,
    summary="Generate demand forecast",
    description="Generate a statistical demand forecast for an item/location.",
)
async def generate_forecast(
    body: ForecastRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> ForecastResponse:
    """Generate a demand forecast series."""

    # 1. Resolve item/location UUIDs
    item_uuid = _resolve_item_uuid(db, body.item_id)
    if item_uuid is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Item '{body.item_id}' not found",
        )

    location_uuid = _resolve_location_uuid(db, body.location_id)
    if location_uuid is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Location '{body.location_id}' not found",
        )

    # 2. Resolve scenario_id (default baseline)
    scenario_uuid = _resolve_scenario_uuid(db, body.scenario_id)

    # 3. Determine horizon dates
    horizon_start = date.today()
    horizon_end = horizon_start + timedelta(days=body.horizon_days)

    # 4. Generate forecast buckets
    buckets = _generate_statistical_forecast(db, item_uuid, location_uuid, horizon_start, horizon_end)

    # 5. Create forecast series record
    series_id = uuid4()
    db.execute(
        """
        INSERT INTO forecast_series (
            series_id, item_id, location_id, scenario_id,
            horizon_start, horizon_end, forecast_method, confidence_level,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            now(), now()
        )
        """,
        (series_id, item_uuid, location_uuid, scenario_uuid,
         horizon_start, horizon_end, body.forecast_method, body.confidence_level),
    )

    # 6. Insert forecast bucket details (assuming table forecast_buckets exists)
    for bucket in buckets:
        bucket_id = uuid4()
        db.execute(
            """
            INSERT INTO forecast_buckets (
                bucket_id, series_id, bucket_start, bucket_end,
                forecast_quantity, confidence_interval_lower, confidence_interval_upper,
                created_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                now()
            )
            """,
            (bucket_id, series_id, bucket.bucket_start, bucket.bucket_end,
             bucket.forecast_quantity, bucket.confidence_interval_lower, bucket.confidence_interval_upper),
        )

    logger.info(
        "demand.forecast item=%s location=%s scenario=%s — forecast series %s created",
        body.item_id, body.location_id, scenario_uuid, series_id,
    )

    return ForecastResponse(
        series_id=series_id,
        item_id=item_uuid,
        location_id=location_uuid,
        scenario_id=scenario_uuid,
        horizon_start=horizon_start,
        horizon_end=horizon_end,
        forecast_method=body.forecast_method,
        confidence_level=body.confidence_level,
        buckets=buckets,
        created_at=str(date.today()),
    )