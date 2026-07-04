"""
ATP API — REST endpoints for Available-to-Promise checks.

Provides:
  - POST /v1/atp/check — Check availability for item/location/quantity/date
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from fastapi import Depends, HTTPException, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.atp.engine import ATPEngine
from ootils_core.atp.models import ATPBucket, ATPResult
from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Request/Response models
# ─────────────────────────────────────────────────────────────

class ATPCheckRequest(BaseModel):
    """Request for ATP availability check."""
    item_id: str = Field(..., description="Item external ID or UUID")
    location_id: str = Field(..., description="Location external ID or UUID")
    quantity: Decimal = Field(..., gt=0, description="Quantity requested")
    requested_date: date = Field(..., description="Date when quantity is needed (YYYY-MM-DD)")
    horizon_days: int = Field(default=365, ge=1, le=730, description="Days to look ahead for availability")


class ShortageDetail(BaseModel):
    """Details about a shortage in a specific bucket."""
    bucket_start: date
    bucket_end: date
    shortage_quantity: Decimal
    cumulative_atp: Decimal


class ATPCheckResponse(BaseModel):
    """Response from ATP availability check."""
    available: bool = Field(..., description="Whether full quantity is available on requested date")
    available_date: Optional[date] = Field(..., description="Earliest date when full quantity is available")
    quantity_available: Decimal = Field(..., description="Maximum quantity available on requested date")
    requested_quantity: Decimal = Field(..., description="Quantity originally requested")
    backorder_quantity: Decimal = Field(..., description="Quantity that cannot be fulfilled (if any)")
    buckets: List[ATPBucketDetail] = Field(default_factory=list, description="Daily ATP breakdown")
    shortage_details: List[ShortageDetail] = Field(default_factory=list, description="Buckets with shortages")
    calculation_time_ms: float = Field(..., description="Time taken to calculate in milliseconds")


class ATPBucketDetail(BaseModel):
    """Simplified bucket for API response."""
    bucket_start: date
    bucket_end: date
    opening_atp: Decimal
    supply_quantity: Decimal
    demand_quantity: Decimal
    closing_atp: Decimal
    is_shortage: bool


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _resolve_item_uuid(db: DictRowConnection, external_id: str) -> Optional[UUID]:
    """Resolve item external_id → item_id UUID."""
    # Try as UUID first
    try:
        return UUID(external_id)
    except ValueError:
        pass
    
    # Try external_id lookup
    row = db.execute(
        "SELECT item_id FROM items WHERE external_id = %s AND status != 'obsolete'",
        (external_id,),
    ).fetchone()
    return row["item_id"] if row else None


def _resolve_location_uuid(db: DictRowConnection, external_id: str) -> Optional[UUID]:
    """Resolve location external_id → location_id UUID."""
    # Try as UUID first
    try:
        return UUID(external_id)
    except ValueError:
        pass
    
    # Try external_id lookup
    row = db.execute(
        "SELECT location_id FROM locations WHERE external_id = %s",
        (external_id,),
    ).fetchone()
    return row["location_id"] if row else None


def _convert_bucket_to_detail(bucket: ATPBucket) -> ATPBucketDetail:
    """Convert ATPBucket to ATPBucketDetail for API response."""
    return ATPBucketDetail(
        bucket_start=bucket.bucket_start,
        bucket_end=bucket.bucket_end,
        opening_atp=bucket.opening_atp,
        supply_quantity=bucket.supply_quantity,
        demand_quantity=bucket.demand_quantity,
        closing_atp=bucket.closing_atp,
        is_shortage=bucket.is_shortage,
    )


def _extract_shortage_details(result: ATPResult) -> List[ShortageDetail]:
    """Extract buckets with shortages from ATP result."""
    shortages = []
    for bucket in result.buckets:
        if bucket.is_shortage or bucket.closing_atp < 0:
            shortages.append(ShortageDetail(
                bucket_start=bucket.bucket_start,
                bucket_end=bucket.bucket_end,
                shortage_quantity=abs(bucket.closing_atp) if bucket.closing_atp < 0 else Decimal("0"),
                cumulative_atp=bucket.closing_atp,
            ))
    return shortages


# ─────────────────────────────────────────────────────────────
# ATP Check Endpoint
# ─────────────────────────────────────────────────────────────

async def check_atp_availability(
    body: ATPCheckRequest,
    scenario_id: UUID = Depends(resolve_scenario_id),
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> ATPCheckResponse:
    """
    Check ATP availability for an item at a location.
    
    Returns whether the requested quantity is available on the requested date,
    along with the earliest available date if not, and a breakdown of daily ATP.
    """
    # Resolve item and location UUIDs
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
    
    # Initialize ATP engine and calculate
    engine = ATPEngine(db_conn=db)
    
    try:
        result = engine.calculate(
            item_id=item_uuid,
            location_id=location_uuid,
            quantity=body.quantity,
            request_date=body.requested_date,
            horizon_days=body.horizon_days,
            scenario_id=scenario_id,
        )
    except Exception as e:
        logger.exception("ATP calculation failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"ATP calculation failed: {str(e)}",
        )
    
    # Build response
    buckets_detail = [_convert_bucket_to_detail(b) for b in result.buckets]
    shortage_details = _extract_shortage_details(result)
    
    return ATPCheckResponse(
        available=result.is_fully_available,
        available_date=result.available_date,
        quantity_available=result.available_quantity,
        requested_quantity=result.request_quantity,
        backorder_quantity=result.backorder_quantity,
        buckets=buckets_detail,
        shortage_details=shortage_details,
        calculation_time_ms=result.calculation_time_ms,
    )
