"""
ATP/CTP FastAPI Routers.

Provides:
  - POST /v1/atp/check — Check ATP availability
  - POST /v1/ctp/check — Check CTP (capacity-constrained) availability
  - POST /v1/ctp/simulate — Binary search for first feasible CTP date
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import List, Optional

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db
from ootils_core.atp.api import (
    ATPCheckRequest,
    ATPCheckResponse,
    ATPBucketDetail,
    _resolve_item_uuid,
    _resolve_location_uuid,
)
from ootils_core.atp.ctp import CTPEngine
from ootils_core.atp.engine import ATPEngine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["atp", "ctp"])


# ─────────────────────────────────────────────────────────────
# CTP Request/Response models
# ─────────────────────────────────────────────────────────────

class CTPCheckRequest(BaseModel):
    """Request for CTP availability check."""
    item_id: str = Field(..., description="Item external ID or UUID")
    location_id: str = Field(..., description="Location external ID or UUID")
    quantity: Decimal = Field(..., gt=0, description="Quantity requested")
    requested_date: date = Field(..., description="Date when quantity is needed (YYYY-MM-DD)")
    horizon_days: int = Field(default=365, ge=1, le=730, description="Days to look ahead")
    include_capacity: bool = Field(default=True, description="Whether to check capacity constraints")


class CapacityViolationOut(BaseModel):
    """Capacity violation details for API response."""
    resource_id: str
    resource_name: str
    violation_date: date
    required_capacity: float
    available_capacity: float
    overload_pct: float


class CTPCheckResponse(BaseModel):
    """Response from CTP availability check."""
    available: bool = Field(..., description="Whether full quantity is available on requested date")
    available_date: Optional[date] = Field(..., description="Earliest date when full quantity is available")
    quantity_available: Decimal = Field(..., description="Maximum quantity available on requested date")
    requested_quantity: Decimal = Field(..., description="Quantity originally requested")
    backorder_quantity: Decimal = Field(..., description="Quantity that cannot be fulfilled")
    capacity_feasible: Optional[bool] = Field(None, description="Whether capacity constraints are satisfied (None if not checked)")
    violations: List[CapacityViolationOut] = Field(default_factory=list, description="Capacity violations")
    critical_resources: List[str] = Field(default_factory=list, description="Critical resources checked")
    buckets: List[ATPBucketDetail] = Field(default_factory=list, description="Daily ATP breakdown")
    calculation_time_ms: float = Field(..., description="Time taken to calculate in milliseconds")


class CTPSimulateRequest(BaseModel):
    """Request for CTP simulation (binary search for first feasible date)."""
    item_id: str = Field(..., description="Item external ID or UUID")
    location_id: str = Field(..., description="Location external ID or UUID")
    quantity: Decimal = Field(..., gt=0, description="Quantity requested")
    start_date: Optional[date] = Field(default=None, description="Start date for search (default: today)")
    max_days: int = Field(default=30, ge=1, le=90, description="Maximum days to search")


class CTPSimulateOption(BaseModel):
    """Single date option from CTP simulation."""
    date: date
    feasible: bool
    atp_available: bool
    capacity_violations: int


class CTPSimulateResponse(BaseModel):
    """Response from CTP simulation."""
    first_feasible_date: Optional[date] = Field(..., description="First date when order is fully feasible")
    options: List[CTPSimulateOption] = Field(..., description="Tested dates with feasibility status")
    total_dates_tested: int = Field(..., description="Number of dates tested")


# ─────────────────────────────────────────────────────────────
# POST /v1/atp/check
# ─────────────────────────────────────────────────────────────

@router.post(
    "/atp/check",
    response_model=ATPCheckResponse,
    summary="Check ATP Availability",
    description=(
        "Check Available-to-Promise (ATP) for an item at a location.\n\n"
        "Returns whether the requested quantity is available on the requested date, "
        "along with the earliest available date if not, and a breakdown of daily ATP."
    ),
)
async def check_atp(
    body: ATPCheckRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> ATPCheckResponse:
    """
    Check ATP availability for an item at a location.
    
    - **item_id**: Item external ID or UUID
    - **location_id**: Location external ID or UUID
    - **quantity**: Quantity requested
    - **requested_date**: Date when quantity is needed
    - **horizon_days**: Days to look ahead for availability (default: 365)
    
    Returns:
    - **available**: Whether full quantity is available on requested date
    - **available_date**: Earliest date when full quantity is available
    - **quantity_available**: Maximum quantity available on requested date
    - **backorder_quantity**: Quantity that cannot be fulfilled
    - **buckets**: Daily ATP breakdown
    - **shortage_details**: Buckets with shortages
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
        )
    except Exception as e:
        logger.exception("ATP calculation failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"ATP calculation failed: {str(e)}",
        )
    
    # Import here to avoid circular dependency
    from ootils_core.atp.api import _convert_bucket_to_detail, _extract_shortage_details
    
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


# ─────────────────────────────────────────────────────────────
# POST /v1/ctp/check
# ─────────────────────────────────────────────────────────────

@router.post(
    "/ctp/check",
    response_model=CTPCheckResponse,
    summary="Check CTP Availability",
    description=(
        "Check Capable-to-Promise (CTP) for an item at a location.\n\n"
        "CTP extends ATP by checking capacity constraints on critical resources. "
        "Returns material availability (ATP) plus capacity feasibility."
    ),
)
async def check_ctp(
    body: CTPCheckRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> CTPCheckResponse:
    """
    Check CTP availability for an item at a location.
    
    - **item_id**: Item external ID or UUID
    - **location_id**: Location external ID or UUID
    - **quantity**: Quantity requested
    - **requested_date**: Date when quantity is needed
    - **horizon_days**: Days to look ahead (default: 365)
    - **include_capacity**: Whether to check capacity constraints (default: True)
    
    Returns:
    - **available**: Whether full quantity is available on requested date
    - **available_date**: Earliest date when full quantity is available
    - **capacity_feasible**: Whether capacity constraints are satisfied
    - **violations**: List of capacity violations (if any)
    - **critical_resources**: Critical resources that were checked
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
    
    # Initialize CTP engine and check
    engine = CTPEngine(db_conn=db)
    
    try:
        result = engine.check(
            item_id=item_uuid,
            location_id=location_uuid,
            quantity=body.quantity,
            requested_date=body.requested_date,
            horizon_days=body.horizon_days,
            include_capacity=body.include_capacity,
        )
    except Exception as e:
        logger.exception("CTP check failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"CTP check failed: {str(e)}",
        )
    
    # Build response
    from ootils_core.atp.api import _convert_bucket_to_detail
    
    buckets_detail = [_convert_bucket_to_detail(b) for b in result.atp_result.buckets]
    
    violations_out = [
        CapacityViolationOut(
            resource_id=v.resource_id,
            resource_name=v.resource_name,
            violation_date=v.violation_date,
            required_capacity=v.required_capacity,
            available_capacity=v.available_capacity,
            overload_pct=v.overload_pct,
        )
        for v in result.violations
    ]
    
    return CTPCheckResponse(
        available=result.atp_result.is_fully_available,
        available_date=result.atp_result.available_date,
        quantity_available=result.atp_result.available_quantity,
        requested_quantity=result.atp_result.request_quantity,
        backorder_quantity=result.atp_result.backorder_quantity,
        capacity_feasible=result.capacity_feasible,
        violations=violations_out,
        critical_resources=result.critical_resources,
        buckets=buckets_detail,
        calculation_time_ms=result.atp_result.calculation_time_ms,
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/ctp/simulate
# ─────────────────────────────────────────────────────────────

@router.post(
    "/ctp/simulate",
    response_model=CTPSimulateResponse,
    summary="Simulate CTP — Find First Feasible Date",
    description=(
        "Binary search over dates to find the first feasible CTP date.\n\n"
        "Tests multiple dates and returns the earliest date when both material "
        "and capacity are available."
    ),
)
async def simulate_ctp(
    body: CTPSimulateRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> CTPSimulateResponse:
    """
    Simulate CTP to find the first feasible date.
    
    Uses binary search to efficiently find the earliest date when:
    1. Material is available (ATP)
    2. Capacity is available on critical resources
    
    - **item_id**: Item external ID or UUID
    - **location_id**: Location external ID or UUID
    - **quantity**: Quantity requested
    - **start_date**: Start date for search (default: today)
    - **max_days**: Maximum days to search (default: 30)
    
    Returns:
    - **first_feasible_date**: First date when order is fully feasible (or None)
    - **options**: List of tested dates with feasibility status
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
    
    # Initialize CTP engine and simulate
    engine = CTPEngine(db_conn=db)
    
    try:
        results = engine.simulate_first_feasible_date(
            item_id=item_uuid,
            location_id=location_uuid,
            quantity=body.quantity,
            start_date=body.start_date,
            max_days=body.max_days,
        )
    except Exception as e:
        logger.exception("CTP simulation failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"CTP simulation failed: {str(e)}",
        )
    
    # Find first feasible date
    first_feasible = None
    options_out = []
    
    for test_date, feasible, status_info in results:
        options_out.append(
            CTPSimulateOption(
                date=test_date,
                feasible=feasible,
                atp_available=status_info["atp_available"],
                capacity_violations=status_info["capacity_violations"],
            )
        )
        if feasible and first_feasible is None:
            first_feasible = test_date
    
    return CTPSimulateResponse(
        first_feasible_date=first_feasible,
        options=options_out,
        total_dates_tested=len(results),
    )
