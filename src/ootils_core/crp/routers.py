"""
CRP (Capacity Requirements Planning) FastAPI Routers.

Provides:
  - POST /v1/crp/calculate — Trigger CRP calculation
  - GET /v1/crp/load-profile/{work_center_id} — Get load profile for a work center
  - GET /v1/crp/overloads — List detected overloads
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import List, Optional, Dict, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status, Path
from pydantic import BaseModel, ConfigDict, Field

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.crp.engine import CRPEngine
from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/crp", tags=["crp"])


# ─────────────────────────────────────────────────────────────
# Request/Response models
# ─────────────────────────────────────────────────────────────

class CRPCalculateRequest(BaseModel):
    """Request for CRP calculation.

    scenario_id is NOT a body field: it is resolved from the ``scenario_id`` query
    param or the ``X-Scenario-ID`` header (see resolve_scenario_id), consistent with
    ATP/RCCP. Defaults to baseline.
    """
    # extra="forbid": scenario_id used to be a body field and moved to
    # query/header. Without this, an old client (or one following a stale
    # openapi.json) POSTing scenario_id in the body would have it silently
    # dropped by Pydantic's default extra="ignore" — landing on baseline with
    # a 200 OK, the exact silent-fallback bug this module was fixed to avoid.
    # A stray body field must fail loudly (422), not disappear.
    model_config = ConfigDict(extra="forbid")

    horizon_days: int = Field(default=90, ge=1, le=365, description="Planning horizon in days")
    work_center_ids: Optional[List[str]] = Field(None, description="Optional list of work center IDs to include")


class LoadBucketOut(BaseModel):
    """Daily load bucket for API response."""
    work_center_id: str
    bucket_date: date
    load_hours: float
    capacity_hours: float
    overload_hours: float
    is_overloaded: bool


class LoadProfileOut(BaseModel):
    """Load profile for a work center."""
    work_center_id: str
    work_center_code: str
    buckets: List[LoadBucketOut]
    total_load_hours: float
    total_capacity_hours: float
    overload_count: int


class OverloadOut(BaseModel):
    """Overload detail for API response."""
    work_center_id: str
    work_center_code: str
    overload_date: date
    load_hours: float
    capacity_hours: float
    excess_hours: float


class CRPCalculateResponse(BaseModel):
    """Response from CRP calculation."""
    calculation_id: str
    horizon_start: date
    horizon_end: date
    planned_orders_count: int
    work_centers_count: int
    overload_count: int
    load_profiles: Dict[str, LoadProfileOut]
    overloads: List[OverloadOut]
    calculation_time_ms: float


class CRPOverloadsResponse(BaseModel):
    """Response from overloads query."""
    horizon_start: date
    horizon_end: date
    total_overloads: int
    work_centers_affected: int
    overloads: List[OverloadOut]
    calculation_time_ms: float


# ─────────────────────────────────────────────────────────────
# POST /v1/crp/calculate
# ─────────────────────────────────────────────────────────────

@router.post(
    "/calculate",
    response_model=CRPCalculateResponse,
    summary="Calculate CRP",
    description=(
        "Perform Capacity Requirements Planning calculation.\n\n"
        "Explodes planned orders into operations via routings, computes load per work center "
        "per day using backward scheduling, and detects overloads (infinite loading)."
    ),
)
async def calculate_crp(
    body: CRPCalculateRequest,
    scenario_id: UUID = Depends(resolve_scenario_id),
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> CRPCalculateResponse:
    """
    Calculate CRP load profiles and detect overloads.

    - **horizon_days**: Planning horizon in days (default: 90, max: 365)
    - **work_center_ids**: Optional list of work center IDs to include (default: all active)
    - **scenario_id**: Scenario to read from (query param or X-Scenario-ID header; default: baseline)

    Returns:
    - **calculation_id**: Unique identifier for this calculation
    - **horizon_start/end**: Date range of the planning horizon
    - **planned_orders_count**: Number of planned orders processed
    - **work_centers_count**: Number of work centers in the result
    - **overload_count**: Total number of overloads detected
    - **load_profiles**: Load profiles per work center
    - **overloads**: List of all detected overloads
    - **calculation_time_ms**: Time taken to calculate
    """
    # Convert work center IDs from strings to UUIDs
    work_center_uuids: Optional[List[UUID]] = None
    if body.work_center_ids:
        work_center_uuids = []
        for wc_id in body.work_center_ids:
            try:
                work_center_uuids.append(UUID(wc_id))
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid work center ID format: {wc_id}",
                )
    
    # Initialize CRP engine and calculate
    engine = CRPEngine(db_conn=db)

    try:
        result = engine.calculate(
            horizon_days=body.horizon_days,
            work_centers=work_center_uuids,
            scenario_id=scenario_id,
        )
    except Exception as e:
        logger.exception("CRP calculation failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="CRP calculation failed.",
        ) from e
    
    # Convert load profiles to output format
    load_profiles_out: Dict[str, LoadProfileOut] = {}
    for wc_uuid, profile in result.load_profiles.items():
        buckets_out = [
            LoadBucketOut(
                work_center_id=str(b.work_center_id),
                bucket_date=b.bucket_date,
                load_hours=float(b.load_hours),
                capacity_hours=float(b.capacity_hours),
                overload_hours=float(b.overload_hours),
                is_overloaded=b.is_overloaded,
            )
            for b in profile.buckets
        ]
        load_profiles_out[str(wc_uuid)] = LoadProfileOut(
            work_center_id=str(profile.work_center_id),
            work_center_code=profile.work_center_code,
            buckets=buckets_out,
            total_load_hours=float(profile.get_total_load()),
            total_capacity_hours=float(profile.get_total_capacity()),
            overload_count=profile.get_overload_count(),
        )
    
    # Convert overloads to output format
    overloads_out = [
        OverloadOut(
            work_center_id=str(o.work_center_id),
            work_center_code=o.work_center_code,
            overload_date=o.overload_date,
            load_hours=float(o.load_hours),
            capacity_hours=float(o.capacity_hours),
            excess_hours=float(o.excess_hours),
        )
        for o in result.overloads
    ]
    
    return CRPCalculateResponse(
        calculation_id=str(result.calculation_id),
        horizon_start=result.horizon_start,
        horizon_end=result.horizon_end,
        planned_orders_count=result.planned_orders_count,
        work_centers_count=result.work_centers_count,
        overload_count=len(result.overloads),
        load_profiles=load_profiles_out,
        overloads=overloads_out,
        calculation_time_ms=result.calculation_time_ms,
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/crp/load-profile/{work_center_id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/load-profile/{work_center_id}",
    response_model=LoadProfileOut,
    summary="Get Work Center Load Profile",
    description=(
        "Get the load profile for a specific work center over the planning horizon.\n\n"
        "Returns daily load buckets showing scheduled load vs. effective capacity."
    ),
)
async def get_load_profile(
    work_center_id: UUID = Path(..., description="Work center UUID"),
    horizon_days: int = Query(default=90, ge=1, le=365, description="Planning horizon in days"),
    scenario_id: UUID = Depends(resolve_scenario_id),
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> LoadProfileOut:
    """
    Get load profile for a specific work center.

    - **work_center_id**: UUID of the work center
    - **horizon_days**: Planning horizon in days (default: 90)
    - **scenario_id**: Scenario to read from (query param or X-Scenario-ID header; default: baseline)

    Returns:
    - **work_center_id**: UUID of the work center
    - **work_center_code**: Human-readable code
    - **buckets**: Daily load buckets
    - **total_load_hours**: Total load across the horizon
    - **total_capacity_hours**: Total capacity across the horizon
    - **overload_count**: Number of days with overloads
    """
    engine = CRPEngine(db_conn=db)

    try:
        profile = engine.get_load_profile(
            work_center_id=work_center_id,
            horizon_days=horizon_days,
            scenario_id=scenario_id,
        )
    except Exception as e:
        logger.exception("Failed to get load profile: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get load profile.",
        ) from e
    
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Work center '{work_center_id}' not found or no load data available",
        )
    
    buckets_out = [
        LoadBucketOut(
            work_center_id=str(b.work_center_id),
            bucket_date=b.bucket_date,
            load_hours=float(b.load_hours),
            capacity_hours=float(b.capacity_hours),
            overload_hours=float(b.overload_hours),
            is_overloaded=b.is_overloaded,
        )
        for b in profile.buckets
    ]
    
    return LoadProfileOut(
        work_center_id=str(profile.work_center_id),
        work_center_code=profile.work_center_code,
        buckets=buckets_out,
        total_load_hours=float(profile.get_total_load()),
        total_capacity_hours=float(profile.get_total_capacity()),
        overload_count=profile.get_overload_count(),
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/crp/overloads
# ─────────────────────────────────────────────────────────────

@router.get(
    "/overloads",
    response_model=CRPOverloadsResponse,
    summary="List CRP Overloads",
    description=(
        "List all capacity overloads detected in the planning horizon.\n\n"
        "Overloads occur when scheduled load exceeds effective capacity at a work center."
    ),
)
async def get_overloads(
    horizon_days: int = Query(default=90, ge=1, le=365, description="Planning horizon in days"),
    work_center_ids: Optional[str] = Query(None, description="Comma-separated list of work center IDs to filter"),
    scenario_id: UUID = Depends(resolve_scenario_id),
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> CRPOverloadsResponse:
    """
    List all detected overloads.

    - **horizon_days**: Planning horizon in days (default: 90)
    - **work_center_ids**: Optional comma-separated list of work center IDs to filter
    - **scenario_id**: Scenario to read from (query param or X-Scenario-ID header; default: baseline)

    Returns:
    - **horizon_start/end**: Date range of the planning horizon
    - **total_overloads**: Total number of overloads
    - **work_centers_affected**: Number of unique work centers with overloads
    - **overloads**: List of overload details
    - **calculation_time_ms**: Time taken to calculate
    """
    # Parse work center IDs if provided
    work_center_uuids: Optional[List[UUID]] = None
    if work_center_ids:
        work_center_uuids = []
        for wc_id in work_center_ids.split(","):
            wc_id = wc_id.strip()
            if wc_id:
                try:
                    work_center_uuids.append(UUID(wc_id))
                except ValueError:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid work center ID format: {wc_id}",
                    )
    
    engine = CRPEngine(db_conn=db)
    
    try:
        overloads = engine.get_overloads(
            horizon_days=horizon_days,
            work_centers=work_center_uuids,
            scenario_id=scenario_id,
        )
    except Exception as e:
        logger.exception("Failed to get overloads: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get overloads.",
        ) from e
    
    # Calculate horizon dates (approximate based on current calculation)
    horizon_start = date.today()
    horizon_end = horizon_start + timedelta(days=horizon_days)
    
    # Count unique work centers affected
    work_centers_affected = len(set(o.work_center_id for o in overloads))
    
    overloads_out = [
        OverloadOut(
            work_center_id=str(o.work_center_id),
            work_center_code=o.work_center_code,
            overload_date=o.overload_date,
            load_hours=float(o.load_hours),
            capacity_hours=float(o.capacity_hours),
            excess_hours=float(o.excess_hours),
        )
        for o in overloads
    ]
    
    # Note: calculation_time_ms is not available from get_overloads directly
    # We'll set it to 0 or estimate based on the query
    return CRPOverloadsResponse(
        horizon_start=horizon_start,
        horizon_end=horizon_end,
        total_overloads=len(overloads),
        work_centers_affected=work_centers_affected,
        overloads=overloads_out,
        calculation_time_ms=0.0,
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/crp/suggest-resolutions
# ─────────────────────────────────────────────────────────────

class ResolutionSuggestionOut(BaseModel):
    """Suggested resolution for an overload."""
    work_center_id: str
    work_center_code: str
    overload_date: date
    excess_hours: float
    suggested_orders: List[Dict[str, Any]]
    total_hours_freed: float
    recommendation: str


class CRPSuggestResolutionsRequest(BaseModel):
    """Request for CRP resolution suggestions."""
    horizon_days: int = Field(default=90, ge=1, le=365, description="Planning horizon in days")
    work_center_ids: Optional[List[str]] = Field(None, description="Optional list of work center IDs to include")
    max_shift_days: int = Field(default=14, ge=1, le=90, description="Maximum days to suggest shifting orders")


class CRPSuggestResolutionsResponse(BaseModel):
    """Response from CRP suggest resolutions."""
    horizon_start: date
    horizon_end: date
    total_suggestions: int
    suggestions: List[ResolutionSuggestionOut]


@router.post(
    "/suggest-resolutions",
    response_model=CRPSuggestResolutionsResponse,
    summary="Suggest Resolutions for Overloads",
    description=(
        "Analyze detected overloads and suggest order date shifts to resolve them.\n\n"
        "Strategy: For each overload, identifies planned orders contributing to the overload "
        "and suggests shifting them to the next available capacity slot."
    ),
)
async def suggest_resolutions(
    body: CRPSuggestResolutionsRequest,
    scenario_id: UUID = Depends(resolve_scenario_id),
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> CRPSuggestResolutionsResponse:
    """
    Suggest resolutions for CRP overloads.

    - **horizon_days**: Planning horizon in days (default: 90)
    - **work_center_ids**: Optional list of work center IDs to analyze
    - **max_shift_days**: Maximum days to suggest shifting (default: 14)
    - **scenario_id**: Scenario to read from (query param or X-Scenario-ID header; default: baseline)

    Returns:
    - **horizon_start/end**: Date range of the planning horizon
    - **total_suggestions**: Number of resolution suggestions
    - **suggestions**: List of suggestions with order shift recommendations
    """
    # Convert work center IDs from strings to UUIDs
    work_center_uuids: Optional[List[UUID]] = None
    if body.work_center_ids:
        work_center_uuids = []
        for wc_id in body.work_center_ids:
            try:
                work_center_uuids.append(UUID(wc_id))
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid work center ID format: {wc_id}",
                )
    
    engine = CRPEngine(db_conn=db)
    
    try:
        suggestions = engine.suggest_resolutions(
            horizon_days=body.horizon_days,
            work_centers=work_center_uuids,
            max_shift_days=body.max_shift_days,
            scenario_id=scenario_id,
        )
    except Exception as e:
        logger.exception("Failed to suggest resolutions: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to suggest resolutions.",
        ) from e
    
    horizon_start = date.today()
    horizon_end = horizon_start + timedelta(days=body.horizon_days)
    
    suggestions_out = [
        ResolutionSuggestionOut(
            work_center_id=s["work_center_id"],
            work_center_code=s["work_center_code"],
            overload_date=date.fromisoformat(s["overload_date"]),
            excess_hours=s["excess_hours"],
            suggested_orders=s["suggested_orders"],
            total_hours_freed=s["total_hours_freed"],
            recommendation=s["recommendation"],
        )
        for s in suggestions
    ]
    
    return CRPSuggestResolutionsResponse(
        horizon_start=horizon_start,
        horizon_end=horizon_end,
        total_suggestions=len(suggestions_out),
        suggestions=suggestions_out,
    )
