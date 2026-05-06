"""
MPS API endpoints — REST interface for Master Production Schedule.

Endpoints:
  POST /v1/mps/aggregate-demand — Aggregate demand and create MPS nodes
  GET  /v1/mps/nodes             — List MPS nodes with filters
  GET  /v1/mps/nodes/{mps_id}    — Get specific MPS node
  POST /v1/mps/capacity-check    — Check capacity feasibility for MPS nodes
  GET  /v1/mps/{id}/suggest-adjustments — Get adjustment suggestions
  POST /v1/mps/{id}/promote-to-mrp — Promote MPS to MRP and trigger BOM explosion
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db
from ootils_core.mps.engine import AggregateDemandEngine
from ootils_core.mps.capacity_engine import CapacityCheckEngine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/mps", tags=["mps"])


# ─────────────────────────────────────────────────────────────
# Pydantic models - Requests
# ─────────────────────────────────────────────────────────────

class AggregateDemandRequest(BaseModel):
    """Request to aggregate demand and create MPS nodes."""
    item_id: str = Field(..., description="Item external ID or UUID")
    location_id: str = Field(..., description="Location external ID or UUID")
    scenario_id: Optional[str] = Field(default=None, description="Scenario ID (defaults to baseline)")
    horizon_start: str = Field(..., description="Horizon start date (YYYY-MM-DD)")
    horizon_end: str = Field(..., description="Horizon end date (YYYY-MM-DD)")
    time_grain: str = Field(default="weekly", pattern="^(daily|weekly|monthly)$")
    forecast_weight: Decimal = Field(default=Decimal("0.5"), ge=0, le=1)
    orders_weight: Decimal = Field(default=Decimal("0.5"), ge=0, le=1)
    clear_existing: bool = Field(default=False, description="Clear existing MPS nodes before creating")

    @field_validator("forecast_weight", "orders_weight")
    @classmethod
    def validate_weights(cls, v: Decimal) -> Decimal:
        if v < 0 or v > 1:
            raise ValueError("Weight must be between 0 and 1")
        return v


# ─────────────────────────────────────────────────────────────
# Pydantic models - Responses
# ─────────────────────────────────────────────────────────────

class MPSNodeSummaryOut(BaseModel):
    """Summary of a single MPS node."""
    mps_id: UUID
    time_bucket: str
    time_bucket_start: date
    time_bucket_end: date
    forecast_quantity: Decimal
    sales_orders_quantity: Decimal
    total_demand: Decimal
    planned_quantity: Decimal
    status: str


class DemandByPeriod(BaseModel):
    """Demand breakdown by period."""
    time_bucket: str
    time_bucket_start: str
    time_bucket_end: str
    forecast_quantity: str
    sales_orders_quantity: str
    total_demand: str


class AggregateDemandResponse(BaseModel):
    """Response from aggregate demand endpoint."""
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    horizon_start: date
    horizon_end: date
    time_grain: str
    mps_nodes_created: int
    mps_nodes_updated: int
    total_demand: Decimal
    demand_by_source: Dict[str, str]
    demand_by_period: List[DemandByPeriod]
    mps_node_ids: List[UUID]
    mps_nodes: List[MPSNodeSummaryOut]


class MPSNodeDetailOut(BaseModel):
    """Detailed MPS node output."""
    mps_id: UUID
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    time_bucket: str
    time_bucket_start: date
    time_bucket_end: date
    time_grain: str
    forecast_quantity: Decimal
    sales_orders_quantity: Decimal
    total_demand: Decimal
    planned_quantity: Decimal
    status: str
    created_by: Optional[str]
    reviewed_by: Optional[str]
    approved_by: Optional[str]
    released_by: Optional[str]
    reviewed_at: Optional[date]
    approved_at: Optional[date]
    released_at: Optional[date]
    notes: Optional[str]
    active: bool
    created_at: date
    updated_at: date


class MPSNodeListResponse(BaseModel):
    """Response from list MPS nodes endpoint."""
    mps_nodes: List[MPSNodeSummaryOut]
    total_count: int
    filters_applied: Dict[str, Any]


# ─────────────────────────────────────────────────────────────
# Pydantic models — Capacity Check
# ─────────────────────────────────────────────────────────────

class CapacityViolationOut(BaseModel):
    """Output model for capacity violation."""
    violation_type: str
    resource_id: UUID
    resource_external_id: str
    resource_name: str
    period_start: date
    period_end: date
    required_capacity: str
    available_capacity: str
    overload_pct: str
    affected_mps_ids: List[UUID]
    severity: str


class AdjustmentSuggestionOut(BaseModel):
    """Output model for adjustment suggestion."""
    suggestion_type: str
    mps_id: UUID
    description: str
    original_quantity: str
    suggested_quantity: Optional[str]
    original_date: date
    suggested_date: Optional[date]
    impact_description: str
    confidence: str


class CapacityCheckRequest(BaseModel):
    """Request to check capacity feasibility for MPS nodes."""
    mps_node_ids: List[str] = Field(..., description="List of MPS node IDs to check")
    horizon_buffer_days: int = Field(default=7, ge=0, le=90, description="Buffer days before/after horizon")


class CapacityCheckResponse(BaseModel):
    """Response from capacity check endpoint."""
    feasible: bool
    violations: List[CapacityViolationOut]
    suggested_adjustments: List[AdjustmentSuggestionOut]
    summary: Dict[str, Any]


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _resolve_item_uuid(db: psycopg.Connection, external_id: str) -> UUID | None:
    """Resolve item external_id → item_id UUID."""
    # Try UUID first
    try:
        uuid = UUID(external_id)
        row = db.execute(
            "SELECT item_id FROM items WHERE item_id = %s AND status != 'obsolete'",
            (uuid,),
        ).fetchone()
        if row:
            return row["item_id"]
    except ValueError:
        pass

    # Try external_id
    row = db.execute(
        "SELECT item_id FROM items WHERE external_id = %s AND status != 'obsolete'",
        (external_id,),
    ).fetchone()
    return row["item_id"] if row else None


def _resolve_location_uuid(db: psycopg.Connection, external_id: str) -> UUID | None:
    """Resolve location external_id → location_id UUID."""
    # Try UUID first
    try:
        uuid = UUID(external_id)
        row = db.execute(
            "SELECT location_id FROM locations WHERE location_id = %s",
            (uuid,),
        ).fetchone()
        if row:
            return row["location_id"]
    except ValueError:
        pass

    # Try external_id
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
            detail=f"Invalid scenario_id '{scenario_id_str}' - must be a valid UUID or 'baseline'",
        )


# ─────────────────────────────────────────────────────────────
# POST /v1/mps/aggregate-demand
# ─────────────────────────────────────────────────────────────

@router.post(
    "/aggregate-demand",
    response_model=AggregateDemandResponse,
    summary="Aggregate demand",
    description=(
        "Aggregate demand from forecast and sales orders, then create MPS nodes.\n\n"
        "**Process:**\n"
        "1. Fetch forecast values for the item/location/scenario\n"
        "2. Fetch sales orders (CustomerOrderDemand) for the item/location\n"
        "3. Aggregate by time bucket (daily/weekly/monthly)\n"
        "4. Apply configurable weights (forecast_weight vs orders_weight)\n"
        "5. Create or update MPS nodes (idempotent upsert)\n\n"
        "**Parameters:**\n"
        "- forecast_weight: Weight applied to forecast quantities (0-1, default 0.5)\n"
        "- orders_weight: Weight applied to sales orders (0-1, default 0.5)\n"
        "- clear_existing: If true, deactivate existing MPS nodes before creating new ones\n\n"
        "**Returns:**\n"
        "- Summary: total demand, breakdown by source, breakdown by period\n"
        "- List of created/updated MPS node IDs\n"
        "- Full MPS node summaries"
    ),
)
async def aggregate_demand(
    body: AggregateDemandRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> AggregateDemandResponse:
    """
    Aggregate demand and create MPS nodes.

    Endpoint principal pour la création de MPS nodes par agrégation
    de la demande forecast et des sales orders.
    """
    # 1. Resolve item/location/scenario UUIDs
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

    scenario_uuid = _resolve_scenario_uuid(db, body.scenario_id)

    # Parse dates
    try:
        horizon_start = date.fromisoformat(body.horizon_start)
        horizon_end = date.fromisoformat(body.horizon_end)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid date format: {e}",
        )

    if horizon_end < horizon_start:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"horizon_end ({horizon_end}) cannot be before horizon_start ({horizon_start})",
        )

    # 2. Execute aggregation
    engine = AggregateDemandEngine()
    try:
        result = engine.aggregate(
            db=db,
            item_id=item_uuid,
            location_id=location_uuid,
            scenario_id=scenario_uuid,
            horizon_start=horizon_start,
            horizon_end=horizon_end,
            time_grain=body.time_grain,
            forecast_weight=body.forecast_weight,
            orders_weight=body.orders_weight,
            clear_existing=body.clear_existing,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )
    except Exception as e:
        logger.exception("mps.aggregate_demand failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Aggregation failed: {str(e)}",
        )

    # 3. Fetch MPS node summaries for response
    mps_summaries = engine.get_mps_nodes_summary(
        db=db,
        item_id=item_uuid,
        location_id=location_uuid,
        scenario_id=scenario_uuid,
        horizon_start=horizon_start,
        horizon_end=horizon_end,
    )

    # 4. Build response
    demand_by_source_str = {
        k: str(v) for k, v in result.demand_by_source.items()
    }

    demand_by_period = [
        DemandByPeriod(
            time_bucket=p["time_bucket"],
            time_bucket_start=p["time_bucket_start"],
            time_bucket_end=p["time_bucket_end"],
            forecast_quantity=p["forecast_quantity"],
            sales_orders_quantity=p["sales_orders_quantity"],
            total_demand=p["total_demand"],
        )
        for p in result.demand_by_period
    ]

    mps_nodes_out = [
        MPSNodeSummaryOut(
            mps_id=s.mps_id,
            time_bucket=s.time_bucket,
            time_bucket_start=s.time_bucket_start,
            time_bucket_end=s.time_bucket_end,
            forecast_quantity=s.forecast_quantity,
            sales_orders_quantity=s.sales_orders_quantity,
            total_demand=s.total_demand,
            planned_quantity=s.planned_quantity,
            status=s.status,
        )
        for s in mps_summaries
    ]

    return AggregateDemandResponse(
        item_id=item_uuid,
        location_id=location_uuid,
        scenario_id=scenario_uuid,
        horizon_start=horizon_start,
        horizon_end=horizon_end,
        time_grain=body.time_grain,
        mps_nodes_created=result.mps_nodes_created,
        mps_nodes_updated=result.mps_nodes_updated,
        total_demand=result.total_demand,
        demand_by_source=demand_by_source_str,
        demand_by_period=demand_by_period,
        mps_node_ids=result.mps_node_ids,
        mps_nodes=mps_nodes_out,
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/mps/nodes
# ─────────────────────────────────────────────────────────────

@router.get(
    "/nodes",
    response_model=MPSNodeListResponse,
    summary="List MPS nodes",
    description=(
        "List MPS nodes with optional filters.\n\n"
        "**Filters:**\n"
        "- item_id: Filter by item external ID or UUID\n"
        "- location_id: Filter by location external ID or UUID\n"
        "- scenario_id: Filter by scenario ID (defaults to baseline)\n"
        "- date_from: Filter nodes starting on or after this date\n"
        "- date_to: Filter nodes ending on or before this date\n"
        "- status: Filter by status (DRAFT, REVIEWED, APPROVED, RELEASED)"
    ),
)
async def list_mps_nodes(
    item_id: Optional[str] = Query(default=None),
    location_id: Optional[str] = Query(default=None),
    scenario_id: Optional[str] = Query(default=None),
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    status: Optional[str] = Query(default=None, pattern="^(DRAFT|REVIEWED|APPROVED|RELEASED)$"),
    limit: int = Query(default=100, ge=1, le=1000),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> MPSNodeListResponse:
    """
    List MPS nodes with optional filters.
    """
    # Build query
    base_query = """
        SELECT
            mps_id, item_id, location_id, scenario_id,
            time_bucket, time_bucket_start, time_bucket_end, time_grain,
            forecast_quantity, sales_orders_quantity, total_demand,
            planned_quantity, status
        FROM mps_nodes
        WHERE active = TRUE
    """

    params: List[Any] = []
    conditions: List[str] = []

    if item_id:
        item_uuid = _resolve_item_uuid(db, item_id)
        if item_uuid:
            conditions.append("item_id = %s")
            params.append(item_uuid)
        else:
            return MPSNodeListResponse(mps_nodes=[], total_count=0, filters_applied={})

    if location_id:
        location_uuid = _resolve_location_uuid(db, location_id)
        if location_uuid:
            conditions.append("location_id = %s")
            params.append(location_uuid)
        else:
            return MPSNodeListResponse(mps_nodes=[], total_count=0, filters_applied={})

    if scenario_id:
        scenario_uuid = _resolve_scenario_uuid(db, scenario_id)
        conditions.append("scenario_id = %s")
        params.append(scenario_uuid)

    if date_from:
        conditions.append("time_bucket_end >= %s")
        params.append(date_from)

    if date_to:
        conditions.append("time_bucket_start <= %s")
        params.append(date_to)

    if status:
        conditions.append("status = %s")
        params.append(status)

    if conditions:
        base_query += " AND " + " AND ".join(conditions)

    base_query += " ORDER BY time_bucket_start ASC LIMIT %s"
    params.append(limit)

    rows = db.execute(base_query, params).fetchall()

    mps_nodes: List[MPSNodeSummaryOut] = []
    for row in rows:
        mps_nodes.append(
            MPSNodeSummaryOut(
                mps_id=row["mps_id"],
                time_bucket=row["time_bucket"],
                time_bucket_start=row["time_bucket_start"],
                time_bucket_end=row["time_bucket_end"],
                forecast_quantity=Decimal(str(row["forecast_quantity"])),
                sales_orders_quantity=Decimal(str(row["sales_orders_quantity"])),
                total_demand=Decimal(str(row["total_demand"])),
                planned_quantity=Decimal(str(row["planned_quantity"])),
                status=row["status"],
            )
        )

    filters_applied = {
        k: v for k, v in {
            "item_id": item_id,
            "location_id": location_id,
            "scenario_id": scenario_id,
            "date_from": date_from,
            "date_to": date_to,
            "status": status,
        }.items() if v is not None
    }

    return MPSNodeListResponse(
        mps_nodes=mps_nodes,
        total_count=len(mps_nodes),
        filters_applied=filters_applied,
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/mps/nodes/{mps_id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/nodes/{mps_id}",
    response_model=MPSNodeDetailOut,
    summary="Get MPS node",
    description="Retrieve a specific MPS node with full details.",
)
async def get_mps_node(
    mps_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> MPSNodeDetailOut:
    """
    Retrieve a specific MPS node by ID.
    """
    row = db.execute(
        """
        SELECT
            mps_id, item_id, location_id, scenario_id,
            time_bucket, time_bucket_start, time_bucket_end, time_grain,
            forecast_quantity, sales_orders_quantity, total_demand,
            planned_quantity, status,
            created_by, reviewed_by, approved_by, released_by,
            reviewed_at, approved_at, released_at,
            notes, active, created_at, updated_at
        FROM mps_nodes
        WHERE mps_id = %s
        """,
        (mps_id,),
    ).fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"MPS node '{mps_id}' not found",
        )

    # Convert datetime to date where needed
    def to_date(val):
        if val is None:
            return None
        if hasattr(val, "date"):
            return val.date()
        return val

    return MPSNodeDetailOut(
        mps_id=row["mps_id"],
        item_id=row["item_id"],
        location_id=row["location_id"],
        scenario_id=row["scenario_id"],
        time_bucket=row["time_bucket"],
        time_bucket_start=row["time_bucket_start"],
        time_bucket_end=row["time_bucket_end"],
        time_grain=row["time_grain"],
        forecast_quantity=Decimal(str(row["forecast_quantity"])),
        sales_orders_quantity=Decimal(str(row["sales_orders_quantity"])),
        total_demand=Decimal(str(row["total_demand"])),
        planned_quantity=Decimal(str(row["planned_quantity"])),
        status=row["status"],
        created_by=row["created_by"],
        reviewed_by=row["reviewed_by"],
        approved_by=row["approved_by"],
        released_by=row["released_by"],
        reviewed_at=to_date(row["reviewed_at"]),
        approved_at=to_date(row["approved_at"]),
        released_at=to_date(row["released_at"]),
        notes=row["notes"],
        active=row["active"],
        created_at=to_date(row["created_at"]),
        updated_at=to_date(row["updated_at"]),
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/mps/capacity-check
# ─────────────────────────────────────────────────────────────

@router.post(
    "/capacity-check",
    response_model=CapacityCheckResponse,
    summary="Check capacity feasibility",
    description=(
        "Check capacity feasibility for a list of MPS nodes before release.\n\n"
        "This endpoint verifies whether the planned production quantities in the "
        "MPS nodes can be executed given the available resource capacity.\n\n"
        "**Process:**\n"
        "1. Fetch MPS nodes by ID\n"
        "2. Identify critical resources via BOM/routing\n"
        "3. Calculate added load from MPS nodes\n"
        "4. Compare with available capacity (considering calendars, overrides)\n"
        "5. Detect violations (overload, calendar conflicts)\n"
        "6. Generate adjustment suggestions (delay, reduce, outsource)\n\n"
        "**Returns:**\n"
        "- feasible: Boolean indicating if all MPS nodes can be executed\n"
        "- violations: List of capacity violations detected\n"
        "- suggested_adjustments: List of actionable suggestions to resolve violations\n"
        "- summary: Overview statistics"
    ),
)
async def capacity_check(
    body: CapacityCheckRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> CapacityCheckResponse:
    """
    Check capacity feasibility for MPS nodes before release.
    
    Endpoint principal pour la vérification capacitaire avant release du MPS.
    """
    # Convert string IDs to UUIDs
    try:
        mps_uuids = [UUID(mps_id) for mps_id in body.mps_node_ids]
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid MPS node ID format: {e}",
        )
    
    # Run capacity check
    engine = CapacityCheckEngine()
    result = engine.check_capacity(
        db=db,
        mps_node_ids=mps_uuids,
        horizon_buffer_days=body.horizon_buffer_days,
    )
    
    # Convert violations to output format
    violations_out = [
        CapacityViolationOut(
            violation_type=v.violation_type,
            resource_id=v.resource_id,
            resource_external_id=v.resource_external_id,
            resource_name=v.resource_name,
            period_start=v.period_start,
            period_end=v.period_end,
            required_capacity=str(v.required_capacity),
            available_capacity=str(v.available_capacity),
            overload_pct=str(v.overload_pct),
            affected_mps_ids=v.affected_mps_ids,
            severity=v.severity,
        )
        for v in result.violations
    ]
    
    # Convert suggestions to output format
    suggestions_out = [
        AdjustmentSuggestionOut(
            suggestion_type=s.suggestion_type,
            mps_id=s.mps_id,
            description=s.description,
            original_quantity=str(s.original_quantity),
            suggested_quantity=str(s.suggested_quantity) if s.suggested_quantity else None,
            original_date=s.original_date,
            suggested_date=s.suggested_date,
            impact_description=s.impact_description,
            confidence=str(s.confidence),
        )
        for s in result.suggested_adjustments
    ]
    
    return CapacityCheckResponse(
        feasible=result.feasible,
        violations=violations_out,
        suggested_adjustments=suggestions_out,
        summary=result.summary,
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/mps/{mps_id}/suggest-adjustments
# ─────────────────────────────────────────────────────────────

@router.get(
    "/{mps_id}/suggest-adjustments",
    response_model=List[AdjustmentSuggestionOut],
    summary="Get adjustment suggestions for MPS node",
    description=(
        "Get adjustment suggestions for a specific MPS node to resolve capacity violations.\n\n"
        "This endpoint analyzes a single MPS node and returns suggestions for how to "
        "adjust it if it contributes to capacity violations.\n\n"
        "**Suggestion types:**\n"
        "- delay: Shift production to a later period with available capacity\n"
        "- reduce: Reduce quantity (with remainder outsourced)\n"
        "- outsource: Outsource part or all of the production\n\n"
        "**Returns:**\n"
        "- List of suggestions ranked by confidence"
    ),
)
async def suggest_adjustments(
    mps_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> List[AdjustmentSuggestionOut]:
    """
    Get adjustment suggestions for a specific MPS node.
    
    Endpoint pour obtenir des suggestions d'ajustement pour un MPS node donné.
    """
    # First verify the MPS node exists
    row = db.execute(
        "SELECT mps_id, planned_quantity, time_bucket_start, time_bucket_end FROM mps_nodes WHERE mps_id = %s AND active = TRUE",
        (mps_id,),
    ).fetchone()
    
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"MPS node '{mps_id}' not found or inactive",
        )
    
    # Run capacity check for this single node
    engine = CapacityCheckEngine()
    result = engine.check_capacity(
        db=db,
        mps_node_ids=[mps_id],
        horizon_buffer_days=7,
    )
    
    # Filter suggestions for this specific MPS node
    mps_suggestions = [s for s in result.suggested_adjustments if s.mps_id == mps_id]
    
    # Convert to output format
    suggestions_out = [
        AdjustmentSuggestionOut(
            suggestion_type=s.suggestion_type,
            mps_id=s.mps_id,
            description=s.description,
            original_quantity=str(s.original_quantity),
            suggested_quantity=str(s.suggested_quantity) if s.suggested_quantity else None,
            original_date=s.original_date,
            suggested_date=s.suggested_date,
            impact_description=s.impact_description,
            confidence=str(s.confidence),
        )
        for s in mps_suggestions
    ]
    
    return suggestions_out


# ─────────────────────────────────────────────────────────────
# POST /v1/mps/{mps_id}/promote-to-mrp
# ─────────────────────────────────────────────────────────────

class PromoteToMRPRequest(BaseModel):
    """Request to promote MPS to MRP."""
    explode_components: bool = Field(default=True, description="If true, trigger BOM explosion for components")
    dry_run: bool = Field(default=False, description="If true, validate without creating records")
    run_crp: bool = Field(default=False, description="If true, trigger CRP calculation after MRP promotion")
    crp_horizon_days: int = Field(default=90, ge=1, le=365, description="CRP planning horizon in days (used if run_crp=true)")


class PromoteToMRPResponse(BaseModel):
    """Response from promote-to-mrp endpoint."""
    mps_id: UUID
    status: str
    transaction_id: str
    planned_supplies_created: int
    mrp_job_id: Optional[str]
    components_exploded: int
    summary: Dict[str, Any]
    # CRP integration (optional)
    crp_triggered: bool = Field(default=False, description="Whether CRP calculation was triggered")
    crp_overload_count: Optional[int] = Field(None, description="Number of overloads detected (if CRP triggered)")
    crp_peak_load_date: Optional[date] = Field(None, description="Date of peak load (if CRP triggered)")


@router.post(
    "/{mps_id}/promote-to-mrp",
    response_model=PromoteToMRPResponse,
    summary="Promote MPS to MRP and trigger BOM explosion",
    description=(
        "Promote an approved MPS node to MRP for supply planning.\n\n"
        "This endpoint:\n"
        "1. Validates MPS status (must be APPROVED)\n"
        "2. Changes status to RELEASED\n"
        "3. Creates PlannedSupply nodes for the finished good\n"
        "4. Optionally triggers MRP BOM explosion for components\n"
        "5. Returns transaction ID and summary of created records\n\n"
        "**Requirements:**\n"
        "- MPS node must exist and be in APPROVED status\n"
        "- User must have appropriate permissions\n\n"
        "**Parameters:**\n"
        "- explode_components: If true, triggers BOM explosion (default: true)\n"
        "- dry_run: If true, validates without creating records (default: false)"
    ),
)
async def promote_to_mrp(
    mps_id: UUID,
    body: PromoteToMRPRequest,
    db: psycopg.Connection = Depends(get_db),
    token: str = Depends(require_auth),
) -> PromoteToMRPResponse:
    """
    Promote MPS to MRP and trigger BOM explosion.
    
    Endpoint pour promouvoir un MPS approuvé vers le MRP.
    """
    # Verify MPS node exists and is APPROVED
    row = db.execute(
        """
        SELECT mps_id, item_id, location_id, scenario_id, planned_quantity, 
               time_bucket_start, time_bucket_end, status
        FROM mps_nodes 
        WHERE mps_id = %s AND active = TRUE
        """,
        (mps_id,),
    ).fetchone()
    
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"MPS node '{mps_id}' not found or inactive",
        )
    
    if row["status"] != "APPROVED":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"MPS node must be in APPROVED status (current: {row['status']})",
        )
    
    # Run promote-to-mrp
    engine = AggregateDemandEngine()
    result = engine.promote_to_mrp(
        db=db,
        mps_id=mps_id,
        explode_components=body.explode_components,
        dry_run=body.dry_run,
        user_id=token,  # Use token as user identifier for audit
    )
    
    # CRP integration: trigger CRP calculation if requested
    crp_triggered = False
    crp_overload_count = None
    crp_peak_load_date = None
    
    if body.run_crp and not body.dry_run:
        try:
            from ootils_core.crp.engine import CRPEngine
            
            crp_engine = CRPEngine(db_conn=db)
            crp_result = crp_engine.calculate(horizon_days=body.crp_horizon_days)
            
            crp_triggered = True
            crp_overload_count = len(crp_result.overloads)
            
            # Find peak load date (date with highest total load across all work centers)
            if crp_result.load_profiles:
                peak_date = None
                peak_load = 0.0
                for profile in crp_result.load_profiles.values():
                    for bucket in profile.buckets:
                        if bucket.load_hours > peak_load:
                            peak_load = bucket.load_hours
                            peak_date = bucket.bucket_date
                crp_peak_load_date = peak_date
        except Exception as e:
            logger.warning("CRP calculation failed during MPS promotion: %s", e)
            # Don't fail the promotion if CRP fails - just log and continue
    
    return PromoteToMRPResponse(
        mps_id=mps_id,
        status=result.status,
        transaction_id=result.transaction_id,
        planned_supplies_created=result.planned_supplies_created,
        mrp_job_id=result.mrp_job_id,
        components_exploded=result.components_exploded,
        summary=result.summary,
        crp_triggered=crp_triggered,
        crp_overload_count=crp_overload_count,
        crp_peak_load_date=crp_peak_load_date,
    )
