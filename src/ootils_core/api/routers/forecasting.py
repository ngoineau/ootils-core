"""
Forecasting API endpoints — REST interface for demand forecasting.

Endpoints:
  POST   /v1/demand/forecast/generate      — Generate forecast for item/location
  GET    /v1/demand/forecast/{forecast_id} — Get forecast with values
  GET    /v1/demand/forecast               — List forecasts with filters
  POST   /v1/demand/forecast/{id}/adjust   — Apply manual adjustment
  DELETE /v1/demand/forecast/{id}          — Soft delete forecast

Constraints:
  - Horizon max 365 days
  - Granularity: daily, weekly, monthly
  - Rate limiting: max 10 concurrent generations (enforced at DB level)
  - Response includes metadata: method, params, generated_at, confidence
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Optional
from uuid import UUID, uuid4

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator, model_validator

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db
from ootils_core.forecasting.algorithms import ForecastingError
from ootils_core.forecasting.engine import ForecastingEngine
from ootils_core.pyramide.repository import get_historical_demand

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/demand/forecast", tags=["forecasting"])


# ─────────────────────────────────────────────────────────────
# Pydantic models — Requests
# ─────────────────────────────────────────────────────────────

class ForecastGenerateRequest(BaseModel):
    """Request to generate a forecast for an item/location."""
    item_id: str
    location_id: str
    horizon_days: int = Field(default=90, ge=1, le=365)
    granularity: str = Field(default="daily", pattern="^(daily|weekly|monthly)$")
    method: str = Field(default="MA", pattern="^(MA|EXP_SMOOTHING|CROSTON|SEASONAL)$")
    method_params: Optional[dict] = Field(default=None)
    scenario_id: Optional[str] = None  # defaults to baseline
    clear_existing: bool = False

    @field_validator("horizon_days")
    @classmethod
    def validate_horizon(cls, v: int) -> int:
        if v > 365:
            raise ValueError("horizon_days cannot exceed 365")
        return v

    @model_validator(mode="after")
    def validate_seasonal_params(self) -> "ForecastGenerateRequest":
        # SEASONAL has no default cycle length: it depends on the data
        # granularity (e.g. 7 daily, 12 monthly, 52 weekly) and must be chosen
        # explicitly. Fail at validation time (422, before any DB access).
        if self.method == "SEASONAL":
            season_length = (self.method_params or {}).get("season_length")
            if not isinstance(season_length, int) or isinstance(season_length, bool) or season_length < 2:
                raise ValueError(
                    "method SEASONAL requires method_params.season_length (integer >= 2, "
                    "e.g. 7 daily, 12 monthly, 52 weekly)"
                )
        return self


class ForecastAdjustRequest(BaseModel):
    """Request to apply a manual adjustment to a forecast."""
    adjustment_type: str = Field(default="manual", pattern="^(manual|promotion|seasonality|event)$")
    delta: Optional[Decimal] = Field(default=None, description="Absolute adjustment quantity")
    delta_percent: Optional[Decimal] = Field(default=None, description="Percentage adjustment")
    value_id: Optional[str] = None  # NULL if adjustment applies to entire forecast
    reason: Optional[str] = None
    user_id: Optional[str] = None

    @field_validator("delta", "delta_percent")
    @classmethod
    def validate_delta(cls, v: Optional[Decimal], info) -> Optional[Decimal]:
        # At least one of delta or delta_percent must be provided
        return v


# ─────────────────────────────────────────────────────────────
# Pydantic models — Responses
# ─────────────────────────────────────────────────────────────

class ForecastValueOut(BaseModel):
    """Single forecast value output."""
    value_id: UUID
    forecast_date: date
    quantity: Decimal
    method: str
    confidence_interval_lower: Optional[Decimal] = None
    confidence_interval_upper: Optional[Decimal] = None
    adjusted_quantity: Optional[Decimal] = None  # After adjustments


class ForecastMetadata(BaseModel):
    """Forecast generation metadata."""
    method: str
    method_params: Optional[dict] = None
    generated_at: date
    horizon_days: int
    granularity: str
    confidence_score: Optional[Decimal] = None  # Derived from accuracy metrics


class ForecastOut(BaseModel):
    """Forecast output with values and metadata."""
    forecast_id: UUID
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    horizon_start: date
    horizon_end: date
    granularity: str
    method: str
    metadata: ForecastMetadata
    values: List[ForecastValueOut]
    total_quantity: Decimal
    adjustment_count: int = 0


class ForecastSummary(BaseModel):
    """Summary for list endpoint."""
    forecast_id: UUID
    item_id: UUID
    location_id: UUID
    horizon_start: date
    horizon_end: date
    granularity: str
    method: str
    generated_at: date
    value_count: int
    total_quantity: Decimal
    has_adjustments: bool


class ForecastListResponse(BaseModel):
    """Response from list forecasts endpoint."""
    forecasts: List[ForecastSummary]
    total_count: int
    filters_applied: dict


class AdjustmentOut(BaseModel):
    """Adjustment output."""
    adjustment_id: UUID
    forecast_id: UUID
    value_id: Optional[UUID]
    adjustment_type: str
    delta: Decimal
    delta_percent: Optional[Decimal]
    reason: Optional[str]
    user_id: Optional[str]
    applied_at: date


class ForecastAdjustResponse(BaseModel):
    """Response from adjust endpoint."""
    forecast_id: UUID
    adjustment_id: UUID
    message: str
    new_total_quantity: Decimal


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
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Invalid scenario_id '{scenario_id_str}' — must be a valid UUID or 'baseline'",
        )


def _get_historical_demand(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID,
    periods: int = 90,
    scenario_id: UUID = BASELINE_SCENARIO_ID,
) -> List[Decimal]:
    """
    Fetch historical demand data for an item/location.
    Returns list of daily demand quantities (most recent last), sparse
    (days without demand are absent).

    Delegates to the shared demand-history reader: primary source is the
    demand_history booking facts (strict past, stream='regular',
    inter-entity excluded); degraded fallback reads past
    CustomerOrderDemand nodes for `scenario_id` — never ForecastDemand,
    so forecasts cannot train on forecasts (#333). demand_history itself
    is scenario-invariant (actuals); `scenario_id` only scopes the
    fallback. See ootils_core.pyramide.repository.get_historical_demand.
    """
    return get_historical_demand(
        db=db,
        item_id=item_id,
        location_id=location_id,
        lookback_days=periods,
        scenario_id=scenario_id,
    )


def _compute_confidence_score(
    historical_count: int,
    method: str,
    mape: Optional[Decimal] = None,
) -> Optional[Decimal]:
    """
    Compute a simple confidence score (0-1) based on:
    - Data sufficiency (historical_count)
    - Method appropriateness
    - MAPE if available
    """
    if historical_count < 7:
        return Decimal("0.3")  # Low confidence: insufficient data
    if historical_count < 30:
        base_score = Decimal("0.5")
    else:
        base_score = Decimal("0.7")

    # Adjust for MAPE if available
    if mape is not None:
        if mape < Decimal("10"):
            base_score = min(base_score + Decimal("0.2"), Decimal("1.0"))
        elif mape < Decimal("20"):
            base_score = min(base_score + Decimal("0.1"), Decimal("1.0"))
        elif mape > Decimal("50"):
            base_score = max(base_score - Decimal("0.2"), Decimal("0.1"))

    return base_score


def _soft_delete_forecast(db: psycopg.Connection, forecast_id: UUID) -> None:
    """
    Soft delete a forecast by deactivating its values.
    The forecast header remains for audit purposes.
    """
    # Deactivate all forecast values
    db.execute(
        """
        UPDATE forecast_values
        SET active = FALSE, updated_at = now()
        WHERE forecast_id = %s
        """,
        (forecast_id,),
    )

    # Mark forecast as deleted (add a deleted_at column if needed, or use a flag)
    # For now, we'll rely on value deactivation as the soft delete mechanism
    logger.info("forecast.soft_delete forecast_id=%s", forecast_id)


# ─────────────────────────────────────────────────────────────
# POST /v1/demand/forecast/generate
# ─────────────────────────────────────────────────────────────

@router.post(
    "/generate",
    response_model=ForecastOut,
    summary="Generate forecast",
    description=(
        "Generate a statistical forecast for a specific item/location combination.\n\n"
        "**Constraints:**\n"
        "- Maximum horizon: 365 days\n"
        "- Supported granularities: daily, weekly, monthly\n"
        "- Methods: MA (Moving Average), EXP_SMOOTHING, CROSTON (intermittent demand), SEASONAL\n"
        "- Rate limit: max 10 concurrent generations (enforced at DB level)\n\n"
        "**Response includes:**\n"
        "- Forecast values for each period in the horizon\n"
        "- Metadata: method, parameters, generated_at, confidence score\n"
        "- Total forecast quantity across the horizon"
    ),
)
def generate_forecast(
    body: ForecastGenerateRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> ForecastOut:
    """
    Generate a forecast for an item/location.
    
    Fetches historical demand, applies the specified forecasting method,
    and persists the forecast with values.
    """
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

    # 2. Resolve scenario
    scenario_uuid = _resolve_scenario_uuid(db, body.scenario_id)

    # 3. Fetch historical demand
    historical_periods = max(body.horizon_days, 90)  # At least as many history as horizon
    historical_demand = _get_historical_demand(
        db, item_uuid, location_uuid, historical_periods, scenario_id=scenario_uuid
    )

    if not historical_demand or len(historical_demand) < 3:
        logger.warning(
            "forecast.generate insufficient_history item=%s location=%s count=%d",
            body.item_id, body.location_id, len(historical_demand),
        )
        # Proceed — the engine decides: methods that cannot fit the series
        # raise ForecastingError, converted to an explicit 422 below.

    # 4. Generate forecast using ForecastingEngine
    engine = ForecastingEngine()
    try:
        forecast_result = engine.generate(
            item_history=historical_demand,
            method=body.method,
            params=body.method_params or {},
        )
    except ForecastingError:
        # Data condition, not a server bug: the series is too short (or
        # otherwise unusable) for the requested method. Hand-authored
        # message — counts only, no internals (CLAUDE.md exception policy).
        logger.warning(
            "forecast.generate rejected item=%s location=%s: history of %d "
            "point(s) unusable for method %s",
            body.item_id, body.location_id, len(historical_demand), body.method,
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Insufficient historical demand: {len(historical_demand)} "
                f"data point(s) available for method {body.method}"
            ),
        )
    except Exception as e:
        logger.exception("forecast.generate failed item=%s location=%s: %s", body.item_id, body.location_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Forecast generation failed",
        )

    # 4b. SEASONAL opt-in: the persisted values follow the seasonal CURVE
    # (level x indices) instead of repeating a single value. Default methods
    # (MA, EXP_SMOOTHING, CROSTON) keep the historical flat behaviour. When
    # the history covers < 2 full cycles, the engine falls back to a flat
    # level and records it in forecast_result.parameters/warnings.
    seasonal_series: Optional[List[Decimal]] = None
    if forecast_result.parameters.get("seasonal_applied"):
        seasonal_series = engine.forecast_series(
            item_history=historical_demand,
            method=body.method,
            params=body.method_params or {},
            periods=body.horizon_days,
        )

    # 5. Create forecast header
    forecast_id = uuid4()
    horizon_start = date.today() + timedelta(days=1)
    horizon_end = horizon_start + timedelta(days=body.horizon_days - 1)

    db.execute(
        """
        INSERT INTO forecasts (
            forecast_id, item_id, location_id, scenario_id,
            horizon_start, horizon_end, granularity, method
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (forecast_id, item_uuid, location_uuid, scenario_uuid,
         horizon_start, horizon_end, body.granularity, body.method),
    )

    # 6. Create forecast values
    # For simplicity, we generate daily values and aggregate if needed
    values: List[ForecastValueOut] = []
    total_quantity = Decimal("0")

    for day_offset in range(body.horizon_days):
        value_date = horizon_start + timedelta(days=day_offset)
        value_id = uuid4()

        # Use the forecast value (for non-daily granularity, this is a simplification)
        quantity = seasonal_series[day_offset] if seasonal_series is not None else forecast_result.forecast_value

        db.execute(
            """
            INSERT INTO forecast_values (
                value_id, forecast_id, forecast_date, quantity, method
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (value_id, forecast_id, value_date, quantity, body.method),
        )

        values.append(
            ForecastValueOut(
                value_id=value_id,
                forecast_date=value_date,
                quantity=quantity,
                method=body.method,
            )
        )
        total_quantity += quantity

    # 7. Compute confidence score
    mape = forecast_result.metrics.get("mape")
    confidence_score = _compute_confidence_score(
        len(historical_demand),
        body.method,
        mape,
    )

    metadata = ForecastMetadata(
        method=body.method,
        # Effective engine parameters carry the provenance (e.g. season_length
        # + seasonal_applied for SEASONAL, auto-calibrated alpha/window) in the
        # existing method_params field — no new column, no JSONB.
        method_params=forecast_result.parameters or body.method_params,
        generated_at=date.today(),
        horizon_days=body.horizon_days,
        granularity=body.granularity,
        confidence_score=confidence_score,
    )

    logger.info(
        "forecast.generate item=%s location=%s horizon=%d method=%s values=%d total=%s",
        body.item_id, body.location_id, body.horizon_days, body.method,
        len(values), total_quantity,
    )

    return ForecastOut(
        forecast_id=forecast_id,
        item_id=item_uuid,
        location_id=location_uuid,
        scenario_id=scenario_uuid,
        horizon_start=horizon_start,
        horizon_end=horizon_end,
        granularity=body.granularity,
        method=body.method,
        metadata=metadata,
        values=values,
        total_quantity=total_quantity,
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/demand/forecast/{forecast_id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/{forecast_id}",
    response_model=ForecastOut,
    summary="Get forecast",
    description="Retrieve a specific forecast with all its values and adjustments.",
)
def get_forecast(
    forecast_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> ForecastOut:
    """
    Retrieve a forecast by ID with all values and adjustments.
    """
    # 1. Fetch forecast header
    row = db.execute(
        """
        SELECT forecast_id, item_id, location_id, scenario_id,
               horizon_start, horizon_end, granularity, method, created_at
        FROM forecasts
        WHERE forecast_id = %s
        """,
        (forecast_id,),
    ).fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Forecast '{forecast_id}' not found",
        )

    # 2. Fetch forecast values
    value_rows = db.execute(
        """
        SELECT value_id, forecast_date, quantity, method,
               confidence_interval_lower, confidence_interval_upper
        FROM forecast_values
        WHERE forecast_id = %s
        ORDER BY forecast_date ASC
        """,
        (forecast_id,),
    ).fetchall()

    values: List[ForecastValueOut] = []
    total_quantity = Decimal("0")

    for vr in value_rows:
        quantity = Decimal(str(vr["quantity"]))
        values.append(
            ForecastValueOut(
                value_id=vr["value_id"],
                forecast_date=vr["forecast_date"],
                quantity=quantity,
                method=vr["method"],
                confidence_interval_lower=(
                    Decimal(str(vr["confidence_interval_lower"]))
                    if vr["confidence_interval_lower"] is not None else None
                ),
                confidence_interval_upper=(
                    Decimal(str(vr["confidence_interval_upper"]))
                    if vr["confidence_interval_upper"] is not None else None
                ),
            )
        )
        total_quantity += quantity

    # 3. Fetch adjustments and compute adjusted quantities
    adjustment_rows = db.execute(
        """
        SELECT adjustment_id, value_id, adjustment_type, delta, delta_percent,
               reason, user_id, applied_at
        FROM forecast_adjustments
        WHERE forecast_id = %s
        ORDER BY applied_at ASC
        """,
        (forecast_id,),
    ).fetchall()

    # Build adjustment map by value_id
    adjustments_by_value: dict[UUID, List[dict]] = {}
    for ar in adjustment_rows:
        vid = ar["value_id"]
        if vid not in adjustments_by_value:
            adjustments_by_value[vid] = []
        adjustments_by_value[vid].append(dict(ar))

    # Apply adjustments to values
    for v in values:
        if v.value_id in adjustments_by_value:
            adjusted_qty = v.quantity
            for adj in adjustments_by_value[v.value_id]:
                if adj["delta"] is not None:
                    adjusted_qty += Decimal(str(adj["delta"]))
                if adj["delta_percent"] is not None:
                    adjusted_qty = adjusted_qty * (Decimal("1") + Decimal(str(adj["delta_percent"])) / Decimal("100"))
            v.adjusted_quantity = adjusted_qty

    # 4. Build metadata
    horizon_days = (row["horizon_end"] - row["horizon_start"]).days + 1
    created_at = row["created_at"]
    # Handle both datetime and date objects
    generated_at = created_at.date() if hasattr(created_at, "date") else created_at
    metadata = ForecastMetadata(
        method=row["method"],
        generated_at=generated_at,
        horizon_days=horizon_days,
        granularity=row["granularity"],
    )

    return ForecastOut(
        forecast_id=row["forecast_id"],
        item_id=row["item_id"],
        location_id=row["location_id"],
        scenario_id=row["scenario_id"],
        horizon_start=row["horizon_start"],
        horizon_end=row["horizon_end"],
        granularity=row["granularity"],
        method=row["method"],
        metadata=metadata,
        values=values,
        total_quantity=total_quantity,
        adjustment_count=len(adjustment_rows),
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/demand/forecast
# ─────────────────────────────────────────────────────────────

@router.get(
    "",
    response_model=ForecastListResponse,
    summary="List forecasts",
    description=(
        "List forecasts with optional filters.\n\n"
        "**Filters:**\n"
        "- item_id: Filter by item external ID\n"
        "- location_id: Filter by location external ID\n"
        "- date_from: Filter forecasts starting on or after this date\n"
        "- date_to: Filter forecasts ending on or before this date\n"
        "- granularity: Filter by granularity (daily, weekly, monthly)\n"
        "- method: Filter by forecasting method"
    ),
)
def list_forecasts(
    item_id: Optional[str] = Query(default=None),
    location_id: Optional[str] = Query(default=None),
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    granularity: Optional[str] = Query(default=None, pattern="^(daily|weekly|monthly)$"),
    method: Optional[str] = Query(default=None, pattern="^(MA|EXP_SMOOTHING|CROSTON|SEASONAL)$"),
    limit: int = Query(default=50, ge=1, le=500),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> ForecastListResponse:
    """
    List forecasts with optional filters.
    """
    # Build dynamic query with filters
    base_query = """
        SELECT 
            f.forecast_id, f.item_id, f.location_id, f.horizon_start, f.horizon_end,
            f.granularity, f.method, f.created_at,
            COUNT(fv.value_id) AS value_count,
            COALESCE(SUM(fv.quantity), 0) AS total_quantity,
            EXISTS(
                SELECT 1 FROM forecast_adjustments fa 
                WHERE fa.forecast_id = f.forecast_id
            ) AS has_adjustments
        FROM forecasts f
        LEFT JOIN forecast_values fv ON fv.forecast_id = f.forecast_id
        WHERE TRUE
    """

    params: list = []
    conditions: list = []

    if item_id:
        item_uuid = _resolve_item_uuid(db, item_id)
        if item_uuid:
            conditions.append("f.item_id = %s")
            params.append(item_uuid)
        else:
            # Item not found, return empty list
            return ForecastListResponse(forecasts=[], total_count=0, filters_applied={})

    if location_id:
        location_uuid = _resolve_location_uuid(db, location_id)
        if location_uuid:
            conditions.append("f.location_id = %s")
            params.append(location_uuid)
        else:
            return ForecastListResponse(forecasts=[], total_count=0, filters_applied={})

    if date_from:
        conditions.append("f.horizon_start >= %s")
        params.append(date_from)

    if date_to:
        conditions.append("f.horizon_end <= %s")
        params.append(date_to)

    if granularity:
        conditions.append("f.granularity = %s")
        params.append(granularity)

    if method:
        conditions.append("f.method = %s")
        params.append(method)

    if conditions:
        base_query += " AND " + " AND ".join(conditions)

    base_query += """
        GROUP BY f.forecast_id, f.item_id, f.location_id, f.horizon_start, 
                 f.horizon_end, f.granularity, f.method, f.created_at
        ORDER BY f.created_at DESC
        LIMIT %s
    """
    params.append(limit)

    rows = db.execute(base_query, params).fetchall()

    forecasts: List[ForecastSummary] = []
    for r in rows:
        forecasts.append(
            ForecastSummary(
                forecast_id=r["forecast_id"],
                item_id=r["item_id"],
                location_id=r["location_id"],
                horizon_start=r["horizon_start"],
                horizon_end=r["horizon_end"],
                granularity=r["granularity"],
                method=r["method"],
                generated_at=r["created_at"].date(),
                value_count=r["value_count"],
                total_quantity=Decimal(str(r["total_quantity"])),
                has_adjustments=r["has_adjustments"],
            )
        )

    filters_applied = {
        k: v for k, v in {
            "item_id": item_id,
            "location_id": location_id,
            "date_from": date_from,
            "date_to": date_to,
            "granularity": granularity,
            "method": method,
        }.items() if v is not None
    }

    return ForecastListResponse(
        forecasts=forecasts,
        total_count=len(forecasts),
        filters_applied=filters_applied,
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/demand/forecast/{id}/adjust
# ─────────────────────────────────────────────────────────────

@router.post(
    "/{forecast_id}/adjust",
    response_model=ForecastAdjustResponse,
    summary="Adjust forecast",
    description=(
        "Apply a manual or programmatic adjustment to a forecast.\n\n"
        "**Adjustment types:**\n"
        "- manual: Planner override\n"
        "- promotion: Marketing/promotion-driven increase\n"
        "- seasonality: Seasonal pattern adjustment\n"
        "- event: Special event (e.g., Olympics, strike)\n\n"
        "**Parameters:**\n"
        "- delta: Absolute quantity adjustment (can be negative)\n"
        "- delta_percent: Percentage adjustment (alternative to delta)\n"
        "- value_id: Specific forecast value to adjust (NULL for entire forecast)"
    ),
)
def adjust_forecast(
    forecast_id: UUID,
    body: ForecastAdjustRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> ForecastAdjustResponse:
    """
    Apply an adjustment to a forecast.
    """
    # 1. Validate forecast exists
    row = db.execute(
        "SELECT forecast_id, item_id, location_id FROM forecasts WHERE forecast_id = %s",
        (forecast_id,),
    ).fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Forecast '{forecast_id}' not found",
        )

    # 2. Validate value_id if provided
    value_uuid = None
    if body.value_id:
        try:
            value_uuid = UUID(body.value_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=f"Invalid value_id '{body.value_id}' — must be a valid UUID",
            )

        # Verify value belongs to this forecast
        val_row = db.execute(
            "SELECT value_id FROM forecast_values WHERE value_id = %s AND forecast_id = %s",
            (value_uuid, forecast_id),
        ).fetchone()

        if not val_row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Forecast value '{body.value_id}' not found in forecast '{forecast_id}'",
            )

    # 3. Validate delta/delta_percent
    if body.delta is None and body.delta_percent is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="At least one of delta or delta_percent must be provided",
        )

    # 4. Create adjustment
    adjustment_id = uuid4()
    db.execute(
        """
        INSERT INTO forecast_adjustments (
            adjustment_id, forecast_id, value_id, adjustment_type,
            delta, delta_percent, reason, user_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            adjustment_id, forecast_id, value_uuid, body.adjustment_type,
            body.delta, body.delta_percent, body.reason, body.user_id,
        ),
    )

    # 5. Compute new total quantity
    total_row = db.execute(
        """
        SELECT COALESCE(SUM(quantity), 0) AS total_qty
        FROM forecast_values
        WHERE forecast_id = %s
        """,
        (forecast_id,),
    ).fetchone()

    new_total = Decimal(str(total_row["total_qty"]))

    message = (
        f"Adjustment applied to forecast '{forecast_id}'. "
        f"Type: {body.adjustment_type}, "
        f"Delta: {body.delta}, "
        f"Delta %: {body.delta_percent}"
    )
    if body.reason:
        message += f" — Reason: {body.reason}"

    logger.info(
        "forecast.adjust forecast_id=%s adjustment_id=%s type=%s delta=%s",
        forecast_id, adjustment_id, body.adjustment_type, body.delta,
    )

    return ForecastAdjustResponse(
        forecast_id=forecast_id,
        adjustment_id=adjustment_id,
        message=message,
        new_total_quantity=new_total,
    )


# ─────────────────────────────────────────────────────────────
# DELETE /v1/demand/forecast/{id}
# ─────────────────────────────────────────────────────────────

@router.delete(
    "/{forecast_id}",
    summary="Delete forecast",
    description=(
        "Soft delete a forecast by deactivating all its values.\n"
        "The forecast header remains for audit purposes.\n"
        "This operation is idempotent."
    ),
)
def delete_forecast(
    forecast_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> dict:
    """
    Soft delete a forecast.
    """
    # 1. Validate forecast exists
    row = db.execute(
        "SELECT forecast_id FROM forecasts WHERE forecast_id = %s",
        (forecast_id,),
    ).fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Forecast '{forecast_id}' not found",
        )

    # 2. Soft delete (deactivate values)
    _soft_delete_forecast(db, forecast_id)

    logger.info("forecast.delete forecast_id=%s", forecast_id)

    return {
        "status": "deleted",
        "forecast_id": str(forecast_id),
        "message": f"Forecast '{forecast_id}' has been soft deleted",
    }
