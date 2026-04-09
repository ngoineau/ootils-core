"""
GET /v1/items/planning-params — Return planning parameters for item/location pairs.
"""
from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID
from decimal import Decimal

import psycopg
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/items", tags=["planning-params"])


class PlanningParamsOut(BaseModel):
    item_id: UUID
    location_id: Optional[UUID]
    safety_stock_qty: Optional[Decimal]
    safety_stock_days: Optional[Decimal]
    reorder_point: Optional[Decimal]
    lot_size: Optional[Decimal]
    lead_time_days: Optional[Decimal]
    effective_from: Optional[str]


class PlanningParamsResponse(BaseModel):
    params: list[PlanningParamsOut]
    total: int


@router.get("/planning-params", response_model=PlanningParamsResponse)
async def get_planning_params(
    item_id: Optional[str] = Query(default=None, description="Filter by item UUID"),
    location_id: Optional[str] = Query(default=None, description="Filter by location UUID"),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> PlanningParamsResponse:
    """Return the latest active planning parameters per (item, location)."""
    # Build query — get most recent params per (item_id, location_id)
    # Uses effective_to IS NULL to get active/current records (SCD2 pattern)
    conditions = [
        "(ipp.effective_to IS NULL OR ipp.effective_to = '9999-12-31'::DATE)"
    ]
    params: list = []

    if item_id:
        try:
            conditions.append("ipp.item_id = %s")
            params.append(UUID(item_id))
        except ValueError:
            pass

    if location_id:
        try:
            conditions.append("ipp.location_id = %s")
            params.append(UUID(location_id))
        except ValueError:
            pass

    where = " AND ".join(conditions)
    rows = db.execute(
        f"""
        SELECT ipp.item_id,
               ipp.location_id,
               ipp.safety_stock_qty,
               ipp.safety_stock_days,
               ipp.reorder_point_qty        AS reorder_point,
               ipp.min_order_qty            AS lot_size,
               ipp.lead_time_total_days     AS lead_time_days,
               ipp.effective_from::text     AS effective_from
        FROM item_planning_params ipp
        WHERE {where}
        ORDER BY ipp.item_id, ipp.location_id
        """,
        params or None,
    ).fetchall()

    result = [
        PlanningParamsOut(
            item_id=UUID(str(r["item_id"])),
            location_id=UUID(str(r["location_id"])) if r["location_id"] else None,
            safety_stock_qty=Decimal(str(r["safety_stock_qty"])) if r["safety_stock_qty"] is not None else None,
            safety_stock_days=Decimal(str(r["safety_stock_days"])) if r["safety_stock_days"] is not None else None,
            reorder_point=Decimal(str(r["reorder_point"])) if r["reorder_point"] is not None else None,
            lot_size=Decimal(str(r["lot_size"])) if r["lot_size"] is not None else None,
            lead_time_days=Decimal(str(r["lead_time_days"])) if r["lead_time_days"] is not None else None,
            effective_from=r["effective_from"],
        )
        for r in rows
    ]

    logger.info("planning_params.fetched count=%d", len(result))
    return PlanningParamsResponse(params=result, total=len(result))
