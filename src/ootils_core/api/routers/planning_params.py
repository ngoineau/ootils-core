"""
GET /v1/items/planning-params — Return planning parameters for item/location pairs.
"""
from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID
from decimal import Decimal

from fastapi import APIRouter, Depends, Query
from psycopg import sql
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db
from ootils_core.db.types import DictRowConnection

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
def get_planning_params(
    item_id: Optional[str] = Query(default=None, description="Filter by item UUID"),
    location_id: Optional[str] = Query(default=None, description="Filter by location UUID"),
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> PlanningParamsResponse:
    """Return the latest active planning parameters per (item, location)."""
    # Build query — get most recent params per (item_id, location_id)
    # Uses effective_to IS NULL to get active/current records (SCD2 pattern)
    conditions: list[sql.Composable] = [
        sql.SQL("(ipp.effective_to IS NULL OR ipp.effective_to = '9999-12-31'::DATE)")
    ]
    params: list = []

    if item_id:
        try:
            parsed_item_id = UUID(item_id)
        except ValueError:
            pass
        else:
            conditions.append(sql.SQL("ipp.item_id = %s"))
            params.append(parsed_item_id)

    if location_id:
        try:
            parsed_location_id = UUID(location_id)
        except ValueError:
            pass
        else:
            conditions.append(sql.SQL("ipp.location_id = %s"))
            params.append(parsed_location_id)

    where = sql.SQL(" AND ").join(conditions)
    query = sql.SQL(
        """
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
        """
    ).format(where=where)
    rows = db.execute(query, params or None).fetchall()

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
