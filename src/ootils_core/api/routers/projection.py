"""
GET /v1/projection — Get projected inventory for an item/location.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.engine.kernel.graph.store import GraphStore

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/projection", tags=["projection"])


class ProjectionBucket(BaseModel):
    bucket_sequence: Optional[int]
    time_span_start: Optional[date]
    time_span_end: Optional[date]
    time_grain: Optional[str]
    opening_stock: Optional[Decimal]
    inflows: Optional[Decimal]
    outflows: Optional[Decimal]
    closing_stock: Optional[Decimal]
    has_shortage: bool
    shortage_qty: Decimal
    safety_stock_qty: Optional[Decimal] = None
    severity_class: Optional[str] = None  # 'stockout' | 'below_safety_stock' | None


class ProjectionResponse(BaseModel):
    series_id: Optional[UUID]
    item_id: str
    location_id: str
    scenario_id: UUID
    grain: Optional[str]
    safety_stock_qty: Optional[Decimal] = None
    buckets: list[ProjectionBucket]


@router.get("", response_model=ProjectionResponse)
async def get_projection(
    item_id: str = Query(..., description="Item identifier (UUID or string key)"),
    location_id: str = Query(..., description="Location identifier (UUID or string key)"),
    grain: Optional[str] = Query(default=None, description="day / week / month"),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> ProjectionResponse:
    """Return projected inventory buckets for an item/location pair."""
    # Resolve item_id and location_id to UUIDs
    try:
        item_uuid = UUID(item_id)
    except ValueError:
        # Lookup by name
        row = db.execute(
            "SELECT item_id FROM items WHERE name = %s LIMIT 1", (item_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Item '{item_id}' not found",
            )
        item_uuid = UUID(str(row["item_id"]))

    try:
        location_uuid = UUID(location_id)
    except ValueError:
        row = db.execute(
            "SELECT location_id FROM locations WHERE name = %s LIMIT 1", (location_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Location '{location_id}' not found",
            )
        location_uuid = UUID(str(row["location_id"]))

    store = GraphStore(db)
    series = store.get_projection_series(item_uuid, location_uuid, scenario_id)

    if series is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No projection series found for item={item_id}, location={location_id}, scenario={scenario_id}",
        )

    nodes = store.get_nodes_by_series(series.series_id)

    # Fetch safety stock for this item/location
    safety_stock_qty: Optional[Decimal] = None
    try:
        ss_row = db.execute(
            """
            SELECT safety_stock_qty FROM item_planning_params
            WHERE item_id = %s
              AND (location_id = %s OR location_id IS NULL)
              AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
            ORDER BY location_id NULLS LAST
            LIMIT 1
            """,
            (item_uuid, location_uuid),
        ).fetchone()
        if ss_row is not None:
            raw_ss = ss_row["safety_stock_qty"]
            if raw_ss is not None:
                safety_stock_qty = Decimal(str(raw_ss))
    except Exception:  # noqa: BLE001
        # Safety stock fetch failure must not break projection
        logger.warning(
            "projection: failed to fetch safety_stock_qty for item=%s location=%s",
            item_uuid,
            location_uuid,
        )

    def _compute_severity_class(closing: Optional[Decimal]) -> Optional[str]:
        if closing is None:
            return None
        if closing < Decimal("0"):
            return "stockout"
        if safety_stock_qty is not None and closing < safety_stock_qty:
            return "below_safety_stock"
        return None

    buckets = [
        ProjectionBucket(
            bucket_sequence=n.bucket_sequence,
            time_span_start=n.time_span_start,
            time_span_end=n.time_span_end,
            time_grain=n.time_grain,
            opening_stock=n.opening_stock,
            inflows=n.inflows,
            outflows=n.outflows,
            closing_stock=n.closing_stock,
            has_shortage=n.has_shortage,
            shortage_qty=n.shortage_qty,
            safety_stock_qty=safety_stock_qty,
            severity_class=_compute_severity_class(n.closing_stock),
        )
        for n in nodes
    ]

    logger.info(
        "projection.fetched series=%s item=%s location=%s scenario=%s buckets=%d safety_stock=%s",
        series.series_id,
        item_id,
        location_id,
        scenario_id,
        len(buckets),
        safety_stock_qty,
    )

    return ProjectionResponse(
        series_id=series.series_id,
        item_id=item_id,
        location_id=location_id,
        scenario_id=scenario_id,
        grain=grain,
        safety_stock_qty=safety_stock_qty,
        buckets=buckets,
    )
