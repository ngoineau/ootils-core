"""
POST /v1/atp/check — Check Available-to-Promise for a demand request.

Checks availability of inventory to promise for a given item, location, and date.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db
from ootils_core.models import ATPResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/atp", tags=["atp"])


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

class ATPCheckRequest(BaseModel):
    item_id: str
    location_id: str
    requested_date: date
    requested_quantity: Decimal
    scenario_id: Optional[str] = None  # defaults to baseline


class ATPCheckResponse(BaseModel):
    atp_check_id: UUID
    scenario_id: UUID
    item_id: UUID
    location_id: UUID
    requested_date: date
    requested_quantity: Decimal
    available_quantity: Decimal
    available_date: date
    shortage_quantity: Decimal
    status: str  # available | partial | unavailable
    allocation_details: list[UUID]  # IDs of allocated supply nodes
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


def _compute_atp(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
    requested_date: date,
    requested_qty: Decimal,
) -> tuple[Decimal, date, list[UUID]]:
    """
    Compute ATP by looking at projected inventory and planned supplies.
    Returns (available_qty, available_date, allocated_supply_ids).
    """
    # 1. Get projected inventory at requested date
    pi_row = db.execute(
        """
        SELECT closing_stock, time_span_start
        FROM nodes
        WHERE node_type = 'ProjectedInventory'
          AND item_id = %s
          AND location_id = %s
          AND scenario_id = %s
          AND active = TRUE
          AND time_span_start <= %s
          AND time_span_end >= %s
        ORDER BY time_span_start DESC
        LIMIT80808080 1
        """,
        (item_id, location_id, scenario_id, requested_date, requested_date),
    ).fetchone()

    available_qty = Decimal("0")
    available_date = requested_date
    allocated_ids = []

    if pi_row:
        closing_stock = Decimal(str(pi_row["closing_stock"])) if pi_row["closing_stock"] is not None else Decimal("0")
        if closing_stock > Decimal("0"):
            available_qty = min(closing_stock, requested_qty)
            # Allocate from this PI node (we'll just record its ID for simplicity)
            allocated_ids.append(pi_row["node_id"])

    # 2. If insufficient, look at planned supplies before requested date
    if available_qty < requested_qty:
        supply_rows = db.execute(
            """
            SELECT node_id, quantity, time_ref
            FROM nodes
            WHERE node_type IN ('PlannedSupply', 'PurchaseOrderSupply', 'OnHandSupply')
              AND item_id = %s
              AND location_id = %s
              AND scenario_id = %s
              AND active = TRUE
              AND time_ref <= %s
            ORDER BY time_ref ASC
            """,
            (item_id, location_id, scenario_id, requested_date),
        ).fetchall()

        remaining_needed = requested_qty - available_qty
        for row in supply_rows:
            supply_qty = Decimal(str(row["quantity"])) if row["quantity"] is not None else Decimal("0")
            if supply_qty <= Decimal("0"):
                continue
            alloc_qty = min(supply_qty, remaining_needed)
            available_qty += alloc_qty
            allocated_ids.append(row["node_id"])
            remaining_needed -= alloc_qty
            if remaining_needed <= Decimal("0"):
                break

    # 3. If still insufficient, find earliest date when full quantity available
    if available_qty < requested_qty:
        # Query future supplies after requested date
        future_row = db.execute(
            """
            SELECT node_id, quantity, time_ref
            FROM nodes
            WHERE node_type IN ('PlannedSupply', 'PurchaseOrderSupply', 'OnHandSupply')
              AND item_id = %s
              AND location_id = %s
              AND scenario_id = %s
              AND active = TRUE
              AND time_ref > %s
            ORDER BY time_ref ASC
            LIMIT 1
            """,
            (item_id, location_id, scenario_id, requested_date),
        ).fetchone()
        if future_row:
            available_date = future_row["time_ref"]
            # For simplicity, assume full quantity available at that date
            # In reality, you'd need to accumulate supplies over time.
        else:
            available_date = requested_date  # no future supply

    return available_qty, available_date, allocated_ids


# ─────────────────────────────────────────────────────────────
# POST /v1/atp/check
# ─────────────────────────────────────────────────────────────

@router.post(
    "/check",
    response_model=ATPCheckResponse,
    summary="Check ATP",
    description="Check Available-to-Promise for a requested demand.",
)
async def check_atp(
    body: ATPCheckRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> ATPCheckResponse:
    """Perform ATP check."""

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

    # "3. Compute ATP"
    available_qty, available_date, allocated_ids = _compute_atp(
        db, item_uuid, location_uuid, scenario_uuid,
        body.requested_date, body.requested_quantity,
    )

    # 4. Determine status
    shortage = body.requested_quantity - available_qty
    if shortage <= Decimal("0"):
        status = "available"
    elif available_qty > Decimal("0"):
        status = "partial"
    else:
        status = "unavailable"

    # 5. Create ATP check record
    atp_check_id = uuid4()
    db.execute(
        """
        INSERT INTO atp_checks (
            atp_check_id, scenario_id, item_id, location_id,
            requested_date, requested_quantity, available_quantity,
            available_date, shortage_quantity, status, allocation_details,
            created_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            now()
        )
        """,
        (atp_check_id, scenario_uuid, item_uuid, location_uuid,
         body.requested_date, body.requested_quantity, available_qty,
         available_date, shortage, status, allocated_ids),
    )

    logger.info(
        "atp.check item=%s location=%s scenario=%s — ATP check %s: status=%s",
        body.item_id, body.location_id, scenario_uuid, atp_check_id, status,
    )

    return ATPCheckResponse(
        atp_check_id=atp_check_id,
        scenario_id=scenario_uuid,
        item_id=item_uuid,
        location_id=location_uuid,
        requested_date=body.requested_date,
        requested_quantity=body.requested_quantity,
        available_quantity=available_qty,
        available_date=available_date,
        shortage_quantity=shortage,
        status=status,
        allocation_details=allocated_ids,
        created_at=str(date.today()),
    )