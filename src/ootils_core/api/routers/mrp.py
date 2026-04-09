"""
POST /v1/mrp/run — Time-phased MRP explosion with graph integration.

Runs MRP for a given item/location, creates PlannedSupply nodes,
wires them into the graph, and triggers propagation.
"""
from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/mrp", tags=["mrp"])


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

class MrpRunRequest(BaseModel):
    item_id: str
    location_id: str
    horizon_days: int = 90
    scenario_id: Optional[str] = None  # defaults to baseline
    clear_existing: bool = False  # if True, delete existing PlannedSupply nodes first


class PlannedOrderOut(BaseModel):
    node_id: UUID
    item_id: UUID
    location_id: UUID
    order_date: date
    need_date: date
    quantity: Decimal
    lot_size_applied: bool
    bucket_id: UUID


class MrpRunResponse(BaseModel):
    scenario_id: UUID
    item_id: str
    location_id: str
    planned_orders_created: int
    planned_orders: list[PlannedOrderOut]
    message: str


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


def _get_planning_params(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID,
) -> dict:
    """Return the current active planning params for item/location. Returns defaults if none found."""
    row = db.execute(
        """
        SELECT lead_time_total_days, min_order_qty, safety_stock_qty
        FROM item_planning_params
        WHERE item_id = %s
          AND location_id = %s
          AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
        ORDER BY effective_from DESC
        LIMIT 1
        """,
        (item_id, location_id),
    ).fetchone()

    if row:
        return {
            "lead_time_total_days": int(row["lead_time_total_days"]) if row["lead_time_total_days"] is not None else 0,
            "min_order_qty": Decimal(str(row["min_order_qty"])) if row["min_order_qty"] is not None else None,
            "safety_stock_qty": Decimal(str(row["safety_stock_qty"])) if row["safety_stock_qty"] is not None else None,
        }
    return {
        "lead_time_total_days": 0,
        "min_order_qty": None,
        "safety_stock_qty": None,
    }


def _get_projection_series_id(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
) -> UUID | None:
    """Return projection_series_id for item/location/scenario."""
    row = db.execute(
        """
        SELECT series_id FROM projection_series
        WHERE item_id = %s AND location_id = %s AND scenario_id = %s
        LIMIT 1
        """,
        (item_id, location_id, scenario_id),
    ).fetchone()
    return row["series_id"] if row else None


def _get_pi_nodes_in_horizon(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
    horizon_days: int,
) -> list[dict]:
    """Return all PI nodes for item/location/scenario within horizon, ordered by time_span_start."""
    today = date.today()
    horizon_end = today + timedelta(days=horizon_days)

    rows = db.execute(
        """
        SELECT node_id, time_span_start, time_span_end, closing_stock
        FROM nodes
        WHERE node_type = 'ProjectedInventory'
          AND item_id = %s
          AND location_id = %s
          AND scenario_id = %s
          AND active = TRUE
          AND time_span_start >= %s
          AND time_span_start <= %s
        ORDER BY time_span_start ASC
        """,
        (item_id, location_id, scenario_id, today, horizon_end),
    ).fetchall()

    return [dict(r) for r in rows]


def _apply_lot_sizing(raw_qty: Decimal, min_order_qty: Decimal | None) -> tuple[Decimal, bool]:
    """
    Apply lot sizing: round up to nearest multiple of min_order_qty if set.
    Returns (final_qty, lot_size_applied).
    """
    if min_order_qty is None or min_order_qty <= 0:
        return raw_qty, False

    if raw_qty <= Decimal("0"):
        return min_order_qty, True  # minimum order even for zero

    # Round up to nearest multiple of min_order_qty
    multiples = math.ceil(float(raw_qty) / float(min_order_qty))
    final_qty = min_order_qty * Decimal(multiples)
    lot_size_applied = (final_qty != raw_qty)
    return final_qty, lot_size_applied


def _clear_existing_planned_supply(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
) -> None:
    """Soft-delete existing PlannedSupply nodes and their edges for item/loc/scenario (source='mrp')."""
    # Soft-delete edges from these nodes first
    db.execute(
        """
        UPDATE edges SET active = FALSE
        WHERE scenario_id = %s
          AND edge_type = 'replenishes'
          AND from_node_id IN (
              SELECT node_id FROM nodes
              WHERE node_type = 'PlannedSupply'
                AND item_id = %s
                AND location_id = %s
                AND scenario_id = %s
                AND source = 'mrp'
                AND active = TRUE
          )
        """,
        (scenario_id, item_id, location_id, scenario_id),
    )

    # Soft-delete the PlannedSupply nodes themselves
    db.execute(
        """
        UPDATE nodes SET active = FALSE, updated_at = now()
        WHERE node_type = 'PlannedSupply'
          AND item_id = %s
          AND location_id = %s
          AND scenario_id = %s
          AND source = 'mrp'
          AND active = TRUE
        """,
        (item_id, location_id, scenario_id),
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/mrp/run
# ─────────────────────────────────────────────────────────────

@router.post(
    "/run",
    response_model=MrpRunResponse,
    summary="Run MRP",
    description=(
        "Time-phased MRP explosion for a given item/location. "
        "Creates PlannedSupply nodes for shortage buckets, wires them via 'replenishes' edges, "
        "and emits ingestion_complete events to trigger graph propagation."
    ),
)
async def run_mrp(
    body: MrpRunRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> MrpRunResponse:
    """
    Time-phased MRP: scan PI shortage buckets, create PlannedSupply nodes,
    wire them into the graph, and trigger propagation via events.
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

    # 2. Resolve scenario_id (default baseline)
    scenario_uuid = _resolve_scenario_uuid(db, body.scenario_id)

    # 3. Get planning params (lead_time, min_order_qty, safety_stock_qty)
    params = _get_planning_params(db, item_uuid, location_uuid)
    lead_time_days: int = params["lead_time_total_days"]
    min_order_qty: Decimal | None = params["min_order_qty"]
    safety_stock_qty: Decimal | None = params["safety_stock_qty"]

    # 4. Get PI nodes in horizon
    pi_nodes = _get_pi_nodes_in_horizon(db, item_uuid, location_uuid, scenario_uuid, body.horizon_days)

    if not pi_nodes:
        logger.info(
            "mrp.run item=%s location=%s scenario=%s — no PI nodes found in horizon",
            body.item_id, body.location_id, scenario_uuid,
        )
        return MrpRunResponse(
            scenario_id=scenario_uuid,
            item_id=body.item_id,
            location_id=body.location_id,
            planned_orders_created=0,
            planned_orders=[],
            message="No projected inventory buckets found in horizon. No planned orders created.",
        )

    # 5. If clear_existing: soft-delete existing PlannedSupply nodes + edges
    if body.clear_existing:
        _clear_existing_planned_supply(db, item_uuid, location_uuid, scenario_uuid)
        logger.info(
            "mrp.run clear_existing item=%s location=%s scenario=%s",
            body.item_id, body.location_id, scenario_uuid,
        )

    # 6. Iterate PI nodes and create PlannedSupply nodes for shortages
    today = date.today()
    planned_orders: list[PlannedOrderOut] = []

    for pi in pi_nodes:
        closing_stock = Decimal(str(pi["closing_stock"])) if pi["closing_stock"] is not None else Decimal("0")
        need_date: date = pi["time_span_start"]

        # Determine if there is a shortage
        has_shortage = False
        raw_net_req = Decimal("0")

        if safety_stock_qty is not None:
            # Shortage if closing_stock < safety_stock_qty
            if closing_stock < safety_stock_qty:
                has_shortage = True
                raw_net_req = safety_stock_qty - closing_stock
        else:
            # Shortage if closing_stock < 0 (stockout)
            if closing_stock < Decimal("0"):
                has_shortage = True
                raw_net_req = abs(closing_stock)

        if not has_shortage or raw_net_req <= Decimal("0"):
            continue

        # Apply lot sizing
        final_qty, lot_size_applied = _apply_lot_sizing(raw_net_req, min_order_qty)

        # Compute order date: need_date - lead_time (clamp to today if in past)
        order_date = need_date - timedelta(days=lead_time_days)
        if order_date < today:
            order_date = today

        # Create PlannedSupply node
        ps_node_id = uuid4()
        db.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                quantity, time_grain, time_ref, is_dirty, active,
                created_at, updated_at
            ) VALUES (
                %s, 'PlannedSupply', %s, %s, %s,
                %s, 'exact_date', %s, TRUE, TRUE,
                now(), now()
            )
            """,
            (ps_node_id, scenario_uuid, item_uuid, location_uuid,
             final_qty, order_date),
        )

        # Create edge: PlannedSupply --replenishes--> PI node
        edge_id = uuid4()
        db.execute(
            """
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active, created_at)
            VALUES (%s, 'replenishes', %s, %s, %s, TRUE, now())
            ON CONFLICT DO NOTHING
            """,
            (edge_id, ps_node_id, pi["node_id"], scenario_uuid),
        )

        # Emit ingestion_complete event for the PI node (triggers propagation)
        event_id = uuid4()
        db.execute(
            """
            INSERT INTO events (event_id, event_type, scenario_id, trigger_node_id, processed, source, created_at)
            VALUES (%s, 'ingestion_complete', %s, %s, FALSE, 'mrp', now())
            """,
            (event_id, scenario_uuid, ps_node_id),
        )

        planned_orders.append(
            PlannedOrderOut(
                node_id=ps_node_id,
                item_id=item_uuid,
                location_id=location_uuid,
                order_date=order_date,
                need_date=need_date,
                quantity=final_qty,
                lot_size_applied=lot_size_applied,
                bucket_id=pi["node_id"],
            )
        )

    count = len(planned_orders)
    logger.info(
        "mrp.run item=%s location=%s scenario=%s horizon=%d — %d planned orders created",
        body.item_id, body.location_id, scenario_uuid, body.horizon_days, count,
    )

    return MrpRunResponse(
        scenario_id=scenario_uuid,
        item_id=body.item_id,
        location_id=body.location_id,
        planned_orders_created=count,
        planned_orders=planned_orders,
        message=(
            f"MRP run complete. {count} planned order(s) created "
            f"over {body.horizon_days}-day horizon."
        ),
    )
