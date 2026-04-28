"""
POST /v1/mrp/run — Unified MRP endpoint with optional APICS mode.

Runs MRP for a given item/location:
- Default (apics_mode=False): Single-level MRP with graph integration
- APICS mode (apics_mode=True): Full multi-level APICS MRP with BOM explosion,
  forecast consumption, and time-phased planning.
"""
from __future__ import annotations

import logging
import math
import time
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Optional
from uuid import UUID, uuid4

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/mrp", tags=["mrp"])


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

class MrpRunRequest(BaseModel):
    """Request for MRP run with optional APICS mode."""
    item_id: str
    location_id: str
    horizon_days: int = 90
    scenario_id: Optional[str] = None  # defaults to baseline
    clear_existing: bool = False
    # APICS Phase 0 options
    apics_mode: bool = Field(
        default=False,
        description="Enable full APICS multi-level MRP with BOM explosion and forecast consumption"
    )
    bucket_grain: str = Field(default="week", pattern="^(day|week|month)$")
    forecast_strategy: str = Field(
        default="MAX",
        pattern="^(MAX|FORECAST_ONLY|ORDERS_ONLY|PRIORITY|max_only|consume_forward|consume_backward|consume_both)$"
    )
    consumption_window_days: int = Field(default=7, ge=1, le=90)
    recalculate_llc: bool = False


class PlannedOrderOut(BaseModel):
    """Single planned order output (simple MRP mode)."""
    node_id: UUID
    item_id: UUID
    location_id: UUID
    order_date: date
    need_date: date
    quantity: Decimal
    lot_size_applied: bool
    bucket_id: UUID


class MrpRunResponse(BaseModel):
    """Response from MRP run (simple mode)."""
    scenario_id: UUID
    item_id: str
    location_id: str
    planned_orders_created: int
    planned_orders: list[PlannedOrderOut]
    message: str


class MrpRunResponseApics(BaseModel):
    """Response from MRP run (APICS mode)."""
    run_id: str
    scenario_id: str
    status: str
    items_processed: int
    total_records: int
    action_messages: int
    nodes_created: int
    edges_created: int
    elapsed_ms: float
    errors: List[str] = []


# ─────────────────────────────────────────────────────────────
# Helpers — Simple MRP
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
    """Soft-delete existing PlannedSupply nodes and their edges for item/loc/scenario."""
    # Soft-delete edges from these PlannedSupply nodes first
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
          AND active = TRUE
        """,
        (item_id, location_id, scenario_id),
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/mrp/run — Unified endpoint
# ─────────────────────────────────────────────────────────────

@router.post(
    "/run",
    response_model=MrpRunResponse,
    summary="Run MRP",
    description=(
        "Time-phased MRP explosion for a given item/location. "
        "Creates PlannedSupply nodes for shortage buckets, wires them via 'replenishes' edges, "
        "and emits ingestion_complete events to trigger graph propagation.\n\n"
        "**APICS Mode:** Set `apics_mode=true` for full multi-level MRP with BOM explosion, "
        "forecast consumption, and time-phased planning. Returns extended response format."
    ),
)
async def run_mrp(
    body: MrpRunRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> MrpRunResponse | MrpRunResponseApics:
    """
    Unified MRP endpoint with optional APICS mode.
    
    - **Simple mode (apics_mode=False):** Single-level MRP for one item/location
    - **APICS mode (apics_mode=True):** Multi-level MRP with BOM explosion, forecast consumption
    """
    
    # Resolve scenario_id (default baseline)
    scenario_uuid = _resolve_scenario_uuid(db, body.scenario_id)
    
    if body.apics_mode:
        # Delegate to APICS engine
        return await _run_apics_mrp(body, db, scenario_uuid)
    else:
        # Use simple single-level MRP
        return await _run_simple_mrp(body, db, scenario_uuid)


async def _run_simple_mrp(
    body: MrpRunRequest,
    db: psycopg.Connection,
    scenario_uuid: UUID,
) -> MrpRunResponse:
    """Execute simple single-level MRP (legacy behavior)."""
    
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

    # 2. Get planning params (lead_time, min_order_qty, safety_stock_qty)
    params = _get_planning_params(db, item_uuid, location_uuid)
    lead_time_days: int = params["lead_time_total_days"]
    min_order_qty: Decimal | None = params["min_order_qty"]
    safety_stock_qty: Decimal | None = params["safety_stock_qty"]

    # 3. Get PI nodes in horizon
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

    # 4. If clear_existing: soft-delete existing PlannedSupply nodes + edges
    if body.clear_existing:
        _clear_existing_planned_supply(db, item_uuid, location_uuid, scenario_uuid)
        logger.info(
            "mrp.run clear_existing item=%s location=%s scenario=%s",
            body.item_id, body.location_id, scenario_uuid,
        )

    # 5. Iterate PI nodes and create PlannedSupply nodes for shortages
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

        # Emit ingestion_complete event for the PlannedSupply node (triggers propagation)
        event_id = uuid4()
        db.execute(
            """
            INSERT INTO events (event_id, event_type, scenario_id, trigger_node_id, processed, source, created_at)
            VALUES (%s, 'ingestion_complete', %s, %s, FALSE, 'engine', now())
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


async def _run_apics_mrp(
    body: MrpRunRequest,
    db: psycopg.Connection,
    scenario_uuid: UUID,
) -> MrpRunResponseApics:
    """Execute full APICS multi-level MRP."""
    from ootils_core.engine.mrp.mrp_apics_engine import MrpApicsEngine, MrpRunConfig
    
    start_time = time.monotonic()
    
    try:
        # Resolve location UUID
        location_uuid = _resolve_location_uuid(db, body.location_id)
        if location_uuid is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Location '{body.location_id}' not found",
            )
        
        # Resolve item IDs if provided
        item_ids = None
        if body.item_id:
            item_uuid = _resolve_item_uuid(db, body.item_id)
            if item_uuid is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Item '{body.item_id}' not found",
                )
            item_ids = [item_uuid]
        
        config = MrpRunConfig(
            scenario_id=scenario_uuid,
            location_id=location_uuid,
            item_ids=item_ids,
            horizon_days=body.horizon_days,
            bucket_grain=body.bucket_grain,
            start_date=date.today(),
            recalculate_llc=body.recalculate_llc,
            forecast_strategy=body.forecast_strategy,
            consumption_window_days=body.consumption_window_days,
        )
        
        engine = MrpApicsEngine(db)
        result = engine.run(config)
        
        db.commit()
        
        elapsed_ms = (time.monotonic() - start_time) * 1000
        
        logger.info(
            "mrp.run.apics scenario=%s items=%d records=%d nodes=%d edges=%d elapsed=%.2fms",
            scenario_uuid, result.items_processed, result.total_records,
            result.nodes_created, result.edges_created, elapsed_ms,
        )
        
        return MrpRunResponseApics(
            run_id=str(result.run_id),
            scenario_id=str(result.scenario_id),
            status=result.status,
            items_processed=result.items_processed,
            total_records=result.total_records,
            action_messages=result.action_messages,
            nodes_created=result.nodes_created,
            edges_created=result.edges_created,
            elapsed_ms=elapsed_ms,
            errors=result.errors,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.exception("MRP APICS run failed: %s", e)
        elapsed_ms = (time.monotonic() - start_time) * 1000
        return MrpRunResponseApics(
            run_id=str(uuid4()),
            scenario_id=str(scenario_uuid),
            status="failed",
            items_processed=0,
            total_records=0,
            action_messages=0,
            nodes_created=0,
            edges_created=0,
            elapsed_ms=elapsed_ms,
            errors=[str(e)],
        )
