"""
API Router for APICS-compliant MRP endpoints.

Endpoints:
- POST /v1/mrp/apics/run – Full multi-level APICS MRP run
- POST /v1/mrp/consumption – Forecast consumption
- POST /v1/mrp/lot-sizing – Lot sizing calculation
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg import Connection
from pydantic import BaseModel, Field

from ootils_core.api.dependencies import get_db, resolve_scenario_id, BASELINE_SCENARIO_ID
from ootils_core.engine.mrp.mrp_apics_engine import MrpApicsEngine, MrpRunConfig
from ootils_core.engine.mrp.forecast_consumer import ForecastConsumer, ForecastConsumerCore, ConsumptionStrategy
from ootils_core.engine.mrp.lot_sizing import LotSizingEngine, LotSizeRule
from ootils_core.engine.mrp.time_fences import TimeFenceChecker, TimeFenceZone
from ootils_core.engine.mrp.llc_calculator import LLCCalculator

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/mrp", tags=["mrp-apics"])


# ─── Request / Response Models ───────────────────────────────────────────────

class MrpApicsRunRequest(BaseModel):
    """Request body for APICS MRP run."""
    scenario_id: Optional[str] = None
    location_id: str = Field(..., min_length=1)
    item_ids: Optional[List[str]] = None
    horizon_days: int = Field(default=90, ge=7, le=365)
    bucket_grain: str = Field(default="week", pattern="^(day|week|month)$")
    start_date: Optional[str] = None  # ISO date string
    recalculate_llc: bool = False
    forecast_strategy: str = Field(default="MAX", pattern="^(MAX|FORECAST_ONLY|ORDERS_ONLY|PRIORITY|max_only|consume_forward|consume_backward|consume_both)$")
    consumption_window_days: int = Field(default=7, ge=1, le=90)


class BucketRecordResponse(BaseModel):
    """Single bucket record in MRP results."""
    period_start: str
    period_end: str
    bucket_sequence: int
    gross_requirements: str
    scheduled_receipts: str
    projected_on_hand: str
    net_requirements: str
    planned_order_receipts: str
    planned_order_releases: str
    has_shortage: bool
    shortage_qty: str
    llc: int
    time_fence_zone: Optional[str] = None
    lot_size_rule_applied: Optional[str] = None


class ActionMessageResponse(BaseModel):
    message_type: str
    item_id: str
    location_id: Optional[str] = None
    period_start: str
    quantity: str
    shortage_qty: str
    time_fence_zone: Optional[str] = None


class MrpApicsRunResponse(BaseModel):
    """Response from APICS MRP run."""
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


class ConsumptionRequest(BaseModel):
    """Request for forecast consumption."""
    item_ids: Optional[List[str]] = None
    location_id: str = Field(..., min_length=1)
    scenario_id: Optional[str] = None
    horizon_days: int = Field(default=90, ge=7, le=365)
    strategy: str = Field(default="MAX", pattern="^(MAX|FORECAST_ONLY|ORDERS_ONLY|PRIORITY|max_only|consume_forward|consume_backward|consume_both)$")
    consumption_window_days: int = Field(default=7, ge=1, le=90)


class ConsumedBucketResponse(BaseModel):
    period_start: str
    period_end: str
    original_forecast: str
    customer_orders: str
    consumed_qty: str
    remaining_forecast: str
    carry_forward: str
    carry_backward: str
    net_demand: str
    strategy: str


class ConsumptionItemResult(BaseModel):
    item_id: str
    location_id: Optional[str] = None
    buckets: List[ConsumedBucketResponse]


class ConsumptionResponse(BaseModel):
    items: List[ConsumptionItemResult]
    strategy: str
    elapsed_ms: float


class LotSizingRequest(BaseModel):
    """Request for lot sizing calculation."""
    net_requirements: str
    projected_on_hand: str
    lot_size_rule: str = "LOTFORLOT"
    min_order_qty: Optional[str] = None
    max_order_qty: Optional[str] = None
    economic_order_qty: Optional[str] = None
    order_multiple_qty: Optional[str] = None
    lot_size_poq_periods: int = 1
    future_net_reqs: Optional[List[str]] = None


class LotSizingResponse(BaseModel):
    planned_order_qty: str
    lot_size_rule_applied: str


class LlcResponse(BaseModel):
    """Response from LLC calculation."""
    items_updated: int
    llc_map: dict


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/apics/run", response_model=MrpApicsRunResponse)
async def run_mrp_apics(
    request: MrpApicsRunRequest,
    db: Connection = Depends(get_db),
):
    """
    Execute a full APICS-compliant multi-level MRP run.

    Processes items from LLC 0 (finished goods) through LLC N (raw materials),
    consuming forecast, calculating gross-to-net, applying lot sizing and time fences,
    and exploding dependent demand through the BOM.
    """
    try:
        # Parse scenario
        scenario_id = BASELINE_SCENARIO_ID
        if request.scenario_id:
            try:
                scenario_id = resolve_scenario_id(db, request.scenario_id)
            except Exception:
                scenario_id = UUID(request.scenario_id)

        location_id = UUID(request.location_id) if request.location_id else None
        item_ids = [UUID(i) for i in request.item_ids] if request.item_ids else None
        start_date = date.fromisoformat(request.start_date) if request.start_date else date.today()

        config = MrpRunConfig(
            scenario_id=scenario_id,
            location_id=location_id,
            item_ids=item_ids,
            horizon_days=request.horizon_days,
            bucket_grain=request.bucket_grain,
            start_date=start_date,
            recalculate_llc=request.recalculate_llc,
            forecast_strategy=request.forecast_strategy,
            consumption_window_days=request.consumption_window_days,
        )

        engine = MrpApicsEngine(db)
        result = engine.run(config)

        db.commit()

        return MrpApicsRunResponse(
            run_id=str(result.run_id),
            scenario_id=str(result.scenario_id),
            status=result.status,
            items_processed=result.items_processed,
            total_records=result.total_records,
            action_messages=result.action_messages,
            nodes_created=result.nodes_created,
            edges_created=result.edges_created,
            elapsed_ms=result.elapsed_ms,
            errors=result.errors,
        )

    except Exception as e:
        db.rollback()
        logger.exception("MRP APICS run failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/consumption", response_model=ConsumptionResponse)
async def run_consumption(
    request: ConsumptionRequest,
    db: Connection = Depends(get_db),
):
    """
    Run forecast consumption against customer orders.

    Supports multiple strategies: MAX, FORECAST_ONLY, ORDERS_ONLY, PRIORITY (backward-compatible: max_only, consume_forward, consume_backward, consume_both).
    Returns net demand per period for each item.
    """
    try:
        start_time = time.monotonic()

        scenario_id = BASELINE_SCENARIO_ID
        if request.scenario_id:
            try:
                scenario_id = resolve_scenario_id(db, request.scenario_id)
            except Exception:
                scenario_id = UUID(request.scenario_id)

        location_id = UUID(request.location_id) if request.location_id else None

        consumer = ForecastConsumer(db, scenario_id)

        if request.item_ids:
            # Process specific items
            items = []
            for item_id_str in request.item_ids:
                item_id = UUID(item_id_str)
                consumed = consumer.consume_item(
                    item_id=item_id,
                    location_id=location_id,
                    horizon_days=request.horizon_days,
                    strategy=request.strategy,
                    window_days=request.consumption_window_days,
                )
                items.append(ConsumptionItemResult(
                    item_id=item_id_str,
                    location_id=request.location_id,
                    buckets=[
                        ConsumedBucketResponse(
                            period_start=b.period_start.isoformat(),
                            period_end=b.period_end.isoformat(),
                            original_forecast=str(b.original_forecast),
                            customer_orders=str(b.customer_orders),
                            consumed_qty=str(b.consumed_qty),
                            remaining_forecast=str(b.remaining_forecast),
                            carry_forward=str(b.carry_forward),
                            carry_backward=str(b.carry_backward),
                            net_demand=str(b.net_demand),
                            strategy=b.strategy,
                        )
                        for b in consumed
                    ],
                ))
        else:
            # Process all items with forecast demand
            consumed_map = consumer.consume_all(
                location_id=location_id,
                horizon_days=request.horizon_days,
                strategy=request.strategy,
                consumption_window_days=request.consumption_window_days,
            )
            items = []
            for item_id, net_demand in consumed_map.items():
                # Re-run consumption to get bucket details
                consumed = consumer.consume_item(
                    item_id=item_id,
                    location_id=location_id,
                    horizon_days=request.horizon_days,
                    strategy=request.strategy,
                )
                items.append(ConsumptionItemResult(
                    item_id=str(item_id),
                    location_id=request.location_id,
                    buckets=[
                        ConsumedBucketResponse(
                            period_start=b.period_start.isoformat(),
                            period_end=b.period_end.isoformat(),
                            original_forecast=str(b.original_forecast),
                            customer_orders=str(b.customer_orders),
                            consumed_qty=str(b.consumed_qty),
                            remaining_forecast=str(b.remaining_forecast),
                            carry_forward=str(b.carry_forward),
                            carry_backward=str(b.carry_backward),
                            net_demand=str(b.net_demand),
                            strategy=b.strategy,
                        )
                        for b in consumed
                    ],
                ))

        elapsed_ms = (time.monotonic() - start_time) * 1000

        return ConsumptionResponse(
            items=items,
            strategy=request.strategy,
            elapsed_ms=elapsed_ms,
        )

    except Exception as e:
        db.rollback()
        logger.exception("Forecast consumption failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/lot-sizing", response_model=LotSizingResponse)
async def calculate_lot_sizing(
    request: LotSizingRequest,
    db: Connection = Depends(get_db),
):
    """
    Calculate lot sizing for a given net requirement.

    Supports: LOTFORLOT, FIXED_QTY, EOQ, POQ, MIN_MAX, MULTIPLE.
    This is a stateless calculation endpoint.
    """
    try:
        engine = LotSizingEngine(db)

        params = {
            "lot_size_rule": request.lot_size_rule,
            "min_order_qty": Decimal(request.min_order_qty) if request.min_order_qty else None,
            "max_order_qty": Decimal(request.max_order_qty) if request.max_order_qty else None,
            "economic_order_qty": Decimal(request.economic_order_qty) if request.economic_order_qty else None,
            "order_multiple_qty": Decimal(request.order_multiple_qty) if request.order_multiple_qty else None,
            "lot_size_poq_periods": request.lot_size_poq_periods,
        }

        future_reqs = None
        if request.future_net_reqs:
            future_reqs = [Decimal(r) for r in request.future_net_reqs]

        lot_qty, rule_applied = engine.calculate_lot_size(
            net_requirements=Decimal(request.net_requirements),
            projected_on_hand=Decimal(request.projected_on_hand),
            planning_params=params,
            future_net_reqs=future_reqs,
        )

        return LotSizingResponse(
            planned_order_qty=str(lot_qty),
            lot_size_rule_applied=rule_applied or "LOTFORLOT",
        )

    except Exception as e:
        logger.exception("Lot sizing calculation failed: %s", e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/apics/llc", response_model=LlcResponse)
async def get_llc(
    recalculate: bool = Query(default=False, description="Recalculate LLCs from BOM"),
    db: Connection = Depends(get_db),
):
    """
    Get Low-Level Codes for all items.
    Optionally recalculate from BOM structure.
    """
    try:
        calc = LLCCalculator(db)

        if recalculate:
            llc_map = calc.calculate_all()
        else:
            llc_map = calc.load_existing_llc()

        return LlcResponse(
            items_updated=len(llc_map) if recalculate else 0,
            llc_map={str(k): v for k, v in llc_map.items()},
        )

    except Exception as e:
        db.rollback()
        logger.exception("LLC calculation failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
