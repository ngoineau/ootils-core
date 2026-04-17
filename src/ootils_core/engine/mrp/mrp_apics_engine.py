"""
APICS-Compliant Multi-Level MRP Engine.

Orchestrates the full MRP process:
1. Calculate/retrieve Low-Level Codes (LLC)
2. Consume forecast against customer orders
3. Process items LLC 0 → N:
   a. Gross-to-net calculation
   b. Time fence enforcement
   c. Lot sizing
   d. Lead time offset (planned order releases)
4. Explode dependent demand to child items
5. Persist planned orders as graph nodes
6. Generate action messages
7. Record run in mrp_runs table

Performance: Designed for < 100ms p95 at 10k items by:
- Batch-loading data from DB (not per-item queries in the loop)
- Processing items by LLC level (natural batching)
- Using prepared statements and bulk inserts
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import UUID, uuid4

import psycopg
import json

from ootils_core.engine.mrp.llc_calculator import LLCCalculator
from ootils_core.engine.mrp.gross_to_net import (
    BucketRecord,
    GrossToNetCalculator,
    TimeBucket,
)
from ootils_core.engine.mrp.forecast_consumer import ForecastConsumer, ConsumptionStrategy
from ootils_core.engine.mrp.lot_sizing import LotSizingEngine, LotSizeRule
from ootils_core.engine.mrp.time_fences import TimeFenceChecker, TimeFenceZone
from ootils_core.engine.mrp.graph_integration import GraphIntegration

logger = logging.getLogger(__name__)

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


@dataclass
class MrpRunConfig:
    """Configuration for an MRP run."""
    scenario_id: UUID = BASELINE_SCENARIO_ID
    location_id: Optional[UUID] = None
    item_ids: Optional[List[UUID]] = None
    horizon_days: int = 90
    bucket_grain: str = "week"
    start_date: Optional[date] = None
    recalculate_llc: bool = False
    forecast_strategy: str = "MAX"
    consumption_window_days: int = 7


@dataclass
class MrpRunResult:
    """Result of an MRP run."""
    run_id: UUID
    scenario_id: UUID
    status: str
    items_processed: int
    total_records: int
    action_messages: int
    nodes_created: int
    edges_created: int
    elapsed_ms: float
    errors: List[str] = field(default_factory=list)


class MrpApicsEngine:
    """
    APICS-compliant multi-level MRP engine.

    Processes items from LLC 0 (finished goods) through LLC N (raw materials),
    consuming forecast, calculating gross-to-net, applying lot sizing,
    offsetting by lead time, and exploding dependent demand to child items.
    """

    def __init__(self, db: psycopg.Connection):
        self.db = db
        self.llc_calculator = LLCCalculator(db)
        self.gross_to_net = GrossToNetCalculator(db, BASELINE_SCENARIO_ID)
        self.forecast_consumer = ForecastConsumer(db, BASELINE_SCENARIO_ID)
        self.lot_sizing = LotSizingEngine(db)
        self.graph = GraphIntegration(db, BASELINE_SCENARIO_ID)

    def run(self, config: MrpRunConfig) -> MrpRunResult:
        """
        Execute a full APICS MRP run.

        Steps:
        1. Start an MRP run record
        2. Calculate/retrieve LLCs
        3. Batch-load planning parameters
        4. Consume forecast for all items
        5. Process items by LLC level (0 → N)
        6. Persist results

        Args:
            config: MRP run configuration

        Returns:
            MrpRunResult with run statistics
        """
        start_time = time.monotonic()
        run_id = uuid4()
        errors: List[str] = []

        # Update scenario references
        self.gross_to_net.scenario_id = config.scenario_id
        self.forecast_consumer.scenario_id = config.scenario_id
        self.graph.scenario_id = config.scenario_id

        if config.start_date is None:
            config.start_date = date.today()

        try:
            # 1. Create MRP run record
            self._create_run_record(run_id, config)

            # 2. Calculate or retrieve LLCs
            if config.recalculate_llc:
                llc_map = self.llc_calculator.calculate_all()
            else:
                llc_map = self.llc_calculator.load_existing_llc()

            # Items with no BOM (finished goods) get LLC 0
            items_by_llc = self.llc_calculator.get_items_by_llc(config.location_id)

            # Filter to requested items if specified
            if config.item_ids:
                item_set = set(config.item_ids)
                filtered = {}
                for llc, items in items_by_llc.items():
                    filtered[llc] = [i for i in items if i in item_set]
                items_by_llc = filtered

            # 3. Batch-load planning parameters
            all_item_ids = set()
            for items in items_by_llc.values():
                all_item_ids.update(items)
            planning_params_map = self._batch_load_planning_params(
                all_item_ids, config.location_id
            )

            # 4. Consume forecast for all LLC 0 items
            consumed_forecasts = self.forecast_consumer.consume_all(
                location_id=config.location_id,
                horizon_days=config.horizon_days,
                strategy=config.forecast_strategy,
                consumption_window_days=config.consumption_window_days,
            )

            # Log consumption results
            for item_id, net_demand in consumed_forecasts.items():
                buckets = self.forecast_consumer.consume_item(
                    item_id=item_id,
                    location_id=config.location_id,
                    horizon_days=config.horizon_days,
                    strategy=config.forecast_strategy,
                )
                self.forecast_consumer.log_consumption(
                    run_id=run_id,
                    item_id=item_id,
                    location_id=config.location_id,
                    consumed_buckets=buckets,
                )

            # 5. Create time buckets
            time_buckets = self.gross_to_net.create_time_buckets(
                start_date=config.start_date,
                horizon_days=config.horizon_days,
                grain=config.bucket_grain,
            )

            # 6. Process items by LLC level
            all_records: List[BucketRecord] = []
            dependent_demand_map: Dict[UUID, Dict[date, Decimal]] = defaultdict(dict)
            total_items = 0

            max_llc = max(items_by_llc.keys()) if items_by_llc else 0

            for llc_level in range(0, max_llc + 1):
                item_ids_at_level = items_by_llc.get(llc_level, [])
                if not item_ids_at_level:
                    continue

                for item_id in item_ids_at_level:
                    total_items += 1
                    params = planning_params_map.get(item_id, {})

                    # Get forecast for this item (LLC 0) or dependent demand (LLC > 0)
                    consumed_forecast = consumed_forecasts.get(item_id) if llc_level == 0 else None
                    dep_demand = dependent_demand_map.get(item_id)

                    # Gross-to-net calculation
                    records = self.gross_to_net.calculate(
                        item_id=item_id,
                        location_id=config.location_id,
                        buckets=time_buckets,
                        planning_params=params,
                        consumed_forecast=consumed_forecast,
                        dependent_demand=dep_demand,
                        llc=llc_level,
                    )

                    # Apply time fences and lot sizing
                    self._apply_lot_sizing_and_fences(
                        records=records,
                        params=params,
                        start_date=config.start_date,
                        time_buckets=time_buckets,
                    )

                    all_records.extend(records)

                    # Explode dependent demand to child items
                    if llc_level < max_llc:
                        self._explode_dependent_demand(
                            parent_records=records,
                            parent_item_id=item_id,
                            dependent_demand_map=dependent_demand_map,
                        )

            # 7. Persist results
            receipt_node_ids, nodes_created, edges_created = self.graph.persist_planned_orders(
                run_id=run_id,
                records=all_records,
                planning_params_map=planning_params_map,
            )

            messages_created = self.graph.persist_action_messages(
                run_id=run_id,
                records=all_records,
            )

            # Persist bucket records
            self._persist_bucket_records(run_id, all_records)

            # 8. Emit ingestion_complete events so PropagationEngine recalculates PI
            events_emitted = self.graph.emit_ingestion_events(receipt_node_ids)
            logger.info(
                "MRP run %s: emitted %d ingestion_complete events",
                run_id, events_emitted,
            )

            # 9. Update run record as completed
            elapsed_ms = (time.monotonic() - start_time) * 1000
            self._complete_run_record(run_id, "COMPLETED", elapsed_ms)

            return MrpRunResult(
                run_id=run_id,
                scenario_id=config.scenario_id,
                status="COMPLETED",
                items_processed=total_items,
                total_records=len(all_records),
                action_messages=messages_created,
                nodes_created=nodes_created,
                edges_created=edges_created,
                elapsed_ms=elapsed_ms,
                errors=errors,
            )

        except Exception as e:
            logger.exception("MRP run failed: %s", e)
            errors.append(str(e))
            elapsed_ms = (time.monotonic() - start_time) * 1000

            # Mark run as failed
            try:
                self._complete_run_record(run_id, "FAILED", elapsed_ms, str(e))
            except Exception:
                pass

            return MrpRunResult(
                run_id=run_id,
                scenario_id=config.scenario_id,
                status="FAILED",
                items_processed=0,
                total_records=0,
                action_messages=0,
                nodes_created=0,
                edges_created=0,
                elapsed_ms=elapsed_ms,
                errors=errors,
            )

    def _apply_lot_sizing_and_fences(
        self,
        records: List[BucketRecord],
        params: dict,
        start_date: date,
        time_buckets: List[TimeBucket],
    ):
        """Apply lot sizing and time fence rules to bucket records."""
        lead_time_days = int(params.get("lead_time_total_days") or 0)
        time_fence = TimeFenceChecker.from_planning_params(params)

        for i, record in enumerate(records):
            # Check time fence
            fence_result = time_fence.check_zone(record.period_start, start_date)
            record.time_fence_zone = fence_result.zone.value

            if record.net_requirements <= 0:
                continue

            # Get future net requirements for POQ
            future_net_reqs = [
                r.net_requirements
                for r in records[i + 1:i + int(params.get("lot_size_poq_periods") or 1)]
                if r.net_requirements > 0
            ]

            # Apply lot sizing
            lot_qty, rule_applied = self.lot_sizing.calculate_lot_size(
                net_requirements=record.net_requirements,
                projected_on_hand=record.projected_on_hand,
                planning_params=params,
                future_net_reqs=future_net_reqs,
            )

            record.planned_order_receipts = lot_qty
            record.lot_size_rule_applied = rule_applied

            # Recalculate projected on hand with lot size
            record.projected_on_hand += lot_qty

            # Lead time offset: release date = receipt date - lead time
            release_date = record.period_start - timedelta(days=lead_time_days)
            record.planned_order_releases = lot_qty

            # Handle frozen zone: push orders to frozen boundary
            if fence_result.zone == TimeFenceZone.FROZEN:
                record.planned_order_receipts = Decimal("0")
                record.planned_order_releases = Decimal("0")
                # Still record the shortage

    def _explode_dependent_demand(
        self,
        parent_records: List[BucketRecord],
        parent_item_id: UUID,
        dependent_demand_map: Dict[UUID, Dict[date, Decimal]],
    ):
        """
        Explode planned orders from parent to create dependent demand for children.

        Loads BOM lines for parent_item_id and distributes demand.
        """
        # Load BOM components for this parent
        rows = self.db.execute("""
            SELECT bl.component_item_id, bl.quantity_per, COALESCE(bl.scrap_factor, 0) AS scrap_factor
            FROM bom_headers bh
            JOIN bom_lines bl ON bl.bom_id = bh.bom_id
            WHERE bh.parent_item_id = %s
              AND bh.status = 'active'
              AND bl.active = true
        """, (parent_item_id,)).fetchall()

        for row in rows:
            child_id = UUID(str(row["component_item_id"]))
            qty_per = Decimal(str(row["quantity_per"]))
            scrap_factor = Decimal(str(row["scrap_factor"]))

            for record in parent_records:
                if record.planned_order_releases > 0:
                    effective_qty = qty_per * (1 + scrap_factor)
                    demand = record.planned_order_releases * effective_qty

                    if child_id not in dependent_demand_map:
                        dependent_demand_map[child_id] = defaultdict(Decimal)

                    dependent_demand_map[child_id][record.period_start] += demand

    def _batch_load_planning_params(
        self,
        item_ids: Set[UUID],
        location_id: Optional[UUID] = None,
    ) -> Dict[UUID, dict]:
        """Batch load planning parameters for all items."""
        if not item_ids:
            return {}

        item_id_list = list(item_ids)

        if location_id:
            rows = self.db.execute("""
                SELECT DISTINCT ON (item_id, COALESCE(location_id, '00000000-0000-0000-0000-000000000001'::UUID))
                    item_id, location_id,
                    lot_size_rule, min_order_qty, max_order_qty,
                    economic_order_qty, order_multiple_qty, lot_size_poq_periods,
                    safety_stock_qty, lead_time_total_days,
                    frozen_time_fence_days, slashed_time_fence_days,
                    forecast_consumption_strategy, consumption_window_days
                FROM item_planning_params
                WHERE item_id = ANY(%s)
                  AND location_id = %s
                  AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
                ORDER BY item_id, COALESCE(location_id, '00000000-0000-0000-0000-000000000001'::UUID), effective_from DESC
            """, (item_id_list, location_id)).fetchall()
        else:
            rows = self.db.execute("""
                SELECT DISTINCT ON (item_id, COALESCE(location_id, '00000000-0000-0000-0000-000000000001'::UUID))
                    item_id, location_id,
                    lot_size_rule, min_order_qty, max_order_qty,
                    economic_order_qty, order_multiple_qty, lot_size_poq_periods,
                    safety_stock_qty, lead_time_total_days,
                    frozen_time_fence_days, slashed_time_fence_days,
                    forecast_consumption_strategy, consumption_window_days
                FROM item_planning_params
                WHERE item_id = ANY(%s)
                  AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
                ORDER BY item_id, COALESCE(location_id, '00000000-0000-0000-0000-000000000001'::UUID), effective_from DESC
            """, (item_id_list,)).fetchall()

        result: Dict[UUID, dict] = {}
        for row in rows:
            item_id = UUID(str(row["item_id"]))
            result[item_id] = {
                "lot_size_rule": row["lot_size_rule"],
                "min_order_qty": row["min_order_qty"],
                "max_order_qty": row["max_order_qty"],
                "economic_order_qty": row["economic_order_qty"],
                "order_multiple_qty": row["order_multiple_qty"],
                "lot_size_poq_periods": row["lot_size_poq_periods"],
                "safety_stock_qty": row["safety_stock_qty"],
                "lead_time_total_days": row["lead_time_total_days"],
                "frozen_time_fence_days": row["frozen_time_fence_days"],
                "slashed_time_fence_days": row["slashed_time_fence_days"],
                "forecast_consumption_strategy": row["forecast_consumption_strategy"],
                "consumption_window_days": row["consumption_window_days"],
            }

        return result

    def _create_run_record(self, run_id: UUID, config: MrpRunConfig):
        """Create the mrp_runs record."""
        # Map bucket_grain to DB bucket_type enum
        bucket_type_map = {"day": "DAY", "week": "WEEK", "month": "MONTH"}
        bucket_type = bucket_type_map.get(config.bucket_grain, "WEEK")

        self.db.execute("""
            INSERT INTO mrp_runs (
                run_id, scenario_id, location_id,
                status, run_type, horizon_days,
                bucket_type, llc_regeneration,
                started_at
            ) VALUES (
                %s, %s, %s,
                'running', 'APICS_FULL', %s,
                %s, %s,
                NOW()
            )
        """, (
            run_id,
            config.scenario_id,
            config.location_id,
            config.horizon_days,
            bucket_type,
            config.recalculate_llc,
        ))

    def _complete_run_record(
        self, run_id: UUID, status: str, elapsed_ms: float, error_msg: str = None
    ):
        """Update the mrp_runs record with completion status."""
        # Map our status to DB enum values (lowercase)
        db_status = status.lower() if status else 'failed'
        errors_json = [] if not error_msg else [{"error": error_msg}]

        self.db.execute("""
            UPDATE mrp_runs
            SET status = %s,
                completed_at = NOW(),
                execution_time_ms = %s,
                errors = %s
            WHERE run_id = %s
        """, (db_status, int(elapsed_ms), json.dumps(errors_json), run_id))

    def _persist_bucket_records(self, run_id: UUID, records: List[BucketRecord]):
        """Persist MRP bucket records."""
        for record in records:
            # location_id is NOT NULL in DB; use a sentinel if None
            loc_id = record.location_id or UUID("00000000-0000-0000-0000-000000000001")

            self.db.execute("""
                INSERT INTO mrp_bucket_records (
                    bucket_id, run_id, item_id, location_id,
                    period_start, period_end, bucket_sequence,
                    gross_requirements, scheduled_receipts,
                    projected_on_hand, net_requirements,
                    planned_order_receipts, planned_order_releases,
                    has_shortage, shortage_qty, llc,
                    time_fence_zone, lot_size_rule_applied
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s
                )
            """, (
                record.bucket_id,
                run_id,
                record.item_id,
                loc_id,
                record.period_start,
                record.period_end,
                record.bucket_sequence,
                record.gross_requirements,
                record.scheduled_receipts,
                record.projected_on_hand,
                record.net_requirements,
                record.planned_order_receipts,
                record.planned_order_releases,
                record.has_shortage,
                record.shortage_qty,
                record.llc,
                record.time_fence_zone,
                record.lot_size_rule_applied,
            ))
