"""
Graph Integration Layer for APICS MRP Engine.

Bridges the MRP engine with the Ootils Core graph-based architecture:
- Creates PlannedSupply nodes for planned order receipts and releases
- Creates replenishes edges (PlannedSupply → ProjectedInventory)
- Creates requires edges (release → receipt within same planned order)
- Creates pegged_to edges (PlannedSupply → demand nodes)
- Emits ingestion_complete events to trigger propagation
- Supports scenario-aware node creation with baseline/branching
- Cleans up previous MRP run artefacts before re-persisting
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from uuid import UUID, uuid4

import psycopg

from ootils_core.engine.mrp.gross_to_net import BucketRecord

logger = logging.getLogger(__name__)

# Baseline scenario UUID (matches models.Scenario.BASELINE_ID)
BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


class GraphIntegration:
    """Integrate MRP results with the graph store.

    Design principles:
    - All node/edge writes go through raw SQL because the nodes table has
      MRP-specific columns (mrp_run_id, planned_order_type, parent_node_id,
      pegged_demand_node_id) that are not covered by GraphStore.upsert_node().
    - PlannedSupply nodes carry ``planned_order_type`` = RECEIPT or RELEASE
      so the engine can distinguish receipt vs. release while keeping a single
      valid ``node_type`` ('PlannedSupply' is in the CHECK constraint;
      'PlannedOrderRelease' is NOT).
    - ``replenishes`` edges wire receipt nodes to PI buckets.
    - ``requires`` edges wire release nodes to their receipt node.
    - ``pegged_to`` edges wire receipt nodes back to demand nodes.
    - After persisting, ``emit_ingestion_events`` inserts one
      ``ingestion_complete`` event per receipt node so the PropagationEngine
      picks up downstream effects.
    """

    def __init__(self, db: psycopg.Connection, scenario_id: UUID):
        self.db = db
        self.scenario_id = scenario_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def persist_planned_orders(
        self,
        run_id: UUID,
        records: List[BucketRecord],
        planning_params_map: Dict[UUID, dict],
    ) -> Tuple[List[UUID], int, int]:
        """
        Persist MRP planned orders as graph nodes and edges.

        Creates per record with planned_order_receipts > 0:
        - PlannedSupply (RECEIPT) node — the supply arriving at due date
        - PlannedSupply (RELEASE) node — offset by lead time
        - ``requires`` edge: release → receipt
        - ``replenishes`` edge: receipt → matching PI bucket
        - ``pegged_to`` edge(s): receipt → demand nodes

        Returns:
            (receipt_node_ids, nodes_created, edges_created)
        """
        receipt_node_ids: List[UUID] = []
        nodes_created = 0
        edges_created = 0

        for record in records:
            if record.planned_order_receipts <= 0:
                continue

            params = planning_params_map.get(record.item_id, {})
            lead_time_days = int(params.get("lead_time_total_days") or 0)

            # --- Receipt node ---
            receipt_node_id = uuid4()
            self.db.execute(
                """
                INSERT INTO nodes (
                    node_id, node_type, scenario_id, item_id, location_id,
                    quantity, qty_uom,
                    time_grain, time_ref, time_span_start, time_span_end,
                    is_dirty, has_shortage, shortage_qty, active,
                    mrp_run_id, planned_order_type,
                    created_at, updated_at
                ) VALUES (
                    %s, 'PlannedSupply', %s, %s, %s,
                    %s, 'EA',
                    'exact_date', %s, %s, %s,
                    TRUE, %s, %s, TRUE,
                    %s, 'RECEIPT',
                    NOW(), NOW()
                )
                """,
                (
                    receipt_node_id,
                    self.scenario_id,
                    record.item_id,
                    record.location_id,
                    record.planned_order_receipts,
                    record.period_start,
                    record.period_start,
                    record.period_end,
                    record.has_shortage,
                    record.shortage_qty,
                    run_id,
                ),
            )
            nodes_created += 1
            receipt_node_ids.append(receipt_node_id)

            # --- Release node (offset by lead time) ---
            release_date = record.period_start - timedelta(days=lead_time_days)
            release_node_id = uuid4()
            self.db.execute(
                """
                INSERT INTO nodes (
                    node_id, node_type, scenario_id, item_id, location_id,
                    quantity, qty_uom,
                    time_grain, time_ref, time_span_start, time_span_end,
                    is_dirty, has_shortage, shortage_qty, active,
                    mrp_run_id, parent_node_id, planned_order_type,
                    created_at, updated_at
                ) VALUES (
                    %s, 'PlannedSupply', %s, %s, %s,
                    %s, 'EA',
                    'exact_date', %s, %s, %s,
                    TRUE, FALSE, 0, TRUE,
                    %s, %s, 'RELEASE',
                    NOW(), NOW()
                )
                """,
                (
                    release_node_id,
                    self.scenario_id,
                    record.item_id,
                    record.location_id,
                    record.planned_order_releases,
                    release_date,
                    release_date,
                    release_date + timedelta(days=7),
                    run_id,
                    receipt_node_id,
                ),
            )
            nodes_created += 1

            # --- requires edge: release → receipt ---
            self.db.execute(
                """
                INSERT INTO edges (
                    edge_id, from_node_id, to_node_id,
                    edge_type, scenario_id,
                    priority, weight_ratio,
                    active, created_at
                ) VALUES (
                    %s, %s, %s,
                    'requires', %s,
                    1, 1.0,
                    TRUE, NOW()
                )
                """,
                (uuid4(), release_node_id, receipt_node_id, self.scenario_id),
            )
            edges_created += 1

            # --- replenishes edge: receipt → PI bucket ---
            pi_wired = self._wire_receipt_to_pi(
                receipt_node_id=receipt_node_id,
                item_id=record.item_id,
                location_id=record.location_id,
                time_ref=record.period_start,
            )
            edges_created += pi_wired

            # --- pegged_to edge: receipt → demand nodes ---
            pegged = self._peg_receipt_to_demand(
                receipt_node_id=receipt_node_id,
                item_id=record.item_id,
                location_id=record.location_id,
                time_ref=record.period_start,
                net_requirements=record.net_requirements,
            )
            edges_created += pegged

        logger.info(
            "persist_planned_orders: created %d nodes, %d edges for run %s (scenario %s)",
            nodes_created, edges_created, run_id, self.scenario_id,
        )
        return receipt_node_ids, nodes_created, edges_created

    def emit_ingestion_events(self, node_ids: List[UUID]) -> int:
        """
        Emit one ``ingestion_complete`` event per receipt node so the
        PropagationEngine recalculates downstream PI buckets.

        Returns the number of events created.
        """
        from datetime import datetime, timezone as _tz

        count = 0
        now = datetime.now(_tz.utc)
        for node_id in node_ids:
            event_id = uuid4()
            self.db.execute(
                """
                INSERT INTO events (
                    event_id, event_type, scenario_id,
                    trigger_node_id, processed, source, created_at
                ) VALUES (
                    %s, 'ingestion_complete', %s,
                    %s, FALSE, 'mrp_engine', %s
                )
                """,
                (event_id, self.scenario_id, node_id, now),
            )
            count += 1
        logger.info(
            "emit_ingestion_events: created %d events for scenario %s",
            count, self.scenario_id,
        )
        return count

    def persist_action_messages(
        self,
        run_id: UUID,
        records: List[BucketRecord],
    ) -> int:
        """Persist MRP action messages for exceptions.

        Uses the actual mrp_action_messages table schema (column = run_id).
        """
        messages_created = 0

        for record in records:
            if not record.has_shortage:
                continue

            message_type, priority, description = self._classify_shortage(record)
            loc_id = record.location_id or UUID("00000000-0000-0000-0000-000000000001")

            self.db.execute(
                """
                INSERT INTO mrp_action_messages (
                    message_id, run_id, item_id, location_id,
                    message_type, priority, description,
                    reference_date, proposed_date,
                    current_qty, proposed_qty,
                    status
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    'NEW'
                )
                """,
                (
                    uuid4(),
                    run_id,
                    record.item_id,
                    loc_id,
                    message_type,
                    priority,
                    description,
                    record.period_start,
                    record.period_end,
                    record.gross_requirements,
                    record.planned_order_receipts,
                ),
            )
            messages_created += 1

        return messages_created

    def create_dependent_demand_edges(
        self,
        parent_records: List[BucketRecord],
        child_item_id: UUID,
        bom_qty_per: Decimal,
        scrap_factor: Decimal = Decimal("0"),
    ) -> Dict[date, Decimal]:
        """
        Compute dependent demand from parent planned orders to child items.

        This does NOT create edges — it returns the demand quantities so the
        MRP engine can feed them into the child item's gross-to-net calc.

        Returns:
            Dict mapping period_start → dependent demand quantity
        """
        dependent_demand: Dict[date, Decimal] = {}

        for record in parent_records:
            if record.planned_order_releases <= 0:
                continue

            effective_qty = bom_qty_per * (1 + scrap_factor)
            demand_qty = record.planned_order_releases * effective_qty

            dependent_demand[record.period_start] = (
                dependent_demand.get(record.period_start, Decimal("0")) + demand_qty
            )

        return dependent_demand

    def cleanup_previous_run(self, run_id: Optional[UUID] = None):
        """
        Clean up nodes/edges from a previous MRP run.

        Two modes:
        - run_id provided: delete only artefacts belonging to that run
        - run_id is None: delete all PlannedSupply nodes for this scenario
        """
        if run_id:
            # Delete edges referencing nodes from this run
            self.db.execute(
                """
                DELETE FROM edges
                WHERE from_node_id IN (
                    SELECT node_id FROM nodes WHERE mrp_run_id = %s
                ) OR to_node_id IN (
                    SELECT node_id FROM nodes WHERE mrp_run_id = %s
                )
                """,
                (run_id, run_id),
            )
            # Delete nodes from this run
            self.db.execute(
                "DELETE FROM nodes WHERE mrp_run_id = %s",
                (run_id,),
            )
        else:
            # Delete all PlannedSupply nodes for scenario
            self.db.execute(
                """
                DELETE FROM edges
                WHERE from_node_id IN (
                    SELECT node_id FROM nodes
                    WHERE node_type = 'PlannedSupply'
                    AND scenario_id = %s
                ) OR to_node_id IN (
                    SELECT node_id FROM nodes
                    WHERE node_type = 'PlannedSupply'
                    AND scenario_id = %s
                )
                """,
                (self.scenario_id, self.scenario_id),
            )
            self.db.execute(
                """
                DELETE FROM nodes
                WHERE node_type = 'PlannedSupply'
                AND scenario_id = %s
                """,
                (self.scenario_id,),
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wire_receipt_to_pi(
        self,
        receipt_node_id: UUID,
        item_id: UUID,
        location_id: Optional[UUID],
        time_ref: date,
    ) -> int:
        """
        Create a ``replenishes`` edge from a PlannedSupply receipt node to
        the matching ProjectedInventory bucket.

        Returns 1 if an edge was created, 0 otherwise.
        """
        loc_id = location_id or UUID("00000000-0000-0000-0000-000000000001")

        pi_row = self.db.execute(
            """
            SELECT node_id FROM nodes
            WHERE node_type = 'ProjectedInventory'
              AND item_id = %s
              AND location_id = %s
              AND scenario_id = %s
              AND active = TRUE
              AND time_span_start <= %s
              AND time_span_end > %s
            ORDER BY time_span_start ASC
            LIMIT 1
            """,
            (item_id, loc_id, self.scenario_id, time_ref, time_ref),
        ).fetchone()

        if pi_row is None:
            logger.debug(
                "_wire_receipt_to_pi: no PI bucket for item=%s loc=%s date=%s",
                item_id, loc_id, time_ref,
            )
            return 0

        pi_node_id = pi_row["node_id"]

        # Idempotent: skip if edge already exists
        existing = self.db.execute(
            """
            SELECT 1 FROM edges
            WHERE from_node_id = %s
              AND to_node_id = %s
              AND edge_type = 'replenishes'
              AND scenario_id = %s
              AND active = TRUE
            LIMIT 1
            """,
            (receipt_node_id, pi_node_id, self.scenario_id),
        ).fetchone()

        if existing:
            logger.debug(
                "_wire_receipt_to_pi: edge already exists PlannedSupply=%s → PI=%s",
                receipt_node_id, pi_node_id,
            )
            return 0

        self.db.execute(
            """
            INSERT INTO edges (
                edge_id, from_node_id, to_node_id,
                edge_type, scenario_id,
                priority, weight_ratio,
                active, created_at
            ) VALUES (
                %s, %s, %s,
                'replenishes', %s,
                0, 1.0,
                TRUE, NOW()
            )
            """,
            (uuid4(), receipt_node_id, pi_node_id, self.scenario_id),
        )
        logger.debug(
            "_wire_receipt_to_pi: wired PlannedSupply=%s → PI=%s via replenishes",
            receipt_node_id, pi_node_id,
        )
        return 1

    def _peg_receipt_to_demand(
        self,
        receipt_node_id: UUID,
        item_id: UUID,
        location_id: Optional[UUID],
        time_ref: date,
        net_requirements: Decimal,
    ) -> int:
        """
        Create ``pegged_to`` edges from a PlannedSupply receipt node to the
        demand nodes that triggered it (ForecastDemand / CustomerOrderDemand).

        Pegging logic:
        - Find demand nodes for the same item+location+scenario overlapping
          the receipt's time_ref.
        - If net_requirements > 0, the receipt exists because of demand → peg it.
        - Distribute pegging across demand nodes proportionally (weight_ratio).

        Returns the number of pegged_to edges created.
        """
        if net_requirements <= 0:
            return 0

        loc_id = location_id or UUID("00000000-0000-0000-0000-000000000001")

        # Primary: demand nodes overlapping the receipt's period
        demand_rows = self.db.execute(
            """
            SELECT node_id, quantity, node_type
            FROM nodes
            WHERE node_type IN ('ForecastDemand', 'CustomerOrderDemand')
              AND item_id = %s
              AND location_id = %s
              AND scenario_id = %s
              AND active = TRUE
              AND time_ref <= %s
              AND time_span_start <= %s
              AND time_span_end > %s
            ORDER BY time_ref ASC
            """,
            (item_id, loc_id, self.scenario_id, time_ref, time_ref, time_ref),
        ).fetchall()

        if not demand_rows:
            # Fallback: any demand nodes for this item+location (broader match)
            demand_rows = self.db.execute(
                """
                SELECT node_id, quantity, node_type
                FROM nodes
                WHERE node_type IN ('ForecastDemand', 'CustomerOrderDemand')
                  AND item_id = %s
                  AND location_id = %s
                  AND scenario_id = %s
                  AND active = TRUE
                ORDER BY time_ref ASC
                LIMIT 5
                """,
                (item_id, loc_id, self.scenario_id),
            ).fetchall()

        if not demand_rows:
            logger.debug(
                "_peg_receipt_to_demand: no demand nodes for item=%s loc=%s",
                item_id, loc_id,
            )
            return 0

        edges_created = 0
        total_demand = sum(
            (row["quantity"] or Decimal("0")) for row in demand_rows
        )

        for row in demand_rows:
            demand_node_id = row["node_id"]
            demand_qty = row["quantity"] or Decimal("0")

            # Proportional weight: fraction of net_requirements this demand accounts for
            if total_demand > 0:
                weight = float(demand_qty / total_demand)
            else:
                weight = 1.0 / len(demand_rows)

            # Idempotent check
            existing = self.db.execute(
                """
                SELECT 1 FROM edges
                WHERE from_node_id = %s
                  AND to_node_id = %s
                  AND edge_type = 'pegged_to'
                  AND scenario_id = %s
                  AND active = TRUE
                LIMIT 1
                """,
                (receipt_node_id, demand_node_id, self.scenario_id),
            ).fetchone()

            if existing:
                continue

            self.db.execute(
                """
                INSERT INTO edges (
                    edge_id, from_node_id, to_node_id,
                    edge_type, scenario_id,
                    priority, weight_ratio,
                    active, created_at
                ) VALUES (
                    %s, %s, %s,
                    'pegged_to', %s,
                    1, %s,
                    TRUE, NOW()
                )
                """,
                (uuid4(), receipt_node_id, demand_node_id, self.scenario_id, round(weight, 6)),
            )
            edges_created += 1
            logger.debug(
                "_peg_receipt_to_demand: pegged PlannedSupply=%s → %s=%s (weight=%.4f)",
                receipt_node_id, row["node_type"], demand_node_id, weight,
            )

        return edges_created

    def _classify_shortage(self, record: BucketRecord) -> tuple:
        """
        Classify shortage type for MRP action message.

        Returns (message_type, priority, description).
        message_type: EXPEDITE | DEFER | CANCEL | RELEASE | RESCHEDULE
        priority: HIGH | MEDIUM | LOW
        """
        if record.projected_on_hand < 0:
            return (
                "EXPEDITE", "HIGH",
                f"Shortage of {abs(record.projected_on_hand)} for item in period {record.period_start}",
            )
        elif record.projected_on_hand == Decimal("0") and record.gross_requirements > 0:
            return (
                "RELEASE", "MEDIUM",
                f"Zero on-hand with demand of {record.gross_requirements} in period {record.period_start}",
            )
        elif record.time_fence_zone == "FROZEN" and record.net_requirements > 0:
            return (
                "RESCHEDULE", "HIGH",
                f"Order needed inside frozen fence for period {record.period_start}",
            )
        elif record.time_fence_zone == "SLASHED" and record.net_requirements > 0:
            return (
                "RESCHEDULE", "MEDIUM",
                f"Order needed inside slashed fence for period {record.period_start}",
            )
        else:
            return (
                "RELEASE", "LOW",
                f"Reorder point reached for period {record.period_start}",
            )