"""
propagator.py — Orchestration of incremental propagation.

Pipeline:
  event → acquire lock → expand dirty subgraph → topo sort
        → compute each node → persist → cascade → complete run
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from uuid import UUID

from ootils_core.models import CalcRun, Node, PlanningEvent, Scenario
from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.engine.kernel.graph.traversal import GraphTraversal
from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
from ootils_core.engine.orchestration.calc_run import CalcRunManager
from ootils_core.engine.kernel.calc.projection import ProjectionKernel
from ootils_core.engine.kernel.explanation.builder import ExplanationBuilder
from ootils_core.engine.kernel.shortage.detector import ShortageDetector

logger = logging.getLogger(__name__)


class PropagationEngine:
    """
    Orchestrates: event → dirty → topo sort → compute → persist → cascade.
    """

    def __init__(
        self,
        store: GraphStore,
        traversal: GraphTraversal,
        dirty: DirtyFlagManager,
        calc_run_mgr: CalcRunManager,
        kernel: ProjectionKernel,
        explanation_builder: Optional[ExplanationBuilder] = None,
        shortage_detector: Optional[ShortageDetector] = None,
    ) -> None:
        self._store = store
        self._traversal = traversal
        self._dirty = dirty
        self._calc_run_mgr = calc_run_mgr
        self._kernel = kernel
        self._explanation_builder = explanation_builder
        self._shortage_detector = shortage_detector

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def process_event(
        self,
        event_id: UUID,
        scenario_id: UUID,
        db,
    ) -> Optional[CalcRun]:
        """
        Main entry point. Acquires advisory lock, expands dirty subgraph, propagates.

        Returns the CalcRun on success, None if lock is held by another run.
        """
        # Load event
        event_row = db.execute(
            "SELECT * FROM events WHERE event_id = %s",
            (event_id,),
        ).fetchone()

        if event_row is None:
            logger.warning("Event %s not found", event_id)
            return None

        # Try to acquire lock and start calc run
        calc_run = self._calc_run_mgr.start_calc_run(
            scenario_id=scenario_id,
            event_ids=[event_id],
            db=db,
        )
        if calc_run is None:
            logger.info("Scenario %s is already locked — skipping", scenario_id)
            return None

        try:
            db.execute("SAVEPOINT propagation_start")
            # Determine time window for dirty expansion
            trigger_node_id = (
                UUID(str(event_row["trigger_node_id"]))
                if event_row.get("trigger_node_id")
                else None
            )

            if trigger_node_id is None:
                logger.info("Event %s has no trigger node — skipping propagation", event_id)
                self._finish_run(calc_run, scenario_id, db)
                return calc_run

            # Time window: from min(old_date, new_date) to horizon end
            # Default: use today ± 365 days if dates not set
            old_date = event_row.get("old_date")
            new_date = event_row.get("new_date")

            if old_date and new_date:
                window_start = min(old_date, new_date)
                window_end = max(old_date, new_date) + timedelta(days=365)
            elif old_date:
                window_start = old_date
                window_end = old_date + timedelta(days=365)
            elif new_date:
                window_start = new_date
                window_end = new_date + timedelta(days=365)
            else:
                # No date context — recompute full downstream
                window_start = date.min
                window_end = date.max

            # Expand dirty subgraph from trigger node
            dirty_node_ids = self._traversal.expand_dirty_subgraph(
                trigger_node_id=trigger_node_id,
                scenario_id=scenario_id,
                time_window=(window_start, window_end),
            )

            # Mark dirty in memory + flush to Postgres for durability
            self._dirty.mark_dirty(dirty_node_ids, scenario_id, calc_run.calc_run_id, db)
            self._dirty.flush_to_postgres(calc_run.calc_run_id, scenario_id, db)

            # Update dirty_node_count
            calc_run.dirty_node_count = len(dirty_node_ids)
            db.execute(
                "UPDATE calc_runs SET dirty_node_count = %s WHERE calc_run_id = %s",
                (calc_run.dirty_node_count, calc_run.calc_run_id),
            )

            # Propagate
            self._propagate(calc_run, dirty_node_ids, db)

            # Complete
            self._finish_run(calc_run, scenario_id, db)
            return calc_run

        except Exception as exc:
            logger.exception("Propagation failed for event %s: %s", event_id, exc)
            db.execute("ROLLBACK TO SAVEPOINT propagation_start")
            self._calc_run_mgr.fail_calc_run(calc_run, str(exc), db)
            raise

    def _finish_run(self, calc_run: CalcRun, scenario_id: UUID, db) -> None:
        """Load scenario and complete the calc run."""
        scenario_row = db.execute(
            "SELECT * FROM scenarios WHERE scenario_id = %s",
            (scenario_id,),
        ).fetchone()

        if scenario_row:
            scenario = Scenario(
                scenario_id=UUID(str(scenario_row["scenario_id"])),
                name=scenario_row["name"],
                baseline_snapshot_id=(
                    UUID(str(scenario_row["baseline_snapshot_id"]))
                    if scenario_row.get("baseline_snapshot_id")
                    else None
                ),
                is_baseline=bool(scenario_row.get("is_baseline", False)),
            )
        else:
            scenario = Scenario(
                scenario_id=scenario_id,
                name="unknown",
            )

        self._calc_run_mgr.complete_calc_run(calc_run, scenario, db)

    # ------------------------------------------------------------------
    # Propagation internals
    # ------------------------------------------------------------------

    def _propagate(
        self,
        calc_run: CalcRun,
        dirty_nodes: set[UUID],
        db,
    ) -> None:
        """
        Topological sort over dirty nodes, then compute each one in order.
        After each computation, cascade to dependents if the result changed.
        """
        if not dirty_nodes:
            return

        scenario_id = calc_run.scenario_id

        # Topological sort of dirty set
        ordered = self._traversal.topological_sort(dirty_nodes, scenario_id)

        # Remaining dirty set (may shrink as we process)
        remaining_dirty = set(dirty_nodes)

        for node_id in ordered:
            if node_id not in remaining_dirty:
                continue  # Already cleared (e.g., cascaded out of window)

            # Load node to determine type
            node = self._store.get_node(node_id, scenario_id)
            if node is None:
                self._dirty.clear_dirty(node_id, scenario_id, calc_run.calc_run_id, db)
                remaining_dirty.discard(node_id)
                continue

            if node.node_type == "ProjectedInventory":
                changed = self._recompute_pi_node(
                    node_id=node_id,
                    scenario_id=scenario_id,
                    calc_run_id=calc_run.calc_run_id,
                    db=db,
                )
                if changed:
                    calc_run.nodes_recalculated += 1
                else:
                    calc_run.nodes_unchanged += 1
            else:
                # Non-PI nodes don't need kernel computation — just mark as processed
                calc_run.nodes_unchanged += 1

            # Clear dirty flag
            self._dirty.clear_dirty(node_id, scenario_id, calc_run.calc_run_id, db)
            remaining_dirty.discard(node_id)

    def _recompute_pi_node(
        self,
        node_id: UUID,
        scenario_id: UUID,
        calc_run_id: UUID,
        db,
    ) -> bool:
        """
        Recompute a single PI node.

        Loads: opening stock (from predecessor PI node or on-hand supply),
               supply events (PO/WO nodes connected via 'replenishes' edges),
               demand events (forecast/customer order nodes via 'consumes' edges).

        Calls kernel.compute_pi_node, persists the result.

        Returns True if the result changed (triggers cascade), False if unchanged.
        """
        node = self._store.get_node(node_id, scenario_id)
        if node is None or node.node_type != "ProjectedInventory":
            return False

        bucket_start = node.time_span_start
        bucket_end = node.time_span_end

        if bucket_start is None or bucket_end is None:
            return False

        # ------------------------------------------------------------------
        # 1. Opening stock: closing_stock of the predecessor PI node in series
        # ------------------------------------------------------------------
        opening_stock = Decimal("0")

        # Find predecessor via 'feeds_forward' edge (to_node_id = this node)
        pred_edges = self._store.get_edges_to(node_id, scenario_id, edge_type="feeds_forward")
        if pred_edges:
            pred_node = self._store.get_node(pred_edges[0].from_node_id, scenario_id)
            if pred_node and pred_node.closing_stock is not None:
                opening_stock = pred_node.closing_stock
        else:
            # First bucket — find on-hand supply nodes for this item/location
            # OnHandSupply nodes connect via 'replenishes' edge too, but at bucket 0
            # Check for any on-hand node feeding into this PI node
            oh_edges = self._store.get_edges_to(node_id, scenario_id, edge_type="replenishes")
            for edge in oh_edges:
                src_node = self._store.get_node(edge.from_node_id, scenario_id)
                if src_node and src_node.node_type == "OnHandSupply":
                    opening_stock += src_node.quantity or Decimal("0")

        # ------------------------------------------------------------------
        # 2. Supply events: nodes connected via 'replenishes' to this PI node
        # ------------------------------------------------------------------
        supply_events: list = []
        replenish_edges = self._store.get_edges_to(node_id, scenario_id, edge_type="replenishes")
        for edge in replenish_edges:
            src_node = self._store.get_node(edge.from_node_id, scenario_id)
            if src_node is None:
                continue
            if src_node.node_type in ("PurchaseOrderSupply", "WorkOrderSupply",
                                       "TransferSupply", "PlannedSupply"):
                # Use time_ref as the supply date
                if src_node.time_ref is not None and src_node.quantity is not None:
                    supply_events.append((src_node.time_ref, src_node.quantity))

        # ------------------------------------------------------------------
        # 3. Demand events: nodes connected via 'consumes' to this PI node
        # ------------------------------------------------------------------
        demand_events: list = []
        consume_edges = self._store.get_edges_to(node_id, scenario_id, edge_type="consumes")
        for edge in consume_edges:
            src_node = self._store.get_node(edge.from_node_id, scenario_id)
            if src_node is None:
                continue
            if src_node.node_type in ("ForecastDemand", "CustomerOrderDemand",
                                       "DependentDemand", "TransferDemand"):
                # For demands with time_span, distribute daily within bucket
                # For now: use time_ref or time_span_start as the demand anchor date
                demand_date = src_node.time_ref or src_node.time_span_start
                if demand_date is not None and src_node.quantity is not None:
                    # If the demand has a time_span, pro-rate daily quantity
                    if (src_node.time_span_start is not None
                            and src_node.time_span_end is not None):
                        span_days = (src_node.time_span_end - src_node.time_span_start).days
                        if span_days > 0:
                            # Daily rate
                            daily_qty = src_node.quantity / Decimal(str(span_days))
                            # Count days within this bucket
                            overlap_start = max(bucket_start, src_node.time_span_start)
                            overlap_end = min(bucket_end, src_node.time_span_end)
                            if overlap_end > overlap_start:
                                overlap_days = (overlap_end - overlap_start).days
                                demand_qty = daily_qty * Decimal(str(overlap_days))
                                # Supply anchor date: bucket_start for pre-computed overlap
                                demand_events.append((bucket_start, demand_qty))
                            continue
                    demand_events.append((demand_date, src_node.quantity))

        # ------------------------------------------------------------------
        # 4. Compute
        # ------------------------------------------------------------------
        result = self._kernel.compute_pi_node(
            opening_stock=opening_stock,
            supply_events=supply_events,
            demand_events=demand_events,
            bucket_start=bucket_start,
            bucket_end=bucket_end,
        )

        # ------------------------------------------------------------------
        # 5. Check if changed
        # ------------------------------------------------------------------
        old_values = (
            node.opening_stock,
            node.inflows,
            node.outflows,
            node.closing_stock,
            node.has_shortage,
            node.shortage_qty,
        )
        new_values = (
            result["opening_stock"],
            result["inflows"],
            result["outflows"],
            result["closing_stock"],
            result["has_shortage"],
            result["shortage_qty"],
        )
        changed = old_values != new_values

        # ------------------------------------------------------------------
        # 6. Persist via GraphStore (all DB writes go through the store layer)
        # ------------------------------------------------------------------
        self._store.update_pi_result(
            node_id=node_id,
            scenario_id=scenario_id,
            calc_run_id=calc_run_id,
            opening_stock=result["opening_stock"],
            inflows=result["inflows"],
            outflows=result["outflows"],
            closing_stock=result["closing_stock"],
            has_shortage=result["has_shortage"],
            shortage_qty=result["shortage_qty"],
        )

        # ------------------------------------------------------------------
        # 7. Explainability (Sprint M3) — inline causal chain generation
        # ------------------------------------------------------------------
        if result["has_shortage"] and self._explanation_builder is not None:
            # Reload node to get freshly persisted shortage fields
            fresh_node = self._store.get_node(node_id, scenario_id)
            if fresh_node is not None:
                try:
                    explanation = self._explanation_builder.build_pi_explanation(
                        pi_node=fresh_node,
                        calc_run_id=calc_run_id,
                        store=self._store,
                        db=db,
                    )
                    self._explanation_builder.persist(explanation, db)
                except Exception as exc:  # noqa: BLE001
                    # Explanation failure must never break the propagation pipeline
                    logger.warning(
                        "ExplanationBuilder failed for node %s: %s",
                        node_id,
                        exc,
                    )

        # ------------------------------------------------------------------
        # 8. Shortage Detection (Sprint M4) — detect and persist shortage records
        # ------------------------------------------------------------------
        if self._shortage_detector is not None:
            try:
                # Reload node to get freshly persisted state
                fresh_node = self._store.get_node(node_id, scenario_id)
                if fresh_node is not None:
                    shortage = self._shortage_detector.detect(
                        pi_node=fresh_node,
                        calc_run_id=calc_run_id,
                        scenario_id=scenario_id,
                        db=db,
                    )
                    if shortage is not None:
                        self._shortage_detector.persist(shortage, db)
            except Exception as exc:  # noqa: BLE001
                # Shortage detection failure must never break the propagation pipeline
                logger.warning(
                    "ShortageDetector failed for node %s: %s",
                    node_id,
                    exc,
                )

        return changed
