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

import psycopg

from ootils_core.models import CalcRun, Node, Scenario
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
        db: psycopg.Connection,
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
                db.execute("UPDATE events SET processed = TRUE WHERE event_id = %s", (event_id,))
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

            # For dated item/location changes, also dirty the PI buckets in the
            # affected window directly. This covers moves where an edge was
            # rewired from an old bucket to a new bucket before propagation,
            # so the old bucket is no longer reachable from the trigger node
            # via current outbound edges.
            trigger_node = self._store.get_node(trigger_node_id, scenario_id)
            if (
                isinstance(trigger_node, Node)
                and trigger_node.item_id is not None
                and trigger_node.location_id is not None
                and (old_date is not None or new_date is not None)
            ):
                impacted_pi_nodes = self._store.get_pi_nodes_for_item_location_in_window(
                    item_id=trigger_node.item_id,
                    location_id=trigger_node.location_id,
                    scenario_id=scenario_id,
                    window_start=window_start,
                    window_end=window_end,
                )
                dirty_node_ids.update(node.node_id for node in impacted_pi_nodes)

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

    def _finish_run(self, calc_run: CalcRun, scenario_id: UUID, db: psycopg.Connection) -> None:
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

        # Mark all events in this calc run as processed
        if calc_run.triggered_by_event_ids:
            db.execute(
                "UPDATE events SET processed = TRUE WHERE event_id = ANY(%s)",
                (list(calc_run.triggered_by_event_ids),),
            )

        # Resolve stale shortages: any shortage from a previous calc_run
        # that was NOT re-detected in this run should be marked resolved.
        if self._shortage_detector is not None:
            try:
                resolved = self._shortage_detector.resolve_stale(
                    scenario_id=scenario_id,
                    calc_run_id=calc_run.calc_run_id,
                    db=db,
                )
                if resolved > 0:
                    logger.info(
                        "_finish_run: resolved %d stale shortages for scenario=%s",
                        resolved, scenario_id,
                    )
            except Exception as exc:
                logger.warning(
                    "_finish_run: resolve_stale failed for scenario=%s: %s",
                    scenario_id, exc,
                )

    # ------------------------------------------------------------------
    # Propagation internals
    # ------------------------------------------------------------------

    def _propagate(
        self,
        calc_run: CalcRun,
        dirty_nodes: set[UUID],
        db: psycopg.Connection,
    ) -> None:
        """
        Topological sort over dirty nodes, then compute each one in order.
        After each computation, cascade to dependents if the result changed.

        Performance contract: pre-loads the dirty set's nodes + incoming
        edges + edge-source nodes in 4 batch queries up front (was ~10
        queries/node before — see REVIEW-2026-05 R2 / SCALABILITY.md BP#1).
        Per-node compute then reads from `cache` dictionaries instead of
        the store.
        """
        if not dirty_nodes:
            return

        scenario_id = calc_run.scenario_id

        # Topological sort of dirty set
        ordered = self._traversal.topological_sort(dirty_nodes, scenario_id)

        # ------------------------------------------------------------------
        # PRE-LOAD: 4 batch queries instead of ~10 per node.
        # ------------------------------------------------------------------
        dirty_list = list(dirty_nodes)
        # 1. All dirty nodes themselves.
        nodes_cache: dict[UUID, Optional[Node]] = dict(
            self._store.get_nodes_by_ids(dirty_list, scenario_id)
        )
        # 2. All incoming edges for the dirty set (feeds_forward, replenishes, consumes).
        edges_by_target: dict[UUID, dict[str, list]] = {nid: {} for nid in dirty_list}
        all_incoming = self._store.get_edges_to_nodes(
            dirty_list, scenario_id,
            edge_types=["feeds_forward", "replenishes", "consumes"],
        )
        for target_id, edges in all_incoming.items():
            for edge in edges:
                edges_by_target.setdefault(target_id, {}).setdefault(edge.edge_type, []).append(edge)
        # 3. All source nodes referenced by those edges.
        source_node_ids = {
            edge.from_node_id
            for edges in all_incoming.values()
            for edge in edges
        }
        # Some predecessors may already be in nodes_cache (e.g. PI[t-1] when both
        # buckets are dirty). Skip those to avoid a redundant query.
        missing_source_ids = [nid for nid in source_node_ids if nid not in nodes_cache]
        if missing_source_ids:
            nodes_cache.update(self._store.get_nodes_by_ids(missing_source_ids, scenario_id))

        # 4. Pre-load all safety-stock parameters for the (item, location) pairs
        # touched by the dirty PI set — drops `_get_safety_stock` from "1 query
        # per PI" to "0 queries per PI" in the common case.
        safety_stock_cache: dict[tuple[UUID, Optional[UUID]], Decimal] = {}
        # Unit costs for severity valuation (#342). Mirrors mrp_core.cost_of
        # — the single valuation precedence used by the watcher fleet:
        # negotiated supplier unit_cost first (preferred supplier, cheapest
        # priced row), then items.standard_cost (migration 042, BOM roll-up).
        # Unpriced items fall back to the detector's proxy of 1 so severity
        # never silently collapses to zero.
        unit_cost_cache: dict[UUID, Decimal] = {}
        if self._shortage_detector is not None:
            pi_pairs = {
                (n.item_id, n.location_id)
                for n in nodes_cache.values()
                if n is not None and n.node_type == "ProjectedInventory" and n.item_id is not None
            }
            if pi_pairs:
                item_ids = list({pair[0] for pair in pi_pairs})
                rows = db.execute(
                    """
                    SELECT item_id, location_id, safety_stock_qty
                    FROM item_planning_params
                    WHERE item_id = ANY(%s)
                      AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
                    """,
                    (item_ids,),
                ).fetchall()
                for r in rows:
                    if r["safety_stock_qty"] is None:
                        continue
                    key = (UUID(str(r["item_id"])), UUID(str(r["location_id"])) if r["location_id"] else None)
                    safety_stock_cache[key] = Decimal(str(r["safety_stock_qty"]))

                cost_rows = db.execute(
                    """
                    SELECT i.item_id,
                           COALESCE(si.unit_cost, i.standard_cost) AS unit_cost
                    FROM items i
                    LEFT JOIN LATERAL (
                        SELECT unit_cost FROM supplier_items
                        WHERE item_id = i.item_id
                          AND unit_cost IS NOT NULL AND unit_cost > 0
                        ORDER BY is_preferred DESC, unit_cost ASC
                        LIMIT 1
                    ) si ON TRUE
                    WHERE i.item_id = ANY(%s)
                    """,
                    (item_ids,),
                ).fetchall()
                for r in cost_rows:
                    if r["unit_cost"] is None:
                        continue
                    unit_cost_cache[UUID(str(r["item_id"]))] = Decimal(str(r["unit_cost"]))

        # Remaining dirty set (may shrink as we process)
        remaining_dirty = set(dirty_nodes)

        # Accumulators for batched writes — one round-trip at the end of the
        # loop instead of two per node. See REVIEW-2026-05 R2 Tier 2.
        pending_updates: list[tuple] = []
        cleared_dirty: list[UUID] = []

        for node_id in ordered:
            if node_id not in remaining_dirty:
                continue  # Already cleared (e.g., cascaded out of window)

            node = nodes_cache.get(node_id)
            if node is None:
                cleared_dirty.append(node_id)
                remaining_dirty.discard(node_id)
                continue

            if node.node_type == "ProjectedInventory":
                changed = self._recompute_pi_node(
                    node_id=node_id,
                    scenario_id=scenario_id,
                    calc_run_id=calc_run.calc_run_id,
                    db=db,
                    nodes_cache=nodes_cache,
                    edges_by_target=edges_by_target,
                    safety_stock_cache=safety_stock_cache,
                    unit_cost_cache=unit_cost_cache,
                    pending_updates=pending_updates,
                )
                if changed:
                    calc_run.nodes_recalculated += 1
                else:
                    calc_run.nodes_unchanged += 1
            else:
                # Non-PI nodes don't need kernel computation — just mark as processed
                calc_run.nodes_unchanged += 1

            cleared_dirty.append(node_id)
            remaining_dirty.discard(node_id)

        # ------------------------------------------------------------------
        # FLUSH: one UPDATE…FROM(VALUES…) for results, one DELETE…ANY(…) for
        # dirty-flag cleanup. Replaces N round-trips with 2.
        # ------------------------------------------------------------------
        if pending_updates:
            self._store.update_pi_results_batch(pending_updates)
        if cleared_dirty:
            self._dirty.clear_dirty_batch(cleared_dirty, scenario_id, calc_run.calc_run_id, db)

    def _recompute_pi_node(
        self,
        node_id: UUID,
        scenario_id: UUID,
        calc_run_id: UUID,
        db: psycopg.Connection,
        nodes_cache: Optional[dict[UUID, Optional[Node]]] = None,
        edges_by_target: Optional[dict[UUID, dict[str, list]]] = None,
        safety_stock_cache: Optional[dict[tuple[UUID, Optional[UUID]], Decimal]] = None,
        unit_cost_cache: Optional[dict[UUID, Decimal]] = None,
        pending_updates: Optional[list[tuple]] = None,
    ) -> bool:
        """
        Recompute a single PI node.

        Loads: opening stock (from predecessor PI node or on-hand supply),
               supply events (PO/WO nodes connected via 'replenishes' edges),
               demand events (forecast/customer order nodes via 'consumes' edges).

        Calls kernel.compute_pi_node, persists the result.

        Returns True if the result changed (triggers cascade), False if unchanged.

        When called from `_propagate`, `nodes_cache` and `edges_by_target` are
        the batch-loaded dicts; reads then hit memory instead of the DB. When
        called standalone (e.g. tests), the parameters default to None and we
        fall back to per-call store lookups — fully backwards compatible.
        """
        # ------------------------------------------------------------------
        # Helpers: resolve node and incoming edges via cache when available.
        # ------------------------------------------------------------------
        def _get_node(nid: UUID) -> Optional[Node]:
            if nodes_cache is not None and nid in nodes_cache:
                return nodes_cache[nid]
            return self._store.get_node(nid, scenario_id)

        def _get_edges_to(nid: UUID, edge_type: str) -> list:
            if edges_by_target is not None:
                return list(edges_by_target.get(nid, {}).get(edge_type, []))
            return self._store.get_edges_to(nid, scenario_id, edge_type=edge_type)

        node = _get_node(node_id)
        if node is None or node.node_type != "ProjectedInventory":
            return False

        bucket_start = node.time_span_start
        # bucket_end is EXCLUSIVE — consistent with ProjectionKernel.apply_contribution_rule
        # and ADR-002d (bucket boundary = start of next bucket).
        # Events on bucket_end itself belong to the next bucket, not this one (fix for #159).
        bucket_end = node.time_span_end

        if bucket_start is None or bucket_end is None:
            return False

        # ------------------------------------------------------------------
        # 1. Opening stock: closing_stock of the predecessor PI node in series
        # ------------------------------------------------------------------
        opening_stock = Decimal("0")

        # Find predecessor via 'feeds_forward' edge (to_node_id = this node)
        pred_edges = _get_edges_to(node_id, "feeds_forward")
        if pred_edges:
            pred_node = _get_node(pred_edges[0].from_node_id)
            if pred_node and pred_node.closing_stock is not None:
                opening_stock = pred_node.closing_stock
        else:
            # First bucket — find on-hand supply nodes for this item/location
            # OnHandSupply nodes connect via 'replenishes' edge too, but at bucket 0
            # Check for any on-hand node feeding into this PI node
            oh_edges = _get_edges_to(node_id, "replenishes")
            for edge in oh_edges:
                src_node = _get_node(edge.from_node_id)
                if src_node and src_node.node_type == "OnHandSupply":
                    opening_stock += src_node.quantity or Decimal("0")

        # ------------------------------------------------------------------
        # 2. Supply events: nodes connected via 'replenishes' to this PI node
        # NOTE: OnHandSupply is excluded here — it is already captured as
        # opening_stock in block 1. Including it again would double-count it
        # as both opening_stock AND an inflow, producing incorrect projections.
        # ------------------------------------------------------------------
        supply_events: list = []
        replenish_edges = _get_edges_to(node_id, "replenishes")
        for edge in replenish_edges:
            src_node = _get_node(edge.from_node_id)
            if src_node is None:
                continue
            if src_node.node_type in ("PurchaseOrderSupply", "WorkOrderSupply",
                                       "TransferSupply", "PlannedSupply"):
                # Use time_ref as the supply date
                if src_node.time_ref is not None and src_node.quantity is not None:
                    supply_events.append((src_node.time_ref, src_node.quantity))
            # OnHandSupply is intentionally skipped — handled as opening_stock above

        # ------------------------------------------------------------------
        # 3. Demand events: nodes connected via 'consumes' to this PI node
        # ------------------------------------------------------------------
        demand_events: list = []
        consume_edges = _get_edges_to(node_id, "consumes")
        for edge in consume_edges:
            src_node = _get_node(edge.from_node_id)
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
                            # Both bucket_end and time_span_end are exclusive —
                            # overlap is [overlap_start, overlap_end) days.
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
        # When called from the batched _propagate loop, pending_updates is a
        # list — accumulate and let the caller flush in one UPDATE…FROM(VALUES).
        # When called standalone (tests, legacy paths), pending_updates is None
        # → write immediately to preserve the original contract.
        # ------------------------------------------------------------------
        if pending_updates is not None:
            pending_updates.append((
                node_id, scenario_id, calc_run_id,
                result["opening_stock"], result["inflows"], result["outflows"],
                result["closing_stock"], result["has_shortage"], result["shortage_qty"],
            ))
        else:
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
                # The persisted state mirrors `result` for the computed PI fields.
                # Mutate the in-memory node with those values instead of reloading
                # from the DB (saves one query per PI node — see REVIEW-2026-05 R2).
                node.opening_stock = result["opening_stock"]
                node.inflows = result["inflows"]
                node.outflows = result["outflows"]
                node.closing_stock = result["closing_stock"]
                node.has_shortage = result["has_shortage"]
                node.shortage_qty = result["shortage_qty"]

                if safety_stock_cache is not None and node.item_id is not None:
                    safety_stock = (
                        safety_stock_cache.get((node.item_id, node.location_id))
                        or safety_stock_cache.get((node.item_id, None))
                    )
                else:
                    safety_stock = self._get_safety_stock(node, db)
                # Severity valuation (#342): items.standard_cost when known,
                # detector proxy (1) otherwise. Standalone calls (no cache)
                # keep the proxy — cost lookup is a batch concern.
                unit_cost = (
                    unit_cost_cache.get(node.item_id)
                    if unit_cost_cache is not None and node.item_id is not None
                    else None
                )
                shortage = self._shortage_detector.detect_with_params(
                    pi_node=node,
                    calc_run_id=calc_run_id,
                    scenario_id=scenario_id,
                    db=db,
                    safety_stock_qty=safety_stock,
                    unit_cost=unit_cost,
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

    def _get_safety_stock(self, node: Node, db: psycopg.Connection) -> Optional[Decimal]:
        """Fetch safety_stock_qty from item_planning_params for this node's item/location."""
        if node.item_id is None:
            return None
        try:
            row = db.execute(
                """
                SELECT safety_stock_qty FROM item_planning_params
                WHERE item_id = %s
                  AND (location_id = %s OR location_id IS NULL)
                  AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
                ORDER BY location_id NULLS LAST
                LIMIT 1
                """,
                (node.item_id, node.location_id),
            ).fetchone()
            if row and row["safety_stock_qty"] is not None:
                return Decimal(str(row["safety_stock_qty"]))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "_get_safety_stock: failed for node %s item=%s: %s",
                node.node_id,
                node.item_id,
                exc,
            )
        return None
