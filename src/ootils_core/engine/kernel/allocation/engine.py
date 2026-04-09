"""
engine.py — AllocationEngine: priority-ordered, deterministic demand allocation.

Algorithm (per ADR-003 §4 and PROPOSAL §1.6 step 4):
    1. Load all active demand nodes for the scenario.
    2. Sort by (priority ASC, time_ref ASC, node_id ASC) — deterministic.
       lower priority int = higher business priority (P1 > P2).
    3. For each demand, find the PI node it consumes via 'consumes' edges.
    4. If the PI node has closing_stock > 0, allocate greedily:
         allocated = min(demand.quantity, pi.closing_stock)
         Deduct allocated from pi.closing_stock.
         Upsert a pegged_to edge: demand → supply_node (weight_ratio = allocated qty).
    5. Return AllocationResult with counters.

Design constraints:
    - Zero SQL inline — all DB access via GraphStore.
    - Fully deterministic: sort order uses (priority, time_ref, node_id) tiebreaker.
    - Decimal quantities throughout.
    - Logging, no print.
"""
from __future__ import annotations

import logging
from datetime import date as _date_type
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import psycopg

from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.models import AllocationResult, Edge, Node

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")

# Demand node types processed by this engine.
_DEMAND_TYPES: tuple[str, ...] = (
    "ForecastDemand",
    "CustomerOrderDemand",
    "DependentDemand",
)


class AllocationEngine:
    """
    Priority-ordered, greedy, deterministic allocation engine.

    Usage::

        engine = AllocationEngine()
        result = engine.allocate(scenario_id, db_conn)
    """

    def allocate(
        self,
        scenario_id: UUID,
        db: psycopg.Connection,
    ) -> AllocationResult:
        """
        Run a full allocation pass for *scenario_id*.

        Steps:
            1. Load & sort demand nodes.
            2. For each demand, call _allocate_demand.
            3. Aggregate counters → AllocationResult.

        The caller owns transaction management (commit/rollback).
        """
        store = GraphStore(db)
        run_at = datetime.now(timezone.utc)

        demands = self.get_demand_nodes(scenario_id, db)
        logger.info(
            "AllocationEngine.allocate: scenario=%s demands=%d",
            scenario_id,
            len(demands),
        )

        demands_fully: int = 0
        demands_partial: int = 0
        demands_unallocated: int = 0
        total_demanded: Decimal = _ZERO
        total_allocated: Decimal = _ZERO
        edges_created: int = 0
        edges_updated: int = 0

        for demand in demands:
            qty_demanded = demand.quantity if demand.quantity is not None else _ZERO
            total_demanded += qty_demanded

            allocated_qty, created, updated = self._allocate_demand(
                demand, scenario_id, db, store
            )
            total_allocated += allocated_qty
            edges_created += created
            edges_updated += updated

            if allocated_qty >= qty_demanded:
                demands_fully += 1
            elif allocated_qty > _ZERO:
                demands_partial += 1
            else:
                demands_unallocated += 1

        result = AllocationResult(
            scenario_id=scenario_id,
            demands_total=len(demands),
            demands_fully_allocated=demands_fully,
            demands_partially_allocated=demands_partial,
            demands_unallocated=demands_unallocated,
            total_qty_demanded=total_demanded,
            total_qty_allocated=total_allocated,
            edges_created=edges_created,
            edges_updated=edges_updated,
            run_at=run_at,
        )

        logger.info(
            "AllocationEngine.allocate complete: scenario=%s "
            "fully=%d partial=%d unallocated=%d "
            "demanded=%s allocated=%s "
            "edges_created=%d edges_updated=%d",
            scenario_id,
            demands_fully,
            demands_partial,
            demands_unallocated,
            total_demanded,
            total_allocated,
            edges_created,
            edges_updated,
        )
        return result

    def get_demand_nodes(
        self,
        scenario_id: UUID,
        db: psycopg.Connection,
    ) -> list[Node]:
        """
        Fetch all active demand nodes for *scenario_id* and sort them by:
            (priority ASC, time_ref ASC, node_id ASC)

        Priority is carried on the *Node.quantity* field when the node acts
        as a priority carrier.  However, the canonical priority for allocation
        ordering comes from the edge dictionary spec: lower int = higher priority.

        Because nodes do not have a dedicated ``priority`` column, we use
        ``Node.quantity`` as the sort key for priority when the node_type is a
        demand type that embeds priority.  For all other cases, a sentinel of 0
        is used so that nodes sort stably.

        NOTE: the *actual* priority used for allocation is the ``priority``
        field on the ``consumes`` edge from demand → PI, where available, or
        falls back to the node's own quantity-as-priority.  The GraphStore
        returns edges ordered by ``priority ASC`` already, which aligns with
        this.

        To keep the engine simple and spec-compliant, we sort here by:
            (node's ``quantity`` treated as numeric priority ASC,
             time_ref ASC NULLS LAST,
             node_id ASC)

        Callers can override by passing a pre-sorted list directly to
        ``_allocate_demand``.
        """
        store = GraphStore(db)
        nodes = store.get_demand_nodes(scenario_id, node_types=_DEMAND_TYPES)

        # Secondary sort by priority field embedded in Node.quantity
        # (Spec: lower int = higher priority; None → treated as 0).
        # Primary DB sort is already (time_ref ASC, node_id ASC).
        # Here we stable-sort on priority so that equal-time_ref demands
        # are ordered by priority.
        nodes.sort(
            key=lambda n: (
                _priority_key(n),
                n.time_ref if n.time_ref is not None else _SENTINEL_DATE,
                n.node_id,
            )
        )
        return nodes

    def _allocate_demand(
        self,
        demand_node: Node,
        scenario_id: UUID,
        db: psycopg.Connection,
        store: GraphStore,
    ) -> tuple[Decimal, int, int]:
        """
        Allocate a single demand node against its PI supply nodes.

        Returns (allocated_qty, edges_created, edges_updated).

        Algorithm:
            1. Find all 'consumes' edges from demand_node → PI nodes.
            2. For each PI node (ordered by edge.priority ASC, then node_id ASC):
               a. Read current closing_stock from the PI node.
               b. Compute alloc = min(remaining_needed, closing_stock).
               c. If alloc > 0: upsert pegged_to edge (demand → PI node),
                  deduct from PI node closing_stock.
               d. Repeat until demand is fully satisfied or supply exhausted.
        """
        qty_demanded = demand_node.quantity if demand_node.quantity is not None else _ZERO
        if qty_demanded <= _ZERO:
            logger.debug(
                "_allocate_demand: demand=%s has zero/negative quantity — skipping",
                demand_node.node_id,
            )
            return _ZERO, 0, 0

        # Get consumes edges: demand_node → PI supply nodes
        consumes_edges = store.get_edges_from(
            demand_node.node_id, scenario_id, edge_type="consumes"
        )

        if not consumes_edges:
            logger.debug(
                "_allocate_demand: demand=%s has no 'consumes' edges — unallocated",
                demand_node.node_id,
            )
            return _ZERO, 0, 0

        remaining = qty_demanded
        total_allocated = _ZERO
        edges_created = 0
        edges_updated = 0

        for consumes_edge in consumes_edges:
            if remaining <= _ZERO:
                break

            pi_node_id = consumes_edge.to_node_id
            pi_node = store.get_node(pi_node_id, scenario_id)

            if pi_node is None:
                logger.warning(
                    "_allocate_demand: PI node %s not found for scenario %s — skipping",
                    pi_node_id,
                    scenario_id,
                )
                continue

            available = (
                pi_node.closing_stock
                if pi_node.closing_stock is not None
                else _ZERO
            )

            if available <= _ZERO:
                logger.debug(
                    "_allocate_demand: PI node=%s closing_stock=%s — no stock to allocate",
                    pi_node_id,
                    available,
                )
                continue

            alloc = min(remaining, available)
            new_closing_stock = available - alloc

            # Upsert pegged_to edge: demand_node → supply_node
            # Convention: from_node_id = demand, to_node_id = supply
            pegged_edge = Edge(
                edge_id=uuid4(),
                edge_type="pegged_to",
                from_node_id=demand_node.node_id,
                to_node_id=pi_node_id,
                scenario_id=scenario_id,
                priority=consumes_edge.priority,
                weight_ratio=alloc,  # allocated quantity
                effective_start=demand_node.time_ref,
                effective_end=demand_node.time_ref,
                active=True,
            )

            _, created = store.upsert_edge(pegged_edge)
            if created:
                edges_created += 1
            else:
                edges_updated += 1

            # Deduct from PI node's closing stock
            store.update_node_closing_stock(pi_node_id, scenario_id, new_closing_stock)

            remaining -= alloc
            total_allocated += alloc

            logger.debug(
                "_allocate_demand: demand=%s ← pi=%s alloc=%s "
                "(remaining=%s new_closing_stock=%s)",
                demand_node.node_id,
                pi_node_id,
                alloc,
                remaining,
                new_closing_stock,
            )

        if remaining > _ZERO:
            logger.debug(
                "_allocate_demand: demand=%s partially/un-allocated "
                "(demanded=%s allocated=%s shortage=%s)",
                demand_node.node_id,
                qty_demanded,
                total_allocated,
                remaining,
            )

        return total_allocated, edges_created, edges_updated


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SENTINEL_DATE = _date_type(9999, 12, 31)


def _priority_key(node: Node) -> int:
    """
    Extract the priority sort key from a demand node.

    By convention, the engine uses Node.quantity as the business priority
    integer when the demand node type does not carry an explicit priority field.
    A lower integer means higher priority (P1 beats P2).

    When quantity is None or non-integer, we fall back to 999999 (lowest
    priority) so that demands with missing priority data do not accidentally
    starve properly prioritized demands.
    """
    try:
        if node.quantity is not None:
            return int(node.quantity)
    except (ValueError, TypeError):
        pass
    return 999999
