"""
store.py — PostgreSQL-backed node and edge persistence.

All writes go through this class — never raw SQL elsewhere in the kernel.
This is the only file in kernel/ that touches the database.
Designed as a clean interface for a future Rust replacement.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import psycopg

from ootils_core.models import (
    CycleDetectedError,
    Edge,
    Node,
    ProjectionSeries,
)


class GraphStore:
    """
    PostgreSQL-backed node and edge persistence.
    All writes go through this class — never raw SQL elsewhere in the kernel.

    conn must be a psycopg3 Connection (or compatible) with dict_row factory.
    The store does NOT manage transactions — callers own commit/rollback.
    """

    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    # Node reads
    # ------------------------------------------------------------------

    def get_node(
        self,
        node_id: UUID,
        scenario_id: UUID,
        for_update: bool = False,
    ) -> Optional[Node]:
        """Fetch a single node by ID and scenario. Returns None if not found.

        Args:
            for_update: If True, appends FOR UPDATE to lock the row until the
                current transaction commits. Use this during allocation to
                prevent concurrent runners from reading stale closing_stock
                and double-deducting supply (#154).
        """
        lock_clause = " FOR UPDATE" if for_update else ""
        row = self._conn.execute(
            f"""
            SELECT * FROM nodes
            WHERE node_id = %s AND scenario_id = %s AND active = TRUE
            {lock_clause}
            """,
            (node_id, scenario_id),
        ).fetchone()
        return _row_to_node(row) if row else None

    def get_nodes_by_series(self, series_id: UUID) -> list[Node]:
        """Return all PI nodes in a projection series, ordered by bucket_sequence."""
        rows = self._conn.execute(
            """
            SELECT * FROM nodes
            WHERE projection_series_id = %s AND active = TRUE
            ORDER BY bucket_sequence ASC, node_id ASC
            """,
            (series_id,),
        ).fetchall()
        return [_row_to_node(r) for r in rows]

    def get_pi_nodes_for_item_location_in_window(
        self,
        item_id: UUID,
        location_id: UUID,
        scenario_id: UUID,
        window_start: date,
        window_end: date,
    ) -> list[Node]:
        """Return active PI buckets for an item/location intersecting a time window."""
        rows = self._conn.execute(
            """
            SELECT * FROM nodes
            WHERE scenario_id = %s
              AND item_id = %s
              AND location_id = %s
              AND node_type = 'ProjectedInventory'
              AND active = TRUE
              AND time_span_start IS NOT NULL
              AND time_span_start >= %s
              AND time_span_start < %s
            ORDER BY bucket_sequence ASC, node_id ASC
            """,
            (scenario_id, item_id, location_id, window_start, window_end),
        ).fetchall()
        return [_row_to_node(r) for r in rows]

    def get_all_nodes(self, scenario_id: UUID) -> list[Node]:
        """Return all active nodes for a scenario (used by traversal)."""
        rows = self._conn.execute(
            """
            SELECT * FROM nodes
            WHERE scenario_id = %s AND active = TRUE
            ORDER BY node_id ASC
            """,
            (scenario_id,),
        ).fetchall()
        return [_row_to_node(r) for r in rows]

    # ------------------------------------------------------------------
    # Edge reads
    # ------------------------------------------------------------------

    def get_edges_from(
        self,
        node_id: UUID,
        scenario_id: UUID,
        edge_type: Optional[str] = None,
    ) -> list[Edge]:
        """Return edges where from_node_id = node_id."""
        if edge_type:
            rows = self._conn.execute(
                """
                SELECT * FROM edges
                WHERE from_node_id = %s AND scenario_id = %s
                  AND edge_type = %s AND active = TRUE
                ORDER BY priority ASC, edge_id ASC
                """,
                (node_id, scenario_id, edge_type),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT * FROM edges
                WHERE from_node_id = %s AND scenario_id = %s AND active = TRUE
                ORDER BY priority ASC, edge_id ASC
                """,
                (node_id, scenario_id),
            ).fetchall()
        return [_row_to_edge(r) for r in rows]

    def get_edges_to(
        self,
        node_id: UUID,
        scenario_id: UUID,
        edge_type: Optional[str] = None,
    ) -> list[Edge]:
        """Return edges where to_node_id = node_id."""
        if edge_type:
            rows = self._conn.execute(
                """
                SELECT * FROM edges
                WHERE to_node_id = %s AND scenario_id = %s
                  AND edge_type = %s AND active = TRUE
                ORDER BY priority ASC, edge_id ASC
                """,
                (node_id, scenario_id, edge_type),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT * FROM edges
                WHERE to_node_id = %s AND scenario_id = %s AND active = TRUE
                ORDER BY priority ASC, edge_id ASC
                """,
                (node_id, scenario_id),
            ).fetchall()
        return [_row_to_edge(r) for r in rows]

    def get_all_edges(self, scenario_id: UUID) -> list[Edge]:
        """Return all active edges for a scenario (used by traversal)."""
        rows = self._conn.execute(
            """
            SELECT * FROM edges
            WHERE scenario_id = %s AND active = TRUE
            ORDER BY edge_id ASC
            """,
            (scenario_id,),
        ).fetchall()
        return [_row_to_edge(r) for r in rows]

    # ------------------------------------------------------------------
    # Node writes
    # ------------------------------------------------------------------

    def upsert_node(self, node: Node) -> Node:
        """
        Insert or update a node. Uses ON CONFLICT (node_id) DO UPDATE.
        Updates updated_at and all mutable fields.
        Returns the node with persisted state.
        """
        self._conn.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                quantity, qty_uom,
                time_grain, time_ref, time_span_start, time_span_end,
                is_dirty, last_calc_run_id, active,
                projection_series_id, bucket_sequence,
                opening_stock, inflows, outflows, closing_stock,
                has_shortage, shortage_qty,
                has_exact_date_inputs, has_week_inputs, has_month_inputs,
                created_at, updated_at
            ) VALUES (
                %(node_id)s, %(node_type)s, %(scenario_id)s, %(item_id)s, %(location_id)s,
                %(quantity)s, %(qty_uom)s,
                %(time_grain)s, %(time_ref)s, %(time_span_start)s, %(time_span_end)s,
                %(is_dirty)s, %(last_calc_run_id)s, %(active)s,
                %(projection_series_id)s, %(bucket_sequence)s,
                %(opening_stock)s, %(inflows)s, %(outflows)s, %(closing_stock)s,
                %(has_shortage)s, %(shortage_qty)s,
                %(has_exact_date_inputs)s, %(has_week_inputs)s, %(has_month_inputs)s,
                %(created_at)s, now()
            )
            ON CONFLICT (node_id) DO UPDATE SET
                quantity             = EXCLUDED.quantity,
                qty_uom              = EXCLUDED.qty_uom,
                time_grain           = EXCLUDED.time_grain,
                time_ref             = EXCLUDED.time_ref,
                time_span_start      = EXCLUDED.time_span_start,
                time_span_end        = EXCLUDED.time_span_end,
                is_dirty             = EXCLUDED.is_dirty,
                last_calc_run_id     = EXCLUDED.last_calc_run_id,
                active               = EXCLUDED.active,
                projection_series_id = EXCLUDED.projection_series_id,
                bucket_sequence      = EXCLUDED.bucket_sequence,
                opening_stock        = EXCLUDED.opening_stock,
                inflows              = EXCLUDED.inflows,
                outflows             = EXCLUDED.outflows,
                closing_stock        = EXCLUDED.closing_stock,
                has_shortage         = EXCLUDED.has_shortage,
                shortage_qty         = EXCLUDED.shortage_qty,
                has_exact_date_inputs = EXCLUDED.has_exact_date_inputs,
                has_week_inputs      = EXCLUDED.has_week_inputs,
                has_month_inputs     = EXCLUDED.has_month_inputs,
                updated_at           = now()
            """,
            _node_to_params(node),
        )
        return node

    # ------------------------------------------------------------------
    # Edge writes (with cycle detection)
    # ------------------------------------------------------------------

    def insert_edge(self, edge: Edge) -> Edge:
        """
        Insert a new edge after validating no cycle is introduced.
        Raises CycleDetectedError if the edge would create a cycle.
        """
        self.validate_no_cycle(edge.from_node_id, edge.to_node_id, edge.scenario_id)
        self._conn.execute(
            """
            INSERT INTO edges (
                edge_id, edge_type, from_node_id, to_node_id, scenario_id,
                priority, weight_ratio, effective_start, effective_end,
                active, created_at
            ) VALUES (
                %(edge_id)s, %(edge_type)s, %(from_node_id)s, %(to_node_id)s, %(scenario_id)s,
                %(priority)s, %(weight_ratio)s, %(effective_start)s, %(effective_end)s,
                %(active)s, %(created_at)s
            )
            """,
            _edge_to_params(edge),
        )
        return edge

    def validate_no_cycle(
        self,
        from_id: UUID,
        to_id: UUID,
        scenario_id: UUID,
    ) -> None:
        """
        DFS-based cycle detection: raises CycleDetectedError if adding
        edge (from_id → to_id) would create a cycle.

        Algorithm: starting from to_id, do a DFS following outbound edges.
        If we can reach from_id, a cycle would be created.
        """
        # Build adjacency map for this scenario (active edges only)
        rows = self._conn.execute(
            """
            SELECT from_node_id, to_node_id FROM edges
            WHERE scenario_id = %s AND active = TRUE
            """,
            (scenario_id,),
        ).fetchall()

        adjacency: dict[UUID, list[UUID]] = {}
        for r in rows:
            fn = UUID(str(r["from_node_id"]))
            tn = UUID(str(r["to_node_id"]))
            adjacency.setdefault(fn, []).append(tn)

        # DFS from to_id: if we reach from_id, it's a cycle
        visited: set[UUID] = set()
        stack: list[UUID] = [to_id]

        while stack:
            current = stack.pop()
            if current == from_id:
                raise CycleDetectedError(from_id, to_id, scenario_id)
            if current in visited:
                continue
            visited.add(current)
            for neighbour in adjacency.get(current, []):
                if neighbour not in visited:
                    stack.append(neighbour)

    def update_pi_result(
        self,
        node_id: UUID,
        scenario_id: UUID,
        calc_run_id: UUID,
        opening_stock: Decimal,
        inflows: Decimal,
        outflows: Decimal,
        closing_stock: Decimal,
        has_shortage: bool,
        shortage_qty: Decimal,
    ) -> None:
        """
        Persist the result of a PI node computation.
        Updates only the computation result fields + clears is_dirty.
        Called exclusively by the propagation engine — keeps all DB writes in the store.
        """
        from datetime import datetime, timezone
        self._conn.execute(
            """
            UPDATE nodes
            SET opening_stock    = %s,
                inflows          = %s,
                outflows         = %s,
                closing_stock    = %s,
                has_shortage     = %s,
                shortage_qty     = %s,
                is_dirty         = FALSE,
                last_calc_run_id = %s,
                updated_at       = %s
            WHERE node_id = %s AND scenario_id = %s
            """,
            (
                opening_stock,
                inflows,
                outflows,
                closing_stock,
                has_shortage,
                shortage_qty,
                calc_run_id,
                datetime.now(timezone.utc),
                node_id,
                scenario_id,
            ),
        )

    # ------------------------------------------------------------------
    # Projection series
    # ------------------------------------------------------------------

    def get_projection_series(
        self,
        item_id: UUID,
        location_id: UUID,
        scenario_id: UUID,
    ) -> Optional[ProjectionSeries]:
        """Return the projection series for (item, location, scenario), or None."""
        row = self._conn.execute(
            """
            SELECT * FROM projection_series
            WHERE item_id = %s AND location_id = %s AND scenario_id = %s
            """,
            (item_id, location_id, scenario_id),
        ).fetchone()
        return _row_to_series(row) if row else None

    def create_projection_series(
        self,
        item_id: UUID,
        location_id: UUID,
        scenario_id: UUID,
        horizon_start: date,
        horizon_end: date,
    ) -> ProjectionSeries:
        """
        Create a new ProjectionSeries.
        Raises if one already exists for (item, location, scenario).
        """
        series = ProjectionSeries(
            series_id=uuid4(),
            item_id=item_id,
            location_id=location_id,
            scenario_id=scenario_id,
            horizon_start=horizon_start,
            horizon_end=horizon_end,
        )
        self._conn.execute(
            """
            INSERT INTO projection_series (
                series_id, item_id, location_id, scenario_id,
                horizon_start, horizon_end, created_at, updated_at
            ) VALUES (
                %(series_id)s, %(item_id)s, %(location_id)s, %(scenario_id)s,
                %(horizon_start)s, %(horizon_end)s, %(created_at)s, %(updated_at)s
            )
            """,
            {
                "series_id": series.series_id,
                "item_id": series.item_id,
                "location_id": series.location_id,
                "scenario_id": series.scenario_id,
                "horizon_start": series.horizon_start,
                "horizon_end": series.horizon_end,
                "created_at": series.created_at,
                "updated_at": series.updated_at,
            },
        )
        return series

    # ------------------------------------------------------------------
    # Allocation-specific reads / writes
    # ------------------------------------------------------------------

    # Demand node types recognised by the allocation engine.
    DEMAND_NODE_TYPES: tuple[str, ...] = (
        "ForecastDemand",
        "CustomerOrderDemand",
        "DependentDemand",
    )

    def get_demand_nodes(
        self,
        scenario_id: UUID,
        *,
        node_types: Optional[tuple[str, ...]] = None,
    ) -> list[Node]:
        """
        Return all active demand nodes for a scenario, ordered by
        (priority ASC, time_ref ASC, node_id ASC) for deterministic allocation.

        priority is stored in the *quantity* field for demand nodes when no
        dedicated column exists, but the canonical sort key comes from the
        edges that carry a priority value.  Here we sort on the node's own
        ``time_ref`` (due date) and ``node_id`` as the stable tiebreaker.
        A separate ``priority`` column is not present on Node, so callers
        should sort by (node.quantity representing priority, time_ref, node_id)
        — but this method returns them pre-sorted by (time_ref ASC, node_id ASC)
        so the allocation engine can apply its own priority layer on top.

        We also expose a ``node_types`` override so tests can inject custom
        type names.
        """
        types = node_types if node_types is not None else self.DEMAND_NODE_TYPES
        placeholders = ", ".join(["%s"] * len(types))
        rows = self._conn.execute(
            f"""
            SELECT * FROM nodes
            WHERE scenario_id = %s
              AND node_type IN ({placeholders})
              AND active = TRUE
            ORDER BY time_ref ASC NULLS LAST, node_id ASC
            """,
            (scenario_id, *types),
        ).fetchall()
        return [_row_to_node(r) for r in rows]

    def get_edges_by_type(
        self,
        scenario_id: UUID,
        edge_type: str,
    ) -> list[Edge]:
        """Return all active edges of a given type for a scenario."""
        rows = self._conn.execute(
            """
            SELECT * FROM edges
            WHERE scenario_id = %s AND edge_type = %s AND active = TRUE
            ORDER BY priority ASC, edge_id ASC
            """,
            (scenario_id, edge_type),
        ).fetchall()
        return [_row_to_edge(r) for r in rows]

    def upsert_edge(self, edge: Edge) -> tuple[Edge, bool]:
        """
        Insert or update an edge identified by
        (from_node_id, to_node_id, edge_type, scenario_id).

        Returns (edge, created) where created=True when a new row was inserted.

        pegged_to edges are exempt from cycle checks because they point from
        demand to supply, the reverse of the computation DAG. All other edge
        types must pass cycle validation before insert.
        """
        existing = self._conn.execute(
            """
            SELECT edge_id FROM edges
            WHERE from_node_id = %s
              AND to_node_id   = %s
              AND edge_type    = %s
              AND scenario_id  = %s
            """,
            (edge.from_node_id, edge.to_node_id, edge.edge_type, edge.scenario_id),
        ).fetchone()

        if existing:
            # Update weight_ratio (allocated qty) and priority.
            self._conn.execute(
                """
                UPDATE edges
                SET weight_ratio = %s,
                    priority     = %s,
                    active       = TRUE
                WHERE from_node_id = %s
                  AND to_node_id   = %s
                  AND edge_type    = %s
                  AND scenario_id  = %s
                """,
                (
                    edge.weight_ratio,
                    edge.priority,
                    edge.from_node_id,
                    edge.to_node_id,
                    edge.edge_type,
                    edge.scenario_id,
                ),
            )
            edge.edge_id = UUID(str(existing["edge_id"]))
            return edge, False
        else:
            if edge.edge_type != "pegged_to":
                self.validate_no_cycle(edge.from_node_id, edge.to_node_id, edge.scenario_id)
            self._conn.execute(
                """
                INSERT INTO edges (
                    edge_id, edge_type, from_node_id, to_node_id, scenario_id,
                    priority, weight_ratio, effective_start, effective_end,
                    active, created_at
                ) VALUES (
                    %(edge_id)s, %(edge_type)s, %(from_node_id)s, %(to_node_id)s, %(scenario_id)s,
                    %(priority)s, %(weight_ratio)s, %(effective_start)s, %(effective_end)s,
                    %(active)s, %(created_at)s
                )
                """,
                _edge_to_params(edge),
            )
            return edge, True

    def update_node_closing_stock(
        self,
        node_id: UUID,
        scenario_id: UUID,
        closing_stock: Decimal,
    ) -> None:
        """
        Persist an updated closing_stock value on a PI node after allocation
        has consumed from it.  Also clears is_dirty so downstream propagation
        knows this node is fresh.
        """
        from datetime import datetime, timezone
        self._conn.execute(
            """
            UPDATE nodes
            SET closing_stock = %s,
                is_dirty      = FALSE,
                updated_at    = %s
            WHERE node_id = %s AND scenario_id = %s
            """,
            (closing_stock, datetime.now(timezone.utc), node_id, scenario_id),
        )

    def get_or_create_projection_series(
        self,
        item_id: UUID,
        location_id: UUID,
        scenario_id: UUID,
        horizon_start: date,
        horizon_end: date,
    ) -> ProjectionSeries:
        """Get existing or create new projection series."""
        existing = self.get_projection_series(item_id, location_id, scenario_id)
        if existing:
            return existing
        return self.create_projection_series(
            item_id, location_id, scenario_id, horizon_start, horizon_end
        )


# ------------------------------------------------------------------
# Row → domain model helpers
# ------------------------------------------------------------------


def _row_to_node(row: dict) -> Node:
    """Convert a DB row dict to a Node dataclass."""
    return Node(
        node_id=UUID(str(row["node_id"])),
        node_type=row["node_type"],
        scenario_id=UUID(str(row["scenario_id"])),
        item_id=UUID(str(row["item_id"])) if row.get("item_id") else None,
        location_id=UUID(str(row["location_id"])) if row.get("location_id") else None,
        quantity=Decimal(str(row["quantity"])) if row.get("quantity") is not None else None,
        qty_uom=row.get("qty_uom"),
        time_grain=row.get("time_grain"),
        time_ref=row.get("time_ref"),
        time_span_start=row.get("time_span_start"),
        time_span_end=row.get("time_span_end"),
        is_dirty=bool(row.get("is_dirty", False)),
        last_calc_run_id=UUID(str(row["last_calc_run_id"])) if row.get("last_calc_run_id") else None,
        active=bool(row.get("active", True)),
        projection_series_id=UUID(str(row["projection_series_id"])) if row.get("projection_series_id") else None,
        bucket_sequence=row.get("bucket_sequence"),
        opening_stock=Decimal(str(row["opening_stock"])) if row.get("opening_stock") is not None else None,
        inflows=Decimal(str(row["inflows"])) if row.get("inflows") is not None else None,
        outflows=Decimal(str(row["outflows"])) if row.get("outflows") is not None else None,
        closing_stock=Decimal(str(row["closing_stock"])) if row.get("closing_stock") is not None else None,
        has_shortage=bool(row.get("has_shortage", False)),
        shortage_qty=Decimal(str(row.get("shortage_qty") or "0")),
        has_exact_date_inputs=bool(row.get("has_exact_date_inputs", False)),
        has_week_inputs=bool(row.get("has_week_inputs", False)),
        has_month_inputs=bool(row.get("has_month_inputs", False)),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


def _node_to_params(node: Node) -> dict:
    """Convert a Node to a psycopg parameter dict."""
    return {
        "node_id": node.node_id,
        "node_type": node.node_type,
        "scenario_id": node.scenario_id,
        "item_id": node.item_id,
        "location_id": node.location_id,
        "quantity": node.quantity,
        "qty_uom": node.qty_uom,
        "time_grain": node.time_grain,
        "time_ref": node.time_ref,
        "time_span_start": node.time_span_start,
        "time_span_end": node.time_span_end,
        "is_dirty": node.is_dirty,
        "last_calc_run_id": node.last_calc_run_id,
        "active": node.active,
        "projection_series_id": node.projection_series_id,
        "bucket_sequence": node.bucket_sequence,
        "opening_stock": node.opening_stock,
        "inflows": node.inflows,
        "outflows": node.outflows,
        "closing_stock": node.closing_stock,
        "has_shortage": node.has_shortage,
        "shortage_qty": node.shortage_qty,
        "has_exact_date_inputs": node.has_exact_date_inputs,
        "has_week_inputs": node.has_week_inputs,
        "has_month_inputs": node.has_month_inputs,
        "created_at": node.created_at,
    }


def _row_to_edge(row: dict) -> Edge:
    """Convert a DB row dict to an Edge dataclass."""
    return Edge(
        edge_id=UUID(str(row["edge_id"])),
        edge_type=row["edge_type"],
        from_node_id=UUID(str(row["from_node_id"])),
        to_node_id=UUID(str(row["to_node_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        priority=int(row.get("priority", 0)),
        weight_ratio=Decimal(str(row.get("weight_ratio") or "1.0")),
        effective_start=row.get("effective_start"),
        effective_end=row.get("effective_end"),
        active=bool(row.get("active", True)),
        created_at=row.get("created_at"),
    )


def _edge_to_params(edge: Edge) -> dict:
    """Convert an Edge to a psycopg parameter dict."""
    return {
        "edge_id": edge.edge_id,
        "edge_type": edge.edge_type,
        "from_node_id": edge.from_node_id,
        "to_node_id": edge.to_node_id,
        "scenario_id": edge.scenario_id,
        "priority": edge.priority,
        "weight_ratio": edge.weight_ratio,
        "effective_start": edge.effective_start,
        "effective_end": edge.effective_end,
        "active": edge.active,
        "created_at": edge.created_at,
    }


def _row_to_series(row: dict) -> ProjectionSeries:
    """Convert a DB row dict to a ProjectionSeries dataclass."""
    return ProjectionSeries(
        series_id=UUID(str(row["series_id"])),
        item_id=UUID(str(row["item_id"])),
        location_id=UUID(str(row["location_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        horizon_start=row["horizon_start"],
        horizon_end=row["horizon_end"],
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )
