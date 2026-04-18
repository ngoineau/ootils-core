"""
GET /v1/graph — Return the planning graph for an item/location.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.engine.kernel.graph.store import GraphStore

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/graph", tags=["graph"])


class NodeOut(BaseModel):
    node_id: UUID
    node_type: str
    quantity: Optional[Decimal]
    time_ref: Optional[date]
    time_span_start: Optional[date]
    time_span_end: Optional[date]
    time_grain: Optional[str]
    has_shortage: bool
    shortage_qty: Decimal
    closing_stock: Optional[Decimal]


class EdgeOut(BaseModel):
    edge_id: UUID
    edge_type: str
    from_node_id: UUID
    to_node_id: UUID
    priority: int
    weight_ratio: Decimal


class GraphResponse(BaseModel):
    nodes: list[NodeOut]
    edges: list[EdgeOut]
    item_id: str
    location_id: str
    scenario_id: UUID
    depth: int


@router.get("", response_model=GraphResponse)
async def get_graph(
    item_id: str = Query(..., description="Item UUID or name"),
    location_id: str = Query(..., description="Location UUID or name"),
    depth: int = Query(default=2, ge=1, le=5, description="Graph traversal depth"),
    from_date: Optional[date] = Query(default=None, alias="from"),
    to_date: Optional[date] = Query(default=None, alias="to"),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> GraphResponse:
    """Return nodes and edges for the planning graph at (item, location, scenario)."""
    # Resolve item UUID
    try:
        item_uuid = UUID(item_id)
    except ValueError:
        row = db.execute(
            "SELECT item_id FROM items WHERE name = %s LIMIT 1", (item_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Item '{item_id}' not found"
            )
        item_uuid = UUID(str(row["item_id"]))

    # Resolve location UUID
    try:
        location_uuid = UUID(location_id)
    except ValueError:
        row = db.execute(
            "SELECT location_id FROM locations WHERE name = %s LIMIT 1", (location_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Location '{location_id}' not found",
            )
        location_uuid = UUID(str(row["location_id"]))

    store = GraphStore(db)

    # Fetch all nodes for this scenario scoped to item/location
    all_nodes = db.execute(
        """
        SELECT * FROM nodes
        WHERE scenario_id = %s
          AND item_id = %s
          AND location_id = %s
          AND active = TRUE
        ORDER BY node_id ASC
        """,
        (scenario_id, item_uuid, location_uuid),
    ).fetchall()

    from ootils_core.engine.kernel.graph.store import _row_to_node

    nodes = [_row_to_node(r) for r in all_nodes]

    # Time window filter if provided
    if from_date or to_date:
        filtered = []
        for n in nodes:
            node_date = n.time_span_start or n.time_ref
            if node_date is None:
                filtered.append(n)
                continue
            if from_date and node_date < from_date:
                continue
            if to_date and node_date > to_date:
                continue
            filtered.append(n)
        nodes = filtered

    # Collect all edges involving these nodes
    node_ids = {n.node_id for n in nodes}
    all_edges = store.get_all_edges(scenario_id)
    edges = [
        e for e in all_edges
        if e.from_node_id in node_ids or e.to_node_id in node_ids
    ]

    logger.info(
        "graph.fetched item=%s location=%s scenario=%s nodes=%d edges=%d",
        item_id,
        location_id,
        scenario_id,
        len(nodes),
        len(edges),
    )

    return GraphResponse(
        nodes=[
            NodeOut(
                node_id=n.node_id,
                node_type=n.node_type,
                quantity=n.quantity,
                time_ref=n.time_ref,
                time_span_start=n.time_span_start,
                time_span_end=n.time_span_end,
                time_grain=n.time_grain,
                has_shortage=n.has_shortage,
                shortage_qty=n.shortage_qty,
                closing_stock=n.closing_stock,
            )
            for n in nodes
        ],
        edges=[
            EdgeOut(
                edge_id=e.edge_id,
                edge_type=e.edge_type,
                from_node_id=e.from_node_id,
                to_node_id=e.to_node_id,
                priority=e.priority,
                weight_ratio=e.weight_ratio,
            )
            for e in edges
        ],
        item_id=item_id,
        location_id=location_id,
        scenario_id=scenario_id,
        depth=depth,
    )


# ---------------------------------------------------------------------------
# GET /v1/nodes — List nodes for UI dropdowns
# ---------------------------------------------------------------------------

class NodeListItem(BaseModel):
    node_id: UUID
    node_type: str
    item_id: Optional[UUID]
    location_id: Optional[UUID]
    scenario_id: UUID
    time_ref: Optional[date]
    qty: Optional[Decimal]
    item_code: Optional[str] = None
    location_code: Optional[str] = None


class NodeListResponse(BaseModel):
    nodes: list[NodeListItem]
    total: int


nodes_router = APIRouter(prefix="/v1/nodes", tags=["nodes"])


@nodes_router.get("", response_model=NodeListResponse)
async def list_nodes(
    item_id: Optional[UUID] = Query(default=None),
    location_id: Optional[UUID] = Query(default=None),
    node_type: Optional[str] = Query(default=None),
    scenario_id: Optional[UUID] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> NodeListResponse:
    """List nodes with optional filters — used for UI dropdowns (e.g. Simulate page)."""
    effective_scenario_id = scenario_id or UUID("00000000-0000-0000-0000-000000000001")

    conditions = ["n.scenario_id = %s", "n.active = TRUE"]
    params: list = [effective_scenario_id]

    if item_id is not None:
        conditions.append("n.item_id = %s")
        params.append(item_id)

    if location_id is not None:
        conditions.append("n.location_id = %s")
        params.append(location_id)

    if node_type is not None:
        conditions.append("n.node_type = %s")
        params.append(node_type)

    where_clause = " AND ".join(conditions)

    # Count total
    count_row = db.execute(
        f"SELECT COUNT(*) AS cnt FROM nodes n WHERE {where_clause}",
        params,
    ).fetchone()
    total = int(count_row["cnt"]) if count_row else 0

    # Fetch with item_code + location_code via LEFT JOIN
    params_with_limit = params + [limit]
    rows = db.execute(
        f"""
        SELECT
            n.node_id,
            n.node_type,
            n.item_id,
            n.location_id,
            n.scenario_id,
            n.time_ref,
            n.quantity AS qty,
            i.name    AS item_code,
            l.name    AS location_code
        FROM nodes n
        LEFT JOIN items     i ON i.item_id     = n.item_id
        LEFT JOIN locations l ON l.location_id = n.location_id
        WHERE {where_clause}
        ORDER BY n.node_type ASC, n.time_ref ASC NULLS LAST
        LIMIT %s
        """,
        params_with_limit,
    ).fetchall()

    nodes = [
        NodeListItem(
            node_id=UUID(str(row["node_id"])),
            node_type=row["node_type"],
            item_id=UUID(str(row["item_id"])) if row["item_id"] else None,
            location_id=UUID(str(row["location_id"])) if row["location_id"] else None,
            scenario_id=UUID(str(row["scenario_id"])),
            time_ref=row["time_ref"],
            qty=row["qty"],
            item_code=row["item_code"],
            location_code=row["location_code"],
        )
        for row in rows
    ]

    logger.info(
        "nodes.list scenario=%s item=%s location=%s type=%s total=%d returned=%d",
        effective_scenario_id,
        item_id,
        location_id,
        node_type,
        total,
        len(nodes),
    )

    return NodeListResponse(nodes=nodes, total=total)
