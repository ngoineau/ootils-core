"""
GET /v1/projection — Get projected inventory for an item/location.
GET /v1/projection/portfolio — Multi-item shortage summary.
GET /v1/projection/pegging/{node_id} — Pegging tree traversal.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
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
router = APIRouter(prefix="/v1/projection", tags=["projection"])


# ─────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────

class SupplyDetail(BaseModel):
    node_id: UUID
    node_type: str
    quantity: Optional[Decimal]
    time_ref: Optional[date]


class DemandDetail(BaseModel):
    node_id: UUID
    node_type: str
    quantity: Optional[Decimal]
    time_ref: Optional[date]


class ProjectionBucket(BaseModel):
    bucket_sequence: Optional[int]
    time_span_start: Optional[date]
    time_span_end: Optional[date]
    time_grain: Optional[str]
    opening_stock: Optional[Decimal]
    inflows: Optional[Decimal]
    outflows: Optional[Decimal]
    closing_stock: Optional[Decimal]
    has_shortage: bool
    shortage_qty: Decimal
    safety_stock_qty: Optional[Decimal] = None
    severity_class: Optional[str] = None  # stockout | below_safety_stock | None
    supply_details: list[SupplyDetail] = []
    demand_details: list[DemandDetail] = []


class ProjectionResponse(BaseModel):
    series_id: Optional[UUID]
    item_id: str
    location_id: str
    scenario_id: UUID
    grain: Optional[str]
    safety_stock_qty: Optional[Decimal] = None
    buckets: list[ProjectionBucket]


class PortfolioItemSummary(BaseModel):
    item_id: UUID
    location_id: UUID
    item_code: Optional[str]
    location_code: Optional[str]
    series_id: UUID
    horizon_start: Optional[date]
    horizon_end: Optional[date]
    shortage_buckets: int
    total_shortage_qty: Decimal
    min_closing_stock: Optional[Decimal]


class PortfolioResponse(BaseModel):
    scenario_id: UUID
    items: list[PortfolioItemSummary]
    total: int


class PeggingNode(BaseModel):
    node_id: UUID
    node_type: str
    quantity: Optional[Decimal]
    time_ref: Optional[date]
    edge_type: str
    depth: int


class PeggingResponse(BaseModel):
    root_node_id: UUID
    pegging_tree: list[PeggingNode]


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _aggregate_buckets(buckets: list[ProjectionBucket], grain: str) -> list[ProjectionBucket]:
    """Aggregate daily PI buckets into weekly or monthly buckets."""
    if grain == "day" or not buckets:
        return buckets

    def bucket_key(b: ProjectionBucket) -> str:
        if b.time_span_start is None:
            return "unknown"
        d = b.time_span_start
        if grain == "week":
            monday = d - timedelta(days=d.weekday())
            return monday.isoformat()
        elif grain == "month":
            return f"{d.year}-{d.month:02d}-01"
        return d.isoformat()

    groups: dict[str, list[ProjectionBucket]] = defaultdict(list)
    for b in buckets:
        groups[bucket_key(b)].append(b)

    aggregated = []
    for key in sorted(groups.keys()):
        group = groups[key]
        first = group[0]
        last = group[-1]
        aggregated.append(ProjectionBucket(
            bucket_sequence=first.bucket_sequence,
            time_span_start=first.time_span_start,
            time_span_end=last.time_span_end,
            time_grain=grain,
            opening_stock=first.opening_stock,
            inflows=sum((b.inflows or Decimal(0)) for b in group),
            outflows=sum((b.outflows or Decimal(0)) for b in group),
            closing_stock=last.closing_stock,
            has_shortage=any(b.has_shortage for b in group),
            shortage_qty=sum((b.shortage_qty or Decimal(0)) for b in group),
            supply_details=[d for b in group for d in b.supply_details],
            demand_details=[d for b in group for d in b.demand_details],
        ))
    return aggregated


def _fetch_supply_demand_details(
    db: psycopg.Connection,
    node_ids: list[UUID],
    scenario_id: UUID,
) -> tuple[dict[UUID, list[SupplyDetail]], dict[UUID, list[DemandDetail]]]:
    """
    Batch-fetch supply and demand details for all PI nodes.
    Returns two dicts keyed by PI node_id.
    """
    supply_by_pi: dict[UUID, list[SupplyDetail]] = {n: [] for n in node_ids}
    demand_by_pi: dict[UUID, list[DemandDetail]] = {n: [] for n in node_ids}

    if not node_ids:
        return supply_by_pi, demand_by_pi

    supply_rows = db.execute(
        """
        SELECT e.to_node_id AS pi_id,
               n.node_id, n.node_type, n.quantity, n.time_ref
        FROM edges e
        JOIN nodes n ON n.node_id = e.from_node_id
        WHERE e.to_node_id = ANY(%s)
          AND e.edge_type = 'replenishes'
          AND e.active = TRUE
          AND n.active = TRUE
        ORDER BY n.time_ref ASC NULLS LAST
        """,
        (node_ids,),
    ).fetchall()

    for r in supply_rows:
        pi_id = UUID(str(r["pi_id"]))
        if pi_id in supply_by_pi:
            supply_by_pi[pi_id].append(SupplyDetail(
                node_id=UUID(str(r["node_id"])),
                node_type=r["node_type"],
                quantity=Decimal(str(r["quantity"])) if r["quantity"] is not None else None,
                time_ref=r["time_ref"],
            ))

    demand_rows = db.execute(
        """
        SELECT e.to_node_id AS pi_id,
               n.node_id, n.node_type, n.quantity, n.time_ref
        FROM edges e
        JOIN nodes n ON n.node_id = e.from_node_id
        WHERE e.to_node_id = ANY(%s)
          AND e.edge_type = 'consumes'
          AND e.active = TRUE
          AND n.active = TRUE
        ORDER BY n.time_ref ASC NULLS LAST
        """,
        (node_ids,),
    ).fetchall()

    for r in demand_rows:
        pi_id = UUID(str(r["pi_id"]))
        if pi_id in demand_by_pi:
            demand_by_pi[pi_id].append(DemandDetail(
                node_id=UUID(str(r["node_id"])),
                node_type=r["node_type"],
                quantity=Decimal(str(r["quantity"])) if r["quantity"] is not None else None,
                time_ref=r["time_ref"],
            ))

    return supply_by_pi, demand_by_pi


# ─────────────────────────────────────────────────────────────
# GET /v1/projection
# ─────────────────────────────────────────────────────────────

@router.get("", response_model=ProjectionResponse)
async def get_projection(
    item_id: str = Query(..., description="Item identifier (UUID or external_id)"),
    location_id: str = Query(..., description="Location identifier (UUID or external_id)"),
    grain: Optional[str] = Query(default=None, description="day / week / month — aggregates daily buckets"),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> ProjectionResponse:
    """Return projected inventory buckets for an item/location pair, with supply/demand breakdown."""
    # Resolve item_id
    try:
        item_uuid = UUID(item_id)
    except ValueError:
        row = db.execute(
            "SELECT item_id FROM items WHERE name = %s OR external_id = %s LIMIT 1",
            (item_id, item_id),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Item '{item_id}' not found")
        item_uuid = UUID(str(row["item_id"]))

    # Resolve location_id
    try:
        location_uuid = UUID(location_id)
    except ValueError:
        row = db.execute(
            "SELECT location_id FROM locations WHERE name = %s OR external_id = %s LIMIT 1",
            (location_id, location_id),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Location '{location_id}' not found")
        location_uuid = UUID(str(row["location_id"]))

    store = GraphStore(db)
    series = store.get_projection_series(item_uuid, location_uuid, scenario_id)

    if series is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No projection series found for item={item_id}, location={location_id}, scenario={scenario_id}",
        )

    nodes = store.get_nodes_by_series(series.series_id)
    node_ids = [n.node_id for n in nodes]

    # Fetch safety stock for this item/location (from item_planning_params)
    safety_stock_qty: Optional[Decimal] = None
    try:
        ss_row = db.execute(
            """
            SELECT safety_stock_qty FROM item_planning_params
            WHERE item_id = %s
              AND (location_id = %s OR location_id IS NULL)
              AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
            ORDER BY location_id NULLS LAST
            LIMIT 1
            """,
            (item_uuid, location_uuid),
        ).fetchone()
        if ss_row is not None and ss_row["safety_stock_qty"] is not None:
            safety_stock_qty = Decimal(str(ss_row["safety_stock_qty"]))
    except Exception:
        logger.warning("projection: failed to fetch safety_stock_qty for item=%s location=%s", item_uuid, location_uuid)

    def _compute_severity_class(closing: Optional[Decimal]) -> Optional[str]:
        if closing is None:
            return None
        if closing < Decimal("0"):
            return "stockout"
        if safety_stock_qty is not None and closing < safety_stock_qty:
            return "below_safety_stock"
        return None

    # Batch-fetch supply/demand details (no N+1)
    supply_by_pi, demand_by_pi = _fetch_supply_demand_details(db, node_ids, scenario_id)

    buckets = [
        ProjectionBucket(
            bucket_sequence=n.bucket_sequence,
            time_span_start=n.time_span_start,
            time_span_end=n.time_span_end,
            time_grain=n.time_grain,
            opening_stock=n.opening_stock,
            inflows=n.inflows,
            outflows=n.outflows,
            closing_stock=n.closing_stock,
            has_shortage=n.has_shortage,
            shortage_qty=n.shortage_qty,
            safety_stock_qty=safety_stock_qty,
            severity_class=_compute_severity_class(n.closing_stock),
            supply_details=supply_by_pi.get(n.node_id, []),
            demand_details=demand_by_pi.get(n.node_id, []),
        )
        for n in nodes
    ]

    # Apply grain aggregation if requested
    if grain and grain in ("week", "month"):
        buckets = _aggregate_buckets(buckets, grain)

    logger.info(
        "projection.fetched series=%s item=%s location=%s scenario=%s buckets=%d grain=%s",
        series.series_id, item_id, location_id, scenario_id, len(buckets), grain,
    )

    return ProjectionResponse(
        series_id=series.series_id,
        item_id=item_id,
        location_id=location_id,
        scenario_id=scenario_id,
        grain=grain or "day",
        safety_stock_qty=safety_stock_qty,
        buckets=buckets,
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/projection/portfolio
# ─────────────────────────────────────────────────────────────

@router.get("/portfolio", response_model=PortfolioResponse)
async def get_portfolio(
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> PortfolioResponse:
    """Return a shortage summary across all items/locations for a scenario."""
    rows = db.execute(
        """
        SELECT
            ps.series_id,
            ps.item_id,
            ps.location_id,
            ps.horizon_start,
            ps.horizon_end,
            i.external_id AS item_code,
            l.external_id AS location_code,
            COUNT(n.node_id) FILTER (WHERE n.has_shortage = TRUE) AS shortage_buckets,
            COALESCE(SUM(n.shortage_qty) FILTER (WHERE n.has_shortage = TRUE), 0) AS total_shortage_qty,
            MIN(n.closing_stock) AS min_closing_stock
        FROM projection_series ps
        JOIN nodes n ON n.projection_series_id = ps.series_id AND n.active = TRUE
        LEFT JOIN items i ON i.item_id = ps.item_id
        LEFT JOIN locations l ON l.location_id = ps.location_id
        WHERE ps.scenario_id = %s
        GROUP BY ps.series_id, ps.item_id, ps.location_id,
                 ps.horizon_start, ps.horizon_end, i.external_id, l.external_id
        ORDER BY total_shortage_qty DESC, shortage_buckets DESC
        """,
        (scenario_id,),
    ).fetchall()

    items = [
        PortfolioItemSummary(
            item_id=UUID(str(r["item_id"])),
            location_id=UUID(str(r["location_id"])),
            item_code=r.get("item_code"),
            location_code=r.get("location_code"),
            series_id=UUID(str(r["series_id"])),
            horizon_start=r.get("horizon_start"),
            horizon_end=r.get("horizon_end"),
            shortage_buckets=int(r["shortage_buckets"] or 0),
            total_shortage_qty=Decimal(str(r["total_shortage_qty"] or 0)),
            min_closing_stock=Decimal(str(r["min_closing_stock"])) if r.get("min_closing_stock") is not None else None,
        )
        for r in rows
    ]

    logger.info("portfolio.fetched scenario=%s items=%d", scenario_id, len(items))
    return PortfolioResponse(scenario_id=scenario_id, items=items, total=len(items))


# ─────────────────────────────────────────────────────────────
# GET /v1/projection/pegging/{node_id}
# ─────────────────────────────────────────────────────────────

@router.get("/pegging/{node_id}", response_model=PeggingResponse)
async def get_pegging(
    node_id: str,
    depth: int = Query(default=3, ge=1, le=5, description="Max traversal depth"),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> PeggingResponse:
    """Trace the pegging tree from a node — which demand consumed which supply."""
    try:
        node_uuid = UUID(node_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid node_id: '{node_id}'",
        )

    tree: list[PeggingNode] = []
    visited: set[UUID] = set()

    def traverse(nid: UUID, current_depth: int, edge_type_used: str = "root") -> None:
        if current_depth > depth or nid in visited:
            return
        visited.add(nid)

        node_row = db.execute(
            "SELECT node_id, node_type, quantity, time_ref FROM nodes WHERE node_id = %s",
            (nid,),
        ).fetchone()
        if not node_row:
            return

        tree.append(PeggingNode(
            node_id=UUID(str(node_row["node_id"])),
            node_type=node_row["node_type"],
            quantity=Decimal(str(node_row["quantity"])) if node_row["quantity"] is not None else None,
            time_ref=node_row["time_ref"],
            edge_type=edge_type_used,
            depth=current_depth,
        ))

        edges = db.execute(
            """
            SELECT to_node_id, edge_type FROM edges
            WHERE from_node_id = %s AND scenario_id = %s
              AND edge_type IN ('pegged_to', 'replenishes', 'consumes')
              AND active = TRUE
            """,
            (nid, scenario_id),
        ).fetchall()

        for e in edges:
            traverse(UUID(str(e["to_node_id"])), current_depth + 1, e["edge_type"])

    traverse(node_uuid, 0)

    logger.info("pegging.fetched root=%s nodes=%d depth=%d", node_id, len(tree), depth)
    return PeggingResponse(root_node_id=node_uuid, pegging_tree=tree)
