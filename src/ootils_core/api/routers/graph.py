"""
GET /v1/graph — Return the planning graph for an item/location.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.db.types import DictRowConnection
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
def get_graph(
    item_id: str = Query(..., description="Item UUID or name"),
    location_id: str = Query(..., description="Location UUID or name"),
    depth: int = Query(default=2, ge=1, le=5, description="Graph traversal depth"),
    from_date: Optional[date] = Query(default=None, alias="from"),
    to_date: Optional[date] = Query(default=None, alias="to"),
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> GraphResponse:
    """Return nodes and edges for the planning graph at (item, location, scenario)."""
    # Resolve item UUID — accept UUID, name, or external_id (aligned with projection.py)
    try:
        item_uuid = UUID(item_id)
    except ValueError:
        row = db.execute(
            "SELECT item_id FROM items WHERE name = %s OR external_id = %s LIMIT 1",
            (item_id, item_id),
        ).fetchone()
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Item '{item_id}' not found"
            )
        item_uuid = UUID(str(row["item_id"]))

    # Resolve location UUID — accept UUID, name, or external_id
    try:
        location_uuid = UUID(location_id)
    except ValueError:
        row = db.execute(
            "SELECT location_id FROM locations WHERE name = %s OR external_id = %s LIMIT 1",
            (location_id, location_id),
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
def list_nodes(
    item_id: Optional[UUID] = Query(default=None),
    location_id: Optional[UUID] = Query(default=None),
    node_type: Optional[str] = Query(default=None),
    scenario_id: Optional[UUID] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: DictRowConnection = Depends(get_db),
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


# ---------------------------------------------------------------------------
# POST /v1/nodes/{node_id}/firm, DELETE /v1/nodes/{node_id}/firm — FPO lifecycle
#
# Firm Planned Order (FPO, migration 061 nodes.is_firm, #346): firming a
# PlannedSupply excludes it from the MRP full-regeneration purge
# (engine/mrp/graph_integration.py:cleanup_previous_run) and nets it as a
# CLOSED/committed receipt in both MRP engines (gross_to_net.py /
# engine/mrp/loader.py), so it stops being re-planned. Un-firming reverts it
# to an ordinary re-generatable planned order.
#
# Decision Ladder: firming/un-firming is a deliberate planner (or governed
# agent) act on a single order, not an irreversible commitment (it can be
# reversed by DELETE) — no approval gate for V1, audited via `events` is
# sufficient. Revisit if firm/unfirm ever needs L2+ governance.
# ---------------------------------------------------------------------------

class FirmRequest(BaseModel):
    """Body of POST /v1/nodes/{node_id}/firm.

    Identity of the actor. The current auth layer doesn't extract subjects
    from the bearer token (single shared token, see api/auth.py), so callers
    pass it explicitly for now — same carve-out as staging.py's
    approved_by/rejected_by.
    """
    actor: str = Field(..., min_length=1, max_length=200)


class FirmResponse(BaseModel):
    node_id: UUID
    is_firm: bool
    scenario_id: UUID
    item_id: Optional[UUID]
    location_id: Optional[UUID]


def _set_node_firm(
    db: DictRowConnection,
    node_id: UUID,
    target_is_firm: bool,
    actor: str,
) -> FirmResponse:
    """Shared implementation for firm/unfirm — validates the node, applies
    the flag, and emits the audit event. Raises HTTPException on validation
    failure."""
    row = db.execute(
        """
        SELECT node_id, node_type, scenario_id, item_id, location_id, is_firm
        FROM nodes
        WHERE node_id = %s AND active = TRUE
        """,
        (node_id,),
    ).fetchone()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Node {node_id} not found (or inactive)",
        )

    if row["node_type"] != "PlannedSupply":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Node {node_id} is a '{row['node_type']}', not a "
                "'PlannedSupply' — only a PlannedSupply can be firmed/unfirmed "
                "into a Firm Planned Order (#346)."
            ),
        )

    previous_is_firm = bool(row["is_firm"])
    scenario_id = UUID(str(row["scenario_id"]))

    db.execute(
        "UPDATE nodes SET is_firm = %s, updated_at = NOW() WHERE node_id = %s",
        (target_is_firm, node_id),
    )

    # Streamable + auditable principle: emit an event so agents/operators
    # can subscribe to FPO lifecycle changes instead of polling. No-op flips
    # (already in the target state) still emit — the caller's intent and
    # actor are worth recording even if the flag didn't change.
    event_id = uuid4()
    now = datetime.now(timezone.utc)
    db.execute(
        """
        INSERT INTO events (
            event_id, event_type, scenario_id,
            trigger_node_id, field_changed, old_text, new_text,
            processed, source, user_ref, created_at
        ) VALUES (
            %s, 'node_firm_changed', %s,
            %s, 'is_firm', %s, %s,
            TRUE, 'api', %s, %s
        )
        """,
        (
            event_id,
            scenario_id,
            node_id,
            str(previous_is_firm).lower(),
            str(target_is_firm).lower(),
            actor,
            now,
        ),
    )

    logger.info(
        "node.firm_changed node_id=%s scenario=%s %s->%s actor=%s event_id=%s",
        node_id,
        scenario_id,
        previous_is_firm,
        target_is_firm,
        actor,
        event_id,
    )

    return FirmResponse(
        node_id=node_id,
        is_firm=target_is_firm,
        scenario_id=scenario_id,
        item_id=UUID(str(row["item_id"])) if row["item_id"] else None,
        location_id=UUID(str(row["location_id"])) if row["location_id"] else None,
    )


@nodes_router.post("/{node_id}/firm", response_model=FirmResponse)
def firm_node(
    node_id: UUID,
    body: FirmRequest,
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> FirmResponse:
    """Firm a PlannedSupply into a Firm Planned Order (is_firm=TRUE, #346).

    A firmed PlannedSupply survives the next MRP full-regeneration purge and
    is netted as a closed/committed receipt by both MRP engines — it is no
    longer re-planned, but stays re-datable by a RESCHEDULE_IN/OUT message.
    """
    return _set_node_firm(db, node_id, target_is_firm=True, actor=body.actor)


@nodes_router.delete("/{node_id}/firm", response_model=FirmResponse)
def unfirm_node(
    node_id: UUID,
    actor: str = Query(..., min_length=1, max_length=200),
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> FirmResponse:
    """Un-firm a Firm Planned Order back into an ordinary PlannedSupply
    (is_firm=FALSE, #346).

    The order becomes re-generatable again: the next MRP full-regeneration
    purge will deactivate it and, if its demand is still open, replace it.

    `actor` is a query parameter (not a body) — DELETE requests in this
    repo don't carry a body (see param_overrides.py's
    clear_param_override_endpoint).
    """
    return _set_node_firm(db, node_id, target_is_firm=False, actor=actor)
