"""
BOM (Bill of Materials) router — MRP-ready endpoints.

Endpoints:
  POST /v1/ingest/bom          — Import a complete BOM for an item
  GET  /v1/bom/{external_id}   — Retrieve active BOM for an item
  POST /v1/bom/explode         — MRP explosion: compute gross/net requirements
"""
from __future__ import annotations

import logging
from collections import defaultdict, deque
from datetime import date
from typing import Optional
from uuid import UUID, uuid4

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["bom"])


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

class BOMComponentInput(BaseModel):
    component_external_id: str = Field(..., description="Component external_id.")
    quantity_per: float = Field(..., gt=0, description="Component quantity per 1 unit of parent (> 0).")
    uom: str = Field("EA", description="Component unit of measure.")
    scrap_factor: float = Field(default=0.0, ge=0.0, lt=1.0, description="Scrap rate [0.0–1.0). E.g. 0.05 = 5% waste.")


class IngestBOMRequest(BaseModel):
    parent_external_id: str = Field(..., description="Parent item external_id (manufactured item).")
    bom_version: str = Field("1.0", description="BOM version (e.g. '1.0', '2.1').")
    effective_from: date = Field(default_factory=date.today, description="BOM effective date.")
    components: list[BOMComponentInput] = Field(..., description="BOM component list.")
    dry_run: bool = Field(False, description="If true, validation only — no DB writes.")


class IngestBOMResponse(BaseModel):
    status: str
    bom_id: Optional[str] = None
    parent_item_id: Optional[str] = None
    components_imported: int
    llc_updated: int


class BOMComponentOutput(BaseModel):
    component_external_id: str
    component_item_id: str
    quantity_per: float
    uom: str
    scrap_factor: float
    llc: int


class BOMResponse(BaseModel):
    parent_external_id: str
    parent_item_id: str
    bom_version: str
    effective_from: date
    components: list[BOMComponentOutput]


class ExplodeRequest(BaseModel):
    item_external_id: str = Field(..., description="Item to explode.")
    quantity: float = Field(..., gt=0, description="Quantity to produce (> 0).")
    location_external_id: Optional[str] = Field(None, description="Production site. Optional.")
    explosion_date: Optional[date] = Field(None, description="Reference date for explosion. Defaults to today.")
    levels: int = Field(default=10, ge=1, le=20, description="Maximum explosion depth [1–20]. Default = 10.")


class ExplodeLineOutput(BaseModel):
    level: int
    component_external_id: str
    component_item_id: str
    gross_requirement: float
    on_hand_qty: float
    net_requirement: float
    has_shortage: bool


class ExplodeResponse(BaseModel):
    parent_external_id: str
    quantity: float
    explosion: list[ExplodeLineOutput]
    total_components: int
    components_with_shortage: int


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _resolve_item_id(db: psycopg.Connection, external_id: str) -> UUID | None:
    """Resolve external_id → item_id. Returns None if not found."""
    row = db.execute(
        "SELECT item_id FROM items WHERE external_id = %s AND status != 'obsolete'",
        (external_id,),
    ).fetchone()
    return row["item_id"] if row else None


def _get_active_bom(db: psycopg.Connection, parent_item_id: UUID) -> dict | None:
    """Return the active bom_header for a given parent_item_id."""
    row = db.execute(
        """
        SELECT bom_id, bom_version, effective_from
        FROM bom_headers
        WHERE parent_item_id = %s AND status = 'active'
        ORDER BY effective_from DESC
        LIMIT 1
        """,
        (parent_item_id,),
    ).fetchone()
    return dict(row) if row else None


def _get_bom_lines(db: psycopg.Connection, bom_id: UUID) -> list[dict]:
    """Return all active bom_lines for a given bom_id, with external_id for each component."""
    rows = db.execute(
        """
        SELECT bl.line_id, bl.component_item_id, bl.quantity_per, bl.uom,
               bl.scrap_factor, bl.llc, i.external_id AS component_external_id
        FROM bom_lines bl
        JOIN items i ON i.item_id = bl.component_item_id
        WHERE bl.bom_id = %s
          AND bl.active = TRUE
        """,
        (bom_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _get_on_hand_qty(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID | None,
) -> float:
    """Return available OnHandSupply quantity for item × location (or 0 if none)."""
    if location_id is None:
        # No location filter: sum all on-hand for item in baseline
        row = db.execute(
            """
            SELECT COALESCE(SUM(quantity), 0) AS qty
            FROM nodes
            WHERE node_type = 'OnHandSupply'
              AND item_id = %s
              AND scenario_id = %s
              AND active = TRUE
            """,
            (item_id, BASELINE_SCENARIO_ID),
        ).fetchone()
    else:
        row = db.execute(
            """
            SELECT COALESCE(SUM(quantity), 0) AS qty
            FROM nodes
            WHERE node_type = 'OnHandSupply'
              AND item_id = %s
              AND location_id = %s
              AND scenario_id = %s
              AND active = TRUE
            """,
            (item_id, location_id, BASELINE_SCENARIO_ID),
        ).fetchone()
    return float(row["qty"]) if row else 0.0


def _detect_cycle(
    db: psycopg.Connection,
    parent_item_id: UUID,
    new_component_ids: list[UUID],
) -> bool:
    """
    DFS cycle detection: return True if adding new_component_ids as children
    of parent_item_id would create a cycle.

    A cycle exists if parent_item_id is reachable from any of new_component_ids
    through the existing BOM graph.
    """
    # Build ancestor set: all items that are ancestors of parent_item_id
    # (i.e., items for which parent_item_id is a descendant)
    # We need to check: is parent_item_id in the subtree of any new_component?
    # Equivalently: does any new_component eventually lead to parent_item_id?

    # Get all BOM edges: (parent_item_id → component_item_id)
    all_edges = db.execute(
        """
        SELECT bh.parent_item_id, bl.component_item_id
        FROM bom_headers bh
        JOIN bom_lines bl ON bl.bom_id = bh.bom_id
        WHERE bh.status = 'active'
          AND bl.active = TRUE
        """
    ).fetchall()

    # Build adjacency: parent → [children]
    children_map: dict[UUID, list[UUID]] = defaultdict(list)
    for edge in all_edges:
        children_map[edge["parent_item_id"]].append(edge["component_item_id"])

    # Add the new edges temporarily
    for comp_id in new_component_ids:
        children_map[parent_item_id].append(comp_id)

    # DFS from parent_item_id — if we reach parent_item_id again, cycle detected
    # More precisely: DFS from parent, check if we can reach parent again
    # Actually: DFS from each new_component → check if we reach parent_item_id
    visited: set[UUID] = set()
    stack = list(new_component_ids)
    while stack:
        node = stack.pop()
        if node == parent_item_id:
            return True
        if node in visited:
            continue
        visited.add(node)
        for child in children_map.get(node, []):
            if child not in visited:
                stack.append(child)
    return False


def _recalculate_llc(db: psycopg.Connection, affected_item_ids: list[UUID]) -> int:
    """
    Recalculate Low-Level Code (LLC) for all components in the BOM graph.
    LLC = deepest level at which an item appears across all BOMs.
    Returns count of updated lines.

    Algorithm: BFS from all roots (items with no parent), assign LLC = max depth.
    """
    # Build full BOM graph (parent → children with bom_line refs)
    all_edges = db.execute(
        """
        SELECT bh.parent_item_id, bl.component_item_id, bl.line_id
        FROM bom_headers bh
        JOIN bom_lines bl ON bl.bom_id = bh.bom_id
        WHERE bh.status = 'active'
          AND bl.active = TRUE
        """
    ).fetchall()

    if not all_edges:
        return 0

    # parent → list of (component_item_id, line_id)
    children_map: dict[UUID, list[tuple[UUID, UUID]]] = defaultdict(list)
    all_parents: set[UUID] = set()
    all_components: set[UUID] = set()

    for edge in all_edges:
        p = edge["parent_item_id"]
        c = edge["component_item_id"]
        lid = edge["line_id"]
        children_map[p].append((c, lid))
        all_parents.add(p)
        all_components.add(c)

    # Roots = parents that are not components of any other item
    roots = all_parents - all_components

    # BFS: track maximum depth for each item
    max_depth: dict[UUID, int] = defaultdict(int)

    queue: deque[tuple[UUID, int]] = deque()
    for root in roots:
        queue.append((root, 0))

    visited_at: dict[UUID, int] = {}

    while queue:
        item_id, depth = queue.popleft()
        # Update max depth for this item
        if max_depth[item_id] < depth:
            max_depth[item_id] = depth
        for child_id, line_id in children_map.get(item_id, []):
            child_depth = depth + 1
            # Always process to ensure max depth propagation
            if visited_at.get(child_id, -1) < child_depth:
                visited_at[child_id] = child_depth
                if max_depth[child_id] < child_depth:
                    max_depth[child_id] = child_depth
                queue.append((child_id, child_depth))

    # Build (line_id → llc) from max_depth of component
    line_to_llc: list[tuple[int, UUID]] = []
    for edge in all_edges:
        c = edge["component_item_id"]
        lid = edge["line_id"]
        llc_val = max_depth.get(c, 0)
        line_to_llc.append((llc_val, lid))

    if not line_to_llc:
        return 0

    # Batch update
    with db.cursor() as cur:
        cur.executemany(
            "UPDATE bom_lines SET llc = %s WHERE line_id = %s",
            line_to_llc,
        )

    return len(line_to_llc)


# ─────────────────────────────────────────────────────────────
# POST /v1/ingest/bom
# ─────────────────────────────────────────────────────────────

@router.post("/ingest/bom", response_model=IngestBOMResponse, summary="Import BOM", description="Import a complete Bill of Materials for an item. Automatically computes Low-Level Codes (LLC).")
async def ingest_bom(
    body: IngestBOMRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> IngestBOMResponse:
    """Import a complete BOM for a parent item. Upsert with cycle detection."""

    # 1. Resolve parent
    parent_item_id = _resolve_item_id(db, body.parent_external_id)
    if parent_item_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"field": "parent_external_id", "error": f"Item '{body.parent_external_id}' not found"}],
        )

    # 2. Resolve all components
    errors: list[dict] = []
    component_ids: list[UUID] = []
    for i, comp in enumerate(body.components):
        cid = _resolve_item_id(db, comp.component_external_id)
        if cid is None:
            errors.append({
                "row": i,
                "component_external_id": comp.component_external_id,
                "error": f"Item '{comp.component_external_id}' not found",
            })
        else:
            component_ids.append(cid)

    if errors:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=errors,
        )

    # 3. Cycle detection
    if _detect_cycle(db, parent_item_id, component_ids):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"error": "BOM cycle detected — a component is an ancestor of the parent item"}],
        )

    # 4. dry_run early exit
    if body.dry_run:
        return IngestBOMResponse(
            status="dry_run",
            components_imported=len(body.components),
            llc_updated=0,
        )

    # 5. Upsert bom_headers
    existing_header = db.execute(
        "SELECT bom_id FROM bom_headers WHERE parent_item_id = %s AND bom_version = %s",
        (parent_item_id, body.bom_version),
    ).fetchone()

    if existing_header:
        bom_id: UUID = existing_header["bom_id"]
        db.execute(
            """
            UPDATE bom_headers
            SET effective_from = %s, status = 'active'
            WHERE bom_id = %s
            """,
            (body.effective_from, bom_id),
        )
    else:
        bom_id = uuid4()
        db.execute(
            """
            INSERT INTO bom_headers (bom_id, parent_item_id, bom_version, effective_from, status)
            VALUES (%s, %s, %s, %s, 'active')
            """,
            (bom_id, parent_item_id, body.bom_version, body.effective_from),
        )

    # 6. Upsert bom_lines
    for comp, comp_id in zip(body.components, component_ids):
        db.execute(
            """
            INSERT INTO bom_lines (bom_id, component_item_id, quantity_per, uom, scrap_factor, active, updated_at)
            VALUES (%s, %s, %s, %s, %s, TRUE, now())
            ON CONFLICT (bom_id, component_item_id) DO UPDATE
                SET quantity_per  = EXCLUDED.quantity_per,
                    uom           = EXCLUDED.uom,
                    scrap_factor  = EXCLUDED.scrap_factor,
                    active        = TRUE,
                    updated_at    = now()
            """,
            (bom_id, comp_id, comp.quantity_per, comp.uom, comp.scrap_factor),
        )

    # 6b. Soft-delete components removed from the payload
    if component_ids:
        db.execute(
            """
            UPDATE bom_lines
            SET active = FALSE, updated_at = now()
            WHERE bom_id = %s
              AND active = TRUE
              AND component_item_id != ALL(%s::uuid[])
            """,
            (bom_id, component_ids),
        )
    else:
        # No components in new payload: deactivate all existing lines
        db.execute(
            """
            UPDATE bom_lines
            SET active = FALSE, updated_at = now()
            WHERE bom_id = %s AND active = TRUE
            """,
            (bom_id,),
        )

    # 7. Recalculate LLC
    llc_count = _recalculate_llc(db, component_ids)

    logger.info(
        "bom.ingest parent=%s version=%s components=%d llc_updated=%d",
        body.parent_external_id, body.bom_version, len(body.components), llc_count,
    )

    return IngestBOMResponse(
        status="ok",
        bom_id=str(bom_id),
        parent_item_id=str(parent_item_id),
        components_imported=len(body.components),
        llc_updated=llc_count,
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/bom/{parent_external_id}
# ─────────────────────────────────────────────────────────────

@router.get("/bom/{parent_external_id}", response_model=BOMResponse, summary="Get BOM", description="Return the active BOM for an item with its components and LLC.")
async def get_bom(
    parent_external_id: str,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> BOMResponse:
    """Return the active BOM for a given item."""
    parent_item_id = _resolve_item_id(db, parent_external_id)
    if parent_item_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Item '{parent_external_id}' not found",
        )

    header = _get_active_bom(db, parent_item_id)
    if header is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active BOM found for '{parent_external_id}'",
        )

    lines = _get_bom_lines(db, header["bom_id"])

    return BOMResponse(
        parent_external_id=parent_external_id,
        parent_item_id=str(parent_item_id),
        bom_version=header["bom_version"],
        effective_from=header["effective_from"],
        components=[
            BOMComponentOutput(
                component_external_id=line["component_external_id"],
                component_item_id=str(line["component_item_id"]),
                quantity_per=float(line["quantity_per"]),
                uom=line["uom"],
                scrap_factor=float(line["scrap_factor"]),
                llc=line["llc"],
            )
            for line in lines
        ],
    )


# ─────────────────────────────────────────────────────────────
# POST /v1/bom/explode
# ─────────────────────────────────────────────────────────────

@router.post("/bom/explode", response_model=ExplodeResponse, summary="MRP explosion", description="Compute gross and net component requirements for a given quantity (multi-level explosion, netting against available stock).")
async def explode_bom(
    body: ExplodeRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> ExplodeResponse:
    """
    MRP explosion: compute gross and net requirements for a given quantity of a parent item.
    DFS with LLC ordering. Handles multi-level BOMs.
    """
    parent_item_id = _resolve_item_id(db, body.item_external_id)
    if parent_item_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Item '{body.item_external_id}' not found",
        )

    # Resolve location if provided
    location_id: UUID | None = None
    if body.location_external_id:
        loc_row = db.execute(
            "SELECT location_id FROM locations WHERE external_id = %s",
            (body.location_external_id,),
        ).fetchone()
        if loc_row is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Location '{body.location_external_id}' not found",
            )
        location_id = loc_row["location_id"]

    explosion_lines: list[ExplodeLineOutput] = []

    def _explode_recursive(item_id: UUID, qty: float, level: int) -> None:
        if level > body.levels:
            return

        header = _get_active_bom(db, item_id)
        if header is None:
            return  # leaf node, no BOM

        lines = _get_bom_lines(db, header["bom_id"])

        # Sort by LLC descending (process higher-level codes first)
        lines_sorted = sorted(lines, key=lambda x: x["llc"], reverse=True)

        for line in lines_sorted:
            comp_id: UUID = line["component_item_id"]
            gross_req = qty * float(line["quantity_per"]) * (1.0 + float(line["scrap_factor"]))
            on_hand = _get_on_hand_qty(db, comp_id, location_id)
            net_req = max(0.0, gross_req - on_hand)

            explosion_lines.append(
                ExplodeLineOutput(
                    level=level,
                    component_external_id=line["component_external_id"],
                    component_item_id=str(comp_id),
                    gross_requirement=round(gross_req, 6),
                    on_hand_qty=round(on_hand, 6),
                    net_requirement=round(net_req, 6),
                    has_shortage=net_req > 0,
                )
            )

            # Recurse with net_requirement as quantity for sub-components
            if net_req > 0:
                _explode_recursive(comp_id, net_req, level + 1)
            else:
                # Even if no net requirement at this level, we may still need
                # to explode sub-components if there's a multi-level assembly
                _explode_recursive(comp_id, gross_req, level + 1)

    _explode_recursive(parent_item_id, body.quantity, level=1)

    shortage_count = sum(1 for line in explosion_lines if line.has_shortage)

    return ExplodeResponse(
        parent_external_id=body.item_external_id,
        quantity=body.quantity,
        explosion=explosion_lines,
        total_components=len(explosion_lines),
        components_with_shortage=shortage_count,
    )
