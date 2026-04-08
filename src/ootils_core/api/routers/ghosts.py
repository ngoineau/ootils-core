"""
Ghosts API router — ADR-010 V1

Endpoints:
  POST /v1/ingest/ghosts       — upsert ghost_node + members + graph node + edges
  GET  /v1/ghosts              — list all ghosts with members
  GET  /v1/ghosts/{ghost_id}   — detail: ghost + members + graph node
  POST /v1/ghosts/{ghost_id}/run — run ghost logic over a time window
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional
from uuid import UUID, uuid4

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db
from ootils_core.engine.ghost.ghost_engine import run_ghost

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ghosts"])

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")

# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

VALID_GHOST_TYPES = {"phase_transition", "capacity_aggregate"}
VALID_GHOST_STATUSES = {"active", "archived", "draft"}
VALID_ROLES = {"incoming", "outgoing", "member"}
VALID_CURVES = {"linear", "step", "sigmoid"}


class GhostMemberInput(BaseModel):
    item_id: UUID = Field(..., description="UUID of the member item.")
    role: str = Field(..., description="Role: incoming | outgoing | member.")
    transition_start_date: Optional[date] = None
    transition_end_date: Optional[date] = None
    transition_curve: str = Field("linear", description="Transition curve: linear | step | sigmoid.")
    weight_at_start: float = Field(1.0, ge=0.0, le=1.0)
    weight_at_end: float = Field(0.0, ge=0.0, le=1.0)

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        if v not in VALID_ROLES:
            raise ValueError(f"role must be one of {sorted(VALID_ROLES)}")
        return v

    @field_validator("transition_curve")
    @classmethod
    def validate_curve(cls, v: str) -> str:
        if v not in VALID_CURVES:
            raise ValueError(f"transition_curve must be one of {sorted(VALID_CURVES)}")
        return v


class IngestGhostRequest(BaseModel):
    name: str = Field(..., description="Ghost name (non-empty).")
    ghost_type: str = Field(..., description="Ghost type: phase_transition | capacity_aggregate.")
    scenario_id: Optional[UUID] = None
    resource_id: Optional[UUID] = None
    status: str = Field("active", description="Ghost status: active | archived | draft.")
    description: Optional[str] = None
    members: list[GhostMemberInput] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("name must not be empty")
        return v

    @field_validator("ghost_type")
    @classmethod
    def validate_ghost_type(cls, v: str) -> str:
        if v not in VALID_GHOST_TYPES:
            raise ValueError(f"ghost_type must be one of {sorted(VALID_GHOST_TYPES)}")
        return v

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in VALID_GHOST_STATUSES:
            raise ValueError(f"status must be one of {sorted(VALID_GHOST_STATUSES)}")
        return v


class GhostRunRequest(BaseModel):
    scenario_id: UUID
    from_date: date
    to_date: date


# ─────────────────────────────────────────────────────────────
# Membership constraint validation
# ─────────────────────────────────────────────────────────────

def _validate_membership(ghost_type: str, members: list[GhostMemberInput]) -> list[str]:
    """
    Enforce membership constraints per ADR-010 D2:
      phase_transition: exactly 1 outgoing + 1 incoming
      capacity_aggregate: N members with role='member', N >= 1
    Returns list of error messages (empty if valid).
    """
    errors: list[str] = []

    if ghost_type == "phase_transition":
        outgoing_count = sum(1 for m in members if m.role == "outgoing")
        incoming_count = sum(1 for m in members if m.role == "incoming")
        member_count = sum(1 for m in members if m.role == "member")

        if outgoing_count != 1:
            errors.append(
                f"phase_transition requires exactly 1 member with role='outgoing', got {outgoing_count}"
            )
        if incoming_count != 1:
            errors.append(
                f"phase_transition requires exactly 1 member with role='incoming', got {incoming_count}"
            )
        if member_count > 0:
            errors.append("phase_transition cannot have members with role='member'")

    elif ghost_type == "capacity_aggregate":
        member_count = sum(1 for m in members if m.role == "member")
        non_member = [m for m in members if m.role != "member"]

        if member_count < 1:
            errors.append(
                f"capacity_aggregate requires at least 1 member with role='member', got {member_count}"
            )
        if non_member:
            bad_roles = [m.role for m in non_member]
            errors.append(
                f"capacity_aggregate cannot have roles 'incoming'/'outgoing', got: {bad_roles}"
            )

    return errors


# ─────────────────────────────────────────────────────────────
# POST /v1/ingest/ghosts
# ─────────────────────────────────────────────────────────────

@router.post(
    "/v1/ingest/ghosts",
    status_code=status.HTTP_201_CREATED,
    summary="Ingest ghost",
    description="Create or update a ghost_node with its members. Also creates Ghost node + edges in the graph.",
)
async def ingest_ghost(
    body: IngestGhostRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """
    Upsert ghost + members. Validates membership constraints before any write.
    Creates/updates a Ghost node in the graph and ghost_member edges.
    """
    # 1. Validate membership constraints (only if members provided)
    if body.members:
        errors = _validate_membership(body.ghost_type, body.members)
        if errors:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=errors,
            )

    # 2. Validate all item_ids exist
    if body.members:
        item_ids = [str(m.item_id) for m in body.members]
        rows = db.execute(
            "SELECT item_id FROM items WHERE item_id = ANY(%s)",
            (item_ids,),
        ).fetchall()
        found_ids = {str(r["item_id"]) for r in rows}
        missing = [iid for iid in item_ids if iid not in found_ids]
        if missing:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[f"item_id not found: {iid}" for iid in missing],
            )

    # 3. Validate resource_id if provided
    if body.resource_id:
        res_row = db.execute(
            "SELECT resource_id FROM resources WHERE resource_id = %s",
            (str(body.resource_id),),
        ).fetchone()
        if res_row is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=[f"resource_id not found: {body.resource_id}"],
            )

    # 4. Upsert ghost_node: keyed by (name, ghost_type, scenario_id)
    existing_ghost = db.execute(
        """
        SELECT ghost_id, node_id FROM ghost_nodes
        WHERE name = %s AND ghost_type = %s
          AND (scenario_id = %s OR (scenario_id IS NULL AND %s IS NULL))
        LIMIT 1
        """,
        (body.name, body.ghost_type, body.scenario_id, body.scenario_id),
    ).fetchone()

    if existing_ghost:
        ghost_id = existing_ghost["ghost_id"]
        node_id = existing_ghost["node_id"]
        db.execute(
            """
            UPDATE ghost_nodes
            SET resource_id = %s, status = %s, description = %s, updated_at = now()
            WHERE ghost_id = %s
            """,
            (body.resource_id, body.status, body.description, ghost_id),
        )
        action = "updated"
    else:
        ghost_id = uuid4()
        # Create the Ghost node in the graph first
        node_id = uuid4()
        scenario_id_for_node = body.scenario_id if body.scenario_id else BASELINE_SCENARIO_ID
        db.execute(
            """
            INSERT INTO nodes (node_id, node_type, scenario_id, active)
            VALUES (%s, 'Ghost', %s, TRUE)
            """,
            (node_id, scenario_id_for_node),
        )
        db.execute(
            """
            INSERT INTO ghost_nodes
                (ghost_id, name, ghost_type, scenario_id, resource_id, node_id, status, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                ghost_id, body.name, body.ghost_type, body.scenario_id,
                body.resource_id, node_id, body.status, body.description,
            ),
        )
        action = "inserted"

    # 5. Upsert members (delete-and-reinsert for idempotence)
    if body.members:
        db.execute("DELETE FROM ghost_members WHERE ghost_id = %s", (ghost_id,))
        if node_id:
            db.execute(
                "DELETE FROM edges WHERE from_node_id = %s AND edge_type = 'ghost_member'",
                (node_id,),
            )

        for m in body.members:
            member_id = uuid4()
            db.execute(
                """
                INSERT INTO ghost_members
                    (member_id, ghost_id, item_id, role,
                     transition_start_date, transition_end_date,
                     transition_curve, weight_at_start, weight_at_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    member_id, ghost_id, m.item_id, m.role,
                    m.transition_start_date, m.transition_end_date,
                    m.transition_curve, m.weight_at_start, m.weight_at_end,
                ),
            )

            # Create ghost_member edge in the graph (Ghost node → Item node)
            if node_id:
                item_node = db.execute(
                    """
                    SELECT node_id FROM nodes
                    WHERE node_type = 'Item' AND item_id = %s AND active = TRUE
                    LIMIT 1
                    """,
                    (m.item_id,),
                ).fetchone()

                if item_node:
                    edge_id = uuid4()
                    db.execute(
                        """
                        INSERT INTO edges (edge_id, from_node_id, to_node_id, edge_type, weight_ratio, active)
                        VALUES (%s, %s, %s, 'ghost_member', %s, TRUE)
                        ON CONFLICT DO NOTHING
                        """,
                        (edge_id, node_id, item_node["node_id"], float(m.weight_at_start)),
                    )

    logger.info("ingest.ghost ghost_id=%s action=%s", ghost_id, action)

    return {
        "ghost_id": str(ghost_id),
        "node_id": str(node_id) if node_id else None,
        "action": action,
        "member_count": len(body.members),
    }


# ─────────────────────────────────────────────────────────────
# GET /v1/ghosts
# ─────────────────────────────────────────────────────────────

@router.get(
    "/v1/ghosts",
    summary="List ghosts",
    description="List all ghost_nodes with their members.",
)
async def list_ghosts(
    ghost_type: Optional[str] = None,
    scenario_id: Optional[UUID] = None,
    ghost_status: Optional[str] = None,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return all ghosts filtered by optional ghost_type, scenario_id, status."""
    params: list[Any] = []
    where_clauses: list[str] = []

    if ghost_type:
        where_clauses.append("g.ghost_type = %s")
        params.append(ghost_type)
    if scenario_id:
        where_clauses.append("g.scenario_id = %s")
        params.append(scenario_id)
    if ghost_status:
        where_clauses.append("g.status = %s")
        params.append(ghost_status)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    ghost_rows = db.execute(
        f"""
        SELECT ghost_id, name, ghost_type, scenario_id, resource_id,
               node_id, status, description, created_at, updated_at
        FROM ghost_nodes g
        {where_sql}
        ORDER BY created_at DESC
        """,
        params,
    ).fetchall()

    results = []
    for g in ghost_rows:
        members = db.execute(
            """
            SELECT member_id, item_id, role,
                   transition_start_date, transition_end_date,
                   transition_curve, weight_at_start, weight_at_end
            FROM ghost_members WHERE ghost_id = %s
            """,
            (g["ghost_id"],),
        ).fetchall()

        results.append({
            "ghost_id": str(g["ghost_id"]),
            "name": g["name"],
            "ghost_type": g["ghost_type"],
            "scenario_id": str(g["scenario_id"]) if g["scenario_id"] else None,
            "resource_id": str(g["resource_id"]) if g["resource_id"] else None,
            "node_id": str(g["node_id"]) if g["node_id"] else None,
            "status": g["status"],
            "description": g["description"],
            "created_at": g["created_at"].isoformat() if g["created_at"] else None,
            "updated_at": g["updated_at"].isoformat() if g["updated_at"] else None,
            "members": [_serialize_member(m) for m in members],
        })

    return {"ghosts": results, "total": len(results)}


# ─────────────────────────────────────────────────────────────
# GET /v1/ghosts/{ghost_id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/v1/ghosts/{ghost_id}",
    summary="Get ghost detail",
    description="Get a ghost's detail including members and graph node.",
)
async def get_ghost(
    ghost_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return a single ghost with its members and graph node."""
    g = db.execute(
        """
        SELECT ghost_id, name, ghost_type, scenario_id, resource_id,
               node_id, status, description, created_at, updated_at
        FROM ghost_nodes WHERE ghost_id = %s
        """,
        (str(ghost_id),),
    ).fetchone()

    if g is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ghost {ghost_id} not found",
        )

    members = db.execute(
        """
        SELECT member_id, item_id, role,
               transition_start_date, transition_end_date,
               transition_curve, weight_at_start, weight_at_end
        FROM ghost_members WHERE ghost_id = %s
        ORDER BY role
        """,
        (str(ghost_id),),
    ).fetchall()

    # Graph node detail
    graph_node = None
    if g["node_id"]:
        node_row = db.execute(
            "SELECT node_id, node_type, scenario_id, active FROM nodes WHERE node_id = %s",
            (g["node_id"],),
        ).fetchone()
        if node_row:
            edges = db.execute(
                """
                SELECT edge_id, to_node_id, edge_type, weight_ratio
                FROM edges
                WHERE from_node_id = %s AND edge_type = 'ghost_member' AND active = TRUE
                """,
                (g["node_id"],),
            ).fetchall()
            graph_node = {
                "node_id": str(node_row["node_id"]),
                "node_type": node_row["node_type"],
                "active": node_row["active"],
                "edges": [
                    {
                        "edge_id": str(e["edge_id"]),
                        "to_node_id": str(e["to_node_id"]),
                        "edge_type": e["edge_type"],
                        "weight_ratio": float(e["weight_ratio"]) if e["weight_ratio"] is not None else None,
                    }
                    for e in edges
                ],
            }

    return {
        "ghost_id": str(g["ghost_id"]),
        "name": g["name"],
        "ghost_type": g["ghost_type"],
        "scenario_id": str(g["scenario_id"]) if g["scenario_id"] else None,
        "resource_id": str(g["resource_id"]) if g["resource_id"] else None,
        "node_id": str(g["node_id"]) if g["node_id"] else None,
        "status": g["status"],
        "description": g["description"],
        "created_at": g["created_at"].isoformat() if g["created_at"] else None,
        "updated_at": g["updated_at"].isoformat() if g["updated_at"] else None,
        "members": [_serialize_member(m) for m in members],
        "graph_node": graph_node,
    }


# ─────────────────────────────────────────────────────────────
# POST /v1/ghosts/{ghost_id}/run
# ─────────────────────────────────────────────────────────────

@router.post(
    "/v1/ghosts/{ghost_id}/run",
    summary="Run ghost logic",
    description="Execute ghost engine over a time window. Returns alerts and summary.",
)
async def run_ghost_endpoint(
    ghost_id: UUID,
    body: GhostRunRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """
    Execute ghost logic (phase_transition or capacity_aggregate) over [from_date, to_date].
    Returns: { ghost_id, ghost_type, alerts: [...], summary: {...} }
    """
    try:
        result = run_ghost(
            db=db,
            ghost_id=str(ghost_id),
            scenario_id=str(body.scenario_id),
            from_date=body.from_date,
            to_date=body.to_date,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        )

    return result


# ─────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────

def _serialize_member(m: Any) -> dict[str, Any]:
    return {
        "member_id": str(m["member_id"]),
        "item_id": str(m["item_id"]),
        "role": m["role"],
        "transition_start_date": m["transition_start_date"].isoformat() if m["transition_start_date"] else None,
        "transition_end_date": m["transition_end_date"].isoformat() if m["transition_end_date"] else None,
        "transition_curve": m["transition_curve"],
        "weight_at_start": float(m["weight_at_start"]),
        "weight_at_end": float(m["weight_at_end"]),
    }
