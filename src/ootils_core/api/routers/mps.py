"""
POST /v1/mps/create — Create a Master Production Schedule node.

Creates an MPS node representing a planned production batch.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db
from ootils_core.models import MPSNode

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/mps", tags=["mps"])


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

class MPSNodeCreateRequest(BaseModel):
    item_id: str
    location_id: str
    planned_date: date
    quantity: Decimal
    uom: str = "EA"
    status: str = "planned"
    priority: int = 0
    lot_number: Optional[str] = None
    work_center: Optional[str] = None
    dependencies: list[str] = []  # external IDs of dependent MPS nodes
    scenario_id: Optional[str] = None  # defaults to baseline


class MPSNodeResponse(BaseModel):
    mps_node_id: UUID
    scenario_id: UUID
    item_id: UUID
    location_id: UUID
    planned_date: date
    quantity: Decimal
    uom: str
    status: str
    priority: int
    lot_number: Optional[str]
    work_center: Optional[str]
    dependencies: list[UUID]
    created_at: str


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _resolve_item_uuid(db: psycopg.Connection, external_id: str) -> UUID | None:
    """Resolve item external_id → item_id UUID."""
    row = db.execute(
        "SELECT item_id FROM items WHERE external_id = %s AND status != 'obsolete'",
        (external_id,),
    ).fetchone()
    return row["item_id"] if row else None


def _resolve_location_uuid(db: psycopg.Connection, external_id: str) -> UUID | None:
    """Resolve location external_id → location_id UUID."""
    row = db.execute(
        "SELECT location_id FROM locations WHERE external_id = %s",
        (external_id,),
    ).fetchone()
    return row["location_id"] if row else None


def _resolve_scenario_uuid(db: psycopg.Connection, scenario_id_str: str | None) -> UUID:
    """Resolve scenario_id string → UUID, defaulting to baseline."""
    if scenario_id_str is None or scenario_id_str.lower() == "baseline":
        return BASELINE_SCENARIO_ID
    try:
        return UUID(scenario_id_str)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid scenario_id '{scenario_id_str}' — must be a valid UUID or 'baseline'",
        )


def _resolve_mps_dependencies(db: psycopg.Connection, scenario_id: UUID, dep_external_ids: list[str]) -> list[UUID]:
    """Resolve list of MPS node external IDs to UUIDs."""
    if not dep_external_ids:
        return []
    # Assuming there's a table mps_nodes with external_id column (to be created)
    # For now, we'll treat them as UUID strings.
    uuids = []
    for ext_id in dep_external_ids:
        try:
            uuids.append(UUID(ext_id))
        except ValueError:
            # If not UUID, maybe lookup by external_id
            row = db.execute(
                "SELECT mps_node_id FROM mps_nodes WHERE external_id = %s AND scenario_id = %s",
                (ext_id, scenario_id),
            ).fetchone()
            if row:
                uuids.append(row["mps_node_id"])
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Dependency MPS node '{ext_id}' not found",
                )
    return uuids


# ─────────────────────────────────────────────────────────────
# POST /v1/mps/create
# ─────────────────────────────────────────────────────────────

@router.post(
    "/create",
    response_model=MPSNodeResponse,
    summary="Create MPS node",
    description="Create a Master Production Schedule node for a planned production batch.",
)
async def create_mps_node(
    body: MPSNodeCreateRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> MPSNodeResponse:
    """Create an MPS node."""

    # 1. Resolve item/location UUIDs
    item_uuid = _resolve_item_uuid(db, body.item_id)
    if item_uuid is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Item '{body.item_id}' not found",
        )

    location_uuid = _resolve_location_uuid(db, body.location_id)
    if location_uuid is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Location '{body.location_id}' not found",
        )

    # 2. Resolve scenario_id (default baseline)
    scenario_uuid = _resolve_scenario_uuid(db, body.scenario_id)

    # 3. Resolve dependencies
    dependency_uuids = _resolve_mps_dependencies(db, scenario_uuid, body.dependencies)

    # 4. Generate MPS node ID
    mps_node_id = uuid4()

    # 5. Insert into database (assuming table mps_nodes exists)
    db.execute(
        """
        INSERT INTO mps_nodes (
            mps_node_id, scenario_id, item_id, location_id, planned_date,
            quantity, uom, status, priority, lot_number, work_center,
            dependencies, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, now(), now()
        )
        """,
        (mps_node_id, scenario_uuid, item_uuid, location_uuid, body.planned_date,
         body.quantity, body.uom, body.status, body.priority, body.lot_number, body.work_center,
         dependency_uuids),
    )

    logger.info(
        "mps.create item=%s location=%s scenario=%s — MPS node %s created",
        body.item_id, body.location_id, scenario_uuid, mps_node_id,
    )

    return MPSNodeResponse(
        mps_node_id=mps_node_id,
        scenario_id=scenario_uuid,
        item_id=item_uuid,
        location_id=location_uuid,
        planned_date=body.planned_date,
        quantity=body.quantity,
        uom=body.uom,
        status=body.status,
        priority=body.priority,
        lot_number=body.lot_number,
        work_center=body.work_center,
        dependencies=dependency_uuids,
        created_at=str(date.today()),  # simplified
    )