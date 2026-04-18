"""
GET /v1/scenarios — List, inspect, and delete scenarios.
"""
from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID

import psycopg
from psycopg import sql
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db, BASELINE_SCENARIO_ID

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/scenarios", tags=["scenarios"])


class ScenarioOut(BaseModel):
    scenario_id: UUID
    name: str
    status: str
    is_baseline: bool
    parent_scenario_id: Optional[UUID] = None
    created_at: str
    updated_at: str


class ScenariosListResponse(BaseModel):
    scenarios: list[ScenarioOut]
    total: int


@router.get("", response_model=ScenariosListResponse)
async def list_scenarios(
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> ScenariosListResponse:
    conditions: list[sql.Composable] = []
    params: list = []
    if status_filter:
        conditions.append(sql.SQL("status = %s"))
        params.append(status_filter)
    where = (
        sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)
        if conditions
        else sql.SQL("")
    )
    total_row = db.execute(
        sql.SQL("SELECT COUNT(*) AS cnt FROM scenarios ") + where,
        params if params else None,
    ).fetchone()
    total = int(total_row["cnt"]) if total_row else 0
    rows = db.execute(
        sql.SQL("SELECT * FROM scenarios ") + where + sql.SQL(" ORDER BY created_at DESC LIMIT %s OFFSET %s"),
        (params + [limit, offset]) if params else [limit, offset],
    ).fetchall()
    return ScenariosListResponse(
        scenarios=[
            ScenarioOut(
                scenario_id=UUID(str(r["scenario_id"])),
                name=r["name"],
                status=r["status"],
                is_baseline=bool(r["is_baseline"]),
                parent_scenario_id=UUID(str(r["parent_scenario_id"])) if r.get("parent_scenario_id") else None,
                created_at=r["created_at"].isoformat() if hasattr(r["created_at"], "isoformat") else str(r["created_at"]),
                updated_at=r["updated_at"].isoformat() if hasattr(r["updated_at"], "isoformat") else str(r["updated_at"]),
            )
            for r in rows
        ],
        total=total,
    )


@router.get("/{scenario_id}", response_model=ScenarioOut)
async def get_scenario(
    scenario_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> ScenarioOut:
    row = db.execute("SELECT * FROM scenarios WHERE scenario_id = %s", (scenario_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Scenario {scenario_id} not found")
    return ScenarioOut(
        scenario_id=UUID(str(row["scenario_id"])),
        name=row["name"],
        status=row["status"],
        is_baseline=bool(row["is_baseline"]),
        parent_scenario_id=UUID(str(row["parent_scenario_id"])) if row.get("parent_scenario_id") else None,
        created_at=row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else str(row["created_at"]),
        updated_at=row["updated_at"].isoformat() if hasattr(row["updated_at"], "isoformat") else str(row["updated_at"]),
    )


@router.delete("/{scenario_id}", status_code=204)
async def delete_scenario(
    scenario_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> None:
    if scenario_id == BASELINE_SCENARIO_ID:
        raise HTTPException(status_code=400, detail="Cannot delete the baseline scenario")
    row = db.execute(
        "SELECT scenario_id, is_baseline FROM scenarios WHERE scenario_id = %s",
        (scenario_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Scenario {scenario_id} not found")
    if row["is_baseline"]:
        raise HTTPException(status_code=400, detail="Cannot delete a baseline scenario")
    db.execute(
        "UPDATE scenarios SET status = 'archived', updated_at = now() WHERE scenario_id = %s",
        (scenario_id,),
    )
    logger.info("scenario.archived scenario_id=%s", scenario_id)
