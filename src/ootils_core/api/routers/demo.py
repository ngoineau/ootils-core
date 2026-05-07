"""Demo endpoints for live product proof flows."""
from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db
from ootils_core.demo.phase1 import run_phase1_demo_from_env

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/demo", tags=["demo"])


class DemoRunSummary(BaseModel):
    demo_run_id: UUID
    demo_name: str
    status: str
    item_external_id: str | None = None
    location_external_id: str | None = None
    forecast_total: Decimal | None = None
    forecast_buckets: int | None = None
    mps_nodes_created: int | None = None
    mps_total_demand: Decimal | None = None
    approval_status: str | None = None
    mrp_status: str | None = None
    planned_supplies_created: int | None = None
    crp_planned_orders_count: int | None = None
    crp_work_centers_count: int | None = None
    crp_load_profiles: int | None = None
    atp_requested_quantity: Decimal | None = None
    atp_quantity_available: Decimal | None = None
    atp_buckets: int | None = None
    duration_ms: int
    error: str | None = None
    artifact: dict[str, Any]
    created_at: datetime


class DemoRunListResponse(BaseModel):
    runs: list[DemoRunSummary]
    total: int



def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


@router.post(
    "/phase1/run",
    summary="Run Phase 1 planning demo",
    description="Run the live Forecast -> MPS -> Approve -> MRP -> CRP -> ATP demo flow with unique seeded demo data.",
)
async def run_phase1_demo_endpoint(token: str = Depends(require_auth)) -> dict:
    """Run the executable Phase 1 demo flow."""
    try:
        return _json_safe(run_phase1_demo_from_env())
    except Exception as exc:  # pragma: no cover - tested through integration/demo gates
        logger.exception("phase1 demo failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Phase 1 demo flow failed",
        ) from exc


@router.get(
    "/phase1/runs",
    response_model=DemoRunListResponse,
    summary="List Phase 1 demo runs",
    description="Return recent persisted Phase 1 demo proof artifacts.",
)
async def list_phase1_demo_runs(
    limit: int = Query(default=5, ge=1, le=50),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> DemoRunListResponse:
    rows = db.execute(
        """
        SELECT
            demo_run_id, demo_name, status, item_external_id, location_external_id,
            forecast_total, forecast_buckets, mps_nodes_created, mps_total_demand,
            approval_status, mrp_status, planned_supplies_created,
            crp_planned_orders_count, crp_work_centers_count, crp_load_profiles,
            atp_requested_quantity, atp_quantity_available, atp_buckets,
            duration_ms, error, artifact, created_at
        FROM demo_runs
        WHERE demo_name = 'phase1'
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    ).fetchall()
    total_row = db.execute(
        "SELECT COUNT(*) AS total FROM demo_runs WHERE demo_name = 'phase1'"
    ).fetchone()
    return DemoRunListResponse(
        runs=[DemoRunSummary(**row) for row in rows],
        total=total_row["total"] if total_row else len(rows),
    )
