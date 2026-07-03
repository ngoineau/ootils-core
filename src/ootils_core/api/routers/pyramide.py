from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db
from ootils_core.pyramide import PyramideError, PyramideRunConfig, PyramideRunner, SUPPORTED_METHODS
from ootils_core.pyramide.repository import (
    commit_run,
    fetch_run_summary,
    fetch_run_values,
    fetch_snapshot_values,
    get_historical_demand,
    list_snapshots,
    persist_run,
    resolve_item_uuid,
    resolve_location_uuid,
    resolve_scenario_uuid,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/forecast", tags=["pyramide"])


class PyramideRunRequest(BaseModel):
    item_id: str
    location_id: str
    horizon_days: int = Field(default=90, ge=1, le=365)
    granularity: str = Field(default="daily", pattern="^(daily|weekly|monthly)$")
    method: str = Field(default="AUTO_SELECT")
    method_params: dict[str, Any] = Field(default_factory=dict)
    scenario_id: str | None = None
    model_strategy: str = Field(default="stat", pattern="^(auto|stat|ml|fm|hybrid)$")
    recon_method: str = Field(default="bottomup", pattern="^(mintrace_wls|bottomup|topdown|middleout|none)$")
    random_seed: int = Field(default=0, ge=0)
    code_version: str = Field(default="local", min_length=1, max_length=64)

    @staticmethod
    def _method_error() -> str:
        return f"method must be one of: {sorted(SUPPORTED_METHODS)}"


class PyramideRunOut(BaseModel):
    # Leaf runs carry (item_id, location_id); aggregate runs (migration
    # 053) carry (hierarchy_id, level, node_code) — the unused side is
    # None. Optional on both sides so an aggregate run never 500s on
    # response validation.
    run_id: UUID
    snapshot_id: UUID
    forecast_id: UUID
    status: str
    item_id: UUID | None = None
    location_id: UUID | None = None
    hierarchy_id: str | None = None
    level: str | None = None
    node_code: str | None = None
    scenario_id: UUID
    horizon_start: date
    horizon_end: date
    granularity: str
    method: str
    model_strategy: str
    recon_method: str
    random_seed: int
    code_version: str
    selected_model: str
    engine_backend: str
    source_history_count: int
    value_count: int
    total_quantity: Decimal
    deterministic_artifact: str
    created_at: datetime
    committed_at: datetime | None = None


class PyramideValueOut(BaseModel):
    value_id: UUID
    forecast_date: date
    quantity: Decimal
    method: str


class PyramideRunResultOut(BaseModel):
    run: PyramideRunOut
    values: list[PyramideValueOut]


class PyramideCommitOut(BaseModel):
    run: PyramideRunOut
    committed: bool
    demand_node_count: int


class PyramideSnapshotOut(BaseModel):
    snapshot_id: UUID
    run_id: UUID
    forecast_id: UUID
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    horizon_start: date
    horizon_end: date
    granularity: str
    method: str
    frozen_at: datetime
    value_count: int
    total_quantity: Decimal


class PyramideSnapshotListOut(BaseModel):
    snapshots: list[PyramideSnapshotOut]
    total_count: int


class PyramideSnapshotDiffValueOut(BaseModel):
    forecast_date: date
    base_quantity: Decimal
    compare_quantity: Decimal
    delta: Decimal


class PyramideSnapshotDiffOut(BaseModel):
    snapshot_id: UUID
    compare_to: UUID
    values: list[PyramideSnapshotDiffValueOut]
    total_delta: Decimal


@router.post(
    "/runs",
    response_model=PyramideRunOut,
    status_code=status.HTTP_201_CREATED,
    summary="Create a Pyramide forecast run",
)
def create_pyramide_run(
    body: PyramideRunRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> PyramideRunOut:
    body.method = body.method.upper()
    if body.method not in SUPPORTED_METHODS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=PyramideRunRequest._method_error(),
        )

    item_uuid = resolve_item_uuid(db, body.item_id)
    if item_uuid is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Item '{body.item_id}' not found")

    location_uuid = resolve_location_uuid(db, body.location_id)
    if location_uuid is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Location '{body.location_id}' not found")

    try:
        scenario_uuid = resolve_scenario_uuid(body.scenario_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Invalid scenario_id '{body.scenario_id}'",
        ) from exc

    # The reader falls back to past CustomerOrderDemand nodes (scoped to
    # scenario_uuid) when demand_history is empty — so this runs BEFORE the
    # history-empty 422 below.
    history = get_historical_demand(
        db=db,
        item_id=item_uuid,
        location_id=location_uuid,
        lookback_days=max(body.horizon_days, 90),
        scenario_id=scenario_uuid,
    )
    if not history:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Historical demand is required to create a Pyramide forecast run",
        )

    config = PyramideRunConfig(
        item_id=item_uuid,
        location_id=location_uuid,
        scenario_id=scenario_uuid,
        horizon_start=date.today() + timedelta(days=1),
        horizon_days=body.horizon_days,
        granularity=body.granularity,
        method=body.method,
        method_params=body.method_params,
        model_strategy=body.model_strategy,
        recon_method=body.recon_method,
        random_seed=body.random_seed,
        code_version=body.code_version,
    )

    try:
        result = PyramideRunner().run(config, history)
    except PyramideError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc

    persisted = persist_run(db, result)
    summary = fetch_run_summary(db, persisted.run_id)
    if summary is None:
        logger.error("pyramide.run persisted but summary missing run_id=%s", persisted.run_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Pyramide run persistence failed")

    return _run_out(summary)


@router.get(
    "/runs/{run_id}",
    response_model=PyramideRunOut,
    summary="Get Pyramide run metadata",
)
def get_pyramide_run(
    run_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> PyramideRunOut:
    summary = fetch_run_summary(db, run_id)
    if summary is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pyramide run '{run_id}' not found")
    return _run_out(summary)


@router.get(
    "/runs/{run_id}/result",
    response_model=PyramideRunResultOut,
    summary="Get frozen values for a Pyramide run",
)
def get_pyramide_run_result(
    run_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> PyramideRunResultOut:
    summary = fetch_run_summary(db, run_id)
    values = fetch_run_values(db, run_id)
    if summary is None or values is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pyramide run '{run_id}' not found")
    return PyramideRunResultOut(
        run=_run_out(summary),
        values=[_value_out(value) for value in values],
    )


@router.post(
    "/runs/{run_id}/commit",
    response_model=PyramideCommitOut,
    summary="Commit a Pyramide run for deterministic consumption",
)
def commit_pyramide_run(
    run_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> PyramideCommitOut:
    summary = commit_run(db, run_id)
    if summary is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pyramide run '{run_id}' not found")
    return PyramideCommitOut(
        run=_run_out(summary.summary),
        committed=True,
        demand_node_count=summary.demand_node_count,
    )


@router.get(
    "/snapshots",
    response_model=PyramideSnapshotListOut,
    summary="List Pyramide forecast snapshots",
)
def list_pyramide_snapshots(
    limit: int = Query(default=100, ge=1, le=500),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> PyramideSnapshotListOut:
    snapshots = list_snapshots(db, limit=limit)
    return PyramideSnapshotListOut(
        snapshots=[_snapshot_out(snapshot) for snapshot in snapshots],
        total_count=len(snapshots),
    )


@router.get(
    "/snapshots/{snapshot_id}/diff",
    response_model=PyramideSnapshotDiffOut,
    summary="Diff two Pyramide snapshots",
)
def diff_pyramide_snapshots(
    snapshot_id: UUID,
    compare_to: UUID = Query(...),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> PyramideSnapshotDiffOut:
    base_values = fetch_snapshot_values(db, snapshot_id)
    compare_values = fetch_snapshot_values(db, compare_to)
    if base_values is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Snapshot '{snapshot_id}' not found")
    if compare_values is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Snapshot '{compare_to}' not found")

    base_by_date = {value.forecast_date: value.quantity for value in base_values}
    compare_by_date = {value.forecast_date: value.quantity for value in compare_values}
    dates = sorted(set(base_by_date) | set(compare_by_date))
    diff_values = []
    total_delta = Decimal("0")
    for forecast_date in dates:
        base_qty = base_by_date.get(forecast_date, Decimal("0"))
        compare_qty = compare_by_date.get(forecast_date, Decimal("0"))
        delta = compare_qty - base_qty
        total_delta += delta
        diff_values.append(
            PyramideSnapshotDiffValueOut(
                forecast_date=forecast_date,
                base_quantity=base_qty,
                compare_quantity=compare_qty,
                delta=delta,
            )
        )

    return PyramideSnapshotDiffOut(
        snapshot_id=snapshot_id,
        compare_to=compare_to,
        values=diff_values,
        total_delta=total_delta,
    )


def _run_out(summary) -> PyramideRunOut:
    return PyramideRunOut(
        run_id=summary.run_id,
        snapshot_id=summary.snapshot_id,
        forecast_id=summary.forecast_id,
        status=summary.status,
        item_id=summary.item_id,
        location_id=summary.location_id,
        hierarchy_id=summary.hierarchy_id,
        level=summary.level,
        node_code=summary.node_code,
        scenario_id=summary.scenario_id,
        horizon_start=summary.horizon_start,
        horizon_end=summary.horizon_end,
        granularity=summary.granularity,
        method=summary.method,
        model_strategy=summary.model_strategy,
        recon_method=summary.recon_method,
        random_seed=summary.random_seed,
        code_version=summary.code_version,
        selected_model=summary.selected_model,
        engine_backend=summary.engine_backend,
        source_history_count=summary.source_history_count,
        value_count=summary.value_count,
        total_quantity=summary.total_quantity,
        deterministic_artifact=summary.deterministic_artifact,
        created_at=summary.created_at,
        committed_at=summary.committed_at,
    )


def _value_out(value) -> PyramideValueOut:
    return PyramideValueOut(
        value_id=value.value_id,
        forecast_date=value.forecast_date,
        quantity=value.quantity,
        method=value.method,
    )


def _snapshot_out(snapshot) -> PyramideSnapshotOut:
    return PyramideSnapshotOut(
        snapshot_id=snapshot.snapshot_id,
        run_id=snapshot.run_id,
        forecast_id=snapshot.forecast_id,
        item_id=snapshot.item_id,
        location_id=snapshot.location_id,
        scenario_id=snapshot.scenario_id,
        horizon_start=snapshot.horizon_start,
        horizon_end=snapshot.horizon_end,
        granularity=snapshot.granularity,
        method=snapshot.method,
        frozen_at=snapshot.frozen_at,
        value_count=snapshot.value_count,
        total_quantity=snapshot.total_quantity,
    )
