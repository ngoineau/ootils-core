from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import get_db
from ootils_core.db.types import DictRowConnection
from ootils_core.pyramide import PyramideError, PyramideRunConfig, PyramideRunner, SUPPORTED_METHODS
from ootils_core.pyramide.confidence import DEFAULT_SLA_DAYS
from ootils_core.pyramide.hierarchy import (
    RECON_MIDDLEOUT,
    SUPPORTED_RECON_METHODS,
    HierarchicalRunConfig,
    HierarchicalRunner,
)
from ootils_core.pyramide.repository import (
    PyramideAggregateCommitError,
    commit_run,
    fetch_accuracy_metrics,
    fetch_run_summary,
    fetch_run_values,
    fetch_snapshot_values,
    get_demand_freshness,
    get_historical_demand,
    list_snapshots,
    persist_run,
    record_stale_demand_finding,
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
    # Single-series leaf endpoint: nothing is reconciled here, so the
    # ONLY honest value is 'none' (recon_method = method EFFECTIVELY
    # applied, migration 054 — provenance never lies). Hierarchical block
    # runs go through HierarchicalRunner, not this endpoint; a client
    # requesting a reconciliation method here gets a clear 422 instead of
    # a silently-false provenance row (review PR3).
    recon_method: str = Field(default="none", pattern="^none$")
    random_seed: int = Field(default=0, ge=0)
    code_version: str = Field(default="local", min_length=1, max_length=64)
    # Freshness SLA (ADR-023): demand_history ingested more than this many
    # days ago is stale — the run is still produced (agents may simulate on
    # stale data) but carries stale_demand=TRUE and a dq_findings
    # STALE_DEMAND row. A PARAMETER (pilot default 7 days), never a
    # hard-coded business constant.
    freshness_sla_days: int = Field(default=DEFAULT_SLA_DAYS, ge=1, le=365)

    @staticmethod
    def _method_error() -> str:
        return f"method must be one of: {sorted(SUPPORTED_METHODS)}"


class PyramideAccuracyMetricOut(BaseModel):
    # horizon None = all-horizons aggregate row; h >= 1 = per-horizon
    # row (only bias + counts are derivable from the persisted backtest
    # residuals — the other metrics need the actuals and stay None).
    # A None metric = "not computable on this data" (None-honest
    # contract of pyramide/accuracy.py), never a masked 0.
    horizon: int | None = None
    mase: Decimal | None = None
    wape: Decimal | None = None
    smape: Decimal | None = None
    bias: Decimal | None = None
    coverage: Decimal | None = None
    n_cutoffs: int
    n_observations: int
    # Forecast Value Added over the seasonal-naive baseline (migration 068,
    # #393 A3-PR3), on the aggregate row (horizon None) only. naive_* = the
    # trivial baseline error scored on the SAME backtest; fva_* = naive_* -
    # stat (POSITIVE = the stat model beats the naive — a negative FVA is a
    # legitimate honest result, never clamped). None = not computable / not
    # comparable (baseline needs >= 1 season of history, or the stat metric
    # is itself None), never a masked 0.
    naive_wape: Decimal | None = None
    naive_mase: Decimal | None = None
    fva_wape: Decimal | None = None
    fva_mase: Decimal | None = None


class PyramideRunOut(BaseModel):
    # Leaf runs carry (item_id, location_id); aggregate runs (migration
    # 053) carry (hierarchy_id, level, node_code) — the unused side is
    # None. Optional on both sides so an aggregate run never 500s on
    # response validation. snapshot_id is None for aggregate runs
    # (snapshots are leaf-only — they exist to feed the graph commit).
    run_id: UUID
    snapshot_id: UUID | None = None
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
    # Backtest accuracy metrics (migration 055) — populated by
    # GET /runs/{run_id} only (backward-compatible optional field):
    # aggregate row first, then per-horizon rows. None on the other
    # endpoints (not fetched); [] = run persisted without a backtest
    # report (e.g. ENSEMBLE_STAT blend, external backend).
    accuracy_metrics: list[PyramideAccuracyMetricOut] | None = None
    # ADR-023 (migration 056): TRUE when the run was generated while the
    # demand_history ingest age exceeded the freshness SLA. Additive,
    # backward-compatible — agents must not auto-act (L0-L2) on TRUE.
    stale_demand: bool = False


class PyramideValueOut(BaseModel):
    value_id: UUID
    forecast_date: date
    quantity: Decimal
    method: str
    # Bornes conformal du bucket (confidence_interval_lower/upper, migration
    # 026 — écrites par persist_run/persist_series_run, exposées ici depuis
    # #394 B1). None = NULL en base = pas de calibration honnête (pas de
    # backtest déterministe, ou trop peu de résidus pour la garantie
    # finite-sample) — jamais 0. Additif, rétro-compatible.
    confidence_lower: Decimal | None = None
    confidence_upper: Decimal | None = None


class PyramideRunResultOut(BaseModel):
    run: PyramideRunOut
    values: list[PyramideValueOut]


class HierarchicalRunRequest(BaseModel):
    # A hierarchical run forecasts ONE summing block of the migration-047
    # hierarchy registry (hierarchy / hierarchy_node / item_hierarchy) and
    # reconciles every level (middle-out deterministic core, MinT-shrink
    # optional edge — HierarchicalRunner, #348). It is NOT a single
    # (item, location) leaf run: it produces N coherent series (aggregate
    # nodes + leaves), each persisted as its own pyramide_runs row. The GET
    # /runs/{run_id}/result endpoint reads any of those run_ids.
    hierarchy_id: str
    block_code: str
    # Leaf series are addressed to this location (locations.external_id) —
    # the network/central site whose graph consumes the demand plan; the
    # per-site split is the DRP layer's job (ADR-020). Generic: the caller
    # chooses, nothing is hardcoded.
    leaf_location_id: str
    horizon_days: int = Field(default=90, ge=1, le=365)
    granularity: str = Field(default="daily", pattern="^(daily|weekly|monthly)$")
    method: str = Field(default="AUTO_SELECT")
    method_params: dict[str, Any] = Field(default_factory=dict)
    scenario_id: str | None = None
    model_strategy: str = Field(default="stat", pattern="^(auto|stat|ml|fm|hybrid)$")
    # recon_method opened to the methods HierarchicalRunner actually
    # supports and tests (#348): 'middleout' (guaranteed deterministic
    # core) and 'mintrace_wls_shrink' (optional Nixtla edge, falls back to
    # middleout when the backend or its aligned inputs are unavailable —
    # the runner reports the EFFECTIVE method, provenance never lies). No
    # 'none': this endpoint always reconciles (a leaf-only run with no
    # reconciliation is POST /runs).
    recon_method: str = Field(
        default=RECON_MIDDLEOUT, pattern="^(middleout|mintrace_wls_shrink)$"
    )
    # Level of the hierarchy to cut blocks at (default: the root/block
    # level). None = the registry default resolved by load_summing_blocks.
    block_level: str | None = None
    # Level at which the base forecast is produced before disaggregating to
    # the leaves. None = the block level itself (one base forecast at the
    # block root). Pass a deeper level for a classic middle-out.
    recon_level: str | None = None
    lookback_days: int = Field(default=365, ge=1, le=3650)
    random_seed: int = Field(default=0, ge=0)
    code_version: str = Field(default="local", min_length=1, max_length=64)

    @staticmethod
    def _method_error() -> str:
        return f"method must be one of: {sorted(SUPPORTED_METHODS)}"


class HierarchicalSeriesOut(BaseModel):
    # One persisted series of the block. kind = 'aggregate' (carries level +
    # node_code, addresses a hierarchy node) or 'leaf' (carries item_id +
    # location_id). snapshot_id is None for aggregates (leaf-only snapshot
    # contract — aggregates never enter the graph). run_id is the handle for
    # GET /runs/{run_id} and /runs/{run_id}/result.
    kind: str
    key: str
    level: str | None = None
    run_id: UUID
    forecast_id: UUID
    snapshot_id: UUID | None = None


class HierarchicalRunResultOut(BaseModel):
    hierarchy_id: str
    block_code: str
    block_level: str
    recon_level: str
    # The reconciliation method EFFECTIVELY applied (never the rejected
    # request): a 'mintrace_wls_shrink' request that fell back reports
    # 'middleout' here, and warnings carries the reason.
    recon_method: str
    scenario_id: UUID
    horizon_start: date
    horizon_end: date
    granularity: str
    method: str
    series: list[HierarchicalSeriesOut]
    # Explainability trail: reconciliation fallbacks, cold-start twin rules,
    # NULL-bound leaves, routing provenance notes. Empty = nothing to flag.
    warnings: list[str]


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
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("calc:run")),
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
        # Typed domain-exception carve-out (cf. CLAUDE.md): hand-authored
        # message from config validation (horizon_days, granularity, method)
        # or a wrapped forecasting-engine error — both DB-free, no SQL/DSN
        # can reach this string.
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)) from exc

    # Freshness gate (ADR-023) — measured at ITEM level: the ingest
    # pipeline loads whole extracts, so a per-warehouse filter would
    # misread a coverage gap as pipeline death. Only a PROVEN breach
    # (ingest_age_days known AND > SLA) marks the run stale; an unknown
    # freshness (e.g. degraded CustomerOrderDemand fallback with an empty
    # demand_history) degrades the confidence score instead — a stale flag
    # is never invented.
    freshness = get_demand_freshness(db, item_id=item_uuid)
    stale = (
        freshness.ingest_age_days is not None
        and freshness.ingest_age_days > body.freshness_sla_days
    )

    # history feeds the FVA seasonal-naive baseline (migration 068): it is
    # the SAME series the run was computed on, so the naive is backtested on
    # identical cutoffs (methodological consistency — see fva.compute_fva).
    persisted = persist_run(db, result, stale_demand=stale, history=history)
    if stale:
        # Exactly once per run (this endpoint is the only writer and the
        # run is created exactly once) — no spam, evidence carries run_id.
        dq_finding_id = record_stale_demand_finding(
            db,
            run_id=persisted.run_id,
            scenario_id=scenario_uuid,
            item_id=item_uuid,
            item_external_id=body.item_id,
            warehouse_external_id=body.location_id,
            freshness=freshness,
            sla_days=body.freshness_sla_days,
        )
        logger.warning(
            "pyramide.run stale demand run_id=%s item=%s ingest_age_days=%s "
            "sla_days=%s dq_finding_id=%s",
            persisted.run_id, body.item_id, freshness.ingest_age_days,
            body.freshness_sla_days, dq_finding_id,
        )
    summary = fetch_run_summary(db, persisted.run_id)
    if summary is None:
        logger.error("pyramide.run persisted but summary missing run_id=%s", persisted.run_id)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Pyramide run persistence failed")

    return _run_out(summary)


@router.post(
    "/hierarchical-runs",
    response_model=HierarchicalRunResultOut,
    status_code=status.HTTP_201_CREATED,
    summary="Create a hierarchical (multi-level, reconciled) Pyramide forecast run",
)
def create_hierarchical_run(
    body: HierarchicalRunRequest,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("calc:run")),
) -> HierarchicalRunResultOut:
    """Forecast and reconcile ONE summing block of the hierarchy registry
    (HierarchicalRunner, #348). Every level is persisted as its own
    pyramide_runs row (aggregates carry hierarchy_id/level/node_code, leaves
    carry item/location); the response lists each run_id so the existing
    GET /runs/{run_id}/result endpoint reads any of them — leaf results carry
    conformal bounds, aggregate results carry NULL bounds (hierarchical
    interval reconciliation is a documented V1 non-goal).

    ``recon_method`` reflects the method EFFECTIVELY applied: a
    'mintrace_wls_shrink' request that fell back reports 'middleout' with the
    reason in ``warnings`` — provenance never lies.
    """
    body.method = body.method.upper()
    if body.method not in SUPPORTED_METHODS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=HierarchicalRunRequest._method_error(),
        )
    if body.recon_method not in SUPPORTED_RECON_METHODS:
        # Defensive: the pattern already constrains recon_method, but keep the
        # runner's catalogue as the single source of truth for the message.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"recon_method must be one of: {sorted(SUPPORTED_RECON_METHODS)}",
        )

    location_uuid = resolve_location_uuid(db, body.leaf_location_id)
    if location_uuid is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Location '{body.leaf_location_id}' not found",
        )

    try:
        scenario_uuid = resolve_scenario_uuid(body.scenario_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Invalid scenario_id '{body.scenario_id}'",
        ) from exc

    config = HierarchicalRunConfig(
        hierarchy_id=body.hierarchy_id,
        block_code=body.block_code,
        leaf_location_id=location_uuid,
        scenario_id=scenario_uuid,
        horizon_start=date.today() + timedelta(days=1),
        horizon_days=body.horizon_days,
        block_level=body.block_level,
        recon_level=body.recon_level,
        granularity=body.granularity,
        method=body.method,
        method_params=body.method_params,
        model_strategy=body.model_strategy,
        recon_method=body.recon_method,
        lookback_days=body.lookback_days,
        random_seed=body.random_seed,
        code_version=body.code_version,
    )

    try:
        result = HierarchicalRunner().run(db, config)
    except (PyramideError, ValueError) as exc:
        # Typed domain-exception carve-out (cf. CLAUDE.md): hand-authored
        # message from config validation (unknown block/level/method,
        # empty node history) — DB-free, no SQL/DSN can reach this string.
        # ValueError is included for the hierarchy registry validation layer
        # (pyramide/hierarchy/summing.py: unknown hierarchy_id, no levels,
        # duplicate codes...) which predates PyramideError and is asserted as
        # ValueError by its own unit tests — its messages are equally
        # hand-authored (they interpolate only caller-supplied identifiers).
        # Without this, an unknown hierarchy_id surfaced as a generic 500.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(exc)
        ) from exc

    logger.info(
        "pyramide.hierarchical_run hierarchy=%s block=%s scenario_id=%s "
        "recon_method=%s series=%d warnings=%d",
        result.hierarchy_id, result.block_code, scenario_uuid,
        result.recon_method, len(result.persisted), len(result.warnings),
    )
    return HierarchicalRunResultOut(
        hierarchy_id=result.hierarchy_id,
        block_code=result.block_code,
        block_level=result.block_level,
        recon_level=result.recon_level,
        recon_method=result.recon_method,
        scenario_id=scenario_uuid,
        horizon_start=result.horizon_start,
        horizon_end=result.horizon_end,
        granularity=result.granularity,
        method=result.method,
        series=[_hierarchical_series_out(series) for series in result.persisted],
        warnings=list(result.warnings),
    )


@router.get(
    "/runs/{run_id}",
    response_model=PyramideRunOut,
    summary="Get Pyramide run metadata",
)
def get_pyramide_run(
    run_id: UUID,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("read")),
) -> PyramideRunOut:
    summary = fetch_run_summary(db, run_id)
    if summary is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pyramide run '{run_id}' not found")
    out = _run_out(summary)
    out.accuracy_metrics = [
        _accuracy_metric_out(metric)
        for metric in fetch_accuracy_metrics(db, run_id)
    ]
    return out


@router.get(
    "/runs/{run_id}/result",
    response_model=PyramideRunResultOut,
    summary="Get frozen values for a Pyramide run",
)
def get_pyramide_run_result(
    run_id: UUID,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("read")),
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
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("ingest")),
) -> PyramideCommitOut:
    try:
        summary = commit_run(db, run_id)
    except PyramideAggregateCommitError as exc:
        # Typed domain-exception carve-out (cf. CLAUDE.md): hand-authored
        # message containing only UUIDs / hierarchy codes — the client
        # needs it to understand why an aggregate run is not committable.
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
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
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("read")),
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
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("read")),
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
        stale_demand=summary.stale_demand,
    )


def _accuracy_metric_out(metric) -> PyramideAccuracyMetricOut:
    return PyramideAccuracyMetricOut(
        horizon=metric.horizon,
        mase=metric.mase,
        wape=metric.wape,
        smape=metric.smape,
        bias=metric.bias,
        coverage=metric.coverage,
        n_cutoffs=metric.n_cutoffs,
        n_observations=metric.n_observations,
        naive_wape=metric.naive_wape,
        naive_mase=metric.naive_mase,
        fva_wape=metric.fva_wape,
        fva_mase=metric.fva_mase,
    )


def _value_out(value) -> PyramideValueOut:
    return PyramideValueOut(
        value_id=value.value_id,
        forecast_date=value.forecast_date,
        quantity=value.quantity,
        method=value.method,
        confidence_lower=value.confidence_lower,
        confidence_upper=value.confidence_upper,
    )


def _hierarchical_series_out(series) -> HierarchicalSeriesOut:
    return HierarchicalSeriesOut(
        kind=series.kind,
        key=series.key,
        level=series.level,
        run_id=series.run_id,
        forecast_id=series.forecast_id,
        snapshot_id=series.snapshot_id,
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
