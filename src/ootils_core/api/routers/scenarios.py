"""
/v1/scenarios — List, inspect, delete persistent scenarios (PG).
/v1/scenarios/sandbox — Create/delete ephemeral engine-side scenarios
                        for what-if workflows (P2.1.f).

# Scope of each endpoint group

| Endpoint                          | Backend  | Persistence    |
|-----------------------------------|----------|----------------|
| GET    /v1/scenarios              | PG only  | persistent     |
| GET    /v1/scenarios/{id}         | PG only  | persistent     |
| DELETE /v1/scenarios/{id}         | PG+engine| persistent     |
| GET    /v1/scenarios/{id}/diff    | PG only  | persistent     |
| POST   /v1/scenarios/{id}/promote | PG only  | persistent     |
| POST   /v1/scenarios/sandbox      | engine   | ephemeral (TTL)|
| GET    /v1/scenarios/sandbox      | engine   | ephemeral      |
| DELETE /v1/scenarios/sandbox/{id} | engine   | ephemeral      |

Sandbox scenarios live in the engine's RAM (P2.1.a-d), are evicted
by TTL (default 1 h idle), and never reach Postgres. The persistent
flavor (Option C "Save as") lands in P2.2.

diff/promote (chantier #341b) expose ScenarioManager.diff/.promote:
promote carries conflict detection (409 + typed conflict list when
the baseline diverged since the overrides were captured) and the
Decision Ladder human gate (promote = applying a scenario to the
baseline = the 'APPLIED' L3+ action class).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from psycopg import sql
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import Principal, require_scope, resolve_gate_kind
from ootils_core.api.dependencies import get_db, BASELINE_SCENARIO_ID
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.recommendation.state_machine import (
    HumanGateError,
    enforce_human_gate,
)
from ootils_core.engine.scenario.compare import (
    ScenarioCompareEntry,
    ScenarioCompareError,
    ScenarioCompareResult,
    compare_scenarios,
    parse_scenario_ids,
    validate_id_count,
)
from ootils_core.engine.scenario.manager import (
    PromoteConflictError,
    ScenarioManager,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/scenarios", tags=["scenarios"])

_TRUTHY = {"1", "true", "yes", "on"}


def _scenario_compare_enabled() -> bool:
    """Kill switch, default ON. Falsy OOTILS_SCENARIO_COMPARE_ENABLED -> 503."""
    return os.environ.get("OOTILS_SCENARIO_COMPARE_ENABLED", "1").strip().lower() in _TRUTHY


def require_scenario_compare_enabled() -> None:
    """FastAPI dependency — checked AFTER auth/scope but BEFORE ``Depends(get_db)``
    (FastAPI resolves synchronous dependencies in signature order and
    short-circuits on the first HTTPException). Auth-first so an unauthenticated
    caller always gets 401 and cannot probe the switch state; kill-switch-before-DB
    so a disabled comparator answers 503 without touching the DB pool. Mirrors
    ``api/routers/outcomes.py``'s ``_outcomes_enabled()``/
    ``require_outcomes_enabled()`` pattern (outcomes.py:86)."""
    if not _scenario_compare_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Scenario compare is disabled (OOTILS_SCENARIO_COMPARE_ENABLED).",
        )


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
def list_scenarios(
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("read")),
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


# ============================================================
# GET /v1/scenarios/compare — SC-1 (chantier SC-1, ADR-free per contract)
# ============================================================
# Registered BEFORE /{scenario_id} on purpose: FastAPI/Starlette commits to
# the first path template that matches a request's URL. If /{scenario_id}
# (a UUID path param) were registered first, "GET /v1/scenarios/compare"
# would match THAT route (any path segment satisfies {scenario_id}) and fail
# UUID validation with a generic 422 — never reaching this handler at all.


class ScenarioCompareKpisOut(BaseModel):
    """One scenario's KPI snapshot. shortage_count/below_safety_stock_count/
    shortage_severity_usd are 0-honest (a real, meaningful 0 for a healthy
    scenario); stock_value_usd/fill_rate_est are None-honest (missing pricing
    / zero demand -> None, never a masked 0 or 1.0) — see
    engine/scenario/compare.py's module docstring for the full KPI contract."""

    shortage_count: int
    below_safety_stock_count: int
    shortage_severity_usd: float
    stock_value_usd: Optional[float] = None
    stock_value_basis_count: int
    stock_value_unpriced_count: int
    fill_rate_est: Optional[float] = None
    fill_rate_basis_count: int


class ScenarioCompareDeltasOut(BaseModel):
    """entry - reference. stock_value_usd_delta/fill_rate_delta can be None
    when either side's own KPI is None."""

    shortage_count_delta: int
    severity_usd_delta: float
    stock_value_usd_delta: Optional[float] = None
    fill_rate_delta: Optional[float] = None


class ScenarioCompareEntryOut(BaseModel):
    """One requested scenario's row. computable=False means kpis/deltas/stale
    are all None and note explains why (no completed calc_run yet)."""

    scenario_id: UUID
    name: str
    status: str
    parent_scenario_id: Optional[UUID] = None
    calc_run_id: Optional[UUID] = None
    computed_at: Optional[datetime] = None
    stale: Optional[bool] = None
    computable: bool
    note: Optional[str] = None
    kpis: Optional[ScenarioCompareKpisOut] = None
    deltas: Optional[ScenarioCompareDeltasOut] = None


class ScenarioCompareOut(BaseModel):
    """comparable = every entry is both computable AND fresh (not stale)."""

    entries: list[ScenarioCompareEntryOut]
    comparable: bool
    reference_scenario_id: UUID
    cost_precedence: str


def _compare_entry_to_out(entry: ScenarioCompareEntry) -> ScenarioCompareEntryOut:
    kpis_out = None
    if entry.kpis is not None:
        kpis_out = ScenarioCompareKpisOut(
            shortage_count=entry.kpis.shortage_count,
            below_safety_stock_count=entry.kpis.below_safety_stock_count,
            shortage_severity_usd=entry.kpis.shortage_severity_usd,
            stock_value_usd=entry.kpis.stock_value_usd,
            stock_value_basis_count=entry.kpis.stock_value_basis_count,
            stock_value_unpriced_count=entry.kpis.stock_value_unpriced_count,
            fill_rate_est=entry.kpis.fill_rate_est,
            fill_rate_basis_count=entry.kpis.fill_rate_basis_count,
        )
    deltas_out = None
    if entry.deltas is not None:
        deltas_out = ScenarioCompareDeltasOut(
            shortage_count_delta=entry.deltas.shortage_count_delta,
            severity_usd_delta=entry.deltas.severity_usd_delta,
            stock_value_usd_delta=entry.deltas.stock_value_usd_delta,
            fill_rate_delta=entry.deltas.fill_rate_delta,
        )
    return ScenarioCompareEntryOut(
        scenario_id=entry.scenario_id,
        name=entry.name,
        status=entry.status,
        parent_scenario_id=entry.parent_scenario_id,
        calc_run_id=entry.calc_run_id,
        computed_at=entry.computed_at,
        stale=entry.stale,
        computable=entry.computable,
        note=entry.note,
        kpis=kpis_out,
        deltas=deltas_out,
    )


def _compare_result_to_out(result: ScenarioCompareResult) -> ScenarioCompareOut:
    return ScenarioCompareOut(
        entries=[_compare_entry_to_out(e) for e in result.entries],
        comparable=result.comparable,
        reference_scenario_id=result.reference_scenario_id,
        cost_precedence=result.cost_precedence,
    )


@router.get(
    "/compare",
    response_model=ScenarioCompareOut,
    summary="Compare KPIs across 2-5 scenarios",
    description=(
        "Read-only KPI comparison (shortages, stock_value_usd, fill_rate_est) "
        "across 2-5 scenarios, scoped by each scenario's latest COMPLETED "
        "calc_run. Deltas are computed against the baseline (if present in "
        "`ids`) or the first id passed. A scenario with no completed calc_run "
        "yields a `computable=false` entry with a note, not a request failure; "
        "a malformed or unknown scenario id fails the WHOLE request (422). "
        "Emits no event/audit row (a pure query path). Requires the `read` scope."
    ),
)
def compare_scenarios_endpoint(
    ids: str = Query(
        ...,
        description="Comma-separated scenario UUIDs, 2..5 (e.g. 'ids=<uuid1>,<uuid2>').",
    ),
    _principal: Principal = Depends(require_scope("read")),
    _enabled: None = Depends(require_scenario_compare_enabled),
    db: DictRowConnection = Depends(get_db),
) -> ScenarioCompareOut:
    try:
        scenario_ids = parse_scenario_ids(ids)
        validate_id_count(scenario_ids)
        result = compare_scenarios(db, scenario_ids)
    except ScenarioCompareError as exc:
        raise HTTPException(status_code=422, detail=exc.detail)

    return _compare_result_to_out(result)


@router.get("/{scenario_id}", response_model=ScenarioOut)
def get_scenario(
    scenario_id: UUID,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("read")),
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
def delete_scenario(
    scenario_id: UUID,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("scenario:write")),
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
    # Best-effort engine-side cleanup if the engine is running and
    # happens to have this scenario forked (e.g., the user opened the
    # persistent scenario as a sandbox earlier). Failures are non-
    # fatal — PG is the source of truth for persistent scenarios.
    try:
        from ootils_core.engine_rust_service.singleton import get_client
        client = get_client()
        client._stub.DeleteScenario(  # noqa: SLF001  (raw stub call)
            __import__("ootils_core._grpc.engine_pb2", fromlist=["DeleteRequest"])
            .DeleteRequest(scenario_id=str(scenario_id)),
            timeout=2.0,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "engine-side DeleteScenario for %s skipped (not found or engine down): %s",
            scenario_id,
            exc,
        )
    logger.info("scenario.archived scenario_id=%s", scenario_id)


# ============================================================
# Diff / promote (persistent, PG) endpoints — chantier #341b
# ============================================================


class ScenarioDiffEntry(BaseModel):
    diff_id: UUID
    node_id: UUID
    field_name: str
    baseline_value: Optional[str] = None
    scenario_value: Optional[str] = None


class ScenarioDiffResponse(BaseModel):
    scenario_id: UUID
    baseline_id: UUID
    baseline_calc_run_id: UUID
    scenario_calc_run_id: UUID
    diffs: list[ScenarioDiffEntry]
    total: int


class PromoteRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=200, description="Username or agent name")
    actor_kind: Literal["human", "agent"] = "human"
    reason: Optional[str] = None


class PromoteConflictOut(BaseModel):
    """One baseline field that diverged since the override captured it."""

    node_id: UUID
    field_name: str
    expected: Optional[str] = None  # baseline value at override time
    actual: Optional[str] = None    # current baseline value


class PromoteResponse(BaseModel):
    promotion_id: UUID
    scenario_id: UUID
    promoted_by: str
    promoted_at: str
    override_count: int
    patched_nodes: int
    siblings_invalidated: int
    conflict_checked: bool
    event_id: UUID


def _load_scenario_or_404(scenario_id: UUID, db: DictRowConnection) -> dict:
    row = db.execute(
        "SELECT * FROM scenarios WHERE scenario_id = %s", (scenario_id,)
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Scenario {scenario_id} not found")
    return row


@router.get("/{scenario_id}/diff", response_model=ScenarioDiffResponse)
def diff_scenario(
    scenario_id: UUID,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("read")),
    baseline_id: UUID = Query(default=BASELINE_SCENARIO_ID),
) -> ScenarioDiffResponse:
    """Field-level diff of a scenario vs a baseline (latest completed calc_runs).

    Delegates to ScenarioManager.diff, which persists the entries in
    scenario_diffs (upsert on the calc_run pair) — re-reading the same pair
    is idempotent. Agents read this from a fork before proposing a promote.
    """
    _load_scenario_or_404(scenario_id, db)
    _load_scenario_or_404(baseline_id, db)

    manager = ScenarioManager()
    try:
        diffs = manager.diff(scenario_id=scenario_id, baseline_id=baseline_id, db=db)
    except ValueError:
        # Raised by _latest_calc_run when a scenario has no completed
        # calc_run. Hand-authored message (never str(exc) in a response).
        raise HTTPException(
            status_code=409,
            detail=(
                f"No completed calc_run found for scenario {scenario_id} "
                f"and/or baseline {baseline_id}. Run a calculation on both "
                "before requesting a diff."
            ),
        )

    if diffs:
        baseline_calc_run_id = diffs[0].baseline_calc_run_id
        scenario_calc_run_id = diffs[0].scenario_calc_run_id
    else:
        # No differences — still resolve the calc_run pair for the response
        baseline_calc_run_id = manager._latest_calc_run(baseline_id, db)  # noqa: SLF001
        scenario_calc_run_id = manager._latest_calc_run(scenario_id, db)  # noqa: SLF001

    return ScenarioDiffResponse(
        scenario_id=scenario_id,
        baseline_id=baseline_id,
        baseline_calc_run_id=baseline_calc_run_id,
        scenario_calc_run_id=scenario_calc_run_id,
        diffs=[
            ScenarioDiffEntry(
                diff_id=d.diff_id,
                node_id=d.node_id,
                field_name=d.field_name,
                baseline_value=d.baseline_value,
                scenario_value=d.scenario_value,
            )
            for d in diffs
        ],
        total=len(diffs),
    )


@router.post("/{scenario_id}/promote", response_model=PromoteResponse)
def promote_scenario(
    scenario_id: UUID,
    body: PromoteRequest,
    db: DictRowConnection = Depends(get_db),
    principal: Principal = Depends(require_scope("recommend:approve")),
) -> PromoteResponse:
    """Promote a scenario's overrides onto the baseline (L3+ decision).

    Conflict-safe (ADR-018 P2.2.c): if the baseline diverged since the
    overrides were captured, the promote aborts with 409 and the typed
    conflict list — nothing is written. Success writes the
    scenario_promotions audit row (migration 052) and emits a
    'scenario_merge' event.

    Two authorization floors stack (#392): the token SCOPE recommend:approve
    (checked by the dependency above, always on the real token's actor_kind)
    AND the Decision Ladder human gate (checked below on ``resolve_gate_kind``
    — see auth.py, #392 defect 9). For a MINTED token the gate always decides
    on the token's real actor_kind; for the LEGACY token, if the body still
    declares an actor_kind, the gate decides on THAT value instead — the exact
    pre-#392 self-declared-gate behaviour, preserved for the shared token
    until PR2 mints per-agent tokens (otherwise an honestly agent-declaring
    caller on the shared legacy token would gain the ability to promote a
    scenario to baseline, which it never had before #392).
    """
    gate_kind = resolve_gate_kind(principal, body.actor_kind)
    if not principal.is_legacy and body.actor_kind != principal.actor_kind:
        # A MINTED token's body disagreeing with its own token is a
        # mis-migrated client signal. A LEGACY principal disagreeing is NOT
        # an anomaly — it's resolve_gate_kind's intended fallback.
        logger.warning(
            "scenario.promote.actor_kind_mismatch body=%s token=%s scenario_id=%s",
            body.actor_kind,
            principal.actor_kind,
            scenario_id,
        )
    # Decision Ladder gate — promoting IS applying a scenario to the
    # baseline, i.e. the 'APPLIED' human-only action class. The rule lives
    # in engine/recommendation/state_machine (single source of truth,
    # shared with the recommendation router/CLI); this router only maps
    # HumanGateError to a 403.
    try:
        enforce_human_gate("APPLIED", gate_kind)
    except HumanGateError:
        raise HTTPException(
            status_code=403,
            detail=(
                "Promoting a scenario to baseline is an L3/L4 decision "
                "reserved to human actors (Decision Ladder, strategy doc §5)."
            ),
        )

    if scenario_id == BASELINE_SCENARIO_ID:
        raise HTTPException(status_code=400, detail="Cannot promote the baseline scenario onto itself")
    row = _load_scenario_or_404(scenario_id, db)
    if row["is_baseline"]:
        raise HTTPException(status_code=400, detail="Cannot promote a baseline scenario")
    if row["status"] != "active":
        raise HTTPException(
            status_code=409,
            detail=(
                f"Scenario {scenario_id} has status '{row['status']}'; "
                "only 'active' scenarios can be promoted."
            ),
        )

    manager = ScenarioManager()
    try:
        result = manager.promote(scenario_id=scenario_id, db=db, promoted_by=body.actor)
    except PromoteConflictError as e:
        # Typed conflict payload built from exception attributes — never
        # str(exc) in a response. Nothing was written by the manager.
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    f"Promote aborted: baseline diverged on {len(e.conflicts)} "
                    "node field(s) since the overrides were captured. "
                    "Re-fork or re-apply the overrides, then retry."
                ),
                "conflicts": [
                    PromoteConflictOut(
                        node_id=c.node_id,
                        field_name=c.field_name,
                        expected=c.expected,
                        actual=c.actual,
                    ).model_dump(mode="json")
                    for c in e.conflicts
                ],
            },
        )

    # Audit row (migration 052) — promote had no trail at all before #341b.
    audit = db.execute(
        """
        INSERT INTO scenario_promotions (
            scenario_id, promoted_by, reason,
            override_count, conflict_checked, siblings_invalidated
        ) VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING promotion_id, promoted_at
        """,
        (
            scenario_id,
            body.actor,
            body.reason,
            result.override_count,
            result.conflict_checked,
            result.siblings_invalidated,
        ),
    ).fetchone()
    if audit is None:
        raise RuntimeError(
            "INSERT ... RETURNING into scenario_promotions returned no row"
        )
    logger.info(
        "scenario.promoted scenario_id=%s promotion_id=%s by=%s overrides=%d",
        scenario_id,
        audit["promotion_id"],
        body.actor,
        result.override_count,
    )

    return PromoteResponse(
        promotion_id=audit["promotion_id"],
        scenario_id=scenario_id,
        promoted_by=body.actor,
        promoted_at=audit["promoted_at"].isoformat()
        if hasattr(audit["promoted_at"], "isoformat")
        else str(audit["promoted_at"]),
        override_count=result.override_count,
        patched_nodes=result.patched_nodes,
        siblings_invalidated=result.siblings_invalidated,
        conflict_checked=result.conflict_checked,
        event_id=result.merge_event_id,
    )


# ============================================================
# Sandbox (ephemeral, engine-side) endpoints — P2.1.f
# ============================================================


class SandboxScenarioOut(BaseModel):
    """An engine-side sandbox scenario. Lives in RAM only; evicted
    after `OOTILS_SCENARIO_TTL_SEC` of idle time (default 1 h)."""

    scenario_id: UUID
    name: str
    parent_baseline_id: UUID
    overlay_size: int
    memory_bytes: int


class SandboxScenarioCreateRequest(BaseModel):
    name: Optional[str] = None


class SandboxScenariosListResponse(BaseModel):
    scenarios: list[SandboxScenarioOut]
    total: int


def _engine_unavailable_response(exc: Exception) -> HTTPException:
    """Translate a singleton connect failure / gRPC error into a 503."""
    return HTTPException(
        status_code=503,
        detail=(
            f"engine sandbox unavailable: {exc}. "
            "Sandbox scenarios require OOTILS_ENGINE=rust-svc + "
            "a running engine process."
        ),
    )


@router.post(
    "/sandbox",
    response_model=SandboxScenarioOut,
    status_code=201,
)
def create_sandbox_scenario(
    body: SandboxScenarioCreateRequest,
    _principal: Principal = Depends(require_scope("scenario:write")),
) -> SandboxScenarioOut:
    """Create a fresh ephemeral what-if scenario in the engine.

    P2.1.f closure for ADR-018: the engine forks the baseline via
    ArcSwap (O(1)) and returns a fresh scenario UUID. The caller
    then includes this UUID in subsequent `POST /v1/events` calls
    to propagate against the sandbox without touching the baseline.

    The scenario lives until either:
      - the caller deletes it (`DELETE /v1/scenarios/sandbox/{id}`)
      - it has been idle for `OOTILS_SCENARIO_TTL_SEC` (default 1 h),
        at which point the engine's TTL eviction task drops it.

    For persistent scenarios that survive engine restarts, use the
    P2.2 "save as" flow (when shipped) which mirrors the overlay
    into Postgres.
    """
    from ootils_core.engine_rust_service.singleton import get_client

    try:
        client = get_client()
        info = client.fork_scenario(
            BASELINE_SCENARIO_ID,
            name=body.name or "",
        )
    except RuntimeError as exc:
        raise _engine_unavailable_response(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("sandbox.fork_scenario_failed name=%s", body.name)
        raise HTTPException(
            status_code=500,
            detail="engine ForkScenario failed.",
        ) from exc

    logger.info(
        "sandbox.created scenario_id=%s name=%s memory_bytes=%d",
        info.id,
        info.name,
        info.memory_bytes,
    )
    return SandboxScenarioOut(
        scenario_id=UUID(info.id),
        name=info.name,
        parent_baseline_id=BASELINE_SCENARIO_ID,
        overlay_size=info.overlay_size,
        memory_bytes=info.memory_bytes,
    )


@router.get(
    "/sandbox",
    response_model=SandboxScenariosListResponse,
)
def list_sandbox_scenarios(
    _principal: Principal = Depends(require_scope("read")),
) -> SandboxScenariosListResponse:
    """List all ephemeral scenarios currently in the engine's RAM.

    Includes the baseline as the first entry (filtered out by the
    `is_baseline` check). Empty list if the engine has no forks
    active."""
    from ootils_core.engine_rust_service.singleton import get_client

    try:
        client = get_client()
        sl = client.list_scenarios()
    except RuntimeError as exc:
        raise _engine_unavailable_response(exc) from exc

    out: list[SandboxScenarioOut] = []
    for s in sl.scenarios:
        sid = UUID(s.id)
        if sid == BASELINE_SCENARIO_ID:
            continue  # baseline is not a sandbox scenario
        out.append(
            SandboxScenarioOut(
                scenario_id=sid,
                name=s.name,
                parent_baseline_id=BASELINE_SCENARIO_ID,
                overlay_size=s.overlay_size,
                memory_bytes=s.memory_bytes,
            )
        )
    return SandboxScenariosListResponse(scenarios=out, total=len(out))


@router.delete("/sandbox/{scenario_id}", status_code=204)
def delete_sandbox_scenario(
    scenario_id: UUID,
    _principal: Principal = Depends(require_scope("scenario:write")),
) -> None:
    """Drop an ephemeral scenario from the engine's RAM immediately.

    NotFound if the UUID isn't a live engine scenario (already
    evicted, never existed, or belongs to PG-only persistent
    scenarios — those use `DELETE /v1/scenarios/{id}`)."""
    if scenario_id == BASELINE_SCENARIO_ID:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete the baseline scenario",
        )

    from ootils_core.engine_rust_service.singleton import get_client
    from ootils_core._grpc import engine_pb2
    import grpc

    try:
        client = get_client()
        req = engine_pb2.DeleteRequest(scenario_id=str(scenario_id))
        result = client._stub.DeleteScenario(req, timeout=5.0)  # noqa: SLF001
    except RuntimeError as exc:
        raise _engine_unavailable_response(exc) from exc
    except grpc.RpcError as exc:
        if exc.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(
                status_code=404,
                detail=f"Sandbox scenario {scenario_id} not found in engine RAM",
            ) from exc
        if exc.code() == grpc.StatusCode.INVALID_ARGUMENT:
            raise HTTPException(status_code=400, detail=exc.details()) from exc
        raise HTTPException(
            status_code=500,
            detail=f"engine DeleteScenario failed: {exc.details()}",
        ) from exc

    logger.info(
        "sandbox.deleted scenario_id=%s overlay_entries_freed=%d",
        scenario_id,
        result.overlay_entries_freed,
    )
