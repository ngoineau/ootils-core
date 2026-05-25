"""
/v1/scenarios — List, inspect, delete persistent scenarios (PG).
/v1/scenarios/sandbox — Create/delete ephemeral engine-side scenarios
                        for what-if workflows (P2.1.f).

# Scope of each endpoint group

| Endpoint                         | Backend  | Persistence    |
|----------------------------------|----------|----------------|
| GET    /v1/scenarios             | PG only  | persistent     |
| GET    /v1/scenarios/{id}        | PG only  | persistent     |
| DELETE /v1/scenarios/{id}        | PG+engine| persistent     |
| POST   /v1/scenarios/sandbox     | engine   | ephemeral (TTL)|
| GET    /v1/scenarios/sandbox     | engine   | ephemeral      |
| DELETE /v1/scenarios/sandbox/{id}| engine   | ephemeral      |

Sandbox scenarios live in the engine's RAM (P2.1.a-d), are evicted
by TTL (default 1 h idle), and never reach Postgres. The persistent
flavor (Option C "Save as") lands in P2.2.
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
async def create_sandbox_scenario(
    body: SandboxScenarioCreateRequest,
    _token: str = Depends(require_auth),
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
        raise HTTPException(
            status_code=500,
            detail=f"engine ForkScenario failed: {exc}",
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
async def list_sandbox_scenarios(
    _token: str = Depends(require_auth),
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
async def delete_sandbox_scenario(
    scenario_id: UUID,
    _token: str = Depends(require_auth),
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
