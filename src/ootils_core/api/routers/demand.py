"""
/v1/demand/descend — the demand-descent run (DESC-1 PR-B, ADR-043).

The HTTP surface over ``engine/descent/run.py::execute_descent``: reads
national (pooled, virtual-channel) demand for a scenario, applies the
scenario-resolved ``demand_split_pct``/``item_dc_eligibility``, and
materializes ordinary ``ForecastDemand``/``CustomerOrderDemand`` nodes on
the real distribution centers — deactivating the national source nodes it
split (anti-double-count). See the engine module's docstring for the full
algorithm (residual imputation, eligibility gate, idempotence).

PROJECTION IS NOT RECOMPUTED BY THIS ENDPOINT (explicit plan decision,
carried through to this response): the derived nodes are wired into the
graph (PI series + consumes/replenishes edges) so the STRUCTURE is correct
immediately, but their PI buckets' numeric fields stay whatever they were
until a SEPARATE recompute call. ``recompute_triggered`` is always
``false`` in this response — callers that need fresh projections after a
descent must follow up with ``POST /v1/calc/run`` (``full_recompute=true``
is the safe default the first time a scenario's demand topology changes
this much) or an incremental ``POST /v1/events``.

SCOPE: ``calc:run`` — per ADR-032's doctrine ("cost != reversibility"), a
descent run invokes engine computation (residual imputation, eligibility
resolution) and its derived graph writes, the same class of action as
``/v1/mrp/run``/``/v1/drp/run``. It is deliberately NOT ``graph:write``
(that scope is reserved for direct master-data mutations, not a computed
derivation) and NOT ``ingest`` (this is not raw external data landing).

SCENARIO_ID: resolved via ``Depends(resolve_scenario_id)`` (query param or
``X-Scenario-ID`` header, default baseline) — North Star doctrine, a fork
can re-run its own descent with its own split percentages without touching
baseline (ADR-043 §1 "Forkable").

Kill switch: ``OOTILS_DESCENT_ENABLED``, default OFF — this is a bulk,
scenario-wide WRITE that deactivates every national demand node it touches;
opt-in like ``OOTILS_PURGE_ENABLED``, unlike the read-mostly switches
elsewhere in the repo. Falsy -> 503, checked AFTER auth/scope but BEFORE the
DB pool (mirrors ``api/routers/maintenance.py``'s pattern).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.descent.run import DescentError, execute_descent

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/demand", tags=["demand"])

_TRUTHY = {"1", "true", "yes", "on"}


def _descent_enabled() -> bool:
    """Kill switch, default OFF. Falsy/unset OOTILS_DESCENT_ENABLED -> 503."""
    return os.environ.get("OOTILS_DESCENT_ENABLED", "0").strip().lower() in _TRUTHY


def require_descent_enabled() -> None:
    """FastAPI dependency — checked AFTER ``Depends(require_scope(...))`` but
    BEFORE ``Depends(get_db)`` (FastAPI resolves synchronous dependencies in
    signature order and short-circuits on the first HTTPException). Auth-first
    so an unauthenticated caller always gets 401 and cannot probe the switch;
    kill-switch-before-DB so a disabled run answers 503 without touching the
    DB pool."""
    if not _descent_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Demand descent is disabled (OOTILS_DESCENT_ENABLED).",
        )


class DescendRequest(BaseModel):
    """Body of POST /v1/demand/descend."""

    dry_run: bool = Field(
        default=False,
        description=(
            "Compute the full plan (eligibility gate, residual imputation, "
            "items_without_shares) without writing anything."
        ),
    )


class DescendResponse(BaseModel):
    """Outcome of one descent run. See engine/descent/run.py::DescentResult."""

    scenario_id: UUID
    dry_run: bool
    descent_run_id: Optional[UUID] = None
    event_id: Optional[UUID] = None
    source_nodes_considered: int
    source_nodes_deactivated: int
    derived_nodes_created: int
    lines_written: int
    items_without_shares: list[UUID]
    recompute_triggered: bool = Field(
        default=False,
        description=(
            "Always false: this run never recomputes the projection (see "
            "module docstring). Call POST /v1/calc/run separately."
        ),
    )


@router.post(
    "/descend",
    response_model=DescendResponse,
    status_code=status.HTTP_200_OK,
    summary="Run the demand-descent (national -> per-DC)",
    description=(
        "Materialize per-DC ForecastDemand/CustomerOrderDemand nodes from a "
        "scenario's national (virtual-channel) demand, applying the "
        "scenario-resolved demand_split_pct/item_dc_eligibility. Deactivates "
        "the split national source nodes (anti-double-count). Does NOT "
        "recompute the projection — call POST /v1/calc/run separately. "
        "Requires the `calc:run` scope; gated by OOTILS_DESCENT_ENABLED "
        "(default OFF)."
    ),
)
def descend(
    body: DescendRequest,
    _principal: Principal = Depends(require_scope("calc:run")),
    _enabled: None = Depends(require_descent_enabled),
    scenario_id: UUID = Depends(resolve_scenario_id),
    db: DictRowConnection = Depends(get_db),
) -> DescendResponse:
    """Run ``execute_descent`` for ``scenario_id``. ``get_db`` owns
    commit/rollback; ``execute_descent`` never commits or rolls back itself
    — a ``DescentError`` (unknown scenario) surfaces as 422, any other
    exception propagates and ``get_db`` rolls back."""
    now = datetime.now(timezone.utc)

    try:
        result = execute_descent(
            db,
            scenario_id=scenario_id,
            now=now,
            dry_run=body.dry_run,
            source="api",
        )
    except DescentError as e:
        # Hand-authored message (scenario UUID only, no DSN/raw DB text) —
        # same carve-out as staging.py's DiffError/param_overlay.py's
        # ParamOverlayError (see CLAUDE.md's "Typed domain exception
        # carve-out").
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(e)) from e

    logger.info(
        "demand.descend scenario_id=%s dry_run=%s sources_considered=%d "
        "sources_deactivated=%d derived_created=%d lines_written=%d "
        "items_without_shares=%d descent_run_id=%s",
        scenario_id, body.dry_run, result.source_nodes_considered,
        result.source_nodes_deactivated, result.derived_nodes_created,
        result.lines_written, len(result.items_without_shares), result.descent_run_id,
    )

    return DescendResponse(
        scenario_id=result.scenario_id,
        dry_run=result.dry_run,
        descent_run_id=result.descent_run_id,
        event_id=result.event_id,
        source_nodes_considered=result.source_nodes_considered,
        source_nodes_deactivated=result.source_nodes_deactivated,
        derived_nodes_created=result.derived_nodes_created,
        lines_written=result.lines_written,
        items_without_shares=list(result.items_without_shares),
        recompute_triggered=False,
    )
