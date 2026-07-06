"""
POST /v1/drp/run — Distribution Requirements Planning transfer run (#395 PR2b).

Runs the deterministic DRP fair-share transfer planner for a scenario and emits
its inter-site TRANSFER moves as GOVERNED L1 DRAFT recommendations (the same
`recommendations` queue + state machine the procurement / reschedule fleet uses,
migrations 039/061/066) — NEVER `shortages` (ADR-021: that table is
ShortageDetector's alone; the DRP is read-only against it).

This is a READ over the graph that EMITS DRAFT recommendations — so it requires
the ``recommend:draft`` scope (not merely ``read``): the caller is proposing
governed actions, exactly like the transfer watcher it shares its emission code
path with (engine/recommendation/transfer.emit_transfer_recommendations). The
legacy single token holds ``admin`` and satisfies this, so no pre-#392 caller
regresses.

Forkable (North Star): the run is scoped to a ``scenario_id`` (default
baseline) resolved from the query param / X-Scenario-ID header — the DRP loads
the distribution plan through that scenario, so an agent can run it inside a
fork with a safety-stock overlay (#347) and get fork-specific transfers without
touching baseline.

Kill switch: ``OOTILS_DRP_ENABLED`` (default enabled). Set to a falsy value to
return 503 on the run verb — an operational escape hatch, independent of
auth/scope, that short-circuits BEFORE touching the DB pool (mirrors
api/routers/param_overrides.py's OOTILS_PARAM_OVERLAY_ENABLED pattern).

Decision Ladder: a DRP transfer is L1 (a new-order draft, reversible until
executed). The concrete level is sourced from
engine/recommendation/transfer.TRANSFER_DECISION_LEVEL and passed into the
emitter — never hardcoded here.
"""
from __future__ import annotations

import logging
import os
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from ootils_core.api.auth import require_scope
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.recommendation.transfer import (
    TRANSFER_DECISION_LEVEL,
    emit_transfer_recommendations,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/drp", tags=["drp"])

_AGENT_NAME = "drp_run"
_TRUTHY = {"1", "true", "yes", "on"}


def _drp_enabled() -> bool:
    """Kill switch, default ON. Falsy OOTILS_DRP_ENABLED -> 503."""
    return os.environ.get("OOTILS_DRP_ENABLED", "1").strip().lower() in _TRUTHY


def require_drp_enabled() -> None:
    """FastAPI dependency — checked AFTER auth/scope but BEFORE ``Depends(get_db)``
    in the endpoint signature (FastAPI resolves synchronous dependencies in
    signature order and short-circuits on the first HTTPException). Auth-first so
    an unauthenticated caller always gets 401 and cannot probe the kill switch's
    state; kill-switch-before-DB so a disabled DRP short-circuits with 503
    without touching the DB pool (an operational kill switch must not itself
    depend on the DB being healthy)."""
    if not _drp_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DRP is disabled (OOTILS_DRP_ENABLED).",
        )


class DrpRunRequest(BaseModel):
    """Body of POST /v1/drp/run. scenario_id is resolved from the query param /
    X-Scenario-ID header (Depends(resolve_scenario_id)), NOT the body, matching
    the forkable read-path convention of the other scenario-scoped routers."""

    horizon_days: int = Field(default=180, ge=1, le=1095)


class DrpRunResponse(BaseModel):
    """Response from a DRP transfer run."""

    scenario_id: UUID
    agent_run_id: UUID
    decision_level: str
    horizon_days: int
    signals: int
    recommendations_emitted: int
    recommendations_idempotent_noop: int
    expired_stale_drafts: int
    unresolved_coords: int
    message: str


@router.post(
    "/run",
    response_model=DrpRunResponse,
    summary="Run DRP (inter-site transfer planning)",
    description=(
        "Deterministic DRP fair-share transfer planner. Computes projected "
        "per-site deficits vs linked-source excess and emits inter-site "
        "TRANSFER moves as governed L1 DRAFT recommendations (idempotent: an "
        "unchanged plan re-run emits zero new rows). Read-only on the graph; "
        "writes only into `recommendations`, never `shortages` (ADR-021). "
        "Scenario-scoped (forkable)."
    ),
)
def run_drp(
    body: DrpRunRequest,
    _principal=Depends(require_scope("recommend:draft")),
    _enabled: None = Depends(require_drp_enabled),
    scenario_id: UUID = Depends(resolve_scenario_id),
    db: DictRowConnection = Depends(get_db),
) -> DrpRunResponse:
    """Run DRP transfer planning for a scenario and emit governed DRAFT
    transfer recommendations.

    Opens a work-ledger row in agent_runs (RUNNING -> COMPLETED/FAILED), then
    delegates to the shared emitter (the SAME code path the transfer watcher
    uses). get_db owns commit/rollback; on any failure the agent_runs row is
    closed FAILED before the exception propagates so a crashed run never leaves
    a RUNNING orphan."""
    scenario_str = str(scenario_id)

    run_row = db.execute(
        "INSERT INTO agent_runs (agent_name, scenario_id, status) "
        "VALUES (%s, %s, 'RUNNING') RETURNING agent_run_id",
        (_AGENT_NAME, scenario_str),
    ).fetchone()
    if run_row is None:
        raise RuntimeError("drp.run: agent_runs INSERT ... RETURNING yielded no row")
    agent_run_id = run_row["agent_run_id"]

    try:
        metrics = emit_transfer_recommendations(
            db,
            scenario_str,
            body.horizon_days,
            agent_name=_AGENT_NAME,
            agent_run_id=agent_run_id,
            decision_level=TRANSFER_DECISION_LEVEL,
        )
    except Exception:
        # Close the ledger row FAILED (best-effort) before re-raising; get_db
        # rolls the transaction back, so this UPDATE is part of the same
        # rolled-back unit — we still log the failure with attribution. The
        # generic app.py handler hides the exception string from the client.
        logger.exception(
            "drp.run.failed scenario_id=%s agent_run_id=%s", scenario_str, agent_run_id
        )
        raise

    db.execute(
        "UPDATE agent_runs SET status='COMPLETED', finished_at=now(), metrics=%s "
        "WHERE agent_run_id=%s",
        (Jsonb(metrics), agent_run_id),
    )

    logger.info(
        "drp.run scenario_id=%s agent_run_id=%s horizon=%d signals=%d emitted=%d "
        "noop=%d expired=%d unresolved=%d",
        scenario_str, agent_run_id, body.horizon_days, metrics["signals"],
        metrics["recommendations_inserted"], metrics["recommendations_idempotent_noop"],
        metrics["expired_stale_drafts"], metrics["unresolved_coords"],
    )

    return DrpRunResponse(
        scenario_id=scenario_id,
        agent_run_id=agent_run_id,
        decision_level=TRANSFER_DECISION_LEVEL,
        horizon_days=body.horizon_days,
        signals=metrics["signals"],
        recommendations_emitted=metrics["recommendations_inserted"],
        recommendations_idempotent_noop=metrics["recommendations_idempotent_noop"],
        expired_stale_drafts=metrics["expired_stale_drafts"],
        unresolved_coords=metrics["unresolved_coords"],
        message=(
            f"DRP run complete. {metrics['recommendations_inserted']} new transfer "
            f"recommendation(s) emitted over {body.horizon_days}-day horizon "
            f"({metrics['recommendations_idempotent_noop']} idempotent no-op)."
        ),
    )
