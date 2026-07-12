"""
/v1/maintenance/purge-preview — read-only purge dry-run surface (PURGE-1,
migration 076).

The HTTP surface over ``engine/maintenance/purge.py``'s planners: a single
GET that runs BOTH ``plan_fork_purge`` and ``plan_shortage_retention`` (both
SELECT-only) and reports, table by table, what a purge run WOULD delete.

NO APPLY ENDPOINT — DELIBERATE (architect decision): a destructive,
FK-ordered, multi-table delete across an archived scenario's full working
state is a job for an operator-run CLI (``scripts/purge_maintenance.py``)
with its own explicit ``--apply`` flag and kill switch, never a single HTTP
POST. This router is preview-only, full stop.

SCOPE: ``admin`` — this is an operator/ops surface (like ``GET /metrics``),
not a planning read a regular scenario/agent caller needs; it is not scoped
by ``scenario_id`` because it reports on EVERY eligible scenario at once
(the whole point of a maintenance preview).

Kill switch: ``OOTILS_PURGE_ENABLED`` (default OFF — unlike the read-mostly
switches elsewhere in the repo, PURGE-1 is a destructive-adjacent capability
and defaults to opt-in, mirroring the CLI's own conservative default).
Falsy -> 503, checked AFTER auth/scope but BEFORE the DB pool (mirrors
``api/routers/outcomes.py``'s ``require_outcomes_enabled`` pattern).
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import get_db
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.maintenance import plan_fork_purge, plan_shortage_retention

logger = logging.getLogger(__name__)

router = APIRouter(tags=["maintenance"])

_TRUTHY = {"1", "true", "yes", "on"}


def _purge_enabled() -> bool:
    """Kill switch, default OFF. Falsy/unset OOTILS_PURGE_ENABLED -> 503."""
    return os.environ.get("OOTILS_PURGE_ENABLED", "0").strip().lower() in _TRUTHY


def require_purge_enabled() -> None:
    """FastAPI dependency — checked AFTER auth/scope but BEFORE ``Depends(get_db)``
    (FastAPI resolves synchronous dependencies in signature order and
    short-circuits on the first HTTPException). Auth-first so an unauthenticated
    caller always gets 401 and cannot probe the switch; kill-switch-before-DB so
    a disabled preview answers 503 without touching the DB pool."""
    if not _purge_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Purge maintenance is disabled (OOTILS_PURGE_ENABLED).",
        )


class PurgeCandidateOut(BaseModel):
    """One archived scenario eligible for fork purge, with its FK-safe
    per-table row breakdown (includes the cascade-only ``ghost_members``
    count for operator visibility even though it is not a direct DELETE
    target — see ``engine/maintenance/purge.py``'s ``PURGE_WHITELIST``)."""

    scenario_id: UUID
    name: str
    archived_at: _dt.datetime
    per_table_counts: dict[str, int]
    rows_total: int


class ShortageRetentionCandidateOut(BaseModel):
    """One scenario with resolved shortages old enough to retire."""

    scenario_id: UUID
    rows_to_delete: int


class PurgePreviewOut(BaseModel):
    """Dry-run report of both PURGE-1 sweeps. Nothing is written — this is a
    pure preview of ``engine/maintenance/purge.py``'s ``plan_fork_purge`` /
    ``plan_shortage_retention``."""

    ttl_days: int
    retention_days: int
    generated_at: _dt.datetime
    fork_purge_candidates: list[PurgeCandidateOut]
    fork_purge_rows_total: int
    shortage_retention_candidates: list[ShortageRetentionCandidateOut]
    shortage_retention_rows_total: int


@router.get(
    "/v1/maintenance/purge-preview",
    response_model=PurgePreviewOut,
    summary="Preview the PURGE-1 fork purge + shortage retention sweeps",
    description=(
        "Read-only dry-run of both maintenance sweeps: which archived, "
        "non-baseline scenarios are past their TTL (per-table row counts a "
        "purge would delete) and which scenarios hold long-resolved "
        "shortages past their retention window. Nothing is written. Requires "
        "the `admin` scope; gated by OOTILS_PURGE_ENABLED (default OFF)."
    ),
)
def purge_preview(
    _principal: Principal = Depends(require_scope("admin")),
    _enabled: None = Depends(require_purge_enabled),
    ttl_days: int = Query(default=7, ge=0, description="Archived-scenario TTL in days."),
    retention_days: int = Query(
        default=30, ge=0, description="Resolved-shortage retention in days."
    ),
    db: DictRowConnection = Depends(get_db),
) -> PurgePreviewOut:
    """Run both planners and report their output verbatim. Both are
    SELECT-only (``plan_fork_purge`` / ``plan_shortage_retention``) — this
    handler writes nothing and get_db's commit is a no-op on an unmodified
    connection."""
    fork_plan = plan_fork_purge(db, ttl_days)
    retention_plan = plan_shortage_retention(db, retention_days)

    logger.info(
        "maintenance.purge_preview ttl_days=%d retention_days=%d "
        "fork_candidates=%d fork_rows=%d retention_candidates=%d retention_rows=%d",
        ttl_days, retention_days,
        len(fork_plan.candidates), fork_plan.rows_total,
        len(retention_plan.candidates), retention_plan.rows_total,
    )

    return PurgePreviewOut(
        ttl_days=ttl_days,
        retention_days=retention_days,
        generated_at=fork_plan.generated_at,
        fork_purge_candidates=[
            PurgeCandidateOut(
                scenario_id=c.scenario_id,
                name=c.name,
                archived_at=c.archived_at,
                per_table_counts=c.per_table_counts,
                rows_total=c.rows_total,
            )
            for c in fork_plan.candidates
        ],
        fork_purge_rows_total=fork_plan.rows_total,
        shortage_retention_candidates=[
            ShortageRetentionCandidateOut(
                scenario_id=c.scenario_id, rows_to_delete=c.rows_to_delete
            )
            for c in retention_plan.candidates
        ],
        shortage_retention_rows_total=retention_plan.rows_total,
    )
