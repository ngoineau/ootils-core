"""
/v1/scenarios/{scenario_id}/param-overrides — REST surface over the scenario
planning-param overlay (chantier #347 PR4).

Thin wrapper over ``engine/scenario/param_overlay.py`` (set/clear/list
CRUD) — this router owns NO business logic beyond HTTP-shape mapping;
``ParamOverlayError`` (whitelist miss, illegal value, orphaned override
target, baseline scenario) is the single source of validation truth and is
mapped to 422 with ``detail=str(e)``. Same carve-out as
``api/routers/staging.py``'s ``DiffError``/``ApprovalError``/
``RejectionError``: every ``ParamOverlayError`` message is hand-authored in
``param_overlay.py`` from UUIDs/field names/enum values only — no DSN, no
raw psycopg exception text — so exposing it verbatim to the client is safe
and is part of the API contract (the client needs the field/UUID to act).

Decision Ladder: posing an overlay override is L0 — pure simulation, never
promoted onto the baseline (ADR-025 §5). No approval gate here; the human
gate that matters is on ``POST /v1/scenarios/{id}/promote``, and promote
never replays param overlays (ADR-025 §5 is a firm decision, not a TODO).

Kill switch: ``OOTILS_PARAM_OVERLAY_ENABLED`` (default enabled). Set to a
falsy value to return 503 on all three verbs — an operational escape hatch,
independent of auth/whitelist validation.
"""
from __future__ import annotations

import logging
import os
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import Principal, require_scope
from ootils_core.db.types import DictRowConnection
from ootils_core.api.dependencies import get_db
from ootils_core.engine.scenario.param_overlay import (
    ParamOverlayError,
    clear_param_override,
    list_param_overrides,
    set_param_override,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/scenarios", tags=["param-overrides"])

_TRUTHY = {"1", "true", "yes", "on"}


def _param_overlay_enabled() -> bool:
    """Kill switch, default ON. Falsy OOTILS_PARAM_OVERLAY_ENABLED -> 503."""
    return os.environ.get("OOTILS_PARAM_OVERLAY_ENABLED", "1").strip().lower() in _TRUTHY


def require_param_overlay_enabled() -> None:
    """FastAPI dependency — checked AFTER ``Depends(require_scope(...))`` but
    BEFORE ``Depends(get_db)`` in every endpoint's signature below (FastAPI resolves
    synchronous dependencies in signature order and short-circuits on the first
    HTTPException). Auth-first ensures an unauthenticated caller always gets 401
    and cannot distinguish 503-vs-401 to probe the kill switch's operational
    state. Kill-switch-before-DB ensures a disabled overlay short-circuits with
    503 without touching the DB pool — an operational kill switch must not
    itself depend on the DB being healthy."""
    if not _param_overlay_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Planning-param overlay is disabled (OOTILS_PARAM_OVERLAY_ENABLED).",
        )


class ParamOverrideIn(BaseModel):
    """Body of POST .../param-overrides — one scenario-scoped field override."""

    item_id: UUID
    location_id: Optional[UUID] = None
    field_name: str = Field(min_length=1)
    value: str = Field(min_length=1)
    applied_by: str = Field(min_length=1, max_length=200)


class ParamOverrideOut(BaseModel):
    override_id: UUID
    scenario_id: UUID
    item_id: UUID
    location_id: Optional[UUID] = None
    field_name: str
    value: str
    applied_at: str
    applied_by: str


class ParamOverridesListResponse(BaseModel):
    scenario_id: UUID
    overrides: list[ParamOverrideOut]
    total: int


def _row_to_out(row: dict) -> ParamOverrideOut:
    applied_at = row["applied_at"]
    return ParamOverrideOut(
        override_id=UUID(str(row["override_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        item_id=UUID(str(row["item_id"])),
        location_id=UUID(str(row["location_id"])) if row.get("location_id") else None,
        field_name=row["field_name"],
        value=row["value"],
        applied_at=applied_at.isoformat() if hasattr(applied_at, "isoformat") else str(applied_at),
        applied_by=row["applied_by"],
    )


@router.post(
    "/{scenario_id}/param-overrides",
    response_model=ParamOverrideOut,
    status_code=status.HTTP_201_CREATED,
)
def set_param_override_endpoint(
    scenario_id: UUID,
    body: ParamOverrideIn,
    _principal: Principal = Depends(require_scope("scenario:write")),
    _enabled: None = Depends(require_param_overlay_enabled),
    db: DictRowConnection = Depends(get_db),
) -> ParamOverrideOut:
    """Set (upsert) a scenario-scoped planning-param overlay override.

    L0 simulation-only write — no Decision Ladder gate, never promoted onto
    baseline (ADR-025 §5). Refused on the baseline scenario, on a
    non-whitelisted field, on an out-of-bounds value, or on an item/location
    with no current planning-params row — all via ParamOverlayError -> 422.
    """
    try:
        override_id = set_param_override(
            db,
            scenario_id=scenario_id,
            item_id=body.item_id,
            field_name=body.field_name,
            value=body.value,
            applied_by=body.applied_by,
            location_id=body.location_id,
        )
    except ParamOverlayError as e:
        # Carve-out (see module docstring): message is hand-authored from
        # UUIDs/field names/enum values only, never a raw DB error.
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(e)) from e

    row = db.execute(
        """
        SELECT override_id, scenario_id, item_id, location_id,
               field_name, value, applied_at, applied_by
        FROM scenario_planning_overrides
        WHERE override_id = %s
        """,
        (override_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            f"param_overrides.set: override {override_id} not found immediately after upsert"
        )
    logger.info(
        "param_overrides.set scenario_id=%s item_id=%s field_name=%s",
        scenario_id, body.item_id, body.field_name,
    )
    return _row_to_out(row)


@router.get(
    "/{scenario_id}/param-overrides",
    response_model=ParamOverridesListResponse,
)
def list_param_overrides_endpoint(
    scenario_id: UUID,
    _principal: Principal = Depends(require_scope("read")),
    _enabled: None = Depends(require_param_overlay_enabled),
    db: DictRowConnection = Depends(get_db),
) -> ParamOverridesListResponse:
    """List every planning-param overlay override for a scenario.

    A baseline scenario_id (or any scenario with no overrides) returns 200
    with an empty list — a baseline can never carry an override
    (set_param_override refuses it), so this is a legitimate empty result,
    not an error.
    """
    rows = list_param_overrides(db, scenario_id)
    logger.info("param_overrides.list scenario_id=%s count=%d", scenario_id, len(rows))
    return ParamOverridesListResponse(
        scenario_id=scenario_id,
        overrides=[_row_to_out(r) for r in rows],
        total=len(rows),
    )


@router.delete(
    "/{scenario_id}/param-overrides/{field_name}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def clear_param_override_endpoint(
    scenario_id: UUID,
    field_name: str,
    item_id: UUID = Query(...),
    location_id: Optional[UUID] = Query(default=None),
    _principal: Principal = Depends(require_scope("scenario:write")),
    _enabled: None = Depends(require_param_overlay_enabled),
    db: DictRowConnection = Depends(get_db),
) -> None:
    """Clear a scenario-scoped planning-param overlay override.

    Idempotent: a missing override is a no-op (still 204) — only an unknown
    field_name or an unknown scenario_id raises (422). No baseline special
    case: clearing on baseline is simply the no-op path (baseline can never
    hold an override).
    """
    try:
        deleted = clear_param_override(
            db,
            scenario_id=scenario_id,
            item_id=item_id,
            field_name=field_name,
            location_id=location_id,
        )
    except ParamOverlayError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(e)) from e

    logger.info(
        "param_overrides.clear scenario_id=%s item_id=%s field_name=%s deleted=%s",
        scenario_id, item_id, field_name, deleted,
    )
