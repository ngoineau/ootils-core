"""
/v1/snapshots — inventory snapshot capture + query (chantier #393 A3-PR1, ADR-030).

The HTTP surface over the proof-machine historisation backbone
(``engine/snapshot/capture.py`` + ``inventory_snapshots``, migration 067):

  * ``POST /v1/snapshots`` captures on-hand per (item, location) for a scenario
    and idempotently upserts one row per coordinate for ``as_of`` (default
    CURRENT_DATE), source='api'. A re-POST for the same scenario/day overwrites,
    never duplicates (ON CONFLICT on the UNIQUE key).
  * ``GET /v1/snapshots`` reads captured rows back, filtered by scenario/as_of/
    item — fully parameterized SQL (no f-string WHERE).

SCOPES (chantier #392):
  * POST requires ``ingest`` — capturing a snapshot WRITES persistent operational
    rows into the system of record, the same class of action as ``/v1/ingest/*``
    (which holds ``ingest``). It is deliberately NOT ``read`` (a write must not
    ride a read scope) and NOT ``recommend:draft`` (a snapshot is a stock FACT,
    not a governed recommendation). Reusing the established write scope avoids
    minting a new ``snapshots:*`` vocabulary the token registry (migration 064)
    would have to learn in PR1; the legacy ``admin`` token satisfies ``ingest``,
    so no pre-#392 caller regresses.
  * GET requires ``read`` — a plain query path.

SCENARIO_ID ON EVERY PATH (North Star doctrine): both verbs resolve scenario_id
(``Depends(resolve_scenario_id)`` on POST; a required query filter on GET) so
the read path is forkable — an agent queries snapshots FROM a fork, not just
baseline. V1 captures baseline only (migration 067) but the surface is
scenario-scoped by construction.

Kill switch: ``OOTILS_SNAPSHOTS_ENABLED`` (default enabled). Falsy → 503 on the
capture verb, checked AFTER auth/scope but BEFORE the DB pool (an operational
escape hatch must not depend on the DB being healthy). Mirrors
``api/routers/drp.py``'s ``OOTILS_DRP_ENABLED`` / ``param_overrides.py``'s
overlay switch.
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.snapshot import capture_snapshot, persist_snapshot

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/snapshots", tags=["snapshots"])

_TRUTHY = {"1", "true", "yes", "on"}


def _snapshots_enabled() -> bool:
    """Kill switch, default ON. Falsy OOTILS_SNAPSHOTS_ENABLED -> 503."""
    return os.environ.get("OOTILS_SNAPSHOTS_ENABLED", "1").strip().lower() in _TRUTHY


def require_snapshots_enabled() -> None:
    """FastAPI dependency — checked AFTER auth/scope but BEFORE ``Depends(get_db)``
    in the capture endpoint (FastAPI resolves synchronous dependencies in
    signature order and short-circuits on the first HTTPException). Auth-first so
    an unauthenticated caller always gets 401 and cannot probe the switch state;
    kill-switch-before-DB so a disabled capturer answers 503 without touching the
    DB pool."""
    if not _snapshots_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Snapshot capture is disabled (OOTILS_SNAPSHOTS_ENABLED).",
        )


class SnapshotCaptureRequest(BaseModel):
    """Body of POST /v1/snapshots. scenario_id is resolved from the query param /
    X-Scenario-ID header (Depends(resolve_scenario_id)), NOT the body — matching
    the forkable read-path convention of the other scenario-scoped routers."""

    as_of: Optional[_dt.date] = Field(
        default=None,
        description="Capture day (YYYY-MM-DD). Defaults to the DB CURRENT_DATE.",
    )


class SnapshotCaptureResponse(BaseModel):
    """Response from a snapshot capture."""

    scenario_id: UUID
    as_of_date: _dt.date
    snapshots_captured: int


class SnapshotOut(BaseModel):
    """One captured coordinate row.

    ``first_shortage_date`` / ``shortage_severity_usd`` are NULL-honest: None
    means no projected shortage at this coordinate on as_of_date (enrichment is
    deferred to a later PR — captured None in PR1; the two are None together)."""

    snapshot_id: UUID
    scenario_id: UUID
    item_id: UUID
    location_id: UUID
    as_of_date: _dt.date
    on_hand_qty: float
    first_shortage_date: Optional[_dt.date] = None
    shortage_severity_usd: Optional[float] = None
    source: str
    captured_at: _dt.datetime


class SnapshotListResponse(BaseModel):
    scenario_id: UUID
    snapshots: list[SnapshotOut]
    total: int


def _row_to_out(row: dict) -> SnapshotOut:
    severity = row["shortage_severity_usd"]
    return SnapshotOut(
        snapshot_id=UUID(str(row["snapshot_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        item_id=UUID(str(row["item_id"])),
        location_id=UUID(str(row["location_id"])),
        as_of_date=row["as_of_date"],
        on_hand_qty=float(row["on_hand_qty"]),
        first_shortage_date=row["first_shortage_date"],
        shortage_severity_usd=float(severity) if severity is not None else None,
        source=row["source"],
        captured_at=row["captured_at"],
    )


@router.post(
    "",
    response_model=SnapshotCaptureResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Capture inventory snapshots",
    description=(
        "Scan on-hand per (item, location) for a scenario and upsert one "
        "`inventory_snapshots` row per coordinate for `as_of` (default "
        "CURRENT_DATE), source='api'. Idempotent: a re-capture of the same "
        "scenario/day overwrites, never duplicates. Per-site, never pooled. "
        "Requires the `ingest` scope (a write of persistent operational rows)."
    ),
)
def capture_snapshots(
    body: SnapshotCaptureRequest,
    _principal: Principal = Depends(require_scope("ingest")),
    _enabled: None = Depends(require_snapshots_enabled),
    scenario_id: UUID = Depends(resolve_scenario_id),
    db: DictRowConnection = Depends(get_db),
) -> SnapshotCaptureResponse:
    """Capture + persist inventory snapshots for a scenario.

    get_db owns commit/rollback. The engine's ``capture_snapshot`` is SELECT-only
    and ``persist_snapshot`` is the single idempotent upsert writer; this handler
    adds no SQL of its own on the write path."""
    scenario_str = str(scenario_id)

    rows = capture_snapshot(db, scenario_str, body.as_of, source="api")
    written = persist_snapshot(db, rows, source="api")

    # Resolve the effective as_of: the rows carry the DB CURRENT_DATE the scan
    # resolved when body.as_of was None. With zero on-hand coordinates there are
    # no rows to read it from, so fall back to the request value (which is the
    # only as_of the caller asked for) or the DB CURRENT_DATE.
    if rows:
        as_of_date = rows[0].as_of_date
    elif body.as_of is not None:
        as_of_date = body.as_of
    else:
        current = db.execute("SELECT CURRENT_DATE AS d").fetchone()
        if current is None:
            raise RuntimeError("snapshots.capture: SELECT CURRENT_DATE yielded no row")
        as_of_date = current["d"]

    logger.info(
        "snapshots.capture scenario_id=%s as_of=%s captured=%d",
        scenario_str, as_of_date, written,
    )

    return SnapshotCaptureResponse(
        scenario_id=scenario_id,
        as_of_date=as_of_date,
        snapshots_captured=written,
    )


@router.get(
    "",
    response_model=SnapshotListResponse,
    summary="Query inventory snapshots",
    description=(
        "Read captured snapshots filtered by scenario (required, forkable), "
        "optional as_of day and item_id. Fully parameterized SQL."
    ),
)
def list_snapshots(
    _principal: Principal = Depends(require_scope("read")),
    scenario_id: UUID = Depends(resolve_scenario_id),
    as_of: Optional[_dt.date] = Query(
        default=None, description="Filter to a single capture day (YYYY-MM-DD)."
    ),
    item_id: Optional[UUID] = Query(
        default=None, description="Filter to a single item."
    ),
    db: DictRowConnection = Depends(get_db),
) -> SnapshotListResponse:
    """Query snapshots for a scenario. scenario_id is always constrained (North
    Star: every read path is scenario-scoped). Optional as_of / item_id narrow
    the result. SQL is fully parameterized — every filter is a static ``col =
    %s`` predicate with its value bound positionally; the assembled WHERE string
    contains only hardcoded column names, never caller data (the house idiom,
    cf. recommendations.py / graph.py)."""
    conditions = ["scenario_id = %s"]
    params: list = [scenario_id]
    if as_of is not None:
        conditions.append("as_of_date = %s")
        params.append(as_of)
    if item_id is not None:
        conditions.append("item_id = %s")
        params.append(item_id)
    where_clause = "WHERE " + " AND ".join(conditions)

    rows = db.execute(
        f"""
        SELECT snapshot_id, scenario_id, item_id, location_id, as_of_date,
               on_hand_qty, first_shortage_date, shortage_severity_usd, source, captured_at
        FROM inventory_snapshots
        {where_clause}
        ORDER BY as_of_date DESC, item_id, location_id
        """,  # noqa: S608 — static columns, parameterized values
        params,
    ).fetchall()

    out = [_row_to_out(r) for r in rows]
    return SnapshotListResponse(scenario_id=scenario_id, snapshots=out, total=len(out))
