"""
POST /v1/ingest/* — Batch import/upsert endpoints for supply chain master data.

Endpoints:
  POST /v1/ingest/items
  POST /v1/ingest/locations
  POST /v1/ingest/suppliers
  POST /v1/ingest/supplier-items
  POST /v1/ingest/on-hand
  POST /v1/ingest/purchase-orders
  POST /v1/ingest/forecast-demand

All endpoints accept JSON only (no TSV/CSV upload — MVP scope).
All DB operations use psycopg3 sync connections (same as other routers).

Behaviour contract (all 7 endpoints):
  - Validate ALL rows first (structural + FK). If ANY error → HTTP 422, nothing persisted.
  - dry_run: validation runs (including FK), but no DB writes; returns 200 with status="dry_run".

Authorization (ADR-042 PR-1): every ``POST /v1/ingest/*`` endpoint below
depends on ``require_direct_ingest`` (not a bare ``require_scope("ingest")``)
— same scope check, plus the ``OOTILS_DIRECT_INGEST_ENABLED`` kill switch
that fences direct ingest behind the governed daily-run pipeline for every
non-legacy caller. See ``require_direct_ingest``'s docstring for the full
rationale and the ``is_legacy`` exemption it relies on.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import date, timedelta
from typing import Any, Optional
from uuid import UUID, uuid4

from psycopg import sql
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field, field_validator

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.dq.engine import run_dq
from ootils_core.engine.graph_wiring import ensure_projection_series, wire_node_to_pi

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/ingest", tags=["ingest"])

_TRUTHY = {"1", "true", "yes", "on"}


def _log_safe(value: str) -> str:
    """Neutralize CR/LF before logging a caller-influenced value — a forged
    token name must not inject log lines (CodeQL py/log-injection; the
    explicit replace() chain is the sanitizer shape CodeQL's taint tracking
    recognizes — keep it that way). Same helper as bom.py's, duplicated
    rather than imported: each router owns its own log-hygiene helper, no
    cross-router coupling for a two-line sanitizer."""
    return str(value).replace("\r", "?").replace("\n", "?")[:200]


def _direct_ingest_enabled() -> bool:
    """Kill switch, DEFAULT ON. Falsy ``OOTILS_DIRECT_INGEST_ENABLED`` fences
    every direct ``POST /v1/ingest/*`` endpoint for non-legacy callers (see
    ``require_direct_ingest``) — ADR-042 decision 2.2. Default ON so nothing
    regresses out of the box (dev/CI/seed keep working unchanged); a governed
    production deployment sets it to ``0`` once the daily-run pipeline
    (Dropbox inbox, ``engine/ingest/daily_orchestrator.py``) is the intended
    entry point."""
    return os.environ.get("OOTILS_DIRECT_INGEST_ENABLED", "1").strip().lower() in _TRUTHY


def require_direct_ingest(
    principal: Principal = Depends(require_scope("ingest")),
) -> Principal:
    """FastAPI dependency for every direct ``POST /v1/ingest/*`` endpoint
    (ADR-042 PR-1, decision 2.2).

    Composes on top of ``require_scope("ingest")`` — authentication and the
    ``ingest`` scope check always run first, so a missing/invalid token or a
    principal without the scope still gets 401/403 exactly as before this
    dependency existed.

    ADR-042 fences *direct* ingest behind the governed daily-run pipeline
    (Dropbox inbox -> ``engine/ingest/daily_orchestrator.py``): a real
    ERP-facing caller is meant to land data through that pipeline, not by
    POSTing straight at ``/v1/ingest/<entity>``. When
    ``OOTILS_DIRECT_INGEST_ENABLED`` is falsy, direct ingest answers 503 for
    every caller EXCEPT the legacy principal (``principal.is_legacy``).

    Why the ``is_legacy`` exemption, specifically: it is the exact mechanism
    that keeps every in-process, TestClient-based caller working unchanged
    when the switch is flipped off in a governed deployment. Each of these
    resolves an ``Authorization: Bearer <OOTILS_API_TOKEN>`` header against
    the LEGACY single-token branch of auth.py's ``resolve_principal`` ->
    ``legacy_principal()`` (``auth.py:221``, ``is_legacy=True``, not a
    minted ``ootk_`` token with a real ``actor_kind``/scope row):
      * the governed daily-run orchestrator itself
        (``interfaces/ingest_exec.py:call_api``, called by
        ``engine/ingest/daily_orchestrator.py`` — the pipeline this fence
        exists to funnel callers TOWARDS must still be able to write);
      * ``scripts/ingest_file.py`` (manual/dev TSV drop, same ``call_api``);
      * seeding/demo TestClient callers (``scripts/demo_e2e.py``,
        ``ootils_core/demo/phase1.py``) and the test suite's own
        ``TestClient`` fixtures.
    A minted per-agent/service token is NOT exempt — only the legacy
    bootstrap credential is, which is the credential every one of the
    callers above actually presents. Falsy switch + non-legacy principal ->
    503 (never a silent 200), with a detail naming the intended path so a
    caller hitting this in error can self-correct."""
    if not _direct_ingest_enabled() and not principal.is_legacy:
        logger.warning(
            "ingest.direct_disabled name=%s token_id=%s actor_kind=%s",
            _log_safe(principal.name),
            principal.token_id,
            principal.actor_kind,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "direct ingest disabled — use the governed daily-run "
                "pipeline (Dropbox inbox)"
            ),
        )
    return principal


# ─────────────────────────────────────────────────────────────
# Shared response models
# ─────────────────────────────────────────────────────────────

class IngestSummary(BaseModel):
    total: int
    inserted: int
    updated: int
    errors: int


class IngestResponse(BaseModel):
    status: str
    summary: IngestSummary
    results: list[dict]
    batch_id: Optional[UUID] = None
    dq_status: Optional[str] = None
    # Number of location_aliases rows upserted (#414). Defaults to 0 so every
    # non-location endpoint and every pre-#414 client keep an unchanged
    # payload — additive, optional, never breaks the existing contract.
    aliases_upserted: int = 0


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _ok(inserted: int, updated: int, total: int, results: list[dict], batch_id: UUID | None = None, dq_status: str | None = None, aliases_upserted: int = 0) -> IngestResponse:
    return IngestResponse(
        status="ok",
        summary=IngestSummary(total=total, inserted=inserted, updated=updated, errors=0),
        results=results,
        batch_id=batch_id,
        dq_status=dq_status,
        aliases_upserted=aliases_upserted,
    )


def _dry_run_response(items: list[Any], label: str = "external_id") -> IngestResponse:
    return IngestResponse(
        status="dry_run",
        summary=IngestSummary(total=len(items), inserted=0, updated=0, errors=0),
        results=[{"action": "dry_run", label: getattr(row, label, "?")} for row in items],
    )


def _raise_422(errors: list[dict]) -> None:
    """Raise HTTP 422 with structured error list. Nothing is persisted."""
    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=errors)


def _idempotency_key_from_request(request: Request, dry_run: bool) -> str | None:
    if dry_run:
        return None

    key = request.headers.get("Idempotency-Key")
    if key is None:
        return None

    key = key.strip()
    if not key:
        return None
    if len(key) > 128:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Idempotency-Key must be 128 characters or fewer",
        )
    return key


def _request_hash(body: BaseModel) -> str:
    normalized = json.dumps(
        body.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _load_idempotent_response(
    db: DictRowConnection,
    entity_type: str,
    request: Request,
    response: Response,
    body: BaseModel,
) -> tuple[str | None, str | None, IngestResponse | None]:
    idempotency_key = _idempotency_key_from_request(request, getattr(body, "dry_run", False))
    if idempotency_key is None:
        return None, None, None

    request_hash = _request_hash(body)
    row = db.execute(
        """
        SELECT entity_type, request_hash, response_json
        FROM ingest_batches
        WHERE idempotency_key = %s
        """,
        (idempotency_key,),
    ).fetchone()

    if row is None:
        return idempotency_key, request_hash, None

    if row["entity_type"] != entity_type or row["request_hash"] != request_hash:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "idempotency.conflict",
                "message": "Idempotency-Key already used with a different ingest payload.",
            },
        )

    if not row["response_json"]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "idempotency.pending",
                "message": "Idempotency-Key already reserved by an in-flight ingest request.",
            },
        )

    response.headers["X-Idempotent-Replay"] = "true"
    return idempotency_key, request_hash, IngestResponse.model_validate_json(row["response_json"])


def _create_ingest_batch(
    db: DictRowConnection,
    entity_type: str,
    rows_data: list[Any],
    source_system: str = "ingest_api",
    submitted_by: str = "ingest_api",
    idempotency_key: str | None = None,
    request_hash: str | None = None,
    correlation_id: str | None = None,
) -> UUID:
    """
    Create an ingest_batch record and persist all rows as ingest_rows.
    Returns the new batch_id.
    """
    import json as _json
    batch_id = uuid4()
    try:
        with db.transaction():
            db.execute(
                """
                INSERT INTO ingest_batches
                    (
                        batch_id, entity_type, source_system, status, total_rows, submitted_by,
                        idempotency_key, request_hash, correlation_id
                    )
                VALUES (%s, %s, %s, 'processing', %s, %s, %s, %s, %s)
                """,
                (
                    batch_id,
                    entity_type,
                    source_system,
                    len(rows_data),
                    submitted_by,
                    idempotency_key,
                    request_hash,
                    correlation_id,
                ),
            )
    except Exception as exc:
        if idempotency_key is not None and getattr(exc, "sqlstate", None) == "23505":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "idempotency.pending",
                    "message": "Idempotency-Key already reserved by another ingest request.",
                },
            ) from exc
        raise

    for i, row in enumerate(rows_data):
        raw = _json.dumps(row if isinstance(row, dict) else row.model_dump(), default=str)
        db.execute(
            """
            INSERT INTO ingest_rows (row_id, batch_id, row_number, raw_content)
            VALUES (%s, %s, %s, %s)
            """,
            (uuid4(), batch_id, i + 1, raw),
        )
    return batch_id


def _finalize_ingest_batch(
    db: DictRowConnection,
    batch_id: UUID,
    response_payload: IngestResponse,
) -> None:
    db.execute(
        """
        UPDATE ingest_batches
        SET status = 'imported',
            processed_at = now(),
            imported_at = now(),
            response_json = %s
        WHERE batch_id = %s
        """,
        (response_payload.model_dump_json(), batch_id),
    )


def _trigger_dq(db: DictRowConnection, batch_id: UUID) -> str:
    """Run DQ pipeline on a batch. Returns dq_status string, never raises."""
    try:
        with db.transaction():
            result = run_dq(db, batch_id)
        return result.batch_dq_status
    except Exception as exc:
        logger.warning("DQ run failed for batch %s: %s", batch_id, exc)
        return "unknown"


# _ensure_projection_series / _wire_node_to_pi moved to
# ootils_core.engine.graph_wiring (DESC-1 PR-B, #477) so the demand-descent
# run can wire its derived per-DC nodes through the SAME code, never a second
# writer of the same graph-structural invariant. Re-exported here unchanged
# (same names, same signatures, same objects) so every existing caller/test
# in this file keeps working byte-for-byte — see graph_wiring.py's module
# docstring for the full rationale.
_ensure_projection_series = ensure_projection_series
_wire_node_to_pi = wire_node_to_pi


def _emit_ingestion_event(db: DictRowConnection, scenario_id: UUID, node_id: UUID) -> None:
    """Create an unprocessed ingestion_complete event to trigger recalculation."""
    from datetime import datetime, timezone
    db.execute(
        """
        INSERT INTO events (event_id, event_type, scenario_id, trigger_node_id, processed, source, created_at)
        VALUES (%s, 'ingestion_complete', %s, %s, FALSE, 'ingestion', %s)
        """,
        (uuid4(), scenario_id, node_id, datetime.now(timezone.utc)),
    )


_ALLOWED_TABLES = {"items", "locations", "suppliers", "supplier_items", "resources"}
_ALLOWED_COLUMNS = {
    "external_id", "item_id", "location_id", "supplier_id",
    "supplier_item_id", "resource_id",
}


def _batch_existing(
    db: DictRowConnection,
    table: str,
    id_col: str,
    pk_col: str,
    external_ids: list[str],
) -> dict[str, UUID]:
    """Return {external_id: pk} for all rows matching the given external_ids.

    Uses psycopg.sql.Identifier for table/column names to prevent SQL injection.
    Table and column names are also validated against allowlists.
    """
    if not external_ids:
        return {}
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Table '{table}' not in allowlist")
    if id_col not in _ALLOWED_COLUMNS or pk_col not in _ALLOWED_COLUMNS:
        raise ValueError(f"Column '{id_col}' or '{pk_col}' not in allowlist")
    query = sql.SQL("SELECT {id_col}, {pk_col} FROM {table} WHERE {id_col} = ANY(%s)").format(
        id_col=sql.Identifier(id_col),
        pk_col=sql.Identifier(pk_col),
        table=sql.Identifier(table),
    )
    rows = db.execute(query, (external_ids,)).fetchall()
    return {r[id_col]: r[pk_col] for r in rows}


# ─────────────────────────────────────────────────────────────
# 1. POST /v1/ingest/items
# ─────────────────────────────────────────────────────────────

VALID_ITEM_TYPES = {"finished_good", "component", "raw_material", "semi_finished"}
VALID_ITEM_STATUSES = {"active", "obsolete", "phase_out"}


class ItemRow(BaseModel):
    external_id: str = Field(..., description="Unique business identifier (e.g. ERP SKU). Upsert key.")
    name: str = Field(..., description="Item label / description.")
    item_type: str = Field("finished_good", description="Item type. Values: finished_good | component | raw_material | semi_finished.")
    uom: str = Field("EA", description="Base unit of measure (e.g. EA, KG, BOX).")
    status: str = Field("active", description="Item status. Values: active | obsolete | phase_out.")

    @field_validator("external_id", "name")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v


class IngestItemsRequest(BaseModel):
    items: list[ItemRow]
    dry_run: bool = False


@router.post("/items", response_model=IngestResponse, summary="Import items", description="Upsert a batch of items. Upsert key: external_id.")
def ingest_items(
    body: IngestItemsRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert items by external_id. All-or-nothing: any validation error → HTTP 422."""
    errors: list[dict] = []

    for i, item in enumerate(body.items):
        row_errs = []
        if item.item_type not in VALID_ITEM_TYPES:
            row_errs.append(f"item_type '{item.item_type}' invalid; valid: {sorted(VALID_ITEM_TYPES)}")
        if item.status not in VALID_ITEM_STATUSES:
            row_errs.append(f"status '{item.status}' invalid; valid: {sorted(VALID_ITEM_STATUSES)}")
        if row_errs:
            errors.append({"external_id": item.external_id, "row": i, "errors": row_errs})

    if errors:
        _raise_422(errors)

    if body.dry_run:
        return _dry_run_response(body.items)

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "items", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "items",
        body.items,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    # Batch-fetch existing items
    existing = _batch_existing(
        db, "items", "external_id", "item_id",
        [it.external_id for it in body.items],
    )

    results: list[dict] = []
    inserted = updated = 0

    for item in body.items:
        if item.external_id in existing:
            db.execute(
                """
                UPDATE items
                SET name = %s, item_type = %s, uom = %s, status = %s, updated_at = now()
                WHERE external_id = %s
                """,
                (item.name, item.item_type, item.uom, item.status, item.external_id),
            )
            results.append({"external_id": item.external_id, "item_id": str(existing[item.external_id]), "action": "updated"})
            updated += 1
        else:
            item_id = uuid4()
            db.execute(
                """
                INSERT INTO items (item_id, external_id, name, item_type, uom, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (item_id, item.external_id, item.name, item.item_type, item.uom, item.status),
            )
            results.append({"external_id": item.external_id, "item_id": str(item_id), "action": "inserted"})
            inserted += 1

    logger.info("ingest.items total=%d inserted=%d updated=%d", len(body.items), inserted, updated)
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.items), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 2. POST /v1/ingest/locations
# ─────────────────────────────────────────────────────────────

VALID_LOCATION_TYPES = {"plant", "dc", "warehouse", "supplier_virtual", "customer_virtual"}


class LocationAliasRow(BaseModel):
    """One alternate source-system code for a site (location_aliases,
    migration 070 — #414/ADR-031). Mirrors the DB CHECK
    (alias <> '' AND btrim(alias) = alias): the alias is stripped and a
    blank one is rejected at the Pydantic boundary so a malformed code fails
    at 422 rather than at the DB CHECK. ``source_system`` defaults to the
    ''_default'' sentinel (never nullable — see migration 070 header)."""

    alias: str = Field(
        ...,
        min_length=1,
        description="Alternate code as it appears in a source feed (e.g. numeric ERP warehouse code).",
    )
    source_system: str = Field(
        "_default",
        description="Origin system of this code; part of the UNIQUE (alias, source_system) key.",
    )

    @field_validator("alias")
    @classmethod
    def alias_non_blank(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("alias must not be blank")
        return stripped


class LocationRow(BaseModel):
    external_id: str = Field(..., description="Site/DC identifier (e.g. DC-ATL). Upsert key.")
    name: str = Field(..., description="Site label / description.")
    location_type: str = Field("dc", description="Location type. Values: plant | dc | warehouse | supplier_virtual | customer_virtual.")
    country: Optional[str] = None
    timezone: Optional[str] = None
    parent_external_id: Optional[str] = Field(None, description="External_id of the parent site (optional, for hierarchies).")
    # Backward-compatible: absent → [] → no alias work, identical behaviour to
    # a pre-#414 payload (location_aliases, migration 070).
    aliases: list[LocationAliasRow] = Field(
        default_factory=list,
        description="Optional alternate source-system codes resolving to this site (#414).",
    )

    @field_validator("external_id", "name")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v


class IngestLocationsRequest(BaseModel):
    locations: list[LocationRow]
    dry_run: bool = False


def _upsert_location_aliases(
    db: DictRowConnection,
    location_id: UUID,
    loc: LocationRow,
) -> int:
    """Upsert a site's aliases into location_aliases (migration 070, #414).

    ON CONFLICT (alias, source_system) DO UPDATE SET location_id — NOT
    DO NOTHING. Rationale: location_aliases is a CORRESPONDENCE table and a
    correspondence gets corrected. Ingest is declared an upsert (the normal
    correction channel for master data), so re-sending an alias that was
    previously mapped to the wrong site must re-point it to the right one —
    DO NOTHING would freeze the first (possibly wrong) mapping and force an
    out-of-band DELETE to fix a typo. The cross-site anti-collision guard in
    the caller already blocks the only dangerous re-map (an alias equal to a
    LIVE external_id of another site); what remains re-mappable here is an
    alias moving between sites via successive ingests, which is exactly the
    correction we want to allow.

    An alias equal to the site's OWN external_id is skipped (silent no-op):
    the site already resolves to that code through locations.external_id, so
    the row would be redundant. Returns the number of alias rows written.
    """
    written = 0
    for alias_row in loc.aliases:
        if alias_row.alias == loc.external_id:
            continue
        db.execute(
            """
            INSERT INTO location_aliases (location_id, alias, source_system)
            VALUES (%s, %s, %s)
            ON CONFLICT (alias, source_system)
            DO UPDATE SET location_id = EXCLUDED.location_id
            """,
            (location_id, alias_row.alias, alias_row.source_system),
        )
        written += 1
    return written


@router.post("/locations", response_model=IngestResponse, summary="Import locations", description="Upsert a batch of sites/DCs. Upsert key: external_id.")
def ingest_locations(
    body: IngestLocationsRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert locations by external_id. All-or-nothing: any validation error → HTTP 422."""
    errors: list[dict] = []

    # Build set of external_ids in the payload (for parent validation)
    payload_ext_ids = {loc.external_id for loc in body.locations}

    # Site identity of every payload external_id ALREADY in DB (empty →
    # brand-new site). Resolved once, up front — validation (Trou A/B below)
    # AND the upsert loop both need "does this external_id already have a
    # location_id, and which one" to tell "this is genuinely the same site"
    # apart from "this collides with a different, pre-existing site". A
    # single batched ANY(%s) query (_batch_existing), reused by both phases —
    # never re-fetched later.
    existing = _batch_existing(
        db, "locations", "external_id", "location_id",
        [loc.external_id for loc in body.locations],
    )

    # Cross-site alias anti-collision (#414/ADR-031). Applicative invariant:
    # ONE code -> EXACTLY ONE site, ACROSS ALL SOURCE SYSTEMS —
    # demand_history.warehouse_id carries no system tag, so the UNION
    # resolution (_warehouse_codes_subquery) is itself system-agnostic; two
    # sites sharing a code under different systems would double-count demand
    # at read time. Not expressible as a single-table DB constraint (see
    # migration 070 header) — the ingest layer owns it, batched, before any
    # write (all-or-nothing intact).
    #
    # One combined batched lookup serves BOTH known holes below: every
    # location_aliases row whose `alias` equals EITHER a payload external_id
    # (Trou A candidate) OR a payload alias string (Trou B candidate), any
    # source_system, in ONE query (no N+1).
    alias_strings = {a.alias for loc in body.locations for a in loc.aliases}
    collision_candidates = list(payload_ext_ids | alias_strings)
    db_alias_rows_by_alias: dict[str, list[tuple[str, UUID]]] = {}
    if collision_candidates:
        alias_rows = db.execute(
            "SELECT alias, source_system, location_id FROM location_aliases WHERE alias = ANY(%s)",
            (collision_candidates,),
        ).fetchall()
        for r in alias_rows:
            db_alias_rows_by_alias.setdefault(r["alias"], []).append(
                (r["source_system"], r["location_id"])
            )

    # Pre-existing check (alias == another site's external_id, DB side):
    # batch-fetch, in one query, every `locations` row whose external_id
    # equals an INCOMING alias string — never a per-row lookup.
    alias_ext_id_owner: dict[str, str] = {}
    if alias_strings:
        ext_id_rows = db.execute(
            "SELECT external_id FROM locations WHERE external_id = ANY(%s)",
            (list(alias_strings),),
        ).fetchall()
        alias_ext_id_owner = {r["external_id"]: r["external_id"] for r in ext_id_rows}

    # Intra-payload alias anti-collision — judged on the CHAIN (the alias
    # STRING alone), NOT the (alias, source_system) pair. Same invariant class
    # as Trou A/B above: demand_history.warehouse_id carries no system tag, so
    # a code declared by two DIFFERENT sites in one batch is ambiguous
    # regardless of which source_system(s) each side used — the per-system
    # UNIQUE key on location_aliases would happily store both rows (different
    # keys) and silently create the exact two-resolutions corruption this
    # whole feature exists to prevent. owner = the FIRST site declaring the
    # code (setdefault); ANY other site later declaring the SAME code, under
    # ANY source_system, is the ambiguity and gets rejected below.
    #
    # Chain-level ownership alone also fully covers the same-site no-op: the
    # SAME site declaring the same code under several systems (the legitimate
    # multi-flux case), or repeating the exact same pair, all resolve to that
    # site's own external_id here — no separate pair-level structure is
    # needed to distinguish them.
    alias_string_owner: dict[str, str] = {}
    for loc in body.locations:
        for alias_row in loc.aliases:
            alias_string_owner.setdefault(alias_row.alias, loc.external_id)

    for i, loc in enumerate(body.locations):
        row_errs = []
        if loc.location_type not in VALID_LOCATION_TYPES:
            row_errs.append(
                f"location_type '{loc.location_type}' invalid; valid: {sorted(VALID_LOCATION_TYPES)}"
            )
        if loc.parent_external_id and loc.parent_external_id not in payload_ext_ids:
            # Check if parent exists in DB
            parent_in_db = db.execute(
                "SELECT 1 FROM locations WHERE external_id = %s",
                (loc.parent_external_id,),
            ).fetchone()
            if not parent_in_db:
                row_errs.append(
                    f"parent_external_id '{loc.parent_external_id}' not found in payload or DB"
                )

        # Trou A: the INCOMING external_id (new site, or an existing site
        # being updated) equals an alias ALREADY IN DB that belongs to a
        # DIFFERENT site. `existing.get(loc.external_id)` is None for a
        # brand-new site, so any DB alias hit is automatically "a different
        # site" (a not-yet-existing site cannot legitimately own a prior
        # alias row).
        own_location_id = existing.get(loc.external_id)
        for source_system, owner_location_id in db_alias_rows_by_alias.get(loc.external_id, []):
            if owner_location_id != own_location_id:
                row_errs.append(
                    f"external_id '{loc.external_id}' collides with an "
                    f"existing alias (source_system '{source_system}') already "
                    f"pointing to a different site (location_id "
                    f"'{owner_location_id}'); a code must resolve to exactly "
                    "one site"
                )

        for alias_row in loc.aliases:
            alias = alias_row.alias
            # alias == the site's OWN external_id → silent no-op (documented):
            # the site already resolves to that code via locations.external_id,
            # so an alias row would be redundant, never a collision.
            if alias == loc.external_id:
                continue
            # alias == a DIFFERENT site's external_id (in this payload or
            # already in the DB) → the alias would resolve to two distinct
            # sites: reject the row (fail-loudly).
            collides_with = None
            if alias in payload_ext_ids:
                collides_with = alias
            elif alias in alias_ext_id_owner:
                collides_with = alias_ext_id_owner[alias]
            if collides_with is not None:
                row_errs.append(
                    f"alias '{alias}' collides with the external_id of another "
                    f"site '{collides_with}' (target site '{loc.external_id}'); "
                    "an alias must not equal another site's external_id"
                )
            # Chain-level intra-payload collision: this alias STRING already
            # claimed by a DIFFERENT site elsewhere in this payload, under
            # ANY source_system → same ambiguity class as Trou A/B, reject.
            # Same site re-declaring its own code (any system, incl. a
            # DIFFERENT one from a prior row — the legitimate multi-flux
            # case) resolves to itself in alias_string_owner (first-declarer
            # is that same site) and is never an error.
            chain_owner_ext_id = alias_string_owner[alias]
            if chain_owner_ext_id != loc.external_id:
                row_errs.append(
                    f"alias '{alias}' (source_system '{alias_row.source_system}') "
                    f"is declared by two different sites in this batch: "
                    f"'{chain_owner_ext_id}' and '{loc.external_id}'; a code "
                    "must resolve to exactly one site within a batch, "
                    "regardless of source_system"
                )

            # Trou B: the INCOMING alias equals a DB alias, under a
            # DIFFERENT source_system, already pointing to a DIFFERENT site.
            # System-agnostic invariant: demand_history.warehouse_id carries
            # no system tag, so a code owned by two sites under two systems
            # is a real ambiguity. The EXACT (alias, source_system) pair is
            # deliberately excluded here — that is the permitted re-map
            # handled by _upsert_location_aliases's ON CONFLICT DO UPDATE
            # (an assumed correction, even when it moves the alias to a new
            # site). Same alias, same SITE, different source_system (the
            # legitimate multi-system-per-site case) is also excluded by the
            # `owner_location_id == own_location_id` check.
            for other_source_system, owner_location_id in db_alias_rows_by_alias.get(alias, []):
                if other_source_system == alias_row.source_system:
                    continue
                if owner_location_id != own_location_id:
                    row_errs.append(
                        f"alias '{alias}' (source_system "
                        f"'{alias_row.source_system}') collides with the same "
                        f"alias already registered under source_system "
                        f"'{other_source_system}' pointing to a different "
                        f"site (location_id '{owner_location_id}'); a code "
                        "must resolve to exactly one site across all source "
                        "systems"
                    )
        if row_errs:
            errors.append({"external_id": loc.external_id, "row": i, "errors": row_errs})

    if errors:
        _raise_422(errors)

    if body.dry_run:
        return _dry_run_response(body.locations)

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "locations", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "locations",
        body.locations,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    # `existing` was already resolved above (before validation, batched) and
    # is reused here — no re-fetch.
    results: list[dict] = []
    inserted = updated = 0
    aliases_upserted = 0

    for loc in body.locations:
        if loc.external_id in existing:
            location_id = existing[loc.external_id]
            # `locations` table has no `updated_at` column (see migration 002:
            # only `created_at`). Older code copy-pasted the `items` UPDATE
            # template and crashed with UndefinedColumn at runtime.
            db.execute(
                """
                UPDATE locations
                SET name = %s, location_type = %s, country = %s, timezone = %s
                WHERE external_id = %s
                """,
                (loc.name, loc.location_type, loc.country, loc.timezone, loc.external_id),
            )
            action = "updated"
            updated += 1
        else:
            location_id = uuid4()
            db.execute(
                """
                INSERT INTO locations (location_id, external_id, name, location_type, country, timezone)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (location_id, loc.external_id, loc.name, loc.location_type, loc.country, loc.timezone),
            )
            action = "inserted"
            inserted += 1

        row_aliases = _upsert_location_aliases(db, location_id, loc)
        aliases_upserted += row_aliases
        result_row: dict[str, Any] = {
            "external_id": loc.external_id,
            "location_id": str(location_id),
            "action": action,
        }
        if loc.aliases:
            result_row["aliases_upserted"] = row_aliases
        results.append(result_row)

    logger.info(
        "ingest.locations total=%d inserted=%d updated=%d aliases_upserted=%d",
        len(body.locations), inserted, updated, aliases_upserted,
    )
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(
        inserted, updated, len(body.locations), results,
        batch_id=batch_id, dq_status=dq_status, aliases_upserted=aliases_upserted,
    )
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 3. POST /v1/ingest/suppliers
# ─────────────────────────────────────────────────────────────

VALID_SUPPLIER_STATUSES = {"active", "inactive", "blocked"}


class SupplierRow(BaseModel):
    external_id: str = Field(..., description="ERP supplier code. Upsert key.")
    name: str = Field(..., description="Legal name.")
    # W-06: lead_time_days must be > 0 when provided
    lead_time_days: Optional[int] = Field(None, gt=0, description="Standard lead time in calendar days.")
    reliability_score: Optional[float] = Field(None, description="Reliability score [0.0–1.0]. 1.0 = perfect.")
    country: Optional[str] = None
    status: str = "active"

    @field_validator("external_id", "name")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v


class IngestSuppliersRequest(BaseModel):
    suppliers: list[SupplierRow]
    dry_run: bool = False


@router.post("/suppliers", response_model=IngestResponse, summary="Import suppliers", description="Upsert a batch of suppliers.")
def ingest_suppliers(
    body: IngestSuppliersRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert suppliers by external_id. All-or-nothing: any validation error → HTTP 422."""
    errors: list[dict] = []

    for i, sup in enumerate(body.suppliers):
        row_errs = []
        if sup.status not in VALID_SUPPLIER_STATUSES:
            row_errs.append(f"status '{sup.status}' invalid; valid: {sorted(VALID_SUPPLIER_STATUSES)}")
        if sup.reliability_score is not None and not (0.0 <= sup.reliability_score <= 1.0):
            row_errs.append(f"reliability_score {sup.reliability_score} must be in [0, 1]")
        if row_errs:
            errors.append({"external_id": sup.external_id, "row": i, "errors": row_errs})

    if errors:
        _raise_422(errors)

    if body.dry_run:
        return _dry_run_response(body.suppliers)

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "suppliers", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "suppliers",
        body.suppliers,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    existing = _batch_existing(
        db, "suppliers", "external_id", "supplier_id",
        [s.external_id for s in body.suppliers],
    )

    results: list[dict] = []
    inserted = updated = 0

    for sup in body.suppliers:
        if sup.external_id in existing:
            db.execute(
                """
                UPDATE suppliers
                SET name = %s, lead_time_days = %s, reliability_score = %s,
                    country = %s, status = %s, updated_at = now()
                WHERE external_id = %s
                """,
                (sup.name, sup.lead_time_days, sup.reliability_score, sup.country, sup.status, sup.external_id),
            )
            results.append({"external_id": sup.external_id, "supplier_id": str(existing[sup.external_id]), "action": "updated"})
            updated += 1
        else:
            supplier_id = uuid4()
            db.execute(
                """
                INSERT INTO suppliers (supplier_id, external_id, name, lead_time_days, reliability_score, country, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (supplier_id, sup.external_id, sup.name, sup.lead_time_days, sup.reliability_score, sup.country, sup.status),
            )
            results.append({"external_id": sup.external_id, "supplier_id": str(supplier_id), "action": "inserted"})
            inserted += 1

    logger.info("ingest.suppliers total=%d inserted=%d updated=%d", len(body.suppliers), inserted, updated)
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.suppliers), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 4. POST /v1/ingest/supplier-items
# ─────────────────────────────────────────────────────────────

class SupplierItemRow(BaseModel):
    supplier_external_id: str
    item_external_id: str
    # W-06: lead_time_days must be > 0
    lead_time_days: int = Field(..., gt=0)
    moq: Optional[float] = None
    unit_cost: Optional[float] = None
    is_preferred: bool = False
    currency: str = "EUR"


class IngestSupplierItemsRequest(BaseModel):
    supplier_items: list[SupplierItemRow]
    dry_run: bool = False


@router.post("/supplier-items", response_model=IngestResponse, summary="Import supplier items", description="Upsert supply conditions per (supplier × item) pair.")
def ingest_supplier_items(
    body: IngestSupplierItemsRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert supplier_items by (supplier_id, item_id). All-or-nothing: any error → HTTP 422."""
    # W-01: resolve FKs first, collect ALL errors before any write
    sup_ext_ids = list({si.supplier_external_id for si in body.supplier_items})
    item_ext_ids = list({si.item_external_id for si in body.supplier_items})

    supplier_map = _batch_existing(db, "suppliers", "external_id", "supplier_id", sup_ext_ids)
    item_map = _batch_existing(db, "items", "external_id", "item_id", item_ext_ids)

    errors: list[dict] = []
    for i, si in enumerate(body.supplier_items):
        row_errs = []
        if si.supplier_external_id not in supplier_map:
            row_errs.append(f"supplier_external_id '{si.supplier_external_id}' not found in DB")
        if si.item_external_id not in item_map:
            row_errs.append(f"item_external_id '{si.item_external_id}' not found in DB")
        if row_errs:
            errors.append({
                "supplier_external_id": si.supplier_external_id,
                "item_external_id": si.item_external_id,
                "row": i,
                "errors": row_errs,
            })

    # W-01+W-02: if any error → 422, nothing persisted
    if errors:
        _raise_422(errors)

    if body.dry_run:
        results: list[dict] = [
            {
                "supplier_external_id": si.supplier_external_id,
                "item_external_id": si.item_external_id,
                "action": "dry_run",
            }
            for si in body.supplier_items
        ]
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.supplier_items), inserted=0, updated=0, errors=0),
            results=results,
        )

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "supplier_items", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "supplier_items",
        body.supplier_items,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    results = []
    inserted = updated = 0

    for si in body.supplier_items:
        supplier_id = supplier_map[si.supplier_external_id]
        item_id = item_map[si.item_external_id]

        # Check if (supplier_id, item_id) already exists
        existing = db.execute(
            "SELECT supplier_item_id FROM supplier_items WHERE supplier_id = %s AND item_id = %s",
            (supplier_id, item_id),
        ).fetchone()

        if existing:
            db.execute(
                """
                UPDATE supplier_items
                SET lead_time_days = %s, moq = %s, unit_cost = %s,
                    is_preferred = %s, currency = %s, updated_at = now()
                WHERE supplier_id = %s AND item_id = %s
                """,
                (si.lead_time_days, si.moq, si.unit_cost, si.is_preferred, si.currency, supplier_id, item_id),
            )
            results.append({
                "supplier_external_id": si.supplier_external_id,
                "item_external_id": si.item_external_id,
                "supplier_item_id": str(existing["supplier_item_id"]),
                "action": "updated",
            })
            updated += 1
        else:
            supplier_item_id = uuid4()
            db.execute(
                """
                INSERT INTO supplier_items
                    (supplier_item_id, supplier_id, item_id, lead_time_days, moq, unit_cost, is_preferred, currency)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (supplier_item_id, supplier_id, item_id, si.lead_time_days, si.moq, si.unit_cost, si.is_preferred, si.currency),
            )
            results.append({
                "supplier_external_id": si.supplier_external_id,
                "item_external_id": si.item_external_id,
                "supplier_item_id": str(supplier_item_id),
                "action": "inserted",
            })
            inserted += 1

    logger.info(
        "ingest.supplier_items total=%d inserted=%d updated=%d",
        len(body.supplier_items), inserted, updated,
    )
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.supplier_items), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 5. POST /v1/ingest/on-hand
# ─────────────────────────────────────────────────────────────

class OnHandRow(BaseModel):
    item_external_id: str
    location_external_id: str
    quantity: float = Field(..., ge=0, description="Available stock quantity (>= 0).")
    uom: str = "EA"
    as_of_date: date


class IngestOnHandRequest(BaseModel):
    on_hand: list[OnHandRow]
    dry_run: bool = False


@router.post("/on-hand", response_model=IngestResponse, summary="Import on-hand stock", description="Upsert available stock (OnHandSupply) per (item × location).")
def ingest_on_hand(
    body: IngestOnHandRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert OnHandSupply nodes in the baseline scenario. All-or-nothing: any error → HTTP 422."""
    # W-01: resolve FKs first, collect ALL errors before any write
    item_ext_ids = list({r.item_external_id for r in body.on_hand})
    loc_ext_ids = list({r.location_external_id for r in body.on_hand})

    item_map = _batch_existing(db, "items", "external_id", "item_id", item_ext_ids)
    loc_map = _batch_existing(db, "locations", "external_id", "location_id", loc_ext_ids)

    errors: list[dict] = []
    for i, row in enumerate(body.on_hand):
        row_errs = []
        if row.item_external_id not in item_map:
            row_errs.append(f"item_external_id '{row.item_external_id}' not found in DB")
        if row.location_external_id not in loc_map:
            row_errs.append(f"location_external_id '{row.location_external_id}' not found in DB")
        if row_errs:
            errors.append({
                "item_external_id": row.item_external_id,
                "location_external_id": row.location_external_id,
                "row": i,
                "errors": row_errs,
            })

    # W-01+W-02: if any FK error → 422, nothing persisted
    if errors:
        _raise_422(errors)

    if body.dry_run:
        results: list[dict] = [
            {"item_external_id": r.item_external_id, "location_external_id": r.location_external_id, "action": "dry_run"}
            for r in body.on_hand
        ]
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.on_hand), inserted=0, updated=0, errors=0),
            results=results,
        )

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "on_hand", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "on_hand",
        body.on_hand,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    results = []
    inserted = updated = 0

    for row in body.on_hand:
        item_id = item_map[row.item_external_id]
        location_id = loc_map[row.location_external_id]

        # Ensure PI series exists for this (item, location)
        _ensure_projection_series(db, item_id, location_id, BASELINE_SCENARIO_ID)

        # Upsert: one OnHandSupply node per (item, location, scenario)
        existing = db.execute(
            """
            SELECT node_id FROM nodes
            WHERE node_type = 'OnHandSupply'
              AND item_id = %s AND location_id = %s
              AND scenario_id = %s
              AND active = TRUE
            LIMIT 1
            """,
            (item_id, location_id, BASELINE_SCENARIO_ID),
        ).fetchone()

        if existing:
            node_id = existing["node_id"]
            db.execute(
                """
                UPDATE nodes
                SET quantity = %s, qty_uom = %s, time_ref = %s, is_dirty = TRUE, updated_at = now()
                WHERE node_id = %s
                """,
                (row.quantity, row.uom, row.as_of_date, node_id),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "OnHandSupply", item_id, location_id, BASELINE_SCENARIO_ID, row.as_of_date)
            results.append({
                "item_external_id": row.item_external_id,
                "location_external_id": row.location_external_id,
                "node_id": str(node_id),
                "action": "updated",
            })
            updated += 1
        else:
            node_id = uuid4()
            db.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, item_id, location_id,
                     quantity, qty_uom, time_grain, time_ref, is_dirty, active)
                VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, %s, 'timeless', %s, TRUE, TRUE)
                """,
                (node_id, BASELINE_SCENARIO_ID, item_id, location_id,
                 row.quantity, row.uom, row.as_of_date),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "OnHandSupply", item_id, location_id, BASELINE_SCENARIO_ID, row.as_of_date)
            results.append({
                "item_external_id": row.item_external_id,
                "location_external_id": row.location_external_id,
                "node_id": str(node_id),
                "action": "inserted",
            })
            inserted += 1

    logger.info(
        "ingest.on_hand total=%d inserted=%d updated=%d",
        len(body.on_hand), inserted, updated,
    )
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.on_hand), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 6. POST /v1/ingest/purchase-orders
# ─────────────────────────────────────────────────────────────

VALID_PO_STATUSES = {"draft", "confirmed", "in_transit", "received", "cancelled"}

# Impact table per docs/contracts/TSV-FILES-SPEC.md §2.7 — this is the
# single source of truth for "does this status still count as active
# supply in the projection". `draft` does not count (not yet committed),
# `received` does not count (already folded into on_hand), `cancelled`
# does not count. Only `confirmed`/`in_transit` are active expected
# receipts. Keep in lockstep with the doc table if it changes.
_PO_ACTIVE_STATUSES = {"confirmed", "in_transit"}


class PurchaseOrderRow(BaseModel):
    external_id: str = Field(..., description="ERP PO number. Upsert key.")
    item_external_id: str = Field(..., description="Ordered item.")
    location_external_id: str = Field(..., description="Receiving site.")
    supplier_external_id: str = Field(..., description="Supplier. Optional.")
    quantity: float = Field(..., gt=0, description="Ordered quantity (> 0).")
    uom: str = "EA"
    expected_delivery_date: date = Field(..., description="Expected receipt date (YYYY-MM-DD).")
    status: str = "confirmed"

    @field_validator("external_id")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in VALID_PO_STATUSES:
            raise ValueError(f"status must be one of {VALID_PO_STATUSES}")
        return v


class IngestPurchaseOrdersRequest(BaseModel):
    purchase_orders: list[PurchaseOrderRow]
    dry_run: bool = False


@router.post("/purchase-orders", response_model=IngestResponse, summary="Import purchase orders", description="Upsert purchase orders (PurchaseOrderSupply) with ERP external_id tracking.")
def ingest_purchase_orders(
    body: IngestPurchaseOrdersRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert PurchaseOrderSupply nodes, tracked via external_references. All-or-nothing: any error → HTTP 422."""
    # W-01: resolve FKs first, collect ALL errors before any write
    item_ext_ids = list({po.item_external_id for po in body.purchase_orders})
    loc_ext_ids = list({po.location_external_id for po in body.purchase_orders})
    sup_ext_ids = list({po.supplier_external_id for po in body.purchase_orders})

    item_map = _batch_existing(db, "items", "external_id", "item_id", item_ext_ids)
    loc_map = _batch_existing(db, "locations", "external_id", "location_id", loc_ext_ids)
    sup_map = _batch_existing(db, "suppliers", "external_id", "supplier_id", sup_ext_ids)

    errors: list[dict] = []
    for i, po in enumerate(body.purchase_orders):
        row_errs = []
        if po.item_external_id not in item_map:
            row_errs.append(f"item_external_id '{po.item_external_id}' not found in DB")
        if po.location_external_id not in loc_map:
            row_errs.append(f"location_external_id '{po.location_external_id}' not found in DB")
        if po.supplier_external_id not in sup_map:
            row_errs.append(f"supplier_external_id '{po.supplier_external_id}' not found in DB")
        if row_errs:
            errors.append({"external_id": po.external_id, "row": i, "errors": row_errs})

    # W-01+W-02: if any FK error → 422, nothing persisted
    if errors:
        _raise_422(errors)

    if body.dry_run:
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.purchase_orders), inserted=0, updated=0, errors=0),
            results=[{"external_id": po.external_id, "action": "dry_run"} for po in body.purchase_orders],
        )

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "purchase_orders", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "purchase_orders",
        body.purchase_orders,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    # Fetch existing PO node references
    po_ext_ids = [po.external_id for po in body.purchase_orders]
    existing_refs_rows = db.execute(
        """
        SELECT external_id, internal_id FROM external_references
        WHERE entity_type = 'purchase_order' AND external_id = ANY(%s)
        """,
        (po_ext_ids,),
    ).fetchall()
    existing_refs: dict[str, UUID] = {r["external_id"]: r["internal_id"] for r in existing_refs_rows}

    results: list[dict] = []
    inserted = updated = 0

    for po in body.purchase_orders:
        item_id = item_map[po.item_external_id]
        location_id = loc_map[po.location_external_id]
        active = po.status in _PO_ACTIVE_STATUSES

        # Ensure PI series exists for this (item, location)
        _ensure_projection_series(db, item_id, location_id, BASELINE_SCENARIO_ID)

        if po.external_id in existing_refs:
            node_id = existing_refs[po.external_id]
            db.execute(
                """
                UPDATE nodes
                SET quantity = %s, qty_uom = %s, time_ref = %s,
                    active = %s, is_dirty = TRUE, updated_at = now()
                WHERE node_id = %s
                """,
                (po.quantity, po.uom, po.expected_delivery_date, active, node_id),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "PurchaseOrderSupply", item_id, location_id, BASELINE_SCENARIO_ID, po.expected_delivery_date)
            results.append({"external_id": po.external_id, "node_id": str(node_id), "action": "updated"})
            updated += 1
        else:
            node_id = uuid4()
            db.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, item_id, location_id,
                     quantity, qty_uom, time_grain, time_ref, is_dirty, active)
                VALUES (%s, 'PurchaseOrderSupply', %s, %s, %s, %s, %s, 'exact_date', %s, TRUE, %s)
                """,
                (node_id, BASELINE_SCENARIO_ID, item_id, location_id,
                 po.quantity, po.uom, po.expected_delivery_date, active),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "PurchaseOrderSupply", item_id, location_id, BASELINE_SCENARIO_ID, po.expected_delivery_date)
            # Register external reference
            db.execute(
                """
                INSERT INTO external_references
                    (entity_type, external_id, source_system, internal_id)
                VALUES ('purchase_order', %s, 'ingest_api', %s)
                ON CONFLICT (entity_type, external_id, source_system) DO UPDATE
                    SET internal_id = EXCLUDED.internal_id, updated_at = now()
                """,
                (po.external_id, node_id),
            )
            results.append({"external_id": po.external_id, "node_id": str(node_id), "action": "inserted"})
            inserted += 1

    logger.info(
        "ingest.purchase_orders total=%d inserted=%d updated=%d",
        len(body.purchase_orders), inserted, updated,
    )
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.purchase_orders), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 7. POST /v1/ingest/forecast-demand
# ─────────────────────────────────────────────────────────────

VALID_TIME_GRAINS = {"exact_date", "day", "week", "month", "timeless"}
VALID_FORECAST_SOURCES = {"statistical", "consensus", "manual", "ml"}


def _forecast_time_span(bucket_date: date, time_grain: str) -> tuple[Optional[date], Optional[date]]:
    """Derive [time_span_start, time_span_end) for a forecast bucket.

    `time_span_start`/`time_span_end` mirror the PI bucket convention
    (start inclusive, end exclusive) so a periodic forecast can be wired to
    every PI bucket it actually covers (`_wire_node_to_pi`) instead of
    being lumped onto its single anchor date. day/week/month are FIXED
    spans off `bucket_date` (day: +1 day, week: +7 days, month: end of the
    calendar month containing `bucket_date`) — a 🎯 pilot knob, deliberately
    NOT the median-gap-to-next-row heuristic Truth B infers at MRP-load
    time (`engine/mrp/loader.py:257`): an explicit `time_grain` is a
    stronger signal than a gap inference, and is available here before the
    full series even exists. `exact_date`/`timeless` forecasts stay point
    demand — no span, single-bucket wire, unchanged from before.
    """
    if time_grain == "day":
        return bucket_date, bucket_date + timedelta(days=1)
    if time_grain == "week":
        return bucket_date, bucket_date + timedelta(days=7)
    if time_grain == "month":
        if bucket_date.month == 12:
            month_end = date(bucket_date.year + 1, 1, 1)
        else:
            month_end = date(bucket_date.year, bucket_date.month + 1, 1)
        return bucket_date, month_end
    return None, None


class ForecastRow(BaseModel):
    item_external_id: str = Field(..., description="Forecasted item.")
    location_external_id: str = Field(..., description="Consumption site.")
    quantity: float = Field(..., description="Forecasted quantity (>= 0).")
    bucket_date: date = Field(..., description="Bucket start date (YYYY-MM-DD).")
    time_grain: str = Field("week", description="Time grain. Values: day | week | month.")
    source: str = "statistical"

    @field_validator("source")
    @classmethod
    def validate_source(cls, v: str) -> str:
        if v not in VALID_FORECAST_SOURCES:
            raise ValueError(
                f"source '{v}' is invalid; valid values: {sorted(VALID_FORECAST_SOURCES)}"
            )
        return v


class IngestForecastRequest(BaseModel):
    forecasts: list[ForecastRow]
    dry_run: bool = False


@router.post("/forecast-demand", response_model=IngestResponse, summary="Import forecast demand", description="Upsert forecasts (ForecastDemand) per (item × location × bucket × grain).")
def ingest_forecast_demand(
    body: IngestForecastRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert ForecastDemand nodes. Keyed by (item, location, bucket_date, time_grain, scenario).
    All-or-nothing: any validation or FK error → HTTP 422.
    """
    # W-01: validate ALL rows (structural + FK) before any write
    errors: list[dict] = []

    for i, fc in enumerate(body.forecasts):
        row_errs = []
        if fc.time_grain not in VALID_TIME_GRAINS:
            row_errs.append(f"time_grain '{fc.time_grain}' invalid; valid: {sorted(VALID_TIME_GRAINS)}")
        if row_errs:
            errors.append({
                "item_external_id": fc.item_external_id,
                "bucket_date": str(fc.bucket_date),
                "row": i,
                "errors": row_errs,
            })

    if errors:
        _raise_422(errors)

    # FK resolution
    item_ext_ids = list({fc.item_external_id for fc in body.forecasts})
    loc_ext_ids = list({fc.location_external_id for fc in body.forecasts})

    item_map = _batch_existing(db, "items", "external_id", "item_id", item_ext_ids)
    loc_map = _batch_existing(db, "locations", "external_id", "location_id", loc_ext_ids)

    fk_errors: list[dict] = []
    for i, fc in enumerate(body.forecasts):
        row_errs = []
        if fc.item_external_id not in item_map:
            row_errs.append(f"item_external_id '{fc.item_external_id}' not found in DB")
        if fc.location_external_id not in loc_map:
            row_errs.append(f"location_external_id '{fc.location_external_id}' not found in DB")
        if row_errs:
            fk_errors.append({
                "item_external_id": fc.item_external_id,
                "bucket_date": str(fc.bucket_date),
                "row": i,
                "errors": row_errs,
            })

    # W-01+W-02: FK errors → 422, nothing persisted
    if fk_errors:
        _raise_422(fk_errors)

    if body.dry_run:
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.forecasts), inserted=0, updated=0, errors=0),
            results=[
                {"item_external_id": fc.item_external_id, "bucket_date": str(fc.bucket_date), "action": "dry_run"}
                for fc in body.forecasts
            ],
        )

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "forecasts", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "forecasts",
        body.forecasts,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    results: list[dict] = []
    inserted = updated = 0

    for fc in body.forecasts:
        item_id = item_map[fc.item_external_id]
        location_id = loc_map[fc.location_external_id]

        # Ensure PI series exists for this (item, location)
        _ensure_projection_series(db, item_id, location_id, BASELINE_SCENARIO_ID)

        time_span_start, time_span_end = _forecast_time_span(fc.bucket_date, fc.time_grain)

        existing = db.execute(
            """
            SELECT node_id FROM nodes
            WHERE node_type = 'ForecastDemand'
              AND item_id = %s AND location_id = %s
              AND scenario_id = %s
              AND time_ref = %s AND time_grain = %s
              AND active = TRUE
            LIMIT 1
            """,
            (item_id, location_id, BASELINE_SCENARIO_ID, fc.bucket_date, fc.time_grain),
        ).fetchone()

        if existing:
            fc_node_id = existing["node_id"]
            db.execute(
                """
                UPDATE nodes
                SET quantity = %s, time_span_start = %s, time_span_end = %s,
                    is_dirty = TRUE, updated_at = now()
                WHERE node_id = %s
                """,
                (fc.quantity, time_span_start, time_span_end, fc_node_id),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, fc_node_id)
            _wire_node_to_pi(
                db, fc_node_id, "ForecastDemand", item_id, location_id, BASELINE_SCENARIO_ID,
                fc.bucket_date, time_span_start=time_span_start, time_span_end=time_span_end,
            )
            results.append({
                "item_external_id": fc.item_external_id,
                "bucket_date": str(fc.bucket_date),
                "node_id": str(fc_node_id),
                "action": "updated",
            })
            updated += 1
        else:
            node_id = uuid4()
            db.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, item_id, location_id,
                     quantity, time_grain, time_ref, time_span_start, time_span_end,
                     is_dirty, active)
                VALUES (%s, 'ForecastDemand', %s, %s, %s, %s, %s, %s, %s, %s, TRUE, TRUE)
                """,
                (node_id, BASELINE_SCENARIO_ID, item_id, location_id,
                 fc.quantity, fc.time_grain, fc.bucket_date, time_span_start, time_span_end),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(
                db, node_id, "ForecastDemand", item_id, location_id, BASELINE_SCENARIO_ID,
                fc.bucket_date, time_span_start=time_span_start, time_span_end=time_span_end,
            )
            results.append({
                "item_external_id": fc.item_external_id,
                "bucket_date": str(fc.bucket_date),
                "node_id": str(node_id),
                "action": "inserted",
            })
            inserted += 1

    logger.info(
        "ingest.forecast_demand total=%d inserted=%d updated=%d",
        len(body.forecasts), inserted, updated,
    )
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.forecasts), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 8. POST /v1/ingest/resources
# ─────────────────────────────────────────────────────────────

VALID_RESOURCE_TYPES = {"machine", "line", "team", "tool"}


class ResourceRow(BaseModel):
    external_id: str = Field(..., description="Unique resource identifier. Upsert key.")
    name: str = Field(..., description="Resource label.")
    resource_type: str = Field(..., description="Resource type. Values: machine | line | team | tool.")
    location_external_id: Optional[str] = Field(None, description="Site where the resource is located (optional).")
    capacity_per_day: float = Field(1.0, gt=0, description="Nominal capacity per working day.")
    capacity_unit: str = Field("units", description="Unit of the capacity measure.")
    notes: Optional[str] = None

    @field_validator("external_id", "name")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v


class IngestResourcesRequest(BaseModel):
    resources: list[ResourceRow]
    dry_run: bool = False


@router.post(
    "/resources",
    response_model=IngestResponse,
    summary="Import resources",
    description="Upsert a batch of resources. Upsert key: external_id. Also creates/updates a Resource node in the graph.",
)
def ingest_resources(
    body: IngestResourcesRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert resources by external_id. Also maintains a Resource node in the graph."""
    errors: list[dict] = []

    # Validate resource_type
    for i, res in enumerate(body.resources):
        row_errs = []
        if res.resource_type not in VALID_RESOURCE_TYPES:
            row_errs.append(
                f"resource_type '{res.resource_type}' invalid; valid: {sorted(VALID_RESOURCE_TYPES)}"
            )
        if row_errs:
            errors.append({"external_id": res.external_id, "row": i, "errors": row_errs})

    if errors:
        _raise_422(errors)

    # Resolve location FKs
    loc_ext_ids = [r.location_external_id for r in body.resources if r.location_external_id]
    loc_map = _batch_existing(db, "locations", "external_id", "location_id", loc_ext_ids) if loc_ext_ids else {}

    fk_errors: list[dict] = []
    for i, res in enumerate(body.resources):
        if res.location_external_id and res.location_external_id not in loc_map:
            fk_errors.append({
                "external_id": res.external_id,
                "row": i,
                "errors": [f"location_external_id '{res.location_external_id}' not found in DB"],
            })

    if fk_errors:
        _raise_422(fk_errors)

    if body.dry_run:
        return _dry_run_response(body.resources)

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "resources", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "resources",
        body.resources,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    # Batch-fetch existing resources
    existing_resources = _batch_existing(
        db, "resources", "external_id", "resource_id",
        [r.external_id for r in body.resources],
    )

    results: list[dict] = []
    inserted = updated = 0

    for res in body.resources:
        location_id = loc_map.get(res.location_external_id) if res.location_external_id else None

        if res.external_id in existing_resources:
            resource_id = existing_resources[res.external_id]
            db.execute(
                """
                UPDATE resources
                SET name = %s, resource_type = %s, location_id = %s,
                    capacity_per_day = %s, capacity_unit = %s, notes = %s,
                    updated_at = now()
                WHERE resource_id = %s
                """,
                (res.name, res.resource_type, location_id,
                 res.capacity_per_day, res.capacity_unit, res.notes,
                 resource_id),
            )
            # Update Resource graph node
            db.execute(
                """
                UPDATE nodes
                SET location_id = %s, updated_at = now()
                WHERE node_type = 'Resource' AND external_id = %s
                """,
                (location_id, res.external_id),
            )
            results.append({
                "external_id": res.external_id,
                "resource_id": str(resource_id),
                "action": "updated",
            })
            updated += 1
        else:
            resource_id = uuid4()
            db.execute(
                """
                INSERT INTO resources
                    (resource_id, external_id, name, resource_type, location_id,
                     capacity_per_day, capacity_unit, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (resource_id, res.external_id, res.name, res.resource_type, location_id,
                 res.capacity_per_day, res.capacity_unit, res.notes),
            )
            # Create Resource graph node (for edge connectivity)
            node_id = uuid4()
            db.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, location_id, external_id, active)
                VALUES (%s, 'Resource', %s, %s, %s, TRUE)
                """,
                (node_id, BASELINE_SCENARIO_ID, location_id, res.external_id),
            )
            results.append({
                "external_id": res.external_id,
                "resource_id": str(resource_id),
                "node_id": str(node_id),
                "action": "inserted",
            })
            inserted += 1

    logger.info(
        "ingest.resources total=%d inserted=%d updated=%d",
        len(body.resources), inserted, updated,
    )
    ingest_response = _ok(inserted, updated, len(body.resources), results, batch_id=batch_id, dq_status=None)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 9. POST /v1/ingest/work-orders
# ─────────────────────────────────────────────────────────────

VALID_WORK_ORDER_STATUSES = {"planned", "in_progress", "completed", "cancelled"}


class WorkOrderRow(BaseModel):
    external_id: str = Field(..., description="ERP work order number. Upsert key.")
    item_external_id: str = Field(..., description="Produced item.")
    location_external_id: str = Field(..., description="Producing plant/site.")
    quantity: float = Field(..., gt=0, description="Planned output quantity (> 0).")
    scheduled_completion_date: date = Field(..., description="Expected completion date (YYYY-MM-DD).")
    status: str = "planned"

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in VALID_WORK_ORDER_STATUSES:
            raise ValueError(f"status must be one of {VALID_WORK_ORDER_STATUSES}")
        return v


class IngestWorkOrdersRequest(BaseModel):
    work_orders: list[WorkOrderRow]
    dry_run: bool = False


@router.post(
    "/work-orders",
    response_model=IngestResponse,
    summary="Import work orders",
    description="Upsert work orders (WorkOrderSupply) with ERP external_id tracking.",
)
def ingest_work_orders(
    body: IngestWorkOrdersRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert WorkOrderSupply nodes, tracked via external_references. All-or-nothing: any error → HTTP 422."""
    item_ext_ids = list({wo.item_external_id for wo in body.work_orders})
    loc_ext_ids = list({wo.location_external_id for wo in body.work_orders})

    item_map = _batch_existing(db, "items", "external_id", "item_id", item_ext_ids)
    loc_map = _batch_existing(db, "locations", "external_id", "location_id", loc_ext_ids)

    errors: list[dict] = []
    for i, wo in enumerate(body.work_orders):
        row_errs = []
        if wo.item_external_id not in item_map:
            row_errs.append(f"item_external_id '{wo.item_external_id}' not found in DB")
        if wo.location_external_id not in loc_map:
            row_errs.append(f"location_external_id '{wo.location_external_id}' not found in DB")
        if row_errs:
            errors.append({"external_id": wo.external_id, "row": i, "errors": row_errs})

    if errors:
        _raise_422(errors)

    if body.dry_run:
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.work_orders), inserted=0, updated=0, errors=0),
            results=[{"external_id": wo.external_id, "action": "dry_run"} for wo in body.work_orders],
        )

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "work_orders", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "work_orders",
        body.work_orders,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    wo_ext_ids = [wo.external_id for wo in body.work_orders]
    existing_refs_rows = db.execute(
        """
        SELECT external_id, internal_id FROM external_references
        WHERE entity_type = 'work_order' AND external_id = ANY(%s)
        """,
        (wo_ext_ids,),
    ).fetchall()
    existing_refs: dict[str, UUID] = {r["external_id"]: r["internal_id"] for r in existing_refs_rows}

    results: list[dict] = []
    inserted = updated = 0

    for wo in body.work_orders:
        item_id = item_map[wo.item_external_id]
        location_id = loc_map[wo.location_external_id]
        active = wo.status not in ("completed", "cancelled")

        _ensure_projection_series(db, item_id, location_id, BASELINE_SCENARIO_ID)

        if wo.external_id in existing_refs:
            node_id = existing_refs[wo.external_id]
            db.execute(
                """
                UPDATE nodes
                SET quantity = %s, time_ref = %s,
                    active = %s, is_dirty = TRUE, updated_at = now()
                WHERE node_id = %s
                """,
                (wo.quantity, wo.scheduled_completion_date, active, node_id),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "WorkOrderSupply", item_id, location_id, BASELINE_SCENARIO_ID, wo.scheduled_completion_date)
            results.append({"external_id": wo.external_id, "node_id": str(node_id), "action": "updated"})
            updated += 1
        else:
            node_id = uuid4()
            db.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, item_id, location_id,
                     quantity, time_grain, time_ref, is_dirty, active)
                VALUES (%s, 'WorkOrderSupply', %s, %s, %s, %s, 'exact_date', %s, TRUE, %s)
                """,
                (node_id, BASELINE_SCENARIO_ID, item_id, location_id,
                 wo.quantity, wo.scheduled_completion_date, active),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "WorkOrderSupply", item_id, location_id, BASELINE_SCENARIO_ID, wo.scheduled_completion_date)
            db.execute(
                """
                INSERT INTO external_references
                    (entity_type, external_id, source_system, internal_id)
                VALUES ('work_order', %s, 'ingest_api', %s)
                ON CONFLICT (entity_type, external_id, source_system) DO UPDATE
                    SET internal_id = EXCLUDED.internal_id, updated_at = now()
                """,
                (wo.external_id, node_id),
            )
            results.append({"external_id": wo.external_id, "node_id": str(node_id), "action": "inserted"})
            inserted += 1

    logger.info(
        "ingest.work_orders total=%d inserted=%d updated=%d",
        len(body.work_orders), inserted, updated,
    )
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.work_orders), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 10. POST /v1/ingest/customer-orders
# ─────────────────────────────────────────────────────────────

VALID_CUSTOMER_ORDER_STATUSES = {"open", "confirmed", "shipped", "delivered", "cancelled"}

# Impact table per docs/contracts/TSV-FILES-SPEC.md §2.8 — the ERP source
# system never emits `delivered` in practice, only `shipped` (the shipment
# already left, demand is gone from the projection's perspective). `open`
# and `confirmed` are the only statuses still counted as future demand.
_CO_ACTIVE_STATUSES = {"open", "confirmed"}


class CustomerOrderRow(BaseModel):
    external_id: str = Field(..., description="ERP sales order number. Upsert key.")
    item_external_id: str = Field(..., description="Ordered item.")
    location_external_id: str = Field(..., description="Shipping/consuming location.")
    quantity: float = Field(..., gt=0, description="Ordered quantity (> 0).")
    requested_delivery_date: date = Field(..., description="Customer requested delivery date (YYYY-MM-DD).")
    status: str = "open"

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in VALID_CUSTOMER_ORDER_STATUSES:
            raise ValueError(f"status must be one of {VALID_CUSTOMER_ORDER_STATUSES}")
        return v


class IngestCustomerOrdersRequest(BaseModel):
    customer_orders: list[CustomerOrderRow]
    dry_run: bool = False


@router.post(
    "/customer-orders",
    response_model=IngestResponse,
    summary="Import customer orders",
    description="Upsert customer orders (CustomerOrderDemand) with ERP external_id tracking.",
)
def ingest_customer_orders(
    body: IngestCustomerOrdersRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert CustomerOrderDemand nodes, tracked via external_references. All-or-nothing: any error → HTTP 422."""
    item_ext_ids = list({co.item_external_id for co in body.customer_orders})
    loc_ext_ids = list({co.location_external_id for co in body.customer_orders})

    item_map = _batch_existing(db, "items", "external_id", "item_id", item_ext_ids)
    loc_map = _batch_existing(db, "locations", "external_id", "location_id", loc_ext_ids)

    errors: list[dict] = []
    for i, co in enumerate(body.customer_orders):
        row_errs = []
        if co.item_external_id not in item_map:
            row_errs.append(f"item_external_id '{co.item_external_id}' not found in DB")
        if co.location_external_id not in loc_map:
            row_errs.append(f"location_external_id '{co.location_external_id}' not found in DB")
        if row_errs:
            errors.append({"external_id": co.external_id, "row": i, "errors": row_errs})

    if errors:
        _raise_422(errors)

    if body.dry_run:
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.customer_orders), inserted=0, updated=0, errors=0),
            results=[{"external_id": co.external_id, "action": "dry_run"} for co in body.customer_orders],
        )

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "customer_orders", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "customer_orders",
        body.customer_orders,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    co_ext_ids = [co.external_id for co in body.customer_orders]
    existing_refs_rows = db.execute(
        """
        SELECT external_id, internal_id FROM external_references
        WHERE entity_type = 'customer_order' AND external_id = ANY(%s)
        """,
        (co_ext_ids,),
    ).fetchall()
    existing_refs: dict[str, UUID] = {r["external_id"]: r["internal_id"] for r in existing_refs_rows}

    results: list[dict] = []
    inserted = updated = 0

    for co in body.customer_orders:
        item_id = item_map[co.item_external_id]
        location_id = loc_map[co.location_external_id]
        active = co.status in _CO_ACTIVE_STATUSES

        _ensure_projection_series(db, item_id, location_id, BASELINE_SCENARIO_ID)

        if co.external_id in existing_refs:
            node_id = existing_refs[co.external_id]
            db.execute(
                """
                UPDATE nodes
                SET quantity = %s, time_ref = %s,
                    active = %s, is_dirty = TRUE, updated_at = now()
                WHERE node_id = %s
                """,
                (co.quantity, co.requested_delivery_date, active, node_id),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "CustomerOrderDemand", item_id, location_id, BASELINE_SCENARIO_ID, co.requested_delivery_date)
            results.append({"external_id": co.external_id, "node_id": str(node_id), "action": "updated"})
            updated += 1
        else:
            node_id = uuid4()
            db.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, item_id, location_id,
                     quantity, time_grain, time_ref, is_dirty, active)
                VALUES (%s, 'CustomerOrderDemand', %s, %s, %s, %s, 'exact_date', %s, TRUE, %s)
                """,
                (node_id, BASELINE_SCENARIO_ID, item_id, location_id,
                 co.quantity, co.requested_delivery_date, active),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "CustomerOrderDemand", item_id, location_id, BASELINE_SCENARIO_ID, co.requested_delivery_date)
            db.execute(
                """
                INSERT INTO external_references
                    (entity_type, external_id, source_system, internal_id)
                VALUES ('customer_order', %s, 'ingest_api', %s)
                ON CONFLICT (entity_type, external_id, source_system) DO UPDATE
                    SET internal_id = EXCLUDED.internal_id, updated_at = now()
                """,
                (co.external_id, node_id),
            )
            results.append({"external_id": co.external_id, "node_id": str(node_id), "action": "inserted"})
            inserted += 1

    logger.info(
        "ingest.customer_orders total=%d inserted=%d updated=%d",
        len(body.customer_orders), inserted, updated,
    )
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.customer_orders), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 11. POST /v1/ingest/transfers
# ─────────────────────────────────────────────────────────────

VALID_TRANSFER_STATUSES = {"planned", "in_transit", "delivered", "cancelled"}

# Impact table per docs/contracts/transfers/format-transfers-tsv.md §4 —
# `planned`/`in_transit` still count as an expected receipt at the
# destination PI; `delivered` no longer counts (already folded into the
# destination's on_hand) and `cancelled` is ignored. Verified against the
# PO/CO terminal-status bug family (docs/contracts/TSV-FILES-SPEC.md
# §2.7/§2.8) — transfers were already correct, kept here for parity/audit
# clarity rather than as a behaviour change.
_TRANSFER_ACTIVE_STATUSES = {"planned", "in_transit"}


class TransferRow(BaseModel):
    external_id: str = Field(..., description="ERP transfer/STO number. Upsert key.")
    item_external_id: str = Field(..., description="Transferred item.")
    from_location_external_id: str = Field(..., description="Shipping location.")
    to_location_external_id: str = Field(..., description="Receiving location.")
    quantity: float = Field(..., gt=0, description="Transfer quantity (> 0).")
    expected_delivery_date: date = Field(..., description="Expected arrival date at destination (YYYY-MM-DD).")
    status: str = "planned"

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in VALID_TRANSFER_STATUSES:
            raise ValueError(f"status must be one of {VALID_TRANSFER_STATUSES}")
        return v


class IngestTransfersRequest(BaseModel):
    transfers: list[TransferRow]
    dry_run: bool = False


@router.post(
    "/transfers",
    response_model=IngestResponse,
    summary="Import transfers",
    description=(
        "Upsert stock transfers (TransferSupply) between two locations. "
        "The node is wired to the PI of the **destination** (to_location)."
    ),
)
def ingest_transfers(
    body: IngestTransfersRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert TransferSupply nodes, tracked via external_references. All-or-nothing: any error → HTTP 422."""
    item_ext_ids = list({t.item_external_id for t in body.transfers})
    from_loc_ext_ids = list({t.from_location_external_id for t in body.transfers})
    to_loc_ext_ids = list({t.to_location_external_id for t in body.transfers})
    all_loc_ext_ids = list(set(from_loc_ext_ids) | set(to_loc_ext_ids))

    item_map = _batch_existing(db, "items", "external_id", "item_id", item_ext_ids)
    loc_map = _batch_existing(db, "locations", "external_id", "location_id", all_loc_ext_ids)

    errors: list[dict] = []
    for i, t in enumerate(body.transfers):
        row_errs = []
        if t.item_external_id not in item_map:
            row_errs.append(f"item_external_id '{t.item_external_id}' not found in DB")
        if t.from_location_external_id not in loc_map:
            row_errs.append(f"from_location_external_id '{t.from_location_external_id}' not found in DB")
        if t.to_location_external_id not in loc_map:
            row_errs.append(f"to_location_external_id '{t.to_location_external_id}' not found in DB")
        if row_errs:
            errors.append({"external_id": t.external_id, "row": i, "errors": row_errs})

    if errors:
        _raise_422(errors)

    if body.dry_run:
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.transfers), inserted=0, updated=0, errors=0),
            results=[{"external_id": t.external_id, "action": "dry_run"} for t in body.transfers],
        )

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "transfers", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db,
        "transfers",
        body.transfers,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    tr_ext_ids = [t.external_id for t in body.transfers]
    existing_refs_rows = db.execute(
        """
        SELECT external_id, internal_id FROM external_references
        WHERE entity_type = 'transfer' AND external_id = ANY(%s)
        """,
        (tr_ext_ids,),
    ).fetchall()
    existing_refs: dict[str, UUID] = {r["external_id"]: r["internal_id"] for r in existing_refs_rows}

    results: list[dict] = []
    inserted = updated = 0

    for t in body.transfers:
        item_id = item_map[t.item_external_id]
        to_location_id = loc_map[t.to_location_external_id]
        active = t.status in _TRANSFER_ACTIVE_STATUSES

        # Wire to destination PI (to_location is the receiving side)
        _ensure_projection_series(db, item_id, to_location_id, BASELINE_SCENARIO_ID)

        if t.external_id in existing_refs:
            node_id = existing_refs[t.external_id]
            db.execute(
                """
                UPDATE nodes
                SET quantity = %s, time_ref = %s,
                    active = %s, is_dirty = TRUE, updated_at = now()
                WHERE node_id = %s
                """,
                (t.quantity, t.expected_delivery_date, active, node_id),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "TransferSupply", item_id, to_location_id, BASELINE_SCENARIO_ID, t.expected_delivery_date)
            results.append({"external_id": t.external_id, "node_id": str(node_id), "action": "updated"})
            updated += 1
        else:
            node_id = uuid4()
            db.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, item_id, location_id,
                     quantity, time_grain, time_ref, is_dirty, active)
                VALUES (%s, 'TransferSupply', %s, %s, %s, %s, 'exact_date', %s, TRUE, %s)
                """,
                (node_id, BASELINE_SCENARIO_ID, item_id, to_location_id,
                 t.quantity, t.expected_delivery_date, active),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "TransferSupply", item_id, to_location_id, BASELINE_SCENARIO_ID, t.expected_delivery_date)
            db.execute(
                """
                INSERT INTO external_references
                    (entity_type, external_id, source_system, internal_id)
                VALUES ('transfer', %s, 'ingest_api', %s)
                ON CONFLICT (entity_type, external_id, source_system) DO UPDATE
                    SET internal_id = EXCLUDED.internal_id, updated_at = now()
                """,
                (t.external_id, node_id),
            )
            results.append({
                "external_id": t.external_id,
                "node_id": str(node_id),
                "from_location": t.from_location_external_id,
                "to_location": t.to_location_external_id,
                "action": "inserted",
            })
            inserted += 1

    logger.info(
        "ingest.transfers total=%d inserted=%d updated=%d",
        len(body.transfers), inserted, updated,
    )
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.transfers), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 11. POST /v1/ingest/planning-params (ADR-014 D3 — SCD2 transparent)
# ─────────────────────────────────────────────────────────────

_LOT_SIZE_RULES = {"LOTFORLOT", "FIXED_QTY", "EOQ", "POQ", "MIN_MAX", "MULTIPLE"}
_FORECAST_STRATEGIES = {"max_only", "consume_forward", "consume_backward", "consume_both"}

# Fields tracked for SCD2 change detection.
# A field omitted from the client payload is NOT compared — see ADR-014 D3
# (partial-push semantics: omission = "keep current value").
# NOTE: lead_time_total_days is GENERATED — derived from sourcing+mfg+transit;
# never tracked / never inserted directly.
_PLANNING_PARAMS_TRACKED_FIELDS = [
    "lead_time_sourcing_days",
    "lead_time_manufacturing_days",
    "lead_time_transit_days",
    "safety_stock_qty",
    "safety_stock_days",
    "reorder_point_qty",
    "min_order_qty",
    "max_order_qty",
    "order_multiple",
    "lot_size_rule",
    "planning_horizon_days",
    "is_make",
    "preferred_supplier_id",  # resolved from preferred_supplier_external_id
    "economic_order_qty",
    "lot_size_poq_periods",
    "order_multiple_qty",
    "frozen_time_fence_days",
    "slashed_time_fence_days",
    "forecast_consumption_strategy",
    "consumption_window_days",
]


class PlanningParamsRow(BaseModel):
    item_external_id: str = Field(..., description="Target item external_id.")
    location_external_id: str = Field(..., description="Target location external_id.")

    # Lead times (integer days)
    lead_time_sourcing_days: Optional[int] = Field(None, ge=0)
    lead_time_manufacturing_days: Optional[int] = Field(None, ge=0)
    lead_time_transit_days: Optional[int] = Field(None, ge=0)

    # Safety stock
    safety_stock_qty: Optional[float] = Field(None, ge=0)
    safety_stock_days: Optional[float] = Field(None, ge=0)

    # Reorder / lot
    reorder_point_qty: Optional[float] = Field(None, ge=0)
    min_order_qty: Optional[float] = Field(None, gt=0)
    max_order_qty: Optional[float] = Field(None, gt=0)
    order_multiple: Optional[float] = Field(None, gt=0)

    # Policy
    lot_size_rule: Optional[str] = Field(None, description="One of: LOTFORLOT/FIXED_QTY/EOQ/POQ/MIN_MAX/MULTIPLE.")
    planning_horizon_days: Optional[int] = Field(None, gt=0)
    is_make: Optional[bool] = Field(None)
    preferred_supplier_external_id: Optional[str] = Field(None, description="External id of preferred supplier (optional).")

    # APICS extensions (mig 021)
    economic_order_qty: Optional[float] = Field(None, gt=0)
    lot_size_poq_periods: Optional[int] = Field(None, gt=0)
    order_multiple_qty: Optional[float] = Field(None, gt=0)
    frozen_time_fence_days: Optional[int] = Field(None, ge=0)
    slashed_time_fence_days: Optional[int] = Field(None, gt=0)
    forecast_consumption_strategy: Optional[str] = Field(None)
    consumption_window_days: Optional[int] = Field(None, gt=0)

    @field_validator("item_external_id", "location_external_id")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v

    @field_validator("lot_size_rule")
    @classmethod
    def valid_lot_size_rule(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in _LOT_SIZE_RULES:
            raise ValueError(f"lot_size_rule must be one of {sorted(_LOT_SIZE_RULES)}")
        return v

    @field_validator("forecast_consumption_strategy")
    @classmethod
    def valid_consumption_strategy(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in _FORECAST_STRATEGIES:
            raise ValueError(f"forecast_consumption_strategy must be one of {sorted(_FORECAST_STRATEGIES)}")
        return v


class IngestPlanningParamsRequest(BaseModel):
    params: list[PlanningParamsRow]
    dry_run: bool = False


def _planning_params_active_row(
    db: DictRowConnection, item_id: UUID, location_id: UUID
) -> Optional[dict]:
    """Return the currently active (effective_to IS NULL) row, or None."""
    row = db.execute(
        """
        SELECT param_id, effective_from,
               lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days,
               safety_stock_qty, safety_stock_days,
               reorder_point_qty,
               min_order_qty, max_order_qty, order_multiple,
               lot_size_rule, planning_horizon_days, is_make,
               preferred_supplier_id,
               economic_order_qty, lot_size_poq_periods, order_multiple_qty,
               frozen_time_fence_days, slashed_time_fence_days,
               forecast_consumption_strategy, consumption_window_days
        FROM item_planning_params
        WHERE item_id = %s AND location_id = %s
          AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
        ORDER BY effective_from DESC
        LIMIT 1
        """,
        (item_id, location_id),
    ).fetchone()
    return dict(row) if row else None


def _row_to_db_dict(row: PlanningParamsRow, supplier_id: Optional[UUID]) -> dict:
    """Build the dict of (DB-column-name → pushed-value) for SCD2 comparison.
    Keys absent from this dict mean "client did not push that field" —
    SCD2 will not consider them in change detection. We use Pydantic's
    `model_dump(exclude_none=False)` minus the meta fields, then drop
    keys whose value is None *iff* the client really didn't pass them.
    Pydantic-V2 detects unset fields via __pydantic_fields_set__.
    """
    sent = row.__pydantic_fields_set__
    incoming: dict = {}
    if "lead_time_sourcing_days" in sent:
        incoming["lead_time_sourcing_days"] = row.lead_time_sourcing_days
    if "lead_time_manufacturing_days" in sent:
        incoming["lead_time_manufacturing_days"] = row.lead_time_manufacturing_days
    if "lead_time_transit_days" in sent:
        incoming["lead_time_transit_days"] = row.lead_time_transit_days
    if "safety_stock_qty" in sent:
        incoming["safety_stock_qty"] = row.safety_stock_qty
    if "safety_stock_days" in sent:
        incoming["safety_stock_days"] = row.safety_stock_days
    if "reorder_point_qty" in sent:
        incoming["reorder_point_qty"] = row.reorder_point_qty
    if "min_order_qty" in sent:
        incoming["min_order_qty"] = row.min_order_qty
    if "max_order_qty" in sent:
        incoming["max_order_qty"] = row.max_order_qty
    if "order_multiple" in sent:
        incoming["order_multiple"] = row.order_multiple
    if "lot_size_rule" in sent:
        incoming["lot_size_rule"] = row.lot_size_rule
    if "planning_horizon_days" in sent:
        incoming["planning_horizon_days"] = row.planning_horizon_days
    if "is_make" in sent:
        incoming["is_make"] = row.is_make
    if "preferred_supplier_external_id" in sent:
        # `supplier_id` was resolved already (None means "clear").
        incoming["preferred_supplier_id"] = supplier_id
    if "economic_order_qty" in sent:
        incoming["economic_order_qty"] = row.economic_order_qty
    if "lot_size_poq_periods" in sent:
        incoming["lot_size_poq_periods"] = row.lot_size_poq_periods
    if "order_multiple_qty" in sent:
        incoming["order_multiple_qty"] = row.order_multiple_qty
    if "frozen_time_fence_days" in sent:
        incoming["frozen_time_fence_days"] = row.frozen_time_fence_days
    if "slashed_time_fence_days" in sent:
        incoming["slashed_time_fence_days"] = row.slashed_time_fence_days
    if "forecast_consumption_strategy" in sent:
        incoming["forecast_consumption_strategy"] = row.forecast_consumption_strategy
    if "consumption_window_days" in sent:
        incoming["consumption_window_days"] = row.consumption_window_days
    return incoming


@router.post(
    "/planning-params",
    response_model=IngestResponse,
    summary="Import planning parameters (SCD2 transparent)",
    description=(
        "Push current state of planning params per (item, location). The endpoint "
        "implements ADR-014 D3 transparent SCD2: the client pushes its current values, "
        "the API compares to the active row and either no-ops, updates in place "
        "(same-day) or rotates (closes active + inserts new with effective_from=today)."
    ),
)
def ingest_planning_params(
    body: IngestPlanningParamsRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """SCD2-transparent upsert of item_planning_params (ADR-014 D3)."""
    from datetime import date
    from ootils_core.scd2 import Scd2Action, decide_action

    # 1. Resolve FKs (item/location/preferred_supplier) in batch — fail fast on missing refs
    item_ext_ids = list({r.item_external_id for r in body.params})
    loc_ext_ids = list({r.location_external_id for r in body.params})
    sup_ext_ids = list({r.preferred_supplier_external_id for r in body.params if r.preferred_supplier_external_id})

    item_map = _batch_existing(db, "items", "external_id", "item_id", item_ext_ids)
    loc_map = _batch_existing(db, "locations", "external_id", "location_id", loc_ext_ids)
    sup_map = _batch_existing(db, "suppliers", "external_id", "supplier_id", sup_ext_ids) if sup_ext_ids else {}

    errors: list[dict] = []
    for i, r in enumerate(body.params):
        row_errs = []
        if r.item_external_id not in item_map:
            row_errs.append(f"item_external_id '{r.item_external_id}' not found in DB")
        if r.location_external_id not in loc_map:
            row_errs.append(f"location_external_id '{r.location_external_id}' not found in DB")
        if r.preferred_supplier_external_id and r.preferred_supplier_external_id not in sup_map:
            row_errs.append(
                f"preferred_supplier_external_id '{r.preferred_supplier_external_id}' not found in DB"
            )
        if row_errs:
            errors.append({
                "item_external_id": r.item_external_id,
                "location_external_id": r.location_external_id,
                "row": i, "errors": row_errs,
            })

    if errors:
        _raise_422(errors)

    today = date.today()

    if body.dry_run:
        # Show what would happen without writing
        results: list[dict] = []
        for r in body.params:
            item_id = item_map[r.item_external_id]
            location_id = loc_map[r.location_external_id]
            supplier_id = sup_map.get(r.preferred_supplier_external_id) if r.preferred_supplier_external_id else None
            active = _planning_params_active_row(db, item_id, location_id)
            incoming = _row_to_db_dict(r, supplier_id)
            decision = decide_action(active, incoming, _PLANNING_PARAMS_TRACKED_FIELDS, today)
            results.append({
                "item_external_id": r.item_external_id,
                "location_external_id": r.location_external_id,
                "action": decision.action.value,
                "changed_fields": list(decision.changed_fields.keys()),
            })
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.params), inserted=0, updated=0, errors=0),
            results=results,
        )

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "planning_params", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db, "planning_params", body.params,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    inserted = updated = 0
    results = []

    for r in body.params:
        item_id = item_map[r.item_external_id]
        location_id = loc_map[r.location_external_id]
        supplier_id = sup_map.get(r.preferred_supplier_external_id) if r.preferred_supplier_external_id else None

        active = _planning_params_active_row(db, item_id, location_id)
        incoming = _row_to_db_dict(r, supplier_id)
        decision = decide_action(active, incoming, _PLANNING_PARAMS_TRACKED_FIELDS, today)

        if decision.action == Scd2Action.NOOP:
            results.append({
                "item_external_id": r.item_external_id,
                "location_external_id": r.location_external_id,
                "action": "noop",
            })
            continue

        if decision.action == Scd2Action.UPDATED_INPLACE:
            # Same-day UPDATE on the active row's param_id.
            # decide_action only yields UPDATED_INPLACE when an active row
            # exists (see scd2.decide_action); guard fail-loudly so a broken
            # invariant surfaces here instead of an opaque NoneType index.
            if active is None:
                raise RuntimeError(
                    "SCD2 UPDATED_INPLACE requires an active row but none was found"
                )
            assignments = ", ".join(f"{k} = %s" for k in decision.changed_fields.keys())
            params_values = list(decision.changed_fields.values()) + [active["param_id"]]
            db.execute(
                f"UPDATE item_planning_params SET {assignments}, updated_at = now() WHERE param_id = %s",
                params_values,
            )
            updated += 1
            results.append({
                "item_external_id": r.item_external_id,
                "location_external_id": r.location_external_id,
                "action": "updated_inplace",
                "changed_fields": list(decision.changed_fields.keys()),
                "param_id": str(active["param_id"]),
            })
            continue

        # CREATED or ROTATED — both need an INSERT. ROTATED also closes the active row first.
        if decision.action == Scd2Action.ROTATED:
            # decide_action only yields ROTATED when an active row exists
            # (see scd2.decide_action); guard fail-loudly so a broken
            # invariant surfaces here instead of an opaque NoneType index.
            if active is None:
                raise RuntimeError(
                    "SCD2 ROTATED requires an active row but none was found"
                )
            # Half-open interval: old.[effective_from, effective_to=today),
            # new.[effective_from=today, effective_to=NULL). Matches the
            # daterange(...) WITH && EXCLUDE constraint, and the CHECK
            # effective_to > effective_from (strict) — even when the
            # active row was created yesterday.
            db.execute(
                "UPDATE item_planning_params SET effective_to = %s, updated_at = now() WHERE param_id = %s",
                (today, active["param_id"]),
            )

        # Build the new row from active values (carry-over) overridden by incoming fields.
        # If active exists (ROTATED), carry over the active values for unspecified
        # fields — SCD2 partial-push semantics (ADR-014 D3): an empty cell means
        # "do not touch", never "clear the value". This branch is untouched by
        # the fix below.
        #
        # If there is no active row (CREATED — first-ever push for this item ×
        # location) and the client did not send a field, the key is simply
        # OMITTED from new_row_values below instead of being set to None. A
        # dict key present with value None still ends up in the INSERT's
        # column list as an explicit `NULL`, which short-circuits the column's
        # DB DEFAULT (migrations 007/021 — e.g. consumption_window_days -> 7,
        # forecast_consumption_strategy -> 'max_only'). Omitting the column
        # entirely lets Postgres apply that DEFAULT instead. Bug fixed
        # 2026-07-16 — see docs/contracts/TSV-FILES-SPEC.md §2.5.
        new_row_values: dict = {}
        for k in _PLANNING_PARAMS_TRACKED_FIELDS:
            if k in incoming:
                new_row_values[k] = incoming[k]
            elif active is not None:
                new_row_values[k] = active.get(k)
            # else: CREATED + field not sent -> key omitted, DB DEFAULT applies.

        # lot_size_rule cannot be NULL in DB (DEFAULT 'LOTFORLOT'); enforce
        if new_row_values.get("lot_size_rule") is None:
            new_row_values["lot_size_rule"] = "LOTFORLOT"
        if new_row_values.get("planning_horizon_days") is None:
            new_row_values["planning_horizon_days"] = 90
        if new_row_values.get("is_make") is None:
            new_row_values["is_make"] = False

        param_id = uuid4()
        all_cols = ["param_id", "item_id", "location_id", "effective_from"] + list(new_row_values.keys())
        all_values = [param_id, item_id, location_id, today] + list(new_row_values.values())
        placeholders = ", ".join(["%s"] * len(all_cols))
        col_list = ", ".join(all_cols)
        db.execute(
            f"INSERT INTO item_planning_params ({col_list}) VALUES ({placeholders})",
            all_values,
        )
        inserted += 1
        results.append({
            "item_external_id": r.item_external_id,
            "location_external_id": r.location_external_id,
            "action": decision.action.value,
            "changed_fields": list(decision.changed_fields.keys()),
            "param_id": str(param_id),
        })

    logger.info(
        "ingest.planning_params total=%d inserted=%d updated=%d noop=%d",
        len(body.params), inserted, updated, len(body.params) - inserted - updated,
    )
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.params), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 12. POST /v1/ingest/routings (ADR-014 D2 — typed time units)
# ─────────────────────────────────────────────────────────────
#
# Full-reload per (item, sequence). One routing per item in V1
# (sequence defaults to 1). Each routing carries N operations.
# Each operation declares its time_unit; must match the target
# resource's capacity_unit (ADR-014 D2). Mismatch = 422.
#
# Normalisation at ingest: time_unit='hour' is converted to 'minute'
# (run_time_per_unit and setup_time multiplied by 60) before storage.

_VALID_TIME_UNITS_INGEST = {"unit", "minute", "hour"}
_VALID_TIME_UNITS_DB = {"unit", "minute"}


class RoutingOperationRow(BaseModel):
    sequence: int = Field(..., gt=0, description="Operation sequence within the routing (1, 2, 3...).")
    resource_external_id: str = Field(..., description="Target resource (work_center / machine / line).")
    setup_time: float = Field(default=0.0, ge=0, description="Setup time (per routing run, not per unit).")
    run_time_per_unit: float = Field(default=0.0, ge=0, description="Time consumed per produced unit.")
    time_unit: str = Field(default="unit", description="Unit for setup_time + run_time_per_unit. One of: unit, minute, hour. 'hour' is normalized to minute (x60) at ingest.")
    description: Optional[str] = None

    @field_validator("resource_external_id")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("resource_external_id must not be empty")
        return v

    @field_validator("time_unit")
    @classmethod
    def valid_time_unit(cls, v: str) -> str:
        if v not in _VALID_TIME_UNITS_INGEST:
            raise ValueError(f"time_unit must be one of {sorted(_VALID_TIME_UNITS_INGEST)}")
        return v


class RoutingRow(BaseModel):
    item_external_id: str = Field(..., description="Item this routing produces.")
    sequence: int = Field(default=1, gt=0, description="Routing sequence per item. V1 fixes sequence=1.")
    description: Optional[str] = None
    operations: list[RoutingOperationRow] = Field(..., min_length=1, description="Operations list, at least one.")

    @field_validator("item_external_id")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("item_external_id must not be empty")
        return v

    @field_validator("operations")
    @classmethod
    def unique_operation_sequences(cls, ops: list[RoutingOperationRow]) -> list[RoutingOperationRow]:
        seqs = [op.sequence for op in ops]
        if len(seqs) != len(set(seqs)):
            raise ValueError("operations must have unique sequence numbers within a routing")
        return ops


class IngestRoutingsRequest(BaseModel):
    routings: list[RoutingRow]
    dry_run: bool = False


def _normalize_op_time_unit(op: RoutingOperationRow) -> tuple[str, float, float]:
    """Return (db_time_unit, setup_time_normalized, run_time_per_unit_normalized).

    'hour' → 'minute' with x60 scaling.
    'unit' and 'minute' stored as-is.
    """
    if op.time_unit == "hour":
        return ("minute", op.setup_time * 60.0, op.run_time_per_unit * 60.0)
    return (op.time_unit, op.setup_time, op.run_time_per_unit)


@router.post(
    "/routings",
    response_model=IngestResponse,
    summary="Import routings (with typed time units, ADR-014 D2)",
    description=(
        "Full-reload per (item, sequence). Each routing carries N operations. "
        "Each operation declares its time_unit (unit | minute | hour); hour is "
        "normalized to minute at ingest. The op's normalized time_unit must "
        "match the target resource's capacity_unit — mismatch = 422."
    ),
)
def ingest_routings(
    body: IngestRoutingsRequest,
    request: Request,
    response: Response,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Full-reload routings + operations per (item, sequence). ADR-014 D2 unit checks at ingest."""
    # 1. Resolve item + resource external_ids in batch
    item_ext_ids = list({r.item_external_id for r in body.routings})
    resource_ext_ids = list({
        op.resource_external_id
        for r in body.routings
        for op in r.operations
    })

    item_map = _batch_existing(db, "items", "external_id", "item_id", item_ext_ids)
    resource_rows = db.execute(
        "SELECT external_id, resource_id, capacity_unit FROM resources WHERE external_id = ANY(%s)",
        (resource_ext_ids,),
    ).fetchall() if resource_ext_ids else []
    resource_map: dict[str, dict] = {
        r["external_id"]: {"resource_id": r["resource_id"], "capacity_unit": r["capacity_unit"]}
        for r in resource_rows
    }

    # 2. Validate FK + unit cohérence in one pass — collect ALL errors
    errors: list[dict] = []
    for i, r in enumerate(body.routings):
        row_errs = []
        if r.item_external_id not in item_map:
            row_errs.append(f"item_external_id '{r.item_external_id}' not found in DB")
        for op_idx, op in enumerate(r.operations):
            if op.resource_external_id not in resource_map:
                row_errs.append(
                    f"operations[{op_idx}].resource_external_id '{op.resource_external_id}' not found in DB"
                )
                continue
            # ADR-014 D2 unit cohérence: normalized op time_unit must match resource capacity_unit
            normalized_unit, _, _ = _normalize_op_time_unit(op)
            res_unit = resource_map[op.resource_external_id]["capacity_unit"]
            if normalized_unit != res_unit:
                row_errs.append(
                    f"operations[{op_idx}] time_unit '{op.time_unit}' (normalized to '{normalized_unit}') "
                    f"does not match resource '{op.resource_external_id}' capacity_unit '{res_unit}' "
                    f"— ADR-014 D2 forbids unit ↔ minute mixing"
                )
        if row_errs:
            errors.append({
                "item_external_id": r.item_external_id,
                "row": i,
                "errors": row_errs,
            })

    if errors:
        _raise_422(errors)

    if body.dry_run:
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.routings), inserted=0, updated=0, errors=0),
            results=[
                {
                    "item_external_id": r.item_external_id,
                    "sequence": r.sequence,
                    "operations_count": len(r.operations),
                    "action": "dry_run",
                }
                for r in body.routings
            ],
        )

    idempotency_key, request_hash, replay = _load_idempotent_response(db, "routings", request, response, body)
    if replay is not None:
        return replay

    batch_id = _create_ingest_batch(
        db, "routings", body.routings,
        submitted_by=getattr(request.state, "client_id", "ingest_api"),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        correlation_id=getattr(request.state, "correlation_id", None),
    )

    inserted = updated = 0
    results: list[dict] = []

    for r in body.routings:
        item_id = item_map[r.item_external_id]
        # Look up existing routing for (item, sequence)
        existing = db.execute(
            "SELECT routing_id FROM routings WHERE item_id = %s AND sequence = %s AND active = TRUE",
            (item_id, r.sequence),
        ).fetchone()

        if existing is not None:
            # Full-reload: DELETE existing routing (CASCADE delete operations) + INSERT new.
            # Preserves the routing_id is NOT a goal — we re-create with a new id.
            db.execute("DELETE FROM routings WHERE routing_id = %s", (existing["routing_id"],))
            action = "replaced"
            updated += 1
        else:
            action = "created"
            inserted += 1

        # INSERT routing
        routing_id = uuid4()
        db.execute(
            """
            INSERT INTO routings (routing_id, item_id, sequence, description, active)
            VALUES (%s, %s, %s, %s, TRUE)
            """,
            (routing_id, item_id, r.sequence, r.description),
        )

        # INSERT operations (normalize units)
        for op in r.operations:
            db_unit, setup_n, run_per_unit_n = _normalize_op_time_unit(op)
            resource_id = resource_map[op.resource_external_id]["resource_id"]
            db.execute(
                """
                INSERT INTO routing_operations (
                    operation_id, routing_id, sequence, resource_id,
                    setup_time, run_time_per_unit, time_unit, description, active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE)
                """,
                (uuid4(), routing_id, op.sequence, resource_id,
                 setup_n, run_per_unit_n, db_unit, op.description),
            )

        results.append({
            "item_external_id": r.item_external_id,
            "sequence": r.sequence,
            "routing_id": str(routing_id),
            "operations_count": len(r.operations),
            "action": action,
        })

    logger.info(
        "ingest.routings total=%d created=%d replaced=%d",
        len(body.routings), inserted, updated,
    )
    dq_status = _trigger_dq(db, batch_id)
    ingest_response = _ok(inserted, updated, len(body.routings), results, batch_id=batch_id, dq_status=dq_status)
    _finalize_ingest_batch(db, batch_id, ingest_response)
    return ingest_response


# ─────────────────────────────────────────────────────────────
# 13. POST /v1/ingest/distribution-links (DRP lanes, DESC-1 PR-D)
# ─────────────────────────────────────────────────────────────
#
# Referential/on-demand entity (ADR-042 doctrine — "à la demande, jamais
# dans le run bloquant quotidien"). The `distribution_links` table already
# exists (migration 029 + 065, transfer_multiple); this is the FIRST
# writer that is not a script/seed. See
# docs/contracts/distribution_links/format-distribution-links-tsv.md for
# the full contract this endpoint implements.
#
# Upsert key: (upstream_location_id, downstream_location_id, item_id),
# item_id NULL = generic lane (all items) — a generic lane and an
# item-specific lane on the SAME (upstream, downstream) pair coexist (spec
# §4, consumed by engine/drp/core.py's specificity rule), they are never
# duplicates of each other. distribution_links carries NO unique
# constraint on that triplet (unlike supplier_items' UNIQUE(supplier_id,
# item_id)), so the upsert is SELECT-then-INSERT/UPDATE, same shape as
# ingest_supplier_items — with `item_id IS NOT DISTINCT FROM %s` for the
# NULL-safe match a plain `=` cannot express.
#
# NO ingest_batches / DQ / idempotency-key plumbing here — deliberately,
# mirroring bom.py's `ingest_bom` (the OTHER referential/topology
# endpoint, not a master-data one): `ingest_batches.entity_type` is a
# named CHECK constraint (migrations 023→035→036, kept in lockstep by
# convention) that would need widening via a NEW migration to accept
# 'distribution_links' — out of scope for this chantier (no migration).
# Widening it is a documented follow-up if/when this endpoint needs the
# audit-trail / DQ-pipeline treatment the master-data endpoints get.
#
# Columns intentionally NOT covered by this endpoint (spec §8 — stay
# script/seed-managed until an ERP need appears): transit_cost_per_unit,
# transit_cost_fixed, maximum_shipment_qty, shipment_frequency,
# shipment_days. An UPDATE here never touches them, so a pre-existing
# value set out-of-band survives a re-push untouched.

class DistributionLinkRow(BaseModel):
    upstream_external_id: str = Field(..., description="Upstream site (source of the lane). FK locations.")
    downstream_external_id: str = Field(..., description="Downstream site (destination of the lane). FK locations, must differ from upstream.")
    item_external_id: Optional[str] = Field(None, description="Scoping item. Empty/absent = generic lane (item_id NULL, valid for every item on this pair).")
    transit_lead_time_days: float = Field(..., ge=0, description="Transit lead time in days. Mandatory in this file — the DB column carries a technical default of 7, but a network transit time is structural data; the file contract refuses to inherit it silently.")
    minimum_shipment_qty: Optional[float] = Field(None, ge=0, description="Minimum quantity per shipment. Default 1 (DB default) when omitted; 0 is a legitimate 'no floor' value.")
    transfer_multiple: Optional[float] = Field(None, gt=0, description="Logistics shipment multiple (case/pallet/truck), rounded DOWN by the DRP planner (ADR-028). Default 1 (no rounding) when omitted. Must be strictly > 0 — 0 or negative is rejected.")
    priority: Optional[int] = Field(None, ge=1, description="Sourcing priority when several lanes serve the same downstream site (1 = most preferred). Default 100 when omitted.")
    active: bool = Field(True, description="Whether this lane is usable by the DRP planner. Not part of the TSV file contract (spec §8) — JSON callers only.")

    @field_validator("upstream_external_id", "downstream_external_id")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v

    @field_validator("item_external_id")
    @classmethod
    def blank_to_none(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            return None
        return v


class IngestDistributionLinksRequest(BaseModel):
    distribution_links: list[DistributionLinkRow]
    dry_run: bool = False


def _distribution_link_key(row: DistributionLinkRow) -> tuple[str, str, Optional[str]]:
    return (row.upstream_external_id, row.downstream_external_id, row.item_external_id)


@router.post(
    "/distribution-links",
    response_model=IngestResponse,
    summary="Import distribution links (DRP lanes)",
    description=(
        "Upsert inter-site replenishment lanes consumed by the DRP planner "
        "(engine/drp). Upsert key: (upstream_external_id, downstream_external_id, "
        "item_external_id) — item_external_id empty/absent = generic lane (all "
        "items), coexists with a specific-item lane on the same pair (see "
        "docs/contracts/distribution_links/format-distribution-links-tsv.md §4)."
    ),
)
def ingest_distribution_links(
    body: IngestDistributionLinksRequest,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_direct_ingest),
) -> IngestResponse:
    """Upsert distribution_links by (upstream, downstream, item) natural key. All-or-nothing: any error → HTTP 422."""
    loc_ext_ids = list({
        ext_id
        for row in body.distribution_links
        for ext_id in (row.upstream_external_id, row.downstream_external_id)
    })
    item_ext_ids = list({
        row.item_external_id for row in body.distribution_links if row.item_external_id
    })

    loc_map = _batch_existing(db, "locations", "external_id", "location_id", loc_ext_ids)
    item_map = _batch_existing(db, "items", "external_id", "item_id", item_ext_ids)

    errors: list[dict] = []
    seen_keys: dict[tuple[str, str, Optional[str]], int] = {}
    for i, row in enumerate(body.distribution_links):
        row_errs: list[str] = []
        if row.upstream_external_id not in loc_map:
            row_errs.append(f"upstream_external_id '{row.upstream_external_id}' not found in DB")
        if row.downstream_external_id not in loc_map:
            row_errs.append(f"downstream_external_id '{row.downstream_external_id}' not found in DB")
        if row.upstream_external_id == row.downstream_external_id:
            row_errs.append(
                f"upstream_external_id and downstream_external_id must differ "
                f"(both = '{row.upstream_external_id}')"
            )
        if row.item_external_id is not None and row.item_external_id not in item_map:
            row_errs.append(f"item_external_id '{row.item_external_id}' not found in DB")

        key = _distribution_link_key(row)
        if key in seen_keys:
            row_errs.append(
                "duplicate (upstream_external_id, downstream_external_id, "
                f"item_external_id) triplet within this payload, also at row {seen_keys[key]}"
            )
        else:
            seen_keys[key] = i

        if row_errs:
            errors.append({
                "upstream_external_id": row.upstream_external_id,
                "downstream_external_id": row.downstream_external_id,
                "item_external_id": row.item_external_id,
                "row": i,
                "errors": row_errs,
            })

    if errors:
        _raise_422(errors)

    if body.dry_run:
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.distribution_links), inserted=0, updated=0, errors=0),
            results=[
                {
                    "upstream_external_id": row.upstream_external_id,
                    "downstream_external_id": row.downstream_external_id,
                    "item_external_id": row.item_external_id,
                    "action": "dry_run",
                }
                for row in body.distribution_links
            ],
        )

    results: list[dict] = []
    inserted = updated = 0

    for row in body.distribution_links:
        upstream_id = loc_map[row.upstream_external_id]
        downstream_id = loc_map[row.downstream_external_id]
        item_id = item_map[row.item_external_id] if row.item_external_id else None
        min_qty = row.minimum_shipment_qty if row.minimum_shipment_qty is not None else 1.0
        multiple = row.transfer_multiple if row.transfer_multiple is not None else 1.0
        priority = row.priority if row.priority is not None else 100

        existing = db.execute(
            """
            SELECT distribution_link_id FROM distribution_links
            WHERE upstream_location_id = %s
              AND downstream_location_id = %s
              AND item_id IS NOT DISTINCT FROM %s
            """,
            (upstream_id, downstream_id, item_id),
        ).fetchone()

        if existing:
            distribution_link_id = existing["distribution_link_id"]
            db.execute(
                """
                UPDATE distribution_links
                SET transit_lead_time_days = %s,
                    minimum_shipment_qty = %s,
                    transfer_multiple = %s,
                    priority = %s,
                    active = %s,
                    updated_at = now()
                WHERE distribution_link_id = %s
                """,
                (row.transit_lead_time_days, min_qty, multiple, priority, row.active, distribution_link_id),
            )
            action = "updated"
            updated += 1
        else:
            distribution_link_id = uuid4()
            db.execute(
                """
                INSERT INTO distribution_links
                    (distribution_link_id, upstream_location_id, downstream_location_id, item_id,
                     transit_lead_time_days, minimum_shipment_qty, transfer_multiple, priority, active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (distribution_link_id, upstream_id, downstream_id, item_id,
                 row.transit_lead_time_days, min_qty, multiple, priority, row.active),
            )
            action = "inserted"
            inserted += 1

        results.append({
            "upstream_external_id": row.upstream_external_id,
            "downstream_external_id": row.downstream_external_id,
            "item_external_id": row.item_external_id,
            "distribution_link_id": str(distribution_link_id),
            "action": action,
        })

    logger.info(
        "ingest.distribution_links total=%d inserted=%d updated=%d",
        len(body.distribution_links), inserted, updated,
    )
    return _ok(inserted, updated, len(body.distribution_links), results)
