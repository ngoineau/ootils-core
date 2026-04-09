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
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Optional
from uuid import UUID, uuid4

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db
from ootils_core.engine.dq.engine import run_dq

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/ingest", tags=["ingest"])

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


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _ok(inserted: int, updated: int, total: int, results: list[dict], batch_id: UUID | None = None, dq_status: str | None = None) -> IngestResponse:
    return IngestResponse(
        status="ok",
        summary=IngestSummary(total=total, inserted=inserted, updated=updated, errors=0),
        results=results,
        batch_id=batch_id,
        dq_status=dq_status,
    )


def _dry_run_response(items: list[Any], label: str = "external_id") -> IngestResponse:
    return IngestResponse(
        status="dry_run",
        summary=IngestSummary(total=len(items), inserted=0, updated=0, errors=0),
        results=[{"action": "dry_run", label: getattr(row, label, "?")} for row in items],
    )


def _raise_422(errors: list[dict]) -> None:
    """Raise HTTP 422 with structured error list. Nothing is persisted."""
    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=errors)


def _create_ingest_batch(
    db: psycopg.Connection,
    entity_type: str,
    rows_data: list[Any],
    source_system: str = "ingest_api",
) -> UUID:
    """
    Create an ingest_batch record and persist all rows as ingest_rows.
    Returns the new batch_id.
    """
    import json as _json
    batch_id = uuid4()
    db.execute(
        """
        INSERT INTO ingest_batches
            (batch_id, entity_type, source_system, status, total_rows, submitted_by)
        VALUES (%s, %s, %s, 'processing', %s, 'ingest_api')
        """,
        (batch_id, entity_type, source_system, len(rows_data)),
    )
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


def _trigger_dq(db: psycopg.Connection, batch_id: UUID) -> str:
    """Run DQ pipeline on a batch. Returns dq_status string, never raises."""
    try:
        result = run_dq(db, batch_id)
        return result.batch_dq_status
    except Exception as exc:
        logger.warning("DQ run failed for batch %s: %s", batch_id, exc)
        return "unknown"


def _ensure_projection_series(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
) -> bool:
    """
    Ensure a ProjectionSeries + PI bucket nodes exist for (item, location, scenario).
    Creates them if missing. Returns True if created, False if already existed.
    """
    from datetime import date, timedelta

    existing = db.execute(
        """
        SELECT series_id FROM projection_series
        WHERE item_id = %s AND location_id = %s AND scenario_id = %s
        """,
        (item_id, location_id, scenario_id),
    ).fetchone()

    if existing:
        return False

    series_id = uuid4()
    today = date.today()
    horizon_start = today
    horizon_end = today + timedelta(days=90)

    db.execute(
        """
        INSERT INTO projection_series (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, now(), now())
        ON CONFLICT (item_id, location_id, scenario_id) DO NOTHING
        """,
        (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end),
    )

    row = db.execute(
        "SELECT series_id FROM projection_series WHERE item_id = %s AND location_id = %s AND scenario_id = %s",
        (item_id, location_id, scenario_id),
    ).fetchone()
    actual_series_id = UUID(str(row["series_id"])) if row else series_id

    for i in range(90):
        day_start = today + timedelta(days=i)
        day_end = day_start + timedelta(days=1)
        db.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                time_grain, time_span_start, time_span_end, time_ref,
                projection_series_id, bucket_sequence,
                opening_stock, inflows, outflows, closing_stock,
                has_shortage, shortage_qty, is_dirty, active,
                created_at, updated_at
            ) VALUES (
                %s, 'ProjectedInventory', %s, %s, %s,
                'day', %s, %s, %s,
                %s, %s,
                0, 0, 0, 0,
                FALSE, 0, TRUE, TRUE,
                now(), now()
            )
            ON CONFLICT DO NOTHING
            """,
            (
                uuid4(), scenario_id, item_id, location_id,
                day_start, day_end, day_start,
                actual_series_id, i,
            ),
        )

    logger.info(
        "_ensure_projection_series: created series + 90 PI buckets for item=%s loc=%s",
        item_id, location_id,
    )
    return True


def _wire_node_to_pi(
    db: psycopg.Connection,
    node_id: UUID,
    node_type: str,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
    time_ref: date,
) -> int:
    """
    Connect a supply/demand node to the matching PI bucket via an edge.
    Returns number of edges created.
    """
    if node_type in ("PurchaseOrderSupply", "WorkOrderSupply", "PlannedSupply", "TransferSupply", "OnHandSupply"):
        edge_type = "replenishes"
        direction = "supply"
    elif node_type in ("ForecastDemand", "CustomerOrderDemand"):
        edge_type = "consumes"
        direction = "demand"
    else:
        return 0

    pi_row = db.execute(
        """
        SELECT node_id FROM nodes
        WHERE node_type = 'ProjectedInventory'
          AND item_id = %s
          AND location_id = %s
          AND scenario_id = %s
          AND active = TRUE
          AND time_span_start <= %s
          AND time_span_end > %s
        ORDER BY time_span_start ASC
        LIMIT 1
        """,
        (item_id, location_id, scenario_id, time_ref, time_ref),
    ).fetchone()

    if pi_row is None:
        logger.debug(
            "_wire_node_to_pi: no PI bucket found for item=%s loc=%s date=%s",
            item_id, location_id, time_ref,
        )
        return 0

    pi_node_id = pi_row["node_id"]
    edge_id = uuid4()
    from_id, to_id = node_id, pi_node_id

    db.execute(
        """
        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active, created_at)
        VALUES (%s, %s, %s, %s, %s, TRUE, now())
        ON CONFLICT DO NOTHING
        """,
        (edge_id, edge_type, from_id, to_id, scenario_id),
    )

    logger.debug(
        "_wire_node_to_pi: wired node=%s (%s) → PI=%s via %s",
        node_id, node_type, pi_node_id, edge_type,
    )
    return 1


def _emit_ingestion_event(db: psycopg.Connection, scenario_id: UUID, node_id: UUID) -> None:
    """Create an unprocessed ingestion_complete event to trigger recalculation."""
    from datetime import datetime, timezone
    db.execute(
        """
        INSERT INTO events (event_id, event_type, scenario_id, trigger_node_id, processed, source, created_at)
        VALUES (%s, 'ingestion_complete', %s, %s, FALSE, 'ingestion', %s)
        """,
        (uuid4(), scenario_id, node_id, datetime.now(timezone.utc)),
    )


def _batch_existing(
    db: psycopg.Connection,
    table: str,
    id_col: str,
    pk_col: str,
    external_ids: list[str],
) -> dict[str, UUID]:
    """Return {external_id: pk} for all rows matching the given external_ids."""
    if not external_ids:
        return {}
    rows = db.execute(
        f"SELECT {id_col}, {pk_col} FROM {table} WHERE {id_col} = ANY(%s)",
        (external_ids,),
    ).fetchall()
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
async def ingest_items(
    body: IngestItemsRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
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
    batch_id = _create_ingest_batch(db, "items", [it.model_dump() for it in body.items])
    dq_status = _trigger_dq(db, batch_id)
    return _ok(inserted, updated, len(body.items), results, batch_id=batch_id, dq_status=dq_status)


# ─────────────────────────────────────────────────────────────
# 2. POST /v1/ingest/locations
# ─────────────────────────────────────────────────────────────

VALID_LOCATION_TYPES = {"plant", "dc", "warehouse", "supplier_virtual", "customer_virtual"}


class LocationRow(BaseModel):
    external_id: str = Field(..., description="Site/DC identifier (e.g. DC-ATL). Upsert key.")
    name: str = Field(..., description="Site label / description.")
    location_type: str = Field("dc", description="Location type. Values: plant | dc | warehouse | supplier_virtual | customer_virtual.")
    country: Optional[str] = None
    timezone: Optional[str] = None
    parent_external_id: Optional[str] = Field(None, description="External_id of the parent site (optional, for hierarchies).")

    @field_validator("external_id", "name")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v


class IngestLocationsRequest(BaseModel):
    locations: list[LocationRow]
    dry_run: bool = False


@router.post("/locations", response_model=IngestResponse, summary="Import locations", description="Upsert a batch of sites/DCs. Upsert key: external_id.")
async def ingest_locations(
    body: IngestLocationsRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> IngestResponse:
    """Upsert locations by external_id. All-or-nothing: any validation error → HTTP 422."""
    errors: list[dict] = []

    # Build set of external_ids in the payload (for parent validation)
    payload_ext_ids = {loc.external_id for loc in body.locations}

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
        if row_errs:
            errors.append({"external_id": loc.external_id, "row": i, "errors": row_errs})

    if errors:
        _raise_422(errors)

    if body.dry_run:
        return _dry_run_response(body.locations)

    existing = _batch_existing(
        db, "locations", "external_id", "location_id",
        [loc.external_id for loc in body.locations],
    )

    results: list[dict] = []
    inserted = updated = 0

    for loc in body.locations:
        if loc.external_id in existing:
            db.execute(
                """
                UPDATE locations
                SET name = %s, location_type = %s, country = %s, timezone = %s, updated_at = now()
                WHERE external_id = %s
                """,
                (loc.name, loc.location_type, loc.country, loc.timezone, loc.external_id),
            )
            results.append({"external_id": loc.external_id, "location_id": str(existing[loc.external_id]), "action": "updated"})
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
            results.append({"external_id": loc.external_id, "location_id": str(location_id), "action": "inserted"})
            inserted += 1

    logger.info("ingest.locations total=%d inserted=%d updated=%d", len(body.locations), inserted, updated)
    batch_id = _create_ingest_batch(db, "locations", [loc.model_dump() for loc in body.locations])
    dq_status = _trigger_dq(db, batch_id)
    return _ok(inserted, updated, len(body.locations), results, batch_id=batch_id, dq_status=dq_status)


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
    moq: Optional[float] = None          # not a suppliers column, accepted but not persisted
    currency: Optional[str] = None       # not a suppliers column, accepted but not persisted
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
async def ingest_suppliers(
    body: IngestSuppliersRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
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
    batch_id = _create_ingest_batch(db, "suppliers", [sup.model_dump() for sup in body.suppliers])
    dq_status = _trigger_dq(db, batch_id)
    return _ok(inserted, updated, len(body.suppliers), results, batch_id=batch_id, dq_status=dq_status)


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
async def ingest_supplier_items(
    body: IngestSupplierItemsRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
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
        results = [
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

    results: list[dict] = []
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
    batch_id = _create_ingest_batch(db, "supplier_items", [si.model_dump() for si in body.supplier_items])
    dq_status = _trigger_dq(db, batch_id)
    return _ok(inserted, updated, len(body.supplier_items), results, batch_id=batch_id, dq_status=dq_status)


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
async def ingest_on_hand(
    body: IngestOnHandRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
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
        results = [
            {"item_external_id": r.item_external_id, "location_external_id": r.location_external_id, "action": "dry_run"}
            for r in body.on_hand
        ]
        return IngestResponse(
            status="dry_run",
            summary=IngestSummary(total=len(body.on_hand), inserted=0, updated=0, errors=0),
            results=results,
        )

    results: list[dict] = []
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
    batch_id = _create_ingest_batch(db, "on_hand", [r.model_dump() for r in body.on_hand])
    dq_status = _trigger_dq(db, batch_id)
    return _ok(inserted, updated, len(body.on_hand), results, batch_id=batch_id, dq_status=dq_status)


# ─────────────────────────────────────────────────────────────
# 6. POST /v1/ingest/purchase-orders
# ─────────────────────────────────────────────────────────────

VALID_PO_STATUSES = {"draft", "confirmed", "in_transit", "received", "cancelled"}


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


class IngestPurchaseOrdersRequest(BaseModel):
    purchase_orders: list[PurchaseOrderRow]
    dry_run: bool = False


@router.post("/purchase-orders", response_model=IngestResponse, summary="Import purchase orders", description="Upsert purchase orders (PurchaseOrderSupply) with ERP external_id tracking.")
async def ingest_purchase_orders(
    body: IngestPurchaseOrdersRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
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
        active = po.status != "cancelled"

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
    batch_id = _create_ingest_batch(db, "purchase_orders", [po.model_dump() for po in body.purchase_orders])
    dq_status = _trigger_dq(db, batch_id)
    return _ok(inserted, updated, len(body.purchase_orders), results, batch_id=batch_id, dq_status=dq_status)


# ─────────────────────────────────────────────────────────────
# 7. POST /v1/ingest/forecast-demand
# ─────────────────────────────────────────────────────────────

VALID_TIME_GRAINS = {"exact_date", "day", "week", "month", "timeless"}
VALID_FORECAST_SOURCES = {"statistical", "consensus", "manual", "ml"}


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
async def ingest_forecast_demand(
    body: IngestForecastRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
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

    results: list[dict] = []
    inserted = updated = 0

    for fc in body.forecasts:
        item_id = item_map[fc.item_external_id]
        location_id = loc_map[fc.location_external_id]

        # Ensure PI series exists for this (item, location)
        _ensure_projection_series(db, item_id, location_id, BASELINE_SCENARIO_ID)

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
                SET quantity = %s, is_dirty = TRUE, updated_at = now()
                WHERE node_id = %s
                """,
                (fc.quantity, fc_node_id),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, fc_node_id)
            _wire_node_to_pi(db, fc_node_id, "ForecastDemand", item_id, location_id, BASELINE_SCENARIO_ID, fc.bucket_date)
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
                     quantity, time_grain, time_ref, is_dirty, active)
                VALUES (%s, 'ForecastDemand', %s, %s, %s, %s, %s, %s, TRUE, TRUE)
                """,
                (node_id, BASELINE_SCENARIO_ID, item_id, location_id,
                 fc.quantity, fc.time_grain, fc.bucket_date),
            )
            _emit_ingestion_event(db, BASELINE_SCENARIO_ID, node_id)
            _wire_node_to_pi(db, node_id, "ForecastDemand", item_id, location_id, BASELINE_SCENARIO_ID, fc.bucket_date)
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
    batch_id = _create_ingest_batch(db, "forecast_demand", [fc.model_dump() for fc in body.forecasts])
    dq_status = _trigger_dq(db, batch_id)
    return _ok(inserted, updated, len(body.forecasts), results, batch_id=batch_id, dq_status=dq_status)


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
async def ingest_resources(
    body: IngestResourcesRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
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
    return _ok(inserted, updated, len(body.resources), results)


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
async def ingest_work_orders(
    body: IngestWorkOrdersRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
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
    batch_id = _create_ingest_batch(db, "work_orders", [wo.model_dump() for wo in body.work_orders])
    dq_status = _trigger_dq(db, batch_id)
    return _ok(inserted, updated, len(body.work_orders), results, batch_id=batch_id, dq_status=dq_status)


# ─────────────────────────────────────────────────────────────
# 10. POST /v1/ingest/customer-orders
# ─────────────────────────────────────────────────────────────

VALID_CUSTOMER_ORDER_STATUSES = {"open", "confirmed", "shipped", "delivered", "cancelled"}


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
async def ingest_customer_orders(
    body: IngestCustomerOrdersRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
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
        active = co.status not in ("delivered", "cancelled")

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
    batch_id = _create_ingest_batch(db, "customer_orders", [co.model_dump() for co in body.customer_orders])
    dq_status = _trigger_dq(db, batch_id)
    return _ok(inserted, updated, len(body.customer_orders), results, batch_id=batch_id, dq_status=dq_status)


# ─────────────────────────────────────────────────────────────
# 11. POST /v1/ingest/transfers
# ─────────────────────────────────────────────────────────────

VALID_TRANSFER_STATUSES = {"planned", "in_transit", "delivered", "cancelled"}


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
async def ingest_transfers(
    body: IngestTransfersRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
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
        from_location_id = loc_map[t.from_location_external_id]
        to_location_id = loc_map[t.to_location_external_id]
        active = t.status not in ("delivered", "cancelled")

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
    batch_id = _create_ingest_batch(db, "transfers", [t.model_dump() for t in body.transfers])
    dq_status = _trigger_dq(db, batch_id)
    return _ok(inserted, updated, len(body.transfers), results, batch_id=batch_id, dq_status=dq_status)
