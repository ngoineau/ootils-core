"""
ingest_file.py — File-based ingestion entry point for Ootils.

Reads a TSV file from data/inbox/, parses it, calls the appropriate
/v1/ingest/<entity> endpoint via the FastAPI TestClient (in-process, no
HTTP server required), and archives the file to data/processed/ or
data/rejected/ with a JSON report alongside.

Usage:
    python scripts/ingest_file.py data/inbox/items.tsv
    python scripts/ingest_file.py data/inbox/items.tsv --dry-run

Environment:
    DATABASE_URL       (required) PostgreSQL DSN
    OOTILS_API_TOKEN   (required) bearer token for in-process API auth

Currently supported entities (V1):
    items.tsv                 → POST /v1/ingest/items
    locations.tsv             → POST /v1/ingest/locations
    suppliers.tsv             → POST /v1/ingest/suppliers
    supplier_items.tsv        → POST /v1/ingest/supplier-items
    item_planning_params.tsv  → POST /v1/ingest/planning-params (SCD2 transparent)
    on_hand.tsv               → POST /v1/ingest/on-hand
    purchase_orders.tsv       → POST /v1/ingest/purchase-orders
    customer_orders.tsv       → POST /v1/ingest/customer-orders
    forecasts.tsv             → POST /v1/ingest/forecast-demand
    transfers.tsv             → POST /v1/ingest/transfers
    bom_header.tsv            → POST /v1/ingest/bom  (bundle mode — merges with bom_components.tsv, N calls)

See docs/contracts/<entity>/format-<entity>-tsv.md for each format spec.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("ingest_file")

INBOX = ROOT / "data" / "inbox"
PROCESSED = ROOT / "data" / "processed"
REJECTED = ROOT / "data" / "rejected"

# ─────────────────────────────────────────────────────────────
# Dispatch table — filename → endpoint + payload body_key
# ─────────────────────────────────────────────────────────────
# Aligned with data-input-canonique-v1/endpoint_mapping.json.
# Extend here when adding a new supported entity.
DISPATCH: dict[str, dict[str, str]] = {
    "items.tsv":                {"endpoint": "/v1/ingest/items",            "body_key": "items"},
    "locations.tsv":            {"endpoint": "/v1/ingest/locations",        "body_key": "locations"},
    "suppliers.tsv":            {"endpoint": "/v1/ingest/suppliers",        "body_key": "suppliers"},
    "supplier_items.tsv":       {"endpoint": "/v1/ingest/supplier-items",   "body_key": "supplier_items"},
    "item_planning_params.tsv": {"endpoint": "/v1/ingest/planning-params",  "body_key": "params"},
    "on_hand.tsv":              {"endpoint": "/v1/ingest/on-hand",          "body_key": "on_hand"},
    "purchase_orders.tsv":      {"endpoint": "/v1/ingest/purchase-orders",  "body_key": "purchase_orders"},
    "customer_orders.tsv":      {"endpoint": "/v1/ingest/customer-orders",  "body_key": "customer_orders"},
    "forecasts.tsv":            {"endpoint": "/v1/ingest/forecast-demand",  "body_key": "forecasts"},
    "transfers.tsv":            {"endpoint": "/v1/ingest/transfers",        "body_key": "transfers"},
    # BOM bundle: entry point is bom_header.tsv, which auto-loads bom_components.tsv
    # alongside and emits N POSTs (one per BOM). Special-cased in main().
    "bom_header.tsv":           {"endpoint": "/v1/ingest/bom",              "body_key": "_bom_bundle"},
}


# ─────────────────────────────────────────────────────────────
# Type coercion helpers
# ─────────────────────────────────────────────────────────────
_TRUE_VALUES = {"true", "1", "yes", "y", "t"}
_FALSE_VALUES = {"false", "0", "no", "n", "f", ""}


def _to_bool(raw: str, *, field: str, line: str) -> bool:
    v = raw.strip().lower()
    if v in _TRUE_VALUES:
        return True
    if v in _FALSE_VALUES:
        return False
    raise ValueError(
        f"line {line}: {field} '{raw}' is not a valid boolean "
        f"(accepted: true/false/1/0/yes/no/y/n/t/f)"
    )


def _to_int(raw: str, *, field: str, line: str) -> int:
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"line {line}: {field} '{raw}' is not a valid integer") from e


def _to_float(raw: str, *, field: str, line: str) -> float:
    try:
        return float(raw)
    except ValueError as e:
        raise ValueError(f"line {line}: {field} '{raw}' is not a valid number") from e


# ─────────────────────────────────────────────────────────────
# TSV parsing
# ─────────────────────────────────────────────────────────────
def parse_tsv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Parse a UTF-8 TSV file into (headers, rows-as-dicts).

    - Tabulation separator (no escape — values must not contain raw tabs).
    - First non-empty line is the header.
    - Empty lines are skipped.
    - BOM (UTF-8 signature) is tolerated and stripped.
    - Returns dicts using header names as keys.
    """
    if not path.exists():
        raise FileNotFoundError(f"input file not found: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"input file is empty: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        rows = [r for r in reader if any(cell.strip() for cell in r)]

    if not rows:
        raise ValueError(f"input file contains no non-empty lines: {path}")

    headers = [h.strip() for h in rows[0]]
    if not all(headers):
        raise ValueError(f"header row contains empty column name: {headers}")

    data_rows: list[dict[str, str]] = []
    for i, raw in enumerate(rows[1:], start=2):  # line numbers 1-based, header is line 1
        if len(raw) != len(headers):
            raise ValueError(
                f"line {i}: column count {len(raw)} != header count {len(headers)}"
            )
        row = {headers[j]: raw[j].strip() for j in range(len(headers))}
        # Tag with the original line number for error reporting
        row["__line__"] = str(i)
        data_rows.append(row)

    return headers, data_rows


# ─────────────────────────────────────────────────────────────
# Payload construction
# ─────────────────────────────────────────────────────────────
def build_items_payload(rows: list[dict[str, str]], dry_run: bool) -> dict[str, Any]:
    """Build the JSON body for POST /v1/ingest/items.

    Applies defaults for optional fields when blank: item_type, uom, status.
    All-or-nothing validation happens server-side; we just pass values through.
    """
    items: list[dict[str, Any]] = []
    for row in rows:
        item = {
            "external_id": row.get("external_id", ""),
            "name": row.get("name", ""),
        }
        # Optional fields — only include if the column exists AND the cell is non-empty,
        # so the server applies its Pydantic defaults otherwise.
        if row.get("item_type"):
            item["item_type"] = row["item_type"]
        if row.get("uom"):
            item["uom"] = row["uom"]
        if row.get("status"):
            item["status"] = row["status"]
        items.append(item)
    return {"items": items, "dry_run": dry_run}


def build_locations_payload(rows: list[dict[str, str]], dry_run: bool) -> dict[str, Any]:
    """Build the JSON body for POST /v1/ingest/locations.

    Sends only non-empty optional fields so the server applies Pydantic defaults
    (e.g. location_type → 'dc'). The API itself validates `parent_external_id`
    refs against the payload + the existing DB.
    """
    locations: list[dict[str, Any]] = []
    for row in rows:
        loc = {
            "external_id": row.get("external_id", ""),
            "name": row.get("name", ""),
        }
        if row.get("location_type"):
            loc["location_type"] = row["location_type"]
        if row.get("country"):
            loc["country"] = row["country"]
        if row.get("timezone"):
            loc["timezone"] = row["timezone"]
        if row.get("parent_external_id"):
            loc["parent_external_id"] = row["parent_external_id"]
        locations.append(loc)
    return {"locations": locations, "dry_run": dry_run}


def build_suppliers_payload(rows: list[dict[str, str]], dry_run: bool) -> dict[str, Any]:
    """Build the JSON body for POST /v1/ingest/suppliers."""
    suppliers: list[dict[str, Any]] = []
    for row in rows:
        line = row.get("__line__", "?")
        sup: dict[str, Any] = {
            "external_id": row.get("external_id", ""),
            "name": row.get("name", ""),
        }
        if row.get("country"):
            sup["country"] = row["country"]
        if row.get("status"):
            sup["status"] = row["status"]
        if row.get("lead_time_days"):
            sup["lead_time_days"] = _to_int(row["lead_time_days"], field="lead_time_days", line=line)
        if row.get("reliability_score"):
            sup["reliability_score"] = _to_float(row["reliability_score"], field="reliability_score", line=line)
        suppliers.append(sup)
    return {"suppliers": suppliers, "dry_run": dry_run}


def build_supplier_items_payload(rows: list[dict[str, str]], dry_run: bool) -> dict[str, Any]:
    """Build the JSON body for POST /v1/ingest/supplier-items.

    Required: supplier_external_id, item_external_id, lead_time_days.
    Optional with defaults: currency='EUR', is_preferred=false.
    Optional nullable: moq, unit_cost.
    """
    pairs: list[dict[str, Any]] = []
    for row in rows:
        line = row.get("__line__", "?")
        si: dict[str, Any] = {
            "supplier_external_id": row.get("supplier_external_id", ""),
            "item_external_id": row.get("item_external_id", ""),
        }
        # lead_time_days is REQUIRED by the API (gt=0). Empty → fail explicitly here.
        if not row.get("lead_time_days"):
            raise ValueError(
                f"line {line}: lead_time_days is required and cannot be empty"
            )
        si["lead_time_days"] = _to_int(row["lead_time_days"], field="lead_time_days", line=line)

        if row.get("currency"):
            si["currency"] = row["currency"]
        if row.get("moq"):
            si["moq"] = _to_float(row["moq"], field="moq", line=line)
        if row.get("unit_cost"):
            si["unit_cost"] = _to_float(row["unit_cost"], field="unit_cost", line=line)
        # is_preferred: column may exist but be blank (treated as False).
        # Only normalize if the column is present in headers — but we already
        # received it through row dict via parse_tsv, so test for presence.
        if "is_preferred" in row:
            si["is_preferred"] = _to_bool(row["is_preferred"], field="is_preferred", line=line)
        pairs.append(si)
    return {"supplier_items": pairs, "dry_run": dry_run}


# Field-type map for item_planning_params columns.
# Server applies SCD2 partial-push: any column ABSENT from payload = "keep current value".
# So we must include a key in the payload ONLY when its TSV cell is non-empty,
# and we must coerce it to the right type.
_IPP_INT_FIELDS = {
    "lead_time_sourcing_days",
    "lead_time_manufacturing_days",
    "lead_time_transit_days",
    "planning_horizon_days",
    "lot_size_poq_periods",
    "frozen_time_fence_days",
    "slashed_time_fence_days",
    "consumption_window_days",
}
_IPP_FLOAT_FIELDS = {
    "safety_stock_qty",
    "safety_stock_days",
    "reorder_point_qty",
    "min_order_qty",
    "max_order_qty",
    "order_multiple",
    "economic_order_qty",
    "order_multiple_qty",
}
_IPP_BOOL_FIELDS = {"is_make"}
_IPP_STRING_FIELDS = {
    "lot_size_rule",
    "preferred_supplier_external_id",
    "forecast_consumption_strategy",
}


def build_item_planning_params_payload(rows: list[dict[str, str]], dry_run: bool) -> dict[str, Any]:
    """Build the JSON body for POST /v1/ingest/planning-params.

    SCD2 partial-push semantics:
      - empty cell → key is OMITTED from the payload → server keeps current value
      - non-empty cell → key included, value coerced to the right type

    Required: item_external_id, location_external_id.
    All other columns optional. The server resolves FKs (items, locations,
    suppliers for preferred_supplier_external_id) before any write.
    """
    params: list[dict[str, Any]] = []
    for row in rows:
        line = row.get("__line__", "?")
        p: dict[str, Any] = {
            "item_external_id": row.get("item_external_id", ""),
            "location_external_id": row.get("location_external_id", ""),
        }
        for col, raw in row.items():
            if col in ("item_external_id", "location_external_id", "__line__"):
                continue
            if not raw:  # empty cell → omit → server keeps current value
                continue
            if col in _IPP_INT_FIELDS:
                p[col] = _to_int(raw, field=col, line=line)
            elif col in _IPP_FLOAT_FIELDS:
                p[col] = _to_float(raw, field=col, line=line)
            elif col in _IPP_BOOL_FIELDS:
                p[col] = _to_bool(raw, field=col, line=line)
            elif col in _IPP_STRING_FIELDS:
                p[col] = raw
            else:
                # Unknown column → pass through as string; server may reject as 422 or ignore.
                # Better to let the API decide than to silently drop.
                p[col] = raw
        params.append(p)
    return {"params": params, "dry_run": dry_run}


_DATE_RE = __import__("re").compile(r"^\d{4}-\d{2}-\d{2}$")


def _to_date_str(raw: str, *, field: str, line: str) -> str:
    """Validate ISO date format YYYY-MM-DD. API expects a string here, not a date object."""
    v = raw.strip()
    if not _DATE_RE.match(v):
        raise ValueError(f"line {line}: {field} '{raw}' must be ISO date YYYY-MM-DD")
    return v


def build_on_hand_payload(rows: list[dict[str, str]], dry_run: bool) -> dict[str, Any]:
    """Build the JSON body for POST /v1/ingest/on-hand.

    Required: item_external_id, location_external_id, quantity, as_of_date.
    Optional: uom (default 'EA').

    Note: `lot_number` is part of the canonical V1 TSV template but the API
    Pydantic model does NOT consume it (V1.0). We drop it silently here to
    avoid sending unknown fields and confusing the user with rejections.
    """
    on_hand: list[dict[str, Any]] = []
    for row in rows:
        line = row.get("__line__", "?")
        if not row.get("quantity"):
            raise ValueError(f"line {line}: quantity is required and cannot be empty")
        if not row.get("as_of_date"):
            raise ValueError(f"line {line}: as_of_date is required and cannot be empty")
        rec: dict[str, Any] = {
            "item_external_id": row.get("item_external_id", ""),
            "location_external_id": row.get("location_external_id", ""),
            "quantity": _to_float(row["quantity"], field="quantity", line=line),
            "as_of_date": _to_date_str(row["as_of_date"], field="as_of_date", line=line),
        }
        if row.get("uom"):
            rec["uom"] = row["uom"]
        # lot_number intentionally dropped (V1.0 API doesn't consume it).
        on_hand.append(rec)
    return {"on_hand": on_hand, "dry_run": dry_run}


def build_purchase_orders_payload(rows: list[dict[str, str]], dry_run: bool) -> dict[str, Any]:
    """Build the JSON body for POST /v1/ingest/purchase-orders.

    Required: external_id, item_external_id, location_external_id,
              supplier_external_id, quantity, expected_delivery_date.
    Optional: uom (default 'EA'), status (default 'confirmed').
    """
    pos: list[dict[str, Any]] = []
    for row in rows:
        line = row.get("__line__", "?")
        # Required fields — explicit blank checks before building
        for required in ("quantity", "expected_delivery_date"):
            if not row.get(required):
                raise ValueError(f"line {line}: {required} is required and cannot be empty")
        po: dict[str, Any] = {
            "external_id": row.get("external_id", ""),
            "item_external_id": row.get("item_external_id", ""),
            "location_external_id": row.get("location_external_id", ""),
            "supplier_external_id": row.get("supplier_external_id", ""),
            "quantity": _to_float(row["quantity"], field="quantity", line=line),
            "expected_delivery_date": _to_date_str(row["expected_delivery_date"], field="expected_delivery_date", line=line),
        }
        if row.get("uom"):
            po["uom"] = row["uom"]
        if row.get("status"):
            po["status"] = row["status"]
        pos.append(po)
    return {"purchase_orders": pos, "dry_run": dry_run}


def build_customer_orders_payload(rows: list[dict[str, str]], dry_run: bool) -> dict[str, Any]:
    """Build the JSON body for POST /v1/ingest/customer-orders.

    Required: external_id, item_external_id, location_external_id,
              quantity, requested_delivery_date.
    Optional: status (default 'open').
    """
    cos: list[dict[str, Any]] = []
    for row in rows:
        line = row.get("__line__", "?")
        for required in ("quantity", "requested_delivery_date"):
            if not row.get(required):
                raise ValueError(f"line {line}: {required} is required and cannot be empty")
        co: dict[str, Any] = {
            "external_id": row.get("external_id", ""),
            "item_external_id": row.get("item_external_id", ""),
            "location_external_id": row.get("location_external_id", ""),
            "quantity": _to_float(row["quantity"], field="quantity", line=line),
            "requested_delivery_date": _to_date_str(row["requested_delivery_date"], field="requested_delivery_date", line=line),
        }
        if row.get("status"):
            co["status"] = row["status"]
        cos.append(co)
    return {"customer_orders": cos, "dry_run": dry_run}


def build_forecasts_payload(rows: list[dict[str, str]], dry_run: bool) -> dict[str, Any]:
    """Build the JSON body for POST /v1/ingest/forecast-demand.

    Required: item_external_id, location_external_id, quantity, bucket_date.
    Optional: time_grain (default 'week'), source (default 'statistical').
    quantity may be 0 (explicit "no forecast" for this bucket) — only blocks if blank.
    """
    forecasts: list[dict[str, Any]] = []
    for row in rows:
        line = row.get("__line__", "?")
        # quantity can be 0 but must be present
        q_raw = row.get("quantity", "")
        if q_raw == "":
            raise ValueError(f"line {line}: quantity is required and cannot be empty")
        if not row.get("bucket_date"):
            raise ValueError(f"line {line}: bucket_date is required and cannot be empty")
        rec: dict[str, Any] = {
            "item_external_id": row.get("item_external_id", ""),
            "location_external_id": row.get("location_external_id", ""),
            "quantity": _to_float(q_raw, field="quantity", line=line),
            "bucket_date": _to_date_str(row["bucket_date"], field="bucket_date", line=line),
        }
        if row.get("time_grain"):
            rec["time_grain"] = row["time_grain"]
        if row.get("source"):
            rec["source"] = row["source"]
        forecasts.append(rec)
    return {"forecasts": forecasts, "dry_run": dry_run}


def build_transfers_payload(rows: list[dict[str, str]], dry_run: bool) -> dict[str, Any]:
    """Build the JSON body for POST /v1/ingest/transfers.

    Required: external_id, item_external_id, from_location_external_id,
              to_location_external_id, quantity, expected_delivery_date.
    Optional: status (default 'planned').
    Local extra check: from != to (also enforced server-side but caught earlier here).
    """
    transfers: list[dict[str, Any]] = []
    for row in rows:
        line = row.get("__line__", "?")
        for required in ("quantity", "expected_delivery_date"):
            if not row.get(required):
                raise ValueError(f"line {line}: {required} is required and cannot be empty")
        f_loc = row.get("from_location_external_id", "")
        t_loc = row.get("to_location_external_id", "")
        if f_loc and t_loc and f_loc == t_loc:
            raise ValueError(
                f"line {line}: from_location_external_id and to_location_external_id "
                f"must differ (both = '{f_loc}')"
            )
        tr: dict[str, Any] = {
            "external_id": row.get("external_id", ""),
            "item_external_id": row.get("item_external_id", ""),
            "from_location_external_id": f_loc,
            "to_location_external_id": t_loc,
            "quantity": _to_float(row["quantity"], field="quantity", line=line),
            "expected_delivery_date": _to_date_str(row["expected_delivery_date"], field="expected_delivery_date", line=line),
        }
        if row.get("status"):
            tr["status"] = row["status"]
        transfers.append(tr)
    return {"transfers": transfers, "dry_run": dry_run}


PAYLOAD_BUILDERS = {
    "items.tsv":                build_items_payload,
    "locations.tsv":            build_locations_payload,
    "suppliers.tsv":            build_suppliers_payload,
    "supplier_items.tsv":       build_supplier_items_payload,
    "item_planning_params.tsv": build_item_planning_params_payload,
    "on_hand.tsv":              build_on_hand_payload,
    "purchase_orders.tsv":      build_purchase_orders_payload,
    "customer_orders.tsv":      build_customer_orders_payload,
    "forecasts.tsv":            build_forecasts_payload,
    "transfers.tsv":            build_transfers_payload,
}


# ─────────────────────────────────────────────────────────────
# API call (in-process via TestClient)
# ─────────────────────────────────────────────────────────────
def call_api(endpoint: str, payload: dict[str, Any], token: str) -> tuple[int, dict]:
    """Call the FastAPI app in-process via TestClient. No HTTP server needed."""
    from fastapi.testclient import TestClient
    from ootils_core.api.app import app

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    with TestClient(app) as client:
        resp = client.post(endpoint, json=payload, headers=headers)
    try:
        body = resp.json()
    except Exception:
        body = {"raw_body": resp.text}
    return resp.status_code, body


# ─────────────────────────────────────────────────────────────
# Archiving
# ─────────────────────────────────────────────────────────────
def archive(source: Path, dest_dir: Path, report: dict[str, Any]) -> tuple[Path, Path]:
    """Move source file to dest_dir with a timestamp-suffixed name + drop report next to it."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    stem = source.stem
    suffix = source.suffix
    new_name = f"{stem}_{ts}{suffix}"
    target = dest_dir / new_name
    shutil.move(str(source), str(target))

    report_path = dest_dir / f"{stem}_{ts}.report.json"
    report_path.write_text(
        json.dumps(report, indent=2, default=str, ensure_ascii=False),
        encoding="utf-8",
    )
    return target, report_path


# ─────────────────────────────────────────────────────────────
# BOM bundle handler — 2 files (header + components) → N API calls (1 per BOM)
# ─────────────────────────────────────────────────────────────
def _build_bom_payloads(
    header_rows: list[dict[str, str]],
    component_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Group components by (parent_external_id, bom_version) and merge with
    header metadata. Returns one payload dict per BOM, ready to POST.

    Raises ValueError if a component references a (parent, version) absent
    from the header, or if a header has zero components.
    """
    # Index headers by (parent, version)
    header_index: dict[tuple[str, str], dict[str, str]] = {}
    for hr in header_rows:
        line = hr.get("__line__", "?")
        parent = hr.get("parent_external_id", "").strip()
        version = hr.get("bom_version", "").strip() or "1.0"
        if not parent:
            raise ValueError(f"bom_header.tsv line {line}: parent_external_id is required")
        key = (parent, version)
        if key in header_index:
            raise ValueError(
                f"bom_header.tsv line {line}: duplicate (parent={parent}, version={version})"
            )
        header_index[key] = hr

    # Group components by (parent, version)
    components_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for cr in component_rows:
        line = cr.get("__line__", "?")
        parent = cr.get("parent_external_id", "").strip()
        version = cr.get("bom_version", "").strip()
        if not parent or not version:
            raise ValueError(
                f"bom_components.tsv line {line}: parent_external_id and bom_version are required"
            )
        key = (parent, version)
        if key not in header_index:
            raise ValueError(
                f"bom_components.tsv line {line}: (parent={parent}, version={version}) "
                f"has no matching row in bom_header.tsv"
            )
        comp_ext = cr.get("component_external_id", "").strip()
        if not comp_ext:
            raise ValueError(
                f"bom_components.tsv line {line}: component_external_id is required"
            )
        if not cr.get("quantity_per"):
            raise ValueError(
                f"bom_components.tsv line {line}: quantity_per is required"
            )
        comp: dict[str, Any] = {
            "component_external_id": comp_ext,
            "quantity_per": _to_float(cr["quantity_per"], field="quantity_per", line=line),
        }
        if cr.get("uom"):
            comp["uom"] = cr["uom"]
        if cr.get("scrap_factor"):
            comp["scrap_factor"] = _to_float(cr["scrap_factor"], field="scrap_factor", line=line)
        components_by_key.setdefault(key, []).append(comp)

    # Build one payload per BOM (every header must have at least 1 component)
    payloads: list[dict[str, Any]] = []
    for (parent, version), hr in header_index.items():
        comps = components_by_key.get((parent, version))
        if not comps:
            raise ValueError(
                f"bom_header.tsv: BOM (parent={parent}, version={version}) "
                f"has no components in bom_components.tsv"
            )
        line = hr.get("__line__", "?")
        payload: dict[str, Any] = {
            "parent_external_id": parent,
            "bom_version": version,
            "components": comps,
        }
        if hr.get("effective_from"):
            payload["effective_from"] = _to_date_str(
                hr["effective_from"], field="effective_from", line=line
            )
        payloads.append(payload)

    return payloads


def handle_bom_bundle(header_path: Path, dry_run: bool, token: str) -> int:
    """Bundle BOM ingestion: reads header + components from the same dir, emits N API calls.

    Returns process exit code (0 if all BOMs OK, non-zero on any failure).
    """
    components_path = header_path.parent / "bom_components.tsv"
    if not components_path.exists():
        logger.error(
            "bom_components.tsv not found next to bom_header.tsv. "
            "Both files must be present in the same directory: %s",
            header_path.parent,
        )
        return 7

    started_at = datetime.now(timezone.utc).isoformat()
    endpoint = "/v1/ingest/bom"

    # ── 1. Parse both files ────────────────────────────────────
    try:
        _, header_rows = parse_tsv(header_path)
        _, component_rows = parse_tsv(components_path)
    except (FileNotFoundError, ValueError) as e:
        logger.error("parse error: %s", e)
        report = {
            "files": ["bom_header.tsv", "bom_components.tsv"],
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "parse_error",
            "error": str(e),
        }
        if not dry_run:
            archive(header_path, REJECTED, report)
            if components_path.exists():
                archive(components_path, REJECTED, {"bundled_with": "bom_header.tsv", "see_report": "above"})
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        return 4

    # ── 2. Cross-file merge & validation ───────────────────────
    try:
        payloads = _build_bom_payloads(header_rows, component_rows)
    except ValueError as e:
        logger.error("bundle validation error: %s", e)
        report = {
            "files": ["bom_header.tsv", "bom_components.tsv"],
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "bundle_validation_error",
            "error": str(e),
            "header_rows": len(header_rows),
            "component_rows": len(component_rows),
        }
        if not dry_run:
            archive(header_path, REJECTED, report)
            if components_path.exists():
                archive(components_path, REJECTED, {"bundled_with": "bom_header.tsv"})
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        return 4

    logger.info(
        "BOM bundle: %d header rows, %d component rows → %d BOMs to ingest",
        len(header_rows), len(component_rows), len(payloads),
    )

    # ── 3. Call API N times (1 per BOM) ────────────────────────
    bom_results: list[dict[str, Any]] = []
    all_ok = True
    for i, payload in enumerate(payloads, 1):
        body_with_dry = dict(payload)
        body_with_dry["dry_run"] = dry_run
        logger.info(
            "  [%d/%d] POST %s for parent=%s version=%s (%d components)",
            i, len(payloads), endpoint,
            payload["parent_external_id"], payload["bom_version"],
            len(payload["components"]),
        )
        try:
            status_code, body = call_api(endpoint, body_with_dry, token)
        except Exception as e:  # noqa: BLE001
            logger.exception("API call crashed for BOM #%d", i)
            bom_results.append({
                "parent_external_id": payload["parent_external_id"],
                "bom_version": payload["bom_version"],
                "outcome": "api_crash",
                "error": str(e),
            })
            all_ok = False
            continue
        accepted = 200 <= status_code < 300
        bom_results.append({
            "parent_external_id": payload["parent_external_id"],
            "bom_version": payload["bom_version"],
            "components_count": len(payload["components"]),
            "outcome": "ok" if accepted else "rejected",
            "http_status": status_code,
            "api_response": body,
        })
        if not accepted:
            all_ok = False

    # ── 4. Archive both files together ─────────────────────────
    outcome = "ok" if all_ok else "partial" if any(r["outcome"] == "ok" for r in bom_results) else "rejected"
    report = {
        "files": ["bom_header.tsv", "bom_components.tsv"],
        "endpoint": endpoint,
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "outcome": outcome,
        "dry_run": dry_run,
        "boms_total": len(payloads),
        "boms_ok": sum(1 for r in bom_results if r["outcome"] == "ok"),
        "boms_failed": sum(1 for r in bom_results if r["outcome"] != "ok"),
        "bom_results": bom_results,
    }

    if dry_run:
        logger.info("DRY-RUN — files not moved. Outcome: %s", outcome)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0 if all_ok else 1

    dest = PROCESSED if all_ok else REJECTED
    archive(header_path, dest, report)
    if components_path.exists():
        archive(components_path, dest, {"bundled_with": "bom_header.tsv", "see_main_report": True})
    logger.info(
        "%s — bundle archived to %s (%d BOMs, %d ok, %d failed)",
        outcome.upper(), dest, len(payloads),
        report["boms_ok"], report["boms_failed"],
    )
    if not all_ok:
        print(
            json.dumps(
                [r for r in bom_results if r["outcome"] != "ok"],
                indent=2, ensure_ascii=False,
            ),
            file=sys.stderr,
        )
    return 0 if all_ok else 1


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingest a TSV file from data/inbox/ into Ootils."
    )
    parser.add_argument("path", help="path to TSV file (typically under data/inbox/)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate but do not persist (no archive either)",
    )
    args = parser.parse_args(argv)

    src = Path(args.path).resolve()
    filename = src.name

    # Pre-flight: env vars
    token = os.environ.get("OOTILS_API_TOKEN")
    if not token:
        logger.error("OOTILS_API_TOKEN not set — refusing to run")
        return 2
    if not os.environ.get("DATABASE_URL"):
        logger.error("DATABASE_URL not set — refusing to run")
        return 2

    # Pre-flight: filename supported?
    if filename == "bom_components.tsv":
        logger.error(
            "bom_components.tsv cannot be ingested alone — it has no metadata. "
            "Use 'bom_header.tsv' as the entry point; the script will auto-load "
            "bom_components.tsv from the same directory."
        )
        return 6
    if filename not in DISPATCH:
        logger.error(
            "unsupported filename '%s'. Supported: %s",
            filename, sorted(DISPATCH.keys()),
        )
        return 3

    # ── BOM bundle: special path (2 files → N API calls) ──
    if filename == "bom_header.tsv":
        return handle_bom_bundle(src, args.dry_run, token)

    cfg = DISPATCH[filename]
    builder = PAYLOAD_BUILDERS[filename]

    started_at = datetime.now(timezone.utc).isoformat()
    logger.info("ingesting %s → %s", src, cfg["endpoint"])

    # ── 1. Parse ───────────────────────────────────────────
    try:
        headers, rows = parse_tsv(src)
    except (FileNotFoundError, ValueError) as e:
        logger.error("parse error: %s", e)
        report = {
            "filename": filename,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "parse_error",
            "error": str(e),
        }
        if not args.dry_run and src.exists():
            archive(src, REJECTED, report)
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        return 4

    logger.info("parsed %d rows (header: %s)", len(rows), headers)

    # ── 2. Build payload ───────────────────────────────────
    payload = builder(rows, args.dry_run)

    # ── 3. Call API ────────────────────────────────────────
    try:
        status_code, body = call_api(cfg["endpoint"], payload, token)
    except Exception as e:  # noqa: BLE001
        logger.exception("API call crashed")
        report = {
            "filename": filename,
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "api_crash",
            "error": str(e),
            "rows_parsed": len(rows),
        }
        if not args.dry_run:
            archive(src, REJECTED, report)
        else:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        return 5

    # ── 4. Outcome + archive ──────────────────────────────
    accepted = 200 <= status_code < 300
    outcome = "ok" if accepted else "rejected"
    summary = body.get("summary", {}) if isinstance(body, dict) else {}

    report = {
        "filename": filename,
        "endpoint": cfg["endpoint"],
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "outcome": outcome,
        "dry_run": args.dry_run,
        "http_status": status_code,
        "rows_parsed": len(rows),
        "api_summary": summary,
        "api_response": body,
    }

    if args.dry_run:
        logger.info("DRY-RUN — file not moved. Outcome: %s (HTTP %d)", outcome, status_code)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0 if accepted else 1

    dest = PROCESSED if accepted else REJECTED
    target, report_path = archive(src, dest, report)
    logger.info(
        "%s — archived to %s (report: %s)",
        outcome.upper(), target, report_path,
    )
    if not accepted:
        # Echo errors to stderr for ops visibility
        print(json.dumps(body, indent=2, ensure_ascii=False), file=sys.stderr)
    return 0 if accepted else 1


if __name__ == "__main__":
    sys.exit(main())
