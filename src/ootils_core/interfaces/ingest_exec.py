"""
ingest_exec.py — the canonical, importable TSV-ingest execution primitives
(ADR-042 PR-4b, the daily orchestrator's prerequisite).

WHY THIS MODULE EXISTS. ``scripts/ingest_file.py`` (PR-4a) is the manual/dev
entry point for loading one TSV drop — filename grammar, per-entity payload
builders, the in-process API call, and archiving. ``engine/ingest/
daily_orchestrator.py`` (PR-4b) needs the EXACT SAME logic to load the
"green" feeds of a governed daily run, with zero drift between the two
callers (a second, slightly-different implementation of "how a TSV becomes
an API payload" would be exactly the kind of silent divergence this repo's
conventions forbid). Directly importing ``scripts/ingest_file.py`` from
``src/ootils_core/`` is improper: ``scripts/`` is NOT part of the installed
package (``[tool.setuptools.packages.find] where = ["src"]``), and the
script reconfigures the root logger as an import side effect
(``logging.basicConfig(...)`` at module scope) — acceptable for a CLI
entry point, not for a library import reachable from engine code.

So the reusable, DB/API-agnostic pieces live HERE — the single canonical
implementation — and ``scripts/ingest_file.py`` becomes a thin re-export
shim over this module for everything it used to define itself, keeping its
CLI behaviour (including the exact objects the PR-4a test suite imports and
monkeypatches, e.g. ``DISPATCH``) byte-for-byte unchanged. Zero second
canonical writer: there is exactly ONE ``parse_tsv``, ONE set of payload
builders, ONE ``call_api``, ONE ``archive``.

DELIBERATELY NOT MOVED HERE (stay in ``scripts/ingest_file.py``, unchanged):
``PROCESSED``/``REJECTED``/``INBOX`` (repo-relative default archive
directories — the daily orchestrator computes its OWN, inbox-relative
destinations, see its module docstring) and ``_find_archived`` (reads those
same module globals directly — the PR-4a test suite monkeypatches them on
``ingest_file`` itself and expects ``_find_archived`` to observe the patch,
which only holds if the function keeps living in that module). BOM-bundle
orchestration (``handle_bom_bundle``) and part-group CLI orchestration
(``handle_part_group``) also stay in ``ingest_file.py``: both are CLI-shaped
(dry-run JSON printing to stdout, process exit codes) — only the DB/API
primitives they call (``parse_tsv``, ``PAYLOAD_BUILDERS``, ``call_api``,
``archive``/``archive_group``, ``build_bom_payloads``) are canonical here.
"""
from __future__ import annotations

import csv
import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ─────────────────────────────────────────────────────────────
# Filename grammar (PR-4a) — pure, no filesystem access
# ─────────────────────────────────────────────────────────────
# Beyond the canonical '<entity>.tsv' name (the DISPATCH keys below), two
# real-world drop conventions are accepted:
#   - daily drop:  '<entity>_<AAAAMMJJ>.tsv'          e.g. on_hand_20260718.tsv
#   - part file:   '<entity>.partNN.tsv'              e.g. forecasts.part01.tsv
#                  '<entity>.partNN_<AAAAMMJJ>.tsv'   e.g. forecasts.part01_20260718.tsv
# The date suffix is ignored for dispatch (informational only — the entity
# alone decides the endpoint). Part files sharing the same (entity, date)
# in the same directory are grouped into ONE logical load — see
# `find_sibling_parts`/`parse_tsv_parts` below and `handle_part_group` in
# scripts/ingest_file.py's main() dispatch section.
_DATE_SUFFIX_RE = re.compile(r"^(?P<stem>.+)_(?P<date>\d{8})$")
_PART_SUFFIX_RE = re.compile(r"^(?P<stem>.+)\.part(?P<part>\d{2,})$")


@dataclass(frozen=True)
class ParsedFilename:
    """Result of `parse_ingest_filename`.

    `entity` is the bare feed-key stem — no '.tsv' extension, no date or
    part marker (e.g. 'on_hand', 'bom_header', 'forecasts'). This parser has
    zero knowledge of which entities are actually supported; callers must
    still check `f"{entity}.tsv"` against DISPATCH — or, for a governed feed
    (``engine.ingest.daily_orchestrator``), translate `entity` (a
    feed_contracts `feed_key`, e.g. 'on-hand') through the active contract's
    `entity_type` first (see that module's docstring — a known kebab-vs-
    snake_case mismatch between the two vocabularies).
    """

    entity: str
    date: str | None
    part: int | None


def parse_ingest_filename(filename: str) -> ParsedFilename:
    """Pure parser: basename -> (entity, date, part). No filesystem access.

    Accepted grammars (basename only, no directory component):
        '<entity>.tsv'                    canonical (e.g. 'items.tsv')
        '<entity>_<AAAAMMJJ>.tsv'         daily drop (e.g. 'on_hand_20260718.tsv')
        '<entity>.partNN.tsv'             part file (e.g. 'forecasts.part01.tsv')
        '<entity>.partNN_<AAAAMMJJ>.tsv'  dated part file

    The date, when present, is returned as its raw 8-digit string (not
    parsed/validated as a real calendar date — same structural-only
    tolerance as `_to_date_str` elsewhere in this file) and is otherwise
    unused: it exists for traceability/grouping, never for dispatch.

    Raises ValueError if `filename` doesn't end in '.tsv', has an empty
    entity segment once date/part markers are stripped, or a malformed
    '.part' marker (no digits).
    """
    if not filename.endswith(".tsv"):
        raise ValueError(f"filename must end with '.tsv': '{filename}'")
    stem = filename[: -len(".tsv")]

    date: str | None = None
    date_match = _DATE_SUFFIX_RE.match(stem)
    if date_match:
        stem = date_match.group("stem")
        date = date_match.group("date")

    part: int | None = None
    part_match = _PART_SUFFIX_RE.match(stem)
    if part_match:
        stem = part_match.group("stem")
        part = int(part_match.group("part"))

    if not stem:
        raise ValueError(f"filename has no entity segment: '{filename}'")

    return ParsedFilename(entity=stem, date=date, part=part)


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
    - No quoting whatsoever (TSV-FILES-SPEC.md §1.1: "aucun guillemet"): a
      literal `"` in a cell (e.g. an inch-mark item description like
      `"U" BOLT 1/4"`) must be preserved verbatim, never treated as a CSV
      quote character. `quoting=csv.QUOTE_NONE` is required here — the
      default QUOTE_MINIMAL silently swallows a leading `"` and everything
      up to the next `"`, corrupting the value.
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
        reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
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
# Part-file grouping (PR-4a)
# ─────────────────────────────────────────────────────────────
def find_sibling_parts(path: Path) -> list[Path]:
    """Return every sibling '.partNN' file for the same (entity, date) as
    `path`, scanned in `path`'s own directory ONLY (never data/processed/
    or data/rejected/ — grouping is scoped to files currently sitting
    together in the inbox), sorted by part number ascending. `path` itself
    is included.

    Raises ValueError if:
      - `path`'s own filename doesn't parse as a part file (part is None);
      - two files in the directory collide on the same part number for
        this (entity, date) group (ambiguous — cannot pick one);
      - `path` itself is not among the files found in its own directory
        (typo, or the file was already archived by a prior run — the
        caller should treat this as a hard error, not silently ingest
        whatever siblings remain).

    Propagates FileNotFoundError if `path.parent` doesn't exist.
    """
    parsed = parse_ingest_filename(path.name)
    if parsed.part is None:
        raise ValueError(f"'{path.name}' is not a '.partNN' file")

    by_part: dict[int, Path] = {}
    for candidate in sorted(path.parent.iterdir()):
        if not candidate.is_file():
            continue
        try:
            c_parsed = parse_ingest_filename(candidate.name)
        except ValueError:
            continue
        if c_parsed.part is None:
            continue
        if c_parsed.entity != parsed.entity or c_parsed.date != parsed.date:
            continue
        if c_parsed.part in by_part:
            raise ValueError(
                f"duplicate part {c_parsed.part:02d} for entity '{parsed.entity}' "
                f"(date={parsed.date}): '{by_part[c_parsed.part].name}' "
                f"and '{candidate.name}'"
            )
        by_part[c_parsed.part] = candidate

    siblings = [by_part[k] for k in sorted(by_part)]
    if not any(p.name == path.name for p in siblings):
        raise ValueError(
            f"'{path.name}' was not found in '{path.parent}' "
            f"(found {len(siblings)} other part(s) for entity '{parsed.entity}'"
            + (f", date {parsed.date}" if parsed.date else "")
            + ") — check the path, or whether this part was already archived"
        )
    return siblings


def parse_tsv_parts(paths: list[Path]) -> tuple[list[str], list[dict[str, str]]]:
    """Parse and concatenate N sibling part files into one logical
    (headers, rows), in the given order (expected: ascending by part
    number, see `find_sibling_parts`).

    Every part is parsed independently via `parse_tsv`; all parts must
    share byte-identical headers (same column names, same order) — a
    mismatch raises ValueError naming the offending file. Each row's
    `__line__` tag is re-prefixed with its source file's name so error
    messages built from it stay traceable across parts, e.g.
    "forecasts.part02.tsv:L14".
    """
    if not paths:
        raise ValueError("no part files to parse")

    headers: list[str] | None = None
    all_rows: list[dict[str, str]] = []
    for p in paths:
        p_headers, p_rows = parse_tsv(p)
        if headers is None:
            headers = p_headers
        elif p_headers != headers:
            raise ValueError(
                f"part file '{p.name}' header {p_headers} does not match "
                f"first part's header {headers}"
            )
        for row in p_rows:
            row = dict(row)
            row["__line__"] = f"{p.name}:L{row.get('__line__', '?')}"
            all_rows.append(row)

    assert headers is not None  # `paths` non-empty => at least one iteration ran
    return headers, all_rows


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
    # alongside and emits N POSTs (one per BOM). Special-cased in
    # scripts/ingest_file.py's main() / handle_bom_bundle — NOT in
    # PAYLOAD_BUILDERS below (no single-payload builder exists for it), and
    # therefore NOT loadable by engine/ingest/daily_orchestrator.py's
    # generic single-payload load loop (a named V1 limitation — see that
    # module's docstring).
    "bom_header.tsv":           {"endpoint": "/v1/ingest/bom",              "body_key": "_bom_bundle"},
}


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


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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
    "suppliers.tsv":             build_suppliers_payload,
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


def archive_group(sources: list[Path], dest_dir: Path, report: dict[str, Any]) -> list[Path]:
    """Archive N sibling files (a part group) together, one timestamped move
    each (same naming as `archive()`), sharing ONE `--report.json` next to
    the FIRST file (caller's ordering — expected ascending part number) and
    a minimal pointer report next to every other file. Mirrors the existing
    bom-bundle archiving pattern (`handle_bom_bundle`, scripts/ingest_file.py)."""
    if not sources:
        raise ValueError("no source files to archive")

    dest_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    primary_name = sources[0].name
    targets: list[Path] = []

    for i, source in enumerate(sources):
        stem = source.stem
        suffix = source.suffix
        target = dest_dir / f"{stem}_{ts}{suffix}"
        shutil.move(str(source), str(target))
        targets.append(target)

        report_path = dest_dir / f"{stem}_{ts}.report.json"
        body = report if i == 0 else {"bundled_with": primary_name, "see_main_report": True}
        report_path.write_text(
            json.dumps(body, indent=2, default=str, ensure_ascii=False),
            encoding="utf-8",
        )

    return targets


# ─────────────────────────────────────────────────────────────
# BOM bundle — payload construction only (the API-calling loop stays a thin
# CLI-shaped wrapper in scripts/ingest_file.py:handle_bom_bundle)
# ─────────────────────────────────────────────────────────────
def build_bom_payloads(
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
