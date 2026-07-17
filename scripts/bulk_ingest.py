"""
bulk_ingest.py — High-performance bulk loader for Ootils canonical V1 TSV.

Bypasses the API layer and uses PostgreSQL COPY + INSERT ... ON CONFLICT DO UPDATE
for raw speed. Suitable for INITIAL pilote loading and bulk refreshes.

For incremental / agent-driven ingestion, use scripts/ingest_file.py (which goes
through the API with DQ, idempotency, batch tracking).

Usage:
    python scripts/bulk_ingest.py data/inbox/items.tsv
    python scripts/bulk_ingest.py data/inbox/items.tsv --dsn postgresql://...

Safety:
- Refuses to write to a DB whose name does not start with 'ootils_' (sanity guard).
- Refuses to touch 'ootils_dev' unless --allow-dev is passed.
- Uses transactions; rollback on any error.

Supported entities (master data first, transactional next):
    items.tsv, locations.tsv, suppliers.tsv, supplier_items.tsv
    item_planning_params.tsv, on_hand.tsv, purchase_orders.tsv,
    customer_orders.tsv, forecasts.tsv, transfers.tsv,
    bom_header.tsv (+ bom_components.tsv bundle)
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("bulk_ingest")


# ─────────────────────────────────────────────────────────────
# Safety guard
# ─────────────────────────────────────────────────────────────
def _guard_db(dsn: str, allow_dev: bool) -> str:
    """Extract DB name from DSN, refuse if unsafe target."""
    # Cheap parse: take last path segment.
    db_name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not db_name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB name '{db_name}' does not start with 'ootils' — safety guard.")
    if db_name == "ootils_dev" and not allow_dev:
        raise SystemExit(
            f"REFUSED: target DB is 'ootils_dev' (semi-prod). Pass --allow-dev to override."
        )
    return db_name


# ─────────────────────────────────────────────────────────────
# Generic COPY loader
# ─────────────────────────────────────────────────────────────
def _copy_tsv_to_temp(
    conn: psycopg.Connection,
    tsv_path: Path,
    temp_table: str,
    columns: list[str],
) -> int:
    """COPY a TSV file into a temp table. Returns row count."""
    cur = conn.cursor()
    cols_decl = ", ".join(f'"{c}" TEXT' for c in columns)
    cur.execute(f"CREATE TEMP TABLE {temp_table} ({cols_decl}) ON COMMIT DROP")

    cols_list = ", ".join(f'"{c}"' for c in columns)
    # QUOTE = byte 0x01 (start-of-heading control char) — virtually impossible
    # to appear in real data. This neutralizes CSV quoting so that double-quote
    # characters in values (e.g. inches '4.5"') are treated as literal text.
    copy_sql = (
        f"COPY {temp_table} ({cols_list}) FROM STDIN "
        f"WITH (FORMAT csv, DELIMITER E'\\t', HEADER true, QUOTE E'\\x01')"
    )
    with tsv_path.open("rb") as f, cur.copy(copy_sql) as copy:
        while data := f.read(1024 * 1024):  # 1 MB chunks
            copy.write(data)

    cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
    return cur.fetchone()[0]


def _read_tsv_header(tsv_path: Path) -> list[str]:
    """Read the header row of a TSV file.

    quoting=csv.QUOTE_NONE — same rationale as scripts/ingest_file.py's
    parse_tsv: TSV-FILES-SPEC.md §1.1 mandates no quoting, and a header
    name containing a literal `"` must not be silently mangled.
    """
    with tsv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        return [h.strip() for h in next(reader)]


# ─────────────────────────────────────────────────────────────
# Per-entity loaders
# ─────────────────────────────────────────────────────────────
def load_items(conn: psycopg.Connection, tsv_path: Path) -> dict[str, Any]:
    """COPY items into temp then UPSERT into items table."""
    expected = ["external_id", "name", "item_type", "uom", "status"]
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_items", header)

    cur = conn.cursor()
    # Apply Pydantic-equivalent defaults for blank cells, drop trailing whitespace.
    cur.execute(
        """
        UPDATE _b_items SET
            external_id = NULLIF(TRIM(external_id), ''),
            name        = NULLIF(TRIM(name), ''),
            item_type   = COALESCE(NULLIF(TRIM(item_type), ''), 'finished_good'),
            uom         = COALESCE(NULLIF(TRIM(uom), ''), 'EA'),
            status      = COALESCE(NULLIF(TRIM(status), ''), 'active')
        """
    )
    # Validate enum values before upsert (fail loud).
    cur.execute(
        "SELECT COUNT(*) FROM _b_items WHERE item_type NOT IN ('finished_good', 'component', 'raw_material', 'semi_finished')"
    )
    bad_type = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM _b_items WHERE status NOT IN ('active', 'obsolete', 'phase_out')")
    bad_status = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM _b_items WHERE external_id IS NULL OR name IS NULL")
    bad_required = cur.fetchone()[0]
    if bad_type or bad_status or bad_required:
        logger.warning(
            "items: %d rows will be skipped (required_missing=%d, bad_item_type=%d, bad_status=%d)",
            bad_type + bad_status + bad_required, bad_required, bad_type, bad_status,
        )

    # Optional valuation columns: standard_cost / cost_currency (back-compatible —
    # absent header => not touched). standard_cost lets the ERP supply item-level
    # cost to close the valuation gap on items without a supplier unit_cost.
    has_cost = "standard_cost" in header
    if has_cost:
        has_ccy = "cost_currency" in header
        cur.execute(
            "UPDATE _b_items SET standard_cost = NULLIF(TRIM(standard_cost), '')"
            + (", cost_currency = COALESCE(NULLIF(TRIM(cost_currency), ''), 'USD')" if has_ccy else "")
        )
        sel_extra = ", NULLIF(standard_cost,'')::numeric" + (", cost_currency" if has_ccy else ", 'USD'")
        ins_cols = ", standard_cost, cost_currency"
        upd_extra = (", standard_cost = COALESCE(EXCLUDED.standard_cost, items.standard_cost)"
                     ", cost_currency = COALESCE(EXCLUDED.cost_currency, items.cost_currency)")
    else:
        sel_extra = ins_cols = upd_extra = ""

    cur.execute(
        f"""
        INSERT INTO items (external_id, name, item_type, uom, status{ins_cols})
        SELECT external_id, name, item_type, uom, status{sel_extra} FROM _b_items
        WHERE external_id IS NOT NULL AND name IS NOT NULL
          AND item_type IN ('finished_good', 'component', 'raw_material', 'semi_finished')
          AND status IN ('active', 'obsolete', 'phase_out')
        ON CONFLICT (external_id) DO UPDATE SET
            name      = EXCLUDED.name,
            item_type = EXCLUDED.item_type,
            uom       = EXCLUDED.uom,
            status    = EXCLUDED.status,
            updated_at = now(){upd_extra}
        """
    )
    affected = cur.rowcount
    return {"entity": "items", "rows_parsed": n, "rows_affected": affected}


def load_locations(conn: psycopg.Connection, tsv_path: Path) -> dict[str, Any]:
    """COPY locations into temp then UPSERT.

    Note: V1.0 schema does NOT store parent_external_id as a FK column on
    locations — the column exists in the TSV contract but is ignored at the
    DB level in this script. (The API ingest_file.py validates parent refs
    but doesn't persist them either, currently.)
    """
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_locations", header)

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE _b_locations SET
            external_id   = NULLIF(TRIM(external_id), ''),
            name          = NULLIF(TRIM(name), ''),
            location_type = COALESCE(NULLIF(TRIM(location_type), ''), 'dc'),
            country       = NULLIF(TRIM(country), ''),
            timezone      = NULLIF(TRIM(timezone), '')
        """
    )
    cur.execute(
        "SELECT COUNT(*) FROM _b_locations WHERE location_type NOT IN ('plant', 'dc', 'warehouse', 'supplier_virtual', 'customer_virtual')"
    )
    bad_type = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM _b_locations WHERE external_id IS NULL OR name IS NULL")
    bad_required = cur.fetchone()[0]
    if bad_type or bad_required:
        logger.warning("locations: %d rows will be skipped (required_missing=%d, bad_location_type=%d)", bad_type + bad_required, bad_required, bad_type)

    cur.execute(
        """
        INSERT INTO locations (external_id, name, location_type, country, timezone)
        SELECT external_id, name, location_type, country, timezone FROM _b_locations
        WHERE external_id IS NOT NULL AND name IS NOT NULL
          AND location_type IN ('plant', 'dc', 'warehouse', 'supplier_virtual', 'customer_virtual')
        ON CONFLICT (external_id) DO UPDATE SET
            name          = EXCLUDED.name,
            location_type = EXCLUDED.location_type,
            country       = EXCLUDED.country,
            timezone      = EXCLUDED.timezone
        """
    )
    return {"entity": "locations", "rows_parsed": n, "rows_affected": cur.rowcount}


def load_suppliers(conn: psycopg.Connection, tsv_path: Path) -> dict[str, Any]:
    """COPY suppliers into temp then UPSERT."""
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_suppliers", header)

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE _b_suppliers SET
            external_id       = NULLIF(TRIM(external_id), ''),
            name              = NULLIF(TRIM(name), ''),
            country           = NULLIF(TRIM(country), ''),
            status            = COALESCE(NULLIF(TRIM(status), ''), 'active'),
            lead_time_days    = NULLIF(TRIM(lead_time_days), ''),
            reliability_score = NULLIF(TRIM(reliability_score), '')
        """
    )
    cur.execute("SELECT COUNT(*) FROM _b_suppliers WHERE status NOT IN ('active', 'inactive', 'blocked')")
    bad_status = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM _b_suppliers WHERE external_id IS NULL OR name IS NULL")
    bad_required = cur.fetchone()[0]
    if bad_status or bad_required:
        logger.warning("suppliers: %d rows will be skipped (required_missing=%d, bad_status=%d)", bad_status + bad_required, bad_required, bad_status)

    cur.execute(
        """
        INSERT INTO suppliers (external_id, name, country, status, lead_time_days, reliability_score)
        SELECT
            external_id,
            name,
            country,
            status,
            CASE WHEN lead_time_days ~ '^[0-9]+$' THEN lead_time_days::INTEGER END,
            CASE WHEN reliability_score ~ '^[0-9.]+$' THEN reliability_score::NUMERIC END
        FROM _b_suppliers
        WHERE external_id IS NOT NULL AND name IS NOT NULL
          AND status IN ('active', 'inactive', 'blocked')
        ON CONFLICT (external_id) DO UPDATE SET
            name              = EXCLUDED.name,
            country           = EXCLUDED.country,
            status            = EXCLUDED.status,
            lead_time_days    = EXCLUDED.lead_time_days,
            reliability_score = EXCLUDED.reliability_score,
            updated_at        = now()
        """
    )
    return {"entity": "suppliers", "rows_parsed": n, "rows_affected": cur.rowcount}


def load_supplier_items(conn: psycopg.Connection, tsv_path: Path, *, strict_fk: bool = False) -> dict[str, Any]:
    """Load supplier_items: requires resolving (supplier_external_id, item_external_id) → IDs.

    Tolerant mode (default): rows with unresolved FKs are skipped and reported.
    """
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_si", header)
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE _b_si SET
            supplier_external_id = NULLIF(TRIM(supplier_external_id), ''),
            item_external_id     = NULLIF(TRIM(item_external_id), ''),
            lead_time_days       = NULLIF(TRIM(lead_time_days), ''),
            currency             = COALESCE(NULLIF(TRIM(currency), ''), 'EUR'),
            moq                  = NULLIF(TRIM(moq), ''),
            unit_cost            = NULLIF(TRIM(unit_cost), ''),
            is_preferred         = LOWER(COALESCE(NULLIF(TRIM(is_preferred), ''), 'false'))
        """
    )

    # Count rows with unresolved FKs (for reporting)
    cur.execute(
        """SELECT COUNT(*) FROM _b_si si
           LEFT JOIN suppliers s ON s.external_id = si.supplier_external_id
           LEFT JOIN items it ON it.external_id = si.item_external_id
           WHERE s.supplier_id IS NULL OR it.item_id IS NULL"""
    )
    skipped = cur.fetchone()[0]
    if strict_fk and skipped > 0:
        raise ValueError(f"supplier_items.tsv: {skipped} rows with unresolved FKs (--strict-fk)")

    # JOIN-based insert naturally filters out unresolved rows
    cur.execute(
        """
        INSERT INTO supplier_items (supplier_id, item_id, lead_time_days, currency, moq, unit_cost, is_preferred)
        SELECT
            s.supplier_id,
            it.item_id,
            si.lead_time_days::INTEGER,
            si.currency,
            CASE WHEN si.moq IS NOT NULL THEN si.moq::NUMERIC END,
            CASE WHEN si.unit_cost IS NOT NULL THEN si.unit_cost::NUMERIC END,
            si.is_preferred IN ('true', '1', 'yes', 'y', 't')
        FROM _b_si si
        JOIN suppliers s ON s.external_id = si.supplier_external_id
        JOIN items it ON it.external_id = si.item_external_id
        ON CONFLICT (supplier_id, item_id) DO UPDATE SET
            lead_time_days = EXCLUDED.lead_time_days,
            currency       = EXCLUDED.currency,
            moq            = EXCLUDED.moq,
            unit_cost      = EXCLUDED.unit_cost,
            is_preferred   = EXCLUDED.is_preferred
        """
    )
    return {"entity": "supplier_items", "rows_parsed": n, "rows_affected": cur.rowcount, "rows_skipped_fk": skipped}


BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


def load_item_planning_params(conn: psycopg.Connection, tsv_path: Path) -> dict[str, Any]:
    """Load item_planning_params. PILOT MODE: latest-wins upsert by (item_id, location_id),
    closing any active row before inserting (preserves SCD2 history at row level).
    """
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_ipp", header)
    cur = conn.cursor()

    # Resolve FKs (items, locations, suppliers for preferred_supplier)
    cur.execute(
        """
        UPDATE _b_ipp SET
            item_external_id = NULLIF(TRIM(item_external_id), ''),
            location_external_id = NULLIF(TRIM(location_external_id), ''),
            preferred_supplier_external_id = NULLIF(TRIM(preferred_supplier_external_id), '')
        """
    )
    cur.execute(
        """SELECT COUNT(*) FROM _b_ipp b
           LEFT JOIN items it ON it.external_id = b.item_external_id
           WHERE it.item_id IS NULL"""
    )
    bad_item = cur.fetchone()[0]
    cur.execute(
        """SELECT COUNT(*) FROM _b_ipp b
           LEFT JOIN locations l ON l.external_id = b.location_external_id
           WHERE l.location_id IS NULL"""
    )
    bad_loc = cur.fetchone()[0]
    cur.execute(
        """SELECT COUNT(*) FROM _b_ipp b
           LEFT JOIN suppliers s ON s.external_id = b.preferred_supplier_external_id
           WHERE b.preferred_supplier_external_id IS NOT NULL AND s.supplier_id IS NULL"""
    )
    bad_sup = cur.fetchone()[0]
    if bad_item or bad_loc or bad_sup:
        logger.warning(
            "item_planning_params: %d rows with unresolved FKs will be skipped (items=%d, locations=%d, suppliers=%d)",
            bad_item + bad_loc + bad_sup, bad_item, bad_loc, bad_sup,
        )

    # Resolve the target (item_id, location_id) pairs once into a temp table so the
    # close/replace below joins on an indexable key instead of a row-constructor IN
    # subquery (faster, and reused by both statements).
    cur.execute(
        """
        CREATE TEMP TABLE _b_ipp_keys ON COMMIT DROP AS
        SELECT DISTINCT it.item_id, l.location_id
        FROM _b_ipp b
        JOIN items it ON it.external_id = b.item_external_id
        JOIN locations l ON l.external_id = b.location_external_id
        """
    )
    # Same-day reload (idempotency): active rows whose effective_from is today were
    # written by an earlier load on the same day and carry no SCD2 history value.
    # They must be DELETED, not closed — closing them to (CURRENT_DATE - 1) would set
    # effective_to < effective_from and violate the ipp_effective_order check.
    cur.execute(
        """
        DELETE FROM item_planning_params ipp
        USING _b_ipp_keys k
        WHERE ipp.effective_to IS NULL
          AND ipp.effective_from >= CURRENT_DATE
          AND ipp.item_id = k.item_id AND ipp.location_id = k.location_id
        """
    )
    # Older active rows: close them at (CURRENT_DATE - 1), preserving SCD2 history.
    # effective_from < CURRENT_DATE guarantees effective_to >= effective_from.
    cur.execute(
        """
        UPDATE item_planning_params ipp
        SET effective_to = CURRENT_DATE - INTERVAL '1 day', updated_at = now()
        FROM _b_ipp_keys k
        WHERE ipp.effective_to IS NULL
          AND ipp.effective_from < CURRENT_DATE
          AND ipp.item_id = k.item_id AND ipp.location_id = k.location_id
        """
    )

    # Insert new active rows. Use ::NUMERIC / ::INTEGER conditional casts on cells.
    cur.execute(
        """
        INSERT INTO item_planning_params (
            item_id, location_id,
            lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days,
            safety_stock_qty, safety_stock_days,
            reorder_point_qty, min_order_qty, max_order_qty, order_multiple,
            lot_size_rule, planning_horizon_days, is_make,
            preferred_supplier_id,
            effective_from, source
        )
        SELECT
            it.item_id, loc.location_id,
            CASE WHEN b.lead_time_sourcing_days ~ '^[0-9]+$' THEN b.lead_time_sourcing_days::INT END,
            CASE WHEN b.lead_time_manufacturing_days ~ '^[0-9]+$' THEN b.lead_time_manufacturing_days::INT END,
            CASE WHEN b.lead_time_transit_days ~ '^[0-9]+$' THEN b.lead_time_transit_days::INT END,
            CASE WHEN b.safety_stock_qty ~ '^[0-9.]+$' THEN b.safety_stock_qty::NUMERIC END,
            CASE WHEN b.safety_stock_days ~ '^[0-9.]+$' THEN b.safety_stock_days::NUMERIC END,
            CASE WHEN b.reorder_point_qty ~ '^[0-9.]+$' THEN b.reorder_point_qty::NUMERIC END,
            CASE WHEN b.min_order_qty ~ '^[0-9.]+$' THEN b.min_order_qty::NUMERIC END,
            CASE WHEN b.max_order_qty ~ '^[0-9.]+$' THEN b.max_order_qty::NUMERIC END,
            CASE WHEN b.order_multiple ~ '^[0-9.]+$' THEN b.order_multiple::NUMERIC END,
            COALESCE(NULLIF(TRIM(b.lot_size_rule), ''), 'LOTFORLOT')::lot_size_rule_type,
            CASE WHEN b.planning_horizon_days ~ '^[0-9]+$' THEN b.planning_horizon_days::INT ELSE 90 END,
            LOWER(COALESCE(NULLIF(TRIM(b.is_make), ''), 'false')) IN ('true', '1', 'yes', 'y', 't'),
            sup.supplier_id,
            CURRENT_DATE,
            'manual'::planning_source_type
        FROM _b_ipp b
        JOIN items it ON it.external_id = b.item_external_id
        JOIN locations loc ON loc.external_id = b.location_external_id
        LEFT JOIN suppliers sup ON sup.external_id = b.preferred_supplier_external_id
        """
    )
    return {"entity": "item_planning_params", "rows_parsed": n, "rows_affected": cur.rowcount}


# ─────────────────────────────────────────────────────────────
# Transactional loaders — PILOT MODE
#   Full reload of `nodes` rows of each type; no events emission, no
#   PI wire-up. After bulk load, run a propagation reset to rebuild
#   edges and PI projections.
# ─────────────────────────────────────────────────────────────
def _bulk_nodes_with_external_ref(
    conn: psycopg.Connection,
    tsv_path: Path,
    node_type: str,
    entity_ref_type: str,  # 'purchase_order' | 'customer_order' | 'transfer'
    column_map: dict[str, str],  # logical name -> tsv column
) -> dict[str, Any]:
    """Generic bulk loader for transactional nodes with external_references.

    Currently used for PO / CO / Transfer.
    """
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_nodes", header)
    cur = conn.cursor()

    # FK resolution will happen inline in the INSERT JOIN.
    # We DELETE pre-existing nodes of this type via external_references mapping for the EIDs in the file,
    # then re-insert.
    cur.execute(
        """
        DELETE FROM nodes
        WHERE node_id IN (
            SELECT er.internal_id FROM external_references er
            JOIN _b_nodes b ON b.external_id = er.external_id
            WHERE er.entity_type = %s AND er.source_system = 'bulk_ingest'
        )
        """,
        (entity_ref_type,),
    )
    cur.execute(
        """
        DELETE FROM external_references
        WHERE entity_type = %s AND source_system = 'bulk_ingest'
          AND external_id IN (SELECT external_id FROM _b_nodes)
        """,
        (entity_ref_type,),
    )

    return {"entity": node_type, "rows_parsed": n, "rows_affected": 0, "_inserted_nodes": True, "_column_map": column_map}


def load_purchase_orders(conn: psycopg.Connection, tsv_path: Path) -> dict[str, Any]:
    """PILOT MODE: full reload PurchaseOrderSupply nodes. external_references skipped
    (re-run script for refresh; for incremental updates, use API ingest_file.py)."""
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_po", header)
    cur = conn.cursor()
    cur.execute("""SELECT COUNT(*) FROM _b_po b LEFT JOIN items it ON it.external_id = TRIM(b.item_external_id) WHERE it.item_id IS NULL""")
    bad_item = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM _b_po b LEFT JOIN locations l ON l.external_id = TRIM(b.location_external_id) WHERE l.location_id IS NULL""")
    bad_loc = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM _b_po b LEFT JOIN suppliers s ON s.external_id = TRIM(b.supplier_external_id) WHERE s.supplier_id IS NULL""")
    bad_sup = cur.fetchone()[0]
    if bad_item or bad_loc or bad_sup:
        logger.warning("purchase_orders: rows with unresolved FKs will be skipped (items=%d, locations=%d, suppliers=%d)", bad_item, bad_loc, bad_sup)
    # Full reload: drop all PurchaseOrderSupply nodes in baseline, then insert
    cur.execute("DELETE FROM nodes WHERE node_type='PurchaseOrderSupply' AND scenario_id=%s::uuid", (BASELINE_SCENARIO_ID,))
    cur.execute(
        """
        INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, qty_uom, time_grain, time_ref, active)
        SELECT 'PurchaseOrderSupply', %s::uuid, it.item_id, loc.location_id,
            CASE WHEN b.quantity ~ '^[0-9.]+$' THEN b.quantity::NUMERIC END,
            COALESCE(NULLIF(TRIM(b.uom), ''), 'EA'),
            'exact_date',
            b.expected_delivery_date::DATE,
            COALESCE(NULLIF(TRIM(b.status), ''), 'confirmed') NOT IN ('cancelled', 'draft')
        FROM _b_po b
        JOIN items it ON it.external_id = TRIM(b.item_external_id)
        JOIN locations loc ON loc.external_id = TRIM(b.location_external_id)
        WHERE b.quantity ~ '^[0-9.]+$' AND b.expected_delivery_date ~ '^[0-9-]+$'
        """,
        (BASELINE_SCENARIO_ID,),
    )
    return {"entity": "purchase_orders", "rows_parsed": n, "rows_affected": cur.rowcount}


def load_customer_orders(conn: psycopg.Connection, tsv_path: Path) -> dict[str, Any]:
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_co", header)
    cur = conn.cursor()
    cur.execute("""SELECT COUNT(*) FROM _b_co b LEFT JOIN items it ON it.external_id = TRIM(b.item_external_id) WHERE it.item_id IS NULL""")
    bad_item = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM _b_co b LEFT JOIN locations l ON l.external_id = TRIM(b.location_external_id) WHERE l.location_id IS NULL""")
    bad_loc = cur.fetchone()[0]
    if bad_item or bad_loc:
        logger.warning("customer_orders: rows with unresolved FKs will be skipped (items=%d, locations=%d)", bad_item, bad_loc)
    # Full reload
    cur.execute("DELETE FROM nodes WHERE node_type='CustomerOrderDemand' AND scenario_id=%s::uuid", (BASELINE_SCENARIO_ID,))
    cur.execute(
        """
        INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, time_grain, time_ref, active)
        SELECT 'CustomerOrderDemand', %s::uuid, it.item_id, loc.location_id,
            CASE WHEN b.quantity ~ '^[0-9.]+$' THEN b.quantity::NUMERIC END,
            'exact_date', b.requested_delivery_date::DATE,
            COALESCE(NULLIF(TRIM(b.status), ''), 'open') NOT IN ('cancelled')
        FROM _b_co b
        JOIN items it ON it.external_id = TRIM(b.item_external_id)
        JOIN locations loc ON loc.external_id = TRIM(b.location_external_id)
        WHERE b.quantity ~ '^[0-9.]+$' AND b.requested_delivery_date ~ '^[0-9-]+$'
        """,
        (BASELINE_SCENARIO_ID,),
    )
    return {"entity": "customer_orders", "rows_parsed": n, "rows_affected": cur.rowcount}


def load_transfers(conn: psycopg.Connection, tsv_path: Path) -> dict[str, Any]:
    """Transfers create a TransferSupply node at the destination + a TransferDemand at the origin."""
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_tr", header)
    cur = conn.cursor()
    cur.execute("""SELECT COUNT(*) FROM _b_tr b LEFT JOIN items it ON it.external_id = TRIM(b.item_external_id) WHERE it.item_id IS NULL""")
    bad_item = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM _b_tr b LEFT JOIN locations l1 ON l1.external_id = TRIM(b.from_location_external_id) LEFT JOIN locations l2 ON l2.external_id = TRIM(b.to_location_external_id) WHERE l1.location_id IS NULL OR l2.location_id IS NULL""")
    bad_loc = cur.fetchone()[0]
    if bad_item or bad_loc:
        logger.warning("transfers: rows with unresolved FKs will be skipped (items=%d, locations=%d)", bad_item, bad_loc)
    # Full reload
    cur.execute("DELETE FROM nodes WHERE node_type='TransferSupply' AND scenario_id=%s::uuid", (BASELINE_SCENARIO_ID,))
    cur.execute(
        """
        INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, time_grain, time_ref, active)
        SELECT 'TransferSupply', %s::uuid, it.item_id, l_to.location_id,
            CASE WHEN b.quantity ~ '^[0-9.]+$' THEN b.quantity::NUMERIC END,
            'exact_date', b.expected_delivery_date::DATE,
            COALESCE(NULLIF(TRIM(b.status), ''), 'planned') NOT IN ('cancelled')
        FROM _b_tr b
        JOIN items it ON it.external_id = TRIM(b.item_external_id)
        JOIN locations l_to ON l_to.external_id = TRIM(b.to_location_external_id)
        WHERE b.quantity ~ '^[0-9.]+$' AND b.expected_delivery_date ~ '^[0-9-]+$'
        """,
        (BASELINE_SCENARIO_ID,),
    )
    return {"entity": "transfers", "rows_parsed": n, "rows_affected": cur.rowcount}


def load_on_hand(conn: psycopg.Connection, tsv_path: Path) -> dict[str, Any]:
    """On-hand: full-reload OnHandSupply nodes for (item, location) pairs in the file."""
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_oh", header)
    cur = conn.cursor()
    cur.execute("""SELECT COUNT(*) FROM _b_oh b LEFT JOIN items it ON it.external_id = TRIM(b.item_external_id) WHERE it.item_id IS NULL""")
    bad_item = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM _b_oh b LEFT JOIN locations l ON l.external_id = TRIM(b.location_external_id) WHERE l.location_id IS NULL""")
    bad_loc = cur.fetchone()[0]
    if bad_item or bad_loc:
        logger.warning("on_hand: rows with unresolved FKs will be skipped (items=%d, locations=%d)", bad_item, bad_loc)

    # Delete existing OnHandSupply nodes for the affected (item, location) pairs.
    # DELETE ... USING a join (not a row-constructor IN subquery, which Postgres
    # plans as a pathological nested loop — observed 17 min on 15k pairs) so the
    # planner uses idx_nodes_item_location_scenario. Seconds instead of minutes.
    cur.execute(
        """
        DELETE FROM nodes n
        USING _b_oh b
        JOIN items it      ON it.external_id  = TRIM(b.item_external_id)
        JOIN locations loc ON loc.external_id = TRIM(b.location_external_id)
        WHERE n.node_type = 'OnHandSupply' AND n.scenario_id = %s::uuid
          AND n.item_id = it.item_id AND n.location_id = loc.location_id
        """,
        (BASELINE_SCENARIO_ID,),
    )
    cur.execute(
        """
        INSERT INTO nodes
            (node_type, scenario_id, item_id, location_id, quantity, qty_uom, time_grain, time_ref, active)
        SELECT 'OnHandSupply', %s::uuid, it.item_id, loc.location_id,
            CASE WHEN b.quantity ~ '^[0-9.]+$' THEN b.quantity::NUMERIC END,
            COALESCE(NULLIF(TRIM(b.uom), ''), 'EA'),
            'exact_date', b.as_of_date::DATE, TRUE
        FROM _b_oh b
        JOIN items it ON it.external_id = TRIM(b.item_external_id)
        JOIN locations loc ON loc.external_id = TRIM(b.location_external_id)
        """,
        (BASELINE_SCENARIO_ID,),
    )
    return {"entity": "on_hand", "rows_parsed": n, "rows_affected": cur.rowcount}


def load_forecasts(conn: psycopg.Connection, tsv_path: Path) -> dict[str, Any]:
    """Forecasts: full-reload ForecastDemand nodes for (item, location, bucket_date, time_grain, source)."""
    header = _read_tsv_header(tsv_path)
    n = _copy_tsv_to_temp(conn, tsv_path, "_b_fc", header)
    cur = conn.cursor()
    cur.execute("""SELECT COUNT(*) FROM _b_fc b LEFT JOIN items it ON it.external_id = TRIM(b.item_external_id) WHERE it.item_id IS NULL""")
    bad_item = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM _b_fc b LEFT JOIN locations l ON l.external_id = TRIM(b.location_external_id) WHERE l.location_id IS NULL""")
    bad_loc = cur.fetchone()[0]
    if bad_item or bad_loc:
        logger.warning("forecasts: rows with unresolved FKs will be skipped (items=%d, locations=%d)", bad_item, bad_loc)

    # Wipe ForecastDemand nodes for affected (item, location) pairs (full reload).
    # DELETE ... USING a join instead of a row-constructor IN subquery, so the
    # planner uses idx_nodes_item_location_scenario (the IN form was a 17-min
    # nested loop on the pilote).
    cur.execute(
        """
        DELETE FROM nodes n
        USING _b_fc b
        JOIN items it      ON it.external_id  = TRIM(b.item_external_id)
        JOIN locations loc ON loc.external_id = TRIM(b.location_external_id)
        WHERE n.node_type = 'ForecastDemand' AND n.scenario_id = %s::uuid
          AND n.item_id = it.item_id AND n.location_id = loc.location_id
        """,
        (BASELINE_SCENARIO_ID,),
    )
    cur.execute(
        """
        INSERT INTO nodes
            (node_type, scenario_id, item_id, location_id, quantity, time_grain, time_ref, time_span_start, time_span_end, active)
        SELECT 'ForecastDemand', %s::uuid, it.item_id, loc.location_id,
            CASE WHEN b.quantity ~ '^-?[0-9.]+$' THEN b.quantity::NUMERIC END,
            COALESCE(NULLIF(TRIM(b.time_grain), ''), 'week'),
            b.bucket_date::DATE,
            b.bucket_date::DATE,
            CASE
                WHEN COALESCE(NULLIF(TRIM(b.time_grain), ''), 'week') = 'day'   THEN b.bucket_date::DATE + INTERVAL '1 day'
                WHEN COALESCE(NULLIF(TRIM(b.time_grain), ''), 'week') = 'week'  THEN b.bucket_date::DATE + INTERVAL '7 days'
                WHEN COALESCE(NULLIF(TRIM(b.time_grain), ''), 'week') = 'month' THEN (b.bucket_date::DATE + INTERVAL '1 month')::DATE
                ELSE b.bucket_date::DATE + INTERVAL '1 day'
            END,
            TRUE
        FROM _b_fc b
        JOIN items it ON it.external_id = TRIM(b.item_external_id)
        JOIN locations loc ON loc.external_id = TRIM(b.location_external_id)
        """,
        (BASELINE_SCENARIO_ID,),
    )
    return {"entity": "forecasts", "rows_parsed": n, "rows_affected": cur.rowcount}


def load_bom_bundle(conn: psycopg.Connection, tsv_path: Path) -> dict[str, Any]:
    """BOM bundle: reads bom_components.tsv from same directory, full-reload bom_headers + bom_lines."""
    components_path = tsv_path.parent / "bom_components.tsv"
    if not components_path.exists():
        raise FileNotFoundError(f"bom_components.tsv not found next to bom_header.tsv at {tsv_path.parent}")

    hdr = _read_tsv_header(tsv_path)
    cmp = _read_tsv_header(components_path)
    n_h = _copy_tsv_to_temp(conn, tsv_path, "_b_bom_h", hdr)
    n_c = _copy_tsv_to_temp(conn, components_path, "_b_bom_c", cmp)

    cur = conn.cursor()
    # Resolve FKs
    cur.execute("""SELECT COUNT(*) FROM _b_bom_h b LEFT JOIN items it ON it.external_id = TRIM(b.parent_external_id) WHERE it.item_id IS NULL""")
    bad_parent = cur.fetchone()[0]
    cur.execute("""SELECT COUNT(*) FROM _b_bom_c b LEFT JOIN items it ON it.external_id = TRIM(b.component_external_id) WHERE it.item_id IS NULL""")
    bad_comp = cur.fetchone()[0]
    if bad_parent or bad_comp:
        logger.warning("BOM: rows with unresolved FKs will be skipped (parents=%d, components=%d)", bad_parent, bad_comp)

    # Delete bom_lines first, then bom_headers for affected parents
    cur.execute(
        """
        DELETE FROM bom_lines WHERE bom_id IN (
            SELECT bh.bom_id FROM bom_headers bh
            JOIN items it ON it.item_id = bh.parent_item_id
            WHERE it.external_id IN (SELECT TRIM(parent_external_id) FROM _b_bom_h)
        )
        """
    )
    cur.execute(
        """
        DELETE FROM bom_headers WHERE parent_item_id IN (
            SELECT it.item_id FROM items it
            WHERE it.external_id IN (SELECT TRIM(parent_external_id) FROM _b_bom_h)
        )
        """
    )

    # Insert bom_headers
    cur.execute(
        """
        INSERT INTO bom_headers (parent_item_id, bom_version, effective_from)
        SELECT it.item_id,
               COALESCE(NULLIF(TRIM(b.bom_version), ''), '1.0'),
               CASE WHEN b.effective_from ~ '^[0-9-]+$' THEN b.effective_from::DATE ELSE CURRENT_DATE END
        FROM _b_bom_h b
        JOIN items it ON it.external_id = TRIM(b.parent_external_id)
        """
    )

    # Insert bom_lines (look up bom_header_id via parent_item_id + version)
    cur.execute(
        """
        INSERT INTO bom_lines (bom_id, component_item_id, quantity_per, uom, scrap_factor)
        SELECT bh.bom_id, comp.item_id,
               c.quantity_per::NUMERIC,
               COALESCE(NULLIF(TRIM(c.uom), ''), 'EA'),
               COALESCE(NULLIF(TRIM(c.scrap_factor), '')::NUMERIC, 0.0)
        FROM _b_bom_c c
        JOIN items parent ON parent.external_id = TRIM(c.parent_external_id)
        JOIN items comp   ON comp.external_id = TRIM(c.component_external_id)
        JOIN bom_headers bh ON bh.parent_item_id = parent.item_id
                           AND bh.bom_version = COALESCE(NULLIF(TRIM(c.bom_version), ''), '1.0')
        """
    )
    return {
        "entity": "bom_bundle",
        "rows_parsed": n_h + n_c,
        "rows_affected": cur.rowcount,
        "headers": n_h,
        "components": n_c,
    }


DISPATCH: dict[str, Any] = {
    "items.tsv":                load_items,
    "locations.tsv":            load_locations,
    "suppliers.tsv":            load_suppliers,
    "supplier_items.tsv":       load_supplier_items,
    "item_planning_params.tsv": load_item_planning_params,
    "on_hand.tsv":              load_on_hand,
    "purchase_orders.tsv":      load_purchase_orders,
    "customer_orders.tsv":      load_customer_orders,
    "forecasts.tsv":            load_forecasts,
    "transfers.tsv":            load_transfers,
    "bom_header.tsv":           load_bom_bundle,
}


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
# Canonical load order — respects FK dependencies
ORDERED_FILES = [
    "items.tsv",
    "locations.tsv",
    "suppliers.tsv",
    "supplier_items.tsv",
    "item_planning_params.tsv",
    "on_hand.tsv",
    "purchase_orders.tsv",
    "customer_orders.tsv",
    "transfers.tsv",
    "forecasts.tsv",
    "bom_header.tsv",
]


def _run_one(dsn: str, src: Path) -> tuple[int, dict]:
    """Run a single file. Returns (exit_code, result_dict)."""
    filename = src.name
    if filename not in DISPATCH:
        return 3, {"error": f"unsupported filename: {filename}"}
    loader = DISPATCH[filename]
    t0 = time.perf_counter()
    try:
        with psycopg.connect(dsn) as conn:
            result = loader(conn, src)
            conn.commit()
        elapsed = time.perf_counter() - t0
        result["elapsed_s"] = round(elapsed, 2)
        result["rate_rows_per_s"] = int(result["rows_parsed"] / elapsed) if elapsed > 0 else 0
        return 0, result
    except (ValueError, psycopg.Error, FileNotFoundError) as e:
        elapsed = time.perf_counter() - t0
        return 1, {"entity": filename, "error": str(e), "elapsed_s": round(elapsed, 2)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bulk loader for Ootils canonical TSV files.")
    parser.add_argument(
        "path",
        help="path to TSV file, OR path to a directory (loads all known files in canonical order)",
    )
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument(
        "--allow-dev",
        action="store_true",
        help="allow writing to ootils_dev (off by default for safety)",
    )
    args = parser.parse_args(argv)

    if not args.dsn:
        logger.error("DATABASE_URL not set and --dsn not provided")
        return 2

    src = Path(args.path).resolve()
    db_name = _guard_db(args.dsn, args.allow_dev)

    if src.is_dir():
        # Batch mode: run all known files in canonical order
        logger.info("Batch bulk ingest from %s → DB=%s", src, db_name)
        results = []
        for filename in ORDERED_FILES:
            file_path = src / filename
            if not file_path.exists():
                logger.info("  skip %s (not present)", filename)
                continue
            logger.info("→ %s ...", filename)
            code, result = _run_one(args.dsn, file_path)
            results.append(result)
            if code == 0:
                logger.info(
                    "  OK %s: parsed=%d affected=%d skipped_fk=%s in %.2fs (%d rows/s)",
                    result["entity"], result["rows_parsed"], result.get("rows_affected", "?"),
                    result.get("rows_skipped_fk", "n/a"),
                    result["elapsed_s"], result["rate_rows_per_s"],
                )
            else:
                logger.error("  FAILED %s: %s", filename, result.get("error", "?"))
        total = sum(r.get("elapsed_s", 0) for r in results)
        ok = sum(1 for r in results if "error" not in r)
        total_rows = sum(r.get("rows_parsed", 0) for r in results)
        logger.info(
            "BATCH DONE: %d/%d files OK, %d rows in %.2fs total",
            ok, len(results), total_rows, total,
        )
        return 0 if ok == len(results) else 1

    # Single-file mode
    logger.info("Bulk ingest: %s → DB=%s", src.name, db_name)
    code, result = _run_one(args.dsn, src)
    if code == 0:
        logger.info(
            "OK %s: parsed=%d affected=%d in %.2fs (%d rows/s)",
            result["entity"], result["rows_parsed"], result.get("rows_affected", "?"),
            result["elapsed_s"], result["rate_rows_per_s"],
        )
    else:
        logger.error("FAILED: %s", result.get("error", "?"))
    return code


if __name__ == "__main__":
    sys.exit(main())
