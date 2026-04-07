#!/usr/bin/env python3
"""
seed_demo_data.py — Seed realistic demo data for Ootils V1 live testing.

Creates a realistic supply chain scenario with shortages that the M7 agent
can detect, explain, and simulate fixes for.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@localhost:5432/ootils_dev python scripts/seed_demo_data.py

Scenario:
    - 2 items: PUMP-01 (critical pump), VALVE-02 (control valve)
    - 2 locations: DC-ATL (Atlanta DC), DC-LAX (LA DC)
    - Planning horizon: today + 90 days, daily buckets
    - Shortages:
        * PUMP-01 @ DC-ATL: PO delayed 8 days → shortage week 3
        * VALVE-02 @ DC-LAX: demand spike > on-hand → shortage week 2
    - Events pre-loaded to trigger recalculation

Idempotency:
    All IDs are derived via uuid5 from a fixed namespace + a stable seed name.
    Running this script multiple times will produce the same UUIDs, so
    ON CONFLICT DO NOTHING suppresses duplicates safely.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid5

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://ootils:ootils@localhost:5432/ootils_dev"
)

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"
TODAY = date.today()

# Fixed namespace for all deterministic UUIDs in this seed script.
# Never change this value — it anchors idempotency across runs.
_SEED_NS = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # DNS namespace (standard)


def _uid(name: str) -> str:
    """Return a deterministic UUID string derived from a stable seed name."""
    return str(uuid5(_SEED_NS, f"ootils-seed-v1:{name}"))


def seed(conn):
    print("🌱 Seeding Ootils demo data...")

    # ------------------------------------------------------------------
    # 1. Items  (deterministic IDs)
    # ------------------------------------------------------------------
    pump_id = _uid("item:PUMP-01")
    valve_id = _uid("item:VALVE-02")

    conn.execute("""
        INSERT INTO items (item_id, name, item_type, uom, status, external_id)
        VALUES (%s, 'PUMP-01 Industrial Pump', 'finished_good', 'EA', 'active', 'PUMP-01'),
               (%s, 'VALVE-02 Control Valve', 'component', 'EA', 'active', 'VALVE-02')
        ON CONFLICT (item_id) DO UPDATE
            SET external_id = EXCLUDED.external_id
    """, (pump_id, valve_id))
    print(f"  ✓ Items: PUMP-01 ({pump_id[:8]}...), VALVE-02 ({valve_id[:8]}...)")

    # ------------------------------------------------------------------
    # 2. Locations  (deterministic IDs)
    # ------------------------------------------------------------------
    atl_id = _uid("location:DC-ATL")
    lax_id = _uid("location:DC-LAX")

    conn.execute("""
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, 'DC-ATL Atlanta Distribution Center', 'dc', 'US', 'DC-ATL'),
               (%s, 'DC-LAX Los Angeles Distribution Center', 'dc', 'US', 'DC-LAX')
        ON CONFLICT (location_id) DO UPDATE
            SET external_id = EXCLUDED.external_id
    """, (atl_id, lax_id))
    print(f"  ✓ Locations: DC-ATL ({atl_id[:8]}...), DC-LAX ({lax_id[:8]}...)")

    # ------------------------------------------------------------------
    # 3. Projection series  (deterministic IDs)
    # ------------------------------------------------------------------
    series_pump_atl = _uid("series:PUMP-01:DC-ATL")
    series_valve_lax = _uid("series:VALVE-02:DC-LAX")
    horizon_end = TODAY + timedelta(days=90)

    conn.execute("""
        INSERT INTO projection_series (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
        VALUES (%s, %s, %s, %s, %s, %s),
               (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        series_pump_atl, pump_id, atl_id, BASELINE_SCENARIO_ID, TODAY, horizon_end,
        series_valve_lax, valve_id, lax_id, BASELINE_SCENARIO_ID, TODAY, horizon_end,
    ))
    print(f"  ✓ Projection series created")

    # ------------------------------------------------------------------
    # 4. PI nodes — 90 daily buckets for each series
    # ------------------------------------------------------------------
    print(f"  → Creating PI nodes (90 days × 2 series)...")
    _seed_pi_nodes_pump_atl(conn, pump_id, atl_id, series_pump_atl)
    _seed_pi_nodes_valve_lax(conn, valve_id, lax_id, series_valve_lax)

    # ------------------------------------------------------------------
    # 5. Supply nodes (POs with deliberate delays)
    # ------------------------------------------------------------------
    _seed_supply_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump_atl, series_valve_lax)

    # ------------------------------------------------------------------
    # 6. Demand nodes
    # ------------------------------------------------------------------
    _seed_demand_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump_atl, series_valve_lax)

    print("  ✓ Supply and demand nodes created")

    # ------------------------------------------------------------------
    # 7. Seed events to trigger recalculation
    # ------------------------------------------------------------------
    event_id = _uid("event:ingestion_complete:seed-v1")
    conn.execute("""
        INSERT INTO events (event_id, event_type, scenario_id, source)
        VALUES (%s, 'ingestion_complete', %s, 'test')
        ON CONFLICT DO NOTHING
    """, (event_id, BASELINE_SCENARIO_ID))
    print("  ✓ Trigger event inserted")

    conn.commit()
    print("\n✅ Seed complete.")
    print(f"   → PUMP-01 @ DC-ATL: PO delayed → shortage expected ~day 18")
    print(f"   → VALVE-02 @ DC-LAX: demand spike → shortage expected ~day 10")
    print(f"\n   API: http://localhost:8000/v1/issues?severity=all")
    print(f"   Docs: http://localhost:8000/docs")


def _seed_pi_nodes_pump_atl(conn, item_id, location_id, series_id):
    """
    PUMP-01 @ DC-ATL scenario:
    - Opening stock: 50 units
    - Daily demand: 3 units
    - PO arriving on day 25 (delayed from day 17) — creates shortage days 17-24
    """
    opening = Decimal("50")
    daily_demand = Decimal("3")
    po_arrival_day = 25  # delayed (original was day 17)
    po_qty = Decimal("200")

    running = opening
    nodes = []
    for i in range(90):
        d = TODAY + timedelta(days=i)
        inflows = po_qty if i == po_arrival_day else Decimal("0")
        outflows = daily_demand
        closing = running + inflows - outflows
        has_shortage = closing < Decimal("0")
        shortage_qty = abs(closing) if has_shortage else Decimal("0")

        nodes.append((
            _uid(f"pi:PUMP-01:DC-ATL:day:{i}"),
            "ProjectedInventory", BASELINE_SCENARIO_ID, item_id, location_id,
            "day", d, d, d + timedelta(days=1),
            True,  # is_dirty — will be recomputed when engine runs
            True,
            str(series_id), i,
            running, inflows, outflows, closing,
            has_shortage, shortage_qty,
        ))
        running = closing

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                time_grain, time_ref, time_span_start, time_span_end,
                is_dirty, active, projection_series_id, bucket_sequence,
                opening_stock, inflows, outflows, closing_stock,
                has_shortage, shortage_qty
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, nodes)


def _seed_pi_nodes_valve_lax(conn, item_id, location_id, series_id):
    """
    VALVE-02 @ DC-LAX scenario:
    - Opening stock: 30 units
    - Base daily demand: 2 units
    - Demand spike on days 8-12: 10 units/day → shortage
    - No PO scheduled
    """
    opening = Decimal("30")
    base_demand = Decimal("2")
    spike_demand = Decimal("10")
    spike_days = range(8, 13)

    running = opening
    nodes = []
    for i in range(90):
        d = TODAY + timedelta(days=i)
        outflows = spike_demand if i in spike_days else base_demand
        inflows = Decimal("0")
        closing = running + inflows - outflows
        has_shortage = closing < Decimal("0")
        shortage_qty = abs(closing) if has_shortage else Decimal("0")

        nodes.append((
            _uid(f"pi:VALVE-02:DC-LAX:day:{i}"),
            "ProjectedInventory", BASELINE_SCENARIO_ID, item_id, location_id,
            "day", d, d, d + timedelta(days=1),
            True, True, str(series_id), i,
            running, inflows, outflows, closing,
            has_shortage, shortage_qty,
        ))
        running = closing

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                time_grain, time_ref, time_span_start, time_span_end,
                is_dirty, active, projection_series_id, bucket_sequence,
                opening_stock, inflows, outflows, closing_stock,
                has_shortage, shortage_qty
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, nodes)


def _seed_supply_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve):
    """Create PO supply nodes."""
    po_pump_id = _uid("node:PO:PUMP-01:DC-ATL:day25")
    po_arrival = TODAY + timedelta(days=25)  # delayed

    conn.execute("""
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_ref, quantity, qty_uom, active
        ) VALUES (%s, 'PurchaseOrderSupply', %s, %s, %s, 'exact_date', %s, 200, 'EA', TRUE)
        ON CONFLICT DO NOTHING
    """, (po_pump_id, BASELINE_SCENARIO_ID, pump_id, atl_id, po_arrival))

    # Wire PO → first shortage PI node via replenishes edge
    pi_row = conn.execute("""
        SELECT node_id FROM nodes
        WHERE projection_series_id = %s
          AND bucket_sequence = 25
          AND active = TRUE
        LIMIT 1
    """, (series_pump,)).fetchone()

    if pi_row:
        edge_id = _uid("edge:replenishes:PO-PUMP-01:day25")
        conn.execute("""
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, priority, active)
            VALUES (%s, 'replenishes', %s, %s, %s, 0, TRUE)
            ON CONFLICT DO NOTHING
        """, (edge_id, po_pump_id, pi_row["node_id"], BASELINE_SCENARIO_ID))


def _seed_demand_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve):
    """Create customer order demand nodes and wire them to PI nodes."""
    # Customer order for PUMP-01: 50 units due day 15
    co_pump_id = _uid("node:CO:PUMP-01:DC-ATL:day15")
    co_due = TODAY + timedelta(days=15)

    conn.execute("""
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_ref, quantity, qty_uom, active
        ) VALUES (%s, 'CustomerOrderDemand', %s, %s, %s, 'exact_date', %s, 50, 'EA', TRUE)
        ON CONFLICT DO NOTHING
    """, (co_pump_id, BASELINE_SCENARIO_ID, pump_id, atl_id, co_due))

    # Wire demand → PI node at day 15
    pi_row = conn.execute("""
        SELECT node_id FROM nodes
        WHERE projection_series_id = %s AND bucket_sequence = 15 AND active = TRUE
        LIMIT 1
    """, (series_pump,)).fetchone()

    if pi_row:
        edge_id = _uid("edge:consumes:CO-PUMP-01:day15")
        conn.execute("""
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, priority, active)
            VALUES (%s, 'consumes', %s, %s, %s, 1, TRUE)
            ON CONFLICT DO NOTHING
        """, (edge_id, co_pump_id, pi_row["node_id"], BASELINE_SCENARIO_ID))

    # Seed shortages table directly for demo
    # (in production, the propagation engine would generate these)
    _seed_shortages(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve)


def _seed_shortages(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve):
    """Seed shortage records directly for demo (bypasses engine for initial seed)."""
    # Create a placeholder calc_run for the seed
    calc_run_id = _uid("calc_run:seed-v1")
    conn.execute("""
        INSERT INTO calc_runs (calc_run_id, scenario_id, status, is_full_recompute)
        VALUES (%s, %s, 'completed', TRUE)
        ON CONFLICT DO NOTHING
    """, (calc_run_id, BASELINE_SCENARIO_ID))

    # PUMP-01 shortages: days 17-24 (before PO arrives on day 25)
    for day in range(17, 25):
        d = TODAY + timedelta(days=day)
        pi_row = conn.execute("""
            SELECT node_id FROM nodes
            WHERE projection_series_id = %s AND bucket_sequence = %s AND active = TRUE
            LIMIT 1
        """, (series_pump, day)).fetchone()
        if not pi_row:
            continue
        days_at_shortage = 1
        shortage_qty = Decimal("3") * (day - 16)  # cumulative
        severity = shortage_qty * days_at_shortage

        shortage_id = _uid(f"shortage:PUMP-01:DC-ATL:day:{day}")
        conn.execute("""
            INSERT INTO shortages (
                shortage_id, scenario_id, pi_node_id, item_id, location_id,
                shortage_date, shortage_qty, severity_score, calc_run_id, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active')
            ON CONFLICT DO NOTHING
        """, (shortage_id, BASELINE_SCENARIO_ID, pi_row["node_id"],
              pump_id, atl_id, d, shortage_qty, severity, calc_run_id))

    # VALVE-02 shortage: day 10 onwards
    for day in range(10, 15):
        d = TODAY + timedelta(days=day)
        pi_row = conn.execute("""
            SELECT node_id FROM nodes
            WHERE projection_series_id = %s AND bucket_sequence = %s AND active = TRUE
            LIMIT 1
        """, (series_valve, day)).fetchone()
        if not pi_row:
            continue
        shortage_qty = Decimal("8") * (day - 9)
        severity = shortage_qty * 1

        shortage_id = _uid(f"shortage:VALVE-02:DC-LAX:day:{day}")
        conn.execute("""
            INSERT INTO shortages (
                shortage_id, scenario_id, pi_node_id, item_id, location_id,
                shortage_date, shortage_qty, severity_score, calc_run_id, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active')
            ON CONFLICT DO NOTHING
        """, (shortage_id, BASELINE_SCENARIO_ID, pi_row["node_id"],
              valve_id, lax_id, d, shortage_qty, severity, calc_run_id))


# ===========================================================================
# ENRICHMENT — MidCo Electronics full causal graph
# ===========================================================================
# Items mapping (consistent with existing seed):
#   Item A = PUMP-01  →  pump_id  = _uid("item:PUMP-01")
#   Item B = VALVE-02 →  valve_id = _uid("item:VALVE-02")
#   Loc  A = DC-ATL   →  atl_id   = _uid("location:DC-ATL")
#   Loc  B = DC-LAX   →  lax_id   = _uid("location:DC-LAX")
# ---------------------------------------------------------------------------


def seed_enrichment(conn):
    """
    Enriches the MidCo Electronics demo with a full causal supply chain graph:
      - Suppliers (Taiwan Semi Components, Euro Parts GmbH)
      - SupplierItems (preferred sources per item)
      - item_planning_params (lead times, safety stock, MOQ per item×location)
      - OnHandSupply nodes (current WMS stock)
      - Additional PurchaseOrderSupply nodes (PO-001, PO-002, PO-003)
      - ForecastDemand nodes (12 weekly buckets per item)
      - CustomerOrderDemand nodes (CO-001, CO-002, CO-003)
      - Edges linking every supply/demand node to the matching ProjectedInventory bucket
    """
    print("\n🔧 Running MidCo enrichment (suppliers, POs, forecasts, COs, edges)...")

    pump_id  = _uid("item:PUMP-01")
    valve_id = _uid("item:VALVE-02")
    atl_id   = _uid("location:DC-ATL")
    lax_id   = _uid("location:DC-LAX")
    series_pump_atl  = _uid("series:PUMP-01:DC-ATL")
    series_valve_lax = _uid("series:VALVE-02:DC-LAX")

    _seed_suppliers(conn, pump_id, valve_id)
    _seed_item_planning_params(conn, pump_id, valve_id, atl_id, lax_id)
    _seed_onhand_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump_atl, series_valve_lax)
    _seed_po_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump_atl, series_valve_lax)
    _seed_forecast_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump_atl, series_valve_lax)
    _seed_co_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump_atl, series_valve_lax)

    conn.commit()
    print("  ✅ Enrichment complete — full causal graph inserted.")


def _seed_suppliers(conn, pump_id, valve_id):
    """Insert suppliers and supplier_items (idempotent via ON CONFLICT DO NOTHING)."""

    sup_tw_id = _uid("supplier:SUP-001")
    sup_de_id = _uid("supplier:SUP-002")

    conn.execute("""
        INSERT INTO suppliers (supplier_id, external_id, name, country, lead_time_days, reliability_score, status)
        VALUES
            (%s, 'SUP-001', 'Taiwan Semi Components', 'TW', 21, 0.920, 'active'),
            (%s, 'SUP-002', 'Euro Parts GmbH',        'DE',  7, 0.980, 'active')
        ON CONFLICT DO NOTHING
    """, (sup_tw_id, sup_de_id))
    print("  ✓ Suppliers: Taiwan Semi Components (SUP-001), Euro Parts GmbH (SUP-002)")

    # supplier_items
    conn.execute("""
        INSERT INTO supplier_items (supplier_item_id, supplier_id, item_id, lead_time_days, moq, unit_cost, currency, is_preferred)
        VALUES
            -- PUMP-01 → SUP-001 preferred
            (%s, %s, %s, 21, 100, 12.50, 'USD', TRUE),
            -- PUMP-01 → SUP-002 alternative
            (%s, %s, %s,  7, 500, 14.00, 'USD', FALSE),
            -- VALVE-02 → SUP-001 preferred
            (%s, %s, %s, 21,  50,  8.75, 'USD', TRUE)
        ON CONFLICT DO NOTHING
    """, (
        _uid("supplier_item:SUP-001:PUMP-01"),  sup_tw_id, pump_id,
        _uid("supplier_item:SUP-002:PUMP-01"),  sup_de_id, pump_id,
        _uid("supplier_item:SUP-001:VALVE-02"), sup_tw_id, valve_id,
    ))
    print("  ✓ SupplierItems: 3 sourcing links created")


def _seed_item_planning_params(conn, pump_id, valve_id, atl_id, lax_id):
    """Insert item_planning_params for PUMP-01@DC-ATL and VALVE-02@DC-LAX."""

    sup_tw_id = _uid("supplier:SUP-001")

    # PUMP-01 @ DC-ATL
    conn.execute("""
        INSERT INTO item_planning_params (
            param_id, item_id, location_id,
            lead_time_sourcing_days, lead_time_transit_days,
            safety_stock_qty, safety_stock_days,
            min_order_qty, order_multiple,
            lot_size_rule, planning_horizon_days,
            is_make, preferred_supplier_id,
            source, effective_from
        ) VALUES (%s, %s, %s, 21, 2, 50, 7, 100, 50, 'LOTFORLOT', 90, FALSE, %s, 'manual', %s)
        ON CONFLICT DO NOTHING
    """, (_uid("ipp:PUMP-01:DC-ATL:v1"), pump_id, atl_id, sup_tw_id, TODAY))

    # VALVE-02 @ DC-LAX
    conn.execute("""
        INSERT INTO item_planning_params (
            param_id, item_id, location_id,
            lead_time_sourcing_days, lead_time_transit_days,
            safety_stock_qty, safety_stock_days,
            min_order_qty, order_multiple,
            lot_size_rule, planning_horizon_days,
            is_make, preferred_supplier_id,
            source, effective_from
        ) VALUES (%s, %s, %s, 21, 3, 30, 5, 50, 25, 'LOTFORLOT', 90, FALSE, %s, 'manual', %s)
        ON CONFLICT DO NOTHING
    """, (_uid("ipp:VALVE-02:DC-LAX:v1"), valve_id, lax_id, sup_tw_id, TODAY))

    print("  ✓ item_planning_params: PUMP-01@DC-ATL, VALVE-02@DC-LAX")


def _find_first_pi(conn, series_id):
    """Return the node_id of bucket_sequence=0 for a given projection series."""
    row = conn.execute("""
        SELECT node_id FROM nodes
        WHERE projection_series_id = %s AND bucket_sequence = 0 AND active = TRUE
        LIMIT 1
    """, (series_id,)).fetchone()
    return row["node_id"] if row else None


def _insert_edge(conn, edge_name, edge_type, from_node_id, to_node_id):
    """Insert an edge idempotently."""
    conn.execute("""
        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, priority, active)
        VALUES (%s, %s, %s, %s, %s, 0, TRUE)
        ON CONFLICT DO NOTHING
    """, (_uid(f"edge:{edge_name}"), edge_type, from_node_id, to_node_id, BASELINE_SCENARIO_ID))


def _seed_onhand_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve):
    """OnHandSupply: current WMS stock for both items."""

    oh_pump_id  = _uid("node:OnHand:PUMP-01:DC-ATL")
    oh_valve_id = _uid("node:OnHand:VALVE-02:DC-LAX")

    conn.execute("""
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_ref, quantity, qty_uom, active
        ) VALUES
            (%s, 'OnHandSupply', %s, %s, %s, 'exact_date', %s, 30, 'EA', TRUE),
            (%s, 'OnHandSupply', %s, %s, %s, 'exact_date', %s, 45, 'EA', TRUE)
        ON CONFLICT DO NOTHING
    """, (
        oh_pump_id,  BASELINE_SCENARIO_ID, pump_id,  atl_id, TODAY,
        oh_valve_id, BASELINE_SCENARIO_ID, valve_id, lax_id, TODAY,
    ))
    print("  ✓ OnHandSupply: PUMP-01@DC-ATL=30, VALVE-02@DC-LAX=45")

    # Edges: OnHand → first PI bucket
    pi_pump  = _find_first_pi(conn, series_pump)
    pi_valve = _find_first_pi(conn, series_valve)
    if pi_pump:
        _insert_edge(conn, "contributes_to:OnHand-PUMP-01:DC-ATL", "replenishes", oh_pump_id, pi_pump)
    if pi_valve:
        _insert_edge(conn, "contributes_to:OnHand-VALVE-02:DC-LAX", "replenishes", oh_valve_id, pi_valve)
    print("  ✓ Edges: OnHandSupply → ProjectedInventory[0] (contributes_to)")


def _seed_po_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve):
    """
    PurchaseOrderSupply nodes — enrichment POs (on top of existing day-25 PO):
      PO-001: PUMP-01 @ DC-ATL, qty=500, day+25  (maps to existing scenario)
      PO-002: PUMP-01 @ DC-ATL, qty=300, day+45
      PO-003: VALVE-02 @ DC-LAX, qty=200, day+28
    """
    po001_id = _uid("node:PO-001:PUMP-01:DC-ATL:day25")
    po002_id = _uid("node:PO-002:PUMP-01:DC-ATL:day45")
    po003_id = _uid("node:PO-003:VALVE-02:DC-LAX:day28")

    conn.execute("""
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_ref, quantity, qty_uom, active
        ) VALUES
            (%s, 'PurchaseOrderSupply', %s, %s, %s, 'exact_date', %s, 500, 'EA', TRUE),
            (%s, 'PurchaseOrderSupply', %s, %s, %s, 'exact_date', %s, 300, 'EA', TRUE),
            (%s, 'PurchaseOrderSupply', %s, %s, %s, 'exact_date', %s, 200, 'EA', TRUE)
        ON CONFLICT DO NOTHING
    """, (
        po001_id, BASELINE_SCENARIO_ID, pump_id,  atl_id, TODAY + timedelta(days=25),
        po002_id, BASELINE_SCENARIO_ID, pump_id,  atl_id, TODAY + timedelta(days=45),
        po003_id, BASELINE_SCENARIO_ID, valve_id, lax_id, TODAY + timedelta(days=28),
    ))
    print("  ✓ PurchaseOrderSupply: PO-001 (day+25), PO-002 (day+45), PO-003 (day+28)")

    # Edges: each PO → nearest PI bucket (by arrival day)
    for po_id, series_id, bucket in [
        (po001_id, series_pump,  25),
        (po002_id, series_pump,  45),
        (po003_id, series_valve, 28),
    ]:
        pi_row = conn.execute("""
            SELECT node_id FROM nodes
            WHERE projection_series_id = %s AND bucket_sequence = %s AND active = TRUE
            LIMIT 1
        """, (series_id, bucket)).fetchone()
        if pi_row:
            _insert_edge(conn, f"replenishes:{po_id}:bucket{bucket}", "replenishes", po_id, pi_row["node_id"])

    print("  ✓ Edges: PurchaseOrderSupply → ProjectedInventory (replenishes)")


def _seed_forecast_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve):
    """
    ForecastDemand — 12 weekly buckets:
      PUMP-01 @ DC-ATL  : 105 units/week (15/day × 7)
      VALVE-02 @ DC-LAX :  56 units/week (8/day × 7)
    """
    nodes = []
    edges_data = []

    for week in range(12):
        week_start = TODAY + timedelta(weeks=week)
        week_end   = week_start + timedelta(days=7)

        # PUMP-01
        fc_pump_id = _uid(f"node:Forecast:PUMP-01:DC-ATL:week{week}")
        nodes.append((
            fc_pump_id, "ForecastDemand", BASELINE_SCENARIO_ID, pump_id, atl_id,
            "week", week_start, week_start, week_end, 105, "EA", True,
        ))

        # VALVE-02
        fc_valve_id = _uid(f"node:Forecast:VALVE-02:DC-LAX:week{week}")
        nodes.append((
            fc_valve_id, "ForecastDemand", BASELINE_SCENARIO_ID, valve_id, lax_id,
            "week", week_start, week_start, week_end, 56, "EA", True,
        ))

        # Map forecast to a PI bucket near the start of the week
        bucket_day = week * 7
        edges_data.append((fc_pump_id,  series_pump,  bucket_day, week))
        edges_data.append((fc_valve_id, series_valve, bucket_day, week))

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                time_grain, time_ref, time_span_start, time_span_end,
                quantity, qty_uom, active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, nodes)

    print(f"  ✓ ForecastDemand: 12 weekly buckets × 2 items = {len(nodes)} nodes")

    # Edges: ForecastDemand → ProjectedInventory (drives)
    edge_count = 0
    for fc_node_id, series_id, bucket_day, week in edges_data:
        # Use the closest available bucket (cap at 89 for 90-day horizon)
        capped_day = min(bucket_day, 89)
        pi_row = conn.execute("""
            SELECT node_id FROM nodes
            WHERE projection_series_id = %s AND bucket_sequence = %s AND active = TRUE
            LIMIT 1
        """, (series_id, capped_day)).fetchone()
        if pi_row:
            _insert_edge(conn, f"drives:{fc_node_id}:pi{capped_day}", "consumes", fc_node_id, pi_row["node_id"])
            edge_count += 1

    print(f"  ✓ Edges: ForecastDemand → ProjectedInventory (drives) — {edge_count} edges")


def _seed_co_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve):
    """
    CustomerOrderDemand (firm orders):
      CO-001: PUMP-01  @ DC-ATL, qty=80,  day+10
      CO-002: PUMP-01  @ DC-ATL, qty=120, day+20
      CO-003: VALVE-02 @ DC-LAX, qty=60,  day+15
    """
    co001_id = _uid("node:CO-001:PUMP-01:DC-ATL:day10")
    co002_id = _uid("node:CO-002:PUMP-01:DC-ATL:day20")
    co003_id = _uid("node:CO-003:VALVE-02:DC-LAX:day15")

    conn.execute("""
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_ref, quantity, qty_uom, active
        ) VALUES
            (%s, 'CustomerOrderDemand', %s, %s, %s, 'exact_date', %s, 80,  'EA', TRUE),
            (%s, 'CustomerOrderDemand', %s, %s, %s, 'exact_date', %s, 120, 'EA', TRUE),
            (%s, 'CustomerOrderDemand', %s, %s, %s, 'exact_date', %s, 60,  'EA', TRUE)
        ON CONFLICT DO NOTHING
    """, (
        co001_id, BASELINE_SCENARIO_ID, pump_id,  atl_id, TODAY + timedelta(days=10),
        co002_id, BASELINE_SCENARIO_ID, pump_id,  atl_id, TODAY + timedelta(days=20),
        co003_id, BASELINE_SCENARIO_ID, valve_id, lax_id, TODAY + timedelta(days=15),
    ))
    print("  ✓ CustomerOrderDemand: CO-001 (day+10), CO-002 (day+20), CO-003 (day+15)")

    # Edges: CustomerOrder → PI bucket at due date
    for co_id, series_id, bucket in [
        (co001_id, series_pump,  10),
        (co002_id, series_pump,  20),
        (co003_id, series_valve, 15),
    ]:
        pi_row = conn.execute("""
            SELECT node_id FROM nodes
            WHERE projection_series_id = %s AND bucket_sequence = %s AND active = TRUE
            LIMIT 1
        """, (series_id, bucket)).fetchone()
        if pi_row:
            _insert_edge(conn, f"drives:{co_id}:bucket{bucket}", "consumes", co_id, pi_row["node_id"])

    print("  ✓ Edges: CustomerOrderDemand → ProjectedInventory (drives)")


def seed_bom(conn):
    """
    Seed BOM (Bill of Materials) data for MidCo Electronics demo items.

    BOMs:
      - PUMP-01 (finished_good) → VALVE-02 (component × 2)
      - VALVE-02 (component)    → no sub-BOM (leaf component in this demo)

    This gives a simple 2-level BOM for MRP explosion testing:
      PUMP-01 needs 2 × VALVE-02 per unit (2% scrap), EA.

    LLC after import:
      VALVE-02 → LLC = 1 (appears at level 1 under PUMP-01)
    """
    print("\n🔧 Seeding BOM data for MidCo Electronics...")

    pump_id  = _uid("item:PUMP-01")
    valve_id = _uid("item:VALVE-02")

    # ── bom_headers: PUMP-01 v1.0 ────────────────────────────────────
    bom_pump_id = _uid("bom:PUMP-01:v1.0")
    conn.execute("""
        INSERT INTO bom_headers (bom_id, parent_item_id, bom_version, effective_from, status)
        VALUES (%s, %s, '1.0', %s, 'active')
        ON CONFLICT (parent_item_id, bom_version) DO UPDATE
            SET effective_from = EXCLUDED.effective_from,
                status         = EXCLUDED.status
    """, (bom_pump_id, pump_id, TODAY))
    print(f"  ✓ bom_headers: PUMP-01 v1.0 ({bom_pump_id[:8]}...)")

    # ── bom_lines: PUMP-01 → VALVE-02 (qty=2, scrap=2%) ─────────────
    bom_line_id = _uid("bom_line:PUMP-01:v1.0:VALVE-02")
    conn.execute("""
        INSERT INTO bom_lines (line_id, bom_id, component_item_id, quantity_per, uom, scrap_factor, llc)
        VALUES (%s, %s, %s, 2.0, 'EA', 0.02, 1)
        ON CONFLICT (bom_id, component_item_id) DO UPDATE
            SET quantity_per = EXCLUDED.quantity_per,
                uom          = EXCLUDED.uom,
                scrap_factor = EXCLUDED.scrap_factor,
                llc          = EXCLUDED.llc
    """, (bom_line_id, bom_pump_id, valve_id))
    print(f"  ✓ bom_lines: PUMP-01 → VALVE-02 (qty=2, scrap=2%, LLC=1)")

    conn.commit()
    print("  ✅ BOM seed complete.")
    print(f"     → Explode PUMP-01 × 100 units:")
    print(f"        gross_requirement VALVE-02 = 100 × 2 × 1.02 = 204 units")
    print(f"        on_hand @ DC-ATL = 30 units")
    print(f"        net_requirement = 174 units (shortage!)")


if __name__ == "__main__":
    print(f"Connecting to {DATABASE_URL}...")
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        seed(conn)
        seed_enrichment(conn)
        seed_bom(conn)
