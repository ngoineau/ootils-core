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
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://ootils:ootils@localhost:5432/ootils_dev"
)

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"
TODAY = date.today()


def seed(conn):
    print("🌱 Seeding Ootils demo data...")

    # ------------------------------------------------------------------
    # 1. Items
    # ------------------------------------------------------------------
    pump_id = str(uuid4())
    valve_id = str(uuid4())

    conn.execute("""
        INSERT INTO items (item_id, name, item_type, uom, status)
        VALUES (%s, 'PUMP-01 Industrial Pump', 'finished_good', 'EA', 'active'),
               (%s, 'VALVE-02 Control Valve', 'component', 'EA', 'active')
        ON CONFLICT DO NOTHING
    """, (pump_id, valve_id))
    print(f"  ✓ Items: PUMP-01 ({pump_id[:8]}...), VALVE-02 ({valve_id[:8]}...)")

    # ------------------------------------------------------------------
    # 2. Locations
    # ------------------------------------------------------------------
    atl_id = str(uuid4())
    lax_id = str(uuid4())

    conn.execute("""
        INSERT INTO locations (location_id, name, location_type, country)
        VALUES (%s, 'DC-ATL Atlanta Distribution Center', 'dc', 'US'),
               (%s, 'DC-LAX Los Angeles Distribution Center', 'dc', 'US')
        ON CONFLICT DO NOTHING
    """, (atl_id, lax_id))
    print(f"  ✓ Locations: DC-ATL ({atl_id[:8]}...), DC-LAX ({lax_id[:8]}...)")

    # ------------------------------------------------------------------
    # 3. Projection series
    # ------------------------------------------------------------------
    series_pump_atl = str(uuid4())
    series_valve_lax = str(uuid4())
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
    conn.execute("""
        INSERT INTO events (event_type, scenario_id, source)
        VALUES ('ingestion_complete', %s, 'seed_script')
    """, (BASELINE_SCENARIO_ID,))
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
            str(uuid4()), "ProjectedInventory", BASELINE_SCENARIO_ID, item_id, location_id,
            "day", d, d, d + timedelta(days=1),
            True,  # is_dirty — will be recomputed when engine runs
            True,
            str(series_id), i,
            running, inflows, outflows, closing,
            has_shortage, shortage_qty,
        ))
        running = closing

    conn.executemany("""
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
            str(uuid4()), "ProjectedInventory", BASELINE_SCENARIO_ID, item_id, location_id,
            "day", d, d, d + timedelta(days=1),
            True, True, str(series_id), i,
            running, inflows, outflows, closing,
            has_shortage, shortage_qty,
        ))
        running = closing

    conn.executemany("""
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
    po_pump_id = str(uuid4())
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
        conn.execute("""
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, priority, active)
            VALUES (%s, 'replenishes', %s, %s, %s, 0, TRUE)
            ON CONFLICT DO NOTHING
        """, (str(uuid4()), po_pump_id, pi_row["node_id"], BASELINE_SCENARIO_ID))


def _seed_demand_nodes(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve):
    """Create customer order demand nodes and wire them to PI nodes."""
    # Customer order for PUMP-01: 50 units due day 15
    co_pump_id = str(uuid4())
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
        conn.execute("""
            INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, priority, active)
            VALUES (%s, 'consumes', %s, %s, %s, 1, TRUE)
            ON CONFLICT DO NOTHING
        """, (str(uuid4()), co_pump_id, pi_row["node_id"], BASELINE_SCENARIO_ID))

    # Seed shortages table directly for demo
    # (in production, the propagation engine would generate these)
    _seed_shortages(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve)


def _seed_shortages(conn, pump_id, valve_id, atl_id, lax_id, series_pump, series_valve):
    """Seed shortage records directly for demo (bypasses engine for initial seed)."""
    # Create a placeholder calc_run for the seed
    calc_run_id = str(uuid4())
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

        conn.execute("""
            INSERT INTO shortages (
                shortage_id, scenario_id, pi_node_id, item_id, location_id,
                shortage_date, shortage_qty, severity_score, calc_run_id, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active')
            ON CONFLICT DO NOTHING
        """, (str(uuid4()), BASELINE_SCENARIO_ID, pi_row["node_id"],
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

        conn.execute("""
            INSERT INTO shortages (
                shortage_id, scenario_id, pi_node_id, item_id, location_id,
                shortage_date, shortage_qty, severity_score, calc_run_id, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active')
            ON CONFLICT DO NOTHING
        """, (str(uuid4()), BASELINE_SCENARIO_ID, pi_row["node_id"],
              valve_id, lax_id, d, shortage_qty, severity, calc_run_id))


if __name__ == "__main__":
    print(f"Connecting to {DATABASE_URL}...")
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        seed(conn)
