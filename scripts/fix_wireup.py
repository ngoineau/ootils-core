"""
fix_wireup.py — Improve supply/demand → PI edges after bootstrap_pi.py.

Replaces the strict-date-match wireup with the proper APS logic:
  - OnHand always feeds PI[0] (opening stock for the series)
  - PO/Transfer with time_ref ∈ horizon → PI[exact_date]
  - CO with time_ref ∈ horizon  → PI[exact_date]
  - Forecast spread across all PI buckets that overlap its time_span
    (quantity divided by number of overlapping buckets via weight_ratio)

Only operates on (item, location) pairs that already have a projection_series.

Usage:
    DATABASE_URL=postgresql://... python scripts/fix_wireup.py
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("fix_wireup")

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


def _guard_db(dsn: str, allow_dev: bool) -> str:
    db_name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not db_name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{db_name}' does not start with 'ootils'.")
    if db_name == "ootils_dev" and not allow_dev:
        raise SystemExit(f"REFUSED: '{db_name}' is semi-prod, pass --allow-dev.")
    return db_name


def rewire(conn: psycopg.Connection) -> dict:
    cur = conn.cursor()
    timings = {}

    # ── 0. Wipe existing replenishes/consumes edges in baseline scenario ──
    t0 = time.perf_counter()
    cur.execute(
        """
        DELETE FROM edges
        WHERE scenario_id = %s::uuid
          AND edge_type IN ('replenishes', 'consumes')
        """,
        (BASELINE_SCENARIO_ID,),
    )
    n_deleted = cur.rowcount
    timings["0_wipe_old_edges_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Wiped %d existing replenishes/consumes edges in %.2fs", n_deleted, timings["0_wipe_old_edges_s"])

    # ── 1. OnHand → PI[0] (opening stock per series) ──────────────
    t0 = time.perf_counter()
    cur.execute(
        """
        INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id, weight_ratio, active)
        SELECT 'replenishes', oh.node_id, pi0.node_id, %s::uuid, 1.0, TRUE
        FROM nodes oh
        JOIN projection_series ps
          ON ps.item_id = oh.item_id
         AND ps.location_id = oh.location_id
         AND ps.scenario_id = oh.scenario_id
        JOIN nodes pi0
          ON pi0.projection_series_id = ps.series_id
         AND pi0.bucket_sequence = 0
         AND pi0.node_type = 'ProjectedInventory'
        WHERE oh.node_type = 'OnHandSupply'
          AND oh.scenario_id = %s::uuid
          AND oh.active = TRUE
        """,
        (BASELINE_SCENARIO_ID, BASELINE_SCENARIO_ID),
    )
    n_oh = cur.rowcount
    timings["1_onhand_to_pi0_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Wired %d OnHand → PI[0] edges in %.2fs", n_oh, timings["1_onhand_to_pi0_s"])

    # ── 2. PO + Transfer → PI[exact_date] ─────────────────────────
    t0 = time.perf_counter()
    cur.execute(
        """
        INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id, weight_ratio, active)
        SELECT 'replenishes', supply.node_id, pi.node_id, %s::uuid, 1.0, TRUE
        FROM nodes supply
        JOIN nodes pi
          ON pi.projection_series_id IS NOT NULL
         AND pi.item_id = supply.item_id
         AND pi.location_id = supply.location_id
         AND pi.node_type = 'ProjectedInventory'
         AND pi.scenario_id = supply.scenario_id
         AND pi.time_span_start = supply.time_ref
        WHERE supply.node_type IN ('PurchaseOrderSupply', 'TransferSupply')
          AND supply.scenario_id = %s::uuid
          AND supply.active = TRUE
        """,
        (BASELINE_SCENARIO_ID, BASELINE_SCENARIO_ID),
    )
    n_po_tr = cur.rowcount
    timings["2_po_transfer_to_pi_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Wired %d PO/Transfer → PI edges in %.2fs", n_po_tr, timings["2_po_transfer_to_pi_s"])

    # ── 3. CustomerOrder → PI[exact_date] ─────────────────────────
    t0 = time.perf_counter()
    cur.execute(
        """
        INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id, weight_ratio, active)
        SELECT 'consumes', co.node_id, pi.node_id, %s::uuid, 1.0, TRUE
        FROM nodes co
        JOIN nodes pi
          ON pi.projection_series_id IS NOT NULL
         AND pi.item_id = co.item_id
         AND pi.location_id = co.location_id
         AND pi.node_type = 'ProjectedInventory'
         AND pi.scenario_id = co.scenario_id
         AND pi.time_span_start = co.time_ref
        WHERE co.node_type = 'CustomerOrderDemand'
          AND co.scenario_id = %s::uuid
          AND co.active = TRUE
        """,
        (BASELINE_SCENARIO_ID, BASELINE_SCENARIO_ID),
    )
    n_co = cur.rowcount
    timings["3_co_to_pi_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Wired %d CO → PI edges in %.2fs", n_co, timings["3_co_to_pi_s"])

    # ── 4. ForecastDemand → PI buckets that overlap [time_span_start, time_span_end[
    #       weight_ratio = 1 / n_buckets to spread the quantity evenly
    t0 = time.perf_counter()
    cur.execute(
        """
        WITH spread AS (
            SELECT f.node_id AS forecast_id, pi.node_id AS pi_id
            FROM nodes f
            JOIN nodes pi
              ON pi.projection_series_id IS NOT NULL
             AND pi.item_id = f.item_id
             AND pi.location_id = f.location_id
             AND pi.node_type = 'ProjectedInventory'
             AND pi.scenario_id = f.scenario_id
             AND pi.time_span_start >= f.time_span_start
             AND pi.time_span_start < f.time_span_end
            WHERE f.node_type = 'ForecastDemand'
              AND f.scenario_id = %s::uuid
              AND f.active = TRUE
        ),
        counts AS (
            SELECT forecast_id, COUNT(*)::numeric AS n_buckets
            FROM spread GROUP BY forecast_id
        )
        INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id, weight_ratio, active)
        SELECT 'consumes', s.forecast_id, s.pi_id, %s::uuid, 1.0 / c.n_buckets, TRUE
        FROM spread s
        JOIN counts c ON c.forecast_id = s.forecast_id
        """,
        (BASELINE_SCENARIO_ID, BASELINE_SCENARIO_ID),
    )
    n_fc = cur.rowcount
    timings["4_forecast_spread_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Wired %d Forecast → PI edges (spread) in %.2fs", n_fc, timings["4_forecast_spread_s"])

    return {
        "edges_deleted": n_deleted,
        "onhand_pi0_edges": n_oh,
        "po_transfer_edges": n_po_tr,
        "co_edges": n_co,
        "forecast_edges": n_fc,
        "total_inserted": n_oh + n_po_tr + n_co + n_fc,
        "timings_s": timings,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Rewire supply/demand → PI edges (post-bootstrap fix).")
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--allow-dev", action="store_true")
    args = parser.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set and --dsn not provided")
        return 2
    db = _guard_db(args.dsn, args.allow_dev)
    logger.info("Fix wireup: DB=%s", db)

    t0 = time.perf_counter()
    with psycopg.connect(args.dsn) as conn:
        result = rewire(conn)
        conn.commit()
    total = round(time.perf_counter() - t0, 2)

    logger.info("=" * 60)
    logger.info("FIX WIREUP DONE in %.2fs", total)
    logger.info("  Edges deleted          : %d", result["edges_deleted"])
    logger.info("  OnHand → PI[0]         : %d", result["onhand_pi0_edges"])
    logger.info("  PO/Transfer → PI[date] : %d", result["po_transfer_edges"])
    logger.info("  CO → PI[date]          : %d", result["co_edges"])
    logger.info("  Forecast → PI (spread) : %d", result["forecast_edges"])
    logger.info("  TOTAL EDGES INSERTED   : %d", result["total_inserted"])
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
