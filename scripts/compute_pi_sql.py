"""
compute_pi_sql.py — Bulk projection compute via PostgreSQL window functions.

Replaces the Python propagation engine for the case "rebuild all PI from
scratch after bulk_ingest + bootstrap_pi + fix_wireup".

The engine event-driven Python (and later Rust) remain the source of truth
for INCREMENTAL updates (1 PO created → propagate only the impacted chain).
This script is for BULK / FULL RECOMPUTE — typically:
  - after initial pilot load
  - after a bulk parameter change
  - nightly refresh
  - before a scenario fork

Pattern :
  1. Aggregate per PI bucket: initial_opening (OnHand at bucket_seq=0),
     inflows (PO/Transfer this date × weight_ratio),
     outflows (CO + Forecasts targeting this PI × weight_ratio)
  2. Window function PARTITION BY series ORDER BY bucket_sequence:
     closing(t) = cumulative_sum(initial_opening + inflows - outflows) up to t
     opening(t) = closing(t) - inflows(t) + outflows(t)
  3. has_shortage = closing < 0, shortage_qty = ABS(closing) when < 0

Usage:
    python scripts/compute_pi_sql.py
    python scripts/compute_pi_sql.py --dsn postgresql://...
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("compute_pi_sql")

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


def _guard_db(dsn: str, allow_dev: bool) -> str:
    db_name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not db_name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{db_name}' does not start with 'ootils'.")
    if db_name == "ootils_dev" and not allow_dev:
        raise SystemExit(f"REFUSED: '{db_name}' is semi-prod, pass --allow-dev.")
    return db_name


def compute(conn: psycopg.Connection) -> dict:
    cur = conn.cursor()
    timings = {}

    # Increase work_mem for this session — bulk aggregation needs RAM.
    # Default 4MB is too small for 540K PI × ~200K edges aggregation.
    cur.execute("SET work_mem = '256MB'")
    cur.execute("SET max_parallel_workers_per_gather = 0")  # avoid parallel that uses /dev/shm

    # ── 1. Aggregate IO per PI bucket into temp table ──────────────
    # initial_opening: OnHand quantity arriving at PI[0]
    # inflows:        sum of PO/Transfer × weight_ratio targeting this PI
    # outflows:       sum of CO/Forecast × weight_ratio targeting this PI
    t0 = time.perf_counter()
    cur.execute(
        """
        CREATE TEMP TABLE _pi_io AS
        SELECT
            pi.node_id,
            pi.projection_series_id,
            pi.bucket_sequence,
            COALESCE(SUM(CASE
                WHEN pi.bucket_sequence = 0
                 AND src.node_type = 'OnHandSupply'
                 AND e.edge_type = 'replenishes'
                THEN src.quantity * e.weight_ratio
                ELSE 0 END), 0)::NUMERIC AS initial_opening,
            COALESCE(SUM(CASE
                WHEN src.node_type IN ('PurchaseOrderSupply', 'TransferSupply')
                 AND e.edge_type = 'replenishes'
                THEN src.quantity * e.weight_ratio
                ELSE 0 END), 0)::NUMERIC AS inflows,
            COALESCE(SUM(CASE
                WHEN e.edge_type = 'consumes'
                THEN src.quantity * e.weight_ratio
                ELSE 0 END), 0)::NUMERIC AS outflows
        FROM nodes pi
        LEFT JOIN edges e
          ON e.to_node_id = pi.node_id
         AND e.edge_type IN ('replenishes', 'consumes')
         AND e.active = TRUE
        LEFT JOIN nodes src
          ON src.node_id = e.from_node_id
        WHERE pi.node_type = 'ProjectedInventory'
          AND pi.scenario_id = %s::uuid
        GROUP BY pi.node_id, pi.projection_series_id, pi.bucket_sequence
        """,
        (BASELINE_SCENARIO_ID,),
    )
    cur.execute("SELECT COUNT(*) FROM _pi_io")
    n_pi = cur.fetchone()[0]
    timings["1_aggregate_io_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Aggregated IO for %d PI buckets in %.2fs", n_pi, timings["1_aggregate_io_s"])

    # ── 2. Compute closing via cumulative sum window function ──────
    # closing(t) = Σ(s=0..t) [initial_opening(s) + inflows(s) - outflows(s)]
    # opening(t) = closing(t) - inflows(t) + outflows(t)  (algebraic identity)
    t0 = time.perf_counter()
    cur.execute(
        """
        CREATE TEMP TABLE _pi_computed AS
        SELECT
            node_id,
            inflows,
            outflows,
            -- closing: cumulative sum of net flow up to this bucket
            SUM(initial_opening + inflows - outflows)
                OVER (PARTITION BY projection_series_id ORDER BY bucket_sequence
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                AS closing_stock
        FROM _pi_io
        """
    )
    timings["2_window_compute_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Computed closing via window function in %.2fs", timings["2_window_compute_s"])

    # ── 3. Bulk UPDATE the PI nodes ─────────────────────────────────
    t0 = time.perf_counter()
    cur.execute(
        """
        UPDATE nodes pi
        SET inflows       = c.inflows,
            outflows      = c.outflows,
            closing_stock = c.closing_stock,
            opening_stock = c.closing_stock - c.inflows + c.outflows,
            has_shortage  = (c.closing_stock < 0),
            shortage_qty  = CASE WHEN c.closing_stock < 0
                                 THEN -c.closing_stock
                                 ELSE 0 END,
            updated_at    = now()
        FROM _pi_computed c
        WHERE pi.node_id = c.node_id
        """
    )
    n_updated = cur.rowcount
    timings["3_update_pi_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Updated %d PI nodes in %.2fs", n_updated, timings["3_update_pi_s"])

    # ── 4. Summary metrics ──────────────────────────────────────────
    t0 = time.perf_counter()
    summary = cur.execute(
        """
        SELECT
            COUNT(*)                            AS total_pi,
            COUNT(*) FILTER (WHERE has_shortage) AS pi_with_shortage,
            COUNT(DISTINCT projection_series_id)
                FILTER (WHERE has_shortage)      AS series_with_shortage,
            COALESCE(MIN(closing_stock), 0)::TEXT AS worst_closing,
            COALESCE(MAX(shortage_qty), 0)::TEXT  AS max_shortage_qty,
            COALESCE(SUM(inflows), 0)::TEXT       AS total_inflows,
            COALESCE(SUM(outflows), 0)::TEXT      AS total_outflows
        FROM nodes
        WHERE node_type = 'ProjectedInventory'
          AND scenario_id = %s::uuid
        """,
        (BASELINE_SCENARIO_ID,),
    ).fetchone()
    timings["4_summary_metrics_s"] = round(time.perf_counter() - t0, 2)

    return {
        "pi_aggregated": n_pi,
        "pi_updated": n_updated,
        "total_pi": summary[0],
        "pi_with_shortage": summary[1],
        "series_with_shortage": summary[2],
        "worst_closing": summary[3],
        "max_shortage_qty": summary[4],
        "total_inflows": summary[5],
        "total_outflows": summary[6],
        "timings_s": timings,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="SQL bulk compute of ProjectedInventory projection.")
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--allow-dev", action="store_true")
    args = parser.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set and --dsn not provided")
        return 2

    db = _guard_db(args.dsn, args.allow_dev)
    logger.info("Compute PI (SQL bulk): DB=%s", db)

    t0 = time.perf_counter()
    with psycopg.connect(args.dsn) as conn:
        result = compute(conn)
        conn.commit()
    total = round(time.perf_counter() - t0, 2)

    logger.info("=" * 60)
    logger.info("PI COMPUTE DONE in %.2fs", total)
    logger.info("  PI aggregated         : %d", result["pi_aggregated"])
    logger.info("  PI updated            : %d", result["pi_updated"])
    logger.info("  PI with shortage      : %d", result["pi_with_shortage"])
    logger.info("  Series with shortage  : %d", result["series_with_shortage"])
    logger.info("  Worst closing         : %s", result["worst_closing"])
    logger.info("  Max shortage qty      : %s", result["max_shortage_qty"])
    logger.info("  Total inflows         : %s", result["total_inflows"])
    logger.info("  Total outflows        : %s", result["total_outflows"])
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
