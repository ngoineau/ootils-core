"""
bootstrap_pi.py — Materialize the ProjectedInventory graph (PI buckets + edges).

After bulk_ingest.py loads supply / demand nodes, this script creates the
ProjectedInventory time-series infrastructure that the engine needs to compute
projections and detect shortages.

For each (item, location) pair that has activity (at least one node), it
creates:
  1. one projection_series row
  2. N daily ProjectedInventory nodes (horizon)
  3. feeds_forward edges PI[t] → PI[t+1] within each series
  4. replenishes / consumes edges from existing supply/demand nodes to the
     matching PI bucket by date

Usage:
    python scripts/bootstrap_pi.py --horizon 540 --sample 1000
    python scripts/bootstrap_pi.py --horizon 540  # full scope (all active pairs)

Safety: only writes to DB whose name starts with 'ootils_' and is not 'ootils_dev'
unless --allow-dev is given.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from uuid import UUID

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("bootstrap_pi")

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


def _guard_db(dsn: str, allow_dev: bool) -> str:
    db_name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not db_name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{db_name}' does not start with 'ootils'.")
    if db_name == "ootils_dev" and not allow_dev:
        raise SystemExit(f"REFUSED: '{db_name}' is semi-prod, pass --allow-dev to override.")
    return db_name


def bootstrap(conn: psycopg.Connection, horizon: int, sample: int | None) -> dict:
    """Bootstrap the PI graph.

    Returns dict with counts and timings.
    """
    cur = conn.cursor()
    today = date.today()
    horizon_start = today
    horizon_end = today + timedelta(days=horizon)
    timings = {}

    # ── 1. Identify (item, location) pairs with activity ──────────
    t0 = time.perf_counter()
    cur.execute(
        """
        CREATE TEMP TABLE _b_pi_pairs AS
        SELECT DISTINCT item_id, location_id
        FROM nodes
        WHERE scenario_id = %s::uuid
          AND item_id IS NOT NULL
          AND location_id IS NOT NULL
          AND node_type IN ('OnHandSupply', 'PurchaseOrderSupply', 'TransferSupply',
                            'CustomerOrderDemand', 'ForecastDemand')
        """,
        (BASELINE_SCENARIO_ID,),
    )
    cur.execute("SELECT COUNT(*) FROM _b_pi_pairs")
    total_pairs = cur.fetchone()[0]
    logger.info("Found %d (item, location) pairs with activity", total_pairs)

    # Sample if requested
    if sample is not None and sample < total_pairs:
        cur.execute(
            "CREATE TEMP TABLE _b_pi_sample AS SELECT * FROM _b_pi_pairs ORDER BY random() LIMIT %s",
            (sample,),
        )
        pairs_table = "_b_pi_sample"
        logger.info("Sampling %d pairs", sample)
    else:
        pairs_table = "_b_pi_pairs"
    cur.execute(f"SELECT COUNT(*) FROM {pairs_table}")
    n_pairs = cur.fetchone()[0]
    timings["1_identify_pairs_s"] = round(time.perf_counter() - t0, 2)

    # ── 2. Create projection_series ───────────────────────────────
    t0 = time.perf_counter()
    cur.execute(
        f"""
        INSERT INTO projection_series (item_id, location_id, scenario_id, horizon_start, horizon_end)
        SELECT p.item_id, p.location_id, %s::uuid, %s::date, %s::date
        FROM {pairs_table} p
        ON CONFLICT (item_id, location_id, scenario_id) DO NOTHING
        """,
        (BASELINE_SCENARIO_ID, horizon_start, horizon_end),
    )
    n_series = cur.rowcount
    timings["2_create_series_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Created %d projection_series in %.2fs", n_series, timings["2_create_series_s"])

    # ── 3. Create PI nodes (one per day per series) ───────────────
    t0 = time.perf_counter()
    cur.execute(
        f"""
        INSERT INTO nodes (
            node_type, scenario_id, item_id, location_id,
            time_grain, time_ref, time_span_start, time_span_end,
            projection_series_id, bucket_sequence,
            opening_stock, inflows, outflows, closing_stock,
            active
        )
        SELECT
            'ProjectedInventory',
            %s::uuid,
            ps.item_id,
            ps.location_id,
            'day',
            (%s::date + (gs.day_offset * INTERVAL '1 day'))::date,
            (%s::date + (gs.day_offset * INTERVAL '1 day'))::date,
            (%s::date + ((gs.day_offset + 1) * INTERVAL '1 day'))::date,
            ps.series_id,
            gs.day_offset,
            0, 0, 0, 0,
            TRUE
        FROM projection_series ps
        JOIN {pairs_table} p ON p.item_id = ps.item_id AND p.location_id = ps.location_id
        CROSS JOIN generate_series(0, %s - 1) AS gs(day_offset)
        WHERE ps.scenario_id = %s::uuid
        """,
        (BASELINE_SCENARIO_ID, horizon_start, horizon_start, horizon_start, horizon, BASELINE_SCENARIO_ID),
    )
    n_pi = cur.rowcount
    timings["3_create_pi_nodes_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Created %d ProjectedInventory nodes in %.2fs", n_pi, timings["3_create_pi_nodes_s"])

    # ── 4. feeds_forward edges PI[t] → PI[t+1] within each series ─
    # Use LEAD window function instead of self-join → single sort+scan, much faster
    t0 = time.perf_counter()
    cur.execute(
        """
        INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id, active)
        SELECT 'feeds_forward', node_id, next_node_id, %s::uuid, TRUE
        FROM (
            SELECT
                node_id,
                LEAD(node_id) OVER (PARTITION BY projection_series_id ORDER BY bucket_sequence) AS next_node_id
            FROM nodes
            WHERE node_type = 'ProjectedInventory'
              AND scenario_id = %s::uuid
              AND projection_series_id IS NOT NULL
        ) sub
        WHERE next_node_id IS NOT NULL
        """,
        (BASELINE_SCENARIO_ID, BASELINE_SCENARIO_ID),
    )
    n_ff = cur.rowcount
    timings["4_feeds_forward_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Created %d feeds_forward edges in %.2fs", n_ff, timings["4_feeds_forward_s"])

    # ── 5. Wire supply nodes (OnHand, PO, Transfer) → PI buckets ──
    # Each supply node points to the PI bucket with matching date.
    t0 = time.perf_counter()
    cur.execute(
        f"""
        INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id, weight_ratio, active)
        SELECT 'replenishes', supply.node_id, pi.node_id, %s::uuid, 1.0, TRUE
        FROM nodes supply
        JOIN {pairs_table} p ON p.item_id = supply.item_id AND p.location_id = supply.location_id
        JOIN nodes pi ON pi.projection_series_id IS NOT NULL
                     AND pi.item_id = supply.item_id
                     AND pi.location_id = supply.location_id
                     AND pi.node_type = 'ProjectedInventory'
                     AND pi.scenario_id = %s::uuid
                     AND pi.time_span_start = supply.time_ref
        WHERE supply.node_type IN ('OnHandSupply', 'PurchaseOrderSupply', 'TransferSupply')
          AND supply.scenario_id = %s::uuid
          AND supply.active = TRUE
          AND supply.time_ref BETWEEN %s::date AND %s::date
        """,
        (BASELINE_SCENARIO_ID, BASELINE_SCENARIO_ID, BASELINE_SCENARIO_ID, horizon_start, horizon_end),
    )
    n_sup_edges = cur.rowcount
    timings["5_supply_edges_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Created %d supply→PI edges in %.2fs", n_sup_edges, timings["5_supply_edges_s"])

    # ── 6. Wire demand nodes (CO, ForecastDemand) → PI buckets ────
    t0 = time.perf_counter()
    cur.execute(
        f"""
        INSERT INTO edges (edge_type, from_node_id, to_node_id, scenario_id, weight_ratio, active)
        SELECT 'consumes', demand.node_id, pi.node_id, %s::uuid, 1.0, TRUE
        FROM nodes demand
        JOIN {pairs_table} p ON p.item_id = demand.item_id AND p.location_id = demand.location_id
        JOIN nodes pi ON pi.projection_series_id IS NOT NULL
                     AND pi.item_id = demand.item_id
                     AND pi.location_id = demand.location_id
                     AND pi.node_type = 'ProjectedInventory'
                     AND pi.scenario_id = %s::uuid
                     AND pi.time_span_start = demand.time_ref
        WHERE demand.node_type IN ('CustomerOrderDemand', 'ForecastDemand')
          AND demand.scenario_id = %s::uuid
          AND demand.active = TRUE
          AND demand.time_ref BETWEEN %s::date AND %s::date
        """,
        (BASELINE_SCENARIO_ID, BASELINE_SCENARIO_ID, BASELINE_SCENARIO_ID, horizon_start, horizon_end),
    )
    n_dem_edges = cur.rowcount
    timings["6_demand_edges_s"] = round(time.perf_counter() - t0, 2)
    logger.info("Created %d demand→PI edges in %.2fs", n_dem_edges, timings["6_demand_edges_s"])

    return {
        "pairs_in_scope": n_pairs,
        "total_pairs_with_activity": total_pairs,
        "horizon_days": horizon,
        "horizon_start": str(horizon_start),
        "horizon_end": str(horizon_end),
        "projection_series_created": n_series,
        "pi_nodes_created": n_pi,
        "feeds_forward_edges": n_ff,
        "supply_edges": n_sup_edges,
        "demand_edges": n_dem_edges,
        "total_rows": n_series + n_pi + n_ff + n_sup_edges + n_dem_edges,
        "timings_s": timings,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bootstrap ProjectedInventory graph after bulk_ingest.")
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--horizon", type=int, default=540, help="Horizon in days (default 540 = 18 months)")
    parser.add_argument("--sample", type=int, default=None, help="Sample N pairs (default: full scope)")
    parser.add_argument("--allow-dev", action="store_true")
    args = parser.parse_args(argv)

    if not args.dsn:
        logger.error("DATABASE_URL not set and --dsn not provided")
        return 2

    db = _guard_db(args.dsn, args.allow_dev)
    logger.info("Bootstrap PI: DB=%s horizon=%dj sample=%s", db, args.horizon, args.sample)

    t0 = time.perf_counter()
    with psycopg.connect(args.dsn) as conn:
        result = bootstrap(conn, args.horizon, args.sample)
        conn.commit()
    total = round(time.perf_counter() - t0, 2)

    logger.info("=" * 60)
    logger.info("PI BOOTSTRAP DONE in %.2fs", total)
    logger.info("  Pairs in scope          : %d / %d", result["pairs_in_scope"], result["total_pairs_with_activity"])
    logger.info("  Horizon                 : %s → %s (%d days)", result["horizon_start"], result["horizon_end"], result["horizon_days"])
    logger.info("  projection_series       : %d", result["projection_series_created"])
    logger.info("  ProjectedInventory nodes: %d", result["pi_nodes_created"])
    logger.info("  feeds_forward edges     : %d", result["feeds_forward_edges"])
    logger.info("  supply→PI edges         : %d", result["supply_edges"])
    logger.info("  demand→PI edges         : %d", result["demand_edges"])
    logger.info("  TOTAL ROWS              : %d", result["total_rows"])
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
