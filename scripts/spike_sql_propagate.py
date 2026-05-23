"""
scripts/spike_sql_propagate.py — Tier 3 SQL-pure propagation spike.

Reformulates the propagation hot path as a single recursive CTE
inside PostgreSQL. Measured against the Python propagator on the
same bench seed (100x365 by default).

What this spike COVERS (= what the bench seed exercises):
- opening_stock from OnHandSupply via 'replenishes' edges (bucket 0)
- closing_stock chain via 'feeds_forward' edges (bucket N → N+1)
- supply events from PO/WO/Transfer/PlannedSupply via 'replenishes'
  where time_ref ∈ [bucket_start, bucket_end)

What this spike DOES NOT cover (= deferred until v0 wins):
- demand events with prorating (consumes edges, span-based)
- shortage detection / safety stock
- explanation tracking
- mixed scenarios (non-baseline)
- batched/incremental dirty propagation (we recompute the whole series)

The point is to answer: IS a SQL-pure rewrite worth pursuing?
We compare wall_seconds + closing_stock parity at the end.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@127.0.0.1:15432/ootils_dev \\
        python scripts/spike_sql_propagate.py --items 100 --buckets 365
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

# Re-use bench helpers
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_propagation import (  # type: ignore[import-not-found]
    _admin_recreate_db,
    _apply_migrations,
    _seed,
    _mark_all_pi_dirty,
    BASELINE_SCENARIO_ID,
)


PROPAGATE_SQL = """
-- Tier 3 spike v0.2: window-function projection instead of recursive CTE.
-- The inventory chain opening[N] = OH + sum_{k<N}(inflows[k] - outflows[k])
-- is a running sum, expressible as a single window over each series.
WITH per_bucket AS (
    SELECT
        pi.node_id,
        pi.projection_series_id,
        pi.bucket_sequence,
        pi.time_span_start,
        pi.time_span_end,
        -- OH seed: only present on bucket 0. SUM() OVER from start of series
        -- broadcasts the bucket-0 OH value across all subsequent buckets.
        CASE WHEN pi.bucket_sequence = 0 THEN COALESCE((
            SELECT SUM(oh.quantity)
            FROM edges r
            JOIN nodes oh ON oh.node_id = r.from_node_id
            WHERE r.to_node_id = pi.node_id
              AND r.edge_type = 'replenishes'
              AND r.scenario_id = pi.scenario_id
              AND r.active = TRUE
              AND oh.node_type = 'OnHandSupply'
              AND oh.active = TRUE
        ), 0)::numeric ELSE 0::numeric END AS oh_seed,
        -- inflows: supply events (non-OH) anchored in this bucket
        COALESCE((
            SELECT SUM(s.quantity)
            FROM edges r
            JOIN nodes s ON s.node_id = r.from_node_id
            WHERE r.to_node_id = pi.node_id
              AND r.edge_type = 'replenishes'
              AND r.scenario_id = pi.scenario_id
              AND r.active = TRUE
              AND s.node_type IN ('PurchaseOrderSupply','WorkOrderSupply','TransferSupply','PlannedSupply')
              AND s.active = TRUE
              AND s.time_ref >= pi.time_span_start
              AND s.time_ref <  pi.time_span_end
        ), 0)::numeric AS inflows,
        0::numeric AS outflows  -- demand path deferred for v0
    FROM nodes pi
    WHERE pi.node_type = 'ProjectedInventory'
      AND pi.scenario_id = %(scenario_id)s
      AND pi.active = TRUE
),
projected AS (
    SELECT
        node_id,
        bucket_sequence,
        inflows,
        outflows,
        -- opening = OH (broadcast from bucket 0) + accumulated (in-out) of preceding buckets
        SUM(oh_seed) OVER (
            PARTITION BY projection_series_id ORDER BY bucket_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        + COALESCE(SUM(inflows - outflows) OVER (
            PARTITION BY projection_series_id ORDER BY bucket_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS opening_stock
    FROM per_bucket
)
UPDATE nodes
SET opening_stock = p.opening_stock,
    inflows       = p.inflows,
    outflows      = p.outflows,
    closing_stock = p.opening_stock + p.inflows - p.outflows,
    is_dirty      = FALSE,
    last_calc_run_id = %(calc_run_id)s,
    updated_at    = now()
FROM projected p
WHERE nodes.node_id = p.node_id;
"""

CLEAR_DIRTY_SQL = """
DELETE FROM dirty_nodes
WHERE calc_run_id = %(calc_run_id)s AND scenario_id = %(scenario_id)s
"""


def _run_sql_propagation(conn: psycopg.Connection, calc_run_id: UUID) -> dict:
    """Drive the recursive-CTE propagation. Returns timing + counts."""
    started = time.perf_counter()
    cur = conn.execute(
        PROPAGATE_SQL,
        {"scenario_id": BASELINE_SCENARIO_ID, "calc_run_id": calc_run_id},
    )
    rows_updated = cur.rowcount
    conn.execute(
        CLEAR_DIRTY_SQL,
        {"scenario_id": BASELINE_SCENARIO_ID, "calc_run_id": calc_run_id},
    )
    conn.commit()
    wall = time.perf_counter() - started
    return {
        "rows_updated": rows_updated,
        "wall_seconds": round(wall, 3),
        "nodes_per_second": round(rows_updated / max(wall, 1e-9), 1),
    }


def _verify_results(conn: psycopg.Connection) -> dict:
    """Sanity-check the SQL results against expected closing_stock for the bench seed.

    Bench seed: 100 OnHand × every series; no supply/demand events.
    Expected: every PI bucket has opening = inflows = closing = 100, outflows = 0.
    """
    rows = conn.execute(
        """
        SELECT
            COUNT(*)                                 AS total,
            COUNT(*) FILTER (WHERE closing_stock = 100) AS closing_100,
            COUNT(*) FILTER (WHERE opening_stock = 100) AS opening_100,
            COUNT(*) FILTER (WHERE inflows = 0)         AS inflows_0,
            COUNT(*) FILTER (WHERE outflows = 0)        AS outflows_0,
            MIN(closing_stock)                          AS min_closing,
            MAX(closing_stock)                          AS max_closing
        FROM nodes
        WHERE node_type = 'ProjectedInventory'
          AND scenario_id = %s
          AND active = TRUE
        """,
        (BASELINE_SCENARIO_ID,),
    ).fetchone()
    return dict(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--items", type=int, default=100)
    parser.add_argument("--buckets", type=int, default=365)
    parser.add_argument("--dbname", default="ootils_test_bench")
    parser.add_argument("--skip-setup", action="store_true")
    args = parser.parse_args()

    base_dsn = os.environ.get("DATABASE_URL")
    if not base_dsn:
        print("FATAL: set DATABASE_URL")
        return 2

    target_dsn = base_dsn.rsplit("/", 1)[0] + f"/{args.dbname}"
    os.environ["DATABASE_URL"] = target_dsn

    if not args.skip_setup:
        _admin_recreate_db(base_dsn, args.dbname)
        _apply_migrations(target_dsn)

    with psycopg.connect(target_dsn, row_factory=dict_row) as conn:
        if not args.skip_setup:
            seed_stats = _seed(conn, items=args.items, buckets=args.buckets)
            print(f"[seed] {seed_stats}")
        calc_run_id, dirty = _mark_all_pi_dirty(conn)
        print(f"[dirty] marked {len(dirty)} PI nodes; calc_run_id={calc_run_id}")

        result = _run_sql_propagation(conn, calc_run_id)
        check = _verify_results(conn)

    print()
    print("=" * 60)
    print("SQL SPIKE RESULT")
    print("=" * 60)
    for k, v in result.items():
        print(f"  {k:24s}  {v}")
    print()
    print("=" * 60)
    print("PARITY CHECK")
    print("=" * 60)
    for k, v in check.items():
        print(f"  {k:24s}  {v}")
    expected_total = args.items * args.buckets
    ok = (
        check["total"] == expected_total
        and check["closing_100"] == expected_total
        and check["opening_100"] == expected_total
        and check["inflows_0"] == expected_total
        and check["outflows_0"] == expected_total
    )
    print()
    print(f"PARITY: {'OK' if ok else 'FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
