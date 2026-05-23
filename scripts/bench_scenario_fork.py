"""
scripts/bench_scenario_fork.py — Measure the cost of ScenarioManager.create_scenario.

Used as the before/after harness for REVIEW-2026-05 R10 (lazy CoW).

Usage:
    DATABASE_URL=postgresql://ootils:ootils@127.0.0.1:15432/ootils_test_fork \\
        OOTILS_API_TOKEN=bench \\
        python scripts/bench_scenario_fork.py --items 50 --buckets 30

The bench:
  1. Recreates `ootils_test_fork` and applies migrations
  2. Seeds the baseline scenario with N items × M buckets (PI nodes +
     feeds_forward edges + an OnHandSupply per item)
  3. Forks the baseline via ScenarioManager.create_scenario, timing the
     call and counting queries
  4. Reports wall time, query count, and storage delta in the nodes table
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

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


def _admin_recreate_db(dsn: str, dbname: str) -> None:
    base = dsn.rsplit("/", 1)[0]
    with psycopg.connect(f"{base}/postgres", autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.execute(f'CREATE DATABASE "{dbname}" OWNER ootils')


def _seed(conn: psycopg.Connection, items: int, buckets: int) -> dict:
    location_id = uuid4()
    today = date.today()
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, 'BENCH-LOC')",
        (location_id,),
    )

    pi_count = 0
    edge_count = 0
    for i in range(items):
        item_id = uuid4()
        conn.execute(
            "INSERT INTO items (item_id, name) VALUES (%s, %s)",
            (item_id, f"BENCH-ITEM-{i:05d}"),
        )

        series_id = uuid4()
        conn.execute(
            """
            INSERT INTO projection_series
                (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (series_id, item_id, location_id, BASELINE_SCENARIO_ID, today, today + timedelta(days=buckets)),
        )

        oh_id = uuid4()
        conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, scenario_id, item_id, location_id,
                 quantity, qty_uom, time_grain, time_ref, is_dirty, active)
            VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, 'EA', 'exact_date', %s, FALSE, TRUE)
            """,
            (oh_id, BASELINE_SCENARIO_ID, item_id, location_id, Decimal("100"), today),
        )

        prev_pi = None
        for b in range(buckets):
            pi_id = uuid4()
            conn.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, item_id, location_id,
                     time_grain, time_span_start, time_span_end,
                     projection_series_id, bucket_sequence, is_dirty, active)
                VALUES (%s, 'ProjectedInventory', %s, %s, %s,
                        'day', %s, %s, %s, %s, FALSE, TRUE)
                """,
                (pi_id, BASELINE_SCENARIO_ID, item_id, location_id,
                 today + timedelta(days=b), today + timedelta(days=b + 1),
                 series_id, b),
            )
            pi_count += 1
            edge_id = uuid4()
            if prev_pi is None:
                conn.execute(
                    """
                    INSERT INTO edges
                        (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
                    VALUES (%s, 'replenishes', %s, %s, %s, TRUE)
                    """,
                    (edge_id, oh_id, pi_id, BASELINE_SCENARIO_ID),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO edges
                        (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
                    VALUES (%s, 'feeds_forward', %s, %s, %s, TRUE)
                    """,
                    (edge_id, prev_pi, pi_id, BASELINE_SCENARIO_ID),
                )
            edge_count += 1
            prev_pi = pi_id

    conn.commit()
    return {"items": items, "buckets": buckets, "pi_nodes": pi_count, "edges": edge_count}


def _fork(conn: psycopg.Connection) -> dict:
    from ootils_core.engine.scenario.manager import ScenarioManager

    mgr = ScenarioManager()

    # Count nodes/edges on baseline
    base_nodes = conn.execute(
        "SELECT COUNT(*) AS n FROM nodes WHERE scenario_id = %s AND active = TRUE",
        (BASELINE_SCENARIO_ID,),
    ).fetchone()["n"]
    base_edges = conn.execute(
        "SELECT COUNT(*) AS n FROM edges WHERE scenario_id = %s AND active = TRUE",
        (BASELINE_SCENARIO_ID,),
    ).fetchone()["n"]

    # Query counter
    counter = {"n": 0}
    real_execute = conn.execute

    def counting(query, params=None, **kwargs):
        counter["n"] += 1
        return real_execute(query, params, **kwargs) if params is not None else real_execute(query, **kwargs)

    conn.execute = counting  # type: ignore[method-assign]
    try:
        start = time.perf_counter()
        scenario = mgr.create_scenario(
            name=f"bench-fork-{uuid4().hex[:6]}",
            parent_scenario_id=BASELINE_SCENARIO_ID,
            db=conn,
        )
        conn.commit()
        elapsed = time.perf_counter() - start
    finally:
        conn.execute = real_execute  # type: ignore[method-assign]

    child_nodes = conn.execute(
        "SELECT COUNT(*) AS n FROM nodes WHERE scenario_id = %s AND active = TRUE",
        (scenario.scenario_id,),
    ).fetchone()["n"]
    child_edges = conn.execute(
        "SELECT COUNT(*) AS n FROM edges WHERE scenario_id = %s AND active = TRUE",
        (scenario.scenario_id,),
    ).fetchone()["n"]

    return {
        "wall_seconds": round(elapsed, 3),
        "queries_total": counter["n"],
        "baseline_nodes": base_nodes,
        "baseline_edges": base_edges,
        "child_nodes": child_nodes,
        "child_edges": child_edges,
        "queries_per_node": round(counter["n"] / max(1, base_nodes + base_edges), 2),
        "rows_per_second": round((base_nodes + base_edges) / max(elapsed, 1e-9), 1),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--items", type=int, default=20)
    parser.add_argument("--buckets", type=int, default=30)
    parser.add_argument("--dbname", default="ootils_test_fork")
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
        from ootils_core.db.connection import OotilsDB
        OotilsDB(target_dsn)

    with psycopg.connect(target_dsn, row_factory=dict_row) as conn:
        if not args.skip_setup:
            seed = _seed(conn, items=args.items, buckets=args.buckets)
            print(f"[seed] {seed}")
        result = _fork(conn)

    print()
    print("=" * 60)
    print("FORK BENCHMARK")
    print("=" * 60)
    for k, v in result.items():
        print(f"  {k:24s}  {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
