"""
scripts/bench_propagation.py — Measure propagation throughput.

Seeds a synthetic scenario with N items × M time buckets and runs a full
incremental propagation, reporting wall time, query count, and throughput.

Used as the before/after harness for REVIEW-2026-05 R2 (batch propagation).

Usage:
    DATABASE_URL=postgresql://ootils:ootils@127.0.0.1:15432/ootils_test_bench \\
        OOTILS_API_TOKEN=bench \\
        python scripts/bench_propagation.py --items 50 --buckets 30

Defaults to a tiny smoke scenario (10 items × 14 buckets) so the script is
safe to run on the dev tunnel without burning minutes.

WARNING: the script DROPs and recreates the target database. Never point it
at a DB you care about. By default it picks `ootils_test_bench` to make
that explicit.
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _admin_recreate_db(dsn: str, dbname: str) -> None:
    """Drop and recreate `dbname` via the postgres DB. Caller must have privileges."""
    base = dsn.rsplit("/", 1)[0]
    admin_dsn = f"{base}/postgres"
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.execute(f'CREATE DATABASE "{dbname}" OWNER ootils')
    print(f"[setup] recreated database {dbname}")


def _apply_migrations(dsn: str) -> None:
    from ootils_core.db.connection import OotilsDB

    OotilsDB(dsn)
    print("[setup] migrations applied")


def _seed(conn: psycopg.Connection, items: int, buckets: int) -> dict:
    """Insert items, locations, PI nodes, on-hand supplies, and feeds_forward edges.

    Returns a summary dict (counts).
    """
    started = time.perf_counter()
    location_id = uuid4()
    today = date.today()
    horizon_start = today
    horizon_end = today + timedelta(days=buckets)

    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, 'BENCH-LOC')",
        (location_id,),
    )

    item_ids: list[UUID] = []
    for i in range(items):
        item_id = uuid4()
        item_ids.append(item_id)
        conn.execute(
            "INSERT INTO items (item_id, name) VALUES (%s, %s)",
            (item_id, f"BENCH-ITEM-{i:05d}"),
        )

    # One projection_series per item
    pi_node_count = 0
    edge_count = 0
    for item_id in item_ids:
        series_id = uuid4()
        conn.execute(
            """
            INSERT INTO projection_series
                (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (series_id, item_id, location_id, BASELINE_SCENARIO_ID, horizon_start, horizon_end),
        )

        # On-hand supply at horizon_start (provides opening_stock for first bucket)
        oh_id = uuid4()
        conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, scenario_id, item_id, location_id,
                 quantity, qty_uom, time_grain, time_ref, is_dirty, active)
            VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, 'EA', 'exact_date', %s, FALSE, TRUE)
            """,
            (oh_id, BASELINE_SCENARIO_ID, item_id, location_id, Decimal("100"), horizon_start),
        )

        # PI buckets — daily grain
        prev_pi_id: UUID | None = None
        for b in range(buckets):
            pi_id = uuid4()
            bucket_start = horizon_start + timedelta(days=b)
            bucket_end = bucket_start + timedelta(days=1)
            conn.execute(
                """
                INSERT INTO nodes
                    (node_id, node_type, scenario_id, item_id, location_id,
                     time_grain, time_span_start, time_span_end,
                     projection_series_id, bucket_sequence,
                     is_dirty, active)
                VALUES (%s, 'ProjectedInventory', %s, %s, %s,
                        'day', %s, %s, %s, %s, TRUE, TRUE)
                """,
                (pi_id, BASELINE_SCENARIO_ID, item_id, location_id,
                 bucket_start, bucket_end, series_id, b),
            )
            pi_node_count += 1

            # feeds_forward edge from previous bucket
            if prev_pi_id is not None:
                edge_id = uuid4()
                conn.execute(
                    """
                    INSERT INTO edges
                        (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
                    VALUES (%s, 'feeds_forward', %s, %s, %s, TRUE)
                    """,
                    (edge_id, prev_pi_id, pi_id, BASELINE_SCENARIO_ID),
                )
                edge_count += 1
            else:
                # On-hand replenishes the first PI bucket
                edge_id = uuid4()
                conn.execute(
                    """
                    INSERT INTO edges
                        (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
                    VALUES (%s, 'replenishes', %s, %s, %s, TRUE)
                    """,
                    (edge_id, oh_id, pi_id, BASELINE_SCENARIO_ID),
                )
                edge_count += 1

            prev_pi_id = pi_id

    conn.commit()
    elapsed = time.perf_counter() - started
    return {
        "items": items,
        "buckets_per_item": buckets,
        "pi_nodes_total": pi_node_count,
        "edges_total": edge_count,
        "seed_seconds": round(elapsed, 2),
    }


def _mark_all_pi_dirty(conn: psycopg.Connection) -> tuple[UUID, set[UUID]]:
    """Create a calc_run and persist a dirty_nodes row for every PI node."""
    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
    from ootils_core.engine.orchestration.calc_run import CalcRunManager

    pi_rows = conn.execute(
        """
        SELECT node_id FROM nodes
        WHERE scenario_id = %s AND node_type = 'ProjectedInventory' AND active = TRUE
        """,
        (BASELINE_SCENARIO_ID,),
    ).fetchall()
    pi_ids = {UUID(str(r["node_id"])) for r in pi_rows}

    calc_mgr = CalcRunManager()
    calc_run = calc_mgr.start_calc_run(
        scenario_id=BASELINE_SCENARIO_ID, event_ids=[], db=conn
    )
    assert calc_run is not None, "could not acquire advisory lock (something else running?)"

    dirty = DirtyFlagManager()
    dirty.mark_dirty(pi_ids, BASELINE_SCENARIO_ID, calc_run.calc_run_id, conn)
    dirty.flush_to_postgres(calc_run.calc_run_id, BASELINE_SCENARIO_ID, conn)
    conn.commit()
    return calc_run.calc_run_id, pi_ids


def _run_propagation(conn: psycopg.Connection, calc_run_id: UUID, dirty: set[UUID]) -> dict:
    """Drive a full _propagate() over the given dirty set; time + count queries."""
    from ootils_core.api.routers.events import _build_propagation_engine
    from ootils_core.models import CalcRun

    engine = _build_propagation_engine(conn)

    # Reload the CalcRun so the manager state is populated
    row = conn.execute(
        "SELECT * FROM calc_runs WHERE calc_run_id = %s",
        (calc_run_id,),
    ).fetchone()
    calc_run = CalcRun(
        calc_run_id=UUID(str(row["calc_run_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        triggered_by_event_ids=[UUID(str(e)) for e in (row.get("triggered_by_event_ids") or [])],
        is_full_recompute=bool(row.get("is_full_recompute", False)),
        dirty_node_count=row.get("dirty_node_count"),
        nodes_recalculated=int(row.get("nodes_recalculated", 0)),
        nodes_unchanged=int(row.get("nodes_unchanged", 0)),
        status=row.get("status", "running"),
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        error_message=row.get("error_message"),
    )

    # Count queries via a custom subclass of psycopg.Connection.execute
    query_count = {"n": 0}
    real_execute = conn.execute

    def counting_execute(query, params=None, **kwargs):
        query_count["n"] += 1
        return real_execute(query, params, **kwargs) if params is not None else real_execute(query, **kwargs)

    conn.execute = counting_execute  # type: ignore[method-assign]
    try:
        wall_start = time.perf_counter()
        engine._propagate(calc_run, dirty, conn)
        wall = time.perf_counter() - wall_start
    finally:
        conn.execute = real_execute  # type: ignore[method-assign]

    return {
        "dirty_nodes": len(dirty),
        "wall_seconds": round(wall, 3),
        "queries_total": query_count["n"],
        "queries_per_node": round(query_count["n"] / max(1, len(dirty)), 2),
        "nodes_per_second": round(len(dirty) / max(wall, 1e-9), 1),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--items", type=int, default=10, help="Items to seed (default: 10)")
    parser.add_argument("--buckets", type=int, default=14, help="PI buckets per item (default: 14)")
    parser.add_argument("--dbname", default="ootils_test_bench",
                        help="Database to recreate (default: ootils_test_bench)")
    parser.add_argument("--skip-setup", action="store_true",
                        help="Skip DB recreate + migrations + seed (reuse existing state).")
    args = parser.parse_args()

    base_dsn = os.environ.get("DATABASE_URL")
    if not base_dsn:
        print("FATAL: set DATABASE_URL (e.g. postgresql://ootils:ootils@127.0.0.1:15432/ootils_test_bench)")
        return 2

    # Rebuild DSN to target the bench DB explicitly — defensive against accidental ootils_dev wipe.
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
        result = _run_propagation(conn, calc_run_id, dirty)

    print()
    print("=" * 60)
    print("BENCHMARK RESULT")
    print("=" * 60)
    for k, v in result.items():
        print(f"  {k:24s}  {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
