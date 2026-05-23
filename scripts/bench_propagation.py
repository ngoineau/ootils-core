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

    Uses bulk INSERT…SELECT against `UNNEST` arrays so the seed itself
    does not become the bottleneck at large scale. Without this, seeding
    100×365 (~73K rows) took ~280s; with bulk it takes <10s.

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

    # 1. Items — single bulk INSERT
    item_ids: list[UUID] = [uuid4() for _ in range(items)]
    item_names: list[str] = [f"BENCH-ITEM-{i:05d}" for i in range(items)]
    conn.execute(
        "INSERT INTO items (item_id, name) SELECT * FROM UNNEST(%s::uuid[], %s::text[])",
        (item_ids, item_names),
    )

    # 2. projection_series — one per item, bulk
    series_ids: list[UUID] = [uuid4() for _ in range(items)]
    conn.execute(
        """
        INSERT INTO projection_series
            (series_id, item_id, location_id, scenario_id, horizon_start, horizon_end)
        SELECT *
        FROM UNNEST(
            %s::uuid[], %s::uuid[],
            ARRAY_FILL(%s::uuid, ARRAY[%s]),
            ARRAY_FILL(%s::uuid, ARRAY[%s]),
            ARRAY_FILL(%s::date, ARRAY[%s]),
            ARRAY_FILL(%s::date, ARRAY[%s])
        )
        """,
        (
            series_ids, item_ids,
            location_id, items,
            BASELINE_SCENARIO_ID, items,
            horizon_start, items,
            horizon_end, items,
        ),
    )

    # 3. OnHandSupply nodes — one per item, bulk
    oh_ids: list[UUID] = [uuid4() for _ in range(items)]
    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             quantity, qty_uom, time_grain, time_ref, is_dirty, active)
        SELECT
            oh.id, 'OnHandSupply', %s, oh.item_id, %s,
            100, 'EA', 'exact_date', %s, FALSE, TRUE
        FROM UNNEST(%s::uuid[], %s::uuid[]) AS oh(id, item_id)
        """,
        (
            BASELINE_SCENARIO_ID, location_id, horizon_start,
            oh_ids, item_ids,
        ),
    )

    # 4. PI nodes — items × buckets bulk INSERT via cross join in SQL
    # We pre-generate UUIDs in Python (gen_random_uuid would also work,
    # but explicit ids let us also bulk-insert the matching edges below).
    pi_node_count = items * buckets
    pi_ids: list[UUID] = [uuid4() for _ in range(pi_node_count)]
    # Build flat arrays: row i = item_index * buckets + bucket_index
    item_id_per_pi: list[UUID] = []
    series_id_per_pi: list[UUID] = []
    bucket_start_per_pi: list = []
    bucket_end_per_pi: list = []
    bucket_seq_per_pi: list[int] = []
    for i in range(items):
        for b in range(buckets):
            item_id_per_pi.append(item_ids[i])
            series_id_per_pi.append(series_ids[i])
            bucket_start_per_pi.append(horizon_start + timedelta(days=b))
            bucket_end_per_pi.append(horizon_start + timedelta(days=b + 1))
            bucket_seq_per_pi.append(b)

    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             time_grain, time_span_start, time_span_end,
             projection_series_id, bucket_sequence,
             is_dirty, active)
        SELECT
            pi.id, 'ProjectedInventory', %s, pi.item_id, %s,
            'day', pi.bs, pi.be, pi.series_id, pi.seq,
            TRUE, TRUE
        FROM UNNEST(
            %s::uuid[], %s::uuid[], %s::date[], %s::date[],
            %s::uuid[], %s::int[]
        ) AS pi(id, item_id, bs, be, series_id, seq)
        """,
        (
            BASELINE_SCENARIO_ID, location_id,
            pi_ids, item_id_per_pi, bucket_start_per_pi, bucket_end_per_pi,
            series_id_per_pi, bucket_seq_per_pi,
        ),
    )

    # 5. Edges — 'replenishes' from OnHand to first PI bucket + 'feeds_forward'
    # between consecutive PI buckets within each series.
    edge_count = pi_node_count  # 1 'replenishes' per item + (buckets-1) 'feeds_forward' per item = items*buckets total
    edge_ids: list[UUID] = [uuid4() for _ in range(edge_count)]
    edge_types: list[str] = []
    edge_from: list[UUID] = []
    edge_to: list[UUID] = []
    for i in range(items):
        base = i * buckets
        # First edge: OnHand → first PI bucket
        edge_types.append("replenishes")
        edge_from.append(oh_ids[i])
        edge_to.append(pi_ids[base])
        # Next: feeds_forward between consecutive PI buckets
        for b in range(1, buckets):
            edge_types.append("feeds_forward")
            edge_from.append(pi_ids[base + b - 1])
            edge_to.append(pi_ids[base + b])

    conn.execute(
        """
        INSERT INTO edges
            (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
        SELECT
            e.id, e.type, e.frm, e.dest, %s, TRUE
        FROM UNNEST(%s::uuid[], %s::text[], %s::uuid[], %s::uuid[]) AS e(id, type, frm, dest)
        """,
        (
            BASELINE_SCENARIO_ID,
            edge_ids, edge_types, edge_from, edge_to,
        ),
    )

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
