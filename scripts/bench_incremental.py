"""
bench_incremental.py — Measure incremental propagation latency.

For each of N randomly-picked PurchaseOrderSupply nodes:
  1. INSERT an event of type `supply_date_changed` on that node
  2. Call engine.process_event(event_id, scenario_id, db)
  3. Wall-clock the propagation

Reports p50 / p95 / max / mean and the dirty-subgraph size distribution.

This is the real-UX bench — full propagation is a stress test, this is
what an interactive event actually costs.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@127.0.0.1:15432/ootils_bench_m \\
    python scripts/bench_incremental.py --n 20 --engine sql
"""
from __future__ import annotations

import argparse
import os
import random
import statistics
import sys
import time
from datetime import date, timedelta
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

BASELINE = UUID("00000000-0000-0000-0000-000000000001")


def _build_engine(conn, flavor: str):
    os.environ["OOTILS_ENGINE"] = flavor
    # purge cached imports so the env var is re-read
    for m in list(sys.modules):
        if m.startswith("ootils_core."):
            del sys.modules[m]

    from ootils_core.api.routers.events import _build_propagation_engine
    return _build_propagation_engine(conn)


def _pick_trigger_nodes(conn, n: int) -> list[UUID]:
    """Pick ForecastDemand triggers that DO have matching PI buckets.

    The seed has a known bug where PO/WO supplies have no item+location
    overlap with PI (cf chip "Fix seed: PO/WO orphans from PI"). We use
    Demand triggers because they have ~91% overlap.
    """
    rows = conn.execute(
        """
        SELECT n.node_id FROM nodes n
        WHERE n.node_type = 'ForecastDemand'
          AND n.active = TRUE
          AND n.scenario_id = %s
          AND EXISTS (
            SELECT 1 FROM nodes p
            WHERE p.node_type = 'ProjectedInventory'
              AND p.scenario_id = n.scenario_id
              AND p.item_id = n.item_id
              AND p.location_id = n.location_id
              AND p.active = TRUE
              AND p.time_span_start IS NOT NULL
          )
        ORDER BY random() LIMIT %s
        """,
        (BASELINE, n),
    ).fetchall()
    return [UUID(str(r["node_id"])) for r in rows]


def _insert_event(conn, trigger_node_id: UUID, today: date) -> UUID:
    """Insert a `demand_qty_changed` event on a ForecastDemand trigger."""
    event_id = uuid4()
    old_qty = 100
    new_qty = old_qty + random.randint(10, 200)
    # Include dates to widen the dirty window (engine uses old/new dates).
    new_date = today + timedelta(days=random.randint(7, 30))
    conn.execute(
        """
        INSERT INTO events (
            event_id, event_type, scenario_id, trigger_node_id,
            field_changed, old_quantity, new_quantity, old_date, new_date,
            processed, source, created_at
        ) VALUES (%s, 'demand_qty_changed', %s, %s, 'quantity', %s, %s, %s, %s, FALSE, 'engine', now())
        """,
        (event_id, BASELINE, trigger_node_id, old_qty, new_qty, today, new_date),
    )
    conn.commit()
    return event_id


def _measure_one(engine, conn, event_id: UUID) -> tuple[float, int, int]:
    t0 = time.perf_counter()
    calc_run = engine.process_event(
        event_id=event_id, scenario_id=BASELINE, db=conn,
    )
    elapsed = (time.perf_counter() - t0) * 1000  # ms
    n_dirty = (calc_run.dirty_node_count if calc_run else 0) or 0
    n_recomputed = (calc_run.nodes_recalculated if calc_run else 0) or 0
    return elapsed, n_dirty, n_recomputed


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--n", type=int, default=20, help="Number of measured events")
    p.add_argument("--warmup", type=int, default=5, help="Warmup events (discarded)")
    p.add_argument("--engine", choices=["sql", "python"], default="sql")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    if not args.dsn:
        print("ERROR: set DATABASE_URL or pass --dsn", file=sys.stderr)
        sys.exit(1)

    random.seed(args.seed)

    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        # Re-build engine after env var is set
        engine = _build_engine(conn, args.engine)
        # Pick warmup + measured triggers (distinct nodes)
        trigger_nodes = _pick_trigger_nodes(conn, args.n + args.warmup)
        warmup_nodes = trigger_nodes[: args.warmup]
        measured_nodes = trigger_nodes[args.warmup :]
        print(f"Engine: {args.engine}  |  warmup: {len(warmup_nodes)}  |  measured: {len(measured_nodes)}")
        print(f"DB: {args.dsn.split('@')[-1]}")
        print()

        today = date.today()

        # ----- Warmup -----
        if warmup_nodes:
            print("--- warmup (discarded) ---")
            for i, node_id in enumerate(warmup_nodes, 1):
                event_id = _insert_event(conn, node_id, today)
                ms, nd, nr = _measure_one(engine, conn, event_id)
                print(f"  [w{i}] elapsed={ms:7.1f} ms  dirty={nd:>4d}  recomputed={nr:>4d}")
            print()

        # ----- Measured -----
        latencies_ms: list[float] = []
        dirty_counts: list[int] = []
        recomputed_counts: list[int] = []
        failures = 0

        print("--- measured ---")
        for i, node_id in enumerate(measured_nodes, 1):
            try:
                event_id = _insert_event(conn, node_id, today)
                elapsed_ms, n_dirty, n_rec = _measure_one(engine, conn, event_id)
                latencies_ms.append(elapsed_ms)
                dirty_counts.append(n_dirty)
                recomputed_counts.append(n_rec)
                print(f"  [{i:3d}/{len(measured_nodes)}] node={str(node_id)[:8]}  "
                      f"elapsed={elapsed_ms:8.1f} ms   dirty={n_dirty:>5d}   recomputed={n_rec:>5d}")
            except Exception as exc:
                failures += 1
                print(f"  [{i:3d}/{len(measured_nodes)}] node={str(node_id)[:8]}  FAIL: {exc}")
                conn.rollback()

        if not latencies_ms:
            print("\nAll events failed.")
            sys.exit(2)

        # Stats
        srt = sorted(latencies_ms)
        p50 = statistics.median(srt)
        p95 = srt[int(0.95 * len(srt))] if len(srt) >= 20 else srt[-1]
        p99 = srt[int(0.99 * len(srt))] if len(srt) >= 100 else srt[-1]
        mean = statistics.mean(srt)
        mx = max(srt)
        mn = min(srt)

        print()
        print("=== Latency (ms) ===")
        print(f"  min  : {mn:8.1f}")
        print(f"  p50  : {p50:8.1f}")
        print(f"  mean : {mean:8.1f}")
        print(f"  p95  : {p95:8.1f}")
        print(f"  p99  : {p99:8.1f}")
        print(f"  max  : {mx:8.1f}")
        print(f"  failures: {failures}/{args.n}")

        dsrt = sorted(dirty_counts)
        rsrt = sorted(recomputed_counts)
        print()
        print("=== Dirty subgraph size (unambiguous: PIs marked dirty + processed) ===")
        print(f"  p50  : {statistics.median(dsrt):8.0f}")
        print(f"  p95  : {dsrt[int(0.95*len(dsrt))] if len(dsrt) >= 20 else dsrt[-1]:8.0f}")
        print(f"  max  : {max(dsrt):8.0f}")
        print()
        print("=== nodes_recalculated (BIASED: 'changed' on Python, 'rows updated' on SQL) ===")
        print(f"  p50  : {statistics.median(rsrt):8.0f}")
        print(f"  p95  : {rsrt[int(0.95*len(rsrt))] if len(rsrt) >= 20 else rsrt[-1]:8.0f}")
        print(f"  max  : {max(rsrt):8.0f}")
        print("  (cf models/__init__.py CalcRun docstring for the divergence)")

        # Throughput on dirty_node_count — comparable across engines
        total_time_s = sum(latencies_ms) / 1000.0
        total_dirty = sum(dirty_counts)
        if total_time_s > 0:
            tput = total_dirty / total_time_s
            print()
            print(f"=== Throughput (dirty-based, cross-engine comparable) ===")
            print(f"    {tput:.0f} node/s   (total {total_dirty} dirty PIs in {total_time_s:.2f}s)")


if __name__ == "__main__":
    main()
