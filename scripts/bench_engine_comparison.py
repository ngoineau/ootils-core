"""
bench_engine_comparison.py — compare OOTILS_ENGINE=python vs sql.

Uses a DB already seeded by scripts/seed_realistic_dataset.py. Triggers
a full propagation with each engine and reports timing.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_bench_s \\
    python scripts/bench_engine_comparison.py
"""
from __future__ import annotations

import argparse
import importlib
import os
import sys
import time
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


def _stats(conn) -> dict:
    return {
        "items": conn.execute("SELECT COUNT(*) AS cnt FROM items").fetchone()["cnt"],
        "locations": conn.execute("SELECT COUNT(*) AS cnt FROM locations").fetchone()["cnt"],
        "pi_nodes_active": conn.execute(
            "SELECT COUNT(*) AS cnt FROM nodes "
            "WHERE active = TRUE AND node_type = 'ProjectedInventory' AND scenario_id = %s",
            (BASELINE_SCENARIO_ID,),
        ).fetchone()["cnt"],
        "edges_active": conn.execute(
            "SELECT COUNT(*) AS cnt FROM edges WHERE active = TRUE AND scenario_id = %s",
            (BASELINE_SCENARIO_ID,),
        ).fetchone()["cnt"],
    }


def _reload_engine_modules():
    """Drop cached imports so OOTILS_ENGINE re-reads at next import."""
    for mod in list(sys.modules.keys()):
        if mod.startswith("ootils_core.engine"):
            del sys.modules[mod]


def _build_engine(conn, engine_flavor: str):
    os.environ["OOTILS_ENGINE"] = engine_flavor
    _reload_engine_modules()

    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
    from ootils_core.engine.kernel.graph.store import GraphStore
    from ootils_core.engine.kernel.graph.traversal import GraphTraversal
    from ootils_core.engine.kernel.calc.projection import ProjectionKernel
    from ootils_core.engine.kernel.shortage.detector import ShortageDetector
    from ootils_core.engine.orchestration.calc_run import CalcRunManager
    from ootils_core.engine.orchestration.propagator import PropagationEngine

    store = GraphStore(conn)
    traversal = GraphTraversal(store)
    dirty = DirtyFlagManager()
    calc_run_mgr = CalcRunManager()
    kernel = ProjectionKernel()
    shortage_detector = ShortageDetector()

    if engine_flavor == "sql":
        from ootils_core.engine.orchestration.propagator_sql import SqlPropagationEngine
        engine_cls = SqlPropagationEngine
    else:
        engine_cls = PropagationEngine

    return engine_cls(
        store=store, traversal=traversal, dirty=dirty,
        calc_run_mgr=calc_run_mgr, kernel=kernel,
        shortage_detector=shortage_detector,
    ), dirty, calc_run_mgr


def _run_full_propagation(dsn: str, engine_flavor: str) -> dict:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        engine, dirty, calc_run_mgr = _build_engine(conn, engine_flavor)

        # Fetch all active PI nodes for baseline scenario
        rows = conn.execute(
            "SELECT node_id FROM nodes "
            "WHERE scenario_id = %s AND node_type = 'ProjectedInventory' AND active = TRUE",
            (BASELINE_SCENARIO_ID,),
        ).fetchall()
        pi_ids = {UUID(str(r["node_id"])) for r in rows}

        # Synthetic event + calc_run
        event_id = uuid4()
        conn.execute(
            "INSERT INTO events (event_id, event_type, scenario_id, processed, source, created_at) "
            "VALUES (%s, 'calc_triggered', %s, FALSE, 'engine', now())",
            (event_id, BASELINE_SCENARIO_ID),
        )
        conn.commit()

        calc_run = calc_run_mgr.start_calc_run(
            scenario_id=BASELINE_SCENARIO_ID,
            event_ids=[event_id],
            db=conn,
        )
        if calc_run is None:
            return {"engine": engine_flavor, "error": "calc_run start failed (scenario locked?)"}

        # Mark all PIs dirty (force full propagation)
        dirty.mark_dirty(pi_ids, BASELINE_SCENARIO_ID, calc_run.calc_run_id, conn)
        dirty.flush_to_postgres(calc_run.calc_run_id, BASELINE_SCENARIO_ID, conn)
        conn.commit()

        # Time the propagation
        t0 = time.perf_counter()
        engine._propagate(calc_run, pi_ids, conn)
        engine._finish_run(calc_run, BASELINE_SCENARIO_ID, conn)
        elapsed = time.perf_counter() - t0
        conn.commit()

        nodes_recomputed = calc_run.nodes_recalculated or 0
        throughput = nodes_recomputed / elapsed if elapsed > 0 else 0

        # Mark event processed so the next run can take the lock
        conn.execute("UPDATE events SET processed = TRUE WHERE event_id = %s", (event_id,))
        conn.commit()

        return {
            "engine": engine_flavor,
            "pi_count_dirty": len(pi_ids),
            "pi_count_recomputed": nodes_recomputed,
            "elapsed_sec": round(elapsed, 3),
            "throughput_per_sec": round(throughput, 1),
        }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    args = p.parse_args()
    if not args.dsn:
        print("ERROR: set DATABASE_URL or pass --dsn", file=sys.stderr)
        sys.exit(1)

    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        stats = _stats(conn)
        print("=== DB stats ===")
        for k, v in stats.items():
            print(f"  {k:30s} {v:>10d}")
        print()

    print("=== Engine: python ===")
    py = _run_full_propagation(args.dsn, "python")
    for k, v in py.items():
        print(f"  {k:30s} {v!r}")
    print()

    print("=== Engine: sql ===")
    sql = _run_full_propagation(args.dsn, "sql")
    for k, v in sql.items():
        print(f"  {k:30s} {v!r}")
    print()

    if "error" not in py and "error" not in sql:
        speedup = py["elapsed_sec"] / sql["elapsed_sec"] if sql["elapsed_sec"] > 0 else 0
        print(f"=== Speedup sql vs python : {speedup:.2f}x  ===")


if __name__ == "__main__":
    main()
