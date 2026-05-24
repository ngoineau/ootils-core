"""
parity_3way.py — Validate byte-identical compute across Python / SQL / Rust
engines on the same realistic dataset.

Setup:
    1. Pick a target DB (DATABASE_URL or --dsn). Must be a profile-S/M/L
       DB seeded with the realistic dataset generator (NOT prod).
    2. Mark ALL active PIs dirty (one synthetic calc_run).
    3. Run each engine's `_propagate` / `project_subgraph` over the same
       dirty set.
    4. Compare results row by row.

The criterion is **0 mismatch**. The script exits non-zero on the first
divergence found.

This is the ADR-016 §week 3 go/no-go gate.

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_bench_l \\
        python scripts/parity_3way.py
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

import ootils_kernel
from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.engine.kernel.graph.traversal import GraphTraversal
from ootils_core.engine.kernel.calc.projection import ProjectionKernel
from ootils_core.engine.kernel.shortage.detector import ShortageDetector
from ootils_core.engine.orchestration.calc_run import CalcRunManager
from ootils_core.engine.orchestration.propagator import PropagationEngine
from ootils_core.engine.orchestration.propagator_sql import SqlPropagationEngine

BASELINE = UUID("00000000-0000-0000-0000-000000000001")
# Decimal tolerance for cross-engine comparison. Both Python Decimal and
# Postgres numeric should give bit-identical results in theory; in
# practice the SQL-side numeric(50,28) cast can introduce a last-digit
# diff vs. Python's default 28-digit context. We allow 1e-18 — far
# tighter than any business value — to keep the test honest.
TOLERANCE = Decimal("1e-18")


def _setup_calc_run(conn) -> tuple[UUID, set[UUID]]:
    """Insert a synthetic event + calc_run, mark all active PIs dirty."""
    event_id = uuid4()
    conn.execute(
        "INSERT INTO events (event_id, event_type, scenario_id, processed, source) "
        "VALUES (%s, 'calc_triggered', %s, FALSE, 'engine')",
        (event_id, BASELINE),
    )
    cm = CalcRunManager()
    cr = cm.start_calc_run(scenario_id=BASELINE, event_ids=[event_id], db=conn)
    if cr is None:
        raise SystemExit("Could not acquire scenario lock — is another run in progress?")

    pi_ids: set[UUID] = {
        UUID(str(r["node_id"]))
        for r in conn.execute(
            "SELECT node_id FROM nodes "
            "WHERE node_type='ProjectedInventory' AND scenario_id=%s AND active=TRUE",
            (BASELINE,),
        ).fetchall()
    }
    d = DirtyFlagManager()
    d.mark_dirty(pi_ids, BASELINE, cr.calc_run_id, conn)
    d.flush_to_postgres(cr.calc_run_id, BASELINE, conn)
    conn.commit()
    return cr.calc_run_id, pi_ids


def _python_results(conn, calc_run_id: UUID, pi_ids: set[UUID]) -> dict[UUID, dict]:
    """Run Python engine in-place and read back results from DB."""
    store = GraphStore(conn)
    engine = PropagationEngine(
        store=store,
        traversal=GraphTraversal(store),
        dirty=DirtyFlagManager(),
        calc_run_mgr=CalcRunManager(),
        kernel=ProjectionKernel(),
        shortage_detector=ShortageDetector(),
    )
    # We bypass process_event since our event is already inserted; call
    # _propagate directly on the dirty set we already have.
    from ootils_core.models import CalcRun
    # Reconstitute the CalcRun in memory from DB
    row = conn.execute("SELECT * FROM calc_runs WHERE calc_run_id=%s", (calc_run_id,)).fetchone()
    cr = CalcRun(
        calc_run_id=UUID(str(row["calc_run_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        triggered_by_event_ids=[UUID(str(e)) for e in (row.get("triggered_by_event_ids") or [])],
    )
    t0 = time.perf_counter()
    engine._propagate(cr, pi_ids, conn)
    elapsed = time.perf_counter() - t0
    conn.commit()
    # Read back
    rows = conn.execute(
        "SELECT node_id, opening_stock, inflows, outflows, closing_stock, "
        "has_shortage, shortage_qty FROM nodes "
        "WHERE node_id = ANY(%s)",
        (list(pi_ids),),
    ).fetchall()
    out = {
        UUID(str(r["node_id"])): {
            "opening_stock": Decimal(str(r["opening_stock"])),
            "inflows": Decimal(str(r["inflows"])),
            "outflows": Decimal(str(r["outflows"])),
            "closing_stock": Decimal(str(r["closing_stock"])),
            "has_shortage": bool(r["has_shortage"]),
            "shortage_qty": Decimal(str(r["shortage_qty"])),
        }
        for r in rows
    }
    return out, elapsed


def _sql_results(conn, calc_run_id: UUID, pi_ids: set[UUID]) -> dict[UUID, dict]:
    """Run SQL engine in-place and read back results."""
    # Reset dirty flags first (Python engine consumed them)
    d = DirtyFlagManager()
    d.mark_dirty(pi_ids, BASELINE, calc_run_id, conn)
    d.flush_to_postgres(calc_run_id, BASELINE, conn)
    conn.commit()

    store = GraphStore(conn)
    engine = SqlPropagationEngine(
        store=store,
        traversal=GraphTraversal(store),
        dirty=DirtyFlagManager(),
        calc_run_mgr=CalcRunManager(),
        kernel=ProjectionKernel(),
        shortage_detector=ShortageDetector(),
    )
    from ootils_core.models import CalcRun
    row = conn.execute("SELECT * FROM calc_runs WHERE calc_run_id=%s", (calc_run_id,)).fetchone()
    cr = CalcRun(
        calc_run_id=UUID(str(row["calc_run_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        triggered_by_event_ids=[UUID(str(e)) for e in (row.get("triggered_by_event_ids") or [])],
    )
    t0 = time.perf_counter()
    engine._propagate(cr, pi_ids, conn)
    elapsed = time.perf_counter() - t0
    conn.commit()
    rows = conn.execute(
        "SELECT node_id, opening_stock, inflows, outflows, closing_stock, "
        "has_shortage, shortage_qty FROM nodes WHERE node_id = ANY(%s)",
        (list(pi_ids),),
    ).fetchall()
    return {
        UUID(str(r["node_id"])): {
            "opening_stock": Decimal(str(r["opening_stock"])),
            "inflows": Decimal(str(r["inflows"])),
            "outflows": Decimal(str(r["outflows"])),
            "closing_stock": Decimal(str(r["closing_stock"])),
            "has_shortage": bool(r["has_shortage"]),
            "shortage_qty": Decimal(str(r["shortage_qty"])),
        }
        for r in rows
    }, elapsed


def _rust_results(dsn: str, calc_run_id: UUID) -> dict[UUID, dict]:
    """Call the Rust engine via PyO3. Doesn't touch the DB writes."""
    # Mark dirty again so the Rust loader can see them.
    with psycopg.connect(dsn, row_factory=dict_row) as c:
        d = DirtyFlagManager()
        # Pull active PIs again (idempotent re-marking)
        pi_ids = {
            UUID(str(r["node_id"]))
            for r in c.execute(
                "SELECT node_id FROM nodes WHERE node_type='ProjectedInventory' "
                "AND scenario_id=%s AND active=TRUE",
                (BASELINE,),
            ).fetchall()
        }
        d.mark_dirty(pi_ids, BASELINE, calc_run_id, c)
        d.flush_to_postgres(calc_run_id, BASELINE, c)
        c.commit()

    t0 = time.perf_counter()
    results, stats = ootils_kernel.project_subgraph(dsn, str(calc_run_id), str(BASELINE))
    elapsed = time.perf_counter() - t0
    out = {
        UUID(r["node_id"]): {
            "opening_stock": Decimal(r["opening_stock"]),
            "inflows": Decimal(r["inflows"]),
            "outflows": Decimal(r["outflows"]),
            "closing_stock": Decimal(r["closing_stock"]),
            "has_shortage": bool(r["has_shortage"]),
            "shortage_qty": Decimal(r["shortage_qty"]),
        }
        for r in results
    }
    return out, elapsed, stats


def _compare(ref: dict[UUID, dict], other: dict[UUID, dict], name: str) -> int:
    """Compare `other` against `ref`. Returns number of mismatches."""
    n_mismatch = 0
    first_5_mismatches: list[str] = []
    missing = set(ref) - set(other)
    extra = set(other) - set(ref)
    if missing:
        n_mismatch += len(missing)
        first_5_mismatches.append(f"  {name} missing {len(missing)} PIs")
    if extra:
        n_mismatch += len(extra)
        first_5_mismatches.append(f"  {name} has {len(extra)} extra PIs")

    common = set(ref) & set(other)
    for k in common:
        r, o = ref[k], other[k]
        for field in ("opening_stock", "inflows", "outflows", "closing_stock"):
            diff = abs(r[field] - o[field])
            if diff > TOLERANCE:
                n_mismatch += 1
                if len(first_5_mismatches) < 5:
                    first_5_mismatches.append(
                        f"  {name} mismatch on {k} {field}: ref={r[field]} other={o[field]} diff={diff}"
                    )
        if r["has_shortage"] != o["has_shortage"]:
            n_mismatch += 1
            if len(first_5_mismatches) < 5:
                first_5_mismatches.append(
                    f"  {name} shortage flag differs on {k}: ref={r['has_shortage']} other={o['has_shortage']}"
                )
        diff = abs(r["shortage_qty"] - o["shortage_qty"])
        if diff > TOLERANCE:
            n_mismatch += 1
            if len(first_5_mismatches) < 5:
                first_5_mismatches.append(
                    f"  {name} shortage_qty mismatch on {k}: ref={r['shortage_qty']} other={o['shortage_qty']}"
                )

    print(f"  {name} vs reference: {n_mismatch} mismatches over {len(common)} common PIs")
    for m in first_5_mismatches:
        print(m)
    return n_mismatch


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument(
        "--skip-python",
        action="store_true",
        help="Skip the Python engine (saturates on profile L 227K PIs). Falls "
        "back to SQL vs Rust only — sufficient since Python≡SQL is already "
        "validated on S and M.",
    )
    args = p.parse_args()
    if not args.dsn:
        print("ERROR: set DATABASE_URL or pass --dsn", file=sys.stderr)
        return 1

    print(f"3-way parity check on {args.dsn.split('@')[-1]}")
    if args.skip_python:
        print("  (Python engine skipped — comparing SQL vs Rust only)")
    print()

    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        calc_run_id, pi_ids = _setup_calc_run(conn)
        print(f"  calc_run_id = {calc_run_id}, {len(pi_ids)} PIs dirty")
        print()

        py_results = None
        py_elapsed = 0.0
        if not args.skip_python:
            print("[Python engine] running...")
            py_results, py_elapsed = _python_results(conn, calc_run_id, pi_ids)
            print(f"  {len(py_results)} PIs computed in {py_elapsed:.2f}s")

        print("[SQL engine] running...")
        sql_results, sql_elapsed = _sql_results(conn, calc_run_id, pi_ids)
        print(f"  {len(sql_results)} PIs computed in {sql_elapsed:.2f}s")

    print("[Rust engine] running (separate connection)...")
    rust_results, rust_elapsed, rust_stats = _rust_results(args.dsn, calc_run_id)
    print(f"  {len(rust_results)} PIs computed in {rust_elapsed:.2f}s")
    print(f"  stats: load_ms={rust_stats['load_ms']:.1f}, compute_ms={rust_stats['compute_ms']:.1f}, "
          f"shortages={rust_stats['n_shortages_detected']}")
    print()

    # Comparison logic:
    # - With Python: use Python as reference (canonical engine).
    # - Without Python: use SQL as reference (proven equivalent to Python on S/M).
    reference, ref_name = (
        (py_results, "Python") if py_results is not None else (sql_results, "SQL")
    )
    print(f"=== Parity comparison (reference = {ref_name}) ===")
    n_diffs = 0
    if py_results is not None:
        n_diffs += _compare(py_results, sql_results, "SQL")
        n_diffs += _compare(py_results, rust_results, "Rust")
    else:
        n_diffs += _compare(reference, rust_results, "Rust")

    print()
    print("=== Summary ===")
    if py_results is not None:
        print(f"  Python : {py_elapsed:.2f}s")
    print(f"  SQL    : {sql_elapsed:.2f}s")
    print(f"  Rust   : {rust_elapsed:.2f}s (load {rust_stats['load_ms']:.0f}ms + compute {rust_stats['compute_ms']:.0f}ms)")
    print()
    if n_diffs == 0:
        engines_compared = "3-way" if py_results is not None else "2-way (SQL/Rust)"
        print(f"PARITY {engines_compared} : OK")
        return 0
    print(f"PARITY FAILED: {n_diffs} diffs total")
    return 2


if __name__ == "__main__":
    sys.exit(main())
