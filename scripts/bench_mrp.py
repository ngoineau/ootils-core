"""
bench_mrp.py — performance harness for the MRP cascade (#301).

The MRP engine lives in `scripts/mrp_core.py` (not in the package). It is a
**read-only** planner: `load_planning_data` issues SELECTs only, and
`consume_demand` / `run_timephased` / `peg_origins` are pure in-memory Python.
This harness therefore runs safely against a loaded DB without any writes —
it opens a READ ONLY transaction as a belt-and-braces guard.

It answers the blind spot left by docs/PERF-BASELINE.md: the propagation engine
is benched, the MRP cascade never was.

Phases timed independently so we can tell DB-bound (load) from compute-bound
(cascade) cost:
    1. load_planning_data   — ~25 aggregate GROUP BY scans + a few cursors
    2. consume_demand       — forecast consumption + demand time fence (Python)
    3. run_timephased       — the LLC level-by-level explosion (Python, the core)
    4. peg_origins          — aggregate cascade with origin attribution (Python)

Usage:
    DATABASE_URL=postgresql://ootils:ootils@host:5432/ootils_pilote_test \
        python scripts/bench_mrp.py --repeats 3 --horizons 270,540,1080
"""
from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from pathlib import Path

import psycopg

# mrp_core is a script-level module; make it importable when run from anywhere.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import mrp_core as core  # noqa: E402


def _time(fn, *a, **k):
    t0 = time.perf_counter()
    out = fn(*a, **k)
    return out, time.perf_counter() - t0


def run_once(dsn: str, horizon_days: int) -> dict:
    """One full MRP pass at a given horizon. Read-only transaction."""
    phases: dict[str, float] = {}
    with psycopg.connect(dsn) as conn:
        # Belt-and-braces: the planner only reads, but make it impossible to
        # write — protects a loaded/pilote DB from any accidental mutation.
        conn.execute("SET statement_timeout = '120s'")
        conn.execute("SET default_transaction_read_only = on")
        # psycopg3: SET default_transaction_read_only only applies to
        # transactions AFTER the current one — commit so the guard is live
        # before the loader runs (same fix as bench_reconciliation).
        conn.commit()

        d, phases["load"] = _time(core.load_planning_data, conn, horizon_days)

    gross, phases["consume"] = _time(core.consume_demand, d)
    r, phases["timephased"] = _time(core.run_timephased, d, gross)
    (_dep, _origin), phases["peg"] = _time(core.peg_origins, d, gross)

    return {
        "horizon_days": horizon_days,
        "n_buckets": d.n_buckets,
        "involved_items": len(d.involved),
        "max_llc": d.max_llc,
        "items_with_demand": len(gross),
        "bom_parents": len(d.bom),
        "planned_orders": len(r["planned"]),
        "n_wo": r["n_wo"],
        "n_po": r["n_po"],
        "past_due": r["past_due"],
        "phases": phases,
        "total_compute": phases["consume"] + phases["timephased"] + phases["peg"],
        "total_wall": sum(phases.values()),
    }


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--horizons", default="540",
                   help="comma-separated horizon_days to sweep (default: 540)")
    args = p.parse_args(argv)
    if not args.dsn:
        print("ERROR: set DATABASE_URL or pass --dsn", file=sys.stderr)
        return 2

    name = args.dsn.rstrip("/").split("/")[-1].split("?")[0]
    print(f"=== MRP bench -- DB={name} repeats={args.repeats} ===\n")

    horizons = [int(h) for h in args.horizons.split(",")]
    for hd in horizons:
        runs = [run_once(args.dsn, hd) for _ in range(args.repeats)]
        meta = runs[0]
        print(f"--- horizon {hd}d  ({meta['n_buckets']} weekly buckets) ---")
        print(f"  involved_items   {meta['involved_items']:>8d}   max_llc {meta['max_llc']}")
        print(f"  items_w_demand   {meta['items_with_demand']:>8d}   bom_parents {meta['bom_parents']}")
        print(f"  planned_orders   {meta['planned_orders']:>8d}   "
              f"WO={meta['n_wo']} PO={meta['n_po']} past_due={meta['past_due']}")
        print(f"  {'phase':<14}{'p50 (s)':>10}{'min':>9}{'max':>9}")
        for ph in ("load", "consume", "timephased", "peg"):
            vals = [r["phases"][ph] for r in runs]
            print(f"  {ph:<14}{statistics.median(vals):>10.3f}"
                  f"{min(vals):>9.3f}{max(vals):>9.3f}")
        tot = [r["total_wall"] for r in runs]
        comp = [r["total_compute"] for r in runs]
        load = [r["phases"]["load"] for r in runs]
        print(f"  {'TOTAL wall':<14}{statistics.median(tot):>10.3f}"
              f"{min(tot):>9.3f}{max(tot):>9.3f}")
        db_pct = 100 * statistics.median(load) / statistics.median(tot)
        print(f"  -> DB-load {db_pct:.0f}% of wall | "
              f"compute {statistics.median(comp):.3f}s "
              f"({meta['planned_orders']/max(statistics.median(comp),1e-9):,.0f} orders/s)\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
