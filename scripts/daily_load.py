"""
daily_load.py — one command for the daily data refresh.

Loading raw TSVs is NOT enough: the planning engine depends on DERIVED state that
must be recomputed after every load — Low-Level Codes (the BOM level-by-level
order) and item standard costs (BOM roll-up). bulk_ingest re-inserts bom_lines
with llc=0, so skipping the recompute silently flattens the BOM graph and
degrades every downstream number (E&O, shortages, valuation) — observed in the
field: a re-ingest of the SAME dump inflated E&O +46% purely because LLC was left
at 0.

This chains the mandatory steps and validates the result so that silent
degradation can't happen:

    1. bulk_ingest   <dir>   (COPY+UPSERT all canonical TSVs in FK order)
    2. compute_llc           (Low-Level Codes for the BOM graph)
    3. compute_cost_rollup   (items.standard_cost via bottom-up BOM roll-up)
    4. validate              (fail loud if the BOM exists but LLC is flat, etc.)

Usage:
    DATABASE_URL=... python scripts/daily_load.py data/inbox [--allow-dev] [--skip-cost]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import psycopg
import mrp_core as core
import bulk_ingest
import compute_llc
import compute_cost_rollup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("daily_load")


def _validate(dsn: str) -> tuple[bool, list[str]]:
    """Post-load sanity. Returns (ok, messages). Fails loud on the silent-degradation
    traps (flat LLC, no on-hand, no demand) rather than letting them pass."""
    msgs, ok = [], True
    with psycopg.connect(dsn) as conn:
        cur = conn.cursor()
        bom_lines = cur.execute("SELECT COUNT(*) FROM bom_lines").fetchone()[0]
        max_llc = cur.execute("SELECT COALESCE(MAX(llc), 0) FROM bom_lines").fetchone()[0]
        items_costed = cur.execute("SELECT COUNT(*) FROM items WHERE standard_cost IS NOT NULL").fetchone()[0]
        items_total = cur.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        on_hand = cur.execute("SELECT COUNT(*) FROM nodes WHERE node_type='OnHandSupply' AND active").fetchone()[0]
        demand = cur.execute(
            "SELECT COUNT(*) FROM nodes WHERE active AND node_type=ANY(%(t)s)",
            {"t": core.DEMAND_TYPES}).fetchone()[0]

    # The exact trap we hit: BOMs present but LLC never recomputed -> flat graph.
    if bom_lines > 0 and max_llc == 0:
        ok = False
        msgs.append(f"✗ BOM graph is FLAT: {bom_lines} bom_lines but max LLC = 0 — compute_llc did not take. "
                    "Planning (E&O, shortages, MRP) will be wrong.")
    else:
        msgs.append(f"✓ LLC computed: max level {max_llc} over {bom_lines} bom_lines")
    cov = (100.0 * items_costed / items_total) if items_total else 0.0
    msgs.append(f"{'✓' if items_costed else '✗'} standard_cost set on {items_costed}/{items_total} items ({cov:.0f}%)")
    msgs.append(f"{'✓' if on_hand else '✗'} on-hand supply nodes: {on_hand}")
    msgs.append(f"{'✓' if demand else '✗'} demand nodes: {demand}")
    if not on_hand or not demand:
        ok = False
    return ok, msgs


def _step(name: str, fn) -> bool:
    logger.info("─" * 70)
    logger.info("STEP: %s", name)
    t0 = time.perf_counter()
    rc = fn()
    dt = time.perf_counter() - t0
    if rc == 0:
        logger.info("  ✓ %s done in %.1fs", name, dt)
        return True
    logger.error("  ✗ %s FAILED (rc=%s) after %.1fs — aborting refresh", name, rc, dt)
    return False


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Daily data refresh: load + recompute derived state + validate.")
    p.add_argument("path", help="directory of canonical TSVs (or a single file)")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--allow-dev", action="store_true")
    p.add_argument("--skip-cost", action="store_true", help="skip the standard_cost roll-up step")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set and --dsn not provided")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    dev = ["--allow-dev"] if args.allow_dev else []
    logger.info("DAILY REFRESH → DB=%s  source=%s", db, args.path)
    t0 = time.perf_counter()

    if not _step("bulk_ingest", lambda: bulk_ingest.main([args.path, "--dsn", args.dsn] + dev)):
        return 1
    if not _step("compute_llc", lambda: compute_llc.main(["--dsn", args.dsn] + dev)):
        return 1
    if not args.skip_cost:
        if not _step("compute_cost_rollup", lambda: compute_cost_rollup.main(["--dsn", args.dsn] + dev)):
            return 1

    logger.info("─" * 70)
    logger.info("STEP: validate")
    ok, msgs = _validate(args.dsn)
    for m in msgs:
        logger.info("  %s", m)

    logger.info("═" * 70)
    if ok:
        logger.info("DAILY REFRESH OK in %.1fs — engine state is consistent.", time.perf_counter() - t0)
        return 0
    logger.error("DAILY REFRESH COMPLETED WITH PROBLEMS in %.1fs — see ✗ above. Do NOT trust the plan.",
                 time.perf_counter() - t0)
    return 1


if __name__ == "__main__":
    sys.exit(main())
