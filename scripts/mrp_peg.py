"""
mrp_peg.py — thin CLI over mrp_core: pegging (trace component demand back to the
finished goods that drive it). Uses consumed demand (forecast consumption), so
pegging is consistent with the time-phased MRP.

Usage:
    DATABASE_URL=... python scripts/mrp_peg.py                 # top components + origins
    DATABASE_URL=... python scripts/mrp_peg.py --item Q0152700 # peg one component
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import psycopg
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("mrp_peg")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="MRP pegging (CLI over mrp_core).")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--item", default=None)
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("MRP pegging: DB=%s", db)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn)
    gross = core.consume_demand(d)
    dependent, origin = core.peg_origins(d, gross)
    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("Pegging computed in %.2fs (%d items, max LLC %d)", elapsed, len(d.involved), d.max_llc)

    ext_to_id = {v: k for k, v in d.names.items()}
    if args.item:
        iid = ext_to_id.get(args.item)
        if iid is None:
            logger.error("item %s not found", args.item)
            return 3
        org = origin.get(iid, {})
        total = sum(org.values()) or 1
        logger.info("=" * 80)
        logger.info("PEGGING — %s", args.item)
        logger.info("  Total dependent demand: %.0f  (LLC %d)", dependent.get(iid, 0), d.llc.get(iid, 0))
        logger.info("  Driven by finished goods / demand origins:")
        logger.info("  %-16s %14s %8s", "origin_FG", "qty", "share")
        for fg, q in sorted(org.items(), key=lambda x: -x[1])[: args.top]:
            logger.info("  %-16s %14.0f %7.1f%%", d.names.get(fg, str(fg)[:8]), q, 100 * q / total)
        logger.info("=" * 80)
    else:
        comps = sorted(((c, dependent[c]) for c in dependent if d.llc.get(c, 0) >= 1), key=lambda x: -x[1])[: args.top]
        logger.info("=" * 96)
        logger.info("TOP %d components by dependent demand — pegged to their main finished goods:", args.top)
        logger.info("=" * 96)
        for c, dem in comps:
            org = origin.get(c, {})
            total = sum(org.values()) or 1
            top3 = sorted(org.items(), key=lambda x: -x[1])[:3]
            peg = ", ".join(f"{d.names.get(fg, str(fg)[:8])} {100*q/total:.0f}%" for fg, q in top3)
            logger.info("  %-16s LLC%-2d dem=%-12.0f ← %s", d.names.get(c, str(c)[:8]), d.llc.get(c, 0), dem, peg)
        logger.info("=" * 96)
    return 0


if __name__ == "__main__":
    sys.exit(main())
