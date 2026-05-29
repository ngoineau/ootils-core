"""
mrp_timephased.py — thin CLI over mrp_core: time-phased level-by-level MRP with
forecast consumption, demand/planning time fences, lead-time offsetting, and
rule-based lot sizing.

All logic lives in mrp_core (single source of truth). See that module for the
algorithm and correctness notes.

Usage:
    DATABASE_URL=... python scripts/mrp_timephased.py [--top 25] [--force-rule POQ] [--poq-periods 8]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import defaultdict

import psycopg
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("mrp_tp")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Time-phased level-by-level MRP (CLI over mrp_core).")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--top", type=int, default=25)
    p.add_argument("--force-rule", default=None)
    p.add_argument("--poq-periods", type=int, default=4)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("Time-phased MRP: DB=%s horizon=%dd", db, args.horizon_days)

    t0 = time.perf_counter()
    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
    load_s = round(time.perf_counter() - t0, 2)

    gross = core.consume_demand(d)
    consumed_saved = 0.0
    for item in set(d.co_b) | set(d.fc_b):
        for t in set(d.co_b.get(item, {})) | set(d.fc_b.get(item, {})):
            o = d.co_b.get(item, {}).get(t, 0.0)
            f = d.fc_b.get(item, {}).get(t, 0.0)
            consumed_saved += (o + f) - gross.get(item, {}).get(t, 0.0)

    t1 = time.perf_counter()
    r = core.run_timephased(d, gross, force_rule=args.force_rule, poq_periods=args.poq_periods)
    cascade_s = round(time.perf_counter() - t1, 2)

    planned = r["planned"]
    by_rel = defaultdict(int)
    for _, _, rel, _, _, _ in planned:
        by_rel[rel] += 1

    logger.info("=" * 92)
    logger.info("TIME-PHASED MRP COMPLETE — load %.2fs + cascade %.2fs", load_s, cascade_s)
    logger.info("  Items in scope        : %d (max LLC %d, %d weekly buckets)", len(d.involved), d.max_llc, d.n_buckets)
    logger.info("  Planned WORK ORDERS   : %d", r["n_wo"])
    logger.info("  Planned PURCHASE ORD. : %d", r["n_po"])
    logger.info("  Planned-order lines   : %d", len(planned))
    logger.info("  PAST-DUE releases (need − lead time already elapsed → EXPEDITE): %d", r["past_due"])
    logger.info("  Inside PLANNING TIME FENCE (frozen → planner action, not auto) : %d", r["within_ptf"])
    logger.info("  Forecast double-count removed by consumption (max_only/DTF)    : %.0f units", consumed_saved)
    logger.info("  Orders by lot-sizing rule: %s", r["rule_orders"])
    logger.info("  Releases by week (next 8 weeks):")
    for wk in range(0, 8):
        logger.info("      week %-2d : %d planned-order releases", wk, by_rel.get(wk, 0))
    logger.info("=" * 92)

    urgent = sorted([x for x in planned if x[5]], key=lambda x: -x[1])[: args.top]
    if urgent:
        logger.info("TOP %d PAST-DUE planned-order releases (must expedite) — biggest qty:", args.top)
        logger.info("  %-16s %-4s %-5s %10s %10s %8s", "item", "kind", "llc", "qty", "need_wk", "rel_wk")
        for item, qty, rel, need, kind, pd in urgent:
            logger.info("  %-16s %-4s %-5d %10.0f %10d %8d", d.names.get(item, str(item)[:8]), kind, d.llc.get(item, 0), qty, need, rel)
        logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
