"""
mrp_demand_query.py — independent-demand volume for items matching a code pattern.

Uses mrp_core (forecast consumption + proration) so the number matches what the
planner actually nets. Filters items whose external_id contains a substring and
sums consumed demand within a window (default current calendar year).

Usage:
    DATABASE_URL=... python scripts/mrp_demand_query.py --like JXI
"""
from __future__ import annotations

import argparse
import datetime as _dt
import logging
import os
import sys

import psycopg
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("demand_query")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Independent-demand volume for items matching a code pattern.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--like", required=True, help="substring to match in item external_id (case-insensitive)")
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--rolling12", action="store_true")
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    core.guard_db(args.dsn, args.allow_dev)

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
    gross = core.consume_demand(d)
    hs = d.horizon_start
    if args.rolling12:
        win_end, label = hs + _dt.timedelta(days=365), f"rolling 12 months ({hs}..{hs + _dt.timedelta(days=365)})"
    else:
        win_end, label = _dt.date(hs.year, 12, 31), f"current year {hs.year} ({hs}..{hs.year}-12-31)"
    last_bucket = d.bucket(win_end)

    pat = args.like.upper()
    matched = {i: ext for i, ext in d.names.items() if pat in (ext or "").upper()}

    rows = []
    tot_consumed = tot_co = tot_fc = 0.0
    for item, ext in matched.items():
        cons = sum(q for t, q in gross.get(item, {}).items() if t <= last_bucket)
        co = sum(q for t, q in d.co_b.get(item, {}).items() if t <= last_bucket)
        fc = sum(q for t, q in d.fc_b.get(item, {}).items() if t <= last_bucket)
        if cons <= 0 and co <= 0 and fc <= 0:
            continue
        rows.append((ext, cons, co, fc, d.llc.get(item, 0), bool(d.is_make.get(item, False))))
        tot_consumed += cons
        tot_co += co
        tot_fc += fc
    rows.sort(key=lambda r: -r[1])

    logger.info("=" * 92)
    logger.info("DEMAND for items LIKE '%s' — %s", args.like, label)
    logger.info("=" * 92)
    logger.info("  Matching items with demand : %d", len(rows))
    logger.info("  CONSUMED demand (max of CO/forecast, prorated) : %s units", f"{tot_consumed:,.0f}")
    logger.info("      (raw customer orders: %s | raw forecast: %s — shown for reference, NOT summed)",
                f"{tot_co:,.0f}", f"{tot_fc:,.0f}")
    logger.info("  " + "-" * 88)
    logger.info("  TOP %d by consumed demand:", args.top)
    logger.info("  %-18s %14s %14s %14s %-4s %-4s", "item", "consumed", "cust_orders", "forecast", "llc", "m/b")
    for ext, cons, co, fc, llc, mk in rows[: args.top]:
        logger.info("  %-18s %14s %14s %14s L%-3d %-4s", ext, f"{cons:,.0f}", f"{co:,.0f}", f"{fc:,.0f}",
                    llc, "MAKE" if mk else "BUY")
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
