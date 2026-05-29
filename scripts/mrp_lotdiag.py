"""
mrp_lotdiag.py — lot-sizing impact diagnostic (CLI over mrp_core).

Runs the time-phased cascade with tracing and reports, per planned order, how
much the final quantity owes to the raw net requirement vs the MOQ floor vs the
order-multiple rounding. Answers: "how much of my plan is driven by MOQ?".

Usage:
    DATABASE_URL=... python scripts/mrp_lotdiag.py [--force-rule POQ] [--poq-periods 4]
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from collections import defaultdict

import psycopg
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("mrp_lotdiag")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Lot-sizing impact diagnostic (CLI over mrp_core).")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--force-rule", default=None)
    p.add_argument("--poq-periods", type=int, default=4)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("Lot-sizing diagnostic: DB=%s", db)

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
    gross = core.consume_demand(d)
    trace = []
    core.run_timephased(d, gross, force_rule=args.force_rule, poq_periods=args.poq_periods, trace=trace)

    n = len(trace)
    if not n:
        logger.info("No planned orders.")
        return 0

    # classify each order by what lifted its qty above the raw net requirement
    exact = moq_bumped = mult_bumped = both = 0
    qty_raw_total = qty_final_total = 0.0
    moq_excess_units = mult_excess_units = 0.0
    moq_items = set()
    for item, shortfall, qty, moq, mult, rule, kind in trace:
        raw = max(0.0, shortfall)
        # reconstruct the two-step lot_size: MOQ floor, then multiple ceil
        after_moq = max(raw, moq) if moq else raw
        after_mult = (math.ceil(after_moq / mult) * mult) if (mult and mult > 0) else after_moq
        m_bump = after_moq > raw + 1e-9
        x_bump = after_mult > after_moq + 1e-9
        if m_bump and x_bump:
            both += 1
        elif m_bump:
            moq_bumped += 1
        elif x_bump:
            mult_bumped += 1
        else:
            exact += 1
        qty_raw_total += raw
        qty_final_total += after_mult
        if m_bump:
            moq_excess_units += after_moq - raw
            moq_items.add(item)
        if x_bump:
            mult_excess_units += after_mult - after_moq

    pct = lambda k: 100.0 * k / n
    logger.info("=" * 88)
    logger.info("LOT-SIZING IMPACT — %d planned orders (rule=%s)", n, args.force_rule or "per-item")
    logger.info("=" * 88)
    logger.info("  Exact net requirement (no MOQ/mult bump)   : %7d  (%.1f%%)", exact, pct(exact))
    logger.info("  Lifted to MOQ floor only                   : %7d  (%.1f%%)", moq_bumped, pct(moq_bumped))
    logger.info("  Rounded by order-multiple only             : %7d  (%.1f%%)", mult_bumped, pct(mult_bumped))
    logger.info("  Both MOQ floor AND multiple rounding        : %7d  (%.1f%%)", both, pct(both))
    logger.info("  --------------------------------------------------------------")
    logger.info("  Orders touched by MOQ (floor)              : %7d  (%.1f%%)  on %d distinct items",
                moq_bumped + both, pct(moq_bumped + both), len(moq_items))
    logger.info("  Orders touched by multiple                 : %7d  (%.1f%%)", mult_bumped + both, pct(mult_bumped + both))
    logger.info("  --------------------------------------------------------------")
    logger.info("  Raw net-requirement units (sum)            : %15s", f"{qty_raw_total:,.0f}")
    logger.info("  Final planned units (after lot sizing)     : %15s", f"{qty_final_total:,.0f}")
    over = qty_final_total - qty_raw_total
    logger.info("  Excess from lot sizing                     : %15s  (+%.1f%%)",
                f"{over:,.0f}", 100.0 * over / qty_raw_total if qty_raw_total else 0.0)
    logger.info("      of which MOQ floor                     : %15s", f"{moq_excess_units:,.0f}")
    logger.info("      of which multiple rounding             : %15s", f"{mult_excess_units:,.0f}")
    logger.info("=" * 88)
    return 0


if __name__ == "__main__":
    sys.exit(main())
