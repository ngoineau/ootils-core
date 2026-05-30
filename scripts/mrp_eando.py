"""
mrp_eando.py — Excess & Obsolete (E&O) inventory valuation (CLI over mrp_core).

Classifies on-hand stock against its consumption rate and values what sits beyond
a coverage threshold:

  - EXCESS   : months_of_coverage > threshold (default 12), finite demand.
               excess_units = on_hand − threshold_months × monthly_use
  - OBSOLETE : on_hand > 0 but NO demand over the horizon → whole on_hand is dead.

Consumption rate per item = total GROSS usage (independent demand + dependent
demand exploded through the BOM, no netting — that's the true burn rate), summed
over the horizon and annualized. months_of_coverage = on_hand / (annual_use / 12).

Valued at the cost fallback chain (supplier unit_cost → item standard_cost);
unpriced E&O units are surfaced, never silently dropped. No FX (reported per
currency).

Usage:
    DATABASE_URL=... python scripts/mrp_eando.py [--months 12] [--top 20]
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time
from collections import defaultdict

import psycopg
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("mrp_eando")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Excess & Obsolete inventory valuation (CLI over mrp_core).")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--months", type=float, default=12.0, help="coverage threshold in months; above = excess")
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("E&O valuation: DB=%s  threshold=%.0f months", db, args.months)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
    gross = core.consume_demand(d)

    # Total GROSS usage per item over the horizon = independent demand + dependent
    # demand exploded through the BOM (no netting → true consumption rate).
    indep = {i: sum(v.values()) for i, v in gross.items()}
    dep = defaultdict(float)
    for level in range(0, d.max_llc + 1):
        for item in d.by_level[level]:
            use = indep.get(item, 0.0) + dep.get(item, 0.0)
            if use <= 0 or not bool(d.is_make.get(item, False)):
                continue
            for comp, qpb, scrap in d.bom.get(item, []):
                dep[comp] += use * qpb * (1.0 + scrap)
    ann_factor = 365.0 / args.horizon_days

    by_class_ccy = defaultdict(lambda: defaultdict(float))   # class -> ccy -> value
    by_class_units = defaultdict(float)
    by_class_items = defaultdict(int)
    unpriced_units = unpriced_items = 0
    rows = []                                                # (item, cls, oh, cover, excess_units, value, ccy)

    for item, oh in d.on_hand.items():
        oh = float(oh or 0)
        if oh <= 0:
            continue
        annual = (indep.get(item, 0.0) + dep.get(item, 0.0)) * ann_factor
        monthly = annual / 12.0
        if annual <= 0:
            cls, cover, excess_units = "OBSOLETE", math.inf, oh
        else:
            cover = oh / monthly
            if cover <= args.months:
                continue                                     # healthy coverage
            cls, excess_units = "EXCESS", oh - args.months * monthly
        uc = d.unit_cost.get(item) or d.std_cost.get(item)
        ccy = (d.cost_ccy.get(item) or d.std_ccy.get(item) or "USD")
        by_class_units[cls] += excess_units
        by_class_items[cls] += 1
        if uc is None:
            unpriced_units += excess_units
            unpriced_items += 1
            value = None
        else:
            value = excess_units * float(uc)
            by_class_ccy[cls][ccy] += value
        rows.append((d.names.get(item, str(item)[:8]), cls, oh, cover, excess_units, value, ccy))

    elapsed = round(time.perf_counter() - t0, 2)
    grand = defaultdict(float)
    for cls in by_class_ccy:
        for ccy, v in by_class_ccy[cls].items():
            grand[ccy] += v

    logger.info("=" * 96)
    logger.info("EXCESS & OBSOLETE — on-hand beyond %.0f months of coverage (%.2fs)", args.months, elapsed)
    logger.info("=" * 96)
    for cls in ("EXCESS", "OBSOLETE"):
        if by_class_items.get(cls):
            ccys = ", ".join(f"{c} {v:,.0f}" for c, v in sorted(by_class_ccy[cls].items(), key=lambda x: -x[1]))
            logger.info("  %-9s : %5d items, %s units beyond threshold, value: %s",
                        cls, by_class_items[cls], f"{by_class_units[cls]:,.0f}", ccys or "—")
    logger.info("  " + "-" * 92)
    logger.info("  TOTAL E&O value (no FX): %s", ", ".join(f"{c} {v:,.0f}" for c, v in sorted(grand.items(), key=lambda x: -x[1])))
    if unpriced_items:
        logger.info("  ⚠ Unpriced E&O: %d items, %s units NOT valued (no cost) — feed dq_watcher MISSING_COST",
                    unpriced_items, f"{unpriced_units:,.0f}")
    logger.info("=" * 96)
    rows.sort(key=lambda r: -(r[5] or 0))
    logger.info("TOP %d E&O exposures by value:", args.top)
    logger.info("  %-16s %-9s %12s %10s %14s %14s %-4s", "item", "class", "on_hand", "cover_mo", "excess_units", "value", "ccy")
    for ext, cls, oh, cover, exu, val, ccy in rows[: args.top]:
        cov_s = "∞" if cover == math.inf else f"{cover:,.1f}"
        val_s = f"{val:,.0f}" if val is not None else "—"
        logger.info("  %-16s %-9s %12s %10s %14s %14s %-4s", ext, cls, f"{oh:,.0f}", cov_s, f"{exu:,.0f}", val_s, ccy)
    logger.info("=" * 96)
    return 0


if __name__ == "__main__":
    sys.exit(main())
