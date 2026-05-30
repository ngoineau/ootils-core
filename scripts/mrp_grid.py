"""
mrp_grid.py — monthly MPS/MRP time-phased grid per item (CLI over mrp_core).

The planner's validation lens: for an item, the canonical time-phased record laid
out month by month, so demand, supply, the projected balance and the plan can be
eyeballed period by period — exactly where lumps, stale dates and netting errors
become visible.

Lines (standard MPS/MRP record):
    Forecast               raw forecast (prorated monthly→weekly upstream)
    Customer orders        booked CO
    Gross requirements     netting demand = consumed independent (max_only/DTF) + dependent (BOM)
    Scheduled receipts     firm PO/WO/transfer arriving
    Planned receipts       MRP planned orders, by need period
    Proj. on-hand (PAB)    on_hand + Σ(receipts + planned − gross), end-of-month
    Planned releases       MRP planned orders, lead-time-offset by release period

Demand is consumption-correct (max_only / demand-time-fence / prorated). Numbers
match run_timephased (same engine).

Usage:
    DATABASE_URL=... python scripts/mrp_grid.py --item JXIQ400NK [--months-out 12]
    DATABASE_URL=... python scripts/mrp_grid.py --like JXI --max-items 6
"""
from __future__ import annotations

import argparse
import datetime as _dt
import logging
import os
import sys
from collections import defaultdict

import psycopg
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("mrp_grid")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Monthly MPS/MRP grid per item (CLI over mrp_core).")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--item", default=None, help="exact item external_id")
    p.add_argument("--like", default=None, help="substring of external_id (multiple items)")
    p.add_argument("--months-out", type=int, default=12)
    p.add_argument("--max-items", type=int, default=6)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    if not args.item and not args.like:
        logger.error("provide --item <external_id> or --like <pattern>")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("MPS/MRP grid: DB=%s", db)

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
    gross = core.consume_demand(d)
    r = core.run_timephased(d, gross)
    hs = d.horizon_start

    ext_to_id = {v: k for k, v in d.names.items()}
    if args.item:
        ids = [ext_to_id[args.item]] if args.item in ext_to_id else []
        if not ids:
            logger.error("item %s not found", args.item)
            return 3
    else:
        pat = args.like.upper()
        ids = [i for i, ext in d.names.items() if pat in (ext or "").upper()]
        ids = sorted(ids, key=lambda i: d.names.get(i, ""))[: args.max_items]
        if not ids:
            logger.error("no item matches --like %s", args.like)
            return 3

    # planned orders indexed per item (receipt at need bucket, release at rel bucket)
    prcpt = defaultdict(lambda: defaultdict(float))
    prel = defaultdict(lambda: defaultdict(float))
    for item, qty, rel, need, kind, pd in r["planned"]:
        prcpt[item][need] += qty
        prel[item][rel] += qty

    # month label per weekly bucket + ordered month list
    bmonth = {t: (hs + _dt.timedelta(weeks=t)).strftime("%Y-%m") for t in range(d.n_buckets)}
    months = sorted(set(bmonth.values()))[: args.months_out]
    mset = set(months)

    def monthly_sum(weekly: dict) -> dict:
        out = defaultdict(float)
        for t, v in weekly.items():
            if bmonth.get(t) in mset:
                out[bmonth[t]] += v
        return out

    def fmt(v):
        return f"{v:,.0f}" if abs(v) >= 0.5 else "·"

    for item in ids:
        ext = d.names.get(item, str(item)[:8])
        oh = float(d.on_hand.get(item, 0) or 0)
        ss = float(d.safety.get(item, 0) or 0)
        make = bool(d.is_make.get(item, False))
        lt = (d.make_lt.get(item) if make else d.buy_lt.get(item)) or core.DEFAULT_LT_DAYS
        llc = d.llc.get(item, 0)

        consumed = gross.get(item, {})
        dep = r["dependent"].get(item, {})
        gross_req = {t: consumed.get(t, 0.0) + dep.get(t, 0.0) for t in set(consumed) | set(dep)}
        sched = d.sched_b.get(item, {})
        pr = prcpt.get(item, {})

        # weekly PAB walk (matches run_timephased), then end-of-month sample
        pab_month = {}
        pa = oh
        for t in range(d.n_buckets):
            pa += sched.get(t, 0.0) + pr.get(t, 0.0) - gross_req.get(t, 0.0)
            if bmonth.get(t) in mset:
                pab_month[bmonth[t]] = pa

        lines = [
            ("Forecast", monthly_sum(d.fc_b.get(item, {}))),
            ("Customer orders", monthly_sum(d.co_b.get(item, {}))),
            ("Gross requirements", monthly_sum(gross_req)),
            ("Scheduled receipts", monthly_sum(sched)),
            ("Planned receipts", monthly_sum(pr)),
            ("Proj. on-hand (PAB)", pab_month),
            ("Planned releases", monthly_sum(prel.get(item, {}))),
        ]

        logger.info("=" * (22 + 9 * len(months)))
        logger.info("ITEM %s  | %s | LLC %d | on-hand %s | safety %s | lead time %dd",
                    ext, "MAKE" if make else "BUY", llc, f"{oh:,.0f}", f"{ss:,.0f}", int(lt))
        logger.info("  %-20s%s", "month →", "".join(f"{m[2:]:>9}" for m in months))  # YY-MM
        for label, series in lines:
            logger.info("  %-20s%s", label, "".join(f"{fmt(series.get(m, 0.0)):>9}" for m in months))
        logger.info("=" * (22 + 9 * len(months)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
