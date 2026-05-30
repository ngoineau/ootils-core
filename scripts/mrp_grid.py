"""
mrp_grid.py — monthly MPS/MRP time-phased grid per item (CLI over mrp_core).

The planner's data-validation lens: the canonical time-phased record laid out by
calendar month, so demand, supply, the projected balance and the plan can be
eyeballed period by period — where lumps, stale dates and netting issues become
visible.

Two layers, deliberately:
  DATA (exact, from source by calendar month) — what the planner validates:
    Forecast            raw forecast demand (forward)
    Customer orders     booked CO (forward)
    Scheduled receipts  firm PO/WO/transfer arriving
  PLAN (from the locked weekly engine, mrp_core) — what the engine decided:
    Gross requirements  consumed demand (max_only / DTF) + dependent (BOM)
    Planned WO/PO recpt MRP planned orders by need month
    Proj. on-hand (PAB) engine projected on-hand, sampled at calendar month-end
    Planned WO/PO rel.  MRP planned orders, lead-time-offset by release month

Gross requirements may differ from raw Forecast/CO by design — that gap IS the
forecast-consumption + proration effect, which is part of what you validate.

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
        ext_to_id = {v: k for k, v in d.names.items()}
        if args.item:
            ids = [ext_to_id[args.item]] if args.item in ext_to_id else []
            if not ids:
                logger.error("item %s not found", args.item)
                return 3
        else:
            pat = args.like.upper()
            ids = sorted((i for i, e in d.names.items() if pat in (e or "").upper()),
                         key=lambda i: d.names.get(i, ""))[: args.max_items]
            if not ids:
                logger.error("no item matches --like %s", args.like)
                return 3
        # DATA lines: exact source aggregation by calendar month (forward only)
        src = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))  # item -> kind -> month -> qty
        for item_id, ntype, m, qty in conn.cursor().execute(
            "SELECT n.item_id, n.node_type, to_char(date_trunc('month', n.time_ref),'YYYY-MM') m, SUM(n.quantity) "
            "FROM nodes n WHERE n.scenario_id=%(b)s AND n.active AND n.item_id = ANY(%(ids)s) "
            "  AND n.time_ref IS NOT NULL AND n.time_ref >= CURRENT_DATE AND n.quantity IS NOT NULL "
            "  AND n.node_type = ANY(%(t)s) GROUP BY 1,2,3",
            {"b": core.BASELINE, "ids": list(ids), "t": core.FIRM_RECEIPT_TYPES + core.DEMAND_TYPES}).fetchall():
            kind = "RECEIPT" if ntype in core.FIRM_RECEIPT_TYPES else ntype
            src[item_id][kind][m] += float(qty or 0)

    gross = core.consume_demand(d)
    r = core.run_timephased(d, gross)
    hs = d.horizon_start

    # PLAN: planned orders indexed by their calendar month (discrete events → exact)
    def bmonth(t):
        return (hs + _dt.timedelta(weeks=t)).strftime("%Y-%m")

    prcpt = defaultdict(lambda: defaultdict(float))      # item -> month -> qty (at need)
    prel = defaultdict(lambda: defaultdict(float))       # item -> month -> qty (at release)
    prcpt_wk = defaultdict(lambda: defaultdict(float))   # item -> need_bucket -> qty (weekly, for the PAB walk)
    for item, qty, rel, need, kind, pd in r["planned"]:
        prcpt[item][bmonth(need)] += qty
        prel[item][bmonth(rel)] += qty
        prcpt_wk[item][need] += qty

    # calendar month columns from today
    today = hs
    months, y, mo = [], today.year, today.month
    for _ in range(args.months_out):
        months.append(f"{y:04d}-{mo:02d}")
        mo += 1
        if mo > 12:
            mo, y = 1, y + 1

    def month_end_bucket(m):
        """Weekly bucket containing the last day of month m, or None if that month
        lies beyond the planning horizon (so the grid shows blank, not a stale
        repeat of the last in-horizon PAB)."""
        yy, mm = (int(x) for x in m.split("-"))
        first_next = _dt.date(yy + 1, 1, 1) if mm == 12 else _dt.date(yy, mm + 1, 1)
        bk = (first_next - _dt.timedelta(days=1) - hs).days // 7
        if bk < 0 or bk >= d.n_buckets:
            return None
        return bk

    def fmt(v):
        return f"{v:,.0f}" if abs(v) >= 0.5 else "·"

    for item in ids:
        ext = d.names.get(item, str(item)[:8])
        oh = float(d.on_hand.get(item, 0) or 0)
        ss = float(d.safety.get(item, 0) or 0)
        make = bool(d.is_make.get(item, False))
        lt = (d.make_lt.get(item) if make else d.buy_lt.get(item)) or core.DEFAULT_LT_DAYS
        llc = d.llc.get(item, 0)
        kind = "WO" if make else "PO"

        s = src.get(item, {})
        fc_m = s.get("ForecastDemand", {})
        co_m = s.get("CustomerOrderDemand", {})
        sched_m = s.get("RECEIPT", {})

        # PLAN: engine gross (consumed + dependent) and PAB, mapped to calendar months
        consumed = gross.get(item, {})
        dep = r["dependent"].get(item, {})
        gross_req_wk = {t: consumed.get(t, 0.0) + dep.get(t, 0.0) for t in set(consumed) | set(dep)}
        grossm = defaultdict(float)
        for t, v in gross_req_wk.items():
            grossm[bmonth(t)] += v
        # PAB walk on weekly engine, sampled at calendar month-end (exact point-in-time)
        sched_wk = d.sched_b.get(item, {})
        item_prcpt = prcpt_wk.get(item, {})
        pa, pab_bucket = oh, []
        for t in range(d.n_buckets):
            pa += sched_wk.get(t, 0.0) + item_prcpt.get(t, 0.0) - gross_req_wk.get(t, 0.0)
            pab_bucket.append(pa)
        pab_m = {m: pab_bucket[b] for m in months if (b := month_end_bucket(m)) is not None}

        lines = [
            ("Forecast", fc_m),
            ("Customer orders", co_m),
            ("Scheduled receipts", sched_m),
            ("Gross requirements", grossm),
            (f"Planned {kind} receipts", prcpt.get(item, {})),
            ("Proj. on-hand (PAB)", pab_m),
            (f"Planned {kind} releases", prel.get(item, {})),
        ]
        width = 22 + 9 * len(months)
        logger.info("=" * width)
        logger.info("ITEM %s  | %s | LLC %d | on-hand %s | safety %s | lead time %dd",
                    ext, "MAKE" if make else "BUY", llc, f"{oh:,.0f}", f"{ss:,.0f}", int(lt))
        logger.info("  %-20s%s", "month →", "".join(f"{m[2:]:>9}" for m in months))
        for label, series in lines:
            logger.info("  %-20s%s", label, "".join(f"{fmt(series.get(m, 0.0)):>9}" for m in months))
        logger.info("  (Forecast/CO/Scheduled = source calendar months; Gross/Planned/PAB = weekly engine)")
        logger.info("=" * width)
    return 0


if __name__ == "__main__":
    sys.exit(main())
