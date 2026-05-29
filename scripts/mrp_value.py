"""
mrp_value.py — value the planned PURCHASE plan in money (CLI over mrp_core).

Runs the time-phased cascade and values every planned PURCHASE order (kind=PO,
all LLC levels — a made item explodes into purchases of its own components) at
the supplier unit cost. Filters to orders RELEASED within a window (default: the
current calendar year, today..Dec 31) — i.e. the spend you would commit this year.

Aggregates by currency (no FX conversion — reported per currency) and by month,
and reports price coverage (unpriced orders are surfaced, never silently dropped).

Usage:
    DATABASE_URL=... python scripts/mrp_value.py [--force-rule POQ] [--poq-periods 4]
        [--basis release|need] [--rolling12]
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
logger = logging.getLogger("mrp_value")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Value the planned purchase plan in money (CLI over mrp_core).")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--force-rule", default=None)
    p.add_argument("--poq-periods", type=int, default=4)
    p.add_argument("--basis", choices=["release", "need"], default="release",
                   help="date a PO is attributed to: release (commit) or need (receipt)")
    p.add_argument("--rolling12", action="store_true", help="window = next 12 months instead of current calendar year")
    p.add_argument("--year", type=int, default=None, help="value a specific calendar year FY (Jan1..Dec31), e.g. 2027")
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--by-supplier", action="store_true", help="break purchase volume/spend down by supplier")
    p.add_argument("--top-suppliers", type=int, default=30, help="suppliers to show (<=0 = all)")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)

    with psycopg.connect(args.dsn) as conn:
        today = conn.cursor().execute("SELECT CURRENT_DATE").fetchone()[0]
        # resolve the valuation window first, so the planning horizon is extended
        # to cover it (a future FY needs more than the default 540 days).
        if args.year is not None:
            win_start = max(today, _dt.date(args.year, 1, 1))
            win_end = _dt.date(args.year, 12, 31)
            label = f"FY {args.year} ({win_start} .. {win_end})"
        elif args.rolling12:
            win_start, win_end = today, today + _dt.timedelta(days=365)
            label = f"rolling 12 months ({win_start} .. {win_end})"
        else:
            win_start, win_end = today, _dt.date(today.year, 12, 31)
            label = f"current year {today.year} ({win_start} .. {win_end})"
        needed = (win_end - today).days + 14
        horizon = max(args.horizon_days, needed)
        d = core.load_planning_data(conn, horizon)
    gross = core.consume_demand(d)
    r = core.run_timephased(d, gross, force_rule=args.force_rule, poq_periods=args.poq_periods)
    hs = d.horizon_start
    logger.info("Purchase-plan valuation: DB=%s  rule=%s  basis=%s  horizon=%dd  window=%s",
                db, args.force_rule or "per-item", args.basis, horizon, label)

    by_ccy = defaultdict(float)
    by_ccy_units = defaultdict(float)
    by_month = defaultdict(lambda: defaultdict(float))
    unpriced_units = 0.0
    unpriced_orders = 0
    priced_orders = 0
    item_cost = defaultdict(lambda: defaultdict(float))  # item -> ccy -> cost
    item_units = defaultdict(float)
    sup_cost = defaultdict(lambda: defaultdict(float))   # supplier -> ccy -> cost
    sup_units = defaultdict(float)
    sup_orders = defaultdict(int)
    sup_items = defaultdict(set)

    for item, qty, rel, need, kind, pd in r["planned"]:
        if kind != "PO":
            continue
        attr = hs + _dt.timedelta(weeks=(rel if args.basis == "release" else need))
        if not (win_start <= attr <= win_end):
            continue
        sup = d.best_sup.get(item)
        sup_label = sup[1] if (sup and sup[1]) else "(no supplier)"
        uc = sup[3] if sup else None
        ccy = (sup[4] if sup else None)
        if uc is None:                       # chosen supplier carries no cost -> fall back
            uc = d.unit_cost.get(item)       # any priced supplier row
            ccy = d.cost_ccy.get(item)
        if uc is None:                       # still none -> item standard cost (incl. BOM roll-up)
            uc = d.std_cost.get(item)
            ccy = d.std_ccy.get(item)
        if uc is None:
            unpriced_units += qty
            unpriced_orders += 1
            sup_units[sup_label] += qty      # volume still attributed to supplier
            sup_orders[sup_label] += 1
            sup_items[sup_label].add(item)
            continue
        ccy = ccy or "USD"
        cost = qty * float(uc)
        by_ccy[ccy] += cost
        by_ccy_units[ccy] += qty
        by_month[attr.strftime("%Y-%m")][ccy] += cost
        priced_orders += 1
        item_cost[item][ccy] += cost
        item_units[item] += qty
        sup_cost[sup_label][ccy] += cost
        sup_units[sup_label] += qty
        sup_orders[sup_label] += 1
        sup_items[sup_label].add(item)

    tot_orders = priced_orders + unpriced_orders
    priced_units = sum(by_ccy_units.values())
    tot_units = priced_units + unpriced_units
    cov = 100.0 * priced_orders / tot_orders if tot_orders else 0.0
    cov_u = 100.0 * priced_units / tot_units if tot_units else 0.0
    logger.info("=" * 92)
    logger.info("PURCHASE PLAN VALUATION — %s", label)
    logger.info("=" * 92)
    logger.info("  Planned PO releases in window : %d  (priced %d / unpriced %d)", tot_orders, priced_orders, unpriced_orders)
    logger.info("  Price coverage by order count : %.1f%%", cov)
    logger.info("  Price coverage by VOLUME      : %.1f%%  (%s priced / %s total units)",
                cov_u, f"{priced_units:,.0f}", f"{tot_units:,.0f}")
    if unpriced_orders:
        logger.info("  ⚠ Unpriced (no supplier cost) : %d orders, %s units — typically low-level components",
                    unpriced_orders, f"{unpriced_units:,.0f}")
    logger.info("  --------------------------------------------------------------")
    logger.info("  PLANNED SPEND by currency (no FX conversion applied):")
    for ccy in sorted(by_ccy, key=lambda c: -by_ccy[c]):
        logger.info("      %-4s : %18s   (%s units)", ccy, f"{by_ccy[ccy]:,.2f}", f"{by_ccy_units[ccy]:,.0f}")
    logger.info("  --------------------------------------------------------------")
    logger.info("  Spend by month (primary currency per row):")
    for mo in sorted(by_month):
        parts = ", ".join(f"{c} {v:,.0f}" for c, v in sorted(by_month[mo].items(), key=lambda x: -x[1]))
        logger.info("      %-8s : %s", mo, parts)
    logger.info("  --------------------------------------------------------------")
    # top items by spend (sum across their currencies, labelled with dominant ccy)
    ranked = sorted(item_cost.items(), key=lambda kv: -sum(kv[1].values()))[: args.top]
    logger.info("  TOP %d items by planned spend in window:", args.top)
    logger.info("      %-16s %14s %-5s %14s", "item", "spend", "ccy", "units")
    for item, ccys in ranked:
        ccy = max(ccys, key=ccys.get)
        logger.info("      %-16s %14s %-5s %14s", d.names.get(item, str(item)[:8]),
                    f"{ccys[ccy]:,.0f}", ccy, f"{item_units[item]:,.0f}")

    if args.by_supplier:
        logger.info("  --------------------------------------------------------------")
        n = args.top_suppliers
        ranked_sup = sorted(sup_units, key=lambda s: (-sum(sup_cost[s].values()), -sup_units[s]))
        logger.info("  PURCHASE VOLUME by supplier (%s):", "all" if n <= 0 else f"top {n} by spend")
        logger.info("      %-12s %-32s %16s %-5s %14s %7s", "code", "supplier", "spend", "ccy", "units", "items")
        shown = ranked_sup if n <= 0 else ranked_sup[:n]
        for s in shown:
            ccys = sup_cost[s]
            if ccys:
                ccy = max(ccys, key=ccys.get)
                spend = f"{ccys[ccy]:,.0f}"
            else:
                ccy, spend = "—", "unpriced"
            name = (d.sup_name.get(s) or "")[:32]
            logger.info("      %-12s %-32s %16s %-5s %14s %7d", s[:12], name, spend, ccy,
                        f"{sup_units[s]:,.0f}", len(sup_items[s]))
        logger.info("      (suppliers in window: %d)", len(sup_units))
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
