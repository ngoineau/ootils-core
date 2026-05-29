"""
shortage_scan.py — item-level shortage scan, thin over mrp_core.

Detects the first forward shortage per item on the SINGLE demand truth
(mrp_core: forecast consumption max_only + demand time fence + proration +
multi-location dedup) via the same virtual projection the Shortage Watcher agent
uses (core.first_shortage). This is the human/CLI counterpart of the agent — same
numbers.

Previously this script ran a raw SQL window function that SUMMED customer orders
+ forecast (double-count) with no proration; it now shares mrp_core so the scan,
the agent, and the MRP all agree.

Modes:
    (default)     list net-short items, worst balance first
    --reorder     classify by reorder feasibility (RECOVERABLE / TIGHT / CRITICAL / NO_SOURCE)
    --recommend   quantified purchase recommendations (supplier, qty, cost, action)

Note on location: planning is pooled at item level (LANES-LATER). Per-(item,
location) projection needs the network connected and is out of scope here.

Usage:
    DATABASE_URL=... python scripts/shortage_scan.py [--reorder | --recommend] [--top 30]
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
logger = logging.getLogger("shortage_scan")


def _classify(lt_days, runway_days, margin_days) -> str:
    if lt_days is None:
        return "NO_SOURCE"          # forward shortage + no supplier to reorder from
    if margin_days >= 0:
        return "RECOVERABLE"        # order now, arrives before shortage
    if margin_days >= -14:
        return "TIGHT"              # missed normal reorder by <=2wk → expedite
    return "CRITICAL"               # too late via normal reorder → expedite / alt source


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Item-level shortage scan (CLI over mrp_core).")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--reorder", action="store_true", help="classify reorder feasibility")
    p.add_argument("--recommend", action="store_true", help="quantified purchase recommendations")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("Shortage scan: DB=%s", db)

    t0 = time.perf_counter()
    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
    gross = core.consume_demand(d)
    short = core.first_shortage(d, gross)
    today = d.horizon_start
    elapsed = round(time.perf_counter() - t0, 2)
    n_demand_items = sum(1 for g in gross.values() if g)

    # ── Recommendation mode ─────────────────────────────────────────
    if args.recommend:
        recs = []
        by_action, spend = {}, {}
        for item, sh in short.items():
            sup = d.best_sup.get(item)
            if not sup:
                continue
            sid, sext, lt, uc, ccy, rel = sup
            ss = float(d.safety.get(item, 0) or 0)
            qty = round(core.lot_size(sh["deficit"] + ss, float(d.moq.get(item) or 0), float(d.mult.get(item) or 0)), 2)
            cost = round(qty * float(uc), 2) if uc is not None else None
            ccy = ccy or "EUR"
            runway = (sh["date"] - today).days
            margin = runway - int(lt or core.DEFAULT_LT_DAYS)
            action = "EXPEDITE" if margin < -14 else ("ORDER_RUSH" if margin < 0 else "ORDER_NOW")
            by_action[action] = by_action.get(action, 0) + 1
            if cost is not None:
                spend[ccy] = spend.get(ccy, 0.0) + cost
            recs.append((d.names.get(item, str(item)[:8]), sh["date"], action, qty, cost, ccy, sext, margin))
        recs.sort(key=lambda r: r[7])
        logger.info("=" * 100)
        logger.info("PURCHASE RECOMMENDATIONS (consumption-correct) in %.2fs — %d forward shortages with supplier",
                    elapsed, len(recs))
        for act in ("EXPEDITE", "ORDER_RUSH", "ORDER_NOW"):
            logger.info("      %-12s %d", act, by_action.get(act, 0))
        logger.info("  Estimated spend: %s", {k: round(v, 2) for k, v in spend.items()})
        logger.info("=" * 100)
        logger.info("TOP %d (most urgent first):", args.top)
        logger.info("  %-14s %-11s %-11s %9s %13s %-5s %-12s", "item", "by_date", "action", "qty", "cost", "ccy", "supplier")
        for ext, fsd, action, qty, cost, ccy, sext, margin in recs[: args.top]:
            cs = f"{cost:,.0f}" if cost is not None else "—"
            logger.info("  %-14s %-11s %-11s %9.0f %13s %-5s %-12s", ext, str(fsd), action, qty, cs, ccy, sext)
        logger.info("=" * 100)
        return 0

    # ── Reorder feasibility mode ────────────────────────────────────
    if args.reorder:
        classified, breakdown = [], {}
        for item, sh in short.items():
            sup = d.best_sup.get(item)
            lt = sup[2] if sup else None
            runway = (sh["date"] - today).days
            margin = runway - int(lt) if lt is not None else None
            cls = _classify(lt, runway, margin if margin is not None else -99999)
            breakdown[cls] = breakdown.get(cls, 0) + 1
            classified.append((d.names.get(item, str(item)[:8]), sh["date"], sh["balance"], lt, runway, margin, cls))
        actionable = sorted([c for c in classified if c[6] in ("CRITICAL", "TIGHT")], key=lambda c: c[5])
        logger.info("=" * 90)
        logger.info("REORDER FEASIBILITY (consumption-correct) in %.2fs — %d net-short items", elapsed, len(short))
        for cls in ("CRITICAL", "TIGHT", "RECOVERABLE", "NO_SOURCE"):
            n = breakdown.get(cls, 0)
            pct = (100 * n / len(short)) if short else 0
            logger.info("      %-12s %6d  (%4.1f%%)", cls, n, pct)
        logger.info("  → ACTIONABLE (CRITICAL+TIGHT): %d", len(actionable))
        logger.info("=" * 90)
        logger.info("TOP %d ACTIONABLE (smallest margin first):", args.top)
        logger.info("  %-15s %-11s %12s %7s %7s %7s  %-11s", "item", "shortage", "net_qty", "lt_d", "runway", "margin", "class")
        for ext, fsd, bal, lt, runway, margin, cls in actionable[: args.top]:
            logger.info("  %-15s %-11s %12.0f %7s %7s %7s  %-11s", ext, str(fsd), float(bal),
                        str(lt), str(runway), str(margin), cls)
        logger.info("=" * 90)
        return 0

    # ── Default: item-level shortage list ───────────────────────────
    rows = sorted(short.items(), key=lambda kv: kv[1]["balance"])
    pct = (100 * len(short) / n_demand_items) if n_demand_items else 0
    logger.info("=" * 78)
    logger.info("ITEM-LEVEL SHORTAGE SCAN (consumption-correct) in %.2fs", elapsed)
    logger.info("  Items with independent demand : %d", n_demand_items)
    logger.info("  Items net-short               : %d (%.1f%%)", len(short), pct)
    logger.info("=" * 78)
    logger.info("TOP %d net-short items — worst balance first:", args.top)
    logger.info("  %-16s %-11s %14s %14s   %s", "item", "date", "net_balance", "deficit", "name")
    for item, sh in rows[: args.top]:
        logger.info("  %-16s %-11s %14.1f %14.1f   %s", d.names.get(item, str(item)[:8]), str(sh["date"]),
                    sh["balance"], sh["deficit"], (d.names.get(item, "") or "")[:28])
    logger.info("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
