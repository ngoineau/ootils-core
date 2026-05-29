"""
mrp_timephased.py — time-phased, level-by-level MRP with lead-time offsetting.

The planning-grade layer on top of the LLC cascade (mrp_run.py proved the cascade;
this adds WHEN, not just HOW MUCH).

Per item, processed in ascending LLC order, walking weekly buckets:
    projected_available[t] = projected_available[t-1] + scheduled_receipts[t]
                             + planned_receipts[t] − gross_requirements[t]
    if projected_available[t] < safety_stock:
        need   = safety_stock − projected_available[t]
        qty    = lot-size(need, MOQ, multiple)
        planned ORDER RECEIPT at bucket t
        planned ORDER RELEASE at bucket (t − lead_time_weeks)   ← the offset
        if is_make: explode BOM → component dependent demand lands in the
                    RELEASE bucket (components are needed when production starts)

Because dependent demand is written into the RELEASE bucket of the parent, and
LLC ordering guarantees all parents are processed before the component, every
component sees its full time-phased dependent demand before it is netted.

A release bucket < 0 (need date − lead time is already in the past) = PAST_DUE
release → must expedite. This is the time-phased equivalent of the reorder
margin.

Bucketing: weekly, horizon 540 days (~78 weeks) from CURRENT_DATE.

Usage:
    DATABASE_URL=... python scripts/mrp_timephased.py [--top 25] [--horizon-days 540]
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("mrp_tp")

BASELINE = "00000000-0000-0000-0000-000000000001"
DEFAULT_LT_DAYS = 30  # fallback when no supplier / no planning lead time


def _guard(dsn, allow_dev):
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


def _m(cur, sql, params=None):
    return {r[0]: r[1] for r in cur.execute(sql, params or {}).fetchall()}


def _lot_size(need, moq, mult):
    qty = need
    if moq and qty < moq:
        qty = moq
    if mult and mult > 0:
        qty = math.ceil(qty / mult) * mult
    return qty


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Time-phased level-by-level MRP.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--top", type=int, default=25)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = _guard(args.dsn, args.allow_dev)
    logger.info("Time-phased MRP: DB=%s horizon=%dd", db, args.horizon_days)
    t0 = time.perf_counter()
    n_buckets = math.ceil(args.horizon_days / 7) + 1

    with psycopg.connect(args.dsn) as conn:
        cur = conn.cursor()
        b = {"b": BASELINE}
        horizon_start = cur.execute("SELECT CURRENT_DATE").fetchone()[0]

        def bucket(d):
            return max(0, (d - horizon_start).days // 7)

        # ── static per-item data ────────────────────────────────────
        llc = _m(cur, "SELECT component_item_id, MAX(llc) FROM bom_lines GROUP BY component_item_id")
        is_make = _m(cur, "SELECT item_id, bool_or(is_make) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        on_hand = _m(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type='OnHandSupply' GROUP BY item_id", b)
        safety = _m(cur, "SELECT item_id, SUM(COALESCE(safety_stock_qty,0)) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        buy_lt = _m(cur, "SELECT item_id, MIN(lead_time_days) FROM supplier_items WHERE lead_time_days IS NOT NULL GROUP BY item_id")
        make_lt = _m(cur, "SELECT item_id, MAX(lead_time_total_days) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        moq = _m(cur, "SELECT item_id, MIN(moq) FROM supplier_items WHERE moq IS NOT NULL GROUP BY item_id")
        mult = _m(cur, "SELECT item_id, MAX(order_multiple) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        names = _m(cur, "SELECT item_id, external_id FROM items")

        bom = defaultdict(list)
        for parent, comp, qpb, scrap in cur.execute(
            "SELECT bh.parent_item_id, bl.component_item_id, bl.quantity_per, bl.scrap_factor "
            "FROM bom_headers bh JOIN bom_lines bl ON bl.bom_id=bh.bom_id "
            "WHERE bh.effective_to IS NULL OR bh.effective_to > CURRENT_DATE"
        ).fetchall():
            bom[parent].append((comp, float(qpb), float(scrap or 0)))

        # ── bucketed independent demand + scheduled receipts ────────
        gross = defaultdict(lambda: defaultdict(float))   # item -> bucket -> qty
        for item, tref, qty in cur.execute(
            "SELECT item_id, time_ref, quantity FROM nodes WHERE scenario_id=%(b)s AND active "
            "AND node_type IN ('CustomerOrderDemand','ForecastDemand') AND time_ref IS NOT NULL AND quantity IS NOT NULL", b
        ).fetchall():
            if tref >= horizon_start:
                gross[item][bucket(tref)] += float(qty)

        sched = defaultdict(lambda: defaultdict(float))   # item -> bucket -> qty
        for item, tref, qty in cur.execute(
            "SELECT item_id, time_ref, quantity FROM nodes WHERE scenario_id=%(b)s AND active "
            "AND node_type IN ('PurchaseOrderSupply','WorkOrderSupply','TransferSupply') AND time_ref IS NOT NULL AND quantity IS NOT NULL", b
        ).fetchall():
            sched[item][bucket(tref)] += float(qty)  # past-dated → bucket 0

        load_s = round(time.perf_counter() - t0, 2)

        # ── involved set grouped by LLC ─────────────────────────────
        involved = set()
        for d in (llc, is_make, on_hand, safety, gross, sched):
            involved.update(d.keys())
        for parent, comps in bom.items():
            involved.add(parent)
            for c, _, _ in comps:
                involved.add(c)
        max_llc = max((llc.get(i, 0) for i in involved), default=0)
        by_level = defaultdict(list)
        for i in involved:
            by_level[llc.get(i, 0)].append(i)

    # ── cascade: process levels, time-phase each item ───────────────
    t1 = time.perf_counter()
    dependent = defaultdict(lambda: defaultdict(float))   # item -> bucket -> qty
    planned = []          # (item, qty, release_bucket, need_bucket, kind, past_due)
    n_wo = n_po = past_due = 0

    for level in range(0, max_llc + 1):
        for item in by_level[level]:
            g = gross.get(item)
            dep = dependent.get(item)
            if not g and not dep:
                continue
            make = bool(is_make.get(item, False))
            ss = float(safety.get(item, 0) or 0)
            lt_days = (make_lt.get(item) if make else buy_lt.get(item)) or DEFAULT_LT_DAYS
            lt_weeks = max(0, math.ceil(float(lt_days) / 7))
            item_moq = float(moq.get(item) or 0)
            item_mult = float(mult.get(item) or 0)

            pa = float(on_hand.get(item, 0) or 0)   # opening projected available
            for t in range(0, n_buckets):
                req = (g.get(t, 0.0) if g else 0.0) + (dep.get(t, 0.0) if dep else 0.0)
                pa = pa + sched.get(item, {}).get(t, 0.0) - req
                if pa < ss:
                    need = ss - pa
                    qty = _lot_size(need, item_moq, item_mult)
                    pa += qty
                    rel = t - lt_weeks
                    pd = rel < 0
                    if pd:
                        rel = 0
                        past_due += 1
                    planned.append((item, qty, rel, t, "WO" if make else "PO", pd))
                    if make:
                        n_wo += 1
                        for comp, qpb, scrap in bom.get(item, []):
                            dependent[comp][rel] += qty * qpb * (1.0 + scrap)
                    else:
                        n_po += 1
    cascade_s = round(time.perf_counter() - t1, 2)

    # ── report ──────────────────────────────────────────────────────
    by_relbucket = defaultdict(int)
    for _, _, rel, _, _, _ in planned:
        by_relbucket[rel] += 1

    logger.info("=" * 92)
    logger.info("TIME-PHASED MRP COMPLETE — load %.2fs + cascade %.2fs", load_s, cascade_s)
    logger.info("  Items in scope        : %d (max LLC %d, %d weekly buckets)", len(involved), max_llc, n_buckets)
    logger.info("  Planned WORK ORDERS   : %d", n_wo)
    logger.info("  Planned PURCHASE ORD. : %d", n_po)
    logger.info("  Planned-order lines   : %d", len(planned))
    logger.info("  PAST-DUE releases (need − lead time already elapsed → EXPEDITE): %d", past_due)
    logger.info("  Releases by week (next 8 weeks):")
    for wk in range(0, 8):
        logger.info("      week %-2d : %d planned-order releases", wk, by_relbucket.get(wk, 0))
    logger.info("=" * 92)

    # most urgent: past-due releases, biggest qty
    urgent = sorted([x for x in planned if x[5]], key=lambda x: -x[1])[: args.top]
    logger.info("TOP %d PAST-DUE planned-order releases (must expedite) — biggest qty:", args.top)
    logger.info("  %-16s %-4s %-5s %10s %10s %8s", "item", "kind", "llc", "qty", "need_wk", "rel_wk")
    for item, qty, rel, need, kind, pd in urgent:
        logger.info("  %-16s %-4s %-5d %10.0f %10d %8d", names.get(item, str(item)[:8]), kind, llc.get(item, 0), qty, need, rel)
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
