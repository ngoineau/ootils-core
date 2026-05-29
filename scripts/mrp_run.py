"""
mrp_run.py — level-by-level MRP with LLC ordering, make/buy planned orders,
and BOM explosion of dependent demand.

THE non-negotiable: items are processed in ascending LLC order, so a component's
dependent demand is fully accumulated from ALL its parents (which sit at strictly
lower LLC, hence processed earlier) before it is netted. Flat item-level netting
cannot do this.

Per item, at its level:
    gross  = independent demand (CO + forecast, forward) + dependent demand
             (accumulated from parent explosions already processed)
    net    = max(0, gross + safety_stock − on_hand − firm_inbound)
    if net > 0:
        is_make  → Planned WO, explode BOM: each component gets
                   dependent += net × qty_per × (1 + scrap)
        is_make=false → Planned PO (stops the cascade for this branch)

CAVEAT (V1): aggregate over the horizon — NOT yet time-phased. The LLC cascade
(the correctness backbone) is exact; lead-time offsetting per level (planned-
order release = need − lead_time, bucketed) is the next layer.

Usage:
    DATABASE_URL=... python scripts/mrp_run.py [--top 20]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import defaultdict

import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("mrp_run")

BASELINE = "00000000-0000-0000-0000-000000000001"


def _guard(dsn, allow_dev):
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


def _load_map(cur, sql, params=None):
    return {r[0]: r[1] for r in cur.execute(sql, params or {}).fetchall()}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Level-by-level MRP (LLC-ordered).")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = _guard(args.dsn, args.allow_dev)
    logger.info("MRP run: DB=%s", db)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        cur = conn.cursor()
        b = {"b": BASELINE}

        # ── Load planning data into memory ──────────────────────────
        llc = _load_map(cur, "SELECT component_item_id, MAX(llc) FROM bom_lines GROUP BY component_item_id")
        is_make = _load_map(cur, "SELECT item_id, bool_or(is_make) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        on_hand = _load_map(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type='OnHandSupply' GROUP BY item_id", b)
        firm = _load_map(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type IN ('PurchaseOrderSupply','WorkOrderSupply','TransferSupply') GROUP BY item_id", b)
        safety = _load_map(cur, "SELECT item_id, SUM(COALESCE(safety_stock_qty,0)) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        indep = _load_map(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type IN ('CustomerOrderDemand','ForecastDemand') AND time_ref >= CURRENT_DATE GROUP BY item_id", b)

        # BOM: parent -> [(component, qty_per, scrap)]
        bom = defaultdict(list)
        for parent, comp, qpb, scrap in cur.execute(
            "SELECT bh.parent_item_id, bl.component_item_id, bl.quantity_per, bl.scrap_factor "
            "FROM bom_headers bh JOIN bom_lines bl ON bl.bom_id=bh.bom_id "
            "WHERE bh.effective_to IS NULL OR bh.effective_to > CURRENT_DATE"
        ).fetchall():
            bom[parent].append((comp, float(qpb), float(scrap or 0)))

        names = _load_map(cur, "SELECT item_id, external_id FROM items")

        load_s = round(time.perf_counter() - t0, 2)

        # ── Build involved-item set, grouped by LLC ─────────────────
        involved = set()
        for d in (llc, is_make, on_hand, firm, safety, indep):
            involved.update(d.keys())
        for parent, comps in bom.items():
            involved.add(parent)
            for comp, _, _ in comps:
                involved.add(comp)
        max_llc = max((llc.get(i, 0) for i in involved), default=0)
        by_level = defaultdict(list)
        for i in involved:
            by_level[llc.get(i, 0)].append(i)

        # ── Cascade LLC 0 → max ─────────────────────────────────────
        t1 = time.perf_counter()
        dependent = defaultdict(float)
        planned_wo, planned_po = [], []
        level_orders = defaultdict(lambda: [0, 0])  # llc -> [#WO, #PO]
        for level in range(0, max_llc + 1):
            for item in by_level[level]:
                gross = float(indep.get(item, 0) or 0) + dependent.get(item, 0)
                if gross <= 0:
                    continue
                avail = float(on_hand.get(item, 0) or 0) + float(firm.get(item, 0) or 0)
                ss = float(safety.get(item, 0) or 0)
                net = gross + ss - avail
                if net <= 0:
                    continue
                make = bool(is_make.get(item, False))
                if make:
                    planned_wo.append((item, net, level))
                    level_orders[level][0] += 1
                    for comp, qpb, scrap in bom.get(item, []):
                        dependent[comp] += net * qpb * (1.0 + scrap)
                else:
                    planned_po.append((item, net, level))
                    level_orders[level][1] += 1
        cascade_s = round(time.perf_counter() - t1, 2)

    total_dep = sum(dependent.values())
    # Component-level planned orders (llc >= 1) = the demand invisible to flat scan
    comp_wo = [x for x in planned_wo if x[2] >= 1]
    comp_po = [x for x in planned_po if x[2] >= 1]

    logger.info("=" * 84)
    logger.info("MRP RUN COMPLETE — load %.2fs + cascade %.2fs", load_s, cascade_s)
    logger.info("  Items in scope        : %d (max LLC %d)", len(involved), max_llc)
    logger.info("  Planned WORK ORDERS   : %d  (make items needing production)", len(planned_wo))
    logger.info("  Planned PURCHASE ORD. : %d  (buy items needing procurement)", len(planned_po))
    logger.info("  Dependent demand generated by BOM explosion: %.0f units total", total_dep)
    logger.info("  Planned orders at LLC >= 1 (DEPENDENT, invisible to flat scan):")
    logger.info("      component WOs : %d", len(comp_wo))
    logger.info("      component POs : %d", len(comp_po))
    logger.info("  Planned orders by level (#WO / #PO):")
    for lvl in range(0, max_llc + 1):
        wo, po = level_orders[lvl]
        if wo or po:
            logger.info("      LLC %-2d : %5d WO / %5d PO", lvl, wo, po)
    logger.info("=" * 84)

    # Top component purchase needs (llc>=1, buy) by net qty
    comp_po_sorted = sorted(comp_po, key=lambda x: -x[1])[: args.top]
    logger.info("TOP %d component PURCHASE needs (LLC>=1, buy) — net qty:", args.top)
    logger.info("  %-16s %-5s %14s", "item", "llc", "net_qty")
    for item, net, lvl in comp_po_sorted:
        logger.info("  %-16s %-5d %14.0f", names.get(item, str(item)[:8]), lvl, net)
    logger.info("=" * 84)
    return 0


if __name__ == "__main__":
    sys.exit(main())
