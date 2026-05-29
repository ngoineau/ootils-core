"""
mrp_peg.py — pegging: trace each component's dependent demand back to the
finished goods (demand origins) that generated it.

Pegging is what makes MRP explainable (North Star): a planner cannot arbitrate a
huge component requirement without knowing WHICH end products drive it.

Method — origin attribution during the LLC cascade (aggregate over horizon):
  - An item's incoming demand mix = its own independent demand (origin = itself,
    a directly-sold item) + the dependent demand inherited from its parents
    (each already attributed to ultimate origins).
  - When a make item explodes onto a component, the exploded quantity is
    distributed across the parent's origin mix proportionally, so the component
    inherits a {finished_good: qty} breakdown that bottoms out at real demand
    sources.

Usage:
    DATABASE_URL=... python scripts/mrp_peg.py                 # top components + origins
    DATABASE_URL=... python scripts/mrp_peg.py --item Q0152700 # peg one component
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
logger = logging.getLogger("mrp_peg")
BASELINE = "00000000-0000-0000-0000-000000000001"


def _guard(dsn, allow_dev):
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


def _m(cur, sql, params=None):
    return {r[0]: r[1] for r in cur.execute(sql, params or {}).fetchall()}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="MRP pegging — trace component demand to finished goods.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--item", default=None, help="component external_id to peg (default: top components)")
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = _guard(args.dsn, args.allow_dev)
    logger.info("MRP pegging: DB=%s", db)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        cur = conn.cursor()
        b = {"b": BASELINE}
        llc = _m(cur, "SELECT component_item_id, MAX(llc) FROM bom_lines GROUP BY component_item_id")
        is_make = _m(cur, "SELECT item_id, bool_or(is_make) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        on_hand = _m(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type='OnHandSupply' GROUP BY item_id", b)
        firm = _m(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type IN ('PurchaseOrderSupply','WorkOrderSupply','TransferSupply') GROUP BY item_id", b)
        safety = _m(cur, "SELECT item_id, SUM(COALESCE(safety_stock_qty,0)) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        indep = _m(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type IN ('CustomerOrderDemand','ForecastDemand') AND time_ref >= CURRENT_DATE GROUP BY item_id", b)
        names = _m(cur, "SELECT item_id, external_id FROM items")
        ext_to_id = {v: k for k, v in names.items()}

        bom = defaultdict(list)
        for parent, comp, qpb, scrap in cur.execute(
            "SELECT bh.parent_item_id, bl.component_item_id, bl.quantity_per, bl.scrap_factor "
            "FROM bom_headers bh JOIN bom_lines bl ON bl.bom_id=bh.bom_id "
            "WHERE bh.effective_to IS NULL OR bh.effective_to > CURRENT_DATE"
        ).fetchall():
            bom[parent].append((comp, float(qpb), float(scrap or 0)))

    # ── cascade with origin attribution ─────────────────────────────
    involved = set()
    for d in (llc, is_make, on_hand, firm, safety, indep):
        involved.update(d.keys())
    for parent, comps in bom.items():
        involved.add(parent)
        for c, _, _ in comps:
            involved.add(c)
    max_llc = max((llc.get(i, 0) for i in involved), default=0)
    by_level = defaultdict(list)
    for i in involved:
        by_level[llc.get(i, 0)].append(i)

    dependent = defaultdict(float)                       # item -> total dependent demand
    origin = defaultdict(lambda: defaultdict(float))     # item -> {origin_fg: qty}

    for level in range(0, max_llc + 1):
        for item in by_level[level]:
            ind = float(indep.get(item, 0) or 0)
            dep = dependent.get(item, 0.0)
            gross = ind + dep
            if gross <= 0:
                continue
            avail = float(on_hand.get(item, 0) or 0) + float(firm.get(item, 0) or 0)
            ss = float(safety.get(item, 0) or 0)
            net = gross + ss - avail
            if net <= 0:
                continue
            # origin mix of this item's demand
            mix = dict(origin.get(item, {}))   # inherited (dependent) origins
            if ind > 0:
                mix[item] = mix.get(item, 0.0) + ind
            total_mix = sum(mix.values()) or 1.0
            if bool(is_make.get(item, False)):
                for comp, qpb, scrap in bom.get(item, []):
                    contrib = net * qpb * (1.0 + scrap)
                    dependent[comp] += contrib
                    oc = origin[comp]
                    for fg, w in mix.items():
                        oc[fg] += contrib * (w / total_mix)

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("Pegging computed in %.2fs (%d items, max LLC %d)", elapsed, len(involved), max_llc)

    if args.item:
        iid = ext_to_id.get(args.item)
        if iid is None:
            logger.error("item %s not found", args.item)
            return 3
        org = origin.get(iid, {})
        total = sum(org.values())
        logger.info("=" * 80)
        logger.info("PEGGING — %s  (%s)", args.item, names.get(iid, "")[:40] if False else "")
        logger.info("  Total dependent demand: %.0f  (LLC %d)", dependent.get(iid, 0), llc.get(iid, 0))
        logger.info("  Driven by finished goods / demand origins:")
        logger.info("  %-16s %14s %8s", "origin_FG", "qty", "share")
        for fg, q in sorted(org.items(), key=lambda x: -x[1])[: args.top]:
            logger.info("  %-16s %14.0f %7.1f%%", names.get(fg, str(fg)[:8]), q, 100 * q / (total or 1))
        logger.info("=" * 80)
    else:
        # top components by dependent demand, with top-3 origins inline
        comps = sorted(((c, dependent[c]) for c in dependent if llc.get(c, 0) >= 1), key=lambda x: -x[1])[: args.top]
        logger.info("=" * 96)
        logger.info("TOP %d components by dependent demand — pegged to their main finished goods:", args.top)
        logger.info("=" * 96)
        for c, dem in comps:
            org = origin.get(c, {})
            total = sum(org.values()) or 1
            top3 = sorted(org.items(), key=lambda x: -x[1])[:3]
            peg = ", ".join(f"{names.get(fg, str(fg)[:8])} {100*q/total:.0f}%" for fg, q in top3)
            logger.info("  %-16s LLC%-2d dem=%-12.0f ← %s", names.get(c, str(c)[:8]), llc.get(c, 0), dem, peg)
        logger.info("=" * 96)
    return 0


if __name__ == "__main__":
    sys.exit(main())
