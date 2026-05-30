"""
compute_cost_rollup.py — populate items.standard_cost by bottom-up BOM roll-up.

Cost lives only on supplier_items.unit_cost (per supplier link), so made items
and uncosted buy items have no value. This computes a standard unit cost for
EVERY reachable item and writes it to items.standard_cost / cost_currency:

  - bought item (no active BOM): cost = its supplier unit_cost (cost-aware pick)
  - made item   (has active BOM): cost = Σ component_cost × qty_per × (1+scrap),
                                  rolled up deepest-LLC-first so components are
                                  costed before their parents

A made item is only costed once ALL its components are costed; items whose
roll-up is incomplete (a component has no cost anywhere) are left NULL and
reported. No FX conversion — a roll-up mixing currencies is summed numerically
and flagged (the pilote is ~99% USD).

Refuses non-ootils DBs (mrp_core.guard_db). Idempotent — safe to re-run.

Usage:
    DATABASE_URL=... python scripts/compute_cost_rollup.py [--allow-dev] [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import defaultdict

import psycopg
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("cost_rollup")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Populate items.standard_cost via bottom-up BOM roll-up.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--allow-dev", action="store_true")
    p.add_argument("--dry-run", action="store_true", help="compute + report, do not write")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("Cost roll-up on DB=%s%s", db, "  (DRY-RUN)" if args.dry_run else "")
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn)  # gives bom, llc, is_make, unit_cost, cost_ccy, names

        cost = {}          # item -> unit cost (numeric)
        ccy = {}           # item -> currency
        src = {}           # item -> 'supplier' | 'rollup'
        mixed = 0          # roll-ups that summed >1 currency
        incomplete = []    # made items left uncosted (missing component cost)

        # seed bought items from supplier costs
        for item, uc in d.unit_cost.items():
            if item not in d.bom:               # buy = no active BOM
                cost[item] = float(uc)
                ccy[item] = d.cost_ccy.get(item, "USD")
                src[item] = "supplier"

        # roll up made items deepest-LLC-first (components before parents)
        all_items = set(d.names) | set(d.bom) | set(d.llc)
        for item in sorted(all_items, key=lambda i: -d.llc.get(i, 0)):
            if item not in d.bom or item in cost:
                continue
            total = 0.0
            ccys = set()
            ok = True
            for comp, qpb, scrap in d.bom[item]:
                c = cost.get(comp)
                if c is None:
                    ok = False
                    break
                total += c * qpb * (1.0 + scrap)
                ccys.add(ccy.get(comp, "USD"))
            if not ok:
                incomplete.append(item)
                continue
            cost[item] = round(total, 6)
            ccy[item] = next(iter(ccys)) if len(ccys) == 1 else "USD"
            if len(ccys) > 1:
                mixed += 1
            src[item] = "rollup"

        n_supplier = sum(1 for s in src.values() if s == "supplier")
        n_rollup = sum(1 for s in src.values() if s == "rollup")
        total_items = len(d.names)
        logger.info("=" * 88)
        logger.info("COST ROLL-UP — computed %d / %d items (%.1f%%) in %.2fs",
                    len(cost), total_items, 100.0 * len(cost) / total_items if total_items else 0, time.perf_counter() - t0)
        logger.info("   from supplier unit_cost : %d", n_supplier)
        logger.info("   from BOM roll-up        : %d", n_rollup)
        logger.info("   made items still uncosted (missing a component cost): %d", len(incomplete))
        if mixed:
            logger.info("   ⚠ roll-ups mixing >1 currency (numeric sum, labelled USD): %d", mixed)
        logger.info("=" * 88)

        if args.dry_run:
            logger.info("DRY-RUN — no writes.")
            return 0

        rows = [(round(cost[i], 6), ccy.get(i, "USD"), i) for i in cost]
        cur = conn.cursor()
        cur.executemany("UPDATE items SET standard_cost=%s, cost_currency=%s, updated_at=now() WHERE item_id=%s", rows)
        conn.commit()
        logger.info("Wrote standard_cost for %d items.", len(rows))

        # quick sanity: a few rolled-up made items
        sample = [i for i in cost if src.get(i) == "rollup"][:6]
        if sample:
            logger.info("Sample rolled-up made items:")
            for i in sample:
                logger.info("   %-16s %14.4f %-4s  (%d components)",
                            d.names.get(i, str(i)[:8]), cost[i], ccy.get(i, "USD"), len(d.bom.get(i, [])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
