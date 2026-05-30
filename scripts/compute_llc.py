"""
compute_llc.py — compute Low-Level Codes (LLC) for every item in the BOM graph.

LLC is the FOUNDATION of correct MRP: each item must be processed at the lowest
level it appears in ANY bill of materials, so that all its parent (dependent)
demand is known before it is netted. Without LLC-ordered, level-by-level
processing, dependent-demand MRP gives wrong results — a component that is
level-1 in one BOM and level-3 in another would be netted too early.

Definition:
    LLC(item) = 0                          if the item is never a component
    LLC(item) = max(LLC(parent)) + 1       over all BOMs where it is a component

Algorithm: iterative relaxation over the parent→component edges until stable.
A bounded pass count detects BOM cycles (a data error) and reports the offenders.

Persists the result into bom_lines.llc (item-level LLC of each line's component).

Usage:
    DATABASE_URL=... python scripts/compute_llc.py
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
logger = logging.getLogger("compute_llc")

MAX_PASSES = 100  # > deepest realistic BOM; exceeding it ⇒ a cycle exists


def _guard(dsn, allow_dev):
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Compute Low-Level Codes for the BOM graph.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = _guard(args.dsn, args.allow_dev)
    logger.info("Compute LLC: DB=%s", db)

    t0 = time.perf_counter()
    with psycopg.connect(args.dsn) as conn:
        cur = conn.cursor()
        # 1. Load parent→component edges (active BOM headers only)
        edges = cur.execute(
            """
            SELECT bh.parent_item_id, bl.component_item_id
            FROM bom_headers bh
            JOIN bom_lines bl ON bl.bom_id = bh.bom_id
            WHERE bh.effective_to IS NULL OR bh.effective_to > CURRENT_DATE
            """
        ).fetchall()
        logger.info("Loaded %d BOM edges", len(edges))

        # adjacency: parent -> [components]
        children = defaultdict(list)
        all_items = set()
        for parent, comp in edges:
            children[parent].append(comp)
            all_items.add(parent)
            all_items.add(comp)

        # 2. Iterative relaxation
        llc = defaultdict(int)  # default 0
        changed = True
        passes = 0
        while changed and passes < MAX_PASSES:
            changed = False
            passes += 1
            for parent, comps in children.items():
                base = llc[parent] + 1
                for comp in comps:
                    if base > llc[comp]:
                        llc[comp] = base
                        changed = True

        if changed:
            # Cycle: find items still at the max level (likely in/near the cycle)
            mx = max(llc.values()) if llc else 0
            offenders = [i for i, v in llc.items() if v >= mx][:10]
            logger.error("BOM CYCLE detected (did not converge in %d passes). "
                         "Suspect items (UUIDs): %s", MAX_PASSES, offenders)
            return 1

        max_llc = max(llc.values()) if llc else 0
        logger.info("Converged in %d passes. Max LLC depth = %d", passes, max_llc)

        # 3. Persist into bom_lines.llc (component's item-level LLC)
        #    Build a temp mapping and bulk-update.
        cur.execute("CREATE TEMP TABLE _llc(item_id UUID PRIMARY KEY, llc INT) ON COMMIT DROP")
        cur.executemany("INSERT INTO _llc(item_id, llc) VALUES (%s, %s)",
                        [(i, v) for i, v in llc.items()])
        updated = cur.execute(
            """
            UPDATE bom_lines bl SET llc = l.llc
            FROM _llc l WHERE l.item_id = bl.component_item_id
            """
        ).rowcount
        conn.commit()

        # 4. Distribution report (item-level)
        dist = defaultdict(int)
        for v in llc.values():
            dist[v] += 1
        # items never appearing as a component = LLC 0 top-level (not in llc dict beyond default)

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("=" * 60)
    logger.info("LLC COMPUTED in %.2fs", elapsed)
    logger.info("  Items in BOM graph : %d", len(all_items))
    logger.info("  bom_lines updated  : %d", updated)
    logger.info("  LLC distribution (level : #items that bottom out there):")
    for lvl in sorted(dist):
        logger.info("      LLC %-3d : %d items", lvl, dist[lvl])
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
