"""
shortage_scan.py — Virtual projection: detect first shortage per (item, location)
WITHOUT materializing ProjectedInventory rows.

Rationale
---------
Materializing N series × H daily buckets (12.5M+ rows) is slow, fragile, and
unnecessary for a first shortage signal. This script computes the running stock
balance directly from supply/demand nodes via a single window function and
returns, per series, the first date the balance goes negative.

This is the primitive the "shortage control tower" wedge actually needs:
"which item × location runs short, when, by how much" — answered in seconds.

PI materialization (bootstrap_pi + compute_pi_sql) is reserved for drill-down on
specific series an agent wants to inspect day-by-day — not for the broad scan.

Semantics (V1, documented caveats)
----------------------------------
- Supply nodes  (OnHandSupply, PurchaseOrderSupply, TransferSupply) = +quantity
- Demand nodes  (CustomerOrderDemand, ForecastDemand)               = -quantity
- Events ordered chronologically by time_ref; running cumulative balance.
- First shortage = first event date where running balance < 0.
- CAVEATS:
    * Forecast monthly buckets are treated as a lump at bucket_start
      (no intra-month spread — see PRORATA-V1.1).
    * Past-dated nodes (e.g. POs with 2020 dates) are included as-is; they sort
      first and fold into the opening balance. Real cleanup = data-source job.

Usage
-----
    DATABASE_URL=postgresql://... python scripts/shortage_scan.py
    python scripts/shortage_scan.py --top 30 --materialize
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time

import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("shortage_scan")

BASELINE = "00000000-0000-0000-0000-000000000001"

SUPPLY_TYPES = ("OnHandSupply", "PurchaseOrderSupply", "TransferSupply")
DEMAND_TYPES = ("CustomerOrderDemand", "ForecastDemand")


def _guard(dsn: str, allow_dev: bool) -> str:
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


SCAN_SQL = """
WITH events AS (
    SELECT item_id, location_id, time_ref AS d, quantity AS q
    FROM nodes
    WHERE scenario_id = %(b)s AND active = TRUE
      AND node_type = ANY(%(supply)s)
      AND item_id IS NOT NULL AND location_id IS NOT NULL
      AND time_ref IS NOT NULL AND quantity IS NOT NULL
    UNION ALL
    SELECT item_id, location_id, time_ref AS d, -quantity AS q
    FROM nodes
    WHERE scenario_id = %(b)s AND active = TRUE
      AND node_type = ANY(%(demand)s)
      AND item_id IS NOT NULL AND location_id IS NOT NULL
      AND time_ref IS NOT NULL AND quantity IS NOT NULL
),
running AS (
    SELECT item_id, location_id, d, q,
           SUM(q) OVER (PARTITION BY item_id, location_id
                        ORDER BY d, q          -- demand (negative q) settles after supply same day
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bal
    FROM events
),
first_short AS (
    SELECT DISTINCT ON (item_id, location_id)
           item_id, location_id,
           d   AS first_shortage_date,
           bal AS balance_at_shortage
    FROM running
    WHERE bal < 0
    ORDER BY item_id, location_id, d
)
SELECT fs.item_id, fs.location_id, fs.first_shortage_date, fs.balance_at_shortage,
       it.external_id AS item_ext, it.name AS item_name,
       loc.external_id AS loc_ext, loc.location_type AS loc_type
FROM first_short fs
JOIN items it     ON it.item_id = fs.item_id
JOIN locations loc ON loc.location_id = fs.location_id
ORDER BY fs.balance_at_shortage ASC
"""


ITEM_SCAN_SQL = """
WITH events AS (
    SELECT item_id, time_ref AS d, quantity AS q
    FROM nodes
    WHERE scenario_id = %(b)s AND active = TRUE
      AND node_type = ANY(%(supply)s)
      AND item_id IS NOT NULL AND time_ref IS NOT NULL AND quantity IS NOT NULL
    UNION ALL
    SELECT item_id, time_ref AS d, -quantity AS q
    FROM nodes
    WHERE scenario_id = %(b)s AND active = TRUE
      AND node_type = ANY(%(demand)s)
      AND item_id IS NOT NULL AND time_ref IS NOT NULL AND quantity IS NOT NULL
),
running AS (
    SELECT item_id, d, q,
           SUM(q) OVER (PARTITION BY item_id
                        ORDER BY d, q
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bal
    FROM events
),
first_short AS (
    SELECT DISTINCT ON (item_id)
           item_id, d AS first_shortage_date, bal AS balance_at_shortage
    FROM running
    WHERE bal < 0
    ORDER BY item_id, d
)
SELECT fs.item_id, fs.first_shortage_date, fs.balance_at_shortage,
       it.external_id AS item_ext, it.name AS item_name, it.item_type
FROM first_short fs
JOIN items it ON it.item_id = fs.item_id
ORDER BY fs.balance_at_shortage ASC
"""


REORDER_SQL = """
WITH events AS (
    SELECT item_id, time_ref AS d, quantity AS q
    FROM nodes
    WHERE scenario_id = %(b)s AND active = TRUE AND node_type = ANY(%(supply)s)
      AND item_id IS NOT NULL AND time_ref IS NOT NULL AND quantity IS NOT NULL
    UNION ALL
    SELECT item_id, time_ref AS d, -quantity AS q
    FROM nodes
    WHERE scenario_id = %(b)s AND active = TRUE AND node_type = ANY(%(demand)s)
      AND item_id IS NOT NULL AND time_ref IS NOT NULL AND quantity IS NOT NULL
),
running AS (
    SELECT item_id, d,
           SUM(q) OVER (PARTITION BY item_id ORDER BY d, q
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bal
    FROM events
),
first_short AS (
    SELECT DISTINCT ON (item_id) item_id, d AS fsd, bal
    FROM running WHERE bal < 0 ORDER BY item_id, d
),
item_lt AS (
    SELECT item_id, MIN(lead_time_days) AS lt_days
    FROM supplier_items WHERE lead_time_days IS NOT NULL
    GROUP BY item_id
)
SELECT it.external_id, it.name, fs.fsd, fs.bal, lt.lt_days,
       (fs.fsd - CURRENT_DATE)                                AS runway_days,
       (fs.fsd - CURRENT_DATE) - COALESCE(lt.lt_days, 99999)  AS margin_days
FROM first_short fs
JOIN items it ON it.item_id = fs.item_id
LEFT JOIN item_lt lt ON lt.item_id = fs.item_id
ORDER BY margin_days ASC
"""


def _classify(lt_days, runway_days, margin_days) -> str:
    if runway_days < 0:
        return "PAST_DUE"           # shortage date already in the past — data artifact / already happened
    if lt_days is None:
        return "NO_SOURCE"          # forward shortage + no supplier to reorder from
    if margin_days >= 0:
        return "RECOVERABLE"        # order now, arrives before shortage
    if margin_days >= -14:
        return "TIGHT"              # missed normal reorder by <=2wk → expedite
    return "CRITICAL"               # forward shortage, too late via normal reorder → expedite/alt source


REC_SQL = """
WITH events AS (
    SELECT item_id, time_ref AS d, quantity AS q
    FROM nodes WHERE scenario_id = %(b)s AND active = TRUE AND node_type = ANY(%(supply)s)
      AND item_id IS NOT NULL AND time_ref IS NOT NULL AND quantity IS NOT NULL
    UNION ALL
    SELECT item_id, time_ref AS d, -quantity AS q
    FROM nodes WHERE scenario_id = %(b)s AND active = TRUE AND node_type = ANY(%(demand)s)
      AND item_id IS NOT NULL AND time_ref IS NOT NULL AND quantity IS NOT NULL
),
running AS (
    SELECT item_id, d,
           SUM(q) OVER (PARTITION BY item_id ORDER BY d, q
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bal
    FROM events
),
first_short AS (
    SELECT DISTINCT ON (item_id) item_id, d AS fsd, bal
    FROM running WHERE bal < 0 ORDER BY item_id, d
),
fwd AS (
    SELECT item_id, fsd, bal FROM first_short WHERE fsd >= CURRENT_DATE
),
best_sup AS (
    SELECT DISTINCT ON (si.item_id)
           si.item_id, si.supplier_id, si.lead_time_days, si.moq, si.unit_cost, si.currency
    FROM supplier_items si WHERE si.lead_time_days IS NOT NULL
    ORDER BY si.item_id, si.is_preferred DESC, si.lead_time_days ASC
),
ipp_agg AS (
    SELECT item_id,
           SUM(COALESCE(safety_stock_qty, 0)) AS ss_pooled,
           MAX(order_multiple)                AS mult
    FROM item_planning_params WHERE effective_to IS NULL
    GROUP BY item_id
)
SELECT it.external_id, it.name,
       f.fsd, (-f.bal) AS deficit,
       s.external_id AS sup_ext, s.name AS sup_name, s.reliability_score,
       bs.lead_time_days, bs.moq, bs.unit_cost, bs.currency,
       COALESCE(ia.ss_pooled, 0) AS ss_pooled, ia.mult,
       (f.fsd - CURRENT_DATE)                       AS runway,
       (f.fsd - CURRENT_DATE) - bs.lead_time_days   AS margin
FROM fwd f
JOIN items it     ON it.item_id = f.item_id
JOIN best_sup bs  ON bs.item_id = f.item_id
JOIN suppliers s  ON s.supplier_id = bs.supplier_id
LEFT JOIN ipp_agg ia ON ia.item_id = f.item_id
ORDER BY margin ASC
"""


def scan_recommend(conn: psycopg.Connection, top: int) -> dict:
    cur = conn.cursor()
    cur.execute("SET work_mem = '256MB'")
    cur.execute("SET max_parallel_workers_per_gather = 0")
    t0 = time.perf_counter()
    rows = cur.execute(
        REC_SQL,
        {"b": BASELINE, "supply": list(SUPPLY_TYPES), "demand": list(DEMAND_TYPES)},
    ).fetchall()
    elapsed = time.perf_counter() - t0

    recs = []
    by_action: dict[str, int] = {}
    spend_by_ccy: dict[str, float] = {}
    for (ext, name, fsd, deficit, sup_ext, sup_name, rel, lt, moq,
         unit_cost, ccy, ss, mult, runway, margin) in rows:
        deficit = float(deficit or 0)
        ss = float(ss or 0)
        # quantity = cover deficit + restore pooled safety stock
        qty = deficit + ss
        if moq:
            qty = max(qty, float(moq))
        if mult:
            qty = math.ceil(qty / float(mult)) * float(mult)
        qty = round(qty, 2)
        cost = round(qty * float(unit_cost), 2) if unit_cost is not None else None
        ccy = ccy or "EUR"
        if margin < -14:
            action = "EXPEDITE"
        elif margin < 0:
            action = "ORDER_RUSH"
        else:
            action = "ORDER_NOW"
        by_action[action] = by_action.get(action, 0) + 1
        if cost is not None:
            spend_by_ccy[ccy] = spend_by_ccy.get(ccy, 0.0) + cost
        recs.append((ext, name, fsd, deficit, qty, cost, ccy, sup_ext, sup_name,
                     rel, lt, runway, margin, action))

    return {
        "elapsed_s": round(elapsed, 2),
        "n_recs": len(recs),
        "by_action": by_action,
        "spend_by_ccy": spend_by_ccy,
        "recs": recs,
        "top": top,
    }


def scan_reorder(conn: psycopg.Connection, top: int) -> dict:
    cur = conn.cursor()
    cur.execute("SET work_mem = '256MB'")
    cur.execute("SET max_parallel_workers_per_gather = 0")
    t0 = time.perf_counter()
    rows = cur.execute(
        REORDER_SQL,
        {"b": BASELINE, "supply": list(SUPPLY_TYPES), "demand": list(DEMAND_TYPES)},
    ).fetchall()
    elapsed = time.perf_counter() - t0

    classified = []
    breakdown: dict[str, int] = {}
    for ext, name, fsd, bal, lt, runway, margin in rows:
        cls = _classify(lt, runway, margin)
        breakdown[cls] = breakdown.get(cls, 0) + 1
        classified.append((ext, name, fsd, bal, lt, runway, margin, cls))

    # Actionable = forward-looking with a real lead time, ranked by smallest margin
    actionable = sorted(
        [c for c in classified if c[7] in ("CRITICAL", "TIGHT")],
        key=lambda c: c[6],  # margin asc
    )

    return {
        "elapsed_s": round(elapsed, 2),
        "total_short": len(rows),
        "breakdown": breakdown,
        "actionable": actionable,
        "top": top,
    }


def scan_by_item(conn: psycopg.Connection, top: int) -> dict:
    cur = conn.cursor()
    cur.execute("SET work_mem = '256MB'")
    cur.execute("SET max_parallel_workers_per_gather = 0")

    t0 = time.perf_counter()
    rows = cur.execute(
        ITEM_SCAN_SQL,
        {"b": BASELINE, "supply": list(SUPPLY_TYPES), "demand": list(DEMAND_TYPES)},
    ).fetchall()
    elapsed = time.perf_counter() - t0

    total_items = cur.execute(
        """
        SELECT COUNT(DISTINCT item_id) FROM nodes
        WHERE scenario_id = %(b)s AND active = TRUE
          AND node_type = ANY(%(all)s) AND item_id IS NOT NULL
        """,
        {"b": BASELINE, "all": list(SUPPLY_TYPES + DEMAND_TYPES)},
    ).fetchone()[0]

    breakdown: dict[str, int] = {}
    for r in rows:
        breakdown[r[5]] = breakdown.get(r[5], 0) + 1  # item_type

    return {
        "elapsed_s": round(elapsed, 2),
        "total_items": total_items,
        "items_with_shortage": len(rows),
        "breakdown": breakdown,
        "rows": rows,
        "top": top,
    }


def scan(conn: psycopg.Connection, top: int, loc_types: list[str] | None) -> dict:
    cur = conn.cursor()
    cur.execute("SET work_mem = '256MB'")
    cur.execute("SET max_parallel_workers_per_gather = 0")

    t0 = time.perf_counter()
    rows = cur.execute(
        SCAN_SQL,
        {"b": BASELINE, "supply": list(SUPPLY_TYPES), "demand": list(DEMAND_TYPES)},
    ).fetchall()
    elapsed = time.perf_counter() - t0

    # Breakdown by location_type (col index 7 = loc_type)
    breakdown: dict[str, int] = {}
    for r in rows:
        breakdown[r[7]] = breakdown.get(r[7], 0) + 1

    # Optional filter for the top list
    if loc_types:
        rows = [r for r in rows if r[7] in loc_types]

    # Total series scanned (distinct item×loc with any supply/demand)
    total_series = cur.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT DISTINCT item_id, location_id FROM nodes
            WHERE scenario_id = %(b)s AND active = TRUE
              AND node_type = ANY(%(all)s)
              AND item_id IS NOT NULL AND location_id IS NOT NULL
        ) s
        """,
        {"b": BASELINE, "all": list(SUPPLY_TYPES + DEMAND_TYPES)},
    ).fetchone()[0]

    return {
        "elapsed_s": round(elapsed, 2),
        "total_series": total_series,
        "series_with_shortage": len(rows),
        "breakdown": breakdown,
        "rows": rows,
        "top": top,
        "loc_types_filter": loc_types,
    }


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Virtual shortage scan (no PI materialization).")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--location-types", default=None,
                   help="comma-separated filter for the top list, e.g. 'plant,dc,warehouse'")
    p.add_argument("--by-item", action="store_true",
                   help="aggregate net supply vs demand per ITEM (ignore location)")
    p.add_argument("--reorder", action="store_true",
                   help="item-level + reorder feasibility (RECOVERABLE / TIGHT / CRITICAL / NO_SOURCE)")
    p.add_argument("--recommend", action="store_true",
                   help="quantified purchase recommendations (supplier, qty, cost, action)")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2

    loc_types = [t.strip() for t in args.location_types.split(",")] if args.location_types else None

    db = _guard(args.dsn, args.allow_dev)

    # ── Recommendation mode ─────────────────────────────────────────
    if args.recommend:
        logger.info("Purchase recommendations: DB=%s", db)
        with psycopg.connect(args.dsn) as conn:
            r = scan_recommend(conn, args.top)
        logger.info("=" * 100)
        logger.info("PURCHASE RECOMMENDATIONS DONE in %.2fs", r["elapsed_s"])
        logger.info("  Forward shortages with a supplier : %d", r["n_recs"])
        logger.info("  By action:")
        for act in ("EXPEDITE", "ORDER_RUSH", "ORDER_NOW"):
            logger.info("      %-12s %d", act, r["by_action"].get(act, 0))
        logger.info("  Estimated spend to cover:")
        for ccy, amt in sorted(r["spend_by_ccy"].items(), key=lambda x: -x[1]):
            logger.info("      %-6s %15.2f", ccy, amt)
        logger.info("=" * 100)
        logger.info("TOP %d recommendations (most urgent first):", r["top"])
        logger.info("  %-14s %-10s %-11s %9s %13s %-9s %-12s %s",
                    "item", "by_date", "action", "order_qty", "cost", "ccy", "supplier", "name")
        for (ext, name, fsd, deficit, qty, cost, ccy, sup_ext, sup_name,
             rel, lt, runway, margin, action) in r["recs"][: r["top"]]:
            nm = (name or "")[:24]
            cost_s = f"{cost:,.0f}" if cost is not None else "—"
            logger.info("  %-14s %-10s %-11s %9.0f %13s %-9s %-12s %s",
                        ext, str(fsd), action, qty, cost_s, ccy, sup_ext, nm)
        logger.info("=" * 100)
        return 0

    # ── Reorder feasibility mode ────────────────────────────────────
    if args.reorder:
        logger.info("Shortage scan (REORDER feasibility): DB=%s", db)
        with psycopg.connect(args.dsn) as conn:
            r = scan_reorder(conn, args.top)
        order = ["CRITICAL", "TIGHT", "RECOVERABLE", "NO_SOURCE", "PAST_DUE"]
        logger.info("=" * 90)
        logger.info("REORDER FEASIBILITY SCAN DONE in %.2fs", r["elapsed_s"])
        logger.info("  Net-short items       : %d", r["total_short"])
        logger.info("  Action classes:")
        for cls in order:
            n = r["breakdown"].get(cls, 0)
            pct = (100 * n / r["total_short"]) if r["total_short"] else 0
            logger.info("      %-12s %6d  (%4.1f%%)", cls, n, pct)
        n_act = len(r["actionable"])
        logger.info("  → ACTIONABLE (forward shortage, has supplier, reorder window tight/missed): %d", n_act)
        logger.info("=" * 90)
        logger.info("TOP %d ACTIONABLE alerts (smallest reorder margin first):", r["top"])
        logger.info("  %-15s %-12s %12s %7s %7s %7s  %-11s %s", "item", "shortage", "net_qty", "lt_d", "runway", "margin", "class", "name")
        for ext, name, fsd, bal, lt, runway, margin, cls in r["actionable"][: r["top"]]:
            nm = (name or "")[:26]
            logger.info("  %-15s %-12s %12.0f %7s %7s %7s  %-11s %s",
                        ext, str(fsd), float(bal), str(lt), str(runway), str(margin), cls, nm)
        logger.info("=" * 90)
        return 0

    # ── Item-level aggregated mode ──────────────────────────────────
    if args.by_item:
        logger.info("Shortage scan (BY ITEM, all locations netted): DB=%s", db)
        with psycopg.connect(args.dsn) as conn:
            r = scan_by_item(conn, args.top)
        pct = (100 * r["items_with_shortage"] / r["total_items"]) if r["total_items"] else 0
        logger.info("=" * 78)
        logger.info("ITEM-LEVEL SHORTAGE SCAN DONE in %.2fs", r["elapsed_s"])
        logger.info("  Items scanned         : %d", r["total_items"])
        logger.info("  Items net-short       : %d (%.1f%%)", r["items_with_shortage"], pct)
        logger.info("  Breakdown by item_type:")
        for it_t, n in sorted(r["breakdown"].items(), key=lambda x: -x[1]):
            logger.info("      %-20s %d", it_t, n)
        logger.info("=" * 78)
        logger.info("TOP %d net-short items — worst balance first:", r["top"])
        logger.info("  %-16s %-14s %-12s %14s   %s", "item", "item_type", "date", "net_balance", "name")
        for row in r["rows"][: r["top"]]:
            item_id, date, bal, item_ext, item_name, item_type = row
            name = (item_name or "")[:32]
            logger.info("  %-16s %-14s %-12s %14.1f   %s", item_ext, item_type, str(date), float(bal), name)
        logger.info("=" * 78)
        return 0

    logger.info("Shortage scan: DB=%s  filter=%s", db, loc_types or "all")

    with psycopg.connect(args.dsn) as conn:
        r = scan(conn, args.top, loc_types)

    total_short = sum(r["breakdown"].values())
    pct = (100 * total_short / r["total_series"]) if r["total_series"] else 0
    logger.info("=" * 70)
    logger.info("SHORTAGE SCAN DONE in %.2fs", r["elapsed_s"])
    logger.info("  Series scanned        : %d", r["total_series"])
    logger.info("  Series with shortage  : %d (%.1f%%)", total_short, pct)
    logger.info("  Breakdown by location_type:")
    for lt, n in sorted(r["breakdown"].items(), key=lambda x: -x[1]):
        logger.info("      %-20s %d", lt, n)
    logger.info("=" * 70)
    label = f"(filter: {','.join(loc_types)})" if loc_types else "(all locations)"
    logger.info("TOP %d shortages %s — worst balance first:", r["top"], label)
    logger.info("  %-16s %-14s %-10s %-12s %14s   %s", "item", "loc", "loc_type", "date", "balance", "name")
    for row in r["rows"][: r["top"]]:
        item_id, loc_id, date, bal, item_ext, item_name, loc_ext, loc_type = row
        name = (item_name or "")[:30]
        logger.info("  %-16s %-14s %-10s %-12s %14.1f   %s", item_ext, loc_ext, loc_type, str(date), float(bal), name)
    logger.info("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
