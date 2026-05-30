"""
mrp_projected_stock.py — projected inventory position (CLI over mrp_core).

TWO views, because of what the pilote network actually allows:

  TOTAL (item-pooled, consumption-correct): the projected on-hand position over
    time. Per item, pa = on_hand + Σ(scheduled receipts − consumed demand) walked
    weekly; sampled at each month-end. Reported as inventory VALUE held
    (Σ max(0, projected_on_hand) × cost) plus net units. This is the correct
    aggregate stock trajectory.

  BY LOCATION (current snapshot only): where stock physically sits today, valued.
    Forward per-location projection is intentionally NOT produced: in this dataset
    all demand sits on customer_virtual while all supply sits on real sites
    (dc/plant/warehouse) — the distribution network isn't connected (LANES-LATER
    #51). Projecting per site would show real sites accumulating forever and
    customer_virtual going infinitely negative — meaningless. It becomes
    meaningful once lanes route customer demand to source sites.

Demand = consumed (max_only / DTF / prorated / multi-location-deduped). Valued via
the cost fallback chain (supplier unit_cost → item standard_cost); unpriced stock
is surfaced.

Usage:
    DATABASE_URL=... python scripts/mrp_projected_stock.py [--months-out 15]
"""
from __future__ import annotations

import argparse
import datetime as _dt
import logging
import os
import sys
import time
from collections import defaultdict

import psycopg
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("projected_stock")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Projected inventory position (CLI over mrp_core).")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--months-out", type=int, default=15, help="how many monthly buckets to print")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("Projected stock: DB=%s", db)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
        gross = core.consume_demand(d)
        hs = d.horizon_start

        # ── TOTAL: project each item's on-hand, sample at month-end, value held ──
        inv_val = defaultdict(lambda: defaultdict(float))   # month -> ccy -> held value
        inv_units = defaultdict(float)                       # month -> net units
        unpriced_oh = 0.0
        items = set(d.on_hand) | set(gross) | set(d.sched_b)
        for item in items:
            pa = float(d.on_hand.get(item, 0) or 0)
            g = gross.get(item, {})
            sc = d.sched_b.get(item, {})
            if pa == 0 and not g and not sc:
                continue
            uc = d.unit_cost.get(item) or d.std_cost.get(item)
            ccy = (d.cost_ccy.get(item) or d.std_ccy.get(item) or "USD")
            last_in_month = {}
            for t in range(d.n_buckets):
                pa += sc.get(t, 0.0) - g.get(t, 0.0)
                last_in_month[(hs + _dt.timedelta(weeks=t)).strftime("%Y-%m")] = pa
            for m, pa_m in last_in_month.items():
                inv_units[m] += pa_m
                held = max(0.0, pa_m)
                if uc is not None:
                    inv_val[m][ccy] += held * float(uc)
                elif held > 0:
                    unpriced_oh += held

        # ── BY LOCATION: current on-hand snapshot, valued (no forward projection) ──
        loc_units = defaultdict(float)
        loc_val = defaultdict(lambda: defaultdict(float))
        loc_items = defaultdict(set)
        loc_type = {}
        for lext, ltype, item_id, qty in conn.cursor().execute(
            "SELECT l.external_id, l.location_type, n.item_id, SUM(n.quantity) "
            "FROM nodes n JOIN locations l ON l.location_id=n.location_id "
            "WHERE n.scenario_id=%(b)s AND n.active AND n.node_type='OnHandSupply' "
            "  AND n.quantity IS NOT NULL GROUP BY 1,2,n.item_id",
            {"b": core.BASELINE}).fetchall():
            q = float(qty or 0)
            if q == 0:
                continue
            loc_units[lext] += q
            loc_type[lext] = ltype
            loc_items[lext].add(item_id)
            uc = d.unit_cost.get(item_id) or d.std_cost.get(item_id)
            if uc is not None:
                ccy = (d.cost_ccy.get(item_id) or d.std_ccy.get(item_id) or "USD")
                loc_val[lext][ccy] += q * float(uc)

        # supply-side stale dates: past-due open receipts collapse onto week 0 and
        # inflate the opening projected position (mirror of stale demand).
        pdr_qty, pdr_n = conn.cursor().execute(
            "SELECT COALESCE(SUM(quantity),0), COUNT(*) FROM nodes "
            "WHERE scenario_id=%(b)s AND active AND node_type=ANY(%(t)s) "
            "  AND time_ref IS NOT NULL AND time_ref < CURRENT_DATE AND quantity IS NOT NULL",
            {"b": core.BASELINE, "t": core.FIRM_RECEIPT_TYPES}).fetchone()

    elapsed = round(time.perf_counter() - t0, 2)
    months = sorted(inv_units)[: args.months_out]

    logger.info("=" * 88)
    logger.info("PROJECTED INVENTORY — TOTAL (item-pooled, consumption-correct) in %.2fs", elapsed)
    logger.info("  Month-end projected on-hand: VALUE held (Σ max(0,proj)·cost) + net units")
    logger.info("  %-9s %20s %18s", "month", "inventory_value", "net_units")
    for m in months:
        val = ", ".join(f"{c} {v:,.0f}" for c, v in sorted(inv_val[m].items(), key=lambda x: -x[1])) or "—"
        logger.info("  %-9s %20s %18s", m, val, f"{inv_units[m]:,.0f}")
    if unpriced_oh:
        logger.info("  ⚠ unpriced on-hand excluded from value: ~%s units (see dq_watcher MISSING_COST)", f"{unpriced_oh:,.0f}")
    if pdr_n:
        logger.info("  ⚠ %d past-due open receipts (%s units) land at week 0 and INFLATE the opening",
                    pdr_n, f"{float(pdr_qty):,.0f}")
        logger.info("    position — supply-side stale dates (mirror of stale demand). True near-term")
        logger.info("    stock is lower; refresh open-PO/WO dates at the source to de-bias.")
    logger.info("=" * 88)
    logger.info("CURRENT STOCK BY LOCATION (today's snapshot — forward per-site projection N/A, see note)")
    logger.info("  %-14s %-16s %16s %18s %8s", "location", "type", "on_hand_units", "value", "items")
    for lext in sorted(loc_units, key=lambda k: -loc_units[k]):
        val = ", ".join(f"{c} {v:,.0f}" for c, v in sorted(loc_val[lext].items(), key=lambda x: -x[1])) or "—"
        logger.info("  %-14s %-16s %16s %18s %8d", lext[:14], loc_type.get(lext, "")[:16],
                    f"{loc_units[lext]:,.0f}", val, len(loc_items[lext]))
    logger.info("  " + "-" * 84)
    logger.info("  NOTE: forward stock projection is reported at TOTAL only. In this dataset demand")
    logger.info("        sits on customer_virtual and supply on real sites (network not connected,")
    logger.info("        LANES-LATER #51), so per-site time projection is not meaningful yet.")
    logger.info("=" * 88)
    return 0


if __name__ == "__main__":
    sys.exit(main())
