"""
agent_eando_watcher.py — Excess & Obsolete Watcher, thin over mrp_core.

The mirror image of the shortage tower: instead of "what to order", it surfaces
"what to stop / free up". For each item whose on-hand sits beyond its coverage
threshold (mrp_core.excess_obsolete), it proposes a governed L1 DRAFT disposition
and values the excess (on-hand beyond the threshold) for capital recovery.

Disposition logic (deterministic, conservative — never auto-scraps):
  STOP_BUY : excess/obsolete AND still has firm inbound supply → stop replenishing
  REVIEW   : obsolete (dead stock) or very deep excess (>3× threshold) → human disposition
  HOLD     : moderate excess that demand will burn down within coverage

Confidence is data-quality aware (an "obsolete" item still on order, or one with
no cost, is NOT asserted as scrap):
  NEEDS_DATA_REVIEW : obsolete but firm inbound exists (contradiction → suspect demand data)
  LOW               : no cost (can't value, can't trust the exposure)
  MEDIUM/HIGH       : clean signal (HIGH when the excess has firm inbound to cancel)

North Star: deterministic core, L1 DRAFT only, auditable (agent_runs), evidence,
idempotent supersede.

Usage:
    DATABASE_URL=... python scripts/agent_eando_watcher.py [--months 12] [--cap 300] [--top 15]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import defaultdict

import psycopg
from psycopg.types.json import Jsonb
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("eando_watcher")
AGENT_NAME = "eando_watcher"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Excess & Obsolete Watcher — governed disposition recommendations.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--months", type=float, default=12.0, help="coverage threshold; above = E&O")
    p.add_argument("--cap", type=int, default=300, help="max recommendations persisted (ranked by excess value)")
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("E&O Watcher running on DB=%s (threshold %.0f months)", db, args.months)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
        gross = core.consume_demand(d)
        eo = core.excess_obsolete(d, gross, months=args.months)

        built = []   # full reco tuples (pre-cap), each with its excess_value for ranking
        totals = defaultdict(lambda: {"items": 0, "units": 0.0, "value": 0.0})
        unpriced_items = 0
        for item, e in eo.items():
            cls = e["class"]
            excess_units = e["excess_units"]
            cover = e["coverage_months"]                       # None = infinite
            firm_in = float(d.firm.get(item, 0) or 0)
            uc, ccy = core.cost_of(d, item)
            ccy = ccy or "USD"
            value = round(excess_units * float(uc), 2) if uc is not None else None

            if firm_in > 0:
                disposition = "STOP_BUY"
            elif cls == "OBSOLETE" or (cover is not None and cover > 3 * args.months):
                disposition = "REVIEW"
            else:
                disposition = "HOLD"

            if uc is None:
                conf = "LOW"
            elif cls == "OBSOLETE" and firm_in > 0:
                conf = "NEEDS_DATA_REVIEW"                      # dead but on order → demand data suspect
            elif firm_in > 0:
                conf = "HIGH"                                  # clear: inbound to cancel
            else:
                conf = "MEDIUM"

            evidence = {"on_hand": round(e["on_hand"], 2), "annual_demand": round(e["annual"], 2),
                        "coverage_months": (round(cover, 1) if cover is not None else None),
                        "excess_units": round(excess_units, 2), "firm_inbound_units": round(firm_in, 2),
                        "unit_cost": float(uc) if uc is not None else None,
                        "rule": "on-hand beyond %.0f months of gross-usage coverage; value = excess_units × cost" % args.months}
            built.append({
                "value_sort": value if value is not None else 0.0,
                "row": (AGENT_NAME, None, core.BASELINE, item, d.names.get(item, str(item)[:8]),
                        cls, round(e["on_hand"], 2), round(e["annual"], 2),
                        (round(cover, 4) if cover is not None else None), round(excess_units, 2), value, ccy,
                        disposition, "L1", "DRAFT", conf, Jsonb(evidence)),
                "disp": disposition, "cls": cls, "value": value, "units": excess_units,
                "ext": d.names.get(item, str(item)[:8]), "cover": cover, "conf": conf})
            t = totals[cls]
            t["items"] += 1
            t["units"] += excess_units
            t["value"] += value or 0.0
            if uc is None:
                unpriced_items += 1

        built.sort(key=lambda x: -x["value_sort"])
        kept = built[: args.cap]

        cur = conn.cursor()
        run_id = cur.execute(
            "INSERT INTO agent_runs (agent_name, scenario_id, status) VALUES (%s,%s,'RUNNING') RETURNING agent_run_id",
            (AGENT_NAME, core.BASELINE)).fetchone()[0]
        superseded = cur.execute(
            "UPDATE eando_recommendations SET status='EXPIRED', updated_at=now() "
            "WHERE agent_name=%s AND scenario_id=%s AND status='DRAFT'", (AGENT_NAME, core.BASELINE)).rowcount
        recs = [(a, run_id, *rest) for (a, _none, *rest) in (b["row"] for b in kept)]
        cur.executemany(
            """INSERT INTO eando_recommendations
               (agent_name, agent_run_id, scenario_id, item_id, item_external_id, classification,
                on_hand, annual_demand, coverage_months, excess_units, excess_value, currency,
                disposition, decision_level, status, confidence, evidence)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", recs)
        by_disp = defaultdict(int)
        for b in kept:
            by_disp[b["disp"]] += 1
        metrics = {"eando_items": len(built), "persisted": len(kept), "superseded": superseded,
                   "unpriced_items": unpriced_items, "by_disposition": dict(by_disp),
                   "by_class": {k: {"items": v["items"], "units": round(v["units"], 0), "value": round(v["value"], 2)}
                                for k, v in totals.items()},
                   "elapsed_s": round(time.perf_counter() - t0, 2)}
        cur.execute("UPDATE agent_runs SET status='COMPLETED', finished_at=now(), metrics=%s WHERE agent_run_id=%s",
                    (Jsonb(metrics), run_id))
        conn.commit()

    m = metrics
    logger.info("=" * 100)
    logger.info("E&O WATCHER — run %s COMPLETED in %.2fs", str(run_id)[:8], m["elapsed_s"])
    logger.info("  E&O items: %d  (persisted top %d by value; prior drafts superseded: %d)",
                m["eando_items"], m["persisted"], superseded)
    for cls in ("EXCESS", "OBSOLETE"):
        c = m["by_class"].get(cls)
        if c:
            logger.info("    %-9s : %5d items, %s units, value %s", cls, c["items"],
                        f"{c['units']:,.0f}", f"{c['value']:,.0f}")
    logger.info("  By disposition (persisted): %s", m["by_disposition"])
    if unpriced_items:
        logger.info("  ⚠ %d E&O items unpriced (value understated — see dq_watcher MISSING_COST)", unpriced_items)
    logger.info("=" * 100)
    logger.info("TOP %d E&O dispositions by value:", args.top)
    logger.info("  %-16s %-9s %-9s %10s %14s %14s %-5s %-7s", "item", "class", "disp", "cover_mo", "excess_u", "value", "ccy", "conf")
    for b in kept[: args.top]:
        cov_s = "∞" if b["cover"] is None else f"{b['cover']:,.1f}"
        val_s = f"{b['value']:,.0f}" if b["value"] is not None else "—"
        logger.info("  %-16s %-9s %-9s %10s %14s %14s %-5s %-7s",
                    b["ext"], b["cls"], b["disp"], cov_s, f"{b['units']:,.0f}", val_s,
                    b["row"][11], b["conf"])
    logger.info("=" * 100)
    return 0


if __name__ == "__main__":
    sys.exit(main())
