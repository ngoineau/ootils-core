"""
agent_material_watcher.py — Material Watcher agent (thin over mrp_core).

Runs the time-phased level-by-level MRP (mrp_core), takes PAST-DUE component
planned orders (LLC>=1), pegs each to its driving finished goods, and writes them
to the governed recommendations table as DRAFT/L1 — the same queue the planner
reviews. Convergence of the MRP and control-tower threads.

North Star contract: deterministic core, L1 DRAFT only (never applies), auditable
(agent_runs), explainable (evidence = pegging + MRP trail), confidence-aware,
idempotent (supersede prior DRAFTs).

Usage:
    DATABASE_URL=... python scripts/agent_material_watcher.py [--top 15]
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
from psycopg.types.json import Jsonb
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("material_watcher")
AGENT_NAME = "material_watcher"


def _confidence(uc, rel):
    if uc is None:
        return "NEEDS_DATA_REVIEW"
    if rel is not None and float(rel) < 0.7:
        return "LOW"
    if rel is not None and float(rel) >= 0.9:
        return "HIGH"
    return "MEDIUM"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Material Watcher — MRP-driven component recommendations.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("Material Watcher running on DB=%s", db)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
        gross = core.consume_demand(d)
        _, origin = core.peg_origins(d, gross)            # pegging
        r = core.run_timephased(d, gross)                 # planned orders
        hs = d.horizon_start

        # aggregate PAST-DUE planned orders per component item (LLC >= 1)
        pastdue_qty = defaultdict(float)
        pastdue_need = {}
        kind_of = {}
        for item, qty, rel, need, kind, pd in r["planned"]:
            if not pd or d.llc.get(item, 0) < 1:
                continue
            pastdue_qty[item] += qty
            pastdue_need[item] = min(pastdue_need.get(item, need), need)
            kind_of[item] = kind

        cur = conn.cursor()   # recommendations / agent_runs schema from migration 039
        run_id = cur.execute(
            "INSERT INTO agent_runs (agent_name, scenario_id, status) VALUES (%s,%s,'RUNNING') RETURNING agent_run_id",
            (AGENT_NAME, core.BASELINE)).fetchone()[0]

        recs, display = [], []
        n_po = n_wo = 0
        spend = defaultdict(float)
        for item, qty in pastdue_qty.items():
            lvl = d.llc.get(item, 0)
            kind = kind_of.get(item, "PO")
            need_date = hs + _dt.timedelta(weeks=int(pastdue_need.get(item, 0)))
            runway = (need_date - hs).days
            org = origin.get(item, {})
            tot = sum(org.values()) or 1.0
            peg = [{"fg": d.names.get(fg, str(fg)[:8]), "pct": round(100 * q / tot, 1)}
                   for fg, q in sorted(org.items(), key=lambda x: -x[1])[:5]]
            sup = d.best_sup.get(item)
            if kind == "PO" and sup:
                sid, sext, lt, uc, ccy, rel = sup
                ccy = ccy or "EUR"
                cost = round(qty * float(uc), 2) if uc is not None else None
                margin = runway - int(lt)
                conf = _confidence(uc, rel)
                if cost is not None:
                    spend[ccy] += cost
                n_po += 1
            else:
                sid = sext = lt = uc = None
                ccy, cost = "EUR", None
                margin = runway - int(d.make_lt.get(item) or core.DEFAULT_LT_DAYS)
                conf = "MEDIUM"
                n_wo += 1
            evidence = {"kind": kind, "llc": lvl, "need_week": int(pastdue_need.get(item, 0)),
                        "pastdue": True, "pegging": peg,
                        "rule": "MRP time-phased past-due (need − lead_time < today)"}
            recs.append((AGENT_NAME, run_id, core.BASELINE, item, d.names.get(item, str(item)[:8]),
                         need_date, qty, qty, cost, ccy, sid, sext, lt, runway, margin,
                         "EXPEDITE", "L1", "DRAFT", conf, Jsonb(evidence)))
            display.append({"ext": d.names.get(item, str(item)[:8]), "kind": kind, "llc": lvl,
                            "qty": qty, "cost": cost, "ccy": ccy, "need": str(need_date),
                            "peg": peg[0]["fg"] if peg else "—", "pegpct": peg[0]["pct"] if peg else 0})

        superseded = cur.execute(
            "UPDATE recommendations SET status='EXPIRED', updated_at=now() "
            "WHERE agent_name=%s AND scenario_id=%s AND status='DRAFT'", (AGENT_NAME, core.BASELINE)).rowcount
        cur.executemany(
            """INSERT INTO recommendations
               (agent_name, agent_run_id, scenario_id, item_id, item_external_id,
                shortage_date, deficit_qty, recommended_qty, estimated_cost, currency,
                supplier_id, supplier_external_id, lead_time_days, runway_days, margin_days,
                action, decision_level, status, confidence, evidence)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", recs)
        metrics = {"component_recommendations": len(recs), "po": n_po, "wo": n_wo,
                   "superseded": superseded, "estimated_spend": {k: round(v, 2) for k, v in spend.items()},
                   "max_llc": d.max_llc, "elapsed_s": round(time.perf_counter() - t0, 2)}
        cur.execute("UPDATE agent_runs SET status='COMPLETED', finished_at=now(), metrics=%s WHERE agent_run_id=%s",
                    (Jsonb(metrics), run_id))
        conn.commit()

    logger.info("=" * 96)
    logger.info("MATERIAL WATCHER — run %s COMPLETED in %.2fs", str(run_id)[:8], metrics["elapsed_s"])
    logger.info("  Component recommendations (DRAFT/L1) : %d  (PO %d / WO %d)", len(recs), n_po, n_wo)
    logger.info("  Prior drafts superseded              : %d", superseded)
    logger.info("  Est. procurement spend               : %s", metrics["estimated_spend"])
    logger.info("=" * 96)
    display.sort(key=lambda x: -(x["cost"] or 0))
    logger.info("TOP %d component EXPEDITE recos (by cost) — with pegging:", args.top)
    logger.info("  %-15s %-4s %-4s %10s %13s %-5s %-11s %s", "item", "kind", "llc", "qty", "cost", "ccy", "by_date", "driven_by")
    for x in display[: args.top]:
        cs = f"{x['cost']:,.0f}" if x["cost"] is not None else "—"
        logger.info("  %-15s %-4s L%-3d %10.0f %13s %-5s %-11s %s (%.0f%%)",
                    x["ext"], x["kind"], x["llc"], x["qty"], cs, x["ccy"], x["need"], x["peg"], x["pegpct"])
    logger.info("=" * 96)
    return 0


if __name__ == "__main__":
    sys.exit(main())
