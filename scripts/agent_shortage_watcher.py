"""
agent_shortage_watcher.py — Shortage Watcher (W01), thin over mrp_core.

The finished-good / independent-demand control tower. Detects forward shortages
on the SINGLE demand truth (mrp_core: forecast consumption max_only + demand time
fence + proration + multi-location dedup), then writes governed L1 DRAFT purchase
recommendations a planner reviews.

Previously this agent ran its own SQL window function that SUMMED customer orders
+ forecast (double-count) and ignored proration/multi-location — it now shares
mrp_core.first_shortage so the control-tower front and the MRP back agree on
demand.

North Star: deterministic core, L1 DRAFT only (never applies), auditable
(agent_runs), explainable (evidence trail), confidence-aware, idempotent
(supersede prior DRAFTs).

Scope: independent-demand items that are PURCHASED (have a supplier). Make items
with independent demand cascade into component needs handled by the material
side; they carry no purchase action here.

Usage:
    DATABASE_URL=postgresql://... python scripts/agent_shortage_watcher.py [--top 15]
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
logger = logging.getLogger("shortage_watcher")
AGENT_NAME = "shortage_watcher"


def _confidence(unit_cost, reliability, past_due_ratio: float) -> str:
    if past_due_ratio > 0.5:
        return "NEEDS_DATA_REVIEW"        # demand book largely stale → don't trust timing
    if unit_cost is None:
        return "LOW"
    if reliability is not None and float(reliability) < 0.7:
        return "LOW"
    if reliability is not None and float(reliability) >= 0.9:
        return "HIGH"
    return "MEDIUM"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Shortage Watcher (W01) — governed DRAFT purchase recommendations.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("Shortage Watcher (W01) running on DB=%s", db)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
        gross = core.consume_demand(d)
        short = core.first_shortage(d, gross)
        today = d.horizon_start

        # demand freshness (data-quality gate): share of raw demand qty past-due
        cur = conn.cursor()
        dn, pdn = cur.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE time_ref < CURRENT_DATE) FROM nodes "
            "WHERE scenario_id=%(b)s AND active AND node_type=ANY(%(t)s) AND time_ref IS NOT NULL",
            {"b": core.BASELINE, "t": core.DEMAND_TYPES}).fetchone()
        past_due_ratio = (pdn / dn) if dn else 0.0

        run_id = cur.execute(
            "INSERT INTO agent_runs (agent_name, scenario_id, status) VALUES (%s,%s,'RUNNING') RETURNING agent_run_id",
            (AGENT_NAME, core.BASELINE)).fetchone()[0]

        recs, display = [], []
        by_action, by_conf = defaultdict(int), defaultdict(int)
        spend = defaultdict(float)
        skipped_no_supplier = 0
        for item, sh in short.items():
            sup = d.best_sup.get(item)
            if not sup:
                skipped_no_supplier += 1      # make / unsourced independent demand → material side
                continue
            sid, sext, lt, uc, ccy, rel = sup
            deficit = sh["deficit"]
            ss = float(d.safety.get(item, 0) or 0)
            qty = core.lot_size(deficit + ss, float(d.moq.get(item) or 0), float(d.mult.get(item) or 0))
            qty = round(qty, 2)
            cost = round(qty * float(uc), 2) if uc is not None else None
            ccy = ccy or "EUR"
            runway = (sh["date"] - today).days
            margin = runway - int(lt or core.DEFAULT_LT_DAYS)
            action = "EXPEDITE" if margin < -14 else ("ORDER_RUSH" if margin < 0 else "ORDER_NOW")
            conf = _confidence(uc, rel, past_due_ratio)
            evidence = {"deficit_qty": round(deficit, 2), "pooled_safety_stock": ss,
                        "moq": float(d.moq.get(item) or 0) or None, "order_multiple": float(d.mult.get(item) or 0) or None,
                        "lead_time_days": lt, "runway_days": runway, "margin_days": margin,
                        "shortage_bucket_week": sh["bucket"], "supplier_reliability": float(rel) if rel is not None else None,
                        "unit_cost": float(uc) if uc is not None else None,
                        "rule": "first weekly bucket where on_hand + receipts − consumed_demand < 0; "
                                "qty = deficit + pooled_safety, MOQ floor, multiple round (consumed demand = max_only/DTF/prorated)"}
            recs.append((AGENT_NAME, run_id, core.BASELINE, item, d.names.get(item, str(item)[:8]),
                         sh["date"], round(deficit, 2), qty, cost, ccy, sid, sext, lt, runway, margin,
                         action, "L1", "DRAFT", conf, Jsonb(evidence)))
            display.append({"ext": d.names.get(item, str(item)[:8]), "fsd": sh["date"], "qty": qty,
                            "cost": cost, "ccy": ccy, "action": action, "conf": conf, "margin": margin})
            by_action[action] += 1
            by_conf[conf] += 1
            if cost is not None:
                spend[ccy] += cost

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
        metrics = {"recommendations": len(recs), "superseded_prior_drafts": superseded,
                   "by_action": dict(by_action), "by_confidence": dict(by_conf),
                   "estimated_spend": {k: round(v, 2) for k, v in spend.items()},
                   "skipped_no_supplier": skipped_no_supplier,
                   "shortage_items": len(short), "demand_nodes": dn, "past_due_demand_nodes": pdn,
                   "past_due_ratio": round(past_due_ratio, 4), "elapsed_s": round(time.perf_counter() - t0, 2)}
        cur.execute("UPDATE agent_runs SET status='COMPLETED', finished_at=now(), metrics=%s WHERE agent_run_id=%s",
                    (Jsonb(metrics), run_id))
        conn.commit()

    m = metrics
    logger.info("=" * 92)
    logger.info("SHORTAGE WATCHER — run %s COMPLETED in %.2fs", str(run_id)[:8], m["elapsed_s"])
    logger.info("  Recommendations written (DRAFT, L1) : %d", m["recommendations"])
    logger.info("  Forward shortage items (total)      : %d  (skipped, no supplier: %d)",
                m["shortage_items"], m["skipped_no_supplier"])
    logger.info("  Prior drafts superseded (EXPIRED)   : %d", m["superseded_prior_drafts"])
    logger.info("  By action     : %s", m["by_action"])
    logger.info("  By confidence : %s", m["by_confidence"])
    logger.info("  Est. spend    : %s", m["estimated_spend"])
    logger.info("  Data freshness: %d/%d demand nodes past-due (%.1f%%)", pdn, dn, 100 * past_due_ratio)
    if past_due_ratio > 0.5:
        logger.info("  ⚠ DATA-QUALITY GATE: >50%% of demand past-due → recos flagged NEEDS_DATA_REVIEW")
    logger.info("=" * 92)
    display.sort(key=lambda x: x["margin"])
    logger.info("TOP %d DRAFT recommendations (smallest margin first):", args.top)
    logger.info("  %-14s %-11s %-11s %9s %13s %-5s %-7s", "item", "by_date", "action", "qty", "cost", "ccy", "conf")
    for x in display[: args.top]:
        cs = f"{x['cost']:,.0f}" if x["cost"] is not None else "—"
        logger.info("  %-14s %-11s %-11s %9.0f %13s %-5s %-7s",
                    x["ext"], str(x["fsd"]), x["action"], x["qty"], cs, x["ccy"], x["conf"])
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
