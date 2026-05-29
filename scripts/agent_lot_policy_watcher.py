"""
agent_lot_policy_watcher.py — Lot Policy Watcher agent ("Pilotage des paramètres").

Negotiated supplier terms (MOQ, order multiple) are the DEFAULT and are always
applied by the engine — this agent NEVER overrides them. It observes the realized
time-phased plan (mrp_core) and proposes parameter ADJUSTMENTS as L1 DRAFT
recommendations into a governed queue (parameter_recommendations); a human
approves before any change.

For each item it derives, under the current policy, the weeks-of-supply one
order covers (avg order qty / weekly demand) and compares it to a target band:
  - WOS too HIGH and MOQ-bound  -> RENEGOTIATE_MOQ (capital tied up; propose a MOQ
                                   sized to the high end of the band)
  - WOS too HIGH and multiple-bound -> REVIEW_MULTIPLE
  - WOS too LOW (lot-for-lot ordering every week) -> SET_LOT_RULE POQ (batch up,
                                   cut transaction count)
Sporadic / thin-signal items -> DATA_REVIEW (NEEDS_DATA_REVIEW confidence).

North Star: deterministic core, L1 DRAFT only (never applies), auditable
(agent_runs + transitions), explainable (evidence = realized-plan footprint),
confidence-aware, idempotent (supersede prior DRAFTs).

Usage:
    DATABASE_URL=... python scripts/agent_lot_policy_watcher.py [--top 15]
        [--wos-low 2] [--wos-high 13] [--poq-target 4]
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
from psycopg.types.json import Jsonb
import mrp_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("lot_policy_watcher")
AGENT_NAME = "lot_policy_watcher"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Lot Policy Watcher — proposes lot-sizing parameter adjustments.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--wos-low", type=float, default=2.0, help="min target weeks-of-supply per order")
    p.add_argument("--wos-high", type=float, default=13.0, help="max target weeks-of-supply per order")
    p.add_argument("--poq-target", type=int, default=4, help="POQ periods to propose when ordering too often")
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("Lot Policy Watcher running on DB=%s (target WOS band %.0f–%.0f wk)", db, args.wos_low, args.wos_high)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
        gross = core.consume_demand(d)
        trace = []
        core.run_timephased(d, gross, trace=trace)
        n_weeks = max(1, d.n_buckets)

        # aggregate the realized-plan footprint per item
        agg = defaultdict(lambda: {"n": 0, "qty": 0.0, "moq_hits": 0, "moq": 0.0, "mult": 0.0, "rule": "LOTFORLOT"})
        for item, shortfall, qty, moq, mult, rule, kind in trace:
            a = agg[item]
            a["n"] += 1
            a["qty"] += qty
            a["moq"] = moq
            a["mult"] = mult
            a["rule"] = rule
            if moq and max(0.0, shortfall) < moq - 1e-9:
                a["moq_hits"] += 1

        recs, display = [], []
        ct_count = defaultdict(int)
        for item, a in agg.items():
            tot_dem = sum(gross.get(item, {}).values())
            active_wk = sum(1 for v in gross.get(item, {}).values() if v > 0)
            if tot_dem <= 0 or a["n"] == 0:
                continue
            weekly = tot_dem / n_weeks
            annual = weekly * 52.0
            avg_qty = a["qty"] / a["n"]
            wos = avg_qty / weekly if weekly > 0 else 0.0
            moq, mult = a["moq"], a["mult"]
            moq_bound = a["moq_hits"] >= max(1, a["n"] * 0.5)

            # confidence from demand signal strength
            if tot_dem < 10 or active_wk < 4:
                conf = "NEEDS_DATA_REVIEW"
            elif active_wk >= 12 and tot_dem >= 100:
                conf = "HIGH"
            else:
                conf = "MEDIUM"

            param = cur_val = prop_val = change = rationale = None
            impact = 0.0
            if wos > args.wos_high and moq_bound and moq > 0:
                # MOQ ties up capital: one order covers far more than the target band
                target = max(1.0, weekly * args.wos_high)
                prop = math.ceil(target)
                if prop < moq:
                    param, change, rationale = "moq", "RENEGOTIATE_MOQ", "MOQ_EXCESS_WOS"
                    cur_val, prop_val = f"{moq:.0f}", f"{prop:.0f}"
                    impact = -((moq - prop) / 2.0)  # cycle-stock reduction
            elif wos > args.wos_high and mult and mult > 0 and weekly * args.wos_high < mult:
                param, change, rationale = "order_multiple", "REVIEW_MULTIPLE", "MULTIPLE_OVERHANG"
                prop = max(1.0, math.ceil(weekly * args.wos_low))
                cur_val, prop_val = f"{mult:.0f}", f"{prop:.0f}"
                impact = -((mult - prop) / 2.0)
            elif wos < args.wos_low and a["rule"] == "LOTFORLOT" and not moq_bound:
                # ordering every week or two with no MOQ discipline -> batch up
                param, change, rationale = "lot_size_rule", "SET_LOT_RULE", "LFL_TOO_FREQUENT"
                cur_val, prop_val = "LOTFORLOT", f"POQ:{args.poq_target}"
                impact = +(weekly * args.poq_target / 2.0)  # cycle stock rises but order count falls

            if param is None:
                continue  # within the healthy band — leave the negotiated policy alone

            evidence = {
                "orders_in_horizon": a["n"], "avg_order_qty": round(avg_qty, 1),
                "weeks_of_supply_per_order": round(wos, 2), "weekly_demand": round(weekly, 2),
                "active_demand_weeks": active_wk, "moq": moq, "order_multiple": mult,
                "current_rule": a["rule"], "moq_bound": moq_bound,
                "target_band_weeks": [args.wos_low, args.wos_high],
                "rule": "WOS-per-order vs target band on the realized time-phased plan",
            }
            recs.append((AGENT_NAME, None, core.BASELINE, item, d.names.get(item, str(item)[:8]),
                         param, cur_val, prop_val, change, rationale,
                         round(wos, 2), round(annual, 1), round(impact, 1),
                         "L1", "DRAFT", conf, Jsonb(evidence)))
            display.append({"ext": d.names.get(item, str(item)[:8]), "param": param, "change": change,
                            "cur": cur_val, "prop": prop_val, "wos": wos, "annual": annual,
                            "impact": impact, "conf": conf})
            ct_count[change] += 1

        # persist: open run, supersede prior drafts, insert
        cur = conn.cursor()
        run_id = cur.execute(
            "INSERT INTO agent_runs (agent_name, scenario_id, status) VALUES (%s,%s,'RUNNING') RETURNING agent_run_id",
            (AGENT_NAME, core.BASELINE)).fetchone()[0]
        recs = [(a, run_id, *rest) for (a, _none, *rest) in recs]  # fill agent_run_id
        superseded = cur.execute(
            "UPDATE parameter_recommendations SET status='EXPIRED', updated_at=now() "
            "WHERE agent_name=%s AND scenario_id=%s AND status='DRAFT'", (AGENT_NAME, core.BASELINE)).rowcount
        cur.executemany(
            """INSERT INTO parameter_recommendations
               (agent_name, agent_run_id, scenario_id, item_id, item_external_id,
                parameter, current_value, proposed_value, change_type, rationale_code,
                weeks_of_supply, annual_demand, est_inventory_impact_units,
                decision_level, status, confidence, evidence)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", recs)
        metrics = {"proposals": len(recs), "by_change_type": dict(ct_count), "superseded": superseded,
                   "items_planned": len(agg), "elapsed_s": round(time.perf_counter() - t0, 2)}
        cur.execute("UPDATE agent_runs SET status='COMPLETED', finished_at=now(), metrics=%s WHERE agent_run_id=%s",
                    (Jsonb(metrics), run_id))
        conn.commit()

    logger.info("=" * 100)
    logger.info("LOT POLICY WATCHER — run %s COMPLETED in %.2fs", str(run_id)[:8], metrics["elapsed_s"])
    logger.info("  Parameter proposals (DRAFT/L1) : %d   by type: %s", len(recs), dict(ct_count))
    logger.info("  Prior drafts superseded        : %d", superseded)
    logger.info("=" * 100)
    display.sort(key=lambda x: -abs(x["impact"]))
    logger.info("TOP %d proposals (by |inventory impact|):", args.top)
    logger.info("  %-15s %-16s %-18s %-12s %8s %12s %-6s", "item", "parameter", "change", "cur->prop", "wos", "annual_dem", "conf")
    for x in display[: args.top]:
        logger.info("  %-15s %-16s %-18s %-12s %8.1f %12s %-6s",
                    x["ext"], x["param"], x["change"], f"{x['cur']}->{x['prop']}",
                    x["wos"], f"{x['annual']:,.0f}", x["conf"])
    logger.info("=" * 100)
    return 0


if __name__ == "__main__":
    sys.exit(main())
