"""
agent_material_watcher.py — Material Watcher agent (thin over mrp_core).

Runs the time-phased level-by-level MRP (mrp_core), takes PAST-DUE component
planned orders (LLC>=1), pegs each to its driving finished goods, and writes them
to the governed recommendations table as DRAFT — the same queue the planner
reviews. Convergence of the MRP and control-tower threads.

Scenario-backed (#340): each run forks ONE what-if scenario
(what-if-material_watcher-<ts>), simulates the EXPEDITE candidates that have an
existing future firm receipt to advance (others carry the not-simulated
marker), stamps every reco's evidence with simulation_scenario_id + its
per-item shortage delta, then archives the fork. Propagation failure demotes
simulated recos to NEEDS_DATA_REVIEW (fail-loudly, no fabricated delta).

North Star contract: deterministic core, DRAFT only (never applies; EXPEDITE
of an existing order = L2 via agent_governance.decision_level), auditable
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
import agent_simulation
from agent_governance import decision_level, governed_run

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

        recs, display = [], []
        n_po = n_wo = 0
        spend = defaultdict(float)

        with governed_run(conn, AGENT_NAME, core.BASELINE, t0=t0) as run:
            candidates = []
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
                candidates.append({"item": item, "action": "EXPEDITE", "need_date": need_date,
                                   "conf": conf, "evidence": evidence, "qty": qty, "cost": cost,
                                   "ccy": ccy, "sid": sid, "sext": sext, "lt": lt,
                                   "runway": runway, "margin": margin, "kind": kind, "llc": lvl,
                                   "peg": peg})

            # Scenario-backed counter-factual (#340): ONE fork for the whole run.
            # Simulable = EXPEDITE with an existing future firm receipt to advance;
            # the rest carries the not-simulated marker. Fork archived by simulate_run.
            receipts = agent_simulation.load_future_receipts(conn)
            sim_summary, sim_results = agent_simulation.simulate_run(
                args.dsn, AGENT_NAME,
                [{"item": c["item"], "action": c["action"], "need_date": c["need_date"]} for c in candidates],
                receipts)

            for c, res in zip(candidates, sim_results):
                item = c["item"]
                conf = agent_simulation.effective_confidence(
                    c["conf"], res["simulated"], sim_summary["propagation_status"])
                evidence = dict(c["evidence"])
                evidence["simulation_scenario_id"] = sim_summary["scenario_id"]
                evidence["simulation"] = agent_simulation.simulation_evidence(sim_summary, res)
                recs.append((AGENT_NAME, run.run_id, core.BASELINE, item, d.names.get(item, str(item)[:8]),
                             c["need_date"], c["qty"], c["qty"], c["cost"], c["ccy"], c["sid"], c["sext"],
                             c["lt"], c["runway"], c["margin"],
                             "EXPEDITE", decision_level("EXPEDITE"), "DRAFT", conf, Jsonb(evidence)))
                display.append({"ext": d.names.get(item, str(item)[:8]), "kind": c["kind"], "llc": c["llc"],
                                "qty": c["qty"], "cost": c["cost"], "ccy": c["ccy"], "need": str(c["need_date"]),
                                "peg": c["peg"][0]["fg"] if c["peg"] else "—",
                                "pegpct": c["peg"][0]["pct"] if c["peg"] else 0})

            superseded = run.supersede("recommendations", "DRAFT", "EXPIRED")
            run.insert(
                "recommendations",
                ["agent_name", "agent_run_id", "scenario_id", "item_id", "item_external_id",
                 "shortage_date", "deficit_qty", "recommended_qty", "estimated_cost", "currency",
                 "supplier_id", "supplier_external_id", "lead_time_days", "runway_days", "margin_days",
                 "action", "decision_level", "status", "confidence", "evidence"],
                recs,
            )
            run.set_metrics({
                "component_recommendations": len(recs), "po": n_po, "wo": n_wo,
                "superseded": superseded,
                "estimated_spend": {k: round(v, 2) for k, v in spend.items()},
                "max_llc": d.max_llc,
                "simulation": sim_summary,
            })
            metrics = run.metrics

    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("=" * 96)
    logger.info("MATERIAL WATCHER — run %s COMPLETED in %.2fs", str(run.run_id)[:8], elapsed)
    logger.info("  Component recommendations (DRAFT)    : %d  (PO %d / WO %d)", len(recs), n_po, n_wo)
    logger.info("  Prior drafts superseded              : %d", metrics["superseded"])
    sim = metrics["simulation"]
    logger.info("  Scenario-backed (#340)               : fork=%s status=%s simulated=%d not-simulated=%d archived=%s",
                sim["scenario_name"] or "—", sim["propagation_status"] or "not-run",
                sim["simulated_candidates"], sim["non_simulated_candidates"], sim["archived"])
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
