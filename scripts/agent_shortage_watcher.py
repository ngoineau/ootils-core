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

Scenario-backed (#340): each run forks ONE what-if scenario
(what-if-shortage_watcher-<ts>), simulates its simulable candidates there
(EXPEDITE = advance an existing firm receipt; ORDER_NOW/ORDER_RUSH draft a NEW
order and are marked not-simulated), stamps every reco's evidence with
simulation_scenario_id + its per-item shortage delta, then archives the fork.
If the fork's propagation fails, simulated recos are demoted to
NEEDS_DATA_REVIEW without delta (fail-loudly). See scripts/agent_simulation.py.

North Star: deterministic core, DRAFT only (never applies; decision level from
agent_governance.decision_level — new-order drafts L1, EXPEDITE of an existing
receipt L2), auditable (agent_runs), explainable (evidence trail),
confidence-aware, idempotent (supersede prior DRAFTs).

Scope: independent-demand items that are PURCHASED (have a supplier). Make items
with independent demand cascade into component needs handled by the material
side; they carry no purchase action here.

Usage:
    DATABASE_URL=postgresql://... python scripts/agent_shortage_watcher.py [--top 15]
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
import agent_subscribe
from agent_governance import decision_level, governed_run

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
    p.add_argument(
        "--subscribe", action="store_true",
        help="Event-driven mode (#401): drain the events stream from this "
             "agent's last cursor and run ONLY if a relevant event "
             "(calc_run_finished / shortage_detected) fired since. Without this "
             "flag: full scan every run (byte-identical to the legacy behaviour).",
    )
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("Shortage Watcher (W01) running on DB=%s", db)
    t0 = time.perf_counter()

    # Event-driven gate (#401 AN-1). Resolve the cursor + drain BEFORE opening a
    # governed_run so a skipped tick leaves NO agent_runs row. seed_cursor is what
    # this run persists (the drained high-water mark, or the from-now seed on the
    # first subscribed run); it is stamped into the run metrics below so the next
    # tick resumes from it.
    seed_cursor: int | None = None
    if args.subscribe:
        with psycopg.connect(args.dsn) as gate_conn:
            prior = agent_subscribe.fetch_stream_cursor(gate_conn, AGENT_NAME, core.BASELINE)
            base_cursor = prior if prior is not None else agent_subscribe.current_max_seq(
                gate_conn, core.BASELINE)
            seed_cursor, relevant = agent_subscribe.drain_stream(
                gate_conn, core.BASELINE, base_cursor)
        if prior is not None and relevant == 0:
            logger.info(
                "Shortage Watcher --subscribe: no relevant event since cursor=%s "
                "(drained to %s) — skipping run.", base_cursor, seed_cursor)
            return 0
        logger.info(
            "Shortage Watcher --subscribe: cursor %s -> %s, relevant=%d (%s) — running.",
            base_cursor, seed_cursor, relevant,
            "first subscribed run, seeded from now" if prior is None else "relevant events",
        )

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
        gross = core.consume_demand(d)
        r = core.run_timephased(d, gross)
        short = core.first_shortage(d, gross)   # for the true deficit-to-safety per item
        today = d.horizon_start

        # FG procurement signal sourced from the TIME-PHASED engine (nets to safety,
        # lot-sized, lead-time offset) — not a single-bucket deficit, so the order is
        # correctly sized for the coverage the engine planned. Scope: items carrying
        # independent demand that are BOUGHT (LLC 0); components (LLC>=1) are the
        # material watcher's job. Take the EARLIEST planned PO per item as the imminent
        # buy; count the rest as future-planned context.
        fg_first, fg_count = {}, defaultdict(int)
        for it, qty_o, rel_o, need_o, kind_o, pd_o in r["planned"]:
            if kind_o != "PO" or it not in gross or d.llc.get(it, 0) != 0:
                continue
            fg_count[it] += 1
            best = fg_first.get(it)
            if best is None or need_o < best["need"]:
                fg_first[it] = {"qty": qty_o, "rel": rel_o, "need": need_o, "pd": pd_o}

        # demand freshness (data-quality gate): share of raw demand qty past-due
        cur = conn.cursor()
        dn, pdn = cur.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE time_ref < CURRENT_DATE) FROM nodes "
            "WHERE scenario_id=%(b)s AND active AND node_type=ANY(%(t)s) AND time_ref IS NOT NULL",
            {"b": core.BASELINE, "t": core.DEMAND_TYPES}).fetchone()
        past_due_ratio = (pdn / dn) if dn else 0.0

        recs, display = [], []
        by_action, by_conf = defaultdict(int), defaultdict(int)
        spend = defaultdict(float)
        skipped_no_supplier = 0

        with governed_run(conn, AGENT_NAME, core.BASELINE, t0=t0) as run:
            candidates = []
            for item, o in fg_first.items():
                sup = d.best_sup.get(item)
                if not sup:
                    skipped_no_supplier += 1      # make / unsourced independent demand → material side
                    continue
                sid, sext, lt, uc, ccy, rel = sup
                qty = round(o["qty"], 2)          # already lot-sized & lead-time-offset by run_timephased
                deficit = round(short.get(item, {}).get("deficit", qty), 2)   # true shortfall-to-safety (≠ lot-sized qty)
                cost = round(qty * float(uc), 2) if uc is not None else None
                ccy = ccy or "EUR"
                need_date = today + _dt.timedelta(weeks=o["need"])
                runway = (need_date - today).days
                margin = runway - int(lt or core.DEFAULT_LT_DAYS)
                action = "EXPEDITE" if (o["pd"] or margin < -14) else ("ORDER_RUSH" if margin < 0 else "ORDER_NOW")
                conf = _confidence(uc, rel, past_due_ratio)
                evidence = {"planned_qty": qty, "release_week": o["rel"], "need_week": o["need"],
                            "past_due": o["pd"], "future_orders_planned": fg_count[item],
                            "moq": float(d.moq.get(item) or 0) or None, "order_multiple": float(d.mult.get(item) or 0) or None,
                            "lead_time_days": lt, "runway_days": runway, "margin_days": margin,
                            "supplier_reliability": float(rel) if rel is not None else None,
                            "unit_cost": float(uc) if uc is not None else None,
                            "rule": "earliest time-phased planned PURCHASE order from run_timephased "
                                    "(nets to safety, lot-sized, lead-time offset); consumed demand = max_only/DTF/prorated"}
                candidates.append({"item": item, "action": action, "need_date": need_date,
                                   "conf": conf, "evidence": evidence, "deficit": deficit,
                                   "qty": qty, "cost": cost, "ccy": ccy, "sid": sid, "sext": sext,
                                   "lt": lt, "runway": runway, "margin": margin})

            # Scenario-backed counter-factual (#340): ONE fork for the whole run,
            # applied to the simulable candidates, delta attributed per item,
            # fork archived by simulate_run.
            receipts = agent_simulation.load_future_receipts(conn)
            sim_summary, sim_results = agent_simulation.simulate_run(
                args.dsn, AGENT_NAME,
                [{"item": c["item"], "action": c["action"], "need_date": c["need_date"]} for c in candidates],
                receipts)

            for c, res in zip(candidates, sim_results):
                item, action = c["item"], c["action"]
                conf = agent_simulation.effective_confidence(
                    c["conf"], res["simulated"], sim_summary["propagation_status"])
                evidence = dict(c["evidence"])
                evidence["simulation_scenario_id"] = sim_summary["scenario_id"]
                evidence["simulation"] = agent_simulation.simulation_evidence(sim_summary, res)
                recs.append((AGENT_NAME, run.run_id, core.BASELINE, item, d.names.get(item, str(item)[:8]),
                             c["need_date"], c["deficit"], c["qty"], c["cost"], c["ccy"], c["sid"], c["sext"],
                             c["lt"], c["runway"], c["margin"],
                             action, decision_level(action), "DRAFT", conf, Jsonb(evidence)))
                display.append({"ext": d.names.get(item, str(item)[:8]), "fsd": c["need_date"], "qty": c["qty"],
                                "cost": c["cost"], "ccy": c["ccy"], "action": action, "conf": conf,
                                "margin": c["margin"]})
                by_action[action] += 1
                by_conf[conf] += 1
                if c["cost"] is not None:
                    spend[c["ccy"]] += c["cost"]

            superseded = run.supersede("recommendations", "DRAFT", "EXPIRED")
            run.insert(
                "recommendations",
                ["agent_name", "agent_run_id", "scenario_id", "item_id", "item_external_id",
                 "shortage_date", "deficit_qty", "recommended_qty", "estimated_cost", "currency",
                 "supplier_id", "supplier_external_id", "lead_time_days", "runway_days", "margin_days",
                 "action", "decision_level", "status", "confidence", "evidence"],
                recs,
            )
            metrics = {"recommendations": len(recs), "superseded_prior_drafts": superseded,
                       "by_action": dict(by_action), "by_confidence": dict(by_conf),
                       "estimated_spend": {k: round(v, 2) for k, v in spend.items()},
                       "skipped_no_supplier": skipped_no_supplier,
                       "fg_items_to_order": len(fg_first), "demand_nodes": dn, "past_due_demand_nodes": pdn,
                       "past_due_ratio": round(past_due_ratio, 4),
                       "simulation": sim_summary}
            # Persist the drained cursor (#401 --subscribe) so the next tick
            # resumes from it. Only in subscribe mode — a plain run stores no
            # cursor, keeping its metrics byte-identical to the legacy shape.
            if seed_cursor is not None:
                metrics[agent_subscribe.STREAM_CURSOR_KEY] = seed_cursor
            run.set_metrics(metrics)

    m = metrics
    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("=" * 92)
    logger.info("SHORTAGE WATCHER — run %s COMPLETED in %.2fs", str(run.run_id)[:8], elapsed)
    logger.info("  Recommendations written (DRAFT)     : %d", m["recommendations"])
    sim = m["simulation"]
    logger.info("  Scenario-backed (#340)              : fork=%s status=%s simulated=%d not-simulated=%d archived=%s",
                sim["scenario_name"] or "—", sim["propagation_status"] or "not-run",
                sim["simulated_candidates"], sim["non_simulated_candidates"], sim["archived"])
    logger.info("  FG items needing a purchase order   : %d  (skipped, no supplier: %d)",
                m["fg_items_to_order"], m["skipped_no_supplier"])
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
