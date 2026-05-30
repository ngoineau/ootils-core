"""
agent_dq_watcher.py — Data-Quality Watcher (DQ fleet), thin over mrp_core.

The engine is only as correct as its inputs. This agent runs a battery of
data-quality rules over the planning substrate and writes findings to dq_findings
(migration 044), each ranked by BUSINESS IMPACT (planned purchase volume, past-due
demand qty) — not by raw count — so the biggest blind spots surface first.

It detects and surfaces; humans / interface owners remediate (the data lives in
the source ERP and the TSV contracts). Same work ledger (agent_runs), idempotent
supersede of the prior OPEN set, evidence trail, confidence via severity.

Rules:
  MISSING_COST          planned-PO item with no supplier unit_cost AND no item standard_cost
  NO_SUPPLIER           planned-PO item with no sourceable supplier (can't place the PO)
  MAKE_WITHOUT_BOM      make item with independent demand but no active BOM (demand can't explode)
  STALE_DEMAND          item whose demand nodes are dated in the past (collapse to week 0 → false expedites)
  ORPHAN_MAKE_FLAG      BOM parent not flagged is_make (would be planned as a phantom purchase)
  EXPIRED_SUPPLIER_TERM supplier_items row whose valid_to is already past (stale price/LT/MOQ)

Usage:
    DATABASE_URL=... python scripts/agent_dq_watcher.py [--cap 200] [--top 20]
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
logger = logging.getLogger("dq_watcher")
AGENT_NAME = "dq_watcher"


def _severity_by_quantile(values: dict, hi=0.90, mid=0.60) -> dict:
    """Map entity -> severity by rank of impact value (top 10% HIGH, next 30%
    MEDIUM, rest LOW). Adaptive, so no magic absolute thresholds."""
    if not values:
        return {}
    ordered = sorted(values.items(), key=lambda kv: kv[1])
    n = len(ordered)
    out = {}
    for i, (k, _v) in enumerate(ordered):
        q = (i + 1) / n
        out[k] = "HIGH" if q > hi else ("MEDIUM" if q > mid else "LOW")
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Data-Quality Watcher — impact-ranked DQ findings.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--cap", type=int, default=200, help="max findings persisted per rule (ranked by impact)")
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    logger.info("DQ Watcher running on DB=%s", db)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days)
        gross = core.consume_demand(d)
        r = core.run_timephased(d, gross)

        # planned purchase volume per item (impact weight for cost/supplier rules)
        planned_po = defaultdict(float)
        for item, qty, rel, need, kind, pd in r["planned"]:
            if kind == "PO":
                planned_po[item] += qty

        cur = conn.cursor()
        findings = []          # (rule, etype, eid, eext, severity, desc, metric, impact, action, evidence)
        totals = {}            # rule -> {"count": n, "impact": sum, "persisted": k}

        def emit(rule, rows):
            """rows: list of dicts; persist top --cap by impact, record totals."""
            rows.sort(key=lambda x: -(x["impact"] or 0))
            totals[rule] = {"count": len(rows), "impact": round(sum(x["impact"] or 0 for x in rows), 2),
                            "persisted": min(len(rows), args.cap)}
            for x in rows[: args.cap]:
                findings.append((rule, x["etype"], x.get("eid"), x["eext"], x["severity"], x["desc"],
                                 x["metric"], round(x["impact"], 2) if x["impact"] is not None else None,
                                 x["action"], Jsonb(x.get("evidence", {}))))

        # ── R1 MISSING_COST — planned PO, no cost anywhere (the 11% unvalued volume) ──
        rows = []
        miss = {it: q for it, q in planned_po.items() if q > 0 and it not in d.unit_cost and it not in d.std_cost}
        sev = _severity_by_quantile(miss)
        for it, q in miss.items():
            rows.append({"etype": "item", "eid": it, "eext": d.names.get(it, str(it)[:8]), "severity": sev[it],
                         "desc": "Item is purchased in the plan but has no unit_cost (supplier_items) nor standard_cost (items)",
                         "metric": "planned_volume_units", "impact": q,
                         "action": "Load unit_cost in supplier_items, or standard_cost via items.tsv / cost roll-up",
                         "evidence": {"planned_purchase_units": round(q, 2)}})
        emit("MISSING_COST", rows)

        # ── R2 NO_SUPPLIER — planned PO, no sourceable supplier ──
        rows = []
        nosup = {it: q for it, q in planned_po.items() if it not in d.best_sup}
        sev = _severity_by_quantile(nosup)
        for it, q in nosup.items():
            rows.append({"etype": "item", "eid": it, "eext": d.names.get(it, str(it)[:8]),
                         "severity": "HIGH" if q > 0 else sev.get(it, "LOW"),
                         "desc": "Item is planned for purchase but has no sourceable supplier (no supplier_items with a lead time)",
                         "metric": "planned_volume_units", "impact": q,
                         "action": "Create a supplier_items link (supplier, lead_time_days, ideally unit_cost/MOQ)",
                         "evidence": {"planned_purchase_units": round(q, 2)}})
        emit("NO_SUPPLIER", rows)

        # ── R3 MAKE_WITHOUT_BOM — make item with demand but no recipe ──
        rows = []
        mwb = {it: sum(v.values()) for it, v in gross.items()
               if bool(d.is_make.get(it, False)) and it not in d.bom and sum(v.values()) > 0}
        sev = _severity_by_quantile(mwb)
        for it, q in mwb.items():
            rows.append({"etype": "item", "eid": it, "eext": d.names.get(it, str(it)[:8]),
                         "severity": "HIGH", "metric": "demand_units", "impact": q,
                         "desc": "Make item carries demand but has no active BOM — demand cannot explode into components",
                         "action": "Load the BOM (bom_header + bom_components), or correct is_make if it is actually bought",
                         "evidence": {"independent_demand_units": round(q, 2)}})
        emit("MAKE_WITHOUT_BOM", rows)

        # ── R4 STALE_DEMAND — demand nodes dated in the past, per item ──
        rows = []
        stale = cur.execute(
            "SELECT n.item_id, i.external_id, SUM(n.quantity) qty, COUNT(*) c "
            "FROM nodes n JOIN items i ON i.item_id=n.item_id "
            "WHERE n.scenario_id=%(b)s AND n.active AND n.node_type=ANY(%(t)s) "
            "  AND n.time_ref IS NOT NULL AND n.time_ref < CURRENT_DATE AND n.quantity IS NOT NULL "
            "GROUP BY n.item_id, i.external_id",
            {"b": core.BASELINE, "t": core.DEMAND_TYPES}).fetchall()
        qmap = {row[0]: float(row[2] or 0) for row in stale}
        sev = _severity_by_quantile(qmap)
        for item_id, ext, qty, c in stale:
            rows.append({"etype": "item", "eid": item_id, "eext": ext, "severity": sev.get(item_id, "LOW"),
                         "metric": "past_due_demand_qty", "impact": float(qty or 0),
                         "desc": f"{c} demand node(s) dated in the past — collapse onto week 0 and inflate past-due / expedites",
                         "action": "Refresh demand dates at the source (stale CO/forecast) or purge obsolete demand",
                         "evidence": {"past_due_nodes": c, "past_due_qty": round(float(qty or 0), 2)}})
        emit("STALE_DEMAND", rows)

        # ── R5 ORPHAN_MAKE_FLAG — BOM parent not flagged is_make ──
        rows = []
        orphans = cur.execute(
            "WITH parents AS (SELECT DISTINCT parent_item_id pid FROM bom_headers "
            "                 WHERE effective_to IS NULL OR effective_to > CURRENT_DATE), "
            "mk AS (SELECT item_id, bool_or(is_make) im FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id) "
            "SELECT p.pid, i.external_id FROM parents p JOIN items i ON i.item_id=p.pid "
            "LEFT JOIN mk ON mk.item_id=p.pid WHERE mk.im IS NOT TRUE").fetchall()
        for pid, ext in orphans:
            rows.append({"etype": "item", "eid": pid, "eext": ext, "severity": "MEDIUM",
                         "metric": "count", "impact": 1.0,
                         "desc": "Item has an active BOM but is not flagged is_make — would be planned as a phantom purchase",
                         "action": "Set is_make=true on item_planning_params for this manufactured item",
                         "evidence": {}})
        emit("ORPHAN_MAKE_FLAG", rows)

        # ── R6 EXPIRED_SUPPLIER_TERM — supplier_items past valid_to ──
        rows = []
        expired = cur.execute(
            "SELECT si.item_id, i.external_id, s.external_id, si.valid_to "
            "FROM supplier_items si JOIN items i ON i.item_id=si.item_id JOIN suppliers s ON s.supplier_id=si.supplier_id "
            "WHERE si.valid_to IS NOT NULL AND si.valid_to < CURRENT_DATE").fetchall()
        for item_id, iext, sext, vto in expired:
            rows.append({"etype": "supplier_item", "eid": item_id, "eext": iext, "severity": "MEDIUM",
                         "metric": "count", "impact": 1.0,
                         "desc": f"Supplier term (supplier {sext}) expired on {vto} but is still selectable — stale price/LT/MOQ",
                         "action": "Refresh the supplier_items term or extend valid_to; planning may use stale costs",
                         "evidence": {"supplier": sext, "valid_to": str(vto)}})
        emit("EXPIRED_SUPPLIER_TERM", rows)

        # ── persist: open run, supersede prior OPEN set, insert ──
        run_id = cur.execute(
            "INSERT INTO agent_runs (agent_name, scenario_id, status) VALUES (%s,%s,'RUNNING') RETURNING agent_run_id",
            (AGENT_NAME, core.BASELINE)).fetchone()[0]
        superseded = cur.execute(
            "UPDATE dq_findings SET status='SUPERSEDED', updated_at=now() "
            "WHERE agent_name=%s AND scenario_id=%s AND status='OPEN'", (AGENT_NAME, core.BASELINE)).rowcount
        rows_to_insert = [(AGENT_NAME, run_id, core.BASELINE, *f) for f in findings]
        cur.executemany(
            """INSERT INTO dq_findings
               (agent_name, agent_run_id, scenario_id, rule_code, entity_type, entity_id,
                entity_external_id, severity, description, impact_metric, impact_value,
                suggested_action, evidence)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows_to_insert)
        metrics = {"findings_persisted": len(findings), "superseded": superseded,
                   "by_rule": totals, "elapsed_s": round(time.perf_counter() - t0, 2)}
        cur.execute("UPDATE agent_runs SET status='COMPLETED', finished_at=now(), metrics=%s WHERE agent_run_id=%s",
                    (Jsonb(metrics), run_id))
        conn.commit()

    logger.info("=" * 100)
    logger.info("DQ WATCHER — run %s COMPLETED in %.2fs", str(run_id)[:8], metrics["elapsed_s"])
    logger.info("  Findings persisted (OPEN) : %d   (prior OPEN superseded: %d)", len(findings), superseded)
    logger.info("  By rule (count / total impact / persisted):")
    for rule in ("MISSING_COST", "NO_SUPPLIER", "MAKE_WITHOUT_BOM", "STALE_DEMAND", "ORPHAN_MAKE_FLAG", "EXPIRED_SUPPLIER_TERM"):
        t = totals.get(rule)
        if t:
            logger.info("      %-22s %6d found / impact %15s / %d persisted",
                        rule, t["count"], f"{t['impact']:,.0f}", t["persisted"])
    logger.info("=" * 100)
    logger.info("TOP %d findings by impact:", args.top)
    logger.info("  %-22s %-8s %-16s %14s  %s", "rule", "severity", "item", "impact", "metric")
    shown = sorted(findings, key=lambda f: -(f[7] or 0))[: args.top]
    for rule, etype, eid, eext, sev_, desc, metric, impact, action, ev in shown:
        logger.info("  %-22s %-8s %-16s %14s  %s", rule, sev_, (eext or "")[:16],
                    f"{impact:,.0f}" if impact is not None else "—", metric)
    logger.info("=" * 100)
    return 0


if __name__ == "__main__":
    sys.exit(main())
