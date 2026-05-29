"""
agent_material_watcher.py — Material Watcher, the MRP-driven agent of the fleet.

Convergence of the two threads built this session:
  - Control tower (shortage_watcher): independent demand, finished-good level.
  - MRP (this agent): dependent demand, component level, multi-LLC.

It runs the time-phased level-by-level MRP, takes the PAST-DUE planned orders
(need − lead time already elapsed → must expedite), pegs each to its driving
finished goods, and writes them into the SAME governed recommendations table
(DRAFT / L1) the planner already reviews.

North Star properties (identical contract to shortage_watcher):
  - Deterministic core (MRP cascade), agent governs.
  - L1 DRAFT only; never applies.
  - Auditable (agent_runs work ledger), explainable (evidence = pegging + MRP trail).
  - Confidence-aware; idempotent (supersedes prior material_watcher DRAFTs).

One recommendation per component item: qty = total past-due, by_date = earliest
need. Evidence carries the pegging (driving FGs), LLC, kind (WO/PO), lot rule.

Usage:
    DATABASE_URL=... python scripts/agent_material_watcher.py [--top 15]
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("material_watcher")

AGENT_NAME = "material_watcher"
BASELINE = "00000000-0000-0000-0000-000000000001"
DEFAULT_LT_DAYS = 30


def _guard(dsn, allow_dev):
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


def _m(cur, sql, params=None):
    return {r[0]: r[1] for r in cur.execute(sql, params or {}).fetchall()}


def _lot(need, moq, mult):
    q = need
    if moq and q < moq:
        q = moq
    if mult and mult > 0:
        q = math.ceil(q / mult) * mult
    return q


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Material Watcher — MRP-driven component expedite recommendations.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = _guard(args.dsn, args.allow_dev)
    logger.info("Material Watcher running on DB=%s", db)
    t0 = time.perf_counter()
    n_buckets = math.ceil(args.horizon_days / 7) + 1

    with psycopg.connect(args.dsn) as conn:
        cur = conn.cursor()
        b = {"b": BASELINE}
        import datetime as _dt
        horizon_start = cur.execute("SELECT CURRENT_DATE").fetchone()[0]

        def bk(d):
            return max(0, (d - horizon_start).days // 7)

        # static data
        llc = _m(cur, "SELECT component_item_id, MAX(llc) FROM bom_lines GROUP BY component_item_id")
        is_make = _m(cur, "SELECT item_id, bool_or(is_make) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        on_hand = _m(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type='OnHandSupply' GROUP BY item_id", b)
        safety = _m(cur, "SELECT item_id, SUM(COALESCE(safety_stock_qty,0)) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        make_lt = _m(cur, "SELECT item_id, MAX(lead_time_total_days) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        moq = _m(cur, "SELECT item_id, MIN(moq) FROM supplier_items WHERE moq IS NOT NULL GROUP BY item_id")
        mult = _m(cur, "SELECT item_id, MAX(order_multiple) FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id")
        names = _m(cur, "SELECT item_id, external_id FROM items")

        # best supplier per item (for buy components)
        best_sup = {}
        for item, sid, sext, lt, uc, ccy, rel in cur.execute(
            """
            SELECT DISTINCT ON (si.item_id) si.item_id, s.supplier_id, s.external_id,
                   si.lead_time_days, si.unit_cost, si.currency, s.reliability_score
            FROM supplier_items si JOIN suppliers s ON s.supplier_id = si.supplier_id
            WHERE si.lead_time_days IS NOT NULL
            ORDER BY si.item_id, si.is_preferred DESC, si.lead_time_days ASC
            """
        ).fetchall():
            best_sup[item] = (sid, sext, lt, uc, ccy, rel)

        bom = defaultdict(list)
        for parent, comp, qpb, scrap in cur.execute(
            "SELECT bh.parent_item_id, bl.component_item_id, bl.quantity_per, bl.scrap_factor "
            "FROM bom_headers bh JOIN bom_lines bl ON bl.bom_id=bh.bom_id "
            "WHERE bh.effective_to IS NULL OR bh.effective_to > CURRENT_DATE"
        ).fetchall():
            bom[parent].append((comp, float(qpb), float(scrap or 0)))

        # bucketed demand + receipts
        gross = defaultdict(lambda: defaultdict(float))
        for item, tref, qty in cur.execute(
            "SELECT item_id, time_ref, quantity FROM nodes WHERE scenario_id=%(b)s AND active "
            "AND node_type IN ('CustomerOrderDemand','ForecastDemand') AND time_ref IS NOT NULL AND quantity IS NOT NULL", b).fetchall():
            if tref >= horizon_start:
                gross[item][bk(tref)] += float(qty)
        sched = defaultdict(lambda: defaultdict(float))
        for item, tref, qty in cur.execute(
            "SELECT item_id, time_ref, quantity FROM nodes WHERE scenario_id=%(b)s AND active "
            "AND node_type IN ('PurchaseOrderSupply','WorkOrderSupply','TransferSupply') AND time_ref IS NOT NULL AND quantity IS NOT NULL", b).fetchall():
            sched[item][bk(tref)] += float(qty)

        # indep aggregate (for pegging)
        indep = _m(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type IN ('CustomerOrderDemand','ForecastDemand') AND time_ref >= CURRENT_DATE GROUP BY item_id", b)
        firm = _m(cur, "SELECT item_id, SUM(quantity) FROM nodes WHERE scenario_id=%(b)s AND active AND node_type IN ('PurchaseOrderSupply','WorkOrderSupply','TransferSupply') GROUP BY item_id", b)

        # involved + levels
        involved = set()
        for d in (llc, is_make, on_hand, safety, gross, sched, indep):
            involved.update(d.keys())
        for parent, comps in bom.items():
            involved.add(parent)
            for c, _, _ in comps:
                involved.add(c)
        max_llc = max((llc.get(i, 0) for i in involved), default=0)
        by_level = defaultdict(list)
        for i in involved:
            by_level[llc.get(i, 0)].append(i)

        # ── pass 1: aggregate cascade with origin attribution (pegging) ──
        dep_ag = defaultdict(float)
        origin = defaultdict(lambda: defaultdict(float))
        for level in range(0, max_llc + 1):
            for item in by_level[level]:
                ind = float(indep.get(item, 0) or 0)
                g_ag = ind + dep_ag.get(item, 0.0)
                if g_ag <= 0:
                    continue
                avail = float(on_hand.get(item, 0) or 0) + float(firm.get(item, 0) or 0)
                net = g_ag + float(safety.get(item, 0) or 0) - avail
                if net <= 0:
                    continue
                mix = dict(origin.get(item, {}))
                if ind > 0:
                    mix[item] = mix.get(item, 0.0) + ind
                tot = sum(mix.values()) or 1.0
                if bool(is_make.get(item, False)):
                    for comp, qpb, scrap in bom.get(item, []):
                        contrib = net * qpb * (1.0 + scrap)
                        dep_ag[comp] += contrib
                        oc = origin[comp]
                        for fg, w in mix.items():
                            oc[fg] += contrib * (w / tot)

        # ── pass 2: time-phased cascade → past-due planned orders per item ──
        dependent = defaultdict(lambda: defaultdict(float))
        pastdue_qty = defaultdict(float)
        pastdue_need = {}     # item -> earliest need bucket
        kind_of = {}
        for level in range(0, max_llc + 1):
            for item in by_level[level]:
                g = gross.get(item)
                dep = dependent.get(item)
                if not g and not dep:
                    continue
                make = bool(is_make.get(item, False))
                ss = float(safety.get(item, 0) or 0)
                lt_days = (make_lt.get(item) if make else (best_sup.get(item, (None, None, None))[2])) or DEFAULT_LT_DAYS
                lt_weeks = max(0, math.ceil(float(lt_days) / 7))
                im_moq = float(moq.get(item) or 0)
                im_mult = float(mult.get(item) or 0)
                sc = sched.get(item, {})
                pa = float(on_hand.get(item, 0) or 0)
                for t in range(0, n_buckets):
                    pa = pa + sc.get(t, 0.0) - (g.get(t, 0.0) if g else 0.0) - (dep.get(t, 0.0) if dep else 0.0)
                    if pa < ss:
                        qty = _lot(ss - pa, im_moq, im_mult)
                        pa += qty
                        rel = t - lt_weeks
                        if rel < 0:   # PAST-DUE → expedite
                            pastdue_qty[item] += qty
                            pastdue_need[item] = min(pastdue_need.get(item, t), t)
                            kind_of[item] = "WO" if make else "PO"
                            if make:
                                for comp, qpb, scrap in bom.get(item, []):
                                    dependent[comp][0] += qty * qpb * (1.0 + scrap)
                        else:
                            if make:
                                for comp, qpb, scrap in bom.get(item, []):
                                    dependent[comp][rel] += qty * qpb * (1.0 + scrap)

        # ── build recommendations (component-level, LLC>=1, past-due) ──
        run_id = cur.execute(
            "INSERT INTO agent_runs (agent_name, scenario_id, status) VALUES (%s,%s,'RUNNING') RETURNING agent_run_id",
            (AGENT_NAME, BASELINE)).fetchone()[0]

        recs = []
        display = []
        n_po = n_wo = 0
        spend = defaultdict(float)
        for item, qty in pastdue_qty.items():
            lvl = llc.get(item, 0)
            if lvl < 1:
                continue   # this agent owns dependent (component) demand; FG handled by shortage_watcher
            kind = kind_of.get(item, "PO")
            need_bucket = pastdue_need.get(item, 0)
            need_date = horizon_start + _dt.timedelta(weeks=int(need_bucket))
            runway = (need_date - horizon_start).days
            org = origin.get(item, {})
            tot = sum(org.values()) or 1.0
            peg = [{"fg": names.get(fg, str(fg)[:8]), "pct": round(100 * q / tot, 1)}
                   for fg, q in sorted(org.items(), key=lambda x: -x[1])[:5]]
            sup = best_sup.get(item)
            if kind == "PO" and sup:
                sid, sext, lt, uc, ccy, rel = sup
                ccy = ccy or "EUR"
                cost = round(qty * float(uc), 2) if uc is not None else None
                margin = runway - int(lt)
                conf = ("NEEDS_DATA_REVIEW" if uc is None else
                        ("LOW" if (rel is not None and float(rel) < 0.7) else
                         ("HIGH" if (rel is not None and float(rel) >= 0.9) else "MEDIUM")))
                if cost is not None:
                    spend[ccy] += cost
                n_po += 1
            else:
                sid = sext = lt = uc = None
                ccy = "EUR"
                cost = None
                margin = runway - int((make_lt.get(item) or DEFAULT_LT_DAYS))
                conf = "MEDIUM"
                n_wo += 1
            evidence = {
                "kind": kind, "llc": lvl, "need_week": int(need_bucket),
                "pastdue": True, "pegging": peg,
                "rule": "MRP time-phased past-due (need − lead_time < today)",
            }
            recs.append((AGENT_NAME, run_id, BASELINE, item, names.get(item, str(item)[:8]),
                         need_date, qty, qty, cost, ccy, sid, sext, lt, runway, margin,
                         "EXPEDITE", "L1", "DRAFT", conf, Jsonb(evidence)))
            display.append({"ext": names.get(item, str(item)[:8]), "kind": kind, "llc": lvl,
                            "qty": qty, "cost": cost, "ccy": ccy, "need": str(need_date),
                            "peg": peg[0]["fg"] if peg else "—", "pegpct": peg[0]["pct"] if peg else 0})

        superseded = cur.execute(
            "UPDATE recommendations SET status='EXPIRED', updated_at=now() "
            "WHERE agent_name=%s AND scenario_id=%s AND status='DRAFT'", (AGENT_NAME, BASELINE)).rowcount
        cur.executemany(
            """INSERT INTO recommendations
               (agent_name, agent_run_id, scenario_id, item_id, item_external_id,
                shortage_date, deficit_qty, recommended_qty, estimated_cost, currency,
                supplier_id, supplier_external_id, lead_time_days, runway_days, margin_days,
                action, decision_level, status, confidence, evidence)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", recs)
        metrics = {
            "component_recommendations": len(recs), "po": n_po, "wo": n_wo,
            "superseded": superseded, "estimated_spend": {k: round(v, 2) for k, v in spend.items()},
            "max_llc": max_llc, "elapsed_s": round(time.perf_counter() - t0, 2),
        }
        cur.execute("UPDATE agent_runs SET status='COMPLETED', finished_at=now(), metrics=%s WHERE agent_run_id=%s",
                    (Jsonb(metrics), run_id))
        conn.commit()

    logger.info("=" * 96)
    logger.info("MATERIAL WATCHER — run %s COMPLETED in %.2fs", str(run_id)[:8], metrics["elapsed_s"])
    logger.info("  Component recommendations (DRAFT/L1) : %d  (PO %d / WO %d)", len(recs), n_po, n_wo)
    logger.info("  Prior drafts superseded              : %d", superseded)
    logger.info("  Est. procurement spend               : %s", metrics["estimated_spend"])
    logger.info("=" * 96)
    display.sort(key=lambda d: -(d["cost"] or 0))
    logger.info("TOP %d component EXPEDITE recos (by cost) — with pegging:", args.top)
    logger.info("  %-15s %-4s %-4s %10s %13s %-5s %-11s %s", "item", "kind", "llc", "qty", "cost", "ccy", "by_date", "driven_by")
    for d in display[: args.top]:
        cs = f"{d['cost']:,.0f}" if d["cost"] is not None else "—"
        logger.info("  %-15s %-4s L%-3d %10.0f %13s %-5s %-11s %s (%.0f%%)",
                    d["ext"], d["kind"], d["llc"], d["qty"], cs, d["ccy"], d["need"], d["peg"], d["pegpct"])
    logger.info("=" * 96)
    logger.info("Component expedites are L1 DRAFT in the same queue as FG recos. "
                "Query: SELECT * FROM recommendations WHERE agent_name='material_watcher' AND status='DRAFT';")
    return 0


if __name__ == "__main__":
    sys.exit(main())
