"""
agent_shortage_watcher.py — Shortage Watcher (W01), the first agent of the fleet.

This is the bridge from "a script that prints" to "an agent that produces
governed, auditable recommendations a planner reviews".

North Star alignment (CLAUDE.md):
  - Deterministic core      : the shortage/recommendation SQL is the source of
                              truth. The agent orchestrates + governs; it never
                              invents numbers.
  - Decision Ladder         : every output is L1 DRAFT. The agent NEVER applies
                              an action. Promotion to APPROVED/APPLIED is a human
                              (L3+) decision via the state machine.
  - Auditable               : every run is logged in agent_runs (work ledger);
                              every recommendation carries an evidence trail.
  - Explainable             : each recommendation cites deficit, lead time,
                              runway, margin, supplier — in the evidence JSONB.
  - Confidence-aware        : data-freshness + supplier-reliability + cost
                              completeness drive a per-reco confidence label;
                              weak inputs => NEEDS_DATA_REVIEW.
  - Idempotent              : re-running supersedes the prior DRAFT set (EXPIRED)
                              and writes a fresh current set.

Usage:
    DATABASE_URL=postgresql://... python scripts/agent_shortage_watcher.py
    python scripts/agent_shortage_watcher.py --top 20 --brief

Safety: refuses DBs not starting with 'ootils'; refuses 'ootils_dev' unless --allow-dev.
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import time

import psycopg
from psycopg.types.json import Jsonb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("shortage_watcher")

AGENT_NAME = "shortage_watcher"
BASELINE = "00000000-0000-0000-0000-000000000001"
SUPPLY_TYPES = ["OnHandSupply", "PurchaseOrderSupply", "TransferSupply"]
DEMAND_TYPES = ["CustomerOrderDemand", "ForecastDemand"]


# ── Schema (idempotent; mirrors migration 039 for pilote runtimes) ──────────
ENSURE_SCHEMA = """
CREATE TABLE IF NOT EXISTS agent_runs (
    agent_run_id  UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name    TEXT NOT NULL,
    scenario_id   UUID NOT NULL,
    status        TEXT NOT NULL DEFAULT 'RUNNING',
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    metrics       JSONB,
    notes         TEXT
);
CREATE INDEX IF NOT EXISTS ix_agent_runs_name ON agent_runs (agent_name, started_at DESC);

CREATE TABLE IF NOT EXISTS recommendations (
    recommendation_id    UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name           TEXT NOT NULL,
    agent_run_id         UUID NOT NULL,
    scenario_id          UUID NOT NULL,
    item_id              UUID NOT NULL,
    item_external_id     TEXT NOT NULL,
    shortage_date        DATE NOT NULL,
    deficit_qty          NUMERIC NOT NULL,
    recommended_qty      NUMERIC NOT NULL,
    estimated_cost       NUMERIC,
    currency             TEXT,
    supplier_id          UUID,
    supplier_external_id TEXT,
    lead_time_days       INTEGER,
    runway_days          INTEGER,
    margin_days          INTEGER,
    action               TEXT NOT NULL,
    decision_level       TEXT NOT NULL DEFAULT 'L1',
    status               TEXT NOT NULL DEFAULT 'DRAFT',
    confidence           TEXT NOT NULL DEFAULT 'MEDIUM',
    evidence             JSONB,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_reco_status    ON recommendations (status);
CREATE INDEX IF NOT EXISTS ix_reco_agent_run ON recommendations (agent_run_id);
CREATE INDEX IF NOT EXISTS ix_reco_item      ON recommendations (item_id);
CREATE INDEX IF NOT EXISTS ix_reco_action    ON recommendations (action, status);
"""

# ── Deterministic core: forward shortages + best supplier + planning params ──
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
fwd AS (SELECT item_id, fsd, bal FROM first_short WHERE fsd >= CURRENT_DATE),
best_sup AS (
    SELECT DISTINCT ON (si.item_id)
           si.item_id, si.supplier_id, si.lead_time_days, si.moq, si.unit_cost, si.currency
    FROM supplier_items si WHERE si.lead_time_days IS NOT NULL
    ORDER BY si.item_id, si.is_preferred DESC, si.lead_time_days ASC
),
ipp_agg AS (
    SELECT item_id, SUM(COALESCE(safety_stock_qty,0)) AS ss_pooled, MAX(order_multiple) AS mult
    FROM item_planning_params WHERE effective_to IS NULL GROUP BY item_id
)
SELECT f.item_id, it.external_id, it.name, f.fsd, (-f.bal) AS deficit,
       bs.supplier_id, s.external_id AS sup_ext, s.reliability_score,
       bs.lead_time_days, bs.moq, bs.unit_cost, bs.currency,
       COALESCE(ia.ss_pooled,0) AS ss_pooled, ia.mult,
       (f.fsd - CURRENT_DATE)                     AS runway,
       (f.fsd - CURRENT_DATE) - bs.lead_time_days AS margin
FROM fwd f
JOIN items it    ON it.item_id = f.item_id
JOIN best_sup bs ON bs.item_id = f.item_id
JOIN suppliers s ON s.supplier_id = bs.supplier_id
LEFT JOIN ipp_agg ia ON ia.item_id = f.item_id
ORDER BY margin ASC
"""

# Total demand freshness: share of demand qty that is past-due (data-quality signal)
FRESHNESS_SQL = """
SELECT
    COUNT(*)                                              AS demand_nodes,
    COUNT(*) FILTER (WHERE time_ref < CURRENT_DATE)       AS past_due_nodes
FROM nodes
WHERE scenario_id = %(b)s AND active = TRUE AND node_type = ANY(%(demand)s)
  AND time_ref IS NOT NULL
"""


def _guard(dsn: str, allow_dev: bool) -> str:
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


def _confidence(unit_cost, reliability, past_due_ratio: float) -> str:
    """Per-recommendation confidence from input quality."""
    if past_due_ratio > 0.5:
        return "NEEDS_DATA_REVIEW"      # demand book is largely stale → don't trust timing
    if unit_cost is None:
        return "LOW"                    # can't cost the action
    if reliability is not None and float(reliability) < 0.7:
        return "LOW"                    # unreliable supplier
    if reliability is not None and float(reliability) >= 0.9:
        return "HIGH"
    return "MEDIUM"


def run(conn: psycopg.Connection, top: int) -> dict:
    cur = conn.cursor()
    cur.execute("SET work_mem = '256MB'")
    cur.execute("SET max_parallel_workers_per_gather = 0")

    # 0. Ensure schema
    cur.execute(ENSURE_SCHEMA)

    # 1. Start run (work ledger)
    run_id = cur.execute(
        "INSERT INTO agent_runs (agent_name, scenario_id, status) VALUES (%s, %s, 'RUNNING') RETURNING agent_run_id",
        (AGENT_NAME, BASELINE),
    ).fetchone()[0]
    t0 = time.perf_counter()

    # 2. Freshness gate (data-quality signal)
    dn, pdn = cur.execute(FRESHNESS_SQL, {"b": BASELINE, "demand": DEMAND_TYPES}).fetchone()
    past_due_ratio = (pdn / dn) if dn else 0.0

    # 3. Deterministic core
    rows = cur.execute(REC_SQL, {"b": BASELINE, "supply": SUPPLY_TYPES, "demand": DEMAND_TYPES}).fetchall()

    # 4. Build recommendations (governance: all L1 DRAFT)
    recs = []
    display = []   # parallel list for the briefing only (carries item name)
    by_action: dict[str, int] = {}
    by_conf: dict[str, int] = {}
    spend: dict[str, float] = {}
    for (item_id, ext, name, fsd, deficit, sup_id, sup_ext, rel,
         lt, moq, unit_cost, ccy, ss, mult, runway, margin) in rows:
        deficit = float(deficit or 0)
        ss = float(ss or 0)
        qty = deficit + ss
        moq_applied = False
        if moq and qty < float(moq):
            qty = float(moq); moq_applied = True
        if mult:
            qty = math.ceil(qty / float(mult)) * float(mult)
        qty = round(qty, 2)
        cost = round(qty * float(unit_cost), 2) if unit_cost is not None else None
        ccy = ccy or "EUR"
        action = "EXPEDITE" if margin < -14 else ("ORDER_RUSH" if margin < 0 else "ORDER_NOW")
        conf = _confidence(unit_cost, rel, past_due_ratio)
        evidence = {
            "deficit_qty": deficit,
            "pooled_safety_stock": ss,
            "moq": float(moq) if moq is not None else None,
            "moq_applied": moq_applied,
            "order_multiple": float(mult) if mult is not None else None,
            "lead_time_days": lt,
            "runway_days": runway,
            "margin_days": margin,
            "supplier_reliability": float(rel) if rel is not None else None,
            "unit_cost": float(unit_cost) if unit_cost is not None else None,
            "rule": "qty = deficit + pooled_safety_stock, raised to MOQ, rounded to order_multiple",
        }
        recs.append((AGENT_NAME, run_id, BASELINE, item_id, ext, fsd, deficit, qty,
                     cost, ccy, sup_id, sup_ext, lt, runway, margin, action,
                     "L1", "DRAFT", conf, Jsonb(evidence)))
        display.append({"ext": ext, "fsd": fsd, "qty": qty, "cost": cost,
                        "ccy": ccy, "action": action, "conf": conf, "name": name})
        by_action[action] = by_action.get(action, 0) + 1
        by_conf[conf] = by_conf.get(conf, 0) + 1
        if cost is not None:
            spend[ccy] = spend.get(ccy, 0.0) + cost

    # 5. Supersede prior DRAFT set from this agent (idempotency)
    superseded = cur.execute(
        "UPDATE recommendations SET status='EXPIRED', updated_at=now() "
        "WHERE agent_name=%s AND scenario_id=%s AND status='DRAFT'",
        (AGENT_NAME, BASELINE),
    ).rowcount

    # 6. Persist fresh recommendations
    cur.executemany(
        """
        INSERT INTO recommendations
            (agent_name, agent_run_id, scenario_id, item_id, item_external_id,
             shortage_date, deficit_qty, recommended_qty, estimated_cost, currency,
             supplier_id, supplier_external_id, lead_time_days, runway_days, margin_days,
             action, decision_level, status, confidence, evidence)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        recs,
    )

    elapsed = round(time.perf_counter() - t0, 2)
    metrics = {
        "recommendations": len(recs),
        "superseded_prior_drafts": superseded,
        "by_action": by_action,
        "by_confidence": by_conf,
        "estimated_spend": {k: round(v, 2) for k, v in spend.items()},
        "demand_nodes": dn,
        "past_due_demand_nodes": pdn,
        "past_due_ratio": round(past_due_ratio, 4),
        "elapsed_s": elapsed,
    }

    # 7. Finish run
    cur.execute(
        "UPDATE agent_runs SET status='COMPLETED', finished_at=now(), metrics=%s WHERE agent_run_id=%s",
        (Jsonb(metrics), run_id),
    )

    return {"run_id": run_id, "metrics": metrics, "display": display, "top": top}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Shortage Watcher agent — produces governed DRAFT recommendations.")
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = _guard(args.dsn, args.allow_dev)
    logger.info("Shortage Watcher (W01) running on DB=%s", db)

    try:
        with psycopg.connect(args.dsn) as conn:
            r = run(conn, args.top)
            conn.commit()
    except Exception as e:  # noqa: BLE001
        logger.exception("agent run failed: %s", e)
        return 1

    m = r["metrics"]
    logger.info("=" * 92)
    logger.info("SHORTAGE WATCHER — run %s COMPLETED in %.2fs", str(r["run_id"])[:8], m["elapsed_s"])
    logger.info("  Recommendations written (DRAFT, L1) : %d", m["recommendations"])
    logger.info("  Prior drafts superseded (EXPIRED)   : %d", m["superseded_prior_drafts"])
    logger.info("  By action     : %s", m["by_action"])
    logger.info("  By confidence : %s", m["by_confidence"])
    logger.info("  Est. spend    : %s", m["estimated_spend"])
    logger.info("  Data freshness: %d/%d demand nodes past-due (%.1f%%)",
                m["past_due_demand_nodes"], m["demand_nodes"], 100 * m["past_due_ratio"])
    if m["past_due_ratio"] > 0.5:
        logger.info("  ⚠ DATA-QUALITY GATE: >50%% of demand is past-due → recos flagged NEEDS_DATA_REVIEW")
    logger.info("=" * 92)
    logger.info("TOP %d DRAFT recommendations (most urgent first):", r["top"])
    logger.info("  %-14s %-10s %-11s %9s %13s %-5s %-7s %s", "item", "by_date", "action", "qty", "cost", "ccy", "conf", "name")
    for d in r["display"][: r["top"]]:
        cost_s = f"{d['cost']:,.0f}" if d["cost"] is not None else "—"
        logger.info("  %-14s %-10s %-11s %9.0f %13s %-5s %-7s %s",
                    d["ext"], str(d["fsd"]), d["action"], d["qty"], cost_s, d["ccy"], d["conf"], (d["name"] or "")[:24])
    logger.info("=" * 92)
    logger.info("Recommendations are L1 DRAFT — awaiting human review. Query: "
                "SELECT * FROM recommendations WHERE status='DRAFT' ORDER BY margin_days;")
    return 0


if __name__ == "__main__":
    sys.exit(main())
