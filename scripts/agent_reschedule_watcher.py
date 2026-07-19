"""
agent_reschedule_watcher.py — Reschedule Watcher (#346 PR-B), thin over mrp_core.

Turns the deterministic reschedule action messages
(mrp_core.reschedule_signals: RESCHEDULE_IN / RESCHEDULE_OUT / CANCEL for
mis-dated or surplus firm receipts) into GOVERNED L2/L3 DRAFT recommendations —
the same `recommendations` queue a planner reviews, the North Star channel.
NEVER mrp_action_messages (architect decision: recommendations is the governed
channel), NEVER shortages (ADR-021: that table is ShortageDetector's alone).

No counter-factual fork (unlike the shortage/material watchers #340): a
reschedule signal is a deterministic fact — "this order is mis-dated vs its
computed need" — so the signal IS its own evidence. There is nothing to
simulate; forking would add noise, not proof.

Scenario-scoped: the watcher runs on a given scenario_id (default baseline) and
loads the plan through that scenario (load_planning_data(scenario=...)). A fork
with a lead-time / safety-stock overlay (#347) shifts need dates, which shifts
the emitted signals — automatically, with no scenario branch here. The
recommendations it writes carry that scenario_id and its own deterministic ids,
so baseline and a fork never collide.

Idempotence / stability (the whole point of #346): the recommendation_id is
DETERMINISTIC over (scenario, target_node, action, proposed_date)
— engine/recommendation/reschedule.reschedule_recommendation_id. Rows are
UPSERTED with ON CONFLICT (recommendation_id) DO NOTHING, so re-running on an
UNCHANGED plan re-derives the same ids and inserts ZERO new rows. This is
stronger than the supersede-then-reinsert pattern of the other watchers (which
mint a fresh UUID each run): a reschedule signal is a stable fact, not a
re-costed proposal. Superseding is used only to EXPIRE prior DRAFTs of this
agent/scenario whose signal no longer fires (the plan changed and the mis-date
was resolved) — those are marked EXPIRED, the still-valid ones are re-affirmed
by the idempotent upsert.

North Star: deterministic core, DRAFT only (never applies; decision level from
agent_governance.decision_level — RESCHEDULE_*/DEFER = L2, CANCEL = L3, the
first watcher-emitted L3, gated to a human by the recommendation state machine
#341), auditable (agent_runs), explainable (evidence = the signal detail),
idempotent (deterministic id + ON CONFLICT).

Usage:
    DATABASE_URL=postgresql://... python scripts/agent_reschedule_watcher.py \
        [--scenario <uuid>] [--horizon-days 540] [--top 15]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import defaultdict

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

import mrp_core as core
import agent_subscribe
from agent_governance import decision_level, governed_run

from ootils_core.engine.recommendation.reschedule import (
    RescheduleRecommendation,
    build_recommendation,
)
from ootils_core.notifications.l3_webhook import notify_l3_pending

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("reschedule_watcher")
AGENT_NAME = "reschedule_watcher"

# Columns written per reschedule recommendation. The three NOT-NULL business
# columns of migration 039 (shortage_date/deficit_qty/recommended_qty) are
# reused per the mapping in engine/recommendation/reschedule; the three
# reschedule columns come from migration 061.
_COLUMNS = (
    "recommendation_id",
    "agent_name",
    "agent_run_id",
    "scenario_id",
    "item_id",
    "item_external_id",
    "shortage_date",
    "deficit_qty",
    "recommended_qty",
    "action",
    "decision_level",
    "target_node_id",
    "current_receipt_date",
    "proposed_date",
    "status",
    "confidence",
    "evidence",
    "anchor_date",
    "stream_seq_hwm",
)


def _upsert(conn: psycopg.Connection, rows: list[tuple]) -> tuple[list, list]:
    """Idempotent insert of reschedule recommendations.

    ON CONFLICT (recommendation_id) DO NOTHING: a re-emitted identical signal
    (same deterministic id) is a no-op. Returns (inserted_ids, affirmed_ids):
    inserted_ids are the rows ACTUALLY written this run (RETURNING yields a row
    only on a real insert, not on a conflict no-op) — used to fire the L3
    webhook exactly once per genuinely-new row; affirmed_ids is EVERY id we
    tried to write (inserted or already present) — the caller uses it to NOT
    expire the still-valid prior DRAFTs. SQL is composed via psycopg.sql (no
    f-strings in the SQL path).
    """
    if not rows:
        return [], []
    col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in _COLUMNS)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in _COLUMNS)
    query = sql.SQL(
        "INSERT INTO recommendations ({cols}) VALUES ({vals}) "
        "ON CONFLICT (recommendation_id) DO NOTHING "
        "RETURNING recommendation_id"
    ).format(cols=col_ids, vals=placeholders)
    inserted_ids: list = []
    cur = conn.cursor()
    for r in rows:
        got = cur.execute(query, r).fetchone()
        if got is not None:
            inserted_ids.append(got[0])
    affirmed = [r[0] for r in rows]
    return inserted_ids, affirmed


def _l3_message(reco: RescheduleRecommendation) -> str:
    """Human-readable one-liner for the L3 webhook (no secret)."""
    prop = reco.proposed_date.isoformat() if reco.proposed_date is not None else "cancel"
    return (
        f"{reco.action} firm receipt {reco.target_node_id} "
        f"(item {reco.item_external_id}) qty {reco.recommended_qty}, "
        f"current date {reco.current_receipt_date.isoformat()} -> {prop}. "
        f"L3 human approval required."
    )


def _expire_stale_drafts(
    conn: psycopg.Connection, scenario_id: str, keep_ids: list
) -> int:
    """EXPIRE this agent/scenario's prior DRAFTs whose signal no longer fires.

    A DRAFT that is NOT in keep_ids (the ids the current run affirmed) means the
    mis-date it flagged was resolved (the plan changed) — mark it EXPIRED so the
    queue reflects reality. Rows in keep_ids are left untouched (their identity
    was just re-affirmed by the idempotent upsert). Scoped to this
    agent + scenario so it never touches another agent's or another fork's rows.
    """
    if keep_ids:
        cur = conn.cursor()
        cur.execute(
            "UPDATE recommendations SET status='EXPIRED', updated_at=now() "
            "WHERE agent_name=%s AND scenario_id=%s AND status='DRAFT' "
            "AND NOT (recommendation_id = ANY(%s))",
            (AGENT_NAME, scenario_id, keep_ids),
        )
        return cur.rowcount
    cur = conn.cursor()
    cur.execute(
        "UPDATE recommendations SET status='EXPIRED', updated_at=now() "
        "WHERE agent_name=%s AND scenario_id=%s AND status='DRAFT'",
        (AGENT_NAME, scenario_id),
    )
    return cur.rowcount


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Reschedule Watcher (#346) — governed DRAFT reschedule recommendations."
    )
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--scenario", default=core.BASELINE,
                   help="scenario_id to run on (default: baseline)")
    p.add_argument("--horizon-days", type=int, default=540)
    p.add_argument("--top", type=int, default=15)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    db = core.guard_db(args.dsn, args.allow_dev)
    scenario = args.scenario
    logger.info("Reschedule Watcher (#346) running on DB=%s scenario=%s", db, scenario)
    t0 = time.perf_counter()

    with psycopg.connect(args.dsn) as conn:
        d = core.load_planning_data(conn, args.horizon_days, scenario=scenario)
        # gross = consumed independent demand (max_only + demand time fence +
        # proration), exactly the input reschedule_signals expects — the same
        # single demand truth the shortage watcher and the MRP cascade use.
        gross = core.consume_demand(d)
        signals = core.reschedule_signals(d, gross)

        # Decision-basis stamps (C2 §3) carried on every reco: anchor_date =
        # d.horizon_start (the plan's as-of date) and stream_seq_hwm = the events
        # high-water mark this run decided against. No --subscribe mode here, so
        # it is the current MAX(stream_seq) for this scenario
        # (agent_subscribe.current_max_seq, the single fleet-wide HWM source).
        anchor_date = d.horizon_start
        stream_seq_hwm = agent_subscribe.current_max_seq(conn, scenario)

        rows: list[tuple] = []
        display: list[dict] = []
        by_action: defaultdict[str, int] = defaultdict(int)
        by_level: defaultdict[str, int] = defaultdict(int)
        recos_by_id: dict = {}
        l3_pending: list[RescheduleRecommendation] = []

        with governed_run(conn, AGENT_NAME, scenario, t0=t0) as run:
            for sig in signals:
                action = sig.action
                reco = build_recommendation(
                    scenario_id=scenario,
                    item_external_id=d.names.get(sig.item_id, str(sig.item_id)[:8]),
                    action=action,
                    decision_level=decision_level(action),
                    node_id=sig.node_id,
                    item_id=sig.item_id,
                    current_receipt_date=sig.current_receipt_date,
                    proposed_date=sig.proposed_date,
                    qty=sig.qty,
                    node_type=sig.node_type,
                    is_firm=sig.is_firm,
                )
                rows.append((
                    reco.recommendation_id, AGENT_NAME, run.run_id, scenario,
                    reco.item_id, reco.item_external_id, reco.shortage_date,
                    reco.deficit_qty, reco.recommended_qty, reco.action,
                    reco.decision_level, reco.target_node_id,
                    reco.current_receipt_date, reco.proposed_date, "DRAFT",
                    reco.confidence, Jsonb(reco.evidence),
                    anchor_date, stream_seq_hwm,
                ))
                recos_by_id[reco.recommendation_id] = reco
                by_action[action] += 1
                by_level[reco.decision_level] += 1
                delta = (
                    (sig.proposed_date - sig.current_receipt_date).days
                    if sig.proposed_date is not None else None
                )
                display.append({
                    "ext": reco.item_external_id, "action": action,
                    "level": reco.decision_level, "cur": str(sig.current_receipt_date),
                    "prop": str(sig.proposed_date) if sig.proposed_date else "—",
                    "delta": delta, "qty": sig.qty,
                })

            inserted_ids, affirmed = _upsert(conn, rows)
            inserted = len(inserted_ids)
            expired = _expire_stale_drafts(conn, scenario, affirmed)
            # Collect the genuinely-new recos for the L3 webhook (fired AFTER
            # commit, below). notify_l3_pending self-gates to L3+ so only CANCEL
            # rows actually notify; a re-run on an unchanged plan inserts nothing
            # and therefore pings nothing.
            l3_pending = [
                recos_by_id[rid] for rid in inserted_ids if rid in recos_by_id
            ]
            metrics = {
                "signals": len(signals),
                "recommendations_affirmed": len(affirmed),
                "recommendations_inserted": inserted,
                "recommendations_idempotent_noop": len(affirmed) - inserted,
                "expired_stale_drafts": expired,
                "by_action": dict(by_action),
                "by_decision_level": dict(by_level),
            }
            run.set_metrics(metrics)

    # Best-effort L3 webhook, POST-COMMIT (the recommendations are durably
    # persisted by governed_run's commit before we ping): the exception finds
    # the human without a UI. Never raises — a webhook failure is swallowed and
    # logged inside notify_l3_pending, so it cannot affect the completed run.
    notified = 0
    for reco in l3_pending:
        if notify_l3_pending(
            recommendation_id=reco.recommendation_id,
            action=reco.action,
            decision_level=reco.decision_level,
            message=_l3_message(reco),
            item_external_id=reco.item_external_id,
            location_external_id=None,
        ):
            notified += 1
    if l3_pending:
        logger.info(
            "L3 webhook: %d/%d new L3+ recommendation(s) notified",
            notified, len(l3_pending),
        )

    m = metrics
    elapsed = round(time.perf_counter() - t0, 2)
    logger.info("=" * 92)
    logger.info("RESCHEDULE WATCHER — run %s COMPLETED in %.2fs", str(run.run_id)[:8], elapsed)
    logger.info("  Signals from mrp_core               : %d", m["signals"])
    logger.info("  Recommendations affirmed (DRAFT)    : %d  (new: %d, idempotent no-op: %d)",
                m["recommendations_affirmed"], m["recommendations_inserted"],
                m["recommendations_idempotent_noop"])
    logger.info("  Prior DRAFTs expired (signal gone)  : %d", m["expired_stale_drafts"])
    logger.info("  By action        : %s", m["by_action"])
    logger.info("  By decision level : %s", m["by_decision_level"])
    logger.info("=" * 92)
    # Sort by absolute date movement (biggest re-date first); CANCEL (delta None)
    # sorts last within its own group.
    display.sort(key=lambda x: (x["delta"] is None, -abs(x["delta"] or 0)))
    logger.info("TOP %d DRAFT reschedule recommendations:", args.top)
    logger.info("  %-14s %-14s %-4s %-11s %-11s %7s %9s",
                "item", "action", "lvl", "current", "proposed", "delta_d", "qty")
    for x in display[: args.top]:
        dd = f"{x['delta']:+d}" if x["delta"] is not None else "—"
        logger.info("  %-14s %-14s %-4s %-11s %-11s %7s %9.0f",
                    x["ext"], x["action"], x["level"], x["cur"], x["prop"], dd, x["qty"])
    logger.info("=" * 92)
    return 0


if __name__ == "__main__":
    sys.exit(main())
