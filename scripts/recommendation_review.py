"""
recommendation_review.py — Human control room for agent recommendations.

The agent (shortage_watcher) produces L1 DRAFT recommendations. This tool is
the HUMAN side of the Decision Ladder: a planner reviews, approves, rejects, and
marks recommendations applied. Every transition is audited (who, when, why).

Non-negotiable (strategy doc §13): no external ERP mutation without explicit
human approval. APPROVED/APPLIED transitions are human (L3/L4) decisions only —
this tool records them; it does not itself push to any ERP.

State machine (single source of truth:
src/ootils_core/engine/recommendation/state_machine.py — shared with the
/v1/recommendations API router):
    DRAFT     → REVIEWED | APPROVED | REJECTED
    REVIEWED  → APPROVED | REJECTED
    APPROVED  → APPLIED  | REJECTED
    EXPIRED / APPLIED / REJECTED = terminal

Schema: tables come from migrations 039 (recommendations) + 040
(recommendation_transitions) — applied automatically at API startup.
This tool no longer creates any table itself.

Commands:
    status                                  inbox summary by status × action
    list   [--action X] [--limit N]         list DRAFT recommendations
    approve --by USER [--action X | --id ID] [--note ...]
    reject  --by USER --reason R [--action X | --id ID]
    apply   --by USER [--id ID | --all-approved]
    history [--id ID]                       transition audit trail

Usage:
    DATABASE_URL=... python scripts/recommendation_review.py status
    DATABASE_URL=... python scripts/recommendation_review.py approve --action EXPEDITE --by ngoineau --note "Q3 ramp"
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import psycopg

from ootils_core.engine.recommendation.state_machine import transition_many

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("reco_review")


def _guard(dsn: str, allow_dev: bool) -> str:
    name = dsn.rstrip("/").split("/")[-1].split("?")[0]
    if not name.startswith("ootils"):
        raise SystemExit(f"REFUSED: DB '{name}' does not start with 'ootils'.")
    if name == "ootils_dev" and not allow_dev:
        raise SystemExit("REFUSED: ootils_dev is semi-prod, pass --allow-dev.")
    return name


def cmd_status(conn):
    rows = conn.execute(
        "SELECT status, action, COUNT(*), COALESCE(SUM(estimated_cost),0) "
        "FROM recommendations GROUP BY status, action ORDER BY status, action"
    ).fetchall()
    logger.info("%-12s %-12s %8s %18s", "status", "action", "count", "est_cost")
    logger.info("-" * 54)
    for st, act, n, cost in rows:
        logger.info("%-12s %-12s %8d %18s", st, act, n, f"{float(cost):,.0f}")


def cmd_list(conn, action, limit):
    q = ("SELECT item_external_id, shortage_date, action, recommended_qty, estimated_cost, "
         "currency, confidence, recommendation_id FROM recommendations WHERE status='DRAFT'")
    params = []
    if action:
        q += " AND action=%s"
        params.append(action)
    q += " ORDER BY margin_days ASC LIMIT %s"
    params.append(limit)
    rows = conn.execute(q, params).fetchall()
    logger.info("%-14s %-10s %-11s %9s %13s %-5s %-7s %s", "item", "by_date", "action", "qty", "cost", "ccy", "conf", "reco_id")
    for ext, fsd, act, qty, cost, ccy, conf, rid in rows:
        cost_s = f"{float(cost):,.0f}" if cost is not None else "—"
        logger.info("%-14s %-10s %-11s %9.0f %13s %-5s %-7s %s", ext, str(fsd), act, float(qty), cost_s, ccy, conf, str(rid)[:8])


def _transition(conn, to_status, actor, reason, action, reco_id, allow_dev):
    """Apply a state transition to a set of recommendations, logging each.

    Delegates to the shared engine state machine (FOR UPDATE locking,
    validation, audit row) — this CLI is a human tool, so actor_kind is
    always 'human'.
    """
    return transition_many(
        conn,
        to_status,
        actor,
        actor_kind="human",
        reason=reason,
        action=action,
        recommendation_id=reco_id,
    )


def cmd_history(conn, reco_id):
    if reco_id:
        rows = conn.execute(
            "SELECT created_at, from_status, to_status, actor, actor_kind, reason "
            "FROM recommendation_transitions WHERE recommendation_id=%s ORDER BY created_at",
            (reco_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT created_at, from_status, to_status, actor, actor_kind, reason "
            "FROM recommendation_transitions ORDER BY created_at DESC LIMIT 40"
        ).fetchall()
    logger.info("%-26s %-10s→%-10s %-14s %-6s %s", "when", "from", "to", "actor", "kind", "reason")
    for ts, frm, to, actor, kind, reason in rows:
        logger.info("%-26s %-10s→%-10s %-14s %-6s %s", str(ts)[:26], frm or "—", to, actor, kind, reason or "")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Human control room for agent recommendations.")
    p.add_argument("command", choices=["status", "list", "approve", "reject", "apply", "history"])
    p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    p.add_argument("--by", help="actor (username) for transitions")
    p.add_argument("--action", help="filter/target by action class (EXPEDITE/ORDER_RUSH/ORDER_NOW)")
    p.add_argument("--id", dest="reco_id", help="target a single recommendation_id")
    p.add_argument("--reason", help="reason (required for reject)")
    p.add_argument("--note", help="optional note")
    p.add_argument("--all-approved", action="store_true", help="apply: target all APPROVED")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--allow-dev", action="store_true")
    args = p.parse_args(argv)
    if not args.dsn:
        logger.error("DATABASE_URL not set")
        return 2
    _guard(args.dsn, args.allow_dev)

    with psycopg.connect(args.dsn) as conn:
        if args.command == "status":
            cmd_status(conn)
        elif args.command == "list":
            cmd_list(conn, args.action, args.limit)
        elif args.command == "history":
            cmd_history(conn, args.reco_id)
        elif args.command in ("approve", "reject", "apply"):
            if not args.by:
                logger.error("--by USER is required for %s", args.command)
                return 2
            if args.command == "reject" and not args.reason:
                logger.error("--reason is required for reject")
                return 2
            to_status = {"approve": "APPROVED", "reject": "REJECTED", "apply": "APPLIED"}[args.command]
            reason = args.reason or args.note
            moved, skipped = _transition(conn, to_status, args.by, reason,
                                         args.action, args.reco_id, args.allow_dev)
            conn.commit()
            logger.info("%s by %s → %d moved to %s, %d skipped (invalid transition)",
                        args.command.upper(), args.by, moved, to_status, skipped)
    return 0


if __name__ == "__main__":
    sys.exit(main())
