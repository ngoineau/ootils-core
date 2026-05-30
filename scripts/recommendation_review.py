"""
recommendation_review.py — Human control room for agent recommendations.

The agent (shortage_watcher) produces L1 DRAFT recommendations. This tool is
the HUMAN side of the Decision Ladder: a planner reviews, approves, rejects, and
marks recommendations applied. Every transition is audited (who, when, why).

Non-negotiable (strategy doc §13): no external ERP mutation without explicit
human approval. APPROVED/APPLIED transitions are human (L3/L4) decisions only —
this tool records them; it does not itself push to any ERP.

State machine:
    DRAFT     → REVIEWED | APPROVED | REJECTED
    REVIEWED  → APPROVED | REJECTED
    APPROVED  → APPLIED  | REJECTED
    EXPIRED / APPLIED / REJECTED = terminal

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

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("reco_review")

ALLOWED = {
    "DRAFT": {"REVIEWED", "APPROVED", "REJECTED"},
    "REVIEWED": {"APPROVED", "REJECTED"},
    "APPROVED": {"APPLIED", "REJECTED"},
}

ENSURE_SCHEMA = """
CREATE TABLE IF NOT EXISTS recommendation_transitions (
    transition_id     UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    recommendation_id UUID NOT NULL,
    from_status       TEXT,
    to_status         TEXT NOT NULL,
    actor             TEXT NOT NULL,
    actor_kind        TEXT NOT NULL DEFAULT 'human',
    reason            TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_reco_trans_reco ON recommendation_transitions (recommendation_id, created_at);
"""


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
    """Apply a state transition to a set of recommendations, logging each."""
    # Select targets + their current status, locking them
    if reco_id:
        targets = conn.execute(
            "SELECT recommendation_id, status FROM recommendations WHERE recommendation_id=%s FOR UPDATE",
            (reco_id,),
        ).fetchall()
    else:
        # bulk by action; for APPLIED, source must be APPROVED, else DRAFT/REVIEWED
        src_statuses = ("APPROVED",) if to_status == "APPLIED" else ("DRAFT", "REVIEWED")
        q = "SELECT recommendation_id, status FROM recommendations WHERE status = ANY(%s)"
        params = [list(src_statuses)]
        if action:
            q += " AND action=%s"
            params.append(action)
        q += " FOR UPDATE"
        targets = conn.execute(q, params).fetchall()

    moved, skipped = 0, 0
    for rid, cur_status in targets:
        if to_status not in ALLOWED.get(cur_status, set()):
            skipped += 1
            continue
        conn.execute(
            "UPDATE recommendations SET status=%s, updated_at=now() WHERE recommendation_id=%s",
            (to_status, rid),
        )
        conn.execute(
            "INSERT INTO recommendation_transitions (recommendation_id, from_status, to_status, actor, actor_kind, reason) "
            "VALUES (%s,%s,%s,%s,'human',%s)",
            (rid, cur_status, to_status, actor, reason),
        )
        moved += 1
    return moved, skipped


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
        conn.execute(ENSURE_SCHEMA)
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
