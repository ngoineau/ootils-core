"""
state_machine.py — Recommendation lifecycle state machine (single source of truth).

The agent fleet produces L1 DRAFT recommendations (migration 039); every status
change is audited in recommendation_transitions (migration 040). This module
owns the transition rules and the transactional write path (row lock + status
update + audit row) so that every consumer — the CLI control room
(scripts/recommendation_review.py) and the REST API
(api/routers/recommendations.py) — enforces the exact same machine.
Do NOT duplicate these rules elsewhere.

State machine:
    DRAFT     → REVIEWED | APPROVED | REJECTED
    REVIEWED  → APPROVED | REJECTED
    APPROVED  → APPLIED  | REJECTED
    APPLIED / REJECTED / EXPIRED = terminal (no outbound transitions)

EXPIRED is reached outside this machine: the watcher agents supersede stale
DRAFT rows directly when they re-run (see scripts/agent_shortage_watcher.py).
Requesting a transition *to* EXPIRED through this module is therefore invalid.

Locking: both write paths SELECT ... FOR UPDATE the target rows, so two
concurrent transitions on the same recommendation serialize and the loser
re-reads a status for which the transition may no longer be valid.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID

from psycopg.rows import dict_row

from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

# Transition rules — keep in sync with the recommendations.status CHECK
# constraint (migration 039). Terminal statuses map to an empty set.
ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "DRAFT": frozenset({"REVIEWED", "APPROVED", "REJECTED"}),
    "REVIEWED": frozenset({"APPROVED", "REJECTED"}),
    "APPROVED": frozenset({"APPLIED", "REJECTED"}),
    "APPLIED": frozenset(),
    "REJECTED": frozenset(),
    "EXPIRED": frozenset(),
}

TERMINAL_STATUSES: frozenset[str] = frozenset(
    status for status, targets in ALLOWED_TRANSITIONS.items() if not targets
)

# Decision Ladder gate (North Star L3+): approving or applying a
# recommendation is a human decision. Enforced HERE — the single source of
# truth of the machine — so no in-process caller (router, CLI, future
# watcher/orchestrator import) can bypass it. actor_kind is self-declared
# until per-token agent scopes land (#350); this gate is the transitional
# floor, not the final authentication story.
HUMAN_ONLY_TARGETS: frozenset[str] = frozenset({"APPROVED", "APPLIED"})


class RecommendationNotFoundError(Exception):
    """Raised when the target recommendation_id does not exist."""

    def __init__(self, recommendation_id: UUID) -> None:
        self.recommendation_id = recommendation_id
        super().__init__(f"Recommendation {recommendation_id} not found")


class InvalidTransitionError(Exception):
    """Raised when a requested transition violates the state machine."""

    def __init__(self, from_status: str, to_status: str) -> None:
        self.from_status = from_status
        self.to_status = to_status
        self.allowed = allowed_targets(from_status)
        allowed_s = ", ".join(sorted(self.allowed)) if self.allowed else "none (terminal status)"
        super().__init__(
            f"Invalid transition {from_status} -> {to_status}; "
            f"allowed from {from_status}: {allowed_s}"
        )


class HumanGateError(Exception):
    """Raised when a non-human actor requests a human-only transition (L3+)."""

    def __init__(self, to_status: str, actor_kind: str) -> None:
        self.to_status = to_status
        self.actor_kind = actor_kind
        super().__init__(
            f"Transition to {to_status} requires a human actor "
            f"(Decision Ladder L3+); got actor_kind={actor_kind!r}"
        )


def enforce_human_gate(to_status: str, actor_kind: str) -> None:
    """Raise HumanGateError if `to_status` is human-only and the actor is not."""
    if to_status in HUMAN_ONLY_TARGETS and actor_kind != "human":
        raise HumanGateError(to_status, actor_kind)


def allowed_targets(from_status: str) -> frozenset[str]:
    """Return the set of statuses reachable from `from_status` (empty if terminal/unknown)."""
    return ALLOWED_TRANSITIONS.get(from_status, frozenset())


def is_valid_transition(from_status: str, to_status: str) -> bool:
    """True if the state machine allows `from_status` → `to_status`."""
    return to_status in allowed_targets(from_status)


def validate_transition(from_status: str, to_status: str) -> None:
    """Raise InvalidTransitionError if the transition is not allowed."""
    if not is_valid_transition(from_status, to_status):
        raise InvalidTransitionError(from_status, to_status)


@dataclass(frozen=True)
class TransitionResult:
    """Outcome of a successfully applied transition (audit row included)."""

    transition_id: UUID
    recommendation_id: UUID
    scenario_id: UUID
    from_status: str
    to_status: str
    actor: str
    actor_kind: str
    reason: Optional[str]
    created_at: datetime


def transition_one(
    conn: DictRowConnection,
    recommendation_id: UUID,
    to_status: str,
    actor: str,
    *,
    actor_kind: str = "human",
    reason: Optional[str] = None,
) -> TransitionResult:
    """
    Apply a single validated transition: lock the row (FOR UPDATE), validate
    against the machine, update recommendations.status, insert the audit row.

    Raises RecommendationNotFoundError / InvalidTransitionError /
    HumanGateError. The caller owns the transaction (commit/rollback) —
    this function only writes.
    """
    enforce_human_gate(to_status, actor_kind)
    # Per-cursor dict_row so this works on any connection row factory
    # (the CLI uses default tuple rows; the API uses dict_row).
    with conn.cursor(row_factory=dict_row) as cur:
        row = cur.execute(
            "SELECT status, scenario_id FROM recommendations "
            "WHERE recommendation_id = %s FOR UPDATE",
            (recommendation_id,),
        ).fetchone()
        if row is None:
            raise RecommendationNotFoundError(recommendation_id)
        from_status = row["status"]
        validate_transition(from_status, to_status)

        cur.execute(
            "UPDATE recommendations SET status = %s, updated_at = now() "
            "WHERE recommendation_id = %s",
            (to_status, recommendation_id),
        )
        audit = cur.execute(
            "INSERT INTO recommendation_transitions "
            "(recommendation_id, from_status, to_status, actor, actor_kind, reason) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "RETURNING transition_id, created_at",
            (recommendation_id, from_status, to_status, actor, actor_kind, reason),
        ).fetchone()
        if audit is None:
            # INSERT ... RETURNING always yields exactly one row; a None here
            # would mean the driver contract is broken — fail loudly rather
            # than index into None below.
            raise RuntimeError(
                "recommendation_transitions INSERT ... RETURNING produced no row "
                f"for recommendation {recommendation_id}"
            )

    logger.info(
        "recommendation.transition reco=%s %s->%s actor=%s kind=%s",
        recommendation_id,
        from_status,
        to_status,
        actor,
        actor_kind,
    )
    return TransitionResult(
        transition_id=audit["transition_id"],
        recommendation_id=recommendation_id,
        scenario_id=row["scenario_id"],
        from_status=from_status,
        to_status=to_status,
        actor=actor,
        actor_kind=actor_kind,
        reason=reason,
        created_at=audit["created_at"],
    )


def transition_many(
    conn: DictRowConnection,
    to_status: str,
    actor: str,
    *,
    actor_kind: str = "human",
    reason: Optional[str] = None,
    action: Optional[str] = None,
    recommendation_id: Optional[UUID | str] = None,
) -> tuple[int, int]:
    """
    Bulk transition with CLI semantics: rows whose current status does not
    allow `to_status` are *skipped* (counted), not raised. Targets are either
    a single recommendation_id or all rows in the plausible source statuses
    (APPROVED for APPLIED, DRAFT/REVIEWED otherwise), optionally filtered by
    action class. All targets are locked FOR UPDATE before any write.

    Returns (moved, skipped). The caller owns the transaction.
    Raises HumanGateError for human-only targets with a non-human actor.
    """
    enforce_human_gate(to_status, actor_kind)
    with conn.cursor(row_factory=dict_row) as cur:
        if recommendation_id:
            targets = cur.execute(
                "SELECT recommendation_id, status FROM recommendations "
                "WHERE recommendation_id = %s FOR UPDATE",
                (recommendation_id,),
            ).fetchall()
        else:
            src_statuses = ("APPROVED",) if to_status == "APPLIED" else ("DRAFT", "REVIEWED")
            q = "SELECT recommendation_id, status FROM recommendations WHERE status = ANY(%s)"
            params: list = [list(src_statuses)]
            if action:
                q += " AND action = %s"
                params.append(action)
            q += " FOR UPDATE"
            targets = cur.execute(q, params).fetchall()

        moved, skipped = 0, 0
        for row in targets:
            rid, cur_status = row["recommendation_id"], row["status"]
            if not is_valid_transition(cur_status, to_status):
                skipped += 1
                continue
            cur.execute(
                "UPDATE recommendations SET status = %s, updated_at = now() "
                "WHERE recommendation_id = %s",
                (to_status, rid),
            )
            cur.execute(
                "INSERT INTO recommendation_transitions "
                "(recommendation_id, from_status, to_status, actor, actor_kind, reason) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (rid, cur_status, to_status, actor, actor_kind, reason),
            )
            moved += 1

    logger.info(
        "recommendation.transition_many to=%s actor=%s kind=%s moved=%d skipped=%d",
        to_status,
        actor,
        actor_kind,
        moved,
        skipped,
    )
    return moved, skipped
