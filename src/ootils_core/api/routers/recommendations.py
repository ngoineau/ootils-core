"""
/v1/recommendations — read + govern agent recommendations (chantier #341a).

The agent fleet writes L1 DRAFT recommendations (migration 039); humans
review, approve, reject, and mark them applied. This router exposes:

  GET  /v1/recommendations                 list (scenario-scoped, paginated)
  GET  /v1/recommendations/{id}            detail + evidence + audit trail
  POST /v1/recommendations/{id}/transition governed status change

The state machine lives in engine/recommendation/state_machine.py — the
single source of truth shared with the CLI control room
(scripts/recommendation_review.py). This router never re-implements the
transition rules.

Every applied transition emits a 'recommendation_transition' event
(migration 051) so agents can stream governance changes (Streamable
principle) instead of polling.
"""
from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any, List, Literal, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.recommendation.state_machine import (
    ALLOWED_TRANSITIONS,
    HumanGateError,
    InvalidTransitionError,
    RecommendationNotFoundError,
    transition_one,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/recommendations", tags=["recommendations"])

# Keep in sync with the CHECK constraints in migration 039.
VALID_STATUSES = frozenset(ALLOWED_TRANSITIONS)
VALID_ACTIONS = frozenset({"EXPEDITE", "ORDER_RUSH", "ORDER_NOW"})

# Decision Ladder guard (strategy doc §5): APPROVED and APPLIED are L3/L4
# decisions — humans only. The rule itself lives in the state machine
# (state_machine.HUMAN_ONLY_TARGETS / enforce_human_gate) so every consumer
# — this router, the CLI, any future in-process caller — hits the same gate;
# the router only maps HumanGateError to a 403. Transitional until per-token
# agent scopes land (#350): actor_kind is self-declared, and once token
# scopes exist an agent token will be structurally unable to claim 'human'.


# ---------------------------------------------------------------------------
# Response / request models
# ---------------------------------------------------------------------------


class RecommendationOut(BaseModel):
    recommendation_id: UUID
    agent_name: str
    agent_run_id: UUID
    scenario_id: UUID
    item_id: UUID
    item_external_id: str
    shortage_date: date
    deficit_qty: Decimal
    recommended_qty: Decimal
    estimated_cost: Optional[Decimal] = None
    currency: Optional[str] = None
    supplier_id: Optional[UUID] = None
    supplier_external_id: Optional[str] = None
    lead_time_days: Optional[int] = None
    runway_days: Optional[int] = None
    margin_days: Optional[int] = None
    action: str
    decision_level: str
    status: str
    confidence: str
    created_at: str
    updated_at: str


class RecommendationsListResponse(BaseModel):
    recommendations: List[RecommendationOut]
    total: int
    limit: int
    offset: int


class TransitionRecord(BaseModel):
    transition_id: UUID
    from_status: Optional[str] = None
    to_status: str
    actor: str
    actor_kind: str
    reason: Optional[str] = None
    created_at: str


class RecommendationDetailOut(RecommendationOut):
    # JSONB carve-out payload (migration 039): forensic evidence trail —
    # unbounded diagnostic shape, surfaced verbatim for explainability.
    evidence: Optional[Any] = None
    transitions: List[TransitionRecord] = []


class TransitionRequest(BaseModel):
    to_status: Literal["DRAFT", "REVIEWED", "APPROVED", "REJECTED", "APPLIED", "EXPIRED"]
    actor: str = Field(min_length=1, max_length=200, description="Username or agent name")
    actor_kind: Literal["human", "agent"] = "human"
    reason: Optional[str] = None


class TransitionResponse(BaseModel):
    recommendation_id: UUID
    transition_id: UUID
    scenario_id: UUID
    from_status: str
    to_status: str
    actor: str
    actor_kind: str
    event_id: UUID


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iso(value: Any) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _to_out(row: dict) -> RecommendationOut:
    return RecommendationOut(
        recommendation_id=row["recommendation_id"],
        agent_name=row["agent_name"],
        agent_run_id=row["agent_run_id"],
        scenario_id=row["scenario_id"],
        item_id=row["item_id"],
        item_external_id=row["item_external_id"],
        shortage_date=row["shortage_date"],
        deficit_qty=row["deficit_qty"],
        recommended_qty=row["recommended_qty"],
        estimated_cost=row.get("estimated_cost"),
        currency=row.get("currency"),
        supplier_id=row.get("supplier_id"),
        supplier_external_id=row.get("supplier_external_id"),
        lead_time_days=row.get("lead_time_days"),
        runway_days=row.get("runway_days"),
        margin_days=row.get("margin_days"),
        action=row["action"],
        decision_level=row["decision_level"],
        status=row["status"],
        confidence=row["confidence"],
        created_at=_iso(row["created_at"]),
        updated_at=_iso(row["updated_at"]),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=RecommendationsListResponse)
def list_recommendations(
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
    scenario_id: UUID = Depends(resolve_scenario_id),
    status_filter: Optional[str] = Query(default=None, alias="status"),
    action: Optional[str] = Query(default=None),
    agent_name: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> RecommendationsListResponse:
    """List recommendations for a scenario (baseline by default), newest first."""
    if status_filter is not None and status_filter not in VALID_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unknown status '{status_filter}'. Valid: {sorted(VALID_STATUSES)}",
        )
    if action is not None and action not in VALID_ACTIONS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Unknown action '{action}'. Valid: {sorted(VALID_ACTIONS)}",
        )

    conditions = ["scenario_id = %s"]
    params: list = [scenario_id]
    if status_filter is not None:
        conditions.append("status = %s")
        params.append(status_filter)
    if action is not None:
        conditions.append("action = %s")
        params.append(action)
    if agent_name is not None:
        conditions.append("agent_name = %s")
        params.append(agent_name)
    where_clause = "WHERE " + " AND ".join(conditions)

    count_row = db.execute(
        f"SELECT COUNT(*) AS total FROM recommendations {where_clause}",  # noqa: S608 — static columns, parameterized values
        params,
    ).fetchone()
    total = int(count_row["total"]) if count_row else 0

    rows = db.execute(
        f"""
        SELECT * FROM recommendations
        {where_clause}
        ORDER BY created_at DESC, recommendation_id
        LIMIT %s OFFSET %s
        """,  # noqa: S608
        params + [limit, offset],
    ).fetchall()

    return RecommendationsListResponse(
        recommendations=[_to_out(r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{recommendation_id}", response_model=RecommendationDetailOut)
def get_recommendation(
    recommendation_id: UUID,
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> RecommendationDetailOut:
    """Recommendation detail: evidence trail (explainability) + full transition history."""
    row = db.execute(
        "SELECT * FROM recommendations WHERE recommendation_id = %s",
        (recommendation_id,),
    ).fetchone()
    if not row:
        raise HTTPException(
            status_code=404, detail=f"Recommendation {recommendation_id} not found"
        )

    transitions = db.execute(
        """
        SELECT transition_id, from_status, to_status, actor, actor_kind, reason, created_at
        FROM recommendation_transitions
        WHERE recommendation_id = %s
        ORDER BY created_at, transition_id
        """,
        (recommendation_id,),
    ).fetchall()

    base = _to_out(row)
    return RecommendationDetailOut(
        **base.model_dump(),
        evidence=row.get("evidence"),
        transitions=[
            TransitionRecord(
                transition_id=t["transition_id"],
                from_status=t.get("from_status"),
                to_status=t["to_status"],
                actor=t["actor"],
                actor_kind=t["actor_kind"],
                reason=t.get("reason"),
                created_at=_iso(t["created_at"]),
            )
            for t in transitions
        ],
    )


@router.post("/{recommendation_id}/transition", response_model=TransitionResponse)
def transition_recommendation(
    recommendation_id: UUID,
    body: TransitionRequest,
    db: DictRowConnection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> TransitionResponse:
    """Apply a governed state-machine transition to one recommendation."""
    try:
        result = transition_one(
            db,
            recommendation_id,
            body.to_status,
            body.actor,
            actor_kind=body.actor_kind,
            reason=body.reason,
        )
    except HumanGateError as e:
        # Decision Ladder gate — enforced by the state machine itself (single
        # source of truth); the router only maps it to HTTP. Transitional
        # until per-token agent scopes land (#350): actor_kind is
        # self-declared.
        raise HTTPException(
            status_code=403,
            detail=(
                f"Transition to '{e.to_status}' is an L3/L4 decision reserved to "
                "human actors (Decision Ladder, strategy doc §5). Agents may move "
                "recommendations to REVIEWED or REJECTED only."
            ),
        )
    except RecommendationNotFoundError:
        raise HTTPException(
            status_code=404, detail=f"Recommendation {recommendation_id} not found"
        )
    except InvalidTransitionError as e:
        # Hand-authored message from typed attributes (never str(exc)):
        # statuses come from the DB CHECK constraint + the Literal-validated body.
        allowed = ", ".join(sorted(e.allowed)) if e.allowed else "none (terminal status)"
        raise HTTPException(
            status_code=409,
            detail=(
                f"Invalid transition {e.from_status} -> {e.to_status} for "
                f"recommendation {recommendation_id}. Allowed from {e.from_status}: {allowed}."
            ),
        )

    # Streamable principle: emit a governance event so agents can subscribe
    # to recommendation state changes. Typed delta columns, no trigger node
    # (a recommendation is not a graph node — no propagation intended).
    event_id = uuid4()
    db.execute(
        """
        INSERT INTO events (
            event_id, event_type, scenario_id,
            field_changed, old_text, new_text,
            processed, source, user_ref, created_at
        ) VALUES (%s, 'recommendation_transition', %s, 'status', %s, %s, FALSE, 'api', %s, now())
        """,
        (event_id, result.scenario_id, result.from_status, result.to_status, body.actor),
    )
    logger.info(
        "recommendation.transition.event event_id=%s reco=%s %s->%s",
        event_id,
        recommendation_id,
        result.from_status,
        result.to_status,
    )

    return TransitionResponse(
        recommendation_id=recommendation_id,
        transition_id=result.transition_id,
        scenario_id=result.scenario_id,
        from_status=result.from_status,
        to_status=result.to_status,
        actor=result.actor,
        actor_kind=result.actor_kind,
        event_id=event_id,
    )
