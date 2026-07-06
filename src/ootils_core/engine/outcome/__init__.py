"""
Recommendation-outcome engine — the proof-of-value core (chantier #393 A3-PR2,
ADR-030).

evaluator: the deterministic (never-LLM) classifier that chains a governed
    recommendation (migration 039) to its observed real-world result and values
    the shortage $ it avoided. ``evaluate_outcome`` is PURE (golden-testable in
    isolation over its five branches); ``evaluate_and_persist`` loads the
    eligible recos + observed ``shortages`` + observation snapshots and upserts
    verdicts into ``recommendation_outcomes`` (migration 069). Read-only on
    recommendations/shortages/inventory_snapshots (ADR-021); writes ONLY
    recommendation_outcomes.
"""
from ootils_core.engine.outcome.evaluator import (
    ACTED_STATUSES,
    AVOIDED_EPS_ABS,
    AVOIDED_EPS_RATIO,
    MATERIALIZED_FLOOR_RATIO,
    VALID_STATUSES,
    ObservedShortage,
    OutcomeRow,
    evaluate_and_persist,
    evaluate_outcome,
)

__all__ = [
    "ACTED_STATUSES",
    "AVOIDED_EPS_ABS",
    "AVOIDED_EPS_RATIO",
    "MATERIALIZED_FLOOR_RATIO",
    "VALID_STATUSES",
    "ObservedShortage",
    "OutcomeRow",
    "evaluate_and_persist",
    "evaluate_outcome",
]
