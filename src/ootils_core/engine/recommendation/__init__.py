"""Recommendation governance — state machine + reschedule mapping for the agent
recommendation lifecycle."""
from .reschedule import (
    RESCHEDULE_ACTIONS,
    RescheduleRecommendation,
    build_recommendation,
    reschedule_recommendation_id,
)
from .state_machine import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    InvalidTransitionError,
    RecommendationNotFoundError,
    TransitionResult,
    allowed_targets,
    is_valid_transition,
    transition_many,
    transition_one,
    validate_transition,
)

__all__ = [
    "ALLOWED_TRANSITIONS",
    "RESCHEDULE_ACTIONS",
    "TERMINAL_STATUSES",
    "InvalidTransitionError",
    "RecommendationNotFoundError",
    "RescheduleRecommendation",
    "TransitionResult",
    "allowed_targets",
    "build_recommendation",
    "is_valid_transition",
    "reschedule_recommendation_id",
    "transition_many",
    "transition_one",
    "validate_transition",
]
