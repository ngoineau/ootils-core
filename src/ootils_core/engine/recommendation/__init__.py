"""Recommendation governance — state machine for the agent recommendation lifecycle."""
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
    "TERMINAL_STATUSES",
    "InvalidTransitionError",
    "RecommendationNotFoundError",
    "TransitionResult",
    "allowed_targets",
    "is_valid_transition",
    "transition_many",
    "transition_one",
    "validate_transition",
]
