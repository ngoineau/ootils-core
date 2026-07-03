"""
Pure unit tests for the recommendation state machine
(src/ootils_core/engine/recommendation/state_machine.py).

Only the transition-rule layer is tested here — no DB. The transactional
write paths (transition_one / transition_many) are covered by
tests/integration/test_recommendations_api_integration.py against real
Postgres.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from ootils_core.engine.recommendation.state_machine import (
    ALLOWED_TRANSITIONS,
    HUMAN_ONLY_TARGETS,
    TERMINAL_STATUSES,
    HumanGateError,
    InvalidTransitionError,
    RecommendationNotFoundError,
    allowed_targets,
    enforce_human_gate,
    is_valid_transition,
    validate_transition,
)

# All statuses of the migration 039 CHECK constraint
ALL_STATUSES = {"DRAFT", "REVIEWED", "APPROVED", "REJECTED", "APPLIED", "EXPIRED"}


class TestHumanGate:
    """Decision Ladder L3+ gate — lives in the ENGINE, not just the router,
    so no in-process caller (future watcher, orchestrator import) can move a
    recommendation to APPROVED/APPLIED with a non-human actor."""

    def test_human_only_targets(self):
        assert HUMAN_ONLY_TARGETS == {"APPROVED", "APPLIED"}

    @pytest.mark.parametrize("to_status", sorted(HUMAN_ONLY_TARGETS))
    def test_agent_blocked_on_human_only_targets(self, to_status):
        with pytest.raises(HumanGateError) as exc:
            enforce_human_gate(to_status, "agent")
        assert to_status in str(exc.value)
        assert "agent" in str(exc.value)

    @pytest.mark.parametrize("to_status", sorted(HUMAN_ONLY_TARGETS))
    def test_human_passes_human_only_targets(self, to_status):
        enforce_human_gate(to_status, "human")  # must not raise

    @pytest.mark.parametrize("to_status", ["REVIEWED", "REJECTED"])
    def test_agent_allowed_on_non_gated_targets(self, to_status):
        enforce_human_gate(to_status, "agent")  # must not raise


class TestTransitionRules:
    def test_machine_covers_every_status(self):
        assert set(ALLOWED_TRANSITIONS) == ALL_STATUSES

    def test_terminal_statuses(self):
        assert TERMINAL_STATUSES == {"APPLIED", "REJECTED", "EXPIRED"}

    @pytest.mark.parametrize(
        "frm,to",
        [
            ("DRAFT", "REVIEWED"),
            ("DRAFT", "APPROVED"),
            ("DRAFT", "REJECTED"),
            ("REVIEWED", "APPROVED"),
            ("REVIEWED", "REJECTED"),
            ("APPROVED", "APPLIED"),
            ("APPROVED", "REJECTED"),
        ],
    )
    def test_valid_transitions(self, frm, to):
        assert is_valid_transition(frm, to)
        validate_transition(frm, to)  # must not raise

    @pytest.mark.parametrize(
        "frm,to",
        [
            ("DRAFT", "APPLIED"),        # cannot skip approval
            ("DRAFT", "EXPIRED"),        # EXPIRED set by watcher supersede, not the machine
            ("DRAFT", "DRAFT"),          # self-loop
            ("REVIEWED", "APPLIED"),
            ("REVIEWED", "DRAFT"),       # no backwards move
            ("APPROVED", "REVIEWED"),
            ("APPROVED", "DRAFT"),
        ],
    )
    def test_invalid_transitions(self, frm, to):
        assert not is_valid_transition(frm, to)
        with pytest.raises(InvalidTransitionError):
            validate_transition(frm, to)

    @pytest.mark.parametrize("terminal", sorted(TERMINAL_STATUSES))
    @pytest.mark.parametrize("to", sorted(ALL_STATUSES))
    def test_terminal_statuses_have_no_outbound(self, terminal, to):
        assert not is_valid_transition(terminal, to)

    def test_unknown_status_has_no_targets(self):
        assert allowed_targets("NO_SUCH_STATUS") == frozenset()
        assert not is_valid_transition("NO_SUCH_STATUS", "APPROVED")

    def test_unknown_target_is_invalid(self):
        assert not is_valid_transition("DRAFT", "NO_SUCH_STATUS")
        with pytest.raises(InvalidTransitionError):
            validate_transition("DRAFT", "NO_SUCH_STATUS")


class TestErrorPayloads:
    def test_invalid_transition_error_attributes(self):
        with pytest.raises(InvalidTransitionError) as exc_info:
            validate_transition("APPROVED", "REVIEWED")
        err = exc_info.value
        assert err.from_status == "APPROVED"
        assert err.to_status == "REVIEWED"
        assert err.allowed == frozenset({"APPLIED", "REJECTED"})
        # message lists the allowed targets for actionability
        assert "APPLIED" in str(err) and "REJECTED" in str(err)

    def test_invalid_transition_error_from_terminal(self):
        with pytest.raises(InvalidTransitionError) as exc_info:
            validate_transition("REJECTED", "APPROVED")
        assert exc_info.value.allowed == frozenset()
        assert "terminal" in str(exc_info.value)

    def test_not_found_error_carries_id(self):
        rid = uuid4()
        err = RecommendationNotFoundError(rid)
        assert err.recommendation_id == rid
        assert str(rid) in str(err)
