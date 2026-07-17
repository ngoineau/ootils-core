"""
tests/test_daily_run_decision.py — Pure unit tests for the governed daily-run
DECISION engine (ADR-042 PR-3, absorbing ADR-037's INT-1 PR3 §0 option (a)).
No database, no network.

Covers the pure half of src/ootils_core/engine/ingest/apply.py —
``decide_daily_run`` + ``_dq_status_to_guard_status`` and the frozen result
dataclasses. The DB-touching half (``plan_daily_run_decision`` /
``record_daily_run_decision``, the ``daily_run_completed`` event, the L3
webhook escalation) lives in
tests/integration/test_daily_run_decision_integration.py.

  1. The COMPLETE decision matrix (ADR-037 §0 option (a)):
       * every feed's guard AND DQ green            -> AUTO_APPROVED
       * any ``blocking`` feed FAILED (guard OR DQ) -> ESCALATED
       * any ``advisory`` feed FAILED               -> DEGRADED (approved-
         degraded, traced in ``reasons`` — never blocked, never silent)
       * blocking FAILED + advisory FAILED          -> ESCALATED (a blocking
         failure always wins)
  2. NOT_EVALUATED (SKIP) handling — the cadrage's tranché, verified exactly:
     a guard or DQ verdict that is not a DEMONSTRATED green (guard
     NOT_EVALUATED, DQ absent/None, DQ inconclusive string) makes the run
     DEGRADED — NEVER silently promoted to AUTO_APPROVED (None-honest), and
     NEVER escalated to a human unless there is an ACTUAL blocking-feed
     FAILURE. FAILED dominates NOT_EVALUATED inside one feed's combined
     verdict.
  3. The DQ-status vocabulary translation (module docstring "DQ STATUS
     VOCABULARY"): only 'validated' is OK, only 'rejected' is FAILED,
     everything else — including 'warning', 'pending', 'running', 'unknown',
     None and any future string — is NOT_EVALUATED, and the translation
     never raises.
  4. Fail-loudly refusals: empty feed_inputs (never a vacuous AUTO_APPROVED
     over zero feeds) and an unrecognized criticality both raise ValueError.
  5. Explainability: every non-green feed carries a reason naming feed_key /
     criticality / guard verdict / DQ status; ``DailyRunDecision.reasons`` is
     exactly the non-green subset (empty on a full green).
  6. Determinism: pure function, caller-supplied ``evaluated_at``, same
     inputs -> structurally equal decisions (frozen dataclasses), input feed
     order preserved.
  7. Lockstep: RunDecisionStatus values are the exact strings the
     ``daily_run_completed`` event's ``field_changed`` column carries
     (emit.py's typed-column contract, migration 079).
"""
from __future__ import annotations

import dataclasses
from datetime import date, datetime, timezone

import pytest

import ootils_core.engine.ingest.apply as apply_mod
from ootils_core.engine.ingest.apply import (
    DailyRunDecision,
    FeedDecisionInput,
    RunDecisionStatus,
    decide_daily_run,
)
from ootils_core.interfaces.guards import GuardStatus

RUN_DATE = date(2026, 7, 13)
STAMP = datetime(2026, 7, 13, 8, 0, tzinfo=timezone.utc)


def _feed(
    feed_key: str = "unit-feed",
    criticality: str = "blocking",
    guard_status: GuardStatus = GuardStatus.OK,
    dq_status: str | None = "validated",
) -> FeedDecisionInput:
    return FeedDecisionInput(
        feed_key=feed_key,
        criticality=criticality,
        guard_status=guard_status,
        dq_status=dq_status,
    )


def _decide(*feeds: FeedDecisionInput) -> DailyRunDecision:
    return decide_daily_run(list(feeds), RUN_DATE, evaluated_at=STAMP)


# ---------------------------------------------------------------------------
# 1. The DQ-status vocabulary translation
# ---------------------------------------------------------------------------


class TestDqStatusToGuardStatus:
    def test_validated_is_the_only_ok(self):
        assert apply_mod._dq_status_to_guard_status("validated") == GuardStatus.OK

    def test_rejected_is_the_only_failed(self):
        assert apply_mod._dq_status_to_guard_status("rejected") == GuardStatus.FAILED

    @pytest.mark.parametrize(
        "inconclusive",
        [
            None,          # feed absent from dq_status_by_feed — no observation
            "pending",     # DQ not finished
            "running",     # DQ not finished
            "unknown",     # _trigger_dq's the-DQ-run-itself-raised sentinel
            "warning",     # allowed by migration 010's CHECK, never emitted today
            "some_future_status",  # a future DQ value must not crash the engine
            "",            # degenerate string
            "VALIDATED",   # case matters — not the vocabulary's 'validated'
        ],
    )
    def test_everything_else_is_not_evaluated_never_raises(self, inconclusive):
        """None-honest: an inconclusive DQ verdict is neither a fabricated
        green nor a false red — and an unrecognized string never raises
        (module docstring "DQ STATUS VOCABULARY")."""
        assert (
            apply_mod._dq_status_to_guard_status(inconclusive)
            == GuardStatus.NOT_EVALUATED
        )


# ---------------------------------------------------------------------------
# 2. Per-feed combined verdict (guard x DQ inside one feed)
# ---------------------------------------------------------------------------


class TestPerFeedCombinedStatus:
    def test_green_guard_and_validated_dq_is_ok(self):
        decision = _decide(_feed())
        assert decision.feeds[0].combined_status == GuardStatus.OK

    def test_failed_guard_dominates_green_dq(self):
        decision = _decide(_feed(guard_status=GuardStatus.FAILED))
        assert decision.feeds[0].combined_status == GuardStatus.FAILED

    def test_rejected_dq_dominates_green_guard(self):
        decision = _decide(_feed(dq_status="rejected"))
        assert decision.feeds[0].combined_status == GuardStatus.FAILED

    def test_failed_guard_dominates_not_evaluated_dq(self):
        """FAILED > NOT_EVALUATED inside one feed: a real guard failure is a
        real failure even when DQ never ran — a blocking feed in this state
        escalates, it does not hide behind the missing DQ verdict."""
        decision = _decide(_feed(guard_status=GuardStatus.FAILED, dq_status=None))
        assert decision.feeds[0].combined_status == GuardStatus.FAILED
        assert decision.status == RunDecisionStatus.ESCALATED

    def test_rejected_dq_dominates_not_evaluated_guard(self):
        decision = _decide(
            _feed(guard_status=GuardStatus.NOT_EVALUATED, dq_status="rejected")
        )
        assert decision.feeds[0].combined_status == GuardStatus.FAILED

    def test_not_evaluated_dq_taints_green_guard_to_not_evaluated(self):
        decision = _decide(_feed(dq_status=None))
        assert decision.feeds[0].combined_status == GuardStatus.NOT_EVALUATED

    def test_not_evaluated_guard_taints_green_dq_to_not_evaluated(self):
        decision = _decide(_feed(guard_status=GuardStatus.NOT_EVALUATED))
        assert decision.feeds[0].combined_status == GuardStatus.NOT_EVALUATED


# ---------------------------------------------------------------------------
# 3. The decision matrix — ADR-037 §0 option (a)
# ---------------------------------------------------------------------------


class TestDecisionMatrix:
    def test_all_green_auto_approves(self):
        """Every feed's guard AND DQ status demonstrated green -> the run is
        AUTO_APPROVED, with zero reasons (nothing to explain away)."""
        decision = _decide(
            _feed("onhand", "blocking"),
            _feed("open-pos", "blocking"),
            _feed("open-wos", "advisory"),
        )
        assert decision.status == RunDecisionStatus.AUTO_APPROVED
        assert decision.reasons == ()
        assert all(f.combined_status == GuardStatus.OK for f in decision.feeds)

    def test_one_blocking_guard_failure_escalates(self):
        decision = _decide(
            _feed("onhand", "blocking", guard_status=GuardStatus.FAILED),
            _feed("open-pos", "blocking"),
        )
        assert decision.status == RunDecisionStatus.ESCALATED

    def test_one_blocking_dq_rejection_escalates_even_with_green_guards(self):
        """The decision combines guards AND DQ: a blocking feed whose guards
        all passed but whose batch DQ was rejected still blocks the run."""
        decision = _decide(
            _feed("onhand", "blocking", dq_status="rejected"),
            _feed("open-pos", "blocking"),
        )
        assert decision.status == RunDecisionStatus.ESCALATED

    def test_one_advisory_guard_failure_degrades_without_blocking(self):
        """An advisory red degrades confidence, never blocks (ADR-037 §0) —
        and it is TRACED: the failed feed's reason is surfaced, never a
        silent degradation."""
        decision = _decide(
            _feed("onhand", "blocking"),
            _feed("forecast-hints", "advisory", guard_status=GuardStatus.FAILED),
        )
        assert decision.status == RunDecisionStatus.DEGRADED
        assert len(decision.reasons) == 1
        assert "forecast-hints" in decision.reasons[0]

    def test_one_advisory_dq_rejection_also_degrades(self):
        decision = _decide(
            _feed("onhand", "blocking"),
            _feed("forecast-hints", "advisory", dq_status="rejected"),
        )
        assert decision.status == RunDecisionStatus.DEGRADED

    def test_blocking_and_advisory_failures_escalate_blocking_wins(self):
        """The combination case: a blocking failure always wins, regardless
        of what any other feed shows — the advisory failure does not soften
        the verdict to DEGRADED, and both culprits stay traced."""
        decision = _decide(
            _feed("onhand", "blocking", guard_status=GuardStatus.FAILED),
            _feed("forecast-hints", "advisory", guard_status=GuardStatus.FAILED),
            _feed("open-pos", "blocking"),
        )
        assert decision.status == RunDecisionStatus.ESCALATED
        assert len(decision.reasons) == 2
        joined = " ".join(decision.reasons)
        assert "onhand" in joined
        assert "forecast-hints" in joined

    def test_advisory_failure_plus_blocking_skip_is_degraded_not_escalated(self):
        """advisory FAILED + blocking NOT_EVALUATED: neither is an ACTUAL
        blocking-feed failure, so no human is paged — but the run is not
        clean either: DEGRADED."""
        decision = _decide(
            _feed("onhand", "blocking", dq_status=None),
            _feed("forecast-hints", "advisory", guard_status=GuardStatus.FAILED),
        )
        assert decision.status == RunDecisionStatus.DEGRADED

    def test_single_green_blocking_feed_auto_approves(self):
        decision = _decide(_feed("onhand", "blocking"))
        assert decision.status == RunDecisionStatus.AUTO_APPROVED


# ---------------------------------------------------------------------------
# 4. NOT_EVALUATED (SKIP) handling — the cadrage's tranché, verified exactly
# ---------------------------------------------------------------------------


class TestNotEvaluatedHandling:
    """The tranché (apply.py docstring): 'not yet confirmed green' is treated
    the same as 'advisory red' — DEGRADED. Never silently promoted to
    AUTO_APPROVED (None-honest), never escalated to a human unless there is
    an ACTUAL blocking-feed FAILURE."""

    def test_blocking_feed_with_absent_dq_is_degraded_never_auto_approved(self):
        decision = _decide(_feed("onhand", "blocking", dq_status=None))
        assert decision.status == RunDecisionStatus.DEGRADED
        assert decision.status != RunDecisionStatus.AUTO_APPROVED

    def test_blocking_feed_with_absent_dq_is_never_escalated(self):
        """A SKIP on a blocking feed is NOT evidence of a problem — no human
        gets paged for a DQ verdict that simply is not in yet."""
        decision = _decide(_feed("onhand", "blocking", dq_status=None))
        assert decision.status != RunDecisionStatus.ESCALATED

    def test_not_evaluated_guard_on_blocking_feed_degrades(self):
        decision = _decide(
            _feed("onhand", "blocking", guard_status=GuardStatus.NOT_EVALUATED)
        )
        assert decision.status == RunDecisionStatus.DEGRADED

    @pytest.mark.parametrize(
        "dq_status", ["pending", "running", "unknown", "warning"]
    )
    def test_inconclusive_dq_strings_degrade(self, dq_status):
        decision = _decide(_feed("onhand", "blocking", dq_status=dq_status))
        assert decision.status == RunDecisionStatus.DEGRADED

    def test_one_skip_among_greens_degrades_the_whole_run(self):
        """AUTO_APPROVED is strict: ONE feed not demonstrably green (here an
        advisory with no DQ observation) is enough to withhold it."""
        decision = _decide(
            _feed("onhand", "blocking"),
            _feed("open-pos", "blocking"),
            _feed("forecast-hints", "advisory", dq_status=None),
        )
        assert decision.status == RunDecisionStatus.DEGRADED
        assert len(decision.reasons) == 1
        assert "forecast-hints" in decision.reasons[0]

    def test_skipped_feed_is_traced_in_reasons(self):
        decision = _decide(_feed("onhand", "blocking", dq_status=None))
        assert len(decision.reasons) == 1
        assert "onhand" in decision.reasons[0]
        assert "not_supplied" in decision.reasons[0]


# ---------------------------------------------------------------------------
# 5. Fail-loudly refusals
# ---------------------------------------------------------------------------


class TestRefusals:
    def test_empty_feed_inputs_raises_never_vacuous_auto_approve(self):
        """Zero feeds evaluated is NOT an all-green run — deciding over
        nothing raises rather than minting a vacuous AUTO_APPROVED."""
        with pytest.raises(ValueError, match="empty"):
            decide_daily_run([], RUN_DATE, evaluated_at=STAMP)

    @pytest.mark.parametrize("bad", ["critical", "BLOCKING", "", "l3"])
    def test_unrecognized_criticality_raises_naming_the_feed(self, bad):
        with pytest.raises(ValueError, match="unit-feed"):
            _decide(_feed("unit-feed", bad))

    def test_valid_criticalities_are_exactly_blocking_and_advisory(self):
        assert apply_mod._VALID_CRITICALITY == {"blocking", "advisory"}


# ---------------------------------------------------------------------------
# 6. Explainability — reasons + per-feed result fields
# ---------------------------------------------------------------------------


class TestExplainability:
    def test_reason_names_feed_criticality_guard_and_dq(self):
        decision = _decide(
            _feed("onhand", "blocking", GuardStatus.FAILED, "rejected")
        )
        result = decision.feeds[0]
        assert "feed_key=onhand" in result.reason
        assert "criticality=blocking" in result.reason
        assert "guard_status=failed" in result.reason
        assert "dq_status=rejected" in result.reason

    def test_absent_dq_is_rendered_not_supplied_never_empty(self):
        decision = _decide(_feed("onhand", "blocking", dq_status=None))
        assert "dq_status=not_supplied" in decision.feeds[0].reason

    def test_reasons_is_exactly_the_non_green_subset(self):
        decision = _decide(
            _feed("green-feed", "blocking"),
            _feed("red-feed", "blocking", guard_status=GuardStatus.FAILED),
            _feed("skip-feed", "advisory", dq_status=None),
        )
        assert len(decision.reasons) == 2
        joined = " ".join(decision.reasons)
        assert "red-feed" in joined
        assert "skip-feed" in joined
        assert "green-feed" not in joined

    def test_feed_fields_carried_verbatim(self):
        decision = _decide(_feed("my-feed", "advisory", GuardStatus.OK, "validated"))
        result = decision.feeds[0]
        assert result.feed_key == "my-feed"
        assert result.criticality == "advisory"
        assert result.guard_status == GuardStatus.OK
        assert result.dq_status == "validated"

    def test_input_feed_order_is_preserved(self):
        decision = _decide(
            _feed("z-feed"), _feed("a-feed"), _feed("m-feed")
        )
        assert [f.feed_key for f in decision.feeds] == ["z-feed", "a-feed", "m-feed"]

    def test_run_date_and_evaluated_at_carried_verbatim(self):
        decision = _decide(_feed())
        assert decision.run_date == RUN_DATE
        assert decision.evaluated_at == STAMP

    def test_default_evaluated_at_is_utc_aware(self):
        """Without a caller-supplied stamp the default is timezone-AWARE UTC
        (never a naive datetime that would poison later comparisons)."""
        decision = decide_daily_run([_feed()], RUN_DATE)
        assert decision.evaluated_at.tzinfo is not None
        assert decision.evaluated_at.utcoffset().total_seconds() == 0


# ---------------------------------------------------------------------------
# 7. Determinism — same inputs, same decision
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_inputs_yield_structurally_equal_decisions(self):
        """Pure function, frozen dataclasses: two calls with identical inputs
        (including evaluated_at) compare EQUAL — the North Star's
        deterministic-core requirement, no wall-clock read inside."""
        feeds = [
            _feed("onhand", "blocking", GuardStatus.FAILED, None),
            _feed("forecast-hints", "advisory", GuardStatus.OK, "validated"),
        ]
        a = decide_daily_run(feeds, RUN_DATE, evaluated_at=STAMP)
        b = decide_daily_run(feeds, RUN_DATE, evaluated_at=STAMP)
        assert a == b
        assert a.feeds == b.feeds
        assert a.status == b.status
        assert a.reasons == b.reasons

    def test_every_matrix_corner_is_reproducible(self):
        for feeds in (
            [_feed()],
            [_feed(guard_status=GuardStatus.FAILED)],
            [_feed(criticality="advisory", dq_status="rejected")],
            [_feed(dq_status=None), _feed("other", "advisory")],
        ):
            assert decide_daily_run(feeds, RUN_DATE, evaluated_at=STAMP) == (
                decide_daily_run(feeds, RUN_DATE, evaluated_at=STAMP)
            )

    def test_decision_dataclasses_are_frozen(self):
        decision = _decide(_feed())
        with pytest.raises(dataclasses.FrozenInstanceError):
            decision.status = RunDecisionStatus.ESCALATED  # type: ignore[misc]
        with pytest.raises(dataclasses.FrozenInstanceError):
            decision.feeds[0].combined_status = GuardStatus.FAILED  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 8. Lockstep — RunDecisionStatus vs the event's field_changed contract
# ---------------------------------------------------------------------------


class TestRunDecisionStatusLockstep:
    def test_values_are_the_event_field_changed_vocabulary(self):
        """record_daily_run_decision stamps decision.status.value into the
        daily_run_completed event's field_changed column (emit.py's
        typed-column contract, migration 079) — these exact strings are what
        stream subscribers key on, so they are pinned here."""
        assert {s.value for s in RunDecisionStatus} == {
            "auto_approved", "escalated", "degraded",
        }

    def test_str_enum_composes_into_event_columns(self):
        # str-Enum: .value is what lands in the TEXT column.
        assert RunDecisionStatus.AUTO_APPROVED.value == "auto_approved"
        assert isinstance(RunDecisionStatus.ESCALATED.value, str)
