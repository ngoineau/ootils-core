"""
tests/test_daily_run_guards.py — Pure unit tests for the daily-run runtime
guards (ADR-042 PR-2, absorbing ADR-037's INT-1 PR2). No database.

Covers src/ootils_core/interfaces/guards.py, which is pure by construction
(no DB, no wall-clock read, no I/O — every timestamp caller-supplied), so
everything here runs DB-free:

  1. compute_expected_arrival_deadline — the V1 "M H * * *" cadence parser:
     happy path (tick + window, UTC-aware), midnight rollover, and the
     fail-loudly refusals (wrong field count, non-daily shapes, step syntax,
     non-integer / out-of-range minute & hour) each NAMING the cadence.
  2. Each of the four guards x the required grid: clear PASS / clear FAIL /
     the EXACT boundary at the threshold / NOT_EVALUATED when the contract
     does not configure the threshold or no honest baseline exists
     (None-honest: never a fabricated OK, never a false FAILED).
  3. GuardResult is exploitable evidence: guard_name + status + a detail
     message carrying the observed value AND the threshold (it feeds the
     daily report and the L3 escalation payload — North Star "Explicable").
  4. FeedGuardEvaluation composition: overall_status FAILED iff any guard
     FAILED; NOT_EVALUATED never blocks on its own; never NOT_EVALUATED
     itself; by_name() lookup; feed_key/criticality carried verbatim.
  5. Determinism: same inputs -> structurally equal results (frozen
     dataclasses), across repeated calls.
  6. Lockstep tripwires: GuardStatus vocabulary vs migration 078's per-guard
     CHECKs (and overall_status's deliberately narrower CHECK), and
     DELETION_RATIO_THRESHOLD vs the staging/diff.py constant it was
     relogged from (ADR-042 decision 2.1).
  7. The timezone contract: an aware deadline compared against a naive
     file_arrived_at/now raises TypeError at the comparison point — never a
     silent miscompute.

DB-touching behaviour (migration 078 schema, record_daily_run persistence,
the prior-day baseline lookup, append-only re-runs) lives in
tests/integration/test_daily_runs_integration.py.
"""
from __future__ import annotations

import dataclasses
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from ootils_core.interfaces.guards import (
    DELETION_RATIO_THRESHOLD,
    GuardResult,
    GuardStatus,
    compute_expected_arrival_deadline,
    evaluate_arrival_window_guard,
    evaluate_deletion_ratio_guard,
    evaluate_feed_guards,
    evaluate_volume_delta_guard,
    evaluate_volume_floor_guard,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_078 = (
    _REPO_ROOT / "src" / "ootils_core" / "db" / "migrations" / "078_daily_runs.sql"
)

# Shared timeline: cadence "0 6 * * *" (daily 06:00 UTC) + a 90-minute window
# puts the arrival deadline at 07:30 UTC on the run date.
RUN_DATE = date(2026, 7, 13)
CADENCE = "0 6 * * *"
WINDOW_MIN = 90
DEADLINE = datetime(2026, 7, 13, 7, 30, tzinfo=timezone.utc)


def _utc(hour: int, minute: int = 0, *, day: int = 13) -> datetime:
    return datetime(2026, 7, day, hour, minute, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 1. compute_expected_arrival_deadline — the V1 cadence parser
# ---------------------------------------------------------------------------


class TestComputeExpectedArrivalDeadline:
    def test_happy_path_tick_plus_window_utc_aware(self):
        deadline = compute_expected_arrival_deadline(CADENCE, WINDOW_MIN, RUN_DATE)
        assert deadline == DEADLINE
        assert deadline.tzinfo == timezone.utc

    def test_window_rolls_past_midnight_into_next_day(self):
        deadline = compute_expected_arrival_deadline("30 23 * * *", 90, RUN_DATE)
        assert deadline == datetime(2026, 7, 14, 1, 0, tzinfo=timezone.utc)

    def test_zero_padded_fields_parse(self):
        # "00 06 * * *" is the same instant as "0 6 * * *" — int() accepts both.
        assert compute_expected_arrival_deadline(
            "00 06 * * *", WINDOW_MIN, RUN_DATE
        ) == DEADLINE

    @pytest.mark.parametrize(
        "cadence",
        [
            "0 6 * *",          # 4 fields
            "0 6 * * * *",      # 6 fields
            "0 6",              # 2 fields
            "",                 # empty
        ],
    )
    def test_wrong_field_count_raises_naming_cadence(self, cadence):
        with pytest.raises(ValueError, match=re.escape(repr(cadence))):
            compute_expected_arrival_deadline(cadence, WINDOW_MIN, RUN_DATE)

    @pytest.mark.parametrize(
        "cadence",
        [
            "0 6 1 * *",    # day-of-month pinned
            "0 6 * 2 *",    # month pinned
            "0 6 * * 1",    # weekday pinned
        ],
    )
    def test_non_daily_shape_refused_not_misparsed(self, cadence):
        """V1 limitation is fail-loudly: anything but 'M H * * *' raises,
        never silently mis-parses (module docstring, ADR-037 🎯 Pilote)."""
        with pytest.raises(ValueError, match="supported V1 shape"):
            compute_expected_arrival_deadline(cadence, WINDOW_MIN, RUN_DATE)

    @pytest.mark.parametrize(
        "cadence",
        [
            "*/15 6 * * *",   # step syntax in minute
            "0 6-8 * * *",    # range syntax in hour
            "* 6 * * *",      # wildcard minute
            "0 * * * *",      # wildcard hour (hourly — not a daily shape)
            "a 6 * * *",      # garbage
        ],
    )
    def test_non_integer_minute_or_hour_raises(self, cadence):
        with pytest.raises(ValueError, match="non-integer minute/hour"):
            compute_expected_arrival_deadline(cadence, WINDOW_MIN, RUN_DATE)

    @pytest.mark.parametrize(
        ("cadence", "fragment"),
        [
            ("60 6 * * *", "minute"),
            ("-1 6 * * *", "minute"),
            ("0 24 * * *", "hour"),
            ("0 -1 * * *", "hour"),
        ],
    )
    def test_out_of_range_minute_or_hour_raises(self, cadence, fragment):
        with pytest.raises(ValueError, match=fragment):
            compute_expected_arrival_deadline(cadence, WINDOW_MIN, RUN_DATE)


# ---------------------------------------------------------------------------
# 2a. Arrival-window guard — PASS / FAIL / boundary / not-yet-evaluable
# ---------------------------------------------------------------------------


def _arrival(**over) -> GuardResult:
    kwargs = dict(
        cadence=CADENCE,
        arrival_window_minutes=WINDOW_MIN,
        run_date=RUN_DATE,
        file_arrived_at=_utc(6, 30),
        now=_utc(8, 0),
    )
    kwargs.update(over)
    return evaluate_arrival_window_guard(**kwargs)


class TestArrivalWindowGuard:
    def test_pass_arrived_before_deadline(self):
        result = _arrival(file_arrived_at=_utc(6, 30))
        assert result.status == GuardStatus.OK
        assert result.guard_name == "arrival_window"

    def test_boundary_arrived_exactly_at_deadline_is_ok(self):
        # <= at the deadline: the window is inclusive of its last instant.
        result = _arrival(file_arrived_at=DEADLINE)
        assert result.status == GuardStatus.OK

    def test_fail_arrived_one_microsecond_after_deadline(self):
        result = _arrival(file_arrived_at=DEADLINE + timedelta(microseconds=1))
        assert result.status == GuardStatus.FAILED

    def test_fail_no_file_after_deadline(self):
        result = _arrival(file_arrived_at=None, now=_utc(9, 0))
        assert result.status == GuardStatus.FAILED

    def test_boundary_no_file_now_exactly_at_deadline_is_failed(self):
        # `now < deadline` is strict: at the deadline instant the window has
        # elapsed and a still-missing file is a real miss.
        result = _arrival(file_arrived_at=None, now=DEADLINE)
        assert result.status == GuardStatus.FAILED

    def test_not_evaluated_no_file_before_deadline(self):
        """This guard's honest SKIP: cadence + window are NOT NULL on every
        contract (no unconfigured case exists), so its only NOT_EVALUATED
        path is 'checked mid-window' — an early check is not evidence the
        feed is missing."""
        result = _arrival(
            file_arrived_at=None, now=DEADLINE - timedelta(microseconds=1)
        )
        assert result.status == GuardStatus.NOT_EVALUATED
        assert "not yet elapsed" in result.detail

    def test_detail_carries_arrival_and_deadline_on_fail(self):
        late = DEADLINE + timedelta(hours=1)
        result = _arrival(file_arrived_at=late)
        assert late.isoformat() in result.detail
        assert DEADLINE.isoformat() in result.detail

    def test_detail_carries_deadline_and_now_when_missing(self):
        now = _utc(9, 0)
        result = _arrival(file_arrived_at=None, now=now)
        assert DEADLINE.isoformat() in result.detail
        assert now.isoformat() in result.detail

    def test_naive_file_arrived_at_raises_typeerror(self):
        """The timezone contract (module docstring): aware-vs-naive raises at
        the comparison, never a silent miscompute."""
        with pytest.raises(TypeError):
            _arrival(file_arrived_at=datetime(2026, 7, 13, 6, 30))

    def test_naive_now_raises_typeerror_when_no_file(self):
        with pytest.raises(TypeError):
            _arrival(file_arrived_at=None, now=datetime(2026, 7, 13, 8, 0))


# ---------------------------------------------------------------------------
# 2b. Volume-floor guard — PASS / FAIL / boundary / SKIP
# ---------------------------------------------------------------------------


class TestVolumeFloorGuard:
    def test_pass_above_floor(self):
        result = evaluate_volume_floor_guard(100, 150)
        assert result.status == GuardStatus.OK
        assert result.guard_name == "volume_floor"

    def test_boundary_exactly_at_floor_is_ok(self):
        assert evaluate_volume_floor_guard(100, 100).status == GuardStatus.OK

    def test_fail_one_below_floor(self):
        assert evaluate_volume_floor_guard(100, 99).status == GuardStatus.FAILED

    def test_skip_when_contract_does_not_configure_floor(self):
        result = evaluate_volume_floor_guard(None, 150)
        assert result.status == GuardStatus.NOT_EVALUATED
        assert "volume_guard_min_rows" in result.detail

    def test_skip_when_no_row_count_observed(self):
        # File missing: no observation is not a floor breach (the
        # arrival_window guard owns that failure).
        result = evaluate_volume_floor_guard(100, None)
        assert result.status == GuardStatus.NOT_EVALUATED
        assert "arrival_window" in result.detail

    def test_configured_zero_floor_is_evaluated_not_skipped(self):
        """0 is a real configured threshold, not an absent one (None-honest:
        the None/0 distinction matters)."""
        assert evaluate_volume_floor_guard(0, 0).status == GuardStatus.OK

    def test_zero_rows_below_positive_floor_fails(self):
        # An empty file is an honest observation — and a floor breach.
        assert evaluate_volume_floor_guard(1, 0).status == GuardStatus.FAILED

    def test_detail_carries_observed_and_threshold(self):
        result = evaluate_volume_floor_guard(100, 99)
        assert "99" in result.detail
        assert "100" in result.detail


# ---------------------------------------------------------------------------
# 2c. Volume-delta guard — PASS / FAIL / boundary / SKIP
# ---------------------------------------------------------------------------

PCT_20 = Decimal("0.20")


class TestVolumeDeltaGuard:
    def test_pass_within_tolerance(self):
        result = evaluate_volume_delta_guard(PCT_20, 110, 100)
        assert result.status == GuardStatus.OK
        assert result.guard_name == "volume_delta"

    def test_boundary_swing_exactly_at_tolerance_is_ok(self):
        # 100 -> 120 is exactly 20%: `>` is strict, the boundary passes.
        # Decimal arithmetic keeps this exact (0.2 == 0.20 as Decimals).
        assert evaluate_volume_delta_guard(PCT_20, 120, 100).status == GuardStatus.OK

    def test_fail_just_above_tolerance(self):
        assert (
            evaluate_volume_delta_guard(PCT_20, 121, 100).status == GuardStatus.FAILED
        )

    def test_boundary_downward_swing_exactly_at_tolerance_is_ok(self):
        # The swing is absolute: -20% sits on the same boundary as +20%.
        assert evaluate_volume_delta_guard(PCT_20, 80, 100).status == GuardStatus.OK

    def test_fail_downward_swing_above_tolerance(self):
        assert (
            evaluate_volume_delta_guard(PCT_20, 79, 100).status == GuardStatus.FAILED
        )

    def test_skip_when_contract_does_not_configure_delta(self):
        result = evaluate_volume_delta_guard(None, 110, 100)
        assert result.status == GuardStatus.NOT_EVALUATED
        assert "volume_guard_max_pct_delta" in result.detail

    def test_skip_when_no_row_count_observed(self):
        result = evaluate_volume_delta_guard(PCT_20, None, 100)
        assert result.status == GuardStatus.NOT_EVALUATED

    def test_skip_when_no_previous_run(self):
        result = evaluate_volume_delta_guard(PCT_20, 110, None)
        assert result.status == GuardStatus.NOT_EVALUATED
        assert "previous run" in result.detail

    def test_skip_when_previous_run_was_empty(self):
        # A zero baseline makes any ratio undefined — NOT_EVALUATED, never a
        # ZeroDivisionError, never a fabricated verdict.
        result = evaluate_volume_delta_guard(PCT_20, 110, 0)
        assert result.status == GuardStatus.NOT_EVALUATED

    def test_detail_carries_both_counts_and_threshold(self):
        result = evaluate_volume_delta_guard(PCT_20, 121, 100)
        assert "121" in result.detail
        assert "100" in result.detail
        assert "20.00%" in result.detail  # the tolerance, rendered as a percent
        assert "21.00%" in result.detail  # the observed swing


# ---------------------------------------------------------------------------
# 2d. Deletion-ratio guard — PASS / FAIL / boundary / SKIP
# ---------------------------------------------------------------------------


class TestDeletionRatioGuard:
    def test_threshold_relogged_from_staging_diff_in_lockstep(self):
        """ADR-042 decision 2.1: the 20% threshold is RELOGGED from
        staging/diff.py — the two constants must never drift apart."""
        from ootils_core.staging.diff import (
            DELETION_RATIO_THRESHOLD as STAGING_THRESHOLD,
        )

        assert DELETION_RATIO_THRESHOLD == STAGING_THRESHOLD == 0.20

    def test_pass_below_threshold(self):
        result = evaluate_deletion_ratio_guard(10, 100)
        assert result.status == GuardStatus.OK
        assert result.guard_name == "deletion_ratio"

    def test_pass_zero_deletions(self):
        assert evaluate_deletion_ratio_guard(0, 100).status == GuardStatus.OK

    def test_boundary_exactly_20_percent_is_ok(self):
        # 20/100 == the 0.20 threshold exactly: `>` is strict.
        assert evaluate_deletion_ratio_guard(20, 100).status == GuardStatus.OK

    def test_fail_just_above_threshold(self):
        assert evaluate_deletion_ratio_guard(21, 100).status == GuardStatus.FAILED

    def test_skip_when_no_previous_active_baseline(self):
        result = evaluate_deletion_ratio_guard(10, None)
        assert result.status == GuardStatus.NOT_EVALUATED

    def test_skip_when_previous_active_zero_or_negative(self):
        assert evaluate_deletion_ratio_guard(10, 0).status == GuardStatus.NOT_EVALUATED
        assert (
            evaluate_deletion_ratio_guard(10, -5).status == GuardStatus.NOT_EVALUATED
        )

    def test_skip_when_no_deleted_count_observed(self):
        result = evaluate_deletion_ratio_guard(None, 100)
        assert result.status == GuardStatus.NOT_EVALUATED
        assert "arrival_window" in result.detail

    def test_detail_carries_counts_ratio_and_threshold(self):
        result = evaluate_deletion_ratio_guard(30, 100)
        assert result.status == GuardStatus.FAILED
        assert "30" in result.detail
        assert "100" in result.detail
        assert "30.00%" in result.detail  # the observed ratio
        assert "20%" in result.detail     # the threshold


# ---------------------------------------------------------------------------
# 3. FeedGuardEvaluation — composition + overall verdict
# ---------------------------------------------------------------------------


def _full_kwargs(**over) -> dict:
    """A fully green evaluation: file on time, floor cleared, delta within
    tolerance, deletions below threshold. Override any field."""
    base = dict(
        feed_key="unit-feed",
        criticality="blocking",
        cadence=CADENCE,
        arrival_window_minutes=WINDOW_MIN,
        volume_guard_min_rows=100,
        volume_guard_max_pct_delta=PCT_20,
        run_date=RUN_DATE,
        file_arrived_at=_utc(6, 30),
        row_count=150,
        previous_row_count=140,
        deleted_count=5,
        previous_active_count=140,
        now=_utc(8, 0),
    )
    base.update(over)
    return base


class TestFeedGuardEvaluation:
    def test_all_green_overall_ok(self):
        evaluation = evaluate_feed_guards(**_full_kwargs())
        assert evaluation.overall_status == GuardStatus.OK
        assert all(r.status == GuardStatus.OK for r in evaluation.results)

    def test_four_guards_present_by_name(self):
        evaluation = evaluate_feed_guards(**_full_kwargs())
        names = [r.guard_name for r in evaluation.results]
        assert names == [
            "arrival_window", "volume_floor", "volume_delta", "deletion_ratio",
        ]
        for name in names:
            assert evaluation.by_name(name).guard_name == name

    def test_by_name_unknown_guard_raises_keyerror(self):
        evaluation = evaluate_feed_guards(**_full_kwargs())
        with pytest.raises(KeyError, match="no_such_guard"):
            evaluation.by_name("no_such_guard")

    def test_one_failed_guard_fails_overall(self):
        evaluation = evaluate_feed_guards(**_full_kwargs(row_count=40))
        assert evaluation.by_name("volume_floor").status == GuardStatus.FAILED
        assert evaluation.overall_status == GuardStatus.FAILED

    def test_not_evaluated_guards_never_block_on_their_own(self):
        """Unconfigured guards + no deletion baseline: 3 of 4 guards
        NOT_EVALUATED, arrival OK -> overall OK (None-honest: absence of a
        threshold or a baseline is not evidence of a problem)."""
        evaluation = evaluate_feed_guards(
            **_full_kwargs(
                volume_guard_min_rows=None,
                volume_guard_max_pct_delta=None,
                deleted_count=None,
                previous_active_count=None,
            )
        )
        assert evaluation.by_name("arrival_window").status == GuardStatus.OK
        assert evaluation.by_name("volume_floor").status == GuardStatus.NOT_EVALUATED
        assert evaluation.by_name("volume_delta").status == GuardStatus.NOT_EVALUATED
        assert evaluation.by_name("deletion_ratio").status == GuardStatus.NOT_EVALUATED
        assert evaluation.overall_status == GuardStatus.OK

    def test_overall_never_not_evaluated_even_if_all_guards_are(self):
        """The documented degenerate case: mid-window check, nothing arrived
        yet, no thresholds, no baselines — every guard NOT_EVALUATED, and
        overall_status is still a real OK (never NOT_EVALUATED, matching
        migration 078's narrower overall_status CHECK)."""
        evaluation = evaluate_feed_guards(
            **_full_kwargs(
                file_arrived_at=None,
                now=_utc(7, 0),  # before the 07:30 deadline
                row_count=None,
                previous_row_count=None,
                volume_guard_min_rows=None,
                volume_guard_max_pct_delta=None,
                deleted_count=None,
                previous_active_count=None,
            )
        )
        assert all(
            r.status == GuardStatus.NOT_EVALUATED for r in evaluation.results
        )
        assert evaluation.overall_status == GuardStatus.OK

    def test_feed_key_and_criticality_carried_verbatim(self):
        evaluation = evaluate_feed_guards(
            **_full_kwargs(feed_key="my-feed", criticality="advisory")
        )
        assert evaluation.feed_key == "my-feed"
        assert evaluation.criticality == "advisory"

    def test_missing_file_after_deadline_fails_arrival_only(self):
        """The 'flux totalement absent' scenario (ADR-037 §6): arrival FAILED,
        the volume/deletion guards honestly NOT_EVALUATED (no observation is
        not a second failure), overall FAILED."""
        evaluation = evaluate_feed_guards(
            **_full_kwargs(
                file_arrived_at=None, row_count=None,
                deleted_count=None, previous_active_count=None,
                now=_utc(9, 0),
            )
        )
        assert evaluation.by_name("arrival_window").status == GuardStatus.FAILED
        assert evaluation.by_name("volume_floor").status == GuardStatus.NOT_EVALUATED
        assert evaluation.by_name("volume_delta").status == GuardStatus.NOT_EVALUATED
        assert evaluation.by_name("deletion_ratio").status == GuardStatus.NOT_EVALUATED
        assert evaluation.overall_status == GuardStatus.FAILED

    def test_guard_result_is_frozen(self):
        result = evaluate_volume_floor_guard(100, 150)
        with pytest.raises(dataclasses.FrozenInstanceError):
            result.status = GuardStatus.FAILED  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 4. Determinism — same inputs, same result
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_full_evaluation_is_reproducible(self):
        """No wall-clock read, no randomness: two calls with identical inputs
        yield structurally EQUAL evaluations (frozen dataclasses compare by
        value) — the North Star's deterministic-core requirement."""
        a = evaluate_feed_guards(**_full_kwargs())
        b = evaluate_feed_guards(**_full_kwargs())
        assert a == b
        assert a.results == b.results
        assert a.overall_status == b.overall_status

    def test_failing_evaluation_is_reproducible_too(self):
        kwargs = _full_kwargs(row_count=40, file_arrived_at=None, now=_utc(9, 0))
        assert evaluate_feed_guards(**kwargs) == evaluate_feed_guards(**kwargs)

    def test_single_guard_results_are_reproducible(self):
        assert evaluate_volume_floor_guard(100, 99) == evaluate_volume_floor_guard(
            100, 99
        )
        assert evaluate_deletion_ratio_guard(21, 100) == evaluate_deletion_ratio_guard(
            21, 100
        )
        assert evaluate_volume_delta_guard(
            PCT_20, 121, 100
        ) == evaluate_volume_delta_guard(PCT_20, 121, 100)


# ---------------------------------------------------------------------------
# 5. Lockstep with migration 078's CHECK vocabulary
# ---------------------------------------------------------------------------


def _check_values(sql: str, column: str) -> set[str]:
    match = re.search(rf"CHECK \({column}\s+IN \(([^)]*)\)\)", sql)
    assert match is not None, f"no CHECK ... IN (...) found for {column!r}"
    return set(re.findall(r"'([^']*)'", match.group(1)))


class TestMigration078Lockstep:
    """guards.py's docstring promises GuardStatus is 'kept in lockstep with
    migration 078's daily_runs per-guard CHECK vocabulary' — this is the
    tripwire (same pattern as test_feed_contracts.py's enum-lockstep tests)."""

    def test_per_guard_checks_match_guardstatus_exactly(self):
        sql = MIGRATION_078.read_text(encoding="utf-8")
        expected = {status.value for status in GuardStatus}
        for column in (
            "arrival_status", "volume_floor_status",
            "volume_delta_status", "deletion_ratio_status",
        ):
            assert _check_values(sql, column) == expected, column

    def test_overall_status_check_is_narrower_no_not_evaluated(self):
        """overall_status never carries 'not_evaluated' — the DB CHECK is
        deliberately narrower than GuardStatus, mirroring
        FeedGuardEvaluation.overall_status's contract."""
        sql = MIGRATION_078.read_text(encoding="utf-8")
        assert _check_values(sql, "overall_status") == {
            GuardStatus.OK.value, GuardStatus.FAILED.value,
        }
