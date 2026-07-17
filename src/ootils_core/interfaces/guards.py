"""
guards.py — pure runtime guards for the daily-run governed-ingest pipeline
(ADR-042 decision 3 step 5; absorbs ADR-037's INT-1 PR2, §6).

Every daily run evaluates, PER FEED, the runtime guards a `feed_contracts`
row (``interfaces/contracts.py``) only DECLARES: has the file arrived within
its cadence + arrival window, does its row count clear the configured
volume floor, did the row count swing more than the configured day/day
tolerance, and did more than the repo-wide deletion-ratio threshold of
previously-active rows disappear. ADR-037 §6 names these as the guards that
catch a feed's silent death modes ("extraction partielle silencieuse" and
"flux totalement absent") before a governed run trusts the feed.

Every function in this module is PURE: no DB, no wall-clock read (``now``
and every timestamp are always caller-supplied), no I/O, never raises on a
"bad" observation — a red guard is data (a ``GuardResult``), not an
exception. The DB-touching counterpart — gathering the active contract and
the previous run's stats, and persisting the evaluation — lives in
``daily_run.py``, split the same way ``engine/maintenance/purge.py`` splits
its pure guard checks (``_verify_purge_guards``) from its DB-touching
plan/apply functions.

NONE-HONEST BY CONSTRUCTION: a guard whose contract does not configure it
(e.g. no ``volume_guard_min_rows``), or that has no baseline to compare
against (no previous run yet, or the file never arrived), returns
``GuardStatus.NOT_EVALUATED`` — never a fabricated OK, never a false
FAILED. ``FeedGuardEvaluation.overall_status`` only turns FAILED on an
ACTUAL guard failure; NOT_EVALUATED guards never block a run on their own.

CADENCE PARSING — V1 LIMITATION: ``compute_expected_arrival_deadline``
supports ONLY the ``"M H * * *"`` cron shape (a fixed daily UTC time) — the
only shape the 3 seed contracts (``config/feed-contracts/*.yaml``) actually
use. Any other field (day-of-month, day-of-week, step/range syntax) raises
``ValueError`` naming the unsupported cadence, rather than silently
mis-parsing it. Widening this is a follow-up once the pilot's real feed
calendars are known (ADR-037 🎯 Pilote).

TIMEZONE CONTRACT: every ``datetime`` this module accepts or returns
(``file_arrived_at``, ``now``, ``compute_expected_arrival_deadline``'s
return value) is UTC and timezone-AWARE. Comparing an aware deadline against
a naive ``file_arrived_at``/``now`` raises ``TypeError`` at the point of
comparison (Python's own guard) rather than silently miscomputing — callers
(the persistence layer, tests) must pass ``tzinfo=timezone.utc`` datetimes.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum


class GuardStatus(str, Enum):
    """One guard's verdict. ``NOT_EVALUATED`` is None-honest: the guard was
    not configured for this feed, or had no baseline to compare against —
    never treated as evidence of a problem. Values are kept in lockstep
    with migration 078's ``daily_runs`` per-guard CHECK vocabulary."""

    OK = "ok"
    FAILED = "failed"
    NOT_EVALUATED = "not_evaluated"


@dataclass(frozen=True)
class GuardResult:
    """One guard's verdict for one feed evaluation. ``detail`` is a
    human-readable, secret-free explanation — it feeds the daily report
    (ADR-042 §5) and, on a red guard, the L3 escalation payload (ADR-037
    PR3), so it must always name the precise reason (North Star
    "Explicable" — never a silent block)."""

    guard_name: str
    status: GuardStatus
    detail: str


# Relogged from staging/diff.py's DELETION_RATIO_THRESHOLD (ADR-013 D4) — a
# repo-wide constant, deliberately NOT a per-feed-contract configurable (see
# ADR-042 decision 2.1: "les deux gardes de valeur sont sauvées ... relogées
# comme gardes du nouveau pipeline gouverné").
DELETION_RATIO_THRESHOLD = 0.20


def compute_expected_arrival_deadline(
    cadence: str, arrival_window_minutes: int, run_date: date
) -> datetime:
    """The latest UTC instant a feed may arrive on ``run_date`` before the
    arrival-window guard treats it as missing.

    V1 supports ONLY the ``"M H * * *"`` cron shape (daily at a fixed UTC
    time) — see module docstring. Raises ``ValueError`` naming the cadence
    on any other shape (unsupported field, out-of-range minute/hour,
    non-integer field).
    """
    fields = cadence.split()
    if len(fields) != 5:
        raise ValueError(
            f"compute_expected_arrival_deadline: cadence {cadence!r} must "
            "have 5 cron fields (minute hour day month weekday)"
        )
    minute_f, hour_f, day_f, month_f, weekday_f = fields
    if day_f != "*" or month_f != "*" or weekday_f != "*":
        raise ValueError(
            f"compute_expected_arrival_deadline: cadence {cadence!r} is not "
            "a supported V1 shape — only 'M H * * *' (daily at a fixed UTC "
            "time) is implemented"
        )
    try:
        minute = int(minute_f)
        hour = int(hour_f)
    except ValueError as exc:
        raise ValueError(
            f"compute_expected_arrival_deadline: cadence {cadence!r} has a "
            "non-integer minute/hour field"
        ) from exc
    if not (0 <= minute <= 59):
        raise ValueError(
            f"compute_expected_arrival_deadline: cadence {cadence!r} minute "
            "field out of range (0-59)"
        )
    if not (0 <= hour <= 23):
        raise ValueError(
            f"compute_expected_arrival_deadline: cadence {cadence!r} hour "
            "field out of range (0-23)"
        )
    tick = datetime(
        run_date.year, run_date.month, run_date.day, hour, minute, tzinfo=timezone.utc
    )
    return tick + timedelta(minutes=arrival_window_minutes)


def evaluate_arrival_window_guard(
    *,
    cadence: str,
    arrival_window_minutes: int,
    run_date: date,
    file_arrived_at: datetime | None,
    now: datetime,
) -> GuardResult:
    """Missing-feed guard (ADR-037 §6, "flux totalement absent"). Evaluated
    relative to the caller-supplied ``now`` — never reads the wall clock,
    so it is fully deterministic in tests.

    Returns NOT_EVALUATED when the deadline has not yet elapsed and the
    file has not arrived — this guard is meant to be evaluated once the
    window has passed, not mid-window; a run checking early is not evidence
    the feed is missing.
    """
    deadline = compute_expected_arrival_deadline(cadence, arrival_window_minutes, run_date)
    if file_arrived_at is not None:
        if file_arrived_at <= deadline:
            return GuardResult(
                "arrival_window",
                GuardStatus.OK,
                f"arrived at {file_arrived_at.isoformat()}, within the "
                f"{deadline.isoformat()} deadline",
            )
        return GuardResult(
            "arrival_window",
            GuardStatus.FAILED,
            f"arrived at {file_arrived_at.isoformat()}, after the "
            f"{deadline.isoformat()} deadline",
        )
    if now < deadline:
        return GuardResult(
            "arrival_window",
            GuardStatus.NOT_EVALUATED,
            f"no file yet, deadline {deadline.isoformat()} not yet elapsed "
            f"(now={now.isoformat()})",
        )
    return GuardResult(
        "arrival_window",
        GuardStatus.FAILED,
        f"no file arrived by the {deadline.isoformat()} deadline "
        f"(now={now.isoformat()})",
    )


def evaluate_volume_floor_guard(
    min_rows: int | None, observed_row_count: int | None
) -> GuardResult:
    """Silent-partial-extraction guard, floor half (ADR-037 §6, "extraction
    partielle silencieuse")."""
    if min_rows is None:
        return GuardResult(
            "volume_floor",
            GuardStatus.NOT_EVALUATED,
            "no volume_guard_min_rows configured on the active contract",
        )
    if observed_row_count is None:
        return GuardResult(
            "volume_floor",
            GuardStatus.NOT_EVALUATED,
            "no row count observed (file missing — see the arrival_window guard)",
        )
    if observed_row_count < min_rows:
        return GuardResult(
            "volume_floor",
            GuardStatus.FAILED,
            f"observed {observed_row_count} rows, below the floor of {min_rows}",
        )
    return GuardResult(
        "volume_floor",
        GuardStatus.OK,
        f"observed {observed_row_count} rows, at or above the floor of {min_rows}",
    )


def evaluate_volume_delta_guard(
    max_pct_delta: Decimal | None,
    observed_row_count: int | None,
    previous_row_count: int | None,
) -> GuardResult:
    """Silent-partial-extraction guard, day/day swing half (ADR-037 §6).

    ``previous_row_count`` of ``None`` (no prior run yet) or ``0`` (a prior
    empty day makes any ratio meaningless/undefined) both yield
    NOT_EVALUATED — neither is an honest baseline to compare against.
    """
    if max_pct_delta is None:
        return GuardResult(
            "volume_delta",
            GuardStatus.NOT_EVALUATED,
            "no volume_guard_max_pct_delta configured on the active contract",
        )
    if observed_row_count is None:
        return GuardResult(
            "volume_delta",
            GuardStatus.NOT_EVALUATED,
            "no row count observed (file missing — see the arrival_window guard)",
        )
    if not previous_row_count:
        return GuardResult(
            "volume_delta",
            GuardStatus.NOT_EVALUATED,
            "no previous run's row count to compare against",
        )
    delta_ratio = Decimal(abs(observed_row_count - previous_row_count)) / Decimal(previous_row_count)
    if delta_ratio > max_pct_delta:
        return GuardResult(
            "volume_delta",
            GuardStatus.FAILED,
            f"row count swung {float(delta_ratio):.2%} vs the previous run's "
            f"{previous_row_count} (today {observed_row_count}), exceeding "
            f"the {float(max_pct_delta):.2%} tolerance",
        )
    return GuardResult(
        "volume_delta",
        GuardStatus.OK,
        f"row count swung {float(delta_ratio):.2%} vs the previous run's "
        f"{previous_row_count} (today {observed_row_count}), within the "
        f"{float(max_pct_delta):.2%} tolerance",
    )


def evaluate_deletion_ratio_guard(
    deleted_count: int | None, previous_active_count: int | None
) -> GuardResult:
    """Relogged from ``staging/diff.py``'s ``DELETION_RATIO_THRESHOLD``
    (ADR-013 D4, ADR-042 decision 2.1): more than 20% of what was active in
    the previous run disappearing from this one is a soft-delete storm, not
    normal day/day variation."""
    if previous_active_count is None or previous_active_count <= 0:
        return GuardResult(
            "deletion_ratio",
            GuardStatus.NOT_EVALUATED,
            "no previous run's active row count to compare against",
        )
    if deleted_count is None:
        return GuardResult(
            "deletion_ratio",
            GuardStatus.NOT_EVALUATED,
            "no deleted-row count observed (file missing — see the "
            "arrival_window guard)",
        )
    ratio = deleted_count / previous_active_count
    if ratio > DELETION_RATIO_THRESHOLD:
        return GuardResult(
            "deletion_ratio",
            GuardStatus.FAILED,
            f"{deleted_count} of {previous_active_count} previously-active "
            f"rows disappeared ({ratio:.2%}), exceeding the "
            f"{DELETION_RATIO_THRESHOLD:.0%} threshold",
        )
    return GuardResult(
        "deletion_ratio",
        GuardStatus.OK,
        f"{deleted_count} of {previous_active_count} previously-active rows "
        f"disappeared ({ratio:.2%}), within the {DELETION_RATIO_THRESHOLD:.0%} "
        "threshold",
    )


@dataclass(frozen=True)
class FeedGuardEvaluation:
    """The full guard verdict for one feed's one daily-run evaluation
    attempt."""

    feed_key: str
    # 'blocking' | 'advisory' — carried through for the caller's DECISION
    # logic (ADR-037 §0 option (a), PR-3 territory); never interpreted here.
    criticality: str
    results: tuple[GuardResult, ...]

    @property
    def overall_status(self) -> GuardStatus:
        """FAILED iff ANY guard FAILED. NOT_EVALUATED guards never fail the
        evaluation on their own — an unconfigured guard or a missing
        baseline is not evidence of a problem (None-honest). Never returns
        NOT_EVALUATED itself."""
        if any(r.status == GuardStatus.FAILED for r in self.results):
            return GuardStatus.FAILED
        return GuardStatus.OK

    def by_name(self, guard_name: str) -> GuardResult:
        for result in self.results:
            if result.guard_name == guard_name:
                return result
        raise KeyError(f"no guard result named {guard_name!r} in this evaluation")


def evaluate_feed_guards(
    *,
    feed_key: str,
    criticality: str,
    cadence: str,
    arrival_window_minutes: int,
    volume_guard_min_rows: int | None,
    volume_guard_max_pct_delta: Decimal | None,
    run_date: date,
    file_arrived_at: datetime | None,
    row_count: int | None,
    previous_row_count: int | None,
    deleted_count: int | None,
    previous_active_count: int | None,
    now: datetime,
) -> FeedGuardEvaluation:
    """Run all four runtime guards for one feed's one daily-run evaluation.

    Pure — no DB, no wall-clock read. The persistence layer
    (``daily_run.py``) gathers these inputs from the active
    ``feed_contracts`` row + the previous ``daily_runs`` row and calls this
    function.
    """
    results = (
        evaluate_arrival_window_guard(
            cadence=cadence,
            arrival_window_minutes=arrival_window_minutes,
            run_date=run_date,
            file_arrived_at=file_arrived_at,
            now=now,
        ),
        evaluate_volume_floor_guard(volume_guard_min_rows, row_count),
        evaluate_volume_delta_guard(volume_guard_max_pct_delta, row_count, previous_row_count),
        evaluate_deletion_ratio_guard(deleted_count, previous_active_count),
    )
    return FeedGuardEvaluation(feed_key=feed_key, criticality=criticality, results=results)
