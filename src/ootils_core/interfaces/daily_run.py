"""
daily_run.py — persistence for the daily-run guard evaluations (``daily_runs``
table, migration 078; ADR-042 decision 3, absorbs ADR-037's INT-1 PR2).

Same DB-boundary split as ``engine/maintenance/purge.py``: a read-only
``plan_*`` helper gathers the DB-dependent inputs one feed's guard
evaluation needs (its active contract, via
``interfaces.contracts.get_active_contract``, and the previous evaluation's
row count for the day/day comparison), and the sole writer —
``record_daily_run`` — re-runs that same plan on FRESH data immediately
before writing (never trusts a stale caller-supplied plan, same defense in
depth as ``purge.py``'s ``_apply_one``), calls the pure guard evaluator
(``interfaces.guards.evaluate_feed_guards``), and INSERTs exactly one
``daily_runs`` row.

SCOPE OF THIS PR (ADR-042's PR-2 / ADR-037's INT-1 PR2): guard evaluation +
persistence only. Deliberately OUT of scope here — see migration 078's
header for the full rationale:

  * the auto-approve/escalate DECISION (ADR-037 §0 option (a)) that combines
    this module's per-feed guard verdicts with a batch's DQ status — PR-3's
    ``engine/ingest/apply.py`` territory.
  * emitting a ``daily_run_completed`` stream event — that event's
    granularity is the RUN AS A WHOLE (every feed evaluated + the governed
    decision taken, ADR-027 convention), which only PR-3's decision engine
    can know it has reached. Emitting one per feed, per evaluation attempt,
    here, would misrepresent "one event per run".
  * deriving ``deleted_count``/``previous_active_count`` from a real file
    diff — the TSV-vs-canonical diffing service (``engine/ingest/apply``)
    does not exist yet in this worktree; ``record_daily_run`` accepts them
    as caller-supplied observation inputs, ``None`` when not yet knowable
    (the deletion_ratio guard treats that as NOT_EVALUATED, never a
    fabricated pass).

Never commits/rolls back — the caller owns the transaction (same
convention as ``interfaces/contracts.py`` and ``engine/maintenance/purge.py``).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import UUID

from psycopg.rows import dict_row

from ootils_core.db.types import DictRowConnection
from ootils_core.interfaces.contracts import FeedContract, get_active_contract
from ootils_core.interfaces.guards import FeedGuardEvaluation, evaluate_feed_guards

logger = logging.getLogger(__name__)


class DailyRunGuardError(Exception):
    """Raised when a daily-run guard evaluation cannot proceed: ``feed_key``
    has no active ``feed_contracts`` row (unregistered or retired). Never
    bypassed — a feed with no contract has nothing to evaluate against."""


@dataclass(frozen=True)
class DailyRunGuardPlan:
    """The DB-dependent inputs one feed's guard evaluation needs, gathered
    read-only. Mirrors ``PurgePlan`` (``engine/maintenance/purge.py``): a
    pure preview, safe to call from a read-only endpoint, writes nothing."""

    contract: FeedContract
    previous_row_count: int | None


@dataclass(frozen=True)
class DailyRunObservation:
    """Caller-supplied raw observation for one feed on one ``run_date`` —
    the DB-free inputs ``record_daily_run`` cannot derive itself in this
    PR's scope (see module docstring)."""

    file_arrived_at: datetime | None
    row_count: int | None
    deleted_count: int | None = None
    previous_active_count: int | None = None


@dataclass(frozen=True)
class DailyRunRecord:
    """The persisted outcome of one ``record_daily_run`` call."""

    daily_run_id: UUID
    feed_key: str
    run_date: date
    evaluation: FeedGuardEvaluation


def plan_daily_run_guard_check(
    conn: DictRowConnection, feed_key: str, run_date: date
) -> DailyRunGuardPlan:
    """SELECT-only: the active contract for ``feed_key`` plus the previous
    evaluation's row count — the most recent ``daily_runs`` row for this
    feed_key strictly BEFORE ``run_date`` by calendar day (not merely the
    last-inserted row, so a same-day re-evaluation never becomes its own
    baseline). Writes nothing.

    Raises ``DailyRunGuardError`` if ``feed_key`` has no active contract.
    """
    contract = get_active_contract(conn, feed_key)
    if contract is None:
        raise DailyRunGuardError(
            f"daily_run guard check: feed_key {feed_key!r} has no active "
            "feed_contracts row — nothing to evaluate against"
        )

    cur = conn.cursor(row_factory=dict_row)
    prev = cur.execute(
        """
        SELECT row_count FROM daily_runs
        WHERE feed_key = %s AND run_date < %s
        ORDER BY run_date DESC, observed_at DESC
        LIMIT 1
        """,
        (feed_key, run_date),
    ).fetchone()
    previous_row_count = (
        int(prev["row_count"])
        if prev is not None and prev["row_count"] is not None
        else None
    )

    return DailyRunGuardPlan(contract=contract, previous_row_count=previous_row_count)


def record_daily_run(
    conn: DictRowConnection,
    feed_key: str,
    run_date: date,
    observation: DailyRunObservation,
    now: datetime | None = None,
) -> DailyRunRecord:
    """The sole writer of the ``daily_runs`` guard-evaluation audit trail.

    Re-gathers ``plan_daily_run_guard_check`` on FRESH data immediately
    before writing (never trusts a plan built earlier — the active contract
    or the previous run's row count may have changed since), runs the pure
    guard evaluator, INSERTs exactly one ``daily_runs`` row, and returns the
    typed result.

    Multiple evaluation attempts for the same ``(feed_key, run_date)`` are
    NOT deduplicated at the DB level (no unique constraint) — this is an
    append-only audit trail (same philosophy as ``calc_runs``/
    ``maintenance_purge_runs``): a feed reported missing at the arrival
    deadline and then re-evaluated once the file lands are two honest,
    distinct rows, not an overwrite.

    Does NOT commit — the caller owns the transaction.
    """
    plan = plan_daily_run_guard_check(conn, feed_key, run_date)
    evaluated_at = now if now is not None else datetime.now(timezone.utc)

    evaluation = evaluate_feed_guards(
        feed_key=feed_key,
        criticality=plan.contract.criticality,
        cadence=plan.contract.cadence,
        arrival_window_minutes=plan.contract.arrival_window_minutes,
        volume_guard_min_rows=plan.contract.volume_guard_min_rows,
        volume_guard_max_pct_delta=plan.contract.volume_guard_max_pct_delta,
        run_date=run_date,
        file_arrived_at=observation.file_arrived_at,
        row_count=observation.row_count,
        previous_row_count=plan.previous_row_count,
        deleted_count=observation.deleted_count,
        previous_active_count=observation.previous_active_count,
        now=evaluated_at,
    )

    cur = conn.cursor(row_factory=dict_row)
    inserted = cur.execute(
        """
        INSERT INTO daily_runs (
            feed_contract_id, feed_key, run_date, observed_at,
            file_arrived_at, row_count, previous_row_count, deleted_count,
            criticality, arrival_status, volume_floor_status,
            volume_delta_status, deletion_ratio_status, overall_status
        ) VALUES (
            %(feed_contract_id)s, %(feed_key)s, %(run_date)s, %(observed_at)s,
            %(file_arrived_at)s, %(row_count)s, %(previous_row_count)s,
            %(deleted_count)s, %(criticality)s, %(arrival_status)s,
            %(volume_floor_status)s, %(volume_delta_status)s,
            %(deletion_ratio_status)s, %(overall_status)s
        ) RETURNING daily_run_id
        """,
        {
            "feed_contract_id": plan.contract.feed_contract_id,
            "feed_key": feed_key,
            "run_date": run_date,
            "observed_at": evaluated_at,
            "file_arrived_at": observation.file_arrived_at,
            "row_count": observation.row_count,
            "previous_row_count": plan.previous_row_count,
            "deleted_count": observation.deleted_count,
            "criticality": plan.contract.criticality,
            "arrival_status": evaluation.by_name("arrival_window").status.value,
            "volume_floor_status": evaluation.by_name("volume_floor").status.value,
            "volume_delta_status": evaluation.by_name("volume_delta").status.value,
            "deletion_ratio_status": evaluation.by_name("deletion_ratio").status.value,
            "overall_status": evaluation.overall_status.value,
        },
    ).fetchone()
    if inserted is None:  # INSERT..RETURNING yields exactly one row — fail loudly
        raise RuntimeError("daily_runs INSERT ... RETURNING yielded no row")
    raw_id = inserted["daily_run_id"]
    daily_run_id = raw_id if isinstance(raw_id, UUID) else UUID(str(raw_id))

    logger.info(
        "daily_run.guard_recorded feed_key=%s run_date=%s daily_run_id=%s "
        "overall_status=%s",
        feed_key, run_date, daily_run_id, evaluation.overall_status.value,
    )

    return DailyRunRecord(
        daily_run_id=daily_run_id,
        feed_key=feed_key,
        run_date=run_date,
        evaluation=evaluation,
    )
