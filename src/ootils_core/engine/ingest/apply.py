"""
apply.py — the governed daily-run decision engine (ADR-042 decision 3 step 7,
absorbs ADR-037's INT-1 PR3 §0 "option (a)"; migration 079).

WHAT THIS PR DOES. Combines, PER RUN_DATE, the per-feed guard verdicts PR-2
persisted (``interfaces.daily_run``/``interfaces.guards``, ``daily_runs``
table, migration 078 — arrival window, volume floor/delta, deletion ratio)
with each feed's DQ status into ONE governed decision (ADR-037 §0):

    auto-approve the run IFF every feed's guard AND DQ status is green;
    a RED guard/DQ verdict on a ``blocking`` feed blocks auto-approval of
    the WHOLE run and escalates to a human via the L3 webhook
    (``notifications.l3_webhook``); a RED verdict on an ``advisory`` feed
    only degrades confidence, it never blocks.

Split the same way ``interfaces/guards.py`` splits pure evaluation from
``interfaces/daily_run.py``'s DB-touching persistence, and the same way
``engine/maintenance/purge.py`` splits ``plan_*``/``apply_*``:
``decide_daily_run`` is PURE (no DB, no wall-clock read, ``evaluated_at`` is
always caller-supplied) — ``plan_daily_run_decision``/
``record_daily_run_decision`` are the DB-touching counterpart.

WHAT THIS PR DELIBERATELY DOES NOT DO — the module name ``apply.py`` is
shared with ADR-042's target end-state, but this PR is narrower than that
name eventually implies:

  * It does NOT extract the canonical multi-entity upsert/writer (items,
    locations, on_hand, purchase_orders, ...) out of
    ``api/routers/ingest.py``. That extraction — "L'upsert canonique est
    extrait dans un service engine/ingest/apply.py ... appelé à la fois par
    le pipeline gouverné et par l'endpoint HTTP existant" — is explicitly
    ADR-042's PR-1 ("Fencer l'ingest direct"), delivered AFTER this PR in
    the pilot's reordered "la valeur d'abord" sequence (ADR-042 §"Plan de
    PRs"). This module's ``decide_daily_run``/``record_daily_run_decision``
    are the FIRST occupants of ``engine/ingest/apply.py`` — PR-1 adds the
    canonical-write functions alongside them, it does not replace this file.
  * It does NOT actually load/write any canonical row. There is no
    file-reading/inbox-scanning capability yet in this worktree (ADR-042's
    PR-4, "scan inbox/orchestration") and no TSV-vs-canonical diffing
    service (migration 078's own header). A governed decision is therefore
    the terminus of THIS PR — what a caller does with an AUTO_APPROVED
    verdict (trigger the eventual loader) is PR-4's orchestration.
  * DQ STATUS HAS NO DB WIRING YET. ``daily_runs`` (migration 078) carries
    no reference to an ``ingest_batches``/``dq_status`` row — that link
    only becomes meaningful once real file loading exists (PR-1/PR-4).
    ``plan_daily_run_decision`` therefore accepts DQ status as a
    CALLER-SUPPLIED observation (``dq_status_by_feed``), the exact same
    "caller-supplied, not yet DB-derivable" pattern
    ``interfaces.daily_run.DailyRunObservation`` already uses for
    ``deleted_count``/``previous_active_count``. A feed absent from the
    mapping gets ``dq_status=None`` (NOT_EVALUATED, never a fabricated
    green) — this is a NAMED INTERIM SEAM, not a finished integration; the
    real wiring lands once PR-1's canonical-write service actually runs DQ
    on a batch derived from a daily-run's file.

DQ STATUS VOCABULARY: ``engine.dq.engine.run_dq``'s ``batch_dq_status`` is
``'validated'`` (clean or with warnings) | ``'rejected'`` (>=1 error) |
``'pending'``/``'running'`` (not finished) | ``'unknown'`` (the DQ run
itself raised, ``api/routers/ingest.py:_trigger_dq`` swallows and returns
this sentinel) — migration 010's CHECK also allows ``'warning'``, though
``_update_batch_status`` never currently emits it. ``_dq_status_to_guard_status``
maps this onto ``interfaces.guards.GuardStatus`` None-honestly: ONLY
``'validated'`` is OK, ONLY ``'rejected'`` is FAILED, everything else
(including ``'warning'`` and ``None``) is NOT_EVALUATED — never a fabricated
green.

ESCALATION PAYLOAD: ADR-037 §5 explicitly leaves this "à définir au moment
de PR3, pas figée par ce PR" — ``notifications.l3_webhook.py`` stays
UNCHANGED in its existing recommendation-shaped contract
(``L3PendingPayload`` requires a ``recommendation_id``, which does not exist
here — a daily-run escalation is not a recommendation). This PR adds a
SIBLING payload/function (``DailyRunEscalationPayload``/
``notify_daily_run_escalation``) to that same module, reusing its transport
(``OOTILS_WEBHOOK_L3_URL``, best-effort, no retry, no secret). Unlike
``notify_l3_pending`` it is NOT gated by an ``L<n>`` decision level — ADR-037
§0 treats ANY blocking-feed failure as human-escalation-worthy by
construction, there is no decision-ladder level attached to a daily run.

NOT DEDUPLICATED ACROSS CALLS: ``daily_runs`` is an append-only audit trail
(migration 078) — a feed can legitimately be re-evaluated intra-day, and the
governed decision for the same run_date can legitimately change between two
calls to ``record_daily_run_decision`` (e.g. ESCALATED -> AUTO_APPROVED once
a late file lands). Each call is its own honest decision attempt and emits
its own ``daily_run_completed`` event — exactly the same philosophy
``interfaces.daily_run.record_daily_run`` already uses for per-feed rows.

Never commits/rolls back — the caller owns the transaction (same convention
as ``interfaces/daily_run.py`` and ``engine/maintenance/purge.py``).
"""
from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from uuid import UUID

from psycopg.rows import dict_row

from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.events.emit import emit_stream_event
from ootils_core.interfaces.guards import GuardStatus
from ootils_core.notifications.l3_webhook import notify_daily_run_escalation

logger = logging.getLogger(__name__)


class RunDecisionStatus(str, Enum):
    """The governed run-level decision (ADR-037 §0 option (a))."""

    AUTO_APPROVED = "auto_approved"
    ESCALATED = "escalated"
    DEGRADED = "degraded"


# engine.dq.engine.run_dq's batch_dq_status vocabulary -> GuardStatus,
# None-honest (see module docstring). Only these two values are conclusive;
# every other string (or an absent observation) is NOT_EVALUATED.
_DQ_STATUS_TO_GUARD_STATUS: dict[str, GuardStatus] = {
    "validated": GuardStatus.OK,
    "rejected": GuardStatus.FAILED,
}

_VALID_CRITICALITY = frozenset({"blocking", "advisory"})


def _dq_status_to_guard_status(dq_status: str | None) -> GuardStatus:
    """Pure vocabulary translation, see module docstring "DQ STATUS
    VOCABULARY". Never raises — an unrecognized string is NOT_EVALUATED,
    not an error (a future DQ status value must not crash the decision
    engine)."""
    if dq_status is None:
        return GuardStatus.NOT_EVALUATED
    return _DQ_STATUS_TO_GUARD_STATUS.get(dq_status, GuardStatus.NOT_EVALUATED)


@dataclass(frozen=True)
class FeedDecisionInput:
    """One feed's inputs to the governed run-level decision: its persisted
    guard verdict (``interfaces.daily_run``, PR-2, migration 078's
    ``overall_status`` — only ever OK or FAILED, never NOT_EVALUATED at that
    column) plus its DQ status (caller-supplied, see module docstring)."""

    feed_key: str
    criticality: str  # 'blocking' | 'advisory', validated in decide_daily_run
    guard_status: GuardStatus
    dq_status: str | None


@dataclass(frozen=True)
class FeedDecisionResult:
    """One feed's combined verdict inside a ``DailyRunDecision``: the guard
    verdict and the DQ verdict collapsed into ONE ``GuardStatus`` (FAILED if
    either is FAILED, else NOT_EVALUATED if either is NOT_EVALUATED, else
    OK), plus a human-readable, secret-free ``reason`` (North Star
    "Explicable" — never a silent block)."""

    feed_key: str
    criticality: str
    guard_status: GuardStatus
    dq_status: str | None
    combined_status: GuardStatus
    reason: str


@dataclass(frozen=True)
class DailyRunDecision:
    """The governed decision for one ``run_date`` (ADR-037 §0 option (a))."""

    run_date: date
    status: RunDecisionStatus
    feeds: tuple[FeedDecisionResult, ...]
    evaluated_at: datetime

    @property
    def reasons(self) -> tuple[str, ...]:
        """The reasons of every feed that was NOT a demonstrated green —
        empty when the run is AUTO_APPROVED."""
        return tuple(f.reason for f in self.feeds if f.combined_status != GuardStatus.OK)


def decide_daily_run(
    feed_inputs: Sequence[FeedDecisionInput],
    run_date: date,
    *,
    evaluated_at: datetime | None = None,
) -> DailyRunDecision:
    """The pure governed decision (ADR-037 §0 option (a)).

    AUTO_APPROVED iff every feed's guard AND DQ status is a demonstrated
    green. ESCALATED iff any ``blocking`` feed's combined verdict is FAILED
    (a blocking feed's failure always wins, regardless of what any other
    feed shows). Otherwise DEGRADED when any feed (blocking or advisory) is
    not a demonstrated green — an ``advisory`` FAILED, or ANY feed's
    NOT_EVALUATED (guard or DQ not conclusively green yet) — degrades
    confidence without blocking the run. This treats "not yet confirmed
    green" the same as "advisory red": neither is silently promoted to
    AUTO_APPROVED (None-honest), and neither is escalated to a human unless
    it is an ACTUAL blocking-feed failure.

    Pure: no DB, no wall-clock read — ``evaluated_at`` defaults to a
    caller-supplied value; this function itself never calls
    ``datetime.now()``.

    Raises ``ValueError`` on an empty ``feed_inputs`` (nothing to decide —
    never a vacuous AUTO_APPROVED over zero feeds) or an unrecognized
    ``criticality``.
    """
    if not feed_inputs:
        raise ValueError("decide_daily_run: feed_inputs is empty — nothing to decide")

    stamp = evaluated_at if evaluated_at is not None else datetime.now(timezone.utc)

    results: list[FeedDecisionResult] = []
    for feed in feed_inputs:
        if feed.criticality not in _VALID_CRITICALITY:
            raise ValueError(
                f"decide_daily_run: feed_key={feed.feed_key!r} has unrecognized "
                f"criticality {feed.criticality!r} — expected 'blocking' or 'advisory'"
            )
        dq_guard_status = _dq_status_to_guard_status(feed.dq_status)
        if feed.guard_status == GuardStatus.FAILED or dq_guard_status == GuardStatus.FAILED:
            combined = GuardStatus.FAILED
        elif (
            feed.guard_status == GuardStatus.NOT_EVALUATED
            or dq_guard_status == GuardStatus.NOT_EVALUATED
        ):
            combined = GuardStatus.NOT_EVALUATED
        else:
            combined = GuardStatus.OK
        reason = (
            f"feed_key={feed.feed_key} criticality={feed.criticality} "
            f"guard_status={feed.guard_status.value} "
            f"dq_status={feed.dq_status or 'not_supplied'}"
        )
        results.append(
            FeedDecisionResult(
                feed_key=feed.feed_key,
                criticality=feed.criticality,
                guard_status=feed.guard_status,
                dq_status=feed.dq_status,
                combined_status=combined,
                reason=reason,
            )
        )

    feeds = tuple(results)
    if any(
        r.combined_status == GuardStatus.FAILED and r.criticality == "blocking" for r in feeds
    ):
        status = RunDecisionStatus.ESCALATED
    elif any(r.combined_status != GuardStatus.OK for r in feeds):
        status = RunDecisionStatus.DEGRADED
    else:
        status = RunDecisionStatus.AUTO_APPROVED

    return DailyRunDecision(run_date=run_date, status=status, feeds=feeds, evaluated_at=stamp)


class DailyRunDecisionError(Exception):
    """Raised when a governed decision cannot be computed for ``run_date``:
    no ``daily_runs`` row exists yet for ANY feed on that date. Never
    bypassed — a run nothing has evaluated has nothing to decide."""


@dataclass(frozen=True)
class DailyRunDecisionPlan:
    """The DB-dependent inputs one run_date's governed decision needs,
    gathered read-only. Mirrors ``DailyRunGuardPlan``/``PurgePlan``: a pure
    preview, safe to call from a read-only endpoint, writes nothing."""

    feed_inputs: tuple[FeedDecisionInput, ...]


def plan_daily_run_decision(
    conn: DictRowConnection,
    run_date: date,
    *,
    dq_status_by_feed: Mapping[str, str] | None = None,
) -> DailyRunDecisionPlan:
    """SELECT-only: the CURRENT guard verdict for every feed evaluated at
    least once on ``run_date`` — the most recent ``daily_runs`` row per
    feed_key, by ``observed_at`` (migration 078's own "current verdict"
    rule). DQ status has no DB wiring yet (see module docstring):
    ``dq_status_by_feed`` is the caller-supplied observation, keyed by
    ``feed_key``; a feed absent from the mapping (or the mapping itself
    being ``None``) gets ``dq_status=None`` (NOT_EVALUATED, never a
    fabricated green). Writes nothing.

    Raises ``DailyRunDecisionError`` when no feed has been evaluated on
    ``run_date`` at all.
    """
    cur = conn.cursor(row_factory=dict_row)
    rows = cur.execute(
        """
        SELECT DISTINCT ON (feed_key)
            feed_key, criticality, overall_status
        FROM daily_runs
        WHERE run_date = %s
        ORDER BY feed_key, observed_at DESC
        """,
        (run_date,),
    ).fetchall()
    if not rows:
        raise DailyRunDecisionError(
            f"daily_run decision: no feed has been evaluated for run_date={run_date} "
            "— nothing to decide"
        )

    dq_map: Mapping[str, str] = dq_status_by_feed or {}
    feed_inputs = tuple(
        FeedDecisionInput(
            feed_key=row["feed_key"],
            criticality=row["criticality"],
            guard_status=GuardStatus(row["overall_status"]),
            dq_status=dq_map.get(row["feed_key"]),
        )
        for row in rows
    )
    return DailyRunDecisionPlan(feed_inputs=feed_inputs)


def record_daily_run_decision(
    conn: DictRowConnection,
    run_date: date,
    *,
    dq_status_by_feed: Mapping[str, str] | None = None,
    now: datetime | None = None,
    scenario_id: UUID = BASELINE_SCENARIO_ID,
    source: str = "engine",
    webhook_url: str | None = None,
) -> DailyRunDecision:
    """The sole emitter of the governed daily-run decision.

    Re-gathers ``plan_daily_run_decision`` on FRESH data immediately before
    deciding (never trusts a plan built earlier — same defense in depth as
    ``interfaces.daily_run.record_daily_run``/``engine.maintenance.purge``'s
    ``apply_*`` functions), runs the pure ``decide_daily_run``, emits
    exactly ONE ``daily_run_completed`` event (migration 079, RUN
    granularity — ADR-027), and — on ESCALATED — best-effort notifies the L3
    webhook once per FAILED ``blocking`` feed (``notifications.l3_webhook``,
    ADR-037 §0; the webhook call never raises and never blocks the caller's
    transaction).

    ``daily_runs`` has no ``scenario_id`` (baseline-by-nature, migration
    078's header) — the event is always scoped to ``BASELINE_SCENARIO_ID``
    unless a caller overrides it (test seams only; every real run IS
    baseline).

    NOT deduplicated across calls for the same ``run_date`` — see module
    docstring "NOT DEDUPLICATED ACROSS CALLS".

    Does NOT commit — the caller owns the transaction.
    """
    plan = plan_daily_run_decision(conn, run_date, dq_status_by_feed=dq_status_by_feed)
    decided_at = now if now is not None else datetime.now(timezone.utc)
    decision = decide_daily_run(plan.feed_inputs, run_date, evaluated_at=decided_at)

    culprits = ",".join(
        result.feed_key for result in decision.feeds if result.combined_status != GuardStatus.OK
    )

    emit_stream_event(
        conn,
        "daily_run_completed",
        scenario_id,
        field_changed=decision.status.value,
        new_date=run_date,
        new_quantity=len(decision.feeds),
        old_text=culprits or None,
        source=source,
    )

    logger.info(
        "daily_run.decision_recorded run_date=%s status=%s feeds=%d",
        run_date, decision.status.value, len(decision.feeds),
    )

    if decision.status == RunDecisionStatus.ESCALATED:
        for result in decision.feeds:
            if result.combined_status == GuardStatus.FAILED and result.criticality == "blocking":
                notify_daily_run_escalation(
                    run_date=run_date,
                    feed_key=result.feed_key,
                    criticality=result.criticality,
                    reason=result.reason,
                    message=(
                        f"Daily run {run_date.isoformat()} blocked: blocking feed "
                        f"'{result.feed_key}' failed its guard/DQ verdict ({result.reason})."
                    ),
                    url=webhook_url,
                )

    return decision
