"""
GET /v1/daily-runs — read-only surface over the governed daily-run pipeline
(ADR-042 decision 3, PR-4c; absorbs ADR-037's INT-1 PR2/PR3 persisted state).

Reads back, for one ``run_date`` (default today, UTC), the two things the
daily-run pipeline persisted about it:

  * every ``daily_runs`` row (migration 078) — the append-only guard-
    evaluation audit trail, one row per (feed_key, evaluation attempt). A
    feed re-evaluated intra-day appears more than once, newest first per
    migration 078's own documented "current verdict" rule
    (``interfaces/daily_run.py``).
  * the governed decision, IF one was taken — read back from the
    ``daily_run_completed`` event (migration 079) rather than recomputed:
    ``engine/ingest/apply.py``'s ``record_daily_run_decision`` is the SOLE
    emitter and its own module docstring is explicit that "the
    'daily_run_completed' event row IS the durable audit record of the
    decision actually taken" (migration 079 header) — this router trusts
    that record instead of re-deriving a second, potentially-diverging
    decision from the raw ``daily_runs`` rows. ``None`` when no decision has
    ever been recorded for this ``run_date`` (nothing applied yet, or a
    dry-run-only day).

BASELINE-ONLY, NO scenario_id (North Star carve-out, documented): a governed
daily run evaluates ERP feed interfaces, not scenario-scoped working state —
same rationale as ``daily_runs`` itself (migration 078 header) and
``inventory_snapshots``/``recommendation_outcomes`` before it (ADR-030). This
endpoint takes no ``scenario_id`` parameter and always reads the baseline
sentinel.

SCOPE: ``read`` — a plain query path over already-persisted facts, no engine
invocation (mirrors ``outcomes.py``'s two GETs / ``snapshots.py``'s GET).

Kill switch: ``OOTILS_DAILY_RUN_REPORT_ENABLED`` (default ON, falsy -> 503,
checked AFTER auth/scope but BEFORE the DB pool) — mirrors
``api/routers/scenarios.py``'s ``require_scenario_compare_enabled``, which
itself mirrors ``api/routers/outcomes.py``'s pattern. Deliberately a
DIFFERENT switch than ``OOTILS_DAILY_RUN_ENABLED`` (the CLI's ``--apply``
gate, default OFF, destructive-adjacent — ``scripts/run_daily_ingest.py``):
this endpoint only READS what has already happened, so it defaults available
even when the write path is deliberately kept off (e.g. staging/demo
environments that want to inspect a seeded daily-run history without ever
being able to trigger a real one).
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import get_db
from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

router = APIRouter(tags=["daily-runs"])

_TRUTHY = {"1", "true", "yes", "on"}


def _daily_run_report_enabled() -> bool:
    """Kill switch, default ON. Falsy OOTILS_DAILY_RUN_REPORT_ENABLED -> 503."""
    return os.environ.get("OOTILS_DAILY_RUN_REPORT_ENABLED", "1").strip().lower() in _TRUTHY


def require_daily_run_report_enabled() -> None:
    """FastAPI dependency — checked AFTER auth/scope but BEFORE ``Depends(get_db)``
    (FastAPI resolves synchronous dependencies in signature order and
    short-circuits on the first HTTPException). Auth-first so an unauthenticated
    caller always gets 401 and cannot probe the switch state; kill-switch-before-DB
    so a disabled surface answers 503 without touching the DB pool. Mirrors
    ``api/routers/scenarios.py``'s ``require_scenario_compare_enabled`` /
    ``api/routers/outcomes.py``'s ``require_outcomes_enabled``."""
    if not _daily_run_report_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Daily-run reporting is disabled (OOTILS_DAILY_RUN_REPORT_ENABLED).",
        )


class DailyRunFeedOut(BaseModel):
    """One ``daily_runs`` evaluation attempt row, verbatim."""

    daily_run_id: UUID
    feed_key: str
    run_date: _dt.date
    observed_at: _dt.datetime
    file_arrived_at: Optional[_dt.datetime] = None
    row_count: Optional[int] = None
    previous_row_count: Optional[int] = None
    deleted_count: Optional[int] = None
    criticality: str
    arrival_status: str
    volume_floor_status: str
    volume_delta_status: str
    deletion_ratio_status: str
    overall_status: str
    created_at: _dt.datetime


class DailyRunDecisionOut(BaseModel):
    """The governed decision recorded for this ``run_date``, read back from
    its ``daily_run_completed`` event (migration 079) — not recomputed.
    ``culprit_feed_keys`` is the comma-joined ``old_text`` split into a list,
    empty when every feed was green (never a single-empty-string entry)."""

    event_id: UUID
    status: str
    decided_at: _dt.datetime
    feeds_evaluated: int
    culprit_feed_keys: list[str]


class DailyRunsListOut(BaseModel):
    run_date: _dt.date
    feeds: list[DailyRunFeedOut]
    total_feeds: int
    decision: Optional[DailyRunDecisionOut] = None


def _row_to_feed_out(row: dict) -> DailyRunFeedOut:
    return DailyRunFeedOut(
        daily_run_id=UUID(str(row["daily_run_id"])),
        feed_key=row["feed_key"],
        run_date=row["run_date"],
        observed_at=row["observed_at"],
        file_arrived_at=row["file_arrived_at"],
        row_count=row["row_count"],
        previous_row_count=row["previous_row_count"],
        deleted_count=row["deleted_count"],
        criticality=row["criticality"],
        arrival_status=row["arrival_status"],
        volume_floor_status=row["volume_floor_status"],
        volume_delta_status=row["volume_delta_status"],
        deletion_ratio_status=row["deletion_ratio_status"],
        overall_status=row["overall_status"],
        created_at=row["created_at"],
    )


def _fetch_feeds(db: DictRowConnection, run_date: _dt.date) -> list[DailyRunFeedOut]:
    rows = db.execute(
        """
        SELECT daily_run_id, feed_key, run_date, observed_at, file_arrived_at,
               row_count, previous_row_count, deleted_count, criticality,
               arrival_status, volume_floor_status, volume_delta_status,
               deletion_ratio_status, overall_status, created_at
        FROM daily_runs
        WHERE run_date = %s
        ORDER BY feed_key, observed_at DESC
        """,
        (run_date,),
    ).fetchall()
    return [_row_to_feed_out(r) for r in rows]


def _fetch_decision(db: DictRowConnection, run_date: _dt.date) -> Optional[DailyRunDecisionOut]:
    """The most recent ``daily_run_completed`` event for this run_date
    (migration 079's typed-column contract, ``engine/events/emit.py``):
    ``field_changed`` = decision status, ``new_date`` = run_date,
    ``new_quantity`` = feeds evaluated, ``old_text`` = comma-joined culprit
    feed_keys (NULL when every feed was green). A run_date can in principle
    carry more than one decision (NOT deduplicated across calls — see
    ``engine/ingest/apply.py``'s module docstring); this reads the latest."""
    row = db.execute(
        """
        SELECT event_id, field_changed, new_quantity, old_text, created_at
        FROM events
        WHERE event_type = 'daily_run_completed'
          AND scenario_id = %s
          AND new_date = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (BASELINE_SCENARIO_ID, run_date),
    ).fetchone()
    if row is None:
        return None
    culprits_raw = row["old_text"]
    culprits = culprits_raw.split(",") if culprits_raw else []
    return DailyRunDecisionOut(
        event_id=UUID(str(row["event_id"])),
        status=row["field_changed"],
        decided_at=row["created_at"],
        feeds_evaluated=int(row["new_quantity"]) if row["new_quantity"] is not None else 0,
        culprit_feed_keys=culprits,
    )


@router.get(
    "/v1/daily-runs",
    response_model=DailyRunsListOut,
    summary="List daily-run guard evaluations + the governed decision for a day",
    description=(
        "Read-only: every `daily_runs` guard-evaluation row for `date` "
        "(default today, UTC) plus the governed decision recorded for that "
        "day, if any (read back from its `daily_run_completed` event, never "
        "recomputed). Baseline-only by nature — no `scenario_id` parameter."
    ),
)
def list_daily_runs(
    _principal: Principal = Depends(require_scope("read")),
    _enabled: None = Depends(require_daily_run_report_enabled),
    date: Optional[_dt.date] = Query(
        default=None,
        description="run_date to inspect (YYYY-MM-DD). Defaults to today (UTC).",
    ),
    db: DictRowConnection = Depends(get_db),
) -> DailyRunsListOut:
    """Fully parameterized, two SELECTs (feeds + decision), no engine call."""
    run_date = date if date is not None else _dt.datetime.now(_dt.timezone.utc).date()

    feeds = _fetch_feeds(db, run_date)
    decision = _fetch_decision(db, run_date)

    logger.info(
        "daily_runs.list run_date=%s feeds=%d decision=%s",
        run_date, len(feeds), decision.status if decision else None,
    )

    return DailyRunsListOut(
        run_date=run_date,
        feeds=feeds,
        total_feeds=len(feeds),
        decision=decision,
    )
