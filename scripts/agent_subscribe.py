"""
agent_subscribe.py — the shared `--subscribe` (cron) drain for the watcher fleet
(chantier AN-1, #401).

North Star "Streamable": agents subscribe to deltas, they do not poll. In
``--subscribe`` mode a watcher does NOT re-scan the whole plan on every cron
tick; it first drains the ``events`` stream from its last cursor and only runs
its expensive pass when the drain reports >=1 RELEVANT event (a calc run
finished / shortages were detected) since it last looked. Nothing changed since
last time ⇒ no work, no recomputation.

DRAIN PATH = DIRECT DB READ, not the HTTP SSE endpoint. The watcher fleet already
talks to Postgres directly (psycopg.connect(DATABASE_URL)); it has NO API base
URL and NO bearer token in its config — a cron watcher is a DB client, not an API
client. Migration 063 documents the keyset SELECT on ``events.stream_seq`` as
"the replayable truth"; ``GET /v1/stream`` is merely a transport over that same
query. So a watcher reading ``events`` directly with the SAME keyset contract
(WHERE scenario_id = %s AND stream_seq > %s ORDER BY stream_seq) IS the stream —
no self-referential HTTP hop back into the API the DB already backs, no new
token/URL config surface, everything in the connection the watcher already holds.

CURSOR PERSISTENCE = agent_runs.metrics JSONB (migration 039). Each subscribed
run stores its final ``stream_cursor`` (the highest stream_seq it drained) in its
own agent_runs metrics; the next run reads the LAST COMPLETED run's cursor to
resume. First-ever subscribed run (no prior cursor) seeds from the current
MAX(stream_seq) — "start from now", never a full replay of history (migration 063
resume semantics).

Opt-in: only the two first watchers (agent_shortage_watcher,
agent_material_watcher) wire ``--subscribe``. Without the flag their behaviour is
byte-identical to before (full scan every run).
"""
from __future__ import annotations

import logging
from typing import Any, Optional, Sequence

import psycopg

logger = logging.getLogger(__name__)

# Metrics key under which a subscribed run persists its drained high-water mark.
STREAM_CURSOR_KEY = "stream_cursor"

# The fleet-emission event types a demand/material watcher acts on: a completed
# calc run (the plan was recomputed) or a shortage-detection pass (new shortages
# persisted). Both are migration-071 types. A recommendation_created /
# snapshot_captured / outcome_evaluated event is NOT a trigger to re-plan, so it
# is drained (advancing the cursor) but does NOT count as "relevant".
RELEVANT_EVENT_TYPES: tuple[str, ...] = ("calc_run_finished", "shortage_detected")


def fetch_stream_cursor(
    conn: psycopg.Connection[Any],
    agent_name: str,
    scenario_id: Any,
) -> Optional[int]:
    """Return the stream cursor persisted by this agent's LAST COMPLETED run.

    Reads ``agent_runs.metrics->>'stream_cursor'`` for the most recent COMPLETED
    run of the same (agent_name, scenario_id), migration 039. Returns the int
    high-water mark to resume AFTER (drain uses stream_seq > cursor), or None when
    there is no prior completed run OR it stored no cursor (a pre-subscribe run) —
    the caller then seeds "from now" (current MAX(stream_seq)) rather than
    replaying history.

    Robust to a malformed/absent stored value: a non-integer stream_cursor is
    treated as "no cursor" (None) rather than crashing the watcher — the run
    degrades to a from-now seed, never a stack trace on a cron tick.
    """
    row = conn.execute(
        """
        SELECT metrics
        FROM agent_runs
        WHERE agent_name = %s AND scenario_id = %s AND status = 'COMPLETED'
        ORDER BY finished_at DESC NULLS LAST, started_at DESC
        LIMIT 1
        """,
        (agent_name, scenario_id),
    ).fetchone()
    if row is None:
        return None
    metrics = _metrics_from_row(row)
    if not isinstance(metrics, dict):
        return None
    return _coerce_cursor(metrics.get(STREAM_CURSOR_KEY))


def current_max_seq(conn: psycopg.Connection[Any], scenario_id: Any) -> int:
    """Current MAX(stream_seq) for a scenario (the "from now" seed).

    Used when there is no prior cursor: the first subscribed run starts from the
    live high-water mark so it never replays the scenario's whole event history
    (migration 063 resume semantics). Returns 0 on an empty stream."""
    row = conn.execute(
        "SELECT COALESCE(MAX(stream_seq), 0) AS seq FROM events WHERE scenario_id = %s",
        (scenario_id,),
    ).fetchone()
    if row is None:
        return 0
    return _scalar_int(row, "seq")


def drain_stream(
    conn: psycopg.Connection[Any],
    scenario_id: Any,
    cursor: int,
    *,
    relevant_types: Sequence[str] = RELEVANT_EVENT_TYPES,
) -> tuple[int, int]:
    """Drain the events stream past ``cursor`` for a scenario (direct DB keyset).

    Returns ``(new_cursor, relevant_count)`` where new_cursor is the highest
    stream_seq seen (or the input cursor when the stream is empty past it), and
    relevant_count is how many drained events are of ``relevant_types`` (the
    watcher runs its pass iff relevant_count > 0).

    Same keyset contract as GET /v1/stream (migration 063): WHERE scenario_id = %s
    AND stream_seq > %s ORDER BY stream_seq. The cursor advances over EVERY drained
    row (relevant or not) so an irrelevant event is not re-seen next tick; only the
    relevant subset is COUNTED. stream_seq is an opaque high-water mark compared
    with `>` only (never gap-checked, never last+1) — a rolled-back INSERT may have
    burned values, which is fine.
    """
    rows = conn.execute(
        """
        SELECT stream_seq, event_type
        FROM events
        WHERE scenario_id = %s AND stream_seq > %s
        ORDER BY stream_seq
        """,
        (scenario_id, cursor),
    ).fetchall()

    new_cursor = cursor
    relevant = 0
    wanted = set(relevant_types)
    for row in rows:
        seq = _scalar_int(row, "stream_seq")
        if seq > new_cursor:
            new_cursor = seq
        if _row_event_type(row) in wanted:
            relevant += 1
    return new_cursor, relevant


# ---------------------------------------------------------------------------
# Row-factory-agnostic accessors — the fleet mixes dict_row (app pool) and
# tuple_row (a watcher's psycopg.connect() default), so every read below works
# for both.
# ---------------------------------------------------------------------------


def _metrics_from_row(row: Any) -> Any:
    if isinstance(row, dict):
        return row.get("metrics")
    return row[0]


def _scalar_int(row: Any, key: str) -> int:
    if isinstance(row, dict):
        return int(row[key] or 0)
    return int(row[0] or 0)


def _row_event_type(row: Any) -> Optional[str]:
    if isinstance(row, dict):
        return row.get("event_type")
    return row[1]


def _coerce_cursor(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning("agent_subscribe: ignoring non-integer stored cursor %r", value)
        return None
