"""
/v1/stream — Server-Sent Events feed of the ``events`` table (chantier #391).

This is the concrete surface of the Streamable North-Star principle: agents
subscribe to change deltas here instead of polling ``GET /v1/events``. The
canonical, replayable truth is a keyset SELECT over the monotonic
``events.stream_seq`` column (migration 063, ``BIGINT GENERATED ALWAYS AS
IDENTITY``); the ``LISTEN``/``NOTIFY`` channel ``ootils_events`` is a *wake-up
signal only* and is assumed lossy — every wake triggers a keyset drain from the
last delivered sequence, so a dropped notification only delays delivery until
the next heartbeat drain, it never loses an event.

Delivery semantics: at-least-once. A reconnecting client resumes from its last
``id:`` (via ``?cursor=`` or the ``Last-Event-ID`` header); on the seam it may
re-receive a frame it already saw. Idempotency is the consumer's job and is
cheap here because every event carries a deterministic ``event_id`` UUID.

Concurrency model: each stream ``async def`` owns a DEDICATED async psycopg
connection opened OUTSIDE the sync pool. This is deliberate and load-bearing —
the sync handler threadpool is capped to the DB pool size (app.py lifespan,
SCALABILITY.md breaking point #6). A long-lived stream borrowing a pool
connection would be a self-inflicted DoS: N concurrent streams would pin the
entire pool and starve every ``def`` handler. The stream therefore never
touches ``get_db``/``dependencies.py``; it connects with the same DSN
resolution ``OotilsDB`` uses (``DATABASE_URL``).

Kill switch: ``OOTILS_STREAM_ENABLED`` (default ON). Falsy -> 503 before any
DB access, mirroring ``param_overrides.py``'s kill-switch pattern.

Budget: ``OOTILS_STREAM_MAX_CONN`` (default 32) concurrent streams, tracked by
a module-level counter guarded by a plain ``threading.Lock`` (sync — callable
outside a running event loop, see the ``_SlotLease`` docstring for why this
matters). Release is double-armed: the generator's own ``finally`` releases
its lease, AND a ``weakref.finalize`` on the generator object releases the
SAME lease if the generator is garbage-collected without ever having been
iterated once. Both paths are idempotent (a released lease is a no-op on a
second release) so whichever fires first wins and the second is harmless.

``once=true`` mode: a bounded catch-up drain (no LISTEN, no heartbeat, no
open-ended wait) that closes the response once the keyset is exhausted. This
is a first-class consumption mode, not a test shim — a cron watcher that
wants a bounded "give me everything since my last cursor" call uses it
without holding a connection open.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
import weakref
from datetime import date, datetime
from decimal import Decimal
from typing import Any, AsyncIterator, Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from starlette.responses import StreamingResponse

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import resolve_scenario_id
from ootils_core.db.connection import DEFAULT_DATABASE_URL
from ootils_core.db.types import AsyncDictRowConnection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1", tags=["stream"])

_TRUTHY = {"1", "true", "yes", "on"}

# NOTIFY channel the migration-063 trigger fires on; payload = scenario_id text.
_LISTEN_CHANNEL = "ootils_events"

# Heartbeat cadence, in seconds since the LAST FRAME EMITTED (data or ping) —
# tracked on a monotonic clock, NOT a fixed per-wake timeout (the LISTEN
# channel is global across scenarios; a wake from another scenario's NOTIFY
# must not reset this clock, or a busy server could starve a quiet stream's
# heartbeat indefinitely — see _stream_events' defect-3 comment). A ping
# frame (no `id:` line, so it does not advance the client cursor) is emitted
# once ~this many seconds have elapsed without any frame — keeps proxies
# from closing an idle connection and lets the client detect a dead stream.
# It also doubles as the periodic keyset-drain safety net for lossy NOTIFY.
# Not used in `once=True` mode (no heartbeat there).
_HEARTBEAT_SECONDS = 15.0

# Keyset page size per drain iteration. Bounded so a client resuming from
# cursor=0 on a large history streams in pages instead of buffering everything.
_DRAIN_BATCH = 500

# Business columns of the events row, in a stable order. Deliberately EXCLUDES
# the bookkeeping columns (processed, processed_at) — the stream carries the
# event's meaning, not its processing lifecycle. `stream_seq` is added by the
# envelope builder (it is the frame id, and also echoed in the body).
_EVENT_COLUMNS = (
    "event_id",
    "event_type",
    "scenario_id",
    "trigger_node_id",
    "field_changed",
    "old_date",
    "new_date",
    "old_quantity",
    "new_quantity",
    "old_text",
    "new_text",
    "source",
    "user_ref",
    "created_at",
)

# Assembled once; `event_type` is filtered app-side via `= ANY(%s)` when the
# caller passes `types` (see _parse_types). Ordered by stream_seq so the client
# cursor advances monotonically. LIMIT keeps each drain page bounded.
_DRAIN_SQL = (
    "SELECT stream_seq, " + ", ".join(_EVENT_COLUMNS) + " "
    "FROM events "
    "WHERE stream_seq > %(cursor)s "
    "AND scenario_id = %(scenario_id)s "
    "AND (%(types)s::text[] IS NULL OR event_type = ANY(%(types)s)) "
    "ORDER BY stream_seq "
    "LIMIT %(limit)s"
)


# ─────────────────────────────────────────────────────────────
# Kill switch + budget (module-level, mirrors param_overrides.py)
# ─────────────────────────────────────────────────────────────

def _stream_enabled() -> bool:
    """Kill switch, default ON. Falsy OOTILS_STREAM_ENABLED -> 503."""
    return os.environ.get("OOTILS_STREAM_ENABLED", "1").strip().lower() in _TRUTHY


def _max_connections() -> int:
    raw = os.environ.get("OOTILS_STREAM_MAX_CONN", "").strip()
    if not raw:
        return 32
    try:
        value = int(raw)
    except ValueError:
        logger.warning("stream.bad_max_conn value=%r — falling back to 32", raw)
        return 32
    return value if value > 0 else 32


_active_streams = 0
# threading.Lock, NOT asyncio.Lock: this lock must be acquirable from
# weakref.finalize's callback, which the GC can invoke from ANY context —
# including outside a running event loop, or during interpreter shutdown.
# asyncio.Lock.acquire() is a coroutine and cannot be awaited there. A plain
# threading.Lock is reentrant-free but cheap to hold for the few instructions
# of an increment/decrement, so it is safe to call from sync code too.
_active_streams_lock = threading.Lock()


class _SlotLease:
    """One reserved budget slot, released at most once.

    MAJOR fix (#391 adversarial review, defect 1): a Starlette
    ``StreamingResponse`` whose client disconnects before the ASGI runtime
    ever pulls the first item from its async generator NEVER runs that
    generator's body — Python does not execute an async generator's frame
    (hence never its ``finally``) until it is iterated at least once, and
    Starlette 0.50 does not call ``body_iterator.aclose()`` on that path
    either. A client that drops the TCP connection immediately after the
    request (killed curl, an aggressive reconnect loop) can therefore win the
    race against the first ``yield`` and leave the slot incremented forever —
    ``OOTILS_STREAM_MAX_CONN`` drops like that permanently wedge the budget
    until a process restart.

    The fix is a lease object with an idempotent ``release()`` (guarded by
    ``_done`` under the same lock), armed on TWO independent paths so at
    least one always fires:
      1. The generator's own ``finally`` calls ``lease.release()`` — the
         normal path when the generator is iterated (started, cancelled, or
         runs to exhaustion).
      2. ``weakref.finalize(gen_obj, lease.release)``, registered on the
         generator object BEFORE it is handed to ``StreamingResponse`` —
         fires when the generator object is garbage-collected, which happens
         even if it was NEVER iterated (the abandoned-generator case above).
    Both paths call the exact same lease, so whichever fires first wins and
    the other is a harmless no-op.
    """

    __slots__ = ("_done",)

    def __init__(self) -> None:
        self._done = False

    def release(self) -> None:
        global _active_streams
        with _active_streams_lock:
            if self._done:
                return
            self._done = True
            if _active_streams > 0:
                _active_streams -= 1


def _acquire_lease() -> _SlotLease | None:
    """Reserve one stream slot. Returns None if over budget (caller -> 503).

    Sync on purpose (plain function, no ``await``) — called from the ``async
    def`` handler like any other expression, but kept callable from
    non-async contexts too (kept symmetrical with ``_SlotLease.release``,
    which the GC finalizer calls that way).
    """
    global _active_streams
    with _active_streams_lock:
        if _active_streams >= _max_connections():
            return None
        _active_streams += 1
    return _SlotLease()


# ─────────────────────────────────────────────────────────────
# Pure helpers (no DB, no IO) — the test-writer wave targets these
# ─────────────────────────────────────────────────────────────

def _resolve_cursor(cursor_param: int | None, last_event_id_header: str | None) -> int | None:
    """Resume precedence: explicit ?cursor= > Last-Event-ID header > None.

    Returns the last-seen sequence to resume AFTER (drain uses stream_seq >
    cursor), or None meaning "no explicit resume point — start from now"
    (the caller then seeds the cursor with the current MAX(stream_seq)).

    cursor=0 is a real value (replay the whole scenario history), NOT None.
    A malformed / negative Last-Event-ID is ignored (treated as absent) rather
    than raising: a reconnecting browser controls that header, and a bad one
    should degrade to "from now", never 4xx the reconnect. An explicit
    ?cursor= is validated at the FastAPI layer (ge=0) so it never reaches here
    malformed.
    """
    if cursor_param is not None:
        return cursor_param
    if last_event_id_header is not None:
        raw = last_event_id_header.strip()
        if raw.isdigit():
            return int(raw)
    return None


def _parse_types(csv: str | None) -> list[str] | None:
    """Parse the optional `types` CSV filter into a de-duplicated list.

    Returns None when unset/blank (no filter → all event types). We do NOT
    validate against the events CHECK enum here: an unknown type simply matches
    zero rows via `event_type = ANY(%s)`, which is the correct, forward-
    compatible behaviour (a client can subscribe to a type this build predates
    without a 422, and gets it once the DB learns it). Order is preserved for
    determinism; blanks are dropped.
    """
    if csv is None:
        return None
    seen: set[str] = set()
    out: list[str] = []
    for raw in csv.split(","):
        token = raw.strip()
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out or None


def _jsonable(value: Any) -> Any:
    """Coerce one event-column value to a JSON-serialisable scalar.

    UUID/date/datetime/Decimal → str (dates/timestamps ISO-8601). Everything
    else (str, int, bool, None) passes through. Decimal → str (not float) to
    preserve exact quantity precision on the wire.
    """
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    return value


def _envelope(row: dict[str, Any]) -> dict[str, Any]:
    """Build the JSON body of one SSE frame from an events row.

    Includes `stream_seq` plus every business column present in the row; NULL
    columns are omitted for compactness (a consumer treats absent == null).
    Bookkeeping columns (processed, processed_at) are never in `_EVENT_COLUMNS`
    so they cannot leak in even if the row dict carries them.
    """
    out: dict[str, Any] = {"stream_seq": int(row["stream_seq"])}
    for col in _EVENT_COLUMNS:
        coerced = _jsonable(row.get(col))
        if coerced is not None:
            out[col] = coerced
    return out


def _sse_frame(seq: int, event_type: str, data_json: str) -> str:
    """Assemble one SSE frame text block.

    ``id:`` sets the client's Last-Event-ID (drives reconnect resume);
    ``event:`` names the type so ``EventSource.addEventListener(type, …)``
    works; ``data:`` is the JSON body. Trailing blank line terminates the
    frame per the SSE spec.
    """
    return f"id: {seq}\nevent: {event_type}\ndata: {data_json}\n\n"


def _heartbeat_frame() -> str:
    """Comment-free ping frame WITHOUT an ``id:`` line, so it does not advance
    the client cursor. Keeps intermediaries from reaping an idle stream."""
    return "event: ping\ndata: {}\n\n"


# ─────────────────────────────────────────────────────────────
# The async IO loop — orchestration only
# ─────────────────────────────────────────────────────────────

async def _open_stream_connection() -> AsyncDictRowConnection:
    """Open a dedicated autocommit async connection (OUTSIDE the sync pool).

    Same DSN resolution as OotilsDB (DATABASE_URL / OOTILS_DSN, default local
    socket). autocommit=True is required for LISTEN/NOTIFY to see committed
    notifications without an explicit transaction boundary.
    """
    return await psycopg.AsyncConnection.connect(
        DEFAULT_DATABASE_URL,
        autocommit=True,
        row_factory=psycopg.rows.dict_row,
    )


async def _seed_cursor(conn: AsyncDictRowConnection, scenario_id: UUID, resume_from: int | None) -> int:
    """Return the sequence to resume AFTER.

    Explicit resume point (?cursor= / Last-Event-ID) is honoured verbatim.
    Otherwise 'from now' = the current MAX(stream_seq) for the scenario, so the
    client only sees events created after it connected. COALESCE(..., 0) means
    an empty scenario starts at 0 (next event delivered).
    """
    if resume_from is not None:
        return resume_from
    row = await (
        await conn.execute(
            "SELECT COALESCE(MAX(stream_seq), 0) AS seq FROM events WHERE scenario_id = %s",
            (scenario_id,),
        )
    ).fetchone()
    return int(row["seq"]) if row else 0


async def _drain(
    conn: AsyncDictRowConnection,
    scenario_id: UUID,
    types: list[str] | None,
    cursor: int,
) -> AsyncIterator[tuple[int, str]]:
    """Yield (new_cursor, frame) for every event past `cursor`, in pages.

    Loops the keyset SELECT until a page returns fewer than _DRAIN_BATCH rows
    (history exhausted). Advances the cursor to each row's stream_seq so the
    caller can persist progress even if the client disconnects mid-drain.
    """
    while True:
        rows = await (
            await conn.execute(
                _DRAIN_SQL,
                {
                    "cursor": cursor,
                    "scenario_id": scenario_id,
                    "types": types,
                    "limit": _DRAIN_BATCH,
                },
            )
        ).fetchall()
        if not rows:
            return
        for row in rows:
            seq = int(row["stream_seq"])
            body = json.dumps(_envelope(row), separators=(",", ":"))
            cursor = seq
            yield seq, _sse_frame(seq, row["event_type"], body)
        if len(rows) < _DRAIN_BATCH:
            return


async def _wait_for_wakeup(conn: AsyncDictRowConnection, timeout: float) -> None:
    """Block up to `timeout` seconds for a NOTIFY on `_LISTEN_CHANNEL`, or
    return early the moment one arrives. Return value is not meaningful — the
    caller always re-drains regardless (NOTIFY is a lossy wake signal, the
    keyset SELECT is the truth); this function exists purely to sleep
    efficiently until "something to check" or "time to heartbeat".

    MINOR fix (#391 adversarial review, defect 2): earlier code did
    `async for _n in conn.notifies(timeout=..., stop_after=1): break`. In
    psycopg 3.3, `notifies()` yields from INSIDE `async with self.lock:` (the
    SAME lock `conn.execute` takes) and detaches the connection's notify
    backlog for the duration. Breaking out of the `async for` abandons that
    generator mid-body, WITHOUT ever running its `finally` (which restores
    the backlog and releases the lock) — the only reason the next drain's
    `conn.execute` wasn't permanently deadlocked was incidental CPython
    refcounting closing the abandoned generator "soon enough". Removing the
    `break` and just letting `stop_after=1` end the generator naturally
    (confirmed in the installed psycopg 3.3.3 source: the loop `break`s
    itself once `nreceived >= stop_after`, then its own `finally` runs) makes
    the backlog-restore / lock-release deterministic instead of GC-timing
    dependent.
    """
    async for _notify in conn.notifies(timeout=timeout, stop_after=1):
        pass


async def _stream_events(
    scenario_id: UUID,
    types: list[str] | None,
    resume_from: int | None,
    correlation_id: str,
    once: bool,
) -> AsyncIterator[str]:
    """The SSE generator body. Owns the dedicated async connection lifecycle.

    Two modes:
      - subscribe (`once=False`, default): LISTEN, then loop {drain to empty
        → wait for a notification or a heartbeat-cadence timeout → repeat}
        forever, until the client disconnects.
      - catch-up (`once=True`): no LISTEN, no heartbeat, no open-ended wait —
        drain to empty exactly once and return, closing the response. This
        is what makes the endpoint usable both by a bounded cron watcher and
        by a TestClient (which buffers the FULL response before returning
        control — an open-ended stream would hang the harness forever, cf.
        defect 4).

    All IO is here; every frame's *content* is built by the pure helpers
    above.
    """
    conn: AsyncDictRowConnection | None = None
    try:
        conn = await _open_stream_connection()
        if not once:
            await conn.execute(f"LISTEN {_LISTEN_CHANNEL}")
        cursor = await _seed_cursor(conn, scenario_id, resume_from)
        logger.info(
            "stream.open scenario_id=%s cursor=%s types=%s once=%s correlation_id=%s",
            scenario_id, cursor, types, once, correlation_id,
        )

        async for seq, frame in _drain(conn, scenario_id, types, cursor):
            cursor = seq
            yield frame

        if once:
            logger.info(
                "stream.once_done scenario_id=%s final_cursor=%s correlation_id=%s",
                scenario_id, cursor, correlation_id,
            )
            return

        # MAJOR fix (#391 adversarial review, defect 3): the wake source
        # (LISTEN on a GLOBAL channel) is decoupled from the heartbeat clock.
        # Every scenario's INSERT fires the SAME `ootils_events` channel —
        # a stream subscribed to a quiet scenario on a busy server would be
        # woken by every OTHER scenario's NOTIFY, re-drain empty every time,
        # and therefore never reach an uninterrupted `timeout` window: zero
        # heartbeats for an unbounded stretch, even though the documented
        # contract is "a ping every ~15s of silence". Fix: track wall-clock
        # time since the last frame (data OR heartbeat) with a monotonic
        # clock, independent of why we woke up, and always wait for exactly
        # the REMAINING budget rather than resetting to the full
        # _HEARTBEAT_SECONDS after every spurious wake.
        last_frame = time.monotonic()
        while True:
            remaining = _HEARTBEAT_SECONDS - (time.monotonic() - last_frame)
            if remaining <= 0:
                yield _heartbeat_frame()
                last_frame = time.monotonic()
                continue

            await _wait_for_wakeup(conn, timeout=remaining)

            emitted = False
            async for seq, frame in _drain(conn, scenario_id, types, cursor):
                cursor = seq
                emitted = True
                yield frame
            if emitted:
                last_frame = time.monotonic()
            # If nothing was emitted (spurious wake from another scenario's
            # NOTIFY, or a real notification that raced a rolled-back
            # transaction), loop back: `remaining` will reflect the clock
            # correctly on the next iteration, no heartbeat is skipped.

    except asyncio.CancelledError:
        # Normal client disconnect (browser closed the EventSource). Not an
        # error — no stack trace, just a debug breadcrumb. Re-raise so the
        # async runtime can finish cancelling the task.
        logger.debug("stream.disconnect scenario_id=%s correlation_id=%s", scenario_id, correlation_id)
        raise
    except Exception as exc:
        # Any other failure (lost DB connection, etc.): log with context, then
        # let the generator terminate. The SSE connection drops; the client's
        # EventSource auto-reconnects and resumes from its Last-Event-ID.
        logger.error(
            "stream.error scenario_id=%s correlation_id=%s error=%s",
            scenario_id, correlation_id, exc,
        )
    finally:
        if conn is not None:
            try:
                await conn.close()
            except Exception as close_exc:
                logger.warning("stream.close_failed correlation_id=%s error=%s", correlation_id, close_exc)


@router.get("/stream")
async def stream_events(
    scenario_id: UUID = Depends(resolve_scenario_id),
    types: Optional[str] = Query(
        default=None,
        description="Optional CSV of event_type values to include. Unknown types match nothing.",
    ),
    cursor: Optional[int] = Query(
        default=None,
        ge=0,
        description="Resume AFTER this stream_seq. 0 = replay full scenario history. "
        "Omit to stream from now. Takes precedence over the Last-Event-ID header.",
    ),
    once: bool = Query(
        default=False,
        description="Bounded catch-up mode: drain the keyset once and close, "
        "instead of LISTENing and staying open. No heartbeat in this mode. "
        "The natural mode for a cron watcher's periodic 'give me everything "
        "since my last cursor' call.",
    ),
    last_event_id: Optional[str] = Header(default=None, alias="Last-Event-ID"),
    _token: str = Depends(require_auth),
) -> StreamingResponse:
    """Server-Sent Events feed of the ``events`` table for one scenario.

    ``scenario_id`` resolves via the shared pool-free resolver (query param or
    ``X-Scenario-ID`` header; default baseline). Auth is required like every
    other ``/v1/*`` endpoint. The kill switch and the concurrency budget are
    checked HERE, before opening any connection, so a disabled or saturated
    stream service answers 503 without touching the DB.
    """
    if not _stream_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event stream is disabled (OOTILS_STREAM_ENABLED).",
        )

    lease = _acquire_lease()
    if lease is None:
        logger.warning("stream.budget_exceeded max=%d", _max_connections())
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event stream at capacity (OOTILS_STREAM_MAX_CONN). Retry shortly.",
        )

    resume_from = _resolve_cursor(cursor, last_event_id)
    parsed_types = _parse_types(types)
    # Correlation id for log stitching. Deliberately not the request's
    # X-Correlation-ID (which lives on request.state) — the generator stays
    # decoupled from the Request object, so a fresh opaque id per stream is
    # enough to follow one stream's lifecycle across log lines.
    correlation_id = f"stream_{os.urandom(4).hex()}"

    async def _guarded_generator() -> AsyncIterator[str]:
        try:
            async for frame in _stream_events(scenario_id, parsed_types, resume_from, correlation_id, once):
                yield frame
        finally:
            # Normal path: the generator was iterated (started, ran to
            # exhaustion, or was cancelled) — release here. See _SlotLease
            # for why a SECOND path (weakref.finalize below) also exists.
            lease.release()

    gen_obj = _guarded_generator()
    # MAJOR fix (#391 adversarial review, defect 1): arm the GC-path release
    # BEFORE handing the generator to StreamingResponse. If the client
    # disconnects before Starlette ever pulls the first item, the generator
    # is dropped unstarted — its `finally` above NEVER runs (an async
    # generator's frame, and therefore its `finally`, only executes once the
    # generator has been iterated at least once) — so `lease.release()` here
    # is the only thing standing between an abandoned generator and a
    # permanently leaked budget slot. `lease.release()` is idempotent, so if
    # the `finally` above already ran, this is a harmless no-op.
    weakref.finalize(gen_obj, lease.release)

    return StreamingResponse(
        gen_obj,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
