"""
tests/integration/test_stream_integration.py — GET /v1/stream SSE integration
tests (chantier #391, StreamChanges).

Real PostgreSQL (via ``migrated_db``: migration 063 auto-applies at OotilsDB
construction, adding ``events.stream_seq`` + the ``ootils_events`` NOTIFY
trigger) + FastAPI TestClient. No mocks.

#391 adversarial-review defect 4 (ADR-027, SPEC-INTERFACES.md §5.2): every
data-carrying read here uses ``?once=true``. In the default subscribe mode
(``once=false``) the generator LISTENs and loops drain-then-wait FOREVER —
Starlette's ``TestClient`` buffers the ENTIRE response before returning
control from ``client.stream(...)``, so an unbounded stream would hang the
test harness permanently, not just flake. ``once=true`` is a first-class
catch-up mode (a cron watcher's "give me everything since my cursor" call):
no LISTEN, no heartbeat, drain once to exhaustion, then the response CLOSES
normally. That natural close is what makes this endpoint testable against a
synchronous client at all, so every frame-reading test below reads to that
natural end (``resp.iter_lines()`` simply stops yielding) rather than
stopping early at an expected count — ``_read_frames`` still carries a hard
``_MAX_LINES`` ceiling as defence in depth against a malformed/unterminated
frame, but it is no longer the thing proving the read is bounded; ``once``
mode's own contract is.

Live NOTIFY wake-up mid-read, and the heartbeat cadence itself, are NOT
exercised here: ``once=true`` never LISTENs and never emits a ping (see
stream.py's ``_stream_events``), so neither is reachable through this
harness. The heartbeat's monotone-clock logic (``last_frame`` /
``remaining``) lives inline in ``_stream_events``'s subscribe-mode loop, not
in an extracted pure helper — there is nothing pure to unit-test it against
without also driving the async drain/LISTEN loop, so it stays uncovered by
test-writer scope; the ADR-027 review is the record of that design's
correctness. Replay-by-cursor (case 4) remains the contract this suite
proves end-to-end: the keyset SELECT over ``stream_seq`` is the replayable
truth, NOTIFY is only ever a latency-cutting wake-up (migration 063 header).
"""
from __future__ import annotations

import json
import os
from uuid import UUID, uuid4

import pytest

from ootils_core.api.routers import stream

from .conftest import requires_db, TEST_DB_URL

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"

# Hard safety ceiling: no single read should ever cross this many raw SSE
# lines. Defence in depth against a malformed frame with no blank-line
# terminator turning _read_frames into an unbounded loop — `once=true`'s own
# natural response close is what actually bounds these reads.
_MAX_LINES = 2000


# ---------------------------------------------------------------------------
# Fixtures — mirror test_api_db.py / test_events_read.py, but seed-free.
# These tests only need the `events` + `scenarios` tables (both created by
# migration 002), never nodes/items, so we skip the (slow, optional) demo seed
# and build the app straight on migrated_db.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_token() -> str:
    """Effective API token for this module, exported to the process env.

    Read back from os.environ (never hard-coded at the call sites) so auth
    headers and the server's _expected_token() are guaranteed identical.
    """
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"
    return os.environ["OOTILS_API_TOKEN"]


@pytest.fixture(scope="module")
def api_client(migrated_db, api_token):
    os.environ["DATABASE_URL"] = migrated_db

    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def auth(api_token):
    return {"Authorization": f"Bearer {api_token}"}


@pytest.fixture
def reset_stream_budget(monkeypatch):
    """Isolate the module-level _active_streams counter for budget tests.

    Snapshot to 0 before the test and restore afterwards so a leaked slot in
    one test never poisons another (and never leaks into the wider suite).
    """
    monkeypatch.setattr(stream, "_active_streams", 0)
    yield
    monkeypatch.setattr(stream, "_active_streams", 0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pg():
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(TEST_DB_URL, row_factory=dict_row)


def _make_scenario(name: str) -> UUID:
    """INSERT a fresh non-baseline scenario, return its id (autocommit)."""
    scenario_id = uuid4()
    import psycopg

    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
            "VALUES (%s, %s, FALSE, 'active')",
            (scenario_id, name),
        )
    return scenario_id


def _insert_event(scenario_id: str | UUID, event_type: str = "test_event", *, user_ref: str | None = None) -> None:
    """INSERT one event and COMMIT so the stream's dedicated connection sees it.

    Uses a short-lived committed transaction (not autocommit-off) because the
    stream opens its OWN autocommit connection and only reads COMMITTED rows.
    """
    import psycopg

    with psycopg.connect(TEST_DB_URL) as conn:
        conn.execute(
            "INSERT INTO events (event_type, scenario_id, source, user_ref) "
            "VALUES (%s, %s, 'test', %s)",
            (event_type, scenario_id, user_ref),
        )
        conn.commit()


def _seq_for(scenario_id: str | UUID, user_ref: str) -> int:
    """stream_seq of the event tagged with a unique user_ref (test correlation)."""
    with _pg() as conn:
        row = conn.execute(
            "SELECT stream_seq FROM events WHERE scenario_id = %s AND user_ref = %s",
            (scenario_id, user_ref),
        ).fetchone()
    assert row is not None, f"event user_ref={user_ref} not found"
    return int(row["stream_seq"])


def _read_frames(resp, max_lines: int = _MAX_LINES) -> list[dict]:
    """Read every SSE frame until the response ends naturally, then return them.

    A frame is the block of ``key: value`` lines up to the blank separator
    line. Returns one dict per frame: {"id": int|None, "event": str,
    "data": <parsed json | raw str>}. In ``once=true`` mode the response
    closes on its own once the keyset drain is exhausted, so
    ``resp.iter_lines()`` simply stops yielding — ``max_lines`` is a defence-
    in-depth ceiling, not the thing bounding this read.
    """
    frames: list[dict] = []
    fields: dict[str, str] = {}
    seen_lines = 0
    for raw in resp.iter_lines():
        seen_lines += 1
        assert seen_lines <= max_lines, (
            f"exceeded {max_lines} SSE lines — once=true response never "
            f"closed, or a frame is missing its blank-line terminator"
        )
        line = raw.rstrip("\r")
        if line == "":
            # Blank line terminates the current frame (if any content buffered).
            if fields:
                frames.append(_finish_frame(fields))
                fields = {}
            continue
        key, _, value = line.partition(":")
        # SSE allows an optional single leading space after the colon.
        if value.startswith(" "):
            value = value[1:]
        fields[key] = value
    if fields:
        # Tolerate a final frame with no trailing blank line (should not
        # happen per the SSE frame format, but don't silently drop data).
        frames.append(_finish_frame(fields))
    return frames


def _finish_frame(fields: dict[str, str]) -> dict:
    raw_data = fields.get("data", "")
    try:
        data: object = json.loads(raw_data)
    except (ValueError, TypeError):
        data = raw_data
    seq = fields.get("id")
    return {
        "id": int(seq) if seq is not None and seq.isdigit() else None,
        "event": fields.get("event", ""),
        "data": data,
    }


# ---------------------------------------------------------------------------
# Case 1 — missing auth → 401
# ---------------------------------------------------------------------------


@requires_db
def test_stream_requires_auth(api_client):
    # Auth is checked before once/cursor parsing — no stream is ever opened.
    resp = api_client.get("/v1/stream")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Case 2 — kill switch OOTILS_STREAM_ENABLED=0 → 503 (before any DB access)
# ---------------------------------------------------------------------------


@requires_db
def test_stream_kill_switch_returns_503(api_client, auth, monkeypatch):
    # The kill switch is checked before _acquire_lease and before `once` has
    # any effect — no stream is ever opened either way.
    monkeypatch.setenv("OOTILS_STREAM_ENABLED", "0")
    resp = api_client.get("/v1/stream?cursor=0&once=true", headers=auth)
    assert resp.status_code == 503
    assert "OOTILS_STREAM_ENABLED" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Case 3 — budget: at-capacity → 503; freed slot → 200.
#
# Per the coordinator's adversarial-review note: do NOT hold two concurrent
# streams open against a synchronous TestClient to prove this — that is
# exactly the "hang forever" trap once=true was introduced to close. Simulate
# saturation directly on the module-level counter instead (it's a plain int
# behind a threading.Lock, `_acquire_lease` reads it synchronously), which
# proves the SAME branch (`_acquire_lease() is None` → 503) without ever
# opening a second stream concurrently.
# ---------------------------------------------------------------------------


@requires_db
def test_stream_budget_exceeded_then_released(api_client, auth, monkeypatch, reset_stream_budget):
    monkeypatch.setenv("OOTILS_STREAM_ENABLED", "1")
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "1")

    # Saturate the budget directly (equivalent to "1 stream already open").
    monkeypatch.setattr(stream, "_active_streams", 1)
    resp = api_client.get("/v1/stream?cursor=0&once=true", headers=auth)
    assert resp.status_code == 503
    assert "OOTILS_STREAM_MAX_CONN" in resp.json()["detail"]

    # Slot freed (equivalent to the earlier stream's lease.release() firing)
    # → a new request succeeds. once=true means this response also closes on
    # its own, so no manual cleanup is needed afterwards.
    monkeypatch.setattr(stream, "_active_streams", 0)
    resp2 = api_client.get("/v1/stream?cursor=0&once=true", headers=auth)
    assert resp2.status_code == 200


# ---------------------------------------------------------------------------
# Case 4 — cursor replay (THE central contract)
# ---------------------------------------------------------------------------


@requires_db
def test_stream_cursor_replay_from_zero_and_resume(api_client, auth, monkeypatch, reset_stream_budget):
    monkeypatch.setenv("OOTILS_STREAM_ENABLED", "1")
    monkeypatch.delenv("OOTILS_STREAM_MAX_CONN", raising=False)

    scenario_id = _make_scenario(f"stream-replay-{uuid4().hex[:8]}")
    tags = [f"replay-{uuid4().hex[:8]}" for _ in range(3)]
    for tag in tags:
        _insert_event(scenario_id, "test_event", user_ref=tag)

    # ?cursor=0&once=true replays the full scenario history then closes —
    # exactly 3 data frames, no heartbeat, no open-ended wait.
    url = f"/v1/stream?scenario_id={scenario_id}&cursor=0&once=true"
    with api_client.stream("GET", url, headers=auth) as resp:
        assert resp.status_code == 200
        frames = _read_frames(resp)

    assert len(frames) == 3
    # id: strictly increasing.
    ids = [f["id"] for f in frames]
    assert all(i is not None for i in ids)
    assert ids == sorted(ids)
    assert ids[0] < ids[1] < ids[2]

    # Envelope correctness on the first frame.
    first = frames[0]
    assert first["event"] == "test_event"
    body = first["data"]
    assert isinstance(body, dict)
    assert "event_id" in body
    assert body["scenario_id"] == str(scenario_id)
    assert body["stream_seq"] == first["id"]
    # Bookkeeping columns never leak.
    assert "processed" not in body
    assert "processed_at" not in body

    # Reconnect AFTER the 2nd event's stream_seq → only the 3rd arrives (proof
    # of resume with no loss and no duplicate).
    second_seq = _seq_for(scenario_id, tags[1])
    third_seq = _seq_for(scenario_id, tags[2])
    resume_url = f"/v1/stream?scenario_id={scenario_id}&cursor={second_seq}&once=true"
    with api_client.stream("GET", resume_url, headers=auth) as resp2:
        assert resp2.status_code == 200
        resumed = _read_frames(resp2)

    assert len(resumed) == 1
    assert resumed[0]["id"] == third_seq
    assert resumed[0]["data"]["stream_seq"] == third_seq


# ---------------------------------------------------------------------------
# Case 5 — default (no cursor) = "from now": a pre-existing event is NOT
# replayed. In once=true mode this is now fully deterministic: the seeded
# cursor (MAX(stream_seq)) excludes the prior event, the drain finds nothing
# past it, and the response closes with ZERO frames — no heartbeat kill
# needed (once=true never emits one).
# ---------------------------------------------------------------------------


@requires_db
def test_stream_no_cursor_starts_from_now(api_client, auth, monkeypatch, reset_stream_budget):
    monkeypatch.setenv("OOTILS_STREAM_ENABLED", "1")
    monkeypatch.delenv("OOTILS_STREAM_MAX_CONN", raising=False)

    scenario_id = _make_scenario(f"stream-fromnow-{uuid4().hex[:8]}")
    # Pre-existing event BEFORE the stream opens.
    _insert_event(scenario_id, "test_event", user_ref=f"prior-{uuid4().hex[:8]}")

    # No cursor → seed = MAX(stream_seq), so the prior event is excluded.
    # once=true never LISTENs/heartbeats, so an empty drain closes the
    # response immediately with no frames at all.
    url = f"/v1/stream?scenario_id={scenario_id}&once=true"
    with api_client.stream("GET", url, headers=auth) as resp:
        assert resp.status_code == 200
        frames = _read_frames(resp)

    assert frames == []


# ---------------------------------------------------------------------------
# Case 6 — types filter: only the subscribed event_type is delivered
# ---------------------------------------------------------------------------


@requires_db
def test_stream_types_filter(api_client, auth, monkeypatch, reset_stream_budget):
    monkeypatch.setenv("OOTILS_STREAM_ENABLED", "1")
    monkeypatch.delenv("OOTILS_STREAM_MAX_CONN", raising=False)

    scenario_id = _make_scenario(f"stream-types-{uuid4().hex[:8]}")
    wanted_tag = f"wanted-{uuid4().hex[:8]}"
    # Two events of DIFFERENT types (both in the migration-002/062 CHECK enum).
    _insert_event(scenario_id, "supply_date_changed", user_ref=f"other-{uuid4().hex[:8]}")
    _insert_event(scenario_id, "test_event", user_ref=wanted_tag)

    wanted_seq = _seq_for(scenario_id, wanted_tag)
    url = f"/v1/stream?scenario_id={scenario_id}&cursor=0&once=true&types=test_event"
    with api_client.stream("GET", url, headers=auth) as resp:
        assert resp.status_code == 200
        frames = _read_frames(resp)

    assert len(frames) == 1
    assert frames[0]["event"] == "test_event"
    assert frames[0]["id"] == wanted_seq
    assert frames[0]["data"]["event_type"] == "test_event"


# ---------------------------------------------------------------------------
# Case 7 — scenario isolation: scenario B's event never surfaces on A's stream
# ---------------------------------------------------------------------------


@requires_db
def test_stream_scenario_isolation(api_client, auth, monkeypatch, reset_stream_budget):
    monkeypatch.setenv("OOTILS_STREAM_ENABLED", "1")
    monkeypatch.delenv("OOTILS_STREAM_MAX_CONN", raising=False)

    scenario_a = _make_scenario(f"stream-iso-a-{uuid4().hex[:8]}")
    scenario_b = _make_scenario(f"stream-iso-b-{uuid4().hex[:8]}")

    a_tag = f"a-only-{uuid4().hex[:8]}"
    _insert_event(scenario_b, "test_event", user_ref=f"b-only-{uuid4().hex[:8]}")
    _insert_event(scenario_a, "test_event", user_ref=a_tag)

    a_seq = _seq_for(scenario_a, a_tag)
    url = f"/v1/stream?scenario_id={scenario_a}&cursor=0&once=true"
    with api_client.stream("GET", url, headers=auth) as resp:
        assert resp.status_code == 200
        frames = _read_frames(resp)

    assert len(frames) == 1
    # Only A's event; its scenario_id in the envelope is A, never B.
    assert frames[0]["id"] == a_seq
    assert frames[0]["data"]["scenario_id"] == str(scenario_a)
    assert frames[0]["data"]["scenario_id"] != str(scenario_b)


# ---------------------------------------------------------------------------
# Case 8 — Last-Event-ID header: reconnect resumes AFTER the header value
# ---------------------------------------------------------------------------


@requires_db
def test_stream_last_event_id_header_resumes(api_client, auth, monkeypatch, reset_stream_budget):
    monkeypatch.setenv("OOTILS_STREAM_ENABLED", "1")
    monkeypatch.delenv("OOTILS_STREAM_MAX_CONN", raising=False)

    scenario_id = _make_scenario(f"stream-lei-{uuid4().hex[:8]}")
    tags = [f"lei-{uuid4().hex[:8]}" for _ in range(2)]
    for tag in tags:
        _insert_event(scenario_id, "test_event", user_ref=tag)

    first_seq = _seq_for(scenario_id, tags[0])
    second_seq = _seq_for(scenario_id, tags[1])

    # Reconnect via Last-Event-ID = first event's seq → only the 2nd arrives.
    headers = {**auth, "Last-Event-ID": str(first_seq)}
    url = f"/v1/stream?scenario_id={scenario_id}&once=true"
    with api_client.stream("GET", url, headers=headers) as resp:
        assert resp.status_code == 200
        frames = _read_frames(resp)

    assert len(frames) == 1
    assert frames[0]["id"] == second_seq
    assert frames[0]["data"]["stream_seq"] == second_seq
