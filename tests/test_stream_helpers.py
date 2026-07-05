"""
tests/test_stream_helpers.py — pure unit tests for the SSE stream helpers
(chantier #391, StreamChanges).

No DB required. Exercises the pure/no-IO surface of
``ootils_core.api.routers.stream``:
  - _resolve_cursor / _parse_types / _jsonable / _envelope   (pure transforms)
  - _sse_frame / _heartbeat_frame                            (SSE wire format)
  - _stream_enabled / _max_connections                       (kill switch + budget config)
  - _acquire_lease / _SlotLease.release                      (sync budget counter + leak-proof release)

DB round-trip coverage (replay cursor, NOTIFY isolation, auth/kill-switch
HTTP status) lives in tests/integration/test_stream_integration.py, in the
bounded ``once=true`` catch-up mode (see ADR-027 adversarial-review fixes).
"""
from __future__ import annotations

import gc
import weakref
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from ootils_core.api.routers import stream
from ootils_core.api.routers.stream import (
    _EVENT_COLUMNS,
    _acquire_lease,
    _envelope,
    _heartbeat_frame,
    _jsonable,
    _max_connections,
    _parse_types,
    _resolve_cursor,
    _sse_frame,
    _stream_enabled,
)


# ---------------------------------------------------------------------------
# _resolve_cursor — resume precedence: ?cursor= > Last-Event-ID > None
# ---------------------------------------------------------------------------


def test_resolve_cursor_explicit_param_wins():
    assert _resolve_cursor(42, "7") == 42


def test_resolve_cursor_explicit_zero_is_a_real_value():
    # cursor=0 means "replay whole history"; it must NOT fall through to None.
    assert _resolve_cursor(0, "7") == 0


def test_resolve_cursor_zero_wins_over_header():
    assert _resolve_cursor(0, None) == 0


def test_resolve_cursor_falls_back_to_numeric_header():
    assert _resolve_cursor(None, "13") == 13


def test_resolve_cursor_header_is_trimmed():
    assert _resolve_cursor(None, "  13  ") == 13


def test_resolve_cursor_non_numeric_header_ignored():
    assert _resolve_cursor(None, "not-a-number") is None


def test_resolve_cursor_negative_header_ignored():
    # isdigit() is False for '-5' → treated as absent, degrade to "from now".
    assert _resolve_cursor(None, "-5") is None


def test_resolve_cursor_empty_header_ignored():
    assert _resolve_cursor(None, "") is None
    assert _resolve_cursor(None, "   ") is None


def test_resolve_cursor_both_absent_is_none():
    assert _resolve_cursor(None, None) is None


def test_resolve_cursor_float_like_header_ignored():
    # '3.5'.isdigit() is False — a decimal string is not a valid cursor.
    assert _resolve_cursor(None, "3.5") is None


# ---------------------------------------------------------------------------
# _parse_types — CSV filter, dedup, order-preserving, blanks dropped
# ---------------------------------------------------------------------------


def test_parse_types_none_is_none():
    assert _parse_types(None) is None


def test_parse_types_empty_string_is_none():
    assert _parse_types("") is None


def test_parse_types_blanks_only_is_none():
    assert _parse_types("   ,  , ") is None


def test_parse_types_single_value_trimmed():
    assert _parse_types("  supply_date_changed  ") == ["supply_date_changed"]


def test_parse_types_preserves_order():
    assert _parse_types("b,a,c") == ["b", "a", "c"]


def test_parse_types_dedups_preserving_first_occurrence_order():
    assert _parse_types("a,b,a,c,b") == ["a", "b", "c"]


def test_parse_types_drops_blank_tokens():
    assert _parse_types("a,,b, ,c") == ["a", "b", "c"]


def test_parse_types_trims_each_token():
    assert _parse_types(" a , b ,c ") == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# _jsonable — scalar coercion to JSON-serialisable values
# ---------------------------------------------------------------------------


def test_jsonable_none_passthrough():
    assert _jsonable(None) is None


def test_jsonable_uuid_to_str():
    u = UUID("11111111-1111-1111-1111-111111111111")
    out = _jsonable(u)
    assert out == "11111111-1111-1111-1111-111111111111"
    assert isinstance(out, str)


def test_jsonable_decimal_to_str_not_float():
    out = _jsonable(Decimal("10.50"))
    assert out == "10.50"
    assert isinstance(out, str)
    assert not isinstance(out, float)


def test_jsonable_decimal_preserves_precision():
    # A float round-trip would corrupt this; str preserves it exactly.
    value = Decimal("0.1000000000000000055511151231257827021181583404541015625")
    out = _jsonable(value)
    assert out == str(value)
    assert isinstance(out, str)


def test_jsonable_date_iso():
    assert _jsonable(date(2026, 7, 5)) == "2026-07-05"


def test_jsonable_datetime_iso():
    dt = datetime(2026, 7, 5, 12, 30, 45, tzinfo=timezone.utc)
    out = _jsonable(dt)
    assert out == dt.isoformat()
    assert out.startswith("2026-07-05T12:30:45")


def test_jsonable_str_passthrough():
    assert _jsonable("hello") == "hello"


def test_jsonable_int_passthrough():
    out = _jsonable(7)
    assert out == 7
    assert isinstance(out, int)


# ---------------------------------------------------------------------------
# _envelope — JSON body from an events row
# ---------------------------------------------------------------------------


def _full_row() -> dict:
    return {
        "stream_seq": 5,
        "event_id": UUID("22222222-2222-2222-2222-222222222222"),
        "event_type": "supply_date_changed",
        "scenario_id": UUID("00000000-0000-0000-0000-000000000001"),
        "trigger_node_id": UUID("33333333-3333-3333-3333-333333333333"),
        "field_changed": "quantity",
        "old_date": date(2026, 1, 1),
        "new_date": date(2026, 2, 1),
        "old_quantity": Decimal("100.5"),
        "new_quantity": Decimal("200.0"),
        "old_text": None,
        "new_text": None,
        "source": "api",
        "user_ref": "agent-x",
        "created_at": datetime(2026, 7, 5, 9, 0, 0, tzinfo=timezone.utc),
    }


def test_envelope_stream_seq_present_as_int():
    env = _envelope(_full_row())
    assert env["stream_seq"] == 5
    assert isinstance(env["stream_seq"], int)


def test_envelope_stream_seq_coerced_from_non_int():
    # Postgres may hand back stream_seq as something int()-able; envelope forces int.
    row = _full_row()
    row["stream_seq"] = "9"
    env = _envelope(row)
    assert env["stream_seq"] == 9
    assert isinstance(env["stream_seq"], int)


def test_envelope_maps_business_columns_coerced():
    env = _envelope(_full_row())
    assert env["event_id"] == "22222222-2222-2222-2222-222222222222"
    assert env["event_type"] == "supply_date_changed"
    assert env["scenario_id"] == "00000000-0000-0000-0000-000000000001"
    assert env["trigger_node_id"] == "33333333-3333-3333-3333-333333333333"
    assert env["field_changed"] == "quantity"
    assert env["old_date"] == "2026-01-01"
    assert env["new_date"] == "2026-02-01"
    assert env["old_quantity"] == "100.5"
    assert env["new_quantity"] == "200.0"
    assert env["source"] == "api"
    assert env["user_ref"] == "agent-x"


def test_envelope_omits_null_columns():
    env = _envelope(_full_row())
    # old_text/new_text were None → omitted (consumer treats absent == null).
    assert "old_text" not in env
    assert "new_text" not in env


def test_envelope_never_leaks_bookkeeping_columns():
    row = _full_row()
    # Even if the row dict carries processing lifecycle columns, they must not appear.
    row["processed"] = True
    row["processed_at"] = datetime(2026, 7, 5, 10, 0, 0, tzinfo=timezone.utc)
    env = _envelope(row)
    assert "processed" not in env
    assert "processed_at" not in env


def test_envelope_bookkeeping_columns_not_in_event_columns_constant():
    # Structural guard: the exclusion is by construction (not in _EVENT_COLUMNS).
    assert "processed" not in _EVENT_COLUMNS
    assert "processed_at" not in _EVENT_COLUMNS


def test_envelope_missing_optional_column_omitted():
    # A row lacking an optional business column simply omits it (row.get → None).
    row = _full_row()
    del row["user_ref"]
    env = _envelope(row)
    assert "user_ref" not in env


def test_envelope_all_values_json_serialisable():
    import json

    env = _envelope(_full_row())
    # Must round-trip through json.dumps with no default= fallback.
    encoded = json.dumps(env, separators=(",", ":"))
    assert '"stream_seq":5' in encoded


# ---------------------------------------------------------------------------
# _sse_frame / _heartbeat_frame — SSE wire format
# ---------------------------------------------------------------------------


def test_sse_frame_exact_format():
    frame = _sse_frame(7, "supply_date_changed", '{"stream_seq":7}')
    assert frame == 'id: 7\nevent: supply_date_changed\ndata: {"stream_seq":7}\n\n'


def test_sse_frame_terminates_with_double_newline():
    frame = _sse_frame(1, "test_event", "{}")
    assert frame.endswith("\n\n")


def test_sse_frame_has_id_line():
    frame = _sse_frame(99, "test_event", "{}")
    lines = frame.split("\n")
    assert lines[0] == "id: 99"
    assert lines[1] == "event: test_event"
    assert lines[2] == "data: {}"


def test_heartbeat_frame_exact_format():
    assert _heartbeat_frame() == "event: ping\ndata: {}\n\n"


def test_heartbeat_frame_has_no_id_line():
    # A ping must NOT advance the client cursor → no `id:` line.
    frame = _heartbeat_frame()
    assert "id:" not in frame
    assert frame.startswith("event: ping")


def test_heartbeat_frame_terminates_with_double_newline():
    assert _heartbeat_frame().endswith("\n\n")


# ---------------------------------------------------------------------------
# _max_connections — budget config from env
# ---------------------------------------------------------------------------


def test_max_connections_default_is_32(monkeypatch):
    monkeypatch.delenv("OOTILS_STREAM_MAX_CONN", raising=False)
    assert _max_connections() == 32


def test_max_connections_custom_value(monkeypatch):
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "5")
    assert _max_connections() == 5


def test_max_connections_blank_falls_back_to_32(monkeypatch):
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "   ")
    assert _max_connections() == 32


def test_max_connections_invalid_falls_back_to_32(monkeypatch):
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "not-a-number")
    assert _max_connections() == 32


def test_max_connections_zero_falls_back_to_32(monkeypatch):
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "0")
    assert _max_connections() == 32


def test_max_connections_negative_falls_back_to_32(monkeypatch):
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "-4")
    assert _max_connections() == 32


# ---------------------------------------------------------------------------
# _stream_enabled — kill switch
# ---------------------------------------------------------------------------


def test_stream_enabled_default_on_when_absent(monkeypatch):
    monkeypatch.delenv("OOTILS_STREAM_ENABLED", raising=False)
    assert _stream_enabled() is True


@pytest.mark.parametrize("truthy", ["1", "true", "TRUE", "yes", "on", " On "])
def test_stream_enabled_truthy_values(monkeypatch, truthy):
    monkeypatch.setenv("OOTILS_STREAM_ENABLED", truthy)
    assert _stream_enabled() is True


@pytest.mark.parametrize("falsy", ["0", "false", "no", "off", "", "disabled"])
def test_stream_enabled_falsy_values(monkeypatch, falsy):
    monkeypatch.setenv("OOTILS_STREAM_ENABLED", falsy)
    assert _stream_enabled() is False


# ---------------------------------------------------------------------------
# _acquire_lease / _SlotLease.release — sync budget counter, leak-proof release
#
# #391 adversarial-review defect 1: the old async _acquire_slot/_release_slot
# pair is gone. The budget is now a plain threading.Lock-guarded counter
# (acquirable from a GC finalizer callback, which can run outside any event
# loop) handing out a _SlotLease whose release() is idempotent — because it
# is armed on TWO independent paths (the generator's own `finally`, AND a
# weakref.finalize on the generator object registered before it is ever
# handed to StreamingResponse) so an abandoned, never-iterated generator
# still frees its slot once garbage-collected.
# ---------------------------------------------------------------------------


@pytest.fixture
def reset_stream_budget(monkeypatch):
    """Isolate the module-level _active_streams counter for each budget test.

    Snapshot it to 0 before the test and restore afterwards so a leaked slot
    in one test never poisons another (and never leaks into the wider suite).
    """
    monkeypatch.setattr(stream, "_active_streams", 0)
    yield
    monkeypatch.setattr(stream, "_active_streams", 0)


def test_acquire_lease_grants_until_max_then_refuses(monkeypatch, reset_stream_budget):
    # (a) Acquisition up to the budget: a lease object each time, then None
    # once the ceiling is reached.
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "2")

    first = _acquire_lease()
    second = _acquire_lease()
    third = _acquire_lease()

    assert first is not None
    assert second is not None
    assert third is None  # over budget → refused, no lease handed out
    assert stream._active_streams == 2


def test_lease_release_allows_reacquire(monkeypatch, reset_stream_budget):
    # (b) Reuse after lease.release(): freeing a slot lets a previously
    # refused caller succeed.
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "1")

    first = _acquire_lease()
    refused = _acquire_lease()  # at capacity
    assert first is not None
    assert refused is None

    first.release()
    reacquired = _acquire_lease()  # slot freed
    assert reacquired is not None
    assert stream._active_streams == 1


def test_lease_release_is_idempotent(monkeypatch, reset_stream_budget):
    # (c) Idempotence: releasing the SAME lease twice decrements the counter
    # exactly once — the second release() is a harmless no-op guarded by the
    # lease's own `_done` flag. This is what makes the double-armed release
    # (generator `finally` + weakref.finalize) safe: whichever fires first
    # wins, the other can never double-decrement or drive the counter < 0.
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "1")

    lease = _acquire_lease()
    assert lease is not None
    assert stream._active_streams == 1

    lease.release()
    assert stream._active_streams == 0
    lease.release()  # second release on the SAME lease — must be a no-op
    assert stream._active_streams == 0  # never goes negative

    # The freed slot is usable exactly once (proves no double-free bonus slot).
    reacquired = _acquire_lease()
    assert reacquired is not None
    assert _acquire_lease() is None


def test_abandoned_never_iterated_generator_releases_lease_via_weakref_finalize(
    monkeypatch, reset_stream_budget
):
    # (d) THE central fix under test: a generator object created but NEVER
    # iterated (mirrors a client disconnecting before Starlette ever pulls the
    # first item — an async generator's frame, and therefore its own
    # `finally`, only runs once iteration has started at least once). Arming
    # weakref.finalize(gen_obj, lease.release) BEFORE the generator is handed
    # off is what reclaims the slot once the abandoned object is collected.
    # Fully synchronous: creating an async generator object and dropping its
    # last reference needs no running event loop.
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "1")

    lease = _acquire_lease()
    assert lease is not None
    assert stream._active_streams == 1

    async def _never_iterated():
        try:
            yield "unused"
        finally:
            # Mirrors _guarded_generator's own release path — never reached
            # here because the generator is never iterated even once.
            lease.release()

    gen_obj = _never_iterated()
    weakref.finalize(gen_obj, lease.release)

    # Drop the only reference and force collection — no `finally` ever ran
    # (the generator's body never started), so only the finalizer can free it.
    del gen_obj
    gc.collect()

    assert stream._active_streams == 0
    # The freed slot is acquirable again — proof the lease was actually released.
    reacquired = _acquire_lease()
    assert reacquired is not None


def test_acquire_lease_reflects_dynamic_max(monkeypatch, reset_stream_budget):
    # _acquire_lease reads _max_connections() live, so raising the ceiling
    # between acquisitions grants a previously-refused slot.
    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "1")

    first = _acquire_lease()
    refused = _acquire_lease()
    assert first is not None
    assert refused is None

    monkeypatch.setenv("OOTILS_STREAM_MAX_CONN", "2")
    assert _acquire_lease() is not None
