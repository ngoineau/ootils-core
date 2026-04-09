"""
test_dependencies.py — Unit tests for ootils_core.api.dependencies.

Covers:
  - get_db generator (success + rollback-on-exception path)
  - resolve_scenario_id: valid UUID, invalid UUID, 'baseline' keyword, missing param, header fallback
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from fastapi import HTTPException

os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api import dependencies as dep_module
from ootils_core.api.dependencies import (
    BASELINE_SCENARIO_ID,
    _get_ootils_db,
    get_db,
    resolve_scenario_id,
)


# ─────────────────────────────────────────────────────────────
# _get_ootils_db singleton
# ─────────────────────────────────────────────────────────────

def test_get_ootils_db_returns_singleton():
    """First call creates a new instance, second call returns the cached one."""
    # Reset module-level cache
    dep_module._db = None

    fake_db = MagicMock(name="OotilsDB")
    with patch("ootils_core.api.dependencies.OotilsDB", return_value=fake_db) as mk:
        db1 = _get_ootils_db()
        db2 = _get_ootils_db()

    assert db1 is fake_db
    assert db2 is fake_db
    # Only constructed once (singleton behavior)
    assert mk.call_count == 1

    # Clean up
    dep_module._db = None


def test_get_ootils_db_returns_cached_if_already_set():
    """If _db is already set, no new construction."""
    sentinel = MagicMock(name="CachedDB")
    dep_module._db = sentinel
    try:
        with patch("ootils_core.api.dependencies.OotilsDB") as mk:
            result = _get_ootils_db()
            assert result is sentinel
            mk.assert_not_called()
    finally:
        dep_module._db = None


# ─────────────────────────────────────────────────────────────
# get_db generator
# ─────────────────────────────────────────────────────────────

class _FakeDB:
    """Fake OotilsDB whose conn() context manager yields a mock connection."""
    def __init__(self, conn_mock):
        self._conn = conn_mock

    def conn(self):
        outer = self
        class _CM:
            def __enter__(self_inner):
                return outer._conn
            def __exit__(self_inner, exc_type, exc, tb):
                return False
        return _CM()


def test_get_db_yields_connection_on_success():
    """Happy path — generator yields a connection and completes cleanly."""
    fake_conn = MagicMock(name="psycopg_conn")
    fake_db = _FakeDB(fake_conn)

    with patch("ootils_core.api.dependencies._get_ootils_db", return_value=fake_db):
        gen = get_db()
        conn = next(gen)
        assert conn is fake_conn
        # Exhaust generator — should not raise
        with pytest.raises(StopIteration):
            next(gen)


def test_get_db_reraises_and_logs_on_exception():
    """If caller throws into generator, it should log + re-raise."""
    fake_conn = MagicMock(name="psycopg_conn")
    fake_db = _FakeDB(fake_conn)

    with patch("ootils_core.api.dependencies._get_ootils_db", return_value=fake_db):
        gen = get_db()
        conn = next(gen)
        assert conn is fake_conn
        # Simulate a downstream failure
        with pytest.raises(RuntimeError, match="boom"):
            gen.throw(RuntimeError("boom"))


# ─────────────────────────────────────────────────────────────
# resolve_scenario_id
# ─────────────────────────────────────────────────────────────

def test_resolve_scenario_id_missing_returns_baseline():
    """No query, no header → baseline UUID."""
    result = resolve_scenario_id(scenario_id=None, x_scenario_id=None)
    assert result == BASELINE_SCENARIO_ID


def test_resolve_scenario_id_literal_baseline_keyword():
    """'baseline' keyword → baseline UUID."""
    result = resolve_scenario_id(scenario_id="baseline", x_scenario_id=None)
    assert result == BASELINE_SCENARIO_ID


def test_resolve_scenario_id_literal_baseline_uppercase():
    """'BASELINE' (uppercase) → baseline UUID (case insensitive)."""
    result = resolve_scenario_id(scenario_id="BASELINE", x_scenario_id=None)
    assert result == BASELINE_SCENARIO_ID


def test_resolve_scenario_id_valid_uuid_via_query():
    """Valid UUID query param parses correctly."""
    raw = "11111111-2222-3333-4444-555555555555"
    result = resolve_scenario_id(scenario_id=raw, x_scenario_id=None)
    assert result == UUID(raw)


def test_resolve_scenario_id_valid_uuid_via_header():
    """When query param is missing, the X-Scenario-ID header wins."""
    raw = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    result = resolve_scenario_id(scenario_id=None, x_scenario_id=raw)
    assert result == UUID(raw)


def test_resolve_scenario_id_header_baseline_keyword():
    """Header value of 'baseline' resolves to baseline UUID."""
    result = resolve_scenario_id(scenario_id=None, x_scenario_id="baseline")
    assert result == BASELINE_SCENARIO_ID


def test_resolve_scenario_id_query_overrides_header():
    """Query param has priority over header when both are provided."""
    raw_q = "11111111-1111-1111-1111-111111111111"
    raw_h = "22222222-2222-2222-2222-222222222222"
    result = resolve_scenario_id(scenario_id=raw_q, x_scenario_id=raw_h)
    assert result == UUID(raw_q)


def test_resolve_scenario_id_invalid_uuid_raises_422():
    """Garbage value → HTTPException 422."""
    with pytest.raises(HTTPException) as excinfo:
        resolve_scenario_id(scenario_id="not-a-uuid", x_scenario_id=None)
    assert excinfo.value.status_code == 422
    assert "Invalid scenario_id" in excinfo.value.detail


def test_resolve_scenario_id_invalid_header_raises_422():
    """Garbage header → HTTPException 422."""
    with pytest.raises(HTTPException) as excinfo:
        resolve_scenario_id(scenario_id=None, x_scenario_id="garbage")
    assert excinfo.value.status_code == 422
