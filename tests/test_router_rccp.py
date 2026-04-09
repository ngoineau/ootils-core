"""
test_router_rccp.py — unit tests for src/ootils_core/api/routers/rccp.py.

Target: 100% coverage.

All DB calls are mocked via FastAPI dependency_overrides.
The mock builds `execute(...)` responses as dicts matching the real dict_row
output: {column: value}.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

# Must set token BEFORE importing the app
os.environ["OOTILS_API_TOKEN"] = "test-token"

from ootils_core.api.app import create_app
from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db
from ootils_core.api.routers.rccp import (
    _bucket_start,
    _bucket_end,
    _next_bucket_start,
    _generate_buckets,
    _count_working_days,
)


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

AUTH_HEADERS = {"Authorization": "Bearer test-token"}


def _make_execute_mock(responses: list[Any]) -> MagicMock:
    """
    Build a MagicMock for conn.execute() where each call pops the next response.
    """
    responses = list(responses)

    def execute_side_effect(*args, **kwargs):
        if not responses:
            result = MagicMock()
            result.fetchone.return_value = None
            result.fetchall.return_value = []
            result.rowcount = 0
            return result
        item = responses.pop(0)
        if isinstance(item, MagicMock):
            return item
        result = MagicMock()
        if isinstance(item, list):
            result.fetchall.return_value = item
            result.fetchone.return_value = item[0] if item else None
        elif isinstance(item, dict):
            result.fetchone.return_value = item
            result.fetchall.return_value = [item]
        elif item is None or item == "noop":
            result.fetchone.return_value = None
            result.fetchall.return_value = []
        else:
            raise TypeError(f"Unexpected response type: {type(item)}")
        result.rowcount = 1
        return result

    return MagicMock(side_effect=execute_side_effect)


def _mock_db(responses: list[Any] | None = None) -> MagicMock:
    conn = MagicMock()
    if responses is None:
        responses = []
    conn.execute = _make_execute_mock(responses)
    cursor_ctx = MagicMock()
    cursor_ctx.__enter__ = MagicMock(return_value=cursor_ctx)
    cursor_ctx.__exit__ = MagicMock(return_value=False)
    cursor_ctx.executemany = MagicMock()
    conn.cursor = MagicMock(return_value=cursor_ctx)
    return conn


def _make_client(mock_conn: MagicMock) -> TestClient:
    app = create_app()

    def override_db():
        yield mock_conn

    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[require_auth] = lambda: "test-token"
    return TestClient(app)


def _resource_row(*, location_id: UUID | None = None) -> dict:
    return {
        "resource_id": uuid4(),
        "external_id": "R1",
        "name": "Resource 1",
        "resource_type": "machine",
        "capacity_per_day": 100.0,
        "capacity_unit": "hours",
        "location_id": location_id,
    }


# ─────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────


def test_rccp_requires_auth():
    app = create_app()
    conn = _mock_db([])
    app.dependency_overrides[get_db] = lambda: conn
    with TestClient(app) as c:
        resp = c.get("/v1/rccp/R1")
    assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────
# Bucket helpers — direct unit tests
# ─────────────────────────────────────────────────────────────


def test_bucket_start_day():
    d = date(2025, 1, 15)  # Wednesday
    assert _bucket_start(d, "day") == d


def test_bucket_start_week():
    d = date(2025, 1, 15)  # Wednesday
    assert _bucket_start(d, "week") == date(2025, 1, 13)  # Monday


def test_bucket_start_month():
    d = date(2025, 1, 15)
    assert _bucket_start(d, "month") == date(2025, 1, 1)


def test_bucket_end_day():
    d = date(2025, 1, 15)
    assert _bucket_end(d, "day") == d


def test_bucket_end_week():
    monday = date(2025, 1, 13)
    assert _bucket_end(monday, "week") == date(2025, 1, 19)


def test_bucket_end_month_january():
    assert _bucket_end(date(2025, 1, 1), "month") == date(2025, 1, 31)


def test_bucket_end_month_december():
    """December branch: start.month == 12 → start.replace(day=31)."""
    assert _bucket_end(date(2025, 12, 1), "month") == date(2025, 12, 31)


def test_bucket_end_month_february():
    assert _bucket_end(date(2025, 2, 1), "month") == date(2025, 2, 28)


def test_next_bucket_start_day():
    assert _next_bucket_start(date(2025, 1, 15), "day") == date(2025, 1, 16)


def test_next_bucket_start_week():
    monday = date(2025, 1, 13)
    assert _next_bucket_start(monday, "week") == date(2025, 1, 20)


def test_next_bucket_start_month():
    assert _next_bucket_start(date(2025, 1, 1), "month") == date(2025, 2, 1)


def test_next_bucket_start_month_december_year_rollover():
    """December branch: start.month == 12 → year+1, month=1."""
    assert _next_bucket_start(date(2025, 12, 1), "month") == date(2026, 1, 1)


def test_generate_buckets_single_day():
    buckets = _generate_buckets(date(2025, 1, 13), date(2025, 1, 13), "day")
    assert buckets == [(date(2025, 1, 13), date(2025, 1, 13))]


def test_generate_buckets_week_truncated_at_to_date():
    """end > to_date branch: week extends past to_date and gets clipped."""
    # Start mid-week, run for 3 days only
    buckets = _generate_buckets(date(2025, 1, 15), date(2025, 1, 17), "week")
    # Monday of week is 2025-01-13, end normally 2025-01-19, clipped to 2025-01-17
    assert buckets[0] == (date(2025, 1, 13), date(2025, 1, 17))


def test_generate_buckets_multiple_weeks():
    buckets = _generate_buckets(date(2025, 1, 13), date(2025, 1, 26), "week")
    assert len(buckets) == 2
    assert buckets[0] == (date(2025, 1, 13), date(2025, 1, 19))
    assert buckets[1] == (date(2025, 1, 20), date(2025, 1, 26))


# ─────────────────────────────────────────────────────────────
# _count_working_days direct tests
# ─────────────────────────────────────────────────────────────


def test_count_working_days_no_location():
    """Pure Mon–Fri heuristic, no DB query."""
    conn = _mock_db([])
    # 2025-01-13 (Mon) … 2025-01-19 (Sun) → 5 working days
    assert _count_working_days(conn, None, date(2025, 1, 13), date(2025, 1, 19)) == 5


def test_count_working_days_with_location_calendar_hit():
    """Calendar query returns count > 0 → use it."""
    conn = _mock_db([
        {"cnt": 4},  # COUNT query
    ])
    result = _count_working_days(conn, str(uuid4()), date(2025, 1, 13), date(2025, 1, 19))
    assert result == 4


def test_count_working_days_with_location_calendar_zero_falls_back():
    """Calendar count == 0 → fall through to Mon–Fri."""
    conn = _mock_db([
        {"cnt": 0},
    ])
    result = _count_working_days(conn, str(uuid4()), date(2025, 1, 13), date(2025, 1, 19))
    assert result == 5  # Mon..Fri


def test_count_working_days_with_location_calendar_none_falls_back():
    """Calendar query returns None → fall through to Mon–Fri."""
    conn = _mock_db([
        None,
    ])
    result = _count_working_days(conn, str(uuid4()), date(2025, 1, 13), date(2025, 1, 19))
    assert result == 5


# ─────────────────────────────────────────────────────────────
# get_rccp endpoint
# ─────────────────────────────────────────────────────────────


def test_rccp_invalid_grain():
    conn = _mock_db([])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={"grain": "fortnight"},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
    assert "grain" in resp.json()["detail"]


def test_rccp_to_date_before_from_date():
    conn = _mock_db([])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={
            "from_date": "2025-02-01",
            "to_date": "2025-01-01",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422
    assert "to_date" in resp.json()["detail"]


def test_rccp_resource_not_found():
    conn = _mock_db([
        None,  # resource lookup
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/MISSING",
        params={
            "from_date": "2025-01-13",
            "to_date": "2025-01-19",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 404


def test_rccp_default_dates_invalid_grain_path_not_taken():
    """Use default from_date / to_date (None → today / +12w)."""
    conn = _mock_db([
        _resource_row(),  # resource
        [],               # load rows
        [],               # capacity overrides
        # default range = 12 weeks ≈ 84 days, mostly weekdays.
        # No location_id → no calendar queries.
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={"grain": "month"},
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["resource"]["external_id"] == "R1"
    assert len(body["buckets"]) >= 1


def test_rccp_no_location_with_load_and_overload():
    """One-week bucket, no location. Load > capacity → overloaded."""
    rrow = _resource_row()  # capacity_per_day=100, no location_id
    # Mon 2025-01-13 to Fri 2025-01-17 → 5 weekdays × 100 = 500 capacity
    # Load = 700 → utilization 140% → overloaded
    conn = _mock_db([
        rrow,
        # load rows
        [
            {"time_ref": date(2025, 1, 13), "quantity": 300.0},
            {"time_ref": date(2025, 1, 15), "quantity": 400.0},
            {"time_ref": None, "quantity": 999.0},  # ignored (None)
        ],
        # capacity overrides
        [],
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={
            "from_date": "2025-01-13",
            "to_date": "2025-01-17",
            "grain": "week",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["buckets"]) == 1
    b = body["buckets"][0]
    assert b["load"] == 700.0
    assert b["capacity"] == 500.0
    assert b["utilization_pct"] == 140.0
    assert b["overloaded"] is True


def test_rccp_capacity_override_path():
    """Override day uses override capacity instead of calendar/heuristic."""
    rrow = _resource_row()
    # Mon..Tue range — Mon overridden to 50, Tue normal weekday → 100. Total 150.
    conn = _mock_db([
        rrow,
        # load
        [{"time_ref": date(2025, 1, 13), "quantity": 50.0}],
        # overrides
        [{"override_date": date(2025, 1, 13), "capacity": 50.0}],
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={
            "from_date": "2025-01-13",  # Mon
            "to_date": "2025-01-14",    # Tue
            "grain": "day",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["buckets"]) == 2
    # Monday: load=50, capacity=50 (override) → 100%
    assert body["buckets"][0]["capacity"] == 50.0
    assert body["buckets"][0]["load"] == 50.0
    assert body["buckets"][0]["utilization_pct"] == 100.0
    assert body["buckets"][0]["overloaded"] is False
    # Tuesday: load=0, capacity=100 (weekday)
    assert body["buckets"][1]["capacity"] == 100.0


def test_rccp_with_location_calendar_working_day_with_factor():
    """location_id present → operational_calendars query.
    cal_row with is_working_day=True and capacity_factor=0.5 →
    capacity_per_day * 0.5 added.
    """
    loc_id = uuid4()
    rrow = _resource_row(location_id=loc_id)
    # 1-day range, Monday. With cal factor 0.5 → capacity 50.
    conn = _mock_db([
        rrow,
        # load
        [],
        # overrides
        [],
        # calendar query for Monday
        {"is_working_day": True, "capacity_factor": 0.5},
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={
            "from_date": "2025-01-13",
            "to_date": "2025-01-13",
            "grain": "day",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["buckets"][0]["capacity"] == 50.0
    assert body["buckets"][0]["load"] == 0.0
    assert body["buckets"][0]["utilization_pct"] == 0.0


def test_rccp_with_location_calendar_working_day_factor_none():
    """capacity_factor=None → defaults to 1.0."""
    loc_id = uuid4()
    rrow = _resource_row(location_id=loc_id)
    conn = _mock_db([
        rrow,
        [],
        [],
        {"is_working_day": True, "capacity_factor": None},
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={
            "from_date": "2025-01-13",
            "to_date": "2025-01-13",
            "grain": "day",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["buckets"][0]["capacity"] == 100.0


def test_rccp_with_location_calendar_non_working_day():
    """cal_row.is_working_day=False → no capacity added."""
    loc_id = uuid4()
    rrow = _resource_row(location_id=loc_id)
    conn = _mock_db([
        rrow,
        [],
        [],
        {"is_working_day": False, "capacity_factor": 1.0},
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={
            "from_date": "2025-01-13",
            "to_date": "2025-01-13",
            "grain": "day",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["buckets"][0]["capacity"] == 0.0
    assert body["buckets"][0]["utilization_pct"] == 0.0


def test_rccp_with_location_calendar_no_entry_falls_back():
    """cal_row is None → fall back to Mon–Fri heuristic."""
    loc_id = uuid4()
    rrow = _resource_row(location_id=loc_id)
    conn = _mock_db([
        rrow,
        [],
        [],
        None,  # calendar query → no entry for Monday
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={
            "from_date": "2025-01-13",
            "to_date": "2025-01-13",
            "grain": "day",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["buckets"][0]["capacity"] == 100.0


def test_rccp_weekend_no_capacity_no_query():
    """Saturday → not weekday → no calendar query → 0 capacity."""
    loc_id = uuid4()
    rrow = _resource_row(location_id=loc_id)
    # 2025-01-18 is a Saturday
    conn = _mock_db([
        rrow,
        [],
        [],
        # NO calendar query expected
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={
            "from_date": "2025-01-18",
            "to_date": "2025-01-18",
            "grain": "day",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["buckets"][0]["capacity"] == 0.0
    # Zero capacity → utilization_pct = 0.0 (the safe-divide branch)
    assert body["buckets"][0]["utilization_pct"] == 0.0
    assert body["buckets"][0]["overloaded"] is False


def test_rccp_resource_with_location_id_serialized():
    """Make sure location_id appears in the response when set."""
    loc_id = uuid4()
    rrow = _resource_row(location_id=loc_id)
    conn = _mock_db([
        rrow,
        [],
        [],
        # 1 weekday (Monday) → 1 calendar query
        {"is_working_day": True, "capacity_factor": 1.0},
    ])
    client = _make_client(conn)
    resp = client.get(
        "/v1/rccp/R1",
        params={
            "from_date": "2025-01-13",
            "to_date": "2025-01-13",
            "grain": "day",
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["resource"]["location_id"] == str(loc_id)
