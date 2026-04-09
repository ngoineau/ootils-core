"""
Comprehensive tests for ootils_core.engine.kernel.calc.calendar

Covers:
- add_working_days (async): n<=0, normal, non-working days, max_iter exhausted
- add_working_days_sync: n<=0, normal, non-working days, max_iter exhausted
- UUID location_id converted to str
- All non-working days in window (max_iter error)
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, call
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.calc.calendar import (
    add_working_days,
    add_working_days_sync,
)


# ---------------------------------------------------------------------------
# Sync version tests: add_working_days_sync
# ---------------------------------------------------------------------------

class TestAddWorkingDaysSync:
    def _make_conn(self, non_working_dates: list[date]) -> MagicMock:
        """Build a mock psycopg3 connection returning the given non-working dates."""
        rows = [{"calendar_date": d} for d in non_working_dates]
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        conn = MagicMock()
        conn.execute.return_value = cursor
        return conn

    def test_n_zero_returns_start_date(self):
        conn = self._make_conn([])
        result = add_working_days_sync(conn, uuid4(), date(2025, 1, 1), 0)
        assert result == date(2025, 1, 1)
        conn.execute.assert_not_called()

    def test_n_negative_returns_start_date(self):
        conn = self._make_conn([])
        result = add_working_days_sync(conn, uuid4(), date(2025, 1, 1), -5)
        assert result == date(2025, 1, 1)
        conn.execute.assert_not_called()

    def test_all_working_days(self):
        """No non-working days in calendar → advance n calendar days."""
        conn = self._make_conn([])
        result = add_working_days_sync(conn, uuid4(), date(2025, 1, 1), 3)
        assert result == date(2025, 1, 4)  # Jan 2, 3, 4

    def test_skip_non_working_days(self):
        """Jan 2 and Jan 3 are non-working → skip them."""
        non_working = [date(2025, 1, 2), date(2025, 1, 3)]
        conn = self._make_conn(non_working)
        result = add_working_days_sync(conn, uuid4(), date(2025, 1, 1), 2)
        # Jan 2 skip, Jan 3 skip, Jan 4 (day 1), Jan 5 (day 2)
        assert result == date(2025, 1, 5)

    def test_single_working_day(self):
        conn = self._make_conn([])
        result = add_working_days_sync(conn, uuid4(), date(2025, 6, 1), 1)
        assert result == date(2025, 6, 2)

    def test_max_iter_exhausted_raises(self):
        """All days in window are non-working → ValueError."""
        start = date(2025, 1, 1)
        max_iter = 5
        # Make all 5 days non-working
        non_working = [start + timedelta(days=i + 1) for i in range(max_iter)]
        conn = self._make_conn(non_working)

        with pytest.raises(ValueError, match="max_iter exhausted"):
            add_working_days_sync(conn, uuid4(), start, 1, max_iter=max_iter)

    def test_uuid_location_id_converted_to_string(self):
        location_id = UUID("12345678-1234-5678-1234-567812345678")
        conn = self._make_conn([])
        add_working_days_sync(conn, location_id, date(2025, 1, 1), 1)

        call_args = conn.execute.call_args
        params = call_args[0][1]
        assert params[0] == str(location_id)

    def test_string_location_id_passed_through(self):
        location_id = "some-location-string"
        conn = self._make_conn([])
        add_working_days_sync(conn, location_id, date(2025, 1, 1), 1)

        call_args = conn.execute.call_args
        params = call_args[0][1]
        assert params[0] == "some-location-string"

    def test_consecutive_non_working_then_working(self):
        """3 non-working days followed by working days, n=3."""
        start = date(2025, 3, 1)
        non_working = [date(2025, 3, 2), date(2025, 3, 3), date(2025, 3, 4)]
        conn = self._make_conn(non_working)
        result = add_working_days_sync(conn, uuid4(), start, 3)
        # Mar 2 skip, Mar 3 skip, Mar 4 skip, Mar 5 (1), Mar 6 (2), Mar 7 (3)
        assert result == date(2025, 3, 7)

    def test_custom_max_iter_sufficient(self):
        conn = self._make_conn([])
        result = add_working_days_sync(conn, uuid4(), date(2025, 1, 1), 2, max_iter=10)
        assert result == date(2025, 1, 3)

    def test_max_iter_exactly_enough(self):
        """max_iter is exactly the number of iterations needed."""
        # n=1, no non-working days, max_iter=1 → exactly 1 iteration
        conn = self._make_conn([])
        result = add_working_days_sync(conn, uuid4(), date(2025, 1, 1), 1, max_iter=1)
        assert result == date(2025, 1, 2)

    def test_window_dates_computed_correctly(self):
        """Verify the SQL query window parameters."""
        start = date(2025, 6, 15)
        conn = self._make_conn([])
        add_working_days_sync(conn, uuid4(), start, 1, max_iter=100)

        call_args = conn.execute.call_args
        params = call_args[0][1]
        assert params[1] == date(2025, 6, 16)   # window_start = start + 1 day
        assert params[2] == date(2025, 9, 23)   # window_end = start + 100 days


# ---------------------------------------------------------------------------
# Async version tests: add_working_days
# ---------------------------------------------------------------------------

def _make_async_conn(non_working_dates: list[date]) -> AsyncMock:
    """Build a mock asyncpg connection returning the given non-working dates."""
    rows = [{"calendar_date": d} for d in non_working_dates]
    conn = AsyncMock()
    conn.fetch.return_value = rows
    return conn


class TestAddWorkingDaysAsync:
    def test_n_zero_returns_start_date(self):
        conn = _make_async_conn([])
        result = asyncio.run(add_working_days(conn, uuid4(), date(2025, 1, 1), 0))
        assert result == date(2025, 1, 1)
        conn.fetch.assert_not_called()

    def test_n_negative_returns_start_date(self):
        conn = _make_async_conn([])
        result = asyncio.run(add_working_days(conn, uuid4(), date(2025, 1, 1), -3))
        assert result == date(2025, 1, 1)
        conn.fetch.assert_not_called()

    def test_all_working_days(self):
        conn = _make_async_conn([])
        result = asyncio.run(add_working_days(conn, uuid4(), date(2025, 1, 1), 3))
        assert result == date(2025, 1, 4)

    def test_skip_non_working_days(self):
        non_working = [date(2025, 1, 2), date(2025, 1, 3)]
        conn = _make_async_conn(non_working)
        result = asyncio.run(add_working_days(conn, uuid4(), date(2025, 1, 1), 2))
        assert result == date(2025, 1, 5)

    def test_max_iter_exhausted_raises(self):
        start = date(2025, 1, 1)
        max_iter = 5
        non_working = [start + timedelta(days=i + 1) for i in range(max_iter)]
        conn = _make_async_conn(non_working)

        with pytest.raises(ValueError, match="max_iter exhausted"):
            asyncio.run(add_working_days(conn, uuid4(), start, 1, max_iter=max_iter))

    def test_uuid_location_id_converted_to_string(self):
        location_id = UUID("12345678-1234-5678-1234-567812345678")
        conn = _make_async_conn([])
        asyncio.run(add_working_days(conn, location_id, date(2025, 1, 1), 1))

        call_args = conn.fetch.call_args
        # asyncpg uses positional params
        assert call_args[0][1] == str(location_id)

    def test_single_working_day(self):
        conn = _make_async_conn([])
        result = asyncio.run(add_working_days(conn, uuid4(), date(2025, 6, 1), 1))
        assert result == date(2025, 6, 2)

    def test_custom_max_iter(self):
        conn = _make_async_conn([])
        result = asyncio.run(add_working_days(conn, uuid4(), date(2025, 1, 1), 2, max_iter=10))
        assert result == date(2025, 1, 3)

    def test_consecutive_non_working_then_working(self):
        start = date(2025, 3, 1)
        non_working = [date(2025, 3, 2), date(2025, 3, 3), date(2025, 3, 4)]
        conn = _make_async_conn(non_working)
        result = asyncio.run(add_working_days(conn, uuid4(), start, 3))
        assert result == date(2025, 3, 7)
