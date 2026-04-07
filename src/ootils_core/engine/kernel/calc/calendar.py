"""
calendar.py — Working-days arithmetic using operational_calendars.

ADR-009 convention: **absence = working day** (safe-by-default).

Public API
----------
- add_working_days(conn, location_id, start_date, n, max_iter) → date   [async, asyncpg]
- add_working_days_sync(conn, location_id, start_date, n, max_iter) → date [sync, psycopg3]
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Union
from uuid import UUID


# ─────────────────────────────────────────────────────────────
# Async version (asyncpg)
# ─────────────────────────────────────────────────────────────

async def add_working_days(
    conn,                       # asyncpg connection
    location_id: Union[str, UUID],
    start_date: date,
    n: int,
    max_iter: int = 365,
) -> date:
    """
    Return start_date + n working days, using operational_calendars.

    Convention (ADR-009): a date absent from operational_calendars is
    considered a working day (safe-by-default — avoids silent delays).

    Parameters
    ----------
    conn         asyncpg connection (async)
    location_id  UUID of the location
    start_date   Inclusive start (day 0, not counted)
    n            Number of working days to advance (must be >= 0)
    max_iter     Safety cap on calendar iterations (default 365)

    Returns
    -------
    date — first date after start_date that is the n-th working day.
    """
    if n <= 0:
        return start_date

    location_id_str = str(location_id)

    # Fetch the non-working days window [start_date+1 … start_date+max_iter]
    window_start = start_date + timedelta(days=1)
    window_end = start_date + timedelta(days=max_iter)

    rows = await conn.fetch(
        """
        SELECT calendar_date
        FROM operational_calendars
        WHERE location_id = $1
          AND calendar_date BETWEEN $2 AND $3
          AND is_working_day = FALSE
        """,
        location_id_str, window_start, window_end,
    )
    non_working: set[date] = {row["calendar_date"] for row in rows}

    current = start_date
    days_counted = 0
    iterations = 0

    while days_counted < n and iterations < max_iter:
        current += timedelta(days=1)
        iterations += 1
        if current not in non_working:
            days_counted += 1

    return current


# ─────────────────────────────────────────────────────────────
# Sync version (psycopg3) — used by the engine (sync DB layer)
# ─────────────────────────────────────────────────────────────

def add_working_days_sync(
    conn,                       # psycopg3 connection
    location_id: Union[str, UUID],
    start_date: date,
    n: int,
    max_iter: int = 365,
) -> date:
    """
    Return start_date + n working days, using operational_calendars.

    Convention (ADR-009): a date absent from operational_calendars is
    considered a working day (safe-by-default).

    Parameters
    ----------
    conn         psycopg3 connection (sync)
    location_id  UUID of the location
    start_date   Inclusive start (day 0, not counted)
    n            Number of working days to advance (must be >= 0)
    max_iter     Safety cap on calendar iterations (default 365)

    Returns
    -------
    date — first date after start_date that is the n-th working day.
    """
    if n <= 0:
        return start_date

    location_id_str = str(location_id)

    window_start = start_date + timedelta(days=1)
    window_end = start_date + timedelta(days=max_iter)

    rows = conn.execute(
        """
        SELECT calendar_date
        FROM operational_calendars
        WHERE location_id = %s
          AND calendar_date BETWEEN %s AND %s
          AND is_working_day = FALSE
        """,
        (location_id_str, window_start, window_end),
    ).fetchall()

    non_working: set[date] = {row["calendar_date"] for row in rows}

    current = start_date
    days_counted = 0
    iterations = 0

    while days_counted < n and iterations < max_iter:
        current += timedelta(days=1)
        iterations += 1
        if current not in non_working:
            days_counted += 1

    return current
