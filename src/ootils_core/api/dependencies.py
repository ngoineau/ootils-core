"""
dependencies.py — Shared FastAPI dependencies.

Provides:
  - get_db: yields a psycopg3 Connection (commit/rollback managed)
  - get_scenario_id: resolves scenario_id from query param or X-Scenario-ID header
"""
from __future__ import annotations

import logging
from typing import Generator
from uuid import UUID

import psycopg
from fastapi import Header, HTTPException, Query, status

from ootils_core.db.connection import OotilsDB

logger = logging.getLogger(__name__)

# Module-level singleton DB handle (lazy-init safe — OotilsDB is stateless beyond DSN)
_db: OotilsDB | None = None

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


def _get_ootils_db() -> OotilsDB:
    global _db
    if _db is None:
        _db = OotilsDB()
    return _db


def get_db() -> Generator[psycopg.Connection, None, None]:
    """
    FastAPI dependency: yield a psycopg3 Connection with dict_row.
    Commits on success, rolls back on any exception.
    """
    db = _get_ootils_db()
    with db.conn() as conn:
        try:
            yield conn
        except Exception:
            logger.exception("db.error — rolling back")
            raise


def resolve_scenario_id(
    scenario_id: str | None = Query(default=None, description="Scenario UUID or 'baseline'"),
    x_scenario_id: str | None = Header(default=None, alias="X-Scenario-ID"),
) -> UUID:
    """
    Resolve scenario_id from query param or X-Scenario-ID header.
    Falls back to the baseline sentinel UUID.
    """
    raw = scenario_id or x_scenario_id
    if raw is None or raw.lower() == "baseline":
        return BASELINE_SCENARIO_ID
    try:
        return UUID(raw)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid scenario_id '{raw}' — must be a valid UUID or 'baseline'",
        )
