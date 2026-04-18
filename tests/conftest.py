# tests/conftest.py
# Exclude legacy tests directory — these target the pre-graph-architecture API
# (InventoryState, SupplyChainDecisionEngine) removed in Sprint 1.
from __future__ import annotations

import os

import pytest

collect_ignore_glob = ["legacy/*.py"]


@pytest.fixture(scope="session", autouse=True)
def _bootstrap_postgres_schema() -> None:
    """Apply SQL migrations once when DATABASE_URL is configured for test runs."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return

    from ootils_core.db.connection import OotilsDB

    OotilsDB(database_url)
