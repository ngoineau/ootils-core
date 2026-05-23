"""Tests for the OOTILS_ENGINE feature flag.

The factory `_build_propagation_engine` selects between the in-process
Python kernel and the SQL-backed engine based on an env var. These tests
exercise only the branching — full parity is covered by
`scripts/parity_sql_vs_python.py`.
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from ootils_core.api.routers.events import _build_propagation_engine
from ootils_core.engine.orchestration.propagator import PropagationEngine
from ootils_core.engine.orchestration.propagator_sql import SqlPropagationEngine


@pytest.fixture
def mock_db() -> MagicMock:
    """A bare MagicMock is fine — the factory never executes SQL during init."""
    return MagicMock()


@pytest.fixture(autouse=True)
def restore_env() -> None:
    """Ensure OOTILS_ENGINE doesn't leak between tests."""
    saved = os.environ.pop("OOTILS_ENGINE", None)
    yield
    if saved is None:
        os.environ.pop("OOTILS_ENGINE", None)
    else:
        os.environ["OOTILS_ENGINE"] = saved


def test_default_returns_python_engine(mock_db: MagicMock) -> None:
    engine = _build_propagation_engine(mock_db)
    assert type(engine) is PropagationEngine


def test_python_returns_python_engine(mock_db: MagicMock) -> None:
    os.environ["OOTILS_ENGINE"] = "python"
    engine = _build_propagation_engine(mock_db)
    assert type(engine) is PropagationEngine


def test_sql_returns_sql_engine(mock_db: MagicMock) -> None:
    os.environ["OOTILS_ENGINE"] = "sql"
    engine = _build_propagation_engine(mock_db)
    assert isinstance(engine, SqlPropagationEngine)
    # And it must still be a PropagationEngine (inherits the lifecycle).
    assert isinstance(engine, PropagationEngine)


def test_case_insensitive_and_whitespace(mock_db: MagicMock) -> None:
    os.environ["OOTILS_ENGINE"] = "  SQL  "
    engine = _build_propagation_engine(mock_db)
    assert isinstance(engine, SqlPropagationEngine)


def test_unknown_value_falls_back_to_python(mock_db: MagicMock, caplog) -> None:
    os.environ["OOTILS_ENGINE"] = "rust"
    engine = _build_propagation_engine(mock_db)
    assert type(engine) is PropagationEngine
    # Should also log a warning so the operator knows their config didn't take.
    assert any("OOTILS_ENGINE" in r.message and "rust" in r.message for r in caplog.records)
