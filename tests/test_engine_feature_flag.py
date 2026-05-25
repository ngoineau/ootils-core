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


def test_default_returns_sql_engine(mock_db: MagicMock) -> None:
    """Default since 2026-05-24 is SQL. M3 explanations are regenerated
    lazily by GET /v1/explain — no explanation_builder wired in the
    propagation factory (would cost ~5s per 1k shortages, eager-mode)."""
    engine = _build_propagation_engine(mock_db)
    assert isinstance(engine, SqlPropagationEngine)
    assert engine._explanation_builder is None  # lazy strategy


def test_python_returns_python_engine(mock_db: MagicMock) -> None:
    os.environ["OOTILS_ENGINE"] = "python"
    engine = _build_propagation_engine(mock_db)
    assert type(engine) is PropagationEngine
    assert engine._explanation_builder is None  # lazy strategy


def test_sql_returns_sql_engine(mock_db: MagicMock) -> None:
    os.environ["OOTILS_ENGINE"] = "sql"
    engine = _build_propagation_engine(mock_db)
    assert isinstance(engine, SqlPropagationEngine)
    assert isinstance(engine, PropagationEngine)
    assert engine._explanation_builder is None  # lazy strategy


def test_case_insensitive_and_whitespace(mock_db: MagicMock) -> None:
    os.environ["OOTILS_ENGINE"] = "  SQL  "
    engine = _build_propagation_engine(mock_db)
    assert isinstance(engine, SqlPropagationEngine)


def test_rust_returns_rust_engine_when_extension_available(mock_db: MagicMock) -> None:
    """OOTILS_ENGINE=rust dispatches to RustPropagationEngine if the
    `ootils_kernel` extension is importable. Skipped on CI cells that
    didn't build the wheel."""
    pytest.importorskip(
        "ootils_kernel",
        reason="Rust kernel extension not installed in this environment",
    )
    from ootils_core.engine.orchestration.propagator_rust import RustPropagationEngine

    os.environ["OOTILS_ENGINE"] = "rust"
    engine = _build_propagation_engine(mock_db)
    assert isinstance(engine, RustPropagationEngine)
    assert isinstance(engine, PropagationEngine)  # hybrid dispatch wrapper
    assert engine._explanation_builder is None  # lazy strategy


def test_unknown_value_falls_back_to_sql(mock_db: MagicMock, caplog) -> None:
    """Unknown value logs a warning and falls back to the default (sql)."""
    os.environ["OOTILS_ENGINE"] = "rustacean"  # not a real backend
    engine = _build_propagation_engine(mock_db)
    assert isinstance(engine, SqlPropagationEngine)
    assert any(
        "OOTILS_ENGINE" in r.message and "rustacean" in r.message for r in caplog.records
    )
