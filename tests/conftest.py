# tests/conftest.py
# Exclude legacy tests directory — these target the pre-graph-architecture API
# (InventoryState, SupplyChainDecisionEngine) removed in Sprint 1.
from __future__ import annotations

import importlib.util
import os

import pytest

collect_ignore_glob = ["legacy/*.py"]


# Register the deterministic Hypothesis profiles (moteur-c1 C1) before any
# property-based test module is collected. Loaded by absolute file path so it
# works whether or not tests/ is an import package; guarded so a lean install
# WITHOUT hypothesis still collects the rest of the suite (the property files
# self-skip on their own import in that case).
def _register_hypothesis_profiles() -> None:
    hyp_path = os.path.join(os.path.dirname(__file__), "conftest_hypothesis.py")
    spec = importlib.util.spec_from_file_location("ootils_conftest_hypothesis", hyp_path)
    if spec is None or spec.loader is None:
        return
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.register_profiles()


try:
    _register_hypothesis_profiles()
except ImportError:
    # hypothesis is a [dev] dependency; absent only in a lean runtime install.
    pass


@pytest.fixture(scope="session", autouse=True)
def _bootstrap_postgres_schema() -> None:
    """Apply SQL migrations once when DATABASE_URL is configured for test runs."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return

    from ootils_core.db.connection import OotilsDB

    OotilsDB(database_url)
