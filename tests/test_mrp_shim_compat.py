"""
Guardrail for ADR-020 PAS 3: the canonical MRP math lives in the packaged
`ootils_core.engine.mrp.core` (+ `.loader` for the DB layer). `scripts/mrp_core.py`
is a thin re-export shim kept so the ~21 CLIs / watcher agents that do
`import mrp_core as core` keep working.

This test pins that the shim re-exports the *same objects* as the package, so the
two can never silently drift (which is exactly how the original two-engine
divergence grew unnoticed — see docs/ADR-020).
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import mrp_core as shim  # noqa: E402

from ootils_core.engine.mrp import core, loader  # noqa: E402

# Every symbol a consumer reaches through `import mrp_core as core`.
CORE_SYMBOLS = [
    "BASELINE", "DEFAULT_LT_DAYS", "SUPPLY_TYPES", "FIRM_RECEIPT_TYPES", "DEMAND_TYPES",
    "lot_size", "cost_of", "_spread_period", "apply_lot_rule", "PlanningData",
    "consume_demand", "run_timephased", "first_shortage", "excess_obsolete", "peg_origins",
]
LOADER_SYMBOLS = ["guard_db", "_m", "load_planning_data"]


@pytest.mark.parametrize("name", CORE_SYMBOLS)
def test_shim_reexports_core_symbol_identically(name):
    assert hasattr(shim, name), f"shim dropped {name} — a consumer would break"
    assert getattr(shim, name) is getattr(core, name), (
        f"shim.{name} is not the packaged core.{name} — they have drifted"
    )


@pytest.mark.parametrize("name", LOADER_SYMBOLS)
def test_shim_reexports_loader_symbol_identically(name):
    assert hasattr(shim, name), f"shim dropped {name} — a consumer would break"
    assert getattr(shim, name) is getattr(loader, name), (
        f"shim.{name} is not the packaged loader.{name} — they have drifted"
    )


def test_core_is_db_free():
    """The canonical math core must not import psycopg / a DB driver — its purity
    is what makes it deterministic and reusable across the DRP and MRP echelons."""
    import inspect

    src = inspect.getsource(core)
    assert "psycopg" not in src, "core.py must stay DB-free (DB belongs in loader.py)"
