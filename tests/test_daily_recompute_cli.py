"""
tests/test_daily_recompute_cli.py — Pure (DB-free) contract test for the
versioned incremental-recompute CLI ``scripts/daily_recompute.py`` (chantier C3
PR2). The script REPLACES the unversioned ``~/daily_recompute.py`` artefact on
the VM: incremental by default (``process_pending``), ``--full`` for the
reconciliation recompute.

Written against the plan's contract while the script is fabricated in parallel:
it SKIPS cleanly until the script lands, then locks the neighbor-script arg /
exit-code convention shared by ``scripts/purge_maintenance.py`` and
``scripts/run_daily_ingest.py`` (both mandated by the plan as the pattern to
mirror):

  * a ``main(argv: list[str] | None = None) -> int`` entry point,
  * ``--dsn`` defaulting to ``$DATABASE_URL``, and a missing DSN ⇒ return code 2
    (the siblings' shared "missing DATABASE_URL / bad CLI args" exit code),
  * ``--full`` recognised as a flag (incremental is the default — no flag).

No DB is touched: every assertion drives ``main`` down the pre-connection
argument-validation path only.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "daily_recompute.py"


def _load_cli():
    """Load scripts/daily_recompute.py as a module, or skip if not yet present.

    The sibling CLIs do ``import mrp_core`` (the scripts/mrp_core.py shim), so
    scripts/ must be importable for the module body to load."""
    if not _SCRIPT.exists():
        pytest.skip("scripts/daily_recompute.py not present yet (C3-PR2 backend built in parallel)")
    scripts_dir = str(_SCRIPT.parent)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location("daily_recompute_cli_under_test", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exposes_neighbor_main_entry_point():
    cli = _load_cli()
    assert hasattr(cli, "main"), "daily_recompute must expose main(argv) like its sibling CLIs"
    assert callable(cli.main)


def test_missing_dsn_returns_exit_code_2(monkeypatch):
    """No $DATABASE_URL and no --dsn ⇒ exit 2 (neighbor convention: missing DSN /
    bad args), before any DB connection is attempted."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    cli = _load_cli()
    assert cli.main([]) == 2


def test_full_flag_is_recognised(monkeypatch):
    """``--full`` is the reconciliation flag (incremental is the default). With no
    DSN it reaches the same missing-DSN exit (2) — proving --full parses as a
    known flag (an UNKNOWN flag would make argparse raise SystemExit instead of
    letting main return the DSN-guard code)."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    cli = _load_cli()
    assert cli.main(["--full"]) == 2
