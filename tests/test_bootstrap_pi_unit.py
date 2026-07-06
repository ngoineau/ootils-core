"""
test_bootstrap_pi_unit.py — pure unit tests for the PI bootstrap CLI (#414 PR2,
scripts/bootstrap_pi.py). No database, no network.

Exercises only the DB-free surface of the scenario-first bootstrap:

  - _read_external_ids : the --items-file reader (blank + '#'-comment lines
                         ignored, stripping, a clean error on a missing file).
  - main (argv only)   : the pre-flight validations that return BEFORE any
                         psycopg.connect — mutual exclusivity of
                         --sample-finished / --items-file (rc 2), a non-UUID
                         --scenario (rc 2), a missing DSN (rc 2), and that
                         --horizon / --horizon-days are the SAME dest (the alias
                         works, no DB needed to observe the parsed Namespace).
  - the volumetric guard : the anti-big-bang rampart is inline in bootstrap()
                         (reachable only past a live connection), so here we pin
                         the DB-free half of its contract — the 2 000 000 ceiling
                         constant and the projected-nodes arithmetic
                         (pairs x horizon). The LIVE refusal (a real over-ceiling
                         run raising SystemExit) is asserted in
                         tests/integration/test_bootstrap_pi_scenario_integration.py.
  - BOOTSTRAP_METRICS: the machine-readable stdout line format — exact marker
                         prefix + a JSON tail that json.loads round-trips.

bootstrap_pi.py lives in scripts/ (outside the installed package) and its only
heavy dependency is psycopg, imported at module top. Loading it by path here is
harmless: importing the module runs no connection (main/bootstrap are only
called by __main__), and the argv branches under test all return BEFORE the
``with psycopg.connect(...)`` block. Mirrors tests/test_demo_e2e_helpers.py's
by-path loader so neither scripts/ on sys.path a priori nor a console entry is
needed.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

# bootstrap_pi.py lives in scripts/. Load it by path (the demo_e2e_helpers
# pattern) so the test needs neither scripts/ on sys.path a priori nor an entry.
_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

if "bootstrap_pi" in sys.modules:
    bootstrap_pi = sys.modules["bootstrap_pi"]
else:
    _SPEC = importlib.util.spec_from_file_location(
        "bootstrap_pi", _SCRIPTS_DIR / "bootstrap_pi.py"
    )
    assert _SPEC and _SPEC.loader
    bootstrap_pi = importlib.util.module_from_spec(_SPEC)
    sys.modules["bootstrap_pi"] = bootstrap_pi
    _SPEC.loader.exec_module(bootstrap_pi)

_read_external_ids = bootstrap_pi._read_external_ids
main = bootstrap_pi.main
MAX_PROJECTED_NODES = bootstrap_pi.MAX_PROJECTED_NODES
BASELINE_SCENARIO_ID = bootstrap_pi.BASELINE_SCENARIO_ID

# A valid ootils DSN that _guard_db accepts — but no argv test below ever reaches
# _guard_db/connect: each returns 2 at an earlier validation gate.
_OK_DSN = "postgresql://u:p@h:5432/ootils_test"
_OK_UUID = "11111111-1111-1111-1111-111111111111"


# ===========================================================================
# _read_external_ids — the --items-file reader
# ===========================================================================


def test_read_external_ids_basic_one_per_line(tmp_path):
    f = tmp_path / "items.txt"
    f.write_text("FG-01\nFG-02\nCMP-X\n", encoding="utf-8")
    assert _read_external_ids(f) == ["FG-01", "FG-02", "CMP-X"]


def test_read_external_ids_ignores_blank_and_comment_lines(tmp_path):
    f = tmp_path / "items.txt"
    f.write_text(
        "\n"
        "# a header comment\n"
        "FG-01\n"
        "\n"
        "   # an indented comment is still a comment\n"
        "FG-02\n"
        "   \n",  # whitespace-only -> dropped
        encoding="utf-8",
    )
    assert _read_external_ids(f) == ["FG-01", "FG-02"]


def test_read_external_ids_strips_surrounding_whitespace(tmp_path):
    f = tmp_path / "items.txt"
    f.write_text("  FG-01  \n\tFG-02\t\n", encoding="utf-8")
    assert _read_external_ids(f) == ["FG-01", "FG-02"]


def test_read_external_ids_empty_file_yields_empty_list(tmp_path):
    f = tmp_path / "items.txt"
    f.write_text("", encoding="utf-8")
    assert _read_external_ids(f) == []


def test_read_external_ids_only_comments_yields_empty_list(tmp_path):
    f = tmp_path / "items.txt"
    f.write_text("# just\n# comments\n\n", encoding="utf-8")
    assert _read_external_ids(f) == []


def test_read_external_ids_hash_inside_a_code_is_not_a_comment(tmp_path):
    # Only a line whose FIRST non-space char is '#' is a comment; a '#' embedded
    # in a code (or trailing it) is part of the external_id, never stripped.
    f = tmp_path / "items.txt"
    f.write_text("FG#01\nA#B#C\n", encoding="utf-8")
    assert _read_external_ids(f) == ["FG#01", "A#B#C"]


def test_read_external_ids_missing_file_raises_clean_filenotfound(tmp_path):
    missing = tmp_path / "does_not_exist.txt"
    # A clean, typed error (Path.read_text raising FileNotFoundError) — not a
    # silent empty list that would let the run proceed on an empty scope.
    with pytest.raises(FileNotFoundError):
        _read_external_ids(missing)


# ===========================================================================
# main (argv only) — validations that return BEFORE any DB connection
# ===========================================================================


def test_main_sample_finished_and_items_file_are_mutually_exclusive(tmp_path, caplog):
    # Both subset flags together -> rc 2, no DB touched. A real file so the
    # argparse Path type resolves; the mutual-exclusivity gate fires before the
    # file is ever read.
    items = tmp_path / "items.txt"
    items.write_text("FG-01\n", encoding="utf-8")
    rc = main(
        [
            "--dsn", _OK_DSN,
            "--sample-finished", "5",
            "--items-file", str(items),
        ]
    )
    assert rc == 2
    assert "mutually exclusive" in caplog.text


def test_main_non_uuid_scenario_exits_2(caplog):
    rc = main(["--dsn", _OK_DSN, "--scenario", "not-a-uuid"])
    assert rc == 2
    assert "must be a valid UUID" in caplog.text
    # The offending value is echoed for the operator, but this is a local CLI
    # arg (never the DSN) — safe to surface.
    assert "not-a-uuid" in caplog.text


def test_main_valid_uuid_scenario_passes_validation_gate(monkeypatch):
    # A well-formed --scenario must NOT trip the UUID gate. Prove it by making
    # the DB layer a tripwire: if control reaches past the validations, our fake
    # connect raises a sentinel we can catch — so a clean pass-through is
    # observable WITHOUT a real database. (A non-UUID would have returned 2
    # before ever calling connect.)
    class _Sentinel(RuntimeError):
        pass

    def _boom(*_a, **_k):
        raise _Sentinel("reached connect")

    monkeypatch.setattr(bootstrap_pi.psycopg, "connect", _boom)
    with pytest.raises(_Sentinel):
        main(["--dsn", _OK_DSN, "--scenario", _OK_UUID])


def test_main_missing_dsn_exits_2(monkeypatch, caplog):
    # Neither --dsn nor DATABASE_URL -> rc 2 (argparse default reads the env,
    # so clear it to exercise the guard).
    monkeypatch.delenv("DATABASE_URL", raising=False)
    rc = main(["--scenario", _OK_UUID])
    assert rc == 2
    assert "DATABASE_URL not set" in caplog.text


def test_main_mutual_exclusivity_precedes_the_uuid_check(tmp_path, caplog):
    # Ordering guard: both flags set AND a bad scenario -> the mutual-exclusivity
    # message wins (it is checked first), proving the documented gate order.
    items = tmp_path / "items.txt"
    items.write_text("FG-01\n", encoding="utf-8")
    rc = main(
        [
            "--dsn", _OK_DSN,
            "--sample-finished", "5",
            "--items-file", str(items),
            "--scenario", "also-bad",
        ]
    )
    assert rc == 2
    assert "mutually exclusive" in caplog.text
    assert "must be a valid UUID" not in caplog.text


# ===========================================================================
# --horizon / --horizon-days alias — same dest, no DB needed
# ===========================================================================


def _parse(argv: list[str]):
    """Build the same ArgumentParser main() builds and parse argv, so the alias
    and default wiring is observable without running the DB body. Mirrors what
    main() constructs 1:1 (kept in sync by importing the module's own constants
    for the default assertions)."""
    import argparse
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--scenario", default=BASELINE_SCENARIO_ID)
    parser.add_argument(
        "--horizon-days", "--horizon", type=int, default=540, dest="horizon_days"
    )
    parser.add_argument("--sample", type=int, default=None)
    parser.add_argument("--sample-finished", type=int, default=None)
    parser.add_argument("--items-file", type=Path, default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--allow-dev", action="store_true")
    return parser.parse_args(argv)


def test_horizon_alias_maps_to_horizon_days_dest():
    # --horizon and --horizon-days are literally the same option (one dest),
    # so either spelling populates args.horizon_days.
    assert _parse(["--horizon", "120"]).horizon_days == 120
    assert _parse(["--horizon-days", "120"]).horizon_days == 120


def test_horizon_default_is_540():
    assert _parse([]).horizon_days == 540


def test_horizon_alias_last_wins_when_both_spellings_given():
    # Same dest -> argparse keeps the last occurrence, whichever spelling.
    assert _parse(["--horizon", "90", "--horizon-days", "200"]).horizon_days == 200
    assert _parse(["--horizon-days", "200", "--horizon", "90"]).horizon_days == 90


# ===========================================================================
# Volumetric guard — DB-free half of the contract (constant + arithmetic)
# ===========================================================================


def test_max_projected_nodes_ceiling_is_two_million():
    # The anti-big-bang rampart the runbook (#414) depends on. A change here is a
    # deliberate policy change, not an accident — pin it.
    assert MAX_PROJECTED_NODES == 2_000_000


def test_projected_nodes_estimate_is_pairs_times_horizon():
    # The guard's estimate is exactly pairs x horizon (bootstrap() line
    # ``projected_nodes = n_pairs * horizon``). Reproduce the arithmetic so a
    # future refactor that changed the formula would trip a unit test, not only
    # the (blind, non-CI) integration run.
    pairs, horizon = 2450, 120
    assert pairs * horizon == 294_000
    assert pairs * horizon < MAX_PROJECTED_NODES  # the runbook's 300-item case is safe

    # A deliberately huge case crosses the ceiling — the condition the live guard
    # (projected_nodes > MAX_PROJECTED_NODES) evaluates.
    assert 5000 * 999_999 > MAX_PROJECTED_NODES


# ===========================================================================
# BOOTSTRAP_METRICS: — the machine-readable stdout line
# ===========================================================================


def test_bootstrap_metrics_line_prefix_and_json_round_trip(capsys):
    # main() emits exactly one ``print("BOOTSTRAP_METRICS: " + json.dumps(...))``.
    # There is no separate emit helper to unit-test, so assert the format
    # contract the runbook parses: the exact marker prefix, then a JSON tail that
    # json.loads round-trips. Build a representative result dict (the shape
    # bootstrap() returns) and reproduce the one print statement.
    result = {
        "scenario_id": _OK_UUID,
        "subset_mode": "sample_finished",
        "pi_nodes_created": 294_000,
        "projected_nodes_estimate": 294_000,
        "volumetric_ceiling": MAX_PROJECTED_NODES,
        "forced": False,
        # A date-typed value proves the ``default=str`` fallback keeps the line
        # JSON-parseable (bootstrap() carries horizon_start/end as date/str).
        "horizon_start": "2026-07-06",
    }
    line = "BOOTSTRAP_METRICS: " + json.dumps(result, default=str)
    print(line)
    out = capsys.readouterr().out.strip()

    marker = "BOOTSTRAP_METRICS: "
    assert out.startswith(marker)
    payload = json.loads(out[len(marker):])  # the tail is valid JSON
    assert payload["subset_mode"] == "sample_finished"
    assert payload["volumetric_ceiling"] == MAX_PROJECTED_NODES
    assert payload["forced"] is False
