"""
tests/integration/test_bootstrap_pi_scenario_integration.py — #414 PR2.

DB-backed tests for the scenario-first PI bootstrap (scripts/bootstrap_pi.py):
the ``bootstrap()`` engine + the ``main()`` CLI, against a real Postgres. No
mocks (CLAUDE.md). Migrations are applied by the ``migrated_db`` fixture exactly
as production applies them (OotilsDB startup).

WRITTEN BLIND — these tests are authored without a local Postgres and are NOT
executed here (the repo runs integration/ only against a throwaway DB, per
CLAUDE.md). They clone the dominant seed+assert pattern of
test_scenario_backed_watchers_integration.py (module-scoped autocommit seed of
dedicated, prefixed master data) and the fork-by-direct-INSERT pattern of
test_snapshot_integration.py / test_param_overlay_integration.py.

Locked contracts under test:
  1. Scenario-scoping: a bootstrap on a FORK creates ProjectedInventory nodes on
     the fork ONLY — baseline gains zero PI nodes. nodes_after - nodes_before on
     the fork equals pairs x horizon (one PI node per active pair per day). The
     ``main()`` CLI emits exactly one BOOTSTRAP_METRICS: <json> line, parseable.
  2. BOM closure: seeding a finished item with a 2-level component sub-tree pulls
     every transitive component into scope, so their pairs are materialised too.
  3. --items-file: an explicit external_id list is honoured (+ the same BOM
     closure); a code absent from the file is out of scope.
  4. Volumetric guard (anti-big-bang): pairs x horizon above the 2 000 000
     ceiling raises SystemExit and writes NOTHING, WITHOUT --force. The passing
     --force path is deliberately NOT exercised here: forcing the guard on an
     over-ceiling case would materialise millions of nodes in the test DB (the
     very debt the guard exists to refuse) — its refusal is the whole contract,
     and the DB-free half (ceiling constant + arithmetic) is pinned in
     tests/test_bootstrap_pi_unit.py.
  5. Retro-compat: bootstrap with NO subset flags on a tiny baseline behaves as
     the historical CLI did — full scope, all active pairs, on baseline.

NOT tested here: the full runbook sequence (fork -> POST /v1/calc/run -> demo_e2e
step 7). That is the PILOT operator's live run (docs/RUNBOOK-pilot-propagation.md),
not a CI invariant — a 300-item x 120-day materialise + full recompute is a
pilot-scale perf exercise, out of scope for the seeded CI battery.

Isolation: bootstrap()'s subset scratch tables are ON COMMIT DROP; the tests that
call bootstrap() directly on the function-scoped ``conn`` fixture COMMIT their
seed but let the module teardown (migrated_db DROPs every public table) reclaim
everything. Each test seeds its own uuid4-suffixed / prefixed master data so runs
never collide. Dates are anchored on the DB-side CURRENT_DATE. No wall-clock
timing assertions.
"""
from __future__ import annotations

import contextlib
import io
import sys
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import TEST_DB_URL, requires_db

# bootstrap_pi.py lives in scripts/ (outside the installed package) and does
# ``import psycopg`` at module top only — no connection at import. Load it by
# path (the demo_e2e_integration seam) so scripts/ need not be a package.
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import bootstrap_pi  # noqa: E402

pytestmark = [requires_db, pytest.mark.smoke]

BASELINE = UUID("00000000-0000-0000-0000-000000000001")

# The five node types bootstrap() treats as "activity" for pair derivation
# (bootstrap_pi.py step 1). We seed OnHandSupply + CustomerOrderDemand.
_ACTIVITY_SUPPLY = "OnHandSupply"
_ACTIVITY_DEMAND = "CustomerOrderDemand"


# ---------------------------------------------------------------------------
# Seed helpers — dedicated, prefixed/suffixed master data (collision-free)
# ---------------------------------------------------------------------------


def _mk_item(conn, external_id: str, *, item_type: str = "finished_good") -> UUID:
    """One active item. external_id is REQUIRED by the --items-file path
    (bootstrap joins items.external_id) and by --sample-finished ranking."""
    return conn.execute(
        "INSERT INTO items (item_id, external_id, name, item_type, status) "
        "VALUES (%s, %s, %s, %s, 'active') RETURNING item_id",
        (uuid4(), external_id, external_id, item_type),
    ).fetchone()["item_id"]


def _mk_location(conn, name: str = "bpi-loc") -> UUID:
    return conn.execute(
        "INSERT INTO locations (location_id, external_id, name) "
        "VALUES (%s, %s, %s) RETURNING location_id",
        (uuid4(), f"{name}-{uuid4()}", name),
    ).fetchone()["location_id"]


def _mk_scenario(conn, name: str = "bpi-fork") -> UUID:
    """A non-baseline scenario (a plain fork row — no ScenarioManager deep-copy;
    scoping is all we assert). Mirrors test_snapshot_integration._seed_scenario."""
    return conn.execute(
        "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
        "VALUES (%s, %s, FALSE, 'active') RETURNING scenario_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["scenario_id"]


def _mk_bom_line(conn, parent_item_id: UUID, component_item_id: UUID, *,
                 qty: float = 1.0, llc: int = 1) -> UUID:
    """One active BOM edge parent -> component. Reuses an existing active header
    for the parent if present (a parent may have several components), else
    creates one. llc defaults to 1 so the component is NOT mistaken for a
    finished (LLC-0) item (the seed shape lesson from test_agent_fleet_smoke)."""
    header = conn.execute(
        "SELECT bom_id FROM bom_headers "
        "WHERE parent_item_id = %s AND status = 'active' LIMIT 1",
        (parent_item_id,),
    ).fetchone()
    if header is None:
        bom_id = conn.execute(
            "INSERT INTO bom_headers (parent_item_id, bom_version, status) "
            "VALUES (%s, '1.0', 'active') RETURNING bom_id",
            (parent_item_id,),
        ).fetchone()["bom_id"]
    else:
        bom_id = header["bom_id"]
    conn.execute(
        "INSERT INTO bom_lines (bom_id, component_item_id, quantity_per, scrap_factor, llc) "
        "VALUES (%s, %s, %s, 0.0, %s)",
        (bom_id, component_item_id, qty, llc),
    )
    return bom_id


def _mk_node(conn, *, node_type: str, scenario_id: UUID, item_id: UUID,
             location_id: UUID, qty: float, days_out: int = 0) -> UUID:
    """One supply/demand activity node scoped to ``scenario_id``, dated relative
    to the DB-side CURRENT_DATE (so supply/demand edges land inside the horizon)."""
    return conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            quantity, time_grain, time_ref, active
        ) VALUES (%s, %s, %s, %s, %s, %s, 'exact_date',
                  (CURRENT_DATE + (%s || ' days')::interval)::date, TRUE)
        RETURNING node_id
        """,
        (uuid4(), node_type, scenario_id, item_id, location_id, qty, days_out),
    ).fetchone()["node_id"]


def _count_pi_nodes(conn, scenario_id: UUID) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM nodes "
            "WHERE scenario_id = %s AND node_type = 'ProjectedInventory'",
            (scenario_id,),
        ).fetchone()["n"]
    )


def _pi_items(conn, scenario_id: UUID) -> set[UUID]:
    """The set of item_ids that got a ProjectedInventory node in this scenario."""
    rows = conn.execute(
        "SELECT DISTINCT item_id FROM nodes "
        "WHERE scenario_id = %s AND node_type = 'ProjectedInventory'",
        (scenario_id,),
    ).fetchall()
    return {r["item_id"] for r in rows}


# ===========================================================================
# 1. Fork isolation + BOOTSTRAP_METRICS line
# ===========================================================================


@requires_db
def test_bootstrap_on_fork_isolated_from_baseline(conn):
    """--sample-finished 2 on a FORK creates PI nodes on the fork only; baseline
    gains zero. nodes_after - nodes_before on the fork = pairs x horizon."""
    fork = _mk_scenario(conn)

    # Two finished items, each with one location + one supply + one demand node
    # SCOPED TO THE FORK -> two active (item, location) pairs on the fork.
    fg1 = _mk_item(conn, f"BPI-FG1-{uuid4()}")
    fg2 = _mk_item(conn, f"BPI-FG2-{uuid4()}")
    loc = _mk_location(conn)
    for it in (fg1, fg2):
        _mk_node(conn, node_type=_ACTIVITY_SUPPLY, scenario_id=fork,
                 item_id=it, location_id=loc, qty=5, days_out=0)
        _mk_node(conn, node_type=_ACTIVITY_DEMAND, scenario_id=fork,
                 item_id=it, location_id=loc, qty=10, days_out=7)

    # A baseline activity node for a THIRD item — must never get a PI node from a
    # fork-scoped bootstrap (the isolation tripwire).
    base_only = _mk_item(conn, f"BPI-BASE-{uuid4()}")
    _mk_node(conn, node_type=_ACTIVITY_SUPPLY, scenario_id=BASELINE,
             item_id=base_only, location_id=loc, qty=3, days_out=0)
    conn.commit()

    baseline_pi_before = _count_pi_nodes(conn, BASELINE)
    fork_pi_before = _count_pi_nodes(conn, fork)
    assert fork_pi_before == 0

    horizon = 30
    result = bootstrap_pi.bootstrap(
        conn, horizon, None, scenario_id=str(fork), sample_finished=2
    )
    conn.commit()

    # 2 pairs x 30 days.
    assert result["pairs_in_scope"] == 2
    assert result["pi_nodes_created"] == 2 * horizon
    assert not result["forced"]

    fork_pi_after = _count_pi_nodes(conn, fork)
    assert fork_pi_after - fork_pi_before == 2 * horizon
    assert result["scenario_nodes_after"] - result["scenario_nodes_before"] >= 2 * horizon

    # Baseline untouched: no new PI nodes, and the baseline-only item never got one.
    assert _count_pi_nodes(conn, BASELINE) == baseline_pi_before
    assert base_only not in _pi_items(conn, BASELINE)
    assert base_only not in _pi_items(conn, fork)


@requires_db
def test_main_emits_parseable_bootstrap_metrics_line(migrated_db):
    """The CLI ``main()`` (its own connection, its own COMMIT) prints exactly one
    machine-readable BOOTSTRAP_METRICS: <json> line whose tail json.loads
    round-trips and reports the fork it targeted. Uses a dedicated fork; the
    module teardown reclaims the committed rows."""
    import json

    with psycopg.connect(TEST_DB_URL, autocommit=True, row_factory=dict_row) as c:
        fork = _mk_scenario(c)
        it = _mk_item(c, f"BPI-METRIC-{uuid4()}")
        loc = _mk_location(c)
        _mk_node(c, node_type=_ACTIVITY_SUPPLY, scenario_id=fork,
                 item_id=it, location_id=loc, qty=5, days_out=0)
        _mk_node(c, node_type=_ACTIVITY_DEMAND, scenario_id=fork,
                 item_id=it, location_id=loc, qty=10, days_out=3)

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = bootstrap_pi.main(
            ["--dsn", TEST_DB_URL, "--allow-dev",
             "--scenario", str(fork), "--sample-finished", "1",
             "--horizon-days", "10"]
        )
    assert rc == 0
    out = buf.getvalue()

    marker = "BOOTSTRAP_METRICS: "
    lines = [ln for ln in out.splitlines() if ln.startswith(marker)]
    assert len(lines) == 1, f"expected exactly one metrics line, got {len(lines)}"
    payload = json.loads(lines[0][len(marker):])
    assert payload["scenario_id"] == str(fork)
    assert payload["subset_mode"] == "sample_finished"
    assert payload["pi_nodes_created"] == 1 * 10
    assert payload["volumetric_ceiling"] == bootstrap_pi.MAX_PROJECTED_NODES
    assert payload["forced"] is False


# ===========================================================================
# 2. BOM closure — components pulled into scope
# ===========================================================================


@requires_db
def test_bom_closure_includes_two_level_components(conn):
    """A finished item with a 2-level BOM (FG -> SUB -> RAW) materialises PI nodes
    for the finished item AND both transitive components. Every level carries an
    activity node so a pair exists for it once it is in scope."""
    fork = _mk_scenario(conn)
    loc = _mk_location(conn)

    fg = _mk_item(conn, f"BPI-FG-{uuid4()}", item_type="finished_good")
    sub = _mk_item(conn, f"BPI-SUB-{uuid4()}", item_type="semi_finished")
    raw = _mk_item(conn, f"BPI-RAW-{uuid4()}", item_type="raw_material")

    # FG -> SUB (llc 1), SUB -> RAW (llc 2). FG is the only LLC-0 (finished) node.
    _mk_bom_line(conn, fg, sub, qty=2.0, llc=1)
    _mk_bom_line(conn, sub, raw, qty=3.0, llc=2)

    # Activity on every level (same location) so each in-scope item yields a pair.
    for it in (fg, sub, raw):
        _mk_node(conn, node_type=_ACTIVITY_SUPPLY, scenario_id=fork,
                 item_id=it, location_id=loc, qty=1, days_out=0)
        _mk_node(conn, node_type=_ACTIVITY_DEMAND, scenario_id=fork,
                 item_id=it, location_id=loc, qty=1, days_out=5)
    conn.commit()

    result = bootstrap_pi.bootstrap(
        conn, 15, None, scenario_id=str(fork), sample_finished=5
    )
    conn.commit()

    # Only FG is finished (LLC 0) -> exactly one seed; closure adds SUB and RAW.
    assert result["seed_items"] == 1
    assert result["scope_items_after_bom_closure"] == 3

    pi = _pi_items(conn, fork)
    assert {fg, sub, raw} <= pi, "BOM closure must project the finished item AND both components"


# ===========================================================================
# 3. --items-file — explicit list honoured + closure
# ===========================================================================


@requires_db
def test_items_file_scope_is_the_explicit_list_plus_closure(conn, tmp_path):
    """--items-file limits the seed to the listed external_ids (+ their BOM
    closure); a real activity item NOT in the file stays out of scope."""
    fork = _mk_scenario(conn)
    loc = _mk_location(conn)

    listed = _mk_item(conn, f"BPI-LIST-{uuid4()}", item_type="finished_good")
    comp = _mk_item(conn, f"BPI-COMP-{uuid4()}", item_type="component")
    other = _mk_item(conn, f"BPI-OTHER-{uuid4()}", item_type="finished_good")

    _mk_bom_line(conn, listed, comp, qty=4.0, llc=1)

    for it in (listed, comp, other):
        _mk_node(conn, node_type=_ACTIVITY_SUPPLY, scenario_id=fork,
                 item_id=it, location_id=loc, qty=1, days_out=0)
        _mk_node(conn, node_type=_ACTIVITY_DEMAND, scenario_id=fork,
                 item_id=it, location_id=loc, qty=1, days_out=2)
    conn.commit()

    # File lists ONLY the finished parent; comments + blanks must be ignored.
    listed_ext = conn.execute(
        "SELECT external_id FROM items WHERE item_id = %s", (listed,)
    ).fetchone()["external_id"]
    items_file = tmp_path / "scope.txt"
    items_file.write_text(
        f"# pilot subset\n{listed_ext}\n\n", encoding="utf-8"
    )

    result = bootstrap_pi.bootstrap(
        conn, 12, None, scenario_id=str(fork), items_file=items_file
    )
    conn.commit()

    assert result["subset_mode"] == "items_file"
    assert result["seed_items"] == 1  # only the listed parent seeded
    assert result["scope_items_after_bom_closure"] == 2  # + its component

    pi = _pi_items(conn, fork)
    assert listed in pi and comp in pi, "listed item + its closure must be projected"
    assert other not in pi, "an item absent from --items-file must NOT be projected"


# ===========================================================================
# 4. Volumetric guard — refusal without --force (never the --force path)
# ===========================================================================


@requires_db
def test_volumetric_guard_refuses_over_ceiling_without_force(conn):
    """pairs x horizon above the 2 000 000 ceiling raises SystemExit and writes
    NOTHING (the guard fires at step 1b, BEFORE any projection_series / PI node
    insert). A huge horizon over a couple of pairs crosses the ceiling cheaply.

    The --force success path is intentionally NOT exercised: forcing this case
    would create millions of PI nodes — the exact debt the guard refuses. Its
    refusal IS the contract; the arithmetic/ceiling constant is unit-pinned."""
    fork = _mk_scenario(conn)
    loc = _mk_location(conn)
    it = _mk_item(conn, f"BPI-GUARD-{uuid4()}")
    _mk_node(conn, node_type=_ACTIVITY_SUPPLY, scenario_id=fork,
             item_id=it, location_id=loc, qty=1, days_out=0)
    _mk_node(conn, node_type=_ACTIVITY_DEMAND, scenario_id=fork,
             item_id=it, location_id=loc, qty=1, days_out=1)
    conn.commit()

    pi_before = _count_pi_nodes(conn, fork)

    # 1 pair x 3 000 000 days = 3 000 000 > 2 000 000 ceiling.
    huge_horizon = bootstrap_pi.MAX_PROJECTED_NODES + 1_000_000
    with pytest.raises(SystemExit) as exc:
        bootstrap_pi.bootstrap(
            conn, huge_horizon, None, scenario_id=str(fork), sample_finished=1
        )
    # The refusal names the ceiling and how to override — a clear operator signal.
    assert "REFUSED" in str(exc.value)
    assert f"{bootstrap_pi.MAX_PROJECTED_NODES:,}" in str(exc.value)

    conn.rollback()  # the aborted bootstrap left an open, partial transaction
    assert _count_pi_nodes(conn, fork) == pi_before, "guard must write no PI nodes"


# ===========================================================================
# 5. Retro-compat — no subset flags = historical full-scope baseline behaviour
# ===========================================================================


@requires_db
def test_no_subset_flags_full_scope_on_baseline(conn):
    """With neither --sample-finished nor --items-file, bootstrap keeps the
    historical behaviour: subset_mode='full', every active (item, location) pair
    in the TARGET scenario is projected. Run on a fork with a couple of seeded
    pairs so the assertion is deterministic (baseline on a shared CI DB may carry
    unrelated activity)."""
    fork = _mk_scenario(conn)
    loc = _mk_location(conn)
    a = _mk_item(conn, f"BPI-FULLA-{uuid4()}")
    b = _mk_item(conn, f"BPI-FULLB-{uuid4()}")
    for it in (a, b):
        _mk_node(conn, node_type=_ACTIVITY_SUPPLY, scenario_id=fork,
                 item_id=it, location_id=loc, qty=1, days_out=0)
        _mk_node(conn, node_type=_ACTIVITY_DEMAND, scenario_id=fork,
                 item_id=it, location_id=loc, qty=1, days_out=4)
    conn.commit()

    horizon = 20
    result = bootstrap_pi.bootstrap(conn, horizon, None, scenario_id=str(fork))
    conn.commit()

    assert result["subset_mode"] == "full"
    assert result["seed_items"] == 0  # no seed/closure phase in full mode
    assert result["scope_items_after_bom_closure"] == 0
    # Both seeded pairs are in scope; every activity pair is projected.
    assert result["pairs_in_scope"] == result["total_pairs_with_activity"] == 2
    assert result["pi_nodes_created"] == 2 * horizon
    assert {a, b} <= _pi_items(conn, fork)
