"""
tests/integration/test_scenario_backed_watchers_integration.py — chantier #340.

Scenario-backed watchers: the shortage and material watchers validate their
draft recommendations by counter-factual — ONE scenario fork per run
(``what-if-<agent>-<ts>``), overrides applied in the fork via the in-process
/v1/simulate path (ootils_core.tools.agent_tools.simulate_overrides), the
shortage delta attributed per reco in its evidence JSONB, and the fork
ARCHIVED at end of run (TTL pattern, never DELETE).

Asserted contract:
  (a) every emitted reco carries, in evidence, EITHER
      simulation_scenario_id + a per-item delta (simulated candidates)
      OR the documented not-simulated marker (``not_simulated_reason``);
  (b) the run's what-if scenario is status='archived' after the run;
  (c) decision_level is DERIVED from the action by the single shared mapping
      (EXPEDITE of an existing receipt = L2, new-order drafts = L1).

The failed-propagation path (propagation_status='failed' ->
confidence=NEEDS_DATA_REVIEW, no delta) is covered by the PURE unit tests in
tests/test_scenario_backed_watchers.py — forcing a deterministic propagation
crash against a real Postgres without mocks is not reliably reproducible in CI.

Seed shape (relative to the DB-side CURRENT_DATE anchor, all BASELINE):
  FG-EXP : bought (LT 14d), on-hand 2, CO demand 60 at day 7 -> the planned PO
           is PAST-DUE -> action EXPEDITE. A firm PurchaseOrderSupply receipt
           of 100 lands at day 40 (AFTER the need date) -> SIMULABLE by
           advancing its time_ref.
  FG-NEW : bought (LT 14d), on-hand 0, CO demand 50 at day 60 -> comfortable
           margin -> ORDER_NOW -> NOT simulable (drafts a NEW order; no node
           to override).
  FG-MK  : made (mfg LT 7d), BOM -> CMP-X (x2), CO demand 30 at day 3 -> the
           dependent CMP-X PO is past-due -> material watcher EXPEDITE.
  CMP-X  : bought component (LT 21d), llc=1, on-hand 0. A firm receipt of 200
           lands at day 45 -> SIMULABLE for the material watcher.
"""
from __future__ import annotations

import datetime as _dt
import sys
from pathlib import Path

import pytest

from .conftest import requires_db

# Import seam: mrp_core + watchers live under scripts/ (outside the package).
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import mrp_core as core  # noqa: E402
import agent_material_watcher  # noqa: E402
import agent_shortage_watcher  # noqa: E402
from agent_governance import decision_level  # noqa: E402

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

pytestmark = [requires_db, pytest.mark.smoke]

BASELINE = core.BASELINE


@pytest.fixture(scope="module")
def seeded_sim_db(migrated_db):
    """Module-scoped seed for the scenario-backed watcher battery (#340).

    All dates are anchored on the DB-side CURRENT_DATE (the same anchor
    mrp_core.load_planning_data uses), never on Python now().
    """
    dsn = migrated_db
    with psycopg.connect(dsn, autocommit=True) as conn:
        cur = conn.cursor()
        today = cur.execute("SELECT CURRENT_DATE").fetchone()[0]

        loc_id = cur.execute(
            "INSERT INTO locations (name, location_type, external_id) "
            "VALUES (%s, %s, %s) RETURNING location_id",
            ("Sim Plant", "plant", "LOC-SIM"),
        ).fetchone()[0]

        sup_id = cur.execute(
            "INSERT INTO suppliers (external_id, name, reliability_score, status) "
            "VALUES (%s, %s, %s, %s) RETURNING supplier_id",
            ("SUP-SIM", "Sim Supplier", 0.95, "active"),
        ).fetchone()[0]

        item_specs = [
            ("FG-EXP", "FG Expedite", "finished_good", 100.0, "EUR"),
            ("FG-NEW", "FG New Order", "finished_good", 80.0, "EUR"),
            ("FG-MK", "FG Make", "finished_good", 250.0, "EUR"),
            ("CMP-X", "Component X", "component", 40.0, "EUR"),
        ]
        item_id = {}
        for ext, name, itype, scost, ccy in item_specs:
            item_id[ext] = cur.execute(
                "INSERT INTO items (external_id, name, item_type, standard_cost, cost_currency) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING item_id",
                (ext, name, itype, scost, ccy),
            ).fetchone()[0]

        def _ipp(ext, is_make, sourcing, manufacturing, transit, safety):
            cur.execute(
                "INSERT INTO item_planning_params "
                "(item_id, location_id, is_make, "
                " lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days, "
                " safety_stock_qty, lot_size_rule, "
                " frozen_time_fence_days, slashed_time_fence_days, forecast_consumption_strategy) "
                # frozen=0 (no frozen zone, CHECK >= 0) but slashed must be > 0
                # (migration 021 CHECK): 1 day keeps the fences out of the
                # way of the J+40/J+45 receipts these tests expedite.
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0,1,%s)",
                (item_id[ext], loc_id, is_make, sourcing, manufacturing, transit,
                 safety, "LOTFORLOT", "max_only"),
            )

        _ipp("FG-EXP", False, 14, 0, 0, 0)
        _ipp("FG-NEW", False, 14, 0, 0, 0)
        _ipp("FG-MK", True, 0, 7, 0, 0)
        _ipp("CMP-X", False, 21, 0, 0, 0)

        def _sup(ext, lead_time, unit_cost):
            cur.execute(
                "INSERT INTO supplier_items "
                "(supplier_id, item_id, lead_time_days, unit_cost, currency, is_preferred) "
                "VALUES (%s,%s,%s,%s,%s,TRUE)",
                (sup_id, item_id[ext], lead_time, unit_cost, "EUR"),
            )

        _sup("FG-EXP", 14, 10.0)
        _sup("FG-NEW", 14, 8.0)
        _sup("CMP-X", 21, 4.0)

        # BOM: FG-MK -> CMP-X (2 each). llc=1 explicitly (DEFAULT 0 would hide
        # the component on level 0 — see test_agent_fleet_smoke docstring).
        bom_id = cur.execute(
            "INSERT INTO bom_headers (parent_item_id, bom_version, status) "
            "VALUES (%s, %s, %s) RETURNING bom_id",
            (item_id["FG-MK"], "1.0", "active"),
        ).fetchone()[0]
        cur.execute(
            "INSERT INTO bom_lines (bom_id, component_item_id, quantity_per, scrap_factor, llc) "
            "VALUES (%s, %s, %s, %s, %s)",
            (bom_id, item_id["CMP-X"], 2.0, 0.0, 1),
        )

        def _node(ntype, ext, days_out, qty):
            cur.execute(
                "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
                " time_grain, time_ref, active) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)",
                (ntype, BASELINE, item_id[ext], loc_id, qty, "exact_date",
                 today + _dt.timedelta(days=days_out)),
            )

        # On-hand
        _node("OnHandSupply", "FG-EXP", 0, 2)
        _node("OnHandSupply", "FG-NEW", 0, 0)
        _node("OnHandSupply", "FG-MK", 0, 0)
        _node("OnHandSupply", "CMP-X", 0, 0)

        # Demand
        _node("CustomerOrderDemand", "FG-EXP", 7, 60)    # near-term -> past-due PO -> EXPEDITE
        _node("CustomerOrderDemand", "FG-NEW", 60, 50)   # far out -> ORDER_NOW
        _node("CustomerOrderDemand", "FG-MK", 3, 30)     # near-term -> past-due CMP-X PO

        # Firm receipts landing AFTER the need dates: the override targets for
        # the EXPEDITE counter-factuals.
        _node("PurchaseOrderSupply", "FG-EXP", 40, 100)
        _node("PurchaseOrderSupply", "CMP-X", 45, 200)

        yield dsn
        # Teardown owned by migrated_db (drops all public tables).


def _drafts(dsn, agent_name):
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(
            "SELECT * FROM recommendations WHERE agent_name=%s AND scenario_id=%s "
            "AND status='DRAFT'",
            (agent_name, BASELINE)).fetchall()


def _sim_block(row):
    ev = row["evidence"]
    assert ev is not None, "every reco must carry an evidence trail"
    assert "simulation" in ev, "scenario-backed reco must carry evidence.simulation (#340)"
    return ev["simulation"]


# ---------------------------------------------------------------------------
# (a) every reco is scenario-backed: simulation_scenario_id + delta OR the
#     documented not-simulated marker.
# ---------------------------------------------------------------------------

def test_shortage_watcher_recos_are_scenario_backed(seeded_sim_db):
    dsn = seeded_sim_db
    assert agent_shortage_watcher.main(["--dsn", dsn, "--allow-dev"]) == 0
    rows = _drafts(dsn, "shortage_watcher")
    assert rows, "seed must produce shortage recommendations"

    by_ext = {r["item_external_id"]: r for r in rows}
    assert "FG-EXP" in by_ext and "FG-NEW" in by_ext

    for r in rows:
        sim = _sim_block(r)
        if sim["simulated"]:
            # simulated -> fork id + a real (possibly zero) per-item delta
            assert r["evidence"]["simulation_scenario_id"], "simulated reco must reference its fork"
            assert sim["delta"] is not None and {"new_shortages", "resolved_shortages", "net_change"} <= set(sim["delta"])
            assert sim["propagation_status"] == "ok"
        else:
            # not simulable -> honest, documented marker; never a fabricated delta
            assert sim["not_simulated_reason"]
            assert sim["delta"] is None

    # The seed pins which side each item falls on.
    fg_exp = _sim_block(by_ext["FG-EXP"])
    assert by_ext["FG-EXP"]["action"] == "EXPEDITE"
    assert fg_exp["simulated"] is True
    assert fg_exp["override"]["field_name"] == "time_ref"

    fg_new = _sim_block(by_ext["FG-NEW"])
    assert by_ext["FG-NEW"]["action"] == "ORDER_NOW"
    assert fg_new["simulated"] is False
    assert "NEW order" in fg_new["not_simulated_reason"]


def test_material_watcher_recos_are_scenario_backed(seeded_sim_db):
    dsn = seeded_sim_db
    assert agent_material_watcher.main(["--dsn", dsn, "--allow-dev"]) == 0
    rows = _drafts(dsn, "material_watcher")
    assert rows, "seed must produce a past-due CMP-X expedite"
    by_ext = {r["item_external_id"]: r for r in rows}
    assert "CMP-X" in by_ext

    cmp_sim = _sim_block(by_ext["CMP-X"])
    assert cmp_sim["simulated"] is True
    assert by_ext["CMP-X"]["evidence"]["simulation_scenario_id"]
    assert cmp_sim["delta"] is not None
    # The pegging trail must survive the evidence enrichment.
    assert by_ext["CMP-X"]["evidence"].get("pegging")


# ---------------------------------------------------------------------------
# (b) the run's what-if fork is archived at end of run (TTL, never DELETE).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("agent,module", [
    ("shortage_watcher", agent_shortage_watcher),
    ("material_watcher", agent_material_watcher),
])
def test_run_scenario_is_archived(seeded_sim_db, agent, module):
    dsn = seeded_sim_db
    assert module.main(["--dsn", dsn, "--allow-dev"]) == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        scens = conn.execute(
            "SELECT scenario_id, name, status FROM scenarios WHERE name LIKE %s",
            (f"what-if-{agent}-%",)).fetchall()
        assert scens, f"{agent}: the run must have created its what-if fork"
        for s in scens:
            assert s["status"] == "archived", f"{agent}: fork {s['name']} not archived"

        # The run metrics reference the fork (auditable end to end).
        run = conn.execute(
            "SELECT metrics FROM agent_runs WHERE agent_name=%s AND status='COMPLETED' "
            "ORDER BY started_at DESC LIMIT 1", (agent,)).fetchone()
        sim = run["metrics"]["simulation"]
        assert sim["archived"] is True
        assert sim["scenario_id"] in {str(s["scenario_id"]) for s in scens}


# ---------------------------------------------------------------------------
# (c) decision level is derived per action by the shared mapping.
# ---------------------------------------------------------------------------

def test_decision_levels_derived_from_action(seeded_sim_db):
    dsn = seeded_sim_db
    assert agent_shortage_watcher.main(["--dsn", dsn, "--allow-dev"]) == 0
    assert agent_material_watcher.main(["--dsn", dsn, "--allow-dev"]) == 0

    for agent in ("shortage_watcher", "material_watcher"):
        for r in _drafts(dsn, agent):
            assert r["decision_level"] == decision_level(r["action"]), (
                f"{agent}/{r['item_external_id']}: level {r['decision_level']} "
                f"!= mapping for action {r['action']}"
            )

    by_ext = {r["item_external_id"]: r for r in _drafts(dsn, "shortage_watcher")}
    assert by_ext["FG-EXP"]["decision_level"] == "L2"   # EXPEDITE of an existing receipt
    assert by_ext["FG-NEW"]["decision_level"] == "L1"   # draft of a NEW order
    cmp_row = {r["item_external_id"]: r for r in _drafts(dsn, "material_watcher")}["CMP-X"]
    assert cmp_row["decision_level"] == "L2"
