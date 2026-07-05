"""tests/integration/test_fpo_lifecycle_integration.py — chantier #346 PR-C.

DB-backed coverage of the Firm Planned Order (FPO) lifecycle against a real
Postgres, no mocks. PR-A/PR-B gave the reschedule signals and their governed
emission; PR-C closes the loop: a firmed PlannedSupply must

  1. survive the full-regeneration purge (cleanup_previous_run) — a non-firm
     PlannedSupply is still soft-deleted;
  2. be netted as committed supply in the math core (loader.sched_b) so it
     produces NO false shortage (the coupling: purge-exclusion WITHOUT netting
     would double-plan; here we prove the netting half);
  3. be mutable through the audited POST/DELETE /v1/nodes/{id}/firm endpoint,
     which emits a `node_firm_changed` event (Streamable) and rejects a
     non-PlannedSupply / missing node;
  4. stay RE-DATABLE — a mis-dated FPO is still seen by the reschedule watcher
     (it is not frozen, only shielded from regeneration).

The end-to-end "an APICS re-run does not double-plan a firmed order" coupling
lives in test_mrp_apics_rerun_integration.py, next to the APICS-run fixtures.

Determinism: every date is anchored on the DB-side CURRENT_DATE, like the
sibling watcher seeds. dict_row throughout — columns accessed by NAME.
"""
from __future__ import annotations

import datetime as _dt
import os
import sys
from pathlib import Path
from uuid import UUID

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import requires_db, TEST_DB_URL

# Import seam: mrp_core lives under scripts/ (outside the package).
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import agent_reschedule_watcher  # noqa: E402
import mrp_core as core  # noqa: E402

from ootils_core.engine.mrp.graph_integration import GraphIntegration  # noqa: E402

pytestmark = requires_db

BASELINE = UUID("00000000-0000-0000-0000-000000000001")
_WEEK = 7


# ---------------------------------------------------------------------------
# Seed helpers (self-contained TRUNCATE seed, like the reschedule watcher tests)
# ---------------------------------------------------------------------------


def _reset_graph(conn):
    conn.execute(
        "TRUNCATE nodes, edges, recommendations, agent_runs, events, "
        "item_planning_params, supplier_items, items, suppliers, locations, "
        "scenario_planning_overrides RESTART IDENTITY CASCADE"
    )
    conn.execute("TRUNCATE shortages RESTART IDENTITY CASCADE")


def _seed_common(conn):
    """One location + one item with an IPP row (so the loader reads params)."""
    loc_id = conn.execute(
        "INSERT INTO locations (name, location_type, external_id) "
        "VALUES (%s, %s, %s) RETURNING location_id",
        ("FPO Plant", "plant", "LOC-FPO"),
    ).fetchone()["location_id"]
    item_id = conn.execute(
        "INSERT INTO items (external_id, name, item_type, standard_cost, cost_currency) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING item_id",
        ("ITM-FPO", "FPO Item", "component", 40.0, "EUR"),
    ).fetchone()["item_id"]
    conn.execute(
        "INSERT INTO item_planning_params "
        "(item_id, location_id, is_make, lead_time_sourcing_days, "
        " lead_time_manufacturing_days, lead_time_transit_days, safety_stock_qty, "
        " lot_size_rule, frozen_time_fence_days, slashed_time_fence_days, "
        " forecast_consumption_strategy) "
        "VALUES (%s,%s,FALSE,14,0,0,0,%s,0,1,%s)",
        (item_id, loc_id, "LOTFORLOT", "max_only"),
    )
    return loc_id, item_id


def _node(conn, ntype, scenario, item_id, loc_id, when, qty, *, is_firm=False):
    """Insert one active node at an exact date; returns its node_id."""
    return conn.execute(
        "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
        " time_grain, time_ref, active, is_firm) "
        "VALUES (%s, %s, %s, %s, %s, 'exact_date', %s, TRUE, %s) RETURNING node_id",
        (ntype, str(scenario), item_id, loc_id, qty, when, is_firm),
    ).fetchone()["node_id"]


def _today(conn):
    return conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]


# ---------------------------------------------------------------------------
# 1. Purge survival — a firm PlannedSupply is shielded from regeneration.
# ---------------------------------------------------------------------------


@requires_db
def test_firm_planned_supply_survives_regeneration_purge(migrated_db):
    """cleanup_previous_run(None) deactivates every NON-firm PlannedSupply for
    the scenario (full-regen contract) but leaves is_firm=TRUE ones active — the
    exclusion that lets a Firm Planned Order persist across MRP runs."""
    with psycopg.connect(TEST_DB_URL, autocommit=True, row_factory=dict_row) as conn:
        _reset_graph(conn)
        loc_id, item_id = _seed_common(conn)
        today = _today(conn)
        firm_id = _node(conn, "PlannedSupply", BASELINE, item_id, loc_id,
                        today + _dt.timedelta(weeks=6), 100, is_firm=True)
        soft_id = _node(conn, "PlannedSupply", BASELINE, item_id, loc_id,
                        today + _dt.timedelta(weeks=8), 50, is_firm=False)

        GraphIntegration(conn, BASELINE).cleanup_previous_run(None)

        firm = conn.execute("SELECT active FROM nodes WHERE node_id=%s", (firm_id,)).fetchone()
        soft = conn.execute("SELECT active FROM nodes WHERE node_id=%s", (soft_id,)).fetchone()

    assert firm["active"] is True, "a firm PlannedSupply (FPO) must survive the purge"
    assert soft["active"] is False, "a non-firm PlannedSupply must be purged"


# ---------------------------------------------------------------------------
# 2. Math-core netting — a firm PlannedSupply counts as supply => no shortage.
# ---------------------------------------------------------------------------


@requires_db
def test_firm_planned_supply_is_netted_in_math_core_no_false_shortage(migrated_db):
    """loader.sched_b must count a firm PlannedSupply as a committed receipt, so
    demand it covers produces NO shortage — the netting half of the purge/netting
    coupling (without it, a survived-but-unnetted FPO would double-plan)."""
    with psycopg.connect(TEST_DB_URL, autocommit=True, row_factory=dict_row) as conn:
        _reset_graph(conn)
        loc_id, item_id = _seed_common(conn)
        today = _today(conn)
        # On-hand 0, demand 100 at week 6, a FIRM PlannedSupply of 100 at week 6.
        _node(conn, "OnHandSupply", BASELINE, item_id, loc_id, today, 0)
        _node(conn, "CustomerOrderDemand", BASELINE, item_id, loc_id,
              today + _dt.timedelta(weeks=6), 100)
        _node(conn, "PlannedSupply", BASELINE, item_id, loc_id,
              today + _dt.timedelta(weeks=6), 100, is_firm=True)

        d = core.load_planning_data(conn, 120, scenario=str(BASELINE))

    # The firm PlannedSupply must appear in the aggregated scheduled-receipts map.
    firm_supply = sum(
        qty for buckets in [d.sched_b.get(item_id, {})] for qty in buckets.values()
    )
    assert firm_supply >= 100, (
        "firm PlannedSupply must be netted into sched_b as committed supply "
        f"(got {firm_supply})"
    )
    # And a shortage pass must see the demand as covered (no shortage).
    gross = core.consume_demand(d)
    fs = core.first_shortage(d, gross)
    assert fs.get(item_id) is None, (
        "a firm PlannedSupply covering the demand must yield no shortage"
    )


# ---------------------------------------------------------------------------
# 3. Firm / unfirm endpoint — audited mutation + node_firm_changed event.
# ---------------------------------------------------------------------------


@pytest.fixture
def app_client(migrated_db):
    """A TestClient over a fresh app, get_db bound to the migrated test DB.

    Depends on migrated_db so the schema exists; self-contained (no demo seed)
    — the tests below seed their own node.
    """
    os.environ.setdefault("OOTILS_API_TOKEN", "test-token")
    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db

    app = create_app()

    def _override_db():
        with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as conn:
            yield conn

    app.dependency_overrides[get_db] = _override_db
    return TestClient(app)


@requires_db
def test_firm_endpoint_sets_flag_emits_event_and_validates(app_client):
    """POST/DELETE /v1/nodes/{id}/firm flips is_firm, emits one
    node_firm_changed event per call, and rejects a non-PlannedSupply (422) and
    a missing node (404). Auth is required (401 without a bearer token)."""
    auth = {"Authorization": "Bearer test-token"}
    with psycopg.connect(TEST_DB_URL, autocommit=True, row_factory=dict_row) as conn:
        _reset_graph(conn)
        loc_id, item_id = _seed_common(conn)
        today = _today(conn)
        ps_id = _node(conn, "PlannedSupply", BASELINE, item_id, loc_id,
                      today + _dt.timedelta(weeks=6), 100, is_firm=False)
        pi_id = _node(conn, "ProjectedInventory", BASELINE, item_id, loc_id, today, 0)

    # No auth -> 401.
    assert app_client.post(f"/v1/nodes/{ps_id}/firm", json={"actor": "planner"}).status_code == 401

    # Firm it.
    r = app_client.post(f"/v1/nodes/{ps_id}/firm", json={"actor": "planner"}, headers=auth)
    assert r.status_code == 200, r.text
    assert r.json()["is_firm"] is True

    # Un-firm it.
    r = app_client.request("DELETE", f"/v1/nodes/{ps_id}/firm",
                           json={"actor": "planner"}, headers=auth)
    assert r.status_code == 200, r.text
    assert r.json()["is_firm"] is False

    # Firming a non-PlannedSupply -> 422; a missing node -> 404.
    assert app_client.post(f"/v1/nodes/{pi_id}/firm",
                           json={"actor": "planner"}, headers=auth).status_code == 422
    from uuid import uuid4
    assert app_client.post(f"/v1/nodes/{uuid4()}/firm",
                           json={"actor": "planner"}, headers=auth).status_code == 404

    # Two successful calls (firm + unfirm) each emitted a node_firm_changed event.
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS c FROM events "
            "WHERE event_type='node_firm_changed' AND trigger_node_id=%s",
            (ps_id,)).fetchone()["c"]
    assert n == 2, f"expected 2 firm-change events (firm + unfirm), got {n}"

    # Final state persisted: is_firm back to FALSE.
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as conn:
        assert conn.execute("SELECT is_firm FROM nodes WHERE node_id=%s",
                            (ps_id,)).fetchone()["is_firm"] is False


# ---------------------------------------------------------------------------
# 4. An FPO stays RE-DATABLE — a mis-dated firm PlannedSupply still fires a
#    reschedule message (it is shielded from regeneration, not frozen).
# ---------------------------------------------------------------------------


@requires_db
def test_firm_planned_supply_is_still_reschedulable(migrated_db):
    """A firm PlannedSupply that is mis-dated relative to its need is still seen
    by the reschedule watcher (it is in sched_orders) and yields a governed
    RESCHEDULE DRAFT — an FPO is not frozen, only regen-shielded."""
    with psycopg.connect(TEST_DB_URL, autocommit=True, row_factory=dict_row) as conn:
        _reset_graph(conn)
        loc_id, item_id = _seed_common(conn)
        today = _today(conn)
        # Demand 100 at week 20; a FIRM PlannedSupply of 100 dated far too early
        # (week 3) => far from its need bucket => RESCHEDULE_OUT.
        _node(conn, "OnHandSupply", BASELINE, item_id, loc_id, today, 0)
        _node(conn, "CustomerOrderDemand", BASELINE, item_id, loc_id,
              today + _dt.timedelta(weeks=20), 100)
        _node(conn, "PlannedSupply", BASELINE, item_id, loc_id,
              today + _dt.timedelta(weeks=3), 100, is_firm=True)

    assert agent_reschedule_watcher.main(["--dsn", TEST_DB_URL, "--allow-dev"]) == 0

    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT action FROM recommendations "
            "WHERE agent_name='reschedule_watcher' AND scenario_id=%s AND status='DRAFT'",
            (str(BASELINE),)).fetchall()
    actions = {r["action"] for r in rows}
    assert actions & {"RESCHEDULE_IN", "RESCHEDULE_OUT"}, (
        f"a mis-dated FPO must still produce a reschedule message; got {actions}"
    )
