"""
tests/integration/test_agent_fleet_smoke.py -- Agent-fleet regression smoke (Lot 0).

CI regression harness for the 5 watcher agents (scripts/agent_*_watcher.py),
which are thin wrappers over scripts/mrp_core.py. Lot 0 is the foundation: a
deterministic seed fixture plus ONE sanity assertion that the shared mrp_core
planning pipeline loads the seed and detects the obvious finished-good shortage.
Per-watcher assertions land in Lots 1-3.

Why this exists: tests/engine_service/test_agent_workflow.py covers the gRPC
engine, NOT the watcher fleet. Nothing else exercises mrp_core end to end
against a real seeded baseline.

Determinism contract (matches mrp_core.load_planning_data):
  - horizon_start is anchored on SELECT CURRENT_DATE returned by the DB, NOT
    Python date.today()/now(). Every seed date is expressed RELATIVE to that
    DB-side anchor, read from the same connection, so the test is stable
    regardless of clock skew between the test process and the DB server.
  - The seed lives in the BASELINE scenario
    (00000000-0000-0000-0000-000000000001), the only scenario mrp_core reads.

Seed shape (5 items / 1 location / 1 supplier, all BASELINE):
  - FG-SHORT   : finished good, bought, thin on-hand + near-term customer order
                 -> first_shortage / shortage_watcher.
  - FG-MAKE    : finished good, made, BOM -> CMP-BUY, NEAR-TERM demand (drives a
                 past-due dependent CMP-BUY PO -> material_watcher).
  - CMP-BUY    : component, bought, llc=1 (set explicitly), thin on-hand
                 -> material_watcher (pegging / past-due).
  - BUY-NOCOST : bought, has demand but neither supplier unit_cost nor item
                 standard_cost -> dq_watcher MISSING_COST.
  - FG-EXCESS  : finished good, massive on-hand vs tiny demand -> eando_watcher.

Watcher coverage (Lots 1-3): shortage / material / dq / eando each fire a
governed artifact on this seed and are asserted directly. lot_policy_watcher
only proposes when realized weeks-of-supply leaves a target band — a violation
that is fragile to engineer on a 5-item seed — so it is held to the universal
run/idempotency contract here, not a per-output assertion.

Hard schema risk handled here: bom_lines.llc has DEFAULT 0 and is only computed
by a post-import LLC pass. mrp_core orders items by level from THIS column
(SELECT component_item_id, MAX(llc) FROM bom_lines). Left at 0, the component
would share level 0 with its parent and the material side would see no past-due
component. We therefore set llc = 1 on the CMP-BUY line explicitly.
"""

from __future__ import annotations

import datetime as _dt
import sys
from pathlib import Path

import pytest

from .conftest import requires_db

# ---------------------------------------------------------------------------
# Import seam: mrp_core + watchers live under scripts/ (outside the package).
# The watchers do a bare "import mrp_core", so scripts/ must be on sys.path.
# ---------------------------------------------------------------------------
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import mrp_core as core  # noqa: E402  (after sys.path mutation, by design)

# The watchers are CLI scripts under scripts/ that do a bare "import mrp_core";
# they expose main(argv) -> int, so we drive them in-process (no subprocess).
import agent_dq_watcher  # noqa: E402
import agent_eando_watcher  # noqa: E402
import agent_lot_policy_watcher  # noqa: E402
import agent_material_watcher  # noqa: E402
import agent_shortage_watcher  # noqa: E402
from agent_governance import decision_level  # noqa: E402

import psycopg  # noqa: E402
from psycopg.rows import dict_row, tuple_row  # noqa: E402

pytestmark = [requires_db, pytest.mark.smoke]

BASELINE = core.BASELINE  # 00000000-0000-0000-0000-000000000001

# Each watcher writes one governed-run row to agent_runs and (when it has
# something to say) artifact rows to its own table, keyed by agent_name. The
# active-status value is what a fresh run leaves behind; a re-run must supersede
# any prior actives so only the latest run's rows remain active (idempotency).
WATCHERS = [
    {"name": "shortage_watcher",  "module": agent_shortage_watcher,
     "table": "recommendations",            "active": "DRAFT", "graded": "full"},
    {"name": "material_watcher",  "module": agent_material_watcher,
     "table": "recommendations",            "active": "DRAFT", "graded": "full"},
    {"name": "eando_watcher",     "module": agent_eando_watcher,
     "table": "eando_recommendations",      "active": "DRAFT", "graded": "full"},
    {"name": "lot_policy_watcher", "module": agent_lot_policy_watcher,
     "table": "parameter_recommendations",  "active": "DRAFT", "graded": "full"},
    {"name": "dq_watcher",        "module": agent_dq_watcher,
     "table": "dq_findings",                "active": "OPEN",  "graded": "dq"},
]
# graded="full": typed decision_level + confidence + evidence columns.
# graded="dq"  : dq_findings carries severity + evidence (no decision_level/confidence).

_DECISION_LEVELS = {"L0", "L1", "L2", "L3", "L4"}
_CONFIDENCES = {"HIGH", "MEDIUM", "LOW", "NEEDS_DATA_REVIEW"}
_SEVERITIES = {"HIGH", "MEDIUM", "LOW"}


# These helpers index rows positionally ([0], tuple-unpack), so they force a
# tuple_row cursor regardless of the connection's row_factory -- callers may pass
# a dict_row connection (used elsewhere for column access) without breaking them.
def _completed_runs(conn, agent_name):
    return conn.cursor(row_factory=tuple_row).execute(
        "SELECT COUNT(*) FROM agent_runs "
        "WHERE agent_name=%s AND scenario_id=%s AND status='COMPLETED'",
        (agent_name, BASELINE)).fetchone()[0]


def _latest_run(conn, agent_name):
    """(agent_run_id, status, metrics, finished_at) of the newest run, or None."""
    return conn.cursor(row_factory=tuple_row).execute(
        "SELECT agent_run_id, status, metrics, finished_at FROM agent_runs "
        "WHERE agent_name=%s AND scenario_id=%s "
        "ORDER BY started_at DESC, finished_at DESC NULLS LAST LIMIT 1",
        (agent_name, BASELINE)).fetchone()


@pytest.fixture(scope="module")
def seeded_fleet_db(migrated_db):
    """Module-scoped: seed the 5-item agent-fleet scenario into BASELINE.

    Depends on migrated_db (applies all migrations, yields the DSN, then drops
    every public table after the module). All dates are anchored on the DB-side
    CURRENT_DATE so the seed stays aligned with mrp_core.load_planning_data
    horizon anchor. Yields the DSN string (same contract as migrated_db).
    """
    import psycopg

    dsn = migrated_db
    with psycopg.connect(dsn, autocommit=True) as conn:
        cur = conn.cursor()
        # Single DB-side anchor -- every relative date is computed from this.
        today = cur.execute("SELECT CURRENT_DATE").fetchone()[0]

        loc_id = cur.execute(
            "INSERT INTO locations (name, location_type, external_id) "
            "VALUES (%s, %s, %s) RETURNING location_id",
            ("Fleet Plant", "plant", "LOC-FLEET"),
        ).fetchone()[0]

        sup_id = cur.execute(
            "INSERT INTO suppliers (external_id, name, reliability_score, status) "
            "VALUES (%s, %s, %s, %s) RETURNING supplier_id",
            ("SUP-FLEET", "Fleet Supplier", 0.95, "active"),
        ).fetchone()[0]

        # (external_id, name, item_type, standard_cost, cost_currency)
        item_specs = [
            ("FG-SHORT", "FG Short", "finished_good", 100.0, "EUR"),
            ("FG-MAKE", "FG Make", "finished_good", 250.0, "EUR"),
            ("CMP-BUY", "Component Buy", "component", 40.0, "EUR"),
            # BUY-NOCOST: deliberately NO standard_cost and NO supplier unit_cost
            # (the dq_watcher MISSING_COST trigger for a later lot).
            ("BUY-NOCOST", "Buy No Cost", "component", None, None),
            ("FG-EXCESS", "FG Excess", "finished_good", 30.0, "EUR"),
        ]
        item_id = {}
        for ext, name, itype, scost, ccy in item_specs:
            item_id[ext] = cur.execute(
                "INSERT INTO items (external_id, name, item_type, standard_cost, cost_currency) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING item_id",
                (ext, name, itype, scost, ccy),
            ).fetchone()[0]

        # lead_time_total_days is GENERATED from the 3 lead-time components, so
        # seed the components (sourcing/manufacturing/transit), not the total.
        # is_make and order_multiple live HERE (not on items). effective_to NULL
        # -> the active row mrp_core reads.
        def _ipp(ext, is_make, sourcing, manufacturing, transit, safety, multiple=None):
            cur.execute(
                "INSERT INTO item_planning_params "
                "(item_id, location_id, is_make, "
                " lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days, "
                " safety_stock_qty, order_multiple, lot_size_rule, "
                " frozen_time_fence_days, slashed_time_fence_days, forecast_consumption_strategy) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,7,30,%s)",
                (item_id[ext], loc_id, is_make, sourcing, manufacturing, transit,
                 safety, multiple, "LOTFORLOT", "max_only"),
            )

        _ipp("FG-SHORT", False, 14, 0, 0, 0)
        _ipp("FG-MAKE", True, 0, 7, 0, 0)
        _ipp("CMP-BUY", False, 21, 0, 0, 5)
        _ipp("BUY-NOCOST", False, 10, 0, 0, 0)
        _ipp("FG-EXCESS", False, 7, 0, 0, 0)

        # lead_time_days is NOT NULL and CHECK > 0. BUY-NOCOST link carries NULL
        # unit_cost on purpose (no priced supplier row -> uncosted).
        def _sup(ext, lead_time, unit_cost):
            cur.execute(
                "INSERT INTO supplier_items "
                "(supplier_id, item_id, lead_time_days, unit_cost, currency, is_preferred) "
                "VALUES (%s,%s,%s,%s,%s,TRUE)",
                (sup_id, item_id[ext], lead_time, unit_cost, "EUR"),
            )

        _sup("FG-SHORT", 14, 100.0)
        _sup("CMP-BUY", 21, 40.0)
        _sup("BUY-NOCOST", 10, None)  # uncosted on purpose
        _sup("FG-EXCESS", 7, 30.0)

        # BOM: FG-MAKE -> CMP-BUY (2 each)
        bom_id = cur.execute(
            "INSERT INTO bom_headers (parent_item_id, bom_version, status) "
            "VALUES (%s, %s, %s) RETURNING bom_id",
            (item_id["FG-MAKE"], "1.0", "active"),
        ).fetchone()[0]
        # llc = 1 set EXPLICITLY (DEFAULT 0 would hide the component on level 0,
        # so the material side would never see it as a dependent-demand component).
        cur.execute(
            "INSERT INTO bom_lines (bom_id, component_item_id, quantity_per, scrap_factor, llc) "
            "VALUES (%s, %s, %s, %s, %s)",
            (bom_id, item_id["CMP-BUY"], 2.0, 0.0, 1),
        )

        # On-hand (OnHandSupply nodes): thin where a shortage is expected,
        # massive for FG-EXCESS. time_ref anchored on the DB CURRENT_DATE.
        def _onhand(ext, qty):
            cur.execute(
                "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
                " time_grain, time_ref, active) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)",
                ("OnHandSupply", BASELINE, item_id[ext], loc_id, qty, "exact_date", today),
            )

        _onhand("FG-SHORT", 5)        # near-term demand of 50 dwarfs this
        _onhand("FG-MAKE", 0)
        _onhand("CMP-BUY", 3)         # thin vs dependent demand from FG-MAKE
        _onhand("BUY-NOCOST", 0)
        _onhand("FG-EXCESS", 100000)  # massive vs tiny demand

        # Customer-order demand (CustomerOrderDemand nodes). Dates RELATIVE to
        # the DB anchor today (never absolute, never Python now()).
        def _demand(ext, days_out, qty, ntype="CustomerOrderDemand"):
            cur.execute(
                "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, quantity, "
                " time_grain, time_ref, active) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)",
                (ntype, BASELINE, item_id[ext], loc_id, qty, "exact_date",
                 today + _dt.timedelta(days=days_out)),
            )

        _demand("FG-SHORT", 21, 50)      # 50 vs 5 on-hand -> shortage
        # FG-MAKE demand is NEAR-TERM on purpose. FG-MAKE has a 7-day manufacturing
        # lead and CMP-BUY a 21-day sourcing lead, so dependent CMP-BUY demand at
        # day 3 forces a component PO whose release date (≈ day 3 − 28) is in the
        # past -> material_watcher sees a PAST-DUE, pegged component. A far-out
        # FG-MAKE date would push that release into the future and the material
        # side would (correctly) draft nothing, leaving the watcher untested.
        _demand("FG-MAKE", 3, 30)        # near-term -> past-due CMP-BUY component PO
        _demand("BUY-NOCOST", 28, 40)    # demand on an uncosted item
        _demand("FG-EXCESS", 90, 10)     # tiny vs 100000 on-hand -> excess

        yield dsn
        # Teardown is owned by migrated_db (drops all public tables).


# ---------------------------------------------------------------------------
# Lot 0 sanity test -- ONE assertion path, no per-watcher logic yet.
# ---------------------------------------------------------------------------

def test_mrp_core_loads_seed_and_detects_fg_short(seeded_fleet_db):
    """load_planning_data ingests the seed and first_shortage flags FG-SHORT.

    Lot 0 ground truth: the shared planning pipeline (the substrate every watcher
    leans on) sees the seeded baseline and produces the obvious finished-good
    shortage. Per-watcher behaviour (recommendations, DQ findings, E&O class,
    pegging) is asserted in Lots 1-3.
    """
    import psycopg

    with psycopg.connect(seeded_fleet_db) as conn:
        d = core.load_planning_data(conn)
        gross = core.consume_demand(d)
        short = core.first_shortage(d, gross)

        # Resolve FG-SHORT item_id (mrp_core keys everything by UUID).
        fg_short_id = conn.execute(
            "SELECT item_id FROM items WHERE external_id = %s", ("FG-SHORT",)
        ).fetchone()[0]

    # The seed loaded: horizon anchored, items present, demand consumed.
    assert d.horizon_start is not None
    assert fg_short_id in d.names
    assert d.names[fg_short_id] == "FG-SHORT"
    assert gross.get(fg_short_id), "FG-SHORT must carry independent (consumed) demand"

    # The whole point: FG-SHORT runs short within the horizon.
    assert fg_short_id in short, "first_shortage must flag FG-SHORT"
    hit = short[fg_short_id]
    assert hit["deficit"] > 0
    assert hit["date"] >= d.horizon_start


# ---------------------------------------------------------------------------
# Lot 1 -- universal governed-run + idempotency contract (all 5 watchers).
#
# This is the anti-regression core: regardless of WHAT a watcher recommends,
# every watcher must (a) exit 0, (b) leave exactly one COMPLETED agent_runs row
# with a metrics block, and (c) be idempotent -- a second run supersedes prior
# actives so only the latest run's rows remain active, and never crashes. A
# watcher that double-writes, forgets to close out its run, or stops superseding
# breaks here even if no per-output assertion ever runs.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("w", WATCHERS, ids=[w["name"] for w in WATCHERS])
def test_watcher_governed_run_is_idempotent(seeded_fleet_db, w):
    dsn = seeded_fleet_db
    mod, name, table, active = w["module"], w["name"], w["table"], w["active"]

    with psycopg.connect(dsn) as conn:
        before = _completed_runs(conn, name)

    # --- First run: clean exit + exactly one new COMPLETED, finished, run. ---
    assert mod.main(["--dsn", dsn, "--allow-dev"]) == 0
    with psycopg.connect(dsn) as conn:
        assert _completed_runs(conn, name) == before + 1
        run_id, status, metrics, finished_at = _latest_run(conn, name)
        assert status == "COMPLETED"
        assert finished_at is not None
        assert metrics is not None  # JSONB metrics block is always written

    # --- Second run: idempotent. No crash; another COMPLETED run; every ACTIVE
    #     output row belongs to the LATEST run (prior actives superseded). ---
    assert mod.main(["--dsn", dsn, "--allow-dev"]) == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        assert _completed_runs(conn, name) == before + 2
        latest_id = _latest_run(conn, name)[0]
        # table comes from the hardcoded WATCHERS registry, never user input.
        active_rows = conn.execute(
            f"SELECT * FROM {table} "  # noqa: S608 -- constant identifier
            "WHERE agent_name=%s AND scenario_id=%s AND status=%s",
            (name, BASELINE, active)).fetchall()
        assert all(r["agent_run_id"] == latest_id for r in active_rows), (
            f"{name}: stale active rows from a superseded run -- supersede broke"
        )
        # Whatever was written is governed: evidence present + typed verdict cols valid.
        for r in active_rows:
            assert r["evidence"] is not None
            if w["graded"] == "full":
                assert r["decision_level"] in _DECISION_LEVELS
                assert r["confidence"] in _CONFIDENCES
            else:  # dq
                assert r["severity"] in _SEVERITIES


# ---------------------------------------------------------------------------
# Lots 2-3 -- targeted per-watcher output assertions. The seed is shaped so each
# of these four watchers MUST produce its signature governed artifact.
# (lot_policy_watcher is covered by the universal contract only -- see module
# docstring for why its per-output trigger is intentionally out of scope.)
# ---------------------------------------------------------------------------

def test_shortage_watcher_drafts_purchase_for_fg_short(seeded_fleet_db):
    dsn = seeded_fleet_db
    assert agent_shortage_watcher.main(["--dsn", dsn, "--allow-dev"]) == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT * FROM recommendations WHERE agent_name='shortage_watcher' "
            "AND scenario_id=%s AND status='DRAFT' AND item_external_id='FG-SHORT'",
            (BASELINE,)).fetchall()
    assert rows, "shortage_watcher must draft a purchase reco for FG-SHORT"
    r = rows[0]
    # Decision ladder (#340): the level is DERIVED from the action by the
    # single shared mapping — new-order drafts L1, EXPEDITE L2.
    assert r["decision_level"] == decision_level(r["action"])
    assert r["confidence"] in _CONFIDENCES
    assert r["action"] in {"EXPEDITE", "ORDER_RUSH", "ORDER_NOW"}
    assert r["recommended_qty"] > 0
    assert r["evidence"] is not None


def test_material_watcher_pegs_past_due_component(seeded_fleet_db):
    dsn = seeded_fleet_db
    assert agent_material_watcher.main(["--dsn", dsn, "--allow-dev"]) == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT * FROM recommendations WHERE agent_name='material_watcher' "
            "AND scenario_id=%s AND status='DRAFT' AND item_external_id='CMP-BUY'",
            (BASELINE,)).fetchall()
    assert rows, "material_watcher must draft an expedite for past-due CMP-BUY"
    r = rows[0]
    # EXPEDITE touches an EXISTING order commitment -> L2 (#340 decision ladder).
    assert r["decision_level"] == decision_level("EXPEDITE") == "L2"
    assert r["action"] == "EXPEDITE"
    assert r["recommended_qty"] > 0
    # Pegging trail back to the driving finished good is the whole point.
    assert r["evidence"] is not None
    assert r["evidence"].get("pegging"), "component reco must carry a pegging trail"


def test_dq_watcher_opens_missing_cost_for_buy_nocost(seeded_fleet_db):
    dsn = seeded_fleet_db
    assert agent_dq_watcher.main(["--dsn", dsn, "--allow-dev"]) == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT * FROM dq_findings WHERE agent_name='dq_watcher' AND scenario_id=%s "
            "AND status='OPEN' AND rule_code='MISSING_COST' "
            "AND entity_external_id='BUY-NOCOST'",
            (BASELINE,)).fetchall()
    assert rows, "dq_watcher must open a MISSING_COST finding for BUY-NOCOST"
    r = rows[0]
    assert r["severity"] in _SEVERITIES
    assert r["evidence"] is not None


def test_eando_watcher_drafts_disposition_for_fg_excess(seeded_fleet_db):
    dsn = seeded_fleet_db
    assert agent_eando_watcher.main(["--dsn", dsn, "--allow-dev"]) == 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT * FROM eando_recommendations WHERE agent_name='eando_watcher' "
            "AND scenario_id=%s AND status='DRAFT' AND item_external_id='FG-EXCESS'",
            (BASELINE,)).fetchall()
    assert rows, "eando_watcher must draft a disposition for FG-EXCESS"
    r = rows[0]
    assert r["classification"] == "EXCESS"
    assert r["decision_level"] == "L1"
    assert r["confidence"] in _CONFIDENCES
    assert r["excess_units"] > 0
    assert r["evidence"] is not None
