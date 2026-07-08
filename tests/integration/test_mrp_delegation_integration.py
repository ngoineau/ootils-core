# ruff: noqa: F401,F811
"""
tests/integration/test_mrp_delegation_integration.py — #423 PR2 / ADR-020 PAS 4.

The APICS write-path engine (``mrp_apics_engine``) no longer carries any MRP
math of its own: ``MrpApicsEngine.run()`` DELEGATES the whole calculation to the
consolidated core (``engine/mrp/loader.py`` → ``core.consume_demand`` →
``core.run_timephased``) and keeps ONLY graph materialization
(``_materialize_core_plan`` → ``graph_integration``). These tests exercise that
delegated path end-to-end through ``POST /v1/mrp/run`` (``apics_mode=True``).

Cases here:
  3. THE headline — an APICS run inside a scenario FORK nets against the FORK's
     on-hand (not baseline's), plans an order baseline would NOT, writes every
     PlannedSupply under the fork's ``scenario_id``, and never leaks onto
     baseline. This replaces the deleted
     ``test_mrp_apics_scenario_isolation_integration.py`` (which drove the
     now-removed ``GrossToNetCalculator`` directly) and absorbs #347 PR3 by also
     proving a fork planning-param overlay flows through the delegated loader.
  4. Post-cascade item filtering — a component whose demand comes ONLY from a
     non-requested parent still gets its correct planned order when
     ``item_ids=[component]``, because the core runs the FULL BOM explosion and
     the item filter is applied AFTER the cascade.

NOT duplicated here (covered elsewhere — see the module report):
  1. Hard cross-engine parity (0.05 band) is a dedicated CI guard
     (``scripts/parity_mrp_engines.py --check`` in ``.github/workflows/ci.yml``)
     plus the pure-unit band test ``tests/test_parity_mrp_check.py``.
  2. Re-run non-accumulation + FPO survive-purge-and-net is covered by
     ``tests/integration/test_mrp_apics_rerun_integration.py`` — its premises
     (columns ``mrp_run_id`` / ``is_firm`` / ``node_type`` / ``scenario_id`` /
     ``active`` / ``quantity``; FPO netting now sourced from the core loader,
     ``loader.py`` sched_b, instead of the deleted ``gross_to_net`` map) still
     hold under delegation, so it needs no change.

Test hygiene: every dedicated entity uses a unique uuid-suffixed ``external_id``
and small quantities (5 / 50 / 30 / 60), so nothing climbs to the top of any
``ORDER BY quantity DESC`` demand ranking other fixtures/harnesses rely on, and
every assertion filters by its own dedicated ids. Setup and verification run on
their own autocommit connection (like ``test_mrp_apics_rerun_integration``) so
the API's separate connection sees the seed. A best-effort FK-ordered cleanup
removes the dedicated rows after each test.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.engine.scenario.param_overlay import set_param_override

from .conftest import TEST_DB_URL, requires_db
from .test_mrp_api import api_client, auth, seeded_db  # noqa: F401

pytestmark = requires_db

BASELINE_ID = UUID("00000000-0000-0000-0000-000000000001")


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


@contextmanager
def _pg():
    """Autocommit dict_row connection to the test DB (setup + verification)."""
    with psycopg.connect(TEST_DB_URL, autocommit=True, row_factory=dict_row) as conn:
        yield conn


def _planned_supply_count(scenario_id: UUID, item_id: UUID) -> int:
    """Active PlannedSupply node count for an (item, scenario)."""
    with _pg() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS c FROM nodes "
            "WHERE node_type = 'PlannedSupply' AND scenario_id = %s "
            "AND item_id = %s AND active = TRUE",
            (scenario_id, item_id),
        ).fetchone()["c"]


def _planned_supply_scenarios(item_id: UUID) -> set:
    """Distinct scenario_ids owning an active PlannedSupply for the item."""
    with _pg() as conn:
        rows = conn.execute(
            "SELECT DISTINCT scenario_id FROM nodes "
            "WHERE node_type = 'PlannedSupply' AND item_id = %s AND active = TRUE",
            (item_id,),
        ).fetchall()
    return {r["scenario_id"] for r in rows}


def _planned_receipts(run_id: UUID, item_id: UUID) -> Decimal:
    """Total planned_order_receipts materialized for an item in a given run."""
    with _pg() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(planned_order_receipts), 0) AS q "
            "FROM mrp_bucket_records WHERE run_id = %s AND item_id = %s",
            (run_id, item_id),
        ).fetchone()
    return Decimal(str(row["q"]))


def _bucket_row_count(run_id: UUID, item_id: UUID) -> int:
    """How many bucket records a run materialized for an item (0 = filtered out)."""
    with _pg() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS c FROM mrp_bucket_records "
            "WHERE run_id = %s AND item_id = %s",
            (run_id, item_id),
        ).fetchone()["c"]


def _receipt_period_start(run_id: UUID, item_id: UUID) -> date:
    """period_start of the (single) materialized receipt bucket for an item."""
    with _pg() as conn:
        row = conn.execute(
            "SELECT period_start FROM mrp_bucket_records "
            "WHERE run_id = %s AND item_id = %s AND planned_order_receipts > 0",
            (run_id, item_id),
        ).fetchone()
    assert row is not None, "expected a materialized receipt bucket record"
    return row["period_start"]


def _db_current_date() -> date:
    """The loader's horizon_start (`SELECT CURRENT_DATE`), read the same way
    ``loader.load_planning_data`` does, so a date-guard assertion anchors on
    the exact value the delegated core used — not an assumed-equal
    ``date.today()`` in the test process."""
    with _pg() as conn:
        return conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]


# ---------------------------------------------------------------------------
# Dedicated-entity registry + FK-ordered cleanup
# ---------------------------------------------------------------------------


@pytest.fixture
def dedicated(seeded_db):  # noqa: F811
    """Track dedicated items/locations/fork scenarios and tear them down after
    the test in FK-safe order (best-effort — the module DB is dropped at
    teardown anyway, and unique ids already isolate the tests)."""
    reg: dict[str, list] = {"items": [], "locations": [], "scenarios": []}
    yield reg
    _cleanup_fk_ordered(reg["items"], reg["locations"], reg["scenarios"])


def _cleanup_fk_ordered(item_ids: list, location_ids: list, scenario_ids: list) -> None:
    if not (item_ids or location_ids or scenario_ids):
        return
    params = {"it": list(item_ids), "loc": list(location_ids), "sc": list(scenario_ids)}
    # Order: children before parents. Each runs under autocommit, so a defensive
    # swallow on one statement does not abort the rest.
    statements = [
        "DELETE FROM edges WHERE from_node_id IN "
        "(SELECT node_id FROM nodes WHERE item_id = ANY(%(it)s)) "
        "OR to_node_id IN (SELECT node_id FROM nodes WHERE item_id = ANY(%(it)s))",
        "DELETE FROM events WHERE trigger_node_id IN "
        "(SELECT node_id FROM nodes WHERE item_id = ANY(%(it)s))",
        "DELETE FROM mrp_action_messages WHERE item_id = ANY(%(it)s)",
        "DELETE FROM mrp_bucket_records WHERE item_id = ANY(%(it)s)",
        # Break the PlannedSupply release→receipt self-reference before the delete.
        "UPDATE nodes SET parent_node_id = NULL WHERE item_id = ANY(%(it)s)",
        "DELETE FROM nodes WHERE item_id = ANY(%(it)s)",
        "DELETE FROM scenario_planning_overrides "
        "WHERE item_id = ANY(%(it)s) OR scenario_id = ANY(%(sc)s)",
        "DELETE FROM item_planning_params WHERE item_id = ANY(%(it)s)",
        "DELETE FROM bom_lines WHERE component_item_id = ANY(%(it)s) "
        "OR bom_id IN (SELECT bom_id FROM bom_headers WHERE parent_item_id = ANY(%(it)s))",
        "DELETE FROM bom_headers WHERE parent_item_id = ANY(%(it)s)",
        "DELETE FROM mrp_runs WHERE scenario_id = ANY(%(sc)s) OR location_id = ANY(%(loc)s)",
        "DELETE FROM scenarios WHERE scenario_id = ANY(%(sc)s)",
        "DELETE FROM items WHERE item_id = ANY(%(it)s)",
        "DELETE FROM locations WHERE location_id = ANY(%(loc)s)",
    ]
    with _pg() as conn:
        for stmt in statements:
            try:
                conn.execute(stmt, params)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_fork_divergent_on_hand(
    reg: dict,
    baseline_on_hand: Decimal = Decimal("100"),
    fork_on_hand: Decimal = Decimal("5"),
    demand: Decimal = Decimal("50"),
):
    """Seed one buy finished-good with baseline on-hand that COVERS its demand,
    fork the baseline by direct INSERT, and give the fork a diverged (lower)
    on-hand that does NOT cover the same demand.

    Returns (item_ext, item_id, loc_ext, loc_id, fork_id).
    """
    suffix = uuid4().hex[:8]
    item_id, loc_id, fork_id = uuid4(), uuid4(), uuid4()
    item_ext = f"DELEG-ISO-FG-{suffix}"
    loc_ext = f"DELEG-ISO-LOC-{suffix}"
    demand_date = date.today() + timedelta(days=14)

    with _pg() as conn:
        conn.execute(
            "INSERT INTO items (item_id, name, item_type, external_id) "
            "VALUES (%s, %s, 'finished_good', %s)",
            (item_id, f"deleg-iso-item-{suffix}", item_ext),
        )
        conn.execute(
            "INSERT INTO locations (location_id, name, external_id) VALUES (%s, %s, %s)",
            (loc_id, f"deleg-iso-loc-{suffix}", loc_ext),
        )
        # Current planning-params row: buy item, small lead time, ZERO safety
        # stock (so net req is exactly demand - on_hand), lot-for-lot.
        conn.execute(
            "INSERT INTO item_planning_params "
            "(item_id, location_id, lead_time_sourcing_days, safety_stock_qty, "
            " lot_size_rule, is_make) "
            "VALUES (%s, %s, 7, 0, 'LOTFORLOT', FALSE)",
            (item_id, loc_id),
        )
        # Fork of baseline (direct INSERT — the fork carries only the nodes we
        # give it below, not a deep copy of baseline).
        conn.execute(
            "INSERT INTO scenarios (scenario_id, name, is_baseline, status, parent_scenario_id) "
            "VALUES (%s, %s, FALSE, 'active', %s)",
            (fork_id, f"deleg-iso-fork-{suffix}", BASELINE_ID),
        )
        # Baseline: on-hand (100) >= demand (50) -> baseline plans NOTHING.
        _insert_node(conn, "OnHandSupply", BASELINE_ID, item_id, loc_id, baseline_on_hand, date.today())
        _insert_node(conn, "ForecastDemand", BASELINE_ID, item_id, loc_id, demand, demand_date)
        # Fork: diverged on-hand (5) < demand (50) -> fork MUST plan.
        _insert_node(conn, "OnHandSupply", fork_id, item_id, loc_id, fork_on_hand, date.today())
        _insert_node(conn, "ForecastDemand", fork_id, item_id, loc_id, demand, demand_date)

    reg["items"].append(item_id)
    reg["locations"].append(loc_id)
    reg["scenarios"].append(fork_id)
    return item_ext, item_id, loc_ext, loc_id, fork_id


def _seed_parent_component_bom(
    reg: dict,
    qty_per: Decimal = Decimal("2"),
    parent_demand: Decimal = Decimal("30"),
):
    """Seed a make parent (with demand, no on-hand) requiring qty_per of a buy
    component (NO independent demand, no on-hand), on baseline. The component's
    only demand comes from the parent's BOM explosion. Parent's manufacturing
    lead time is fixed at 7 days (1 week; LOTFORLOT, zero safety stock) so a
    caller can hand-derive the expected release/need bucket from `demand_date`
    alone — see the date-guard assertion in
    ``test_single_item_run_gets_dependent_demand_from_unrequested_parent``.

    Returns (parent_ext, parent_id, comp_ext, comp_id, loc_ext, demand_date).
    """
    suffix = uuid4().hex[:8]
    parent_id, comp_id, loc_id = uuid4(), uuid4(), uuid4()
    parent_ext = f"DELEG-CASC-P-{suffix}"
    comp_ext = f"DELEG-CASC-C-{suffix}"
    loc_ext = f"DELEG-CASC-L-{suffix}"
    demand_date = date.today() + timedelta(days=35)

    with _pg() as conn:
        conn.execute(
            "INSERT INTO items (item_id, name, item_type, external_id) "
            "VALUES (%s, %s, 'finished_good', %s)",
            (parent_id, f"deleg-casc-parent-{suffix}", parent_ext),
        )
        conn.execute(
            "INSERT INTO items (item_id, name, item_type, external_id) "
            "VALUES (%s, %s, 'component', %s)",
            (comp_id, f"deleg-casc-comp-{suffix}", comp_ext),
        )
        conn.execute(
            "INSERT INTO locations (location_id, name, external_id) VALUES (%s, %s, %s)",
            (loc_id, f"deleg-casc-loc-{suffix}", loc_ext),
        )
        # Parent: make, manufacturing lead time. Component: buy, sourcing lead
        # time. Both zero safety, lot-for-lot -> qty is exactly the net req.
        conn.execute(
            "INSERT INTO item_planning_params "
            "(item_id, location_id, lead_time_manufacturing_days, safety_stock_qty, "
            " lot_size_rule, is_make) "
            "VALUES (%s, %s, 7, 0, 'LOTFORLOT', TRUE)",
            (parent_id, loc_id),
        )
        conn.execute(
            "INSERT INTO item_planning_params "
            "(item_id, location_id, lead_time_sourcing_days, safety_stock_qty, "
            " lot_size_rule, is_make) "
            "VALUES (%s, %s, 7, 0, 'LOTFORLOT', FALSE)",
            (comp_id, loc_id),
        )
        # BOM: parent requires qty_per of component. llc(component)=1 drives the
        # core's level-by-level cascade order (loader reads bom_lines.llc).
        bom_id = uuid4()
        conn.execute(
            "INSERT INTO bom_headers (bom_id, parent_item_id, bom_version) "
            "VALUES (%s, %s, '1.0')",
            (bom_id, parent_id),
        )
        conn.execute(
            "INSERT INTO bom_lines "
            "(bom_id, component_item_id, quantity_per, scrap_factor, llc) "
            "VALUES (%s, %s, %s, 0, 1)",
            (bom_id, comp_id, qty_per),
        )
        # Demand on the PARENT only — none on the component.
        _insert_node(conn, "ForecastDemand", BASELINE_ID, parent_id, loc_id, parent_demand, demand_date)

    reg["items"].extend([parent_id, comp_id])
    reg["locations"].append(loc_id)
    return parent_ext, parent_id, comp_ext, comp_id, loc_ext, demand_date


def _insert_node(conn, node_type, scenario_id, item_id, location_id, quantity, time_ref):
    conn.execute(
        "INSERT INTO nodes "
        "(node_id, node_type, scenario_id, item_id, location_id, quantity, "
        " time_grain, time_ref, active) "
        "VALUES (%s, %s, %s, %s, %s, %s, 'exact_date', %s, TRUE)",
        (uuid4(), node_type, scenario_id, item_id, location_id, quantity, time_ref),
    )


# ---------------------------------------------------------------------------
# Case 3 — headline: MRP run on an isolated fork (replaces the deleted test)
# ---------------------------------------------------------------------------


def test_apics_run_on_fork_nets_fork_on_hand_and_stays_isolated(api_client, auth, dedicated):
    """A delegated APICS run inside a fork nets against the FORK's on-hand.

    Baseline (on-hand 100 >= demand 50) plans nothing; the fork (on-hand 5 <
    demand 50) plans an order. Every PlannedSupply the fork run creates carries
    the fork's scenario_id, and baseline is never touched — proving the loader
    reads the fork's stock, not baseline's (the #333 regression the deleted
    GrossToNetCalculator test guarded, now re-proved through the delegated path).
    """
    item_ext, item_id, loc_ext, loc_id, fork_id = _seed_fork_divergent_on_hand(dedicated)

    base_payload = {
        "item_id": item_ext,
        "location_id": loc_ext,
        "apics_mode": True,
        "horizon_days": 90,
        "bucket_grain": "week",
        "forecast_strategy": "MAX",
    }

    # 1. Baseline run: on-hand covers demand -> zero planned supply for the item.
    r_base = api_client.post("/v1/mrp/run", json=base_payload, headers=auth)
    assert r_base.status_code == 200, r_base.text
    d_base = r_base.json()
    assert d_base["status"] == "COMPLETED", d_base["errors"]
    assert _planned_supply_count(BASELINE_ID, item_id) == 0, (
        "baseline on-hand (100) covers demand (50) — it must plan no order, "
        "so the fork's order below is genuinely one baseline would NOT produce"
    )

    # 2. Fork run: diverged on-hand (5) does not cover demand (50) -> must plan.
    fork_payload = {**base_payload, "scenario_id": str(fork_id)}
    r_fork = api_client.post("/v1/mrp/run", json=fork_payload, headers=auth)
    assert r_fork.status_code == 200, r_fork.text
    d_fork = r_fork.json()
    assert d_fork["status"] == "COMPLETED", d_fork["errors"]
    assert d_fork["scenario_id"] == str(fork_id)

    # (a) The fork planned an order against its own on-hand.
    assert _planned_supply_count(fork_id, item_id) > 0, (
        "the fork nets 50 demand against its diverged on-hand of 5 — it must "
        "plan a PlannedSupply that baseline (on-hand 100) does not"
    )

    # (b) Every PlannedSupply for the item is the fork's — none leaked scenarios.
    owners = _planned_supply_scenarios(item_id)
    assert owners == {fork_id}, (
        f"all PlannedSupply for the item must belong to the fork; got {owners}"
    )

    # (c) Baseline still has no PlannedSupply for the item (no cross-scenario leak).
    assert _planned_supply_count(BASELINE_ID, item_id) == 0


def test_apics_fork_param_overlay_flows_through_delegated_loader(api_client, auth, dedicated):
    """#347 PR3 absorbed: a fork planning-param overlay is respected by the
    delegated core loader. Raising the fork's safety_stock_qty grows the netted
    requirement, so the same fork run plans a strictly larger order."""
    item_ext, item_id, loc_ext, loc_id, fork_id = _seed_fork_divergent_on_hand(dedicated)
    fork_payload = {
        "item_id": item_ext,
        "location_id": loc_ext,
        "apics_mode": True,
        "horizon_days": 90,
        "scenario_id": str(fork_id),
    }

    # Run 1 — no overlay: net req = demand(50) - on_hand(5) = 45.
    r1 = api_client.post("/v1/mrp/run", json=fork_payload, headers=auth)
    assert r1.status_code == 200, r1.text
    assert r1.json()["status"] == "COMPLETED", r1.json()["errors"]
    qty_before = _planned_receipts(UUID(r1.json()["run_id"]), item_id)
    assert qty_before > 0, "fork run must plan before the overlay is applied"

    # Apply a fork-scoped safety-stock overlay (item-global): 0 -> 500.
    with _pg() as conn:
        set_param_override(conn, fork_id, item_id, "safety_stock_qty", "500", "test-deleg")

    # Run 2 — same fork, overlay in effect: net req grows by the added safety stock.
    r2 = api_client.post("/v1/mrp/run", json=fork_payload, headers=auth)
    assert r2.status_code == 200, r2.text
    assert r2.json()["status"] == "COMPLETED", r2.json()["errors"]
    qty_after = _planned_receipts(UUID(r2.json()["run_id"]), item_id)

    assert qty_after > qty_before, (
        f"fork safety_stock overlay (0 -> 500) must flow through the delegated "
        f"loader and grow the plan: {qty_before} -> {qty_after}"
    )


# ---------------------------------------------------------------------------
# Case 4 — single-item run still runs the full cascade (filter is post-cascade)
# ---------------------------------------------------------------------------


def test_single_item_run_gets_dependent_demand_from_unrequested_parent(api_client, auth, dedicated):
    """A run scoped to ONE component still explodes the full BOM.

    The component has no independent demand and its parent is NOT in item_ids,
    yet the component receives its correct dependent-demand order (parent 30 x
    qty_per 2 = 60). This is only possible because the delegated core runs the
    complete cascade and ``_materialize_core_plan`` applies the item filter
    AFTER it — the parent is computed but not materialized.

    Also locks the component's receipt DATE (not just its quantity) against a
    need<->release swap or an off-by-one bucket, hand-derived from the seed's
    lead time (#423 PR2 review fix — neither the quantity assertion above nor
    a cross-engine parity check would catch a date defect: both only compare
    totals).
    """
    parent_ext, parent_id, comp_ext, comp_id, loc_ext, demand_date = _seed_parent_component_bom(dedicated)

    # Request ONLY the component.
    payload = {
        "item_id": comp_ext,
        "location_id": loc_ext,
        "apics_mode": True,
        "horizon_days": 90,
        "bucket_grain": "week",
    }
    resp = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["status"] == "COMPLETED", data["errors"]
    run_id = UUID(data["run_id"])

    # The component was planned from the parent's (unrequested) demand: 30 x 2.
    comp_qty = _planned_receipts(run_id, comp_id)
    assert comp_qty == Decimal("60"), (
        f"component must be planned from the parent's dependent demand "
        f"(30 x qty_per 2 = 60); got {comp_qty}"
    )
    assert _planned_supply_count(BASELINE_ID, comp_id) > 0, (
        "the component's planned order must be materialized to the graph"
    )

    # --- Date guard: need<->release lock (#423 PR2 review fix) -------------
    # Hand-derive the component's expected RECEIPT bucket from the seed alone:
    #   demand_bucket        = (demand_date - horizon_start).days // 7
    #                         = 35 // 7 = 5   (demand_date = today + 35d)
    #   parent_release_bucket = demand_bucket - parent_lt_weeks
    #                         = 5 - ceil(7 days / 7) = 5 - 1 = 4
    #   comp_need_bucket      = parent_release_bucket = 4
    #     (BOM explosion attaches the component's dependent demand at the
    #     PARENT's release bucket — core.run_timephased: `dependent[comp][rel]
    #     += qty * qpb`, rel being the parent's own release bucket — so the
    #     component's need bucket IS the parent's release bucket, not its
    #     demand bucket. A need<->release swap in the core or in
    #     `_materialize_core_plan` would shift this by exactly
    #     `parent_lt_weeks` buckets and land here on bucket 5, not 4.)
    # horizon_start is read from the DB the same way the loader computes it
    # (`SELECT CURRENT_DATE`), not assumed equal to the test process's
    # ``date.today()``.
    horizon_start = _db_current_date()
    demand_bucket = (demand_date - horizon_start).days // 7
    parent_lt_weeks = 1  # ceil(7 days / 7) — the seed's fixed manufacturing lead time
    expected_need_bucket = demand_bucket - parent_lt_weeks
    expected_receipt_date = horizon_start + timedelta(weeks=expected_need_bucket)

    comp_receipt_date = _receipt_period_start(run_id, comp_id)
    assert comp_receipt_date == expected_receipt_date, (
        f"component receipt date mismatch (need<->release swap or off-by-one "
        f"bucket suspected): expected {expected_receipt_date} "
        f"(bucket {expected_need_bucket}), got {comp_receipt_date}"
    )

    # The parent was excluded by the item filter — computed, but not materialized.
    assert _bucket_row_count(run_id, parent_id) == 0, (
        "the parent is not in item_ids: the post-cascade filter must drop it "
        "from materialization even though the cascade planned it"
    )
    assert _planned_supply_count(BASELINE_ID, parent_id) == 0
