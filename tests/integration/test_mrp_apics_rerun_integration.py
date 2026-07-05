# ruff: noqa: F401,F811
"""Integration tests for APICS MRP re-run cleanup (issue #337).

Before the fix, ``GraphIntegration.cleanup_previous_run`` had no caller, so
every APICS re-run stacked a fresh set of PlannedSupply nodes on top of the
previous run's — double-counting planned supply. The engine now deactivates
all previous PlannedSupply nodes/edges for the scenario at the start of each
run (full-regeneration contract), so two successive runs must yield an
identical active planned-supply picture.
"""
from __future__ import annotations

from decimal import Decimal
from uuid import UUID

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import requires_db, TEST_DB_URL
from .test_mrp_api import api_client, auth, seeded_db  # noqa: F401

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


@pytest.fixture
def demand_item_location(seeded_db):  # noqa: F811
    """Pick an (item, location) pair that actually carries demand.

    The generic ``test_item_location`` fixture picks item and location with
    two independent ``LIMIT 1`` queries, so the pair may have no demand at
    all — an MRP run on it would create zero PlannedSupply and the re-run
    assertions would pass vacuously (0 == 0). Here we select the pair with
    the largest future demand so run 1 is guaranteed to plan something.
    """
    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        row = conn.execute(
            """
            SELECT i.external_id AS item_ext, i.item_id AS item_uuid,
                   l.external_id AS loc_ext, l.location_id AS loc_uuid
            FROM nodes n
            JOIN items i ON i.item_id = n.item_id
            JOIN locations l ON l.location_id = n.location_id
            WHERE n.node_type IN ('CustomerOrderDemand', 'ForecastDemand')
              AND n.scenario_id = %s
              AND n.active = TRUE
            GROUP BY i.external_id, i.item_id, l.external_id, l.location_id
            ORDER BY SUM(n.quantity) DESC
            LIMIT 1
            """,
            (BASELINE_SCENARIO_ID,),
        ).fetchone()

    if row is None:
        pytest.skip("No demand nodes in seeded DB for MRP re-run tests")

    return {
        "item_id": str(row["item_ext"]),
        "item_uuid": str(row["item_uuid"]),
        "location_id": str(row["loc_ext"]),
        "location_uuid": str(row["loc_uuid"]),
    }


def _active_planned_supply_stats(scenario_id: UUID) -> dict:
    """Return count and total quantity of ACTIVE PlannedSupply nodes for a scenario."""
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt, COALESCE(SUM(quantity), 0) AS total_qty
            FROM nodes
            WHERE node_type = 'PlannedSupply'
              AND scenario_id = %s
              AND active = TRUE
            """,
            (scenario_id,),
        ).fetchone()
    return {"count": row["cnt"], "total_qty": Decimal(str(row["total_qty"]))}


def _active_count_for_run(run_id: UUID) -> int:
    """Return the number of ACTIVE nodes belonging to a given MRP run."""
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM nodes WHERE mrp_run_id = %s AND active = TRUE",
            (run_id,),
        ).fetchone()
    return row["cnt"]


@requires_db
def test_apics_rerun_does_not_accumulate_planned_supply(api_client, auth, demand_item_location):
    """Two successive APICS runs on the same scenario must not double-count.

    The active PlannedSupply node count and total quantity must be identical
    after the second run, and the first run's nodes must have been
    deactivated (soft-deleted) by the second run's cleanup.
    """
    payload = {
        "item_id": demand_item_location["item_id"],
        "location_id": demand_item_location["location_id"],
        "apics_mode": True,
        "horizon_days": 90,
        "bucket_grain": "week",
        "forecast_strategy": "MAX",
    }

    # --- First run ---
    resp1 = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp1.status_code == 200, resp1.text
    data1 = resp1.json()
    assert data1["status"] == "COMPLETED", data1["errors"]
    run1_id = UUID(data1["run_id"])

    stats_after_run1 = _active_planned_supply_stats(BASELINE_SCENARIO_ID)
    # Guard against a vacuous pass: the pair was chosen because it carries
    # demand, so the first run must actually have planned supply.
    assert stats_after_run1["count"] > 0, (
        "First APICS run created no PlannedSupply — the re-run assertions "
        "below would pass vacuously (0 == 0)"
    )

    # --- Second run (identical parameters) ---
    resp2 = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp2.status_code == 200, resp2.text
    data2 = resp2.json()
    assert data2["status"] == "COMPLETED", data2["errors"]
    run2_id = UUID(data2["run_id"])
    assert run2_id != run1_id

    stats_after_run2 = _active_planned_supply_stats(BASELINE_SCENARIO_ID)

    # No accumulation: identical active node count and stable quantity totals.
    assert stats_after_run2["count"] == stats_after_run1["count"], (
        f"PlannedSupply nodes accumulated across re-runs: "
        f"{stats_after_run1['count']} → {stats_after_run2['count']}"
    )
    assert stats_after_run2["total_qty"] == stats_after_run1["total_qty"], (
        f"PlannedSupply total quantity drifted across re-runs: "
        f"{stats_after_run1['total_qty']} → {stats_after_run2['total_qty']}"
    )

    # The second run's cleanup must have deactivated the first run's nodes.
    assert _active_count_for_run(run1_id) == 0, (
        "First run's PlannedSupply nodes should be inactive after re-run"
    )

    # The surviving active picture belongs to the second run.
    assert _active_count_for_run(run2_id) == stats_after_run2["count"]


@requires_db
def test_apics_rerun_purge_is_scenario_scoped(api_client, auth, demand_item_location):
    """The regeneration purge is scenario-wide, not limited to the run's items.

    A scenario-wide run (deprecated endpoint, no item filter) followed by a
    single-item run must leave only the second run's nodes active: the
    cleanup contract purges by node_type='PlannedSupply' + scenario_id,
    deliberately wider than the run's item/location filter (no FPOs yet —
    chantier C2.2).
    """
    # Run A: scenario-wide (deprecated endpoint takes a raw location UUID)
    resp_a = api_client.post(
        "/v1/mrp/apics/run",
        json={
            "location_id": demand_item_location["location_uuid"],
            "horizon_days": 90,
        },
        headers=auth,
    )
    assert resp_a.status_code == 200, resp_a.text
    data_a = resp_a.json()
    assert data_a["status"] == "COMPLETED", data_a["errors"]
    run_a_id = UUID(data_a["run_id"])

    # Guard against a vacuous pass: the scenario-wide run must have planned
    # something before we can prove the next run purges it.
    assert _active_count_for_run(run_a_id) > 0, (
        "Scenario-wide APICS run created no PlannedSupply — the purge "
        "assertions below would pass vacuously"
    )

    # Run B: single item on the unified endpoint
    resp_b = api_client.post(
        "/v1/mrp/run",
        json={
            "item_id": demand_item_location["item_id"],
            "location_id": demand_item_location["location_id"],
            "apics_mode": True,
            "horizon_days": 90,
        },
        headers=auth,
    )
    assert resp_b.status_code == 200, resp_b.text
    data_b = resp_b.json()
    assert data_b["status"] == "COMPLETED", data_b["errors"]
    run_b_id = UUID(data_b["run_id"])

    # Only the latest run owns active PlannedSupply nodes.
    assert _active_count_for_run(run_a_id) == 0, (
        "Scenario-wide run's nodes should be purged by the following run"
    )
    stats = _active_planned_supply_stats(BASELINE_SCENARIO_ID)
    assert _active_count_for_run(run_b_id) == stats["count"]


def _firm_all_planned_supply(scenario_id: UUID) -> int:
    """Firm every active PlannedSupply for the scenario; returns how many."""
    with psycopg.connect(TEST_DB_URL, autocommit=True, row_factory=dict_row) as conn:
        cur = conn.execute(
            "UPDATE nodes SET is_firm = TRUE "
            "WHERE node_type = 'PlannedSupply' AND scenario_id = %s AND active = TRUE",
            (scenario_id,),
        )
        return cur.rowcount


@requires_db
def test_apics_rerun_keeps_firm_planned_orders_and_does_not_double_plan(
    api_client, auth, demand_item_location  # noqa: F811
):
    """#346 PR-C coupling — the invariant that justifies the whole PR.

    Firming a run's PlannedSupply into Firm Planned Orders must make them BOTH
    (a) survive the next regeneration purge (unlike the non-firm case, where
    run 1's nodes are fully deactivated) AND (b) be netted as committed supply,
    so the re-run does NOT double-plan the demand they already cover. Purge
    exclusion WITHOUT netting would double-plan; this proves the two together.
    """
    payload = {
        "item_id": demand_item_location["item_id"],
        "location_id": demand_item_location["location_id"],
        "apics_mode": True,
        "horizon_days": 90,
        "bucket_grain": "week",
        "forecast_strategy": "MAX",
    }

    # --- Run 1: plan the demand ---
    resp1 = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp1.status_code == 200, resp1.text
    assert resp1.json()["status"] == "COMPLETED", resp1.json()["errors"]
    run1_id = UUID(resp1.json()["run_id"])
    stats1 = _active_planned_supply_stats(BASELINE_SCENARIO_ID)
    assert stats1["count"] > 0, "run 1 must plan supply (else the test is vacuous)"

    # Firm every PlannedSupply run 1 produced -> they become FPOs.
    n_firmed = _firm_all_planned_supply(BASELINE_SCENARIO_ID)
    assert n_firmed == stats1["count"]

    # --- Run 2: identical parameters ---
    resp2 = api_client.post("/v1/mrp/run", json=payload, headers=auth)
    assert resp2.status_code == 200, resp2.text
    assert resp2.json()["status"] == "COMPLETED", resp2.json()["errors"]

    # (a) The firmed run-1 PlannedSupply SURVIVED the regeneration purge.
    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as conn:
        survived = conn.execute(
            "SELECT COUNT(*) AS c FROM nodes WHERE mrp_run_id = %s "
            "AND node_type = 'PlannedSupply' AND is_firm = TRUE AND active = TRUE",
            (run1_id,),
        ).fetchone()["c"]
    assert survived == n_firmed, (
        f"firmed run-1 PlannedSupply must survive the re-run: "
        f"{n_firmed} firmed, {survived} still active"
    )

    # (b) No double-planning: the firmed supply is netted, so run 2 adds no new
    #     supply for the already-covered demand — total active qty must not grow.
    stats2 = _active_planned_supply_stats(BASELINE_SCENARIO_ID)
    assert stats2["total_qty"] == stats1["total_qty"], (
        f"double-planning across the FPO re-run: active PlannedSupply qty grew "
        f"{stats1['total_qty']} -> {stats2['total_qty']}"
    )
