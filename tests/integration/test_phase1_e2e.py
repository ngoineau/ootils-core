"""
tests/integration/test_phase1_e2e.py — Phase 1 live DB/API proof.

Exercises the real FastAPI routers on a migrated PostgreSQL test database:
Forecast → MPS aggregate → approve/promote to planned supply → CRP → ATP.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4


from .conftest import requires_db
# Re-export the api_client / auth / seeded_db fixtures defined in test_api_db.py
# so this module sees them via pytest's fixture resolution. Without this import
# (or moving the fixtures into conftest.py), pytest errors with
# "fixture 'api_client' not found" when this file is collected on its own —
# which is exactly how CI invokes it. F401 is intentional: the imports are
# the side effect we want.
from .test_api_db import api_client, auth, seeded_db  # noqa: F401

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


@requires_db
def test_phase1_forecast_mps_crp_atp_rest_e2e(api_client, auth, seeded_db):
    """Prove the Phase 1 chain works through REST endpoints on PostgreSQL."""
    import psycopg
    from psycopg.rows import dict_row

    today = date.today()
    item_external_id = f"E2E-FG-{uuid4().hex[:8]}"
    location_external_id = f"E2E-PLANT-{uuid4().hex[:8]}"
    item_id = uuid4()
    location_id = uuid4()
    work_center_id = uuid4()
    routing_id = uuid4()
    operation_id = uuid4()

    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        # Deterministic master data for this test only.
        conn.execute(
            """
            INSERT INTO items (item_id, name, item_type, uom, status, external_id)
            VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)
            """,
            (item_id, "Phase 1 E2E Finished Good", item_external_id),
        )
        conn.execute(
            """
            INSERT INTO locations (location_id, name, location_type, country, external_id)
            VALUES (%s, %s, 'plant', 'US', %s)
            """,
            (location_id, "Phase 1 E2E Plant", location_external_id),
        )

        # Forecast generation needs historical demand from graph demand nodes.
        historical = []
        for i in range(14, 0, -1):
            demand_date = today - timedelta(days=i)
            historical.append((
                uuid4(),
                "CustomerOrderDemand",
                BASELINE_SCENARIO_ID,
                item_id,
                location_id,
                Decimal("10"),
                "EA",
                "day",
                demand_date,
                demand_date,
                demand_date + timedelta(days=1),
                True,
            ))
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO nodes (
                    node_id, node_type, scenario_id, item_id, location_id,
                    quantity, qty_uom, time_grain, time_ref, time_span_start,
                    time_span_end, active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                historical,
            )

        # Migration 034 (ADR-014 D1+D2): work_centers merged into resources.
        # Maps: work_center_id→resource_id, code→external_id, description→name.
        # routing_operations.work_center_id renamed to resource_id.
        conn.execute(
            """
            INSERT INTO resources (
                resource_id, external_id, name, resource_type,
                capacity_per_day, capacity_unit, efficiency, active
            )
            VALUES (%s, %s, 'Phase 1 E2E Cell', 'work_center', 80, 'unit', 1.0, true)
            """,
            (work_center_id, f"E2E-WC-{uuid4().hex[:8]}"),
        )
        conn.execute(
            """
            INSERT INTO routings (routing_id, item_id, sequence, description, active)
            VALUES (%s, %s, 1, 'Phase 1 E2E routing', true)
            """,
            (routing_id, item_id),
        )
        conn.execute(
            """
            INSERT INTO routing_operations (
                operation_id, routing_id, sequence, resource_id,
                setup_time, run_time_per_unit, description, active
            ) VALUES (%s, %s, 10, %s, 2, 0.5, 'Assemble', true)
            """,
            (operation_id, routing_id, work_center_id),
        )
        conn.commit()

    forecast_resp = api_client.post(
        "/v1/demand/forecast/generate",
        headers=auth,
        json={
            "item_id": item_external_id,
            "location_id": location_external_id,
            "horizon_days": 21,
            "granularity": "daily",
            "method": "MA",
            "scenario_id": "baseline",
        },
    )
    assert forecast_resp.status_code == 200, forecast_resp.text
    forecast = forecast_resp.json()
    assert len(forecast["values"]) == 21
    assert Decimal(str(forecast["total_quantity"])) > 0

    horizon_start = today + timedelta(days=1)
    horizon_end = horizon_start + timedelta(days=20)
    mps_resp = api_client.post(
        "/v1/mps/aggregate-demand",
        headers=auth,
        json={
            "item_id": item_external_id,
            "location_id": location_external_id,
            "scenario_id": "baseline",
            "horizon_start": horizon_start.isoformat(),
            "horizon_end": horizon_end.isoformat(),
            "time_grain": "weekly",
            "forecast_weight": "1.0",
            "orders_weight": "0.0",
            "clear_existing": True,
        },
    )
    assert mps_resp.status_code == 200, mps_resp.text
    mps = mps_resp.json()
    assert mps["mps_nodes_created"] >= 1
    assert Decimal(str(mps["total_demand"])) > 0
    mps_id = mps["mps_node_ids"][0]

    approve_resp = api_client.post(
        f"/v1/mps/{mps_id}/approve",
        headers=auth,
        json={"approved_by": "integration-test", "notes": "Phase 1 E2E approval"},
    )
    assert approve_resp.status_code == 200, approve_resp.text
    approved = approve_resp.json()
    assert approved["previous_status"] == "DRAFT"
    assert approved["status"] == "APPROVED"

    promote_resp = api_client.post(
        f"/v1/mps/{mps_id}/promote-to-mrp",
        headers=auth,
        json={"explode_components": False, "dry_run": False, "run_crp": False},
    )
    assert promote_resp.status_code == 200, promote_resp.text
    promoted = promote_resp.json()
    assert promoted["planned_supplies_created"] == 1
    assert promoted["status"] == "RELEASED"

    # Make promoted supply available to ATP and CRP status filters.
    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        conn.execute(
            "UPDATE planned_supply SET status = 'RELEASED' WHERE source_id = %s",
            (mps_id,),
        )
        conn.commit()

    crp_resp = api_client.post(
        "/v1/crp/calculate",
        headers=auth,
        json={"horizon_days": 30, "scenario_id": BASELINE_SCENARIO_ID},
    )
    assert crp_resp.status_code == 200, crp_resp.text
    crp = crp_resp.json()
    assert crp["planned_orders_count"] >= 1
    assert crp["work_centers_count"] >= 1
    assert crp["load_profiles"], "CRP should generate a load profile from the promoted MPS supply"

    atp_resp = api_client.post(
        "/v1/atp/check",
        headers=auth,
        json={
            "item_id": item_external_id,
            "location_id": location_external_id,
            "quantity": "5",
            "requested_date": horizon_start.isoformat(),
            "horizon_days": 30,
        },
    )
    assert atp_resp.status_code == 200, atp_resp.text
    atp = atp_resp.json()
    assert Decimal(str(atp["quantity_available"])) >= 0
    assert atp["requested_quantity"] == "5"
    assert atp["buckets"], "ATP should return daily buckets for the request horizon"
