"""Executable Phase 1 demo flow.

Runs the validated Forecast -> MPS -> Approve -> MRP -> CRP -> ATP chain against
PostgreSQL using the real FastAPI routers. The seed data is unique per run so the
flow can be triggered repeatedly from the demo UI.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


def _post(client, path: str, auth: dict[str, str], payload: dict) -> dict:
    response = client.post(path, headers=auth, json=payload)
    if response.status_code != 200:
        raise RuntimeError(f"POST {path} failed: {response.status_code} {response.text}")
    return response.json()


def run_phase1_demo(database_url: str, token: str) -> dict:
    """Run the Phase 1 demo chain and return compact proof metrics."""
    import psycopg
    from fastapi.testclient import TestClient
    from psycopg.rows import dict_row

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    # Apply migrations using the same DB wrapper as production/tests.
    OotilsDB(database_url)

    app = create_app()

    def override_db():
        db = OotilsDB(database_url)
        with db.conn() as conn:
            yield conn

    app.dependency_overrides[get_db] = override_db

    today = date.today()
    item_external_id = f"DEMO-FG-{uuid4().hex[:8]}"
    location_external_id = f"DEMO-PLANT-{uuid4().hex[:8]}"
    item_id = uuid4()
    location_id = uuid4()
    work_center_id = uuid4()
    routing_id = uuid4()
    operation_id = uuid4()

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        conn.execute(
            """
            INSERT INTO items (item_id, name, item_type, uom, status, external_id)
            VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)
            """,
            (item_id, "Demo Finished Good", item_external_id),
        )
        conn.execute(
            """
            INSERT INTO locations (location_id, name, location_type, country, external_id)
            VALUES (%s, %s, 'plant', 'US', %s)
            """,
            (location_id, "Demo Plant", location_external_id),
        )

        historical = []
        for days_ago in range(28, 0, -1):
            demand_date = today - timedelta(days=days_ago)
            qty = Decimal("12") + Decimal(days_ago % 5)
            historical.append((
                uuid4(), "CustomerOrderDemand", BASELINE_SCENARIO_ID,
                item_id, location_id, qty, "EA", "day", demand_date,
                demand_date, demand_date + timedelta(days=1), True,
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

        conn.execute(
            """
            INSERT INTO work_centers (work_center_id, code, description, capacity_per_day, efficiency, active)
            VALUES (%s, %s, 'Demo assembly cell', 80, 1.0, true)
            """,
            (work_center_id, f"DEMO-WC-{uuid4().hex[:8]}"),
        )
        conn.execute(
            """
            INSERT INTO routings (routing_id, item_id, sequence, description, active)
            VALUES (%s, %s, 1, 'Demo routing', true)
            """,
            (routing_id, item_id),
        )
        conn.execute(
            """
            INSERT INTO routing_operations (
                operation_id, routing_id, sequence, work_center_id,
                setup_time, run_time_per_unit, description, active
            ) VALUES (%s, %s, 10, %s, 2, 0.5, 'Assemble', true)
            """,
            (operation_id, routing_id, work_center_id),
        )
        conn.commit()

    auth = {"Authorization": f"Bearer {token}"}
    horizon_start = today + timedelta(days=1)
    horizon_end = horizon_start + timedelta(days=20)

    with TestClient(app) as client:
        forecast = _post(client, "/v1/demand/forecast/generate", auth, {
            "item_id": item_external_id,
            "location_id": location_external_id,
            "horizon_days": 21,
            "granularity": "daily",
            "method": "MA",
            "scenario_id": "baseline",
        })

        mps = _post(client, "/v1/mps/aggregate-demand", auth, {
            "item_id": item_external_id,
            "location_id": location_external_id,
            "scenario_id": "baseline",
            "horizon_start": horizon_start.isoformat(),
            "horizon_end": horizon_end.isoformat(),
            "time_grain": "weekly",
            "forecast_weight": "1.0",
            "orders_weight": "0.0",
            "clear_existing": True,
        })
        mps_id = mps["mps_node_ids"][0]

        approval = _post(client, f"/v1/mps/{mps_id}/approve", auth, {
            "approved_by": "phase1-demo",
            "notes": "Approved by Phase 1 demo flow",
        })

        promoted = _post(client, f"/v1/mps/{mps_id}/promote-to-mrp", auth, {
            "explode_components": False,
            "dry_run": False,
            "run_crp": False,
        })

        # CRP consumes released planned supply. This is deterministic demo setup;
        # the approval/promotion workflow itself remains API-driven.
        with psycopg.connect(database_url, row_factory=dict_row) as conn:
            conn.execute("UPDATE planned_supply SET status = 'RELEASED' WHERE source_id = %s", (mps_id,))
            conn.commit()

        crp = _post(client, "/v1/crp/calculate", auth, {
            "horizon_days": 30,
            "scenario_id": BASELINE_SCENARIO_ID,
        })

        atp = _post(client, "/v1/atp/check", auth, {
            "item_id": item_external_id,
            "location_id": location_external_id,
            "quantity": "5",
            "requested_date": horizon_start.isoformat(),
            "horizon_days": 30,
        })

    app.dependency_overrides.clear()

    return {
        "status": "ok",
        "item_external_id": item_external_id,
        "location_external_id": location_external_id,
        "forecast": {
            "buckets": len(forecast["values"]),
            "total_quantity": forecast["total_quantity"],
            "method": forecast.get("method"),
        },
        "mps": {
            "mps_nodes_created": mps["mps_nodes_created"],
            "total_demand": mps["total_demand"],
            "first_mps_id": mps_id,
        },
        "approval": {
            "previous_status": approval["previous_status"],
            "status": approval["status"],
        },
        "mrp_promotion": {
            "status": promoted["status"],
            "planned_supplies_created": promoted["planned_supplies_created"],
        },
        "crp": {
            "planned_orders_count": crp["planned_orders_count"],
            "work_centers_count": crp["work_centers_count"],
            "load_profiles": len(crp["load_profiles"]),
        },
        "atp": {
            "requested_quantity": atp["requested_quantity"],
            "quantity_available": atp["quantity_available"],
            "buckets": len(atp["buckets"]),
        },
    }


def run_phase1_demo_from_env() -> dict:
    """Run demo using production-style environment variables."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    token = os.environ.get("OOTILS_API_TOKEN") or "phase1-demo-token"
    return run_phase1_demo(database_url, token)
