"""
Scenario-isolation integration tests for ATP/CTP and RCCP (#338).

Before the fix, the ATP engine queries on ``planned_supply`` /
``customer_order_demand`` and the RCCP/CTP load queries on ``nodes`` had no
``scenario_id`` predicate — a fork that wrote supply/demand contaminated the
baseline answers (and vice-versa). These tests seed a baseline dataset plus a
fork scenario with *extra* supplies/demands and assert:

  (a) the baseline response does NOT include the fork rows;
  (b) the response with ``?scenario_id=<fork>`` (or ``X-Scenario-ID`` header)
      reflects the fork rows only.

Runs against a real PostgreSQL database (no mocks), on a fresh migrated DB —
no demo seed needed, every row is created (and cleaned up) locally.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}
BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


# ---------------------------------------------------------------------------
# Shared module-scoped fixtures (mirror tests/integration/test_atp_api_integration.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from fastapi.testclient import TestClient

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def auth():
    return AUTH_HEADERS


@pytest.fixture(scope="module")
def fork_scenario_id(migrated_db) -> str:
    """Create a fork scenario row (parent = baseline), return its id."""
    import psycopg
    from psycopg.rows import dict_row

    scenario_id = uuid4()
    with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
        conn.execute(
            """
            INSERT INTO scenarios (scenario_id, name, parent_scenario_id)
            VALUES (%s, %s, %s::UUID)
            """,
            (scenario_id, "atp-rccp-isolation-fork", BASELINE_SCENARIO_ID),
        )
        conn.commit()
    return str(scenario_id)


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


def _insert_item_and_location(conn) -> tuple[UUID, UUID]:
    """Create a unique item + location, return their ids."""
    item_id = uuid4()
    location_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"Iso Test Item {item_id}"),
    )
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"Iso Test Loc {location_id}"),
    )
    return item_id, location_id


def _seed_on_hand(conn, *, item_id, location_id, quantity, as_of_date) -> UUID:
    on_hand_id = uuid4()
    conn.execute(
        """
        INSERT INTO on_hand_supply (on_hand_id, item_id, location_id, quantity, as_of_date)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (on_hand_id, item_id, location_id, quantity, as_of_date),
    )
    return on_hand_id


def _seed_planned_supply(
    conn, *, item_id, location_id, quantity, due_date, scenario_id=BASELINE_SCENARIO_ID
) -> UUID:
    ps_id = uuid4()
    conn.execute(
        """
        INSERT INTO planned_supply
            (planned_supply_id, item_id, location_id, scenario_id, quantity, due_date, status, priority)
        VALUES (%s, %s, %s, %s::UUID, %s, %s, 'RELEASED', 0)
        """,
        (ps_id, item_id, location_id, scenario_id, quantity, due_date),
    )
    return ps_id


def _seed_demand(
    conn, *, item_id, location_id, quantity, requested_date, scenario_id=BASELINE_SCENARIO_ID
) -> UUID:
    cod_id = uuid4()
    conn.execute(
        """
        INSERT INTO customer_order_demand
            (customer_order_demand_id, item_id, location_id, scenario_id, quantity,
             requested_date, status, priority, is_committed)
        VALUES (%s, %s, %s, %s::UUID, %s, %s, 'CONFIRMED', 0, TRUE)
        """,
        (cod_id, item_id, location_id, scenario_id, quantity, requested_date),
    )
    return cod_id


def _teardown_atp_rows(conn, *, item_id, location_id) -> None:
    """Delete every ATP row written for this item/location."""
    conn.execute("DELETE FROM customer_order_demand WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM planned_supply WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM on_hand_supply WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _insert_resource_with_node(conn, *, external_id: str, scenario_id: str) -> dict:
    """Insert a resources row + a 'Resource' graph node in the given scenario."""
    resource_id = uuid4()
    conn.execute(
        """
        INSERT INTO resources
            (resource_id, external_id, name, resource_type, capacity_per_day, capacity_unit)
        VALUES (%s, %s, 'Iso Test Resource', 'machine', 100.0, 'unit')
        """,
        (resource_id, external_id),
    )
    node_id = _insert_resource_node(conn, external_id=external_id, scenario_id=scenario_id)
    return {"resource_id": resource_id, "node_id": node_id}


def _insert_resource_node(conn, *, external_id: str, scenario_id: str) -> UUID:
    """Insert only a 'Resource' graph node (fork copy of an existing resource)."""
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (node_id, node_type, scenario_id, external_id, active)
        VALUES (%s, 'Resource', %s::UUID, %s, TRUE)
        """,
        (node_id, scenario_id, external_id),
    )
    return node_id


def _insert_supply_node_with_edge(
    conn,
    *,
    node_type: str,
    item_id,
    time_ref: date,
    quantity: float,
    resource_node_id,
    scenario_id: str,
) -> UUID:
    """Insert a supply node + consumes_resource edge, both in the given scenario."""
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, quantity, time_grain, time_ref, active)
        VALUES (%s, %s, %s::UUID, %s, %s, 'exact_date', %s, TRUE)
        """,
        (node_id, node_type, scenario_id, item_id, quantity, time_ref),
    )
    conn.execute(
        """
        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
        VALUES (%s, 'consumes_resource', %s, %s, %s::UUID, TRUE)
        """,
        (uuid4(), node_id, resource_node_id, scenario_id),
    )
    return node_id


def _teardown_rccp_rows(conn, *, external_id: str, item_id, location_id) -> None:
    """Delete the resource, its graph nodes (all scenarios), edges and supply nodes."""
    node_rows = conn.execute(
        "SELECT node_id FROM nodes WHERE node_type = 'Resource' AND external_id = %s",
        (external_id,),
    ).fetchall()
    for nr in node_rows:
        supply_rows = conn.execute(
            """
            SELECT n.node_id FROM nodes n
            JOIN edges e ON e.from_node_id = n.node_id
            WHERE e.edge_type = 'consumes_resource' AND e.to_node_id = %s
            """,
            (nr["node_id"],),
        ).fetchall()
        for sr in supply_rows:
            conn.execute("DELETE FROM edges WHERE from_node_id = %s", (sr["node_id"],))
            conn.execute("DELETE FROM nodes WHERE node_id = %s", (sr["node_id"],))
        conn.execute(
            "DELETE FROM edges WHERE from_node_id = %s OR to_node_id = %s",
            (nr["node_id"], nr["node_id"]),
        )
        conn.execute("DELETE FROM nodes WHERE node_id = %s", (nr["node_id"],))
    conn.execute("DELETE FROM resources WHERE external_id = %s", (external_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _stable(body: dict) -> dict:
    """Strip the timing field so two identical calculations compare equal."""
    return {k: v for k, v in body.items() if k != "calculation_time_ms"}


# ---------------------------------------------------------------------------
# ATP / CTP — baseline vs fork isolation
# ---------------------------------------------------------------------------


class TestATPScenarioIsolation:
    """POST /v1/atp/check and /v1/ctp/check must be scenario-scoped."""

    def _seed(self, migrated_db, fork_scenario_id):
        """Baseline: on_hand 100 + demand 20@D+1. Fork: +supply 50@D+5, +demand 100@D+1."""
        import psycopg
        from psycopg.rows import dict_row

        today = date.today()
        with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
            item_id, location_id = _insert_item_and_location(conn)
            _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                          quantity=100, as_of_date=today)
            _seed_demand(conn, item_id=item_id, location_id=location_id,
                         quantity=20, requested_date=today + timedelta(days=1))
            # Fork-only rows — must NOT leak into baseline answers
            _seed_planned_supply(conn, item_id=item_id, location_id=location_id,
                                 quantity=50, due_date=today + timedelta(days=5),
                                 scenario_id=fork_scenario_id)
            _seed_demand(conn, item_id=item_id, location_id=location_id,
                         quantity=100, requested_date=today + timedelta(days=1),
                         scenario_id=fork_scenario_id)
            conn.commit()
        return item_id, location_id, today

    def test_atp_baseline_ignores_fork_and_fork_sees_own_rows(
        self, api_client, auth, migrated_db, fork_scenario_id
    ):
        import psycopg
        from psycopg.rows import dict_row

        item_id, location_id, today = self._seed(migrated_db, fork_scenario_id)
        payload = {
            "item_id": str(item_id),
            "location_id": str(location_id),
            "quantity": 150,
            "requested_date": today.isoformat(),
            "horizon_days": 30,
        }
        try:
            # (a) Baseline (no scenario param): on_hand 100 - demand 20 = 80.
            # The fork's +50 supply and +100 demand must be invisible.
            resp = api_client.post("/v1/atp/check", json=payload, headers=auth)
            assert resp.status_code == 200, resp.text
            baseline = resp.json()
            assert baseline["available"] is False
            assert float(baseline["quantity_available"]) == 80.0
            assert float(baseline["backorder_quantity"]) == 70.0

            # (b) Fork: on_hand 100 (shared snapshot) - fork demand 100 + fork
            # supply 50 = 50 available at D+5.
            resp = api_client.post(
                "/v1/atp/check", json=payload,
                params={"scenario_id": fork_scenario_id}, headers=auth,
            )
            assert resp.status_code == 200, resp.text
            fork = resp.json()
            assert fork["available"] is False
            assert float(fork["quantity_available"]) == 50.0
            assert float(fork["backorder_quantity"]) == 100.0

            # X-Scenario-ID header is equivalent to the query param
            # (calculation_time_ms is timing noise — excluded from comparison)
            resp = api_client.post(
                "/v1/atp/check", json=payload,
                headers={**auth, "X-Scenario-ID": fork_scenario_id},
            )
            assert resp.status_code == 200, resp.text
            assert _stable(resp.json()) == _stable(fork)

            # Baseline answer is unchanged after fork reads (no contamination)
            resp = api_client.post("/v1/atp/check", json=payload, headers=auth)
            assert resp.status_code == 200, resp.text
            assert _stable(resp.json()) == _stable(baseline)
        finally:
            with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
                _teardown_atp_rows(conn, item_id=item_id, location_id=location_id)
                conn.commit()

    def test_ctp_check_is_scenario_scoped(
        self, api_client, auth, migrated_db, fork_scenario_id
    ):
        """CTP wraps ATP — same isolation contract on the material side."""
        import psycopg
        from psycopg.rows import dict_row

        item_id, location_id, today = self._seed(migrated_db, fork_scenario_id)
        payload = {
            "item_id": str(item_id),
            "location_id": str(location_id),
            "quantity": 150,
            "requested_date": today.isoformat(),
            "horizon_days": 30,
            "include_capacity": True,
        }
        try:
            resp = api_client.post("/v1/ctp/check", json=payload, headers=auth)
            assert resp.status_code == 200, resp.text
            assert float(resp.json()["quantity_available"]) == 80.0

            resp = api_client.post(
                "/v1/ctp/check", json=payload,
                params={"scenario_id": fork_scenario_id}, headers=auth,
            )
            assert resp.status_code == 200, resp.text
            assert float(resp.json()["quantity_available"]) == 50.0
        finally:
            with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
                _teardown_atp_rows(conn, item_id=item_id, location_id=location_id)
                conn.commit()

    def test_atp_invalid_scenario_id_is_422(self, api_client, auth):
        payload = {
            "item_id": str(uuid4()),
            "location_id": str(uuid4()),
            "quantity": 1,
            "requested_date": date.today().isoformat(),
        }
        resp = api_client.post(
            "/v1/atp/check", json=payload,
            params={"scenario_id": "not-a-uuid"}, headers=auth,
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# RCCP — baseline vs fork isolation
# ---------------------------------------------------------------------------


class TestRCCPScenarioIsolation:
    """GET /v1/rccp/{resource} load aggregation must be scenario-scoped."""

    def test_rccp_load_baseline_vs_fork(
        self, api_client, auth, migrated_db, fork_scenario_id
    ):
        import psycopg
        from psycopg.rows import dict_row

        ext = f"RES-ISO-{uuid4().hex[:6]}"
        load_date = date(2026, 1, 13)
        with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
            item_id, location_id = _insert_item_and_location(conn)
            # Baseline: resources row + Resource node + WorkOrderSupply 300
            ids = _insert_resource_with_node(
                conn, external_id=ext, scenario_id=BASELINE_SCENARIO_ID
            )
            _insert_supply_node_with_edge(
                conn, node_type="WorkOrderSupply", item_id=item_id,
                time_ref=load_date, quantity=300.0,
                resource_node_id=ids["node_id"], scenario_id=BASELINE_SCENARIO_ID,
            )
            # Fork: Resource node copy + extra PlannedSupply 400 (fork-only)
            fork_node_id = _insert_resource_node(
                conn, external_id=ext, scenario_id=fork_scenario_id
            )
            _insert_supply_node_with_edge(
                conn, node_type="PlannedSupply", item_id=item_id,
                time_ref=load_date, quantity=400.0,
                resource_node_id=fork_node_id, scenario_id=fork_scenario_id,
            )
            conn.commit()

        params = {
            "from_date": load_date.isoformat(),
            "to_date": load_date.isoformat(),
            "grain": "day",
        }
        try:
            # (a) Baseline: load = 300 only — the fork's 400 must not be summed
            # (pre-fix this returned 700).
            resp = api_client.get(f"/v1/rccp/{ext}", params=params, headers=auth)
            assert resp.status_code == 200, resp.text
            assert resp.json()["buckets"][0]["load"] == 300.0

            # (b) Fork: load = 400 only (the fork graph is self-contained).
            resp = api_client.get(
                f"/v1/rccp/{ext}",
                params={**params, "scenario_id": fork_scenario_id},
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            assert resp.json()["buckets"][0]["load"] == 400.0

            # X-Scenario-ID header path
            resp = api_client.get(
                f"/v1/rccp/{ext}", params=params,
                headers={**auth, "X-Scenario-ID": fork_scenario_id},
            )
            assert resp.status_code == 200, resp.text
            assert resp.json()["buckets"][0]["load"] == 400.0
        finally:
            with psycopg.connect(migrated_db, row_factory=dict_row) as conn:
                _teardown_rccp_rows(
                    conn, external_id=ext, item_id=item_id, location_id=location_id
                )
                conn.commit()

    def test_rccp_invalid_scenario_id_is_422(self, api_client, auth):
        resp = api_client.get(
            "/v1/rccp/ANY-RES",
            params={"grain": "day", "scenario_id": "not-a-uuid"},
            headers=auth,
        )
        assert resp.status_code == 422
