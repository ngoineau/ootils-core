"""
Integration tests for the RCCP FastAPI router against a real PostgreSQL
database (no mocks).

Ported from tests/test_router_rccp.py — every test that previously mocked
``conn.execute()`` for resources / load aggregation / capacity overrides /
operational_calendars queries is re-implemented here against a real DB.

The seed (scripts/seed_demo_data.py) does NOT include any `resources`
rows — RCCP work-center data lives outside the demo seed. Each test that
needs a resource inserts it locally with a unique external_id and cleans
up afterwards.

The pure-helper tests (`_bucket_start`, `_generate_buckets`, …) remain in
the slim tests/test_router_rccp.py file — no DB required there.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from uuid import uuid4

import pytest

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

SEED_SCRIPT = Path(__file__).parents[2] / "scripts" / "seed_demo_data.py"
AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}
BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


def _run_seed():
    env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    return subprocess.run(
        [sys.executable, str(SEED_SCRIPT)],
        capture_output=True, text=True, env=env,
    )


# ---------------------------------------------------------------------------
# Shared module-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def seeded_db(migrated_db):
    result = _run_seed()
    if result.returncode != 0:
        pytest.skip(f"Seed failed: {result.stderr[:500]}")
    return migrated_db


@pytest.fixture(scope="module")
def api_client(seeded_db):
    os.environ["DATABASE_URL"] = seeded_db
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from fastapi.testclient import TestClient

    app = create_app()

    def override_db():
        db = OotilsDB(seeded_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture(scope="module")
def auth():
    return AUTH_HEADERS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _insert_resource(
    conn,
    *,
    external_id: str,
    capacity_per_day: float = 100.0,
    resource_type: str = "machine",
    location_id=None,
) -> dict:
    """Insert a resource row + corresponding 'Resource' graph node."""
    resource_id = uuid4()
    conn.execute(
        """
        INSERT INTO resources
            (resource_id, external_id, name, resource_type, capacity_per_day, capacity_unit, location_id)
        VALUES (%s, %s, 'Test Resource', %s, %s, 'hours', %s)
        """,
        (resource_id, external_id, resource_type, capacity_per_day, location_id),
    )
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (node_id, node_type, scenario_id, external_id, active)
        VALUES (%s, 'Resource', %s::UUID, %s, TRUE)
        """,
        (node_id, BASELINE_SCENARIO_ID, external_id),
    )
    return {"resource_id": resource_id, "node_id": node_id}


def _insert_work_order_supply(
    conn, *, item_id: str, time_ref: date, quantity: float, resource_node_id
) -> str:
    """Insert a WorkOrderSupply node + consumes_resource edge → returns node_id."""
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, quantity, time_grain, time_ref, active)
        VALUES (%s, 'WorkOrderSupply', %s::UUID, %s, %s, 'exact_date', %s, TRUE)
        """,
        (node_id, BASELINE_SCENARIO_ID, item_id, quantity, time_ref),
    )
    conn.execute(
        """
        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
        VALUES (%s, 'consumes_resource', %s, %s, %s::UUID, TRUE)
        """,
        (uuid4(), node_id, resource_node_id, BASELINE_SCENARIO_ID),
    )
    return str(node_id)


def _cleanup_resource(conn, external_id: str):
    """Remove the resource + its graph node + capacity overrides + edges + supply nodes."""
    # Find resource and node
    res_row = conn.execute(
        "SELECT resource_id FROM resources WHERE external_id = %s", (external_id,)
    ).fetchone()
    node_row = conn.execute(
        "SELECT node_id FROM nodes WHERE node_type = 'Resource' AND external_id = %s",
        (external_id,),
    ).fetchone()

    if res_row:
        conn.execute(
            "DELETE FROM resource_capacity_overrides WHERE resource_id = %s",
            (res_row["resource_id"],),
        )

    if node_row:
        node_id = node_row["node_id"]
        # Edges referencing this resource node
        # Delete supply nodes that have a consumes_resource edge to this resource
        rows = conn.execute(
            """
            SELECT n.node_id FROM nodes n
            JOIN edges e ON e.from_node_id = n.node_id
            WHERE e.edge_type = 'consumes_resource'
              AND e.to_node_id = %s
              AND n.node_type IN ('WorkOrderSupply', 'PlannedSupply')
            """,
            (node_id,),
        ).fetchall()
        for r in rows:
            conn.execute("DELETE FROM edges WHERE from_node_id = %s", (r["node_id"],))
            conn.execute("DELETE FROM nodes WHERE node_id = %s", (r["node_id"],))
        # Delete remaining edges to/from the resource node
        conn.execute(
            "DELETE FROM edges WHERE from_node_id = %s OR to_node_id = %s",
            (node_id, node_id),
        )
        conn.execute("DELETE FROM nodes WHERE node_id = %s", (node_id,))

    if res_row:
        conn.execute(
            "DELETE FROM resources WHERE resource_id = %s",
            (res_row["resource_id"],),
        )


def _seed_pump_item_id(conn) -> str:
    return str(
        conn.execute("SELECT item_id FROM items WHERE external_id = 'PUMP-01'")
        .fetchone()["item_id"]
    )


# ---------------------------------------------------------------------------
# Endpoint — error paths
# ---------------------------------------------------------------------------


class TestRCCPEndpointErrors:
    def test_rccp_resource_not_found(self, api_client, auth):
        resp = api_client.get(
            "/v1/rccp/NO-SUCH-RESOURCE-XYZ",
            params={"from_date": "2026-01-13", "to_date": "2026-01-19"},
            headers=auth,
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Endpoint — happy paths against real DB
# ---------------------------------------------------------------------------


class TestRCCPEndpointDefaults:
    def test_default_dates_returns_buckets(self, api_client, auth, seeded_db):
        """No from_date/to_date → defaults today / +12 weeks. month grain."""
        import psycopg
        from psycopg.rows import dict_row

        ext = f"RES-DEF-{uuid4().hex[:6]}"
        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            _insert_resource(conn, external_id=ext, capacity_per_day=100.0)
            conn.commit()
        try:
            resp = api_client.get(
                f"/v1/rccp/{ext}",
                params={"grain": "month"},
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["resource"]["external_id"] == ext
            assert len(body["buckets"]) >= 1
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_resource(conn, ext)
                conn.commit()


class TestRCCPEndpointWithLoad:
    def test_load_and_overload_no_location(self, api_client, auth, seeded_db):
        """One-week bucket, no location. Load > capacity → overloaded.

        capacity_per_day=100, Mon→Fri = 5 weekdays × 100 = 500 capacity.
        Load = 300 + 400 = 700 → 140% → overloaded.
        """
        import psycopg
        from psycopg.rows import dict_row

        ext = f"RES-LOAD-{uuid4().hex[:6]}"
        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            ids = _insert_resource(conn, external_id=ext, capacity_per_day=100.0)
            pump_id = _seed_pump_item_id(conn)
            # Add two WorkOrderSupply rows in the window
            _insert_work_order_supply(
                conn,
                item_id=pump_id,
                time_ref=date(2026, 1, 13),  # Monday
                quantity=300.0,
                resource_node_id=ids["node_id"],
            )
            _insert_work_order_supply(
                conn,
                item_id=pump_id,
                time_ref=date(2026, 1, 15),  # Wednesday
                quantity=400.0,
                resource_node_id=ids["node_id"],
            )
            conn.commit()

        try:
            resp = api_client.get(
                f"/v1/rccp/{ext}",
                params={
                    "from_date": "2026-01-13",
                    "to_date": "2026-01-17",
                    "grain": "week",
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert len(body["buckets"]) == 1
            b = body["buckets"][0]
            assert b["load"] == 700.0
            assert b["capacity"] == 500.0
            assert b["utilization_pct"] == 140.0
            assert b["overloaded"] is True
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_resource(conn, ext)
                conn.commit()

    def test_capacity_override_path(self, api_client, auth, seeded_db):
        """Override day uses override capacity instead of weekday heuristic."""
        import psycopg
        from psycopg.rows import dict_row

        ext = f"RES-OVR-{uuid4().hex[:6]}"
        override_date = date(2026, 1, 13)  # Monday
        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            ids = _insert_resource(conn, external_id=ext, capacity_per_day=100.0)
            pump_id = _seed_pump_item_id(conn)
            _insert_work_order_supply(
                conn,
                item_id=pump_id,
                time_ref=override_date,
                quantity=50.0,
                resource_node_id=ids["node_id"],
            )
            # Override Monday capacity to 50
            conn.execute(
                """
                INSERT INTO resource_capacity_overrides
                    (resource_id, override_date, capacity, reason)
                VALUES (%s, %s, 50.0, 'test-override')
                """,
                (ids["resource_id"], override_date),
            )
            conn.commit()

        try:
            resp = api_client.get(
                f"/v1/rccp/{ext}",
                params={
                    "from_date": "2026-01-13",  # Mon
                    "to_date": "2026-01-14",    # Tue
                    "grain": "day",
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert len(body["buckets"]) == 2
            # Monday: load=50, capacity=50 (override) → 100%
            assert body["buckets"][0]["capacity"] == 50.0
            assert body["buckets"][0]["load"] == 50.0
            assert body["buckets"][0]["utilization_pct"] == 100.0
            assert body["buckets"][0]["overloaded"] is False
            # Tuesday: load=0, capacity=100 (weekday)
            assert body["buckets"][1]["capacity"] == 100.0
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_resource(conn, ext)
                conn.commit()


class TestRCCPEndpointWithCalendar:
    def test_calendar_working_day_with_factor(self, api_client, auth, seeded_db):
        """location_id present → operational_calendars query.
        is_working_day=True, capacity_factor=0.5 → capacity_per_day × 0.5 added.
        """
        import psycopg
        from psycopg.rows import dict_row

        ext = f"RES-CAL-{uuid4().hex[:6]}"
        cal_date = date(2026, 1, 13)  # Monday
        try:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                # Use seeded DC-ATL location
                loc_row = conn.execute(
                    "SELECT location_id FROM locations WHERE external_id = 'DC-ATL'"
                ).fetchone()
                loc_id = loc_row["location_id"]
                _insert_resource(
                    conn,
                    external_id=ext,
                    capacity_per_day=100.0,
                    location_id=loc_id,
                )
                # Insert calendar row (clear conflict first)
                conn.execute(
                    """
                    INSERT INTO operational_calendars
                        (location_id, calendar_date, is_working_day, capacity_factor)
                    VALUES (%s, %s, TRUE, 0.5)
                    ON CONFLICT (location_id, calendar_date) DO UPDATE
                        SET is_working_day = EXCLUDED.is_working_day,
                            capacity_factor = EXCLUDED.capacity_factor
                    """,
                    (loc_id, cal_date),
                )
                conn.commit()

            resp = api_client.get(
                f"/v1/rccp/{ext}",
                params={
                    "from_date": cal_date.isoformat(),
                    "to_date": cal_date.isoformat(),
                    "grain": "day",
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["buckets"][0]["capacity"] == 50.0
            assert body["buckets"][0]["load"] == 0.0
            assert body["buckets"][0]["utilization_pct"] == 0.0
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                # Clean up the calendar row we created (restore prior state if it was different).
                # Safe to delete because the date 2026-01-13 isn't a US holiday in the seed.
                conn.execute(
                    "DELETE FROM operational_calendars WHERE calendar_date = %s",
                    (cal_date,),
                )
                _cleanup_resource(conn, ext)
                conn.commit()

    def test_calendar_non_working_day_zero_capacity(self, api_client, auth, seeded_db):
        """is_working_day=False → no capacity added."""
        import psycopg
        from psycopg.rows import dict_row

        ext = f"RES-HOL-{uuid4().hex[:6]}"
        cal_date = date(2026, 1, 13)  # Monday
        try:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                loc_row = conn.execute(
                    "SELECT location_id FROM locations WHERE external_id = 'DC-ATL'"
                ).fetchone()
                loc_id = loc_row["location_id"]
                _insert_resource(
                    conn,
                    external_id=ext,
                    capacity_per_day=100.0,
                    location_id=loc_id,
                )
                conn.execute(
                    """
                    INSERT INTO operational_calendars
                        (location_id, calendar_date, is_working_day, capacity_factor)
                    VALUES (%s, %s, FALSE, 1.0)
                    ON CONFLICT (location_id, calendar_date) DO UPDATE
                        SET is_working_day = EXCLUDED.is_working_day,
                            capacity_factor = EXCLUDED.capacity_factor
                    """,
                    (loc_id, cal_date),
                )
                conn.commit()

            resp = api_client.get(
                f"/v1/rccp/{ext}",
                params={
                    "from_date": cal_date.isoformat(),
                    "to_date": cal_date.isoformat(),
                    "grain": "day",
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["buckets"][0]["capacity"] == 0.0
            assert body["buckets"][0]["utilization_pct"] == 0.0
            assert body["buckets"][0]["overloaded"] is False
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                conn.execute(
                    "DELETE FROM operational_calendars WHERE calendar_date = %s",
                    (cal_date,),
                )
                _cleanup_resource(conn, ext)
                conn.commit()


class TestRCCPEndpointWeekend:
    def test_saturday_no_capacity(self, api_client, auth, seeded_db):
        """Saturday → weekday() >= 5 → no calendar query, 0 capacity."""
        import psycopg
        from psycopg.rows import dict_row

        ext = f"RES-SAT-{uuid4().hex[:6]}"
        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            loc_row = conn.execute(
                "SELECT location_id FROM locations WHERE external_id = 'DC-ATL'"
            ).fetchone()
            loc_id = loc_row["location_id"]
            _insert_resource(
                conn,
                external_id=ext,
                capacity_per_day=100.0,
                location_id=loc_id,
            )
            conn.commit()
        try:
            # 2026-01-17 is a Saturday
            resp = api_client.get(
                f"/v1/rccp/{ext}",
                params={
                    "from_date": "2026-01-17",
                    "to_date": "2026-01-17",
                    "grain": "day",
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["buckets"][0]["capacity"] == 0.0
            assert body["buckets"][0]["utilization_pct"] == 0.0
            assert body["buckets"][0]["overloaded"] is False
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_resource(conn, ext)
                conn.commit()


class TestRCCPResourceSerialization:
    def test_resource_with_location_id_serialized(self, api_client, auth, seeded_db):
        """resource.location_id appears in the response when set."""
        import psycopg
        from psycopg.rows import dict_row

        ext = f"RES-SER-{uuid4().hex[:6]}"
        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            loc_row = conn.execute(
                "SELECT location_id FROM locations WHERE external_id = 'DC-ATL'"
            ).fetchone()
            loc_id = loc_row["location_id"]
            _insert_resource(
                conn,
                external_id=ext,
                capacity_per_day=100.0,
                location_id=loc_id,
            )
            conn.commit()
        try:
            resp = api_client.get(
                f"/v1/rccp/{ext}",
                params={
                    "from_date": "2026-01-13",
                    "to_date": "2026-01-13",
                    "grain": "day",
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            assert resp.json()["resource"]["location_id"] == str(loc_id)
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_resource(conn, ext)
                conn.commit()


# ---------------------------------------------------------------------------
# _count_working_days against a real DB (location+calendar variants)
# ---------------------------------------------------------------------------


class TestCountWorkingDaysDB:
    """The location-specific branches of _count_working_days need a real DB."""

    def test_with_location_calendar_hit(self, api_client, auth, seeded_db):
        import psycopg
        from psycopg.rows import dict_row
        from ootils_core.api.routers.rccp import _count_working_days

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            loc_row = conn.execute(
                "SELECT location_id FROM locations WHERE external_id = 'DC-ATL'"
            ).fetchone()
            loc_id = str(loc_row["location_id"])
            # Seed 4 working days inside [2026-03-02, 2026-03-08]
            dates = [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4), date(2026, 3, 5)]
            for d in dates:
                conn.execute(
                    """
                    INSERT INTO operational_calendars
                        (location_id, calendar_date, is_working_day, capacity_factor)
                    VALUES (%s, %s, TRUE, 1.0)
                    ON CONFLICT (location_id, calendar_date) DO UPDATE
                        SET is_working_day = TRUE
                    """,
                    (loc_id, d),
                )
            conn.commit()

            try:
                count = _count_working_days(conn, loc_id, date(2026, 3, 2), date(2026, 3, 8))
                assert count == 4
            finally:
                for d in dates:
                    conn.execute(
                        "DELETE FROM operational_calendars WHERE location_id = %s AND calendar_date = %s",
                        (loc_id, d),
                    )
                conn.commit()

    def test_with_location_calendar_zero_falls_back(self, seeded_db):
        """Calendar count == 0 → Mon–Fri fallback (5 over Mon–Sun)."""
        import psycopg
        from psycopg.rows import dict_row
        from ootils_core.api.routers.rccp import _count_working_days

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            loc_row = conn.execute(
                "SELECT location_id FROM locations WHERE external_id = 'DC-ATL'"
            ).fetchone()
            loc_id = str(loc_row["location_id"])
            # No calendar rows in this date window (pick a date range that's
            # not covered by the seed's holiday list)
            result = _count_working_days(
                conn, loc_id, date(2026, 6, 1), date(2026, 6, 7)
            )
            # 2026-06-01 is a Monday → Mon..Fri = 5
            assert result == 5
