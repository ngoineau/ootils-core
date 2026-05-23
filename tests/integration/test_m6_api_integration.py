"""
Integration tests for the Sprint M6 FastAPI routers
(/v1/events, /v1/projection, /v1/issues, /v1/explain, /v1/simulate, /v1/graph)
against a real PostgreSQL database (no mocks).

Ported from tests/test_m6_api.py — every test that previously patched
GraphStore / ShortageDetector / ExplanationBuilder / ScenarioManager /
_build_propagation_engine is re-implemented here using the seeded
test database (PUMP-01 / VALVE-02 items at DC-ATL / DC-LAX locations
with pre-loaded shortages, per scripts/seed_demo_data.py).

Because we use real engines and real seeded data, assertions are
written against the response *structure* rather than against mock
return values. Specific numeric outcomes are only asserted when the
seed data unambiguously produces them.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

SEED_SCRIPT = Path(__file__).parents[2] / "scripts" / "seed_demo_data.py"
AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}
BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


def _run_seed():
    env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    return subprocess.run(
        [sys.executable, str(SEED_SCRIPT)],
        capture_output=True, text=True, env=env,
    )


# ---------------------------------------------------------------------------
# Shared module-scoped fixtures (mirror tests/integration/test_atp_api_integration.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def seeded_db(migrated_db):
    """Module-scoped: migrated DB with seed data loaded once."""
    result = _run_seed()
    if result.returncode != 0:
        pytest.skip(f"Seed failed: {result.stderr[:500]}")
    return migrated_db


@pytest.fixture(scope="module")
def api_client(seeded_db):
    """Module-scoped FastAPI TestClient bound to the real test DB."""
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


@pytest.fixture(scope="module")
def seeded_ids(seeded_db):
    """Resolve seeded PUMP-01 / DC-ATL / VALVE-02 / DC-LAX UUIDs from the DB."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        pump = conn.execute(
            "SELECT item_id FROM items WHERE external_id = 'PUMP-01'"
        ).fetchone()
        valve = conn.execute(
            "SELECT item_id FROM items WHERE external_id = 'VALVE-02'"
        ).fetchone()
        atl = conn.execute(
            "SELECT location_id FROM locations WHERE external_id = 'DC-ATL'"
        ).fetchone()
        lax = conn.execute(
            "SELECT location_id FROM locations WHERE external_id = 'DC-LAX'"
        ).fetchone()

    assert pump and valve and atl and lax, "Seed missing PUMP-01/VALVE-02/DC-ATL/DC-LAX"

    return {
        "pump_id": UUID(str(pump["item_id"])),
        "valve_id": UUID(str(valve["item_id"])),
        "atl_id": UUID(str(atl["location_id"])),
        "lax_id": UUID(str(lax["location_id"])),
    }


def _conn(seeded_db):
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(seeded_db, row_factory=dict_row)


# ---------------------------------------------------------------------------
# POST /v1/events — real DB write + propagation
# ---------------------------------------------------------------------------


class TestPostEvents:
    """POST /v1/events against a real DB."""

    def test_post_event_success(self, api_client, auth, seeded_db):
        """POST event without trigger_node_id → 202, event row persisted."""
        payload = {
            "event_type": "supply_date_changed",
            "source": "integration-test",
            "field_changed": "due_date",
            "new_date": "2026-12-01",
        }
        resp = api_client.post("/v1/events", json=payload, headers=auth)
        assert resp.status_code == 202, resp.text
        data = resp.json()
        assert data["status"] == "queued"
        assert "event_id" in data
        assert "scenario_id" in data

        # Verify the row exists in events table — and clean up
        event_id = data["event_id"]
        with _conn(seeded_db) as c:
            try:
                row = c.execute(
                    "SELECT event_type, source FROM events WHERE event_id = %s",
                    (event_id,),
                ).fetchone()
                assert row is not None
                assert row["event_type"] == "supply_date_changed"
                assert row["source"] == "integration-test"
            finally:
                c.execute("DELETE FROM events WHERE event_id = %s", (event_id,))
                c.commit()

    def test_post_event_onhand_updated(self, api_client, auth, seeded_db):
        """A different valid event_type — onhand_updated, manual source."""
        payload = {"event_type": "onhand_updated", "source": "manual"}
        resp = api_client.post("/v1/events", json=payload, headers=auth)
        assert resp.status_code == 202, resp.text
        event_id = resp.json()["event_id"]

        # Cleanup
        with _conn(seeded_db) as c:
            c.execute("DELETE FROM events WHERE event_id = %s", (event_id,))
            c.commit()


# ---------------------------------------------------------------------------
# GET /v1/projection — real GraphStore + seeded series
# ---------------------------------------------------------------------------


class TestGetProjection:
    """GET /v1/projection against a real DB."""

    def test_projection_with_external_ids(self, api_client, auth):
        """PUMP-01 @ DC-ATL has a seeded projection series with buckets."""
        resp = api_client.get(
            "/v1/projection?item_id=PUMP-01&location_id=DC-ATL",
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "series_id" in data
        assert UUID(data["series_id"])  # parseable
        assert data["item_id"] == "PUMP-01"
        assert data["location_id"] == "DC-ATL"
        assert isinstance(data["buckets"], list)
        assert len(data["buckets"]) > 0
        # Each bucket has the expected shape
        b = data["buckets"][0]
        for key in ("bucket_sequence", "opening_stock", "closing_stock",
                    "has_shortage", "shortage_qty"):
            assert key in b

    def test_projection_with_uuids(self, api_client, auth, seeded_ids):
        """Same query using UUIDs directly."""
        resp = api_client.get(
            f"/v1/projection?item_id={seeded_ids['pump_id']}"
            f"&location_id={seeded_ids['atl_id']}",
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert len(data["buckets"]) > 0

    def test_projection_not_found_unknown_item(self, api_client, auth):
        """Unknown item external_id → 404."""
        resp = api_client.get(
            "/v1/projection?item_id=NOT-A-REAL-ITEM&location_id=DC-ATL",
            headers=auth,
        )
        assert resp.status_code == 404

    def test_projection_not_found_unknown_location(self, api_client, auth):
        """Known item but unknown location → 404."""
        resp = api_client.get(
            "/v1/projection?item_id=PUMP-01&location_id=NOT-A-REAL-LOC",
            headers=auth,
        )
        assert resp.status_code == 404

    def test_projection_no_series_for_unmatched_pair(self, api_client, auth):
        """
        Valid item+location pair but no projection_series wired between them
        (PUMP-01 only has series at DC-ATL, not DC-LAX) → 404 on series lookup.
        """
        resp = api_client.get(
            "/v1/projection?item_id=PUMP-01&location_id=DC-LAX",
            headers=auth,
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /v1/issues — real ShortageDetector + seeded shortages
# ---------------------------------------------------------------------------


class TestGetIssues:
    """GET /v1/issues against a real DB."""

    def test_issues_all_severities(self, api_client, auth):
        """Seed has shortages — fetching with severity=all returns ≥1."""
        resp = api_client.get(
            "/v1/issues?severity=all&horizon_days=120",
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "issues" in data
        assert "total" in data
        assert isinstance(data["issues"], list)
        # Seeded shortages exist within 120 days
        assert data["total"] >= 1
        # Shape check on first issue
        i = data["issues"][0]
        for key in ("node_id", "shortage_qty", "severity_score", "severity",
                    "shortage_date"):
            assert key in i

    def test_issues_severity_filter(self, api_client, auth):
        """Filtering by a specific severity returns only that severity."""
        resp = api_client.get(
            "/v1/issues?severity=all&horizon_days=120",
            headers=auth,
        )
        assert resp.status_code == 200
        all_data = resp.json()
        # Pick a severity present in the seed and verify the filter behaves
        if all_data["total"] == 0:
            pytest.skip("Seed produced no shortages within horizon")
        present_severity = all_data["issues"][0]["severity"]
        filtered = api_client.get(
            f"/v1/issues?severity={present_severity}&horizon_days=120",
            headers=auth,
        )
        assert filtered.status_code == 200
        fdata = filtered.json()
        assert all(i["severity"] == present_severity for i in fdata["issues"])

    def test_issues_horizon_excludes_far_future(self, api_client, auth):
        """Very small horizon should yield <= number of issues for large horizon."""
        small = api_client.get(
            "/v1/issues?severity=all&horizon_days=1",
            headers=auth,
        )
        large = api_client.get(
            "/v1/issues?severity=all&horizon_days=365",
            headers=auth,
        )
        assert small.status_code == 200 and large.status_code == 200
        assert small.json()["total"] <= large.json()["total"]


# ---------------------------------------------------------------------------
# GET /v1/explain — real ExplanationBuilder
# ---------------------------------------------------------------------------


class TestGetExplain:
    """GET /v1/explain against a real DB."""

    def test_explain_not_found_random_node(self, api_client, auth):
        """A random (unrelated) UUID has no explanation → 404."""
        resp = api_client.get(
            f"/v1/explain?node_id={uuid4()}",
            headers=auth,
        )
        assert resp.status_code == 404

    def test_explain_shortage_node_if_explainable(self, api_client, auth, seeded_db):
        """
        For a real PI node that has a shortage, the explanation builder either:
          - returns a real Explanation (200 with causal_path), OR
          - returns None (404).
        Both outcomes are valid — assertion is shape-only on success.
        """
        with _conn(seeded_db) as c:
            row = c.execute(
                """
                SELECT node_id FROM nodes
                WHERE node_type = 'ProjectedInventory'
                  AND has_shortage = TRUE
                  AND scenario_id = %s
                  AND active = TRUE
                LIMIT 1
                """,
                (BASELINE_SCENARIO_ID,),
            ).fetchone()

        if row is None:
            pytest.skip("Seed produced no shortage PI nodes")

        node_id = row["node_id"]
        resp = api_client.get(f"/v1/explain?node_id={node_id}", headers=auth)
        assert resp.status_code in (200, 404), resp.text

        if resp.status_code == 200:
            data = resp.json()
            assert "explanation_id" in data
            assert "summary" in data
            assert "causal_path" in data
            assert isinstance(data["causal_path"], list)
            assert data["target_node_id"] == str(node_id)


# ---------------------------------------------------------------------------
# POST /v1/simulate — real ScenarioManager.create_scenario + apply_override
# ---------------------------------------------------------------------------


def _cleanup_scenario(seeded_db, scenario_id: str | UUID) -> None:
    """Remove a scenario and its dependent rows (overrides, nodes, events, etc.)."""
    sid = str(scenario_id)
    with _conn(seeded_db) as c:
        # Delete in dependency order
        c.execute("DELETE FROM scenario_overrides WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM shortages WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM explanations WHERE calc_run_id IN "
                  "(SELECT calc_run_id FROM calc_runs WHERE scenario_id = %s)", (sid,))
        c.execute("DELETE FROM calc_runs WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM events WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM edges WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM nodes WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM projection_series WHERE scenario_id = %s", (sid,))
        c.execute("DELETE FROM scenarios WHERE scenario_id = %s", (sid,))
        c.commit()


class TestPostSimulate:
    """POST /v1/simulate against a real DB."""

    def test_simulate_no_overrides_creates_scenario(self, api_client, auth, seeded_db):
        """POST /simulate with no overrides → 201, new scenario deep-copied from baseline."""
        name = f"int-empty-sim-{uuid4().hex[:8]}"
        resp = api_client.post(
            "/v1/simulate",
            json={"scenario_name": name},
            headers=auth,
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()
        assert data["status"] == "created"
        assert data["override_count"] == 0
        assert data["scenario_name"] == name
        scenario_id = data["scenario_id"]
        assert UUID(scenario_id)
        # base_scenario_id should be the baseline
        assert data["base_scenario_id"] == str(BASELINE_SCENARIO_ID)

        try:
            # Verify row exists
            with _conn(seeded_db) as c:
                row = c.execute(
                    "SELECT name, is_baseline FROM scenarios WHERE scenario_id = %s",
                    (scenario_id,),
                ).fetchone()
                assert row is not None
                assert row["name"] == name
                assert row["is_baseline"] is False
        finally:
            _cleanup_scenario(seeded_db, scenario_id)

    def test_simulate_with_quantity_override(self, api_client, auth, seeded_db, seeded_ids):
        """
        Create a scenario then apply an override on a real seeded node
        (any node with a quantity column, e.g. a SupplyNode in the seed).
        """
        # Find a real node we can override quantity on (baseline supply node).
        with _conn(seeded_db) as c:
            row = c.execute(
                """
                SELECT node_id FROM nodes
                WHERE scenario_id = %s
                  AND node_type IN ('PurchaseOrderSupply', 'CustomerOrderDemand',
                                    'WorkOrderSupply', 'WorkOrderDemand')
                  AND quantity IS NOT NULL
                  AND active = TRUE
                LIMIT 1
                """,
                (BASELINE_SCENARIO_ID,),
            ).fetchone()
        if row is None:
            pytest.skip("Seed produced no overridable supply/demand nodes")
        node_id = str(row["node_id"])

        name = f"int-qty-sim-{uuid4().hex[:8]}"
        resp = api_client.post(
            "/v1/simulate",
            json={
                "scenario_name": name,
                "overrides": [
                    {"node_id": node_id, "field_name": "quantity", "new_value": "999"}
                ],
            },
            headers=auth,
        )
        assert resp.status_code == 201, resp.text
        data = resp.json()
        scenario_id = data["scenario_id"]

        try:
            assert data["override_count"] == 1
            assert data["failed_overrides"] == []
            # Verify override row landed in scenario_overrides
            with _conn(seeded_db) as c:
                ovr = c.execute(
                    """
                    SELECT field_name, new_value FROM scenario_overrides
                    WHERE scenario_id = %s
                    """,
                    (scenario_id,),
                ).fetchall()
                assert len(ovr) == 1
                assert ovr[0]["field_name"] == "quantity"
                assert str(ovr[0]["new_value"]) == "999"
        finally:
            _cleanup_scenario(seeded_db, scenario_id)

    def test_simulate_all_overrides_fail_returns_422(self, api_client, auth, seeded_db):
        """
        Non-regression #48 (DB-backed half): if every override targets a node
        that does not exist in the new scenario, apply_override raises and
        the router returns 422 with the sanitised failed_overrides list.

        Per chantier 2 of audit 2026-05-23, the per-override 'error' field is
        a generic message (not str(exc)). We assert presence + node_id /
        field_name echo, NOT error content.
        """
        # A clearly nonexistent node_id (random UUID)
        bogus_node = str(uuid4())
        name = f"int-bad-sim-{uuid4().hex[:8]}"
        resp = api_client.post(
            "/v1/simulate",
            json={
                "scenario_name": name,
                "overrides": [
                    {
                        "node_id": bogus_node,
                        "field_name": "quantity",  # whitelisted, so Pydantic passes
                        "new_value": "10",
                    }
                ],
            },
            headers=auth,
        )
        assert resp.status_code == 422, f"Expected 422, got {resp.status_code}: {resp.text}"
        body = resp.json()
        detail = body.get("detail", {})
        assert isinstance(detail, dict), f"Expected dict detail, got: {detail}"
        assert "failed_overrides" in detail
        failed = detail["failed_overrides"]
        assert len(failed) == 1
        # node_id / field_name are echoed (so client can correlate)
        assert failed[0]["node_id"] == bogus_node
        assert failed[0]["field_name"] == "quantity"
        # error field is present + non-empty, but sanitised — no substring check
        assert failed[0]["error"]

        # Even though the response is 422, the scenario itself was created
        # before overrides were applied. Find it by name and clean up.
        with _conn(seeded_db) as c:
            row = c.execute(
                "SELECT scenario_id FROM scenarios WHERE name = %s", (name,),
            ).fetchone()
        if row is not None:
            _cleanup_scenario(seeded_db, row["scenario_id"])


# ---------------------------------------------------------------------------
# GET /v1/graph — real GraphStore + seeded nodes/edges
# ---------------------------------------------------------------------------


class TestGetGraph:
    """GET /v1/graph against a real DB."""

    def test_graph_with_uuids(self, api_client, auth, seeded_ids):
        """Graph for PUMP-01 @ DC-ATL returns nodes + edges from the seed."""
        resp = api_client.get(
            f"/v1/graph?item_id={seeded_ids['pump_id']}"
            f"&location_id={seeded_ids['atl_id']}",
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "nodes" in data
        assert "edges" in data
        assert isinstance(data["nodes"], list)
        assert isinstance(data["edges"], list)
        assert data["depth"] == 2  # default
        # PUMP-01 @ DC-ATL has projection buckets seeded → at least 1 node
        assert len(data["nodes"]) > 0

    def test_graph_with_external_id_item_404_on_unknown_location(self, api_client, auth):
        """
        The graph router only resolves item/location by name (not external_id).
        An external_id that is not also stored as 'name' → 404.
        Use a clearly bogus value to assert the 404 path.
        """
        resp = api_client.get(
            f"/v1/graph?item_id={uuid4()}&location_id=__not_a_real_location__",
            headers=auth,
        )
        # First the item UUID is parsed OK (random UUID), then location name lookup fails
        assert resp.status_code == 404

    def test_graph_default_depth(self, api_client, auth, seeded_ids):
        """Default depth=2 echoed in response."""
        resp = api_client.get(
            f"/v1/graph?item_id={seeded_ids['valve_id']}"
            f"&location_id={seeded_ids['lax_id']}",
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["depth"] == 2
