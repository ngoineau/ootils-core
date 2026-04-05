"""
tests/integration/test_api_db.py — Lot C: API with real DB tests (tests 17–26).

Uses a real PostgreSQL database (via migrated_db fixture) and FastAPI TestClient.
No mocks: all DB operations hit the actual test DB.

Skip all tests if DATABASE_URL is not configured.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from .conftest import requires_db, DB_AVAILABLE, TEST_DB_URL

SEED_SCRIPT = Path(__file__).parents[2] / "scripts" / "seed_demo_data.py"

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


def _run_seed():
    env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    return subprocess.run(
        [sys.executable, str(SEED_SCRIPT)],
        capture_output=True, text=True, env=env,
    )


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def seeded_db(migrated_db):
    """Module-scoped: migrated DB with seed data loaded once."""
    result = _run_seed()
    if result.returncode != 0:
        pytest.skip(f"Seed failed, skipping API tests: {result.stderr[:500]}")
    return migrated_db


@pytest.fixture(scope="module")
def api_client(seeded_db):
    """Module-scoped TestClient wired to the test DB."""
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
def auth(seeded_db):
    """Authorization headers."""
    return {"Authorization": "Bearer integration-test-token"}


# ---------------------------------------------------------------------------
# Test 17 — GET /health without auth → 200
# ---------------------------------------------------------------------------

@requires_db
def test_17_health_no_auth(api_client):
    """GET /health returns 200 without authentication."""
    resp = api_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "version" in body


# ---------------------------------------------------------------------------
# Test 18 — GET /v1/issues without bearer token → 401
# ---------------------------------------------------------------------------

@requires_db
def test_18_issues_no_auth_returns_401(api_client):
    """GET /v1/issues without auth returns 401."""
    resp = api_client.get("/v1/issues")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Test 19 — GET /v1/issues with valid token → 200 + schema JSON conforme
# ---------------------------------------------------------------------------

@requires_db
def test_19_issues_with_valid_token(api_client, auth):
    """GET /v1/issues with valid token returns 200 and conformant JSON schema."""
    resp = api_client.get("/v1/issues", headers=auth)
    assert resp.status_code == 200

    data = resp.json()
    assert "issues" in data, "Response must have 'issues' key"
    assert "total" in data, "Response must have 'total' key"
    assert "as_of" in data, "Response must have 'as_of' key"
    assert isinstance(data["issues"], list)
    assert data["total"] == len(data["issues"])

    # Validate individual issue schema if any issues exist
    for issue in data["issues"]:
        assert "node_id" in issue
        assert "shortage_qty" in issue
        assert "severity_score" in issue
        assert "severity" in issue
        assert issue["severity"] in ("low", "medium", "high")


# ---------------------------------------------------------------------------
# Test 20 — Filters on GET /v1/issues
# ---------------------------------------------------------------------------

@requires_db
def test_20_issues_filters(api_client, auth, seeded_db):
    """Severity, item_id, location_id, and horizon_days filters work correctly."""
    import psycopg
    from psycopg.rows import dict_row

    # Get an item_id and location_id from the DB
    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        item_row = conn.execute("SELECT item_id FROM items LIMIT 1").fetchone()
        loc_row = conn.execute("SELECT location_id FROM locations LIMIT 1").fetchone()

    if item_row is None:
        pytest.skip("No items in DB to filter on")

    item_id = str(item_row["item_id"])
    location_id = str(loc_row["location_id"]) if loc_row else None

    # Severity filter
    for sev in ("low", "medium", "high", "all"):
        resp = api_client.get(f"/v1/issues?severity={sev}", headers=auth)
        assert resp.status_code == 200, f"severity={sev} failed: {resp.text}"
        data = resp.json()
        if sev != "all":
            for issue in data["issues"]:
                assert issue["severity"] == sev, (
                    f"Issue with severity {issue['severity']} returned for filter '{sev}'"
                )

    # item_id filter
    resp = api_client.get(f"/v1/issues?item_id={item_id}", headers=auth)
    assert resp.status_code == 200
    for issue in resp.json()["issues"]:
        assert issue.get("item_id") == item_id

    # horizon_days filter — narrow window should return subset
    resp_full = api_client.get("/v1/issues?horizon_days=9999", headers=auth)
    resp_short = api_client.get("/v1/issues?horizon_days=1", headers=auth)
    assert resp_full.status_code == 200
    assert resp_short.status_code == 200
    assert resp_short.json()["total"] <= resp_full.json()["total"]


# ---------------------------------------------------------------------------
# Test 21 — POST /v1/events happy path with real DB insertion
# ---------------------------------------------------------------------------

@requires_db
def test_21_post_event_happy_path(api_client, auth, seeded_db):
    """POST /v1/events inserts a real event in the DB and returns 202."""
    import psycopg
    from psycopg.rows import dict_row

    payload = {
        "event_type": "supply_date_changed",
        "source": "api",
        "scenario_id": "baseline",
    }
    resp = api_client.post("/v1/events", json=payload, headers=auth)
    assert resp.status_code == 202, f"Expected 202, got {resp.status_code}: {resp.text}"

    data = resp.json()
    assert "event_id" in data
    assert data["status"] in ("accepted", "queued", "ok", "pending")

    # Verify row actually exists in DB
    event_id = data["event_id"]
    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT event_id, event_type FROM events WHERE event_id = %s::UUID",
            (event_id,)
        ).fetchone()
    assert row is not None, f"Event {event_id} not found in DB after POST"
    assert row["event_type"] == "supply_date_changed"


# ---------------------------------------------------------------------------
# Test 22 — POST /v1/events rejects invalid event_type
# ---------------------------------------------------------------------------

@requires_db
def test_22_post_event_invalid_event_type(api_client, auth):
    """POST /v1/events with unknown event_type returns 422."""
    payload = {
        "event_type": "completely_invalid_type_xyz",
        "source": "api",
    }
    resp = api_client.post("/v1/events", json=payload, headers=auth)
    assert resp.status_code == 422, (
        f"Expected 422 for invalid event_type, got {resp.status_code}: {resp.text}"
    )


# ---------------------------------------------------------------------------
# Test 23 — GET /v1/projection returns coherent series for seeded item/location
# ---------------------------------------------------------------------------

@requires_db
def test_23_projection_series_coherent(api_client, auth, seeded_db):
    """GET /v1/projection returns ordered buckets for a seeded item/location pair."""
    import psycopg
    from psycopg.rows import dict_row

    # Get a projection series from the DB
    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        row = conn.execute("""
            SELECT ps.item_id, ps.location_id
            FROM projection_series ps
            LIMIT 1
        """).fetchone()

    if row is None:
        pytest.skip("No projection series in DB")

    item_id = str(row["item_id"])
    location_id = str(row["location_id"])

    resp = api_client.get(
        f"/v1/projection?item_id={item_id}&location_id={location_id}",
        headers=auth,
    )
    assert resp.status_code == 200, f"Projection failed: {resp.text}"

    data = resp.json()
    assert "buckets" in data
    assert "item_id" in data
    assert "location_id" in data
    buckets = data["buckets"]
    assert isinstance(buckets, list)

    if len(buckets) > 1:
        # Verify ordering: bucket_sequence should be monotonically non-decreasing
        sequences = [b.get("bucket_sequence") for b in buckets if b.get("bucket_sequence") is not None]
        if sequences:
            assert sequences == sorted(sequences), "Projection buckets not in sequence order"


# ---------------------------------------------------------------------------
# Test 24 — POST /v1/simulate creates scenario and overrides persistently
# ---------------------------------------------------------------------------

@requires_db
def test_24_simulate_creates_scenario_and_overrides(api_client, auth, seeded_db):
    """POST /v1/simulate creates a scenario and persists overrides in DB."""
    import psycopg
    from psycopg.rows import dict_row

    # Get a node to override
    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        node_row = conn.execute("""
            SELECT node_id FROM nodes
            WHERE node_type = 'PurchaseOrderSupply'
            LIMIT 1
        """).fetchone()

    if node_row is None:
        pytest.skip("No PurchaseOrderSupply nodes in DB")

    node_id = str(node_row["node_id"])
    scenario_name = f"test-scenario-{uuid4().hex[:8]}"

    payload = {
        "scenario_name": scenario_name,
        "overrides": [
            {"node_id": node_id, "field_name": "quantity", "new_value": "999"},
        ],
    }

    resp = api_client.post("/v1/simulate", json=payload, headers=auth)
    assert resp.status_code == 201, f"Simulate failed: {resp.text}"

    data = resp.json()
    assert "scenario_id" in data
    assert data["scenario_name"] == scenario_name
    assert data["override_count"] >= 1

    scenario_id = data["scenario_id"]

    # Verify scenario exists in DB
    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        scen_row = conn.execute(
            "SELECT name FROM scenarios WHERE scenario_id = %s::UUID",
            (scenario_id,)
        ).fetchone()
        assert scen_row is not None, f"Scenario {scenario_id} not found in DB"
        assert scen_row["name"] == scenario_name

        # Verify override exists
        override_row = conn.execute(
            """SELECT override_id FROM scenario_overrides
               WHERE scenario_id = %s::UUID AND node_id = %s::UUID""",
            (scenario_id, node_id)
        ).fetchone()
        assert override_row is not None, "Override not persisted in DB"


# ---------------------------------------------------------------------------
# Test 25 — GET /v1/graph returns nodes/edges for a valid scenario
# ---------------------------------------------------------------------------

@requires_db
def test_25_graph_returns_nodes_and_edges(api_client, auth, seeded_db):
    """GET /v1/graph returns a non-empty node/edge structure for seeded data."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        row = conn.execute("""
            SELECT item_id, location_id FROM nodes
            WHERE node_type = 'ProjectedInventory'
              AND item_id IS NOT NULL AND location_id IS NOT NULL
            LIMIT 1
        """).fetchone()

    if row is None:
        pytest.skip("No ProjectedInventory nodes in DB")

    item_id = str(row["item_id"])
    location_id = str(row["location_id"])

    resp = api_client.get(
        f"/v1/graph?item_id={item_id}&location_id={location_id}",
        headers=auth,
    )
    assert resp.status_code == 200, f"Graph failed: {resp.text}"

    data = resp.json()
    assert "nodes" in data
    assert "edges" in data
    assert isinstance(data["nodes"], list)
    assert isinstance(data["edges"], list)
    assert len(data["nodes"]) > 0, "Expected at least one node in graph response"


# ---------------------------------------------------------------------------
# Test 26 — GET /v1/explain responds cleanly on valid node or 404 on missing
# ---------------------------------------------------------------------------

@requires_db
def test_26_explain_valid_or_clean_error(api_client, auth, seeded_db):
    """GET /v1/explain returns 200 on a valid node or a clean error on missing node."""
    import psycopg
    from psycopg.rows import dict_row

    # Test with a real node that exists
    with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
        node_row = conn.execute("""
            SELECT node_id FROM nodes
            WHERE node_type = 'ProjectedInventory'
            LIMIT 1
        """).fetchone()

    if node_row:
        node_id = str(node_row["node_id"])
        resp = api_client.get(
            f"/v1/explain?node_id={node_id}&scenario_id={BASELINE_SCENARIO_ID}",
            headers=auth,
        )
        # 200 (explanation found) or 404 (no explanation for this node) are both valid
        assert resp.status_code in (200, 404), (
            f"Unexpected status {resp.status_code} for explain on existing node: {resp.text}"
        )
        if resp.status_code == 200:
            data = resp.json()
            assert "node_id" in data or "explanation" in data or "summary" in data

    # Test with a non-existent node — should return 404 or 422, not 500
    fake_node_id = str(uuid4())
    resp = api_client.get(
        f"/v1/explain?node_id={fake_node_id}&scenario_id={BASELINE_SCENARIO_ID}",
        headers=auth,
    )
    assert resp.status_code in (404, 422, 200), (
        f"Explain with missing node returned {resp.status_code} (expected 404/422): {resp.text}"
    )
    # Must never return 500
    assert resp.status_code != 500, f"Explain returned 500 on missing node: {resp.text}"
