"""
tests/integration/test_events_read.py — GET /v1/events integration tests.

Tests 30–36: read event log via real DB + FastAPI TestClient.
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
# Shared fixtures (mirrored from test_api_db.py)
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
    return {"Authorization": "Bearer integration-test-token"}


# ---------------------------------------------------------------------------
# Test 30 — GET /v1/events without auth → 401
# ---------------------------------------------------------------------------

@requires_db
def test_30_get_events_no_auth(api_client):
    """GET /v1/events without auth returns 401."""
    resp = api_client.get("/v1/events")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Test 31 — GET /v1/events basic schema validation
# ---------------------------------------------------------------------------

@requires_db
def test_31_get_events_schema(api_client, auth):
    """GET /v1/events returns 200 with correct envelope schema."""
    resp = api_client.get("/v1/events", headers=auth)
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"

    data = resp.json()
    assert "events" in data, "Response must have 'events' key"
    assert "total" in data, "Response must have 'total' key"
    assert "limit" in data, "Response must have 'limit' key"
    assert "offset" in data, "Response must have 'offset' key"
    assert isinstance(data["events"], list)
    assert data["limit"] == 50  # default
    assert data["offset"] == 0  # default


# ---------------------------------------------------------------------------
# Test 32 — GET /v1/events individual event record fields
# ---------------------------------------------------------------------------

@requires_db
def test_32_get_events_record_fields(api_client, auth):
    """Each event record contains all required fields."""
    # First create an event so we have at least one record
    payload = {
        "event_type": "test_event",
        "source": "integration-test",
        "scenario_id": "baseline",
    }
    post_resp = api_client.post("/v1/events", json=payload, headers=auth)
    assert post_resp.status_code == 202

    resp = api_client.get("/v1/events", headers=auth)
    assert resp.status_code == 200

    data = resp.json()
    assert data["total"] >= 1

    for event in data["events"]:
        assert "event_id" in event
        assert "event_type" in event
        assert "processed" in event
        assert "source" in event
        assert "created_at" in event
        # Optional fields may be None but must be present
        assert "scenario_id" in event
        assert "trigger_node_id" in event
        assert "field_changed" in event
        assert "old_date" in event
        assert "new_date" in event
        assert "old_quantity" in event
        assert "new_quantity" in event


# ---------------------------------------------------------------------------
# Test 33 — GET /v1/events ordering (most recent first)
# ---------------------------------------------------------------------------

@requires_db
def test_33_get_events_ordered_desc(api_client, auth):
    """Events are returned in created_at DESC order."""
    resp = api_client.get("/v1/events", headers=auth)
    assert resp.status_code == 200
    events = resp.json()["events"]

    if len(events) >= 2:
        timestamps = [e["created_at"] for e in events]
        # DESC: each timestamp should be >= next
        for i in range(len(timestamps) - 1):
            assert timestamps[i] >= timestamps[i + 1], (
                f"Events not in DESC order at index {i}: "
                f"{timestamps[i]} < {timestamps[i + 1]}"
            )


# ---------------------------------------------------------------------------
# Test 34 — GET /v1/events pagination (limit / offset)
# ---------------------------------------------------------------------------

@requires_db
def test_34_get_events_pagination(api_client, auth):
    """Limit and offset parameters correctly paginate results."""
    # Ensure at least 3 events exist
    for i in range(3):
        api_client.post(
            "/v1/events",
            json={"event_type": "test_event", "source": f"pagination-test-{i}", "scenario_id": "baseline"},
            headers=auth,
        )

    resp_all = api_client.get("/v1/events?limit=500", headers=auth)
    assert resp_all.status_code == 200
    total = resp_all.json()["total"]

    if total < 2:
        pytest.skip("Not enough events to test pagination")

    # Page 1
    resp_p1 = api_client.get("/v1/events?limit=2&offset=0", headers=auth)
    assert resp_p1.status_code == 200
    data_p1 = resp_p1.json()
    assert len(data_p1["events"]) == 2
    assert data_p1["limit"] == 2
    assert data_p1["offset"] == 0
    assert data_p1["total"] == total

    # Page 2
    resp_p2 = api_client.get("/v1/events?limit=2&offset=2", headers=auth)
    assert resp_p2.status_code == 200
    data_p2 = resp_p2.json()
    assert data_p2["offset"] == 2

    # Pages should not overlap (different event_ids)
    ids_p1 = {e["event_id"] for e in data_p1["events"]}
    ids_p2 = {e["event_id"] for e in data_p2["events"]}
    assert ids_p1.isdisjoint(ids_p2), "Pages overlap — pagination broken"


# ---------------------------------------------------------------------------
# Test 35 — GET /v1/events filter by event_type
# ---------------------------------------------------------------------------

@requires_db
def test_35_get_events_filter_event_type(api_client, auth):
    """Filter by event_type returns only matching events."""
    # Create a known-type event
    api_client.post(
        "/v1/events",
        json={"event_type": "ingestion_complete", "source": "filter-test", "scenario_id": "baseline"},
        headers=auth,
    )

    resp = api_client.get("/v1/events?event_type=ingestion_complete", headers=auth)
    assert resp.status_code == 200
    data = resp.json()
    for event in data["events"]:
        assert event["event_type"] == "ingestion_complete", (
            f"Unexpected event_type '{event['event_type']}' in filtered response"
        )

    # Invalid event_type → 422
    resp_bad = api_client.get("/v1/events?event_type=not_a_valid_type", headers=auth)
    assert resp_bad.status_code == 422


# ---------------------------------------------------------------------------
# Test 36 — GET /v1/events filter by processed flag
# ---------------------------------------------------------------------------

@requires_db
def test_36_get_events_filter_processed(api_client, auth):
    """Filter by processed=false returns only unprocessed events."""
    # Newly posted events start as processed=false
    resp = api_client.get("/v1/events?processed=false", headers=auth)
    assert resp.status_code == 200
    data = resp.json()
    for event in data["events"]:
        assert event["processed"] is False, (
            f"Processed event returned for processed=false filter"
        )

    # processed=true — should return 200 (list may be empty)
    resp_true = api_client.get("/v1/events?processed=true", headers=auth)
    assert resp_true.status_code == 200
    for event in resp_true.json()["events"]:
        assert event["processed"] is True
