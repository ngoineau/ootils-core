"""
tests/integration/test_bom.py — Integration tests for BOM endpoints.

Tests:
  - test_ingest_bom_basic
  - test_ingest_bom_dry_run
  - test_ingest_bom_cycle_detection
  - test_ingest_bom_unknown_component
  - test_get_bom
  - test_bom_explode_basic
  - test_bom_explode_with_onhand
  - test_bom_401

Requires a running PostgreSQL instance:
    DATABASE_URL=postgresql://ootils:ootils@localhost:5432/ootils_test pytest tests/integration/test_bom.py -v
"""
from __future__ import annotations

import os
from uuid import uuid4

import pytest

from .conftest import requires_db, DB_AVAILABLE, TEST_DB_URL

# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def bom_client(migrated_db):
    """Module-scoped TestClient wired to a migrated test DB."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "test-token"

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
    return {"Authorization": "Bearer test-token"}


# Unique prefix per test run to avoid UUID collisions
PREFIX = str(uuid4())[:8]


def uid(base: str) -> str:
    return f"{PREFIX}-{base}"


# ─────────────────────────────────────────────────────────────
# Helpers: seed items/locations into DB before BOM tests
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def seeded_items(bom_client, auth):
    """
    Seed parent item + 3 components for BOM tests.
    Returns dict: {external_id: str, ...}
    """
    parent_ext = uid("PUMP-PARENT")
    comp1_ext = uid("MOTOR-COMP")
    comp2_ext = uid("SEAL-COMP")
    comp3_ext = uid("BOLT-COMP")
    # Extra items for cycle test
    item_a_ext = uid("CYCLE-A")
    item_b_ext = uid("CYCLE-B")

    resp = bom_client.post(
        "/v1/ingest/items",
        json={
            "items": [
                {"external_id": parent_ext, "name": "Test Pump Parent", "item_type": "finished_good"},
                {"external_id": comp1_ext,  "name": "Motor Component",   "item_type": "component"},
                {"external_id": comp2_ext,  "name": "Seal Component",    "item_type": "component"},
                {"external_id": comp3_ext,  "name": "Bolt Component",    "item_type": "raw_material"},
                {"external_id": item_a_ext, "name": "Cycle Item A",      "item_type": "semi_finished"},
                {"external_id": item_b_ext, "name": "Cycle Item B",      "item_type": "component"},
            ],
            "dry_run": False,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text

    return {
        "parent": parent_ext,
        "comp1": comp1_ext,
        "comp2": comp2_ext,
        "comp3": comp3_ext,
        "cycle_a": item_a_ext,
        "cycle_b": item_b_ext,
    }


@pytest.fixture(scope="module")
def seeded_location(bom_client, auth):
    """Seed a DC location for explode tests."""
    loc_ext = uid("DC-TEST")
    resp = bom_client.post(
        "/v1/ingest/locations",
        json={
            "locations": [
                {"external_id": loc_ext, "name": "Test DC", "location_type": "dc"},
            ],
            "dry_run": False,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    return loc_ext


# ─────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────

@requires_db
def test_bom_401(bom_client):
    """Without auth → 401."""
    resp = bom_client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": "some-item",
            "components": [],
        },
    )
    assert resp.status_code == 401


@requires_db
def test_ingest_bom_basic(bom_client, auth, seeded_items):
    """Import BOM with 2 components, verify bom_headers + bom_lines in DB."""
    resp = bom_client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": seeded_items["parent"],
            "bom_version": "1.0",
            "effective_from": "2026-01-01",
            "components": [
                {
                    "component_external_id": seeded_items["comp1"],
                    "quantity_per": 1.0,
                    "uom": "EA",
                    "scrap_factor": 0.02,
                },
                {
                    "component_external_id": seeded_items["comp2"],
                    "quantity_per": 2.0,
                    "uom": "EA",
                    "scrap_factor": 0.0,
                },
            ],
            "dry_run": False,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["status"] == "ok"
    assert data["components_imported"] == 2
    assert data["bom_id"] is not None
    assert data["parent_item_id"] is not None
    assert data["llc_updated"] >= 0


@requires_db
def test_ingest_bom_dry_run(bom_client, auth, seeded_items, conn):
    """dry_run=True — nothing written to DB."""
    parent_ext = uid("DRY-PUMP")
    # Create a unique parent for dry_run test to ensure isolation
    bom_client.post(
        "/v1/ingest/items",
        json={"items": [{"external_id": parent_ext, "name": "Dry Run Pump", "item_type": "finished_good"}], "dry_run": False},
        headers=auth,
    )

    resp = bom_client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": parent_ext,
            "bom_version": "1.0",
            "effective_from": "2026-01-01",
            "components": [
                {
                    "component_external_id": seeded_items["comp1"],
                    "quantity_per": 1.0,
                    "uom": "EA",
                    "scrap_factor": 0.0,
                },
            ],
            "dry_run": True,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["status"] == "dry_run"

    # Verify nothing was written: no bom_header for this parent
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM bom_headers bh JOIN items i ON i.item_id = bh.parent_item_id WHERE i.external_id = %s",
        (parent_ext,),
    ).fetchone()
    assert row["cnt"] == 0


@requires_db
def test_ingest_bom_unknown_component(bom_client, auth, seeded_items):
    """component_external_id that doesn't exist → 422."""
    resp = bom_client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": seeded_items["parent"],
            "bom_version": "2.0",
            "components": [
                {
                    "component_external_id": "DOES-NOT-EXIST-12345",
                    "quantity_per": 1.0,
                    "uom": "EA",
                    "scrap_factor": 0.0,
                },
            ],
            "dry_run": False,
        },
        headers=auth,
    )
    assert resp.status_code == 422, resp.text


@requires_db
def test_ingest_bom_cycle_detection(bom_client, auth, seeded_items):
    """
    Cycle detection: A → B → A should be rejected with 422.
    Steps:
      1. Create BOM: cycle_a → cycle_b
      2. Attempt BOM: cycle_b → cycle_a  (creates cycle)
    """
    # Step 1: cycle_a → cycle_b (valid)
    resp1 = bom_client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": seeded_items["cycle_a"],
            "bom_version": "1.0",
            "components": [
                {
                    "component_external_id": seeded_items["cycle_b"],
                    "quantity_per": 1.0,
                    "uom": "EA",
                    "scrap_factor": 0.0,
                },
            ],
            "dry_run": False,
        },
        headers=auth,
    )
    assert resp1.status_code == 200, f"Step 1 failed: {resp1.text}"

    # Step 2: cycle_b → cycle_a (cycle!) → must fail 422
    resp2 = bom_client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": seeded_items["cycle_b"],
            "bom_version": "1.0",
            "components": [
                {
                    "component_external_id": seeded_items["cycle_a"],
                    "quantity_per": 2.0,
                    "uom": "EA",
                    "scrap_factor": 0.0,
                },
            ],
            "dry_run": False,
        },
        headers=auth,
    )
    assert resp2.status_code == 422, f"Expected 422 for cycle, got {resp2.status_code}: {resp2.text}"


@requires_db
def test_get_bom(bom_client, auth, seeded_items):
    """GET /v1/bom/{external_id} returns the active BOM."""
    # Ensure BOM exists (ingest first)
    bom_client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": seeded_items["parent"],
            "bom_version": "1.0",
            "effective_from": "2026-01-01",
            "components": [
                {
                    "component_external_id": seeded_items["comp1"],
                    "quantity_per": 1.0,
                    "uom": "EA",
                    "scrap_factor": 0.02,
                },
                {
                    "component_external_id": seeded_items["comp2"],
                    "quantity_per": 2.0,
                    "uom": "EA",
                    "scrap_factor": 0.0,
                },
            ],
            "dry_run": False,
        },
        headers=auth,
    )

    resp = bom_client.get(
        f"/v1/bom/{seeded_items['parent']}",
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["parent_external_id"] == seeded_items["parent"]
    assert data["bom_version"] == "1.0"
    assert data["effective_from"] == "2026-01-01"
    assert len(data["components"]) >= 1

    # Check component fields
    comp_ids = {c["component_external_id"] for c in data["components"]}
    assert seeded_items["comp1"] in comp_ids

    for comp in data["components"]:
        assert "quantity_per" in comp
        assert "uom" in comp
        assert "scrap_factor" in comp
        assert "llc" in comp


@requires_db
def test_bom_explode_basic(bom_client, auth, seeded_items, seeded_location):
    """
    Basic explosion: no on-hand stock → net_requirement == gross_requirement.
    """
    # Ensure BOM exists
    bom_client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": seeded_items["parent"],
            "bom_version": "1.0",
            "effective_from": "2026-01-01",
            "components": [
                {
                    "component_external_id": seeded_items["comp1"],
                    "quantity_per": 2.0,
                    "uom": "EA",
                    "scrap_factor": 0.0,
                },
                {
                    "component_external_id": seeded_items["comp2"],
                    "quantity_per": 3.0,
                    "uom": "EA",
                    "scrap_factor": 0.0,
                },
            ],
            "dry_run": False,
        },
        headers=auth,
    )

    resp = bom_client.post(
        "/v1/bom/explode",
        json={
            "item_external_id": seeded_items["parent"],
            "quantity": 10.0,
            "explosion_date": "2026-04-07",
            "levels": 5,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["parent_external_id"] == seeded_items["parent"]
    assert data["quantity"] == 10.0
    assert data["total_components"] >= 1

    # Check gross requirements
    explosion_map = {e["component_external_id"]: e for e in data["explosion"]}

    if seeded_items["comp1"] in explosion_map:
        line = explosion_map[seeded_items["comp1"]]
        assert line["level"] == 1
        # gross = 10 * 2.0 * (1 + 0.0) = 20.0
        assert abs(line["gross_requirement"] - 20.0) < 0.01
        # No on-hand seeded for this DC and item, net == gross
        assert line["net_requirement"] <= line["gross_requirement"]


@requires_db
def test_bom_explode_with_onhand(bom_client, auth, seeded_items, seeded_location):
    """
    Explosion with on-hand stock: net_requirement = max(0, gross - on_hand).
    We seed on-hand for comp3 then explode a BOM that uses comp3.
    """
    # Use a unique parent item for this test
    parent_ext = uid("PUMP-WITH-STOCK")
    bom_client.post(
        "/v1/ingest/items",
        json={
            "items": [{"external_id": parent_ext, "name": "Pump With Stock", "item_type": "finished_good"}],
            "dry_run": False,
        },
        headers=auth,
    )

    # Seed BOM: parent → comp3 (quantity_per=5)
    bom_client.post(
        "/v1/ingest/bom",
        json={
            "parent_external_id": parent_ext,
            "bom_version": "1.0",
            "components": [
                {
                    "component_external_id": seeded_items["comp3"],
                    "quantity_per": 5.0,
                    "uom": "EA",
                    "scrap_factor": 0.0,
                },
            ],
            "dry_run": False,
        },
        headers=auth,
    )

    # Seed on-hand for comp3 at seeded_location: 20 units
    bom_client.post(
        "/v1/ingest/on-hand",
        json={
            "on_hand": [
                {
                    "item_external_id": seeded_items["comp3"],
                    "location_external_id": seeded_location,
                    "quantity": 20.0,
                    "uom": "EA",
                    "as_of_date": "2026-04-07",
                },
            ],
            "dry_run": False,
        },
        headers=auth,
    )

    # Explode 10 units of parent
    # gross = 10 * 5 = 50, on_hand = 20, net = 30
    resp = bom_client.post(
        "/v1/bom/explode",
        json={
            "item_external_id": parent_ext,
            "quantity": 10.0,
            "location_external_id": seeded_location,
            "explosion_date": "2026-04-07",
            "levels": 5,
        },
        headers=auth,
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["total_components"] >= 1
    explosion_map = {e["component_external_id"]: e for e in data["explosion"]}
    assert seeded_items["comp3"] in explosion_map

    line = explosion_map[seeded_items["comp3"]]
    assert abs(line["gross_requirement"] - 50.0) < 0.01
    assert line["on_hand_qty"] == 20.0
    assert abs(line["net_requirement"] - 30.0) < 0.01
    assert line["has_shortage"] is True
    assert data["components_with_shortage"] >= 1
