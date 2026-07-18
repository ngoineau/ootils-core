"""
tests/integration/test_distribution_links_ingest_integration.py — DESC-1 PR-D:
POST /v1/ingest/distribution-links against a real PostgreSQL, no mocks
(CLAUDE.md). The mocked-DB branch coverage lives in
tests/test_ingest_distribution_links.py; this file asserts what a FakeDB
cannot: the real SELECT-then-INSERT/UPDATE upsert on the NULL-safe
(upstream, downstream, item) natural key, the columns the endpoint must NEVER
touch, and the FK validation against real rows.

The three contract axes (module-scoped api_client, pattern of
test_ingest_routings_integration.py; each test seeds its OWN uuid-suffixed
referential rows via the real /v1/ingest/* endpoints):

  1. INGEST 2 LANES — one generic (item empty -> item_id NULL) + one
     item-specific lane on the SAME (upstream, downstream) pair: both are
     inserted (coexistence, spec §4 — the DRP specificity rule consumes them,
     never a duplicate refusal), FKs resolved to the real location/item UUIDs,
     server defaults (min 1, multiple 1, priority 100, active TRUE) applied.
  2. IDEMPOTENT UPSERT — a re-push of the same 2 keys with different values is
     an UPDATE, not a doublon: summary says updated=2/inserted=0, the table
     still holds exactly 2 rows for the pair, the distribution_link_id is
     STABLE (never re-minted), the pushed values landed — and the columns the
     file contract does NOT cover (maximum_shipment_qty here, set out-of-band
     between the two pushes) survive the re-push untouched (spec §8).
  3. LOCATIONS INCONNUES — unknown upstream AND downstream -> nominative 422
     naming each missing external_id, and ZERO rows written (all-or-nothing).

Isolation: unique PREFIX per test; neutralized by DEACTIVATION in a finally
block (lanes active=FALSE, items obsoleted) — never a DELETE. The module-scoped
``migrated_db`` teardown drops the schema as the backstop.
"""
from __future__ import annotations

import os
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

TOKEN = "integration-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """Module-scoped FastAPI TestClient bound to the real test DB (same
    pattern as test_ingest_routings_integration.py)."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = TOKEN

    from fastapi.testclient import TestClient
    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def _conn():
    return psycopg.connect(TEST_DB_URL, row_factory=dict_row)


def _seed(api_client, prefix: str) -> dict:
    """Two DCs + one item through the REAL ingest endpoints; returns the
    resolved UUIDs keyed by role."""
    resp = api_client.post(
        "/v1/ingest/locations",
        json={"locations": [
            {"external_id": f"{prefix}-UP", "name": "Lane upstream DC",
             "location_type": "dc"},
            {"external_id": f"{prefix}-DOWN", "name": "Lane downstream DC",
             "location_type": "dc"},
        ]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text
    resp = api_client.post(
        "/v1/ingest/items",
        json={"items": [{"external_id": f"{prefix}-ITEM", "name": "Lane item",
                         "item_type": "finished_good", "uom": "EA",
                         "status": "active"}]},
        headers=AUTH,
    )
    assert resp.status_code == 200, resp.text

    with _conn() as c:
        up, down = (
            c.execute(
                "SELECT location_id FROM locations WHERE external_id = %s",
                (f"{prefix}-{tag}",),
            ).fetchone()["location_id"]
            for tag in ("UP", "DOWN")
        )
        item = c.execute(
            "SELECT item_id FROM items WHERE external_id = %s",
            (f"{prefix}-ITEM",),
        ).fetchone()["item_id"]
    return {"up": up, "down": down, "item": item}


def _neutralize(prefix: str, up: UUID, down: UUID) -> None:
    """Deactivation, never DELETE: lanes off, items obsoleted."""
    with psycopg.connect(TEST_DB_URL, autocommit=True) as c:
        c.execute(
            "UPDATE distribution_links SET active = FALSE "
            "WHERE upstream_location_id = %s AND downstream_location_id = %s",
            (up, down),
        )
        c.execute(
            "UPDATE items SET status = 'obsolete' WHERE external_id LIKE %s",
            (prefix + "%",),
        )


def _lanes_for_pair(c, up: UUID, down: UUID) -> list[dict]:
    return c.execute(
        "SELECT distribution_link_id, item_id, transit_lead_time_days, "
        "minimum_shipment_qty, transfer_multiple, maximum_shipment_qty, "
        "priority, active "
        "FROM distribution_links "
        "WHERE upstream_location_id = %s AND downstream_location_id = %s "
        "ORDER BY item_id NULLS FIRST",
        (up, down),
    ).fetchall()


class TestDistributionLinksIngest:
    def test_ingests_generic_and_specific_lane_verified_in_db(self, api_client):
        prefix = f"DLINK-{uuid4().hex[:8]}"
        ids = _seed(api_client, prefix)
        try:
            resp = api_client.post(
                "/v1/ingest/distribution-links",
                json={"distribution_links": [
                    # Generic lane: blank item -> item_id NULL, all defaults.
                    {"upstream_external_id": f"{prefix}-UP",
                     "downstream_external_id": f"{prefix}-DOWN",
                     "item_external_id": "",
                     "transit_lead_time_days": 7},
                    # Item-specific lane on the SAME pair: coexists (spec §4).
                    {"upstream_external_id": f"{prefix}-UP",
                     "downstream_external_id": f"{prefix}-DOWN",
                     "item_external_id": f"{prefix}-ITEM",
                     "transit_lead_time_days": 3,
                     "minimum_shipment_qty": 5,
                     "transfer_multiple": 10,
                     "priority": 2},
                ]},
                headers=AUTH,
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["summary"]["inserted"] == 2
            assert body["summary"]["updated"] == 0
            assert [r["action"] for r in body["results"]] == ["inserted", "inserted"]

            with _conn() as c:
                lanes = _lanes_for_pair(c, ids["up"], ids["down"])
            assert len(lanes) == 2
            generic, specific = lanes
            # Generic lane: item_id NULL + server defaults.
            assert generic["item_id"] is None
            assert float(generic["transit_lead_time_days"]) == 7.0
            assert float(generic["minimum_shipment_qty"]) == 1.0
            assert float(generic["transfer_multiple"]) == 1.0
            assert generic["priority"] == 100
            assert generic["active"] is True
            # Specific lane: FK resolved to the real item, explicit values.
            assert specific["item_id"] == ids["item"]
            assert float(specific["transit_lead_time_days"]) == 3.0
            assert float(specific["minimum_shipment_qty"]) == 5.0
            assert float(specific["transfer_multiple"]) == 10.0
            assert specific["priority"] == 2
            assert specific["active"] is True
        finally:
            _neutralize(prefix, ids["up"], ids["down"])

    def test_repush_updates_in_place_never_duplicates(self, api_client):
        prefix = f"DLINK-{uuid4().hex[:8]}"
        ids = _seed(api_client, prefix)
        try:
            lanes = [
                {"upstream_external_id": f"{prefix}-UP",
                 "downstream_external_id": f"{prefix}-DOWN",
                 "transit_lead_time_days": 7},
                {"upstream_external_id": f"{prefix}-UP",
                 "downstream_external_id": f"{prefix}-DOWN",
                 "item_external_id": f"{prefix}-ITEM",
                 "transit_lead_time_days": 3},
            ]
            r1 = api_client.post(
                "/v1/ingest/distribution-links",
                json={"distribution_links": lanes},
                headers=AUTH,
            )
            assert r1.status_code == 200, r1.text
            assert r1.json()["summary"]["inserted"] == 2
            ids_before = {r["item_external_id"]: r["distribution_link_id"]
                          for r in r1.json()["results"]}

            # Out-of-band value on a column the file contract does NOT cover
            # (spec §8) — must survive the re-push untouched.
            with psycopg.connect(TEST_DB_URL, autocommit=True) as c:
                c.execute(
                    "UPDATE distribution_links SET maximum_shipment_qty = 999 "
                    "WHERE upstream_location_id = %s AND downstream_location_id = %s "
                    "AND item_id IS NULL",
                    (ids["up"], ids["down"]),
                )

            # Re-push the SAME 2 keys with different covered values.
            lanes[0]["transit_lead_time_days"] = 14
            lanes[0]["priority"] = 5
            lanes[1]["transfer_multiple"] = 25
            r2 = api_client.post(
                "/v1/ingest/distribution-links",
                json={"distribution_links": lanes},
                headers=AUTH,
            )
            assert r2.status_code == 200, r2.text
            body2 = r2.json()
            assert body2["summary"]["updated"] == 2
            assert body2["summary"]["inserted"] == 0
            assert [r["action"] for r in body2["results"]] == ["updated", "updated"]
            # The link identity is STABLE across pushes (upsert, not re-mint).
            ids_after = {r["item_external_id"]: r["distribution_link_id"]
                         for r in body2["results"]}
            assert ids_after == ids_before

            with _conn() as c:
                rows = _lanes_for_pair(c, ids["up"], ids["down"])
            assert len(rows) == 2, "re-push must never create a doublon"
            generic, specific = rows
            assert generic["item_id"] is None
            assert float(generic["transit_lead_time_days"]) == 14.0
            assert generic["priority"] == 5
            # The uncovered column survived (spec §8).
            assert float(generic["maximum_shipment_qty"]) == 999.0
            assert specific["item_id"] == ids["item"]
            assert float(specific["transfer_multiple"]) == 25.0
            # A value the re-push did not resend falls back to the server
            # default on UPDATE too (full-row upsert of the covered columns).
            assert float(specific["minimum_shipment_qty"]) == 1.0
        finally:
            _neutralize(prefix, ids["up"], ids["down"])

    def test_unknown_locations_422_nominative_and_nothing_written(self, api_client):
        prefix = f"DLINK-{uuid4().hex[:8]}"
        ids = _seed(api_client, prefix)
        try:
            resp = api_client.post(
                "/v1/ingest/distribution-links",
                json={"distribution_links": [
                    {"upstream_external_id": f"{prefix}-GHOST-UP",
                     "downstream_external_id": f"{prefix}-GHOST-DOWN",
                     "transit_lead_time_days": 7},
                    # A valid row in the same batch: all-or-nothing, it must
                    # NOT be written either.
                    {"upstream_external_id": f"{prefix}-UP",
                     "downstream_external_id": f"{prefix}-DOWN",
                     "transit_lead_time_days": 7},
                ]},
                headers=AUTH,
            )
            assert resp.status_code == 422, resp.text
            detail = resp.json()["detail"]
            errs = [e for row in detail for e in row["errors"]]
            assert any(f"upstream_external_id '{prefix}-GHOST-UP' not found" in e
                       for e in errs)
            assert any(f"downstream_external_id '{prefix}-GHOST-DOWN' not found" in e
                       for e in errs)
            with _conn() as c:
                assert _lanes_for_pair(c, ids["up"], ids["down"]) == []
        finally:
            _neutralize(prefix, ids["up"], ids["down"])
