"""
Integration tests for the BOM FastAPI router against a real PostgreSQL
database (no mocks).

Ported from tests/test_router_bom.py — every test that previously mocked
``conn.execute`` for _resolve_item_id / _get_active_bom / _get_bom_lines /
_detect_cycle / _recalculate_llc / _get_on_hand_qty is re-implemented here
using the seeded test database (PUMP-01 / VALVE-02 items at DC-ATL / DC-LAX
locations, seeded BOM PUMP-01 → 2× VALVE-02 with 2% scrap).

Tests that need rows beyond the seed (extra items, fresh BOM headers,
cycle setups) insert per-test rows and clean them up afterwards.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

SEED_SCRIPT = Path(__file__).parents[2] / "scripts" / "seed_demo_data.py"
AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}


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


# ---------------------------------------------------------------------------
# Per-test helper utilities
# ---------------------------------------------------------------------------


def _insert_item(conn, external_id: str, name: str = "Test Item") -> str:
    """Insert an item with the given external_id, returns the item_id (str)."""
    item_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        VALUES (%s, %s, %s, 'finished_good', 'EA', 'active')
        """,
        (item_id, external_id, name),
    )
    return str(item_id)


def _cleanup_external_ids(conn, external_ids: list[str]):
    """Remove BOM headers/lines and items matching any of the given external_ids."""
    if not external_ids:
        return
    # Remove BOM rows tied to these items (cascade on bom_headers removes bom_lines)
    conn.execute(
        """
        DELETE FROM bom_headers
        WHERE parent_item_id IN (
            SELECT item_id FROM items WHERE external_id = ANY(%s)
        )
        """,
        (external_ids,),
    )
    # bom_lines that reference these items as components (in unrelated BOMs)
    conn.execute(
        """
        DELETE FROM bom_lines
        WHERE component_item_id IN (
            SELECT item_id FROM items WHERE external_id = ANY(%s)
        )
        """,
        (external_ids,),
    )
    conn.execute(
        "DELETE FROM items WHERE external_id = ANY(%s)",
        (external_ids,),
    )


# ---------------------------------------------------------------------------
# POST /v1/ingest/bom — DB-backed tests
# ---------------------------------------------------------------------------


class TestIngestBOMEndpoint:
    """POST /v1/ingest/bom against the real DB."""

    def test_ingest_bom_parent_not_found(self, api_client, auth):
        resp = api_client.post(
            "/v1/ingest/bom",
            json={
                "parent_external_id": "MISSING-PARENT-XYZ",
                "components": [
                    {"component_external_id": "C1", "quantity_per": 1.0},
                ],
            },
            headers=auth,
        )
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert any("MISSING-PARENT-XYZ" in str(d) for d in detail)

    def test_ingest_bom_component_not_found(self, api_client, auth, seeded_db):
        """Parent exists (PUMP-01 in seed) but component does not."""
        resp = api_client.post(
            "/v1/ingest/bom",
            json={
                "parent_external_id": "PUMP-01",
                "components": [
                    {"component_external_id": "NO-SUCH-COMPONENT-XYZ", "quantity_per": 1.0},
                ],
            },
            headers=auth,
        )
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert any("NO-SUCH-COMPONENT-XYZ" in str(d) for d in detail)

    def test_ingest_bom_cycle_detected(self, api_client, auth, seeded_db):
        """Construct a cycle: existing BOM has X→Y. New ingest tries Y→X."""
        import psycopg
        from psycopg.rows import dict_row

        ext_x = f"CYC-X-{uuid4().hex[:6]}"
        ext_y = f"CYC-Y-{uuid4().hex[:6]}"
        created = [ext_x, ext_y]

        with psycopg.connect(seeded_db, row_factory=dict_row, autocommit=False) as conn:
            # Insert two items
            _insert_item(conn, ext_x)
            _insert_item(conn, ext_y)
            # Create existing BOM X → Y
            x_id = conn.execute(
                "SELECT item_id FROM items WHERE external_id = %s", (ext_x,)
            ).fetchone()["item_id"]
            y_id = conn.execute(
                "SELECT item_id FROM items WHERE external_id = %s", (ext_y,)
            ).fetchone()["item_id"]
            bom_id = uuid4()
            conn.execute(
                """
                INSERT INTO bom_headers (bom_id, parent_item_id, bom_version, effective_from, status)
                VALUES (%s, %s, '1.0', CURRENT_DATE, 'active')
                """,
                (bom_id, x_id),
            )
            conn.execute(
                """
                INSERT INTO bom_lines (line_id, bom_id, component_item_id, quantity_per, uom, scrap_factor, llc)
                VALUES (%s, %s, %s, 1.0, 'EA', 0.0, 0)
                """,
                (uuid4(), bom_id, y_id),
            )
            conn.commit()

        try:
            # Now try Y → X, which would create a cycle
            resp = api_client.post(
                "/v1/ingest/bom",
                json={
                    "parent_external_id": ext_y,
                    "components": [
                        {"component_external_id": ext_x, "quantity_per": 1.0},
                    ],
                },
                headers=auth,
            )
            assert resp.status_code == 422
            detail = resp.json()["detail"]
            assert any("cycle" in str(d).lower() for d in detail)
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row, autocommit=False) as conn:
                _cleanup_external_ids(conn, created)
                conn.commit()

    def test_ingest_bom_dry_run(self, api_client, auth, seeded_db):
        """dry_run=True returns 'dry_run' without persisting."""
        # Use seeded items PUMP-01 (parent) and VALVE-02 (component) so resolution succeeds.
        # Capture pre-existing BOM count for PUMP-01 to verify no new write.
        import psycopg
        from psycopg.rows import dict_row

        resp = api_client.post(
            "/v1/ingest/bom",
            json={
                "parent_external_id": "PUMP-01",
                "bom_version": "999.0-dry-run-test",  # version that doesn't exist
                "components": [
                    {"component_external_id": "VALVE-02", "quantity_per": 3.0},
                ],
                "dry_run": True,
            },
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "dry_run"
        assert body["components_imported"] == 1
        assert body["llc_updated"] == 0

        # Verify nothing new was written for v999.0
        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            post_row = conn.execute(
                """
                SELECT COUNT(*) AS n FROM bom_headers bh
                JOIN items i ON i.item_id = bh.parent_item_id
                WHERE i.external_id = 'PUMP-01' AND bh.bom_version = '999.0-dry-run-test'
                """
            ).fetchone()
            assert post_row["n"] == 0, "dry_run wrote a header"

    def test_ingest_bom_new_header_success(self, api_client, auth, seeded_db):
        """Create a fresh BOM on a brand-new parent + component."""
        import psycopg
        from psycopg.rows import dict_row

        ext_parent = f"INGEST-P-{uuid4().hex[:6]}"
        ext_comp = f"INGEST-C-{uuid4().hex[:6]}"
        created = [ext_parent, ext_comp]

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            _insert_item(conn, ext_parent)
            _insert_item(conn, ext_comp)
            conn.commit()

        try:
            resp = api_client.post(
                "/v1/ingest/bom",
                json={
                    "parent_external_id": ext_parent,
                    "bom_version": "1.0",
                    "effective_from": "2026-01-01",
                    "components": [
                        {
                            "component_external_id": ext_comp,
                            "quantity_per": 2.0,
                            "uom": "EA",
                            "scrap_factor": 0.05,
                        },
                    ],
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["status"] == "ok"
            assert body["components_imported"] == 1
            assert body["llc_updated"] >= 1
            assert body["parent_item_id"] is not None

            # Verify DB state
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                row = conn.execute(
                    """
                    SELECT bl.quantity_per, bl.uom, bl.scrap_factor
                    FROM bom_lines bl
                    JOIN bom_headers bh ON bh.bom_id = bl.bom_id
                    JOIN items i ON i.item_id = bh.parent_item_id
                    WHERE i.external_id = %s AND bl.active = TRUE
                    """,
                    (ext_parent,),
                ).fetchone()
                assert row is not None
                assert float(row["quantity_per"]) == 2.0
                assert row["uom"] == "EA"
                assert float(row["scrap_factor"]) == 0.05
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_external_ids(conn, created)
                conn.commit()

    def test_ingest_bom_existing_header_update(self, api_client, auth, seeded_db):
        """Second ingest on same parent + version updates the existing header."""
        import psycopg
        from psycopg.rows import dict_row

        ext_parent = f"UPD-P-{uuid4().hex[:6]}"
        ext_comp = f"UPD-C-{uuid4().hex[:6]}"
        created = [ext_parent, ext_comp]

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            _insert_item(conn, ext_parent)
            _insert_item(conn, ext_comp)
            conn.commit()

        try:
            # First ingest
            r1 = api_client.post(
                "/v1/ingest/bom",
                json={
                    "parent_external_id": ext_parent,
                    "bom_version": "1.0",
                    "components": [
                        {"component_external_id": ext_comp, "quantity_per": 1.0},
                    ],
                },
                headers=auth,
            )
            assert r1.status_code == 200, r1.text
            bom_id_1 = r1.json()["bom_id"]

            # Second ingest same version — should re-use the same bom_id (UPDATE branch)
            r2 = api_client.post(
                "/v1/ingest/bom",
                json={
                    "parent_external_id": ext_parent,
                    "bom_version": "1.0",
                    "components": [
                        {"component_external_id": ext_comp, "quantity_per": 5.0},
                    ],
                },
                headers=auth,
            )
            assert r2.status_code == 200, r2.text
            assert r2.json()["bom_id"] == bom_id_1

            # Verify quantity was updated
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                row = conn.execute(
                    """
                    SELECT bl.quantity_per FROM bom_lines bl
                    JOIN bom_headers bh ON bh.bom_id = bl.bom_id
                    JOIN items i ON i.item_id = bh.parent_item_id
                    WHERE i.external_id = %s AND bl.active = TRUE
                    """,
                    (ext_parent,),
                ).fetchone()
                assert float(row["quantity_per"]) == 5.0
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_external_ids(conn, created)
                conn.commit()

    def test_ingest_bom_empty_components_deactivates_all(self, api_client, auth, seeded_db):
        """Empty components list deactivates all lines of existing BOM."""
        import psycopg
        from psycopg.rows import dict_row

        ext_parent = f"EMPTY-P-{uuid4().hex[:6]}"
        ext_comp = f"EMPTY-C-{uuid4().hex[:6]}"
        created = [ext_parent, ext_comp]

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            _insert_item(conn, ext_parent)
            _insert_item(conn, ext_comp)
            conn.commit()

        try:
            # First ingest with a component
            api_client.post(
                "/v1/ingest/bom",
                json={
                    "parent_external_id": ext_parent,
                    "components": [
                        {"component_external_id": ext_comp, "quantity_per": 1.0},
                    ],
                },
                headers=auth,
            )

            # Second ingest with empty components
            resp = api_client.post(
                "/v1/ingest/bom",
                json={
                    "parent_external_id": ext_parent,
                    "components": [],
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["status"] == "ok"
            assert body["components_imported"] == 0

            # Verify the previously-active line was deactivated
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS n FROM bom_lines bl
                    JOIN bom_headers bh ON bh.bom_id = bl.bom_id
                    JOIN items i ON i.item_id = bh.parent_item_id
                    WHERE i.external_id = %s AND bl.active = TRUE
                    """,
                    (ext_parent,),
                ).fetchone()
                assert row["n"] == 0
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_external_ids(conn, created)
                conn.commit()


# ---------------------------------------------------------------------------
# GET /v1/bom/{parent_external_id} — DB-backed tests
# ---------------------------------------------------------------------------


class TestGetBOMEndpoint:
    def test_get_bom_item_not_found(self, api_client, auth):
        resp = api_client.get(
            "/v1/bom/MISSING-XYZ", headers=auth
        )
        assert resp.status_code == 404
        assert "MISSING-XYZ" in resp.json()["detail"]

    def test_get_bom_no_active_header(self, api_client, auth, seeded_db):
        """Item exists but has no BOM (use VALVE-02 — leaf in seed)."""
        resp = api_client.get("/v1/bom/VALVE-02", headers=auth)
        # VALVE-02 has no BOM in the seed (it's a leaf component)
        assert resp.status_code == 404

    def test_get_bom_success(self, api_client, auth):
        """PUMP-01 has a seeded BOM: 1 component (VALVE-02), qty=2, scrap=0.02."""
        resp = api_client.get("/v1/bom/PUMP-01", headers=auth)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["parent_external_id"] == "PUMP-01"
        assert body["bom_version"] == "1.0"
        assert len(body["components"]) == 1
        comp = body["components"][0]
        assert comp["component_external_id"] == "VALVE-02"
        assert comp["quantity_per"] == 2.0
        assert comp["uom"] == "EA"
        assert abs(comp["scrap_factor"] - 0.02) < 1e-6


# ---------------------------------------------------------------------------
# POST /v1/bom/explode — DB-backed tests
# ---------------------------------------------------------------------------


class TestExplodeBOMEndpoint:
    def test_explode_bom_item_not_found(self, api_client, auth):
        resp = api_client.post(
            "/v1/bom/explode",
            json={"item_external_id": "MISSING-XYZ", "quantity": 10},
            headers=auth,
        )
        assert resp.status_code == 404

    def test_explode_bom_location_not_found(self, api_client, auth):
        """Item exists, but location doesn't → 422."""
        resp = api_client.post(
            "/v1/bom/explode",
            json={
                "item_external_id": "PUMP-01",
                "quantity": 5,
                "location_external_id": "NOWHERE-XYZ",
            },
            headers=auth,
        )
        assert resp.status_code == 422

    def test_explode_bom_leaf_item_no_bom(self, api_client, auth):
        """VALVE-02 is a leaf (no BOM) → explosion empty."""
        resp = api_client.post(
            "/v1/bom/explode",
            json={"item_external_id": "VALVE-02", "quantity": 10},
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["total_components"] == 0
        assert body["components_with_shortage"] == 0
        assert body["explosion"] == []

    def test_explode_bom_pump_with_seed_data(self, api_client, auth):
        """
        Explode PUMP-01 × 100 units.
        Seed BOM: PUMP-01 → 2× VALVE-02 with scrap 0.02 → gross = 100 * 2 * 1.02 = 204.
        OnHand VALVE-02 @ DC-LAX = 45 → shortage at DC-LAX = 204 - 45 = 159.
        Without location filter: SUM of all OnHand VALVE-02 = 45.
        """
        resp = api_client.post(
            "/v1/bom/explode",
            json={
                "item_external_id": "PUMP-01",
                "quantity": 100,
                "location_external_id": "DC-LAX",
            },
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["total_components"] == 1
        line = body["explosion"][0]
        assert line["level"] == 1
        assert line["component_external_id"] == "VALVE-02"
        assert abs(line["gross_requirement"] - 204.0) < 1e-3
        # OnHand VALVE-02 @ DC-LAX = 45 per seed
        assert line["on_hand_qty"] == 45.0
        assert abs(line["net_requirement"] - 159.0) < 1e-3
        assert line["has_shortage"] is True
        assert body["components_with_shortage"] == 1

    def test_explode_bom_pump_no_location_aggregates_all_onhand(self, api_client, auth):
        """Without location filter, on_hand sums across all locations.

        Seed: only OnHandSupply VALVE-02 lives @ DC-LAX (qty=45). Sum = 45.
        """
        resp = api_client.post(
            "/v1/bom/explode",
            json={"item_external_id": "PUMP-01", "quantity": 100},
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        line = body["explosion"][0]
        assert line["on_hand_qty"] == 45.0  # only seed OnHand for VALVE-02

    def test_explode_bom_levels_cap(self, api_client, auth):
        """levels=1 returns at most level-1 components (PUMP-01 → VALVE-02 only)."""
        resp = api_client.post(
            "/v1/bom/explode",
            json={"item_external_id": "PUMP-01", "quantity": 1, "levels": 1},
            headers=auth,
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        # All entries must be at level 1
        for line in body["explosion"]:
            assert line["level"] == 1
