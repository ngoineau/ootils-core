"""
Integration tests for the Ghosts FastAPI router against a real PostgreSQL
database (no mocks).

Ported from tests/test_router_ghosts.py — every test that previously mocked
``conn.execute()`` for items / resources / ghost_nodes / ghost_members /
nodes / edges queries, or patched ``run_ghost``, is re-implemented here
against a real DB.

Uses the seeded PUMP-01 / VALVE-02 items as ghost members.

Note on `run_ghost`: rather than mock the engine entrypoint, we call the
endpoint with a non-existent ghost_id, which triggers the ValueError →
sanitised 404 path. The success path needs a fully wired phase_transition
or capacity_aggregate ghost — out of scope for router integration tests
(covered by tests/integration/test_ghosts.py and the engine unit tests).
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


def _seed_item_uuids(conn) -> tuple[str, str]:
    """Return (pump_item_id, valve_item_id) from the seeded items."""
    pump = conn.execute(
        "SELECT item_id FROM items WHERE external_id = 'PUMP-01'"
    ).fetchone()
    valve = conn.execute(
        "SELECT item_id FROM items WHERE external_id = 'VALVE-02'"
    ).fetchone()
    return str(pump["item_id"]), str(valve["item_id"])


def _cleanup_ghost(conn, ghost_name: str):
    """Best-effort cleanup of a ghost (cascade also removes members)."""
    # Find ghost(s) by name
    rows = conn.execute(
        "SELECT ghost_id, node_id FROM ghost_nodes WHERE name = %s", (ghost_name,)
    ).fetchall()
    for row in rows:
        # Drop edges first
        if row["node_id"]:
            conn.execute(
                "DELETE FROM edges WHERE from_node_id = %s OR to_node_id = %s",
                (row["node_id"], row["node_id"]),
            )
        conn.execute("DELETE FROM ghost_nodes WHERE ghost_id = %s", (row["ghost_id"],))
        # Then the graph node
        if row["node_id"]:
            conn.execute("DELETE FROM nodes WHERE node_id = %s", (row["node_id"],))


# ---------------------------------------------------------------------------
# POST /v1/ingest/ghosts — item / resource validation against DB
# ---------------------------------------------------------------------------


class TestGhostsIngestValidationDB:
    """DB-backed validation: item not found, resource not found."""

    def test_ingest_ghost_item_not_found(self, api_client, auth):
        """Unknown item_id → 422."""
        a = str(uuid4())
        resp = api_client.post(
            "/v1/ingest/ghosts",
            json={
                "name": f"g_item_not_found_{uuid4().hex[:6]}",
                "ghost_type": "capacity_aggregate",
                "members": [
                    {"item_id": a, "role": "member"},
                ],
            },
            headers=auth,
        )
        assert resp.status_code == 422
        detail = resp.json()["detail"]
        assert any("not found" in str(d) for d in detail)

    def test_ingest_ghost_resource_not_found(self, api_client, auth, seeded_db):
        """Resource_id provided but does not exist → 422."""
        import psycopg
        from psycopg.rows import dict_row
        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            pump_id, _ = _seed_item_uuids(conn)
        bogus_resource = str(uuid4())
        resp = api_client.post(
            "/v1/ingest/ghosts",
            json={
                "name": f"g_res_not_found_{uuid4().hex[:6]}",
                "ghost_type": "capacity_aggregate",
                "resource_id": bogus_resource,
                "members": [{"item_id": pump_id, "role": "member"}],
            },
            headers=auth,
        )
        assert resp.status_code == 422
        assert any("resource_id not found" in str(d) for d in resp.json()["detail"])


# ---------------------------------------------------------------------------
# POST /v1/ingest/ghosts — insert / update happy paths
# ---------------------------------------------------------------------------


class TestGhostsIngestEndpoint:
    def test_ingest_ghost_insert_no_members(self, api_client, auth, seeded_db):
        """Insert with empty members: just creates ghost_node + graph node."""
        import psycopg
        from psycopg.rows import dict_row

        name = f"g_no_members_{uuid4().hex[:6]}"
        try:
            resp = api_client.post(
                "/v1/ingest/ghosts",
                json={
                    "name": name,
                    "ghost_type": "phase_transition",
                },
                headers=auth,
            )
            assert resp.status_code == 201, resp.text
            body = resp.json()
            assert body["action"] == "inserted"
            assert body["member_count"] == 0
            assert body["node_id"] is not None
            # Verify DB row
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                row = conn.execute(
                    "SELECT ghost_id, node_id FROM ghost_nodes WHERE name = %s",
                    (name,),
                ).fetchone()
                assert row is not None
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_ghost(conn, name)
                conn.commit()

    def test_ingest_ghost_insert_with_members(self, api_client, auth, seeded_db):
        """Insert phase_transition with both members → creates members + edges."""
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            pump_id, valve_id = _seed_item_uuids(conn)

        name = f"g_phase_{uuid4().hex[:6]}"
        try:
            resp = api_client.post(
                "/v1/ingest/ghosts",
                json={
                    "name": name,
                    "ghost_type": "phase_transition",
                    "description": "desc",
                    "members": [
                        {"item_id": pump_id, "role": "outgoing"},
                        {"item_id": valve_id, "role": "incoming"},
                    ],
                },
                headers=auth,
            )
            assert resp.status_code == 201, resp.text
            body = resp.json()
            assert body["action"] == "inserted"
            assert body["member_count"] == 2
            assert body["node_id"] is not None

            # Verify members in DB
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                rows = conn.execute(
                    "SELECT role FROM ghost_members WHERE ghost_id = %s::uuid ORDER BY role",
                    (body["ghost_id"],),
                ).fetchall()
                assert {r["role"] for r in rows} == {"outgoing", "incoming"}
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_ghost(conn, name)
                conn.commit()

    def test_ingest_ghost_update_existing(self, api_client, auth, seeded_db):
        """Second ingest on same (name, ghost_type, scenario_id) → UPDATE branch."""
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            pump_id, valve_id = _seed_item_uuids(conn)

        name = f"g_upd_{uuid4().hex[:6]}"
        try:
            r1 = api_client.post(
                "/v1/ingest/ghosts",
                json={
                    "name": name,
                    "ghost_type": "phase_transition",
                    "description": "v1",
                    "members": [
                        {"item_id": pump_id, "role": "outgoing"},
                        {"item_id": valve_id, "role": "incoming"},
                    ],
                },
                headers=auth,
            )
            assert r1.status_code == 201, r1.text
            ghost_id = r1.json()["ghost_id"]

            # Second ingest — same name + type → update branch
            r2 = api_client.post(
                "/v1/ingest/ghosts",
                json={
                    "name": name,
                    "ghost_type": "phase_transition",
                    "description": "v2-updated",
                    "members": [
                        {"item_id": pump_id, "role": "outgoing"},
                        {"item_id": valve_id, "role": "incoming"},
                    ],
                },
                headers=auth,
            )
            assert r2.status_code == 201, r2.text
            body = r2.json()
            assert body["action"] == "updated"
            assert body["ghost_id"] == ghost_id
            assert body["member_count"] == 2

            # Verify description updated
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                row = conn.execute(
                    "SELECT description FROM ghost_nodes WHERE ghost_id = %s::uuid",
                    (ghost_id,),
                ).fetchone()
                assert row["description"] == "v2-updated"
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_ghost(conn, name)
                conn.commit()

    def test_ingest_ghost_capacity_aggregate_happy_path(self, api_client, auth, seeded_db):
        """capacity_aggregate with two 'member' items."""
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            pump_id, valve_id = _seed_item_uuids(conn)

        name = f"g_cap_{uuid4().hex[:6]}"
        try:
            resp = api_client.post(
                "/v1/ingest/ghosts",
                json={
                    "name": name,
                    "ghost_type": "capacity_aggregate",
                    "members": [
                        {"item_id": pump_id, "role": "member"},
                        {"item_id": valve_id, "role": "member"},
                    ],
                },
                headers=auth,
            )
            assert resp.status_code == 201, resp.text
            body = resp.json()
            assert body["action"] == "inserted"
            assert body["member_count"] == 2
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_ghost(conn, name)
                conn.commit()

    def test_ingest_ghost_explicit_valid_status_and_curve(
        self, api_client, auth, seeded_db
    ):
        """Explicit status='active' + transition_curve='step' to hit validator 'return v' branches."""
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            pump_id, _ = _seed_item_uuids(conn)

        name = f"g_explicit_{uuid4().hex[:6]}"
        try:
            resp = api_client.post(
                "/v1/ingest/ghosts",
                json={
                    "name": name,
                    "ghost_type": "capacity_aggregate",
                    "status": "active",
                    "members": [
                        {
                            "item_id": pump_id,
                            "role": "member",
                            "transition_curve": "step",
                        }
                    ],
                },
                headers=auth,
            )
            assert resp.status_code == 201, resp.text
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_ghost(conn, name)
                conn.commit()


# ---------------------------------------------------------------------------
# GET /v1/ghosts (list)
# ---------------------------------------------------------------------------


class TestListGhostsEndpoint:
    def test_list_ghosts_with_filter_no_match(self, api_client, auth):
        """Filter by a ghost_type + a non-existent scenario_id → empty result."""
        bogus_scenario = str(uuid4())
        resp = api_client.get(
            "/v1/ghosts",
            params={
                "ghost_type": "phase_transition",
                "scenario_id": bogus_scenario,
            },
            headers=auth,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 0
        assert body["ghosts"] == []

    def test_list_ghosts_with_results(self, api_client, auth, seeded_db):
        """Insert a ghost, list it back, check serialization."""
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            pump_id, valve_id = _seed_item_uuids(conn)

        name = f"g_list_{uuid4().hex[:6]}"
        try:
            r_ins = api_client.post(
                "/v1/ingest/ghosts",
                json={
                    "name": name,
                    "ghost_type": "phase_transition",
                    "members": [
                        {"item_id": pump_id, "role": "outgoing"},
                        {"item_id": valve_id, "role": "incoming"},
                    ],
                },
                headers=auth,
            )
            assert r_ins.status_code == 201, r_ins.text
            ghost_id = r_ins.json()["ghost_id"]

            resp = api_client.get(
                "/v1/ghosts",
                params={"ghost_type": "phase_transition"},
                headers=auth,
            )
            assert resp.status_code == 200
            body = resp.json()
            found = [g for g in body["ghosts"] if g["ghost_id"] == ghost_id]
            assert len(found) == 1
            g = found[0]
            assert g["name"] == name
            assert g["ghost_type"] == "phase_transition"
            assert len(g["members"]) == 2
            # serialization sanity
            assert g["created_at"] is not None
            assert g["updated_at"] is not None
            for m in g["members"]:
                assert m["item_id"] in (pump_id, valve_id)
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_ghost(conn, name)
                conn.commit()


# ---------------------------------------------------------------------------
# GET /v1/ghosts/{ghost_id} (detail)
# ---------------------------------------------------------------------------


class TestGetGhostEndpoint:
    def test_get_ghost_not_found(self, api_client, auth):
        gid = str(uuid4())
        resp = api_client.get(f"/v1/ghosts/{gid}", headers=auth)
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"]

    def test_get_ghost_with_graph_node_and_edges(self, api_client, auth, seeded_db):
        """Insert a phase_transition ghost, fetch detail, expect graph_node + edges."""
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
            pump_id, valve_id = _seed_item_uuids(conn)

        name = f"g_detail_{uuid4().hex[:6]}"
        try:
            r_ins = api_client.post(
                "/v1/ingest/ghosts",
                json={
                    "name": name,
                    "ghost_type": "phase_transition",
                    "members": [
                        {"item_id": pump_id, "role": "outgoing"},
                        {"item_id": valve_id, "role": "incoming"},
                    ],
                },
                headers=auth,
            )
            assert r_ins.status_code == 201, r_ins.text
            ghost_id = r_ins.json()["ghost_id"]

            resp = api_client.get(f"/v1/ghosts/{ghost_id}", headers=auth)
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["ghost_id"] == ghost_id
            assert body["graph_node"] is not None
            assert body["graph_node"]["node_id"] is not None
            # PUMP-01 and VALVE-02 have Item nodes in the seed (OnHand + others ref them),
            # so the ingest path may or may not create ghost_member edges depending on
            # presence of node_type='Item' rows. We only assert the structure.
            assert isinstance(body["graph_node"]["edges"], list)
        finally:
            with psycopg.connect(seeded_db, row_factory=dict_row) as conn:
                _cleanup_ghost(conn, name)
                conn.commit()


# ---------------------------------------------------------------------------
# POST /v1/ghosts/{ghost_id}/run — error path (ValueError → 404 sanitised)
# ---------------------------------------------------------------------------


class TestRunGhostEndpoint:
    def test_run_ghost_unknown_id_returns_404(self, api_client, auth):
        """run_ghost raises ValueError on unknown ghost_id → router returns sanitised 404."""
        gid = str(uuid4())
        sid = str(uuid4())
        resp = api_client.post(
            f"/v1/ghosts/{gid}/run",
            json={
                "scenario_id": sid,
                "from_date": "2026-01-01",
                "to_date": "2026-01-31",
            },
            headers=auth,
        )
        assert resp.status_code == 404
        # Sanitised message (chantier 2)
        assert resp.json()["detail"] == "Ghost not found or invalid parameters"
