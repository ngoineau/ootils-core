"""
tests/integration/test_ghosts.py — Ghosts V1 integration tests (ADR-010).

Covers:
  1.  Migration OK — ghost_nodes table exists
  2.  Migration OK — ghost_members table exists
  3.  ghost_nodes columns correct
  4.  ghost_members columns correct
  5.  Ingest ghost phase_transition (valid)
  6.  Ingest ghost phase_transition (membership constraint violation → 422)
  7.  Ingest ghost phase_transition (only 1 member → violation)
  8.  Ingest ghost capacity_aggregate (valid)
  9.  GET /v1/ghosts (list)
  10. GET /v1/ghosts/{ghost_id} (detail)
  11. GET /v1/ghosts/{ghost_id} (404)
  12. Calcul weight linear — correct interpolation
  13. Calcul weight linear inverse (0→1)
  14. Calcul weight step — stays at start until end
  15. Calcul weight step after end
  16. Calcul weight — no dates → weight_at_start
  17. POST /v1/ghosts/{ghost_id}/run phase_transition
  18. POST /v1/ghosts/{ghost_id}/run capacity_aggregate
  19. Alerte transition_inconsistency générée
  20. Alerte capacity_overload générée
  21. Ghost node créé dans la table nodes
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from uuid import uuid4

import pytest

from .conftest import requires_db, DB_AVAILABLE, TEST_DB_URL

# FastAPI test client
try:
    from fastapi.testclient import TestClient
    from ootils_core.api.app import app

    client = TestClient(app, raise_server_exceptions=False)
    AUTH_HEADERS = {"Authorization": "Bearer dev-token"}
    APP_AVAILABLE = True
except Exception:
    APP_AVAILABLE = False

requires_app = pytest.mark.skipif(not APP_AVAILABLE, reason="App not importable")

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _tables(conn) -> set[str]:
    rows = conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    ).fetchall()
    return {r["tablename"] for r in rows}


def _columns(conn, table: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    ).fetchall()
    return {r["column_name"] for r in rows}


def _create_item(conn, external_id: str = None) -> str:
    item_id = str(uuid4())
    ext_id = external_id or f"ITEM-{item_id[:8]}"
    conn.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        VALUES (%s, %s, %s, 'finished_good', 'EA', 'active')
        ON CONFLICT (external_id) DO NOTHING
        """,
        (item_id, ext_id, f"Test Item {ext_id}"),
    )
    row = conn.execute("SELECT item_id FROM items WHERE external_id = %s", (ext_id,)).fetchone()
    conn.commit()
    return str(row["item_id"])


def _create_resource(conn, capacity: float = 100.0) -> str:
    resource_id = str(uuid4())
    ext_id = f"RES-{resource_id[:8]}"
    conn.execute(
        """
        INSERT INTO resources (resource_id, external_id, name, resource_type, capacity_per_day)
        VALUES (%s, %s, %s, 'line', %s)
        """,
        (resource_id, ext_id, f"Resource {ext_id}", capacity),
    )
    conn.commit()
    return resource_id


# ─────────────────────────────────────────────────────────────
# Tests 1-4 — Migration
# ─────────────────────────────────────────────────────────────

@requires_db
def test_migration_ghost_nodes_table_exists(migrated_db, conn):
    """Migration creates ghost_nodes table."""
    assert "ghost_nodes" in _tables(conn)


@requires_db
def test_migration_ghost_members_table_exists(migrated_db, conn):
    """Migration creates ghost_members table."""
    assert "ghost_members" in _tables(conn)


@requires_db
def test_migration_ghost_nodes_columns(migrated_db, conn):
    """ghost_nodes has all required columns."""
    cols = _columns(conn, "ghost_nodes")
    for col in ("ghost_id", "name", "ghost_type", "scenario_id", "resource_id",
                "node_id", "status", "description", "created_at", "updated_at"):
        assert col in cols, f"Column {col!r} missing from ghost_nodes"


@requires_db
def test_migration_ghost_members_columns(migrated_db, conn):
    """ghost_members has all required columns."""
    cols = _columns(conn, "ghost_members")
    for col in ("member_id", "ghost_id", "item_id", "role",
                "transition_start_date", "transition_end_date",
                "transition_curve", "weight_at_start", "weight_at_end"):
        assert col in cols, f"Column {col!r} missing from ghost_members"


# ─────────────────────────────────────────────────────────────
# Tests 5-8 — Ingest
# ─────────────────────────────────────────────────────────────

@requires_db
@requires_app
def test_ingest_ghost_phase_transition_valid(migrated_db, conn):
    """POST /v1/ingest/ghosts — valid phase_transition with 1 outgoing + 1 incoming."""
    item_a = _create_item(conn, "PT-VALID-A")
    item_b = _create_item(conn, "PT-VALID-B")

    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "Valid PT Ghost",
            "ghost_type": "phase_transition",
            "members": [
                {
                    "item_id": item_a, "role": "outgoing",
                    "transition_start_date": "2026-05-01",
                    "transition_end_date": "2026-08-31",
                    "transition_curve": "linear",
                    "weight_at_start": 1.0, "weight_at_end": 0.0,
                },
                {
                    "item_id": item_b, "role": "incoming",
                    "transition_start_date": "2026-05-01",
                    "transition_end_date": "2026-08-31",
                    "transition_curve": "linear",
                    "weight_at_start": 0.0, "weight_at_end": 1.0,
                },
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert "ghost_id" in data
    assert data["action"] == "inserted"
    assert data["member_count"] == 2


@requires_db
@requires_app
def test_ingest_ghost_phase_transition_violation_two_incoming(migrated_db, conn):
    """POST /v1/ingest/ghosts — 2 incoming (no outgoing) → 422."""
    item_a = _create_item(conn, "PT-VIO1-A")
    item_b = _create_item(conn, "PT-VIO1-B")

    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "Bad PT Ghost 2inc",
            "ghost_type": "phase_transition",
            "members": [
                {"item_id": item_a, "role": "incoming"},
                {"item_id": item_b, "role": "incoming"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422, resp.text


@requires_db
@requires_app
def test_ingest_ghost_phase_transition_violation_one_member(migrated_db, conn):
    """POST /v1/ingest/ghosts — phase_transition with 1 outgoing only → 422."""
    item_a = _create_item(conn, "PT-VIO2-A")

    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "Bad PT Ghost 1out",
            "ghost_type": "phase_transition",
            "members": [{"item_id": item_a, "role": "outgoing"}],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 422, resp.text


@requires_db
@requires_app
def test_ingest_ghost_capacity_aggregate(migrated_db, conn):
    """POST /v1/ingest/ghosts — valid capacity_aggregate with 2 members."""
    item_a = _create_item(conn, "CA-VALID-A")
    item_b = _create_item(conn, "CA-VALID-B")
    resource_id = _create_resource(conn)

    resp = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "Valid CA Ghost",
            "ghost_type": "capacity_aggregate",
            "resource_id": resource_id,
            "members": [
                {"item_id": item_a, "role": "member"},
                {"item_id": item_b, "role": "member"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["member_count"] == 2
    assert data["action"] == "inserted"


# ─────────────────────────────────────────────────────────────
# Tests 9-11 — GET endpoints
# ─────────────────────────────────────────────────────────────

@requires_db
@requires_app
def test_list_ghosts(migrated_db, conn):
    """GET /v1/ghosts returns list."""
    resp = client.get("/v1/ghosts", headers=AUTH_HEADERS)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "ghosts" in data
    assert isinstance(data["ghosts"], list)
    assert "total" in data


@requires_db
@requires_app
def test_get_ghost_detail(migrated_db, conn):
    """GET /v1/ghosts/{ghost_id} returns detail with members and graph_node."""
    item_a = _create_item(conn, "DET-A")
    item_b = _create_item(conn, "DET-B")

    create = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "Detail Test Ghost",
            "ghost_type": "phase_transition",
            "members": [
                {"item_id": item_a, "role": "outgoing", "weight_at_start": 1.0, "weight_at_end": 0.0},
                {"item_id": item_b, "role": "incoming", "weight_at_start": 0.0, "weight_at_end": 1.0},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert create.status_code == 201
    ghost_id = create.json()["ghost_id"]

    resp = client.get(f"/v1/ghosts/{ghost_id}", headers=AUTH_HEADERS)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["ghost_id"] == ghost_id
    assert data["ghost_type"] == "phase_transition"
    assert len(data["members"]) == 2
    assert "graph_node" in data
    assert data["graph_node"]["node_type"] == "Ghost"


@requires_db
@requires_app
def test_get_ghost_not_found(migrated_db):
    """GET /v1/ghosts/{ghost_id} → 404 for unknown id."""
    resp = client.get(f"/v1/ghosts/{uuid4()}", headers=AUTH_HEADERS)
    assert resp.status_code == 404


# ─────────────────────────────────────────────────────────────
# Tests 12-16 — Weight calculation (pure unit tests, no DB)
# ─────────────────────────────────────────────────────────────

def test_weight_linear_at_midpoint():
    """Linear weight ≈ 0.5 at midpoint of transition window."""
    from ootils_core.engine.ghost.phase_transition import compute_weight

    start = date(2026, 6, 1)
    end = date(2026, 6, 21)  # 20 days
    mid = date(2026, 6, 11)  # 10 days in = 50%

    w = compute_weight(mid, start, end, "linear", 1.0, 0.0)
    assert abs(w - 0.5) < 0.01, f"Expected ~0.5 at midpoint, got {w}"


def test_weight_linear_inverse():
    """Linear inverse: 0.0 → 1.0 transition."""
    from ootils_core.engine.ghost.phase_transition import compute_weight

    start = date(2026, 6, 1)
    end = date(2026, 9, 1)

    assert compute_weight(date(2026, 4, 1), start, end, "linear", 0.0, 1.0) == 0.0
    assert compute_weight(date(2026, 10, 1), start, end, "linear", 0.0, 1.0) == 1.0
    mid_days = (end - start).days // 2
    w = compute_weight(start + timedelta(days=mid_days), start, end, "linear", 0.0, 1.0)
    assert 0.4 < w < 0.6


def test_weight_step_during_transition():
    """Step curve stays at weight_at_start during transition."""
    from ootils_core.engine.ghost.phase_transition import compute_weight

    start = date(2026, 5, 1)
    end = date(2026, 9, 1)

    w = compute_weight(date(2026, 6, 15), start, end, "step", 1.0, 0.0)
    assert w == 1.0, f"Expected 1.0 during step, got {w}"


def test_weight_step_after_end():
    """Step curve flips to weight_at_end after transition_end_date."""
    from ootils_core.engine.ghost.phase_transition import compute_weight

    start = date(2026, 5, 1)
    end = date(2026, 9, 1)

    w = compute_weight(date(2026, 10, 1), start, end, "step", 1.0, 0.0)
    assert w == 0.0, f"Expected 0.0 after step end, got {w}"


def test_weight_no_dates():
    """No transition dates → always weight_at_start."""
    from ootils_core.engine.ghost.phase_transition import compute_weight

    w = compute_weight(date(2026, 6, 1), None, None, "linear", 0.75, 0.25)
    assert w == 0.75


# ─────────────────────────────────────────────────────────────
# Tests 17-18 — /run endpoints
# ─────────────────────────────────────────────────────────────

@requires_db
@requires_app
def test_run_ghost_phase_transition(migrated_db, conn):
    """POST /v1/ghosts/{ghost_id}/run — phase_transition returns 7 weight samples."""
    item_a = _create_item(conn, "RUN-PT-A")
    item_b = _create_item(conn, "RUN-PT-B")

    create = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "Run PT",
            "ghost_type": "phase_transition",
            "members": [
                {
                    "item_id": item_a, "role": "outgoing",
                    "transition_start_date": "2026-06-01",
                    "transition_end_date": "2026-06-30",
                    "weight_at_start": 1.0, "weight_at_end": 0.0,
                },
                {
                    "item_id": item_b, "role": "incoming",
                    "transition_start_date": "2026-06-01",
                    "transition_end_date": "2026-06-30",
                    "weight_at_start": 0.0, "weight_at_end": 1.0,
                },
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert create.status_code == 201
    ghost_id = create.json()["ghost_id"]

    run = client.post(
        f"/v1/ghosts/{ghost_id}/run",
        json={
            "scenario_id": "00000000-0000-0000-0000-000000000001",
            "from_date": "2026-06-01",
            "to_date": "2026-06-07",
        },
        headers=AUTH_HEADERS,
    )
    assert run.status_code == 200, run.text
    data = run.json()
    assert data["ghost_type"] == "phase_transition"
    assert isinstance(data["alerts"], list)
    assert len(data["summary"]["weight_samples"]) == 7


@requires_db
@requires_app
def test_run_ghost_capacity_aggregate(migrated_db, conn):
    """POST /v1/ghosts/{ghost_id}/run — capacity_aggregate returns 3 periods."""
    item_a = _create_item(conn, "RUN-CA-A")
    item_b = _create_item(conn, "RUN-CA-B")
    resource_id = _create_resource(conn)

    create = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "Run CA",
            "ghost_type": "capacity_aggregate",
            "resource_id": resource_id,
            "members": [
                {"item_id": item_a, "role": "member"},
                {"item_id": item_b, "role": "member"},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert create.status_code == 201
    ghost_id = create.json()["ghost_id"]

    run = client.post(
        f"/v1/ghosts/{ghost_id}/run",
        json={
            "scenario_id": "00000000-0000-0000-0000-000000000001",
            "from_date": "2026-06-01",
            "to_date": "2026-06-03",
        },
        headers=AUTH_HEADERS,
    )
    assert run.status_code == 200, run.text
    data = run.json()
    assert data["ghost_type"] == "capacity_aggregate"
    assert len(data["summary"]["periods"]) == 3


# ─────────────────────────────────────────────────────────────
# Test 19 — transition_inconsistency alert
# ─────────────────────────────────────────────────────────────

@requires_db
def test_transition_inconsistency_alert(migrated_db, conn):
    """Engine generates transition_inconsistency when sum deviates > 10% from baseline."""
    from ootils_core.engine.ghost.phase_transition import run_phase_transition

    import psycopg
    from psycopg.rows import dict_row

    item_a = _create_item(conn, "INC-A")
    item_b = _create_item(conn, "INC-B")
    ghost_id = str(uuid4())
    scenario_id = "00000000-0000-0000-0000-000000000001"

    conn.execute(
        """INSERT INTO ghost_nodes (ghost_id, name, ghost_type, status)
           VALUES (%s, 'Inc Ghost', 'phase_transition', 'active')""",
        (ghost_id,),
    )
    conn.execute(
        """INSERT INTO ghost_members
               (ghost_id, item_id, role, transition_start_date, transition_end_date,
                transition_curve, weight_at_start, weight_at_end)
           VALUES (%s, %s, 'outgoing', '2026-06-01', '2026-06-30', 'linear', 1.0, 0.0)""",
        (ghost_id, item_a),
    )
    conn.execute(
        """INSERT INTO ghost_members
               (ghost_id, item_id, role, transition_start_date, transition_end_date,
                transition_curve, weight_at_start, weight_at_end)
           VALUES (%s, %s, 'incoming', '2026-06-01', '2026-06-30', 'linear', 0.0, 1.0)""",
        (ghost_id, item_b),
    )
    # At 2026-06-15: ratio ≈ 0.467, weight_A ≈ 0.533
    # proj_a = 500 → baseline = 500 / 0.533 ≈ 938
    # proj_b = 700 → observed = 1200, delta_pct ≈ 28% > 10%
    conn.execute(
        """INSERT INTO nodes (node_id, node_type, scenario_id, item_id, quantity, time_grain, time_ref, active)
           VALUES (%s, 'ProjectedInventory', %s, %s, 500, 'day', '2026-06-15', TRUE)""",
        (str(uuid4()), scenario_id, item_a),
    )
    conn.execute(
        """INSERT INTO nodes (node_id, node_type, scenario_id, item_id, quantity, time_grain, time_ref, active)
           VALUES (%s, 'ProjectedInventory', %s, %s, 700, 'day', '2026-06-15', TRUE)""",
        (str(uuid4()), scenario_id, item_b),
    )
    conn.commit()

    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as c:
        result = run_phase_transition(
            c, ghost_id, scenario_id,
            date(2026, 6, 15), date(2026, 6, 15),
        )

    assert len(result["alerts"]) > 0, "Expected transition_inconsistency alert"
    assert result["alerts"][0]["type"] == "transition_inconsistency"
    assert result["alerts"][0]["delta_pct"] > 0.10


# ─────────────────────────────────────────────────────────────
# Test 20 — capacity_overload alert
# ─────────────────────────────────────────────────────────────

@requires_db
def test_capacity_overload_alert(migrated_db, conn):
    """Engine generates capacity_overload when load 80+60=140 > capacity 100."""
    from ootils_core.engine.ghost.capacity_aggregate import run_capacity_aggregate

    import psycopg
    from psycopg.rows import dict_row

    item_a = _create_item(conn, "OVL-A")
    item_b = _create_item(conn, "OVL-B")
    resource_id = _create_resource(conn, capacity=100.0)
    ghost_id = str(uuid4())
    scenario_id = "00000000-0000-0000-0000-000000000001"

    conn.execute(
        """INSERT INTO ghost_nodes (ghost_id, name, ghost_type, resource_id, status)
           VALUES (%s, 'Overload Ghost', 'capacity_aggregate', %s, 'active')""",
        (ghost_id, resource_id),
    )
    conn.execute(
        "INSERT INTO ghost_members (ghost_id, item_id, role) VALUES (%s, %s, 'member')",
        (ghost_id, item_a),
    )
    conn.execute(
        "INSERT INTO ghost_members (ghost_id, item_id, role) VALUES (%s, %s, 'member')",
        (ghost_id, item_b),
    )
    conn.execute(
        """INSERT INTO nodes (node_id, node_type, scenario_id, item_id, quantity, time_grain, time_ref, active)
           VALUES (%s, 'WorkOrderSupply', %s, %s, 80, 'day', '2026-06-10', TRUE)""",
        (str(uuid4()), scenario_id, item_a),
    )
    conn.execute(
        """INSERT INTO nodes (node_id, node_type, scenario_id, item_id, quantity, time_grain, time_ref, active)
           VALUES (%s, 'WorkOrderSupply', %s, %s, 60, 'day', '2026-06-10', TRUE)""",
        (str(uuid4()), scenario_id, item_b),
    )
    conn.commit()

    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as c:
        result = run_capacity_aggregate(
            c, ghost_id, scenario_id,
            date(2026, 6, 10), date(2026, 6, 10),
        )

    assert len(result["alerts"]) > 0, "Expected capacity_overload alert"
    assert result["alerts"][0]["type"] == "capacity_overload"
    assert result["alerts"][0]["load"] == 140.0
    assert result["alerts"][0]["slack"] == -40.0


# ─────────────────────────────────────────────────────────────
# Test 21 — Ghost node in nodes table
# ─────────────────────────────────────────────────────────────

@requires_db
@requires_app
def test_ghost_node_in_nodes_table(migrated_db, conn):
    """Ingest creates a node with node_type='Ghost' in the nodes table."""
    item_a = _create_item(conn, "NODECHK-A")
    item_b = _create_item(conn, "NODECHK-B")

    create = client.post(
        "/v1/ingest/ghosts",
        json={
            "name": "Node Check Ghost",
            "ghost_type": "phase_transition",
            "members": [
                {"item_id": item_a, "role": "outgoing", "weight_at_start": 1.0, "weight_at_end": 0.0},
                {"item_id": item_b, "role": "incoming", "weight_at_start": 0.0, "weight_at_end": 1.0},
            ],
        },
        headers=AUTH_HEADERS,
    )
    assert create.status_code == 201, create.text
    node_id = create.json().get("node_id")
    assert node_id is not None

    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(TEST_DB_URL, row_factory=dict_row) as c:
        row = c.execute(
            "SELECT node_type FROM nodes WHERE node_id = %s", (node_id,)
        ).fetchone()

    assert row is not None, f"Node {node_id} not found in nodes table"
    assert row["node_type"] == "Ghost"
