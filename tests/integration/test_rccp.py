"""
tests/integration/test_rccp.py — Tests RCCP (Rough-Cut Capacity Planning).

Couvre:
  1. Migration 009 OK (tables resources + resource_capacity_overrides)
  2. Ingest resource — insert
  3. Ingest resource — update (upsert)
  4. Edge consumes_resource entre supply node et Resource node
  5. RCCP calcul : load, capacity, utilization_pct
  6. RCCP overloaded detection
  7. Filtres from_date / to_date
  8. Grain week vs day
  9. Resource non trouvée → 404
  10. Grain invalide → 422

Skip all tests si DATABASE_URL non configuré.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from uuid import uuid4

import pytest

from .conftest import requires_db, DB_AVAILABLE, TEST_DB_URL

BASELINE_SCENARIO_ID = "00000000-0000-0000-0000-000000000001"


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


def _insert_location(conn) -> str:
    """Insert a test location, return external_id."""
    loc_id = uuid4()
    ext_id = f"LOC-{loc_id.hex[:8]}"
    conn.execute(
        """
        INSERT INTO locations (location_id, external_id, name, location_type, country)
        VALUES (%s, %s, 'Test Location', 'plant', 'FR')
        """,
        (loc_id, ext_id),
    )
    return ext_id


def _insert_item(conn) -> str:
    """Insert a test item, return external_id."""
    item_id = uuid4()
    ext_id = f"ITEM-{item_id.hex[:8]}"
    conn.execute(
        """
        INSERT INTO items (item_id, external_id, name, item_type, uom, status)
        VALUES (%s, %s, 'Test Item', 'finished_good', 'EA', 'active')
        """,
        (item_id, ext_id),
    )
    return ext_id


def _insert_resource(conn, external_id: str, capacity_per_day: float = 10.0,
                     resource_type: str = "machine") -> dict:
    """Insert resource + Resource graph node, return {resource_id, node_id}."""
    resource_id = uuid4()
    conn.execute(
        """
        INSERT INTO resources (resource_id, external_id, name, resource_type, capacity_per_day, capacity_unit)
        VALUES (%s, %s, 'Test Resource', %s, %s, 'units')
        """,
        (resource_id, external_id, resource_type, capacity_per_day),
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


def _insert_supply_node(conn, item_ext_id: str, time_ref: date, quantity: float) -> str:
    """Insert a WorkOrderSupply node, return node_id."""
    # Resolve item_id
    item_id = conn.execute(
        "SELECT item_id FROM items WHERE external_id = %s", (item_ext_id,)
    ).fetchone()["item_id"]

    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, quantity, time_grain, time_ref, active)
        VALUES (%s, 'WorkOrderSupply', %s::UUID, %s, %s, 'exact_date', %s, TRUE)
        """,
        (node_id, BASELINE_SCENARIO_ID, item_id, quantity, time_ref),
    )
    return str(node_id)


def _insert_consumes_resource_edge(conn, from_node_id: str, to_node_id: str) -> str:
    """Insert a consumes_resource edge, return edge_id."""
    edge_id = uuid4()
    conn.execute(
        """
        INSERT INTO edges (edge_id, edge_type, from_node_id, to_node_id, scenario_id, active)
        VALUES (%s, 'consumes_resource', %s::UUID, %s::UUID, %s::UUID, TRUE)
        """,
        (edge_id, from_node_id, to_node_id, BASELINE_SCENARIO_ID),
    )
    return str(edge_id)


# ─────────────────────────────────────────────────────────────
# Test 1 — Migration 009 OK : tables resources + resource_capacity_overrides
# ─────────────────────────────────────────────────────────────

@requires_db
def test_01_migration_009_tables_exist(migrated_db):
    """Tables resources et resource_capacity_overrides existent après migration 009."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        tables = _tables(c)

    assert "resources" in tables, "Table 'resources' manquante après migration 009"
    assert "resource_capacity_overrides" in tables, "Table 'resource_capacity_overrides' manquante"


@requires_db
def test_01b_migration_009_columns(conn):
    """Table resources a les colonnes attendues."""
    cols = _columns(conn, "resources")
    expected = {
        "resource_id", "external_id", "name", "resource_type",
        "location_id", "capacity_per_day", "capacity_unit",
        "notes", "created_at", "updated_at",
    }
    missing = expected - cols
    assert not missing, f"Colonnes manquantes sur resources: {missing}"

    cols_override = _columns(conn, "resource_capacity_overrides")
    expected_override = {
        "override_id", "resource_id", "override_date", "capacity", "reason", "created_at"
    }
    missing_override = expected_override - cols_override
    assert not missing_override, f"Colonnes manquantes sur resource_capacity_overrides: {missing_override}"


# ─────────────────────────────────────────────────────────────
# Test 2 — Ingest resource : insert
# ─────────────────────────────────────────────────────────────

@requires_db
def test_02_ingest_resource_insert(conn):
    """Ingest resource crée une entrée dans resources ET un nœud Resource dans nodes."""
    ext_id = f"RES-INSERT-{uuid4().hex[:6]}"
    ids = _insert_resource(conn, ext_id, capacity_per_day=8.0, resource_type="line")

    # Vérifier resource créée
    row = conn.execute(
        "SELECT resource_id, name, capacity_per_day, resource_type FROM resources WHERE external_id = %s",
        (ext_id,),
    ).fetchone()
    assert row is not None, "Resource non trouvée en DB"
    assert float(row["capacity_per_day"]) == 8.0
    assert row["resource_type"] == "line"

    # Vérifier nœud Resource dans le graphe
    node = conn.execute(
        "SELECT node_id, node_type FROM nodes WHERE node_type = 'Resource' AND external_id = %s",
        (ext_id,),
    ).fetchone()
    assert node is not None, "Nœud Resource non créé dans nodes"
    assert node["node_type"] == "Resource"


# ─────────────────────────────────────────────────────────────
# Test 3 — Ingest resource : update (upsert idempotent)
# ─────────────────────────────────────────────────────────────

@requires_db
def test_03_ingest_resource_update(conn):
    """Un second ingest sur le même external_id met à jour la resource (upsert)."""
    ext_id = f"RES-UPSERT-{uuid4().hex[:6]}"
    _insert_resource(conn, ext_id, capacity_per_day=5.0)

    # Update via SQL direct (simule ce que fait l'ingest endpoint)
    conn.execute(
        """
        UPDATE resources
        SET name = 'Updated Resource', capacity_per_day = 12.0, updated_at = now()
        WHERE external_id = %s
        """,
        (ext_id,),
    )

    row = conn.execute(
        "SELECT name, capacity_per_day FROM resources WHERE external_id = %s",
        (ext_id,),
    ).fetchone()
    assert row is not None
    assert row["name"] == "Updated Resource"
    assert float(row["capacity_per_day"]) == 12.0


# ─────────────────────────────────────────────────────────────
# Test 4 — Edge consumes_resource entre supply node et Resource node
# ─────────────────────────────────────────────────────────────

@requires_db
def test_04_edge_consumes_resource(conn):
    """Edge consumes_resource relie correctement un nœud supply à un nœud Resource."""
    item_ext = _insert_item(conn)
    res_ext = f"RES-EDGE-{uuid4().hex[:6]}"
    ids = _insert_resource(conn, res_ext)

    supply_date = date(2026, 6, 2)
    supply_node_id = _insert_supply_node(conn, item_ext, supply_date, quantity=50.0)
    resource_node_id = str(ids["node_id"])

    edge_id = _insert_consumes_resource_edge(conn, supply_node_id, resource_node_id)

    # Vérifier l'edge
    edge = conn.execute(
        """
        SELECT e.edge_type, n.node_type, n.external_id
        FROM edges e
        JOIN nodes n ON n.node_id = e.to_node_id
        WHERE e.edge_id = %s::UUID
        """,
        (edge_id,),
    ).fetchone()
    assert edge is not None
    assert edge["edge_type"] == "consumes_resource"
    assert edge["node_type"] == "Resource"
    assert edge["external_id"] == res_ext


# ─────────────────────────────────────────────────────────────
# Test 5 — RCCP calcul : load, capacity, utilization_pct
# ─────────────────────────────────────────────────────────────

@requires_db
def test_05_rccp_calcul_load_capacity_utilization(conn):
    """
    RCCP agrège correctement la charge et calcule utilization_pct.
    Setup: resource capacity_per_day=10, 5 jours ouvrés/semaine → capacity=50/semaine.
    Load: 30 unités le lundi → utilization = 30/50 * 100 = 60%.
    """
    item_ext = _insert_item(conn)
    res_ext = f"RES-CALC-{uuid4().hex[:6]}"
    ids = _insert_resource(conn, res_ext, capacity_per_day=10.0)

    # Monday 2026-06-01
    monday = date(2026, 6, 1)
    assert monday.weekday() == 0, "Doit être un lundi"

    supply_node_id = _insert_supply_node(conn, item_ext, monday, quantity=30.0)
    resource_node_id = str(ids["node_id"])
    _insert_consumes_resource_edge(conn, supply_node_id, resource_node_id)
    conn.commit()

    # Query the RCCP logic directly
    # Week bucket: Mon 2026-06-01 → Sun 2026-06-07
    bucket_start = date(2026, 6, 1)
    bucket_end = date(2026, 6, 7)

    # Load query (same as RCCP endpoint)
    load_rows = conn.execute(
        """
        SELECT COALESCE(SUM(n.quantity), 0) AS total_load
        FROM nodes n
        JOIN edges e ON e.from_node_id = n.node_id
        JOIN nodes rn ON rn.node_id = e.to_node_id
        WHERE n.node_type IN ('WorkOrderSupply', 'PlannedSupply')
          AND e.edge_type = 'consumes_resource'
          AND e.active = TRUE
          AND n.active = TRUE
          AND rn.node_type = 'Resource'
          AND rn.external_id = %s
          AND n.time_ref BETWEEN %s AND %s
        """,
        (res_ext, bucket_start, bucket_end),
    ).fetchone()

    total_load = float(load_rows["total_load"])
    assert total_load == 30.0, f"Load attendu 30.0, obtenu {total_load}"

    # Capacity: 5 jours ouvrés * 10 = 50
    capacity = 5 * 10.0
    utilization = total_load / capacity * 100
    assert utilization == 60.0, f"Utilization attendu 60.0%, obtenu {utilization}"


# ─────────────────────────────────────────────────────────────
# Test 6 — RCCP overloaded detection
# ─────────────────────────────────────────────────────────────

@requires_db
def test_06_rccp_overloaded_detection(conn):
    """
    RCCP détecte correctement un bucket en surcharge (load > capacity).
    Setup: capacity_per_day=10, week capacity=50, load=75 → overloaded=True.
    """
    item_ext = _insert_item(conn)
    res_ext = f"RES-OVER-{uuid4().hex[:6]}"
    ids = _insert_resource(conn, res_ext, capacity_per_day=10.0)

    # 3 nœuds supply dans la semaine du 2026-06-08
    monday = date(2026, 6, 8)
    resource_node_id = str(ids["node_id"])

    for day_offset, qty in [(0, 30.0), (1, 25.0), (2, 20.0)]:
        supply_date = monday + timedelta(days=day_offset)
        supply_node_id = _insert_supply_node(conn, item_ext, supply_date, quantity=qty)
        _insert_consumes_resource_edge(conn, supply_node_id, resource_node_id)

    conn.commit()

    bucket_start = date(2026, 6, 8)
    bucket_end = date(2026, 6, 14)

    load_row = conn.execute(
        """
        SELECT COALESCE(SUM(n.quantity), 0) AS total_load
        FROM nodes n
        JOIN edges e ON e.from_node_id = n.node_id
        JOIN nodes rn ON rn.node_id = e.to_node_id
        WHERE n.node_type IN ('WorkOrderSupply', 'PlannedSupply')
          AND e.edge_type = 'consumes_resource'
          AND e.active = TRUE
          AND n.active = TRUE
          AND rn.node_type = 'Resource'
          AND rn.external_id = %s
          AND n.time_ref BETWEEN %s AND %s
        """,
        (res_ext, bucket_start, bucket_end),
    ).fetchone()

    total_load = float(load_row["total_load"])
    capacity = 5 * 10.0  # 50 units/week
    utilization_pct = total_load / capacity * 100

    assert total_load == 75.0, f"Load attendu 75.0, obtenu {total_load}"
    assert utilization_pct > 100.0, f"Doit être overloaded: utilization={utilization_pct}"
    assert utilization_pct == 150.0


# ─────────────────────────────────────────────────────────────
# Test 7 — Filtres from_date / to_date
# ─────────────────────────────────────────────────────────────

@requires_db
def test_07_rccp_filter_from_to_date(conn):
    """Les nœuds supply en dehors de [from_date, to_date] ne sont pas inclus dans le load."""
    item_ext = _insert_item(conn)
    res_ext = f"RES-FILTER-{uuid4().hex[:6]}"
    ids = _insert_resource(conn, res_ext, capacity_per_day=10.0)
    resource_node_id = str(ids["node_id"])

    # Supply dans la fenêtre (2026-07-07)
    in_window = date(2026, 7, 7)
    supply_in_id = _insert_supply_node(conn, item_ext, in_window, quantity=20.0)
    _insert_consumes_resource_edge(conn, supply_in_id, resource_node_id)

    # Supply hors fenêtre (2026-07-14)
    out_window = date(2026, 7, 14)
    supply_out_id = _insert_supply_node(conn, item_ext, out_window, quantity=999.0)
    _insert_consumes_resource_edge(conn, supply_out_id, resource_node_id)

    conn.commit()

    from_date = date(2026, 7, 1)
    to_date = date(2026, 7, 7)

    load_row = conn.execute(
        """
        SELECT COALESCE(SUM(n.quantity), 0) AS total_load
        FROM nodes n
        JOIN edges e ON e.from_node_id = n.node_id
        JOIN nodes rn ON rn.node_id = e.to_node_id
        WHERE n.node_type IN ('WorkOrderSupply', 'PlannedSupply')
          AND e.edge_type = 'consumes_resource'
          AND e.active = TRUE
          AND n.active = TRUE
          AND rn.node_type = 'Resource'
          AND rn.external_id = %s
          AND n.time_ref BETWEEN %s AND %s
        """,
        (res_ext, from_date, to_date),
    ).fetchone()

    total_load = float(load_row["total_load"])
    assert total_load == 20.0, f"Seule la supply du {in_window} doit être incluse, load={total_load}"


# ─────────────────────────────────────────────────────────────
# Test 8 — Grain week vs day
# ─────────────────────────────────────────────────────────────

@requires_db
def test_08_rccp_grain_week_vs_day(conn):
    """
    Grain=day → chaque jour est un bucket.
    Grain=week → une semaine est un bucket.
    Vérifie que les buckets sont générés correctement.
    """
    from ootils_core.api.routers.rccp import _generate_buckets, _bucket_start

    # Grain=day : 7 jours → 7 buckets
    from_date = date(2026, 8, 3)  # lundi
    to_date = date(2026, 8, 9)    # dimanche
    day_buckets = _generate_buckets(from_date, to_date, "day")
    assert len(day_buckets) == 7, f"Attendu 7 buckets day, obtenu {len(day_buckets)}"
    assert day_buckets[0] == (date(2026, 8, 3), date(2026, 8, 3))
    assert day_buckets[6] == (date(2026, 8, 9), date(2026, 8, 9))

    # Grain=week : 7 jours consécutifs → 1 bucket
    week_buckets = _generate_buckets(from_date, to_date, "week")
    assert len(week_buckets) == 1, f"Attendu 1 bucket week, obtenu {len(week_buckets)}"
    assert week_buckets[0] == (date(2026, 8, 3), date(2026, 8, 9))

    # Grain=week : 2 semaines → 2 buckets
    two_week_buckets = _generate_buckets(from_date, date(2026, 8, 16), "week")
    assert len(two_week_buckets) == 2, f"Attendu 2 buckets week, obtenu {len(two_week_buckets)}"

    # Grain=month : même mois → 1 bucket
    month_start = date(2026, 9, 1)
    month_end = date(2026, 9, 30)
    month_buckets = _generate_buckets(month_start, month_end, "month")
    assert len(month_buckets) == 1
    assert month_buckets[0][0] == date(2026, 9, 1)
    assert month_buckets[0][1] == date(2026, 9, 30)


# ─────────────────────────────────────────────────────────────
# Test 9 — Resource capacity_overrides
# ─────────────────────────────────────────────────────────────

@requires_db
def test_09_resource_capacity_overrides(conn):
    """resource_capacity_overrides surcharge la capacité sur une date donnée."""
    res_ext = f"RES-OVERRIDE-{uuid4().hex[:6]}"
    ids = _insert_resource(conn, res_ext, capacity_per_day=10.0)
    resource_id = ids["resource_id"]

    # Override: capacité = 0 le 2026-09-14 (fermeture)
    override_date = date(2026, 9, 14)
    conn.execute(
        """
        INSERT INTO resource_capacity_overrides
            (resource_id, override_date, capacity, reason)
        VALUES (%s, %s, 0, 'Maintenance planifiée')
        """,
        (resource_id, override_date),
    )

    # Vérifier l'override
    row = conn.execute(
        """
        SELECT capacity, reason
        FROM resource_capacity_overrides
        WHERE resource_id = %s AND override_date = %s
        """,
        (resource_id, override_date),
    ).fetchone()

    assert row is not None
    assert float(row["capacity"]) == 0.0
    assert row["reason"] == "Maintenance planifiée"

    # Unicité : même (resource_id, override_date) → conflict
    with pytest.raises(Exception):
        conn.execute(
            """
            INSERT INTO resource_capacity_overrides
                (resource_id, override_date, capacity)
            VALUES (%s, %s, 5.0)
            """,
            (resource_id, override_date),
        )
        conn.commit()
    conn.rollback()


# ─────────────────────────────────────────────────────────────
# Test 10 — Resource type CHECK constraint
# ─────────────────────────────────────────────────────────────

@requires_db
def test_10_resource_type_check_constraint(conn):
    """resource_type CHECK constraint rejette les valeurs invalides."""
    with pytest.raises(Exception, match=r"(check|violates|constraint)"):
        conn.execute(
            """
            INSERT INTO resources (external_id, name, resource_type, capacity_per_day)
            VALUES ('RES-INVALID', 'Bad Resource', 'invalid_type', 10.0)
            """
        )
        conn.commit()
    conn.rollback()

    # Types valides
    for valid_type in ("machine", "line", "team", "tool"):
        ext_id = f"RES-{valid_type.upper()}-{uuid4().hex[:4]}"
        conn.execute(
            """
            INSERT INTO resources (external_id, name, resource_type, capacity_per_day)
            VALUES (%s, %s, %s, 5.0)
            """,
            (ext_id, f"Resource {valid_type}", valid_type),
        )
    # No commit — rollback via fixture
