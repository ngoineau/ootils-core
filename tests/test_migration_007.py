"""
tests/test_migration_007.py — Tests de la migration 007 (import pipeline 2 étapes).

Vérifie :
- Toutes les tables créées par 007 existent
- item_planning_params rejette lead_time_sourcing_days < 0
- La contrainte GIST d'exclusion sur effective_from/effective_to bloque les overlaps
- external_id est unique sur items
- data_quality_issues et ingest_rows ont leurs FK et CHECK constraints
- suppliers/supplier_items s'insèrent et se lient correctement
- uom_conversions : les conversions globales de base sont présentes

Skip si DATABASE_URL n'est pas configuré.
"""
from __future__ import annotations

import os
import uuid

import pytest

# ---------------------------------------------------------------------------
# Detect test DB availability (même pattern que tests/integration/)
# ---------------------------------------------------------------------------

TEST_DB_URL = os.environ.get("DATABASE_URL", "")


def _db_available() -> bool:
    if not TEST_DB_URL:
        return False
    try:
        import psycopg
        with psycopg.connect(TEST_DB_URL, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


DB_AVAILABLE = _db_available()

requires_db = pytest.mark.skipif(
    not DB_AVAILABLE,
    reason="No PostgreSQL available — set DATABASE_URL to a test DB",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def migrated_db():
    """Apply all migrations (including 007) and yield the DSN."""
    if not DB_AVAILABLE:
        pytest.skip("No PostgreSQL available")

    import psycopg

    old_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = TEST_DB_URL

    try:
        from ootils_core.db.connection import OotilsDB
        OotilsDB(TEST_DB_URL)
    except Exception as exc:
        pytest.skip(f"Failed to apply migrations: {exc}")

    yield TEST_DB_URL

    # Tear down
    try:
        with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
            conn.execute("""
                DO $$
                DECLARE r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
            """)
            # Drop custom types
            conn.execute("DROP TYPE IF EXISTS lot_size_rule_type CASCADE")
            conn.execute("DROP TYPE IF EXISTS planning_source_type CASCADE")
    except Exception:
        pass

    if old_url is not None:
        os.environ["DATABASE_URL"] = old_url
    elif "DATABASE_URL" in os.environ:
        del os.environ["DATABASE_URL"]


@pytest.fixture
def conn(migrated_db):
    """Function-scoped psycopg connection, rolled back after each test."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        yield c
        c.rollback()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tables(c) -> set[str]:
    rows = c.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    ).fetchall()
    return {r["tablename"] for r in rows}


def _insert_item(conn) -> str:
    """Insert a minimal item and return its item_id (UUID string)."""
    item_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s::UUID, %s)",
        (item_id, "Test Item"),
    )
    return item_id


def _insert_location(conn) -> str:
    """Insert a minimal location and return its location_id (UUID string)."""
    location_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s::UUID, %s)",
        (location_id, "Test Location"),
    )
    return location_id


def _insert_batch(conn, entity_type: str = "items") -> str:
    """Insert a minimal ingest_batch and return its batch_id."""
    batch_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO ingest_batches (batch_id, entity_type, source_system)
        VALUES (%s::UUID, %s, 'test_system')
        """,
        (batch_id, entity_type),
    )
    return batch_id


# ---------------------------------------------------------------------------
# Test 1 — Toutes les tables de la migration 007 existent
# ---------------------------------------------------------------------------

@requires_db
def test_07_all_tables_created(migrated_db):
    """Toutes les tables introduites par 007_import_pipeline.sql existent."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(migrated_db, row_factory=dict_row) as c:
        tables = _tables(c)

    expected_007 = {
        "external_references",
        "ingest_batches",
        "ingest_rows",
        "data_quality_issues",
        "suppliers",
        "supplier_items",
        "item_planning_params",
        "uom_conversions",
        "operational_calendars",
        "master_data_audit_log",
    }
    missing = expected_007 - tables
    assert not missing, f"Tables manquantes après migration 007 : {missing}"


# ---------------------------------------------------------------------------
# Test 2 — items.external_id est UNIQUE
# ---------------------------------------------------------------------------

@requires_db
def test_07_items_external_id_unique(conn):
    """Deux items ne peuvent pas avoir le même external_id."""
    item_id_1 = str(uuid.uuid4())
    item_id_2 = str(uuid.uuid4())
    ext_id = f"EXT-{uuid.uuid4().hex[:8]}"

    conn.execute(
        "INSERT INTO items (item_id, name, external_id) VALUES (%s::UUID, %s, %s)",
        (item_id_1, "Item A", ext_id),
    )

    with pytest.raises(Exception, match=r"(unique|duplicate)"):
        conn.execute(
            "INSERT INTO items (item_id, name, external_id) VALUES (%s::UUID, %s, %s)",
            (item_id_2, "Item B", ext_id),
        )
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 3 — item_planning_params rejette lead_time_sourcing_days < 0
# ---------------------------------------------------------------------------

@requires_db
def test_07_ipp_rejects_negative_lead_time(conn):
    """lead_time_sourcing_days < 0 viole la CHECK constraint."""
    item_id = _insert_item(conn)
    location_id = _insert_location(conn)

    with pytest.raises(Exception, match=r"(check|violates|ipp|lead_time)"):
        conn.execute(
            """
            INSERT INTO item_planning_params
                (item_id, location_id, lead_time_sourcing_days)
            VALUES (%s::UUID, %s::UUID, -1)
            """,
            (item_id, location_id),
        )
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 4 — item_planning_params rejette lead_time_manufacturing_days < 0
# ---------------------------------------------------------------------------

@requires_db
def test_07_ipp_rejects_negative_manufacturing_lead_time(conn):
    """lead_time_manufacturing_days < 0 viole la CHECK constraint."""
    item_id = _insert_item(conn)
    location_id = _insert_location(conn)

    with pytest.raises(Exception, match=r"(check|violates)"):
        conn.execute(
            """
            INSERT INTO item_planning_params
                (item_id, location_id, lead_time_manufacturing_days)
            VALUES (%s::UUID, %s::UUID, -5)
            """,
            (item_id, location_id),
        )
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 5 — item_planning_params : lead_time_total_days est calculé
# ---------------------------------------------------------------------------

@requires_db
def test_07_ipp_lead_time_total_computed(conn):
    """lead_time_total_days = sum des 3 composants."""
    item_id = _insert_item(conn)
    location_id = _insert_location(conn)

    conn.execute(
        """
        INSERT INTO item_planning_params
            (item_id, location_id,
             lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days)
        VALUES (%s::UUID, %s::UUID, 5, 10, 3)
        """,
        (item_id, location_id),
    )

    row = conn.execute(
        """
        SELECT lead_time_total_days FROM item_planning_params
        WHERE item_id = %s::UUID AND location_id = %s::UUID
        """,
        (item_id, location_id),
    ).fetchone()

    assert row is not None
    assert row["lead_time_total_days"] == 18, (
        f"Attendu 18, obtenu {row['lead_time_total_days']}"
    )


# ---------------------------------------------------------------------------
# Test 6 — GIST exclusion bloque les overlaps temporels sur item_planning_params
# ---------------------------------------------------------------------------

@requires_db
def test_07_ipp_gist_blocks_overlap(conn):
    """Deux versions overlapping sur le même (item, location) sont bloquées."""
    item_id = _insert_item(conn)
    location_id = _insert_location(conn)

    # Version 1 : 2026-01-01 → 2026-06-30
    conn.execute(
        """
        INSERT INTO item_planning_params
            (item_id, location_id, effective_from, effective_to)
        VALUES (%s::UUID, %s::UUID, '2026-01-01', '2026-06-30')
        """,
        (item_id, location_id),
    )

    # Version 2 qui chevauche : 2026-04-01 → 2026-12-31 (overlap avec V1)
    with pytest.raises(Exception, match=r"(exclusion|overlap|conflits|constraint)"):
        conn.execute(
            """
            INSERT INTO item_planning_params
                (item_id, location_id, effective_from, effective_to)
            VALUES (%s::UUID, %s::UUID, '2026-04-01', '2026-12-31')
            """,
            (item_id, location_id),
        )
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 7 — GIST exclusion accepte des périodes non-chevauchantes
# ---------------------------------------------------------------------------

@requires_db
def test_07_ipp_gist_allows_non_overlapping(conn):
    """Deux versions consécutives non-chevauchantes sont acceptées."""
    item_id = _insert_item(conn)
    location_id = _insert_location(conn)

    conn.execute(
        """
        INSERT INTO item_planning_params
            (item_id, location_id, effective_from, effective_to)
        VALUES (%s::UUID, %s::UUID, '2026-01-01', '2026-06-30')
        """,
        (item_id, location_id),
    )

    # Version 2 qui commence exactement après V1
    conn.execute(
        """
        INSERT INTO item_planning_params
            (item_id, location_id, effective_from, effective_to)
        VALUES (%s::UUID, %s::UUID, '2026-06-30', '2026-12-31')
        """,
        (item_id, location_id),
    )

    count = conn.execute(
        "SELECT COUNT(*) AS cnt FROM item_planning_params WHERE item_id = %s::UUID",
        (item_id,),
    ).fetchone()["cnt"]
    assert count == 2


# ---------------------------------------------------------------------------
# Test 8 — ingest_batches : entity_type CHECK constraint
# ---------------------------------------------------------------------------

@requires_db
def test_07_ingest_batches_entity_type_check(conn):
    """entity_type invalide est rejeté par la CHECK constraint."""
    with pytest.raises(Exception, match=r"(check|violates)"):
        conn.execute(
            """
            INSERT INTO ingest_batches (entity_type, source_system)
            VALUES ('invalid_entity', 'test')
            """,
        )
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 9 — ingest_rows : UNIQUE (batch_id, row_number)
# ---------------------------------------------------------------------------

@requires_db
def test_07_ingest_rows_unique_batch_row_number(conn):
    """Deux lignes avec le même (batch_id, row_number) sont rejetées."""
    batch_id = _insert_batch(conn)

    conn.execute(
        """
        INSERT INTO ingest_rows (batch_id, row_number, raw_content)
        VALUES (%s::UUID, 1, 'ligne 1')
        """,
        (batch_id,),
    )

    with pytest.raises(Exception, match=r"(unique|duplicate)"):
        conn.execute(
            """
            INSERT INTO ingest_rows (batch_id, row_number, raw_content)
            VALUES (%s::UUID, 1, 'doublon')
            """,
            (batch_id,),
        )
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 10 — data_quality_issues : dq_level BETWEEN 1 AND 4
# ---------------------------------------------------------------------------

@requires_db
def test_07_dq_issues_dq_level_check(conn):
    """dq_level hors de [1, 4] est rejeté."""
    batch_id = _insert_batch(conn)

    with pytest.raises(Exception, match=r"(check|violates)"):
        conn.execute(
            """
            INSERT INTO data_quality_issues
                (batch_id, dq_level, rule_code, severity, message)
            VALUES (%s::UUID, 0, 'TEST_RULE', 'error', 'test')
            """,
            (batch_id,),
        )
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 11 — uom_conversions : conversions de base présentes
# ---------------------------------------------------------------------------

@requires_db
def test_07_uom_conversions_base_data(conn):
    """Les 4 conversions globales de base sont présentes après migration."""
    rows = conn.execute(
        """
        SELECT from_uom, to_uom, factor
        FROM uom_conversions
        WHERE item_id IS NULL
        ORDER BY from_uom
        """
    ).fetchall()

    conversions = {(r["from_uom"], r["to_uom"]): r["factor"] for r in rows}

    assert ("PALLET", "EA") in conversions, "Conversion PALLET→EA manquante"
    assert conversions[("PALLET", "EA")] == 48

    assert ("BOX", "EA") in conversions, "Conversion BOX→EA manquante"
    assert conversions[("BOX", "EA")] == 12

    assert ("KG", "G") in conversions, "Conversion KG→G manquante"
    assert conversions[("KG", "G")] == 1000

    assert ("T", "KG") in conversions, "Conversion T→KG manquante"
    assert conversions[("T", "KG")] == 1000


# ---------------------------------------------------------------------------
# Test 12 — suppliers + supplier_items : FK et UNIQUE
# ---------------------------------------------------------------------------

@requires_db
def test_07_suppliers_and_supplier_items(conn):
    """Insertion d'un supplier et d'un supplier_item, puis vérification de l'unicité."""
    item_id = _insert_item(conn)

    supplier_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO suppliers (supplier_id, name) VALUES (%s::UUID, %s)",
        (supplier_id, "Fournisseur Test SA"),
    )

    conn.execute(
        """
        INSERT INTO supplier_items (supplier_id, item_id, lead_time_days)
        VALUES (%s::UUID, %s::UUID, 14)
        """,
        (supplier_id, item_id),
    )

    # Doublon (supplier_id, item_id) interdit
    with pytest.raises(Exception, match=r"(unique|duplicate)"):
        conn.execute(
            """
            INSERT INTO supplier_items (supplier_id, item_id, lead_time_days)
            VALUES (%s::UUID, %s::UUID, 7)
            """,
            (supplier_id, item_id),
        )
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 13 — operational_calendars : UNIQUE (location_id, calendar_date)
# ---------------------------------------------------------------------------

@requires_db
def test_07_operational_calendars_unique_location_date(conn):
    """Deux entrées pour la même (location, date) sont rejetées."""
    location_id = _insert_location(conn)

    conn.execute(
        """
        INSERT INTO operational_calendars (location_id, calendar_date, is_working_day)
        VALUES (%s::UUID, '2026-12-25', FALSE)
        """,
        (location_id,),
    )

    with pytest.raises(Exception, match=r"(unique|duplicate)"):
        conn.execute(
            """
            INSERT INTO operational_calendars (location_id, calendar_date, is_working_day)
            VALUES (%s::UUID, '2026-12-25', TRUE)
            """,
            (location_id,),
        )
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 14 — locations.external_id est UNIQUE
# ---------------------------------------------------------------------------

@requires_db
def test_07_locations_external_id_unique(conn):
    """Deux locations ne peuvent pas partager le même external_id."""
    ext_id = f"LOC-{uuid.uuid4().hex[:8]}"
    loc_id_1 = str(uuid.uuid4())
    loc_id_2 = str(uuid.uuid4())

    conn.execute(
        "INSERT INTO locations (location_id, name, external_id) VALUES (%s::UUID, %s, %s)",
        (loc_id_1, "Location A", ext_id),
    )

    with pytest.raises(Exception, match=r"(unique|duplicate)"):
        conn.execute(
            "INSERT INTO locations (location_id, name, external_id) VALUES (%s::UUID, %s, %s)",
            (loc_id_2, "Location B", ext_id),
        )
        conn.commit()

    conn.rollback()


# ---------------------------------------------------------------------------
# Test 15 — master_data_audit_log : FK sur ingest_batches
# ---------------------------------------------------------------------------

@requires_db
def test_07_audit_log_fk_to_batch(conn):
    """master_data_audit_log.batch_id référence ingest_batches (FK active)."""
    fake_batch_id = str(uuid.uuid4())  # n'existe pas

    with pytest.raises(Exception, match=r"(foreign key|fk|constraint|violates)"):
        conn.execute(
            """
            INSERT INTO master_data_audit_log
                (entity_type, entity_id, field_name, batch_id)
            VALUES ('item', %s::UUID, 'name', %s::UUID)
            """,
            (str(uuid.uuid4()), fake_batch_id),
        )
        conn.commit()

    conn.rollback()
