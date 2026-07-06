"""
tests/integration/test_location_aliases_integration.py — Location aliases
(#414, ADR-031) against a real PostgreSQL database (no mocks).

Covers the three surfaces of PR1:
  1. src/ootils_core/pyramide/repository.py:_warehouse_codes_subquery,
     branched into get_historical_demand (leaf reader) and
     get_demand_freshness — a site now resolves to its own external_id
     UNION its location_aliases codes (migration 070).
  2. POST /v1/ingest/locations extended with the optional ``aliases`` field:
     upsert (DO UPDATE re-map), plus the ingest-layer validations enforcing
     the applicative invariant ONE code -> EXACTLY ONE site, ACROSS ALL
     SOURCE SYSTEMS (demand_history.warehouse_id carries no system tag, so
     the UNION resolution is system-agnostic): alias vs external_id
     anti-collision in BOTH directions (payload alias vs external_ids;
     payload external_id vs DB aliases — Trou A), cross-system DB collision
     (Trou B), chain-level intra-payload collision (alias STRING alone,
     regardless of source_system), blank rejection. The ONLY permitted
     overlap with an existing DB row is the EXACT (alias, source_system)
     pair — the assumed re-map correction (single row re-pointed, never
     two rows).
  3. Schema-level guarantees of migration 070 (FK ON DELETE RESTRICT,
     idempotent re-run of the migration SQL).

Every test creates its OWN item/location entities with fresh, test-prefixed
external_ids so the seeded PUMP-01/DC-ATL data and the other tests are never
perturbed, and cleans up the rows it inserted (style mirrors
test_demand_history_readers_integration.py). Flight lesson: demand_history
is pooled per item and CROSSES locations, so every test uses a dedicated
item — never a shared one — to keep site series isolated.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

SEED_SCRIPT = Path(__file__).parents[2] / "scripts" / "seed_demo_data.py"
MIGRATION_070 = (
    Path(__file__).parents[2]
    / "src" / "ootils_core" / "db" / "migrations"
    / "070_location_aliases.sql"
)
AUTH_TOKEN = "integration-test-token"
AUTH_HEADERS = {"Authorization": f"Bearer {AUTH_TOKEN}"}
TODAY = date.today()

# Unique per-run prefix so repeated runs against the same DB never collide on
# external_id (locations.external_id is UNIQUE).
PREFIX = str(uuid4())[:8]


def uid(base: str) -> str:
    return f"{PREFIX}-{base}"


def _run_seed():
    env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    return subprocess.run(
        [sys.executable, str(SEED_SCRIPT)],
        capture_output=True, text=True, env=env,
    )


# ---------------------------------------------------------------------------
# Fixtures (mirror test_demand_history_readers_integration.py)
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
    os.environ["OOTILS_API_TOKEN"] = AUTH_TOKEN

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
# Direct-DB helpers (autocommit) for setup/teardown
# ---------------------------------------------------------------------------


def _db_conn(dsn):
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _create_item(conn, item_ext: str) -> UUID:
    item_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, name, item_type, uom, status, external_id)
        VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)
        """,
        (item_id, f"{item_ext} test item", item_ext),
    )
    return item_id


def _create_location(conn, loc_ext: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{loc_ext} test DC", loc_ext),
    )
    return loc_id


def _add_alias(conn, location_id: UUID, alias: str, source_system: str = "_default") -> None:
    conn.execute(
        """
        INSERT INTO location_aliases (location_id, alias, source_system)
        VALUES (%s, %s, %s)
        """,
        (location_id, alias, source_system),
    )


def _insert_dh(
    conn,
    item_id: UUID,
    item_code: str,
    warehouse_id: str | None,
    booked_date: date,
    qty: int,
    stream: str = "regular",
    fulfillment: str | None = "standard",
):
    conn.execute(
        """
        INSERT INTO demand_history (
            item_id, item_code, stream, booked_date,
            ordered_quantity, value_ext, counts_for_asp,
            warehouse_id, fulfillment, order_number, ingested_at
        ) VALUES (%s, %s, %s, %s, %s, 0, FALSE, %s, %s, 'TEST-DH', now())
        """,
        (item_id, item_code, stream, booked_date, qty, warehouse_id, fulfillment),
    )


def _cleanup_item(conn, item_id: UUID):
    conn.execute("DELETE FROM demand_history WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))


def _cleanup_location(conn, location_id: UUID):
    """Aliases must go before the location (FK ON DELETE RESTRICT)."""
    conn.execute("DELETE FROM location_aliases WHERE location_id = %s", (location_id,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _cleanup_location_by_ext(conn, external_id: str):
    """Delete a location (and its aliases) created via the ingest endpoint,
    keyed by external_id — the batch bookkeeping rows are left to the
    module teardown (drop-all)."""
    row = conn.execute(
        "SELECT location_id FROM locations WHERE external_id = %s", (external_id,)
    ).fetchone()
    if row is not None:
        _cleanup_location(conn, row["location_id"])


def _reader(conn, item_id, location_id, lookback_days=90):
    from ootils_core.pyramide.repository import get_historical_demand
    return get_historical_demand(
        db=conn,
        item_id=item_id,
        location_id=location_id,
        lookback_days=lookback_days,
    )


def _freshness(conn, item_id=None, warehouse_id=None):
    from ootils_core.pyramide.repository import get_demand_freshness
    return get_demand_freshness(conn, item_id=item_id, warehouse_id=warehouse_id)


# ===========================================================================
# 1. Site with no alias → leaf reader strictly identical to pre-070
# ===========================================================================


class TestLeafReaderNoAlias:
    def test_no_alias_series_unchanged(self, seeded_db):
        """A location with zero aliases resolves to exactly the external_id
        set it did before this table existed (empty-UNION identity)."""
        from decimal import Decimal
        with _db_conn(seeded_db) as conn:
            item_id = _create_item(conn, uid("A-ITEM"))
            loc_id = _create_location(conn, uid("A-LOC"))
            _insert_dh(conn, item_id, uid("A-ITEM"), uid("A-LOC"),
                       TODAY - timedelta(days=3), 7)
            _insert_dh(conn, item_id, uid("A-ITEM"), uid("A-LOC"),
                       TODAY - timedelta(days=2), 11)
            try:
                assert _reader(conn, item_id, loc_id) == [Decimal("7"), Decimal("11")]
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)


# ===========================================================================
# 2. demand_history split across external_id AND an alias → one folded series
# ===========================================================================


class TestLeafReaderAliasFolds:
    def test_external_id_and_alias_fold_into_one_series(self, seeded_db):
        """Rows under the canonical external_id AND under an alias code both
        collapse into the single per-site series (per booked_date sum)."""
        from decimal import Decimal
        with _db_conn(seeded_db) as conn:
            item_id = _create_item(conn, uid("B-ITEM"))
            loc_id = _create_location(conn, uid("B-LOC"))
            _add_alias(conn, loc_id, uid("B-ALIAS"))
            # Same day, one row under the canonical code + one under the alias:
            # they must sum on that booked_date.
            _insert_dh(conn, item_id, uid("B-ITEM"), uid("B-LOC"),
                       TODAY - timedelta(days=4), 10)
            _insert_dh(conn, item_id, uid("B-ITEM"), uid("B-ALIAS"),
                       TODAY - timedelta(days=4), 5)
            # A different day under the alias only.
            _insert_dh(conn, item_id, uid("B-ITEM"), uid("B-ALIAS"),
                       TODAY - timedelta(days=2), 8)
            try:
                # date ASC: day-4 = 10+5 = 15, then day-2 = 8
                assert _reader(conn, item_id, loc_id) == [Decimal("15"), Decimal("8")]
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)


# ===========================================================================
# 3. An alias belonging to ANOTHER site does not leak into the first's series
# ===========================================================================


class TestLeafReaderNoLeakAcrossSites:
    def test_other_sites_alias_does_not_leak(self, seeded_db):
        """Site A's series must not pick up demand booked under a code that is
        an alias of a DIFFERENT site B (scoping is per location_id)."""
        from decimal import Decimal
        with _db_conn(seeded_db) as conn:
            item_id = _create_item(conn, uid("C-ITEM"))
            loc_a = _create_location(conn, uid("C-LOC-A"))
            loc_b = _create_location(conn, uid("C-LOC-B"))
            _add_alias(conn, loc_b, uid("C-ALIAS-B"))
            # Booked under A's own code → counts for A.
            _insert_dh(conn, item_id, uid("C-ITEM"), uid("C-LOC-A"),
                       TODAY - timedelta(days=3), 9)
            # Booked under B's alias → must NOT count for A.
            _insert_dh(conn, item_id, uid("C-ITEM"), uid("C-ALIAS-B"),
                       TODAY - timedelta(days=2), 100)
            try:
                assert _reader(conn, item_id, loc_a) == [Decimal("9")]
                # Sanity: B's series DOES pick up its own alias' demand.
                assert _reader(conn, item_id, loc_b) == [Decimal("100")]
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_a)
                _cleanup_location(conn, loc_b)


# ===========================================================================
# 4. get_demand_freshness alias resolution
# ===========================================================================


class TestFreshnessAliasResolution:
    def test_canonical_code_aggregates_alias_rows(self, seeded_db):
        """(a) Freshness queried by the canonical external_id aggregates
        ingested_at of rows booked under the ALIAS code — freshness is a
        property of the pipeline feeding the whole site."""
        with _db_conn(seeded_db) as conn:
            item_id = _create_item(conn, uid("D-ITEM"))
            loc_id = _create_location(conn, uid("D-LOC"))
            _add_alias(conn, loc_id, uid("D-ALIAS"))
            # The ONLY row for this item is booked under the alias code.
            _insert_dh(conn, item_id, uid("D-ITEM"), uid("D-ALIAS"),
                       TODAY - timedelta(days=5), 12)
            try:
                fresh = _freshness(conn, item_id=item_id, warehouse_id=uid("D-LOC"))
                # If the alias were not folded in, max_ingested_at would be None.
                assert fresh.max_ingested_at is not None
                assert fresh.ingest_age_days is not None
                assert fresh.last_booked_date == TODAY - timedelta(days=5)
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)

    def test_unmatched_code_keeps_literal_filter(self, seeded_db):
        """(b) A warehouse_id that matches NO locations row keeps the exact
        literal-equality filter of old — the owner-is-None branch."""
        with _db_conn(seeded_db) as conn:
            item_id = _create_item(conn, uid("D2-ITEM"))
            # No location with this external_id at all: the code is a pure
            # literal warehouse string that never resolves to a site.
            literal_wh = uid("D2-ORPHAN-WH")
            _insert_dh(conn, item_id, uid("D2-ITEM"), literal_wh,
                       TODAY - timedelta(days=6), 4)
            # A different warehouse string for the same item — must NOT be
            # aggregated when we filter on literal_wh.
            _insert_dh(conn, item_id, uid("D2-ITEM"), uid("D2-OTHER-WH"),
                       TODAY - timedelta(days=1), 400)
            try:
                fresh = _freshness(conn, item_id=item_id, warehouse_id=literal_wh)
                assert fresh.last_booked_date == TODAY - timedelta(days=6)
                # The day-1 row under the other code is excluded by the literal
                # filter, so the latest booked_date stays day-6.
            finally:
                _cleanup_item(conn, item_id)

    def test_no_alias_site_freshness_unchanged(self, seeded_db):
        """(c) A site with no aliases yields the same freshness it would have
        before 070 (single-code resolution, still via the owning location)."""
        with _db_conn(seeded_db) as conn:
            item_id = _create_item(conn, uid("D3-ITEM"))
            loc_id = _create_location(conn, uid("D3-LOC"))
            _insert_dh(conn, item_id, uid("D3-ITEM"), uid("D3-LOC"),
                       TODAY - timedelta(days=2), 3)
            try:
                fresh = _freshness(conn, item_id=item_id, warehouse_id=uid("D3-LOC"))
                assert fresh.last_booked_date == TODAY - timedelta(days=2)
                assert fresh.max_ingested_at is not None
            finally:
                _cleanup_item(conn, item_id)
                _cleanup_location(conn, loc_id)


# ===========================================================================
# 5. Ingest happy path: aliases persisted, response count correct
# ===========================================================================


class TestIngestAliasHappyPath:
    def test_post_locations_with_aliases_persists(self, api_client, auth, seeded_db):
        ext = uid("E-DC")
        resp = api_client.post(
            "/v1/ingest/locations",
            json={
                "locations": [
                    {
                        "external_id": ext,
                        "name": "Aliased DC",
                        "location_type": "dc",
                        "aliases": [
                            {"alias": uid("E-87"), "source_system": "erp"},
                            {"alias": uid("E-286"), "source_system": "erp"},
                        ],
                    }
                ]
            },
            headers=auth,
        )
        try:
            assert resp.status_code == 200, resp.text
            data = resp.json()
            assert data["status"] == "ok"
            assert data["summary"]["inserted"] == 1
            assert data["aliases_upserted"] == 2
            assert data["results"][0]["aliases_upserted"] == 2

            with _db_conn(seeded_db) as conn:
                loc_row = conn.execute(
                    "SELECT location_id FROM locations WHERE external_id = %s", (ext,)
                ).fetchone()
                assert loc_row is not None
                rows = conn.execute(
                    """
                    SELECT alias, source_system, location_id
                    FROM location_aliases
                    WHERE location_id = %s
                    ORDER BY alias
                    """,
                    (loc_row["location_id"],),
                ).fetchall()
                aliases = {(r["alias"], r["source_system"]) for r in rows}
                assert aliases == {
                    (uid("E-87"), "erp"),
                    (uid("E-286"), "erp"),
                }
                assert all(r["location_id"] == loc_row["location_id"] for r in rows)
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext)


# ===========================================================================
# 6. DO UPDATE re-map: same (alias, source_system) moves site A → site B
# ===========================================================================


class TestIngestAliasReMap:
    def test_alias_remaps_to_new_site_no_duplicate(self, api_client, auth, seeded_db):
        """Two successive POSTs mapping the same (alias, source_system) first
        to site A, then to site B → the single row now points to B (DO UPDATE
        SET location_id), no duplicate."""
        ext_a = uid("F-DC-A")
        ext_b = uid("F-DC-B")
        alias = uid("F-SHARED")
        try:
            r1 = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_a, "name": "Site A",
                     "aliases": [{"alias": alias, "source_system": "erp"}]}
                ]},
                headers=auth,
            )
            assert r1.status_code == 200, r1.text

            r2 = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_b, "name": "Site B",
                     "aliases": [{"alias": alias, "source_system": "erp"}]}
                ]},
                headers=auth,
            )
            assert r2.status_code == 200, r2.text

            with _db_conn(seeded_db) as conn:
                loc_b = conn.execute(
                    "SELECT location_id FROM locations WHERE external_id = %s", (ext_b,)
                ).fetchone()["location_id"]
                rows = conn.execute(
                    "SELECT location_id FROM location_aliases "
                    "WHERE alias = %s AND source_system = %s",
                    (alias, "erp"),
                ).fetchall()
                assert len(rows) == 1, "re-map must not create a duplicate row"
                assert rows[0]["location_id"] == loc_b, "alias must now point to B"
        finally:
            with _db_conn(seeded_db) as conn:
                # Alias row currently belongs to B; delete it before either site.
                conn.execute(
                    "DELETE FROM location_aliases WHERE alias = %s AND source_system = %s",
                    (alias, "erp"),
                )
                _cleanup_location_by_ext(conn, ext_a)
                _cleanup_location_by_ext(conn, ext_b)


# ===========================================================================
# 7. Cross-site external_id collision → 422, nothing persisted
# ===========================================================================


class TestIngestCrossSiteCollision:
    def test_alias_equals_existing_other_site_external_id_422(
        self, api_client, auth, seeded_db
    ):
        """(a) alias == the external_id of another site ALREADY in the DB →
        422, and the batch's location is NOT persisted."""
        existing_ext = uid("G-EXISTING")
        new_ext = uid("G-NEW")
        # Pre-create the site whose external_id will be collided with.
        with _db_conn(seeded_db) as conn:
            _create_location(conn, existing_ext)
        try:
            resp = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": new_ext, "name": "Colliding",
                     "aliases": [{"alias": existing_ext, "source_system": "erp"}]}
                ]},
                headers=auth,
            )
            assert resp.status_code == 422, resp.text
            detail = resp.json()["detail"]
            assert isinstance(detail, list)
            assert existing_ext in str(detail)
            # Nothing persisted for the new site.
            with _db_conn(seeded_db) as conn:
                assert conn.execute(
                    "SELECT 1 FROM locations WHERE external_id = %s", (new_ext,)
                ).fetchone() is None
                assert conn.execute(
                    "SELECT 1 FROM location_aliases WHERE alias = %s", (existing_ext,)
                ).fetchone() is None
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, existing_ext)
                _cleanup_location_by_ext(conn, new_ext)

    def test_alias_equals_another_payload_site_external_id_422(
        self, api_client, auth, seeded_db
    ):
        """(b) alias == the external_id of a DIFFERENT site in the SAME payload
        → 422, nothing persisted."""
        ext_x = uid("G2-X")
        ext_y = uid("G2-Y")
        try:
            resp = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_x, "name": "Site X"},
                    {"external_id": ext_y, "name": "Site Y",
                     "aliases": [{"alias": ext_x, "source_system": "erp"}]},
                ]},
                headers=auth,
            )
            assert resp.status_code == 422, resp.text
            assert ext_x in str(resp.json()["detail"])
            # All-or-nothing: neither X nor Y persisted.
            with _db_conn(seeded_db) as conn:
                assert conn.execute(
                    "SELECT 1 FROM locations WHERE external_id = ANY(%s)",
                    ([ext_x, ext_y],),
                ).fetchone() is None
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext_x)
                _cleanup_location_by_ext(conn, ext_y)


# ===========================================================================
# 8. Intra-payload (alias, source_system) cross-site duplicate → 422
# ===========================================================================


class TestIngestIntraPayloadDuplicate:
    def test_same_pair_two_sites_422_both_ext_ids_in_message(
        self, api_client, auth, seeded_db
    ):
        """Two DIFFERENT sites declaring the identical (alias, source_system)
        in one payload → 422; the message names BOTH external_ids."""
        ext_p = uid("H-P")
        ext_q = uid("H-Q")
        shared = uid("H-SHARED")
        try:
            resp = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_p, "name": "Site P",
                     "aliases": [{"alias": shared, "source_system": "erp"}]},
                    {"external_id": ext_q, "name": "Site Q",
                     "aliases": [{"alias": shared, "source_system": "erp"}]},
                ]},
                headers=auth,
            )
            assert resp.status_code == 422, resp.text
            msg = str(resp.json()["detail"])
            assert ext_p in msg and ext_q in msg
            with _db_conn(seeded_db) as conn:
                assert conn.execute(
                    "SELECT 1 FROM locations WHERE external_id = ANY(%s)",
                    ([ext_p, ext_q],),
                ).fetchone() is None
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext_p)
                _cleanup_location_by_ext(conn, ext_q)

    def test_same_site_repeats_pair_is_noop_200(self, api_client, auth, seeded_db):
        """The SAME site repeating its own (alias, source_system) resolves to
        itself — not ambiguous, so 200 (silent no-op on the duplicate)."""
        ext = uid("H2-DC")
        alias = uid("H2-ALIAS")
        try:
            resp = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext, "name": "Site",
                     "aliases": [
                         {"alias": alias, "source_system": "erp"},
                         {"alias": alias, "source_system": "erp"},
                     ]},
                ]},
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            with _db_conn(seeded_db) as conn:
                rows = conn.execute(
                    "SELECT 1 FROM location_aliases WHERE alias = %s AND source_system = %s",
                    (alias, "erp"),
                ).fetchall()
                # UNIQUE (alias, source_system) → exactly one row survives.
                assert len(rows) == 1
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext)


# ===========================================================================
# 9. Blank / whitespace alias handling
# ===========================================================================


class TestIngestBlankAlias:
    def test_whitespace_only_alias_pydantic_422(self, api_client, auth, seeded_db):
        """alias '  ' → 422 at the Pydantic boundary (LocationAliasRow
        strips, then rejects blank); nothing persisted."""
        ext = uid("I-DC")
        try:
            resp = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext, "name": "Site",
                     "aliases": [{"alias": "  ", "source_system": "erp"}]},
                ]},
                headers=auth,
            )
            assert resp.status_code == 422, resp.text
            with _db_conn(seeded_db) as conn:
                assert conn.execute(
                    "SELECT 1 FROM locations WHERE external_id = %s", (ext,)
                ).fetchone() is None
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext)

    def test_padded_alias_is_stored_stripped(self, api_client, auth, seeded_db):
        """alias ' 87 ' → stored as '87' (Pydantic strip mirrors the DB CHECK
        btrim(alias) = alias)."""
        ext = uid("I2-DC")
        core = uid("I2-87")
        padded = f"  {core}  "
        try:
            resp = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext, "name": "Site",
                     "aliases": [{"alias": padded, "source_system": "erp"}]},
                ]},
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            with _db_conn(seeded_db) as conn:
                row = conn.execute(
                    "SELECT alias FROM location_aliases WHERE source_system = %s "
                    "AND location_id = (SELECT location_id FROM locations WHERE external_id = %s)",
                    ("erp", ext),
                ).fetchone()
                assert row is not None
                assert row["alias"] == core  # stripped, no leading/trailing spaces
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext)


# ===========================================================================
# 10. source_system default + per-system distinctness
# ===========================================================================


class TestSourceSystemScope:
    def test_default_source_system_and_two_systems_distinct(
        self, api_client, auth, seeded_db
    ):
        """The SAME alias string under two different source_systems yields two
        distinct rows (UNIQUE is per system); an alias sent without a
        source_system lands under the '_default' sentinel."""
        ext = uid("J-DC")
        same_alias = uid("J-87")
        default_alias = uid("J-DEFAULT")
        try:
            resp = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext, "name": "Site",
                     "aliases": [
                         {"alias": same_alias, "source_system": "erp_a"},
                         {"alias": same_alias, "source_system": "erp_b"},
                         # No source_system → defaults to '_default'.
                         {"alias": default_alias},
                     ]},
                ]},
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            assert resp.json()["aliases_upserted"] == 3
            with _db_conn(seeded_db) as conn:
                loc_id = conn.execute(
                    "SELECT location_id FROM locations WHERE external_id = %s", (ext,)
                ).fetchone()["location_id"]
                # Same alias, two systems → two rows.
                same_rows = conn.execute(
                    "SELECT source_system FROM location_aliases "
                    "WHERE location_id = %s AND alias = %s ORDER BY source_system",
                    (loc_id, same_alias),
                ).fetchall()
                assert [r["source_system"] for r in same_rows] == ["erp_a", "erp_b"]
                # Default-sentinel row.
                default_row = conn.execute(
                    "SELECT source_system FROM location_aliases "
                    "WHERE location_id = %s AND alias = %s",
                    (loc_id, default_alias),
                ).fetchone()
                assert default_row["source_system"] == "_default"
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext)


# ===========================================================================
# 11. FK ON DELETE RESTRICT — deleting a site that owns aliases fails
# ===========================================================================


class TestForeignKeyRestrict:
    def test_all_location_aliases_fks_are_restrict(self, conn):
        """Schema guarantee: every FK from location_aliases to locations
        declares ON DELETE RESTRICT (confdeltype = 'r')."""
        rows = conn.execute(
            """
            SELECT c.conname, c.confdeltype
            FROM pg_constraint c
            WHERE c.contype = 'f'
              AND c.conrelid = 'location_aliases'::regclass
              AND c.confrelid = 'locations'::regclass
            """
        ).fetchall()
        assert rows, "no FK from location_aliases to locations found"
        offenders = [r for r in rows if r["confdeltype"] != "r"]
        assert offenders == [], f"FKs without ON DELETE RESTRICT: {offenders}"

    def test_delete_location_with_alias_raises_fk_violation(self, conn):
        """DELETE on a location that still owns an alias fails with
        ForeignKeyViolation (mirrors test_scenario_fk_retention)."""
        import psycopg

        loc_id = uuid4()
        conn.execute(
            "INSERT INTO locations (location_id, name, external_id) VALUES (%s, %s, %s)",
            (loc_id, "fk-restrict-loc", uid("K-LOC")),
        )
        conn.execute(
            "INSERT INTO location_aliases (location_id, alias, source_system) "
            "VALUES (%s, %s, %s)",
            (loc_id, uid("K-ALIAS"), "erp"),
        )
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            conn.execute("DELETE FROM locations WHERE location_id = %s", (loc_id,))
        # Fixture rolls back; no manual cleanup needed.


# ===========================================================================
# 12. Migration 070 is idempotent — re-running its SQL is a clean no-op
# ===========================================================================


class TestMigrationIdempotent:
    def test_reexecuting_070_sql_is_noop(self, conn):
        """Re-running migration 070's SQL against an already-migrated DB must
        succeed with no error (defensive-idempotence contract: CREATE TABLE /
        INDEX IF NOT EXISTS). Mirrors test_08_bootstrap_rerun_is_idempotent,
        scoped to the single migration file."""
        sql_text = MIGRATION_070.read_text(encoding="utf-8")
        # The file wraps its own BEGIN/COMMIT; run it verbatim on a fresh
        # autocommit connection so the transaction control inside the file is
        # honoured (the fixture `conn` is used only for the post-check).
        # Reconnect via TEST_DB_URL (full credentials) rather than
        # conn.info.dsn — psycopg3 scrubs the password from ConnectionInfo.dsn.
        import psycopg
        with psycopg.connect(TEST_DB_URL, autocommit=True) as raw:
            raw.execute(sql_text)  # must not raise on the second application

        # Table + index still present and well-formed afterwards.
        assert conn.execute(
            "SELECT to_regclass('public.location_aliases') AS t"
        ).fetchone()["t"] is not None
        idx = conn.execute(
            "SELECT indexname FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = 'location_aliases'"
        ).fetchall()
        names = {r["indexname"] for r in idx}
        assert "idx_location_aliases_location" in names


# ===========================================================================
# 13. Trou A — a payload external_id colliding with a DB alias of another
#     site → 422 (the reverse direction of case 7: code arrives as an
#     external_id, the prior claim is an alias row)
# ===========================================================================


class TestIngestExternalIdCollidesWithDbAlias:
    def test_new_site_external_id_equals_existing_alias_422(
        self, api_client, auth, seeded_db
    ):
        """DB holds alias code→site A (posted by a first ingest); a second
        POST creating a NEW site whose external_id IS that code → 422 and
        nothing persisted. Without this guard the code would resolve to two
        sites (A via location_aliases, the new site via
        locations.external_id) — the exact ambiguity the system-agnostic
        invariant forbids."""
        ext_a = uid("M-DC-A")
        code = uid("M-87")
        try:
            r1 = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_a, "name": "Site A",
                     "aliases": [{"alias": code, "source_system": "erp"}]}
                ]},
                headers=auth,
            )
            assert r1.status_code == 200, r1.text
            loc_a_id = r1.json()["results"][0]["location_id"]

            r2 = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": code, "name": "Colliding new site"}
                ]},
                headers=auth,
            )
            assert r2.status_code == 422, r2.text
            detail = r2.json()["detail"]
            assert isinstance(detail, list)
            msg = str(detail)
            # In substance, not word-for-word: the colliding code (which is
            # also the offending site's external_id, present via the error
            # wrapper) + the owning site, which the message names by its
            # location_id (the router reports the DB-side owner as a UUID).
            assert code in msg
            assert loc_a_id in msg

            with _db_conn(seeded_db) as conn:
                # The new site was NOT persisted.
                assert conn.execute(
                    "SELECT 1 FROM locations WHERE external_id = %s", (code,)
                ).fetchone() is None
                # The pre-existing alias row is untouched and still points to A.
                row = conn.execute(
                    "SELECT location_id FROM location_aliases "
                    "WHERE alias = %s AND source_system = %s",
                    (code, "erp"),
                ).fetchone()
                assert row is not None
                assert str(row["location_id"]) == loc_a_id
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext_a)
                # Defensive: if the 422 regressed and the site got written.
                _cleanup_location_by_ext(conn, code)


# ===========================================================================
# 14. Trou B — same alias string in DB under ANOTHER source_system:
#     different site → 422; SAME site → 200 (legitimate multi-system)
# ===========================================================================


class TestIngestCrossSystemDbCollision:
    def test_same_code_other_system_other_site_422(self, api_client, auth, seeded_db):
        """DB has (code, 'erpA')→site A; POST (code, 'erpB')→site B → 422.
        The resolution UNION is system-agnostic, so the code pointing at two
        sites under two systems is a real ambiguity — only the EXACT pair is
        the permitted re-map (case 16)."""
        ext_a = uid("N-DC-A")
        ext_b = uid("N-DC-B")
        code = uid("N-87")
        try:
            r1 = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_a, "name": "Site A",
                     "aliases": [{"alias": code, "source_system": "erpA"}]}
                ]},
                headers=auth,
            )
            assert r1.status_code == 200, r1.text
            loc_a_id = r1.json()["results"][0]["location_id"]

            r2 = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_b, "name": "Site B",
                     "aliases": [{"alias": code, "source_system": "erpB"}]}
                ]},
                headers=auth,
            )
            assert r2.status_code == 422, r2.text
            msg = str(r2.json()["detail"])
            # Substance: the code, the offending site (error wrapper), the
            # owning site (named by location_id), and both systems.
            assert code in msg
            assert ext_b in msg
            assert loc_a_id in msg
            assert "erpA" in msg and "erpB" in msg

            with _db_conn(seeded_db) as conn:
                # B not persisted; the alias table still has ONLY the erpA
                # row, still pointing to A.
                assert conn.execute(
                    "SELECT 1 FROM locations WHERE external_id = %s", (ext_b,)
                ).fetchone() is None
                rows = conn.execute(
                    "SELECT source_system, location_id FROM location_aliases "
                    "WHERE alias = %s",
                    (code,),
                ).fetchall()
                assert [(r["source_system"], str(r["location_id"])) for r in rows] == [
                    ("erpA", loc_a_id)
                ]
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext_a)
                _cleanup_location_by_ext(conn, ext_b)

    def test_same_code_other_system_same_site_200(self, api_client, auth, seeded_db):
        """Counter-case: POST (code, 'erpB')→site A while DB has
        (code, 'erpA')→site A → 200. Same site, two systems = the legitimate
        multi-flux case (no ambiguity: both rows resolve to A) — two rows in
        DB afterwards."""
        ext_a = uid("N2-DC-A")
        code = uid("N2-87")
        try:
            r1 = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_a, "name": "Site A",
                     "aliases": [{"alias": code, "source_system": "erpA"}]}
                ]},
                headers=auth,
            )
            assert r1.status_code == 200, r1.text
            loc_a_id = r1.json()["results"][0]["location_id"]

            r2 = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_a, "name": "Site A",
                     "aliases": [{"alias": code, "source_system": "erpB"}]}
                ]},
                headers=auth,
            )
            assert r2.status_code == 200, r2.text
            assert r2.json()["summary"]["updated"] == 1

            with _db_conn(seeded_db) as conn:
                rows = conn.execute(
                    "SELECT source_system, location_id FROM location_aliases "
                    "WHERE alias = %s ORDER BY source_system",
                    (code,),
                ).fetchall()
                assert [r["source_system"] for r in rows] == ["erpA", "erpB"]
                assert all(str(r["location_id"]) == loc_a_id for r in rows)
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext_a)


# ===========================================================================
# 15. Intra-payload CROSS-SYSTEM collision — the chain (alias string) is
#     owned by one site per batch, regardless of source_system
# ===========================================================================


class TestIngestIntraPayloadCrossSystem:
    def test_two_sites_same_code_different_systems_422(
        self, api_client, auth, seeded_db
    ):
        """One POST where site P declares (code, 'erpA') and site Q declares
        (code, 'erpB') → 422 naming BOTH external_ids; nothing persisted.
        The per-system UNIQUE key would happily store both rows (different
        keys) — the chain-level ingest guard is what prevents the silent
        two-resolutions corruption."""
        ext_p = uid("O-P")
        ext_q = uid("O-Q")
        code = uid("O-87")
        try:
            resp = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_p, "name": "Site P",
                     "aliases": [{"alias": code, "source_system": "erpA"}]},
                    {"external_id": ext_q, "name": "Site Q",
                     "aliases": [{"alias": code, "source_system": "erpB"}]},
                ]},
                headers=auth,
            )
            assert resp.status_code == 422, resp.text
            msg = str(resp.json()["detail"])
            assert code in msg
            assert ext_p in msg and ext_q in msg

            with _db_conn(seeded_db) as conn:
                assert conn.execute(
                    "SELECT 1 FROM locations WHERE external_id = ANY(%s)",
                    ([ext_p, ext_q],),
                ).fetchone() is None
                assert conn.execute(
                    "SELECT 1 FROM location_aliases WHERE alias = %s", (code,)
                ).fetchone() is None
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext_p)
                _cleanup_location_by_ext(conn, ext_q)

    def test_one_site_same_code_two_systems_200(self, api_client, auth, seeded_db):
        """Counter-case: ONE site declaring (code, 'erpA') AND (code, 'erpB')
        in the same POST → 200 (the chain resolves to that site alone), two
        rows in DB. Complements case 10, locked here explicitly as the
        counter-case of the cross-system rejection above."""
        ext_a = uid("O2-DC")
        code = uid("O2-87")
        try:
            resp = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_a, "name": "Site A",
                     "aliases": [
                         {"alias": code, "source_system": "erpA"},
                         {"alias": code, "source_system": "erpB"},
                     ]},
                ]},
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            assert resp.json()["aliases_upserted"] == 2
            with _db_conn(seeded_db) as conn:
                loc_id = conn.execute(
                    "SELECT location_id FROM locations WHERE external_id = %s",
                    (ext_a,),
                ).fetchone()["location_id"]
                rows = conn.execute(
                    "SELECT source_system, location_id FROM location_aliases "
                    "WHERE alias = %s ORDER BY source_system",
                    (code,),
                ).fetchall()
                assert [r["source_system"] for r in rows] == ["erpA", "erpB"]
                assert all(r["location_id"] == loc_id for r in rows)
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_location_by_ext(conn, ext_a)


# ===========================================================================
# 16. Exact-pair re-map remains permitted — the assumed correction, never
#     an ambiguity ('_default' variant; the 'erp' variant is case 6)
# ===========================================================================


class TestIngestExactPairReMapDefault:
    def test_default_pair_remap_points_to_new_site_single_row(
        self, api_client, auth, seeded_db
    ):
        """DB has (code, '_default')→site A; POST (code, '_default')→site B
        (source_system omitted → the '_default' sentinel) → 200 and the
        SINGLE row now points to B. The exact pair hits the ON CONFLICT
        DO UPDATE — there are never two rows, so no ambiguity ever exists:
        this is the one overlap the strengthened validation deliberately
        lets through."""
        ext_a = uid("P-DC-A")
        ext_b = uid("P-DC-B")
        code = uid("P-87")
        try:
            r1 = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_a, "name": "Site A",
                     "aliases": [{"alias": code}]}  # → '_default'
                ]},
                headers=auth,
            )
            assert r1.status_code == 200, r1.text

            r2 = api_client.post(
                "/v1/ingest/locations",
                json={"locations": [
                    {"external_id": ext_b, "name": "Site B",
                     "aliases": [{"alias": code}]}  # same '_default' pair
                ]},
                headers=auth,
            )
            assert r2.status_code == 200, r2.text

            with _db_conn(seeded_db) as conn:
                loc_b = conn.execute(
                    "SELECT location_id FROM locations WHERE external_id = %s",
                    (ext_b,),
                ).fetchone()["location_id"]
                rows = conn.execute(
                    "SELECT location_id FROM location_aliases "
                    "WHERE alias = %s AND source_system = '_default'",
                    (code,),
                ).fetchall()
                assert len(rows) == 1, "exact-pair re-map must never duplicate"
                assert rows[0]["location_id"] == loc_b, "row must now point to B"
        finally:
            with _db_conn(seeded_db) as conn:
                conn.execute(
                    "DELETE FROM location_aliases "
                    "WHERE alias = %s AND source_system = '_default'",
                    (code,),
                )
                _cleanup_location_by_ext(conn, ext_a)
                _cleanup_location_by_ext(conn, ext_b)
