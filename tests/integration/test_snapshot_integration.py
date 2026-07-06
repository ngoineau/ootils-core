"""
tests/integration/test_snapshot_integration.py — DB-backed tests for the
inventory-snapshot backbone (chantier #393 A3-PR1, ADR-030) against a real
Postgres. Migration 067 (inventory_snapshots) is applied by the ``migrated_db``
fixture exactly as production applies it (OotilsDB startup). No mocks — CLAUDE.md.

Two surfaces are under test:

  * The engine (``engine/snapshot/capture.py``): ``capture_snapshot`` (SELECT-only
    per-(item, location) on-hand scan) + ``persist_snapshot`` (the single
    idempotent upsert). These use the function-scoped ``conn`` fixture directly.
  * The HTTP surface (``api/routers/snapshots.py``): POST captures (scope
    ``ingest`` + kill switch), GET queries (scope ``read``). These use a
    TestClient with ``get_db`` overridden onto the test DB and minted tokens
    (the #392 pattern of test_agent_floor_integration.py).

Locked contracts:
  1. Per-SITE, never pooled (the DRP lesson / ADR-028): two OnHand nodes on the
     SAME (item, location) SUM to one row; the SAME item across TWO locations
     yields TWO distinct rows; a NULL item/location coordinate is excluded.
  2. Idempotent upsert on the UNIQUE (scenario, item, location, as_of): a
     re-capture of the same day overwrites (one row/coord), never duplicates,
     and re-stamps captured_at; an empty batch writes nothing.
  3. NULL-honest shortage pair: first_shortage_date and shortage_severity_usd
     are BOTH NULL on every PR1 row (never 0).
  4. POST /v1/snapshots: 201 with {scenario_id, as_of_date, snapshots_captured};
     401 unauthenticated; 403 with a read-only token (write needs ingest);
     503 when OOTILS_SNAPSHOTS_ENABLED is falsy (no row written); source='api'.
  5. GET /v1/snapshots: requires read; filters by scenario/as_of/item_id;
     deterministic ORDER BY; parameterized SQL (an item_id filter is a bound
     value, never string-interpolated).
  6. Forkable: a capture on baseline and on a fork land on distinct,
     scenario-scoped rows.
  7. CLI (scripts/snapshot_inventory.py): main(["--dsn", ..., "--allow-dev"])
     exits 0 and persists rows with source='cli'.

Every test seeds its own uuid4-suffixed master data and cleans the coordinates
it created (or relies on the module teardown that DROPs every public table).
Dates are anchored on the DB-side CURRENT_DATE. No wall-clock timing assertions.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from ootils_core.engine.snapshot import capture_snapshot, persist_snapshot

from .conftest import requires_db

# Import seam for the CLI (scripts/ is outside the package; it does a bare
# "import mrp_core", so scripts/ must be on sys.path).
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

pytestmark = requires_db

# Seeded by migration 002 (is_baseline=TRUE) — the only baseline scenario.
BASELINE = UUID("00000000-0000-0000-0000-000000000001")
LEGACY_TOKEN = "integration-test-token"


# ---------------------------------------------------------------------------
# Seed helpers (calqued on test_drp_loader_integration.py). The snapshot stores
# RAW UUID coordinates, so external_id is not strictly needed — a uuid4 suffix is
# carried only to keep each seed's master data collision-free.
# ---------------------------------------------------------------------------


def _seed_item(conn, name: str = "snap-item") -> UUID:
    return conn.execute(
        "INSERT INTO items (item_id, external_id, name) VALUES (%s, %s, %s) RETURNING item_id",
        (uuid4(), f"{name}-{uuid4()}", name),
    ).fetchone()["item_id"]


def _seed_location(conn, name: str = "snap-loc") -> UUID:
    return conn.execute(
        "INSERT INTO locations (location_id, external_id, name) "
        "VALUES (%s, %s, %s) RETURNING location_id",
        (uuid4(), f"{name}-{uuid4()}", name),
    ).fetchone()["location_id"]


def _seed_scenario(conn, name: str = "snap-fork") -> UUID:
    """A non-baseline scenario (a fork). The capture scan is scenario-scoped by
    node.scenario_id; a plain fork row (no ScenarioManager deep-copy) is all we
    need to prove scoping."""
    return conn.execute(
        "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
        "VALUES (%s, %s, FALSE, 'active') RETURNING scenario_id",
        (uuid4(), f"{name}-{uuid4()}"),
    ).fetchone()["scenario_id"]


def _seed_on_hand(conn, *, scenario_id, item_id, location_id, qty, active=True) -> UUID:
    """An OnHandSupply node explicitly scoped to ``scenario_id`` (the capture
    scan is strictly scenario-scoped and filters node_type='OnHandSupply' AND
    active). item_id / location_id may be None to exercise the NULL exclusion."""
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            quantity, time_grain, time_ref, active
        ) VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, 'exact_date', CURRENT_DATE, %s)
        """,
        (node_id, scenario_id, item_id, location_id, qty, active),
    )
    return node_id


def _snapshots_for(conn, scenario_id, item_id) -> list[dict]:
    return conn.execute(
        "SELECT * FROM inventory_snapshots "
        "WHERE scenario_id = %s AND item_id = %s "
        "ORDER BY location_id, as_of_date",
        (scenario_id, item_id),
    ).fetchall()


# ===========================================================================
# 1. Capture per-site, NOT pooled — THE central case
# ===========================================================================


def test_capture_sums_two_nodes_on_same_coordinate(conn):
    """Two OnHandSupply nodes on the SAME (item, location) collapse to ONE
    SnapshotRow whose on_hand_qty is the Decimal SUM — the per-coordinate
    aggregation."""
    item_id = _seed_item(conn)
    loc = _seed_location(conn)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=loc, qty=10)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=loc, qty=32)
    conn.commit()

    rows = capture_snapshot(conn, str(BASELINE), source="cli")
    mine = [r for r in rows if r.item_id == item_id and r.location_id == loc]

    assert len(mine) == 1, "two nodes on one coordinate must collapse to one row"
    assert mine[0].on_hand_qty == 42
    assert mine[0].scenario_id == BASELINE


def test_capture_same_item_two_locations_stays_distinct_not_pooled(conn):
    """THE anti-pooling proof: ONE item at TWO locations yields TWO distinct
    rows (one per location), never a single item-pooled row. This is the whole
    reason the snapshot exists per-site (ADR-028)."""
    item_id = _seed_item(conn)
    east = _seed_location(conn, "snap-east")
    west = _seed_location(conn, "snap-west")
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=east, qty=8)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=west, qty=20)
    conn.commit()

    rows = capture_snapshot(conn, str(BASELINE), source="cli")
    mine = {r.location_id: r for r in rows if r.item_id == item_id}

    assert set(mine) == {east, west}, "one item / two locations -> two rows"
    assert mine[east].on_hand_qty == 8
    assert mine[west].on_hand_qty == 20
    # Never summed together onto a single item-level coordinate.
    assert mine[east].on_hand_qty + mine[west].on_hand_qty == 28


def test_capture_excludes_null_item_or_location(conn):
    """A node with a NULL item_id OR a NULL location_id is excluded (both
    coordinates are required; the migration-067 FKs are NOT NULL). Only the
    fully-coordinated node survives the scan."""
    item_id = _seed_item(conn)
    loc = _seed_location(conn)
    # Fully-coordinated -> kept.
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=loc, qty=5)
    # NULL location -> excluded.
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=None, qty=99)
    # NULL item -> excluded.
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=None, location_id=loc, qty=77)
    conn.commit()

    rows = capture_snapshot(conn, str(BASELINE), source="cli")
    mine = [r for r in rows if r.item_id == item_id and r.location_id == loc]

    assert len(mine) == 1
    assert mine[0].on_hand_qty == 5
    # The un-located node's quantity is NOT folded into the kept row.
    assert all(r.on_hand_qty != 99 for r in rows)


def test_capture_excludes_inactive_nodes(conn):
    """The scan filters ``active`` — an inactive OnHandSupply never contributes
    (belt on the WHERE clause)."""
    item_id = _seed_item(conn)
    loc = _seed_location(conn)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=loc, qty=15)
    _seed_on_hand(
        conn, scenario_id=BASELINE, item_id=item_id, location_id=loc, qty=500, active=False
    )
    conn.commit()

    rows = capture_snapshot(conn, str(BASELINE), source="cli")
    mine = [r for r in rows if r.item_id == item_id and r.location_id == loc]

    assert len(mine) == 1
    assert mine[0].on_hand_qty == 15, "inactive node's 500 must not be summed in"


def test_capture_rows_sorted_by_coordinate(conn):
    """Output is deterministically sorted by (item_id, location_id) so a golden
    caller sees a stable order regardless of physical scan order."""
    item_id = _seed_item(conn)
    loc_a = _seed_location(conn, "snap-a")
    loc_b = _seed_location(conn, "snap-b")
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=loc_a, qty=1)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=loc_b, qty=2)
    conn.commit()

    rows = capture_snapshot(conn, str(BASELINE), source="cli")
    keyed = [(str(r.item_id), str(r.location_id)) for r in rows]
    assert keyed == sorted(keyed), "rows must be sorted by (item_id, location_id)"


# ===========================================================================
# 2. Idempotent upsert
# ===========================================================================


def test_persist_is_idempotent_on_same_day(conn):
    """capture+persist twice for the same scenario/day writes ONE row per
    coordinate, not two — the ON CONFLICT DO UPDATE on the UNIQUE key."""
    item_id = _seed_item(conn)
    loc = _seed_location(conn)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=loc, qty=12)
    conn.commit()

    rows = capture_snapshot(conn, str(BASELINE), source="cli")
    n1 = persist_snapshot(conn, rows, source="cli")
    n2 = persist_snapshot(conn, rows, source="cli")
    conn.commit()

    assert n1 >= 1 and n2 >= 1  # both report the coordinate as written
    stored = _snapshots_for(conn, BASELINE, item_id)
    assert len(stored) == 1, "same scenario/day -> one row per coordinate, never two"
    assert stored[0]["on_hand_qty"] == 12
    assert stored[0]["source"] == "cli"


def test_recapture_updates_on_hand_and_restamps_captured_at(conn):
    """A second capture after on-hand changed OVERWRITES on_hand_qty in place
    and re-stamps captured_at (EXCLUDED ... captured_at = now())."""
    item_id = _seed_item(conn)
    loc = _seed_location(conn)
    node = _seed_on_hand(
        conn, scenario_id=BASELINE, item_id=item_id, location_id=loc, qty=100
    )
    conn.commit()

    first = capture_snapshot(conn, str(BASELINE), source="cli")
    persist_snapshot(conn, first, source="cli")
    conn.commit()
    captured_at_1 = _snapshots_for(conn, BASELINE, item_id)[0]["captured_at"]

    # On-hand drops to 60 for the same coordinate/day.
    conn.execute("UPDATE nodes SET quantity = 60 WHERE node_id = %s", (node,))
    conn.commit()

    second = capture_snapshot(conn, str(BASELINE), source="cli")
    persist_snapshot(conn, second, source="cli")
    conn.commit()

    stored = _snapshots_for(conn, BASELINE, item_id)
    assert len(stored) == 1, "still one row for the coordinate/day"
    assert stored[0]["on_hand_qty"] == 60, "on_hand overwritten in place"
    assert stored[0]["captured_at"] >= captured_at_1, "captured_at re-stamped"


def test_persist_empty_batch_writes_nothing(conn):
    """persist_snapshot([]) is a no-op returning 0 — no row appears."""
    item_id = _seed_item(conn)
    conn.commit()

    assert persist_snapshot(conn, [], source="cli") == 0
    conn.commit()
    assert _snapshots_for(conn, BASELINE, item_id) == []


# ===========================================================================
# 3. NULL-honest shortage pair
# ===========================================================================


def test_persisted_rows_have_both_shortage_columns_null(conn):
    """PR1 contract: every persisted row carries first_shortage_date AND
    shortage_severity_usd as NULL together — never 0, never one-set-one-null."""
    item_a = _seed_item(conn, "snap-null-a")
    item_b = _seed_item(conn, "snap-null-b")
    loc = _seed_location(conn)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_a, location_id=loc, qty=3)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_b, location_id=loc, qty=0)
    conn.commit()

    rows = capture_snapshot(conn, str(BASELINE), source="cli")
    persist_snapshot(conn, rows, source="cli")
    conn.commit()

    for item_id in (item_a, item_b):
        stored = _snapshots_for(conn, BASELINE, item_id)
        assert len(stored) == 1
        assert stored[0]["first_shortage_date"] is None
        assert stored[0]["shortage_severity_usd"] is None


# ===========================================================================
# 4/6. Forkability — baseline vs a fork land on distinct scoped rows
# ===========================================================================


def test_capture_is_scenario_scoped_baseline_vs_fork(conn):
    """Same (item, location) with DIFFERENT on-hand on baseline vs a fork:
    capturing each scenario reads only that scenario's node, and persisting both
    yields two rows distinguished by scenario_id (forkable historisation)."""
    item_id = _seed_item(conn)
    loc = _seed_location(conn)
    fork = _seed_scenario(conn)
    _seed_on_hand(conn, scenario_id=BASELINE, item_id=item_id, location_id=loc, qty=10)
    _seed_on_hand(conn, scenario_id=fork, item_id=item_id, location_id=loc, qty=999)
    conn.commit()

    base_rows = capture_snapshot(conn, str(BASELINE), source="cli")
    fork_rows = capture_snapshot(conn, str(fork), source="cli")

    base_mine = [r for r in base_rows if r.item_id == item_id]
    fork_mine = [r for r in fork_rows if r.item_id == item_id]
    assert len(base_mine) == 1 and base_mine[0].on_hand_qty == 10
    assert len(fork_mine) == 1 and fork_mine[0].on_hand_qty == 999
    assert base_mine[0].scenario_id == BASELINE
    assert fork_mine[0].scenario_id == fork

    persist_snapshot(conn, base_rows, source="cli")
    persist_snapshot(conn, fork_rows, source="cli")
    conn.commit()

    base_stored = _snapshots_for(conn, BASELINE, item_id)
    fork_stored = _snapshots_for(conn, fork, item_id)
    assert len(base_stored) == 1 and base_stored[0]["on_hand_qty"] == 10
    assert len(fork_stored) == 1 and fork_stored[0]["on_hand_qty"] == 999


# ===========================================================================
# HTTP surface — app fixtures (the #392 test_agent_floor_integration pattern)
# ===========================================================================


@pytest.fixture(scope="module")
def api_client(migrated_db):
    """TestClient with get_db overridden onto the test DB (mirrors
    test_agent_floor_integration.py). The override means each request runs on a
    fresh OotilsDB connection bound to the test DSN."""
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = LEGACY_TOKEN

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


@pytest.fixture(autouse=True)
def _clear_token_cache():
    """Clear the in-process minted-token cache around every test so a seed in
    one test never leaks a cached auth decision into another."""
    import ootils_core.api.auth as auth

    auth._token_cache.clear()
    yield
    auth._token_cache.clear()


def _db_conn(dsn):
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _mint_token(dsn, *, actor_kind: str, scopes: list[str]) -> str:
    """Insert one api_tokens row; return the cleartext. The DB stores its
    SHA-256 via the same hash_token the auth layer uses on lookup."""
    from ootils_core.api.auth import hash_token, token_prefix

    clear = f"ootk_{actor_kind}_{uuid4().hex}"
    token_id = uuid4()
    with _db_conn(dsn) as c:
        c.execute(
            """
            INSERT INTO api_tokens (
                token_id, name, actor_kind, token_hash, token_prefix, scopes
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                token_id,
                f"snap-{actor_kind}-{token_id}",
                actor_kind,
                hash_token(clear),
                token_prefix(clear),
                scopes,
            ),
        )
    return clear


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def seed_api(migrated_db):
    """Seed one (item, location) with on-hand in BASELINE for the HTTP tests;
    return (item_id, location_id). Cleans its own coordinate afterwards."""
    with _db_conn(migrated_db) as c:
        item_id = c.execute(
            "INSERT INTO items (item_id, external_id, name) VALUES (%s, %s, %s) RETURNING item_id",
            (uuid4(), f"snap-api-{uuid4()}", "snap-api-item"),
        ).fetchone()["item_id"]
        loc = c.execute(
            "INSERT INTO locations (location_id, external_id, name) VALUES (%s, %s, %s) "
            "RETURNING location_id",
            (uuid4(), f"snap-api-loc-{uuid4()}", "snap-api-loc"),
        ).fetchone()["location_id"]
        c.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                quantity, time_grain, time_ref, active
            ) VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, 'exact_date', CURRENT_DATE, TRUE)
            """,
            (uuid4(), str(BASELINE), item_id, loc, 25),
        )
    yield item_id, loc
    with _db_conn(migrated_db) as c:
        c.execute("DELETE FROM inventory_snapshots WHERE item_id = %s", (item_id,))
        c.execute("DELETE FROM nodes WHERE item_id = %s", (item_id,))


# ===========================================================================
# 4. POST /v1/snapshots
# ===========================================================================


class TestCapturePost:
    def test_capture_201_and_persists_source_api(self, api_client, seed_api, migrated_db):
        item_id, loc = seed_api
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["ingest"])

        resp = api_client.post("/v1/snapshots", json={}, headers=_bearer(clear))
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["scenario_id"] == str(BASELINE)
        assert body["as_of_date"] is not None
        assert body["snapshots_captured"] >= 1

        with _db_conn(migrated_db) as c:
            row = c.execute(
                "SELECT on_hand_qty, source FROM inventory_snapshots "
                "WHERE scenario_id = %s AND item_id = %s AND location_id = %s",
                (str(BASELINE), item_id, loc),
            ).fetchone()
        assert row is not None
        assert row["on_hand_qty"] == 25
        assert row["source"] == "api", "the endpoint stamps source='api'"

    def test_capture_401_without_token(self, api_client, seed_api):
        resp = api_client.post("/v1/snapshots", json={})
        assert resp.status_code == 401, resp.text

    def test_capture_403_with_read_only_token(self, api_client, seed_api, migrated_db):
        """A write of persistent rows requires ``ingest`` — a read-only token is
        blocked on the scope floor (a write must not ride a read scope)."""
        clear = _mint_token(migrated_db, actor_kind="agent", scopes=["read"])
        resp = api_client.post("/v1/snapshots", json={}, headers=_bearer(clear))
        assert resp.status_code == 403, resp.text
        assert "ingest" in resp.json()["detail"].lower()

    def test_capture_503_when_kill_switch_off_writes_nothing(
        self, api_client, seed_api, migrated_db
    ):
        """OOTILS_SNAPSHOTS_ENABLED falsy -> 503 on the capture verb (checked
        after auth/scope but before the DB), and NO row is written — the escape
        hatch fully short-circuits the handler."""
        item_id, _loc = seed_api
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["ingest"])
        prev = os.environ.get("OOTILS_SNAPSHOTS_ENABLED")
        os.environ["OOTILS_SNAPSHOTS_ENABLED"] = "0"
        try:
            resp = api_client.post("/v1/snapshots", json={}, headers=_bearer(clear))
        finally:
            if prev is None:
                del os.environ["OOTILS_SNAPSHOTS_ENABLED"]
            else:
                os.environ["OOTILS_SNAPSHOTS_ENABLED"] = prev
        assert resp.status_code == 503, resp.text

        with _db_conn(migrated_db) as c:
            n = c.execute(
                "SELECT COUNT(*) AS n FROM inventory_snapshots WHERE item_id = %s",
                (item_id,),
            ).fetchone()["n"]
        assert n == 0, "a disabled capturer must not have written any row"

    def test_capture_respects_explicit_as_of(self, api_client, seed_api, migrated_db):
        """A body-supplied as_of is honoured on the persisted row and echoed in
        the response."""
        item_id, loc = seed_api
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["ingest"])
        resp = api_client.post(
            "/v1/snapshots", json={"as_of": "2026-03-15"}, headers=_bearer(clear)
        )
        assert resp.status_code == 201, resp.text
        assert resp.json()["as_of_date"] == "2026-03-15"

        with _db_conn(migrated_db) as c:
            row = c.execute(
                "SELECT as_of_date FROM inventory_snapshots "
                "WHERE item_id = %s AND location_id = %s",
                (item_id, loc),
            ).fetchone()
        assert str(row["as_of_date"]) == "2026-03-15"


# ===========================================================================
# 5. GET /v1/snapshots
# ===========================================================================


class TestQueryGet:
    def _capture(self, api_client, migrated_db, as_of: str | None = None):
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["ingest"])
        body = {} if as_of is None else {"as_of": as_of}
        resp = api_client.post("/v1/snapshots", json=body, headers=_bearer(clear))
        assert resp.status_code == 201, resp.text
        return resp.json()["as_of_date"]

    def test_get_403_without_read_scope(self, api_client, seed_api, migrated_db):
        clear = _mint_token(migrated_db, actor_kind="agent", scopes=["recommend:draft"])
        resp = api_client.get("/v1/snapshots", headers=_bearer(clear))
        assert resp.status_code == 403, resp.text
        assert "read" in resp.json()["detail"].lower()

    def test_get_returns_captured_rows_for_scenario(self, api_client, seed_api, migrated_db):
        item_id, loc = seed_api
        self._capture(api_client, migrated_db)
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])

        resp = api_client.get("/v1/snapshots", headers=_bearer(clear))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["scenario_id"] == str(BASELINE)
        mine = [
            s
            for s in body["snapshots"]
            if s["item_id"] == str(item_id) and s["location_id"] == str(loc)
        ]
        assert len(mine) == 1
        assert mine[0]["on_hand_qty"] == 25.0
        assert mine[0]["source"] == "api"
        assert mine[0]["first_shortage_date"] is None
        assert mine[0]["shortage_severity_usd"] is None

    def test_get_filters_by_item_id(self, api_client, seed_api, migrated_db):
        item_id, _loc = seed_api
        self._capture(api_client, migrated_db)
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])

        # A DIFFERENT item filter returns none of our coordinate.
        other = uuid4()
        resp = api_client.get(
            "/v1/snapshots", params={"item_id": str(other)}, headers=_bearer(clear)
        )
        assert resp.status_code == 200, resp.text
        assert all(s["item_id"] != str(item_id) for s in resp.json()["snapshots"])

        # Filtering to OUR item returns exactly our row(s).
        resp = api_client.get(
            "/v1/snapshots", params={"item_id": str(item_id)}, headers=_bearer(clear)
        )
        assert resp.status_code == 200, resp.text
        got = resp.json()["snapshots"]
        assert got and all(s["item_id"] == str(item_id) for s in got)

    def test_get_filters_by_as_of(self, api_client, seed_api, migrated_db):
        item_id, _loc = seed_api
        self._capture(api_client, migrated_db, as_of="2026-02-10")
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])

        resp = api_client.get(
            "/v1/snapshots", params={"as_of": "2026-02-10"}, headers=_bearer(clear)
        )
        assert resp.status_code == 200, resp.text
        mine = [s for s in resp.json()["snapshots"] if s["item_id"] == str(item_id)]
        assert mine and all(s["as_of_date"] == "2026-02-10" for s in mine)

        # A different day excludes our row.
        resp = api_client.get(
            "/v1/snapshots", params={"as_of": "2020-01-01"}, headers=_bearer(clear)
        )
        assert resp.status_code == 200, resp.text
        assert all(s["item_id"] != str(item_id) for s in resp.json()["snapshots"])

    def test_get_item_id_filter_is_parameterized_not_injected(
        self, api_client, seed_api, migrated_db
    ):
        """SQL-injection guard: item_id is typed as UUID by FastAPI, so a value
        carrying SQL metacharacters is rejected at the boundary (422) rather
        than reaching the query — the filter value is never string-interpolated
        into the WHERE. A malformed UUID cannot break or subvert the SQL."""
        clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])
        resp = api_client.get(
            "/v1/snapshots",
            params={"item_id": "'; DROP TABLE inventory_snapshots; --"},
            headers=_bearer(clear),
        )
        assert resp.status_code == 422, resp.text

        # The table is unharmed: a normal query still works.
        ok = api_client.get("/v1/snapshots", headers=_bearer(clear))
        assert ok.status_code == 200, ok.text

    def test_get_order_is_deterministic(self, api_client, migrated_db):
        """ORDER BY as_of_date DESC, item_id, location_id — the same query
        returns the same order across calls, and item_id is a non-decreasing
        secondary key within a single as_of day."""
        # Seed two items on two locations, captured for one explicit day.
        with _db_conn(migrated_db) as c:
            items = []
            for _ in range(2):
                iid = c.execute(
                    "INSERT INTO items (item_id, external_id, name) VALUES (%s, %s, %s) "
                    "RETURNING item_id",
                    (uuid4(), f"snap-ord-{uuid4()}", "snap-ord"),
                ).fetchone()["item_id"]
                items.append(iid)
                for _l in range(2):
                    lid = c.execute(
                        "INSERT INTO locations (location_id, external_id, name) "
                        "VALUES (%s, %s, %s) RETURNING location_id",
                        (uuid4(), f"snap-ord-loc-{uuid4()}", "snap-ord-loc"),
                    ).fetchone()["location_id"]
                    c.execute(
                        """
                        INSERT INTO nodes (node_id, node_type, scenario_id, item_id,
                            location_id, quantity, time_grain, time_ref, active)
                        VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, 'exact_date',
                            CURRENT_DATE, TRUE)
                        """,
                        (uuid4(), str(BASELINE), iid, lid, 1),
                    )
        try:
            ing = _mint_token(migrated_db, actor_kind="service", scopes=["ingest"])
            api_client.post(
                "/v1/snapshots", json={"as_of": "2026-05-20"}, headers=_bearer(ing)
            )
            clear = _mint_token(migrated_db, actor_kind="service", scopes=["read"])
            r1 = api_client.get(
                "/v1/snapshots", params={"as_of": "2026-05-20"}, headers=_bearer(clear)
            )
            r2 = api_client.get(
                "/v1/snapshots", params={"as_of": "2026-05-20"}, headers=_bearer(clear)
            )
            assert r1.status_code == 200 and r2.status_code == 200
            keys1 = [(s["item_id"], s["location_id"]) for s in r1.json()["snapshots"]]
            keys2 = [(s["item_id"], s["location_id"]) for s in r2.json()["snapshots"]]
            assert keys1 == keys2, "same query -> same order across calls"
            # Within a single as_of day, item_id is non-decreasing (the DESC is on
            # as_of_date only; item_id/location_id ascend as the tiebreak).
            item_seq = [s["item_id"] for s in r1.json()["snapshots"]]
            assert item_seq == sorted(item_seq)
        finally:
            with _db_conn(migrated_db) as c:
                for iid in items:
                    c.execute(
                        "DELETE FROM inventory_snapshots WHERE item_id = %s", (iid,)
                    )
                    c.execute("DELETE FROM nodes WHERE item_id = %s", (iid,))


# ===========================================================================
# 7. CLI — scripts/snapshot_inventory.py
# ===========================================================================


def test_cli_main_exits_zero_and_persists_source_cli(migrated_db):
    """snapshot_inventory.main(["--dsn", dsn, "--allow-dev"]) exits 0 and writes
    rows with source='cli'. --allow-dev clears the mrp_core.guard_db semi-prod
    refusal for an ``ootils_*`` test DB, exactly as the watcher CLIs are driven
    in test_agent_fleet_smoke.py."""
    import snapshot_inventory  # noqa: PLC0415 - scripts/ import seam

    with _db_conn(migrated_db) as c:
        item_id = c.execute(
            "INSERT INTO items (item_id, external_id, name) VALUES (%s, %s, %s) RETURNING item_id",
            (uuid4(), f"snap-cli-{uuid4()}", "snap-cli-item"),
        ).fetchone()["item_id"]
        loc = c.execute(
            "INSERT INTO locations (location_id, external_id, name) VALUES (%s, %s, %s) "
            "RETURNING location_id",
            (uuid4(), f"snap-cli-loc-{uuid4()}", "snap-cli-loc"),
        ).fetchone()["location_id"]
        c.execute(
            """
            INSERT INTO nodes (node_id, node_type, scenario_id, item_id, location_id,
                quantity, time_grain, time_ref, active)
            VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, 'exact_date', CURRENT_DATE, TRUE)
            """,
            (uuid4(), str(BASELINE), item_id, loc, 33),
        )
    try:
        rc = snapshot_inventory.main(["--dsn", migrated_db, "--allow-dev"])
        assert rc == 0

        with _db_conn(migrated_db) as c:
            row = c.execute(
                "SELECT on_hand_qty, source FROM inventory_snapshots "
                "WHERE scenario_id = %s AND item_id = %s AND location_id = %s",
                (str(BASELINE), item_id, loc),
            ).fetchone()
        assert row is not None
        assert row["on_hand_qty"] == 33
        assert row["source"] == "cli"
    finally:
        with _db_conn(migrated_db) as c:
            c.execute("DELETE FROM inventory_snapshots WHERE item_id = %s", (item_id,))
            c.execute("DELETE FROM nodes WHERE item_id = %s", (item_id,))
