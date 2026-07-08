"""
Integration tests for the DEM-1 head/tail routing WIRING (PR-1) against a real
PostgreSQL database (no mocks). This is the backend counterpart of the DB-free
unit suite tests/test_pyramide_routing_wiring.py: there the seams are
monkeypatched, here the whole chain runs end to end through the two Pyramide
forecast endpoints and lands routed_method/routed_level/routing_reason
(migration 058) on real pyramide_runs rows.

Six cases (the wiring the unit suite can only assert in isolation):

  1. POST /v1/forecast/hierarchical-runs with auto_route=True on a seeded
     block -> routed_method/routed_level/routing_reason are non-NULL on 100 %
     of the block's series (aggregates AND leaves), read back through
     GET /runs/{run_id} for every persisted series.
  2. Single-series method=AUTO_SELECT + auto_route=True -> the routed method
     REPLACES the executed method and is traced: pyramide_runs.method ==
     routed_method (a concrete method, not AUTO_SELECT).
  3. Single-series with an EXPLICIT method + auto_route=True -> the executed
     method is NOT overwritten, yet the router's recommendation is still
     recorded as provenance (routed_method present and != the executed one).
  4. auto_route=False (the default) -> routed_* stay NULL: the opt-in is a
     byte-identical kill switch.
  5. Cold-start (an item with only 3 demand points) -> the router picks the
     conservative cold-start branch and the run completes WITHOUT crashing
     (the FM recommendation executes via its deterministic AUTO_SELECT
     fallback — the FM backend is optional and absent here).
  6. A series routed to a concrete executable method runs WITH it: the frozen
     forecast values (GET /runs/{run_id}/result) all carry that method.

PREMISES verified against the code before writing (blind — this file was never
executed):
  - build_series_features / build_routing_decisions live in
    pyramide/repository.py; auto_route is an opt-in bool on BOTH
    PyramideRunRequest and HierarchicalRunRequest (api/routers/pyramide.py).
    On the single-series endpoint the router pins level='leaf' and only
    overwrites method when the caller left it AUTO_SELECT. On the
    hierarchical endpoint build_routing_decisions produces one decision per
    series of the block and HierarchicalRunner persists routing per series
    (routing=config.routing_decisions.get(ref.key)) — so a routed run stamps
    provenance on every series.
  - Routing is DETERMINISTIC from features computed off demand_history:
      * a sparse-CALENDAR series (bookings on few days over a wide span,
        span >= cold_start_days=60) -> zero_ratio > intermittent_zero_ratio
        (0.6) -> INTERMITTENT -> CROSTON at leaf (a concrete, executable
        method != AUTO_SELECT); CROSTON is robust on the dense positive-only
        series get_historical_demand returns (validate min_length=1).
      * a very short series (span < 60) -> COLD_START -> FM_CHRONOS at leaf.
        FM_CHRONOS with no strict_backend falls back to a deterministic
        AUTO_SELECT (engines.PyramideForecastEngine.forecast_foundation_batch)
        — so the run never 422s on the absent optional FM backend.
  - The seeded block reuses the family/product shape of
    test_pyramide_hierarchy_integration.py / test_pyramide_b1_integration.py
    (hierarchy / hierarchy_node / item_hierarchy + demand_history), with a
    reconciliation node that cold-starts to FM_CHRONOS and executes via the
    same AUTO_SELECT fallback (recon_method stays 'middleout', so no MinT
    batch-only refusal).
  - migration 058 columns are routed_method / routed_level / routing_reason
    on pyramide_runs, exposed on PyramideRunOut; all-or-none, routed_level in
    ('leaf','aggregate').
  - demand_history.ingested_at DEFAULTS now() -> every seed reads FRESH, so
    the ADR-023 freshness gate never marks these runs stale.

Conventions mirror test_pyramide_b1_integration.py: an AUTOCOMMIT _db_conn for
seeding/teardown (the FastAPI TestClient opens its OWN connection via the
get_db override, so it only sees committed rows), FK-ordered cleanup PER TEST
in a finally block, and the anti-flake rule — every seeded booking sits at
TODAY-6 or earlier (booked_date < server CURRENT_DATE), never on a same-day
edge.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")
TODAY = date.today()

# Weekly-seasonal block pattern (mirrors test_pyramide_b1_integration.py):
# a KNOWN 7-day pattern scaled per leaf, seeded over 8 full weeks.
WEEK_PATTERN = (2, 1, 1, 1, 3, 4, 2)
AMPLITUDES = {"A1": 10, "A2": 6, "A3": 4}
HISTORY_DAYS = range(4, 60)  # 56 consecutive days = 8 full weeks


# ---------------------------------------------------------------------------
# DB access + seeding helpers (dedicated entities, unique codes per test)
# ---------------------------------------------------------------------------


def _db_conn(dsn):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _create_item(conn, ext_id: str) -> UUID:
    item_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, name, item_type, uom, status, external_id)
        VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)
        """,
        (item_id, f"{ext_id} dem1 routing item", ext_id),
    )
    return item_id


def _create_location(conn, ext_id: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{ext_id} dem1 routing DC", ext_id),
    )
    return loc_id


def _seed_booking(conn, item_id: UUID, item_code: str, warehouse_id: str,
                  booked_date: date, qty) -> None:
    """One demand_history booking. warehouse_id must equal the leaf location's
    external_id so the single-series site filter (_warehouse_codes_subquery)
    resolves the row. ingested_at defaults now() -> fresh."""
    conn.execute(
        """
        INSERT INTO demand_history (
            item_id, item_code, stream, booked_date, ordered_quantity,
            value_ext, counts_for_asp, fulfillment, order_number, warehouse_id
        ) VALUES (%s, %s, 'regular', %s, %s, 0, FALSE, 'standard', 'DEM1-RW', %s)
        """,
        (item_id, item_code, booked_date, qty, warehouse_id),
    )


def _seed_intermittent(conn, item_id: UUID, code: str, warehouse_id: str) -> None:
    """A sparse-CALENDAR series: 6 bookings over a ~92-day span. The router
    reads history_depth_days = span (>= cold_start_days=60, so NOT cold-start)
    and zero_ratio = (span - 6)/span ~ 0.94 (> intermittent_zero_ratio=0.6),
    so it classifies INTERMITTENT and routes to CROSTON at leaf — a concrete
    executable method distinct from AUTO_SELECT."""
    days_back = (100, 80, 60, 40, 20, 8)
    quantities = (10, 12, 8, 15, 9, 11)
    for back, qty in zip(days_back, quantities):
        _seed_booking(conn, item_id, code, warehouse_id,
                      TODAY - timedelta(days=back), qty)


def _seed_cold_start(conn, item_id: UUID, code: str, warehouse_id: str) -> None:
    """An item with only 3 demand points, clustered over a 4-day span
    (span < cold_start_days=60) -> the router classifies COLD_START and routes
    to FM_CHRONOS at leaf (no twin, no aggregate signal on a standalone
    series)."""
    for back, qty in ((10, 5), (8, 7), (6, 6)):
        _seed_booking(conn, item_id, code, warehouse_id,
                      TODAY - timedelta(days=back), qty)


def _cleanup_item(conn, item_id: UUID, location_id: UUID | None) -> None:
    """FK-ordered teardown (mirrors test_pyramide_b1_integration.py). Deleting
    pyramide_runs / forecasts cascades to snapshots, forecast_values and
    accuracy_metrics; demand_history and forecasts must go before the item
    (ON DELETE RESTRICT)."""
    conn.execute(
        "DELETE FROM pyramide_snapshots WHERE forecast_id IN "
        "(SELECT forecast_id FROM forecasts WHERE item_id = %s)",
        (item_id,),
    )
    conn.execute(
        "DELETE FROM pyramide_runs WHERE forecast_id IN "
        "(SELECT forecast_id FROM forecasts WHERE item_id = %s)",
        (item_id,),
    )
    conn.execute("DELETE FROM forecasts WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM demand_history WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    if location_id is not None:
        conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _seed_block(conn, h: str, tag: str):
    """FAM-<tag> (family) -> PRD-<tag>1 (2 items) + PRD-<tag>2 (1 item), each
    leaf carrying 8 weeks of dense weekly-seasonal demand (mirrors
    test_pyramide_b1_integration.py:_seed_block). The reconciliation node
    (family) reads a 56-day span < cold_start_days=60 with no ASP, so it
    routes to FM_CHRONOS and executes via the deterministic AUTO_SELECT
    fallback — no strict backend, no MinT (recon stays middleout). AUTOCOMMIT
    variant so the API TestClient's own connection sees the rows."""
    conn.execute(
        """
        INSERT INTO hierarchy (hierarchy_id, domain, scope, levels, is_default)
        VALUES (%s, 'product', 'local', %s, FALSE)
        """,
        (h, ["family", "product"]),
    )
    fam, prd1, prd2 = f"FAM-{tag}", f"PRD-{tag}1", f"PRD-{tag}2"
    for code, level, parent in [
        (fam, "family", None),
        (prd1, "product", fam),
        (prd2, "product", fam),
    ]:
        conn.execute(
            "INSERT INTO hierarchy_node (hierarchy_id, code, level, parent_code) "
            "VALUES (%s, %s, %s, %s)",
            (h, code, level, parent),
        )

    items: dict[str, UUID] = {}
    for suffix, leaf_code in [("A1", prd1), ("A2", prd1), ("A3", prd2)]:
        item_id = uuid4()
        ext = f"DEM1-{tag}-{suffix}"
        conn.execute(
            "INSERT INTO items (item_id, name, item_type, uom, status, external_id) "
            "VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)",
            (item_id, f"{ext} item", ext),
        )
        conn.execute(
            "INSERT INTO item_hierarchy (item_id, hierarchy_id, leaf_code) "
            "VALUES (%s, %s, %s)",
            (item_id, h, leaf_code),
        )
        items[suffix] = item_id
        for back in HISTORY_DAYS:
            day = TODAY - timedelta(days=back)
            qty = AMPLITUDES[suffix] * WEEK_PATTERN[day.weekday()]
            conn.execute(
                """
                INSERT INTO demand_history (
                    item_id, item_code, stream, booked_date,
                    ordered_quantity, value_ext, counts_for_asp,
                    fulfillment, order_number
                ) VALUES (%s, %s, 'regular', %s, %s, 0, FALSE, 'standard', 'DEM1')
                """,
                (item_id, ext, day, qty),
            )

    loc_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name, location_type, country, external_id) "
        "VALUES (%s, %s, 'dc', 'US', %s)",
        (loc_id, f"DEM1-{tag} DC", f"DEM1-{tag}-DC"),
    )
    return fam, prd1, prd2, items, loc_id


def _cleanup_block(conn, h: str, items: dict[str, UUID], loc_id: UUID) -> None:
    """FK-safe teardown of a seeded block (mirrors
    test_pyramide_b1_integration.py:_cleanup_block)."""
    item_ids = list(items.values())
    conn.execute(
        "DELETE FROM pyramide_snapshots WHERE forecast_id IN "
        "(SELECT forecast_id FROM forecasts WHERE hierarchy_id = %s "
        " OR item_id = ANY(%s))",
        (h, item_ids),
    )
    conn.execute(
        "DELETE FROM pyramide_runs WHERE forecast_id IN "
        "(SELECT forecast_id FROM forecasts WHERE hierarchy_id = %s "
        " OR item_id = ANY(%s))",
        (h, item_ids),
    )
    conn.execute(
        "DELETE FROM forecasts WHERE hierarchy_id = %s OR item_id = ANY(%s)",
        (h, item_ids),
    )
    conn.execute(
        "DELETE FROM nodes WHERE node_type = 'ForecastDemand' AND location_id = %s",
        (loc_id,),
    )
    conn.execute("DELETE FROM demand_history WHERE item_id = ANY(%s)", (item_ids,))
    conn.execute("DELETE FROM item_hierarchy WHERE hierarchy_id = %s", (h,))
    conn.execute("DELETE FROM items WHERE item_id = ANY(%s)", (item_ids,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (loc_id,))
    conn.execute("DELETE FROM hierarchy_node WHERE hierarchy_id = %s", (h,))
    conn.execute("DELETE FROM hierarchy WHERE hierarchy_id = %s", (h,))


# ---------------------------------------------------------------------------
# FastAPI app / client wiring (mirrors test_pyramide_b1_integration.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def app(migrated_db):
    os.environ["DATABASE_URL"] = migrated_db
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    application = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    application.dependency_overrides[get_db] = override_db
    yield application
    application.dependency_overrides.clear()


@pytest.fixture
def client(app):
    from fastapi.testclient import TestClient

    with TestClient(app) as c:
        yield c


@pytest.fixture
def auth():
    return {"Authorization": "Bearer integration-test-token"}


def _post_run(client, auth, code: str, **body):
    payload = {"item_id": code, "location_id": f"DC-{code}", "horizon_days": 7}
    payload.update(body)
    return client.post("/v1/forecast/runs", json=payload, headers=auth)


# ---------------------------------------------------------------------------
# 1. Hierarchical auto_route stamps routing on EVERY series
# ---------------------------------------------------------------------------


def test_hierarchical_auto_route_stamps_routing_on_every_series(
    migrated_db, client, auth
):
    """POST /hierarchical-runs auto_route=True -> the router builds a decision
    for every series of the block and the runner persists it per series, so
    reading each series' run back through GET /runs/{run_id} yields non-NULL
    routed_method / routed_level / routing_reason on 100 % of them (the 3
    aggregate nodes AND the 3 leaves)."""
    h = "dem1-h-rw-all"
    with _db_conn(migrated_db) as conn:
        fam, prd1, prd2, items, loc_id = _seed_block(conn, h, "ALL")
        try:
            resp = client.post(
                "/v1/forecast/hierarchical-runs",
                json={
                    "hierarchy_id": h,
                    "block_code": fam,
                    "leaf_location_id": "DEM1-ALL-DC",
                    "horizon_days": 14,
                    "lookback_days": 90,
                    "auto_route": True,
                },
                headers=auth,
            )
            assert resp.status_code == 201, resp.text
            series = resp.json()["series"]
            # 3 aggregates (fam + prd1 + prd2) + 3 leaves = 6 series, all routed.
            assert len(series) == 6
            assert [s["kind"] for s in series].count("aggregate") == 3
            assert [s["kind"] for s in series].count("leaf") == 3

            for s in series:
                run = client.get(
                    f"/v1/forecast/runs/{s['run_id']}", headers=auth
                )
                assert run.status_code == 200, run.text
                body = run.json()
                assert body["routed_method"] is not None, (s["kind"], s["key"])
                assert body["routed_level"] in {"leaf", "aggregate"}, body
                assert body["routing_reason"], body
        finally:
            _cleanup_block(conn, h, items, loc_id)


# ---------------------------------------------------------------------------
# 2. Single-series AUTO_SELECT + auto_route -> routed method executes
# ---------------------------------------------------------------------------


def test_single_series_auto_select_executes_routed_method(
    migrated_db, client, auth
):
    """method=AUTO_SELECT + auto_route=True on an intermittent series: the
    router recommends CROSTON, that recommendation REPLACES the executed
    method (pyramide_runs.method == routed_method), and the provenance is
    traced (a concrete method, not AUTO_SELECT)."""
    code = f"DEM1RW2-{uuid4().hex[:8].upper()}"
    with _db_conn(migrated_db) as conn:
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            _seed_intermittent(conn, item_id, code, f"DC-{code}")
            resp = _post_run(client, auth, code, auto_route=True)
            assert resp.status_code == 201, resp.text
            body = resp.json()
            assert body["routed_method"] == "CROSTON", body
            assert body["routed_method"] != "AUTO_SELECT"
            assert body["routed_level"] == "leaf", body
            assert body["routing_reason"], body
            # The routed recommendation became the executed method contract.
            assert body["method"] == body["routed_method"]
        finally:
            _cleanup_item(conn, item_id, location_id)


# ---------------------------------------------------------------------------
# 3. Single-series explicit method -> not overwritten, recommendation recorded
# ---------------------------------------------------------------------------


def test_single_series_explicit_method_survives_but_records_routing(
    migrated_db, client, auth
):
    """An EXPLICIT method + auto_route=True: the executed method is NOT
    overwritten (a caller who asked for MA gets MA), yet the router's
    recommendation is still recorded as routed provenance (present and != the
    executed method)."""
    code = f"DEM1RW3-{uuid4().hex[:8].upper()}"
    with _db_conn(migrated_db) as conn:
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            _seed_intermittent(conn, item_id, code, f"DC-{code}")
            resp = _post_run(
                client, auth, code,
                auto_route=True, method="MA", method_params={"window": 3},
            )
            assert resp.status_code == 201, resp.text
            body = resp.json()
            # Explicit method survives untouched.
            assert body["method"] == "MA", body
            # The recommendation is still recorded, and it is a different
            # method (the router would have picked CROSTON for this series).
            assert body["routed_method"] == "CROSTON", body
            assert body["routed_method"] != body["method"]
            assert body["routed_level"] == "leaf", body
            assert body["routing_reason"], body
        finally:
            _cleanup_item(conn, item_id, location_id)


# ---------------------------------------------------------------------------
# 4. auto_route=False (default) -> routed_* NULL (byte-identical kill switch)
# ---------------------------------------------------------------------------


def test_auto_route_off_leaves_routing_columns_null(migrated_db, client, auth):
    """The opt-in defaults False: without auto_route the run is byte-identical
    to the historical behaviour — routed_method / routed_level /
    routing_reason all stay NULL."""
    code = f"DEM1RW4-{uuid4().hex[:8].upper()}"
    with _db_conn(migrated_db) as conn:
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            _seed_intermittent(conn, item_id, code, f"DC-{code}")
            # auto_route omitted entirely (default False).
            resp = _post_run(client, auth, code)
            assert resp.status_code == 201, resp.text
            body = resp.json()
            assert body["routed_method"] is None, body
            assert body["routed_level"] is None, body
            assert body["routing_reason"] is None, body
        finally:
            _cleanup_item(conn, item_id, location_id)


# ---------------------------------------------------------------------------
# 5. Cold-start -> conservative route, no crash
# ---------------------------------------------------------------------------


def test_cold_start_routes_conservatively_without_crashing(
    migrated_db, client, auth
):
    """An item with only 3 demand points routes to the conservative cold-start
    branch (FM_CHRONOS at leaf). auto_route=True on an AUTO_SELECT request
    therefore hands execution to FM_CHRONOS — which, with the optional FM
    backend absent and no strict_backend, falls back to a deterministic
    AUTO_SELECT: the run COMPLETES (201) and carries the cold-start
    provenance, it never crashes."""
    code = f"DEM1RW5-{uuid4().hex[:8].upper()}"
    with _db_conn(migrated_db) as conn:
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            _seed_cold_start(conn, item_id, code, f"DC-{code}")
            resp = _post_run(client, auth, code, auto_route=True)
            assert resp.status_code == 201, resp.text
            body = resp.json()
            assert body["routed_method"] == "FM_CHRONOS", body
            assert body["routed_level"] == "leaf", body
            assert "cold-start" in body["routing_reason"].lower(), body
            # The FM recommendation is the executed method contract; internally
            # it fell back to AUTO_SELECT, but that never surfaces as an error.
            assert body["method"] == "FM_CHRONOS"
        finally:
            _cleanup_item(conn, item_id, location_id)


# ---------------------------------------------------------------------------
# 6. A routed method actually runs: the frozen values carry it
# ---------------------------------------------------------------------------


def test_routed_method_is_carried_in_result_values(migrated_db, client, auth):
    """A series routed to CROSTON (concrete, executable) runs WITH it: the
    frozen forecast values read back through GET /runs/{run_id}/result all
    carry method == the routed method — proof the routed recommendation drove
    execution, not just provenance."""
    code = f"DEM1RW6-{uuid4().hex[:8].upper()}"
    with _db_conn(migrated_db) as conn:
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            _seed_intermittent(conn, item_id, code, f"DC-{code}")
            resp = _post_run(client, auth, code, auto_route=True)
            assert resp.status_code == 201, resp.text
            run_id = resp.json()["run_id"]
            assert resp.json()["routed_method"] == "CROSTON"

            result = client.get(
                f"/v1/forecast/runs/{run_id}/result", headers=auth
            )
            assert result.status_code == 200, result.text
            payload = result.json()
            assert payload["run"]["routed_method"] == "CROSTON"
            assert payload["run"]["method"] == "CROSTON"
            values = payload["values"]
            assert values, "expected at least one frozen forecast value"
            assert all(v["method"] == "CROSTON" for v in values), values
            # Sanity: demand forecasts are clamped >= 0.
            for v in values:
                assert Decimal(v["quantity"]) >= Decimal("0")
        finally:
            _cleanup_item(conn, item_id, location_id)
