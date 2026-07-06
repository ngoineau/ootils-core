"""
Integration tests for #394 B1 against a real PostgreSQL database (no mocks):

  1. conformal bounds exposed on GET /v1/forecast/runs/{run_id}/result — a
     calibrable leaf seed yields >= 1 bucket with non-None lower/upper and the
     sanity ordering lower <= value <= upper; a too-short seed yields None
     bounds on EVERY bucket (never 0);
  2. the new POST /v1/forecast/hierarchical-runs — a seeded block produces a
     201 with aggregate + leaf series (each carrying a run_id), recon_method
     == 'middleout', and GET /runs/{run_id}/result works for BOTH a leaf (CI
     exposed) and an aggregate (CI None, value_count > 0);
  3. backward-compat — POST /runs without recon_method still 201; the old
     endpoint rejects recon_method='middleout' (its ^none$ pattern holds);
  4. errors — unknown leaf_location_id -> 404; unknown block_code (VALID
     hierarchy) -> 422; recon_method='banana' -> 422 before the DB; malformed
     scenario_id -> 422;
  5. auth — POST /hierarchical-runs without a Bearer token -> 401;
  6. MinT provenance — recon_method='mintrace_wls_shrink' -> 201 with an
     EFFECTIVE recon_method in {mintrace_wls_shrink, middleout} (asserts
     membership, since the Nixtla backend may be absent in CI — provenance,
     not a fixed value, is the point).

Seeds mirror tests/integration/test_forecast_conformal_integration.py (the
calibrable noisy leaf series) and tests/integration/test_pyramide_reconcile_
integration.py (the _seed_block family/product generator), adapted to an
AUTOCOMMIT connection with an explicit teardown so the FastAPI TestClient —
which opens its OWN connection through the get_db override — sees the rows.

Anti-flake rule (inherited from those files): every seeded demand fact sits at
TODAY-2 or earlier (booked_date < server CURRENT_DATE), the forecast horizon
starts at TODAY+2. No timing assertions anywhere.
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

# Deterministic "noise" (mirrors test_forecast_conformal_integration.py):
# amplitude around a base level of 100, zero randomness (ADR-003 discipline
# applies to fixtures too). Period 8 vs MA window 4 exposes the full residual
# spread over the rolling backtest.
NOISE_PATTERN = (-8, 5, -3, 9, -6, 2, -7, 8)
BASE_LEVEL = 100

# _seed_block generator (mirrors test_pyramide_reconcile_integration.py):
# a KNOWN 7-day weekly pattern scaled per leaf; 8 full weeks so every weekday
# appears 8 times.
WEEK_PATTERN = (2, 1, 1, 1, 3, 4, 2)  # sum = 14
AMPLITUDES = {"A1": 10, "A2": 6, "A3": 4}
HISTORY_DAYS = range(4, 60)  # 56 consecutive days = 8 full weeks


# ---------------------------------------------------------------------------
# DB access + seeding helpers
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
        (item_id, f"{ext_id} b1 test item", ext_id),
    )
    return item_id


def _create_location(conn, ext_id: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{ext_id} b1 test DC", ext_id),
    )
    return loc_id


def _seed_demand(conn, item_id: UUID, item_code: str, warehouse_id: str,
                 booked_date: date, qty) -> None:
    conn.execute(
        """
        INSERT INTO demand_history (
            item_id, item_code, stream, booked_date, ordered_quantity,
            value_ext, counts_for_asp, org_id, fulfillment, order_number,
            warehouse_id
        ) VALUES (%s, %s, 'regular', %s, %s, 0, FALSE, 'PPS', 'standard',
                  'TEST-B1', %s)
        """,
        (item_id, item_code, booked_date, qty, warehouse_id),
    )


def _seed_noisy_series(conn, item_id: UUID, item_code: str, warehouse_id: str,
                       days: int) -> list[Decimal]:
    """``days`` consecutive daily facts ending at TODAY - 2, quantity =
    BASE_LEVEL + NOISE_PATTERN cycling. Returns quantities in date order."""
    quantities: list[Decimal] = []
    start = TODAY - timedelta(days=2 + days - 1)
    for i in range(days):
        qty = Decimal(BASE_LEVEL + NOISE_PATTERN[i % len(NOISE_PATTERN)])
        _seed_demand(conn, item_id, item_code, warehouse_id,
                     start + timedelta(days=i), qty)
        quantities.append(qty)
    return quantities


def _run_and_persist(conn, item_id: UUID, location_id: UUID,
                     history: list[Decimal], horizon_days: int):
    """Run the leaf runner on an in-memory series and persist it (mirrors the
    conformal-seed helper). Returns (result, persisted)."""
    from ootils_core.pyramide import PyramideRunConfig, PyramideRunner
    from ootils_core.pyramide.repository import persist_run

    config = PyramideRunConfig(
        item_id=item_id,
        location_id=location_id,
        scenario_id=BASELINE_SCENARIO_ID,
        horizon_start=TODAY + timedelta(days=2),
        horizon_days=horizon_days,
        granularity="daily",
        method="MA",
        method_params={"window": 4},
    )
    result = PyramideRunner().run(config, history)
    persisted = persist_run(conn, result)
    return result, persisted


def _cleanup_item(conn, item_id: UUID, location_id: UUID | None) -> None:
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
    """FAM-<tag> (family) -> PRD-<tag>1 (2 items) + PRD-<tag>2 (1 item),
    with 8 weeks of KNOWN weekly-seasonal demand per leaf (mirrors
    test_pyramide_reconcile_integration.py:_seed_block). AUTOCOMMIT variant so
    the API TestClient sees the rows."""
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
        ext = f"B1-{tag}-{suffix}"
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
                ) VALUES (%s, %s, 'regular', %s, %s, 0, FALSE, 'standard', 'B1')
                """,
                (item_id, ext, day, qty),
            )

    loc_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name, location_type, country, external_id) "
        "VALUES (%s, %s, 'dc', 'US', %s)",
        (loc_id, f"B1-{tag} DC", f"B1-{tag}-DC"),
    )
    return fam, prd1, prd2, items, loc_id


def _cleanup_block(conn, h: str, items: dict[str, UUID], loc_id: UUID) -> None:
    """FK-safe teardown of a seeded block (forecasts cascade their values)."""
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
# FastAPI app / client wiring (mirrors the seeded-DB pattern)
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


# ---------------------------------------------------------------------------
# 1. Conformal bounds on GET /runs/{run_id}/result
# ---------------------------------------------------------------------------


def test_result_exposes_conformal_bounds_when_calibrated(migrated_db, client, auth):
    """A long calibrable seed: GET /runs/{run_id}/result returns >= 1 bucket
    with non-None bounds and lower <= value <= upper (sanity)."""
    with _db_conn(migrated_db) as conn:
        code = f"B1CONF-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            seeded = _seed_noisy_series(conn, item_id, code, f"DC-{code}", days=124)
            _, persisted = _run_and_persist(
                conn, item_id, location_id, seeded, horizon_days=14
            )

            resp = client.get(
                f"/v1/forecast/runs/{persisted.run_id}/result", headers=auth
            )
            assert resp.status_code == 200, resp.text
            values = resp.json()["values"]
            assert len(values) == 14
            with_bounds = [
                v for v in values
                if v["confidence_lower"] is not None
                and v["confidence_upper"] is not None
            ]
            assert with_bounds, "expected at least one calibrated bucket"
            for v in with_bounds:
                lower = Decimal(v["confidence_lower"])
                upper = Decimal(v["confidence_upper"])
                point = Decimal(v["quantity"])
                assert lower >= Decimal("0")
                assert lower <= point <= upper
        finally:
            _cleanup_item(conn, item_id, location_id)


def test_result_bounds_are_none_without_calibration(migrated_db, client, auth):
    """A too-short seed (not enough backtest residuals for the finite-sample
    guarantee): EVERY bucket's bounds are None — honest, never 0."""
    with _db_conn(migrated_db) as conn:
        code = f"B1SHORT-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            seeded = _seed_noisy_series(conn, item_id, code, f"DC-{code}", days=5)
            _, persisted = _run_and_persist(
                conn, item_id, location_id, seeded, horizon_days=3
            )

            resp = client.get(
                f"/v1/forecast/runs/{persisted.run_id}/result", headers=auth
            )
            assert resp.status_code == 200, resp.text
            values = resp.json()["values"]
            assert len(values) == 3
            for v in values:
                assert v["confidence_lower"] is None
                assert v["confidence_upper"] is None
        finally:
            _cleanup_item(conn, item_id, location_id)


# ---------------------------------------------------------------------------
# 2. POST /hierarchical-runs — nominal
# ---------------------------------------------------------------------------


def test_hierarchical_run_nominal(migrated_db, client, auth):
    """A seeded block -> 201 with aggregate + leaf series (each with a run_id),
    recon_method == 'middleout'; GET result works for a LEAF (CI exposed) and
    an AGGREGATE (CI None, value_count > 0)."""
    h = "b1-h-nominal"
    with _db_conn(migrated_db) as conn:
        fam, prd1, prd2, items, loc_id = _seed_block(conn, h, "NOM")
        try:
            resp = client.post(
                "/v1/forecast/hierarchical-runs",
                json={
                    "hierarchy_id": h,
                    "block_code": fam,
                    "leaf_location_id": f"B1-{'NOM'}-DC",
                    "horizon_days": 14,
                    "method": "SEASONAL",
                    "method_params": {"season_length": 7},
                    "lookback_days": 90,
                },
                headers=auth,
            )
            assert resp.status_code == 201, resp.text
            body = resp.json()
            assert body["hierarchy_id"] == h
            assert body["block_code"] == fam
            assert body["recon_method"] == "middleout"
            assert body["block_level"] == "family"

            series = body["series"]
            # 3 aggregates (fam + prd1 + prd2) + 3 leaves.
            kinds = [s["kind"] for s in series]
            assert kinds.count("aggregate") == 3
            assert kinds.count("leaf") == 3
            for s in series:
                assert UUID(s["run_id"])  # every series is queryable

            leaf_item_ids = {str(i) for i in items.values()}
            leaf_series = next(s for s in series if s["key"] in leaf_item_ids)
            agg_series = next(s for s in series if s["key"] == fam)

            # A leaf result: bounds exposed (>= 1 calibrated bucket expected on
            # 8 seasonal cycles of node residuals).
            leaf_res = client.get(
                f"/v1/forecast/runs/{leaf_series['run_id']}/result", headers=auth
            )
            assert leaf_res.status_code == 200, leaf_res.text
            leaf_body = leaf_res.json()
            assert leaf_body["run"]["value_count"] == 14
            assert leaf_body["run"]["item_id"] is not None
            leaf_bounded = [
                v for v in leaf_body["values"]
                if v["confidence_lower"] is not None
            ]
            assert leaf_bounded, "leaf run should carry conformal bounds"
            for v in leaf_bounded:
                lower = Decimal(v["confidence_lower"])
                upper = Decimal(v["confidence_upper"])
                assert lower <= Decimal(v["quantity"]) <= upper

            # An aggregate result: CI None everywhere, but value_count > 0.
            agg_res = client.get(
                f"/v1/forecast/runs/{agg_series['run_id']}/result", headers=auth
            )
            assert agg_res.status_code == 200, agg_res.text
            agg_body = agg_res.json()
            assert agg_body["run"]["value_count"] == 14
            assert agg_body["run"]["item_id"] is None
            assert agg_body["run"]["node_code"] == fam
            assert agg_body["run"]["level"] == "family"
            assert len(agg_body["values"]) == 14
            for v in agg_body["values"]:
                assert v["confidence_lower"] is None
                assert v["confidence_upper"] is None
        finally:
            _cleanup_block(conn, h, items, loc_id)


# ---------------------------------------------------------------------------
# 3. Backward compatibility of the leaf POST /runs endpoint
# ---------------------------------------------------------------------------


def test_post_runs_without_recon_method_still_created(migrated_db, client, auth):
    """The leaf endpoint keeps working unchanged when recon_method is omitted
    (it defaults to 'none') — 201."""
    with _db_conn(migrated_db) as conn:
        code = f"B1BC-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        forecast_id = None
        try:
            _seed_noisy_series(conn, item_id, code, f"DC-{code}", days=60)
            resp = client.post(
                "/v1/forecast/runs",
                json={
                    "item_id": code,
                    "location_id": f"DC-{code}",
                    "horizon_days": 7,
                    "method": "MA",
                    "method_params": {"window": 4},
                },
                headers=auth,
            )
            assert resp.status_code == 201, resp.text
            body = resp.json()
            forecast_id = body["forecast_id"]
            assert body["recon_method"] == "none"
        finally:
            _cleanup_item(conn, item_id, location_id)
            assert forecast_id is None or UUID(forecast_id)


def test_post_runs_rejects_reconciliation_method(migrated_db, client, auth):
    """The OLD endpoint's ^none$ pattern holds: recon_method='middleout' on
    POST /runs is a 422 (before any DB work)."""
    resp = client.post(
        "/v1/forecast/runs",
        json={
            "item_id": "ANY-ITEM",
            "location_id": "ANY-LOC",
            "recon_method": "middleout",
        },
        headers=auth,
    )
    assert resp.status_code == 422, resp.text
    locs = {
        tuple(err["loc"]) for err in resp.json()["detail"]
        if isinstance(err, dict) and "loc" in err
    }
    assert any("recon_method" in loc for loc in locs)


# ---------------------------------------------------------------------------
# 4. Error paths
# ---------------------------------------------------------------------------


def test_hierarchical_run_unknown_location_404(migrated_db, client, auth):
    """Unknown leaf_location_id -> 404 (resolve_location_uuid returns None)."""
    h = "b1-h-badloc"
    with _db_conn(migrated_db) as conn:
        fam, _, _, items, loc_id = _seed_block(conn, h, "BADLOC")
        try:
            resp = client.post(
                "/v1/forecast/hierarchical-runs",
                json={
                    "hierarchy_id": h,
                    "block_code": fam,
                    "leaf_location_id": "NO-SUCH-LOCATION-XYZ",
                    "method": "SEASONAL",
                    "method_params": {"season_length": 7},
                },
                headers=auth,
            )
            assert resp.status_code == 404, resp.text
            assert "not found" in resp.json()["detail"].lower()
        finally:
            _cleanup_block(conn, h, items, loc_id)


def test_hierarchical_run_unknown_block_422(migrated_db, client, auth):
    """A VALID hierarchy but an unknown block_code -> 422 (PyramideError from
    _load_block; the handler maps PyramideError to 422)."""
    h = "b1-h-badblock"
    with _db_conn(migrated_db) as conn:
        _, _, _, items, loc_id = _seed_block(conn, h, "BADBLK")
        try:
            resp = client.post(
                "/v1/forecast/hierarchical-runs",
                json={
                    "hierarchy_id": h,
                    "block_code": "FAM-DOES-NOT-EXIST",
                    "leaf_location_id": f"B1-{'BADBLK'}-DC",
                    "method": "SEASONAL",
                    "method_params": {"season_length": 7},
                },
                headers=auth,
            )
            assert resp.status_code == 422, resp.text
        finally:
            _cleanup_block(conn, h, items, loc_id)


def test_hierarchical_run_bad_recon_method_422_before_db(migrated_db, client, auth):
    """recon_method='banana' is rejected by the Pydantic pattern -> 422, before
    any resolution/DB work (no seed needed)."""
    resp = client.post(
        "/v1/forecast/hierarchical-runs",
        json={
            "hierarchy_id": "whatever",
            "block_code": "whatever",
            "leaf_location_id": "whatever",
            "recon_method": "banana",
        },
        headers=auth,
    )
    assert resp.status_code == 422, resp.text
    locs = {
        tuple(err["loc"]) for err in resp.json()["detail"]
        if isinstance(err, dict) and "loc" in err
    }
    assert any("recon_method" in loc for loc in locs)


def test_hierarchical_run_malformed_scenario_id_422(migrated_db, client, auth):
    """A syntactically invalid scenario_id -> 422 (resolve_scenario_uuid raises
    ValueError, the handler maps it to 422)."""
    h = "b1-h-badscenario"
    with _db_conn(migrated_db) as conn:
        fam, _, _, items, loc_id = _seed_block(conn, h, "BADSC")
        try:
            resp = client.post(
                "/v1/forecast/hierarchical-runs",
                json={
                    "hierarchy_id": h,
                    "block_code": fam,
                    "leaf_location_id": f"B1-{'BADSC'}-DC",
                    "scenario_id": "not-a-uuid",
                    "method": "SEASONAL",
                    "method_params": {"season_length": 7},
                },
                headers=auth,
            )
            assert resp.status_code == 422, resp.text
            assert "scenario_id" in resp.json()["detail"].lower()
        finally:
            _cleanup_block(conn, h, items, loc_id)


# ---------------------------------------------------------------------------
# 5. Auth
# ---------------------------------------------------------------------------


def test_hierarchical_run_without_bearer_401(migrated_db, client):
    """The router uses require_auth: no Bearer token -> 401."""
    resp = client.post(
        "/v1/forecast/hierarchical-runs",
        json={
            "hierarchy_id": "whatever",
            "block_code": "whatever",
            "leaf_location_id": "whatever",
        },
    )
    assert resp.status_code == 401, resp.text


# ---------------------------------------------------------------------------
# 6. MinT provenance never lies
# ---------------------------------------------------------------------------


def test_hierarchical_run_mint_reports_effective_method(migrated_db, client, auth):
    """recon_method='mintrace_wls_shrink' -> 201 with an EFFECTIVE recon_method
    in {mintrace_wls_shrink, middleout}: the runner reports the method it
    actually applied (fallback to middleout when the Nixtla backend or its
    aligned inputs are unavailable). Membership, not a fixed value — provenance
    is the object of the test."""
    h = "b1-h-mint"
    with _db_conn(migrated_db) as conn:
        fam, _, _, items, loc_id = _seed_block(conn, h, "MINT")
        try:
            resp = client.post(
                "/v1/forecast/hierarchical-runs",
                json={
                    "hierarchy_id": h,
                    "block_code": fam,
                    "leaf_location_id": f"B1-{'MINT'}-DC",
                    "horizon_days": 14,
                    "method": "SEASONAL",
                    "method_params": {"season_length": 7},
                    "lookback_days": 90,
                    "recon_method": "mintrace_wls_shrink",
                },
                headers=auth,
            )
            assert resp.status_code == 201, resp.text
            body = resp.json()
            assert body["recon_method"] in {"mintrace_wls_shrink", "middleout"}
            # If it fell back, the runner must say so in warnings (provenance).
            if body["recon_method"] == "middleout":
                assert any(
                    "fell back" in w or "skipped" in w or "falling back" in w
                    for w in body["warnings"]
                ), body["warnings"]
        finally:
            _cleanup_block(conn, h, items, loc_id)
