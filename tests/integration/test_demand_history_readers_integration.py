"""
Integration tests for the demand-history readers (#331, solde de #333)
against a real PostgreSQL database (no mocks).

Covered readers:
  - src/ootils_core/pyramide/repository.py:get_historical_demand
    (single source of truth — primary demand_history source + degraded
    CustomerOrderDemand fallback)
  - src/ootils_core/api/routers/forecasting.py:_get_historical_demand
    (thin delegation, exercised through POST /v1/demand/forecast/generate)
  - POST /v1/forecast/runs (Pyramide) through the same reader.

Each test creates its OWN item/location pair (fresh external_ids) so the
seeded PUMP-01/VALVE-02 data and the other tests are never perturbed, and
cleans up the rows it inserted (style mirrors _delete_forecast in
test_forecasting_api_integration.py).
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db, TEST_DB_URL

pytestmark = requires_db

SEED_SCRIPT = Path(__file__).parents[2] / "scripts" / "seed_demo_data.py"
AUTH_HEADERS = {"Authorization": "Bearer integration-test-token"}
BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")
TODAY = date.today()


def _run_seed():
    env = {**os.environ, "DATABASE_URL": TEST_DB_URL}
    return subprocess.run(
        [sys.executable, str(SEED_SCRIPT)],
        capture_output=True, text=True, env=env,
    )


# ---------------------------------------------------------------------------
# Shared module-scoped fixtures (mirror tests/integration/test_forecasting_api_integration.py)
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
# Helpers — direct DB access for setup/teardown
# ---------------------------------------------------------------------------


def _db_conn(dsn):
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _create_pair(conn, item_ext: str, loc_ext: str) -> tuple[UUID, UUID]:
    """Create a fresh item/location pair; return (item_uuid, location_uuid)."""
    item_id, loc_id = uuid4(), uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, name, item_type, uom, status, external_id)
        VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)
        """,
        (item_id, f"{item_ext} test item", item_ext),
    )
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{loc_ext} test DC", loc_ext),
    )
    return item_id, loc_id


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
            warehouse_id, fulfillment, order_number
        ) VALUES (%s, %s, %s, %s, %s, 0, FALSE, %s, %s, 'TEST-DH')
        """,
        (item_id, item_code, stream, booked_date, qty, warehouse_id, fulfillment),
    )


def _insert_demand_node(
    conn,
    node_type: str,
    item_id: UUID,
    location_id: UUID,
    time_ref: date,
    qty: int,
    scenario_id: UUID = BASELINE_SCENARIO_ID,
) -> UUID:
    """Insert a demand node like real ingestion does: time_span_start NULL,
    only time_ref set (ADR-019) — exercises the COALESCE fallback path."""
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_ref, quantity, qty_uom, active
        ) VALUES (%s, %s, %s, %s, %s, 'exact_date', %s, %s, 'EA', TRUE)
        """,
        (node_id, node_type, scenario_id, item_id, location_id, time_ref, qty),
    )
    return node_id


def _cleanup_pair(conn, item_id: UUID, location_id: UUID):
    """Best-effort teardown of everything a test attached to its pair."""
    conn.execute("DELETE FROM demand_history WHERE item_id = %s", (item_id,))
    conn.execute(
        """
        DELETE FROM edges WHERE from_node_id IN (
            SELECT node_id FROM nodes WHERE item_id = %s
        ) OR to_node_id IN (SELECT node_id FROM nodes WHERE item_id = %s)
        """,
        (item_id, item_id),
    )
    conn.execute(
        "DELETE FROM events WHERE trigger_node_id IN "
        "(SELECT node_id FROM nodes WHERE item_id = %s)",
        (item_id,),
    )
    conn.execute(
        "DELETE FROM pyramide_snapshot_demand_nodes WHERE demand_node_id IN "
        "(SELECT node_id FROM nodes WHERE item_id = %s)",
        (item_id,),
    )
    conn.execute("DELETE FROM nodes WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM projection_series WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM pyramide_snapshots WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM pyramide_runs WHERE item_id = %s", (item_id,))
    conn.execute(
        "DELETE FROM forecast_adjustments WHERE forecast_id IN "
        "(SELECT forecast_id FROM forecasts WHERE item_id = %s)",
        (item_id,),
    )
    conn.execute(
        "DELETE FROM forecast_values WHERE forecast_id IN "
        "(SELECT forecast_id FROM forecasts WHERE item_id = %s)",
        (item_id,),
    )
    conn.execute("DELETE FROM forecasts WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _reader(conn, item_id, location_id, lookback_days=90, scenario_id=BASELINE_SCENARIO_ID):
    from ootils_core.pyramide.repository import get_historical_demand
    return get_historical_demand(
        db=conn,
        item_id=item_id,
        location_id=location_id,
        lookback_days=lookback_days,
        scenario_id=scenario_id,
    )


# ---------------------------------------------------------------------------
# (a) demand_history is the primary training source
# ---------------------------------------------------------------------------


class TestPrimarySourceDemandHistory:
    def test_generate_trains_on_demand_history(self, api_client, auth, seeded_db):
        """MA forecast equals the mean of the inserted booking facts —
        proving the signal comes from demand_history, not graph nodes."""
        with _db_conn(seeded_db) as conn:
            item_id, loc_id = _create_pair(conn, "DH-A-ITEM", "DH-A-LOC")
            for days_ago in range(1, 11):  # 10 past days, constant qty=20
                _insert_dh(conn, item_id, "DH-A-ITEM", "DH-A-LOC",
                           TODAY - timedelta(days=days_ago), 20)
        try:
            resp = api_client.post(
                "/v1/demand/forecast/generate",
                json={
                    "item_id": "DH-A-ITEM",
                    "location_id": "DH-A-LOC",
                    "horizon_days": 7,
                    "method": "MA",
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            data = resp.json()
            assert len(data["values"]) == 7
            # MA over a constant-20 series must forecast exactly 20.
            for value in data["values"]:
                assert Decimal(str(value["quantity"])) == Decimal("20")
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_pair(conn, item_id, loc_id)

    def test_pyramide_run_source_history_count(self, api_client, auth, seeded_db):
        """POST /v1/forecast/runs — source_history_count equals the number
        of distinct booked days inserted into demand_history."""
        with _db_conn(seeded_db) as conn:
            item_id, loc_id = _create_pair(conn, "DH-B-ITEM", "DH-B-LOC")
            for days_ago in range(1, 13):  # 12 distinct past days
                _insert_dh(conn, item_id, "DH-B-ITEM", "DH-B-LOC",
                           TODAY - timedelta(days=days_ago), 5)
        try:
            resp = api_client.post(
                "/v1/forecast/runs",
                json={
                    "item_id": "DH-B-ITEM",
                    "location_id": "DH-B-LOC",
                    "horizon_days": 14,
                    "method": "MA",
                },
                headers=auth,
            )
            assert resp.status_code == 201, resp.text
            assert resp.json()["source_history_count"] == 12
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_pair(conn, item_id, loc_id)


# ---------------------------------------------------------------------------
# (b) Anti-contamination: ForecastDemand is NEVER a training signal (#333)
# ---------------------------------------------------------------------------


class TestForecastDemandExclusion:
    def test_past_forecast_demand_node_is_ignored(self, api_client, auth, seeded_db):
        """A huge PAST ForecastDemand node for the pair does not change the
        forecast — training reads demand_history only."""
        with _db_conn(seeded_db) as conn:
            item_id, loc_id = _create_pair(conn, "DH-C-ITEM", "DH-C-LOC")
            for days_ago in range(1, 11):
                _insert_dh(conn, item_id, "DH-C-ITEM", "DH-C-LOC",
                           TODAY - timedelta(days=days_ago), 20)
            _insert_demand_node(conn, "ForecastDemand", item_id, loc_id,
                                TODAY - timedelta(days=5), 99999)
        try:
            resp = api_client.post(
                "/v1/demand/forecast/generate",
                json={
                    "item_id": "DH-C-ITEM",
                    "location_id": "DH-C-LOC",
                    "horizon_days": 5,
                    "method": "MA",
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            for value in resp.json()["values"]:
                assert Decimal(str(value["quantity"])) == Decimal("20")
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_pair(conn, item_id, loc_id)

    def test_committed_pyramide_run_does_not_self_contaminate(
        self, api_client, auth, seeded_db
    ):
        """End of #333 auto-contamination: run → commit (writes ForecastDemand
        nodes) → re-run: the training history is unchanged."""
        with _db_conn(seeded_db) as conn:
            item_id, loc_id = _create_pair(conn, "DH-D-ITEM", "DH-D-LOC")
            for days_ago in range(1, 9):  # 8 past days
                _insert_dh(conn, item_id, "DH-D-ITEM", "DH-D-LOC",
                           TODAY - timedelta(days=days_ago), 10)
        try:
            body = {
                "item_id": "DH-D-ITEM",
                "location_id": "DH-D-LOC",
                "horizon_days": 7,
                "method": "MA",
            }
            first = api_client.post("/v1/forecast/runs", json=body, headers=auth)
            assert first.status_code == 201, first.text
            assert first.json()["source_history_count"] == 8

            commit = api_client.post(
                f"/v1/forecast/runs/{first.json()['run_id']}/commit",
                headers=auth,
            )
            assert commit.status_code == 200, commit.text
            assert commit.json()["demand_node_count"] > 0  # ForecastDemand written

            second = api_client.post("/v1/forecast/runs", json=body, headers=auth)
            assert second.status_code == 201, second.text
            # History identical: committed ForecastDemand nodes are ignored.
            assert second.json()["source_history_count"] == 8
            assert second.json()["total_quantity"] == first.json()["total_quantity"]
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_pair(conn, item_id, loc_id)


# ---------------------------------------------------------------------------
# (c) Degraded fallback: CustomerOrderDemand nodes, with explicit warning
# ---------------------------------------------------------------------------


class TestCustomerOrderFallback:
    def test_fallback_reads_past_customer_orders_only(self, seeded_db, caplog):
        """No demand_history rows → past baseline CustomerOrderDemand nodes
        feed the series; ForecastDemand stays excluded; a degraded-mode
        warning is logged."""
        with _db_conn(seeded_db) as conn:
            item_id, loc_id = _create_pair(conn, "DH-E-ITEM", "DH-E-LOC")
            _insert_demand_node(conn, "CustomerOrderDemand", item_id, loc_id,
                                TODAY - timedelta(days=10), 30)
            _insert_demand_node(conn, "CustomerOrderDemand", item_id, loc_id,
                                TODAY - timedelta(days=5), 50)
            # Future CO: excluded (strict past)
            _insert_demand_node(conn, "CustomerOrderDemand", item_id, loc_id,
                                TODAY + timedelta(days=5), 400)
            # Past ForecastDemand: excluded even in fallback (#333)
            _insert_demand_node(conn, "ForecastDemand", item_id, loc_id,
                                TODAY - timedelta(days=7), 99999)
            try:
                with caplog.at_level(
                    logging.WARNING, logger="ootils_core.pyramide.repository"
                ):
                    history = _reader(conn, item_id, loc_id)
                assert history == [Decimal("30"), Decimal("50")]
                assert any(
                    "falling back to CustomerOrderDemand" in rec.getMessage()
                    for rec in caplog.records
                )
            finally:
                _cleanup_pair(conn, item_id, loc_id)

    def test_generate_succeeds_in_degraded_mode(self, api_client, auth, seeded_db):
        """API path: generate works off the fallback when demand_history is empty."""
        with _db_conn(seeded_db) as conn:
            item_id, loc_id = _create_pair(conn, "DH-F-ITEM", "DH-F-LOC")
            _insert_demand_node(conn, "CustomerOrderDemand", item_id, loc_id,
                                TODAY - timedelta(days=8), 40)
            _insert_demand_node(conn, "CustomerOrderDemand", item_id, loc_id,
                                TODAY - timedelta(days=4), 40)
        try:
            resp = api_client.post(
                "/v1/demand/forecast/generate",
                json={
                    "item_id": "DH-F-ITEM",
                    "location_id": "DH-F-LOC",
                    "horizon_days": 5,
                    "method": "MA",
                },
                headers=auth,
            )
            assert resp.status_code == 200, resp.text
            for value in resp.json()["values"]:
                assert Decimal(str(value["quantity"])) == Decimal("40")
        finally:
            with _db_conn(seeded_db) as conn:
                _cleanup_pair(conn, item_id, loc_id)


# ---------------------------------------------------------------------------
# (d) Fallback scenario isolation
# ---------------------------------------------------------------------------


class TestFallbackScenarioIsolation:
    def test_fork_nodes_invisible_from_baseline_and_vice_versa(self, seeded_db):
        with _db_conn(seeded_db) as conn:
            item_id, loc_id = _create_pair(conn, "DH-G-ITEM", "DH-G-LOC")
            fork_id = uuid4()
            conn.execute(
                """
                INSERT INTO scenarios (scenario_id, name, parent_scenario_id, is_baseline, status)
                VALUES (%s, 'dh-reader-fork', %s, FALSE, 'active')
                """,
                (fork_id, BASELINE_SCENARIO_ID),
            )
            _insert_demand_node(conn, "CustomerOrderDemand", item_id, loc_id,
                                TODAY - timedelta(days=6), 25, scenario_id=fork_id)
            try:
                # Baseline read: fork-only node is invisible.
                assert _reader(conn, item_id, loc_id) == []
                # Fork read: sees the fork's node.
                assert _reader(conn, item_id, loc_id, scenario_id=fork_id) == [Decimal("25")]
            finally:
                _cleanup_pair(conn, item_id, loc_id)
                conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (fork_id,))


# ---------------------------------------------------------------------------
# (e) Business filters on demand_history
# ---------------------------------------------------------------------------


class TestDemandHistoryBusinessFilters:
    def test_stream_fulfillment_and_date_filters(self, seeded_db):
        with _db_conn(seeded_db) as conn:
            item_id, loc_id = _create_pair(conn, "DH-H-ITEM", "DH-H-LOC")
            # Counted: regular / standard / strict past
            _insert_dh(conn, item_id, "DH-H-ITEM", "DH-H-LOC",
                       TODAY - timedelta(days=3), 10)
            # Excluded: warranty stream (separate forecast)
            _insert_dh(conn, item_id, "DH-H-ITEM", "DH-H-LOC",
                       TODAY - timedelta(days=4), 100, stream="warranty")
            # Excluded: inter-entity flow (PPS→PCC double-count)
            _insert_dh(conn, item_id, "DH-H-ITEM", "DH-H-LOC",
                       TODAY - timedelta(days=5), 100, fulfillment="inter_entity")
            # Excluded: today (partial day, strict past) and future
            _insert_dh(conn, item_id, "DH-H-ITEM", "DH-H-LOC", TODAY, 100)
            _insert_dh(conn, item_id, "DH-H-ITEM", "DH-H-LOC",
                       TODAY + timedelta(days=1), 100)
            try:
                assert _reader(conn, item_id, loc_id) == [Decimal("10")]
            finally:
                _cleanup_pair(conn, item_id, loc_id)

    def test_null_fulfillment_rows_are_counted(self, seeded_db):
        """fulfillment is nullable — NULL rows stay in the regular series."""
        with _db_conn(seeded_db) as conn:
            item_id, loc_id = _create_pair(conn, "DH-I-ITEM", "DH-I-LOC")
            _insert_dh(conn, item_id, "DH-I-ITEM", "DH-I-LOC",
                       TODAY - timedelta(days=2), 6, fulfillment=None)
            try:
                assert _reader(conn, item_id, loc_id) == [Decimal("6")]
            finally:
                _cleanup_pair(conn, item_id, loc_id)


# ---------------------------------------------------------------------------
# (f) Warehouse → location mapping
# ---------------------------------------------------------------------------


class TestWarehouseMapping:
    def test_unmatched_or_null_warehouse_rows_leave_the_site_series(self, seeded_db):
        with _db_conn(seeded_db) as conn:
            item_id, loc_id = _create_pair(conn, "DH-J-ITEM", "DH-J-LOC")
            # Counted: warehouse_id matches the location external_id
            _insert_dh(conn, item_id, "DH-J-ITEM", "DH-J-LOC",
                       TODAY - timedelta(days=3), 7)
            # Excluded from this site's series: other / unknown DC and NULL
            _insert_dh(conn, item_id, "DH-J-ITEM", "UNKNOWN-DC",
                       TODAY - timedelta(days=2), 100)
            _insert_dh(conn, item_id, "DH-J-ITEM", None,
                       TODAY - timedelta(days=1), 100)
            try:
                assert _reader(conn, item_id, loc_id) == [Decimal("7")]
            finally:
                _cleanup_pair(conn, item_id, loc_id)


# ---------------------------------------------------------------------------
# (g) Seed non-regression: the seeded demand_history feeds the demo pairs
# ---------------------------------------------------------------------------


class TestSeededDemandHistory:
    def test_seed_populates_past_bookings_for_demo_pairs(self, seeded_db):
        with _db_conn(seeded_db) as conn:
            rows = conn.execute(
                """
                SELECT dh.warehouse_id, COUNT(*) AS n,
                       MAX(dh.booked_date) AS latest
                FROM demand_history dh
                WHERE dh.order_number = 'SEED-DH'
                GROUP BY dh.warehouse_id
                ORDER BY dh.warehouse_id
                """
            ).fetchall()
            by_wh = {r["warehouse_id"]: r for r in rows}
            assert set(by_wh) == {"DC-ATL", "DC-LAX"}
            for r in by_wh.values():
                assert r["n"] == 90
                assert r["latest"] < TODAY  # strict past

    def test_seeded_pair_reads_through_reader(self, seeded_db):
        """PUMP-01 @ DC-ATL: 90 sparse daily sums of 15 from the seed."""
        with _db_conn(seeded_db) as conn:
            item_row = conn.execute(
                "SELECT item_id FROM items WHERE external_id = 'PUMP-01'"
            ).fetchone()
            loc_row = conn.execute(
                "SELECT location_id FROM locations WHERE external_id = 'DC-ATL'"
            ).fetchone()
            history = _reader(conn, item_row["item_id"], loc_row["location_id"])
            assert len(history) == 90
            assert all(q == Decimal("15") for q in history)
