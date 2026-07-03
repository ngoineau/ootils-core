"""
Integration tests for Pyramide axis D — PR-D4 (confidence + freshness,
ADR-023) against a real PostgreSQL database (no mocks).

Covered:
  - get_demand_freshness: empty table -> all-None (freshness is never
    invented); seeded rows -> ingest_age_days / coverage_lag_days
    computed on the DB server clock, with global / per-item /
    per-item x warehouse filters;
  - POST /v1/forecast/runs on provably stale demand (ingested_at pushed
    past the SLA) -> run created with stale_demand=TRUE, EXACTLY ONE
    dq_findings STALE_DEMAND row whose evidence carries the run_id, and
    an attributed agent_runs ledger row; a fresh ingest -> no finding,
    stale_demand=FALSE;
  - POST /v1/demand/forecast/generate exposes confidence_components
    (additive contract) and a confidence_score reproducible from the
    traced components x documented default weights.

Conventions mirror test_pyramide_accuracy_metrics_integration.py:
module-scoped migrated_db, autocommit _db_conn + finally-cleanup.
Anti-flake rule: all seeded booked_date values are strictly in the past.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from .conftest import requires_db

pytestmark = requires_db

TODAY = date.today()
NOISE_PATTERN = (-8, 5, -3, 9, -6, 2, -7, 8)
BASE_LEVEL = 100


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
        (item_id, f"{ext_id} confidence test item", ext_id),
    )
    return item_id


def _create_location(conn, ext_id: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{ext_id} confidence test DC", ext_id),
    )
    return loc_id


def _seed_demand(conn, item_id: UUID, ext_id: str, warehouse: str,
                 days: int, ingest_age_days: int) -> None:
    """Deterministic booking facts, booked_date strictly past, with an
    EXPLICIT ingested_at so the freshness age is controlled by the test
    (the column default is now())."""
    for back in range(2, 2 + days):
        day = TODAY - timedelta(days=back)
        qty = BASE_LEVEL + NOISE_PATTERN[back % len(NOISE_PATTERN)]
        conn.execute(
            """
            INSERT INTO demand_history (
                item_id, item_code, stream, booked_date, ordered_quantity,
                value_ext, counts_for_asp, fulfillment, order_number,
                warehouse_id, ingested_at
            ) VALUES (%s, %s, 'regular', %s, %s, 0, FALSE, 'standard', 'D4',
                      %s, now() - (%s::int * INTERVAL '1 day'))
            """,
            (item_id, ext_id, day, qty, warehouse, ingest_age_days),
        )


def _cleanup(conn, item_id: UUID, location_id: UUID) -> None:
    conn.execute("DELETE FROM demand_history WHERE item_id = %s", (item_id,))
    conn.execute(
        "DELETE FROM dq_findings WHERE entity_id = %s AND rule_code = 'STALE_DEMAND'",
        (item_id,),
    )
    conn.execute(
        "DELETE FROM agent_runs WHERE agent_name = 'pyramide_freshness_gate' "
        "AND agent_run_id NOT IN (SELECT agent_run_id FROM dq_findings)",
    )
    conn.execute(
        """
        DELETE FROM pyramide_snapshots WHERE forecast_id IN (
            SELECT forecast_id FROM forecasts WHERE item_id = %s
        )
        """,
        (item_id,),
    )
    conn.execute(
        """
        DELETE FROM pyramide_runs WHERE forecast_id IN (
            SELECT forecast_id FROM forecasts WHERE item_id = %s
        )
        """,
        (item_id,),
    )
    conn.execute(
        """
        DELETE FROM forecast_values WHERE forecast_id IN (
            SELECT forecast_id FROM forecasts WHERE item_id = %s
        )
        """,
        (item_id,),
    )
    conn.execute("DELETE FROM forecasts WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _test_client(dsn):
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"
    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB

    app = create_app()

    def override_db():
        db = OotilsDB(dsn)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db
    return app, TestClient(app), {"Authorization": "Bearer integration-test-token"}


# ---------------------------------------------------------------------------
# get_demand_freshness
# ---------------------------------------------------------------------------


def test_get_demand_freshness_no_rows_returns_all_none(migrated_db):
    """No qualifying row -> every field None: an unknown freshness is
    never invented (the confidence score degrades prudently instead)."""
    from ootils_core.pyramide.repository import get_demand_freshness

    with _db_conn(migrated_db) as conn:
        freshness = get_demand_freshness(conn, item_id=uuid4())
        assert freshness.last_booked_date is None
        assert freshness.max_ingested_at is None
        assert freshness.ingest_age_days is None
        assert freshness.coverage_lag_days is None


def test_get_demand_freshness_computed_ages_and_filters(migrated_db):
    """Two items with different ingest ages: the global view follows the
    freshest ingest; per-item and per-item x warehouse filters isolate
    each series. Ages are whole days computed on the DB server clock."""
    from ootils_core.pyramide.repository import get_demand_freshness

    with _db_conn(migrated_db) as conn:
        code = f"D4F-{uuid4().hex[:8].upper()}"
        stale_item = _create_item(conn, f"{code}-STALE")
        fresh_item = _create_item(conn, f"{code}-FRESH")
        loc = _create_location(conn, f"DC-{code}")
        try:
            _seed_demand(conn, stale_item, f"{code}-STALE", f"DC-{code}",
                         days=10, ingest_age_days=12)
            _seed_demand(conn, fresh_item, f"{code}-FRESH", f"DC-{code}-2",
                         days=10, ingest_age_days=0)

            stale = get_demand_freshness(conn, item_id=stale_item)
            assert stale.ingest_age_days == 12
            # Most recent booked_date seeded is TODAY - 2 (server clock:
            # allow the day to roll over mid-test).
            assert stale.coverage_lag_days in (2, 3)
            assert stale.last_booked_date is not None
            assert stale.max_ingested_at is not None

            fresh = get_demand_freshness(conn, item_id=fresh_item)
            assert fresh.ingest_age_days == 0

            # Global view follows the freshest ingest across all rows.
            overall = get_demand_freshness(conn)
            assert overall.ingest_age_days == 0

            # item x warehouse: the stale item never shipped through the
            # fresh item's DC -> no row -> all-None (not the item view).
            cross = get_demand_freshness(
                conn, item_id=stale_item, warehouse_id=f"DC-{code}-2"
            )
            assert cross.ingest_age_days is None
            same = get_demand_freshness(
                conn, item_id=stale_item, warehouse_id=f"DC-{code}"
            )
            assert same.ingest_age_days == 12
        finally:
            conn.execute("DELETE FROM demand_history WHERE item_id = %s", (fresh_item,))
            conn.execute("DELETE FROM items WHERE item_id = %s", (fresh_item,))
            _cleanup(conn, stale_item, loc)


# ---------------------------------------------------------------------------
# Freshness gate on POST /v1/forecast/runs
# ---------------------------------------------------------------------------


def test_stale_run_emits_one_finding_and_marks_provenance(migrated_db):
    """Demand ingested 20 days ago vs SLA 7: the run is still produced
    (agents may simulate on stale data) but carries stale_demand=TRUE,
    and EXACTLY ONE dq_findings STALE_DEMAND row exists with the run_id
    in its evidence, attributed to a COMPLETED agent_runs ledger row."""
    with _db_conn(migrated_db) as conn:
        code = f"D4S-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        app, client, auth = _test_client(migrated_db)
        try:
            _seed_demand(conn, item_id, code, f"DC-{code}",
                         days=60, ingest_age_days=20)
            with client:
                response = client.post(
                    "/v1/forecast/runs",
                    headers=auth,
                    json={
                        "item_id": code,
                        "location_id": f"DC-{code}",
                        "horizon_days": 7,
                        "method": "MA",
                        "method_params": {"window": 4},
                        "freshness_sla_days": 7,
                    },
                )
            assert response.status_code == 201
            payload = response.json()
            assert payload["stale_demand"] is True
            run_id = payload["run_id"]

            # Provenance persisted on the run itself (migration 056).
            row = conn.execute(
                "SELECT stale_demand FROM pyramide_runs WHERE run_id = %s",
                (run_id,),
            ).fetchone()
            assert row["stale_demand"] is True

            # Exactly one STALE_DEMAND finding, evidence -> run_id.
            findings = conn.execute(
                """
                SELECT f.*, r.status AS ledger_status, r.agent_name AS ledger_agent
                FROM dq_findings f
                JOIN agent_runs r ON r.agent_run_id = f.agent_run_id
                WHERE f.rule_code = 'STALE_DEMAND' AND f.entity_id = %s
                """,
                (item_id,),
            ).fetchall()
            assert len(findings) == 1
            finding = findings[0]
            assert finding["evidence"]["pyramide_run_id"] == run_id
            assert finding["evidence"]["sla_days"] == 7
            assert finding["evidence"]["ingest_age_days"] == 20
            assert finding["impact_value"] == 20
            assert finding["ledger_status"] == "COMPLETED"
            assert finding["ledger_agent"] == "pyramide_freshness_gate"
        finally:
            app.dependency_overrides.clear()
            _cleanup(conn, item_id, location_id)


def test_fresh_run_emits_no_finding(migrated_db):
    """Fresh ingest (age 0 <= SLA): no STALE_DEMAND row, stale_demand
    FALSE — the gate only fires on PROVEN staleness."""
    with _db_conn(migrated_db) as conn:
        code = f"D4N-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        app, client, auth = _test_client(migrated_db)
        try:
            _seed_demand(conn, item_id, code, f"DC-{code}",
                         days=60, ingest_age_days=0)
            with client:
                response = client.post(
                    "/v1/forecast/runs",
                    headers=auth,
                    json={
                        "item_id": code,
                        "location_id": f"DC-{code}",
                        "horizon_days": 7,
                        "method": "MA",
                        "method_params": {"window": 4},
                    },
                )
            assert response.status_code == 201
            assert response.json()["stale_demand"] is False
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM dq_findings "
                "WHERE rule_code = 'STALE_DEMAND' AND entity_id = %s",
                (item_id,),
            ).fetchone()
            assert count["n"] == 0
        finally:
            app.dependency_overrides.clear()
            _cleanup(conn, item_id, location_id)


# ---------------------------------------------------------------------------
# /v1/demand/forecast/generate — confidence contract (ADR-023)
# ---------------------------------------------------------------------------


def test_generate_exposes_traced_confidence(migrated_db):
    """confidence_score comes from the deterministic composer and the
    additive confidence_components trace makes it reproducible by hand
    (components x documented default weights 0.5/0.25/0.25)."""
    with _db_conn(migrated_db) as conn:
        code = f"D4G-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        app, client, auth = _test_client(migrated_db)
        try:
            _seed_demand(conn, item_id, code, f"DC-{code}",
                         days=60, ingest_age_days=1)
            with client:
                response = client.post(
                    "/v1/demand/forecast/generate",
                    headers=auth,
                    json={
                        "item_id": code,
                        "location_id": f"DC-{code}",
                        "horizon_days": 14,
                        "method": "MA",
                        "freshness_sla_days": 7,
                    },
                )
            assert response.status_code == 200
            meta = response.json()["metadata"]
            trace = meta["confidence_components"]
            components = trace["components"]

            assert set(components) == {"accuracy", "depth", "freshness"}
            score = Decimal(str(meta["confidence_score"]))
            assert Decimal("0") <= score <= Decimal("1")

            # Reproducible by hand from the trace (default weights).
            recomposed = (
                Decimal(str(components["accuracy"])) * Decimal("0.5")
                + Decimal(str(components["depth"])) * Decimal("0.25")
                + Decimal(str(components["freshness"])) * Decimal("0.25")
            ).quantize(Decimal("0.0001"))
            assert score == recomposed

            # Fresh ingest (age 1 <= SLA 7) -> full freshness, not stale.
            assert trace["stale"] is False
            assert Decimal(str(components["freshness"])) == Decimal("1")
            assert trace["ingest_age_days"] == 1
            assert trace["sla_days"] == 7
            # 60 observed demand days out of a 365-day saturation.
            assert trace["history_depth_days"] == 60
            # No Pyramide backtest for this series yet -> prudent default
            # accuracy, with the absent source named honestly.
            assert trace["accuracy_source"] is None
            assert Decimal(str(components["accuracy"])) == Decimal("0.25")
            assert "prudent default" in trace["explanation"]
        finally:
            app.dependency_overrides.clear()
            _cleanup(conn, item_id, location_id)


def test_generate_accuracy_component_uses_persisted_wape(migrated_db):
    """Once a Pyramide run has persisted an aggregate backtest WAPE for
    the series, /generate maps it into the accuracy component:
    accuracy = 1/(1+wape), source named in the trace."""
    with _db_conn(migrated_db) as conn:
        code = f"D4W-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        app, client, auth = _test_client(migrated_db)
        try:
            _seed_demand(conn, item_id, code, f"DC-{code}",
                         days=60, ingest_age_days=1)
            with client:
                run_response = client.post(
                    "/v1/forecast/runs",
                    headers=auth,
                    json={
                        "item_id": code,
                        "location_id": f"DC-{code}",
                        "horizon_days": 7,
                        "method": "MA",
                        "method_params": {"window": 4},
                    },
                )
                assert run_response.status_code == 201

                response = client.post(
                    "/v1/demand/forecast/generate",
                    headers=auth,
                    json={
                        "item_id": code,
                        "location_id": f"DC-{code}",
                        "horizon_days": 14,
                        "method": "MA",
                    },
                )
            assert response.status_code == 200
            trace = response.json()["metadata"]["confidence_components"]
            assert trace["accuracy_source"] == "pyramide_accuracy_metrics"

            wape = conn.execute(
                """
                SELECT pam.wape
                FROM pyramide_accuracy_metrics pam
                JOIN pyramide_runs pr ON pr.run_id = pam.run_id
                WHERE pr.item_id = %s AND pam.horizon IS NULL
                """,
                (item_id,),
            ).fetchone()["wape"]
            expected = (Decimal("1") / (Decimal("1") + Decimal(str(wape)))).quantize(
                Decimal("0.0001")
            )
            assert Decimal(str(trace["components"]["accuracy"])) == expected
        finally:
            app.dependency_overrides.clear()
            _cleanup(conn, item_id, location_id)
