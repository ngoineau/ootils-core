"""
Integration tests for Pyramide axis D — PR-D3 (persisted backtest
accuracy metrics, migration 055) against a real PostgreSQL database
(no mocks).

Covered:
  - a leaf Pyramide run on a synthetic deterministic seed persists ONE
    aggregate row (horizon NULL, the report's pooled metrics) + one row
    PER HORIZON (bias + counts only — the other metrics need the
    actuals the report does not carry, so they stay NULL, None-honest);
  - re-persisting the SAME run_id replaces the row set (DELETE +
    INSERT, documented in persist_accuracy_metrics) instead of
    violating the UNIQUE NULLS NOT DISTINCT (run_id, horizon)
    constraint;
  - a hierarchical run persists metrics for every LEAF run (node-model
    report transported like the conformal bounds) and for the
    reconciliation-level aggregate node; S-summed non-backtested levels
    get none;
  - a run WITHOUT a backtest report (ENSEMBLE_STAT blend) persists
    ZERO metric rows — documented absence, never invented metrics;
  - GET /v1/forecast/runs/{run_id} exposes the rows in the optional
    accuracy_metrics field (aggregate first), backward-compatible.

Conventions mirror test_forecast_conformal_integration.py:
module-scoped migrated_db, autocommit _db_conn + finally-cleanup for
leaf tests, rollback-scoped ``conn`` fixture for the heavier
hierarchical seed. Anti-flake rule: all seeded demand facts are at
TODAY - 2 or earlier (booked_date < server CURRENT_DATE).
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from .conftest import requires_db

pytestmark = requires_db

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")
TODAY = date.today()

# Deterministic "noise" (ADR-003 discipline applies to fixtures too).
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
        (item_id, f"{ext_id} accuracy test item", ext_id),
    )
    return item_id


def _create_location(conn, ext_id: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{ext_id} accuracy test DC", ext_id),
    )
    return loc_id


def _synthetic_series(days: int) -> list[Decimal]:
    return [
        Decimal(BASE_LEVEL + NOISE_PATTERN[i % len(NOISE_PATTERN)])
        for i in range(days)
    ]


def _cleanup(conn, item_id: UUID, location_id: UUID) -> None:
    """FK order: pyramide bookkeeping first (accuracy metrics cascade
    from pyramide_runs), then forecast tables, then master data."""
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
    conn.execute("DELETE FROM forecasts WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _run_and_persist(conn, item_id: UUID, location_id: UUID,
                     history: list[Decimal], horizon_days: int,
                     method: str = "MA"):
    from ootils_core.pyramide import PyramideRunConfig, PyramideRunner
    from ootils_core.pyramide.repository import persist_run

    config = PyramideRunConfig(
        item_id=item_id,
        location_id=location_id,
        scenario_id=BASELINE_SCENARIO_ID,
        horizon_start=TODAY + timedelta(days=2),
        horizon_days=horizon_days,
        granularity="daily",
        method=method,
        method_params={"window": 4} if method == "MA" else {},
    )
    result = PyramideRunner().run(config, history)
    persisted = persist_run(conn, result)
    return result, persisted


def test_leaf_run_round_trips_aggregate_and_per_horizon_rows(migrated_db):
    """Mono-series run -> one aggregate row (report metrics verbatim) +
    one row per horizon (bias + counts only), read back in order
    (aggregate first) by fetch_accuracy_metrics."""
    from ootils_core.pyramide.repository import fetch_accuracy_metrics

    with _db_conn(migrated_db) as conn:
        code = f"ACC-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            history = _synthetic_series(60)
            horizon = 7
            result, persisted = _run_and_persist(
                conn, item_id, location_id, history, horizon_days=horizon,
            )
            report = result.accuracy_report
            assert report is not None  # MA on a long series backtests

            metrics = fetch_accuracy_metrics(conn, persisted.run_id)
            # Aggregate row first, then horizons 1..7 ascending.
            assert [m.horizon for m in metrics] == [None] + list(range(1, horizon + 1))

            aggregate = metrics[0]
            assert aggregate.mase == report.mase
            assert aggregate.wape == report.wape
            assert aggregate.smape == report.smape
            assert aggregate.bias == report.bias
            assert aggregate.coverage is None  # point-forecast backtest
            assert aggregate.n_cutoffs == report.n_cutoffs
            assert aggregate.n_observations == report.n_observations

            for row in metrics[1:]:
                residuals = report.per_horizon_residuals[row.horizon]
                expected_bias = -sum(residuals, Decimal(0)) / Decimal(len(residuals))
                assert row.bias == expected_bias  # sign contract: f - a
                # Residual-only rows: the actuals-dependent metrics are
                # NULL — persisted honesty, never invented values.
                assert row.mase is None and row.wape is None
                assert row.smape is None and row.coverage is None
                assert row.n_cutoffs == row.n_observations == len(residuals)
        finally:
            _cleanup(conn, item_id, location_id)


def test_re_persisting_same_run_replaces_rows_unique_respected(migrated_db):
    """persist_accuracy_metrics is DELETE + INSERT per run_id: calling it
    again for the same run must not violate the UNIQUE NULLS NOT
    DISTINCT (run_id, horizon) constraint (which also dedupes the
    horizon-NULL aggregate row) and must leave exactly one row set."""
    from ootils_core.pyramide.repository import (
        fetch_accuracy_metrics,
        persist_accuracy_metrics,
    )

    with _db_conn(migrated_db) as conn:
        code = f"ACC-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            result, persisted = _run_and_persist(
                conn, item_id, location_id, _synthetic_series(60), horizon_days=5,
            )
            first = fetch_accuracy_metrics(conn, persisted.run_id)
            assert len(first) == 6  # aggregate + 5 horizons

            written = persist_accuracy_metrics(
                conn, persisted.run_id, result.accuracy_report
            )
            assert written == 6
            second = fetch_accuracy_metrics(conn, persisted.run_id)
            assert len(second) == 6
            assert [m.horizon for m in second] == [m.horizon for m in first]
            assert [m.bias for m in second] == [m.bias for m in first]
        finally:
            _cleanup(conn, item_id, location_id)


def test_run_without_backtest_report_persists_zero_rows(migrated_db):
    """ENSEMBLE_STAT blends candidates: no single candidate's residuals
    describe the blend, so accuracy_report is None (engines contract)
    and the run must persist ZERO metric rows — absence is the honest
    provenance, never metrics borrowed from another model."""
    from ootils_core.pyramide.repository import fetch_accuracy_metrics

    with _db_conn(migrated_db) as conn:
        code = f"ACC-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            result, persisted = _run_and_persist(
                conn, item_id, location_id, _synthetic_series(60),
                horizon_days=5, method="ENSEMBLE_STAT",
            )
            assert result.accuracy_report is None
            assert fetch_accuracy_metrics(conn, persisted.run_id) == []
        finally:
            _cleanup(conn, item_id, location_id)


def test_hierarchical_run_persists_metrics_per_leaf_run(conn):
    """Hierarchical middle-out run (same seed shape as the conformal
    integration test): every LEAF run gets metric rows — the recon
    node's backtest report transported by the disaggregation share
    (scale-free metrics unchanged, bias scaled), same provenance as its
    conformal bounds. The reconciliation-level aggregate carries its own
    (unscaled) report."""
    from ootils_core.pyramide.hierarchy import (
        HierarchicalRunConfig,
        HierarchicalRunner,
    )
    from ootils_core.pyramide.repository import fetch_accuracy_metrics

    h = "d3-h-accuracy"
    fam, prd1, prd2 = f"FAM-{h}", f"PRD-{h}1", f"PRD-{h}2"
    conn.execute(
        """
        INSERT INTO hierarchy (hierarchy_id, domain, scope, levels, is_default)
        VALUES (%s, 'product', 'local', %s, FALSE)
        """,
        (h, ["family", "product"]),
    )
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

    week_pattern = (2, 1, 1, 1, 3, 4, 2)
    perturbation = (-2, 1, 0, 2, -1)  # period 5, coprime with 7
    amplitudes = {"A1": 10, "A2": 6, "A3": 4}
    items: dict[str, UUID] = {}
    for suffix, leaf_code in [("A1", prd1), ("A2", prd1), ("A3", prd2)]:
        item_id = uuid4()
        ext = f"D3-{h}-{suffix}"
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
        for i, back in enumerate(range(4, 60)):
            day = TODAY - timedelta(days=back)
            qty = (
                amplitudes[suffix] * week_pattern[day.weekday()]
                + perturbation[i % len(perturbation)]
            )
            conn.execute(
                """
                INSERT INTO demand_history (
                    item_id, item_code, stream, booked_date,
                    ordered_quantity, value_ext, counts_for_asp,
                    fulfillment, order_number
                ) VALUES (%s, %s, 'regular', %s, %s, 0, FALSE, 'standard', 'D3')
                """,
                (item_id, ext, day, qty),
            )
    loc_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name, location_type, country, external_id) "
        "VALUES (%s, %s, 'dc', 'US', %s)",
        (loc_id, f"D3-{h} DC", f"D3-{h}-DC"),
    )

    result = HierarchicalRunner().run(
        conn,
        HierarchicalRunConfig(
            hierarchy_id=h,
            block_code=fam,
            leaf_location_id=loc_id,
            scenario_id=BASELINE_SCENARIO_ID,
            horizon_start=TODAY + timedelta(days=2),
            horizon_days=14,
            granularity="daily",
            method="SEASONAL",
            method_params={"season_length": 7},
            lookback_days=90,
        ),
    )

    leaf_keys = {str(i) for i in items.values()}
    leaf_rows_seen = 0
    aggregate_with_metrics = 0
    for series in result.persisted:
        metrics = fetch_accuracy_metrics(conn, series.run_id)
        if series.key in leaf_keys:
            assert series.kind == "leaf"
            # Every leaf run carries metric rows: aggregate first, then
            # per-horizon rows with bias + counts only.
            assert metrics, f"leaf {series.key} has no accuracy metrics"
            assert metrics[0].horizon is None
            assert all(m.horizon >= 1 for m in metrics[1:])
            assert all(
                m.mase is None and m.wape is None and m.smape is None
                for m in metrics[1:]
            )
            leaf_rows_seen += 1
        elif metrics:
            # Only backtested aggregates (the reconciliation level) own
            # a report; S-summed levels persist nothing.
            assert metrics[0].horizon is None
            aggregate_with_metrics += 1
    assert leaf_rows_seen == 3
    assert aggregate_with_metrics >= 1


def test_get_run_endpoint_exposes_accuracy_metrics(migrated_db):
    """GET /v1/forecast/runs/{run_id}: the optional accuracy_metrics
    field carries the persisted rows (aggregate first) — additive,
    backward-compatible contract change."""
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

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
    auth = {"Authorization": "Bearer integration-test-token"}

    with _db_conn(migrated_db) as conn:
        code = f"ACC-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            _, persisted = _run_and_persist(
                conn, item_id, location_id, _synthetic_series(60), horizon_days=4,
            )
            with TestClient(app) as client:
                response = client.get(
                    f"/v1/forecast/runs/{persisted.run_id}", headers=auth
                )
            assert response.status_code == 200
            payload = response.json()
            metrics = payload["accuracy_metrics"]
            assert [m["horizon"] for m in metrics] == [None, 1, 2, 3, 4]
            aggregate = metrics[0]
            assert aggregate["wape"] is not None
            assert aggregate["coverage"] is None
            assert aggregate["n_cutoffs"] >= 1
            for row in metrics[1:]:
                assert row["bias"] is not None
                assert row["mase"] is None
        finally:
            app.dependency_overrides.clear()
            _cleanup(conn, item_id, location_id)
