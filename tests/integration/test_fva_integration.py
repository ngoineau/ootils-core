"""
Integration tests for Pyramide axis A3 — Forecast Value Added persistence
(migration 068, #393 A3-PR3) against a real PostgreSQL database (no mocks).

Mirrors test_pyramide_accuracy_metrics_integration.py: a real leaf
PyramideRunner run seeds forecasts + pyramide_runs + pyramide_accuracy_metrics
via persist_run, and we assert the FVA columns behave to contract:

  - a LONG history persists naive_wape/naive_mase/fva_wape/fva_mase on the
    AGGREGATE row (horizon NULL) only; per-horizon rows keep them NULL (they
    carry no actuals, like every other non-residual metric of migration 055);
  - a run persisted WITHOUT history (history=None, the pre-FVA default) leaves
    all four columns NULL — non-regressive, the accuracy metrics unchanged;
  - re-persisting the SAME run (persist_accuracy_metrics is DELETE + INSERT)
    keeps the four values stable (idempotent);
  - GET /v1/forecast/runs/{run_id} round-trips the four fields None-honestly;
  - a SHORT history (< 1 season available at the stat's first cutoff) persists
    NULL FVA even though the run's own accuracy metrics are present — the
    honest "baseline not computable on insufficient history".

Anti-flake rule (inherited): the FVA path reads NO clock and NO demand_history
row (the series is passed to persist_run directly), so nothing here depends on
booked_date/ingested_at timing.
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

# Deterministic weekly pattern (ADR-003 discipline applies to fixtures too):
# a seasonal daily series so the seasonal-naive baseline is well-defined.
WEEK_PATTERN = (20, 22, 25, 30, 28, 15, 12)


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
        (item_id, f"{ext_id} fva test item", ext_id),
    )
    return item_id


def _create_location(conn, ext_id: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{ext_id} fva test DC", ext_id),
    )
    return loc_id


def _seasonal_series(days: int) -> list[Decimal]:
    # Weekly seasonal shape plus a slow trend so neither the naive nor the
    # stat is trivially perfect — the FVA is a real, non-degenerate number.
    return [
        Decimal(WEEK_PATTERN[i % 7] + i // 14)
        for i in range(days)
    ]


def _cleanup(conn, item_id: UUID, location_id: UUID) -> None:
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


def _run(conn, item_id: UUID, location_id: UUID, history: list[Decimal],
         horizon_days: int):
    from ootils_core.pyramide import PyramideRunConfig, PyramideRunner

    config = PyramideRunConfig(
        item_id=item_id,
        location_id=location_id,
        scenario_id=BASELINE_SCENARIO_ID,
        horizon_start=TODAY + timedelta(days=2),
        horizon_days=horizon_days,
        granularity="daily",  # season_length default = 7
        method="MA",
        method_params={"window": 4},
    )
    return PyramideRunner().run(config, history)


def _aggregate_row(conn, run_id: UUID) -> dict:
    return conn.execute(
        """
        SELECT horizon, wape, mase, naive_wape, naive_mase, fva_wape, fva_mase
        FROM pyramide_accuracy_metrics
        WHERE run_id = %s AND horizon IS NULL
        """,
        (run_id,),
    ).fetchone()


def _per_horizon_rows(conn, run_id: UUID) -> list[dict]:
    return conn.execute(
        """
        SELECT horizon, naive_wape, naive_mase, fva_wape, fva_mase
        FROM pyramide_accuracy_metrics
        WHERE run_id = %s AND horizon IS NOT NULL
        ORDER BY horizon
        """,
        (run_id,),
    ).fetchall()


def test_long_history_persists_fva_on_aggregate_row_only(migrated_db):
    """persist_run(..., history=<long series>) computes the seasonal-naive on
    the stat's own cutoffs and writes naive/fva on the aggregate row; the
    per-horizon rows keep the four columns NULL."""
    from ootils_core.pyramide.repository import fetch_accuracy_metrics, persist_run

    with _db_conn(migrated_db) as conn:
        code = f"FVA-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            history = _seasonal_series(60)
            result = _run(conn, item_id, location_id, history, horizon_days=7)
            assert result.accuracy_report is not None

            persisted = persist_run(conn, result, history=history)

            aggregate = _aggregate_row(conn, persisted.run_id)
            # season=7 default, min_train = 60 - 52 = 8 >= 7 => naive aligned.
            assert aggregate["naive_wape"] is not None
            assert aggregate["naive_mase"] is not None
            assert aggregate["fva_wape"] is not None
            assert aggregate["fva_mase"] is not None
            # FVA = naive - stat, read back exactly against the stat columns.
            assert aggregate["fva_wape"] == aggregate["naive_wape"] - aggregate["wape"]
            assert aggregate["fva_mase"] == aggregate["naive_mase"] - aggregate["mase"]

            for row in _per_horizon_rows(conn, persisted.run_id):
                assert row["naive_wape"] is None
                assert row["naive_mase"] is None
                assert row["fva_wape"] is None
                assert row["fva_mase"] is None

            # The typed reader surfaces the same values on the aggregate row.
            metrics = fetch_accuracy_metrics(conn, persisted.run_id)
            assert metrics[0].horizon is None
            assert metrics[0].fva_wape == aggregate["fva_wape"]
            assert metrics[0].naive_mase == aggregate["naive_mase"]
            assert all(m.fva_wape is None for m in metrics[1:])
        finally:
            _cleanup(conn, item_id, location_id)


def test_run_without_history_leaves_fva_null(migrated_db):
    """persist_run WITHOUT history (the pre-FVA default) is non-regressive:
    the accuracy metrics persist as before and all four FVA columns stay
    NULL — the honest 'baseline not computed', never a masked 0."""
    from ootils_core.pyramide.repository import persist_run

    with _db_conn(migrated_db) as conn:
        code = f"FVA-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            history = _seasonal_series(60)
            result = _run(conn, item_id, location_id, history, horizon_days=5)
            assert result.accuracy_report is not None

            persisted = persist_run(conn, result)  # history omitted

            aggregate = _aggregate_row(conn, persisted.run_id)
            # Accuracy metrics themselves are unchanged (stat wape present)...
            assert aggregate["wape"] is not None
            # ...but FVA is fully NULL (no baseline requested).
            assert aggregate["naive_wape"] is None
            assert aggregate["naive_mase"] is None
            assert aggregate["fva_wape"] is None
            assert aggregate["fva_mase"] is None
        finally:
            _cleanup(conn, item_id, location_id)


def test_short_history_persists_null_fva_but_keeps_accuracy(migrated_db):
    """A history shorter than one season at the stat's first cutoff: the run
    still backtests (accuracy metrics present) but the seasonal-naive has no
    value one season ago there, so FVA is NULL — insufficient-history honesty,
    distinct from a fabricated 0."""
    from ootils_core.pyramide.repository import persist_run

    with _db_conn(migrated_db) as conn:
        code = f"FVA-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            # 9 daily points, season 7 => min_train = 9 - n_cutoffs < 7.
            history = _seasonal_series(9)
            result = _run(conn, item_id, location_id, history, horizon_days=3)
            assert result.accuracy_report is not None  # MA still backtests

            persisted = persist_run(conn, result, history=history)

            aggregate = _aggregate_row(conn, persisted.run_id)
            assert aggregate["wape"] is not None  # run's own metrics intact
            assert aggregate["naive_wape"] is None
            assert aggregate["fva_wape"] is None
            assert aggregate["naive_mase"] is None
            assert aggregate["fva_mase"] is None
        finally:
            _cleanup(conn, item_id, location_id)


def test_re_persisting_run_keeps_fva_stable(migrated_db):
    """persist_accuracy_metrics is DELETE + INSERT: re-persisting the same run
    with the same fva rewrites identical FVA values (idempotent), never
    duplicating the aggregate row nor drifting the numbers."""
    from ootils_core.pyramide.fva import compute_fva, resolve_season_length
    from ootils_core.pyramide.repository import persist_accuracy_metrics, persist_run

    with _db_conn(migrated_db) as conn:
        code = f"FVA-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            history = _seasonal_series(60)
            result = _run(conn, item_id, location_id, history, horizon_days=5)
            persisted = persist_run(conn, result, history=history)

            first = _aggregate_row(conn, persisted.run_id)
            assert first["fva_wape"] is not None

            # Re-persist with an independently recomputed FvaResult on the same
            # run_id (same DELETE + INSERT path persist_run took).
            fva = compute_fva(
                history,
                resolve_season_length("daily", {"window": 4}),
                stat_report=result.accuracy_report,
            )
            persist_accuracy_metrics(
                conn, persisted.run_id, result.accuracy_report, fva=fva,
            )

            # Exactly one aggregate row survives, with identical values.
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM pyramide_accuracy_metrics "
                "WHERE run_id = %s AND horizon IS NULL",
                (persisted.run_id,),
            ).fetchone()["n"]
            assert count == 1

            second = _aggregate_row(conn, persisted.run_id)
            assert second["naive_wape"] == first["naive_wape"]
            assert second["naive_mase"] == first["naive_mase"]
            assert second["fva_wape"] == first["fva_wape"]
            assert second["fva_mase"] == first["fva_mase"]
        finally:
            _cleanup(conn, item_id, location_id)


def test_get_run_endpoint_exposes_fva_fields(migrated_db):
    """GET /v1/forecast/runs/{run_id}: the aggregate accuracy_metrics entry
    carries the four FVA fields (None-honest), a purely additive contract
    change."""
    os.environ["OOTILS_API_TOKEN"] = "integration-test-token"

    from fastapi.testclient import TestClient

    from ootils_core.api.app import create_app
    from ootils_core.api.dependencies import get_db
    from ootils_core.db.connection import OotilsDB
    from ootils_core.pyramide.repository import persist_run

    app = create_app()

    def override_db():
        db = OotilsDB(migrated_db)
        with db.conn() as c:
            yield c

    app.dependency_overrides[get_db] = override_db
    auth = {"Authorization": "Bearer integration-test-token"}

    with _db_conn(migrated_db) as conn:
        code = f"FVA-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            history = _seasonal_series(60)
            result = _run(conn, item_id, location_id, history, horizon_days=4)
            persisted = persist_run(conn, result, history=history)

            with TestClient(app) as client:
                response = client.get(
                    f"/v1/forecast/runs/{persisted.run_id}", headers=auth
                )
            assert response.status_code == 200
            metrics = response.json()["accuracy_metrics"]

            aggregate = metrics[0]
            assert aggregate["horizon"] is None
            assert aggregate["naive_wape"] is not None
            assert aggregate["naive_mase"] is not None
            assert aggregate["fva_wape"] is not None
            assert aggregate["fva_mase"] is not None

            # Per-horizon rows expose the four fields as null (None-honest).
            for row in metrics[1:]:
                assert row["naive_wape"] is None
                assert row["fva_wape"] is None
        finally:
            app.dependency_overrides.clear()
            _cleanup(conn, item_id, location_id)
