"""
Integration tests for Pyramide routing provenance (axis B, PR-B1,
migration 058) against a real PostgreSQL database (no mocks).

Light by design (full routed execution is B2). Covered:
  - persist_run with a RoutingDecision round-trips routed_method /
    routed_level / routing_reason on pyramide_runs;
  - persist_run WITHOUT routing (the default, historical behaviour)
    leaves the three columns NULL — "run not routed, method requested
    explicitly";
  - persist_series_run (leaf + aggregate paths of a hierarchical run)
    accepts and persists the decision the HierarchicalRunner propagates;
  - the all-or-nothing CHECK rejects a partial provenance row;
  - the routed_level CHECK rejects values outside ('leaf','aggregate').

Conventions mirror test_pyramide_accuracy_metrics_integration.py:
module-scoped migrated_db, rollback-scoped ``conn`` fixture.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from .conftest import requires_db

pytestmark = requires_db

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")
TODAY = date.today()


def _create_item(conn, ext_id: str) -> UUID:
    item_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, name, item_type, uom, status, external_id)
        VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)
        """,
        (item_id, f"{ext_id} routing test item", ext_id),
    )
    return item_id


def _create_location(conn, ext_id: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{ext_id} routing test DC", ext_id),
    )
    return loc_id


def _run_result(item_id: UUID, location_id: UUID):
    from ootils_core.pyramide.models import (
        PyramideRunConfig,
        PyramideRunResult,
        PyramideValue,
    )

    horizon_start = TODAY + timedelta(days=1)
    config = PyramideRunConfig(
        item_id=item_id,
        location_id=location_id,
        scenario_id=BASELINE_SCENARIO_ID,
        horizon_start=horizon_start,
        horizon_days=2,
        granularity="daily",
        method="MA",
    )
    values = tuple(
        PyramideValue(
            bucket_index=i,
            forecast_date=horizon_start + timedelta(days=i),
            quantity=Decimal("10"),
            method="MA",
        )
        for i in range(2)
    )
    return PyramideRunResult(
        config=config,
        values=values,
        source_history_count=30,
        selected_model="MA(3)",
        engine_backend="internal",
    )


def _fetch_routing_row(conn, run_id: UUID):
    return conn.execute(
        """
        SELECT routed_method, routed_level, routing_reason
        FROM pyramide_runs WHERE run_id = %s
        """,
        (run_id,),
    ).fetchone()


def _tail_decision():
    from ootils_core.pyramide.routing import RoutingDecision

    return RoutingDecision(
        method="AUTO_SELECT",
        level="aggregate",
        reason=(
            "sparse C-class series (zero_ratio 0.84 > 0.6): forecast at "
            "aggregate + MinT disaggregation"
        ),
        features_used={"series_class": "tail"},
    )


class TestPersistRunRoutingRoundTrip:
    def test_routed_run_round_trips_the_three_columns(self, conn):
        from ootils_core.pyramide.repository import persist_run

        item_id = _create_item(conn, f"RTG-{uuid4().hex[:8]}")
        location_id = _create_location(conn, f"RTG-{uuid4().hex[:8]}")
        decision = _tail_decision()
        record = persist_run(conn, _run_result(item_id, location_id), routing=decision)

        row = _fetch_routing_row(conn, record.run_id)
        assert row["routed_method"] == "AUTO_SELECT"
        assert row["routed_level"] == "aggregate"
        assert row["routing_reason"] == decision.reason

    def test_unrouted_run_keeps_null_columns(self, conn):
        from ootils_core.pyramide.repository import persist_run

        item_id = _create_item(conn, f"RTG-{uuid4().hex[:8]}")
        location_id = _create_location(conn, f"RTG-{uuid4().hex[:8]}")
        record = persist_run(conn, _run_result(item_id, location_id))

        row = _fetch_routing_row(conn, record.run_id)
        assert row["routed_method"] is None
        assert row["routed_level"] is None
        assert row["routing_reason"] is None


class TestPersistSeriesRunRouting:
    def _series_kwargs(self, **overrides):
        horizon_start = TODAY + timedelta(days=1)
        kwargs = dict(
            scenario_id=BASELINE_SCENARIO_ID,
            horizon_start=horizon_start,
            horizon_end=horizon_start + timedelta(days=1),
            granularity="daily",
            method="MA",
            model_strategy="stat",
            recon_method="none",
            random_seed=0,
            code_version="test",
            selected_model="MA(3)",
            engine_backend="internal",
            source_history_count=30,
            bucket_dates=[horizon_start, horizon_start + timedelta(days=1)],
            quantities=[Decimal("5"), Decimal("5")],
            value_method="MA",
        )
        kwargs.update(overrides)
        return kwargs

    def test_leaf_series_run_persists_routing(self, conn):
        from ootils_core.pyramide.repository import persist_series_run
        from ootils_core.pyramide.routing import RoutingDecision

        item_id = _create_item(conn, f"RTG-{uuid4().hex[:8]}")
        location_id = _create_location(conn, f"RTG-{uuid4().hex[:8]}")
        decision = RoutingDecision(
            method="CROSTON",
            level="leaf",
            reason="intermittent demand (zero_ratio 0.7 > 0.6): Croston at leaf",
        )
        record = persist_series_run(
            conn,
            **self._series_kwargs(item_id=item_id, location_id=location_id),
            routing=decision,
        )
        row = _fetch_routing_row(conn, record.run_id)
        assert row["routed_method"] == "CROSTON"
        assert row["routed_level"] == "leaf"
        assert row["routing_reason"] == decision.reason

    def test_leaf_series_run_without_routing_keeps_null(self, conn):
        from ootils_core.pyramide.repository import persist_series_run

        item_id = _create_item(conn, f"RTG-{uuid4().hex[:8]}")
        location_id = _create_location(conn, f"RTG-{uuid4().hex[:8]}")
        record = persist_series_run(
            conn, **self._series_kwargs(item_id=item_id, location_id=location_id)
        )
        row = _fetch_routing_row(conn, record.run_id)
        assert row["routed_method"] is None
        assert row["routed_level"] is None
        assert row["routing_reason"] is None


def _seed_forecast(conn, item_id: UUID, location_id: UUID) -> UUID:
    forecast_id = uuid4()
    horizon_start = TODAY + timedelta(days=1)
    conn.execute(
        """
        INSERT INTO forecasts (
            forecast_id, item_id, location_id, scenario_id,
            horizon_start, horizon_end, granularity, method
        ) VALUES (%s, %s, %s, %s, %s, %s, 'daily', 'MA')
        """,
        (
            forecast_id, item_id, location_id, BASELINE_SCENARIO_ID,
            horizon_start, horizon_start + timedelta(days=1),
        ),
    )
    return forecast_id


class TestRoutingChecks:
    def _insert_run(self, conn, routing_columns: str, routing_values: str):
        item_id = _create_item(conn, f"RTG-{uuid4().hex[:8]}")
        location_id = _create_location(conn, f"RTG-{uuid4().hex[:8]}")
        forecast_id = _seed_forecast(conn, item_id, location_id)
        horizon_start = TODAY + timedelta(days=1)
        conn.execute(
            f"""
            INSERT INTO pyramide_runs (
                run_id, forecast_id, item_id, location_id, scenario_id,
                horizon_start, horizon_end, granularity, method,
                model_strategy, recon_method, random_seed, code_version,
                selected_model, engine_backend, source_history_count,
                status, deterministic_artifact, {routing_columns}
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'daily', 'MA',
                      'stat', 'none', 0, 'test', 'MA(3)', 'internal', 0,
                      'generated', 'forecast_values', {routing_values})
            """,
            (
                uuid4(), forecast_id, item_id, location_id,
                BASELINE_SCENARIO_ID,
                horizon_start, horizon_start + timedelta(days=1),
            ),
        )

    def test_partial_provenance_is_rejected(self, conn):
        # routed_method without level/reason violates the all-or-nothing
        # CHECK: a partial routing provenance would be unauditable.
        import psycopg

        with pytest.raises(psycopg.errors.CheckViolation):
            self._insert_run(conn, "routed_method", "'CROSTON'")
        conn.rollback()

    def test_bad_routed_level_is_rejected(self, conn):
        import psycopg

        with pytest.raises(psycopg.errors.CheckViolation):
            self._insert_run(
                conn,
                "routed_method, routed_level, routing_reason",
                "'MA', 'middle', 'bad level'",
            )
        conn.rollback()
