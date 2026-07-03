"""
Integration tests for the Pyramide FM weights seal (axis B, PR-B2,
migration 059) against a real PostgreSQL database (no mocks).

Light by design (the real Chronos inference runs only in the dedicated
CI job — marker 'foundation' in tests/test_pyramide_foundation.py).
Covered here:
  - persist_run of a NON-FM result leaves pyramide_runs.model_revision
    NULL ("scellé des poids FM — NULL pour les méthodes non-FM");
  - persist_run of a result carrying a model_revision round-trips it;
  - persist_series_run (hierarchical path) round-trips model_revision
    for both the aggregate and the default-NULL case.

Conventions mirror test_pyramide_routing_integration.py: module-scoped
migrated_db, rollback-scoped ``conn`` fixture.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest  # noqa: F401  (kept for fixture discovery parity)

from .conftest import requires_db

pytestmark = requires_db

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")
TODAY = date.today()
FAKE_SHA = "0123456789abcdef0123456789abcdef01234567"


def _create_item(conn, ext_id: str) -> UUID:
    item_id = uuid4()
    conn.execute(
        """
        INSERT INTO items (item_id, name, item_type, uom, status, external_id)
        VALUES (%s, %s, 'finished_good', 'EA', 'active', %s)
        """,
        (item_id, f"{ext_id} foundation test item", ext_id),
    )
    return item_id


def _create_location(conn, ext_id: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{ext_id} foundation test DC", ext_id),
    )
    return loc_id


def _run_result(item_id: UUID, location_id: UUID, **result_overrides):
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
    kwargs = dict(
        config=config,
        values=values,
        source_history_count=30,
        selected_model="MA(3)",
        engine_backend="internal",
    )
    kwargs.update(result_overrides)
    return PyramideRunResult(**kwargs)


def _fetch_model_revision(conn, run_id: UUID):
    return conn.execute(
        "SELECT model_revision FROM pyramide_runs WHERE run_id = %s",
        (run_id,),
    ).fetchone()["model_revision"]


class TestPersistRunModelRevision:
    def test_non_fm_run_keeps_null_model_revision(self, conn):
        from ootils_core.pyramide.repository import persist_run

        item_id = _create_item(conn, f"FND-{uuid4().hex[:8]}")
        location_id = _create_location(conn, f"FND-{uuid4().hex[:8]}")
        record = persist_run(conn, _run_result(item_id, location_id))

        assert _fetch_model_revision(conn, record.run_id) is None

    def test_fm_run_round_trips_the_weights_seal(self, conn):
        from ootils_core.pyramide.repository import persist_run

        item_id = _create_item(conn, f"FND-{uuid4().hex[:8]}")
        location_id = _create_location(conn, f"FND-{uuid4().hex[:8]}")
        record = persist_run(
            conn,
            _run_result(
                item_id,
                location_id,
                selected_model=f"FM_CHRONOS(amazon/chronos-2@{FAKE_SHA})",
                engine_backend="chronos:hf_commit_sha",
                model_revision=FAKE_SHA,
            ),
        )

        assert _fetch_model_revision(conn, record.run_id) == FAKE_SHA


class TestPersistSeriesRunModelRevision:
    def _series_kwargs(self, **overrides):
        horizon_start = TODAY + timedelta(days=1)
        kwargs = dict(
            scenario_id=BASELINE_SCENARIO_ID,
            horizon_start=horizon_start,
            horizon_end=horizon_start + timedelta(days=1),
            granularity="daily",
            method="FM_CHRONOS",
            model_strategy="fm",
            recon_method="middleout",
            random_seed=0,
            code_version="test",
            selected_model=f"FM_CHRONOS(amazon/chronos-2@{FAKE_SHA})",
            engine_backend="chronos:hf_commit_sha",
            source_history_count=30,
            bucket_dates=[horizon_start, horizon_start + timedelta(days=1)],
            quantities=[Decimal("5"), Decimal("5")],
            value_method="FM_CHRONOS",
        )
        kwargs.update(overrides)
        return kwargs

    def test_leaf_series_run_round_trips_model_revision(self, conn):
        from ootils_core.pyramide.repository import persist_series_run

        item_id = _create_item(conn, f"FND-{uuid4().hex[:8]}")
        location_id = _create_location(conn, f"FND-{uuid4().hex[:8]}")
        record = persist_series_run(
            conn,
            **self._series_kwargs(item_id=item_id, location_id=location_id),
            model_revision=FAKE_SHA,
        )
        assert _fetch_model_revision(conn, record.run_id) == FAKE_SHA

    def test_series_run_defaults_to_null_model_revision(self, conn):
        from ootils_core.pyramide.repository import persist_series_run

        item_id = _create_item(conn, f"FND-{uuid4().hex[:8]}")
        location_id = _create_location(conn, f"FND-{uuid4().hex[:8]}")
        record = persist_series_run(
            conn,
            **self._series_kwargs(
                item_id=item_id,
                location_id=location_id,
                method="MA",
                model_strategy="stat",
                selected_model="MA(3)",
                engine_backend="internal",
                value_method="MA",
            ),
        )
        assert _fetch_model_revision(conn, record.run_id) is None
