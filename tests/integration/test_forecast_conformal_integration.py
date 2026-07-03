"""
Integration tests for Pyramide axis D — PR-D2 (persisted conformal
intervals) against a real PostgreSQL database (no mocks).

Covered:
  - a leaf Pyramide run on a synthetic seed with KNOWN deterministic
    "noise" persists non-NULL confidence_interval_lower/upper for every
    bucket (migration 026 columns finally alive), with
    lower >= 0 and lower <= point <= upper;
  - empirical coverage of the persisted bounds on the seed's holdout
    window is plausible for the nominal 1 - alpha = 0.8;
  - a short series (not enough backtest residuals for the finite-sample
    guarantee) persists NULL bounds and says so in the run provenance
    (result.warnings) — never invented bounds;
  - a HIERARCHICAL run persists non-NULL bounds for the LEAVES (node
    backtest residuals transported by the disaggregation share) and NULL
    bounds for every AGGREGATE (interval reconciliation is frontier —
    documented V1 non-objective, spec §2.D);
  - GET /v1/demand/forecast/{forecast_id} exposes the persisted bounds
    without any contract change (fields were already Optional).

Conventions (mirrors test_pyramide_hierarchy_integration.py):
module-scoped migrated_db, autocommit _db_conn for seeding/teardown,
every test creates its OWN items / locations / demand facts (fresh
external ids) and cleans up in a finally block; the hierarchical test
uses the rollback-scoped ``conn`` fixture instead (mirrors
test_pyramide_reconcile_integration.py — the registry seed is heavier,
rollback keeps it self-cleaning). Anti-flake rule for the demand-history
reader (booked_date < server CURRENT_DATE): all seeded rows are at
TODAY - 2 or earlier.
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

# Deterministic "noise" around a base level of 100: amplitude ±9, zero
# randomness (ADR-003 discipline applies to the test fixture too). The
# pattern period (8) is coprime enough with the MA window (4) that the
# rolling backtest observes the full residual spread.
NOISE_PATTERN = (-8, 5, -3, 9, -6, 2, -7, 8)
BASE_LEVEL = 100
HOLDOUT = 14


# ---------------------------------------------------------------------------
# Helpers — DB access, seeding, teardown
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
        (item_id, f"{ext_id} conformal test item", ext_id),
    )
    return item_id


def _create_location(conn, ext_id: str) -> UUID:
    loc_id = uuid4()
    conn.execute(
        """
        INSERT INTO locations (location_id, name, location_type, country, external_id)
        VALUES (%s, %s, 'dc', 'US', %s)
        """,
        (loc_id, f"{ext_id} conformal test DC", ext_id),
    )
    return loc_id


def _seed_demand(conn, item_id: UUID, item_code: str, warehouse_id: str,
                 booked_date: date, qty) -> None:
    """One booking fact routed to the leaf reader: stream='regular',
    fulfillment standard, warehouse_id = the location's external_id (the
    read-time mapping of migration 047)."""
    conn.execute(
        """
        INSERT INTO demand_history (
            item_id, item_code, stream, booked_date, ordered_quantity,
            value_ext, counts_for_asp, org_id, fulfillment, order_number,
            warehouse_id
        ) VALUES (%s, %s, 'regular', %s, %s, 0, FALSE, 'PPS', 'standard',
                  'TEST-CONF', %s)
        """,
        (item_id, item_code, booked_date, qty, warehouse_id),
    )


def _seed_noisy_series(conn, item_id: UUID, item_code: str, warehouse_id: str,
                       days: int) -> list[Decimal]:
    """``days`` consecutive daily facts ending at TODAY - 2, quantity =
    BASE_LEVEL + NOISE_PATTERN cycling. Returns the quantities in
    date-ascending order (the exact series the reader must reproduce)."""
    quantities: list[Decimal] = []
    start = TODAY - timedelta(days=2 + days - 1)
    for i in range(days):
        qty = Decimal(BASE_LEVEL + NOISE_PATTERN[i % len(NOISE_PATTERN)])
        _seed_demand(conn, item_id, item_code, warehouse_id,
                     start + timedelta(days=i), qty)
        quantities.append(qty)
    return quantities


def _cleanup(conn, item_id: UUID, location_id: UUID) -> None:
    """FK order: pyramide bookkeeping first, then forecast tables (values
    cascade from forecasts), then facts, then master data."""
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
    conn.execute("DELETE FROM demand_history WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))


def _run_and_persist(conn, item_id: UUID, location_id: UUID,
                     history: list[Decimal], horizon_days: int,
                     method_params: dict | None = None):
    """Run the leaf runner on an in-memory series and persist the result.
    Returns (result, persisted)."""
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
        method_params={"window": 4, **(method_params or {})},
    )
    result = PyramideRunner().run(config, history)
    persisted = persist_run(conn, result)
    return result, persisted


def _fetch_bounds(conn, forecast_id: UUID):
    return conn.execute(
        """
        SELECT forecast_date, quantity,
               confidence_interval_lower AS lower,
               confidence_interval_upper AS upper
        FROM forecast_values
        WHERE forecast_id = %s
        ORDER BY forecast_date ASC
        """,
        (forecast_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_leaf_run_persists_conformal_bounds_with_plausible_coverage(migrated_db):
    """End-to-end on a synthetic seed with known deterministic noise:
    demand_history -> shared reader -> PyramideRunner (train split) ->
    persist_run -> forecast_values columns. The last HOLDOUT days of the
    seed are NOT given to the runner and serve as the coverage holdout."""
    from ootils_core.pyramide.repository import get_historical_demand

    with _db_conn(migrated_db) as conn:
        code = f"CONF-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            seeded = _seed_noisy_series(conn, item_id, code, f"DC-{code}", days=124)

            history = get_historical_demand(
                conn, item_id, location_id, lookback_days=200,
                scenario_id=BASELINE_SCENARIO_ID,
            )
            assert history == seeded  # the reader reproduces the seed

            train, holdout = history[:-HOLDOUT], history[-HOLDOUT:]
            result, persisted = _run_and_persist(
                conn, item_id, location_id, train, horizon_days=HOLDOUT,
            )
            # Long series: no conformal degradation expected.
            assert not [w for w in result.warnings if "conformal" in w]

            rows = _fetch_bounds(conn, persisted.forecast_id)
            assert len(rows) == HOLDOUT
            covered = 0
            for row, actual in zip(rows, holdout):
                lower = row["lower"]
                upper = row["upper"]
                point = Decimal(str(row["quantity"]))
                # Columns finally alive (APS review finding): non-NULL.
                assert lower is not None and upper is not None
                lower = Decimal(str(lower))
                upper = Decimal(str(upper))
                assert lower >= Decimal("0")
                assert lower <= point <= upper
                if lower <= actual <= upper:
                    covered += 1

            # Nominal coverage is 1 - alpha = 0.8; the seed's noise is
            # exchangeable-by-construction so the empirical coverage on
            # the holdout must be plausible (anti-flake band, not an
            # exact-quantile assertion).
            coverage = covered / HOLDOUT
            assert coverage >= 0.6, f"implausible coverage {coverage}"
        finally:
            _cleanup(conn, item_id, location_id)


def test_short_series_persists_null_bounds_with_provenance(migrated_db):
    """Not enough residuals for the finite-sample guarantee (default
    alpha 0.2 needs 9 per horizon): the run must persist NULL bounds and
    say so in its provenance — never invented bounds."""
    with _db_conn(migrated_db) as conn:
        code = f"CONF-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            seeded = _seed_noisy_series(conn, item_id, code, f"DC-{code}", days=5)
            result, persisted = _run_and_persist(
                conn, item_id, location_id, seeded, horizon_days=3,
            )

            assert any(
                "conformal" in w and "NULL" in w for w in result.warnings
            ), result.warnings

            rows = _fetch_bounds(conn, persisted.forecast_id)
            assert len(rows) == 3
            for row in rows:
                assert row["lower"] is None
                assert row["upper"] is None
        finally:
            _cleanup(conn, item_id, location_id)


def test_hierarchical_run_bounds_leaves_filled_aggregates_null(conn):
    """Hierarchical middle-out run on a seeded 1-family / 3-leaf block with
    a weekly seasonal pattern PLUS a deterministic 5-periodic perturbation
    (coprime with the 7-day season, so the SEASONAL base model backtests
    with non-zero residuals): every LEAF bucket gets non-NULL bounds
    (node residual offsets scaled by the leaf's disaggregation share)
    while every AGGREGATE keeps NULL bounds (interval reconciliation is
    frontier — V1 non-objective)."""
    from ootils_core.pyramide.hierarchy import (
        HierarchicalRunConfig,
        HierarchicalRunner,
    )

    h = "d2-h-conformal"
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

    # 56 consecutive days (8 full weeks) per leaf: amplitude x weekly
    # pattern + 5-periodic perturbation. Both cycles are deterministic
    # (ADR-003 discipline applies to fixtures); minimum quantity stays
    # strictly positive (4*1 - 2 = 2).
    week_pattern = (2, 1, 1, 1, 3, 4, 2)
    perturbation = (-2, 1, 0, 2, -1)  # period 5, coprime with 7
    amplitudes = {"A1": 10, "A2": 6, "A3": 4}
    items: dict[str, UUID] = {}
    for suffix, leaf_code in [("A1", prd1), ("A2", prd1), ("A3", prd2)]:
        item_id = uuid4()
        ext = f"D2-{h}-{suffix}"
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
                ) VALUES (%s, %s, 'regular', %s, %s, 0, FALSE, 'standard', 'D2')
                """,
                (item_id, ext, day, qty),
            )
    loc_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name, location_type, country, external_id) "
        "VALUES (%s, %s, 'dc', 'US', %s)",
        (loc_id, f"D2-{h} DC", f"D2-{h}-DC"),
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

    # 8 seasonal cycles -> 29+ backtest residuals per horizon at the node,
    # well above the alpha=0.2 requirement (9): no degradation warning.
    assert not [w for w in result.warnings if "conformal" in w], result.warnings

    leaf_keys = {str(i) for i in items.values()}
    any_strict = False
    for series in result.persisted:
        rows = _fetch_bounds(conn, series.forecast_id)
        assert len(rows) == 14
        if series.key in leaf_keys:
            assert series.kind == "leaf"
            for row in rows:
                assert row["lower"] is not None and row["upper"] is not None
                lower = Decimal(str(row["lower"]))
                upper = Decimal(str(row["upper"]))
                point = Decimal(str(row["quantity"]))
                assert lower >= Decimal("0")
                assert lower <= point <= upper
                if lower < upper:
                    any_strict = True
        else:
            # Aggregates: bounds stay NULL in V1 (interval reconciliation
            # across levels is a documented non-objective).
            assert series.kind == "aggregate"
            for row in rows:
                assert row["lower"] is None and row["upper"] is None
    # The 5-periodic perturbation guarantees non-zero residuals: the
    # intervals cannot all collapse onto the point forecast.
    assert any_strict


def test_get_forecast_endpoint_exposes_persisted_bounds(migrated_db):
    """GET /v1/demand/forecast/{forecast_id} already declared the interval
    fields (Optional, previously always null): with the columns now
    populated the endpoint must return them without contract change."""
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
        code = f"CONF-{uuid4().hex[:8].upper()}"
        item_id = _create_item(conn, code)
        location_id = _create_location(conn, f"DC-{code}")
        try:
            seeded = _seed_noisy_series(conn, item_id, code, f"DC-{code}", days=60)
            _, persisted = _run_and_persist(
                conn, item_id, location_id, seeded, horizon_days=5,
            )

            with TestClient(app) as client:
                response = client.get(
                    f"/v1/demand/forecast/{persisted.forecast_id}", headers=auth
                )
            assert response.status_code == 200
            payload = response.json()
            assert len(payload["values"]) == 5
            for value in payload["values"]:
                lower = value["confidence_interval_lower"]
                upper = value["confidence_interval_upper"]
                assert lower is not None and upper is not None
                assert Decimal("0") <= Decimal(lower)
                assert Decimal(lower) <= Decimal(value["quantity"]) <= Decimal(upper)
        finally:
            app.dependency_overrides.clear()
            _cleanup(conn, item_id, location_id)
