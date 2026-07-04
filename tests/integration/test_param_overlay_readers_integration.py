"""
tests/integration/test_param_overlay_readers_integration.py — DB-backed
coverage for chantier #347 PR2: the four MRP batch readers wired onto
ootils_core.engine.scenario.param_overlay.resolved_params_sql() (ADR-025).

Scope: this file asserts the READ side only — that each reader (a) resolves
byte-identically to the pre-overlay behaviour on baseline (scenario_id=None),
(b) sees a scenario-scoped override once one is set, and, for the loader
(core A pooling reader), (c) that a location-scoped override applies BEFORE
the SUM/MAX pooling across locations, not after.

Reuses the seed helpers from test_param_overlay_integration.py (fresh off
PR1) rather than re-inventing item/location/scenario/planning-params seeding
— see that file's docstring for the fixture-style rationale.
"""
from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from ootils_core.engine.mrp.forecast_consumer import ForecastConsumer
from ootils_core.engine.mrp.loader import load_planning_data
from ootils_core.engine.mrp.lot_sizing import LotSizingEngine
from ootils_core.engine.mrp.mrp_apics_engine import MrpApicsEngine
from ootils_core.engine.scenario.param_overlay import set_param_override

from .conftest import requires_db
from .test_param_overlay_integration import (
    _seed_item,
    _seed_item_loc_params,
    _seed_location,
    _seed_planning_params,
    _seed_scenario,
)

pytestmark = requires_db

BASELINE = "00000000-0000-0000-0000-000000000001"


# ---------------------------------------------------------------------------
# 1. loader.py — load_planning_data (core A, pooled item-level aggregates)
# ---------------------------------------------------------------------------


def test_loader_baseline_unchanged_with_no_scenario(conn):
    """scenario=BASELINE (the default) must resolve exactly like the
    pre-overlay code path: no override exists, so the LEFT JOIN LATERAL
    degenerates to nothing and the pooled aggregate is the raw base value."""
    item_id, location_id = _seed_item_loc_params(conn, safety_stock_qty=10)
    conn.commit()

    d = load_planning_data(conn, horizon_days=90, scenario=BASELINE)

    assert d.safety.get(item_id) == 10
    assert d.make_lt.get(item_id) == 14  # lead_time_sourcing_days default


def test_loader_scenario_sees_override(conn):
    """A safety_stock_qty override set on a fork is visible to a
    scenario-scoped load_planning_data() call, baseline is untouched."""
    item_id, location_id = _seed_item_loc_params(conn, safety_stock_qty=10)
    scenario_id = _seed_scenario(conn)
    conn.commit()

    set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "20", "watcher-test",
    )
    conn.commit()

    d_scenario = load_planning_data(conn, horizon_days=90, scenario=str(scenario_id))
    d_baseline = load_planning_data(conn, horizon_days=90, scenario=BASELINE)

    assert d_scenario.safety.get(item_id) == Decimal("20")
    assert d_baseline.safety.get(item_id) == 10


def test_loader_pooling_applies_override_before_sum_across_locations(conn):
    """Core-A pooling case: two locations for the same item, an override on
    only ONE location. The override must be resolved per-row BEFORE the
    SUM(safety_stock_qty) pooling, not after — so the pooled total reflects
    the overridden value for that one location plus the untouched base value
    for the other."""
    item_id = _seed_item(conn)
    loc_a = _seed_location(conn, "readers-loc-a")
    loc_b = _seed_location(conn, "readers-loc-b")
    _seed_planning_params(conn, item_id, loc_a, safety_stock_qty=10)
    _seed_planning_params(conn, item_id, loc_b, safety_stock_qty=10)
    scenario_id = _seed_scenario(conn)
    conn.commit()

    # Override loc_a only: 10 -> 25. loc_b stays base (10).
    set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "25", "watcher-test",
        location_id=loc_a,
    )
    conn.commit()

    d_scenario = load_planning_data(conn, horizon_days=90, scenario=str(scenario_id))
    d_baseline = load_planning_data(conn, horizon_days=90, scenario=BASELINE)

    assert d_scenario.safety.get(item_id) == Decimal("35")  # 25 + 10
    assert d_baseline.safety.get(item_id) == 20  # 10 + 10, unaffected


def test_loader_pooling_lead_time_max_reflects_override_on_one_location(conn):
    """Same pooling invariant, MAX aggregate: an override raising
    lead_time_sourcing_days on one of two locations must win the MAX once
    resolved, matching the "override applies before pooling" contract."""
    item_id = _seed_item(conn)
    loc_a = _seed_location(conn, "readers-lt-loc-a")
    loc_b = _seed_location(conn, "readers-lt-loc-b")
    _seed_planning_params(
        conn, item_id, loc_a,
        lead_time_sourcing_days=5, lead_time_manufacturing_days=0, lead_time_transit_days=0,
    )
    _seed_planning_params(
        conn, item_id, loc_b,
        lead_time_sourcing_days=5, lead_time_manufacturing_days=0, lead_time_transit_days=0,
    )
    scenario_id = _seed_scenario(conn)
    conn.commit()

    set_param_override(
        conn, scenario_id, item_id, "lead_time_sourcing_days", "40", "watcher-test",
        location_id=loc_b,
    )
    conn.commit()

    d_scenario = load_planning_data(conn, horizon_days=90, scenario=str(scenario_id))
    d_baseline = load_planning_data(conn, horizon_days=90, scenario=BASELINE)

    assert d_scenario.make_lt.get(item_id) == 40
    assert d_baseline.make_lt.get(item_id) == 5


def test_loader_lead_time_null_component_matches_generated_column(conn):
    """Baseline-parity regression (#347 PR2): the base lead_time_total_days
    is a GENERATED column COALESCE(s,0)+COALESCE(m,0)+COALESCE(t,0) — never
    NULL. The loader recomputes the total from the resolved components and
    MUST apply the same per-component COALESCE. Real ERP rows routinely leave
    components NULL (make items carry only manufacturing, buy items only
    sourcing); a bare `s + m + t` would NULL-propagate the whole sum and
    silently fall back to DEFAULT_LT_DAYS. Seed sourcing=30, manufacturing
    and transit NULL -> make_lt must be 30, not None."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn, "readers-loader-lt-null")
    _seed_planning_params(
        conn, item_id, location_id,
        lead_time_sourcing_days=30,
        lead_time_manufacturing_days=None,
        lead_time_transit_days=None,
    )
    conn.commit()

    d = load_planning_data(conn, horizon_days=90, scenario=BASELINE)

    assert d.make_lt.get(item_id) == 30


# ---------------------------------------------------------------------------
# 2. mrp_apics_engine.py — MrpApicsEngine._batch_load_planning_params
# ---------------------------------------------------------------------------


def test_batch_load_lead_time_null_component_matches_generated_column(conn):
    """Same NULL-component baseline-parity regression for the APICS reader's
    lead_time_total_days recompute (#347 PR2)."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn, "readers-apics-lt-null")
    _seed_planning_params(
        conn, item_id, location_id,
        lead_time_sourcing_days=30,
        lead_time_manufacturing_days=None,
        lead_time_transit_days=None,
    )
    conn.commit()

    engine = MrpApicsEngine(conn)
    result = engine._batch_load_planning_params(
        {item_id}, location_id, scenario_id=None
    )

    assert result[item_id]["lead_time_total_days"] == 30


def test_batch_load_planning_params_baseline_unchanged(conn):
    item_id, location_id = _seed_item_loc_params(conn, safety_stock_qty=10)
    conn.commit()

    engine = MrpApicsEngine(conn)
    result = engine._batch_load_planning_params({item_id}, location_id, scenario_id=None)

    assert result[item_id]["safety_stock_qty"] == 10


def test_batch_load_planning_params_sees_scenario_override(conn):
    item_id, location_id = _seed_item_loc_params(conn, safety_stock_qty=10)
    scenario_id = _seed_scenario(conn)
    conn.commit()

    set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "99", "watcher-test",
    )
    conn.commit()

    engine = MrpApicsEngine(conn)
    resolved = engine._batch_load_planning_params(
        {item_id}, location_id, scenario_id=scenario_id
    )
    baseline = engine._batch_load_planning_params(
        {item_id}, location_id, scenario_id=None
    )

    assert resolved[item_id]["safety_stock_qty"] == Decimal("99")
    assert baseline[item_id]["safety_stock_qty"] == 10


def test_batch_load_does_not_apply_legacy_order_multiple_fallback(conn):
    """Baseline-parity regression (#347 PR2): the APICS reader selects
    order_multiple_qty RAW, exactly as the pre-PR2 query did. It must NOT
    fall back to the legacy `order_multiple` column when order_multiple_qty
    is NULL — doing so would silently start rounding planned orders on
    baseline for items whose modern column is unset. The legacy cross-column
    fallback lives ONLY in LotSizingEngine.get_planning_params (where it
    predates #347); the two MRP engines' column choice diverges by design."""
    item_id, location_id = _seed_item_loc_params(
        conn, order_multiple_qty=None,
    )
    conn.execute(
        "UPDATE item_planning_params SET order_multiple = 6 WHERE item_id = %s",
        (item_id,),
    )
    scenario_id = _seed_scenario(conn)
    conn.commit()

    set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "50", "watcher-test",
    )
    conn.commit()

    engine = MrpApicsEngine(conn)
    resolved = engine._batch_load_planning_params(
        {item_id}, location_id, scenario_id=scenario_id
    )

    # No legacy fallback: order_multiple_qty stays NULL even though the legacy
    # `order_multiple` column holds 6. The unrelated overlay still applies.
    assert resolved[item_id]["order_multiple_qty"] is None
    assert resolved[item_id]["safety_stock_qty"] == Decimal("50")


# ---------------------------------------------------------------------------
# 3. lot_sizing.py — LotSizingEngine.get_planning_params
# ---------------------------------------------------------------------------


def test_lot_sizing_get_planning_params_baseline_unchanged(conn):
    item_id, location_id = _seed_item_loc_params(conn, safety_stock_qty=10)
    conn.commit()

    engine = LotSizingEngine(conn)
    params = engine.get_planning_params(item_id, location_id, scenario_id=None)

    assert params["safety_stock_qty"] == 10


def test_lot_sizing_keeps_legacy_order_multiple_fallback(conn):
    """Intentional-divergence lock (#347 PR2): unlike the APICS reader,
    LotSizingEngine.get_planning_params DOES apply the legacy
    COALESCE(order_multiple_qty, order_multiple) fallback — because its
    pre-#347 query already did. This test pins that asymmetry so a future
    "harmonisation" doesn't silently flip either engine's baseline."""
    item_id, location_id = _seed_item_loc_params(conn, order_multiple_qty=None)
    conn.execute(
        "UPDATE item_planning_params SET order_multiple = 6 WHERE item_id = %s",
        (item_id,),
    )
    conn.commit()

    engine = LotSizingEngine(conn)
    params = engine.get_planning_params(item_id, location_id, scenario_id=None)

    assert params["order_multiple_qty"] == Decimal("6")


def test_lot_sizing_get_planning_params_sees_scenario_override(conn):
    item_id, location_id = _seed_item_loc_params(conn, min_order_qty=None)
    scenario_id = _seed_scenario(conn)
    conn.commit()

    set_param_override(
        conn, scenario_id, item_id, "min_order_qty", "15", "watcher-test",
    )
    conn.commit()

    engine = LotSizingEngine(conn)
    resolved = engine.get_planning_params(item_id, location_id, scenario_id=scenario_id)
    baseline = engine.get_planning_params(item_id, location_id, scenario_id=None)

    assert resolved["min_order_qty"] == Decimal("15")
    assert baseline["min_order_qty"] is None


# ---------------------------------------------------------------------------
# 4. forecast_consumer.py — ForecastConsumer._get_consumption_params
# ---------------------------------------------------------------------------


def test_forecast_consumer_baseline_unchanged(conn):
    item_id, location_id = _seed_item_loc_params(
        conn, forecast_consumption_strategy="max_only", consumption_window_days=7,
    )
    conn.commit()

    consumer = ForecastConsumer(conn, uuid4())  # non-baseline UUID irrelevant here
    consumer.scenario_id = None
    params = consumer._get_consumption_params(item_id, location_id)

    assert params["window_days"] == 7


def test_forecast_consumer_sees_scenario_override(conn):
    item_id, location_id = _seed_item_loc_params(
        conn, forecast_consumption_strategy="max_only", consumption_window_days=7,
    )
    scenario_id = _seed_scenario(conn)
    conn.commit()

    set_param_override(
        conn, scenario_id, item_id, "consumption_window_days", "21", "watcher-test",
    )
    conn.commit()

    consumer = ForecastConsumer(conn, scenario_id)
    resolved = consumer._get_consumption_params(item_id, location_id)

    consumer_baseline = ForecastConsumer(conn, uuid4())
    consumer_baseline.scenario_id = None
    baseline = consumer_baseline._get_consumption_params(item_id, location_id)

    assert resolved["window_days"] == 21
    assert baseline["window_days"] == 7
