"""
tests/integration/test_consumption_window_integration.py — DB-backed coverage
for the forecast-consumption WINDOW (#349) end to end through
load_planning_data + the scenario planning-param overlay (#347, ADR-025).

Scope:
  1. The loader reads `consumption_window_days` (DB days) and populates
     PlanningData.consume_window as WEEKLY BUCKETS (days -> round(days/7)),
     scenario-resolved: baseline sees the base default, a fork that overrides
     consumption_window_days sees the overridden window.
  2. On an Early-Buy displaced case (firm booking several buckets before its
     forecast), the fork's larger window (35 days => 5 buckets) nets the pair
     once while the baseline window (7 days => 1 bucket) is too small to bridge
     the gap and double-counts. The displacement is deliberately > 1 bucket so
     the baseline window genuinely cannot reach and the difference is driven
     purely by the overlaid window.

Reuses the seed helpers from test_param_overlay_integration.py (same
item/location/scenario/planning-params seeding used across the #347 reader
tests). Dates are anchored on the DB CURRENT_DATE, never Python now().
"""
from __future__ import annotations

import datetime as _dt
from uuid import uuid4

from ootils_core.engine.mrp.loader import load_planning_data
from ootils_core.engine.scenario.param_overlay import set_param_override

from .conftest import requires_db
from .test_param_overlay_integration import (
    _seed_item,
    _seed_location,
    _seed_planning_params,
    _seed_scenario,
)

pytestmark = requires_db

BASELINE = "00000000-0000-0000-0000-000000000001"


def _seed_node(conn, node_type, item_id, location_id, qty, tref):
    conn.execute(
        "INSERT INTO nodes (node_type, scenario_id, item_id, location_id, "
        " quantity, time_grain, time_ref, active) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)",
        (node_type, BASELINE, item_id, location_id, qty, "exact_date", tref),
    )


def test_loader_populates_consume_window_from_days(conn):
    """The loader converts the base consumption_window_days (7) to weekly
    buckets: round(7/7) == 1."""
    item_id, location_id = _seed_item_loc_params_window(conn, consumption_window_days=7)
    conn.commit()

    d = load_planning_data(conn, horizon_days=180, scenario=BASELINE)

    assert d.consume_window.get(item_id) == 1


def test_loader_consume_window_rounds_days_to_buckets(conn):
    """35 days => round(35/7) == 5 weekly buckets."""
    item_id, location_id = _seed_item_loc_params_window(conn, consumption_window_days=35)
    conn.commit()

    d = load_planning_data(conn, horizon_days=180, scenario=BASELINE)

    assert d.consume_window.get(item_id) == 5


def test_fork_override_widens_window_and_nets_early_buy(conn):
    """End-to-end #349 x #347: a fork overriding consumption_window_days to 35
    (=> 5 buckets) nets an Early-Buy booking against its forecast that sits
    several buckets away, while the baseline (base window 7 days => 1 bucket)
    cannot bridge the gap and double-counts.

    Layout (anchored on CURRENT_DATE): a firm booking of 100 at day 7 (bucket 1)
    and a forecast of 120 at day 35 (bucket 5) — 4 buckets apart. The baseline
    1-bucket window cannot reach; the fork's 5-bucket window does.
    """
    item_id = _seed_item(conn)
    location_id = _seed_location(conn, "cw-early-buy-loc")
    # Base window 7 days => 1 bucket (too small for the 4-bucket displacement).
    _seed_planning_params(
        conn, item_id, location_id,
        forecast_consumption_strategy="max_only", consumption_window_days=7,
        safety_stock_qty=0,
    )
    scenario_id = _seed_scenario(conn)
    today = conn.execute("SELECT CURRENT_DATE AS d").fetchone()["d"]

    _seed_node(conn, "CustomerOrderDemand", item_id, location_id, 100,
               today + _dt.timedelta(days=7))
    _seed_node(conn, "ForecastDemand", item_id, location_id, 120,
               today + _dt.timedelta(days=35))
    conn.commit()

    # Fork override: window 35 days => 5 buckets, enough to bridge the gap.
    set_param_override(
        conn, scenario_id, item_id, "consumption_window_days", "35", "test-349",
    )
    conn.commit()

    d_base = load_planning_data(conn, horizon_days=180, scenario=BASELINE)
    d_fork = load_planning_data(conn, horizon_days=180, scenario=str(scenario_id))

    assert d_base.consume_window.get(item_id) == 1
    assert d_fork.consume_window.get(item_id) == 5

    from ootils_core.engine.mrp.core import consume_demand

    g_base = consume_demand(d_base)
    g_fork = consume_demand(d_fork)

    base_total = sum(g_base.get(item_id, {}).values())
    fork_total = sum(g_fork.get(item_id, {}).values())

    # Baseline: window too small to net -> booking 100 + forecast 120 = 220.
    assert base_total == 220.0
    # Fork: booking consumes the neighbouring forecast -> netted to 120.
    assert fork_total == 120.0
    # The whole point of #349: the fork's net demand DIFFERS from baseline on an
    # Early-Buy displaced case, driven purely by the overlaid window.
    assert fork_total < base_total


def _seed_item_loc_params_window(conn, **params):
    item_id = _seed_item(conn)
    location_id = _seed_location(conn, f"cw-{uuid4().hex[:8]}")
    _seed_planning_params(conn, item_id, location_id, **params)
    return item_id, location_id
