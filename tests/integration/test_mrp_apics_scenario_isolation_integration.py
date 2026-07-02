"""
tests/integration/test_mrp_apics_scenario_isolation_integration.py — Issue #333.

The APICS gross-to-net calculator used to hardcode the baseline scenario UUID
inside _get_initial_on_hand, so an MRP run inside a scenario fork silently
netted against *baseline* on-hand — a fork with diverged stock produced a
wrong (baseline-shaped) plan.

These tests fork the baseline via ScenarioManager, diverge the fork's copied
OnHandSupply node, and verify that netting reads the fork's stock, not the
baseline's — and that baseline runs are unchanged.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

from .conftest import requires_db

BASELINE_ID = UUID("00000000-0000-0000-0000-000000000001")

BASELINE_ON_HAND = Decimal("100")
FORK_ON_HAND = Decimal("5")


def _setup_fork_with_divergent_on_hand(conn):
    """
    Seed one item/location with baseline on-hand = 100, fork the baseline,
    then set the fork's copied OnHandSupply quantity to 5.

    Returns (item_id, location_id, fork_scenario_id).
    """
    from ootils_core.engine.scenario.manager import ScenarioManager

    item_id = uuid4()
    location_id = uuid4()

    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, 'mrp-iso-test-item')",
        (item_id,),
    )
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, 'mrp-iso-test-loc')",
        (location_id,),
    )
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            quantity, time_grain, time_ref, active
        ) VALUES (%s, 'OnHandSupply', %s, %s, %s, %s, 'exact_date', CURRENT_DATE, TRUE)
        """,
        (uuid4(), BASELINE_ID, item_id, location_id, BASELINE_ON_HAND),
    )

    fork = ScenarioManager().create_scenario(
        "mrp-apics-isolation-fork", BASELINE_ID, conn
    )

    # Diverge the fork: the deep-copied OnHandSupply drops from 100 to 5.
    result = conn.execute(
        """
        UPDATE nodes
        SET quantity = %s, updated_at = NOW()
        WHERE scenario_id = %s
          AND node_type = 'OnHandSupply'
          AND item_id = %s
          AND location_id = %s
          AND active = TRUE
        """,
        (FORK_ON_HAND, fork.scenario_id, item_id, location_id),
    )
    assert result.rowcount == 1, "fork should contain exactly one copied OnHandSupply node"

    return item_id, location_id, fork.scenario_id


@requires_db
def test_initial_on_hand_reads_fork_not_baseline(conn):
    """_get_initial_on_hand scoped to a fork returns the fork's on-hand."""
    from ootils_core.engine.mrp import GrossToNetCalculator

    item_id, location_id, fork_id = _setup_fork_with_divergent_on_hand(conn)

    fork_calc = GrossToNetCalculator(conn, fork_id)
    assert fork_calc._get_initial_on_hand(item_id, location_id) == FORK_ON_HAND
    # Location-less variant hits the second query branch — same scoping rule.
    assert fork_calc._get_initial_on_hand(item_id, None) == FORK_ON_HAND


@requires_db
def test_initial_on_hand_baseline_unchanged(conn):
    """Baseline-scoped calculator still reads baseline stock (regression guard)."""
    from ootils_core.engine.mrp import GrossToNetCalculator

    item_id, location_id, _fork_id = _setup_fork_with_divergent_on_hand(conn)

    baseline_calc = GrossToNetCalculator(conn, BASELINE_ID)
    assert baseline_calc._get_initial_on_hand(item_id, location_id) == BASELINE_ON_HAND


@requires_db
def test_netting_in_fork_uses_fork_on_hand(conn):
    """
    Full gross-to-net chain inside the fork: 50 units of demand against the
    fork's on-hand of 5 must produce a net requirement. Against the baseline
    on-hand of 100 (the pre-fix behaviour) there would be no shortage at all.
    """
    from ootils_core.engine.mrp import GrossToNetCalculator

    item_id, location_id, fork_id = _setup_fork_with_divergent_on_hand(conn)

    calc = GrossToNetCalculator(conn, fork_id)
    buckets = calc.create_time_buckets(date.today(), horizon_days=28, grain="week")
    demand = Decimal("50")

    records = calc.calculate(
        item_id=item_id,
        location_id=location_id,
        buckets=buckets,
        planning_params={},  # safety stock defaults to 0
        consumed_forecast={buckets[0].start: demand},
        llc=0,
    )

    first = records[0]
    # PAB(0) = fork on-hand (5) - demand (50) = -45, NOT baseline 100 - 50 = 50.
    assert first.projected_on_hand == FORK_ON_HAND - demand
    assert first.net_requirements == demand - FORK_ON_HAND
    assert first.has_shortage is True
