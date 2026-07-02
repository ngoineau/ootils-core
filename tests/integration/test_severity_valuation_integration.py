# ruff: noqa: F401,F811
"""Integration tests for shortage severity valuation (#342).

Before the fix, severity_score = shortage_qty × days × 1 (unit-cost proxy)
in BOTH engines — the shortage ranking ignored item value entirely. The
severity is now valued with the same precedence as ``mrp_core.cost_of``
(the watcher fleet's valuation): negotiated supplier unit_cost (preferred
supplier, cheapest priced row) first, then ``items.standard_cost``
(migration 042 BOM roll-up), then the proxy of 1 for unpriced items.

These tests exercise ``SHORTAGES_SQL`` — the single SQL implementation
shared by the SQL engine (default) AND the Rust engine wrapper (which
delegates shortage detection to the same statement). The Python kernel
path is covered at unit level (tests/test_m4_shortage.py::
test_custom_unit_cost_scales_severity) plus the batch cost cache in
``propagator.py`` which mirrors the same SELECT precedence.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

from ootils_core.engine.orchestration.propagator_sql import SHORTAGES_SQL

from .conftest import requires_db

pytestmark = requires_db


# ---------------------------------------------------------------------------
# Seed helpers (mirroring test_m4_shortage_integration.py)
# ---------------------------------------------------------------------------


def _insert_scenario(conn) -> UUID:
    scenario_id = uuid4()
    conn.execute(
        "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
        (scenario_id, f"Severity Valuation Scenario {scenario_id}"),
    )
    return scenario_id


def _insert_calc_run(conn, scenario_id: UUID) -> UUID:
    calc_run_id = uuid4()
    conn.execute(
        """
        INSERT INTO calc_runs (calc_run_id, scenario_id, status, is_full_recompute)
        VALUES (%s, %s, 'completed', TRUE)
        """,
        (calc_run_id, scenario_id),
    )
    return calc_run_id


def _insert_item(conn, *, standard_cost=None) -> UUID:
    item_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name, standard_cost) VALUES (%s, %s, %s)",
        (item_id, f"Severity Test Item {item_id}", standard_cost),
    )
    return item_id


def _insert_location(conn) -> UUID:
    location_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"Severity Test Loc {location_id}"),
    )
    return location_id


def _insert_supplier_price(conn, item_id: UUID, unit_cost, *, is_preferred=True) -> None:
    supplier_id = uuid4()
    conn.execute(
        "INSERT INTO suppliers (supplier_id, name) VALUES (%s, %s)",
        (supplier_id, f"Severity Test Supplier {supplier_id}"),
    )
    conn.execute(
        """
        INSERT INTO supplier_items (supplier_id, item_id, lead_time_days, unit_cost, is_preferred)
        VALUES (%s, %s, 10, %s, %s)
        """,
        (supplier_id, item_id, unit_cost, is_preferred),
    )


def _insert_dirty_stockout_pi(
    conn,
    *,
    scenario_id: UUID,
    calc_run_id: UUID,
    item_id: UUID,
    location_id: UUID,
    closing_stock: Decimal,
) -> UUID:
    """One-day PI bucket with a stockout, marked dirty for the calc run."""
    node_id = uuid4()
    conn.execute(
        """
        INSERT INTO nodes (
            node_id, node_type, scenario_id, item_id, location_id,
            time_grain, time_span_start, time_span_end,
            closing_stock, opening_stock, inflows, outflows,
            has_shortage, shortage_qty
        ) VALUES (
            %s, 'ProjectedInventory', %s, %s, %s,
            'day', %s, %s,
            %s, 0, 0, 0,
            TRUE, %s
        )
        """,
        (
            node_id, scenario_id, item_id, location_id,
            date(2026, 7, 1), date(2026, 7, 2),
            closing_stock, abs(closing_stock),
        ),
    )
    conn.execute(
        """
        INSERT INTO dirty_nodes (calc_run_id, node_id, scenario_id)
        VALUES (%s, %s, %s)
        """,
        (calc_run_id, node_id, scenario_id),
    )
    return node_id


def _severity_for(conn, pi_node_id: UUID) -> Decimal:
    row = conn.execute(
        "SELECT severity_score FROM shortages WHERE pi_node_id = %s",
        (pi_node_id,),
    ).fetchone()
    assert row is not None, f"no shortage row for PI {pi_node_id}"
    return Decimal(str(row["severity_score"]))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_severity_valued_with_cost_precedence(conn):
    """qty 10 × 1 day × cost — supplier price wins over standard_cost,
    standard_cost wins over nothing, unpriced falls back to the proxy of 1.

    On main (proxy-only), all three severities would be 10 and the ranking
    would be flat — this test fails there.
    """
    scenario_id = _insert_scenario(conn)
    calc_run_id = _insert_calc_run(conn, scenario_id)
    location_id = _insert_location(conn)

    # Item A: BOTH a negotiated supplier price (5) and a standard_cost (2)
    # → the supplier price must win (mrp_core.cost_of precedence).
    item_a = _insert_item(conn, standard_cost=Decimal("2"))
    _insert_supplier_price(conn, item_a, Decimal("5"))

    # Item B: standard_cost only (2).
    item_b = _insert_item(conn, standard_cost=Decimal("2"))

    # Item C: unpriced → proxy 1.
    item_c = _insert_item(conn)

    pi_a = _insert_dirty_stockout_pi(
        conn, scenario_id=scenario_id, calc_run_id=calc_run_id,
        item_id=item_a, location_id=location_id, closing_stock=Decimal("-10"),
    )
    pi_b = _insert_dirty_stockout_pi(
        conn, scenario_id=scenario_id, calc_run_id=calc_run_id,
        item_id=item_b, location_id=location_id, closing_stock=Decimal("-10"),
    )
    pi_c = _insert_dirty_stockout_pi(
        conn, scenario_id=scenario_id, calc_run_id=calc_run_id,
        item_id=item_c, location_id=location_id, closing_stock=Decimal("-10"),
    )

    conn.execute(
        SHORTAGES_SQL,
        {"calc_run_id": calc_run_id, "scenario_id": scenario_id},
    )

    assert _severity_for(conn, pi_a) == Decimal("50")  # 10 × 1 × 5 (supplier)
    assert _severity_for(conn, pi_b) == Decimal("20")  # 10 × 1 × 2 (standard)
    assert _severity_for(conn, pi_c) == Decimal("10")  # 10 × 1 × 1 (proxy)


def test_equal_quantities_rank_by_value(conn):
    """Two shortages of identical quantity are ranked by item value —
    the control-tower prioritisation the wedge sells (#342)."""
    scenario_id = _insert_scenario(conn)
    calc_run_id = _insert_calc_run(conn, scenario_id)
    location_id = _insert_location(conn)

    cheap = _insert_item(conn, standard_cost=Decimal("1.50"))
    expensive = _insert_item(conn, standard_cost=Decimal("400"))

    _insert_dirty_stockout_pi(
        conn, scenario_id=scenario_id, calc_run_id=calc_run_id,
        item_id=cheap, location_id=location_id, closing_stock=Decimal("-25"),
    )
    _insert_dirty_stockout_pi(
        conn, scenario_id=scenario_id, calc_run_id=calc_run_id,
        item_id=expensive, location_id=location_id, closing_stock=Decimal("-25"),
    )

    conn.execute(
        SHORTAGES_SQL,
        {"calc_run_id": calc_run_id, "scenario_id": scenario_id},
    )

    rows = conn.execute(
        """
        SELECT item_id, severity_score FROM shortages
        WHERE scenario_id = %s AND status = 'active'
        ORDER BY severity_score DESC
        """,
        (scenario_id,),
    ).fetchall()
    assert len(rows) == 2
    assert UUID(str(rows[0]["item_id"])) == expensive
    assert Decimal(str(rows[0]["severity_score"])) == Decimal("10000")  # 25 × 1 × 400
    assert Decimal(str(rows[1]["severity_score"])) == Decimal("37.5")  # 25 × 1 × 1.5
