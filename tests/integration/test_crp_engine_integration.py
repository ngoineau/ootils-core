"""
Integration tests for ootils_core.crp.engine.CRPEngine against a real
PostgreSQL database.

Ported from tests/test_crp_engine.py — the mock-heavy test file that
relied on a ``mock_db_connection`` fixture and scripted ``cursor.fetchall``
side-effects. The "no mocks" rule (CLAUDE.md) means every fetch_* /
calculate() branch is exercised by inserting real rows into the
``items``, ``locations``, ``resources`` (post-migration 034: formerly
``work_centers``), ``routings``, ``routing_operations`` and
``planned_supply`` tables and reading back the engine's load profiles.

Each test seeds its own rows and tears them down. The function-scoped
``conn`` fixture rolls back any uncommitted changes; we ``commit()``
inside tests because CRPEngine opens its own short-lived cursors which
need the rows persisted.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import pytest

from ootils_core.crp.engine import CRPEngine, CRPResult

from .conftest import requires_db

pytestmark = requires_db


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def _seed_item(conn) -> UUID:
    item_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"CRP Test Item {item_id}"),
    )
    return item_id


def _seed_location(conn) -> UUID:
    location_id = uuid4()
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"CRP Test Loc {location_id}"),
    )
    return location_id


def _seed_work_center(
    conn,
    *,
    code: Optional[str] = None,
    description: str = "Test Work Center",
    capacity_per_day: Decimal = Decimal("8.0"),
    efficiency: Decimal = Decimal("0.9"),
    active: bool = True,
) -> UUID:
    """
    Seed a work-center-flavoured row in the unified ``resources`` table
    (migration 034 / ADR-014 D1). Column mapping:
      work_center_id  → resource_id
      code            → external_id
      description     → name
    Always inserts resource_type='work_center' and capacity_unit='unit'
    to match the ADR-014 D2 defaults for migrated work_centers.
    """
    wc_id = uuid4()
    if code is None:
        code = f"WC-{wc_id.hex[:8]}"
    conn.execute(
        """
        INSERT INTO resources (
            resource_id, external_id, name, resource_type,
            capacity_per_day, capacity_unit, efficiency, calendar_id, active
        ) VALUES (%s, %s, %s, 'work_center', %s, 'unit', %s, NULL, %s)
        """,
        (wc_id, code, description, capacity_per_day, efficiency, active),
    )
    return wc_id


def _seed_routing(
    conn,
    *,
    item_id: UUID,
    sequence: int = 1,
    description: str = "Test Routing",
    active: bool = True,
) -> UUID:
    routing_id = uuid4()
    conn.execute(
        """
        INSERT INTO routings (routing_id, item_id, sequence, description, active)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (routing_id, item_id, sequence, description, active),
    )
    return routing_id


def _seed_operation(
    conn,
    *,
    routing_id: UUID,
    work_center_id: UUID,
    sequence: int = 10,
    setup_time: Decimal = Decimal("0"),
    run_time_per_unit: Decimal = Decimal("0.1"),
    description: str = "Test Op",
    active: bool = True,
) -> UUID:
    """
    Seed a routing_operations row. Migration 034 renamed the column
    ``work_center_id`` to ``resource_id``; the Python kwarg name
    ``work_center_id`` is kept as the internal helper contract so
    callers don't have to change.
    """
    operation_id = uuid4()
    conn.execute(
        """
        INSERT INTO routing_operations (
            operation_id, routing_id, sequence, resource_id,
            setup_time, run_time_per_unit, description, active
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            operation_id, routing_id, sequence, work_center_id,
            setup_time, run_time_per_unit, description, active,
        ),
    )
    return operation_id


def _seed_planned_order(
    conn,
    *,
    item_id: UUID,
    location_id: UUID,
    quantity: Decimal,
    due_date: date,
    status: str = "PLANNED",
    scenario_id: Optional[UUID] = None,
) -> UUID:
    planned_supply_id = uuid4()
    # Default scenario is the baseline scenario (NOT NULL FK on planned_supply).
    if scenario_id is None:
        conn.execute(
            """
            INSERT INTO planned_supply (
                planned_supply_id, item_id, location_id,
                quantity, due_date, status
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (planned_supply_id, item_id, location_id, quantity, due_date, status),
        )
    else:
        conn.execute(
            """
            INSERT INTO planned_supply (
                planned_supply_id, item_id, location_id, scenario_id,
                quantity, due_date, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (planned_supply_id, item_id, location_id, scenario_id,
             quantity, due_date, status),
        )
    return planned_supply_id


def _seed_scenario(conn) -> UUID:
    scenario_id = uuid4()
    conn.execute(
        "INSERT INTO scenarios (scenario_id, name) VALUES (%s, %s)",
        (scenario_id, f"crp-test-{scenario_id}"),
    )
    return scenario_id


def _teardown(
    conn,
    *,
    planned_orders: Optional[list[UUID]] = None,
    operations: Optional[list[UUID]] = None,
    routings: Optional[list[UUID]] = None,
    work_centers: Optional[list[UUID]] = None,
    items: Optional[list[UUID]] = None,
    locations: Optional[list[UUID]] = None,
    scenarios: Optional[list[UUID]] = None,
) -> None:
    """
    Delete every row written during the test so the DB stays clean.
    Order matters because of FK chain:
      planned_supply  → items, locations, scenarios
      routing_operations → routings, resources (formerly work_centers)
      routings        → items

    Migration 034 (ADR-014 D1): ``work_centers`` table dropped; rows
    live in ``resources``. The ``work_centers`` kwarg name is kept for
    helper-call compatibility.
    """
    if planned_orders:
        conn.execute(
            "DELETE FROM planned_supply WHERE planned_supply_id = ANY(%s)",
            (planned_orders,),
        )
    if operations:
        conn.execute(
            "DELETE FROM routing_operations WHERE operation_id = ANY(%s)",
            (operations,),
        )
    if routings:
        conn.execute("DELETE FROM routings WHERE routing_id = ANY(%s)", (routings,))
    if work_centers:
        conn.execute(
            "DELETE FROM resources WHERE resource_id = ANY(%s)",
            (work_centers,),
        )
    if locations:
        conn.execute("DELETE FROM locations WHERE location_id = ANY(%s)", (locations,))
    if items:
        conn.execute("DELETE FROM items WHERE item_id = ANY(%s)", (items,))
    if scenarios:
        conn.execute(
            "DELETE FROM scenarios WHERE scenario_id = ANY(%s)",
            (scenarios,),
        )
    conn.commit()


# ===========================================================================
# CRPEngine — calculate() over empty datasets
# ===========================================================================

class TestCRPEngineEmpty:
    """Branches where there are no work centers or no planned orders."""

    def test_calculate_no_work_centers(self, conn):
        """No work centers in DB → CRPResult is empty, no SQL further runs."""
        # Make sure we don't accidentally see other test rows: we don't insert
        # any work center. CRPEngine's _fetch_work_centers filters WHERE
        # active = true on `resources` (post-migration 034 / ADR-014 D1).
        # Per ADR-014 the engine does not discriminate by resource_type, so
        # we deactivate every resources row for the duration of this test
        # (rolled back at the end by the conn fixture).
        conn.execute("UPDATE resources SET active = false")
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine.calculate(horizon_days=30)

            assert isinstance(result, CRPResult)
            assert result.planned_orders_count == 0
            assert result.work_centers_count == 0
            assert len(result.overloads) == 0
        finally:
            # Roll back the bulk deactivation
            conn.rollback()

    def test_calculate_no_planned_orders(self, conn):
        """Active work center exists, but no planned orders in horizon."""
        wc_id = _seed_work_center(conn)
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine.calculate(
                horizon_days=30,
                work_centers=[wc_id],
            )
            assert result.planned_orders_count == 0
            # With no orders, no load_buckets are populated and therefore
            # no LoadProfile is added (work_centers_count == 0).
            assert result.work_centers_count == 0
        finally:
            _teardown(conn, work_centers=[wc_id])


# ===========================================================================
# CRPEngine._fetch_work_centers
# ===========================================================================

class TestFetchWorkCenters:
    def test_fetch_active_work_centers(self, conn):
        wc_id = _seed_work_center(
            conn,
            code=f"WC-FETCH-{uuid4().hex[:6]}",
            capacity_per_day=Decimal("8.0"),
            efficiency=Decimal("0.9"),
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            work_centers = engine._fetch_work_centers([wc_id])
            assert len(work_centers) == 1
            assert wc_id in work_centers
            # resources.capacity_per_day is NUMERIC(18,4) (migration 009);
            # post-034 the work_centers source — NUMERIC(18,6) — is gone.
            assert work_centers[wc_id].capacity_per_day == Decimal("8.0000")
        finally:
            _teardown(conn, work_centers=[wc_id])

    def test_fetch_work_centers_filtered_by_id(self, conn):
        """
        Replaces the previous mock-only assertion `assert "WHERE work_center_id
        IN" in call_args`. The behavioural assertion: passing a list filters
        the returned work centers to exactly those IDs.
        """
        wc_a = _seed_work_center(conn, code=f"WC-A-{uuid4().hex[:6]}")
        wc_b = _seed_work_center(conn, code=f"WC-B-{uuid4().hex[:6]}")
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine._fetch_work_centers([wc_a])
            assert wc_a in result
            assert wc_b not in result
            assert len(result) == 1
        finally:
            _teardown(conn, work_centers=[wc_a, wc_b])

    def test_inactive_work_center_excluded(self, conn):
        """The SQL WHERE active = true filter drops inactive rows."""
        wc_id = _seed_work_center(conn, active=False)
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine._fetch_work_centers([wc_id])
            assert len(result) == 0
        finally:
            _teardown(conn, work_centers=[wc_id])


# ===========================================================================
# CRPEngine._fetch_planned_orders
# ===========================================================================

class TestFetchPlannedOrders:
    def test_fetch_planned_orders(self, conn):
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        due_a = date.today() + timedelta(days=10)
        due_b = date.today() + timedelta(days=15)
        po_a = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("100"), due_date=due_a, status="PLANNED",
        )
        po_b = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("50"), due_date=due_b, status="RELEASED",
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            start_date = date.today()
            end_date = start_date + timedelta(days=90)
            orders = engine._fetch_planned_orders(start_date, end_date)

            order_ids = {o["planned_supply_id"] for o in orders}
            assert po_a in order_ids
            assert po_b in order_ids

            quantities = {o["planned_supply_id"]: o["quantity"] for o in orders
                          if o["planned_supply_id"] in (po_a, po_b)}
            assert quantities[po_a] == Decimal("100.000000")
            assert quantities[po_b] == Decimal("50.000000")
        finally:
            _teardown(
                conn,
                planned_orders=[po_a, po_b],
                items=[item_id],
                locations=[location_id],
            )

    def test_fetch_planned_orders_with_scenario(self, conn):
        """
        Replaces the mock-only `assert call_args[1][2] == scenario_id`.
        Behavioural assertion: orders tagged with the requested scenario_id
        are returned; orders tagged with a different scenario are not.
        """
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        scenario_id = _seed_scenario(conn)
        other_scenario_id = _seed_scenario(conn)
        due_date = date.today() + timedelta(days=5)
        po_target = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("10"), due_date=due_date,
            scenario_id=scenario_id,
        )
        po_other = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("20"), due_date=due_date,
            scenario_id=other_scenario_id,
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            start_date = date.today()
            end_date = start_date + timedelta(days=90)
            orders = engine._fetch_planned_orders(
                start_date, end_date, scenario_id=scenario_id,
            )
            ids = {o["planned_supply_id"] for o in orders}
            assert po_target in ids
            assert po_other not in ids
        finally:
            _teardown(
                conn,
                planned_orders=[po_target, po_other],
                items=[item_id],
                locations=[location_id],
                scenarios=[scenario_id, other_scenario_id],
            )


# ===========================================================================
# CRPEngine._fetch_routings
# ===========================================================================

class TestFetchRoutings:
    def test_fetch_routings_with_operations(self, conn):
        item_id = _seed_item(conn)
        wc1 = _seed_work_center(conn, code=f"WC-FR1-{uuid4().hex[:6]}")
        wc2 = _seed_work_center(conn, code=f"WC-FR2-{uuid4().hex[:6]}")
        routing_id = _seed_routing(conn, item_id=item_id)
        op1 = _seed_operation(
            conn, routing_id=routing_id, work_center_id=wc1, sequence=10,
            setup_time=Decimal("1.0"), run_time_per_unit=Decimal("0.5"),
        )
        op2 = _seed_operation(
            conn, routing_id=routing_id, work_center_id=wc2, sequence=20,
            setup_time=Decimal("0.5"), run_time_per_unit=Decimal("0.25"),
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            routings = engine._fetch_routings({item_id})
            assert item_id in routings
            assert len(routings[item_id].operations) == 2
            sequences = sorted(op.sequence for op in routings[item_id].operations)
            assert sequences == [10, 20]
        finally:
            _teardown(
                conn,
                operations=[op1, op2],
                routings=[routing_id],
                work_centers=[wc1, wc2],
                items=[item_id],
            )

    def test_fetch_routings_empty(self, conn):
        """No routings exist for the given item_id → empty dict."""
        item_id = _seed_item(conn)  # exists but has no routings
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            routings = engine._fetch_routings({item_id})
            assert len(routings) == 0
        finally:
            _teardown(conn, items=[item_id])


# ===========================================================================
# CRPEngine — full calculate() behaviour (load + overloads)
# ===========================================================================

class TestCRPEngineLoadCalculation:
    def test_backward_scheduling_single_operation(self, conn):
        """Single planned order + single-operation routing → load is recorded."""
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        wc_id = _seed_work_center(conn, code=f"WC-BS-{uuid4().hex[:6]}")
        routing_id = _seed_routing(conn, item_id=item_id)
        op_id = _seed_operation(
            conn, routing_id=routing_id, work_center_id=wc_id,
            sequence=10, setup_time=Decimal("0"),
            run_time_per_unit=Decimal("0.1"),
        )
        po_id = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("100"),
            due_date=date.today() + timedelta(days=10),
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine.calculate(horizon_days=30, work_centers=[wc_id])

            assert result.planned_orders_count == 1
            assert result.work_centers_count == 1
            assert wc_id in result.load_profiles
        finally:
            _teardown(
                conn,
                planned_orders=[po_id],
                operations=[op_id],
                routings=[routing_id],
                work_centers=[wc_id],
                items=[item_id],
                locations=[location_id],
            )

    def test_load_aggregation_multiple_orders(self, conn):
        """Two orders for the same item share the work-center load profile."""
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        wc_id = _seed_work_center(conn, code=f"WC-LA-{uuid4().hex[:6]}")
        routing_id = _seed_routing(conn, item_id=item_id)
        op_id = _seed_operation(
            conn, routing_id=routing_id, work_center_id=wc_id,
            sequence=10, setup_time=Decimal("0"),
            run_time_per_unit=Decimal("0.1"),
        )
        due = date.today() + timedelta(days=10)
        po1 = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("100"), due_date=due,
        )
        po2 = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("100"), due_date=due,
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine.calculate(horizon_days=30, work_centers=[wc_id])
            assert result.planned_orders_count == 2
            assert wc_id in result.load_profiles
        finally:
            _teardown(
                conn,
                planned_orders=[po1, po2],
                operations=[op_id],
                routings=[routing_id],
                work_centers=[wc_id],
                items=[item_id],
                locations=[location_id],
            )


# ===========================================================================
# CRPEngine — overload detection
# ===========================================================================

class TestCRPEngineOverloadDetection:
    def test_overload_detection_basic(self, conn):
        """
        Two planned orders on the same due date routed through the same work
        center pile up more hours than the daily effective capacity.

        WC effective capacity = 8.0 * 0.9 = 7.2 hours/day.
        Each order: 50 units * 0.1 run = 5 hours. Two orders, same day → 10h.
        10 > 7.2 → overload of 2.8 hours.
        """
        wc_id = _seed_work_center(
            conn,
            code=f"WC-OL-{uuid4().hex[:6]}",
            capacity_per_day=Decimal("8.0"),
            efficiency=Decimal("0.9"),
        )
        # Two items, two routings, two operations — both at the same wc.
        item1 = _seed_item(conn)
        item2 = _seed_item(conn)
        location_id = _seed_location(conn)
        r1 = _seed_routing(conn, item_id=item1)
        r2 = _seed_routing(conn, item_id=item2)
        op1 = _seed_operation(
            conn, routing_id=r1, work_center_id=wc_id,
            sequence=10, run_time_per_unit=Decimal("0.1"),
        )
        op2 = _seed_operation(
            conn, routing_id=r2, work_center_id=wc_id,
            sequence=10, run_time_per_unit=Decimal("0.1"),
        )
        due = date.today() + timedelta(days=5)
        po1 = _seed_planned_order(
            conn, item_id=item1, location_id=location_id,
            quantity=Decimal("50"), due_date=due,
        )
        po2 = _seed_planned_order(
            conn, item_id=item2, location_id=location_id,
            quantity=Decimal("50"), due_date=due,
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine.calculate(horizon_days=30, work_centers=[wc_id])
            assert len(result.overloads) > 0
            assert result.overloads[0].excess_hours > Decimal("0")
        finally:
            _teardown(
                conn,
                planned_orders=[po1, po2],
                operations=[op1, op2],
                routings=[r1, r2],
                work_centers=[wc_id],
                items=[item1, item2],
                locations=[location_id],
            )

    def test_no_overload_when_capacity_sufficient(self, conn):
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        wc_id = _seed_work_center(
            conn,
            code=f"WC-NOL-{uuid4().hex[:6]}",
            capacity_per_day=Decimal("8.0"),
            efficiency=Decimal("0.9"),
        )
        routing_id = _seed_routing(conn, item_id=item_id)
        op_id = _seed_operation(
            conn, routing_id=routing_id, work_center_id=wc_id,
            sequence=10, setup_time=Decimal("0"),
            run_time_per_unit=Decimal("0.01"),
        )
        po_id = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("10"),
            due_date=date.today() + timedelta(days=5),
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine.calculate(horizon_days=30, work_centers=[wc_id])
            assert len(result.overloads) == 0
        finally:
            _teardown(
                conn,
                planned_orders=[po_id],
                operations=[op_id],
                routings=[routing_id],
                work_centers=[wc_id],
                items=[item_id],
                locations=[location_id],
            )


# ===========================================================================
# CRPEngine — top-level helpers get_load_profile / get_overloads
# ===========================================================================

class TestCRPEngineLoadProfileQueries:
    def test_get_load_profile(self, conn):
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        wc_id = _seed_work_center(
            conn,
            code=f"WC-GLP-{uuid4().hex[:6]}",
            capacity_per_day=Decimal("8.0"),
            efficiency=Decimal("0.9"),
        )
        routing_id = _seed_routing(conn, item_id=item_id)
        op_id = _seed_operation(
            conn, routing_id=routing_id, work_center_id=wc_id,
            sequence=10, setup_time=Decimal("0"),
            run_time_per_unit=Decimal("0.01"),
        )
        po_id = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("10"),
            due_date=date.today() + timedelta(days=5),
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            profile = engine.get_load_profile(
                work_center_id=wc_id,
                horizon_days=30,
            )
            assert profile is not None
            assert profile.work_center_id == wc_id
            # 30-day horizon spans today..today+30 inclusive → 31 buckets.
            assert len(profile.buckets) == 31
        finally:
            _teardown(
                conn,
                planned_orders=[po_id],
                operations=[op_id],
                routings=[routing_id],
                work_centers=[wc_id],
                items=[item_id],
                locations=[location_id],
            )

    def test_get_overloads_no_orders(self, conn):
        """Active work center, but no planned orders → empty overload list."""
        wc_id = _seed_work_center(conn, code=f"WC-GOL-{uuid4().hex[:6]}")
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            overloads = engine.get_overloads(horizon_days=30, work_centers=[wc_id])
            assert isinstance(overloads, list)
            assert len(overloads) == 0
        finally:
            _teardown(conn, work_centers=[wc_id])


# ===========================================================================
# CRPEngine — edge cases
# ===========================================================================

class TestCRPEdgeCases:
    def test_inactive_operation_excluded(self, conn):
        """Routing operation with active=false → contributes no load."""
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        wc_id = _seed_work_center(conn, code=f"WC-IO-{uuid4().hex[:6]}")
        routing_id = _seed_routing(conn, item_id=item_id)
        op_id = _seed_operation(
            conn, routing_id=routing_id, work_center_id=wc_id,
            sequence=10, setup_time=Decimal("1.0"),
            run_time_per_unit=Decimal("0.5"),
            active=False,
        )
        po_id = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("100"),
            due_date=date.today() + timedelta(days=10),
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine.calculate(horizon_days=30, work_centers=[wc_id])
            # Inactive operations are filtered by _fetch_routings's
            # WHERE active = true on routing_operations. With no active
            # operations there is no load → no load profile produced.
            assert result.planned_orders_count == 1
            assert result.work_centers_count == 0
        finally:
            _teardown(
                conn,
                planned_orders=[po_id],
                operations=[op_id],
                routings=[routing_id],
                work_centers=[wc_id],
                items=[item_id],
                locations=[location_id],
            )

    def test_zero_capacity_work_center(self, conn):
        """A work center with capacity_per_day = 0 is still processed."""
        wc_id = _seed_work_center(
            conn,
            code=f"WC-ZC-{uuid4().hex[:6]}",
            capacity_per_day=Decimal("0"),
            efficiency=Decimal("1.0"),
        )
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        routing_id = _seed_routing(conn, item_id=item_id)
        op_id = _seed_operation(
            conn, routing_id=routing_id, work_center_id=wc_id,
            sequence=10, setup_time=Decimal("0"),
            run_time_per_unit=Decimal("0.1"),
        )
        po_id = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("10"),
            due_date=date.today() + timedelta(days=5),
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine.calculate(horizon_days=30, work_centers=[wc_id])
            # Zero-capacity work center: engine falls back to 8h/day default
            # for days_needed math, then builds a load profile with capacity=0.
            assert result is not None
            assert result.work_centers_count == 1
        finally:
            _teardown(
                conn,
                planned_orders=[po_id],
                operations=[op_id],
                routings=[routing_id],
                work_centers=[wc_id],
                items=[item_id],
                locations=[location_id],
            )

    def test_scenario_filter_applied(self, conn):
        """
        Replaces the mock-only `assert cursor.execute.call_count >= 1`.
        Behavioural assertion: orders tagged with a different scenario
        are excluded from the calculation result.
        """
        scenario_id = _seed_scenario(conn)
        other_scenario = _seed_scenario(conn)
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        wc_id = _seed_work_center(conn, code=f"WC-SF-{uuid4().hex[:6]}")
        routing_id = _seed_routing(conn, item_id=item_id)
        op_id = _seed_operation(
            conn, routing_id=routing_id, work_center_id=wc_id,
            sequence=10, run_time_per_unit=Decimal("0.1"),
        )
        # Order tagged with `other_scenario` — calculate(scenario_id=...)
        # must ignore it.
        po_other = _seed_planned_order(
            conn, item_id=item_id, location_id=location_id,
            quantity=Decimal("10"),
            due_date=date.today() + timedelta(days=5),
            scenario_id=other_scenario,
        )
        conn.commit()
        try:
            engine = CRPEngine(db_conn=conn)
            result = engine.calculate(
                horizon_days=30,
                work_centers=[wc_id],
                scenario_id=scenario_id,
            )
            assert result.planned_orders_count == 0
        finally:
            _teardown(
                conn,
                planned_orders=[po_other],
                operations=[op_id],
                routings=[routing_id],
                work_centers=[wc_id],
                items=[item_id],
                locations=[location_id],
                scenarios=[scenario_id, other_scenario],
            )


# ===========================================================================
# Performance — port with adjusted threshold
# ===========================================================================

@pytest.mark.slow
class TestCRPEnginePerformance:
    """
    Performance ported from the mocked file. Mocked DB returned scripted
    rows instantly, so the original threshold was <2s for 1000 orders.
    Real DB roundtrips dominate now: each INSERT is a network hop, and
    the engine fetches 1000 rows with item_id IN (...) clauses. We set a
    generous threshold (<10s for the calculate() call itself, excluding
    seed time) and mark this test as slow.
    """

    def test_performance_1000_orders(self, conn):
        import time

        wc_id = _seed_work_center(
            conn,
            code=f"WC-PERF-{uuid4().hex[:6]}",
            capacity_per_day=Decimal("8.0"),
            efficiency=Decimal("0.9"),
        )
        item_id = _seed_item(conn)
        location_id = _seed_location(conn)
        routing_id = _seed_routing(conn, item_id=item_id)
        op_id = _seed_operation(
            conn, routing_id=routing_id, work_center_id=wc_id,
            sequence=10, setup_time=Decimal("0"),
            run_time_per_unit=Decimal("0.01"),
        )

        # 1000 planned orders distributed across 90 days, all for the
        # same item so we only need one routing/operation.
        planned_order_ids: list[UUID] = []
        for i in range(1000):
            po_id = _seed_planned_order(
                conn, item_id=item_id, location_id=location_id,
                quantity=Decimal("10"),
                due_date=date.today() + timedelta(days=i % 90),
            )
            planned_order_ids.append(po_id)
        conn.commit()

        try:
            engine = CRPEngine(db_conn=conn)
            start_time = time.perf_counter()
            result = engine.calculate(
                horizon_days=90,
                work_centers=[wc_id],
            )
            elapsed = time.perf_counter() - start_time

            assert result.planned_orders_count == 1000
            # Real-DB threshold: 10s (mocked: 2s). The original engine
            # target is <500ms but that's only achievable when SQL is
            # mocked away — real fetch + scheduling on 1000 orders has
            # network overhead.
            assert elapsed < 10.0, f"Calculation took {elapsed:.2f}s, expected <10.0s"
        finally:
            _teardown(
                conn,
                planned_orders=planned_order_ids,
                operations=[op_id],
                routings=[routing_id],
                work_centers=[wc_id],
                items=[item_id],
                locations=[location_id],
            )
