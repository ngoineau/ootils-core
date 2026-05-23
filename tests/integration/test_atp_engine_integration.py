"""
Integration tests for ``ootils_core.atp.engine.ATPEngine`` against a real
PostgreSQL database (no mocks).

Ported from ``tests/test_atp_engine.py`` — the previous mock-heavy version
fabricated cursor return values with ``MagicMock``. Per CLAUDE.md ("Tests run
against real Postgres, no mocks"), each test here seeds real rows into
``on_hand_supply`` / ``planned_supply`` / ``customer_order_demand`` (plus the
referenced ``items`` / ``locations``), runs ``engine.calculate(...)`` against
the live ``conn``, asserts on the resulting ``ATPResult``, then tears down
every row it inserted.

Performance tests use the ``@pytest.mark.slow`` mark with a generous
threshold — real DB roundtrips add latency vs. the original mocked
``<100ms`` target.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from ootils_core.atp.engine import ATPEngine
from ootils_core.atp.models import ATPConfig

from .conftest import requires_db

pytestmark = requires_db


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


def _insert_item_and_location(conn) -> tuple[UUID, UUID]:
    """Create a unique item + location, return their ids."""
    item_id = uuid4()
    location_id = uuid4()
    conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s)",
        (item_id, f"ATP Test Item {item_id}"),
    )
    conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s)",
        (location_id, f"ATP Test Loc {location_id}"),
    )
    return item_id, location_id


def _seed_on_hand(
    conn,
    *,
    item_id: UUID,
    location_id: UUID,
    quantity: Decimal,
    as_of_date: date,
) -> UUID:
    on_hand_id = uuid4()
    conn.execute(
        """
        INSERT INTO on_hand_supply (on_hand_id, item_id, location_id, quantity, as_of_date)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (on_hand_id, item_id, location_id, quantity, as_of_date),
    )
    return on_hand_id


def _seed_planned_supply(
    conn,
    *,
    item_id: UUID,
    location_id: UUID,
    quantity: Decimal,
    due_date: date,
    status: str = "RELEASED",
    priority: int = 0,
) -> UUID:
    ps_id = uuid4()
    conn.execute(
        """
        INSERT INTO planned_supply
            (planned_supply_id, item_id, location_id, quantity, due_date, status, priority)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (ps_id, item_id, location_id, quantity, due_date, status, priority),
    )
    return ps_id


def _seed_demand(
    conn,
    *,
    item_id: UUID,
    location_id: UUID,
    quantity: Decimal,
    requested_date: date,
    status: str = "CONFIRMED",
    priority: int = 0,
    is_committed: bool = True,
) -> UUID:
    cod_id = uuid4()
    conn.execute(
        """
        INSERT INTO customer_order_demand
            (customer_order_demand_id, item_id, location_id, quantity,
             requested_date, status, priority, is_committed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (cod_id, item_id, location_id, quantity, requested_date, status, priority, is_committed),
    )
    return cod_id


def _teardown(conn, *, item_id: UUID, location_id: UUID) -> None:
    """Delete every row we may have written for this item/location."""
    conn.execute(
        "DELETE FROM customer_order_demand WHERE item_id = %s AND location_id = %s",
        (item_id, location_id),
    )
    conn.execute(
        "DELETE FROM planned_supply WHERE item_id = %s AND location_id = %s",
        (item_id, location_id),
    )
    conn.execute(
        "DELETE FROM on_hand_supply WHERE item_id = %s AND location_id = %s",
        (item_id, location_id),
    )
    conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))
    conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
    conn.commit()


# ---------------------------------------------------------------------------
# Calculate — standard scenarios
# ---------------------------------------------------------------------------


class TestATPEngineCalculate:
    """ATP calculation against real seeded supply/demand rows."""

    def test_basic_atp_calculation_with_onhand(self, conn):
        """100 on-hand, no demand → request of 50 is fully available today."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("100"), as_of_date=request_date)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("50"), request_date)

            assert result.available_quantity == Decimal("50")
            assert result.available_date == request_date
            assert result.is_fully_available is True
            assert result.backorder_quantity == Decimal("0")
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_basic_atp_calculation_with_demand(self, conn):
        """100 on-hand minus 40 committed demand still covers a request of 50."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("100"), as_of_date=request_date)
        _seed_demand(conn, item_id=item_id, location_id=location_id,
                     quantity=Decimal("40"), requested_date=request_date)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("50"), request_date)

            # ATP = 100 - 40 = 60; request 50 → fully available
            assert result.available_quantity == Decimal("50")
            assert result.is_fully_available is True
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_partial_shortage_scenario(self, conn):
        """100 on-hand minus 80 demand → 20 available against a 50-unit request."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("100"), as_of_date=request_date)
        _seed_demand(conn, item_id=item_id, location_id=location_id,
                     quantity=Decimal("80"), requested_date=request_date)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("50"), request_date)

            # ATP = 100 - 80 = 20, request 50 → only 20 available
            assert result.available_quantity == Decimal("20")
            assert result.is_fully_available is False
            assert result.backorder_quantity == Decimal("30")
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_backorder_scenario_with_planned_supply(self, conn):
        """50 on-hand, 80 demand today, 100 planned supply in 5 days → 50 available on day 5."""
        request_date = date.today()
        future_date = request_date + timedelta(days=5)
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("50"), as_of_date=request_date)
        _seed_planned_supply(conn, item_id=item_id, location_id=location_id,
                             quantity=Decimal("100"), due_date=future_date)
        _seed_demand(conn, item_id=item_id, location_id=location_id,
                     quantity=Decimal("80"), requested_date=request_date)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("50"), request_date)

            # Day 0: 50 - 80 = -30; day 5: -30 + 100 = 70 → 50 fulfillable on day 5
            assert result.available_quantity == Decimal("50")
            assert result.available_date == future_date
            assert result.is_fully_available is False  # not on request_date
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_no_supply_scenario(self, conn):
        """No on-hand, no planned supply → zero available, no date."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("50"), request_date)

            assert result.available_quantity == Decimal("0")
            assert result.available_date is None
            assert result.is_fully_available is False
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_no_demand_scenario(self, conn):
        """100 on-hand, no demand → 50-unit request fully available today."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("100"), as_of_date=request_date)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("50"), request_date)

            assert result.available_quantity == Decimal("50")
            assert result.available_date == request_date
            assert result.is_fully_available is True
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_multiple_planned_supplies(self, conn):
        """50 on-hand, +30 on day 5, +50 on day 10 → 100 fulfillable on day 10."""
        request_date = date.today()
        day5 = request_date + timedelta(days=5)
        day10 = request_date + timedelta(days=10)
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("50"), as_of_date=request_date)
        _seed_planned_supply(conn, item_id=item_id, location_id=location_id,
                             quantity=Decimal("30"), due_date=day5)
        _seed_planned_supply(conn, item_id=item_id, location_id=location_id,
                             quantity=Decimal("50"), due_date=day10)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("100"), request_date)

            # Day 0: 50, Day 5: 80, Day 10: 130 → 100 fulfillable on day 10
            assert result.available_quantity == Decimal("100")
            assert result.available_date == day10
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_cumulative_atp_netting(self, conn):
        """100 on-hand, 60 demand day0, 40 demand day1, +50 supply day2 → 50 on day 2."""
        request_date = date.today()
        day1 = request_date + timedelta(days=1)
        day2 = request_date + timedelta(days=2)
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("100"), as_of_date=request_date)
        _seed_planned_supply(conn, item_id=item_id, location_id=location_id,
                             quantity=Decimal("50"), due_date=day2)
        _seed_demand(conn, item_id=item_id, location_id=location_id,
                     quantity=Decimal("60"), requested_date=request_date)
        _seed_demand(conn, item_id=item_id, location_id=location_id,
                     quantity=Decimal("40"), requested_date=day1)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("50"), request_date)

            # Day 0: 100-60=40; Day 1: 40-40=0; Day 2: 0+50=50 → 50 on day 2
            assert result.available_quantity == Decimal("50")
            assert result.available_date == day2
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_request_beyond_horizon(self, conn):
        """Request 400 days out exceeds the default 365-day horizon."""
        request_date = date.today()
        future_date = request_date + timedelta(days=400)
        item_id, location_id = _insert_item_and_location(conn)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            # NOTE: The engine builds buckets starting at request_date, so
            # passing a future request_date pushes the whole horizon forward.
            # To reproduce the original "beyond horizon" semantics we explicitly
            # cap horizon_days so that future_date falls outside.
            result = engine.calculate(
                item_id, location_id, Decimal("50"), future_date, horizon_days=1
            )

            assert result.available_quantity == Decimal("0")
            assert result.available_date is None
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestATPEngineEdgeCases:
    """Boundary conditions exercised against real rows."""

    def test_zero_quantity_request(self, conn):
        """Requesting zero units with no supply still returns zero available."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("0"), request_date)
            assert result.available_quantity == Decimal("0")
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_exact_atp_match(self, conn):
        """100 on-hand, 50 demand → request of 50 is exactly satisfied."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("100"), as_of_date=request_date)
        _seed_demand(conn, item_id=item_id, location_id=location_id,
                     quantity=Decimal("50"), requested_date=request_date)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("50"), request_date)

            # ATP = 100 - 50 = 50 → exact match
            assert result.available_quantity == Decimal("50")
            assert result.is_fully_available is True
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_negative_atp_scenario(self, conn):
        """50 on-hand, 100 demand, no planned supply → zero available, no date."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("50"), as_of_date=request_date)
        _seed_demand(conn, item_id=item_id, location_id=location_id,
                     quantity=Decimal("100"), requested_date=request_date)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("10"), request_date)

            # ATP = -50 — no positive bucket → nothing available
            assert result.available_quantity == Decimal("0")
            assert result.available_date is None
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_bucket_calculation(self, conn):
        """horizon_days=5 → exactly 5 daily buckets, first opening_atp = on-hand."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("100"), as_of_date=request_date)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(
                item_id, location_id, Decimal("10"), request_date, horizon_days=5
            )

            assert len(result.buckets) == 5
            assert result.buckets[0].bucket_start == request_date
            assert result.buckets[0].opening_atp == Decimal("100")
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_planned_supply_status_filter(self, conn):
        """PLANNED-status supplies are ignored; only RELEASED/APPROVED count."""
        request_date = date.today()
        day5 = request_date + timedelta(days=5)
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("0"), as_of_date=request_date)
        # PLANNED should NOT be visible; RELEASED should be.
        _seed_planned_supply(conn, item_id=item_id, location_id=location_id,
                             quantity=Decimal("999"), due_date=day5, status="PLANNED")
        _seed_planned_supply(conn, item_id=item_id, location_id=location_id,
                             quantity=Decimal("50"), due_date=day5, status="RELEASED")
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("50"), request_date)

            # Only the 50-unit RELEASED supply should land on day 5
            assert result.available_quantity == Decimal("50")
            assert result.available_date == day5
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_demand_status_filter(self, conn):
        """DRAFT-status demands are ignored; only CONFIRMED/RELEASED net."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("100"), as_of_date=request_date)
        # DRAFT should NOT net; CONFIRMED should.
        _seed_demand(conn, item_id=item_id, location_id=location_id,
                     quantity=Decimal("999"), requested_date=request_date, status="DRAFT")
        _seed_demand(conn, item_id=item_id, location_id=location_id,
                     quantity=Decimal("30"), requested_date=request_date, status="CONFIRMED")
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            result = engine.calculate(item_id, location_id, Decimal("50"), request_date)

            # ATP = 100 - 30 = 70, request 50 → fully available
            assert result.available_quantity == Decimal("50")
            assert result.is_fully_available is True
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)


# ---------------------------------------------------------------------------
# Helper methods exercised end-to-end
# ---------------------------------------------------------------------------


class TestATPEngineHelpers:
    """check_available / get_available_date exercised against real data."""

    def test_check_available_true(self, conn):
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("100"), as_of_date=request_date)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            assert engine.check_available(
                item_id, location_id, Decimal("50"), request_date
            ) is True
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_check_available_false(self, conn):
        """20 on-hand can't cover a 50-unit request → False."""
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("20"), as_of_date=request_date)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            assert engine.check_available(
                item_id, location_id, Decimal("50"), request_date
            ) is False
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)

    def test_get_available_date_returns_future_date(self, conn):
        """50 on-hand, +50 in 5 days → 100-unit request available on day 5."""
        today = date.today()
        day5 = today + timedelta(days=5)
        item_id, location_id = _insert_item_and_location(conn)
        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("50"), as_of_date=today)
        _seed_planned_supply(conn, item_id=item_id, location_id=location_id,
                             quantity=Decimal("50"), due_date=day5)
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn)
            available_date = engine.get_available_date(
                item_id, location_id, Decimal("100")
            )
            assert available_date == day5
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)


# ---------------------------------------------------------------------------
# Performance — real DB roundtrip threshold is necessarily looser than mocks
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestATPEnginePerformance:
    """Performance against a real DB. Threshold is generous: real psycopg
    roundtrips + bucket allocation over a 1-year horizon dominate over the
    pure-Python work, so the original '<100ms with mocked DB' target is not
    realistic here. We assert a 1500ms ceiling and emit the actual measurement
    via the calculation_time_ms field for visibility."""

    def test_performance_one_year_horizon(self, conn):
        request_date = date.today()
        item_id, location_id = _insert_item_and_location(conn)

        _seed_on_hand(conn, item_id=item_id, location_id=location_id,
                      quantity=Decimal("1000"), as_of_date=request_date)
        for i in range(1, 13):
            _seed_planned_supply(
                conn, item_id=item_id, location_id=location_id,
                quantity=Decimal("100"),
                due_date=request_date + timedelta(days=i * 30),
            )
        for i in range(1, 25):
            _seed_demand(
                conn, item_id=item_id, location_id=location_id,
                quantity=Decimal("50"),
                requested_date=request_date + timedelta(days=i * 15),
            )
        conn.commit()
        try:
            engine = ATPEngine(db_conn=conn, config=ATPConfig(default_horizon_days=365))
            result = engine.calculate(
                item_id, location_id, Decimal("100"), request_date, horizon_days=365
            )
            # Real DB: be honest about the threshold. 1500ms is well above
            # observed local runs but leaves headroom for slower CI.
            assert result.calculation_time_ms < 1500, (
                f"Calculation took {result.calculation_time_ms:.2f}ms, "
                f"should be <1500ms with real DB"
            )
            # And we should have produced exactly 365 daily buckets.
            assert len(result.buckets) == 365
        finally:
            _teardown(conn, item_id=item_id, location_id=location_id)
