"""
test_sprint2_temporal.py — Sprint 2 tests for TemporalBridge and ZoneTransitionEngine.

Sections:
  1. Pure unit tests (no DB) — TemporalBridge aggregate/disaggregate logic
  2. Pure unit tests — Zone boundary detection helpers
  3. ZoneTransitionEngine idempotency (mocked DB)
  4. Integration tests (require DATABASE_URL) — full round-trip with Postgres
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from unittest.mock import MagicMock, call, patch
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.temporal.bridge import (
    AggregatedBucket,
    TemporalBridge,
    _bucket_key_for_grain,
    _bucket_end_for_grain,
    _grain_rank,
    _week_start,
    _month_start,
)
from ootils_core.engine.kernel.temporal.zone_transition import (
    ZoneTransitionEngine,
    is_monday,
    is_first_of_month,
    next_weekly_boundary,
    next_monthly_boundary,
)
from ootils_core.models import Node


# ===========================================================================
# Helpers
# ===========================================================================


def _make_pi_node(
    series_id: UUID,
    scenario_id: UUID,
    time_grain: str,
    time_span_start: date,
    time_span_end: date,
    bucket_sequence: int,
    opening_stock: Decimal = Decimal("0"),
    inflows: Decimal = Decimal("0"),
    outflows: Decimal = Decimal("0"),
    closing_stock: Optional[Decimal] = None,
    has_shortage: bool = False,
    shortage_qty: Decimal = Decimal("0"),
    active: bool = True,
    is_dirty: bool = False,
) -> Node:
    if closing_stock is None:
        closing_stock = opening_stock + inflows - outflows
    return Node(
        node_id=uuid4(),
        node_type="ProjectedInventory",
        scenario_id=scenario_id,
        projection_series_id=series_id,
        bucket_sequence=bucket_sequence,
        time_grain=time_grain,
        time_ref=time_span_start,
        time_span_start=time_span_start,
        time_span_end=time_span_end,
        opening_stock=opening_stock,
        inflows=inflows,
        outflows=outflows,
        closing_stock=closing_stock,
        has_shortage=has_shortage,
        shortage_qty=shortage_qty,
        active=active,
        is_dirty=is_dirty,
    )


def _mock_db_with_nodes(nodes: list[Node]) -> MagicMock:
    """Return a mock psycopg connection that returns the given nodes from get_nodes_by_series."""
    db = MagicMock()

    # Bridge calls GraphStore.get_nodes_by_series which calls db.execute(query, (series_id,))
    # We need to mock db.execute().fetchall() to return row-like dicts
    def node_to_row(n: Node) -> dict:
        return {
            "node_id": str(n.node_id),
            "node_type": n.node_type,
            "scenario_id": str(n.scenario_id),
            "item_id": str(n.item_id) if n.item_id else None,
            "location_id": str(n.location_id) if n.location_id else None,
            "quantity": n.quantity,
            "qty_uom": n.qty_uom,
            "time_grain": n.time_grain,
            "time_ref": n.time_ref,
            "time_span_start": n.time_span_start,
            "time_span_end": n.time_span_end,
            "is_dirty": n.is_dirty,
            "last_calc_run_id": None,
            "active": n.active,
            "projection_series_id": str(n.projection_series_id) if n.projection_series_id else None,
            "bucket_sequence": n.bucket_sequence,
            "opening_stock": n.opening_stock,
            "inflows": n.inflows,
            "outflows": n.outflows,
            "closing_stock": n.closing_stock,
            "has_shortage": n.has_shortage,
            "shortage_qty": n.shortage_qty,
            "has_exact_date_inputs": n.has_exact_date_inputs,
            "has_week_inputs": n.has_week_inputs,
            "has_month_inputs": n.has_month_inputs,
            "created_at": n.created_at,
            "updated_at": n.updated_at,
        }

    rows = [node_to_row(n) for n in nodes]
    mock_result = MagicMock()
    mock_result.fetchall.return_value = rows
    db.execute.return_value = mock_result
    return db


# ===========================================================================
# 1. Grain helpers — unit tests
# ===========================================================================


class TestGrainHelpers:
    def test_grain_rank_ordering(self):
        assert _grain_rank("day") < _grain_rank("week")
        assert _grain_rank("week") < _grain_rank("month")
        assert _grain_rank("month") < _grain_rank("quarter")

    def test_grain_rank_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown grain"):
            _grain_rank("fortnight")

    def test_week_start_on_monday(self):
        monday = date(2026, 3, 30)  # Monday
        assert _week_start(monday) == monday

    def test_week_start_on_wednesday(self):
        wednesday = date(2026, 4, 1)  # Wednesday
        assert _week_start(wednesday) == date(2026, 3, 30)  # Monday

    def test_week_start_on_sunday(self):
        sunday = date(2026, 4, 5)
        assert _week_start(sunday) == date(2026, 3, 30)

    def test_month_start(self):
        assert _month_start(date(2026, 4, 15)) == date(2026, 4, 1)
        assert _month_start(date(2026, 4, 1)) == date(2026, 4, 1)

    def test_bucket_end_day(self):
        d = date(2026, 4, 1)
        assert _bucket_end_for_grain(d, "day") == date(2026, 4, 2)

    def test_bucket_end_week(self):
        monday = date(2026, 3, 30)
        assert _bucket_end_for_grain(monday, "week") == date(2026, 4, 6)

    def test_bucket_end_month(self):
        assert _bucket_end_for_grain(date(2026, 4, 1), "month") == date(2026, 5, 1)
        assert _bucket_end_for_grain(date(2026, 12, 1), "month") == date(2027, 1, 1)


# ===========================================================================
# 2. Zone boundary detection — unit tests
# ===========================================================================


class TestZoneBoundaryDetection:
    def test_is_monday_true(self):
        # 2026-03-30 is a Monday
        assert is_monday(date(2026, 3, 30)) is True

    def test_is_monday_false(self):
        # 2026-03-31 is a Tuesday
        assert is_monday(date(2026, 3, 31)) is False

    def test_is_first_of_month_true(self):
        assert is_first_of_month(date(2026, 4, 1)) is True

    def test_is_first_of_month_false(self):
        assert is_first_of_month(date(2026, 4, 2)) is False

    def test_next_weekly_boundary_snaps_to_monday(self):
        today = date(2026, 4, 4)  # Saturday
        boundary = next_weekly_boundary(today, daily_horizon_weeks=13)
        assert boundary.weekday() == 0, "next_weekly_boundary must return a Monday"

    def test_next_weekly_boundary_already_monday(self):
        today = date(2026, 3, 30)  # Monday
        boundary = next_weekly_boundary(today, daily_horizon_weeks=13)
        assert boundary.weekday() == 0

    def test_next_monthly_boundary_is_first(self):
        today = date(2026, 4, 4)
        boundary = next_monthly_boundary(today, weekly_horizon_months=3)
        assert boundary.day == 1

    def test_next_monthly_boundary_december_wraps(self):
        today = date(2026, 11, 1)
        boundary = next_monthly_boundary(today, weekly_horizon_months=3)
        assert boundary == date(2027, 2, 1)

    def test_combined_monday_and_first(self):
        # Find a date that is both Monday and 1st — 2026-06-01 is a Monday
        d = date(2026, 6, 1)
        assert is_monday(d) is True
        assert is_first_of_month(d) is True


# ===========================================================================
# 3. TemporalBridge.aggregate — unit tests (mocked DB)
# ===========================================================================


class TestTemporalBridgeAggregate:
    """TemporalBridge.aggregate: daily → weekly grouping."""

    def setup_method(self):
        self.bridge = TemporalBridge()
        self.series_id = uuid4()
        self.scenario_id = uuid4()

    def _make_week_of_daily_nodes(
        self,
        week_start: date,
        opening: Decimal,
        daily_inflow: Decimal = Decimal("0"),
        daily_outflow: Decimal = Decimal("2"),
    ) -> list[Node]:
        """Create 7 daily PI nodes for a week starting on week_start."""
        nodes = []
        running = opening
        for i in range(7):
            d = week_start + timedelta(days=i)
            closing = running + daily_inflow - daily_outflow
            nodes.append(
                _make_pi_node(
                    series_id=self.series_id,
                    scenario_id=self.scenario_id,
                    time_grain="day",
                    time_span_start=d,
                    time_span_end=d + timedelta(days=1),
                    bucket_sequence=i,
                    opening_stock=running,
                    inflows=daily_inflow,
                    outflows=daily_outflow,
                    closing_stock=closing,
                )
            )
            running = closing
        return nodes

    def test_aggregate_7_daily_into_1_weekly(self):
        week_start = date(2026, 3, 30)  # Monday
        nodes = self._make_week_of_daily_nodes(
            week_start, opening=Decimal("100"), daily_outflow=Decimal("5")
        )
        db = _mock_db_with_nodes(nodes)

        buckets = self.bridge.aggregate(self.series_id, "week", db)

        assert len(buckets) == 1
        b = buckets[0]
        assert b.time_grain == "week"
        assert b.time_span_start == week_start
        assert b.time_span_end == week_start + timedelta(weeks=1)
        # opening = first day's opening
        assert b.opening_stock == Decimal("100")
        # inflows = sum of daily inflows = 0 × 7 = 0
        assert b.inflows == Decimal("0")
        # outflows = sum = 5 × 7 = 35
        assert b.outflows == Decimal("35")
        # closing = last day's closing = 100 - 35 = 65
        assert b.closing_stock == Decimal("65")
        assert b.approximated is False
        assert len(b.source_node_ids) == 7

    def test_aggregate_14_daily_into_2_weekly(self):
        week1 = date(2026, 3, 30)
        week2 = week1 + timedelta(weeks=1)
        nodes = (
            self._make_week_of_daily_nodes(week1, opening=Decimal("100"), daily_outflow=Decimal("10"))
            + self._make_week_of_daily_nodes(week2, opening=Decimal("30"), daily_outflow=Decimal("3"))
        )
        # Fix bucket_sequence
        for idx, n in enumerate(nodes):
            n.bucket_sequence = idx
        db = _mock_db_with_nodes(nodes)

        buckets = self.bridge.aggregate(self.series_id, "week", db)

        assert len(buckets) == 2
        assert buckets[0].time_span_start == week1
        assert buckets[1].time_span_start == week2

    def test_aggregate_empty_series_returns_empty(self):
        db = _mock_db_with_nodes([])
        buckets = self.bridge.aggregate(self.series_id, "week", db)
        assert buckets == []

    def test_aggregate_daily_to_monthly(self):
        # 30 daily nodes in April 2026
        april_start = date(2026, 4, 1)
        nodes = []
        running = Decimal("200")
        for i in range(30):
            d = april_start + timedelta(days=i)
            closing = running - Decimal("1")
            nodes.append(
                _make_pi_node(
                    series_id=self.series_id,
                    scenario_id=self.scenario_id,
                    time_grain="day",
                    time_span_start=d,
                    time_span_end=d + timedelta(days=1),
                    bucket_sequence=i,
                    opening_stock=running,
                    inflows=Decimal("0"),
                    outflows=Decimal("1"),
                    closing_stock=closing,
                )
            )
            running = closing
        db = _mock_db_with_nodes(nodes)

        buckets = self.bridge.aggregate(self.series_id, "month", db)

        assert len(buckets) == 1
        b = buckets[0]
        assert b.time_grain == "month"
        assert b.time_span_start == date(2026, 4, 1)
        assert b.opening_stock == Decimal("200")
        assert b.outflows == Decimal("30")
        assert b.closing_stock == Decimal("170")

    def test_aggregate_no_shortage_propagation(self):
        week_start = date(2026, 3, 30)
        nodes = self._make_week_of_daily_nodes(week_start, opening=Decimal("100"))
        # Force a shortage on day 3
        nodes[3].has_shortage = True
        nodes[3].shortage_qty = Decimal("5")
        db = _mock_db_with_nodes(nodes)

        buckets = self.bridge.aggregate(self.series_id, "week", db)
        assert buckets[0].has_shortage is True
        assert buckets[0].shortage_qty == Decimal("5")

    def test_aggregate_invalid_target_grain_raises(self):
        db = _mock_db_with_nodes([])
        with pytest.raises(ValueError, match="Unknown grain"):
            self.bridge.aggregate(self.series_id, "fortnight", db)


# ===========================================================================
# 4. TemporalBridge.disaggregate — unit tests (mocked DB)
# ===========================================================================


class TestTemporalBridgeDisaggregate:
    """TemporalBridge.disaggregate: monthly → daily FLAT distribution."""

    def setup_method(self):
        self.bridge = TemporalBridge()
        self.series_id = uuid4()
        self.scenario_id = uuid4()

    def _make_monthly_node(
        self,
        month_start: date,
        inflows: Decimal = Decimal("0"),
        outflows: Decimal = Decimal("0"),
        opening_stock: Decimal = Decimal("100"),
    ) -> Node:
        """Create a single monthly PI node."""
        year = month_start.year + (month_start.month // 12)
        month = (month_start.month % 12) + 1
        month_end = date(year, month, 1)
        closing = opening_stock + inflows - outflows
        return _make_pi_node(
            series_id=self.series_id,
            scenario_id=self.scenario_id,
            time_grain="month",
            time_span_start=month_start,
            time_span_end=month_end,
            bucket_sequence=0,
            opening_stock=opening_stock,
            inflows=inflows,
            outflows=outflows,
            closing_stock=closing,
        )

    def test_disaggregate_monthly_to_daily_flat_count(self):
        # April has 30 days
        node = self._make_monthly_node(date(2026, 4, 1), outflows=Decimal("300"))
        db = _mock_db_with_nodes([node])

        buckets = self.bridge.disaggregate(self.series_id, "month", "day", db)
        assert len(buckets) == 30

    def test_disaggregate_monthly_to_daily_all_approximated(self):
        node = self._make_monthly_node(date(2026, 4, 1), outflows=Decimal("300"))
        db = _mock_db_with_nodes([node])

        buckets = self.bridge.disaggregate(self.series_id, "month", "day", db)
        assert all(b.approximated is True for b in buckets)

    def test_disaggregate_monthly_to_daily_flat_distribution(self):
        """Each daily bucket should receive outflows / 30 = 10."""
        node = self._make_monthly_node(
            date(2026, 4, 1),
            opening_stock=Decimal("300"),
            outflows=Decimal("300"),
        )
        db = _mock_db_with_nodes([node])

        buckets = self.bridge.disaggregate(self.series_id, "month", "day", db)
        # Per-bucket outflow = 300 / 30 = 10 exactly
        for b in buckets[:-1]:
            assert b.outflows == Decimal("10"), f"Expected 10, got {b.outflows} for {b.time_span_start}"

    def test_disaggregate_monthly_to_daily_opening_carries_forward(self):
        """opening_stock of sub-bucket i+1 = closing_stock of sub-bucket i."""
        node = self._make_monthly_node(
            date(2026, 4, 1),
            opening_stock=Decimal("300"),
            outflows=Decimal("300"),
        )
        db = _mock_db_with_nodes([node])

        buckets = self.bridge.disaggregate(self.series_id, "month", "day", db)
        for i in range(len(buckets) - 1):
            assert buckets[i + 1].opening_stock == buckets[i].closing_stock, (
                f"Carry-forward failed at index {i}: "
                f"closing={buckets[i].closing_stock}, next_opening={buckets[i+1].opening_stock}"
            )

    def test_disaggregate_closing_conservation(self):
        """Last sub-bucket's closing_stock should equal the source node's closing_stock."""
        node = self._make_monthly_node(
            date(2026, 4, 1),
            opening_stock=Decimal("300"),
            inflows=Decimal("60"),
            outflows=Decimal("120"),
        )
        db = _mock_db_with_nodes([node])

        buckets = self.bridge.disaggregate(self.series_id, "month", "day", db)
        assert buckets[-1].closing_stock == node.closing_stock, (
            f"Last sub-bucket closing {buckets[-1].closing_stock} != "
            f"source closing {node.closing_stock}"
        )

    def test_disaggregate_shortage_detected_in_sub_buckets(self):
        """If total outflows > opening_stock, some sub-buckets should show shortages."""
        node = self._make_monthly_node(
            date(2026, 4, 1),
            opening_stock=Decimal("10"),
            outflows=Decimal("300"),
        )
        db = _mock_db_with_nodes([node])

        buckets = self.bridge.disaggregate(self.series_id, "month", "day", db)
        shortage_buckets = [b for b in buckets if b.has_shortage]
        assert len(shortage_buckets) > 0, "Expected at least one bucket with shortage"

    def test_disaggregate_finer_than_source_required(self):
        """Disaggregating to same or coarser grain must raise."""
        node = self._make_monthly_node(date(2026, 4, 1))
        db = _mock_db_with_nodes([node])
        with pytest.raises(ValueError, match="must be finer"):
            self.bridge.disaggregate(self.series_id, "month", "month", db)
        with pytest.raises(ValueError, match="must be finer"):
            self.bridge.disaggregate(self.series_id, "week", "month", db)

    def test_disaggregate_skips_nodes_at_wrong_grain(self):
        """Nodes at a different grain than source_grain should be skipped."""
        # Mix: 1 monthly + 1 daily node in the series
        monthly_node = self._make_monthly_node(date(2026, 4, 1), outflows=Decimal("60"))
        daily_node = _make_pi_node(
            series_id=self.series_id,
            scenario_id=self.scenario_id,
            time_grain="day",
            time_span_start=date(2026, 5, 1),
            time_span_end=date(2026, 5, 2),
            bucket_sequence=1,
            outflows=Decimal("5"),
        )
        db = _mock_db_with_nodes([monthly_node, daily_node])

        # Only the monthly node should be disaggregated
        buckets = self.bridge.disaggregate(self.series_id, "month", "day", db)
        # All returned buckets should fall within April
        assert all(b.time_span_start < date(2026, 5, 1) for b in buckets)

    def test_disaggregate_monthly_to_weekly(self):
        """Monthly → weekly: should produce ~4-5 weekly buckets."""
        node = self._make_monthly_node(date(2026, 4, 1), outflows=Decimal("400"))
        db = _mock_db_with_nodes([node])

        buckets = self.bridge.disaggregate(self.series_id, "month", "week", db)
        # April 2026 has 5 partial/full ISO weeks touching it
        assert 4 <= len(buckets) <= 5
        assert all(b.time_grain == "week" for b in buckets)

    def test_disaggregate_empty_series(self):
        db = _mock_db_with_nodes([])
        buckets = self.bridge.disaggregate(self.series_id, "month", "day", db)
        assert buckets == []


# ===========================================================================
# 5. ZoneTransitionEngine idempotency — unit tests (mocked DB)
# ===========================================================================


class TestZoneTransitionEngineIdempotency:
    """ZoneTransitionEngine: idempotent re-run behavior."""

    def setup_method(self):
        self.engine = ZoneTransitionEngine()
        self.series_id = uuid4()
        self.scenario_id = uuid4()

    def _make_mock_db_for_transition(
        self,
        has_completed_run: bool,
        weekly_nodes: Optional[list[Node]] = None,
        lock_acquired: bool = True,
    ) -> MagicMock:
        """
        Build a mock DB for ZoneTransitionEngine tests.

        Sequence of db.execute() calls:
          1. pg_try_advisory_lock → returns lock_acquired
          2. zone_transition_runs SELECT (idempotency check) → returns completed or None
          3. zone_transition_runs INSERT (if not already done)
          4. nodes SELECT (get_nodes_by_series)
          5. upsert_node calls
          6. zone_transition_runs UPDATE (complete/fail)
          7. pg_advisory_unlock
        """
        db = MagicMock()

        call_count = [0]

        def mock_execute(sql, params=None):
            call_count[0] += 1
            mock_result = MagicMock()
            sql_stripped = sql.strip()

            if "pg_try_advisory_lock" in sql_stripped:
                mock_result.fetchone.return_value = {"locked": lock_acquired}

            elif "SELECT status FROM zone_transition_runs" in sql_stripped:
                if has_completed_run:
                    mock_result.fetchone.return_value = {"status": "completed"}
                else:
                    mock_result.fetchone.return_value = None

            elif "INSERT INTO zone_transition_runs" in sql_stripped:
                mock_result.fetchone.return_value = None

            elif "SELECT id FROM zone_transition_runs" in sql_stripped:
                run_id = uuid4()
                mock_result.fetchone.return_value = {"id": str(run_id)}

            elif "SELECT * FROM nodes" in sql_stripped or "projection_series_id" in sql_stripped:
                nodes = weekly_nodes or []
                rows = [_node_to_row(n) for n in nodes]
                mock_result.fetchall.return_value = rows

            elif "INSERT INTO nodes" in sql_stripped or "ON CONFLICT" in sql_stripped:
                mock_result.fetchone.return_value = None

            elif "pg_advisory_unlock" in sql_stripped:
                mock_result.fetchone.return_value = {"pg_advisory_unlock": True}

            else:
                mock_result.fetchone.return_value = None
                mock_result.fetchall.return_value = []

            return mock_result

        db.execute.side_effect = mock_execute
        return db

    def test_idempotent_skip_when_already_completed(self):
        """If the transition was already completed, run_transition returns False."""
        # Monday
        monday = date(2026, 3, 30)
        weekly_node = _make_pi_node(
            series_id=self.series_id,
            scenario_id=self.scenario_id,
            time_grain="week",
            time_span_start=monday,
            time_span_end=monday + timedelta(weeks=1),
            bucket_sequence=0,
        )
        db = self._make_mock_db_for_transition(
            has_completed_run=True,
            weekly_nodes=[weekly_node],
        )

        results = self.engine.run_transition(self.series_id, self.scenario_id, monday, db)
        assert results["weekly_to_daily"] is False, "Should skip already-completed transition"

    def test_transition_runs_on_monday_not_yet_done(self):
        """First run on a Monday should execute the weekly_to_daily transition."""
        monday = date(2026, 3, 30)
        weekly_node = _make_pi_node(
            series_id=self.series_id,
            scenario_id=self.scenario_id,
            time_grain="week",
            time_span_start=monday,
            time_span_end=monday + timedelta(weeks=1),
            bucket_sequence=0,
            opening_stock=Decimal("100"),
        )
        db = self._make_mock_db_for_transition(
            has_completed_run=False,
            weekly_nodes=[weekly_node],
        )

        results = self.engine.run_transition(self.series_id, self.scenario_id, monday, db)
        assert results["weekly_to_daily"] is True

    def test_no_transition_on_non_monday_non_first(self):
        """A regular weekday (not Monday, not 1st) should trigger no transition."""
        tuesday = date(2026, 3, 31)
        db = MagicMock()
        results = self.engine.run_transition(self.series_id, self.scenario_id, tuesday, db)
        assert results["weekly_to_daily"] is False
        assert results["monthly_to_weekly"] is False
        # DB should not be touched at all
        db.execute.assert_not_called()

    def test_lock_not_acquired_raises(self):
        """If advisory lock is held, run_transition must raise RuntimeError."""
        monday = date(2026, 3, 30)
        db = self._make_mock_db_for_transition(
            has_completed_run=False,
            weekly_nodes=[],
            lock_acquired=False,
        )
        with pytest.raises(RuntimeError, match="advisory lock"):
            self.engine.run_transition(self.series_id, self.scenario_id, monday, db)

    def test_combined_transition_on_first_monday(self):
        """A date that is both Monday AND 1st should run both transitions."""
        # 2026-06-01 is a Monday and 1st of month
        first_monday = date(2026, 6, 1)
        assert is_monday(first_monday) and is_first_of_month(first_monday)

        weekly_node = _make_pi_node(
            series_id=self.series_id,
            scenario_id=self.scenario_id,
            time_grain="week",
            time_span_start=first_monday,
            time_span_end=first_monday + timedelta(weeks=1),
            bucket_sequence=0,
        )
        monthly_node = _make_pi_node(
            series_id=self.series_id,
            scenario_id=self.scenario_id,
            time_grain="month",
            time_span_start=date(2026, 6, 1),
            time_span_end=date(2026, 7, 1),
            bucket_sequence=4,
        )
        # Build a DB that returns not-done for both, then returns the right nodes
        db = MagicMock()

        def multi_execute(sql, params=None):
            mock_result = MagicMock()
            sql_stripped = sql.strip()
            if "pg_try_advisory_lock" in sql_stripped:
                mock_result.fetchone.return_value = {"locked": True}
            elif "SELECT status FROM zone_transition_runs" in sql_stripped:
                mock_result.fetchone.return_value = None
            elif "SELECT id FROM zone_transition_runs" in sql_stripped:
                mock_result.fetchone.return_value = {"id": str(uuid4())}
            elif "SELECT * FROM nodes" in sql_stripped:
                # Return both types; bridge/ZoneTransitionEngine filters by grain
                rows = [_node_to_row(weekly_node), _node_to_row(monthly_node)]
                mock_result.fetchall.return_value = rows
            elif "pg_advisory_unlock" in sql_stripped:
                mock_result.fetchone.return_value = {"pg_advisory_unlock": True}
            else:
                mock_result.fetchone.return_value = None
                mock_result.fetchall.return_value = []
            return mock_result

        db.execute.side_effect = multi_execute

        results = self.engine.run_transition(self.series_id, self.scenario_id, first_monday, db)
        assert results["weekly_to_daily"] is True
        assert results["monthly_to_weekly"] is True


# ===========================================================================
# 6. ZoneTransitionEngine — structural mutation unit tests
# ===========================================================================


class TestZoneTransitionSplitBuckets:
    """Test bucket splitting logic without DB (mocked store)."""

    def setup_method(self):
        self.engine = ZoneTransitionEngine()
        self.series_id = uuid4()
        self.scenario_id = uuid4()

    def test_split_weekly_to_daily_creates_7_nodes(self):
        monday = date(2026, 3, 30)
        source = _make_pi_node(
            series_id=self.series_id,
            scenario_id=self.scenario_id,
            time_grain="week",
            time_span_start=monday,
            time_span_end=monday + timedelta(weeks=1),
            bucket_sequence=5,
            opening_stock=Decimal("100"),
        )

        store = MagicMock()
        upserted: list[Node] = []

        def capture_upsert(node: Node) -> Node:
            upserted.append(node)
            return node

        store.upsert_node.side_effect = capture_upsert

        new_nodes = self.engine._split_weekly_to_daily(
            source_node=source,
            scenario_id=self.scenario_id,
            series_id=self.series_id,
            db=MagicMock(),
            store=store,
        )

        # 7 new daily nodes + 1 archive of source = 8 upserts
        assert len(new_nodes) == 7
        assert all(n.time_grain == "day" for n in new_nodes)
        assert all(n.is_dirty is True for n in new_nodes)
        # Source node should be archived (active=False)
        assert source.active is False
        # Bucket sequences start at source's sequence
        assert new_nodes[0].bucket_sequence == 5
        assert new_nodes[6].bucket_sequence == 11

    def test_split_weekly_to_daily_spans_correct_days(self):
        monday = date(2026, 3, 30)
        source = _make_pi_node(
            series_id=self.series_id,
            scenario_id=self.scenario_id,
            time_grain="week",
            time_span_start=monday,
            time_span_end=monday + timedelta(weeks=1),
            bucket_sequence=0,
        )
        store = MagicMock()
        store.upsert_node.side_effect = lambda n: n

        new_nodes = self.engine._split_weekly_to_daily(
            source, self.scenario_id, self.series_id, MagicMock(), store
        )
        for i, node in enumerate(new_nodes):
            expected_start = monday + timedelta(days=i)
            assert node.time_span_start == expected_start, (
                f"Node {i}: expected start {expected_start}, got {node.time_span_start}"
            )
            assert node.time_span_end == expected_start + timedelta(days=1)

    def test_split_monthly_to_weekly_creates_4_to_5_nodes(self):
        # April 2026: 30 days, 5 ISO weeks touch April (w/partial weeks)
        april_start = date(2026, 4, 1)
        source = _make_pi_node(
            series_id=self.series_id,
            scenario_id=self.scenario_id,
            time_grain="month",
            time_span_start=april_start,
            time_span_end=date(2026, 5, 1),
            bucket_sequence=10,
        )
        store = MagicMock()
        store.upsert_node.side_effect = lambda n: n

        new_nodes = self.engine._split_monthly_to_weekly(
            source, self.scenario_id, self.series_id, MagicMock(), store
        )
        assert 4 <= len(new_nodes) <= 5
        assert all(n.time_grain == "week" for n in new_nodes)
        assert all(n.is_dirty is True for n in new_nodes)
        assert source.active is False

    def test_split_monthly_to_weekly_covers_full_span(self):
        """All sub-bucket spans should collectively cover [span_start, span_end)."""
        april_start = date(2026, 4, 1)
        april_end = date(2026, 5, 1)
        source = _make_pi_node(
            series_id=self.series_id,
            scenario_id=self.scenario_id,
            time_grain="month",
            time_span_start=april_start,
            time_span_end=april_end,
            bucket_sequence=0,
        )
        store = MagicMock()
        store.upsert_node.side_effect = lambda n: n

        new_nodes = self.engine._split_monthly_to_weekly(
            source, self.scenario_id, self.series_id, MagicMock(), store
        )
        # First sub-bucket starts at or before april_start
        assert new_nodes[0].time_span_start <= april_start
        # Last sub-bucket end = span_end
        assert new_nodes[-1].time_span_end == april_end


# ===========================================================================
# Helpers for test data
# ===========================================================================


def _node_to_row(n: Node) -> dict:
    """Convert Node to a row dict (mirrors GraphStore._row_to_node expectations)."""
    return {
        "node_id": str(n.node_id),
        "node_type": n.node_type,
        "scenario_id": str(n.scenario_id),
        "item_id": str(n.item_id) if n.item_id else None,
        "location_id": str(n.location_id) if n.location_id else None,
        "quantity": n.quantity,
        "qty_uom": n.qty_uom,
        "time_grain": n.time_grain,
        "time_ref": n.time_ref,
        "time_span_start": n.time_span_start,
        "time_span_end": n.time_span_end,
        "is_dirty": n.is_dirty,
        "last_calc_run_id": None,
        "active": n.active,
        "projection_series_id": str(n.projection_series_id) if n.projection_series_id else None,
        "bucket_sequence": n.bucket_sequence,
        "opening_stock": n.opening_stock,
        "inflows": n.inflows,
        "outflows": n.outflows,
        "closing_stock": n.closing_stock,
        "has_shortage": n.has_shortage,
        "shortage_qty": n.shortage_qty,
        "has_exact_date_inputs": n.has_exact_date_inputs,
        "has_week_inputs": n.has_week_inputs,
        "has_month_inputs": n.has_month_inputs,
        "created_at": n.created_at,
        "updated_at": n.updated_at,
    }


# ===========================================================================
# 7. Integration tests — require DATABASE_URL
# ===========================================================================

DATABASE_URL = os.environ.get("DATABASE_URL")
requires_db = pytest.mark.skipif(
    not DATABASE_URL,
    reason="DATABASE_URL not set — skipping DB integration tests",
)


@requires_db
def test_temporal_bridge_aggregate_integration():
    """
    Integration: create daily PI nodes in DB, aggregate to weekly via TemporalBridge.
    Validates that Bridge reads correctly from live DB and aggregates.
    """
    import psycopg
    from psycopg.rows import dict_row

    from ootils_core.engine.kernel.graph.store import GraphStore

    item_id = uuid4()
    location_id = uuid4()
    scenario_id = UUID("00000000-0000-0000-0000-000000000001")
    series_id = uuid4()
    today = date.today()
    # Ensure we start on a Monday for clean weekly grouping
    week_start = today - timedelta(days=today.weekday())

    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        conn.execute("INSERT INTO items (item_id, name) VALUES (%s, %s)", (item_id, "Bridge Test Item"))
        conn.execute("INSERT INTO locations (location_id, name) VALUES (%s, %s)", (location_id, "Bridge Test Loc"))
        conn.execute(
            """
            INSERT INTO projection_series (series_id, item_id, location_id, scenario_id,
                horizon_start, horizon_end)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (series_id, item_id, location_id, scenario_id, week_start, week_start + timedelta(days=7)),
        )

        # Create 7 daily nodes
        running = Decimal("70")
        for i in range(7):
            d = week_start + timedelta(days=i)
            closing = running - Decimal("2")
            conn.execute(
                """
                INSERT INTO nodes (
                    node_id, node_type, scenario_id, item_id, location_id,
                    time_grain, time_ref, time_span_start, time_span_end,
                    projection_series_id, bucket_sequence,
                    opening_stock, inflows, outflows, closing_stock
                ) VALUES (%s, 'ProjectedInventory', %s, %s, %s,
                    'day', %s, %s, %s, %s, %s, %s, 0, 2, %s)
                """,
                (uuid4(), scenario_id, item_id, location_id,
                 d, d, d + timedelta(days=1), series_id, i, running, closing),
            )
            running = closing

        conn.commit()

        bridge = TemporalBridge()
        buckets = bridge.aggregate(series_id, "week", conn)

        assert len(buckets) == 1
        b = buckets[0]
        assert b.time_grain == "week"
        assert b.opening_stock == Decimal("70")
        assert b.outflows == Decimal("14")  # 2 × 7
        assert b.closing_stock == Decimal("56")  # 70 - 14
        assert b.approximated is False

        # Cleanup
        conn.execute("DELETE FROM nodes WHERE projection_series_id = %s", (series_id,))
        conn.execute("DELETE FROM projection_series WHERE series_id = %s", (series_id,))
        conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))
        conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
        conn.commit()


@requires_db
def test_zone_transition_engine_idempotency_integration():
    """
    Integration: run weekly_to_daily transition twice — second run must be a no-op.
    Validates UNIQUE(idempotency_key) and completed status check.
    """
    import psycopg
    from psycopg.rows import dict_row

    item_id = uuid4()
    location_id = uuid4()
    scenario_id = UUID("00000000-0000-0000-0000-000000000001")
    series_id = uuid4()

    # Find next Monday
    today = date.today()
    days_until_monday = (7 - today.weekday()) % 7 or 7
    next_monday = today + timedelta(days=days_until_monday)

    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        conn.execute("INSERT INTO items (item_id, name) VALUES (%s, %s)", (item_id, "ZTE Test Item"))
        conn.execute("INSERT INTO locations (location_id, name) VALUES (%s, %s)", (location_id, "ZTE Test Loc"))
        conn.execute(
            """
            INSERT INTO projection_series (series_id, item_id, location_id, scenario_id,
                horizon_start, horizon_end)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (series_id, item_id, location_id, scenario_id,
             next_monday, next_monday + timedelta(weeks=1)),
        )

        # Create a weekly node
        weekly_node_id = uuid4()
        conn.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                time_grain, time_ref, time_span_start, time_span_end,
                projection_series_id, bucket_sequence,
                opening_stock, inflows, outflows, closing_stock
            ) VALUES (%s, 'ProjectedInventory', %s, %s, %s,
                'week', %s, %s, %s, %s, 0,
                100, 50, 30, 120)
            """,
            (weekly_node_id, scenario_id, item_id, location_id,
             next_monday, next_monday, next_monday + timedelta(weeks=1), series_id),
        )
        conn.commit()

        engine = ZoneTransitionEngine()

        # First run — should execute
        results1 = engine.run_transition(series_id, scenario_id, next_monday, conn)
        conn.commit()
        assert results1["weekly_to_daily"] is True

        # Second run — must be idempotent no-op
        results2 = engine.run_transition(series_id, scenario_id, next_monday, conn)
        conn.commit()
        assert results2["weekly_to_daily"] is False, "Second run should be skipped (idempotent)"

        # Verify 7 daily nodes were created
        count = conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM nodes
            WHERE projection_series_id = %s AND time_grain = 'day' AND active = TRUE
            """,
            (series_id,),
        ).fetchone()
        assert count["cnt"] == 7

        # Verify original weekly node is archived
        archived = conn.execute(
            "SELECT active FROM nodes WHERE node_id = %s", (weekly_node_id,)
        ).fetchone()
        assert archived["active"] is False

        # Cleanup
        conn.execute(
            "DELETE FROM zone_transition_runs WHERE idempotency_key LIKE %s",
            (f"%{series_id}%",),
        )
        conn.execute("DELETE FROM nodes WHERE projection_series_id = %s", (series_id,))
        conn.execute("DELETE FROM projection_series WHERE series_id = %s", (series_id,))
        conn.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))
        conn.execute("DELETE FROM items WHERE item_id = %s", (item_id,))
        conn.commit()
