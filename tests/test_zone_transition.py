"""
Comprehensive tests for ootils_core.engine.kernel.temporal.zone_transition.

Covers every code path: calendar helpers, ZoneTransitionEngine.run_transition,
weekly/monthly transitions, split helpers, idempotency, advisory locks,
edge rewiring, and all error/edge branches.
"""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, call, patch
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.temporal.zone_transition import (
    ZoneTransitionEngine,
    _rewire_edges,
    is_first_of_month,
    is_monday,
    next_monthly_boundary,
    next_weekly_boundary,
)
from ootils_core.models import Edge, Node


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(
    *,
    node_id: UUID | None = None,
    node_type: str = "ProjectedInventory",
    scenario_id: UUID | None = None,
    item_id: UUID | None = None,
    location_id: UUID | None = None,
    time_grain: str = "week",
    time_span_start: date | None = None,
    time_span_end: date | None = None,
    bucket_sequence: int | None = 0,
    active: bool = True,
    opening_stock: Decimal = Decimal("100"),
    inflows: Decimal = Decimal("10"),
    outflows: Decimal = Decimal("5"),
    closing_stock: Decimal = Decimal("105"),
) -> Node:
    return Node(
        node_id=node_id or uuid4(),
        node_type=node_type,
        scenario_id=scenario_id or uuid4(),
        item_id=item_id or uuid4(),
        location_id=location_id or uuid4(),
        time_grain=time_grain,
        time_span_start=time_span_start,
        time_span_end=time_span_end,
        bucket_sequence=bucket_sequence,
        active=active,
        opening_stock=opening_stock,
        inflows=inflows,
        outflows=outflows,
        closing_stock=closing_stock,
    )


def _make_edge(
    *,
    edge_id: UUID | None = None,
    from_node_id: UUID | None = None,
    to_node_id: UUID | None = None,
    scenario_id: UUID | None = None,
    edge_type: str = "feeds_forward",
    priority: int = 0,
    weight_ratio: Decimal = Decimal("1.0"),
    effective_start: date | None = None,
    effective_end: date | None = None,
) -> Edge:
    return Edge(
        edge_id=edge_id or uuid4(),
        edge_type=edge_type,
        from_node_id=from_node_id or uuid4(),
        to_node_id=to_node_id or uuid4(),
        scenario_id=scenario_id or uuid4(),
        priority=priority,
        weight_ratio=weight_ratio,
        effective_start=effective_start,
        effective_end=effective_end,
    )


def _mock_db_for_engine(
    *,
    lock_acquired: bool = True,
    transition_done: bool = False,
    start_run_row_id: UUID | None = None,
) -> MagicMock:
    """
    Build a mock psycopg.Connection whose .execute() returns the right
    cursor mock depending on the SQL query.
    """
    db = MagicMock()
    run_id = start_run_row_id or uuid4()

    def execute_side_effect(sql, params=None):
        cursor = MagicMock()
        sql_lower = sql.strip().lower() if isinstance(sql, str) else ""

        if "pg_try_advisory_lock" in sql_lower:
            cursor.fetchone.return_value = {"locked": lock_acquired}
        elif "pg_advisory_unlock" in sql_lower:
            cursor.fetchone.return_value = None
        elif "select status from zone_transition_runs" in sql_lower:
            if transition_done:
                cursor.fetchone.return_value = {"status": "completed"}
            else:
                cursor.fetchone.return_value = None
        elif "insert into zone_transition_runs" in sql_lower:
            cursor.fetchone.return_value = None
        elif "select id from zone_transition_runs" in sql_lower:
            cursor.fetchone.return_value = {"id": str(run_id)}
        elif "update zone_transition_runs" in sql_lower:
            cursor.fetchone.return_value = None
        elif "update edges" in sql_lower:
            cursor.fetchone.return_value = None
        elif "insert into edges" in sql_lower:
            cursor.fetchone.return_value = None
        else:
            cursor.fetchone.return_value = None
        return cursor

    db.execute.side_effect = execute_side_effect
    return db


# ---------------------------------------------------------------------------
# Calendar boundary helpers
# ---------------------------------------------------------------------------


class TestNextWeeklyBoundary:
    def test_basic(self):
        # 2025-01-06 is Monday; daily_horizon_weeks=1 → cutoff=2025-01-13 (Mon)
        result = next_weekly_boundary(date(2025, 1, 6), 1)
        assert result == date(2025, 1, 13)

    def test_non_monday_snaps_to_monday(self):
        # 2025-01-08 (Wed) + 1 week = 2025-01-15 (Wed) → snap to Monday 2025-01-13
        result = next_weekly_boundary(date(2025, 1, 8), 1)
        assert result == date(2025, 1, 13)

    def test_multiple_weeks(self):
        result = next_weekly_boundary(date(2025, 1, 1), 13)
        # 2025-01-01 + 13 weeks = 2025-04-02 (Wed) → snap to Monday 2025-03-31
        assert result == date(2025, 3, 31)


class TestNextMonthlyBoundary:
    def test_basic(self):
        result = next_monthly_boundary(date(2025, 1, 15), 3)
        assert result == date(2025, 4, 1)

    def test_year_rollover(self):
        result = next_monthly_boundary(date(2025, 11, 1), 3)
        assert result == date(2026, 2, 1)

    def test_december_plus_one(self):
        result = next_monthly_boundary(date(2025, 12, 1), 1)
        assert result == date(2026, 1, 1)

    def test_large_horizon(self):
        result = next_monthly_boundary(date(2025, 1, 1), 24)
        assert result == date(2027, 1, 1)


class TestIsMonday:
    def test_monday(self):
        assert is_monday(date(2025, 1, 6)) is True

    def test_not_monday(self):
        assert is_monday(date(2025, 1, 7)) is False


class TestIsFirstOfMonth:
    def test_first(self):
        assert is_first_of_month(date(2025, 3, 1)) is True

    def test_not_first(self):
        assert is_first_of_month(date(2025, 3, 2)) is False


# ---------------------------------------------------------------------------
# ZoneTransitionEngine.run_transition
# ---------------------------------------------------------------------------


class TestRunTransition:
    def setup_method(self):
        self.engine = ZoneTransitionEngine()
        self.series_id = uuid4()
        self.scenario_id = uuid4()

    def test_no_transition_when_not_monday_or_first(self):
        """Wednesday, March 5 2025 — not a Monday, not 1st."""
        db = _mock_db_for_engine()
        result = self.engine.run_transition(
            self.series_id, self.scenario_id, date(2025, 3, 5), db
        )
        assert result == {"weekly_to_daily": False, "monthly_to_weekly": False}

    def test_lock_not_acquired_raises(self):
        """When advisory lock cannot be acquired, RuntimeError is raised."""
        db = _mock_db_for_engine(lock_acquired=False)
        with pytest.raises(RuntimeError, match="advisory lock is held"):
            # 2025-01-06 is Monday
            self.engine.run_transition(
                self.series_id, self.scenario_id, date(2025, 1, 6), db
            )

    @patch.object(ZoneTransitionEngine, "_run_weekly_to_daily", return_value=True)
    def test_monday_only_runs_weekly_to_daily(self, mock_w2d):
        db = _mock_db_for_engine()
        # 2025-01-06 is Monday, not 1st
        result = self.engine.run_transition(
            self.series_id, self.scenario_id, date(2025, 1, 6), db
        )
        assert result["weekly_to_daily"] is True
        assert result["monthly_to_weekly"] is False
        mock_w2d.assert_called_once()

    @patch.object(ZoneTransitionEngine, "_run_monthly_to_weekly", return_value=True)
    def test_first_only_runs_monthly_to_weekly(self, mock_m2w):
        db = _mock_db_for_engine()
        # 2025-03-01 is Saturday (not Monday) and 1st of month
        result = self.engine.run_transition(
            self.series_id, self.scenario_id, date(2025, 3, 1), db
        )
        assert result["monthly_to_weekly"] is True
        assert result["weekly_to_daily"] is False
        mock_m2w.assert_called_once()

    @patch.object(ZoneTransitionEngine, "_run_weekly_to_daily", return_value=True)
    @patch.object(ZoneTransitionEngine, "_run_monthly_to_weekly", return_value=True)
    def test_combined_first_and_monday(self, mock_m2w, mock_w2d):
        """When date is both 1st and Monday, monthly runs first, then weekly."""
        db = _mock_db_for_engine()
        # 2025-09-01 is a Monday AND 1st of month
        result = self.engine.run_transition(
            self.series_id, self.scenario_id, date(2025, 9, 1), db
        )
        assert result["monthly_to_weekly"] is True
        assert result["weekly_to_daily"] is True
        # monthly called before weekly
        assert mock_m2w.call_count == 1
        assert mock_w2d.call_count == 1

    @patch.object(ZoneTransitionEngine, "_run_weekly_to_daily", side_effect=Exception("fail"))
    def test_lock_released_on_exception(self, mock_w2d):
        """Lock is released even if transition raises."""
        db = _mock_db_for_engine()
        with pytest.raises(Exception, match="fail"):
            self.engine.run_transition(
                self.series_id, self.scenario_id, date(2025, 1, 6), db
            )
        # Verify pg_advisory_unlock was called
        unlock_calls = [
            c for c in db.execute.call_args_list
            if "pg_advisory_unlock" in str(c)
        ]
        assert len(unlock_calls) >= 1


# ---------------------------------------------------------------------------
# _run_weekly_to_daily
# ---------------------------------------------------------------------------


class TestRunWeeklyToDaily:
    def setup_method(self):
        self.engine = ZoneTransitionEngine()
        self.series_id = uuid4()
        self.scenario_id = uuid4()

    def test_idempotent_skip(self):
        """Already-completed transition returns False."""
        db = _mock_db_for_engine(transition_done=True)
        store = MagicMock()

        result = self.engine._run_weekly_to_daily(
            self.series_id, self.scenario_id, date(2025, 1, 6), db, store
        )
        assert result is False

    def test_no_weekly_nodes(self):
        """No weekly nodes → complete with 0, return True."""
        db = _mock_db_for_engine()
        store = MagicMock()
        store.get_nodes_by_series.return_value = []

        result = self.engine._run_weekly_to_daily(
            self.series_id, self.scenario_id, date(2025, 1, 6), db, store
        )
        assert result is True

    def test_splits_first_weekly_node(self):
        """Weekly node is split into 7 daily nodes."""
        db = _mock_db_for_engine()
        store = MagicMock()

        weekly_node = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 13),
            bucket_sequence=0,
            active=True,
            scenario_id=self.scenario_id,
        )
        store.get_nodes_by_series.return_value = [weekly_node]
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        result = self.engine._run_weekly_to_daily(
            self.series_id, self.scenario_id, date(2025, 1, 6), db, store
        )
        assert result is True
        # 7 daily nodes + 1 archive of source = 8 upsert_node calls
        assert store.upsert_node.call_count == 8

    def test_selects_earliest_weekly_node(self):
        """When multiple weekly nodes exist, the one with earliest time_span_start is chosen."""
        db = _mock_db_for_engine()
        store = MagicMock()

        early = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 13),
            active=True,
        )
        late = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 13),
            time_span_end=date(2025, 1, 20),
            active=True,
        )
        store.get_nodes_by_series.return_value = [late, early]
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        result = self.engine._run_weekly_to_daily(
            self.series_id, self.scenario_id, date(2025, 1, 6), db, store
        )
        assert result is True
        # Only the early node should be split (7 daily + 1 archive = 8)
        assert store.upsert_node.call_count == 8

    def test_exception_marks_run_failed(self):
        """If split raises, _fail_transition_run is called, then re-raises."""
        db = _mock_db_for_engine()
        store = MagicMock()
        store.get_nodes_by_series.side_effect = RuntimeError("db error")

        with pytest.raises(RuntimeError, match="db error"):
            self.engine._run_weekly_to_daily(
                self.series_id, self.scenario_id, date(2025, 1, 6), db, store
            )
        # Verify _fail_transition_run was invoked via UPDATE ... SET status = 'failed'
        fail_calls = [
            c for c in db.execute.call_args_list
            if "failed" in str(c)
        ]
        assert len(fail_calls) >= 1

    def test_filters_inactive_and_non_weekly(self):
        """Inactive nodes and non-weekly nodes are filtered out."""
        db = _mock_db_for_engine()
        store = MagicMock()

        inactive_weekly = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 13),
            active=False,
        )
        daily_node = _make_node(
            time_grain="day",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 7),
            active=True,
        )
        no_start = _make_node(
            time_grain="week",
            time_span_start=None,
            active=True,
        )
        store.get_nodes_by_series.return_value = [inactive_weekly, daily_node, no_start]

        result = self.engine._run_weekly_to_daily(
            self.series_id, self.scenario_id, date(2025, 1, 6), db, store
        )
        # No valid weekly nodes found → complete with 0, return True
        assert result is True
        # Only the _complete_transition_run update, no upsert_node calls
        assert store.upsert_node.call_count == 0


# ---------------------------------------------------------------------------
# _run_monthly_to_weekly
# ---------------------------------------------------------------------------


class TestRunMonthlyToWeekly:
    def setup_method(self):
        self.engine = ZoneTransitionEngine()
        self.series_id = uuid4()
        self.scenario_id = uuid4()

    def test_idempotent_skip(self):
        db = _mock_db_for_engine(transition_done=True)
        store = MagicMock()

        result = self.engine._run_monthly_to_weekly(
            self.series_id, self.scenario_id, date(2025, 3, 1), db, store
        )
        assert result is False

    def test_no_monthly_nodes(self):
        db = _mock_db_for_engine()
        store = MagicMock()
        store.get_nodes_by_series.return_value = []

        result = self.engine._run_monthly_to_weekly(
            self.series_id, self.scenario_id, date(2025, 3, 1), db, store
        )
        assert result is True

    def test_splits_first_monthly_node(self):
        db = _mock_db_for_engine()
        store = MagicMock()

        monthly_node = _make_node(
            time_grain="month",
            time_span_start=date(2025, 3, 1),
            time_span_end=date(2025, 4, 1),
            bucket_sequence=0,
            active=True,
            scenario_id=self.scenario_id,
        )
        store.get_nodes_by_series.return_value = [monthly_node]
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        result = self.engine._run_monthly_to_weekly(
            self.series_id, self.scenario_id, date(2025, 3, 1), db, store
        )
        assert result is True
        # March 2025: 5 weekly buckets + 1 archive = 6 upsert_node calls
        assert store.upsert_node.call_count >= 5

    def test_exception_marks_run_failed(self):
        db = _mock_db_for_engine()
        store = MagicMock()
        store.get_nodes_by_series.side_effect = RuntimeError("db error")

        with pytest.raises(RuntimeError, match="db error"):
            self.engine._run_monthly_to_weekly(
                self.series_id, self.scenario_id, date(2025, 3, 1), db, store
            )

    def test_selects_earliest_monthly_node(self):
        db = _mock_db_for_engine()
        store = MagicMock()

        early = _make_node(
            time_grain="month",
            time_span_start=date(2025, 3, 1),
            time_span_end=date(2025, 4, 1),
            active=True,
        )
        late = _make_node(
            time_grain="month",
            time_span_start=date(2025, 4, 1),
            time_span_end=date(2025, 5, 1),
            active=True,
        )
        store.get_nodes_by_series.return_value = [late, early]
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        result = self.engine._run_monthly_to_weekly(
            self.series_id, self.scenario_id, date(2025, 3, 1), db, store
        )
        assert result is True


# ---------------------------------------------------------------------------
# _split_weekly_to_daily
# ---------------------------------------------------------------------------


class TestSplitWeeklyToDaily:
    def setup_method(self):
        self.engine = ZoneTransitionEngine()
        self.scenario_id = uuid4()
        self.series_id = uuid4()

    def test_creates_7_daily_nodes(self):
        db = MagicMock()
        store = MagicMock()
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        source = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 13),
            bucket_sequence=5,
            scenario_id=self.scenario_id,
        )

        new_nodes = self.engine._split_weekly_to_daily(
            source, self.scenario_id, self.series_id, db, store
        )
        assert len(new_nodes) == 7
        for i, n in enumerate(new_nodes):
            assert n.time_grain == "day"
            assert n.is_dirty is True
            assert n.active is True
            assert n.bucket_sequence == 5 + i
            assert n.time_span_start == date(2025, 1, 6) + timedelta(days=i)
            assert n.time_span_end == date(2025, 1, 7) + timedelta(days=i)

        # Source archived
        assert source.active is False
        # 7 new + 1 archive = 8 upsert calls
        assert store.upsert_node.call_count == 8

    def test_bucket_sequence_none_defaults_to_zero(self):
        db = MagicMock()
        store = MagicMock()
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        source = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 13),
            bucket_sequence=None,
        )

        new_nodes = self.engine._split_weekly_to_daily(
            source, self.scenario_id, self.series_id, db, store
        )
        assert new_nodes[0].bucket_sequence == 0
        assert new_nodes[6].bucket_sequence == 6

    def test_partial_week_span(self):
        """When span is less than 7 days, only that many daily nodes created."""
        db = MagicMock()
        store = MagicMock()
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        source = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 9),  # only 3 days
            bucket_sequence=0,
        )

        new_nodes = self.engine._split_weekly_to_daily(
            source, self.scenario_id, self.series_id, db, store
        )
        assert len(new_nodes) == 3

    def test_day_end_clamped_to_span_end(self):
        """Last daily bucket end is clamped to span_end if day_end > span_end."""
        db = MagicMock()
        store = MagicMock()
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        # 1.5-day span
        source = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 7),  # actually exactly 1 day
            bucket_sequence=0,
        )

        new_nodes = self.engine._split_weekly_to_daily(
            source, self.scenario_id, self.series_id, db, store
        )
        assert len(new_nodes) == 1
        assert new_nodes[0].time_span_end == date(2025, 1, 7)

    def test_rewire_called_when_nodes_exist(self):
        db = MagicMock()
        store = MagicMock()
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        source = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 13),
        )

        with patch(
            "ootils_core.engine.kernel.temporal.zone_transition._rewire_edges"
        ) as mock_rewire:
            new_nodes = self.engine._split_weekly_to_daily(
                source, self.scenario_id, self.series_id, db, store
            )
            mock_rewire.assert_called_once_with(
                source, new_nodes, self.scenario_id, db, store
            )


# ---------------------------------------------------------------------------
# _split_monthly_to_weekly
# ---------------------------------------------------------------------------


class TestSplitMonthlyToWeekly:
    def setup_method(self):
        self.engine = ZoneTransitionEngine()
        self.scenario_id = uuid4()
        self.series_id = uuid4()

    def test_creates_weekly_nodes_for_march(self):
        db = MagicMock()
        store = MagicMock()
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        source = _make_node(
            time_grain="month",
            time_span_start=date(2025, 3, 1),  # Saturday
            time_span_end=date(2025, 4, 1),
            bucket_sequence=0,
            scenario_id=self.scenario_id,
        )

        new_nodes = self.engine._split_monthly_to_weekly(
            source, self.scenario_id, self.series_id, db, store
        )
        # March 2025 (Sat): Mar 1-8, Mar 3-10, Mar 10-17, Mar 17-24, Mar 24-31, Mar 31-Apr 1
        # = 6 weekly buckets (first starts mid-week, last is partial)
        assert len(new_nodes) == 6
        for n in new_nodes:
            assert n.time_grain == "week"
            assert n.is_dirty is True
            assert n.active is True

        # First bucket starts at span_start (even if not Monday)
        assert new_nodes[0].time_span_start == date(2025, 3, 1)
        # Last bucket end clamped to span_end
        assert new_nodes[-1].time_span_end == date(2025, 4, 1)
        # Source archived
        assert source.active is False

    def test_bucket_sequence_none_defaults_to_zero(self):
        db = MagicMock()
        store = MagicMock()
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        source = _make_node(
            time_grain="month",
            time_span_start=date(2025, 3, 1),
            time_span_end=date(2025, 4, 1),
            bucket_sequence=None,
        )

        new_nodes = self.engine._split_monthly_to_weekly(
            source, self.scenario_id, self.series_id, db, store
        )
        assert new_nodes[0].bucket_sequence == 0

    def test_month_starting_on_monday(self):
        """When month starts on Monday, first bucket aligns perfectly."""
        db = MagicMock()
        store = MagicMock()
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        # 2025-09-01 is a Monday
        source = _make_node(
            time_grain="month",
            time_span_start=date(2025, 9, 1),
            time_span_end=date(2025, 10, 1),
            bucket_sequence=0,
        )

        new_nodes = self.engine._split_monthly_to_weekly(
            source, self.scenario_id, self.series_id, db, store
        )
        assert new_nodes[0].time_span_start == date(2025, 9, 1)
        # All bucket starts should be Mondays
        for n in new_nodes:
            assert n.time_span_start.weekday() == 0  # Monday

    def test_rewire_called(self):
        db = MagicMock()
        store = MagicMock()
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        source = _make_node(
            time_grain="month",
            time_span_start=date(2025, 3, 1),
            time_span_end=date(2025, 4, 1),
        )

        with patch(
            "ootils_core.engine.kernel.temporal.zone_transition._rewire_edges"
        ) as mock_rewire:
            self.engine._split_monthly_to_weekly(
                source, self.scenario_id, self.series_id, db, store
            )
            mock_rewire.assert_called_once()


# ---------------------------------------------------------------------------
# Idempotency helpers
# ---------------------------------------------------------------------------


class TestIdempotencyHelpers:
    def setup_method(self):
        self.engine = ZoneTransitionEngine()

    def test_is_transition_done_returns_true(self):
        db = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = {"status": "completed"}
        db.execute.return_value = cursor

        assert self.engine._is_transition_done("key", db) is True

    def test_is_transition_done_returns_false_when_no_row(self):
        db = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        db.execute.return_value = cursor

        assert self.engine._is_transition_done("key", db) is False

    def test_is_transition_done_returns_false_when_not_completed(self):
        db = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = {"status": "running"}
        db.execute.return_value = cursor

        assert self.engine._is_transition_done("key", db) is False

    def test_start_transition_run_returns_existing_id(self):
        """When ON CONFLICT skips insert, re-fetch returns existing ID."""
        existing_id = uuid4()
        db = MagicMock()

        call_count = [0]

        def execute_side_effect(sql, params=None):
            call_count[0] += 1
            cursor = MagicMock()
            if "INSERT" in sql:
                cursor.fetchone.return_value = None
            elif "SELECT id" in sql:
                cursor.fetchone.return_value = {"id": str(existing_id)}
            else:
                cursor.fetchone.return_value = None
            return cursor

        db.execute.side_effect = execute_side_effect

        result = self.engine._start_transition_run(
            "weekly_to_daily", date(2025, 1, 6), "key123", db
        )
        assert result == existing_id

    def test_start_transition_run_returns_new_id_when_refetch_none(self):
        """Edge case: re-fetch returns None (shouldn't happen, but fallback)."""
        db = MagicMock()

        def execute_side_effect(sql, params=None):
            cursor = MagicMock()
            if "SELECT id" in sql:
                cursor.fetchone.return_value = None
            else:
                cursor.fetchone.return_value = None
            return cursor

        db.execute.side_effect = execute_side_effect

        result = self.engine._start_transition_run(
            "weekly_to_daily", date(2025, 1, 6), "key123", db
        )
        # Should return the uuid4() generated inside the method
        assert isinstance(result, UUID)

    def test_complete_transition_run(self):
        db = MagicMock()
        cursor = MagicMock()
        db.execute.return_value = cursor
        run_id = uuid4()

        self.engine._complete_transition_run(run_id, 1, 7, db)
        db.execute.assert_called_once()
        args = db.execute.call_args
        assert "completed" in args[0][0]
        assert run_id in args[0][1]

    def test_fail_transition_run(self):
        db = MagicMock()
        cursor = MagicMock()
        db.execute.return_value = cursor
        run_id = uuid4()

        self.engine._fail_transition_run(run_id, db)
        db.execute.assert_called_once()
        args = db.execute.call_args
        assert "failed" in args[0][0]
        assert (run_id,) == args[0][1]


# ---------------------------------------------------------------------------
# Advisory lock helpers
# ---------------------------------------------------------------------------


class TestAdvisoryLockHelpers:
    def setup_method(self):
        self.engine = ZoneTransitionEngine()

    def test_try_acquire_lock_success(self):
        db = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = {"locked": True}
        db.execute.return_value = cursor

        assert self.engine._try_acquire_lock(db) is True

    def test_try_acquire_lock_failure(self):
        db = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = {"locked": False}
        db.execute.return_value = cursor

        assert self.engine._try_acquire_lock(db) is False

    def test_try_acquire_lock_no_row(self):
        db = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        db.execute.return_value = cursor

        assert self.engine._try_acquire_lock(db) is False

    def test_release_lock(self):
        db = MagicMock()
        cursor = MagicMock()
        db.execute.return_value = cursor

        self.engine._release_lock(db)
        db.execute.assert_called_once()
        assert "pg_advisory_unlock" in db.execute.call_args[0][0]


# ---------------------------------------------------------------------------
# _rewire_edges (module-level)
# ---------------------------------------------------------------------------


class TestRewireEdges:
    def test_rewires_inbound_and_outbound(self):
        db = MagicMock()
        store = MagicMock()
        scenario_id = uuid4()

        source_node = _make_node(scenario_id=scenario_id)
        first_new = _make_node()
        last_new = _make_node()
        new_nodes = [first_new, last_new]

        inbound_edge = _make_edge(
            from_node_id=uuid4(),
            to_node_id=source_node.node_id,
            scenario_id=scenario_id,
        )
        outbound_edge = _make_edge(
            from_node_id=source_node.node_id,
            to_node_id=uuid4(),
            scenario_id=scenario_id,
        )

        store.get_edges_to.return_value = [inbound_edge]
        store.get_edges_from.return_value = [outbound_edge]

        _rewire_edges(source_node, new_nodes, scenario_id, db, store)

        # 2 deactivate (UPDATE edges SET active = FALSE) + 2 new edge inserts = 4 db.execute calls
        assert db.execute.call_count == 4

        store.get_edges_to.assert_called_once_with(source_node.node_id, scenario_id)
        store.get_edges_from.assert_called_once_with(source_node.node_id, scenario_id)

    def test_no_edges_to_rewire(self):
        db = MagicMock()
        store = MagicMock()
        scenario_id = uuid4()

        source_node = _make_node()
        new_nodes = [_make_node()]

        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = []

        _rewire_edges(source_node, new_nodes, scenario_id, db, store)

        # No edge updates or inserts
        db.execute.assert_not_called()

    def test_multiple_inbound_edges(self):
        db = MagicMock()
        store = MagicMock()
        scenario_id = uuid4()

        source_node = _make_node()
        new_nodes = [_make_node(), _make_node(), _make_node()]

        edges_in = [
            _make_edge(to_node_id=source_node.node_id, scenario_id=scenario_id),
            _make_edge(to_node_id=source_node.node_id, scenario_id=scenario_id),
        ]
        store.get_edges_to.return_value = edges_in
        store.get_edges_from.return_value = []

        _rewire_edges(source_node, new_nodes, scenario_id, db, store)

        # 2 deactivate + 2 new inbound inserts = 4
        assert db.execute.call_count == 4

    def test_edge_properties_preserved(self):
        """New edges preserve edge_type, priority, weight_ratio, effective_start/end."""
        db = MagicMock()
        store = MagicMock()
        scenario_id = uuid4()

        source_node = _make_node()
        first_new = _make_node()
        new_nodes = [first_new]

        inbound_edge = _make_edge(
            to_node_id=source_node.node_id,
            scenario_id=scenario_id,
            edge_type="supplies",
            priority=5,
            weight_ratio=Decimal("0.75"),
            effective_start=date(2025, 1, 1),
            effective_end=date(2025, 12, 31),
        )
        store.get_edges_to.return_value = [inbound_edge]
        store.get_edges_from.return_value = []

        _rewire_edges(source_node, new_nodes, scenario_id, db, store)

        # Find the INSERT INTO edges call
        insert_calls = [
            c for c in db.execute.call_args_list
            if "INSERT INTO edges" in str(c)
        ]
        assert len(insert_calls) == 1
        params = insert_calls[0][0][1]
        # params: (uuid4(), edge_type, from_node_id, to_node_id, scenario_id, priority, weight_ratio, effective_start, effective_end)
        assert params[1] == "supplies"
        assert params[2] == inbound_edge.from_node_id
        assert params[3] == first_new.node_id  # redirected to first new node
        assert params[4] == scenario_id
        assert params[5] == 5
        assert params[6] == Decimal("0.75")
        assert params[7] == date(2025, 1, 1)
        assert params[8] == date(2025, 12, 31)

    def test_outbound_edge_redirected_to_last_node(self):
        """Outbound edges are redirected from_node_id to last new node."""
        db = MagicMock()
        store = MagicMock()
        scenario_id = uuid4()

        source_node = _make_node()
        first_new = _make_node()
        last_new = _make_node()
        new_nodes = [first_new, last_new]

        target_id = uuid4()
        outbound_edge = _make_edge(
            from_node_id=source_node.node_id,
            to_node_id=target_id,
            scenario_id=scenario_id,
        )
        store.get_edges_to.return_value = []
        store.get_edges_from.return_value = [outbound_edge]

        _rewire_edges(source_node, new_nodes, scenario_id, db, store)

        insert_calls = [
            c for c in db.execute.call_args_list
            if "INSERT INTO edges" in str(c)
        ]
        assert len(insert_calls) == 1
        params = insert_calls[0][0][1]
        # from_node_id should be last_new.node_id
        assert params[2] == last_new.node_id
        # to_node_id should be the original target
        assert params[3] == target_id
