"""
Comprehensive tests for ootils_core.engine.kernel.temporal.bridge.

Covers every code path: AggregatedBucket, grain helpers, TemporalBridge
aggregate/disaggregate, and all internal helpers.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.temporal.bridge import (
    AggregatedBucket,
    TemporalBridge,
    _bucket_end_for_grain,
    _bucket_key_for_grain,
    _grain_rank,
    _month_start,
    _week_start,
)
from ootils_core.models import Node, NodeTypeTemporalPolicy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(
    *,
    node_id: UUID | None = None,
    time_grain: str = "day",
    time_span_start: date | None = None,
    time_span_end: date | None = None,
    bucket_sequence: int | None = 0,
    opening_stock: Decimal | None = Decimal("100"),
    inflows: Decimal | None = Decimal("10"),
    outflows: Decimal | None = Decimal("5"),
    closing_stock: Decimal | None = Decimal("105"),
    has_shortage: bool = False,
    shortage_qty: Decimal = Decimal("0"),
    active: bool = True,
    scenario_id: UUID | None = None,
) -> Node:
    return Node(
        node_id=node_id or uuid4(),
        node_type="ProjectedInventory",
        scenario_id=scenario_id or uuid4(),
        time_grain=time_grain,
        time_span_start=time_span_start,
        time_span_end=time_span_end,
        bucket_sequence=bucket_sequence,
        opening_stock=opening_stock,
        inflows=inflows,
        outflows=outflows,
        closing_stock=closing_stock,
        has_shortage=has_shortage,
        shortage_qty=shortage_qty,
        active=active,
    )


# ---------------------------------------------------------------------------
# AggregatedBucket frozen dataclass
# ---------------------------------------------------------------------------


class TestAggregatedBucket:
    def test_creation_and_defaults(self):
        bucket = AggregatedBucket(
            time_grain="day",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 2),
            opening_stock=Decimal("10"),
            inflows=Decimal("5"),
            outflows=Decimal("3"),
            closing_stock=Decimal("12"),
            has_shortage=False,
            shortage_qty=Decimal("0"),
            source_node_ids=[uuid4()],
        )
        assert bucket.approximated is False

    def test_approximated_true(self):
        bucket = AggregatedBucket(
            time_grain="day",
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 2),
            opening_stock=Decimal("0"),
            inflows=Decimal("0"),
            outflows=Decimal("0"),
            closing_stock=Decimal("0"),
            has_shortage=False,
            shortage_qty=Decimal("0"),
            source_node_ids=[],
            approximated=True,
        )
        assert bucket.approximated is True


# ---------------------------------------------------------------------------
# Grain helpers
# ---------------------------------------------------------------------------


class TestGrainRank:
    def test_valid_grains(self):
        assert _grain_rank("day") == 0
        assert _grain_rank("week") == 1
        assert _grain_rank("month") == 2
        assert _grain_rank("quarter") == 3

    def test_invalid_grain_raises(self):
        with pytest.raises(ValueError, match="Unknown grain"):
            _grain_rank("hour")


class TestWeekStart:
    def test_monday_returns_self(self):
        # 2025-01-06 is a Monday
        assert _week_start(date(2025, 1, 6)) == date(2025, 1, 6)

    def test_wednesday_returns_monday(self):
        # 2025-01-08 is a Wednesday
        assert _week_start(date(2025, 1, 8)) == date(2025, 1, 6)

    def test_sunday_returns_monday(self):
        # 2025-01-12 is a Sunday
        assert _week_start(date(2025, 1, 12)) == date(2025, 1, 6)


class TestMonthStart:
    def test_first_day(self):
        assert _month_start(date(2025, 3, 1)) == date(2025, 3, 1)

    def test_mid_month(self):
        assert _month_start(date(2025, 3, 15)) == date(2025, 3, 1)

    def test_last_day(self):
        assert _month_start(date(2025, 12, 31)) == date(2025, 12, 1)


class TestBucketKeyForGrain:
    def test_day(self):
        d = date(2025, 3, 15)
        assert _bucket_key_for_grain(d, "day") == d

    def test_week(self):
        # 2025-03-15 is Saturday; Monday is 2025-03-10
        assert _bucket_key_for_grain(date(2025, 3, 15), "week") == date(2025, 3, 10)

    def test_month(self):
        assert _bucket_key_for_grain(date(2025, 3, 15), "month") == date(2025, 3, 1)

    def test_unsupported_grain_raises(self):
        with pytest.raises(ValueError, match="Unsupported grain for bucketing"):
            _bucket_key_for_grain(date(2025, 1, 1), "quarter")


class TestBucketEndForGrain:
    def test_day(self):
        assert _bucket_end_for_grain(date(2025, 1, 1), "day") == date(2025, 1, 2)

    def test_week(self):
        assert _bucket_end_for_grain(date(2025, 1, 6), "week") == date(2025, 1, 13)

    def test_month_normal(self):
        assert _bucket_end_for_grain(date(2025, 3, 1), "month") == date(2025, 4, 1)

    def test_month_december_wraps_year(self):
        assert _bucket_end_for_grain(date(2025, 12, 1), "month") == date(2026, 1, 1)

    def test_unsupported_grain_raises(self):
        with pytest.raises(ValueError, match="Unsupported grain for bucket end"):
            _bucket_end_for_grain(date(2025, 1, 1), "quarter")


# ---------------------------------------------------------------------------
# TemporalBridge.get_policy
# ---------------------------------------------------------------------------


class TestGetPolicy:
    def setup_method(self):
        self.bridge = TemporalBridge()
        self.db = MagicMock()
        self.policy_id = uuid4()

    def test_returns_policy_when_found(self):
        row = {
            "policy_id": str(self.policy_id),
            "node_type": "ProjectedInventory",
            "zone1_grain": "day",
            "zone1_end_days": 90,
            "zone2_grain": "week",
            "zone2_end_days": 180,
            "zone3_grain": "month",
            "week_start_dow": 0,
            "active": True,
            "created_at": None,
            "updated_at": None,
        }
        cursor_mock = MagicMock()
        cursor_mock.fetchone.return_value = row
        self.db.execute.return_value = cursor_mock

        policy = self.bridge.get_policy("ProjectedInventory", self.db)

        assert isinstance(policy, NodeTypeTemporalPolicy)
        assert policy.policy_id == self.policy_id
        assert policy.node_type == "ProjectedInventory"
        assert policy.zone1_grain == "day"
        assert policy.zone2_grain == "week"
        assert policy.zone3_grain == "month"
        assert policy.active is True

    def test_raises_key_error_when_not_found(self):
        cursor_mock = MagicMock()
        cursor_mock.fetchone.return_value = None
        self.db.execute.return_value = cursor_mock

        with pytest.raises(KeyError, match="No active temporal policy"):
            self.bridge.get_policy("NonExistent", self.db)

    def test_row_get_returns_none_for_missing_timestamps(self):
        """row.get('created_at') and row.get('updated_at') may be absent."""
        row = {
            "policy_id": str(self.policy_id),
            "node_type": "PI",
            "zone1_grain": "day",
            "zone1_end_days": 90,
            "zone2_grain": "week",
            "zone2_end_days": 180,
            "zone3_grain": "month",
            "week_start_dow": 0,
            "active": 1,
        }
        cursor_mock = MagicMock()
        cursor_mock.fetchone.return_value = row
        self.db.execute.return_value = cursor_mock

        policy = self.bridge.get_policy("PI", self.db)
        assert policy.active is True
        assert policy.created_at is None
        assert policy.updated_at is None


# ---------------------------------------------------------------------------
# TemporalBridge.aggregate
# ---------------------------------------------------------------------------


class TestAggregate:
    def setup_method(self):
        self.bridge = TemporalBridge()
        self.db = MagicMock()
        self.series_id = uuid4()

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_empty_nodes_returns_empty(self, mock_load):
        mock_load.return_value = []
        result = self.bridge.aggregate(self.series_id, "week", self.db)
        assert result == []

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_invalid_grain_raises(self, mock_load):
        with pytest.raises(ValueError, match="Unknown grain"):
            self.bridge.aggregate(self.series_id, "hour", self.db)

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_single_day_node_aggregated_to_week(self, mock_load):
        nid = uuid4()
        node = _make_node(
            node_id=nid,
            time_grain="day",
            time_span_start=date(2025, 1, 6),  # Monday
            time_span_end=date(2025, 1, 7),
            bucket_sequence=0,
            opening_stock=Decimal("100"),
            inflows=Decimal("10"),
            outflows=Decimal("5"),
            closing_stock=Decimal("105"),
        )
        mock_load.return_value = [node]

        result = self.bridge.aggregate(self.series_id, "week", self.db)
        assert len(result) == 1
        b = result[0]
        assert b.time_grain == "week"
        assert b.time_span_start == date(2025, 1, 6)
        assert b.time_span_end == date(2025, 1, 13)
        assert b.opening_stock == Decimal("100")
        assert b.closing_stock == Decimal("105")
        assert b.inflows == Decimal("10")
        assert b.outflows == Decimal("5")
        assert b.approximated is False
        assert nid in b.source_node_ids

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_multiple_days_aggregate_to_week(self, mock_load):
        """7 daily nodes aggregated to 1 weekly bucket."""
        nodes = []
        for i in range(7):
            d = date(2025, 1, 6) + timedelta(days=i)
            nodes.append(
                _make_node(
                    time_grain="day",
                    time_span_start=d,
                    time_span_end=d + timedelta(days=1),
                    bucket_sequence=i,
                    opening_stock=Decimal("100") + Decimal(str(i * 5)),
                    inflows=Decimal("10"),
                    outflows=Decimal("5"),
                    closing_stock=Decimal("105") + Decimal(str(i * 5)),
                    has_shortage=(i == 3),
                    shortage_qty=Decimal("2") if i == 3 else Decimal("0"),
                )
            )
        mock_load.return_value = nodes

        result = self.bridge.aggregate(self.series_id, "week", self.db)
        assert len(result) == 1
        b = result[0]
        assert b.inflows == Decimal("70")   # 10 * 7
        assert b.outflows == Decimal("35")  # 5 * 7
        assert b.opening_stock == Decimal("100")    # first node
        assert b.closing_stock == Decimal("135")    # last node: 105 + 6*5
        assert b.has_shortage is True
        assert b.shortage_qty == Decimal("2")
        assert len(b.source_node_ids) == 7

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_node_without_time_span_start_is_skipped(self, mock_load):
        node = _make_node(time_span_start=None)
        mock_load.return_value = [node]

        result = self.bridge.aggregate(self.series_id, "day", self.db)
        assert result == []

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_nodes_with_none_values_default_to_zero(self, mock_load):
        node = _make_node(
            time_span_start=date(2025, 1, 1),
            time_span_end=date(2025, 1, 2),
            opening_stock=None,
            inflows=None,
            outflows=None,
            closing_stock=None,
            shortage_qty=None,
            bucket_sequence=None,
        )
        mock_load.return_value = [node]

        result = self.bridge.aggregate(self.series_id, "day", self.db)
        assert len(result) == 1
        b = result[0]
        assert b.opening_stock == Decimal("0")
        assert b.inflows == Decimal("0")
        assert b.outflows == Decimal("0")
        assert b.closing_stock == Decimal("0")
        assert b.shortage_qty == Decimal("0")

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_aggregate_to_month(self, mock_load):
        """Aggregate daily nodes to monthly buckets."""
        nodes = []
        for i in range(3):
            d = date(2025, 3, 1) + timedelta(days=i)
            nodes.append(
                _make_node(
                    time_span_start=d,
                    time_span_end=d + timedelta(days=1),
                    bucket_sequence=i,
                    opening_stock=Decimal("50"),
                    inflows=Decimal("10"),
                    outflows=Decimal("3"),
                    closing_stock=Decimal("57"),
                )
            )
        mock_load.return_value = nodes

        result = self.bridge.aggregate(self.series_id, "month", self.db)
        assert len(result) == 1
        b = result[0]
        assert b.time_span_start == date(2025, 3, 1)
        assert b.time_span_end == date(2025, 4, 1)
        assert b.inflows == Decimal("30")
        assert b.outflows == Decimal("9")

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_aggregate_multiple_buckets(self, mock_load):
        """Nodes spanning two different weeks produce two weekly buckets."""
        n1 = _make_node(
            time_span_start=date(2025, 1, 6),  # week 1
            time_span_end=date(2025, 1, 7),
            bucket_sequence=0,
        )
        n2 = _make_node(
            time_span_start=date(2025, 1, 13),  # week 2
            time_span_end=date(2025, 1, 14),
            bucket_sequence=1,
        )
        mock_load.return_value = [n2, n1]  # intentionally out of order

        result = self.bridge.aggregate(self.series_id, "week", self.db)
        assert len(result) == 2
        assert result[0].time_span_start < result[1].time_span_start


# ---------------------------------------------------------------------------
# TemporalBridge.disaggregate
# ---------------------------------------------------------------------------


class TestDisaggregate:
    def setup_method(self):
        self.bridge = TemporalBridge()
        self.db = MagicMock()
        self.series_id = uuid4()

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_target_not_finer_raises(self, mock_load):
        with pytest.raises(ValueError, match="must be finer"):
            self.bridge.disaggregate(self.series_id, "day", "week", self.db)

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_same_grain_raises(self, mock_load):
        with pytest.raises(ValueError, match="must be finer"):
            self.bridge.disaggregate(self.series_id, "week", "week", self.db)

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_unsupported_distribution_raises(self, mock_load):
        with pytest.raises(NotImplementedError, match="not implemented"):
            self.bridge.disaggregate(
                self.series_id, "week", "day", self.db, distribution="WEIGHTED"
            )

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_empty_nodes_returns_empty(self, mock_load):
        mock_load.return_value = []
        result = self.bridge.disaggregate(self.series_id, "week", "day", self.db)
        assert result == []

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_weekly_to_daily_flat(self, mock_load):
        """A weekly node with 70 inflows, 35 outflows split into 7 daily buckets."""
        nid = uuid4()
        node = _make_node(
            node_id=nid,
            time_grain="week",
            time_span_start=date(2025, 1, 6),  # Monday
            time_span_end=date(2025, 1, 13),   # next Monday
            opening_stock=Decimal("100"),
            inflows=Decimal("70"),
            outflows=Decimal("35"),
            closing_stock=Decimal("135"),
        )
        mock_load.return_value = [node]

        result = self.bridge.disaggregate(self.series_id, "week", "day", self.db)
        assert len(result) == 7
        assert all(b.approximated is True for b in result)
        assert all(b.time_grain == "day" for b in result)
        assert all(nid in b.source_node_ids for b in result)

        # Flat: 70 / 7 = 10 per bucket, 35 / 7 = 5 per bucket
        for b in result:
            assert b.inflows == Decimal("10")
            assert b.outflows == Decimal("5")

        # First bucket opening should match source opening
        assert result[0].opening_stock == Decimal("100")
        # Stock flows: each bucket: closing = opening + 10 - 5 = opening + 5
        assert result[0].closing_stock == Decimal("105")
        assert result[1].opening_stock == Decimal("105")

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_weekly_to_daily_flat_with_remainder(self, mock_load):
        """Inflows=10 over 7 days: 1 each + 3 remainder on last bucket."""
        node = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 13),
            opening_stock=Decimal("0"),
            inflows=Decimal("10"),
            outflows=Decimal("0"),
            closing_stock=Decimal("10"),
        )
        mock_load.return_value = [node]

        result = self.bridge.disaggregate(self.series_id, "week", "day", self.db)
        assert len(result) == 7
        # 10 / 7 = 1 remainder 3 → first 6 get 1, last gets 1+3=4
        for b in result[:6]:
            assert b.inflows == Decimal("1")
        assert result[6].inflows == Decimal("4")  # 1 + 3

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_skip_nodes_at_different_grain(self, mock_load):
        """When disaggregating weekly→daily, daily nodes are skipped."""
        daily_node = _make_node(
            time_grain="day",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 7),
        )
        mock_load.return_value = [daily_node]

        result = self.bridge.disaggregate(self.series_id, "week", "day", self.db)
        assert result == []

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_skip_node_missing_time_span(self, mock_load):
        """Node with no time_span_start or time_span_end is skipped."""
        node = _make_node(
            time_grain="week",
            time_span_start=None,
            time_span_end=None,
        )
        mock_load.return_value = [node]

        result = self.bridge.disaggregate(self.series_id, "week", "day", self.db)
        assert result == []

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_skip_node_missing_time_span_end_only(self, mock_load):
        node = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=None,
        )
        mock_load.return_value = [node]

        result = self.bridge.disaggregate(self.series_id, "week", "day", self.db)
        assert result == []

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_disaggregate_with_shortage(self, mock_load):
        """When outflows exceed opening+inflows, sub-buckets have has_shortage=True."""
        node = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 13),
            opening_stock=Decimal("0"),
            inflows=Decimal("0"),
            outflows=Decimal("7"),
            closing_stock=Decimal("-7"),
        )
        mock_load.return_value = [node]

        result = self.bridge.disaggregate(self.series_id, "week", "day", self.db)
        assert len(result) == 7
        # Each day: outflows=1, inflows=0, so closing goes -1 each day
        assert result[0].closing_stock == Decimal("-1")
        assert result[0].has_shortage is True
        assert result[0].shortage_qty == Decimal("1")

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_disaggregate_month_to_week(self, mock_load):
        """Monthly node disaggregated to weekly buckets."""
        node = _make_node(
            time_grain="month",
            time_span_start=date(2025, 3, 1),   # Saturday
            time_span_end=date(2025, 4, 1),
            opening_stock=Decimal("100"),
            inflows=Decimal("310"),
            outflows=Decimal("155"),
            closing_stock=Decimal("255"),
        )
        mock_load.return_value = [node]

        result = self.bridge.disaggregate(self.series_id, "month", "week", self.db)
        assert len(result) > 0
        assert all(b.time_grain == "week" for b in result)
        assert all(b.approximated is True for b in result)
        # Results sorted by time_span_start
        starts = [b.time_span_start for b in result]
        assert starts == sorted(starts)

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_disaggregate_month_to_day(self, mock_load):
        """Monthly node disaggregated to daily buckets."""
        node = _make_node(
            time_grain="month",
            time_span_start=date(2025, 2, 1),
            time_span_end=date(2025, 3, 1),  # 28 days in Feb 2025
            opening_stock=Decimal("0"),
            inflows=Decimal("28"),
            outflows=Decimal("0"),
        )
        mock_load.return_value = [node]

        result = self.bridge.disaggregate(self.series_id, "month", "day", self.db)
        assert len(result) == 28  # Feb 2025 has 28 days
        assert result[0].time_span_start == date(2025, 2, 1)
        assert result[-1].time_span_end == date(2025, 3, 1)

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_disaggregate_none_inflows_outflows_default_zero(self, mock_load):
        """None inflows/outflows treated as 0."""
        node = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 13),
            opening_stock=None,
            inflows=None,
            outflows=None,
        )
        mock_load.return_value = [node]

        result = self.bridge.disaggregate(self.series_id, "week", "day", self.db)
        assert len(result) == 7
        for b in result:
            assert b.inflows == Decimal("0")
            assert b.outflows == Decimal("0")

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_disaggregate_results_sorted(self, mock_load):
        """Multiple source nodes produce results sorted by time_span_start."""
        nodes = [
            _make_node(
                time_grain="week",
                time_span_start=date(2025, 1, 13),
                time_span_end=date(2025, 1, 20),
                opening_stock=Decimal("0"),
                inflows=Decimal("7"),
                outflows=Decimal("0"),
            ),
            _make_node(
                time_grain="week",
                time_span_start=date(2025, 1, 6),
                time_span_end=date(2025, 1, 13),
                opening_stock=Decimal("0"),
                inflows=Decimal("7"),
                outflows=Decimal("0"),
            ),
        ]
        mock_load.return_value = nodes

        result = self.bridge.disaggregate(self.series_id, "week", "day", self.db)
        starts = [b.time_span_start for b in result]
        assert starts == sorted(starts)

    @patch.object(TemporalBridge, "_load_series_nodes")
    def test_sub_buckets_zero_produces_continue(self, mock_load):
        """When span_start == span_end, no sub-buckets are enumerated → skip."""
        node = _make_node(
            time_grain="week",
            time_span_start=date(2025, 1, 6),
            time_span_end=date(2025, 1, 6),  # zero-length span
        )
        mock_load.return_value = [node]

        result = self.bridge.disaggregate(self.series_id, "week", "day", self.db)
        assert result == []


# ---------------------------------------------------------------------------
# TemporalBridge._load_series_nodes
# ---------------------------------------------------------------------------


class TestLoadSeriesNodes:
    def test_delegates_to_graph_store(self):
        bridge = TemporalBridge()
        db = MagicMock()
        series_id = uuid4()

        mock_store = MagicMock()
        expected_nodes = [_make_node()]
        mock_store.get_nodes_by_series.return_value = expected_nodes

        with patch(
            "ootils_core.engine.kernel.temporal.bridge.GraphStore",
            return_value=mock_store,
        ):
            result = bridge._load_series_nodes(series_id, db)

        assert result == expected_nodes
        mock_store.get_nodes_by_series.assert_called_once_with(series_id)


# ---------------------------------------------------------------------------
# TemporalBridge._enumerate_sub_buckets
# ---------------------------------------------------------------------------


class TestEnumerateSubBuckets:
    def setup_method(self):
        self.bridge = TemporalBridge()

    def test_daily_sub_buckets_for_week(self):
        result = self.bridge._enumerate_sub_buckets(
            date(2025, 1, 6), date(2025, 1, 13), "day"
        )
        assert len(result) == 7
        assert result[0] == (date(2025, 1, 6), date(2025, 1, 7))
        assert result[6] == (date(2025, 1, 12), date(2025, 1, 13))

    def test_weekly_sub_buckets_for_month(self):
        result = self.bridge._enumerate_sub_buckets(
            date(2025, 3, 1), date(2025, 4, 1), "week"
        )
        assert len(result) > 0
        # First sub-bucket starts at the Monday of the week containing March 1
        assert result[0][0] == date(2025, 2, 24)  # Monday before March 1 (Sat)
        # Last sub-bucket end is clamped to span_end
        assert result[-1][1] <= date(2025, 4, 1)

    def test_empty_span_returns_empty(self):
        result = self.bridge._enumerate_sub_buckets(
            date(2025, 1, 6), date(2025, 1, 6), "day"
        )
        assert result == []

    def test_partial_week_clamped(self):
        """Sub-bucket end is clamped when it exceeds span_end."""
        # 3-day span, weekly sub-bucket
        result = self.bridge._enumerate_sub_buckets(
            date(2025, 1, 6), date(2025, 1, 9), "week"
        )
        assert len(result) == 1
        assert result[0] == (date(2025, 1, 6), date(2025, 1, 9))

    def test_month_sub_buckets(self):
        """Month sub-bucket within a quarter-like span."""
        result = self.bridge._enumerate_sub_buckets(
            date(2025, 1, 1), date(2025, 4, 1), "month"
        )
        assert len(result) == 3
        assert result[0] == (date(2025, 1, 1), date(2025, 2, 1))
        assert result[1] == (date(2025, 2, 1), date(2025, 3, 1))
        assert result[2] == (date(2025, 3, 1), date(2025, 4, 1))
