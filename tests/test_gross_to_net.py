"""
Unit tests for GrossToNetCalculator — APICS CPIM standard scenarios.

These tests use a mock database (no live Postgres required) and validate
the core POH-chain algorithm against textbook APICS examples.

Test categories:
  1. POH chain fundamentals (basic arithmetic, safety stock)
  2. Time bucket creation and alignment
  3. Net requirements generation
  4. Multi-bucket chain propagation
  5. LLC-dependent demand integration
  6. Edge cases (zero demand, zero on-hand, no supply)
  7. apply_planned_orders re-chaining after lot-sizing
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

from ootils_core.engine.mrp.gross_to_net import (
    BucketRecord,
    GrossToNetCalculator,
    TimeBucket,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Fixed UUIDs for deterministic tests
ITEM_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
LOC_1 = UUID("11111111-1111-1111-1111-111111111111")
SCENARIO = UUID("00000000-0000-0000-0000-000000000001")


def _make_buckets(start: date, weeks: int) -> list[TimeBucket]:
    """Create weekly buckets starting Monday of start's week."""
    monday = start - timedelta(days=start.weekday())
    buckets = []
    for i in range(weeks):
        bstart = monday + timedelta(weeks=i)
        bend = bstart + timedelta(days=7)
        buckets.append(TimeBucket(sequence=i, start=bstart, end=bend, grain="week"))
    return buckets


def _make_record(
    seq: int,
    period_start: date,
    period_end: date,
    gr: Decimal = Decimal("0"),
    sr: Decimal = Decimal("0"),
    poh: Decimal = Decimal("0"),
    nr: Decimal = Decimal("0"),
    por: Decimal = Decimal("0"),
    porel: Decimal = Decimal("0"),
    poh_after: Decimal = Decimal("0"),
    ss_violation: bool = False,
    shortage: Decimal = Decimal("0"),
) -> BucketRecord:
    """Quick BucketRecord factory for test assertions."""
    return BucketRecord(
        bucket_id=uuid4(),
        item_id=ITEM_A,
        location_id=LOC_1,
        period_start=period_start,
        period_end=period_end,
        bucket_sequence=seq,
        gross_requirements=gr,
        scheduled_receipts=sr,
        projected_on_hand=poh,
        net_requirements=nr,
        planned_order_receipts=por,
        planned_order_releases=porel,
        projected_on_hand_after=poh_after,
        has_shortage=ss_violation,
        shortage_qty=shortage,
    )


def _mock_db():
    """Return a mock psycopg connection that returns empty result sets."""
    db = MagicMock()
    # Default: .execute().fetchone() returns {"qty": Decimal("0")}
    cursor_mock = MagicMock()
    cursor_mock.fetchone.return_value = {"qty": Decimal("0")}
    cursor_mock.fetchall.return_value = []
    db.execute.return_value = cursor_mock
    return db


# ---------------------------------------------------------------------------
# Test: Time Bucket Creation
# ---------------------------------------------------------------------------

class TestTimeBucketCreation(unittest.TestCase):
    """Test create_time_buckets alignment and boundaries."""

    def test_weekly_buckets_start_monday(self):
        """Weekly buckets should start on Monday (ISO week alignment)."""
        db = _mock_db()
        calc = GrossToNetCalculator(db, SCENARIO)

        # Wednesday, April 16, 2026 — snaps back to Monday April 13
        # Horizon: Apr 16 + 28 days = May 14
        # Snapped start: Apr 13, so effective span = Apr 13 → May 14 = 31 days
        # That's 5 weekly buckets (4 full + 1 partial)
        start = date(2026, 4, 16)
        buckets = calc.create_time_buckets(start, 28, grain="week")

        # First bucket should start on Monday April 13
        self.assertEqual(buckets[0].start, date(2026, 4, 13))
        # Each bucket should be 7 days (except possibly the last)
        self.assertEqual(buckets[0].end, date(2026, 4, 20))
        # 5 buckets: Apr 13-20, 20-27, 27-May4, May4-11, May11-14
        self.assertEqual(len(buckets), 5)

    def test_daily_buckets(self):
        """Daily buckets: each bucket is one day."""
        db = _mock_db()
        calc = GrossToNetCalculator(db, SCENARIO)
        start = date(2026, 4, 13)
        buckets = calc.create_time_buckets(start, 7, grain="day")
        self.assertEqual(len(buckets), 7)
        self.assertEqual(buckets[0].start, date(2026, 4, 13))
        self.assertEqual(buckets[0].end, date(2026, 4, 14))

    def test_monthly_buckets(self):
        """Monthly buckets: each bucket is one calendar month."""
        db = _mock_db()
        calc = GrossToNetCalculator(db, SCENARIO)
        start = date(2026, 1, 1)
        buckets = calc.create_time_buckets(start, 90, grain="month")
        self.assertGreaterEqual(len(buckets), 3)
        self.assertEqual(buckets[0].start, date(2026, 1, 1))
        self.assertEqual(buckets[0].end, date(2026, 2, 1))

    def test_horizon_clipping(self):
        """Last bucket should not exceed the horizon."""
        db = _mock_db()
        calc = GrossToNetCalculator(db, SCENARIO)
        start = date(2026, 4, 13)  # Monday
        buckets = calc.create_time_buckets(start, 10, grain="week")
        # Horizon end = start + 10 days = Apr 23
        horizon_end = start + timedelta(days=10)
        for b in buckets:
            self.assertLessEqual(b.start, horizon_end)
            self.assertLessEqual(b.end, horizon_end)

    def test_start_already_monday(self):
        """If start is already Monday, no shift needed."""
        db = _mock_db()
        calc = GrossToNetCalculator(db, SCENARIO)
        start = date(2026, 4, 13)  # Monday
        buckets = calc.create_time_buckets(start, 14, grain="week")
        self.assertEqual(buckets[0].start, date(2026, 4, 13))


# ---------------------------------------------------------------------------
# Test: POH Chain — APICS Standard Scenarios
# ---------------------------------------------------------------------------

class TestPohChainAPICS(unittest.TestCase):
    """
    Test the core POH chain algorithm using APICS CPIM textbook scenarios.

    Reference: APICS CPIM "Projected On-Hand" calculation
      POH(t) = POH(t-1) + SR(t) - GR(t)
      NR(t)  = max(0, SS - POH(t))
    """

    def setUp(self):
        self.db = _mock_db()
        self.calc = GrossToNetCalculator(self.db, SCENARIO)
        self.buckets = _make_buckets(date(2026, 4, 13), 8)

    def _patch_initial_on_hand(self, qty: Decimal):
        """Patch _get_initial_on_hand to return a fixed quantity."""
        self.calc._get_initial_on_hand = MagicMock(return_value=qty)

    def _patch_scheduled_receipts(self, sr_map: dict):
        """Patch _get_scheduled_receipts_map to return a fixed map."""
        self.calc._get_scheduled_receipts_map = MagicMock(return_value=sr_map)

    def _patch_gross_requirements(self, gr_map: dict):
        """Patch _build_gross_requirements_map to return a fixed map."""
        self.calc._build_gross_requirements_map = MagicMock(return_value=gr_map)

    def test_basic_poh_chain_no_ss(self):
        """
        Classic APICS example: on-hand=50, no SS, constant demand=30/week, no SR.

        Week 1: POH = 50 + 0 - 30 = 20, NR = 0
        Week 2: POH = 20 + 0 - 30 = -10, NR = 10 (SS=0, so NR=max(0,0-(-10))=10)
        """
        self._patch_initial_on_hand(Decimal("50"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("30"),
            self.buckets[1].start: Decimal("30"),
        })

        params = {"safety_stock_qty": Decimal("0")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:2], params, llc=0
        )

        # Week 1
        self.assertEqual(records[0].gross_requirements, Decimal("30"))
        self.assertEqual(records[0].scheduled_receipts, Decimal("0"))
        self.assertEqual(records[0].projected_on_hand, Decimal("20"))
        self.assertEqual(records[0].net_requirements, Decimal("0"))

        # Week 2
        self.assertEqual(records[1].gross_requirements, Decimal("30"))
        self.assertEqual(records[1].projected_on_hand, Decimal("-10"))
        self.assertEqual(records[1].net_requirements, Decimal("10"))

    def test_poh_with_safety_stock(self):
        """
        APICS with safety stock: on-hand=50, SS=20, demand=40/week.

        Week 1: POH = 50 + 0 - 40 = 10 < SS(20) → NR = 20 - 10 = 10
        Week 2: POH = 10 + 0 - 40 = -30 < SS(20) → NR = 20 - (-30) = 50
        """
        self._patch_initial_on_hand(Decimal("50"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("40"),
            self.buckets[1].start: Decimal("40"),
        })

        params = {"safety_stock_qty": Decimal("20")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:2], params, llc=0
        )

        # Week 1
        self.assertEqual(records[0].projected_on_hand, Decimal("10"))
        self.assertEqual(records[0].net_requirements, Decimal("10"))
        self.assertTrue(records[0].has_shortage)

        # Week 2
        self.assertEqual(records[1].projected_on_hand, Decimal("-30"))
        self.assertEqual(records[1].net_requirements, Decimal("50"))
        self.assertTrue(records[1].has_shortage)

    def test_poh_with_scheduled_receipts(self):
        """
        APICS with scheduled receipts: on-hand=20, SS=10, demand=50/week,
        SR=100 in week 2.

        Week 1: POH = 20 + 0 - 50 = -30 → NR = 10 - (-30) = 40
        Week 2: POH = -30 + 100 - 50 = 20 ≥ SS(10) → NR = 0
        """
        self._patch_initial_on_hand(Decimal("20"))
        self._patch_scheduled_receipts({
            self.buckets[1].start: Decimal("100"),
        })
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("50"),
            self.buckets[1].start: Decimal("50"),
        })

        params = {"safety_stock_qty": Decimal("10")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:2], params, llc=0
        )

        # Week 1
        self.assertEqual(records[0].projected_on_hand, Decimal("-30"))
        self.assertEqual(records[0].net_requirements, Decimal("40"))

        # Week 2
        self.assertEqual(records[1].projected_on_hand, Decimal("20"))
        self.assertEqual(records[1].net_requirements, Decimal("0"))

    def test_ss_covers_demand_no_nr(self):
        """
        When on-hand covers demand + SS: no net requirements.

        on-hand=100, SS=10, demand=30/week
        Week 1: POH = 100 - 30 = 70 ≥ SS(10) → NR = 0
        Week 2: POH = 70 - 30 = 40 ≥ SS(10) → NR = 0
        Week 3: POH = 40 - 30 = 10 = SS → NR = 0 (exactly at SS)
        """
        self._patch_initial_on_hand(Decimal("100"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("30"),
            self.buckets[1].start: Decimal("30"),
            self.buckets[2].start: Decimal("30"),
        })

        params = {"safety_stock_qty": Decimal("10")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:3], params, llc=0
        )

        self.assertEqual(records[0].projected_on_hand, Decimal("70"))
        self.assertEqual(records[0].net_requirements, Decimal("0"))
        self.assertFalse(records[0].has_shortage)

        self.assertEqual(records[1].projected_on_hand, Decimal("40"))
        self.assertEqual(records[1].net_requirements, Decimal("0"))

        self.assertEqual(records[2].projected_on_hand, Decimal("10"))
        self.assertEqual(records[2].net_requirements, Decimal("0"))
        # POH == SS exactly: not a shortage
        self.assertFalse(records[2].has_shortage)

    def test_zero_demand_zero_sr(self):
        """
        Zero demand, zero SR: POH stays at on-hand, no net requirements.

        on-hand=50, SS=10, no demand.
        Week 1-4: POH = 50, NR = 0
        """
        self._patch_initial_on_hand(Decimal("50"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({})

        params = {"safety_stock_qty": Decimal("10")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:4], params, llc=0
        )

        for rec in records:
            self.assertEqual(rec.gross_requirements, Decimal("0"))
            self.assertEqual(rec.scheduled_receipts, Decimal("0"))
            self.assertEqual(rec.projected_on_hand, Decimal("50"))
            self.assertEqual(rec.net_requirements, Decimal("0"))
            self.assertFalse(rec.has_shortage)

    def test_cpim_textbook_scenario(self):
        """
        Classic APICS CPIM textbook scenario:
        On-hand = 23, SS = 0, LT = 2 weeks, L4L

        Week 1:  GR=20  SR=0   POH=23-20=3   NR=0
        Week 2:  GR=25  SR=0   POH=3-25=-22   NR=22
        Week 3:  GR=30  SR=0   POH=-22-30=-52  NR=52
        Week 4:  GR=15  SR=0   POH=-52-15=-67  NR=67
        """
        self._patch_initial_on_hand(Decimal("23"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("20"),
            self.buckets[1].start: Decimal("25"),
            self.buckets[2].start: Decimal("30"),
            self.buckets[3].start: Decimal("15"),
        })

        params = {"safety_stock_qty": Decimal("0")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:4], params, llc=0
        )

        self.assertEqual(records[0].projected_on_hand, Decimal("3"))
        self.assertEqual(records[0].net_requirements, Decimal("0"))

        self.assertEqual(records[1].projected_on_hand, Decimal("-22"))
        self.assertEqual(records[1].net_requirements, Decimal("22"))

        self.assertEqual(records[2].projected_on_hand, Decimal("-52"))
        self.assertEqual(records[2].net_requirements, Decimal("52"))

        self.assertEqual(records[3].projected_on_hand, Decimal("-67"))
        self.assertEqual(records[3].net_requirements, Decimal("67"))

    def test_scheduled_receipts_restore_poh(self):
        """
        SR that brings POH above SS eliminates net requirements.

        on-hand=5, SS=20, demand=10/week, SR=50 in week 2.

        Week 1: POH = 5 + 0 - 10 = -5 → NR = 20 - (-5) = 25
        Week 2: POH = -5 + 50 - 10 = 35 ≥ SS(20) → NR = 0
        Week 3: POH = 35 - 10 = 25 ≥ SS(20) → NR = 0
        """
        self._patch_initial_on_hand(Decimal("5"))
        self._patch_scheduled_receipts({
            self.buckets[1].start: Decimal("50"),
        })
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("10"),
            self.buckets[1].start: Decimal("10"),
            self.buckets[2].start: Decimal("10"),
        })

        params = {"safety_stock_qty": Decimal("20")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:3], params, llc=0
        )

        self.assertEqual(records[0].projected_on_hand, Decimal("-5"))
        self.assertEqual(records[0].net_requirements, Decimal("25"))

        self.assertEqual(records[1].projected_on_hand, Decimal("35"))
        self.assertEqual(records[1].net_requirements, Decimal("0"))

        self.assertEqual(records[2].projected_on_hand, Decimal("25"))
        self.assertEqual(records[2].net_requirements, Decimal("0"))

    def test_dependent_demand_llc_gt_0(self):
        """
        When llc > 0 and dependent_demand is provided, it overrides
        forecast/DB demand as gross requirements.

        on-hand=0, SS=0, dependent demand: week1=100, week2=50
        Week 1: GR=100, POH=-100, NR=100 (SS-PAB = 0-(-100) = 100)
        Week 2: GR=50, POH=-150, NR=150 (SS-PAB = 0-(-150) = 150)

        Note: With SS=0, NR = |PAB| when PAB < 0. The NR accumulates
        because the POH chain carries forward — you need 150 total units
        to bring PAB back to 0 (safety stock).
        """
        self._patch_initial_on_hand(Decimal("0"))
        self._patch_scheduled_receipts({})

        dep_demand = {
            self.buckets[0].start: Decimal("100"),
            self.buckets[1].start: Decimal("50"),
        }

        params = {"safety_stock_qty": Decimal("0")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:2], params,
            dependent_demand=dep_demand, llc=1
        )

        self.assertEqual(records[0].gross_requirements, Decimal("100"))
        self.assertEqual(records[0].projected_on_hand, Decimal("-100"))
        self.assertEqual(records[0].net_requirements, Decimal("100"))
        self.assertEqual(records[0].llc, 1)

        self.assertEqual(records[1].gross_requirements, Decimal("50"))
        self.assertEqual(records[1].projected_on_hand, Decimal("-150"))
        # NR = max(0, SS - PAB) = max(0, 0 - (-150)) = 150
        self.assertEqual(records[1].net_requirements, Decimal("150"))

    def test_consumed_forecast_overrides_db(self):
        """
        When consumed_forecast is provided for LLC=0, it's used as GR
        instead of querying the database.
        """
        self._patch_initial_on_hand(Decimal("100"))
        self._patch_scheduled_receipts({})

        consumed = {
            self.buckets[0].start: Decimal("40"),
            self.buckets[1].start: Decimal("60"),
        }

        params = {"safety_stock_qty": Decimal("10")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:2], params,
            consumed_forecast=consumed, llc=0
        )

        self.assertEqual(records[0].gross_requirements, Decimal("40"))
        self.assertEqual(records[1].gross_requirements, Decimal("60"))

        self.assertEqual(records[0].projected_on_hand, Decimal("60"))
        self.assertEqual(records[1].projected_on_hand, Decimal("0"))
        # Week 2: POH=0 < SS=10 → NR=10
        self.assertEqual(records[1].net_requirements, Decimal("10"))

    def test_multi_period_sr_chain(self):
        """
        Multiple scheduled receipts across the horizon.

        on-hand=10, SS=5, demand=30/week
        SR: week1=20, week3=40

        Week 1: POH = 10 + 20 - 30 = 0  < SS(5) → NR = 5
        Week 2: POH = 0 + 0 - 30 = -30 < SS(5) → NR = 35
        Week 3: POH = -30 + 40 - 30 = -20 < SS(5) → NR = 25
        Week 4: POH = -20 + 0 - 30 = -50 < SS(5) → NR = 55
        """
        self._patch_initial_on_hand(Decimal("10"))
        self._patch_scheduled_receipts({
            self.buckets[0].start: Decimal("20"),
            self.buckets[2].start: Decimal("40"),
        })
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("30"),
            self.buckets[1].start: Decimal("30"),
            self.buckets[2].start: Decimal("30"),
            self.buckets[3].start: Decimal("30"),
        })

        params = {"safety_stock_qty": Decimal("5")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:4], params, llc=0
        )

        self.assertEqual(records[0].projected_on_hand, Decimal("0"))
        self.assertEqual(records[0].net_requirements, Decimal("5"))

        self.assertEqual(records[1].projected_on_hand, Decimal("-30"))
        self.assertEqual(records[1].net_requirements, Decimal("35"))

        self.assertEqual(records[2].projected_on_hand, Decimal("-20"))
        self.assertEqual(records[2].net_requirements, Decimal("25"))

        self.assertEqual(records[3].projected_on_hand, Decimal("-50"))
        self.assertEqual(records[3].net_requirements, Decimal("55"))


# ---------------------------------------------------------------------------
# Test: apply_planned_orders (re-chaining after lot-sizing)
# ---------------------------------------------------------------------------

class TestApplyPlannedOrders(unittest.TestCase):
    """
    Test the apply_planned_orders re-chaining step.

    After lot-sizing sets planned_order_receipts on each BucketRecord,
    call apply_planned_orders to compute projected_on_hand_after:

        POH_after(t) = PAB(t) + POR(t)
        PAB(t+1)     = POH_after(t) + SR(t+1) - GR(t+1)
    """

    def test_apply_por_single_bucket(self):
        """Single bucket: POH_after = PAB + POR."""
        buckets = _make_buckets(date(2026, 4, 13), 1)
        records = [
            BucketRecord(
                bucket_id=uuid4(), item_id=ITEM_A, location_id=LOC_1,
                period_start=buckets[0].start, period_end=buckets[0].end,
                bucket_sequence=0,
                gross_requirements=Decimal("30"),
                scheduled_receipts=Decimal("0"),
                projected_on_hand=Decimal("20"),
                net_requirements=Decimal("0"),
                planned_order_receipts=Decimal("50"),
                planned_order_releases=Decimal("0"),
                projected_on_hand_after=Decimal("0"),
            )
        ]

        result = GrossToNetCalculator.apply_planned_orders(records)

        self.assertEqual(result[0].projected_on_hand_after, Decimal("70"))
        # PAB(20) + POR(50) = 70

    def test_apply_por_chain(self):
        """
        Multi-bucket chain with POR:
        W1: PAB=20, POR=50 → POH_after=70
        W2: PAB recalculated = 70 + 0 - 30 = 40, POR=0 → POH_after=40
        W3: PAB recalculated = 40 + 0 - 40 = 0, POR=100 → POH_after=100
        """
        buckets = _make_buckets(date(2026, 4, 13), 3)
        records = [
            BucketRecord(
                bucket_id=uuid4(), item_id=ITEM_A, location_id=LOC_1,
                period_start=buckets[0].start, period_end=buckets[0].end,
                bucket_sequence=0,
                gross_requirements=Decimal("30"),
                scheduled_receipts=Decimal("0"),
                projected_on_hand=Decimal("20"),
                net_requirements=Decimal("0"),
                planned_order_receipts=Decimal("50"),
                projected_on_hand_after=Decimal("0"),
            ),
            BucketRecord(
                bucket_id=uuid4(), item_id=ITEM_A, location_id=LOC_1,
                period_start=buckets[1].start, period_end=buckets[1].end,
                bucket_sequence=1,
                gross_requirements=Decimal("30"),
                scheduled_receipts=Decimal("0"),
                projected_on_hand=Decimal("-10"),  # Original PAB (before POR)
                net_requirements=Decimal("10"),
                planned_order_receipts=Decimal("0"),
                projected_on_hand_after=Decimal("0"),
            ),
            BucketRecord(
                bucket_id=uuid4(), item_id=ITEM_A, location_id=LOC_1,
                period_start=buckets[2].start, period_end=buckets[2].end,
                bucket_sequence=2,
                gross_requirements=Decimal("40"),
                scheduled_receipts=Decimal("0"),
                projected_on_hand=Decimal("-50"),
                net_requirements=Decimal("50"),
                planned_order_receipts=Decimal("100"),
                projected_on_hand_after=Decimal("0"),
            ),
        ]

        result = GrossToNetCalculator.apply_planned_orders(records)

        # W1: POH_after = 20 + 50 = 70
        self.assertEqual(result[0].projected_on_hand_after, Decimal("70"))

        # W2: PAB re-chained = 70 + 0 - 30 = 40, POR=0 → after = 40
        self.assertEqual(result[1].projected_on_hand, Decimal("40"))
        self.assertEqual(result[1].projected_on_hand_after, Decimal("40"))

        # W3: PAB re-chained = 40 + 0 - 40 = 0, POR=100 → after = 100
        self.assertEqual(result[2].projected_on_hand, Decimal("0"))
        self.assertEqual(result[2].projected_on_hand_after, Decimal("100"))

    def test_apply_por_empty_records(self):
        """apply_planned_orders on empty list returns empty."""
        result = GrossToNetCalculator.apply_planned_orders([])
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# Test: Edge Cases
# ---------------------------------------------------------------------------

class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def setUp(self):
        self.db = _mock_db()
        self.calc = GrossToNetCalculator(self.db, SCENARIO)
        self.buckets = _make_buckets(date(2026, 4, 13), 4)

    def _patch_initial_on_hand(self, qty: Decimal):
        self.calc._get_initial_on_hand = MagicMock(return_value=qty)

    def _patch_scheduled_receipts(self, sr_map: dict):
        self.calc._get_scheduled_receipts_map = MagicMock(return_value=sr_map)

    def _patch_gross_requirements(self, gr_map: dict):
        self.calc._build_gross_requirements_map = MagicMock(return_value=gr_map)

    def test_zero_on_hand_zero_ss_zero_demand(self):
        """All zeros: no demand, no supply, no stock → POH stays 0, NR=0."""
        self._patch_initial_on_hand(Decimal("0"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({})

        params = {"safety_stock_qty": Decimal("0")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:2], params, llc=0
        )

        for rec in records:
            self.assertEqual(rec.projected_on_hand, Decimal("0"))
            self.assertEqual(rec.net_requirements, Decimal("0"))

    def test_very_large_ss(self):
        """
        Safety stock larger than on-hand → immediate net requirement.

        on-hand=5, SS=100, demand=10
        Week 1: POH = 5 - 10 = -5, NR = 100 - (-5) = 105
        """
        self._patch_initial_on_hand(Decimal("5"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("10"),
        })

        params = {"safety_stock_qty": Decimal("100")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:1], params, llc=0
        )

        self.assertEqual(records[0].projected_on_hand, Decimal("-5"))
        self.assertEqual(records[0].net_requirements, Decimal("105"))

    def test_sr_equals_gr(self):
        """
        When SR exactly equals GR: POH unchanged from previous.

        on-hand=50, SS=10, SR=30, GR=30
        Week 1: POH = 50 + 30 - 30 = 50 ≥ SS → NR = 0
        """
        self._patch_initial_on_hand(Decimal("50"))
        self._patch_scheduled_receipts({
            self.buckets[0].start: Decimal("30"),
        })
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("30"),
        })

        params = {"safety_stock_qty": Decimal("10")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:1], params, llc=0
        )

        self.assertEqual(records[0].projected_on_hand, Decimal("50"))
        self.assertEqual(records[0].net_requirements, Decimal("0"))
        self.assertFalse(records[0].has_shortage)

    def test_partial_week_demand(self):
        """
        Demand only in some weeks: POH accumulates in zero-demand weeks.

        on-hand=100, SS=0, GR: W1=80, W2=0, W3=0, W4=50
        W1: POH=100-80=20, NR=0
        W2: POH=20-0=20, NR=0
        W3: POH=20-0=20, NR=0
        W4: POH=20-50=-30, NR=30
        """
        self._patch_initial_on_hand(Decimal("100"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("80"),
            # W2, W3: no demand
            self.buckets[3].start: Decimal("50"),
        })

        params = {"safety_stock_qty": Decimal("0")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:4], params, llc=0
        )

        self.assertEqual(records[0].projected_on_hand, Decimal("20"))
        self.assertEqual(records[1].projected_on_hand, Decimal("20"))
        self.assertEqual(records[2].projected_on_hand, Decimal("20"))
        self.assertEqual(records[3].projected_on_hand, Decimal("-30"))
        self.assertEqual(records[3].net_requirements, Decimal("30"))

    def test_negative_poh_accumulates(self):
        """
        Negative POH accumulates across periods (no supply coming).

        on-hand=0, SS=0, demand=10/week for 4 weeks.
        W1: POH=-10, W2: POH=-20, W3: POH=-30, W4: POH=-40
        """
        self._patch_initial_on_hand(Decimal("0"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("10"),
            self.buckets[1].start: Decimal("10"),
            self.buckets[2].start: Decimal("10"),
            self.buckets[3].start: Decimal("10"),
        })

        params = {"safety_stock_qty": Decimal("0")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:4], params, llc=0
        )

        expected_poh = [Decimal("-10"), Decimal("-20"), Decimal("-30"), Decimal("-40")]
        for i, expected in enumerate(expected_poh):
            self.assertEqual(records[i].projected_on_hand, expected)

    def test_safety_stock_exactly_met(self):
        """
        When POH exactly equals SS: no shortage, NR=0.

        on-hand=30, SS=10, demand=20
        W1: POH=30-20=10=SS → NR=0, no shortage
        """
        self._patch_initial_on_hand(Decimal("30"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("20"),
        })

        params = {"safety_stock_qty": Decimal("10")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:1], params, llc=0
        )

        self.assertEqual(records[0].projected_on_hand, Decimal("10"))
        self.assertEqual(records[0].net_requirements, Decimal("0"))
        self.assertFalse(records[0].has_shortage)

    def test_decimal_precision(self):
        """
        Decimal precision is preserved through the POH chain.

        on-hand=100.5, SS=10.25, demand=33.75
        W1: POH = 100.5 - 33.75 = 66.75 ≥ 10.25 → NR=0
        W2: POH = 66.75 - 33.75 = 33 ≥ 10.25 → NR=0
        W3: POH = 33 - 33.75 = -0.75 < 10.25 → NR = 10.25 - (-0.75) = 11
        """
        self._patch_initial_on_hand(Decimal("100.5"))
        self._patch_scheduled_receipts({})
        self._patch_gross_requirements({
            self.buckets[0].start: Decimal("33.75"),
            self.buckets[1].start: Decimal("33.75"),
            self.buckets[2].start: Decimal("33.75"),
        })

        params = {"safety_stock_qty": Decimal("10.25")}
        records = self.calc.calculate(
            ITEM_A, LOC_1, self.buckets[:3], params, llc=0
        )

        self.assertEqual(records[0].projected_on_hand, Decimal("66.75"))
        self.assertEqual(records[1].projected_on_hand, Decimal("33"))
        self.assertEqual(records[2].projected_on_hand, Decimal("-0.75"))
        self.assertEqual(records[2].net_requirements, Decimal("11"))


# ---------------------------------------------------------------------------
# Test: DB Integration (mocked)
# ---------------------------------------------------------------------------

class TestDBIntegration(unittest.TestCase):
    """Test that DB queries are called correctly (with mocks)."""

    def test_get_initial_on_hand_with_location(self):
        """_get_initial_on_hand should query with location_id when provided."""
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = {"qty": Decimal("150")}
        calc = GrossToNetCalculator(db, SCENARIO)

        result = calc._get_initial_on_hand(ITEM_A, LOC_1)
        self.assertEqual(result, Decimal("150"))
        # Verify execute was called
        db.execute.assert_called()

    def test_get_initial_on_hand_without_location(self):
        """_get_initial_on_hand should work without location_id."""
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = {"qty": Decimal("200")}
        calc = GrossToNetCalculator(db, SCENARIO)

        result = calc._get_initial_on_hand(ITEM_A, None)
        self.assertEqual(result, Decimal("200"))

    def test_get_initial_on_hand_null_result(self):
        """_get_initial_on_hand returns 0 when no rows found."""
        db = _mock_db()
        db.execute.return_value.fetchone.return_value = None
        calc = GrossToNetCalculator(db, SCENARIO)

        result = calc._get_initial_on_hand(ITEM_A, LOC_1)
        self.assertEqual(result, Decimal("0"))


# ---------------------------------------------------------------------------
# Test: _coalesce_decimal helper
# ---------------------------------------------------------------------------

class TestCoalesceDecimal(unittest.TestCase):
    """Test the _coalesce_decimal static method."""

    def test_none_returns_default(self):
        result = GrossToNetCalculator._coalesce_decimal(None, Decimal("0"))
        self.assertEqual(result, Decimal("0"))

    def test_int_value(self):
        result = GrossToNetCalculator._coalesce_decimal(10, Decimal("0"))
        self.assertEqual(result, Decimal("10"))

    def test_float_value(self):
        result = GrossToNetCalculator._coalesce_decimal(10.5, Decimal("0"))
        self.assertEqual(result, Decimal("10.5"))

    def test_decimal_value(self):
        result = GrossToNetCalculator._coalesce_decimal(Decimal("42.5"), Decimal("0"))
        self.assertEqual(result, Decimal("42.5"))

    def test_string_value(self):
        result = GrossToNetCalculator._coalesce_decimal("100", Decimal("0"))
        self.assertEqual(result, Decimal("100"))


# ---------------------------------------------------------------------------
# Test: _date_to_bucket_start helper
# ---------------------------------------------------------------------------

class TestDateToBucketStart(unittest.TestCase):
    """Test the _date_to_bucket_start static method."""

    def test_date_in_bucket(self):
        buckets = _make_buckets(date(2026, 4, 13), 4)
        # April 15 (Wednesday) falls in the week starting April 13
        result = GrossToNetCalculator._date_to_bucket_start(date(2026, 4, 15), buckets)
        self.assertEqual(result, date(2026, 4, 13))

    def test_date_before_horizon(self):
        buckets = _make_buckets(date(2026, 4, 13), 4)
        result = GrossToNetCalculator._date_to_bucket_start(date(2026, 4, 10), buckets)
        self.assertIsNone(result)

    def test_date_after_horizon(self):
        buckets = _make_buckets(date(2026, 4, 13), 4)
        # After last bucket end
        result = GrossToNetCalculator._date_to_bucket_start(date(2026, 5, 15), buckets)
        self.assertIsNone(result)

    def test_date_at_bucket_start(self):
        buckets = _make_buckets(date(2026, 4, 13), 4)
        result = GrossToNetCalculator._date_to_bucket_start(date(2026, 4, 13), buckets)
        self.assertEqual(result, date(2026, 4, 13))

    def test_date_at_bucket_end_exclusive(self):
        buckets = _make_buckets(date(2026, 4, 13), 4)
        # April 20 is the END of bucket 0, should map to bucket 1 start
        result = GrossToNetCalculator._date_to_bucket_start(date(2026, 4, 20), buckets)
        self.assertEqual(result, date(2026, 4, 20))


if __name__ == "__main__":
    unittest.main()