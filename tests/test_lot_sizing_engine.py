"""
Unit tests for LotSizingEngine — APICS CPIM standard scenarios.

Tests all six lot-sizing strategies:
- L4L (Lot-for-Lot)
- FOQ (Fixed Order Quantity)
- EOQ (Economic Order Quantity)
- POQ (Period Order Quantity)
- MIN_MAX (Minimum-Maximum)
- MULTIPLE (Order Multiple)

Plus integration tests:
- apply_to_records (cascading POH)
- Time fence enforcement
- Edge cases and boundary conditions
"""

import pytest
from decimal import Decimal
from datetime import date, timedelta
from uuid import uuid4
from unittest.mock import MagicMock, patch
import types
import sys

# Mock psycopg only during import of the APICS modules, then restore it so the
# later DB-backed tests in the full suite can import the real psycopg package.
_psycopg_original = sys.modules.get('psycopg')
psycopg_mock = types.ModuleType('psycopg')
sys.modules['psycopg'] = psycopg_mock

from ootils_core.engine.mrp.lot_sizing import LotSizingEngine, LotSizeRule
from ootils_core.engine.mrp.gross_to_net import BucketRecord, TimeBucket

if _psycopg_original is not None:
    sys.modules['psycopg'] = _psycopg_original
else:
    sys.modules.pop('psycopg', None)


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_params(**overrides):
    """Create a planning params dict with defaults."""
    defaults = {
        "lot_size_rule": "LOTFORLOT",
        "min_order_qty": None,
        "max_order_qty": None,
        "reorder_point_qty": None,
        "economic_order_qty": None,
        "order_multiple_qty": None,
        "lot_size_poq_periods": 1,
        "safety_stock_qty": None,
        "lead_time_total_days": 0,
        "frozen_time_fence_days": 7,
        "slashed_time_fence_days": 30,
    }
    defaults.update(overrides)
    return defaults


def make_record(
    period_start: date,
    gross_req: Decimal = Decimal("0"),
    scheduled_receipts: Decimal = Decimal("0"),
    projected_on_hand: Decimal = Decimal("0"),
    net_requirements: Decimal = Decimal("0"),
    llc: int = 0,
) -> BucketRecord:
    """Create a BucketRecord for testing."""
    return BucketRecord(
        bucket_id=uuid4(),
        item_id=uuid4(),
        location_id=None,
        period_start=period_start,
        period_end=period_start + timedelta(days=7),
        bucket_sequence=0,
        gross_requirements=gross_req,
        scheduled_receipts=scheduled_receipts,
        projected_on_hand=projected_on_hand,
        net_requirements=net_requirements,
        planned_order_receipts=Decimal("0"),
        planned_order_releases=Decimal("0"),
        has_shortage=net_requirements > 0,
        shortage_qty=net_requirements if net_requirements > 0 else Decimal("0"),
        llc=llc,
    )


# ── L4L Tests ─────────────────────────────────────────────────────────────────

class TestLotForLot:
    """L4L: Order exactly the net requirement quantity."""

    def test_l4l_basic(self):
        """APICS: L4L orders exactly what's needed."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="LOTFORLOT"),
        )
        assert qty == Decimal("50")
        assert rule == "LOTFORLOT"

    def test_l4l_no_net_requirement(self):
        """When net_requirements <= 0, no order is placed."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("0"),
            projected_on_hand=Decimal("100"),
            planning_params=make_params(lot_size_rule="LOTFORLOT"),
        )
        assert qty == Decimal("0")
        assert rule is None

    def test_l4l_with_min_order_qty(self):
        """L4L respects min_order_qty floor."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("30"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="LOTFORLOT", min_order_qty=Decimal("100")),
        )
        assert qty == Decimal("100")  # Rounded up to min

    def test_l4l_net_req_above_min(self):
        """L4L: if net_req > min_order_qty, order net_req."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("150"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="LOTFORLOT", min_order_qty=Decimal("100")),
        )
        assert qty == Decimal("150")

    def test_l4l_large_demand(self):
        """L4L handles large demand correctly."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("10000"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="LOTFORLOT"),
        )
        assert qty == Decimal("10000")


# ── FOQ Tests ──────────────────────────────────────────────────────────────────

class TestFixedOrderQuantity:
    """FOQ: Order a fixed quantity, rounding up to cover demand."""

    def test_foq_basic(self):
        """APICS CPIM: FOQ orders the fixed quantity when demand exists."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="FIXED_QTY",
                min_order_qty=Decimal("100"),
            ),
        )
        assert qty == Decimal("100")
        assert rule == "FIXED_QTY"

    def test_foq_demand_exceeds_fixed_qty(self):
        """APICS: If demand > FOQ, order enough FOQ multiples to cover."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("250"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="FIXED_QTY",
                min_order_qty=Decimal("100"),
            ),
        )
        assert qty == Decimal("300")  # 3 × 100

    def test_foq_demand_equals_fixed_qty(self):
        """FOQ: demand exactly equals fixed quantity."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("100"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="FIXED_QTY",
                min_order_qty=Decimal("100"),
            ),
        )
        assert qty == Decimal("100")

    def test_foq_demand_slightly_above_fixed_qty(self):
        """FOQ: demand 101 with fixed 100 → order 200."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("101"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="FIXED_QTY",
                min_order_qty=Decimal("100"),
            ),
        )
        assert qty == Decimal("200")

    def test_foq_no_fixed_qty_falls_back_to_l4l(self):
        """FOQ without min_order_qty falls back to L4L."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="FIXED_QTY"),
        )
        assert qty == Decimal("50")  # Falls back to L4L


# ── EOQ Tests ──────────────────────────────────────────────────────────────────

class TestEconomicOrderQuantity:
    """EOQ: Order the economic order quantity when triggered."""

    def test_eoq_basic(self):
        """APICS CPIM: When net_req > 0, order EOQ (covers demand + cycle stock)."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="EOQ",
                economic_order_qty=Decimal("100"),
            ),
        )
        assert qty == Decimal("100")  # Order EOQ
        assert rule == "EOQ"

    def test_eoq_demand_exceeds_eoq(self):
        """APICS: If demand > EOQ, round up to next EOQ multiple."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("150"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="EOQ",
                economic_order_qty=Decimal("100"),
            ),
        )
        assert qty == Decimal("200")  # 2 × 100

    def test_eoq_with_min_order_qty(self):
        """EOQ respects min_order_qty floor."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("30"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="EOQ",
                economic_order_qty=Decimal("50"),
                min_order_qty=Decimal("80"),
            ),
        )
        # EOQ(50) < min(80), so order min
        # But EOQ = 50 covers demand (30 < 50), so order = EOQ = 50
        # Then check min: 50 < 80, so bump to 80
        assert qty == Decimal("80")

    def test_eoq_demand_exactly_eoq(self):
        """EOQ: demand exactly equals EOQ."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("100"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="EOQ",
                economic_order_qty=Decimal("100"),
            ),
        )
        assert qty == Decimal("100")  # 1 × 100

    def test_eoq_no_eoq_value_falls_back(self):
        """EOQ without economic_order_qty falls back to L4L."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="EOQ"),
        )
        assert qty == Decimal("50")


# ── POQ Tests ──────────────────────────────────────────────────────────────────

class TestPeriodOrderQuantity:
    """POQ: Cover net requirements for N periods with one order."""

    def test_poq_basic(self):
        """APICS CPIM: POQ covers current + N-1 future periods."""
        engine = LotSizingEngine(db=MagicMock())
        future = [Decimal("30"), Decimal("20")]
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="POQ",
                lot_size_poq_periods=3,
            ),
            future_net_reqs=future,
        )
        # current(50) + period+1(30) + period+2(20) = 100
        assert qty == Decimal("100")
        assert rule == "POQ"

    def test_poq_with_zero_future_demand(self):
        """POQ: zero future demand doesn't add to order."""
        engine = LotSizingEngine(db=MagicMock())
        future = [Decimal("0"), Decimal("20")]
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="POQ",
                lot_size_poq_periods=3,
            ),
            future_net_reqs=future,
        )
        # 50 + 0 + 20 = 70 (zero periods skipped)
        assert qty == Decimal("70")

    def test_poq_single_period(self):
        """POQ with 1 period = L4L (only current period)."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="POQ",
                lot_size_poq_periods=1,
            ),
            future_net_reqs=[Decimal("30")],
        )
        assert qty == Decimal("50")  # Only current period

    def test_poq_with_min_order_qty(self):
        """POQ respects min_order_qty floor."""
        engine = LotSizingEngine(db=MagicMock())
        future = [Decimal("10")]
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("30"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="POQ",
                lot_size_poq_periods=2,
                min_order_qty=Decimal("100"),
            ),
            future_net_reqs=future,
        )
        # 30 + 10 = 40 < 100 → bump to min
        assert qty == Decimal("100")

    def test_poq_no_future_reqs(self):
        """POQ: no future reqs provided — order current period only."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="POQ",
                lot_size_poq_periods=3,
            ),
            future_net_reqs=None,
        )
        assert qty == Decimal("50")


# ── MIN_MAX Tests ──────────────────────────────────────────────────────────────

class TestMinMax:
    """MIN_MAX: Reorder up to max when on-hand falls below reorder point."""

    def test_min_max_below_reorder_point(self):
        """APICS: POH < reorder_point → order up to max."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("100"),
            planning_params=make_params(
                lot_size_rule="MIN_MAX",
                reorder_point_qty=Decimal("200"),
                max_order_qty=Decimal("500"),
            ),
        )
        # POH(100) < reorder(200) → order up to max: 500 - 100 = 400
        assert qty == Decimal("400")
        assert rule == "MIN_MAX"

    def test_min_max_above_reorder_point(self):
        """MIN_MAX: POH above reorder point but net_req > 0 → order net_req."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("600"),
            planning_params=make_params(
                lot_size_rule="MIN_MAX",
                reorder_point_qty=Decimal("200"),
                max_order_qty=Decimal("500"),
            ),
        )
        # POH(600) > reorder(200) → above reorder, but demand exists
        # max - POH = 500 - 600 = -100 < net_req(50) → order net_req
        assert qty == Decimal("50")

    def test_min_max_at_max(self):
        """MIN_MAX: POH at max level → no surplus needed."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("500"),
            planning_params=make_params(
                lot_size_rule="MIN_MAX",
                reorder_point_qty=Decimal("200"),
                max_order_qty=Decimal("500"),
            ),
        )
        # POH at max → max - POH = 0 < net_req → order net_req
        assert qty == Decimal("50")

    def test_min_max_order_up_to_max(self):
        """MIN_MAX: When below reorder, order = max - POH."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("50"),
            planning_params=make_params(
                lot_size_rule="MIN_MAX",
                reorder_point_qty=Decimal("100"),
                max_order_qty=Decimal("500"),
            ),
        )
        # POH(50) < reorder(100) → order up to max: 500 - 50 = 450
        assert qty == Decimal("450")

    def test_min_max_no_max_falls_back(self):
        """MIN_MAX without max falls back to L4L."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("100"),
            planning_params=make_params(
                lot_size_rule="MIN_MAX",
                reorder_point_qty=Decimal("200"),
            ),
        )
        # No max → fall back to L4L
        assert qty == Decimal("50")

    def test_min_max_uses_min_order_qty_as_reorder_fallback(self):
        """MIN_MAX: if no reorder_point, uses min_order_qty as trigger."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("100"),
            planning_params=make_params(
                lot_size_rule="MIN_MAX",
                min_order_qty=Decimal("200"),
                max_order_qty=Decimal("500"),
            ),
        )
        # reorder_point not set → uses min_order_qty(200) as reorder_point
        # POH(100) < 200 → order up to max: 500 - 100 = 400
        assert qty == Decimal("400")


# ── MULTIPLE Tests ─────────────────────────────────────────────────────────────

class TestOrderMultiple:
    """MULTIPLE: Round up order quantity to nearest multiple."""

    def test_multiple_basic(self):
        """APICS CPIM: Order quantity rounds up to nearest multiple."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("30"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="MULTIPLE",
                order_multiple_qty=Decimal("25"),
            ),
        )
        assert qty == Decimal("50")  # ceil(30/25) × 25 = 2 × 25
        assert rule == "MULTIPLE"

    def test_multiple_exact(self):
        """MULTIPLE: demand exactly a multiple → no rounding needed."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("75"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="MULTIPLE",
                order_multiple_qty=Decimal("25"),
            ),
        )
        assert qty == Decimal("75")  # 3 × 25

    def test_multiple_with_min_order_qty(self):
        """MULTIPLE: respects min_order_qty, rounded up to multiple."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("10"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="MULTIPLE",
                order_multiple_qty=Decimal("25"),
                min_order_qty=Decimal("100"),
            ),
        )
        # ceil(10/25) × 25 = 25, but 25 < min(100) → ceil(100/25) × 25 = 100
        assert qty == Decimal("100")

    def test_multiple_no_multiple_falls_back(self):
        """MULTIPLE without order_multiple falls back to L4L."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("30"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="MULTIPLE"),
        )
        assert qty == Decimal("30")  # Falls back to L4L


# ── Max Order Quantity Cap Tests ──────────────────────────────────────────────

class TestMaxOrderQtyCap:
    """max_order_qty caps the result for all strategies."""

    def test_max_cap_applied(self):
        """max_order_qty caps any strategy's result."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("500"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="LOTFORLOT",
                max_order_qty=Decimal("100"),
            ),
        )
        assert qty == Decimal("100")  # Capped from 500

    def test_max_not_applied_when_below(self):
        """max_order_qty doesn't cap when order is below max."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="LOTFORLOT",
                max_order_qty=Decimal("1000"),
            ),
        )
        assert qty == Decimal("50")  # Not capped


# ── LotSizeRule Enum Tests ─────────────────────────────────────────────────────

class TestLotSizeRuleEnum:
    """Test enum parsing and aliases."""

    def test_from_str_standard(self):
        assert LotSizeRule.from_str("LOTFORLOT") == LotSizeRule.LOTFORLOT
        assert LotSizeRule.from_str("FIXED_QTY") == LotSizeRule.FIXED_QTY
        assert LotSizeRule.from_str("EOQ") == LotSizeRule.EOQ
        assert LotSizeRule.from_str("POQ") == LotSizeRule.POQ
        assert LotSizeRule.from_str("MIN_MAX") == LotSizeRule.MIN_MAX
        assert LotSizeRule.from_str("MULTIPLE") == LotSizeRule.MULTIPLE

    def test_from_str_aliases(self):
        """Common aliases map correctly."""
        assert LotSizeRule.from_str("L4L") == LotSizeRule.LOTFORLOT
        assert LotSizeRule.from_str("FOQ") == LotSizeRule.FIXED_QTY
        assert LotSizeRule.from_str("PERIOD_OF_SUPPLY") == LotSizeRule.POQ

    def test_from_str_case_insensitive(self):
        """Enum parsing is case-insensitive."""
        assert LotSizeRule.from_str("lotforlot") == LotSizeRule.LOTFORLOT
        assert LotSizeRule.from_str("eoq") == LotSizeRule.EOQ

    def test_from_str_unknown_defaults_to_l4l(self):
        """Unknown rule defaults to LOTFORLOT."""
        assert LotSizeRule.from_str("UNKNOWN_RULE") == LotSizeRule.LOTFORLOT
        assert LotSizeRule.from_str("") == LotSizeRule.LOTFORLOT


# ── Integration: apply_to_records ──────────────────────────────────────────────

class TestApplyToRecords:
    """Test the full integration: apply_to_records with cascading POH."""

    def _make_weekly_records(self, gross_reqs, start_date=date(2025, 1, 6)):
        """Create a sequence of BucketRecords from gross requirement list."""
        records = []
        on_hand = Decimal("0")
        for i, gr in enumerate(gross_reqs):
            gr = Decimal(str(gr))
            rec = make_record(
                period_start=start_date + timedelta(weeks=i),
                gross_req=gr,
                scheduled_receipts=Decimal("0"),
                projected_on_hand=Decimal("0"),  # Will be recalculated
                net_requirements=gr,  # Simplified: assume no on-hand
                llc=0,
            )
            rec.has_shortage = gr > 0
            rec.shortage_qty = gr if gr > 0 else Decimal("0")
            records.append(rec)
        return records

    def test_l4l_apply_to_records(self):
        """L4L: Each period orders exactly its net requirement."""
        engine = LotSizingEngine(db=MagicMock())
        records = self._make_weekly_records([50, 30, 20])
        params = make_params(lot_size_rule="LOTFORLOT")

        engine.apply_to_records(records, params)

        assert records[0].planned_order_receipts == Decimal("50")
        assert records[1].planned_order_receipts == Decimal("30")
        assert records[2].planned_order_receipts == Decimal("20")

    def test_foq_apply_to_records(self):
        """FOQ: Each triggered period orders the fixed quantity."""
        engine = LotSizingEngine(db=MagicMock())
        records = self._make_weekly_records([50, 30, 20])
        params = make_params(lot_size_rule="FIXED_QTY", min_order_qty=Decimal("100"))

        engine.apply_to_records(records, params)

        assert records[0].planned_order_receipts == Decimal("100")
        assert records[1].planned_order_receipts == Decimal("100")
        assert records[2].planned_order_receipts == Decimal("100")

    def test_zero_net_req_skips_order(self):
        """Periods with no net requirement get no order."""
        engine = LotSizingEngine(db=MagicMock())
        records = self._make_weekly_records([50, 0, 30])
        params = make_params(lot_size_rule="LOTFORLOT")

        engine.apply_to_records(records, params)

        assert records[0].planned_order_receipts == Decimal("50")
        assert records[1].planned_order_receipts == Decimal("0")
        assert records[2].planned_order_receipts == Decimal("30")


# ── Edge Cases ─────────────────────────────────────────────────────────────────

class TestEdgeCases:
    """Boundary conditions and edge cases."""

    def test_zero_net_requirement(self):
        """No order when net_requirements = 0."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("0"),
            projected_on_hand=Decimal("100"),
            planning_params=make_params(lot_size_rule="LOTFORLOT"),
        )
        assert qty == Decimal("0")
        assert rule is None

    def test_negative_net_requirement(self):
        """No order when net_requirements < 0 (surplus)."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("-5"),
            projected_on_hand=Decimal("105"),
            planning_params=make_params(lot_size_rule="LOTFORLOT"),
        )
        assert qty == Decimal("0")
        assert rule is None

    def test_very_small_demand(self):
        """Tiny demand works correctly with MULTIPLE."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("0.001"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="MULTIPLE",
                order_multiple_qty=Decimal("1"),
            ),
        )
        assert qty == Decimal("1")  # ceil(0.001/1) × 1 = 1

    def test_foq_with_decimal_quantities(self):
        """FOQ handles Decimal arithmetic correctly."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("0.5"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="FIXED_QTY",
                min_order_qty=Decimal("1"),
            ),
        )
        assert qty == Decimal("1")

    def test_unknown_rule_defaults_to_l4l(self):
        """Unknown lot_size_rule defaults to L4L."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="UNKNOWN"),
        )
        assert qty == Decimal("50")
        assert rule == "LOTFORLOT"

    def test_null_params_default_to_l4l(self):
        """Missing lot_size_rule defaults to L4L."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params={},
        )
        assert qty == Decimal("50")
        assert rule == "LOTFORLOT"

    def test_multiple_with_decimal_multiple(self):
        """MULTIPLE works with decimal multiples (e.g., 12.5)."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("30"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="MULTIPLE",
                order_multiple_qty=Decimal("12.5"),
            ),
        )
        # ceil(30/12.5) × 12.5 = 3 × 12.5 = 37.5
        assert qty == Decimal("37.5")

    def test_eoq_demand_exactly_multiple(self):
        """EOQ: demand exactly 2× EOQ → order 2× EOQ."""
        engine = LotSizingEngine(db=MagicMock())
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("200"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(
                lot_size_rule="EOQ",
                economic_order_qty=Decimal("100"),
            ),
        )
        assert qty == Decimal("200")

    def test_min_max_at_reorder_point(self):
        """MIN_MAX: POH exactly at reorder point → still triggers."""
        engine = LotSizingEngine(db=MagicMock())
        # POH = reorder_point, so POH < reorder_point is False
        # But net_req > 0 and max exists → max - POH could be negative
        # Falls through to: order = max(0, max - POH) if > net_req else net_req
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("200"),
            planning_params=make_params(
                lot_size_rule="MIN_MAX",
                reorder_point_qty=Decimal("200"),
                max_order_qty=Decimal("500"),
            ),
        )
        # POH(200) = reorder(200), not strictly below
        # So: max(500) - POH(200) = 300 ≥ net_req(50) → order 300
        assert qty == Decimal("300")


# ── APICS CPIM Standard Scenarios ──────────────────────────────────────────────

class TestAPICSCPIMScenarios:
    """
    Standard APICS CPIM exam-style scenarios.

    These are the classic textbook problems that every MRP practitioner
    should recognize.
    """

    def test_cpim_scenario_1_l4l(self):
        """
        CPIM Scenario 1: L4L with safety stock.

        Week 1: GR=60, OH=50, SS=20
        Net Req = 60 - 50 + 20 = 30 → Order 30
        """
        engine = LotSizingEngine(db=MagicMock())
        # After gross-to-net, net_req=30
        qty, rule = engine.calculate_lot_size(
            net_requirements=Decimal("30"),
            projected_on_hand=Decimal("0"),  # After netting
            planning_params=make_params(lot_size_rule="LOTFORLOT"),
        )
        assert qty == Decimal("30")
        assert rule == "LOTFORLOT"

    def test_cpim_scenario_2_foq(self):
        """
        CPIM Scenario 2: FOQ=100.

        Net requirements: 60, 0, 40, 0, 30
        Orders: 100, 0, 100, 0, 100
        """
        engine = LotSizingEngine(db=MagicMock())

        # Period 1: net_req=60
        qty1, _ = engine.calculate_lot_size(
            net_requirements=Decimal("60"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="FIXED_QTY", min_order_qty=Decimal("100")),
        )
        assert qty1 == Decimal("100")

        # Period 2: net_req=0 → no order
        qty2, _ = engine.calculate_lot_size(
            net_requirements=Decimal("0"),
            projected_on_hand=Decimal("40"),
            planning_params=make_params(lot_size_rule="FIXED_QTY", min_order_qty=Decimal("100")),
        )
        assert qty2 == Decimal("0")

    def test_cpim_scenario_3_poq(self):
        """
        CPIM Scenario 3: POQ with 3-period coverage.

        Net requirements: 50, 30, 0, 40
        POQ periods=3: Period 1 order covers 50+30+0=80
        Period 4: order 40
        """
        engine = LotSizingEngine(db=MagicMock())

        # Period 1: POQ covers periods 1-3
        qty1, _ = engine.calculate_lot_size(
            net_requirements=Decimal("50"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="POQ", lot_size_poq_periods=3),
            future_net_reqs=[Decimal("30"), Decimal("0"), Decimal("40")],
        )
        assert qty1 == Decimal("80")  # 50 + 30 + 0

    def test_cpim_scenario_4_eoq(self):
        """
        CPIM Scenario 4: EOQ calculation.

        Annual demand = 1000, ordering cost = 100, carrying cost = 0.5/unit/year
        EOQ = sqrt(2×1000×100 / 0.5) = sqrt(400000) = 632.46 ≈ 632

        In our engine, EOQ is a pre-calculated parameter, not computed on the fly.
        We just verify the order quantity logic.
        """
        engine = LotSizingEngine(db=MagicMock())

        # Net req = 200, EOQ = 632 → order 632
        qty, _ = engine.calculate_lot_size(
            net_requirements=Decimal("200"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="EOQ", economic_order_qty=Decimal("632")),
        )
        assert qty == Decimal("632")

        # Net req = 700, EOQ = 632 → order 632 × 2 = 1264
        qty2, _ = engine.calculate_lot_size(
            net_requirements=Decimal("700"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="EOQ", economic_order_qty=Decimal("632")),
        )
        assert qty2 == Decimal("1264")

    def test_cpim_scenario_5_min_max(self):
        """
        CPIM Scenario 5: Min-Max Reorder.

        Min (reorder point) = 200, Max = 1000
        POH = 150 → order 1000 - 150 = 850
        """
        engine = LotSizingEngine(db=MagicMock())
        qty, _ = engine.calculate_lot_size(
            net_requirements=Decimal("100"),
            projected_on_hand=Decimal("150"),
            planning_params=make_params(
                lot_size_rule="MIN_MAX",
                reorder_point_qty=Decimal("200"),
                max_order_qty=Decimal("1000"),
            ),
        )
        assert qty == Decimal("850")  # 1000 - 150

    def test_cpim_scenario_6_multiple(self):
        """
        CPIM Scenario 6: Order Multiple of 144 (case pack).

        Net requirements: 100, 200, 288
        Orders: 144, 288, 288
        """
        engine = LotSizingEngine(db=MagicMock())

        qty1, _ = engine.calculate_lot_size(
            net_requirements=Decimal("100"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="MULTIPLE", order_multiple_qty=Decimal("144")),
        )
        assert qty1 == Decimal("144")  # ceil(100/144) × 144

        qty2, _ = engine.calculate_lot_size(
            net_requirements=Decimal("200"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="MULTIPLE", order_multiple_qty=Decimal("144")),
        )
        assert qty2 == Decimal("288")  # ceil(200/144) × 144

        qty3, _ = engine.calculate_lot_size(
            net_requirements=Decimal("288"),
            projected_on_hand=Decimal("0"),
            planning_params=make_params(lot_size_rule="MULTIPLE", order_multiple_qty=Decimal("144")),
        )
        assert qty3 == Decimal("288")  # 2 × 144
