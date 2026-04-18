"""
Lot Sizing Engine for APICS-compliant MRP (Phase 0).

Implements six lot-sizing strategies per APICS CPIM standards:
- L4L (Lot-for-Lot): Order exactly the net requirement quantity
- FOQ (Fixed Order Quantity): Order a fixed quantity whenever triggered
- EOQ (Economic Order Quantity): Order the EOQ, rounding up to cover demand
- POQ (Period Order Quantity): Cover net requirements for N future periods
- MIN_MAX (Min-Max): Reorder up to max when on-hand falls below min (reorder point)
- MULTIPLE (Order Multiple): Round up order quantity to the nearest multiple

All calculations use Decimal for precision.
Configuration is read from item_planning_params table.
"""

from __future__ import annotations

import logging
import math
from enum import Enum
from decimal import Decimal
from typing import List, Optional, Tuple

import psycopg

logger = logging.getLogger(__name__)


class LotSizeRule(str, Enum):
    """Lot sizing rules — maps to DB enum lot_size_rule_type + extended rules."""
    LOTFORLOT = "LOTFORLOT"
    FIXED_QTY = "FIXED_QTY"
    EOQ = "EOQ"
    POQ = "POQ"
    MIN_MAX = "MIN_MAX"
    MULTIPLE = "MULTIPLE"

    @classmethod
    def from_str(cls, value: str) -> "LotSizeRule":
        """Parse a string to LotSizeRule, with fallback to L4L."""
        mapping = {
            "LOTFORLOT": cls.LOTFORLOT,
            "L4L": cls.LOTFORLOT,
            "FIXED_QTY": cls.FIXED_QTY,
            "FOQ": cls.FIXED_QTY,
            "EOQ": cls.EOQ,
            "POQ": cls.POQ,
            "PERIOD_OF_SUPPLY": cls.POQ,
            "MIN_MAX": cls.MIN_MAX,
            "MULTIPLE": cls.MULTIPLE,
        }
        return mapping.get(value.upper(), cls.LOTFORLOT)


class LotSizingEngine:
    """
    Apply lot sizing rules according to APICS CPIM standards.

    Each strategy receives:
    - net_requirements: the current period's net requirements (> 0)
    - projected_on_hand: POH after gross-to-net, before lot-sizing order
    - planning_params: dict with lot sizing config from item_planning_params
    - future_net_reqs: list of net requirements for future periods (for POQ)

    Returns:
    - (planned_order_qty, lot_size_rule_applied)
    """

    def __init__(self, db: psycopg.Connection):
        self.db = db

    def calculate_lot_size(
        self,
        net_requirements: Decimal,
        projected_on_hand: Decimal,
        planning_params: dict,
        future_net_reqs: Optional[List[Decimal]] = None,
    ) -> Tuple[Decimal, str]:
        """
        Calculate the planned order receipt quantity for a single period.

        Per APICS, lot sizing is applied ONLY when net_requirements > 0.
        If net_requirements <= 0, no order is placed.

        Args:
            net_requirements: Net requirements for current period (must be > 0 to trigger)
            projected_on_hand: Projected on-hand before this order (after gross-to-net)
            planning_params: Dict with lot_size_rule, min_order_qty, max_order_qty,
                             economic_order_qty, order_multiple_qty, lot_size_poq_periods,
                             reorder_point_qty (for MIN_MAX)
            future_net_reqs: Net requirements for subsequent periods (for POQ)

        Returns:
            (planned_order_qty, lot_size_rule_applied)
        """
        if net_requirements <= 0:
            return Decimal("0"), None

        rule_str = planning_params.get("lot_size_rule", "LOTFORLOT")
        rule = LotSizeRule.from_str(rule_str)

        # Extract params with safe defaults
        min_qty = self._d(planning_params.get("min_order_qty"))
        max_qty = self._d(planning_params.get("max_order_qty"))
        eoq = self._d(planning_params.get("economic_order_qty"))
        multiple = self._d(planning_params.get("order_multiple_qty"))
        poq_periods = int(planning_params.get("lot_size_poq_periods") or 1)
        reorder_point = self._d(planning_params.get("reorder_point_qty"))

        result: Decimal = Decimal("0")

        if rule == LotSizeRule.LOTFORLOT:
            result = self._lot_for_lot(net_requirements, min_qty)

        elif rule == LotSizeRule.FIXED_QTY:
            result = self._fixed_order_quantity(net_requirements, min_qty)

        elif rule == LotSizeRule.EOQ:
            result = self._economic_order_quantity(net_requirements, eoq, min_qty)

        elif rule == LotSizeRule.POQ:
            result = self._period_order_quantity(
                net_requirements, future_net_reqs, poq_periods, min_qty
            )

        elif rule == LotSizeRule.MIN_MAX:
            result = self._min_max(
                net_requirements, projected_on_hand,
                reorder_point or min_qty, max_qty
            )

        elif rule == LotSizeRule.MULTIPLE:
            result = self._order_multiple(net_requirements, multiple, min_qty)

        # Enforce max_order_qty cap (applies to all rules)
        if max_qty and max_qty > 0 and result > max_qty:
            result = max_qty

        return result, rule.value

    # ── Strategy implementations ────────────────────────────────────────────

    @staticmethod
    def _lot_for_lot(net_req: Decimal, min_qty: Optional[Decimal]) -> Decimal:
        """
        L4L: Order exactly what's needed this period.

        APICS: The simplest rule — order quantity equals net requirement.
        If min_order_qty is set, order at least min_qty.
        """
        order = net_req
        if min_qty and order < min_qty:
            order = min_qty
        return order

    @staticmethod
    def _fixed_order_quantity(
        net_req: Decimal,
        fixed_qty: Optional[Decimal],
    ) -> Decimal:
        """
        FOQ: Order a fixed quantity whenever there's a net requirement.

        APICS: When triggered, order exactly fixed_qty. If net_req > fixed_qty,
        order enough multiples of fixed_qty to cover demand.

        Example: fixed_qty=100, net_req=50 → order 100
                 fixed_qty=100, net_req=250 → order 300 (3 × 100)
        """
        if not fixed_qty or fixed_qty <= 0:
            # No fixed qty defined — fall back to L4L
            return net_req

        # Round up to next multiple of fixed_qty
        multiples = math.ceil(net_req / fixed_qty)
        return fixed_qty * Decimal(str(multiples))

    @staticmethod
    def _economic_order_quantity(
        net_req: Decimal,
        eoq: Optional[Decimal],
        min_qty: Optional[Decimal],
    ) -> Decimal:
        """
        EOQ: Order the Economic Order Quantity when triggered.

        APICS: When net_req > 0, order the EOQ. If net_req > EOQ,
        round up to the next EOQ multiple. Respects min_order_qty floor.

        Example: eoq=100, net_req=50 → order 100
                 eoq=100, net_req=150 → order 200
                 eoq=100, min_qty=30, net_req=20 → order 100 (EOQ > min_qty)
        """
        if not eoq or eoq <= 0:
            # No EOQ defined — fall back to L4L with min_qty
            order = net_req
            if min_qty and order < min_qty:
                order = min_qty
            return order

        # Order at least EOQ; if demand exceeds EOQ, round up to multiples
        if net_req <= eoq:
            order = eoq
        else:
            multiples = math.ceil(net_req / eoq)
            order = eoq * Decimal(str(multiples))

        # Respect min_order_qty floor
        if min_qty and order < min_qty:
            order = min_qty

        return order

    @staticmethod
    def _period_order_quantity(
        net_req: Decimal,
        future_net_reqs: Optional[List[Decimal]],
        poq_periods: int,
        min_qty: Optional[Decimal],
    ) -> Decimal:
        """
        POQ: Cover net requirements for N periods with one order.

        APICS: Sum the net requirements for the current period plus the next
        (poq_periods - 1) future periods. This lets you combine demand into
        fewer, larger orders.

        Example: net_reqs=[50, 30, 20], poq_periods=3 → order 100
                 net_reqs=[50, 0, 20], poq_periods=3 → order 70
        """
        total = net_req
        if future_net_reqs and poq_periods > 1:
            periods_to_cover = min(poq_periods - 1, len(future_net_reqs))
            total += sum(
                future_net_reqs[i]
                for i in range(periods_to_cover)
                if future_net_reqs[i] > 0
            )

        if total <= 0:
            total = net_req  # Safety fallback

        if min_qty and total < min_qty:
            total = min_qty

        return total

    @staticmethod
    def _min_max(
        net_req: Decimal,
        projected_on_hand: Decimal,
        reorder_point: Optional[Decimal],
        max_qty: Optional[Decimal],
    ) -> Decimal:
        """
        MIN_MAX: Order up to max when on-hand falls below reorder point.

        APICS: When POH < reorder_point (or net_req triggers), order enough
        to bring inventory up to max_qty. If no reorder_point, use net_req
        as the trigger (order to max_qty).

        Example: max=500, POH=100, net_req=50 → order 400 (= 500 - 100)
                 max=500, POH=600, net_req=50 → no order (POH above max? Still order net_req)
        """
        trigger = reorder_point if reorder_point else Decimal("0")

        # If projected on hand is below reorder point, order up to max
        if projected_on_hand < trigger:
            if max_qty and max_qty > 0:
                order = max_qty - projected_on_hand
                return max(order, Decimal("0"))
            else:
                # No max defined — fall back to L4L
                return net_req

        # Even if above reorder point, net requirements > 0 means we still need
        # to cover demand. Order up to max or just net_req if no max.
        if max_qty and max_qty > 0:
            order = max_qty - projected_on_hand
            if order < net_req:
                # max - POH doesn't cover demand — order at least net_req
                return net_req
            return max(order, Decimal("0"))
        else:
            return net_req

    @staticmethod
    def _order_multiple(
        net_req: Decimal,
        multiple: Optional[Decimal],
        min_qty: Optional[Decimal],
    ) -> Decimal:
        """
        MULTIPLE: Round up the order quantity to the nearest multiple.

        APICS: Order the smallest multiple of `order_multiple` that covers
        net_requirements. Also respects min_order_qty floor.

        Example: multiple=25, net_req=30 → order 50
                 multiple=25, min_qty=100, net_req=30 → order 100
                 multiple=25, net_req=75 → order 75
        """
        if not multiple or multiple <= 0:
            # No multiple defined — fall back to L4L with min_qty
            order = net_req
            if min_qty and order < min_qty:
                order = min_qty
            return order

        # Round up to nearest multiple
        multiples = math.ceil(net_req / multiple)
        order = multiple * Decimal(str(multiples))

        # Respect min_order_qty floor (also rounded to multiple)
        if min_qty and order < min_qty:
            min_multiples = math.ceil(min_qty / multiple)
            order = multiple * Decimal(str(min_multiples))

        return order

    def apply_to_records(
        self,
        records: list,
        planning_params: dict,
        lead_time_days: int = 0,
        start_date=None,
    ) -> None:
        """
        Apply lot sizing to a full sequence of BucketRecords, then cascade
        projected on-hand forward and set planned order releases.

        This is the main integration point with Gross-to-Net: call this
        after gross-to-net calculation and before lead-time offset.

        The method mutates records in place:
        - Sets planned_order_receipts from lot sizing
        - Cascades POH updates forward
        - Sets planned_order_releases (same as receipts, offset happens later)

        Args:
            records: List of BucketRecord (from GrossToNetCalculator)
            planning_params: Planning parameters dict
            lead_time_days: Lead time for release offset
            start_date: Reference date for time fence checks
        """
        from ootils_core.engine.mrp.time_fences import TimeFenceChecker, TimeFenceZone

        if not records:
            return

        time_fence = TimeFenceChecker.from_planning_params(planning_params)
        poq_periods = int(planning_params.get("lot_size_poq_periods") or 1)

        for i, record in enumerate(records):
            # Time fence check
            if start_date and record.period_start:
                fence_result = time_fence.check_zone(record.period_start, start_date)
                record.time_fence_zone = fence_result.zone.value
            else:
                record.time_fence_zone = TimeFenceZone.LIQUID.value

            if record.net_requirements <= 0:
                # No net requirement — no order needed
                # But we still cascade POH forward
                if i > 0:
                    record.projected_on_hand = (
                        records[i - 1].projected_on_hand
                        + record.scheduled_receipts
                        - record.gross_requirements
                    )
                continue

            # Get future net requirements for POQ
            future_net_reqs = []
            if poq_periods > 1:
                for j in range(i + 1, min(i + poq_periods, len(records))):
                    if records[j].net_requirements > 0:
                        future_net_reqs.append(records[j].net_requirements)

            # Apply lot sizing
            lot_qty, rule_applied = self.calculate_lot_size(
                net_requirements=record.net_requirements,
                projected_on_hand=record.projected_on_hand,
                planning_params=planning_params,
                future_net_reqs=future_net_reqs,
            )

            # Frozen zone: suppress new orders
            if record.time_fence_zone == TimeFenceZone.FROZEN.value:
                record.planned_order_receipts = Decimal("0")
                record.planned_order_releases = Decimal("0")
                record.lot_size_rule_applied = rule_applied or "FROZEN"
                # Still cascade POH
                if i == 0:
                    pass  # POH already set by gross-to-net
                else:
                    record.projected_on_hand = (
                        records[i - 1].projected_on_hand
                        + record.scheduled_receipts
                        - record.gross_requirements
                    )
                continue

            record.planned_order_receipts = lot_qty
            record.lot_size_rule_applied = rule_applied

            # Update POH: add the planned receipt
            record.projected_on_hand += lot_qty

            # For POQ: zero out future net requirements that are covered
            if rule_applied == LotSizeRule.POQ.value and poq_periods > 1:
                remaining = lot_qty - record.net_requirements
                for j in range(i + 1, min(i + poq_periods, len(records))):
                    if remaining <= 0:
                        break
                    if records[j].net_requirements > 0:
                        covered = min(remaining, records[j].net_requirements)
                        remaining -= covered
                        # Zero out the covered requirement — order already placed
                        records[j].net_requirements -= covered
                        records[j].planned_order_receipts = Decimal("0")
                        records[j].lot_size_rule_applied = "COVERED_BY_POQ"

            # Planned order release = receipt qty (lead time offset applied later)
            record.planned_order_releases = lot_qty

        # Final pass: cascade POH forward for all records
        for i in range(1, len(records)):
            prev = records[i - 1]
            curr = records[i]
            # Only recalculate if we haven't already (frozen zone or no requirements)
            if curr.net_requirements <= 0 and curr.planned_order_receipts <= 0:
                curr.projected_on_hand = (
                    prev.projected_on_hand
                    + curr.scheduled_receipts
                    - curr.gross_requirements
                )

    def get_planning_params(
        self, item_id, location_id=None
    ) -> dict:
        """
        Load planning params for an item/location from DB.

        Maps DB column names to the parameters expected by the engine:
        - order_multiple → order_multiple_qty
        - reorder_point_qty (for MIN_MAX)
        - economic_order_qty, lot_size_poq_periods (added by migration 021)
        """
        loc_filter = "AND location_id = %s" if location_id else ""
        params: list = [item_id]
        if location_id:
            params.append(location_id)

        row = self.db.execute(f"""
            SELECT
                lot_size_rule,
                min_order_qty,
                max_order_qty,
                reorder_point_qty,
                safety_stock_qty,
                COALESCE(order_multiple_qty, order_multiple) AS order_multiple,
                lead_time_total_days,
                frozen_time_fence_days,
                slashed_time_fence_days,
                forecast_consumption_strategy,
                consumption_window_days,
                economic_order_qty,
                lot_size_poq_periods,
                planning_horizon_days
            FROM item_planning_params
            WHERE item_id = %s
              {loc_filter}
              AND (effective_to IS NULL OR effective_to = '9999-12-31'::DATE)
            ORDER BY effective_from DESC
            LIMIT 1
        """, params).fetchone()

        if row:
            return {
                "lot_size_rule": row["lot_size_rule"] or "LOTFORLOT",
                "min_order_qty": row["min_order_qty"],
                "max_order_qty": row["max_order_qty"],
                "reorder_point_qty": row["reorder_point_qty"],
                "safety_stock_qty": row["safety_stock_qty"],
                "order_multiple_qty": row["order_multiple"],  # DB column name differs
                "economic_order_qty": row["economic_order_qty"],
                "lot_size_poq_periods": row["lot_size_poq_periods"] or 1,
                "lead_time_total_days": row["lead_time_total_days"] or 0,
                "frozen_time_fence_days": row["frozen_time_fence_days"] or 7,
                "slashed_time_fence_days": row["slashed_time_fence_days"] or 30,
                "forecast_consumption_strategy": row["forecast_consumption_strategy"] or "MAX",
                "consumption_window_days": row["consumption_window_days"] or 7,
                "planning_horizon_days": row["planning_horizon_days"] or 90,
            }

        # Defaults
        return {
            "lot_size_rule": "LOTFORLOT",
            "min_order_qty": None,
            "max_order_qty": None,
            "reorder_point_qty": None,
            "safety_stock_qty": None,
            "order_multiple_qty": None,
            "economic_order_qty": None,
            "lot_size_poq_periods": 1,
            "lead_time_total_days": 0,
            "frozen_time_fence_days": 7,
            "slashed_time_fence_days": 30,
            "forecast_consumption_strategy": "MAX",
            "consumption_window_days": 7,
            "planning_horizon_days": 90,
        }

    @staticmethod
    def _d(value) -> Optional[Decimal]:
        """Convert a value to Decimal, returning None if null/zero."""
        if value is None:
            return None
        try:
            d = Decimal(str(value))
            return d if d > 0 else None
        except Exception:
            return None
