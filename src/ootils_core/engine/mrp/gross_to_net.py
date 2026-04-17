"""
Gross-to-Net Calculator for APICS-compliant MRP.

Implements the standard APICS CPIM Projected On-Hand chain:

  POH(t) = POH(t-1) + SR(t) - GR(t)         [before planned orders]
  PAB(t) = POH(t)                              [same as POH before lot-sizing]
  NR(t)  = max(0, SS - PAB(t))                [if PAB < SS, net req = SS - PAB]

After lot-sizing fills in POR(t):
  POH_after(t) = PAB(t) + POR(t)             [projected on-hand including planned orders]

Where:
  POH = Projected On-Hand (before planned orders)
  PAB = Projected Available Balance (same as POH in this context)
  SR  = Scheduled Receipts (confirmed supply: PO, WO, transfers)
  GR  = Gross Requirements (independent + dependent demand)
  NR  = Net Requirements
  SS  = Safety Stock
  POR = Planned Order Receipts (output of lot sizing, filled later)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Sequence
from uuid import UUID, uuid4

import psycopg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TimeBucket:
    """A time period in the planning horizon."""
    sequence: int
    start: date
    end: date
    grain: str = "week"  # day, week, month


@dataclass
class BucketRecord:
    """
    One row of the time-phased MRP record for an item/location.

    Fields follow APICS standard MRP grid layout:
      Period | GR | SR | PAB | NR | POR | PORel | POH_after
    """
    bucket_id: UUID
    item_id: UUID
    location_id: Optional[UUID]
    period_start: date
    period_end: date
    bucket_sequence: int

    # --- Inputs ---
    gross_requirements: Decimal = Decimal("0")
    scheduled_receipts: Decimal = Decimal("0")

    # --- Calculated (gross-to-net phase, before lot-sizing) ---
    projected_on_hand: Decimal = Decimal("0")       # PAB = POH(t-1) + SR - GR
    net_requirements: Decimal = Decimal("0")          # max(0, SS - PAB)

    # --- Filled by lot-sizing / lead-time offset phase ---
    planned_order_receipts: Decimal = Decimal("0")
    planned_order_releases: Decimal = Decimal("0")

    # --- Post-lot-sizing ---
    projected_on_hand_after: Decimal = Decimal("0")  # PAB + POR

    # --- Flags & metadata ---
    has_shortage: bool = False
    shortage_qty: Decimal = Decimal("0")
    llc: int = 0
    time_fence_zone: Optional[str] = None
    lot_size_rule_applied: Optional[str] = None


# ---------------------------------------------------------------------------
# Calculator
# ---------------------------------------------------------------------------

class GrossToNetCalculator:
    """
    APICS Gross-to-Net calculator.

    Given an item, its planning parameters, pre-consumed forecast, and
    dependent demand from parent items, produces a full time-phased MRP
    record (list of BucketRecord).

    The POH chain is computed bucket-by-bucket, carrying the running
    balance forward.  This is the standard APICS CPIM algorithm.

    Usage:
        calc = GrossToNetCalculator(db, scenario_id)
        buckets = calc.create_time_buckets(start, 90, grain="week")
        records = calc.calculate(item_id, loc_id, buckets, params,
                                 consumed_forecast=..., dependent_demand=...)
        # Then apply lot-sizing and lead-time offset externally.
    """

    def __init__(self, db: psycopg.Connection, scenario_id: UUID):
        self.db = db
        self.scenario_id = scenario_id

    # ------------------------------------------------------------------
    # Time buckets
    # ------------------------------------------------------------------

    def create_time_buckets(
        self,
        start_date: date,
        horizon_days: int,
        grain: str = "week",
    ) -> List[TimeBucket]:
        """
        Create time buckets spanning the planning horizon.

        Weekly buckets align to ISO weeks (Monday start).
        Daily and monthly buckets are also supported.
        """
        buckets: List[TimeBucket] = []
        current = start_date
        seq = 0
        horizon_end = start_date + timedelta(days=horizon_days)

        # Snap start to Monday for weekly grain
        if grain == "week":
            current = start_date - timedelta(days=start_date.weekday())

        while current < horizon_end:
            if grain == "day":
                end = current + timedelta(days=1)
            elif grain == "week":
                end = current + timedelta(days=7)
            elif grain == "month":
                # First day of next month
                if current.month == 12:
                    end = date(current.year + 1, 1, 1)
                else:
                    end = date(current.year, current.month + 1, 1)
            else:
                end = current + timedelta(days=7)

            # Don't exceed the horizon
            if end > horizon_end:
                end = horizon_end

            buckets.append(TimeBucket(
                sequence=seq,
                start=current,
                end=end,
                grain=grain,
            ))
            current = end
            seq += 1

        return buckets

    # ------------------------------------------------------------------
    # Main calculate method
    # ------------------------------------------------------------------

    def calculate(
        self,
        item_id: UUID,
        location_id: Optional[UUID],
        buckets: List[TimeBucket],
        planning_params: dict,
        consumed_forecast: Optional[Dict[date, Decimal]] = None,
        dependent_demand: Optional[Dict[date, Decimal]] = None,
        llc: int = 0,
    ) -> List[BucketRecord]:
        """
        Calculate gross-to-net for one item across all time buckets.

        APICS POH chain algorithm:
          For each period t (in order):
            GR(t)  = demand + dependent demand
            SR(t)  = scheduled receipts (confirmed PO, WO, transfers)
            PAB(t) = PAB(t-1) + SR(t) - GR(t)
            NR(t)  = max(0, SS - PAB(t))   if PAB(t) < SS

        Safety stock replenishment:
          If PAB drops below SS, a net requirement is generated to bring
          PAB back up to SS.  This is the standard APICS formulation.

        Args:
            item_id: Item to process
            location_id: Optional location filter
            buckets: Time buckets for the horizon
            planning_params: Dict with safety_stock_qty, lead_time_total_days, etc.
            consumed_forecast: Pre-consumed forecast {period_start: net_demand}
            dependent_demand: Dependent demand from parent items {date: qty}
            llc: Low-Level Code for this item

        Returns:
            List of BucketRecord with all MRP calculations.
            planned_order_receipts and planned_order_releases are left at 0;
            they are filled by the lot-sizing and lead-time-offset phases.
        """
        safety_stock = self._coalesce_decimal(
            planning_params.get("safety_stock_qty"), Decimal("0")
        )

        # --- Load data from DB ---
        on_hand = self._get_initial_on_hand(item_id, location_id)
        sr_map = self._get_scheduled_receipts_map(item_id, location_id, buckets)

        # Precompute gross requirements per bucket
        gr_map = self._build_gross_requirements_map(
            item_id, location_id, buckets,
            consumed_forecast, dependent_demand, llc,
        )

        # --- POH chain ---
        records: List[BucketRecord] = []
        pab = on_hand  # running balance: starts at on-hand

        for bucket in buckets:
            gr = gr_map.get(bucket.start, Decimal("0"))
            sr = sr_map.get(bucket.start, Decimal("0"))

            # PAB(t) = PAB(t-1) + SR(t) - GR(t)
            pab = pab + sr - gr

            # Net requirements: bring PAB up to safety stock
            if pab < safety_stock:
                net_req = safety_stock - pab
            else:
                net_req = Decimal("0")

            # Shortage tracking
            has_shortage = pab < safety_stock
            shortage_qty = net_req if has_shortage else Decimal("0")

            record = BucketRecord(
                bucket_id=uuid4(),
                item_id=item_id,
                location_id=location_id,
                period_start=bucket.start,
                period_end=bucket.end,
                bucket_sequence=bucket.sequence,
                # Inputs
                gross_requirements=gr,
                scheduled_receipts=sr,
                # Calculated
                projected_on_hand=pab,
                net_requirements=net_req,
                # Placeholders (filled by lot-sizing phase)
                planned_order_receipts=Decimal("0"),
                planned_order_releases=Decimal("0"),
                projected_on_hand_after=pab,  # will be updated after lot-sizing
                # Flags
                has_shortage=has_shortage,
                shortage_qty=shortage_qty,
                llc=llc,
            )
            records.append(record)

        return records

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    @staticmethod
    def apply_planned_orders(records: List[BucketRecord]) -> List[BucketRecord]:
        """
        Recalculate POH-after after lot-sizing fills planned_order_receipts.

        After the external lot-sizing phase sets planned_order_receipts on
        each record, call this method to update projected_on_hand_after:

            POH_after(t) = PAB(t) + POR(t)
            PAB(t+1)     = POH_after(t)  (carries forward to next bucket)

        This must be called AFTER lot-sizing has been applied.

        Returns:
            The same list of records, mutated in place with updated
            projected_on_hand_after values.
        """
        if not records:
            return records

        # First bucket carries forward the initial on-hand before its own POR
        # PAB[0] already includes on-hand + SR - GR.  Adding POR gives
        # POH_after[0] = PAB[0] + POR[0].
        # Then for next bucket: PAB[1] = POH_after[0] + SR[1] - GR[1]
        # But PAB[1] was already computed WITHOUT POR from previous buckets
        # (since lot-sizing runs after gross-to-net). So we need to re-chain.

        # Re-chain: POH_after(t) = PAB(t) + POR(t)
        #           PAB(t+1)      = POH_after(t) + SR(t+1) - GR(t+1)

        running = records[0].projected_on_hand  # initial PAB from gross-to-net
        # But this PAB doesn't include prior planned orders yet.
        # We need to re-compute from scratch using POR.

        # Start from PAB of first record (which is on-hand + SR - GR, no POR)
        # Then for each record:
        #   POH_after(t) = PAB(t) + POR(t)
        #   PAB(t+1)     = POH_after(t) + SR(t+1) - GR(t+1)

        for i, rec in enumerate(records):
            rec.projected_on_hand_after = rec.projected_on_hand + rec.planned_order_receipts

            # Carry forward: update PAB for next bucket
            if i + 1 < len(records):
                next_rec = records[i + 1]
                # PAB(t+1) = POH_after(t) + SR(t+1) - GR(t+1)
                # But SR and GR are already stored in next_rec.
                # We need to recalculate next PAB based on current POH_after.
                next_rec.projected_on_hand = (
                    rec.projected_on_hand_after
                    + next_rec.scheduled_receipts
                    - next_rec.gross_requirements
                )
                # Recalculate net requirements for next bucket
                # (this is a secondary correction — lot sizing may have
                #  resolved the shortage, so NR may become 0)
                # Note: NR was already computed in the gross-to-net pass.
                # In the re-chain, we do NOT recompute NR because lot-sizing
                # has already decided what to order. NR was the input to
                # lot-sizing.

        # Final record: projected_on_hand_after = projected_on_hand + POR
        records[-1].projected_on_hand_after = (
            records[-1].projected_on_hand + records[-1].planned_order_receipts
        )

        return records

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_initial_on_hand(
        self, item_id: UUID, location_id: Optional[UUID]
    ) -> Decimal:
        """Get current on-hand inventory for item/location from baseline scenario."""
        BASELINE = UUID("00000000-0000-0000-0000-000000000001")

        if location_id:
            row = self.db.execute("""
                SELECT COALESCE(SUM(quantity), 0) AS qty
                FROM nodes
                WHERE node_type = 'OnHandSupply'
                  AND item_id = %s
                  AND location_id = %s
                  AND scenario_id = %s
                  AND active = true
            """, (item_id, location_id, BASELINE)).fetchone()
        else:
            row = self.db.execute("""
                SELECT COALESCE(SUM(quantity), 0) AS qty
                FROM nodes
                WHERE node_type = 'OnHandSupply'
                  AND item_id = %s
                  AND scenario_id = %s
                  AND active = true
            """, (item_id, BASELINE)).fetchone()

        if row and row["qty"] is not None:
            return Decimal(str(row["qty"]))
        return Decimal("0")

    def _get_scheduled_receipts_map(
        self,
        item_id: UUID,
        location_id: Optional[UUID],
        buckets: List[TimeBucket],
    ) -> Dict[date, Decimal]:
        """
        Get confirmed scheduled receipts (PO, WO, transfers) per period.

        These are supply nodes that are already committed — purchase orders,
        work orders, and transfers with confirmed dates.
        """
        loc_filter = "AND location_id = %s" if location_id else ""
        params: list = [item_id, self.scenario_id]
        if location_id:
            params.append(location_id)

        rows = self.db.execute(f"""
            SELECT time_ref, quantity
            FROM nodes
            WHERE node_type IN ('PurchaseOrderSupply', 'WorkOrderSupply', 'TransferSupply')
              AND item_id = %s
              AND scenario_id = %s
              {loc_filter}
              AND active = true
              AND quantity > 0
        """, params).fetchall()

        # Map each receipt to its bucket
        result: Dict[date, Decimal] = defaultdict(Decimal)
        for row in rows:
            receipt_date = row["time_ref"]
            qty = Decimal(str(row["quantity"]))
            bucket_start = self._date_to_bucket_start(receipt_date, buckets)
            if bucket_start is not None:
                result[bucket_start] += qty

        return dict(result)

    def _build_gross_requirements_map(
        self,
        item_id: UUID,
        location_id: Optional[UUID],
        buckets: List[TimeBucket],
        consumed_forecast: Optional[Dict[date, Decimal]],
        dependent_demand: Optional[Dict[date, Decimal]],
        llc: int,
    ) -> Dict[date, Decimal]:
        """
        Build gross requirements per bucket.

        For LLC > 0 (components): use dependent demand from parent explosions.
        For LLC = 0 (finished goods): use consumed forecast, or fall back to
        max(forecast, customer orders).

        Args:
            item_id: Item ID
            location_id: Optional location filter
            buckets: Time buckets
            consumed_forecast: Pre-consumed forecast (period_start → net_demand)
            dependent_demand: Dependent demand from parents (date → qty)
            llc: Low-Level Code

        Returns:
            Dict mapping bucket.start → gross_requirements
        """
        gr_map: Dict[date, Decimal] = defaultdict(Decimal)

        # 1. Dependent demand (for component items, LLC > 0)
        if llc > 0 and dependent_demand:
            for d, qty in dependent_demand.items():
                bucket_start = self._date_to_bucket_start(d, buckets)
                if bucket_start is not None:
                    gr_map[bucket_start] += qty
            return dict(gr_map)

        # 2. Consumed forecast (pre-computed by ForecastConsumer)
        if consumed_forecast:
            for d, qty in consumed_forecast.items():
                bucket_start = self._date_to_bucket_start(d, buckets)
                if bucket_start is not None:
                    gr_map[bucket_start] += qty
            return dict(gr_map)

        # 3. Fallback: load from DB — max(forecast, customer_orders)
        for bucket in buckets:
            forecast_qty = self._get_forecast_qty(
                item_id, location_id, bucket.start, bucket.end
            )
            orders_qty = self._get_customer_orders_qty(
                item_id, location_id, bucket.start, bucket.end
            )
            gr_map[bucket.start] = max(forecast_qty, orders_qty)

        return dict(gr_map)

    def _get_forecast_qty(
        self,
        item_id: UUID,
        location_id: Optional[UUID],
        period_start: date,
        period_end: date,
    ) -> Decimal:
        """Load forecast demand quantity for a period from DB."""
        loc_filter = "AND location_id = %s" if location_id else ""
        params: list = [item_id, self.scenario_id]
        if location_id:
            params.append(location_id)
        params.extend([period_start, period_end])

        row = self.db.execute(f"""
            SELECT COALESCE(SUM(quantity), 0) AS qty
            FROM nodes
            WHERE node_type = 'ForecastDemand'
              AND item_id = %s
              AND scenario_id = %s
              {loc_filter}
              AND active = true
              AND time_span_start >= %s
              AND time_span_start < %s
        """, params).fetchone()

        return Decimal(str(row["qty"])) if row else Decimal("0")

    def _get_customer_orders_qty(
        self,
        item_id: UUID,
        location_id: Optional[UUID],
        period_start: date,
        period_end: date,
    ) -> Decimal:
        """Load customer order demand quantity for a period from DB."""
        loc_filter = "AND location_id = %s" if location_id else ""
        params: list = [item_id, self.scenario_id]
        if location_id:
            params.append(location_id)
        params.extend([period_start, period_end])

        row = self.db.execute(f"""
            SELECT COALESCE(SUM(quantity), 0) AS qty
            FROM nodes
            WHERE node_type = 'CustomerOrderDemand'
              AND item_id = %s
              AND scenario_id = %s
              {loc_filter}
              AND active = true
              AND time_ref >= %s
              AND time_ref < %s
        """, params).fetchone()

        return Decimal(str(row["qty"])) if row else Decimal("0")

    @staticmethod
    def _date_to_bucket_start(
        target: date, buckets: List[TimeBucket]
    ) -> Optional[date]:
        """Find which bucket a date falls into; return bucket.start or None."""
        for bucket in buckets:
            if bucket.start <= target < bucket.end:
                return bucket.start
        return None

    @staticmethod
    def _coalesce_decimal(value, default: Decimal) -> Decimal:
        """Convert value to Decimal, returning default if null/empty."""
        if value is None:
            return default
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))