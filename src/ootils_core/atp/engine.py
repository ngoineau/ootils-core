"""
ATP Engine (Available-to-Promise) — Core calculation engine.

Implements cumulative ATP calculation:
  ATP = OnHand + Scheduled Receipts - Committed Demand

Features:
- Daily bucket calculation over configurable horizon
- Netting: consume supplies in date order (FIFO)
- Support for OnHandSupply, PlannedSupply, CustomerOrderDemand
- Performance target: <100ms for 1 year horizon
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Optional, Tuple, Dict, Any
from uuid import UUID

from ootils_core.atp.models import (
    ATPResult,
    ATPBucket,
    ATPSupply,
    ATPDemand,
    ATPConfig,
)
from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")


def _row_values(row: Any, keys: List[str]) -> List[Any]:
    """Return values from psycopg dict_row or tuple/list rows."""
    if isinstance(row, dict):
        return [row[key] for key in keys]
    return list(row[: len(keys)])


class ATPEngine:
    """
    Available-to-Promise calculation engine.
    
    Implements cumulative ATP algorithm:
      ATP = OnHand + Scheduled Receipts - Committed Demand
    
    The engine:
    1. Fetches supply sources (OnHand, PlannedSupply from MPS/MRP)
    2. Fetches demand commitments (Customer Orders)
    3. Builds daily buckets over the horizon
    4. Nets demand against supply in date order
    5. Returns available quantity and date for a request
    
    Performance: <100ms for 1 year horizon with daily buckets
    """
    
    def __init__(self, db_conn: Optional[DictRowConnection] = None, config: Optional[ATPConfig] = None):
        """
        Initialize the ATP engine.
        
        Args:
            db_conn: PostgreSQL connection (optional, can be set later)
            config: ATP configuration (optional, uses defaults if not provided)
        """
        self._conn = db_conn
        self._config = config or ATPConfig()
    
    @property
    def connection(self) -> Optional[DictRowConnection]:
        """Get the database connection."""
        return self._conn
    
    @connection.setter
    def connection(self, conn: DictRowConnection):
        """Set the database connection."""
        self._conn = conn
    
    def calculate(
        self,
        item_id: UUID,
        location_id: UUID,
        quantity: Decimal,
        request_date: date,
        horizon_days: Optional[int] = None,
        scenario_id: Optional[UUID] = None,
    ) -> ATPResult:
        """
        Calculate ATP for an item at a location.

        Args:
            item_id: The item to check availability for
            location_id: The location to check availability at
            quantity: Quantity requested
            request_date: Date when quantity is needed
            horizon_days: Number of days to look ahead (default: config.default_horizon_days)
            scenario_id: Scenario to read supplies/demands from (default: baseline)

        Returns:
            ATPResult with available quantity, date, and bucket breakdown
        
        Raises:
            ValueError: If no database connection is set
            RuntimeError: If calculation fails
        """
        if self._conn is None:
            raise ValueError("Database connection not set. Call engine.connection = conn first.")
        
        start_time = time.perf_counter()

        horizon_days = horizon_days or self._config.default_horizon_days
        horizon_end = request_date + timedelta(days=horizon_days)
        scenario_id = scenario_id or BASELINE_SCENARIO_ID

        logger.debug(
            "ATP calculation started: item=%s, location=%s, qty=%s, date=%s, horizon=%s days, scenario=%s",
            item_id, location_id, quantity, request_date, horizon_days, scenario_id
        )

        # Step 1: Fetch supplies (OnHand + PlannedSupply)
        supplies = self._fetch_supplies(item_id, location_id, request_date, horizon_end, scenario_id)

        # Step 2: Fetch demands (Customer Orders)
        demands = self._fetch_demands(item_id, location_id, request_date, horizon_end, scenario_id)
        
        # Step 3: Build daily buckets and calculate cumulative ATP
        buckets = self._calculate_cumulative_atp(
            supplies, demands, request_date, horizon_end
        )
        
        # Step 4: Find available quantity and date for the request
        available_qty, available_date = self._find_availability(
            buckets, request_date, quantity
        )
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        
        result = ATPResult(
            item_id=item_id,
            location_id=location_id,
            request_date=request_date,
            request_quantity=quantity,
            available_quantity=available_qty,
            available_date=available_date,
            buckets=buckets,
            calculation_time_ms=elapsed_ms,
        )
        
        logger.info(
            "ATP calculation completed: item=%s, location=%s, available=%s, date=%s, time=%.2fms",
            item_id, location_id, available_qty, available_date, elapsed_ms
        )
        
        return result
    
    def _fetch_supplies(
        self,
        item_id: UUID,
        location_id: UUID,
        start_date: date,
        end_date: date,
        scenario_id: UUID,
    ) -> List[ATPSupply]:
        """
        Fetch supply sources from database.

        Sources:
        - OnHandSupply: Current on-hand inventory (snapshot at start_date)
        - PlannedSupply: Released MPS/MRP planned supplies (scheduled receipts)

        Args:
            item_id: The item to fetch supplies for
            location_id: The location to fetch supplies at
            start_date: Start of horizon
            end_date: End of horizon
            scenario_id: Scenario to read planned supplies from

        Returns:
            List of ATPSupply records, sorted by available_date
        """
        # Private helper — only invoked after check_atp() has validated _conn.
        assert self._conn is not None, "engine.connection must be set before _fetch_supplies"
        supplies: List[ATPSupply] = []

        with self._conn.cursor() as cur:
            # Fetch OnHandSupply (snapshot at start_date)
            # OnHand is treated as available at start_date with highest priority.
            # NOTE: on_hand_supply (migration 030) has no scenario_id column —
            # the physical inventory snapshot is shared across scenarios.
            cur.execute("""
                SELECT 
                    oh.on_hand_id,
                    oh.item_id,
                    oh.location_id,
                    oh.quantity,
                    oh.as_of_date
                FROM on_hand_supply oh
                WHERE oh.item_id = %s
                  AND oh.location_id = %s
                  AND oh.as_of_date <= %s
                ORDER BY oh.as_of_date DESC
                LIMIT 1
            """, (item_id, location_id, start_date))
            
            row = cur.fetchone()
            if row:
                on_hand_id, ih_item, ih_loc, qty, as_of_date = _row_values(
                    row,
                    ["on_hand_id", "item_id", "location_id", "quantity", "as_of_date"],
                )
                if Decimal(str(qty)) > 0:
                    supplies.append(ATPSupply(
                        supply_id=on_hand_id,
                        supply_type="on_hand",
                        item_id=ih_item,
                        location_id=ih_loc,
                        quantity=Decimal(str(qty)),
                        available_date=start_date,  # On-hand available from start
                        priority=0,  # Highest priority (consumed first)
                    ))
            
            # Fetch PlannedSupply (released MPS/MRP)
            # These are scheduled receipts within the horizon
            cur.execute("""
                SELECT 
                    ps.planned_supply_id,
                    ps.item_id,
                    ps.location_id,
                    ps.quantity,
                    ps.due_date,
                    ps.priority
                FROM planned_supply ps
                WHERE ps.item_id = %s
                  AND ps.location_id = %s
                  AND ps.scenario_id = %s
                  AND ps.due_date >= %s
                  AND ps.due_date < %s
                  AND ps.status IN ('RELEASED', 'APPROVED')
                ORDER BY ps.due_date, ps.priority
            """, (item_id, location_id, scenario_id, start_date, end_date))
            
            for row in cur.fetchall():
                ps_id, ps_item, ps_loc, qty, due_date, priority = _row_values(
                    row,
                    ["planned_supply_id", "item_id", "location_id", "quantity", "due_date", "priority"],
                )
                if Decimal(str(qty)) > 0:
                    supplies.append(ATPSupply(
                        supply_id=ps_id,
                        supply_type="planned_supply",
                        item_id=ps_item,
                        location_id=ps_loc,
                        quantity=Decimal(str(qty)),
                        available_date=due_date,
                        priority=priority or 999,
                    ))
        
        # Sort by date, then priority
        supplies.sort(key=lambda s: (s.available_date, s.priority))
        logger.debug("Fetched %d supply records", len(supplies))
        return supplies
    
    def _fetch_demands(
        self,
        item_id: UUID,
        location_id: UUID,
        start_date: date,
        end_date: date,
        scenario_id: UUID,
    ) -> List[ATPDemand]:
        """
        Fetch demand commitments from database.

        Sources:
        - CustomerOrderDemand: Committed customer orders

        Args:
            item_id: The item to fetch demands for
            location_id: The location to fetch demands at
            start_date: Start of horizon
            end_date: End of horizon
            scenario_id: Scenario to read demands from

        Returns:
            List of ATPDemand records, sorted by demand_date
        """
        # Private helper — only invoked after check_atp() has validated _conn.
        assert self._conn is not None, "engine.connection must be set before _fetch_demands"
        demands: List[ATPDemand] = []

        with self._conn.cursor() as cur:
            # Fetch CustomerOrderDemand (committed orders)
            cur.execute("""
                SELECT 
                    cod.customer_order_demand_id,
                    cod.item_id,
                    cod.location_id,
                    cod.quantity,
                    cod.requested_date,
                    cod.priority,
                    cod.is_committed
                FROM customer_order_demand cod
                WHERE cod.item_id = %s
                  AND cod.location_id = %s
                  AND cod.scenario_id = %s
                  AND cod.requested_date >= %s
                  AND cod.requested_date < %s
                  AND cod.status IN ('CONFIRMED', 'RELEASED')
                ORDER BY cod.requested_date, cod.priority
            """, (item_id, location_id, scenario_id, start_date, end_date))
            
            for row in cur.fetchall():
                cod_id, cod_item, cod_loc, qty, req_date, priority, is_committed = _row_values(
                    row,
                    ["customer_order_demand_id", "item_id", "location_id", "quantity", "requested_date", "priority", "is_committed"],
                )
                if Decimal(str(qty)) > 0:
                    demands.append(ATPDemand(
                        demand_id=cod_id,
                        demand_type="customer_order",
                        item_id=cod_item,
                        location_id=cod_loc,
                        quantity=Decimal(str(qty)),
                        demand_date=req_date,
                        priority=priority or 999,
                        committed=is_committed or False,
                    ))
        
        # Sort by date, then priority
        demands.sort(key=lambda d: (d.demand_date, d.priority))
        logger.debug("Fetched %d demand records", len(demands))
        return demands
    
    def _calculate_cumulative_atp(
        self,
        supplies: List[ATPSupply],
        demands: List[ATPDemand],
        start_date: date,
        end_date: date,
    ) -> List[ATPBucket]:
        """
        Calculate cumulative ATP over daily buckets.
        
        Algorithm:
        1. Create daily buckets from start_date to end_date
        2. Initialize opening ATP with on-hand supply
        3. For each bucket in date order:
           - Add supplies due on that date
           - Subtract demands due on that date
           - Compute closing ATP
           - Carry forward to next bucket
        
        Args:
            supplies: List of supply records
            demands: List of demand records
            start_date: Start of horizon
            end_date: End of horizon
        
        Returns:
            List of ATPBucket records with cumulative ATP
        """
        buckets: List[ATPBucket] = []
        
        # Index supplies and demands by date for efficient lookup
        supply_by_date: Dict[date, Decimal] = {}
        for supply in supplies:
            if supply.available_date not in supply_by_date:
                supply_by_date[supply.available_date] = _ZERO
            supply_by_date[supply.available_date] += supply.quantity
        
        demand_by_date: Dict[date, Decimal] = {}
        for demand in demands:
            if demand.demand_date not in demand_by_date:
                demand_by_date[demand.demand_date] = _ZERO
            demand_by_date[demand.demand_date] += demand.quantity
        
        # Calculate daily buckets
        current_date = start_date
        opening_atp = supply_by_date.get(start_date, _ZERO)  # On-hand at start
        
        while current_date < end_date:
            bucket_end = current_date + timedelta(days=1)
            
            bucket = ATPBucket(
                bucket_start=current_date,
                bucket_end=bucket_end,
                opening_atp=opening_atp,
                supply_quantity=supply_by_date.get(current_date, _ZERO),
                demand_quantity=demand_by_date.get(current_date, _ZERO),
            )
            
            # For first bucket, supply is already in opening_atp
            if current_date == start_date:
                bucket.supply_quantity = _ZERO  # Already counted in opening
            
            bucket.compute_closing()
            buckets.append(bucket)
            
            # Carry forward to next bucket
            opening_atp = bucket.closing_atp
            current_date = bucket_end
        
        logger.debug("Calculated %d ATP buckets", len(buckets))
        return buckets
    
    def _find_availability(
        self,
        buckets: List[ATPBucket],
        request_date: date,
        request_quantity: Decimal,
    ) -> Tuple[Decimal, Optional[date]]:
        """
        Find available quantity and date for a request.
        
        Algorithm:
        1. Find the bucket containing request_date
        2. Check cumulative ATP from that bucket forward
        3. Return the maximum available quantity and earliest date
        
        Args:
            buckets: List of ATP buckets
            request_date: Date when quantity is needed
            request_quantity: Quantity requested
        
        Returns:
            Tuple of (available_quantity, available_date)
            - If fully available on request_date: (request_quantity, request_date)
            - If partially available: (qty, earliest_date)
            - If not available: (0, None)
        """
        # Find the bucket for request_date
        target_bucket_idx = None
        for i, bucket in enumerate(buckets):
            if bucket.bucket_start <= request_date < bucket.bucket_end:
                target_bucket_idx = i
                break
        
        # If request_date is before our horizon, use first bucket
        if target_bucket_idx is None:
            if request_date < buckets[0].bucket_start:
                target_bucket_idx = 0
            else:
                # Request is beyond horizon
                return _ZERO, None
        
        # Check cumulative ATP from target bucket forward
        cumulative_atp = _ZERO
        remaining_qty = request_quantity
        
        for i in range(target_bucket_idx, len(buckets)):
            bucket = buckets[i]
            cumulative_atp = bucket.closing_atp
            
            # Check if we can fulfill the request
            if cumulative_atp >= remaining_qty:
                # Available at this bucket's start date
                return request_quantity, bucket.bucket_start
        
        # Not fully available - return what's available
        if cumulative_atp > 0:
            # Find the last bucket with positive ATP
            for i in range(len(buckets) - 1, target_bucket_idx - 1, -1):
                if buckets[i].closing_atp > 0:
                    return buckets[i].closing_atp, buckets[i].bucket_start
        
        return _ZERO, None
    
    def check_available(
        self,
        item_id: UUID,
        location_id: UUID,
        quantity: Decimal,
        request_date: date,
        scenario_id: Optional[UUID] = None,
    ) -> bool:
        """
        Quick check if quantity is available on request_date.

        Args:
            item_id: The item to check
            location_id: The location to check
            quantity: Quantity needed
            request_date: Date when needed
            scenario_id: Scenario to check availability in (default: baseline)

        Returns:
            True if quantity is available on request_date, False otherwise
        """
        result = self.calculate(item_id, location_id, quantity, request_date, scenario_id=scenario_id)
        return result.is_fully_available
    
    def get_available_date(
        self,
        item_id: UUID,
        location_id: UUID,
        quantity: Decimal,
        scenario_id: Optional[UUID] = None,
    ) -> Optional[date]:
        """
        Get the earliest date when quantity will be available.

        Args:
            item_id: The item to check
            location_id: The location to check
            quantity: Quantity needed
            scenario_id: Scenario to check availability in (default: baseline)

        Returns:
            Earliest available date, or None if never available in horizon
        """
        result = self.calculate(item_id, location_id, quantity, date.today(), scenario_id=scenario_id)
        return result.available_date
