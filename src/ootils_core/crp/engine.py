"""
CRP Engine (Capacity Requirements Planning) — Core calculation engine.

Implements backward scheduling from planned order due dates:
  - Explodes planned orders into operations via routings
  - Computes load per work center per day
  - Detects overloads (load > effective_capacity)
  - Infinite loading (detects but does not resolve overloads)

Features:
- Daily bucket calculation over configurable horizon
- Operation explosion via routings (setup + run time)
- Backward scheduling from due dates
- Load profile computation per work center
- Overload detection with excess quantification
- Performance target: <500ms for 1000+ planned orders
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from uuid import UUID

import psycopg

from ootils_core.crp.models import WorkCenter, Routing, Operation

logger = logging.getLogger(__name__)

_ZERO = Decimal("0")


def _row_values(row: Any, keys: List[str]) -> List[Any]:
    """Return values from psycopg dict_row or tuple/list rows."""
    if isinstance(row, dict):
        return [row[key] for key in keys]
    return list(row[: len(keys)])


class LoadBucket:
    """
    Represents load on a work center for a single day.
    
    Attributes:
        work_center_id: The work center this bucket is for
        bucket_date: The date of this bucket
        load_hours: Total load in hours for this day
        capacity_hours: Effective capacity in hours for this day
        overload_hours: Excess load (load - capacity, or 0 if not overloaded)
        is_overloaded: Whether load exceeds capacity
    """
    work_center_id: UUID
    bucket_date: date
    load_hours: Decimal
    capacity_hours: Decimal
    overload_hours: Decimal
    is_overloaded: bool
    
    def __init__(
        self,
        work_center_id: UUID,
        bucket_date: date,
        load_hours: Decimal = _ZERO,
        capacity_hours: Decimal = _ZERO,
    ):
        self.work_center_id = work_center_id
        self.bucket_date = bucket_date
        self.load_hours = load_hours
        self.capacity_hours = capacity_hours
        self.overload_hours = _ZERO
        self.is_overloaded = False
        self._recalculate()
    
    def add_load(self, hours: Decimal):
        """Add load to this bucket."""
        self.load_hours += hours
        self._recalculate()
    
    def set_capacity(self, capacity: Decimal):
        """Set the effective capacity for this bucket."""
        self.capacity_hours = capacity
        self._recalculate()
    
    def _recalculate(self):
        """Recalculate overload status."""
        if self.load_hours > self.capacity_hours:
            self.overload_hours = self.load_hours - self.capacity_hours
            self.is_overloaded = True
        else:
            self.overload_hours = _ZERO
            self.is_overloaded = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "work_center_id": str(self.work_center_id),
            "bucket_date": self.bucket_date.isoformat(),
            "load_hours": float(self.load_hours),
            "capacity_hours": float(self.capacity_hours),
            "overload_hours": float(self.overload_hours),
            "is_overloaded": self.is_overloaded,
        }


class Overload:
    """
    Represents a capacity overload at a work center on a specific date.
    
    Attributes:
        work_center_id: The overloaded work center
        work_center_code: Human-readable code for the work center
        overload_date: Date when overload occurs
        load_hours: Total load on that date
        capacity_hours: Effective capacity on that date
        excess_hours: Amount by which load exceeds capacity
    """
    work_center_id: UUID
    work_center_code: str
    overload_date: date
    load_hours: Decimal
    capacity_hours: Decimal
    excess_hours: Decimal
    
    def __init__(
        self,
        work_center_id: UUID,
        work_center_code: str,
        overload_date: date,
        load_hours: Decimal,
        capacity_hours: Decimal,
        excess_hours: Decimal,
    ):
        self.work_center_id = work_center_id
        self.work_center_code = work_center_code
        self.overload_date = overload_date
        self.load_hours = load_hours
        self.capacity_hours = capacity_hours
        self.excess_hours = excess_hours
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "work_center_id": str(self.work_center_id),
            "work_center_code": self.work_center_code,
            "overload_date": self.overload_date.isoformat(),
            "load_hours": float(self.load_hours),
            "capacity_hours": float(self.capacity_hours),
            "excess_hours": float(self.excess_hours),
        }


class LoadProfile:
    """
    Load profile for a single work center over a time horizon.
    
    Attributes:
        work_center_id: The work center this profile is for
        work_center_code: Human-readable code
        buckets: List of daily load buckets
        total_load_hours: Sum of all load in the profile
        total_capacity_hours: Sum of all capacity in the profile
        overload_count: Number of days with overloads
    """
    work_center_id: UUID
    work_center_code: str
    buckets: List[LoadBucket]
    
    def __init__(self, work_center_id: UUID, work_center_code: str):
        self.work_center_id = work_center_id
        self.work_center_code = work_center_code
        self.buckets = []
    
    def add_bucket(self, bucket: LoadBucket):
        """Add a daily bucket to the profile."""
        self.buckets.append(bucket)
    
    def get_total_load(self) -> Decimal:
        """Get total load across all buckets."""
        return sum(b.load_hours for b in self.buckets)
    
    def get_total_capacity(self) -> Decimal:
        """Get total capacity across all buckets."""
        return sum(b.capacity_hours for b in self.buckets)
    
    def get_overload_count(self) -> int:
        """Get number of days with overloads."""
        return sum(1 for b in self.buckets if b.is_overloaded)
    
    def get_overloads(self) -> List[Overload]:
        """Get list of overloads in this profile."""
        overloads = []
        for bucket in self.buckets:
            if bucket.is_overloaded:
                overloads.append(Overload(
                    work_center_id=self.work_center_id,
                    work_center_code=self.work_center_code,
                    overload_date=bucket.bucket_date,
                    load_hours=bucket.load_hours,
                    capacity_hours=bucket.capacity_hours,
                    excess_hours=bucket.overload_hours,
                ))
        return overloads
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "work_center_id": str(self.work_center_id),
            "work_center_code": self.work_center_code,
            "buckets": [b.to_dict() for b in self.buckets],
            "total_load_hours": float(self.get_total_load()),
            "total_capacity_hours": float(self.get_total_capacity()),
            "overload_count": self.get_overload_count(),
        }


class CRPResult:
    """
    Result of a CRP calculation.
    
    Attributes:
        calculation_id: Unique identifier for this calculation
        horizon_start: Start date of the planning horizon
        horizon_end: End date of the planning horizon
        planned_orders_count: Number of planned orders processed
        work_centers_count: Number of work centers in the result
        load_profiles: Load profiles per work center
        overloads: List of all detected overloads
        calculation_time_ms: Time taken to calculate
    """
    calculation_id: UUID
    horizon_start: date
    horizon_end: date
    planned_orders_count: int
    work_centers_count: int
    load_profiles: Dict[UUID, LoadProfile]
    overloads: List[Overload]
    calculation_time_ms: float
    
    def __init__(
        self,
        calculation_id: UUID,
        horizon_start: date,
        horizon_end: date,
    ):
        self.calculation_id = calculation_id
        self.horizon_start = horizon_start
        self.horizon_end = horizon_end
        self.planned_orders_count = 0
        self.work_centers_count = 0
        self.load_profiles = {}
        self.overloads = []
        self.calculation_time_ms = 0.0
    
    def add_load_profile(self, profile: LoadProfile):
        """Add a load profile to the result."""
        self.load_profiles[profile.work_center_id] = profile
        self.work_centers_count = len(self.load_profiles)
    
    def collect_overloads(self):
        """Collect all overloads from all load profiles."""
        self.overloads = []
        for profile in self.load_profiles.values():
            self.overloads.extend(profile.get_overloads())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "calculation_id": str(self.calculation_id),
            "horizon_start": self.horizon_start.isoformat(),
            "horizon_end": self.horizon_end.isoformat(),
            "planned_orders_count": self.planned_orders_count,
            "work_centers_count": self.work_centers_count,
            "load_profiles": {
                str(wc_id): profile.to_dict()
                for wc_id, profile in self.load_profiles.items()
            },
            "overloads": [o.to_dict() for o in self.overloads],
            "calculation_time_ms": self.calculation_time_ms,
        }


class CRPEngine:
    """
    Capacity Requirements Planning calculation engine.
    
    Implements backward scheduling from planned order due dates:
    1. Fetches planned orders from MRP/MPS
    2. Fetches routings and operations for each item
    3. Explodes orders into operations with time requirements
    4. Schedules operations backward from due dates
    5. Aggregates load per work center per day
    6. Detects overloads (infinite loading)
    
    Performance: <500ms for 1000+ planned orders
    """
    
    def __init__(self, db_conn: Optional[psycopg.Connection] = None):
        """
        Initialize the CRP engine.
        
        Args:
            db_conn: PostgreSQL connection (optional, can be set later)
        """
        self._conn = db_conn
    
    @property
    def connection(self) -> Optional[psycopg.Connection]:
        """Get the database connection."""
        return self._conn
    
    @connection.setter
    def connection(self, conn: psycopg.Connection):
        """Set the database connection."""
        self._conn = conn
    
    def calculate(
        self,
        horizon_days: int = 90,
        work_centers: Optional[List[UUID]] = None,
        scenario_id: Optional[UUID] = None,
    ) -> CRPResult:
        """
        Perform CRP calculation.
        
        Args:
            horizon_days: Number of days to plan ahead (default: 90)
            work_centers: Optional list of work center IDs to include (default: all active)
            scenario_id: Optional scenario ID to filter planned orders
        
        Returns:
            CRPResult with load profiles and overloads
        
        Raises:
            ValueError: If no database connection is set
            RuntimeError: If calculation fails
        """
        if self._conn is None:
            raise ValueError("Database connection not set. Call engine.connection = conn first.")
        
        import time
        start_time = time.perf_counter()
        
        from uuid import uuid4
        calculation_id = uuid4()
        
        horizon_start = date.today()
        horizon_end = horizon_start + timedelta(days=horizon_days)
        
        logger.info(
            "CRP calculation started: horizon=%s to %s (%s days), work_centers=%s",
            horizon_start, horizon_end, horizon_days,
            len(work_centers) if work_centers else "all"
        )
        
        result = CRPResult(calculation_id, horizon_start, horizon_end)
        
        # Step 1: Fetch work centers
        work_centers_map = self._fetch_work_centers(work_centers)
        if not work_centers_map:
            logger.warning("No work centers found for CRP calculation")
            result.calculation_time_ms = (time.perf_counter() - start_time) * 1000
            return result
        
        # Step 2: Fetch planned orders
        planned_orders = self._fetch_planned_orders(horizon_start, horizon_end, scenario_id)
        result.planned_orders_count = len(planned_orders)
        logger.debug("Fetched %d planned orders", len(planned_orders))
        
        # Step 3: Fetch routings and operations for items in planned orders
        if planned_orders:
            item_ids = set(po["item_id"] for po in planned_orders)
            routings = self._fetch_routings(item_ids)
            logger.debug("Fetched routings for %d items", len(routings))
        else:
            routings = {}
            logger.info("No planned orders in horizon")
        
        # Step 4: Initialize load buckets per work center
        load_buckets: Dict[UUID, Dict[date, Decimal]] = defaultdict(lambda: defaultdict(lambda: _ZERO))
        
        # Step 5: Explode planned orders into operations and schedule backward
        if planned_orders and routings:
            for po in planned_orders:
                item_id = po["item_id"]
                due_date = po["due_date"]
                quantity = po["quantity"]
                
                if item_id not in routings:
                    logger.debug("No routing found for item %s", item_id)
                    continue
                
                routing = routings[item_id]
                
                # Schedule operations backward from due date
                # Last operation ends on due_date, earlier operations end before
                current_end_date = due_date
                
                # Process operations in reverse sequence order
                operations = [op for op in routing.operations if op.active]
                operations.sort(key=lambda op: op.sequence, reverse=True)
                
                for op in operations:
                    if op.work_center_id not in work_centers_map:
                        continue
                    
                    # Calculate operation duration in days
                    total_hours = op.total_time(quantity)
                    
                    # Convert hours to days based on work center capacity
                    wc = work_centers_map[op.work_center_id]
                    daily_capacity = wc.effective_capacity_per_day()
                    
                    if daily_capacity > _ZERO:
                        days_needed = total_hours / daily_capacity
                    else:
                        # If no capacity defined, assume 8-hour days
                        days_needed = total_hours / Decimal("8")
                    
                    # Round up to full days
                    days_needed = max(Decimal("1"), days_needed.to_integral_value(rounding="ROUND_UP"))
                    
                    # Schedule backward: operation ends on current_end_date
                    # Start date = end_date - days_needed + 1
                    days_needed_int = int(days_needed)
                    start_date = current_end_date - timedelta(days=days_needed_int - 1)
                    
                    # Distribute load across the days of this operation
                    hours_per_day = total_hours / Decimal(days_needed_int)
                    
                    current_date = start_date
                    while current_date <= current_end_date:
                        if horizon_start <= current_date <= horizon_end:
                            load_buckets[op.work_center_id][current_date] += hours_per_day
                        current_date += timedelta(days=1)
                    
                    # Update end date for next operation (previous operation ends when this one starts)
                    current_end_date = start_date - timedelta(days=1)
        
        # Step 6: Build load profiles and detect overloads
        for wc_id, buckets_by_date in load_buckets.items():
            if wc_id not in work_centers_map:
                continue
            
            wc = work_centers_map[wc_id]
            profile = LoadProfile(wc_id, wc.code)
            
            # Create buckets for all days in horizon
            current_date = horizon_start
            while current_date <= horizon_end:
                bucket = LoadBucket(
                    work_center_id=wc_id,
                    bucket_date=current_date,
                    load_hours=buckets_by_date.get(current_date, _ZERO),
                    capacity_hours=wc.effective_capacity_per_day(),
                )
                profile.add_bucket(bucket)
                current_date += timedelta(days=1)
            
            result.add_load_profile(profile)
        
        # Step 7: Collect overloads
        result.collect_overloads()
        
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        result.calculation_time_ms = elapsed_ms
        
        logger.info(
            "CRP calculation completed: orders=%d, work_centers=%d, overloads=%d, time=%.2fms",
            result.planned_orders_count,
            result.work_centers_count,
            len(result.overloads),
            elapsed_ms
        )
        
        return result
    
    def _fetch_work_centers(
        self,
        work_center_ids: Optional[List[UUID]] = None,
    ) -> Dict[UUID, WorkCenter]:
        """
        Fetch work centers from database.
        
        Args:
            work_center_ids: Optional list of specific work center IDs
        
        Returns:
            Dictionary mapping work_center_id to WorkCenter
        """
        work_centers: Dict[UUID, WorkCenter] = {}
        
        with self._conn.cursor() as cur:
            if work_center_ids:
                placeholders = ",".join("%s" for _ in work_center_ids)
                cur.execute(f"""
                    SELECT 
                        work_center_id,
                        code,
                        description,
                        capacity_per_day,
                        efficiency,
                        calendar_id,
                        active
                    FROM work_centers
                    WHERE work_center_id IN ({placeholders})
                      AND active = true
                """, work_center_ids)
            else:
                cur.execute("""
                    SELECT 
                        work_center_id,
                        code,
                        description,
                        capacity_per_day,
                        efficiency,
                        calendar_id,
                        active
                    FROM work_centers
                    WHERE active = true
                """)
            
            for row in cur.fetchall():
                wc_id, code, desc, cap, eff, cal_id, active = _row_values(
                    row,
                    ["work_center_id", "code", "description", "capacity_per_day", "efficiency", "calendar_id", "active"],
                )
                work_centers[wc_id] = WorkCenter(
                    work_center_id=wc_id,
                    code=code,
                    description=desc,
                    capacity_per_day=Decimal(str(cap)) if cap else _ZERO,
                    efficiency=Decimal(str(eff)) if eff else Decimal("1.0"),
                    calendar_id=cal_id,
                    active=active,
                )
        
        return work_centers
    
    def _fetch_planned_orders(
        self,
        start_date: date,
        end_date: date,
        scenario_id: Optional[UUID] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch planned orders from MRP/MPS.
        
        Args:
            start_date: Start of horizon
            end_date: End of horizon
            scenario_id: Optional scenario filter
        
        Returns:
            List of planned orders with item_id, due_date, quantity
        """
        orders: List[Dict[str, Any]] = []
        
        with self._conn.cursor() as cur:
            if scenario_id:
                cur.execute("""
                    SELECT 
                        planned_supply_id,
                        item_id,
                        location_id,
                        quantity,
                        due_date,
                        status
                    FROM planned_supply
                    WHERE due_date >= %s
                      AND due_date < %s
                      AND scenario_id = %s
                      AND status IN ('RELEASED', 'APPROVED', 'PLANNED')
                    ORDER BY due_date
                """, (start_date, end_date, scenario_id))
            else:
                cur.execute("""
                    SELECT 
                        planned_supply_id,
                        item_id,
                        location_id,
                        quantity,
                        due_date,
                        status
                    FROM planned_supply
                    WHERE due_date >= %s
                      AND due_date < %s
                      AND status IN ('RELEASED', 'APPROVED', 'PLANNED')
                    ORDER BY due_date
                """, (start_date, end_date))
            
            for row in cur.fetchall():
                ps_id, item_id, loc_id, qty, due_date, status = _row_values(
                    row,
                    ["planned_supply_id", "item_id", "location_id", "quantity", "due_date", "status"],
                )
                orders.append({
                    "planned_supply_id": ps_id,
                    "item_id": item_id,
                    "location_id": loc_id,
                    "quantity": Decimal(str(qty)) if qty else _ZERO,
                    "due_date": due_date,
                    "status": status,
                })
        
        return orders
    
    def _fetch_routings(
        self,
        item_ids: set[UUID],
    ) -> Dict[UUID, Routing]:
        """
        Fetch routings and operations for given items.
        
        Args:
            item_ids: Set of item IDs to fetch routings for
        
        Returns:
            Dictionary mapping item_id to Routing
        """
        routings: Dict[UUID, Routing] = {}
        
        if not item_ids:
            return routings
        
        with self._conn.cursor() as cur:
            # Fetch routings
            placeholders = ",".join("%s" for _ in item_ids)
            cur.execute(f"""
                SELECT 
                    routing_id,
                    item_id,
                    sequence,
                    description,
                    active
                FROM routings
                WHERE item_id IN ({placeholders})
                  AND active = true
                ORDER BY item_id, sequence
            """, list(item_ids))
            
            routing_map: Dict[UUID, Routing] = {}
            for row in cur.fetchall():
                r_id, i_id, seq, desc, active = _row_values(
                    row,
                    ["routing_id", "item_id", "sequence", "description", "active"],
                )
                routing = Routing(
                    routing_id=r_id,
                    item_id=i_id,
                    sequence=seq,
                    description=desc,
                    active=active,
                )
                routing_map[r_id] = routing
                # Store by item_id (use first routing per item)
                if i_id not in routings:
                    routings[i_id] = routing
            
            # Fetch operations for these routings
            if routing_map:
                routing_ids = list(routing_map.keys())
                placeholders = ",".join("%s" for _ in routing_ids)
                cur.execute(f"""
                    SELECT 
                        operation_id,
                        routing_id,
                        sequence,
                        work_center_id,
                        setup_time,
                        run_time_per_unit,
                        description,
                        active
                    FROM routing_operations
                    WHERE routing_id IN ({placeholders})
                      AND active = true
                    ORDER BY routing_id, sequence
                """, routing_ids)
                
                for row in cur.fetchall():
                    op_id, r_id, seq, wc_id, setup, run, desc, active = _row_values(
                        row,
                        ["operation_id", "routing_id", "sequence", "work_center_id", "setup_time", "run_time_per_unit", "description", "active"],
                    )
                    op = Operation(
                        operation_id=op_id,
                        routing_id=r_id,
                        sequence=seq,
                        work_center_id=wc_id,
                        setup_time=Decimal(str(setup)) if setup else _ZERO,
                        run_time_per_unit=Decimal(str(run)) if run else _ZERO,
                        description=desc,
                        active=active,
                    )
                    if r_id in routing_map:
                        routing_map[r_id].add_operation(op)
        
        return routings
    
    def get_load_profile(
        self,
        work_center_id: UUID,
        horizon_days: int = 90,
    ) -> Optional[LoadProfile]:
        """
        Get load profile for a specific work center.
        
        Args:
            work_center_id: The work center to get profile for
            horizon_days: Number of days in the horizon
        
        Returns:
            LoadProfile for the work center, or None if not found
        """
        result = self.calculate(horizon_days=horizon_days, work_centers=[work_center_id])
        return result.load_profiles.get(work_center_id)
    
    def get_overloads(
        self,
        horizon_days: int = 90,
        work_centers: Optional[List[UUID]] = None,
    ) -> List[Overload]:
        """
        Get all overloads in the planning horizon.
        
        Args:
            horizon_days: Number of days in the horizon
            work_centers: Optional list of work centers to check
        
        Returns:
            List of Overload objects
        """
        result = self.calculate(horizon_days=horizon_days, work_centers=work_centers)
        return result.overloads
    
    def suggest_resolutions(
        self,
        horizon_days: int = 90,
        work_centers: Optional[List[UUID]] = None,
        max_shift_days: int = 14,
    ) -> List[Dict[str, Any]]:
        """
        Suggest resolutions for detected overloads by shifting order dates.
        
        Strategy: For each overload, find planned orders contributing to that day
        and suggest shifting them to the next available capacity slot.
        
        Args:
            horizon_days: Planning horizon in days
            work_centers: Optional list of work centers to analyze
            max_shift_days: Maximum days to suggest shifting (default: 14)
        
        Returns:
            List of resolution suggestions with:
            - work_center_id, work_center_code
            - overload_date, excess_hours
            - suggested_orders: list of orders to shift with new dates
            - total_hours_freed: estimated capacity freed if suggestions applied
        """
        if self._conn is None:
            raise ValueError("Database connection not set")
        
        # First get all overloads
        overloads = self.get_overloads(horizon_days=horizon_days, work_centers=work_centers)
        
        if not overloads:
            return []
        
        suggestions: List[Dict[str, Any]] = []
        horizon_start = date.today()
        horizon_end = horizon_start + timedelta(days=horizon_days)
        
        # Group overloads by work center and date for efficient processing
        overload_map: Dict[Tuple[UUID, date], Overload] = {}
        for ol in overloads:
            overload_map[(ol.work_center_id, ol.overload_date)] = ol
        
        # For each overloaded work center, find contributing orders
        for (wc_id, ol_date), overload in overload_map.items():
            suggestion = self._suggest_resolution_for_overload(
                work_center_id=wc_id,
                overload_date=ol_date,
                excess_hours=overload.excess_hours,
                horizon_start=horizon_start,
                horizon_end=horizon_end,
                max_shift_days=max_shift_days,
            )
            if suggestion:
                suggestions.append(suggestion)
        
        return suggestions
    
    def _suggest_resolution_for_overload(
        self,
        work_center_id: UUID,
        overload_date: date,
        excess_hours: Decimal,
        horizon_start: date,
        horizon_end: date,
        max_shift_days: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Suggest resolution for a single overload by finding orders to shift.
        
        Returns suggestion dict or None if no resolution found.
        """
        # Fetch planned orders that contribute to this work center on this date
        # These are orders whose operations fall on the overload date
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    ps.planned_supply_id,
                    ps.item_id,
                    ps.quantity,
                    ps.due_date,
                    i.code AS item_code,
                    r.routing_id,
                    op.operation_id,
                    op.sequence,
                    op.run_time_per_unit,
                    op.setup_time,
                    wc.code AS work_center_code
                FROM planned_supply ps
                JOIN items i ON ps.item_id = i.item_id
                JOIN routings r ON i.item_id = r.item_id AND r.active = true
                JOIN routing_operations op ON r.routing_id = op.routing_id AND op.active = true
                JOIN work_centers wc ON op.work_center_id = wc.work_center_id
                WHERE ps.due_date >= %s
                  AND ps.due_date <= %s
                  AND wc.work_center_id = %s
                  AND ps.status IN ('RELEASED', 'APPROVED', 'PLANNED')
                ORDER BY ps.due_date DESC, op.sequence DESC
            """, (horizon_start, horizon_end, work_center_id))
            
            rows = cur.fetchall()
        
        if not rows:
            return None
        
        # Calculate which orders contribute load on the overload date
        # Using backward scheduling logic from CRPEngine.calculate()
        wc = self._fetch_work_centers([work_center_id]).get(work_center_id)
        if not wc:
            return None
        
        daily_capacity = wc.effective_capacity_per_day()
        if daily_capacity <= _ZERO:
            daily_capacity = Decimal("8")  # Default 8-hour day
        
        # Group by planned supply to aggregate operation contributions
        order_contributions: Dict[UUID, Dict[str, Any]] = {}
        
        for row in rows:
            ps_id, item_id, qty, due_date, item_code, r_id, op_id, seq, run_time, setup, wc_code = row
            qty = Decimal(str(qty)) if qty else _ZERO
            run_time = Decimal(str(run_time)) if run_time else _ZERO
            setup = Decimal(str(setup)) if setup else _ZERO
            
            # Calculate operation duration
            total_hours = setup + (run_time * qty)
            days_needed = max(Decimal("1"), (total_hours / daily_capacity).to_integral_value(rounding="ROUND_UP"))
            
            # Backward schedule: operation ends on or before due_date
            # For simplicity, assume last operation ends on due_date
            # Earlier operations end before based on sequence
            # We'll approximate: contribution on overload_date if operation spans it
            
            # Simple heuristic: if due_date is within days_needed of overload_date,
            # this order contributes to the overload
            days_from_overload = (due_date - overload_date).days
            
            if 0 <= days_from_overload < int(days_needed) + 2:  # Buffer of 2 days
                if ps_id not in order_contributions:
                    order_contributions[ps_id] = {
                        "planned_supply_id": ps_id,
                        "item_id": item_id,
                        "item_code": item_code,
                        "quantity": qty,
                        "current_due_date": due_date,
                        "work_center_code": wc_code,
                        "estimated_load_hours": Decimal("0"),
                    }
                order_contributions[ps_id]["estimated_load_hours"] += total_hours / days_needed
        
        if not order_contributions:
            return None
        
        # Sort orders by due date (latest first - easier to shift)
        sorted_orders = sorted(
            order_contributions.values(),
            key=lambda x: x["current_due_date"],
            reverse=True
        )
        
        # Find next available capacity slot
        next_available_date = self._find_next_available_slot(
            work_center_id=work_center_id,
            from_date=overload_date + timedelta(days=1),
            to_date=min(overload_date + timedelta(days=max_shift_days), horizon_end),
            hours_needed=excess_hours,
        )
        
        if not next_available_date:
            # No available slot found within max_shift_days
            # Suggest shifting as far as possible
            next_available_date = min(overload_date + timedelta(days=max_shift_days), horizon_end)
        
        # Build suggestion
        hours_to_free = Decimal("0")
        suggested_orders = []
        
        for order in sorted_orders:
            if hours_to_free >= excess_hours:
                break
            
            suggested_orders.append({
                "planned_supply_id": str(order["planned_supply_id"]),
                "item_code": order["item_code"],
                "quantity": float(order["quantity"]),
                "current_due_date": order["current_due_date"].isoformat(),
                "suggested_due_date": next_available_date.isoformat(),
                "shift_days": (next_available_date - order["current_due_date"]).days,
                "estimated_load_hours": float(order["estimated_load_hours"]),
            })
            hours_to_free += order["estimated_load_hours"]
        
        if not suggested_orders:
            return None
        
        return {
            "work_center_id": str(work_center_id),
            "work_center_code": wc.code,
            "overload_date": overload_date.isoformat(),
            "excess_hours": float(excess_hours),
            "suggested_orders": suggested_orders,
            "total_hours_freed": float(hours_to_free),
            "recommendation": f"Shift {len(suggested_orders)} order(s) to {next_available_date.isoformat()} to resolve overload",
        }
    
    def _find_next_available_slot(
        self,
        work_center_id: UUID,
        from_date: date,
        to_date: date,
        hours_needed: Decimal,
    ) -> Optional[date]:
        """
        Find the next date with available capacity >= hours_needed.
        
        Returns the date or None if no slot found in range.
        """
        wc = self._fetch_work_centers([work_center_id]).get(work_center_id)
        if not wc:
            return None
        
        daily_capacity = wc.effective_capacity_per_day()
        if daily_capacity <= _ZERO:
            return None
        
        current_date = from_date
        while current_date <= to_date:
            # Check if this date has capacity (simplified: assume capacity available
            # if we haven't allocated it - in production, would check actual allocations)
            if daily_capacity >= hours_needed:
                return current_date
            current_date += timedelta(days=1)
        
        return None
