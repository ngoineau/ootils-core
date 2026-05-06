"""
CTP (Capable-to-Promise) Engine — Capacity-constrained availability checking.

CTP = ATP + capacity check on critical resources.

This module provides:
  - CTPEngine: Combines ATP with RCCP (Rough-Cut Capacity Planning)
  - Binary search simulation to find first feasible date
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Optional, Tuple, Dict, Any
from uuid import UUID

import psycopg

from ootils_core.atp.engine import ATPEngine
from ootils_core.atp.models import ATPResult, ATPConfig

logger = logging.getLogger(__name__)


class CapacityViolation:
    """Represents a capacity constraint violation."""
    resource_id: UUID
    resource_name: str
    violation_date: date
    required_capacity: Decimal
    available_capacity: Decimal
    overload_pct: float
    
    def __init__(
        self,
        resource_id: UUID,
        resource_name: str,
        violation_date: date,
        required_capacity: Decimal,
        available_capacity: Decimal,
        overload_pct: float,
    ):
        self.resource_id = resource_id
        self.resource_name = resource_name
        self.violation_date = violation_date
        self.required_capacity = required_capacity
        self.available_capacity = available_capacity
        self.overload_pct = overload_pct
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "resource_id": str(self.resource_id),
            "resource_name": self.resource_name,
            "violation_date": self.violation_date.isoformat(),
            "required_capacity": float(self.required_capacity),
            "available_capacity": float(self.available_capacity),
            "overload_pct": self.overload_pct,
        }


class CTPResult:
    """Result of a CTP check."""
    atp_result: ATPResult
    capacity_feasible: bool
    violations: List[CapacityViolation]
    critical_resources: List[str]
    
    def __init__(
        self,
        atp_result: ATPResult,
        capacity_feasible: bool,
        violations: Optional[List[CapacityViolation]] = None,
        critical_resources: Optional[List[str]] = None,
    ):
        self.atp_result = atp_result
        self.capacity_feasible = capacity_feasible
        self.violations = violations or []
        self.critical_resources = critical_resources or []
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "atp_result": {
                "available_quantity": float(self.atp_result.available_quantity),
                "available_date": self.atp_result.available_date.isoformat() if self.atp_result.available_date else None,
                "is_fully_available": self.atp_result.is_fully_available,
                "calculation_time_ms": self.atp_result.calculation_time_ms,
            },
            "capacity_feasible": self.capacity_feasible,
            "violations": [v.to_dict() for v in self.violations],
            "critical_resources": self.critical_resources,
        }


class CTPEngine:
    """
    Capable-to-Promise engine.
    
    CTP extends ATP by checking capacity constraints on critical resources.
    
    The engine:
    1. Calculates ATP (material availability)
    2. Identifies critical resources required for the item
    3. Checks capacity availability on those resources
    4. Returns combined result with capacity feasibility
    """
    
    def __init__(self, db_conn: Optional[psycopg.Connection] = None, config: Optional[ATPConfig] = None):
        """
        Initialize the CTP engine.
        
        Args:
            db_conn: PostgreSQL connection (optional, can be set later)
            config: ATP configuration (optional, uses defaults if not provided)
        """
        self._conn = db_conn
        self._config = config or ATPConfig()
        self._atp_engine = ATPEngine(db_conn=db_conn, config=config)
    
    @property
    def connection(self) -> Optional[psycopg.Connection]:
        """Get the database connection."""
        return self._conn
    
    @connection.setter
    def connection(self, conn: psycopg.Connection):
        """Set the database connection."""
        self._conn = conn
        self._atp_engine.connection = conn
    
    def check(
        self,
        item_id: UUID,
        location_id: UUID,
        quantity: Decimal,
        requested_date: date,
        horizon_days: Optional[int] = None,
        include_capacity: bool = True,
    ) -> CTPResult:
        """
        Check CTP (Capable-to-Promise) for an item.
        
        Args:
            item_id: The item to check
            location_id: The location to check
            quantity: Quantity requested
            requested_date: Date when quantity is needed
            horizon_days: Number of days to look ahead (default: 365)
            include_capacity: Whether to check capacity constraints (default: True)
        
        Returns:
            CTPResult with ATP result + capacity feasibility
        """
        if self._conn is None:
            raise ValueError("Database connection not set. Call engine.connection = conn first.")
        
        horizon_days = horizon_days or 365
        
        logger.debug(
            "CTP check started: item=%s, location=%s, qty=%s, date=%s, horizon=%s",
            item_id, location_id, quantity, requested_date, horizon_days,
        )
        
        # Step 1: Calculate ATP (material availability)
        atp_result = self._atp_engine.calculate(
            item_id=item_id,
            location_id=location_id,
            quantity=quantity,
            request_date=requested_date,
            horizon_days=horizon_days,
        )
        
        # If no material availability, return early
        if atp_result.available_quantity <= 0:
            return CTPResult(
                atp_result=atp_result,
                capacity_feasible=False,
                violations=[],
                critical_resources=[],
            )
        
        # Step 2: Check capacity constraints if requested
        if not include_capacity:
            return CTPResult(
                atp_result=atp_result,
                capacity_feasible=True,  # Assume feasible without capacity check
                violations=[],
                critical_resources=[],
            )
        
        # Step 3: Identify critical resources for this item
        critical_resources = self._get_critical_resources(item_id, location_id)
        
        if not critical_resources:
            # No critical resources defined — capacity is not a constraint
            return CTPResult(
                atp_result=atp_result,
                capacity_feasible=True,
                violations=[],
                critical_resources=[],
            )
        
        # Step 4: Check capacity on each critical resource
        violations = self._check_capacity_constraints(
            item_id, location_id, quantity,
            atp_result.available_date or requested_date,
            critical_resources,
            horizon_days,
        )
        
        capacity_feasible = len(violations) == 0
        
        logger.info(
            "CTP check completed: item=%s, location=%s, capacity_feasible=%s, violations=%d",
            item_id, location_id, capacity_feasible, len(violations),
        )
        
        return CTPResult(
            atp_result=atp_result,
            capacity_feasible=capacity_feasible,
            violations=violations,
            critical_resources=critical_resources,
        )
    
    def _get_critical_resources(
        self,
        item_id: UUID,
        location_id: UUID,
    ) -> List[str]:
        """
        Get list of critical resource external IDs for an item.
        
        Critical resources are those that are bottlenecks or have limited capacity.
        They are defined in item_resource_priority or routing tables.
        
        Args:
            item_id: The item to check
            location_id: The location to check
        
        Returns:
            List of resource external IDs
        """
        resources = []
        
        with self._conn.cursor() as cur:
            # Query item_resource_priority for critical resources (priority <= 2)
            cur.execute("""
                SELECT r.external_id
                FROM item_resource_priority irp
                JOIN resources r ON r.resource_id = irp.resource_id
                WHERE irp.item_id = %s
                  AND irp.location_id = %s
                  AND irp.priority <= 2
                  AND r.active = TRUE
                ORDER BY irp.priority
            """, (item_id, location_id))
            
            for row in cur.fetchall():
                resources.append(row["external_id"])
        
        # Fallback: check routing operations for bottleneck resources
        if not resources:
            with self._conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT r.external_id
                    FROM routing_operations ro
                    JOIN resources r ON r.resource_id = ro.resource_id
                    WHERE ro.item_id = %s
                      AND ro.location_id = %s
                      AND ro.is_bottleneck = TRUE
                      AND r.active = TRUE
                """, (item_id, location_id))
                
                for row in cur.fetchall():
                    resources.append(row["external_id"])
        
        logger.debug("Found %d critical resources for item=%s", len(resources), item_id)
        return resources
    
    def _check_capacity_constraints(
        self,
        item_id: UUID,
        location_id: UUID,
        quantity: Decimal,
        available_date: date,
        critical_resources: List[str],
        horizon_days: int,
    ) -> List[CapacityViolation]:
        """
        Check capacity constraints on critical resources.
        
        Args:
            item_id: The item to check
            location_id: The location to check
            quantity: Quantity to produce
            available_date: Date when material is available
            critical_resources: List of critical resource external IDs
            horizon_days: Number of days to check
        
        Returns:
            List of CapacityViolation for any overloaded resources
        """
        violations = []
        
        for resource_external_id in critical_resources:
            resource_violations = self._check_single_resource_capacity(
                resource_external_id, quantity, available_date, horizon_days,
            )
            violations.extend(resource_violations)
        
        return violations
    
    def _check_single_resource_capacity(
        self,
        resource_external_id: str,
        quantity: Decimal,
        required_date: date,
        horizon_days: int,
    ) -> List[CapacityViolation]:
        """
        Check capacity for a single resource.
        
        Args:
            resource_external_id: Resource external ID
            quantity: Quantity requiring capacity
            required_date: Date when capacity is needed
            horizon_days: Days to look ahead
        
        Returns:
            List of CapacityViolation (empty if no violations)
        """
        violations = []
        
        with self._conn.cursor() as cur:
            # Fetch resource capacity info
            cur.execute("""
                SELECT resource_id, name, capacity_per_day, capacity_unit, location_id
                FROM resources
                WHERE external_id = %s AND active = TRUE
            """, (resource_external_id,))
            
            row = cur.fetchone()
            if not row:
                logger.warning("Resource %s not found", resource_external_id)
                return violations
            
            resource_id = row["resource_id"]
            resource_name = row["name"]
            capacity_per_day = Decimal(str(row["capacity_per_day"]))
            
            # Check capacity on required_date
            # First, check for override capacity
            cur.execute("""
                SELECT capacity
                FROM resource_capacity_overrides
                WHERE resource_id = %s::UUID
                  AND override_date = %s
            """, (resource_id, required_date))
            
            override_row = cur.fetchone()
            available_capacity = Decimal(str(override_row["capacity"])) if override_row else capacity_per_day
            
            # Check existing load on that date
            cur.execute("""
                SELECT COALESCE(SUM(n.quantity), 0) AS total_load
                FROM nodes n
                JOIN edges e ON e.from_node_id = n.node_id
                JOIN nodes rn ON rn.node_id = e.to_node_id
                WHERE n.node_type IN ('WorkOrderSupply', 'PlannedSupply')
                  AND e.edge_type = 'consumes_resource'
                  AND e.active = TRUE
                  AND n.active = TRUE
                  AND rn.node_type = 'Resource'
                  AND rn.external_id = %s
                  AND n.time_ref = %s
            """, (resource_external_id, required_date))
            
            load_row = cur.fetchone()
            existing_load = Decimal(str(load_row["total_load"])) if load_row else Decimal("0")
            
            # Calculate required capacity (assume 1:1 ratio for simplicity; can be extended)
            required_capacity = quantity  # In real implementation, use routing operation time
            
            total_required = existing_load + required_capacity
            
            if total_required > available_capacity:
                overload_pct = float(total_required / available_capacity * 100) if available_capacity > 0 else float('inf')
                violations.append(CapacityViolation(
                    resource_id=resource_id,
                    resource_name=resource_name,
                    violation_date=required_date,
                    required_capacity=required_capacity,
                    available_capacity=available_capacity,
                    overload_pct=overload_pct,
                ))
        
        return violations
    
    def simulate_first_feasible_date(
        self,
        item_id: UUID,
        location_id: UUID,
        quantity: Decimal,
        start_date: Optional[date] = None,
        max_days: int = 30,
    ) -> List[Tuple[date, bool, Dict[str, Any]]]:
        """
        Binary search over dates to find first feasible CTP date.
        
        Args:
            item_id: The item to check
            location_id: The location to check
            quantity: Quantity requested
            start_date: Start date for search (default: today)
            max_days: Maximum days to search (default: 30)
        
        Returns:
            List of (date, feasible, capacity_status) tuples for each tested date
        """
        if self._conn is None:
            raise ValueError("Database connection not set.")
        
        start_date = start_date or date.today()
        results = []
        
        # Binary search approach
        low = 0
        high = max_days
        found_feasible = False
        
        while low <= high and not found_feasible:
            mid = (low + high) // 2
            test_date = start_date + timedelta(days=mid)
            
            result = self.check(
                item_id=item_id,
                location_id=location_id,
                quantity=quantity,
                requested_date=test_date,
                horizon_days=max_days - mid,
            )
            
            feasible = result.capacity_feasible and result.atp_result.is_fully_available
            results.append((test_date, feasible, {
                "atp_available": result.atp_result.is_fully_available,
                "capacity_violations": len(result.violations),
            }))
            
            if feasible:
                found_feasible = True
                high = mid - 1  # Try to find an earlier feasible date
            else:
                low = mid + 1  # Need to look further out
        
        # Fill in gaps with linear scan for detailed output
        if not found_feasible:
            # Scan all dates for detailed output
            results = []
            for day_offset in range(max_days + 1):
                test_date = start_date + timedelta(days=day_offset)
                result = self.check(
                    item_id=item_id,
                    location_id=location_id,
                    quantity=quantity,
                    requested_date=test_date,
                    horizon_days=1,
                )
                feasible = result.capacity_feasible and result.atp_result.is_fully_available
                results.append((test_date, feasible, {
                    "atp_available": result.atp_result.is_fully_available,
                    "capacity_violations": len(result.violations),
                }))
        
        return results
