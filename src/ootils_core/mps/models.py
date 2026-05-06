"""
MPS (Master Production Schedule) models.

The MPS consolidates demand for finished goods before propagation to MRP.
It aggregates forecast and sales orders into time buckets (weekly by default).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional
from uuid import UUID


class MPSStatus(str, Enum):
    """Status of an MPS node in the approval workflow."""
    DRAFT = "DRAFT"           # Initially created, can be modified
    REVIEWED = "REVIEWED"     # Under review, modifications require justification
    APPROVED = "APPROVED"     # Approved for execution
    RELEASED = "RELEASED"     # Released to MRP for supply planning


@dataclass
class MPSNode:
    """
    Master Production Schedule node.
    
    Represents the consolidated demand plan for a finished good at a specific
    location and time bucket. Serves as the bridge between demand forecasting
    and supply planning (MRP).
    
    Attributes:
        mps_id: Unique identifier for this MPS node
        item_id: The finished good being planned
        location_id: The location (plant/DC) where production/distribution occurs
        time_bucket: The time period (e.g., 2026-W15 for weekly buckets)
        time_bucket_start: Start date of the time bucket
        time_bucket_end: End date of the time bucket
        time_grain: Granularity: 'daily', 'weekly', or 'monthly'
        
        # Source demand quantities
        forecast_quantity: Quantity from statistical forecast
        sales_orders_quantity: Quantity from confirmed sales orders
        total_demand: Sum of forecast and sales orders (computed)
        
        # Planning output
        planned_quantity: The approved production/distribution quantity
        status: Current workflow status (DRAFT -> REVIEWED -> APPROVED -> RELEASED)
        
        # Audit trail
        created_by: User/system that created this record
        reviewed_by: User that reviewed this record
        approved_by: User that approved this record
        released_by: User that released this record
        reviewed_at: Timestamp of review
        approved_at: Timestamp of approval
        released_at: Timestamp of release
        
        # Metadata
        notes: Optional comments or justification
        active: Whether this node is currently active
    """
    mps_id: UUID
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    time_bucket: str
    time_bucket_start: date
    time_bucket_end: date
    time_grain: str = "weekly"  # 'daily' | 'weekly' | 'monthly'
    
    # Source demand quantities
    forecast_quantity: Decimal = Decimal("0")
    sales_orders_quantity: Decimal = Decimal("0")
    total_demand: Decimal = Decimal("0")
    
    # Planning output
    planned_quantity: Decimal = Decimal("0")
    status: MPSStatus = MPSStatus.DRAFT
    
    # Audit trail
    created_by: Optional[str] = None
    reviewed_by: Optional[str] = None
    approved_by: Optional[str] = None
    released_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    approved_at: Optional[datetime] = None
    released_at: Optional[datetime] = None
    
    # Metadata
    notes: Optional[str] = None
    active: bool = True
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def compute_total_demand(self) -> Decimal:
        """Compute total demand from forecast and sales orders."""
        self.total_demand = self.forecast_quantity + self.sales_orders_quantity
        return self.total_demand
    
    def can_transition_to(self, new_status: MPSStatus) -> tuple[bool, str]:
        """
        Validate status transition.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        transitions = {
            MPSStatus.DRAFT: {MPSStatus.REVIEWED, MPSStatus.DRAFT},
            MPSStatus.REVIEWED: {MPSStatus.APPROVED, MPSStatus.DRAFT},
            MPSStatus.APPROVED: {MPSStatus.RELEASED, MPSStatus.REVIEWED},
            MPSStatus.RELEASED: set(),  # Terminal state
        }
        
        if new_status not in transitions.get(self.status, set()):
            return False, f"Cannot transition from {self.status.value} to {new_status.value}"
        
        return True, ""
    
    def transition_to(self, new_status: MPSStatus, user: str) -> tuple[bool, str]:
        """
        Transition to a new status with audit trail.
        
        Args:
            new_status: The target status
            user: The user performing the transition
            
        Returns:
            Tuple of (success, error_message)
        """
        is_valid, error = self.can_transition_to(new_status)
        if not is_valid:
            return False, error
        
        now = datetime.now(timezone.utc)
        
        if new_status == MPSStatus.REVIEWED:
            self.reviewed_by = user
            self.reviewed_at = now
        elif new_status == MPSStatus.APPROVED:
            self.approved_by = user
            self.approved_at = now
        elif new_status == MPSStatus.RELEASED:
            self.released_by = user
            self.released_at = now
        
        self.status = new_status
        self.updated_at = now
        return True, ""


@dataclass
class MPSPlannedForEdge:
    """
    Edge linking MPSNode to Item.
    
    Represents the 'mps_planned_for' relationship: an MPS node plans
    production/distribution for a specific item.
    """
    edge_id: UUID
    mps_node_id: UUID
    item_id: UUID
    scenario_id: UUID
    active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class MPSSuppliesEdge:
    """
    Edge linking MPSNode to PlannedSupply (future MRP output).
    
    Represents the 'mps_supplies' relationship: when MRP creates planned
    supplies, they are pegged to the MPS node that triggered them.
    """
    edge_id: UUID
    mps_node_id: UUID
    planned_supply_node_id: UUID
    scenario_id: UUID
    quantity_pegged: Decimal = Decimal("0")
    active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
