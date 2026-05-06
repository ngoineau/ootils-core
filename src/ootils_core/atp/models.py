"""
ATP (Available-to-Promise) models.

Dataclasses for ATP calculation engine results and intermediate data.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import List, Optional
from uuid import UUID


@dataclass
class ATPSupply:
    """
    Represents a supply source for ATP calculation.
    
    Attributes:
        supply_id: Unique identifier for this supply record
        supply_type: Type of supply ('on_hand', 'planned_supply', 'purchase_order', 'work_order')
        item_id: The item being supplied
        location_id: The location where supply is available
        quantity: Available quantity
        available_date: Date when supply becomes available
        priority: Priority for consumption (lower = consumed first)
    """
    supply_id: UUID
    supply_type: str
    item_id: UUID
    location_id: UUID
    quantity: Decimal
    available_date: date
    priority: int = 0
    
    def __post_init__(self):
        """Validate supply data."""
        if self.quantity < 0:
            raise ValueError(f"Supply quantity cannot be negative: {self.quantity}")


@dataclass
class ATPDemand:
    """
    Represents a demand commitment for ATP calculation.
    
    Attributes:
        demand_id: Unique identifier for this demand record
        demand_type: Type of demand ('customer_order', 'forecast', 'safety_stock')
        item_id: The item being demanded
        location_id: The location where demand occurs
        quantity: Demanded quantity
        demand_date: Date when demand is required
        priority: Priority for netting (lower = netted first)
        committed: Whether this demand is already committed against ATP
    """
    demand_id: UUID
    demand_type: str
    item_id: UUID
    location_id: UUID
    quantity: Decimal
    demand_date: date
    priority: int = 0
    committed: bool = False
    
    def __post_init__(self):
        """Validate demand data."""
        if self.quantity < 0:
            raise ValueError(f"Demand quantity cannot be negative: {self.quantity}")


@dataclass
class ATPBucket:
    """
    Represents a time bucket in ATP calculation.
    
    Attributes:
        bucket_start: Start date of the bucket
        bucket_end: End date of the bucket
        opening_atp: ATP at start of bucket
        supply_quantity: Total supply in bucket
        demand_quantity: Total demand in bucket
        closing_atp: ATP at end of bucket
        is_shortage: Whether bucket has negative ATP
    """
    bucket_start: date
    bucket_end: date
    opening_atp: Decimal = Decimal("0")
    supply_quantity: Decimal = Decimal("0")
    demand_quantity: Decimal = Decimal("0")
    closing_atp: Decimal = Decimal("0")
    is_shortage: bool = False
    
    def compute_closing(self) -> Decimal:
        """Compute closing ATP for this bucket."""
        self.closing_atp = self.opening_atp + self.supply_quantity - self.demand_quantity
        self.is_shortage = self.closing_atp < 0
        return self.closing_atp


@dataclass
class ATPResult:
    """
    Result of an ATP calculation.
    
    Attributes:
        item_id: The item for which ATP was calculated
        location_id: The location for which ATP was calculated
        request_date: The date for which ATP was requested
        request_quantity: The quantity requested
        available_quantity: Quantity available to promise
        available_date: Date when quantity is available (may be later than request_date)
        is_fully_available: Whether full request quantity is available on request_date
        backorder_quantity: Quantity that cannot be fulfilled (if any)
        buckets: Daily buckets with ATP breakdown
        calculation_time_ms: Time taken to calculate in milliseconds
    """
    item_id: UUID
    location_id: UUID
    request_date: date
    request_quantity: Decimal
    available_quantity: Decimal = Decimal("0")
    available_date: Optional[date] = None
    is_fully_available: bool = False
    backorder_quantity: Decimal = Decimal("0")
    buckets: List[ATPBucket] = field(default_factory=list)
    calculation_time_ms: float = 0.0
    
    def __post_init__(self):
        """Compute derived fields."""
        # is_fully_available is True only if:
        # 1. Full quantity is available AND
        # 2. It's available on the request_date (not later)
        if self.available_quantity >= self.request_quantity and self.available_date == self.request_date:
            self.is_fully_available = True
            self.backorder_quantity = Decimal("0")
        elif self.available_quantity < self.request_quantity:
            self.backorder_quantity = self.request_quantity - self.available_quantity
            self.is_fully_available = False
        else:
            # Quantity available but on a later date
            self.is_fully_available = False
            self.backorder_quantity = Decimal("0")


@dataclass
class ATPRequest:
    """
    Request for ATP calculation.
    
    Attributes:
        item_id: The item to check availability for
        location_id: The location to check availability at
        quantity: Quantity requested
        request_date: Date when quantity is needed
        horizon_days: Number of days to look ahead for availability (default 365)
        include_forecast: Whether to include forecast demand in netting (default True)
    """
    item_id: UUID
    location_id: UUID
    quantity: Decimal
    request_date: date
    horizon_days: int = 365
    include_forecast: bool = True


@dataclass
class ATPConfig:
    """
    Configuration for ATP calculation.
    
    Attributes:
        time_grain: Granularity of time buckets ('daily', 'weekly', 'monthly')
        netting_rule: Rule for netting demand against supply ('fifo', 'lifo', 'priority')
        consume_on_hand_first: Whether to consume on-hand before planned supply
        respect_supply_priority: Whether to respect supply priority ordering
        default_horizon_days: Default horizon for ATP calculation
    """
    time_grain: str = "daily"
    netting_rule: str = "fifo"
    consume_on_hand_first: bool = True
    respect_supply_priority: bool = True
    default_horizon_days: int = 365
