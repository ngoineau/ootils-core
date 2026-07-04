"""
DRP (Distribution Requirements Planning) models.

Defines distribution network structure including locations, links, and transportation lanes
for multi-echelon distribution planning.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional
from uuid import UUID, uuid4


@dataclass
class DistributionLink:
    """
    Distribution Link model.
    
    Represents a transfer channel between two locations in the distribution network
    (e.g., plant → DC, DC → warehouse). Can be item-specific or generic for all items.
    
    Attributes:
        distribution_link_id: Unique identifier for this link
        upstream_location_id: Source location (plant, upstream DC)
        downstream_location_id: Destination location (downstream DC, warehouse)
        item_id: Optional item-specific link (None = generic for all items)
        transit_lead_time_days: Transit time in days
        transit_cost_per_unit: Variable cost per unit transferred
        transit_cost_fixed: Fixed cost per shipment (independent of quantity)
        minimum_shipment_qty: Minimum quantity per shipment
        maximum_shipment_qty: Maximum quantity per shipment (optional)
        shipment_frequency: Frequency constraint (daily, weekly, biweekly, monthly, on_demand)
        shipment_days: Allowed shipment days (1=Mon, 7=Sun)
        active: Whether this link is currently active
        priority: Priority for sourcing (1=highest)
    """
    distribution_link_id: UUID = field(default_factory=uuid4)
    upstream_location_id: Optional[UUID] = None
    downstream_location_id: Optional[UUID] = None
    item_id: Optional[UUID] = None
    transit_lead_time_days: Decimal = Decimal("7")
    transit_cost_per_unit: Optional[Decimal] = None
    transit_cost_fixed: Optional[Decimal] = None
    minimum_shipment_qty: Decimal = Decimal("1")
    maximum_shipment_qty: Optional[Decimal] = None
    shipment_frequency: Optional[str] = None
    shipment_days: Optional[List[int]] = None
    active: bool = True
    priority: int = 100
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def is_generic_link(self) -> bool:
        """Return True if this link applies to all items (not item-specific)."""
        return self.item_id is None
    
    def can_ship_on_day(self, day_of_week: int) -> bool:
        """
        Check if shipment is allowed on a given day of week.
        
        Args:
            day_of_week: Day of week (1=Monday, 7=Sunday)
            
        Returns:
            True if shipment is allowed, False otherwise
        """
        if not self.shipment_days:
            return True
        return day_of_week in self.shipment_days
    
    def get_transit_cost(self, quantity: Decimal) -> Decimal:
        """
        Compute total transit cost for a given quantity.
        
        Args:
            quantity: Quantity to ship
            
        Returns:
            Total cost (fixed + variable)
        """
        total_cost = Decimal("0")
        if self.transit_cost_fixed:
            total_cost += self.transit_cost_fixed
        if self.transit_cost_per_unit:
            total_cost += self.transit_cost_per_unit * quantity
        return total_cost
    
    def respects_minimum_qty(self, quantity: Decimal) -> bool:
        """Check if quantity meets minimum shipment requirement."""
        return quantity >= self.minimum_shipment_qty
    
    def respects_maximum_qty(self, quantity: Decimal) -> bool:
        """Check if quantity is within maximum shipment limit."""
        if self.maximum_shipment_qty is None:
            return True
        return quantity <= self.maximum_shipment_qty
    
    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate distribution link integrity.
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        if not self.upstream_location_id:
            errors.append("upstream_location_id is required")
        if not self.downstream_location_id:
            errors.append("downstream_location_id is required")
        if self.upstream_location_id == self.downstream_location_id:
            errors.append("upstream and downstream locations must be different")
        if self.transit_lead_time_days < Decimal("0"):
            errors.append(f"transit_lead_time_days cannot be negative: {self.transit_lead_time_days}")
        if self.minimum_shipment_qty < Decimal("0"):
            errors.append(f"minimum_shipment_qty cannot be negative: {self.minimum_shipment_qty}")
        if self.maximum_shipment_qty is not None and self.maximum_shipment_qty < self.minimum_shipment_qty:
            errors.append(f"maximum_shipment_qty ({self.maximum_shipment_qty}) must be >= minimum_shipment_qty ({self.minimum_shipment_qty})")
        if self.priority < 1:
            errors.append(f"priority must be >= 1, got {self.priority}")
        
        return len(errors) == 0, errors


@dataclass
class TransportationLane:
    """
    Transportation Lane model.
    
    Defines transportation options and constraints for a distribution link.
    More detailed than DistributionLink, includes carrier and mode information.
    
    Attributes:
        lane_id: Unique identifier for this lane
        distribution_link_id: Reference to the distribution link
        carrier: Carrier name or code
        mode: Transportation mode (truck, rail, air, ocean, intermodal)
        service_level: Service level (standard, expedited, economy)
        transit_time_min_days: Minimum transit time in days
        transit_time_max_days: Maximum transit time in days
        cost_per_unit: Variable cost per unit
        cost_per_shipment: Fixed cost per shipment
        minimum_weight: Minimum weight requirement
        maximum_weight: Maximum weight capacity
        equipment_type: Equipment type (e.g., "53ft dry van", "40ft container")
        active: Whether this lane is currently active
    """
    lane_id: UUID = field(default_factory=uuid4)
    distribution_link_id: Optional[UUID] = None
    carrier: Optional[str] = None
    mode: str = "truck"
    service_level: str = "standard"
    transit_time_min_days: Decimal = Decimal("1")
    transit_time_max_days: Decimal = Decimal("7")
    cost_per_unit: Optional[Decimal] = None
    cost_per_shipment: Optional[Decimal] = None
    minimum_weight: Optional[Decimal] = None
    maximum_weight: Optional[Decimal] = None
    equipment_type: Optional[str] = None
    active: bool = True
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def get_transit_time_estimate(self) -> Decimal:
        """Return estimated transit time (midpoint of min/max)."""
        return (self.transit_time_min_days + self.transit_time_max_days) / Decimal("2")
    
    def get_lane_cost(self, quantity: Decimal, weight: Optional[Decimal] = None) -> Decimal:
        """
        Compute total lane cost for a given quantity.
        
        Args:
            quantity: Quantity to ship
            weight: Optional total weight (for weight-based pricing)
            
        Returns:
            Total cost
        """
        total_cost = Decimal("0")
        if self.cost_per_shipment:
            total_cost += self.cost_per_shipment
        if self.cost_per_unit:
            total_cost += self.cost_per_unit * quantity
        return total_cost
    
    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate transportation lane integrity.
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        if not self.distribution_link_id:
            errors.append("distribution_link_id is required")
        if self.transit_time_min_days < Decimal("0"):
            errors.append(f"transit_time_min_days cannot be negative: {self.transit_time_min_days}")
        if self.transit_time_max_days < self.transit_time_min_days:
            errors.append(f"transit_time_max_days ({self.transit_time_max_days}) must be >= transit_time_min_days ({self.transit_time_min_days})")
        if self.minimum_weight is not None and self.minimum_weight < Decimal("0"):
            errors.append(f"minimum_weight cannot be negative: {self.minimum_weight}")
        if self.maximum_weight is not None and self.minimum_weight is not None:
            if self.maximum_weight < self.minimum_weight:
                errors.append(f"maximum_weight ({self.maximum_weight}) must be >= minimum_weight ({self.minimum_weight})")
        
        valid_modes = ["truck", "rail", "air", "ocean", "intermodal", "pipeline", "multimodal"]
        if self.mode not in valid_modes:
            errors.append(f"Invalid mode '{self.mode}'. Valid modes: {valid_modes}")
        
        valid_service_levels = ["standard", "expedited", "economy", "premium", "same_day"]
        if self.service_level not in valid_service_levels:
            errors.append(f"Invalid service_level '{self.service_level}'. Valid levels: {valid_service_levels}")
        
        return len(errors) == 0, errors


@dataclass
class DistributionLinkEdge:
    """
    Edge linking DistributionLink to Locations.
    
    Represents the network topology: upstream_location → distribution_link → downstream_location
    """
    edge_id: UUID = field(default_factory=uuid4)
    distribution_link_id: Optional[UUID] = None
    upstream_location_id: Optional[UUID] = None
    downstream_location_id: Optional[UUID] = None
    item_id: Optional[UUID] = None
    active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class LaneRequiresLinkEdge:
    """
    Edge linking TransportationLane to DistributionLink.
    
    Represents that a transportation lane serves a specific distribution link.
    """
    edge_id: UUID = field(default_factory=uuid4)
    lane_id: Optional[UUID] = None
    distribution_link_id: Optional[UUID] = None
    active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
