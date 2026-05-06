"""
CRP (Capacity Requirements Planning) models.

Defines work centers, routings, and operations for detailed capacity planning.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID


@dataclass
class WorkCenter:
    """
    Work Center model.
    
    Represents a production resource or group of resources where operations
    are performed. Used for detailed capacity planning in CRP.
    
    Attributes:
        work_center_id: Unique identifier for this work center
        code: Short alphanumeric code (e.g., "WC-001", "ASSEMBLY-A")
        description: Human-readable description
        capacity_per_day: Maximum output capacity per day (in standard hours or units)
        efficiency: Efficiency factor (0.0 to 1.0, default 1.0)
        calendar_id: Reference to a calendar defining working days/hours
        active: Whether this work center is currently active
    """
    work_center_id: UUID
    code: str
    description: str
    capacity_per_day: Decimal = Decimal("0")
    efficiency: Decimal = Decimal("1.0")
    calendar_id: Optional[UUID] = None
    active: bool = True
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def effective_capacity_per_day(self) -> Decimal:
        """Compute effective capacity accounting for efficiency."""
        return self.capacity_per_day * self.efficiency
    
    def validate_efficiency(self) -> tuple[bool, str]:
        """Validate efficiency is within valid range."""
        if self.efficiency < Decimal("0") or self.efficiency > Decimal("1"):
            return False, f"Efficiency must be between 0 and 1, got {self.efficiency}"
        return True, ""


@dataclass
class Operation:
    """
    Operation model.
    
    Represents a single step within a routing, performed at a specific
    work center with defined setup and run times.
    
    Attributes:
        operation_id: Unique identifier for this operation
        routing_id: The routing this operation belongs to
        sequence: Sequence number within the routing (1-indexed)
        work_center_id: The work center where this operation is performed
        setup_time: Setup time in hours (independent of quantity)
        run_time_per_unit: Run time per unit in hours
        description: Optional description of the operation
        active: Whether this operation is currently active
    """
    operation_id: UUID
    routing_id: UUID
    sequence: int
    work_center_id: UUID
    setup_time: Decimal = Decimal("0")
    run_time_per_unit: Decimal = Decimal("0")
    description: Optional[str] = None
    active: bool = True
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def total_time(self, quantity: Decimal) -> Decimal:
        """
        Compute total time required for a given quantity.
        
        Args:
            quantity: Number of units to produce
            
        Returns:
            Total time in hours (setup + run_time_per_unit * quantity)
        """
        return self.setup_time + (self.run_time_per_unit * quantity)
    
    def validate_sequence(self) -> tuple[bool, str]:
        """Validate sequence is positive."""
        if self.sequence <= 0:
            return False, f"Sequence must be positive, got {self.sequence}"
        return True, ""
    
    def validate_times(self) -> tuple[bool, str]:
        """Validate times are non-negative."""
        if self.setup_time < Decimal("0"):
            return False, f"Setup time cannot be negative, got {self.setup_time}"
        if self.run_time_per_unit < Decimal("0"):
            return False, f"Run time per unit cannot be negative, got {self.run_time_per_unit}"
        return True, ""


@dataclass
class Routing:
    """
    Routing model.
    
    Defines the sequence of operations required to produce an item.
    A routing links an item to its manufacturing process.
    
    Attributes:
        routing_id: Unique identifier for this routing
        item_id: The item this routing produces
        sequence: Routing sequence number (for alternate routings)
        description: Human-readable description
        operations: List of operations in this routing
        active: Whether this routing is currently active
    """
    routing_id: UUID
    item_id: UUID
    sequence: int = 1
    description: Optional[str] = None
    operations: list[Operation] = field(default_factory=list)
    active: bool = True
    
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def add_operation(self, operation: Operation) -> tuple[bool, str]:
        """
        Add an operation to this routing.
        
        Args:
            operation: The operation to add
            
        Returns:
            Tuple of (success, error_message)
        """
        # Validate sequence uniqueness within this routing
        for existing in self.operations:
            if existing.sequence == operation.sequence and existing.active:
                return False, f"Duplicate operation sequence: {operation.sequence}"
        
        self.operations.append(operation)
        self.operations.sort(key=lambda op: op.sequence)
        self.updated_at = datetime.now(timezone.utc)
        return True, ""
    
    def remove_operation(self, operation_id: UUID) -> tuple[bool, str]:
        """
        Remove an operation from this routing.
        
        Args:
            operation_id: The operation to remove
            
        Returns:
            Tuple of (success, error_message)
        """
        for i, op in enumerate(self.operations):
            if op.operation_id == operation_id:
                self.operations.pop(i)
                self.updated_at = datetime.now(timezone.utc)
                return True, ""
        return False, f"Operation {operation_id} not found"
    
    def get_total_time(self, quantity: Decimal) -> Decimal:
        """
        Compute total routing time for a given quantity.
        
        Args:
            quantity: Number of units to produce
            
        Returns:
            Total time in hours (sum of all operation times)
        """
        return sum(op.total_time(quantity) for op in self.operations if op.active)
    
    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate routing integrity.
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        if not self.operations:
            errors.append("Routing must have at least one operation")
        
        sequences = set()
        for op in self.operations:
            if op.sequence in sequences:
                errors.append(f"Duplicate operation sequence: {op.sequence}")
            sequences.add(op.sequence)
            
            valid, err = op.validate_times()
            if not valid:
                errors.append(f"Operation {op.operation_id}: {err}")
        
        return len(errors) == 0, errors


@dataclass
class WorkCenterCalendarEdge:
    """
    Edge linking WorkCenter to Calendar.
    
    Represents the 'work_center_requires_calendar' relationship:
    a work center uses a specific calendar for capacity calculations.
    """
    edge_id: UUID
    work_center_id: UUID
    calendar_id: UUID
    active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class RoutingRequiresCapacityEdge:
    """
    Edge linking Routing/Operation to WorkCenter.
    
    Represents the 'requires_capacity' relationship: an operation
    requires capacity from a specific work center.
    """
    edge_id: UUID
    routing_id: UUID
    operation_id: UUID
    work_center_id: UUID
    scenario_id: UUID
    active: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
