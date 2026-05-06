"""
Unit tests for CRP (Capacity Requirements Planning) models.

Tests WorkCenter, Routing, Operation, and edge models.
"""
import pytest
from decimal import Decimal
from uuid import uuid4
from datetime import datetime, timezone

from ootils_core.crp.models import (
    WorkCenter,
    Routing,
    Operation,
    WorkCenterCalendarEdge,
    RoutingRequiresCapacityEdge,
)


class TestWorkCenter:
    """Tests for WorkCenter model."""

    def test_create_work_center_minimal(self):
        """Test creating a work center with minimal fields."""
        wc_id = uuid4()
        wc = WorkCenter(
            work_center_id=wc_id,
            code="WC-001",
            description="Assembly Line A",
        )
        
        assert wc.work_center_id == wc_id
        assert wc.code == "WC-001"
        assert wc.description == "Assembly Line A"
        assert wc.capacity_per_day == Decimal("0")
        assert wc.efficiency == Decimal("1.0")
        assert wc.calendar_id is None
        assert wc.active is True
        assert isinstance(wc.created_at, datetime)

    def test_create_work_center_full(self):
        """Test creating a work center with all fields."""
        wc_id = uuid4()
        calendar_id = uuid4()
        wc = WorkCenter(
            work_center_id=wc_id,
            code="WC-002",
            description="Painting Booth",
            capacity_per_day=Decimal("100"),
            efficiency=Decimal("0.85"),
            calendar_id=calendar_id,
            active=True,
        )
        
        assert wc.capacity_per_day == Decimal("100")
        assert wc.efficiency == Decimal("0.85")
        assert wc.calendar_id == calendar_id

    def test_effective_capacity_calculation(self):
        """Test effective capacity computation with efficiency."""
        wc = WorkCenter(
            work_center_id=uuid4(),
            code="WC-003",
            description="Test Center",
            capacity_per_day=Decimal("100"),
            efficiency=Decimal("0.75"),
        )
        
        effective = wc.effective_capacity_per_day()
        assert effective == Decimal("75")

    def test_effective_capacity_full_efficiency(self):
        """Test effective capacity when efficiency is 100%."""
        wc = WorkCenter(
            work_center_id=uuid4(),
            code="WC-004",
            description="Full Efficiency",
            capacity_per_day=Decimal("200"),
            efficiency=Decimal("1.0"),
        )
        
        assert wc.effective_capacity_per_day() == Decimal("200")

    def test_validate_efficiency_valid(self):
        """Test efficiency validation with valid values."""
        for eff in [Decimal("0"), Decimal("0.5"), Decimal("1.0")]:
            wc = WorkCenter(
                work_center_id=uuid4(),
                code="WC-005",
                description="Test",
                efficiency=eff,
            )
            valid, error = wc.validate_efficiency()
            assert valid is True
            assert error == ""

    def test_validate_efficiency_negative(self):
        """Test efficiency validation with negative value."""
        wc = WorkCenter(
            work_center_id=uuid4(),
            code="WC-006",
            description="Test",
            efficiency=Decimal("-0.1"),
        )
        valid, error = wc.validate_efficiency()
        assert valid is False
        assert "must be between 0 and 1" in error

    def test_validate_efficiency_over_one(self):
        """Test efficiency validation with value > 1."""
        wc = WorkCenter(
            work_center_id=uuid4(),
            code="WC-007",
            description="Test",
            efficiency=Decimal("1.5"),
        )
        valid, error = wc.validate_efficiency()
        assert valid is False
        assert "must be between 0 and 1" in error


class TestOperation:
    """Tests for Operation model."""

    def test_create_operation_minimal(self):
        """Test creating an operation with minimal fields."""
        op_id = uuid4()
        routing_id = uuid4()
        wc_id = uuid4()
        
        op = Operation(
            operation_id=op_id,
            routing_id=routing_id,
            sequence=1,
            work_center_id=wc_id,
        )
        
        assert op.operation_id == op_id
        assert op.routing_id == routing_id
        assert op.sequence == 1
        assert op.work_center_id == wc_id
        assert op.setup_time == Decimal("0")
        assert op.run_time_per_unit == Decimal("0")
        assert op.description is None
        assert op.active is True

    def test_create_operation_full(self):
        """Test creating an operation with all fields."""
        op = Operation(
            operation_id=uuid4(),
            routing_id=uuid4(),
            sequence=2,
            work_center_id=uuid4(),
            setup_time=Decimal("1.5"),
            run_time_per_unit=Decimal("0.25"),
            description="Quality Inspection",
            active=True,
        )
        
        assert op.setup_time == Decimal("1.5")
        assert op.run_time_per_unit == Decimal("0.25")
        assert op.description == "Quality Inspection"

    def test_total_time_calculation(self):
        """Test total time computation for a quantity."""
        op = Operation(
            operation_id=uuid4(),
            routing_id=uuid4(),
            sequence=1,
            work_center_id=uuid4(),
            setup_time=Decimal("2"),
            run_time_per_unit=Decimal("0.5"),
        )
        
        # Total = setup + (run_time * quantity)
        # Total = 2 + (0.5 * 100) = 2 + 50 = 52
        total = op.total_time(Decimal("100"))
        assert total == Decimal("52")

    def test_total_time_zero_quantity(self):
        """Test total time with zero quantity (setup only)."""
        op = Operation(
            operation_id=uuid4(),
            routing_id=uuid4(),
            sequence=1,
            work_center_id=uuid4(),
            setup_time=Decimal("3"),
            run_time_per_unit=Decimal("0.1"),
        )
        
        assert op.total_time(Decimal("0")) == Decimal("3")

    def test_validate_sequence_valid(self):
        """Test sequence validation with valid values."""
        for seq in [1, 5, 100]:
            op = Operation(
                operation_id=uuid4(),
                routing_id=uuid4(),
                sequence=seq,
                work_center_id=uuid4(),
            )
            valid, error = op.validate_sequence()
            assert valid is True
            assert error == ""

    def test_validate_sequence_zero(self):
        """Test sequence validation with zero."""
        op = Operation(
            operation_id=uuid4(),
            routing_id=uuid4(),
            sequence=0,
            work_center_id=uuid4(),
        )
        valid, error = op.validate_sequence()
        assert valid is False
        assert "must be positive" in error

    def test_validate_sequence_negative(self):
        """Test sequence validation with negative value."""
        op = Operation(
            operation_id=uuid4(),
            routing_id=uuid4(),
            sequence=-1,
            work_center_id=uuid4(),
        )
        valid, error = op.validate_sequence()
        assert valid is False
        assert "must be positive" in error

    def test_validate_times_valid(self):
        """Test times validation with valid values."""
        op = Operation(
            operation_id=uuid4(),
            routing_id=uuid4(),
            sequence=1,
            work_center_id=uuid4(),
            setup_time=Decimal("1"),
            run_time_per_unit=Decimal("0.5"),
        )
        valid, error = op.validate_times()
        assert valid is True
        assert error == ""

    def test_validate_times_negative_setup(self):
        """Test times validation with negative setup time."""
        op = Operation(
            operation_id=uuid4(),
            routing_id=uuid4(),
            sequence=1,
            work_center_id=uuid4(),
            setup_time=Decimal("-1"),
            run_time_per_unit=Decimal("0.5"),
        )
        valid, error = op.validate_times()
        assert valid is False
        assert "Setup time cannot be negative" in error

    def test_validate_times_negative_run(self):
        """Test times validation with negative run time."""
        op = Operation(
            operation_id=uuid4(),
            routing_id=uuid4(),
            sequence=1,
            work_center_id=uuid4(),
            setup_time=Decimal("1"),
            run_time_per_unit=Decimal("-0.1"),
        )
        valid, error = op.validate_times()
        assert valid is False
        assert "Run time per unit cannot be negative" in error


class TestRouting:
    """Tests for Routing model."""

    def test_create_routing_minimal(self):
        """Test creating a routing with minimal fields."""
        routing_id = uuid4()
        item_id = uuid4()
        
        routing = Routing(
            routing_id=routing_id,
            item_id=item_id,
        )
        
        assert routing.routing_id == routing_id
        assert routing.item_id == item_id
        assert routing.sequence == 1
        assert routing.description is None
        assert routing.operations == []
        assert routing.active is True

    def test_create_routing_with_sequence(self):
        """Test creating a routing with alternate sequence."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
            sequence=2,
            description="Alternate Routing",
        )
        
        assert routing.sequence == 2
        assert routing.description == "Alternate Routing"

    def test_add_operation(self):
        """Test adding an operation to a routing."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        op = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,
            work_center_id=uuid4(),
        )
        
        success, error = routing.add_operation(op)
        assert success is True
        assert error == ""
        assert len(routing.operations) == 1
        assert routing.operations[0].operation_id == op.operation_id

    def test_add_operation_sorts_by_sequence(self):
        """Test that operations are sorted by sequence after add."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        # Add operations out of order
        op3 = Operation(operation_id=uuid4(), routing_id=routing.routing_id, sequence=3, work_center_id=uuid4())
        op1 = Operation(operation_id=uuid4(), routing_id=routing.routing_id, sequence=1, work_center_id=uuid4())
        op2 = Operation(operation_id=uuid4(), routing_id=routing.routing_id, sequence=2, work_center_id=uuid4())
        
        routing.add_operation(op3)
        routing.add_operation(op1)
        routing.add_operation(op2)
        
        assert len(routing.operations) == 3
        assert routing.operations[0].sequence == 1
        assert routing.operations[1].sequence == 2
        assert routing.operations[2].sequence == 3

    def test_add_operation_duplicate_sequence(self):
        """Test adding operation with duplicate sequence fails."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        op1 = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,
            work_center_id=uuid4(),
        )
        op2 = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,  # Duplicate
            work_center_id=uuid4(),
        )
        
        routing.add_operation(op1)
        success, error = routing.add_operation(op2)
        
        assert success is False
        assert "Duplicate operation sequence" in error
        assert len(routing.operations) == 1

    def test_remove_operation(self):
        """Test removing an operation from a routing."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        op = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,
            work_center_id=uuid4(),
        )
        
        routing.add_operation(op)
        assert len(routing.operations) == 1
        
        success, error = routing.remove_operation(op.operation_id)
        assert success is True
        assert len(routing.operations) == 0

    def test_remove_operation_not_found(self):
        """Test removing non-existent operation."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        success, error = routing.remove_operation(uuid4())
        assert success is False
        assert "not found" in error

    def test_get_total_time(self):
        """Test computing total routing time for a quantity."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        # Op1: setup=1, run=0.5
        # Op2: setup=2, run=0.25
        op1 = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,
            work_center_id=uuid4(),
            setup_time=Decimal("1"),
            run_time_per_unit=Decimal("0.5"),
        )
        op2 = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=2,
            work_center_id=uuid4(),
            setup_time=Decimal("2"),
            run_time_per_unit=Decimal("0.25"),
        )
        
        routing.add_operation(op1)
        routing.add_operation(op2)
        
        # For quantity 100:
        # Op1: 1 + 0.5*100 = 51
        # Op2: 2 + 0.25*100 = 27
        # Total: 78
        total = routing.get_total_time(Decimal("100"))
        assert total == Decimal("78")

    def test_get_total_time_excludes_inactive(self):
        """Test that inactive operations are excluded from total time."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        op1 = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,
            work_center_id=uuid4(),
            setup_time=Decimal("1"),
            run_time_per_unit=Decimal("0.5"),
            active=True,
        )
        op2 = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=2,
            work_center_id=uuid4(),
            setup_time=Decimal("2"),
            run_time_per_unit=Decimal("0.25"),
            active=False,  # Inactive
        )
        
        routing.add_operation(op1)
        routing.add_operation(op2)
        
        # Only op1 should be counted: 1 + 0.5*100 = 51
        total = routing.get_total_time(Decimal("100"))
        assert total == Decimal("51")

    def test_validate_empty_routing(self):
        """Test validation fails for empty routing."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        valid, errors = routing.validate()
        assert valid is False
        assert any("must have at least one operation" in e for e in errors)

    def test_validate_valid_routing(self):
        """Test validation passes for valid routing."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        op = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,
            work_center_id=uuid4(),
            setup_time=Decimal("1"),
            run_time_per_unit=Decimal("0.5"),
        )
        
        routing.add_operation(op)
        valid, errors = routing.validate()
        assert valid is True
        assert errors == []

    def test_validate_duplicate_sequences(self):
        """Test validation catches duplicate sequences."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        # Manually add operations with duplicate sequences (bypass add_operation check)
        op1 = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,
            work_center_id=uuid4(),
        )
        op2 = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,  # Duplicate
            work_center_id=uuid4(),
        )
        
        routing.operations = [op1, op2]
        valid, errors = routing.validate()
        assert valid is False
        assert any("Duplicate operation sequence" in e for e in errors)

    def test_validate_invalid_operation_times(self):
        """Test validation catches invalid operation times."""
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        op = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,
            work_center_id=uuid4(),
            setup_time=Decimal("-1"),  # Invalid
            run_time_per_unit=Decimal("0.5"),
        )
        
        routing.add_operation(op)
        valid, errors = routing.validate()
        assert valid is False
        assert any("Setup time cannot be negative" in e for e in errors)


class TestWorkCenterCalendarEdge:
    """Tests for WorkCenterCalendarEdge model."""

    def test_create_edge(self):
        """Test creating a work center calendar edge."""
        edge_id = uuid4()
        wc_id = uuid4()
        cal_id = uuid4()
        
        edge = WorkCenterCalendarEdge(
            edge_id=edge_id,
            work_center_id=wc_id,
            calendar_id=cal_id,
        )
        
        assert edge.edge_id == edge_id
        assert edge.work_center_id == wc_id
        assert edge.calendar_id == cal_id
        assert edge.active is True
        assert isinstance(edge.created_at, datetime)


class TestRoutingRequiresCapacityEdge:
    """Tests for RoutingRequiresCapacityEdge model."""

    def test_create_edge(self):
        """Test creating a routing requires capacity edge."""
        edge_id = uuid4()
        routing_id = uuid4()
        op_id = uuid4()
        wc_id = uuid4()
        scenario_id = uuid4()
        
        edge = RoutingRequiresCapacityEdge(
            edge_id=edge_id,
            routing_id=routing_id,
            operation_id=op_id,
            work_center_id=wc_id,
            scenario_id=scenario_id,
        )
        
        assert edge.edge_id == edge_id
        assert edge.routing_id == routing_id
        assert edge.operation_id == op_id
        assert edge.work_center_id == wc_id
        assert edge.scenario_id == scenario_id
        assert edge.active is True


class TestIntegration:
    """Integration tests for CRP models working together."""

    def test_full_routing_workflow(self):
        """Test complete routing creation with operations."""
        # Create work center
        wc = WorkCenter(
            work_center_id=uuid4(),
            code="WC-ASSEMBLY",
            description="Final Assembly",
            capacity_per_day=Decimal("500"),
            efficiency=Decimal("0.9"),
        )
        
        # Create routing
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
            sequence=1,
            description="Standard Assembly Routing",
        )
        
        # Add operations
        op1 = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=10,
            work_center_id=wc.work_center_id,
            setup_time=Decimal("0.5"),
            run_time_per_unit=Decimal("0.1"),
            description="Sub-assembly",
        )
        
        op2 = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=20,
            work_center_id=wc.work_center_id,
            setup_time=Decimal("1"),
            run_time_per_unit=Decimal("0.2"),
            description="Final assembly",
        )
        
        routing.add_operation(op1)
        routing.add_operation(op2)
        
        # Validate
        valid, errors = routing.validate()
        assert valid is True
        assert len(errors) == 0
        
        # Compute time for 1000 units
        total_time = routing.get_total_time(Decimal("1000"))
        # Op1: 0.5 + 0.1*1000 = 100.5
        # Op2: 1 + 0.2*1000 = 201
        # Total: 301.5
        assert total_time == Decimal("301.5")
        
        # Create edge for capacity requirement
        edge = RoutingRequiresCapacityEdge(
            edge_id=uuid4(),
            routing_id=routing.routing_id,
            operation_id=op1.operation_id,
            work_center_id=wc.work_center_id,
            scenario_id=uuid4(),
        )
        
        assert edge.active is True

    def test_work_center_capacity_planning(self):
        """Test work center capacity calculations."""
        wc = WorkCenter(
            work_center_id=uuid4(),
            code="WC-PAINT",
            description="Painting Station",
            capacity_per_day=Decimal("80"),  # 80 hours/day
            efficiency=Decimal("0.85"),
        )
        
        # Effective capacity
        effective = wc.effective_capacity_per_day()
        assert effective == Decimal("68")  # 80 * 0.85
        
        # Validate efficiency
        valid, error = wc.validate_efficiency()
        assert valid is True
        
        # Routing that uses this work center
        routing = Routing(
            routing_id=uuid4(),
            item_id=uuid4(),
        )
        
        # Operation requiring 2 hours setup + 0.5 hours/unit
        op = Operation(
            operation_id=uuid4(),
            routing_id=routing.routing_id,
            sequence=1,
            work_center_id=wc.work_center_id,
            setup_time=Decimal("2"),
            run_time_per_unit=Decimal("0.5"),
        )
        
        routing.add_operation(op)
        
        # How many units can we produce in one day?
        # Available time: 68 hours
        # Time per batch: 2 + 0.5 * quantity = 68
        # 0.5 * quantity = 66
        # quantity = 132
        quantity = Decimal("132")
        time_needed = op.total_time(quantity)
        assert time_needed <= effective
        
        # 133 units would exceed capacity
        time_133 = op.total_time(Decimal("133"))
        assert time_133 > effective
