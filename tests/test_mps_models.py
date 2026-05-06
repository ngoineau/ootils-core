"""
Unit tests for MPS (Master Production Schedule) models.
MPS-001: MPS Node Model and Time Buckets
"""
import pytest
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

from ootils_core.mps.models import MPSNode, MPSStatus, MPSPlannedForEdge, MPSSuppliesEdge


class TestMPSStatus:
    """Test MPSStatus enum."""
    
    def test_status_values(self):
        """Verify all status values are defined."""
        assert MPSStatus.DRAFT.value == "DRAFT"
        assert MPSStatus.REVIEWED.value == "REVIEWED"
        assert MPSStatus.APPROVED.value == "APPROVED"
        assert MPSStatus.RELEASED.value == "RELEASED"


class TestMPSNode:
    """Test MPSNode model."""
    
    @pytest.fixture
    def sample_mps_node(self):
        """Create a sample MPS node for testing."""
        return MPSNode(
            mps_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            time_bucket="2026-W15",
            time_bucket_start=date(2026, 4, 6),
            time_bucket_end=date(2026, 4, 12),
            time_grain="weekly",
            forecast_quantity=Decimal("100.0"),
            sales_orders_quantity=Decimal("50.0"),
        )
    
    def test_mps_node_creation(self, sample_mps_node):
        """Test basic MPS node creation."""
        assert sample_mps_node.status == MPSStatus.DRAFT
        assert sample_mps_node.forecast_quantity == Decimal("100.0")
        assert sample_mps_node.sales_orders_quantity == Decimal("50.0")
        assert sample_mps_node.total_demand == Decimal("0")  # Not computed yet
        assert sample_mps_node.planned_quantity == Decimal("0")
        assert sample_mps_node.active is True
    
    def test_compute_total_demand(self, sample_mps_node):
        """Test total demand computation."""
        result = sample_mps_node.compute_total_demand()
        assert result == Decimal("150.0")
        assert sample_mps_node.total_demand == Decimal("150.0")
    
    def test_default_values(self):
        """Test default field values."""
        node = MPSNode(
            mps_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            time_bucket="2026-W15",
            time_bucket_start=date(2026, 4, 6),
            time_bucket_end=date(2026, 4, 12),
        )
        
        assert node.forecast_quantity == Decimal("0")
        assert node.sales_orders_quantity == Decimal("0")
        assert node.total_demand == Decimal("0")
        assert node.planned_quantity == Decimal("0")
        assert node.status == MPSStatus.DRAFT
        assert node.time_grain == "weekly"
        assert node.active is True
        assert node.notes is None
    
    def test_time_grain_options(self):
        """Test different time grain options."""
        for grain in ["daily", "weekly", "monthly"]:
            node = MPSNode(
                mps_id=uuid4(),
                item_id=uuid4(),
                location_id=uuid4(),
                scenario_id=uuid4(),
                time_bucket="2026-W15",
                time_bucket_start=date(2026, 4, 6),
                time_bucket_end=date(2026, 4, 12),
                time_grain=grain,
            )
            assert node.time_grain == grain


class TestMPSNodeStatusTransitions:
    """Test MPS node status transition logic."""
    
    @pytest.fixture
    def draft_node(self):
        """Create an MPS node in DRAFT status."""
        return MPSNode(
            mps_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            time_bucket="2026-W15",
            time_bucket_start=date(2026, 4, 6),
            time_bucket_end=date(2026, 4, 12),
            status=MPSStatus.DRAFT,
        )
    
    def test_can_transition_draft_to_reviewed(self, draft_node):
        """Test DRAFT -> REVIEWED transition is valid."""
        is_valid, error = draft_node.can_transition_to(MPSStatus.REVIEWED)
        assert is_valid is True
        assert error == ""
    
    def test_can_transition_draft_to_draft(self, draft_node):
        """Test DRAFT -> DRAFT transition is valid (no-op)."""
        is_valid, error = draft_node.can_transition_to(MPSStatus.DRAFT)
        assert is_valid is True
        assert error == ""
    
    def test_cannot_transition_draft_to_approved(self, draft_node):
        """Test DRAFT -> APPROVED transition is invalid."""
        is_valid, error = draft_node.can_transition_to(MPSStatus.APPROVED)
        assert is_valid is False
        assert "Cannot transition from DRAFT to APPROVED" in error
    
    def test_cannot_transition_draft_to_released(self, draft_node):
        """Test DRAFT -> RELEASED transition is invalid."""
        is_valid, error = draft_node.can_transition_to(MPSStatus.RELEASED)
        assert is_valid is False
        assert "Cannot transition from DRAFT to RELEASED" in error
    
    def test_transition_to_reviewed_updates_audit_trail(self, draft_node):
        """Test REVIEWED transition updates audit fields."""
        success, error = draft_node.transition_to(MPSStatus.REVIEWED, "user123")
        
        assert success is True
        assert draft_node.status == MPSStatus.REVIEWED
        assert draft_node.reviewed_by == "user123"
        assert draft_node.reviewed_at is not None
        assert draft_node.updated_at is not None
    
    def test_reviewed_to_approved(self, draft_node):
        """Test REVIEWED -> APPROVED transition."""
        # First transition to REVIEWED
        draft_node.transition_to(MPSStatus.REVIEWED, "reviewer1")
        
        # Then to APPROVED
        success, error = draft_node.transition_to(MPSStatus.APPROVED, "approver1")
        
        assert success is True
        assert draft_node.status == MPSStatus.APPROVED
        assert draft_node.approved_by == "approver1"
        assert draft_node.approved_at is not None
    
    def test_approved_to_released(self, draft_node):
        """Test APPROVED -> RELEASED transition."""
        draft_node.transition_to(MPSStatus.REVIEWED, "reviewer1")
        draft_node.transition_to(MPSStatus.APPROVED, "approver1")
        
        success, error = draft_node.transition_to(MPSStatus.RELEASED, "planner1")
        
        assert success is True
        assert draft_node.status == MPSStatus.RELEASED
        assert draft_node.released_by == "planner1"
        assert draft_node.released_at is not None
    
    def test_released_is_terminal_state(self, draft_node):
        """Test RELEASED is a terminal state (no outgoing transitions)."""
        draft_node.transition_to(MPSStatus.REVIEWED, "reviewer1")
        draft_node.transition_to(MPSStatus.APPROVED, "approver1")
        draft_node.transition_to(MPSStatus.RELEASED, "planner1")
        
        # Try to transition to any other state
        for status in [MPSStatus.DRAFT, MPSStatus.REVIEWED, MPSStatus.APPROVED]:
            is_valid, error = draft_node.can_transition_to(status)
            assert is_valid is False
    
    def test_reviewed_can_go_back_to_draft(self, draft_node):
        """Test REVIEWED -> DRAFT transition (revert)."""
        draft_node.transition_to(MPSStatus.REVIEWED, "reviewer1")
        
        success, error = draft_node.transition_to(MPSStatus.DRAFT, "user123")
        
        assert success is True
        assert draft_node.status == MPSStatus.DRAFT
    
    def test_approved_can_go_back_to_reviewed(self, draft_node):
        """Test APPROVED -> REVIEWED transition (revert)."""
        draft_node.transition_to(MPSStatus.REVIEWED, "reviewer1")
        draft_node.transition_to(MPSStatus.APPROVED, "approver1")
        
        success, error = draft_node.transition_to(MPSStatus.REVIEWED, "user123")
        
        assert success is True
        assert draft_node.status == MPSStatus.REVIEWED


class TestMPSPlannedForEdge:
    """Test MPSPlannedForEdge model."""
    
    def test_edge_creation(self):
        """Test basic edge creation."""
        edge = MPSPlannedForEdge(
            edge_id=uuid4(),
            mps_node_id=uuid4(),
            item_id=uuid4(),
            scenario_id=uuid4(),
        )
        
        assert edge.active is True
        assert edge.created_at is not None
    
    def test_edge_with_explicit_active(self):
        """Test edge creation with explicit active flag."""
        edge = MPSPlannedForEdge(
            edge_id=uuid4(),
            mps_node_id=uuid4(),
            item_id=uuid4(),
            scenario_id=uuid4(),
            active=False,
        )
        
        assert edge.active is False


class TestMPSSuppliesEdge:
    """Test MPSSuppliesEdge model."""
    
    def test_edge_creation(self):
        """Test basic supplies edge creation."""
        edge = MPSSuppliesEdge(
            edge_id=uuid4(),
            mps_node_id=uuid4(),
            planned_supply_node_id=uuid4(),
            scenario_id=uuid4(),
        )
        
        assert edge.active is True
        assert edge.quantity_pegged == Decimal("0")
        assert edge.created_at is not None
    
    def test_edge_with_pegged_quantity(self):
        """Test edge with pegged quantity."""
        edge = MPSSuppliesEdge(
            edge_id=uuid4(),
            mps_node_id=uuid4(),
            planned_supply_node_id=uuid4(),
            scenario_id=uuid4(),
            quantity_pegged=Decimal("150.0"),
        )
        
        assert edge.quantity_pegged == Decimal("150.0")


class TestMPSNodeWithRealData:
    """Integration-style tests with realistic data."""
    
    def test_weekly_bucket_pattern(self):
        """Test weekly time bucket pattern."""
        node = MPSNode(
            mps_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            time_bucket="2026-W15",
            time_bucket_start=date(2026, 4, 6),  # Monday
            time_bucket_end=date(2026, 4, 12),    # Sunday
            time_grain="weekly",
            forecast_quantity=Decimal("500.0"),
            sales_orders_quantity=Decimal("250.0"),
            planned_quantity=Decimal("750.0"),
        )
        
        node.compute_total_demand()
        assert node.total_demand == Decimal("750.0")
        assert node.planned_quantity == node.total_demand
    
    def test_monthly_bucket_pattern(self):
        """Test monthly time bucket pattern."""
        node = MPSNode(
            mps_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            time_bucket="2026-04",
            time_bucket_start=date(2026, 4, 1),
            time_bucket_end=date(2026, 4, 30),
            time_grain="monthly",
            forecast_quantity=Decimal("2000.0"),
            sales_orders_quantity=Decimal("1500.0"),
        )
        
        node.compute_total_demand()
        assert node.total_demand == Decimal("3500.0")
    
    def test_daily_bucket_pattern(self):
        """Test daily time bucket pattern."""
        node = MPSNode(
            mps_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            time_bucket="2026-04-06",
            time_bucket_start=date(2026, 4, 6),
            time_bucket_end=date(2026, 4, 6),
            time_grain="daily",
            forecast_quantity=Decimal("100.0"),
            sales_orders_quantity=Decimal("75.0"),
        )
        
        node.compute_total_demand()
        assert node.total_demand == Decimal("175.0")
    
    def test_full_workflow_simulation(self):
        """Simulate complete MPS workflow from DRAFT to RELEASED."""
        node = MPSNode(
            mps_id=uuid4(),
            item_id=uuid4(),
            location_id=uuid4(),
            scenario_id=uuid4(),
            time_bucket="2026-W15",
            time_bucket_start=date(2026, 4, 6),
            time_bucket_end=date(2026, 4, 12),
            forecast_quantity=Decimal("1000.0"),
            sales_orders_quantity=Decimal("500.0"),
            created_by="planner_001",
        )
        
        # Compute demand
        node.compute_total_demand()
        assert node.total_demand == Decimal("1500.0")
        
        # Set planned quantity
        node.planned_quantity = Decimal("1500.0")
        
        # Review
        success, _ = node.transition_to(MPSStatus.REVIEWED, "manager_001")
        assert success
        
        # Approve
        success, _ = node.transition_to(MPSStatus.APPROVED, "director_001")
        assert success
        
        # Release to MRP
        success, _ = node.transition_to(MPSStatus.RELEASED, "mrp_system")
        assert success
        
        # Verify audit trail
        assert node.created_by == "planner_001"
        assert node.reviewed_by == "manager_001"
        assert node.approved_by == "director_001"
        assert node.released_by == "mrp_system"
        assert node.reviewed_at is not None
        assert node.approved_at is not None
        assert node.released_at is not None
