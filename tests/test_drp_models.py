"""
Unit tests for DRP (Distribution Requirements Planning) models.

Tests DistributionLink, TransportationLane, and edge models.
"""
import pytest
from decimal import Decimal
from uuid import uuid4
from datetime import datetime, timezone

from ootils_core.drp.models import (
    DistributionLink,
    TransportationLane,
    DistributionLinkEdge,
    LaneRequiresLinkEdge,
)


class TestDistributionLink:
    """Tests for DistributionLink model."""

    def test_create_distribution_link_minimal(self):
        """Test creating a distribution link with minimal fields."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
        )
        
        assert link.upstream_location_id is not None
        assert link.downstream_location_id is not None
        assert link.item_id is None
        assert link.transit_lead_time_days == Decimal("7")
        assert link.transit_cost_per_unit is None
        assert link.transit_cost_fixed is None
        assert link.minimum_shipment_qty == Decimal("1")
        assert link.maximum_shipment_qty is None
        assert link.shipment_frequency is None
        assert link.shipment_days is None
        assert link.active is True
        assert link.priority == 100
        assert isinstance(link.created_at, datetime)

    def test_create_distribution_link_full(self):
        """Test creating a distribution link with all fields."""
        upstream_id = uuid4()
        downstream_id = uuid4()
        item_id = uuid4()
        
        link = DistributionLink(
            upstream_location_id=upstream_id,
            downstream_location_id=downstream_id,
            item_id=item_id,
            transit_lead_time_days=Decimal("5"),
            transit_cost_per_unit=Decimal("10.50"),
            transit_cost_fixed=Decimal("100"),
            minimum_shipment_qty=Decimal("100"),
            maximum_shipment_qty=Decimal("500"),
            shipment_frequency="weekly",
            shipment_days=[2, 4],
            active=True,
            priority=50,
        )
        
        assert link.upstream_location_id == upstream_id
        assert link.downstream_location_id == downstream_id
        assert link.item_id == item_id
        assert link.transit_lead_time_days == Decimal("5")
        assert link.transit_cost_per_unit == Decimal("10.50")
        assert link.transit_cost_fixed == Decimal("100")
        assert link.minimum_shipment_qty == Decimal("100")
        assert link.maximum_shipment_qty == Decimal("500")
        assert link.shipment_frequency == "weekly"
        assert link.shipment_days == [2, 4]
        assert link.priority == 50

    def test_is_generic_link_true(self):
        """Test is_generic_link returns True when item_id is None."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            item_id=None,
        )
        assert link.is_generic_link() is True

    def test_is_generic_link_false(self):
        """Test is_generic_link returns False when item_id is set."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            item_id=uuid4(),
        )
        assert link.is_generic_link() is False

    def test_can_ship_on_day_no_restrictions(self):
        """Test can_ship_on_day when no shipment_days restriction."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            shipment_days=None,
        )
        for day in range(1, 8):
            assert link.can_ship_on_day(day) is True

    def test_can_ship_on_day_with_restrictions(self):
        """Test can_ship_on_day with specific shipment_days."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            shipment_days=[2, 4],  # Tuesday and Thursday
        )
        assert link.can_ship_on_day(1) is False  # Monday
        assert link.can_ship_on_day(2) is True   # Tuesday
        assert link.can_ship_on_day(3) is False  # Wednesday
        assert link.can_ship_on_day(4) is True   # Thursday
        assert link.can_ship_on_day(5) is False  # Friday

    def test_get_transit_cost_variable_only(self):
        """Test transit cost calculation with variable cost only."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            transit_cost_per_unit=Decimal("5"),
            transit_cost_fixed=None,
        )
        assert link.get_transit_cost(Decimal("10")) == Decimal("50")
        assert link.get_transit_cost(Decimal("100")) == Decimal("500")

    def test_get_transit_cost_fixed_only(self):
        """Test transit cost calculation with fixed cost only."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            transit_cost_per_unit=None,
            transit_cost_fixed=Decimal("100"),
        )
        assert link.get_transit_cost(Decimal("10")) == Decimal("100")
        assert link.get_transit_cost(Decimal("100")) == Decimal("100")

    def test_get_transit_cost_both(self):
        """Test transit cost calculation with both fixed and variable costs."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            transit_cost_per_unit=Decimal("2"),
            transit_cost_fixed=Decimal("50"),
        )
        assert link.get_transit_cost(Decimal("25")) == Decimal("100")  # 50 + 2*25

    def test_get_transit_cost_no_costs(self):
        """Test transit cost calculation when no costs defined."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            transit_cost_per_unit=None,
            transit_cost_fixed=None,
        )
        assert link.get_transit_cost(Decimal("100")) == Decimal("0")

    def test_respects_minimum_qty_true(self):
        """Test respects_minimum_qty when quantity meets minimum."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            minimum_shipment_qty=Decimal("100"),
        )
        assert link.respects_minimum_qty(Decimal("100")) is True
        assert link.respects_minimum_qty(Decimal("150")) is True

    def test_respects_minimum_qty_false(self):
        """Test respects_minimum_qty when quantity below minimum."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            minimum_shipment_qty=Decimal("100"),
        )
        assert link.respects_minimum_qty(Decimal("50")) is False

    def test_respects_maximum_qty_true(self):
        """Test respects_maximum_qty when quantity within limit."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            maximum_shipment_qty=Decimal("500"),
        )
        assert link.respects_maximum_qty(Decimal("400")) is True
        assert link.respects_maximum_qty(Decimal("500")) is True

    def test_respects_maximum_qty_false(self):
        """Test respects_maximum_qty when quantity exceeds limit."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            maximum_shipment_qty=Decimal("500"),
        )
        assert link.respects_maximum_qty(Decimal("600")) is False

    def test_respects_maximum_qty_none(self):
        """Test respects_maximum_qty when no maximum defined."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            maximum_shipment_qty=None,
        )
        assert link.respects_maximum_qty(Decimal("10000")) is True

    def test_validate_valid(self):
        """Test validate with valid distribution link."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            transit_lead_time_days=Decimal("7"),
            minimum_shipment_qty=Decimal("1"),
            maximum_shipment_qty=Decimal("100"),
            priority=1,
        )
        valid, errors = link.validate()
        assert valid is True
        assert len(errors) == 0

    def test_validate_same_locations_error(self):
        """Test validate catches same upstream and downstream locations."""
        location_id = uuid4()
        link = DistributionLink(
            upstream_location_id=location_id,
            downstream_location_id=location_id,
        )
        valid, errors = link.validate()
        assert valid is False
        assert "upstream and downstream locations must be different" in errors

    def test_validate_missing_upstream_error(self):
        """Test validate catches missing upstream_location_id."""
        link = DistributionLink(
            upstream_location_id=None,
            downstream_location_id=uuid4(),
        )
        valid, errors = link.validate()
        assert valid is False
        assert "upstream_location_id is required" in errors

    def test_validate_missing_downstream_error(self):
        """Test validate catches missing downstream_location_id."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=None,
        )
        valid, errors = link.validate()
        assert valid is False
        assert "downstream_location_id is required" in errors

    def test_validate_negative_lead_time_error(self):
        """Test validate catches negative transit_lead_time_days."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            transit_lead_time_days=Decimal("-5"),
        )
        valid, errors = link.validate()
        assert valid is False
        assert any("transit_lead_time_days cannot be negative" in e for e in errors)

    def test_validate_negative_minimum_qty_error(self):
        """Test validate catches negative minimum_shipment_qty."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            minimum_shipment_qty=Decimal("-10"),
        )
        valid, errors = link.validate()
        assert valid is False
        assert any("minimum_shipment_qty cannot be negative" in e for e in errors)

    def test_validate_maximum_less_than_minimum_error(self):
        """Test validate catches maximum < minimum."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            minimum_shipment_qty=Decimal("100"),
            maximum_shipment_qty=Decimal("50"),
        )
        valid, errors = link.validate()
        assert valid is False
        assert any("maximum_shipment_qty" in e and "minimum_shipment_qty" in e for e in errors)

    def test_validate_invalid_priority_error(self):
        """Test validate catches priority < 1."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            priority=0,
        )
        valid, errors = link.validate()
        assert valid is False
        assert any("priority must be >= 1" in e for e in errors)


class TestTransportationLane:
    """Tests for TransportationLane model."""

    def test_create_transportation_lane_minimal(self):
        """Test creating a transportation lane with minimal fields."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
        )
        
        assert lane.distribution_link_id is not None
        assert lane.carrier is None
        assert lane.mode == "truck"
        assert lane.service_level == "standard"
        assert lane.transit_time_min_days == Decimal("1")
        assert lane.transit_time_max_days == Decimal("7")
        assert lane.cost_per_unit is None
        assert lane.cost_per_shipment is None
        assert lane.minimum_weight is None
        assert lane.maximum_weight is None
        assert lane.equipment_type is None
        assert lane.active is True
        assert isinstance(lane.created_at, datetime)

    def test_create_transportation_lane_full(self):
        """Test creating a transportation lane with all fields."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            carrier="FedEx",
            mode="air",
            service_level="expedited",
            transit_time_min_days=Decimal("1"),
            transit_time_max_days=Decimal("2"),
            cost_per_unit=Decimal("15"),
            cost_per_shipment=Decimal("200"),
            minimum_weight=Decimal("10"),
            maximum_weight=Decimal("1000"),
            equipment_type="Cargo aircraft",
            active=True,
        )
        
        assert lane.carrier == "FedEx"
        assert lane.mode == "air"
        assert lane.service_level == "expedited"
        assert lane.transit_time_min_days == Decimal("1")
        assert lane.transit_time_max_days == Decimal("2")
        assert lane.cost_per_unit == Decimal("15")
        assert lane.cost_per_shipment == Decimal("200")
        assert lane.minimum_weight == Decimal("10")
        assert lane.maximum_weight == Decimal("1000")
        assert lane.equipment_type == "Cargo aircraft"

    def test_get_transit_time_estimate(self):
        """Test transit time estimate calculation."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            transit_time_min_days=Decimal("3"),
            transit_time_max_days=Decimal("7"),
        )
        assert lane.get_transit_time_estimate() == Decimal("5")

    def test_get_transit_time_estimate_equal(self):
        """Test transit time estimate when min equals max."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            transit_time_min_days=Decimal("5"),
            transit_time_max_days=Decimal("5"),
        )
        assert lane.get_transit_time_estimate() == Decimal("5")

    def test_get_lane_cost_variable_only(self):
        """Test lane cost calculation with variable cost only."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            cost_per_unit=Decimal("3"),
            cost_per_shipment=None,
        )
        assert lane.get_lane_cost(Decimal("10")) == Decimal("30")

    def test_get_lane_cost_fixed_only(self):
        """Test lane cost calculation with fixed cost only."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            cost_per_unit=None,
            cost_per_shipment=Decimal("150"),
        )
        assert lane.get_lane_cost(Decimal("10")) == Decimal("150")
        assert lane.get_lane_cost(Decimal("100")) == Decimal("150")

    def test_get_lane_cost_both(self):
        """Test lane cost calculation with both costs."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            cost_per_unit=Decimal("2"),
            cost_per_shipment=Decimal("100"),
        )
        assert lane.get_lane_cost(Decimal("50")) == Decimal("200")  # 100 + 2*50

    def test_get_lane_cost_no_costs(self):
        """Test lane cost calculation when no costs defined."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            cost_per_unit=None,
            cost_per_shipment=None,
        )
        assert lane.get_lane_cost(Decimal("100")) == Decimal("0")

    def test_validate_valid(self):
        """Test validate with valid transportation lane."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            mode="truck",
            service_level="standard",
            transit_time_min_days=Decimal("3"),
            transit_time_max_days=Decimal("5"),
        )
        valid, errors = lane.validate()
        assert valid is True
        assert len(errors) == 0

    def test_validate_missing_distribution_link_error(self):
        """Test validate catches missing distribution_link_id."""
        lane = TransportationLane(
            distribution_link_id=None,
        )
        valid, errors = lane.validate()
        assert valid is False
        assert "distribution_link_id is required" in errors

    def test_validate_negative_transit_time_error(self):
        """Test validate catches negative transit_time_min_days."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            transit_time_min_days=Decimal("-2"),
            transit_time_max_days=Decimal("5"),
        )
        valid, errors = lane.validate()
        assert valid is False
        assert any("transit_time_min_days cannot be negative" in e for e in errors)

    def test_validate_transit_time_max_less_than_min_error(self):
        """Test validate catches max < min transit time."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            transit_time_min_days=Decimal("7"),
            transit_time_max_days=Decimal("3"),
        )
        valid, errors = lane.validate()
        assert valid is False
        assert any("transit_time_max_days" in e and "transit_time_min_days" in e for e in errors)

    def test_validate_negative_minimum_weight_error(self):
        """Test validate catches negative minimum_weight."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            minimum_weight=Decimal("-50"),
        )
        valid, errors = lane.validate()
        assert valid is False
        assert any("minimum_weight cannot be negative" in e for e in errors)

    def test_validate_maximum_weight_less_than_minimum_error(self):
        """Test validate catches maximum_weight < minimum_weight."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            minimum_weight=Decimal("100"),
            maximum_weight=Decimal("50"),
        )
        valid, errors = lane.validate()
        assert valid is False
        assert any("maximum_weight" in e and "minimum_weight" in e for e in errors)

    def test_validate_invalid_mode_error(self):
        """Test validate catches invalid mode."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            mode="teleport",
        )
        valid, errors = lane.validate()
        assert valid is False
        assert any("Invalid mode" in e for e in errors)

    def test_validate_valid_modes(self):
        """Test validate accepts all valid modes."""
        valid_modes = ["truck", "rail", "air", "ocean", "intermodal", "pipeline", "multimodal"]
        for mode in valid_modes:
            lane = TransportationLane(
                distribution_link_id=uuid4(),
                mode=mode,
            )
            valid, errors = lane.validate()
            assert valid is True, f"Mode '{mode}' should be valid but got errors: {errors}"

    def test_validate_invalid_service_level_error(self):
        """Test validate catches invalid service_level."""
        lane = TransportationLane(
            distribution_link_id=uuid4(),
            service_level="instant",
        )
        valid, errors = lane.validate()
        assert valid is False
        assert any("Invalid service_level" in e for e in errors)

    def test_validate_valid_service_levels(self):
        """Test validate accepts all valid service levels."""
        valid_levels = ["standard", "expedited", "economy", "premium", "same_day"]
        for level in valid_levels:
            lane = TransportationLane(
                distribution_link_id=uuid4(),
                service_level=level,
            )
            valid, errors = lane.validate()
            assert valid is True, f"Service level '{level}' should be valid but got errors: {errors}"


class TestDistributionLinkEdge:
    """Tests for DistributionLinkEdge model."""

    def test_create_edge_minimal(self):
        """Test creating a distribution link edge with minimal fields."""
        edge = DistributionLinkEdge(
            distribution_link_id=uuid4(),
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
        )
        
        assert edge.edge_id is not None
        assert edge.distribution_link_id is not None
        assert edge.upstream_location_id is not None
        assert edge.downstream_location_id is not None
        assert edge.item_id is None
        assert edge.active is True
        assert isinstance(edge.created_at, datetime)

    def test_create_edge_with_item(self):
        """Test creating a distribution link edge with item_id."""
        item_id = uuid4()
        edge = DistributionLinkEdge(
            distribution_link_id=uuid4(),
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            item_id=item_id,
        )
        
        assert edge.item_id == item_id


class TestLaneRequiresLinkEdge:
    """Tests for LaneRequiresLinkEdge model."""

    def test_create_edge_minimal(self):
        """Test creating a lane requires link edge with minimal fields."""
        edge = LaneRequiresLinkEdge(
            lane_id=uuid4(),
            distribution_link_id=uuid4(),
        )
        
        assert edge.edge_id is not None
        assert edge.lane_id is not None
        assert edge.distribution_link_id is not None
        assert edge.active is True
        assert isinstance(edge.created_at, datetime)


class TestDistributionLinkIntegration:
    """Integration tests for DistributionLink with TransportationLane."""

    def test_link_with_multiple_lanes(self):
        """Test a distribution link can have multiple transportation lanes."""
        link_id = uuid4()
        
        lane1 = TransportationLane(
            distribution_link_id=link_id,
            carrier="Carrier A",
            mode="truck",
            cost_per_unit=Decimal("5"),
        )
        
        lane2 = TransportationLane(
            distribution_link_id=link_id,
            carrier="Carrier B",
            mode="rail",
            cost_per_unit=Decimal("3"),
        )
        
        lane3 = TransportationLane(
            distribution_link_id=link_id,
            carrier="Carrier C",
            mode="air",
            cost_per_unit=Decimal("15"),
        )
        
        # All lanes should reference the same distribution link
        assert lane1.distribution_link_id == link_id
        assert lane2.distribution_link_id == link_id
        assert lane3.distribution_link_id == link_id
        
        # Different carriers should have different costs
        quantity = Decimal("100")
        assert lane1.get_lane_cost(quantity) == Decimal("500")
        assert lane2.get_lane_cost(quantity) == Decimal("300")
        assert lane3.get_lane_cost(quantity) == Decimal("1500")

    def test_link_and_lane_cost_comparison(self):
        """Test comparing costs between distribution link and its lanes."""
        link = DistributionLink(
            upstream_location_id=uuid4(),
            downstream_location_id=uuid4(),
            transit_cost_per_unit=Decimal("2"),
            transit_cost_fixed=Decimal("50"),
        )
        
        lane = TransportationLane(
            distribution_link_id=link.distribution_link_id,
            cost_per_unit=Decimal("3"),
            cost_per_shipment=Decimal("75"),
        )
        
        quantity = Decimal("100")
        link_cost = link.get_transit_cost(quantity)
        lane_cost = lane.get_lane_cost(quantity)
        
        assert link_cost == Decimal("250")  # 50 + 2*100
        assert lane_cost == Decimal("375")  # 75 + 3*100
        
        # Total cost would be link + lane
        total_cost = link_cost + lane_cost
        assert total_cost == Decimal("625")
