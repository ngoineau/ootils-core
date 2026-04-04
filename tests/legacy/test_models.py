# DEPRECATED: legacy tests from pre-graph-architecture. Skipped — models removed.
import pytest
pytestmark = pytest.mark.skip(reason="Legacy pre-graph API — InventoryState removed in Sprint 1")

"""Tests for supply chain data models."""

import pytest

from ootils_core.models import InventoryState, OrderRecommendation, Product, Supplier


class TestProduct:
    def test_basic_creation(self):
        p = Product(sku="SKU-001", name="Widget", unit_cost=10.0)
        assert p.sku == "SKU-001"
        assert p.name == "Widget"
        assert p.unit_cost == 10.0
        assert p.holding_cost_rate == 0.25
        assert p.service_level == 0.95

    def test_annual_holding_cost(self):
        p = Product(sku="A", name="B", unit_cost=20.0, holding_cost_rate=0.25)
        assert p.annual_holding_cost_per_unit == 5.0

    def test_invalid_unit_cost(self):
        with pytest.raises(ValueError, match="unit_cost"):
            Product(sku="A", name="B", unit_cost=0.0)

    def test_invalid_holding_cost_rate(self):
        with pytest.raises(ValueError, match="holding_cost_rate"):
            Product(sku="A", name="B", unit_cost=10.0, holding_cost_rate=0.0)

    def test_invalid_service_level_too_high(self):
        with pytest.raises(ValueError, match="service_level"):
            Product(sku="A", name="B", unit_cost=10.0, service_level=1.0)

    def test_invalid_service_level_too_low(self):
        with pytest.raises(ValueError, match="service_level"):
            Product(sku="A", name="B", unit_cost=10.0, service_level=0.0)

    def test_invalid_lead_time(self):
        with pytest.raises(ValueError, match="lead_time_days"):
            Product(sku="A", name="B", unit_cost=10.0, lead_time_days=-1)

    def test_ordering_cost_zero_is_valid(self):
        p = Product(sku="A", name="B", unit_cost=10.0, ordering_cost=0.0)
        assert p.ordering_cost == 0.0


class TestSupplier:
    def test_basic_creation(self):
        s = Supplier(name="Fast Co", lead_time_days=7)
        assert s.name == "Fast Co"
        assert s.lead_time_days == 7
        assert s.reliability_score == 1.0
        assert s.active is True

    def test_effective_unit_cost(self):
        s = Supplier(name="Supplier A", lead_time_days=10, unit_price_multiplier=1.1)
        assert s.effective_unit_cost(100.0) == pytest.approx(110.0)

    def test_clamp_quantity_min(self):
        s = Supplier(name="Supplier A", lead_time_days=10, min_order_quantity=50)
        assert s.clamp_quantity(10) == 50

    def test_clamp_quantity_max(self):
        s = Supplier(name="Supplier A", lead_time_days=10, max_order_quantity=100)
        assert s.clamp_quantity(200) == 100

    def test_clamp_quantity_within_range(self):
        s = Supplier(name="A", lead_time_days=5, min_order_quantity=10, max_order_quantity=100)
        assert s.clamp_quantity(50) == 50

    def test_invalid_reliability_score(self):
        with pytest.raises(ValueError, match="reliability_score"):
            Supplier(name="A", lead_time_days=5, reliability_score=1.5)

    def test_invalid_max_lt_min(self):
        with pytest.raises(ValueError, match="max_order_quantity"):
            Supplier(name="A", lead_time_days=5, min_order_quantity=100, max_order_quantity=50)

    def test_invalid_price_multiplier(self):
        with pytest.raises(ValueError, match="unit_price_multiplier"):
            Supplier(name="A", lead_time_days=5, unit_price_multiplier=0.0)


class TestInventoryState:
    def _product(self):
        return Product(sku="SKU-1", name="Item", unit_cost=5.0)

    def test_days_of_supply(self):
        state = InventoryState(product=self._product(), current_stock=100, daily_demand=10.0)
        assert state.days_of_supply == pytest.approx(10.0)

    def test_days_of_supply_zero_demand(self):
        state = InventoryState(product=self._product(), current_stock=100, daily_demand=0.0)
        assert state.days_of_supply == float("inf")

    def test_effective_stock(self):
        state = InventoryState(
            product=self._product(),
            current_stock=50,
            daily_demand=5.0,
            open_order_quantity=30,
        )
        assert state.effective_stock == 80.0

    def test_invalid_negative_stock(self):
        with pytest.raises(ValueError, match="current_stock"):
            InventoryState(product=self._product(), current_stock=-1, daily_demand=5.0)
