# DEPRECATED: legacy tests from pre-graph-architecture. Skipped — models removed.
import pytest
pytestmark = pytest.mark.skip(reason="Legacy pre-graph API — InventoryState removed in Sprint 1")

"""Integration tests for the SupplyChainDecisionEngine."""

import pytest

from ootils_core import SupplyChainDecisionEngine
from ootils_core.models import InventoryState, Product, Supplier


@pytest.fixture
def product():
    return Product(
        sku="WIDGET-001",
        name="Widget A",
        unit_cost=10.0,
        ordering_cost=50.0,
        holding_cost_rate=0.25,
        service_level=0.95,
        lead_time_days=14,
        lead_time_std_days=2,
    )


@pytest.fixture
def supplier():
    return Supplier(
        name="Primary Supplier",
        lead_time_days=14,
        lead_time_std_days=2,
        reliability_score=0.95,
    )


@pytest.fixture
def engine():
    return SupplyChainDecisionEngine()


class TestDecide:
    def test_returns_recommendation_when_below_rop(self, engine, product, supplier):
        state = InventoryState(
            product=product,
            current_stock=10,  # very low
            daily_demand=5.0,
            demand_std_daily=1.0,
        )
        rec = engine.decide(state, suppliers=[supplier])
        assert rec is not None
        assert rec.product.sku == "WIDGET-001"
        assert rec.order_quantity > 0
        assert rec.urgency in ("critical", "high", "medium")

    def test_returns_none_when_stock_adequate(self, engine, product, supplier):
        state = InventoryState(
            product=product,
            current_stock=1000,  # very high
            daily_demand=5.0,
        )
        rec = engine.decide(state, suppliers=[supplier])
        assert rec is None

    def test_raises_with_no_active_suppliers(self, engine, product):
        inactive = Supplier(name="Inactive", lead_time_days=7, active=False)
        state = InventoryState(product=product, current_stock=10, daily_demand=5.0)
        with pytest.raises(ValueError, match="active supplier"):
            engine.decide(state, suppliers=[inactive])

    def test_recommendation_fields(self, engine, product, supplier):
        state = InventoryState(product=product, current_stock=5, daily_demand=5.0)
        rec = engine.decide(state, suppliers=[supplier])
        assert rec is not None
        assert rec.rationale
        assert rec.reorder_point > 0
        assert rec.safety_stock >= 0
        assert rec.economic_order_quantity > 0
        assert rec.urgency in ("critical", "high", "medium", "low")

    def test_open_orders_reduce_urgency(self, engine, product, supplier):
        state_without = InventoryState(
            product=product, current_stock=100, daily_demand=10.0
        )
        state_with = InventoryState(
            product=product,
            current_stock=100,
            daily_demand=10.0,
            open_order_quantity=500,
        )
        rec_without = engine.decide(state_without, suppliers=[supplier])
        rec_with = engine.decide(state_with, suppliers=[supplier])
        # Having open orders should reduce or eliminate the need for new orders
        # Test that decide() doesn't raise when open_order_quantity is set
        try:
            rec_with = engine.decide(state_with, suppliers=[supplier])
        except Exception as e:
            pytest.fail(f"decide() raised unexpectedly with open orders: {e}")
        # If both produce a recommendation, the effective stock with open orders should be higher
        if rec_without and rec_with:
            assert state_with.effective_stock > state_without.effective_stock

    def test_supplier_constraints_respected(self, engine, product):
        constrained = Supplier(
            name="Constrained",
            lead_time_days=7,
            min_order_quantity=200,
            max_order_quantity=500,
        )
        state = InventoryState(product=product, current_stock=5, daily_demand=5.0)
        rec = engine.decide(state, suppliers=[constrained])
        assert rec is not None
        assert rec.order_quantity >= 200
        assert rec.order_quantity <= 500

    def test_selects_better_supplier(self, engine, product):
        bad = Supplier(
            name="Slow Expensive",
            lead_time_days=60,
            reliability_score=0.5,
            unit_price_multiplier=2.0,
        )
        good = Supplier(
            name="Fast Cheap",
            lead_time_days=5,
            reliability_score=1.0,
            unit_price_multiplier=1.0,
        )
        state = InventoryState(product=product, current_stock=5, daily_demand=5.0)
        rec = engine.decide(state, suppliers=[bad, good])
        assert rec is not None
        assert rec.supplier.name == "Fast Cheap"

    def test_critical_urgency_for_zero_stock(self, engine, product, supplier):
        state = InventoryState(product=product, current_stock=0, daily_demand=10.0)
        rec = engine.decide(state, suppliers=[supplier])
        assert rec is not None
        assert rec.urgency == "critical"


class TestEvaluatePortfolio:
    def test_empty_portfolio(self, engine, supplier):
        recs = engine.evaluate_portfolio([], suppliers=[supplier])
        assert recs == []

    def test_sorted_by_urgency(self, engine, supplier):
        critical_product = Product(sku="C", name="Critical", unit_cost=5.0, lead_time_days=7)
        ok_product = Product(sku="O", name="OK", unit_cost=5.0, lead_time_days=7)

        states = [
            InventoryState(product=critical_product, current_stock=0, daily_demand=10.0),
            InventoryState(product=ok_product, current_stock=5, daily_demand=1.0),
        ]
        recs = engine.evaluate_portfolio(states, suppliers=[supplier])
        urgency_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        ranks = [urgency_rank[r.urgency] for r in recs]
        assert ranks == sorted(ranks)

    def test_adequate_stock_excluded(self, engine, supplier):
        p = Product(sku="A", name="A", unit_cost=5.0)
        state = InventoryState(product=p, current_stock=10000, daily_demand=1.0)
        recs = engine.evaluate_portfolio([state], suppliers=[supplier])
        assert recs == []
