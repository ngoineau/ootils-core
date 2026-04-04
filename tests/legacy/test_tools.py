# DEPRECATED: legacy tests from pre-graph-architecture. Skipped — models removed.
import pytest
pytestmark = pytest.mark.skip(reason="Legacy pre-graph API — InventoryState removed in Sprint 1")

"""Tests for the AI agent tool interface."""

import pytest

from ootils_core.tools import SupplyChainTools


@pytest.fixture
def tools():
    return SupplyChainTools()


@pytest.fixture
def basic_supplier_list():
    return [{"name": "Supplier A", "lead_time_days": 14}]


class TestCalculateReorderPoint:
    def test_basic(self, tools):
        result = tools.calculate_reorder_point({"daily_demand": 10, "lead_time_days": 14})
        assert result["status"] == "ok"
        assert result["result"]["reorder_point"] == pytest.approx(140.0)
        assert result["result"]["safety_stock"] == pytest.approx(0.0)

    def test_with_variability(self, tools):
        result = tools.calculate_reorder_point(
            {
                "daily_demand": 50,
                "lead_time_days": 14,
                "demand_std_daily": 8,
                "lead_time_std_days": 2,
                "service_level": 0.95,
            }
        )
        assert result["status"] == "ok"
        assert result["result"]["safety_stock"] > 0
        assert result["result"]["reorder_point"] > 50 * 14  # above average demand

    def test_missing_required_param(self, tools):
        result = tools.calculate_reorder_point({"daily_demand": 10})
        assert result["status"] == "error"


class TestCalculateEOQ:
    def test_classic(self, tools):
        result = tools.calculate_eoq(
            {"annual_demand": 1000, "ordering_cost": 50, "unit_cost": 10}
        )
        assert result["status"] == "ok"
        assert result["result"]["eoq"] == pytest.approx(200.0, rel=0.01)

    def test_total_cost_positive(self, tools):
        result = tools.calculate_eoq(
            {"annual_demand": 1000, "ordering_cost": 50, "unit_cost": 10, "holding_cost_rate": 0.25}
        )
        assert result["result"]["annual_total_cost"] > 0

    def test_missing_required(self, tools):
        result = tools.calculate_eoq({"annual_demand": 1000, "ordering_cost": 50})
        assert result["status"] == "error"


class TestRecommendOrder:
    def _params(self, stock=5, demand=5.0, suppliers=None):
        default_suppliers = [{"name": "Supplier A", "lead_time_days": 14}]
        return {
            "sku": "SKU-001",
            "name": "Widget",
            "unit_cost": 10.0,
            "current_stock": stock,
            "daily_demand": demand,
            "suppliers": default_suppliers if suppliers is None else suppliers,
        }

    def test_recommendation_returned_when_needed(self, tools):
        result = tools.recommend_order(self._params(stock=5))
        assert result["status"] == "ok"
        assert result["result"]["order_quantity"] > 0
        assert result["result"]["rationale"]

    def test_no_action_when_stock_adequate(self, tools):
        result = tools.recommend_order(self._params(stock=10000))
        assert result["status"] == "no_action"

    def test_missing_supplier_returns_error(self, tools):
        result = tools.recommend_order(self._params(suppliers=[]))
        assert result["status"] == "error"

    def test_urgency_present(self, tools):
        result = tools.recommend_order(self._params(stock=0))
        assert result["status"] == "ok"
        assert result["result"]["urgency"] == "critical"

    def test_multiple_suppliers_selects_best(self, tools):
        result = tools.recommend_order(
            self._params(
                stock=5,
                suppliers=[
                    {"name": "Slow", "lead_time_days": 60, "reliability_score": 0.5},
                    {"name": "Fast", "lead_time_days": 5, "reliability_score": 1.0},
                ],
            )
        )
        assert result["status"] == "ok"
        assert result["result"]["supplier"] == "Fast"


class TestRankSuppliers:
    def test_basic_ranking(self, tools):
        result = tools.rank_suppliers(
            {
                "unit_cost": 10.0,
                "suppliers": [
                    {"name": "Slow", "lead_time_days": 60},
                    {"name": "Fast", "lead_time_days": 5},
                ],
            }
        )
        assert result["status"] == "ok"
        ranked = result["result"]["ranked_suppliers"]
        assert len(ranked) == 2
        assert ranked[0]["name"] == "Fast"

    def test_empty_suppliers(self, tools):
        result = tools.rank_suppliers({"unit_cost": 10.0, "suppliers": []})
        assert result["status"] == "error"


class TestAssessRisk:
    def test_critical_when_no_stock(self, tools):
        result = tools.assess_risk(
            {
                "current_stock": 0,
                "daily_demand": 10,
                "reorder_point": 100,
                "safety_stock": 30,
            }
        )
        assert result["status"] == "ok"
        assert result["result"]["urgency"] == "critical"

    def test_low_when_stock_adequate(self, tools):
        result = tools.assess_risk(
            {
                "current_stock": 500,
                "daily_demand": 10,
                "reorder_point": 100,
                "safety_stock": 30,
            }
        )
        assert result["status"] == "ok"
        assert result["result"]["urgency"] == "low"

    def test_assessment_text_present(self, tools):
        result = tools.assess_risk(
            {"current_stock": 50, "daily_demand": 10, "reorder_point": 100, "safety_stock": 30}
        )
        assert result["result"]["assessment"]


class TestToolSchemas:
    def test_schemas_returned(self, tools):
        schemas = tools.tool_schemas()
        assert isinstance(schemas, list)
        assert len(schemas) == 5

    def test_schema_format(self, tools):
        schemas = tools.tool_schemas()
        for schema in schemas:
            assert schema["type"] == "function"
            assert "function" in schema
            assert "name" in schema["function"]
            assert "description" in schema["function"]
            assert "parameters" in schema["function"]
