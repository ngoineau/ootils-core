# DEPRECATED: legacy tests from pre-graph-architecture. Skipped — models removed.
import pytest
pytestmark = pytest.mark.skip(reason="Legacy pre-graph API — InventoryState removed in Sprint 1")

"""Tests for supplier selection logic."""

import pytest

from ootils_core.engine.supplier_selection import rank_suppliers, select_supplier
from ootils_core.models import Supplier


def make_supplier(name, lead_time=14, reliability=1.0, price_mult=1.0):
    return Supplier(
        name=name,
        lead_time_days=lead_time,
        reliability_score=reliability,
        unit_price_multiplier=price_mult,
    )


class TestSelectSupplier:
    def test_single_supplier(self):
        s = make_supplier("Only One")
        selected = select_supplier([s], base_unit_cost=10.0)
        assert selected.name == "Only One"

    def test_prefers_faster_lead_time(self):
        slow = make_supplier("Slow", lead_time=30)
        fast = make_supplier("Fast", lead_time=5)
        selected = select_supplier([slow, fast], base_unit_cost=10.0)
        assert selected.name == "Fast"

    def test_prefers_lower_cost(self):
        expensive = make_supplier("Expensive", lead_time=14, price_mult=1.5)
        cheap = make_supplier("Cheap", lead_time=14, price_mult=1.0)
        selected = select_supplier([expensive, cheap], base_unit_cost=10.0)
        assert selected.name == "Cheap"

    def test_prefers_higher_reliability(self):
        unreliable = make_supplier("Unreliable", lead_time=7, reliability=0.6)
        reliable = make_supplier("Reliable", lead_time=7, reliability=1.0)
        selected = select_supplier([unreliable, reliable], base_unit_cost=10.0)
        assert selected.name == "Reliable"

    def test_empty_list_raises(self):
        with pytest.raises(ValueError):
            select_supplier([], base_unit_cost=10.0)


class TestRankSuppliers:
    def test_returns_all_suppliers(self):
        suppliers = [make_supplier(f"S{i}") for i in range(5)]
        ranked = rank_suppliers(suppliers, base_unit_cost=10.0)
        assert len(ranked) == 5

    def test_first_is_best(self):
        best = make_supplier("Best", lead_time=5, reliability=1.0, price_mult=1.0)
        worst = make_supplier("Worst", lead_time=60, reliability=0.5, price_mult=2.0)
        ranked = rank_suppliers([worst, best], base_unit_cost=10.0)
        assert ranked[0][0].name == "Best"
        assert ranked[-1][0].name == "Worst"

    def test_scores_are_descending(self):
        suppliers = [make_supplier(f"S{i}", lead_time=i * 5 + 1) for i in range(4)]
        ranked = rank_suppliers(suppliers, base_unit_cost=10.0)
        scores = [score for _, score in ranked]
        assert scores == sorted(scores, reverse=True)
