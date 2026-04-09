"""
Core supply chain decision engine.

.. deprecated::
    This module is **legacy** and no longer the primary engine.
    The current planning engine uses a graph-based kernel located under
    ``ootils_core.engine.kernel``. This file is retained for backward
    compatibility only and should not be used in new code.

The :class:`SupplyChainDecisionEngine` is the main entry-point. It combines
inventory policy calculations (EOQ, reorder point, safety stock) with supplier
selection logic to produce :class:`~ootils_core.models.OrderRecommendation`
objects that are ready to be acted upon by humans or AI agents.
"""

from __future__ import annotations

import math
from typing import Sequence

from ootils_core.models import InventoryState, OrderRecommendation, Supplier
from ootils_core.engine.policies import (
    economic_order_quantity,
    reorder_point,
    safety_stock,
    urgency_level,
)
from ootils_core.engine.supplier_selection import select_supplier


class SupplyChainDecisionEngine:
    """Evaluates inventory states and recommends purchasing decisions.

    The engine is stateless: all inputs are provided per call so it can be
    embedded in agent runtimes, workflows, or traditional web applications
    without lifecycle concerns.

    Example::

        from ootils_core import SupplyChainDecisionEngine
        from ootils_core.models import Product, Supplier, InventoryState

        engine = SupplyChainDecisionEngine()
        product = Product(sku="SKU-001", name="Widget A", unit_cost=10.0)
        supplier = Supplier(name="Fast Co", lead_time_days=7)
        state = InventoryState(product=product, current_stock=20, daily_demand=5.0)

        recommendation = engine.decide(state, suppliers=[supplier])
        print(recommendation.rationale)
    """

    def decide(
        self,
        state: InventoryState,
        suppliers: Sequence[Supplier],
        annual_demand: float | None = None,
    ) -> OrderRecommendation | None:
        """Evaluate *state* and return a purchasing recommendation, or ``None``.

        A recommendation is returned when the effective stock (on-hand plus
        open orders) falls at or below the calculated reorder point.  If
        stock is adequate, ``None`` is returned so callers can treat it as a
        "no-action" signal.

        Args:
            state: Current inventory snapshot for one product.
            suppliers: List of candidate suppliers.  At least one must be
                active.
            annual_demand: Override for annual demand (units/year).  Defaults
                to ``state.daily_demand * 365``.

        Returns:
            An :class:`~ootils_core.models.OrderRecommendation` when action is
            needed, otherwise ``None``.

        Raises:
            ValueError: If no active suppliers are provided.
        """
        active_suppliers = [s for s in suppliers if s.active]
        if not active_suppliers:
            raise ValueError(
                "At least one active supplier is required to generate a recommendation."
            )

        product = state.product
        lead_time = product.lead_time_days
        lead_time_std = product.lead_time_std_days

        ss = safety_stock(
            daily_demand=state.daily_demand,
            demand_std_daily=state.demand_std_daily,
            lead_time_days=lead_time,
            lead_time_std_days=lead_time_std,
            service_level=product.service_level,
        )

        rop = reorder_point(
            daily_demand=state.daily_demand,
            lead_time_days=lead_time,
            safety_stock=ss,
        )

        if state.effective_stock > rop:
            return None

        annual = annual_demand if annual_demand is not None else state.daily_demand * 365
        eoq = economic_order_quantity(
            annual_demand=annual,
            ordering_cost=product.ordering_cost,
            unit_cost=product.unit_cost,
            holding_cost_rate=product.holding_cost_rate,
        )

        supplier = select_supplier(active_suppliers, product.unit_cost)
        order_qty = supplier.clamp_quantity(math.ceil(eoq))

        urgency = urgency_level(
            current_stock=state.current_stock,
            daily_demand=state.daily_demand,
            reorder_point=rop,
            safety_stock=ss,
        )

        rationale = self._build_rationale(
            state=state,
            supplier=supplier,
            ss=ss,
            rop=rop,
            eoq=eoq,
            order_qty=order_qty,
            urgency=urgency,
        )

        return OrderRecommendation(
            product=product,
            supplier=supplier,
            order_quantity=order_qty,
            reorder_point=rop,
            safety_stock=ss,
            economic_order_quantity=eoq,
            rationale=rationale,
            urgency=urgency,
            metadata={
                "effective_stock": state.effective_stock,
                "days_of_supply": state.days_of_supply,
                "annual_demand_used": annual,
                "supplier_score": _supplier_score(supplier, product.unit_cost),
            },
        )

    def evaluate_portfolio(
        self,
        states: Sequence[InventoryState],
        suppliers: Sequence[Supplier],
    ) -> list[OrderRecommendation]:
        """Evaluate multiple products and return all active recommendations.

        Results are sorted by urgency (``"critical"`` first) so that agents
        and humans can act on the most pressing items immediately.

        Args:
            states: Collection of inventory snapshots (one per product).
            suppliers: Shared pool of candidate suppliers.

        Returns:
            A list of :class:`~ootils_core.models.OrderRecommendation` objects,
            sorted from most to least urgent.
        """
        _urgency_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        recommendations: list[OrderRecommendation] = []

        for state in states:
            rec = self.decide(state, suppliers)
            if rec is not None:
                recommendations.append(rec)

        recommendations.sort(key=lambda r: _urgency_rank.get(r.urgency, 99))
        return recommendations

    @staticmethod
    def _build_rationale(
        *,
        state: InventoryState,
        supplier: Supplier,
        ss: float,
        rop: float,
        eoq: float,
        order_qty: int,
        urgency: str,
    ) -> str:
        product = state.product
        lines = [
            f"Product '{product.name}' ({product.sku}) requires replenishment.",
            f"Current on-hand stock: {state.current_stock:.0f} units "
            f"({state.days_of_supply:.1f} days of supply).",
            f"Open orders in transit: {state.open_order_quantity:.0f} units.",
            f"Effective stock ({state.effective_stock:.0f}) ≤ reorder point ({rop:.1f}).",
            f"Safety stock: {ss:.1f} units (service level {product.service_level:.0%}).",
            f"Economic order quantity: {eoq:.1f} units; "
            f"order quantity after constraints: {order_qty} units.",
            f"Selected supplier: '{supplier.name}' "
            f"(lead time {supplier.lead_time_days:.0f} days, "
            f"reliability {supplier.reliability_score:.0%}).",
            f"Urgency: {urgency.upper()}.",
        ]
        return " ".join(lines)


def _supplier_score(supplier: Supplier, base_unit_cost: float) -> float:
    """Composite score used internally for supplier ranking (higher is better)."""
    cost_score = 1.0 / supplier.effective_unit_cost(base_unit_cost)
    lead_time_score = 1.0 / (supplier.lead_time_days + 1)
    return supplier.reliability_score * (0.5 * cost_score + 0.5 * lead_time_score)
