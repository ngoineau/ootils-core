"""
GELÉ (2026-07-19) — jamais branché en production ; candidat de réactivation :
MEIO (chantier 6b) ; ne pas compter en couverture.
Aucun chemin servi n'importe ce module (seuls des tests le référencent). Gelé
lors du chantier moteur-c7 ; le code est laissé intact volontairement. Voir
docs/CARTE-CODE.md.

Inventory replenishment policies.

All functions are pure (no side-effects) so they can be called freely by
agents and tests alike.
"""

from __future__ import annotations

import math

# Z-scores for common service levels (normal distribution).
# Maps service_level -> z_score.
_Z_SCORES: dict[float, float] = {
    0.50: 0.000,
    0.75: 0.674,
    0.80: 0.842,
    0.85: 1.036,
    0.90: 1.282,
    0.91: 1.341,
    0.92: 1.405,
    0.93: 1.476,
    0.94: 1.555,
    0.95: 1.645,
    0.96: 1.751,
    0.97: 1.881,
    0.98: 2.054,
    0.99: 2.326,
    0.999: 3.090,
}


def z_score(service_level: float) -> float:
    """Return the one-tailed normal z-score for *service_level*.

    Linear interpolation is used for values not in the lookup table.

    Args:
        service_level: Desired service level in the range (0, 1).

    Returns:
        Corresponding z-score.

    Raises:
        ValueError: If *service_level* is outside (0, 1).
    """
    if not 0 < service_level < 1:
        raise ValueError(f"service_level must be in (0, 1), got {service_level}")

    if service_level in _Z_SCORES:
        return _Z_SCORES[service_level]

    levels = sorted(_Z_SCORES.keys())
    for i in range(len(levels) - 1):
        lo, hi = levels[i], levels[i + 1]
        if lo <= service_level <= hi:
            t = (service_level - lo) / (hi - lo)
            return _Z_SCORES[lo] + t * (_Z_SCORES[hi] - _Z_SCORES[lo])

    return _Z_SCORES[levels[-1]]


def safety_stock(
    daily_demand: float,
    demand_std_daily: float,
    lead_time_days: float,
    lead_time_std_days: float,
    service_level: float,
) -> float:
    """Calculate the safety stock required to achieve *service_level*.

    Uses the combined variance formula::

        SS = z * sqrt(L * σ_d² + d² * σ_L²)

    where *L* is lead time, *d* is average daily demand, *σ_d* is demand
    standard deviation, and *σ_L* is lead time standard deviation.

    Args:
        daily_demand: Average daily demand (units/day).
        demand_std_daily: Standard deviation of daily demand (units/day).
        lead_time_days: Average lead time in days.
        lead_time_std_days: Standard deviation of lead time in days.
        service_level: Desired probability of no stock-out during lead time.

    Returns:
        Safety stock in units (always >= 0).
    """
    z = z_score(service_level)
    variance = (lead_time_days * demand_std_daily**2) + (daily_demand**2 * lead_time_std_days**2)
    return z * math.sqrt(variance)


def reorder_point(
    daily_demand: float,
    lead_time_days: float,
    safety_stock: float,
) -> float:
    """Calculate the reorder point (ROP).

    ROP is the inventory level at which a new order should be placed so that
    it arrives before stock-out (accounting for safety stock).

    Args:
        daily_demand: Average daily demand (units/day).
        lead_time_days: Average supplier lead time in days.
        safety_stock: Safety stock buffer in units.

    Returns:
        Reorder point in units.
    """
    return daily_demand * lead_time_days + safety_stock


def economic_order_quantity(
    annual_demand: float,
    ordering_cost: float,
    unit_cost: float,
    holding_cost_rate: float,
) -> float:
    """Calculate the Economic Order Quantity (EOQ) using the Wilson formula.

    EOQ minimises the total annual ordering + holding cost::

        EOQ = sqrt(2 * D * S / (H))

    where *D* is annual demand, *S* is ordering cost, and *H* is the annual
    holding cost per unit.

    Args:
        annual_demand: Annual demand in units.
        ordering_cost: Fixed cost per order placed.
        unit_cost: Purchase cost per unit.
        holding_cost_rate: Annual holding cost as a fraction of unit cost.

    Returns:
        Economic order quantity in units (>= 1).
    """
    if annual_demand <= 0:
        return 1.0
    holding_cost_per_unit = unit_cost * holding_cost_rate
    if holding_cost_per_unit <= 0:
        return 1.0
    return math.sqrt((2 * annual_demand * ordering_cost) / holding_cost_per_unit)


def urgency_level(
    current_stock: float,
    daily_demand: float,
    reorder_point: float,
    safety_stock: float,
) -> str:
    """Classify the urgency of a replenishment decision.

    Args:
        current_stock: Current on-hand inventory (units).
        daily_demand: Average daily demand (units/day).
        reorder_point: Calculated reorder point (units).
        safety_stock: Safety stock buffer (units).

    Returns:
        One of ``"critical"``, ``"high"``, ``"medium"``, or ``"low"``.
    """
    if daily_demand <= 0:
        return "low"

    days_of_supply = current_stock / daily_demand

    if current_stock <= 0 or days_of_supply < 1:
        return "critical"
    if current_stock <= safety_stock or days_of_supply < 3:
        return "high"
    if current_stock <= reorder_point:
        return "medium"
    return "low"
