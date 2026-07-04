"""
order_history.py — 12 months of closed CustomerOrderDemand nodes.

Generates ~120 000 historic orders over the past 12 months. Each order
is stored as a CustomerOrderDemand node with `active=FALSE` and a
`time_ref` in the past — keeping them in the same table as forward
orders for query simplicity, but invisible to the forward propagation
engine (which filters on active=TRUE).

Patterns layered into the historical signal:
- ABC by volume: top 20% of FGs hold ~70% of orders (same as open orders)
- Seasonality: +25% in Nov/Dec, -15% in Aug (consumer-style pattern)
- Trend: slight upward drift over the 12 months (matches the forward trend
  in forecasts.py, so the forecast/actual comparison is consistent)
- Day-of-week: weekends get ~35% of weekday volume
- Some obsolete FGs DO appear in the historic record (they were sold
  before being retired) — exercises queries that join history to items
  with status filters
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection
from ootils_core.seed.config import Profile
from ootils_core.seed.master.items import ItemSet
from ootils_core.seed.master.locations import LocationSet
from ootils_core.seed.transactional.nodes import BASELINE_SCENARIO_ID


# Total historical orders over the 12-month window
_TOTAL_HISTORIC_ORDERS = 120_000
# Horizon back (days)
_HORIZON_DAYS_BACK = 365
# ABC split (same as customer_orders.py for consistency)
_ABC_ITEM_PCT = (0.20, 0.30)         # A=top 20%, B=next 30%, C=rest
_ABC_VOLUME_PCT = (0.70, 0.20)       # A holds 70% of orders, B 20%, C 10%
# Seasonality amplitude and peak month
_SEASON_AMPLITUDE = 0.25
_SEASON_PEAK_MONTH = 11
# Annual growth rate (matches forecasts.py upper bound)
_ANNUAL_TREND = 0.07
# Weekend volume ratio (weekend orders / weekday orders)
_WEEKEND_RATIO = 0.35


@dataclass(frozen=True)
class HistoricOrderNode:
    node_id: UUID
    item_id: UUID
    location_id: UUID
    quantity: Decimal
    time_ref: date


def _seasonality_factor(month: int) -> float:
    phase = (month - _SEASON_PEAK_MONTH) / 12.0 * 2 * math.pi
    return 1.0 + _SEASON_AMPLITUDE * math.cos(phase)


def _trend_factor(days_back: int) -> float:
    """Apply the annual trend: orders today should be ~+7% vs 12 months ago."""
    years_back = days_back / 365.0
    return (1.0 + _ANNUAL_TREND) ** (-years_back)


def _day_weight(d: date) -> float:
    return _WEEKEND_RATIO if d.weekday() >= 5 else 1.0


def generate_order_history(
    profile: Profile,
    item_set: ItemSet,
    loc_set: LocationSet,
) -> list[HistoricOrderNode]:
    """Build ~120K historic orders distributed over the past 12 months.

    Includes obsolete FGs (~5% of the historical pool) since they were
    actively sold before being retired — keeps the historical record honest.
    """
    rng = random.Random(profile.seed + 8001)
    # ALL FGs except those that have always been phase_out — but the seed makes
    # status independent of historic activity, so we accept obsolete FGs in the
    # history. Filter only inactive items if needed; for now, include all FGs.
    fgs = item_set.at_level(0)
    if not fgs:
        return []
    dcs = loc_set.dcs()
    if not dcs:
        return []

    # ABC pools
    shuffled = list(fgs)
    rng.shuffle(shuffled)
    n = len(shuffled)
    a_end = int(n * _ABC_ITEM_PCT[0])
    b_end = a_end + int(n * _ABC_ITEM_PCT[1])
    pool_a = shuffled[:a_end]
    pool_b = shuffled[a_end:b_end]
    pool_c = shuffled[b_end:]

    # Day weights — build a probability distribution over the 365 days back
    # that incorporates seasonality, trend, and weekend dampening.
    today = date.today()
    day_dates: list[date] = [today - timedelta(days=i) for i in range(1, _HORIZON_DAYS_BACK + 1)]
    weights: list[float] = []
    for i, d in enumerate(day_dates, start=1):
        w = _seasonality_factor(d.month) * _trend_factor(i) * _day_weight(d)
        weights.append(w)

    # Pre-compute order counts per ABC class
    n_a = int(_TOTAL_HISTORIC_ORDERS * _ABC_VOLUME_PCT[0])
    n_b = int(_TOTAL_HISTORIC_ORDERS * _ABC_VOLUME_PCT[1])
    n_c = _TOTAL_HISTORIC_ORDERS - n_a - n_b

    nodes: list[HistoricOrderNode] = []

    def _emit(pool: list, count: int) -> None:
        if not pool or count <= 0:
            return
        # Sample `count` order dates from day_dates weighted by `weights`. We
        # use rng.choices for speed (with replacement is fine — many orders/day).
        sampled_dates = rng.choices(day_dates, weights=weights, k=count)
        for ord_date in sampled_dates:
            item = rng.choice(pool)
            dc = rng.choice(dcs)
            nodes.append(HistoricOrderNode(
                node_id=uuid4(),
                item_id=item.item_id,
                location_id=dc.location_id,
                quantity=Decimal(rng.randint(1, 50)),
                time_ref=ord_date,
            ))

    _emit(pool_a, n_a)
    _emit(pool_b, n_b)
    _emit(pool_c, n_c)
    return nodes


def insert_order_history(
    conn: DictRowConnection,
    nodes: list[HistoricOrderNode],
    batch_size: int = 50_000,
) -> int:
    """Bulk-insert historic orders as CustomerOrderDemand with active=FALSE.

    Chunks the insert into batches of up to `batch_size` rows to keep the
    UNNEST array payloads sane (120K UUID arrays in a single statement work
    but pg parameter encoding gets slow; 50K is a safe sweet spot).
    """
    if not nodes:
        return 0

    total = 0
    for start in range(0, len(nodes), batch_size):
        chunk = nodes[start:start + batch_size]
        ids = [n.node_id for n in chunk]
        item_ids = [n.item_id for n in chunk]
        loc_ids = [n.location_id for n in chunk]
        qtys = [n.quantity for n in chunk]
        refs = [n.time_ref for n in chunk]
        cur = conn.execute(
            """
            INSERT INTO nodes
                (node_id, node_type, scenario_id, item_id, location_id,
                 quantity, qty_uom, time_grain, time_ref, is_dirty, active)
            SELECT
                h.id, 'CustomerOrderDemand', %s, h.item_id, h.loc_id,
                h.qty, 'EA', 'exact_date', h.ref, FALSE, FALSE
            FROM UNNEST(
                %s::uuid[], %s::uuid[], %s::uuid[], %s::numeric[], %s::date[]
            ) AS h(id, item_id, loc_id, qty, ref)
            """,
            (BASELINE_SCENARIO_ID, ids, item_ids, loc_ids, qtys, refs),
        )
        total += cur.rowcount or 0
    return total
