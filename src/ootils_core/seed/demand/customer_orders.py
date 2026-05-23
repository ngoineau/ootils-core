"""
customer_orders.py — open CustomerOrderDemand nodes for FGs at DCs.

Orders are point-in-time (specific due dates over the next 60 days),
distributed by ABC class so high-volume FGs see many small orders and
low-volume FGs see a few. Total ~6 000 open orders for profile M.

Each order:
  - item_id: a finished good
  - location_id: a DC
  - quantity: small (1-50), typical B2B order line
  - time_ref: due date in [today, today+60d)

The ABC split is decided here (top 20% of FGs are "A", next 30% are "B",
the rest "C"). A-class FGs get ~70% of order volume, B-class ~20%, C-class ~10%.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg

from ootils_core.seed.config import Profile
from ootils_core.seed.master.items import ItemSet
from ootils_core.seed.master.locations import LocationSet
from ootils_core.seed.transactional.nodes import BASELINE_SCENARIO_ID


# Total open orders across all FGs for profile M
_TOTAL_OPEN_ORDERS = 6000
# Forward horizon for open orders
_HORIZON_DAYS = 60
# ABC split: 20/30/50 by item count, 70/20/10 by volume
_ABC_ITEM_PCT = (0.20, 0.30)  # cumulative — A is top 20%, B is next 30%, C is rest
_ABC_VOLUME_PCT = (0.70, 0.20)  # A gets 70%, B 20%, C 10%


@dataclass(frozen=True)
class OrderNode:
    node_id: UUID
    item_id: UUID
    location_id: UUID
    quantity: Decimal
    time_ref: date


def generate_customer_orders(
    profile: Profile,
    item_set: ItemSet,
    loc_set: LocationSet,
) -> list[OrderNode]:
    rng = random.Random(profile.seed + 7001)
    fgs = [it for it in item_set.at_level(0) if it.status != "obsolete"]
    if not fgs:
        return []
    dcs = loc_set.dcs()
    if not dcs:
        return []

    # ABC assignment: random shuffle, then slice. Deterministic since rng is seeded.
    shuffled = list(fgs)
    rng.shuffle(shuffled)
    n = len(shuffled)
    a_end = int(n * _ABC_ITEM_PCT[0])
    b_end = a_end + int(n * _ABC_ITEM_PCT[1])
    pool_a = shuffled[:a_end]
    pool_b = shuffled[a_end:b_end]
    pool_c = shuffled[b_end:]

    # Allocate order counts per class
    n_a = int(_TOTAL_OPEN_ORDERS * _ABC_VOLUME_PCT[0])
    n_b = int(_TOTAL_OPEN_ORDERS * _ABC_VOLUME_PCT[1])
    n_c = _TOTAL_OPEN_ORDERS - n_a - n_b

    today = date.today()
    nodes: list[OrderNode] = []

    def _emit_for_pool(pool: list, count: int) -> None:
        if not pool:
            return
        for _ in range(count):
            item = rng.choice(pool)
            dc = rng.choice(dcs)
            offset = rng.randint(0, _HORIZON_DAYS - 1)
            nodes.append(OrderNode(
                node_id=uuid4(),
                item_id=item.item_id,
                location_id=dc.location_id,
                quantity=Decimal(rng.randint(1, 50)),
                time_ref=today + timedelta(days=offset),
            ))

    _emit_for_pool(pool_a, n_a)
    _emit_for_pool(pool_b, n_b)
    _emit_for_pool(pool_c, n_c)
    return nodes


def insert_customer_orders(conn: psycopg.Connection, nodes: list[OrderNode]) -> int:
    if not nodes:
        return 0
    ids = [n.node_id for n in nodes]
    item_ids = [n.item_id for n in nodes]
    loc_ids = [n.location_id for n in nodes]
    qtys = [n.quantity for n in nodes]
    refs = [n.time_ref for n in nodes]
    cur = conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             quantity, qty_uom, time_grain, time_ref, is_dirty, active)
        SELECT
            o.id, 'CustomerOrderDemand', %s, o.item_id, o.loc_id,
            o.qty, 'EA', 'exact_date', o.ref, FALSE, TRUE
        FROM UNNEST(
            %s::uuid[], %s::uuid[], %s::uuid[], %s::numeric[], %s::date[]
        ) AS o(id, item_id, loc_id, qty, ref)
        """,
        (BASELINE_SCENARIO_ID, ids, item_ids, loc_ids, qtys, refs),
    )
    return cur.rowcount or 0
