"""
planning_params.py — generate item_planning_params (the network design).

For each (item, location) pair we choose to "instantiate", we emit one
SCD2-versioned row carrying:
- lead times split into sourcing / manufacturing / transit
- safety stock policy (either a qty or 0)
- min/max order qty + multiple + lot_size_rule
- is_make flag (True at plants for items that are produced there)
- preferred_supplier_id (set for purchased items at plants that buy)

Item-location footprint (which items "exist" where):
- L0 FG: 1-3 producer plants + ALL 3 DCs (distribution everywhere)
- L1 SA, L2 cmp made: 1-2 producer plants
- L3 part:
    - if bought (30% from supplier_items): at 2-3 random plants (used as
      component by upstream BOMs there)
    - if made (70%):                         at 1-2 producer plants
- L4 raw: at 2-4 plants (consumed in BOMs there)

Profile M expected volumes:
- FG entries:    500 × ~5 ≈ 2 500
- SA entries:    900 × ~1.5 ≈ 1 350
- L2 entries:   1200 × ~1.5 ≈ 1 800
- L3 entries:   1300 × ~2  ≈ 2 600
- L4 entries:   1100 × ~3  ≈ 3 300
- Total:                 ~11 500 rows
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg

from ootils_core.seed.config import Profile
from ootils_core.seed.master.items import ItemRecord, ItemSet
from ootils_core.seed.master.locations import LocationRecord, LocationSet
from ootils_core.seed.network.supplier_items import SupplierItemSet


# Lot-size rule mix by level. Made items typically LOTFORLOT; bought raws
# often FIXED_QTY (MOQ multiples).
_LOT_RULE_PROBS_MADE = {"LOTFORLOT": 0.80, "FIXED_QTY": 0.15, "MIN_MAX": 0.05}
_LOT_RULE_PROBS_BOUGHT = {"FIXED_QTY": 0.55, "MIN_MAX": 0.30, "LOTFORLOT": 0.15}


def _weighted_pick(probs: dict, rng: random.Random) -> str:
    r = rng.random()
    cum = 0.0
    for k, p in probs.items():
        cum += p
        if r < cum:
            return k
    return next(iter(probs))


@dataclass(frozen=True)
class PlanningParamRecord:
    param_id: UUID
    item_id: UUID
    location_id: UUID
    lead_time_sourcing_days: int | None
    lead_time_manufacturing_days: int | None
    lead_time_transit_days: int | None
    safety_stock_qty: Decimal | None
    safety_stock_days: Decimal | None
    reorder_point_qty: Decimal | None
    min_order_qty: Decimal | None
    max_order_qty: Decimal | None
    order_multiple: Decimal | None
    lot_size_rule: str
    planning_horizon_days: int
    is_make: bool
    preferred_supplier_id: UUID | None
    effective_from: date


@dataclass
class PlanningParamSet:
    records: list[PlanningParamRecord]

    @property
    def total(self) -> int:
        return len(self.records)


def _pick_plants(plants: list[LocationRecord], k: int, rng: random.Random) -> list[LocationRecord]:
    k = min(k, len(plants))
    return rng.sample(plants, k)


def _supplier_for_item(
    item_id: UUID,
    si_set: SupplierItemSet,
    rng: random.Random,
) -> UUID | None:
    """Return the preferred supplier_id for this item, if any."""
    preferred = [r for r in si_set.records if r.item_id == item_id and r.is_preferred]
    if not preferred:
        return None
    return preferred[0].supplier_id


def _safety_stock_for(level: int, status: str, rng: random.Random) -> tuple[Decimal, Decimal] | None:
    """Return (safety_stock_qty, safety_stock_days) or None if no safety stock.

    Higher level / FG => more frequent safety stock. Obsolete items: never.
    """
    if status == "obsolete":
        return None
    has_ss_prob = {0: 0.85, 1: 0.65, 2: 0.55, 3: 0.45, 4: 0.55}.get(level, 0.5)
    if rng.random() > has_ss_prob:
        return None
    # Quantity scale also depends on level — FGs see fewer units, raws more.
    qty_range = {0: (10, 50), 1: (20, 100), 2: (50, 200), 3: (100, 500), 4: (200, 2000)}[level]
    qty = Decimal(rng.randint(*qty_range))
    days = Decimal(rng.randint(3, 14))
    return qty, days


def generate_planning_params(
    profile: Profile,
    item_set: ItemSet,
    loc_set: LocationSet,
    si_set: SupplierItemSet,
) -> PlanningParamSet:
    """Build the (item, location) network with policies attached."""
    rng = random.Random(profile.seed + 4001)
    plants = loc_set.plants()
    dcs = loc_set.dcs()
    bought_ids = si_set.bought_items
    effective_from = date.today()
    records: list[PlanningParamRecord] = []

    # Helper to compose one record.
    def _make(item: ItemRecord, loc: LocationRecord, is_make: bool, supplier_id: UUID | None) -> PlanningParamRecord:
        ss = _safety_stock_for(item.level, item.status, rng)
        ss_qty, ss_days = (ss[0], ss[1]) if ss else (None, None)

        # Lead times: split based on role
        lt_sourcing = None
        lt_mfg = None
        lt_transit = None
        if is_make:
            lt_mfg = rng.randint(1, 5)
            # Some made items also have minor sourcing lead (e.g. raw alloc)
            if rng.random() < 0.3:
                lt_sourcing = rng.randint(1, 3)
        elif supplier_id is not None:
            # Use a midrange representative; per-item lt is on supplier_items
            lt_sourcing = rng.randint(5, 70)
        else:
            # Transferred — typically from a plant. Transit only.
            lt_transit = rng.randint(2, 7)

        # Lot rules and MOQ
        rule = _weighted_pick(_LOT_RULE_PROBS_MADE if is_make else _LOT_RULE_PROBS_BOUGHT, rng)
        moq = rng.randint(10, 100) if not is_make else rng.randint(1, 20)
        order_multiple = rng.choice([1, 1, 1, 5, 10])  # mostly 1, sometimes packed lots
        max_order_qty = moq * rng.randint(20, 100)
        # Reorder point = safety_stock + a small buffer; if no safety stock, omit
        reorder_pt = (ss_qty * Decimal(str(round(rng.uniform(1.2, 2.0), 2)))).quantize(Decimal("1")) if ss_qty else None

        return PlanningParamRecord(
            param_id=uuid4(),
            item_id=item.item_id,
            location_id=loc.location_id,
            lead_time_sourcing_days=lt_sourcing,
            lead_time_manufacturing_days=lt_mfg,
            lead_time_transit_days=lt_transit,
            safety_stock_qty=ss_qty,
            safety_stock_days=ss_days,
            reorder_point_qty=reorder_pt,
            min_order_qty=Decimal(moq),
            max_order_qty=Decimal(max_order_qty),
            order_multiple=Decimal(order_multiple),
            lot_size_rule=rule,
            planning_horizon_days=90,
            is_make=is_make,
            preferred_supplier_id=supplier_id,
            effective_from=effective_from,
        )

    # L0 FG: 1-3 plants make + 3 DCs distribute
    for fg in item_set.at_level(0):
        producer_plants = _pick_plants(plants, rng.randint(1, 3), rng)
        for p in producer_plants:
            records.append(_make(fg, p, is_make=True, supplier_id=None))
        for d in dcs:
            records.append(_make(fg, d, is_make=False, supplier_id=None))

    # L1 SA, L2 cmp: made at 1-2 plants
    for lvl in (1, 2):
        for it in item_set.at_level(lvl):
            for p in _pick_plants(plants, rng.randint(1, 2), rng):
                records.append(_make(it, p, is_make=True, supplier_id=None))

    # L3 parts: bought or made
    for pt in item_set.at_level(3):
        if pt.item_id in bought_ids:
            sup = _supplier_for_item(pt.item_id, si_set, rng)
            for p in _pick_plants(plants, rng.randint(2, 3), rng):
                records.append(_make(pt, p, is_make=False, supplier_id=sup))
        else:
            for p in _pick_plants(plants, rng.randint(1, 2), rng):
                records.append(_make(pt, p, is_make=True, supplier_id=None))

    # L4 raw: all bought, distributed to 2-4 plants
    for rm in item_set.at_level(4):
        sup = _supplier_for_item(rm.item_id, si_set, rng)
        for p in _pick_plants(plants, rng.randint(2, 4), rng):
            records.append(_make(rm, p, is_make=False, supplier_id=sup))

    return PlanningParamSet(records=records)


def insert_planning_params(
    conn: psycopg.Connection,
    pp_set: PlanningParamSet,
) -> int:
    """Bulk-insert item_planning_params via UNNEST."""
    if not pp_set.records:
        return 0
    cur = conn.execute(
        """
        INSERT INTO item_planning_params (
            param_id, item_id, location_id,
            lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days,
            safety_stock_qty, safety_stock_days,
            reorder_point_qty, min_order_qty, max_order_qty, order_multiple,
            lot_size_rule, planning_horizon_days,
            is_make, preferred_supplier_id, effective_from
        )
        SELECT * FROM UNNEST(
            %s::uuid[], %s::uuid[], %s::uuid[],
            %s::int[],  %s::int[],  %s::int[],
            %s::numeric[], %s::numeric[],
            %s::numeric[], %s::numeric[], %s::numeric[], %s::numeric[],
            %s::lot_size_rule_type[], %s::int[],
            %s::bool[], %s::uuid[], %s::date[]
        )
        """,
        (
            [r.param_id for r in pp_set.records],
            [r.item_id for r in pp_set.records],
            [r.location_id for r in pp_set.records],
            [r.lead_time_sourcing_days for r in pp_set.records],
            [r.lead_time_manufacturing_days for r in pp_set.records],
            [r.lead_time_transit_days for r in pp_set.records],
            [r.safety_stock_qty for r in pp_set.records],
            [r.safety_stock_days for r in pp_set.records],
            [r.reorder_point_qty for r in pp_set.records],
            [r.min_order_qty for r in pp_set.records],
            [r.max_order_qty for r in pp_set.records],
            [r.order_multiple for r in pp_set.records],
            [r.lot_size_rule for r in pp_set.records],
            [r.planning_horizon_days for r in pp_set.records],
            [r.is_make for r in pp_set.records],
            [r.preferred_supplier_id for r in pp_set.records],
            [r.effective_from for r in pp_set.records],
        ),
    )
    return cur.rowcount or 0
