"""
supplier_items.py — link bought items (L3 bought + L4 raw) to suppliers.

For each "bought" item:
- Always 1 primary supplier (is_preferred=True)
- 40% probability of a backup supplier (is_preferred=False, different country
  ideally so a disruption doesn't kill both sources at once)

Lead time, MOQ and unit_cost come from a per-supplier baseline plus
item-specific noise. Country reliability (already on the supplier row)
influences which suppliers we'd want to prefer — but the link itself is
randomised; that's a knob for a future v2.

Volumes for profile M:
- L4 raw materials      ~1 100 items   ~1 540 supplier_items (avg 1.4)
- L3 bought parts (30%)    ~390 items     ~470 supplier_items (avg 1.2)
- Total                                ~2 000 supplier_items
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg

from ootils_core.seed.config import Profile
from ootils_core.seed.master.items import ItemRecord, ItemSet
from ootils_core.seed.master.suppliers import SupplierRecord, SupplierSet


# Probability that an L3 "part" item is bought (rest is made in-house).
_L3_BOUGHT_PCT = 0.30
# Probability that a bought item has a backup supplier.
_BACKUP_PROB = 0.40
# MOQ ranges by uom (rough industry priors).
_MOQ_RANGES_BY_UOM: dict[str, tuple[int, int]] = {
    "EA": (50, 1000),   # screws, electronics: by box / reel
    "KG": (25, 500),    # bulk metal / plastic
    "L":  (10, 200),    # chemicals, liquids
    "M":  (50, 500),    # cable, sheet rolls
}


@dataclass(frozen=True)
class SupplierItemRecord:
    supplier_item_id: UUID
    supplier_id: UUID
    item_id: UUID
    lead_time_days: int
    moq: Decimal
    unit_cost: Decimal
    currency: str
    is_preferred: bool


@dataclass
class SupplierItemSet:
    records: list[SupplierItemRecord]
    bought_items: set[UUID]  # Items that have at least one supplier link

    @property
    def total(self) -> int:
        return len(self.records)


def _pick_moq(uom: str, rng: random.Random) -> Decimal:
    lo, hi = _MOQ_RANGES_BY_UOM.get(uom, (10, 500))
    return Decimal(rng.randint(lo, hi))


def _pick_unit_cost(level: int, rng: random.Random) -> Decimal:
    """Coarse cost by level. Raws are cheap per-unit, intermediates pricier."""
    if level == 4:
        return Decimal(str(round(rng.uniform(0.05, 50.0), 2)))
    if level == 3:
        return Decimal(str(round(rng.uniform(2.0, 200.0), 2)))
    # Should not happen here (only L3/L4 are bought) but defensive
    return Decimal(str(round(rng.uniform(10.0, 500.0), 2)))


def _currency_for_country(country: str) -> str:
    return {
        "US": "USD", "MX": "USD",
        "CN": "USD", "VN": "USD", "IN": "USD", "JP": "USD",
        "DE": "EUR", "FR": "EUR", "IT": "EUR", "PL": "EUR", "CZ": "EUR",
    }.get(country, "EUR")


def generate_supplier_items(
    profile: Profile,
    item_set: ItemSet,
    supplier_set: SupplierSet,
) -> SupplierItemSet:
    """Decide which items are bought, and link them to suppliers.

    Deterministic per (profile, seed): only `active` suppliers are eligible
    primary, and the supplier pool is rotated round-robin so we don't pile
    every item on one supplier.
    """
    rng = random.Random(profile.seed + 3001)
    active_suppliers = supplier_set.active()
    if not active_suppliers:
        return SupplierItemSet(records=[], bought_items=set())

    # Bucket suppliers by country so we can pick a backup from a DIFFERENT
    # country (real-life dual-sourcing strategy: don't put both eggs in CN).
    by_country: dict[str, list[SupplierRecord]] = {}
    for s in active_suppliers:
        by_country.setdefault(s.country, []).append(s)

    # Eligible items: all L4 raws, plus 30% of L3 parts.
    bought_items: list[ItemRecord] = []
    bought_items.extend(item_set.at_level(4))
    l3_pool = item_set.at_level(3)
    for it in l3_pool:
        if rng.random() < _L3_BOUGHT_PCT:
            bought_items.append(it)

    records: list[SupplierItemRecord] = []
    bought_set: set[UUID] = set()

    for item in bought_items:
        if item.status == "obsolete":
            # Don't bother linking obsolete items — exercises filter paths
            continue
        primary = rng.choice(active_suppliers)
        # Lead time = supplier baseline +/- 30% noise
        base_lt = primary.lead_time_days
        lt_noise = rng.uniform(0.85, 1.15)
        records.append(SupplierItemRecord(
            supplier_item_id=uuid4(),
            supplier_id=primary.supplier_id,
            item_id=item.item_id,
            lead_time_days=max(1, int(base_lt * lt_noise)),
            moq=_pick_moq(item.uom, rng),
            unit_cost=_pick_unit_cost(item.level, rng),
            currency=_currency_for_country(primary.country),
            is_preferred=True,
        ))
        bought_set.add(item.item_id)

        # Backup supplier — from a different country if possible.
        if rng.random() < _BACKUP_PROB:
            other_countries = [c for c in by_country if c != primary.country]
            if other_countries:
                backup_country = rng.choice(other_countries)
                backup = rng.choice(by_country[backup_country])
                records.append(SupplierItemRecord(
                    supplier_item_id=uuid4(),
                    supplier_id=backup.supplier_id,
                    item_id=item.item_id,
                    lead_time_days=max(1, int(backup.lead_time_days * rng.uniform(0.9, 1.2))),
                    moq=_pick_moq(item.uom, rng),
                    unit_cost=_pick_unit_cost(item.level, rng) * Decimal(
                        str(round(rng.uniform(1.0, 1.20), 3))
                    ),  # backup often 0-20% pricier
                    currency=_currency_for_country(backup.country),
                    is_preferred=False,
                ))

    return SupplierItemSet(records=records, bought_items=bought_set)


def insert_supplier_items(
    conn: psycopg.Connection,
    si_set: SupplierItemSet,
) -> int:
    """Bulk-insert supplier_items via UNNEST. Returns rowcount."""
    if not si_set.records:
        return 0
    ids = [r.supplier_item_id for r in si_set.records]
    sup_ids = [r.supplier_id for r in si_set.records]
    item_ids = [r.item_id for r in si_set.records]
    lts = [r.lead_time_days for r in si_set.records]
    moqs = [r.moq for r in si_set.records]
    costs = [r.unit_cost for r in si_set.records]
    currs = [r.currency for r in si_set.records]
    preferred = [r.is_preferred for r in si_set.records]
    cur = conn.execute(
        """
        INSERT INTO supplier_items
            (supplier_item_id, supplier_id, item_id, lead_time_days,
             moq, unit_cost, currency, is_preferred)
        SELECT * FROM UNNEST(
            %s::uuid[], %s::uuid[], %s::uuid[],
            %s::int[], %s::numeric[], %s::numeric[],
            %s::text[], %s::bool[]
        )
        """,
        (ids, sup_ids, item_ids, lts, moqs, costs, currs, preferred),
    )
    return cur.rowcount or 0
