"""
items.py — pyramid item generator.

Decomposes the 5K SKU target into a 5-level BOM pyramid:
  L0 FG          -> 'finished_good'
  L1 SubAssembly -> 'semi_finished'
  L2 Component   -> 'component'
  L3 Part        -> 'component'   (same DB type, different graph position)
  L4 Raw         -> 'raw_material'

Each item carries its `level` in memory so downstream generators (BOMs,
sourcing) can route by level. The DB only stores item_type.

Naming convention:
  FG-00001  - finished good
  SA-00001  - sub-assembly
  CP-00001  - component (L2)
  PT-00001  - part (L3)
  RM-00001  - raw material

Status mix: ~85% active, ~10% phase_out, ~5% obsolete. The non-active
items exercise temporal/lifecycle code paths.
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection
from ootils_core.seed.config import Profile, UomMix


# DB item_type per pyramid level. L2/L3 share 'component'.
_LEVEL_TO_TYPE: dict[int, str] = {
    0: "finished_good",
    1: "semi_finished",
    2: "component",
    3: "component",
    4: "raw_material",
}

# Name prefix per level (stable so tests can refer to families by name).
_LEVEL_PREFIX: dict[int, str] = {
    0: "FG",
    1: "SA",
    2: "CP",
    3: "PT",
    4: "RM",
}


@dataclass(frozen=True)
class ItemRecord:
    """One generated item — kept in memory so downstream generators can route by level."""
    item_id: UUID
    name: str
    item_type: str
    uom: str
    status: str
    level: int  # 0-4, BOM pyramid level (not persisted; implicit in the graph)


@dataclass
class ItemSet:
    """Bundle of all generated items grouped by level for fast lookup."""
    by_level: dict[int, list[ItemRecord]]

    @property
    def all(self) -> list[ItemRecord]:
        return [it for lvl in sorted(self.by_level) for it in self.by_level[lvl]]

    @property
    def total(self) -> int:
        return sum(len(v) for v in self.by_level.values())

    def at_level(self, level: int) -> list[ItemRecord]:
        return self.by_level.get(level, [])


def _pick_uom(level: int, mix: UomMix, rng: random.Random) -> str:
    """Pick a UoM from the per-level distribution defined in UomMix."""
    pool = {
        0: mix.fg_uom,
        1: mix.sub_assembly_uom,
        2: mix.component_uom,
        3: mix.part_uom,
        4: mix.raw_uom,
    }[level]
    return rng.choice(pool)


def _pick_status(profile: Profile, rng: random.Random) -> str:
    """Sample a status weighted by the profile's StatusDistribution."""
    r = rng.random()
    if r < profile.status_dist.active_pct:
        return "active"
    if r < profile.status_dist.active_pct + profile.status_dist.phase_out_pct:
        return "phase_out"
    return "obsolete"


def generate_items(profile: Profile) -> ItemSet:
    """Build the in-memory item pyramid from a profile. No DB access."""
    rng = random.Random(profile.seed)
    pyramid = profile.pyramid
    level_counts = {
        0: pyramid.fg,
        1: pyramid.sub_assembly,
        2: pyramid.component,
        3: pyramid.part,
        4: pyramid.raw_material,
    }

    by_level: dict[int, list[ItemRecord]] = {}
    for level, count in level_counts.items():
        prefix = _LEVEL_PREFIX[level]
        item_type = _LEVEL_TO_TYPE[level]
        bucket: list[ItemRecord] = []
        for i in range(count):
            bucket.append(ItemRecord(
                item_id=uuid4(),
                name=f"{prefix}-{i + 1:05d}",
                item_type=item_type,
                uom=_pick_uom(level, profile.uom_mix, rng),
                status=_pick_status(profile, rng),
                level=level,
            ))
        by_level[level] = bucket
    return ItemSet(by_level=by_level)


def insert_items(conn: DictRowConnection, item_set: ItemSet) -> int:
    """Bulk-insert all items via UNNEST. Returns rowcount."""
    ids = [it.item_id for it in item_set.all]
    names = [it.name for it in item_set.all]
    types = [it.item_type for it in item_set.all]
    uoms = [it.uom for it in item_set.all]
    statuses = [it.status for it in item_set.all]
    cur = conn.execute(
        """
        INSERT INTO items (item_id, name, item_type, uom, status)
        SELECT * FROM UNNEST(%s::uuid[], %s::text[], %s::text[], %s::text[], %s::text[])
        """,
        (ids, names, types, uoms, statuses),
    )
    return cur.rowcount or 0
