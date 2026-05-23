"""
boms.py — 5-level BOM generator.

For every non-raw-material item (L0-L3), emits one BOM header + 3-7
component lines drawn from items below it in the pyramid. Children are
80% from the level immediately below, 20% from two levels below — this
mimics real discrete-manuf shapes where some FGs use a screw or piece of
raw stock directly without going through a sub-assembly.

The pyramid construction guarantees acyclicity (children always from a
strictly lower level), but we still run a graphlib check during
validation to catch any future structural mistakes.

Volumes for profile M (5K items):
  L0 parents:  500 * ~5  =  2500 lines
  L1 parents:  900 * ~5  =  4500 lines
  L2 parents: 1200 * ~4  =  4800 lines
  L3 parents: 1300 * ~3  =  3900 lines
  L4 raw mat: 0 BOMs
  Total: ~15 700 BOM lines
"""
from __future__ import annotations

import graphlib
import random
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg

from ootils_core.seed.config import Profile
from ootils_core.seed.master.items import ItemRecord, ItemSet


# Average children per parent BOM, per parent level. Discrete manuf shapes:
# L0 FG and L1 SA typically have wider BOMs (5-6); deeper levels narrower.
_CHILDREN_RANGE: dict[int, tuple[int, int]] = {
    0: (4, 7),
    1: (4, 7),
    2: (3, 6),
    3: (2, 4),
}

# % probability that a given child comes from level+2 instead of level+1.
# 20% in real life — e.g. FG using a raw material directly (label, packaging).
_LEVEL_SKIP_PCT = 0.20


@dataclass(frozen=True)
class BomHeader:
    bom_id: UUID
    parent_item_id: UUID
    bom_version: str
    status: str


@dataclass(frozen=True)
class BomLine:
    line_id: UUID
    bom_id: UUID
    component_item_id: UUID
    quantity_per: Decimal
    uom: str
    scrap_factor: Decimal


@dataclass
class BomSet:
    headers: list[BomHeader]
    lines: list[BomLine]

    @property
    def total_lines(self) -> int:
        return len(self.lines)

    @property
    def total_headers(self) -> int:
        return len(self.headers)


def _pick_children_count(parent_level: int, rng: random.Random) -> int:
    lo, hi = _CHILDREN_RANGE[parent_level]
    return rng.randint(lo, hi)


def _pick_quantity_per(child: ItemRecord, rng: random.Random) -> Decimal:
    """Quantity-per is small for assembled pieces, can be larger for raw materials."""
    if child.level == 4:
        # Raw materials in KG/L/M: continuous quantities like 0.5, 1.2, 3.5
        if child.uom in ("KG", "L", "M"):
            return Decimal(str(round(rng.uniform(0.1, 5.0), 2)))
        # Raw materials in EA (e.g. screws, electronic parts): integers 1-20
        return Decimal(rng.randint(1, 20))
    # Assembled parts: 1-4 of each
    return Decimal(rng.randint(1, 4))


def _pick_scrap_factor(child_level: int, rng: random.Random) -> Decimal:
    """Scrap is typically 0 for assembled parts, 1-5% for raws (cutting losses, etc)."""
    if child_level == 4:
        return Decimal(str(round(rng.uniform(0.01, 0.05), 3)))
    return Decimal("0.0")


def generate_boms(profile: Profile, item_set: ItemSet) -> BomSet:
    """Build BOM headers + lines from the item pyramid. No DB access.

    Uses the profile RNG seed offset by a constant so this generator's
    randomness is independent of items/locations/suppliers.
    """
    rng = random.Random(profile.seed + 2001)
    headers: list[BomHeader] = []
    lines: list[BomLine] = []

    for parent_level in (0, 1, 2, 3):
        parents = item_set.at_level(parent_level)
        if not parents:
            continue

        # Determine the candidate child pools (level+1, level+2).
        pool_lvl1 = item_set.at_level(parent_level + 1)
        pool_lvl2 = item_set.at_level(parent_level + 2) if parent_level + 2 <= 4 else []

        if not pool_lvl1 and not pool_lvl2:
            # Should not happen with the default profile, but defensively skip.
            continue

        for parent in parents:
            # Active items always get a BOM. Phase-out / obsolete get one too
            # (their BOM may simply be unused) — matches real ERP behaviour.
            header = BomHeader(
                bom_id=uuid4(),
                parent_item_id=parent.item_id,
                bom_version="1.0",
                # 10% of phase_out items get inactive BOMs to exercise filtering.
                status="inactive" if (parent.status == "phase_out" and rng.random() < 0.1)
                       else "active",
            )
            headers.append(header)

            n_children = _pick_children_count(parent_level, rng)
            # Ensure no duplicate components within a single BOM
            picked_ids: set[UUID] = set()
            attempts = 0
            while len(picked_ids) < n_children and attempts < n_children * 3:
                attempts += 1
                # 80% from level+1, 20% from level+2 (if available)
                use_skip = pool_lvl2 and rng.random() < _LEVEL_SKIP_PCT
                pool = pool_lvl2 if use_skip else pool_lvl1
                if not pool:
                    pool = pool_lvl1 or pool_lvl2
                child = rng.choice(pool)
                if child.item_id in picked_ids:
                    continue
                picked_ids.add(child.item_id)
                lines.append(BomLine(
                    line_id=uuid4(),
                    bom_id=header.bom_id,
                    component_item_id=child.item_id,
                    quantity_per=_pick_quantity_per(child, rng),
                    uom=child.uom,
                    scrap_factor=_pick_scrap_factor(child.level, rng),
                ))

    return BomSet(headers=headers, lines=lines)


def insert_boms(conn: psycopg.Connection, bom_set: BomSet) -> tuple[int, int]:
    """Bulk-insert headers then lines via UNNEST. Returns (n_headers, n_lines)."""
    # Headers
    h_ids = [h.bom_id for h in bom_set.headers]
    h_parents = [h.parent_item_id for h in bom_set.headers]
    h_versions = [h.bom_version for h in bom_set.headers]
    h_statuses = [h.status for h in bom_set.headers]
    cur1 = conn.execute(
        """
        INSERT INTO bom_headers (bom_id, parent_item_id, bom_version, status)
        SELECT * FROM UNNEST(%s::uuid[], %s::uuid[], %s::text[], %s::text[])
        """,
        (h_ids, h_parents, h_versions, h_statuses),
    )
    n_h = cur1.rowcount or 0

    # Lines
    l_ids = [ln.line_id for ln in bom_set.lines]
    l_bom_ids = [ln.bom_id for ln in bom_set.lines]
    l_comp_ids = [ln.component_item_id for ln in bom_set.lines]
    l_qtys = [ln.quantity_per for ln in bom_set.lines]
    l_uoms = [ln.uom for ln in bom_set.lines]
    l_scraps = [ln.scrap_factor for ln in bom_set.lines]
    cur2 = conn.execute(
        """
        INSERT INTO bom_lines
            (line_id, bom_id, component_item_id, quantity_per, uom, scrap_factor)
        SELECT * FROM UNNEST(
            %s::uuid[], %s::uuid[], %s::uuid[],
            %s::numeric[], %s::text[], %s::numeric[]
        )
        """,
        (l_ids, l_bom_ids, l_comp_ids, l_qtys, l_uoms, l_scraps),
    )
    n_l = cur2.rowcount or 0
    return n_h, n_l


def validate_acyclic(bom_set: BomSet, item_set: ItemSet) -> None:
    """Assert the BOM graph has no cycles.

    Raises graphlib.CycleError if a cycle is detected — never expected with
    the pyramid construction, but cheap insurance against future structural
    drift (e.g. when we add alternates or substitutions).
    """
    # parent -> set(children) via bom_headers + bom_lines
    parent_by_bom: dict[UUID, UUID] = {h.bom_id: h.parent_item_id for h in bom_set.headers}
    children: dict[UUID, set[UUID]] = {}
    for line in bom_set.lines:
        parent = parent_by_bom[line.bom_id]
        children.setdefault(parent, set()).add(line.component_item_id)

    sorter = graphlib.TopologicalSorter(children)
    sorter.prepare()
    # Drain — raises CycleError if a cycle exists
    while sorter.is_active():
        ready = list(sorter.get_ready())
        if not ready:
            break
        for node in ready:
            sorter.done(node)
