"""
nodes.py — generic helpers + the 4 supply-node generators (OH, PO, WO, transfer).

All four node types share the same target table (`nodes`) and column set;
only `node_type` and the time_ref semantics differ. We factor the bulk-insert
through a single helper and keep the per-type logic minimal.

Volumes for profile M (5K SKUs with ~11K planning_params entries):
  OnHandSupply      ~11 000   one per planning_params row, quantity centred
                              around safety_stock * random(0.5, 2.0)
  PurchaseOrderSupply ~5-8K   0-3 open POs per bought (item, plant)
  WorkOrderSupply     ~5-8K   0-2 in-flight WOs per made (item, plant)
  TransferSupply         500  FG inventory in transit plant -> DC
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection
from ootils_core.seed.config import Profile
from ootils_core.seed.master.items import ItemSet
from ootils_core.seed.master.locations import LocationSet
from ootils_core.seed.network.planning_params import PlanningParamSet


BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


@dataclass(frozen=True)
class SupplyNode:
    """A single node row destined for the `nodes` table."""
    node_id: UUID
    node_type: str  # 'OnHandSupply' | 'PurchaseOrderSupply' | 'WorkOrderSupply' | 'TransferSupply'
    item_id: UUID
    location_id: UUID
    quantity: Decimal
    qty_uom: str
    time_grain: str  # 'exact_date'
    time_ref: date


@dataclass
class TransactionalSet:
    on_hand: list[SupplyNode]
    purchase_orders: list[SupplyNode]
    work_orders: list[SupplyNode]
    transfers: list[SupplyNode]

    @property
    def total(self) -> int:
        return (len(self.on_hand) + len(self.purchase_orders)
                + len(self.work_orders) + len(self.transfers))

    @property
    def all_nodes(self) -> list[SupplyNode]:
        return [*self.on_hand, *self.purchase_orders, *self.work_orders, *self.transfers]


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------


def _item_uom(item_id: UUID, item_set: ItemSet) -> str:
    """Find the UoM for an item — used to set qty_uom on the supply node."""
    # Build a lookup once. The caller can do this too; we keep it lazy here
    # at the cost of a linear search. With 5K items this is fine but if it
    # becomes hot, pass an index in.
    for it in item_set.all:
        if it.item_id == item_id:
            return it.uom
    return "EA"


def _build_item_uom_index(item_set: ItemSet) -> dict[UUID, str]:
    return {it.item_id: it.uom for it in item_set.all}


def generate_on_hand(
    profile: Profile,
    item_set: ItemSet,
    pp_set: PlanningParamSet,
) -> list[SupplyNode]:
    """One OnHandSupply per planning_params entry.

    Quantity is calibrated relative to the planned safety_stock so that
    downstream propagation yields a meaningful shortage rate. Phase 7 will
    fine-tune the on-hand stock to hit the profile's target_shortage_pct.

    For now:
      - 25% of pairs: 0 OH (stockout candidate — supplied by POs/WOs/transfers)
      - 75% of pairs: random(0.5, 2.0) * safety_stock_qty (or MOQ if no SS)
    """
    rng = random.Random(profile.seed + 5001)
    uom_idx = _build_item_uom_index(item_set)
    today = date.today()
    nodes: list[SupplyNode] = []

    for pp in pp_set.records:
        if rng.random() < 0.25:
            qty = Decimal("0")
        else:
            base = pp.safety_stock_qty if pp.safety_stock_qty else (pp.min_order_qty or Decimal("10"))
            qty = (base * Decimal(str(round(rng.uniform(0.5, 2.0), 3)))).quantize(Decimal("1"))
        nodes.append(SupplyNode(
            node_id=uuid4(),
            node_type="OnHandSupply",
            item_id=pp.item_id,
            location_id=pp.location_id,
            quantity=qty,
            qty_uom=uom_idx.get(pp.item_id, "EA"),
            time_grain="exact_date",
            time_ref=today,
        ))
    return nodes


def generate_purchase_orders(
    profile: Profile,
    item_set: ItemSet,
    pp_set: PlanningParamSet,
) -> list[SupplyNode]:
    """0-3 open POs per bought (item, plant) pair, with future ETAs.

    Quantity = MOQ * random(1, 3). time_ref drawn from
    [today + lt_sourcing, today + lt_sourcing + 60d] so POs land across
    the near-term horizon.
    """
    rng = random.Random(profile.seed + 5002)
    uom_idx = _build_item_uom_index(item_set)
    today = date.today()
    nodes: list[SupplyNode] = []

    # Only entries where the item is bought from a supplier (is_make=False
    # AND preferred_supplier_id is not None — transfers have no supplier).
    for pp in pp_set.records:
        if pp.is_make or pp.preferred_supplier_id is None:
            continue
        n_pos = rng.randint(0, 3)
        for _ in range(n_pos):
            lt = pp.lead_time_sourcing_days or 30
            eta = today + timedelta(days=rng.randint(lt, lt + 60))
            qty = (pp.min_order_qty or Decimal("10")) * Decimal(rng.randint(1, 3))
            nodes.append(SupplyNode(
                node_id=uuid4(),
                node_type="PurchaseOrderSupply",
                item_id=pp.item_id,
                location_id=pp.location_id,
                quantity=qty,
                qty_uom=uom_idx.get(pp.item_id, "EA"),
                time_grain="exact_date",
                time_ref=eta,
            ))
    return nodes


def generate_work_orders(
    profile: Profile,
    item_set: ItemSet,
    pp_set: PlanningParamSet,
) -> list[SupplyNode]:
    """0-2 in-flight WOs per made (item, plant) pair.

    Completion date = today + manufacturing_lead_time +/- a few days.
    """
    rng = random.Random(profile.seed + 5003)
    uom_idx = _build_item_uom_index(item_set)
    today = date.today()
    nodes: list[SupplyNode] = []

    for pp in pp_set.records:
        if not pp.is_make:
            continue
        n_wos = rng.randint(0, 2)
        for _ in range(n_wos):
            lt = pp.lead_time_manufacturing_days or 3
            done = today + timedelta(days=rng.randint(1, max(lt, 1) + 14))
            qty = (pp.min_order_qty or Decimal("10")) * Decimal(rng.randint(1, 4))
            nodes.append(SupplyNode(
                node_id=uuid4(),
                node_type="WorkOrderSupply",
                item_id=pp.item_id,
                location_id=pp.location_id,
                quantity=qty,
                qty_uom=uom_idx.get(pp.item_id, "EA"),
                time_grain="exact_date",
                time_ref=done,
            ))
    return nodes


def generate_transfers(
    profile: Profile,
    item_set: ItemSet,
    loc_set: LocationSet,
    pp_set: PlanningParamSet,
) -> list[SupplyNode]:
    """In-transit inventory: FG transfers from producer plants to DCs.

    We pick a sample of (FG, DC) entries from planning_params (is_make=False
    + supplier=None means it's transferred to that DC) and emit one
    TransferSupply per ~10 such entries.
    """
    rng = random.Random(profile.seed + 5004)
    uom_idx = _build_item_uom_index(item_set)
    today = date.today()
    nodes: list[SupplyNode] = []

    # Candidate entries: items at DCs that are transferred (not made there,
    # no supplier — these come from a producer plant via TransferSupply).
    dc_ids = {d.location_id for d in loc_set.dcs()}
    candidates = [
        pp for pp in pp_set.records
        if pp.location_id in dc_ids and not pp.is_make and pp.preferred_supplier_id is None
    ]
    # Pick ~33% of FG-at-DC candidates to have an in-flight transfer. FGs
    # cycle through DCs frequently in real distribution networks.
    sample_size = max(1, len(candidates) // 3)
    sample = rng.sample(candidates, min(sample_size, len(candidates)))

    for pp in sample:
        transit = pp.lead_time_transit_days or 5
        arrival = today + timedelta(days=rng.randint(1, transit + 3))
        qty = (pp.min_order_qty or Decimal("10")) * Decimal(rng.randint(1, 3))
        nodes.append(SupplyNode(
            node_id=uuid4(),
            node_type="TransferSupply",
            item_id=pp.item_id,
            location_id=pp.location_id,
            quantity=qty,
            qty_uom=uom_idx.get(pp.item_id, "EA"),
            time_grain="exact_date",
            time_ref=arrival,
        ))
    return nodes


def generate_transactional(
    profile: Profile,
    item_set: ItemSet,
    loc_set: LocationSet,
    pp_set: PlanningParamSet,
) -> TransactionalSet:
    return TransactionalSet(
        on_hand=generate_on_hand(profile, item_set, pp_set),
        purchase_orders=generate_purchase_orders(profile, item_set, pp_set),
        work_orders=generate_work_orders(profile, item_set, pp_set),
        transfers=generate_transfers(profile, item_set, loc_set, pp_set),
    )


# ---------------------------------------------------------------------------
# Bulk insertion
# ---------------------------------------------------------------------------


def insert_transactional(
    conn: DictRowConnection,
    tx_set: TransactionalSet,
) -> int:
    """Bulk-insert all four node types in ONE statement via UNNEST."""
    nodes = tx_set.all_nodes
    if not nodes:
        return 0

    ids = [n.node_id for n in nodes]
    types = [n.node_type for n in nodes]
    item_ids = [n.item_id for n in nodes]
    loc_ids = [n.location_id for n in nodes]
    qtys = [n.quantity for n in nodes]
    uoms = [n.qty_uom for n in nodes]
    grains = [n.time_grain for n in nodes]
    refs = [n.time_ref for n in nodes]

    cur = conn.execute(
        """
        INSERT INTO nodes
            (node_id, node_type, scenario_id, item_id, location_id,
             quantity, qty_uom, time_grain, time_ref, is_dirty, active)
        SELECT
            n.id, n.tp, %s, n.item_id, n.loc_id,
            n.qty, n.uom, n.grain, n.ref, FALSE, TRUE
        FROM UNNEST(
            %s::uuid[], %s::text[], %s::uuid[], %s::uuid[],
            %s::numeric[], %s::text[], %s::text[], %s::date[]
        ) AS n(id, tp, item_id, loc_id, qty, uom, grain, ref)
        """,
        (
            BASELINE_SCENARIO_ID,
            ids, types, item_ids, loc_ids,
            qtys, uoms, grains, refs,
        ),
    )
    return cur.rowcount or 0
