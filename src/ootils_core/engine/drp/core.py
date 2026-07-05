"""
Canonical packaged DRP math core (ADR-020 §Unité de planification / ADR-028).
DB-free, pure, deterministic — the distribution echelon of the single netting
cascade (MRP is the make/buy echelon; both share the same gross-to-net maths
applied to different graph arcs: distribution arcs here, BOM arcs in mrp/core).

Planning key = (item, location). This is the ONE structural difference from the
MRP math core (engine/mrp/core.py), which plans item-level (pooled across
locations). Every projection, excess and signal below is scoped to a single
(item, location) coordinate; a transfer link moves a deficit at one location's
coordinate against another location's excess.

SCOPE — V1 (checkpoint pilote, business defaults LOCKED):
  * Single-hop transfers only. Multi-hop (source -> hub -> dest) is OUT of scope
    — a deficit is served directly from a linked source's excess, never relayed.
  * Greedy priority allocation, NOT fair-share and NOT network optimisation: a
    source's excess is consumed link-by-link in (priority, source) order; the
    first eligible destinations drain it. Fair-share splitting of a scarce
    source across competing destinations is a PR2+ refinement.
  * Net demand per (item, location) per bucket = simple max(orders, forecast)
    (see projected_deficits / DRPData construction in loader.py). The per-item
    forecast-consumption WINDOW of #349 (backward-before-forward cross-bucket
    consumption) is item-level machinery living in mrp/core.consume_demand; a
    per-location windowed variant is a PR2+ refinement, deliberately not
    reimplemented here. window==0 is exactly the golden-master max semantics.

Determinism: every public function returns results in a fully sorted order and
never relies on dict iteration order. Ties in link selection resolve by
(priority, source_location) so the plan is reproducible run-to-run.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TransferLink:
    """A single active distribution lane between two locations.

    Sourced from distribution_links (migration 029) by the loader.
    lead_buckets is the transit lead time expressed in WEEKLY buckets
    (ceil(transit_lead_time_days / 7)) — the DRP core works in the same weekly
    bucket grid as the MRP core. min_qty / max_qty mirror the link's
    minimum_shipment_qty / maximum_shipment_qty (max_qty None => uncapped).
    priority is the sourcing preference (1 = highest), used to order candidate
    sources for a destination in deficit.

    item is None for a GENERIC lane (distribution_links.item_id NULL — usable by
    any item on that (source, dest) pair) or an item key for an ITEM-SPECIFIC
    lane.

    link_ref is a stable per-row discriminant (the loader populates it from
    distribution_links.distribution_link_id, stringified) used ONLY as a final,
    last-resort determinism tie-break (#395 F6) when two link rows are
    otherwise identical for sorting purposes — same source, same dest, same
    priority (e.g. two genuinely duplicate lanes on the same pair at the same
    priority). Without it, such a tie would be broken by physical scan/list
    order, which is not a business signal and would make the emitted plan
    depend on incidental row order. An empty string (the default — no known
    row identity, e.g. a hand-built test link) still participates in the sort
    key like any other value; it does not special-case anything.

    Field order/default matter for BOTH new fields: item and link_ref are
    appended LAST, each with a default, specifically so every pre-existing
    6-positional-argument construction site (TransferLink(source, dest,
    lead_buckets, min_qty, max_qty, priority)) keeps working unchanged and
    resolves to a generic (item=None) lane with no ref — backward compatible
    by construction, not by convention.
    """

    source_location: str
    dest_location: str
    lead_buckets: int
    min_qty: float
    max_qty: float | None
    priority: int
    item: str | None = None
    link_ref: str = ""


@dataclass(frozen=True)
class TransferSignal:
    """A proposed inter-site transfer with its evidence embedded (explainability,
    ADR-004): the deficit it covers, the source excess that funded it, and the
    ship/arrival timing.

    arrival_bucket may be AT OR AFTER deficit_bucket when ship_bucket floored to
    0 (the transit lead time does not fit before the deficit). The signal is
    STILL emitted — that is the honest state of the plan (the transfer is the
    best available action even though it lands late); a consumer reads
    arrival_bucket > deficit_bucket as "covered late". Never silently dropped.
    """

    item: str
    source_location: str
    dest_location: str
    qty: float
    ship_bucket: int
    arrival_bucket: int
    deficit_bucket: int
    deficit_qty: float
    source_excess_before: float


def _projected_deficit_for_coord(
    demand: dict[int, float],
    on_hand: float,
    safety: float,
    horizon_buckets: int,
) -> tuple[int, float] | None:
    """First bucket where projected on-hand at ONE (item, location) coordinate
    drops below safety stock, and the quantity needed to climb back to safety.

    Variant of mrp/core.first_shortage scoped to a single coordinate (that
    function keys by item and reads a PlanningData; here demand/on_hand/safety
    are already the per-coordinate values, and the DRP deficit projection has NO
    scheduled receipts — the whole point of the pass is to discover the transfer
    receipts the coordinate needs). Semantics are otherwise IDENTICAL to
    first_shortage:

      * walk weekly buckets accumulating on top of on_hand;
      * each bucket subtracts that bucket's net demand;
      * trigger at the SAFETY threshold (pa < safety), not at stockout — safety
        is the reorder trigger that leaves lead time to recover, matching the
        MRP projection; for a coordinate with safety==0 this reduces to the
        first negative (stockout) bucket;
      * deficit = safety - pa, i.e. the quantity that restores safety (consumers
        must NOT add safety again).

    Returns (deficit_bucket, deficit_qty) or None if the coordinate never breaks
    safety over the horizon.
    """
    pa = on_hand
    for t in range(horizon_buckets):
        pa -= demand.get(t, 0.0)
        if pa < safety:
            return t, safety - pa
    return None


def projected_deficits(
    demand_by_loc: dict[tuple[str, str], dict[int, float]],
    on_hand_by_loc: dict[tuple[str, str], float],
    safety_by_loc: dict[tuple[str, str], float],
    horizon_buckets: int,
) -> dict[tuple[str, str], tuple[int, float]]:
    """First-shortage projection per (item, location) coordinate.

    For every coordinate carrying demand OR on-hand OR safety, run the
    single-coordinate safety-threshold projection (see
    _projected_deficit_for_coord, the per-location variant of
    mrp/core.first_shortage). Returns {(item, location): (deficit_bucket,
    deficit_qty)} for the coordinates that DO break safety on the horizon;
    coordinates that stay at or above safety are omitted (no deficit, no key).

    Deterministic: independent of dict iteration order (each coordinate is
    projected in isolation; the result dict carries no ordering semantics — the
    caller, transfer_signals, sorts).
    """
    coords = set(demand_by_loc) | set(on_hand_by_loc) | set(safety_by_loc)
    out: dict[tuple[str, str], tuple[int, float]] = {}
    for coord in coords:
        demand = demand_by_loc.get(coord, {})
        on_hand = on_hand_by_loc.get(coord, 0.0)
        safety = safety_by_loc.get(coord, 0.0)
        hit = _projected_deficit_for_coord(demand, on_hand, safety, horizon_buckets)
        if hit is not None:
            out[coord] = hit
    return out


def excess_by_location(
    demand_by_loc: dict[tuple[str, str], dict[int, float]],
    on_hand_by_loc: dict[tuple[str, str], float],
    safety_by_loc: dict[tuple[str, str], float],
    horizon_buckets: int,
) -> dict[tuple[str, str], float]:
    """Distributable excess per (item, location) coordinate.

    excess = on_hand - (total net demand over the horizon + safety), floored at
    0. Conservative BY DESIGN: a source never offers stock it needs to cover its
    own horizon demand or to hold its own safety buffer — only the surplus above
    both is transferable. A coordinate with no surplus (or unknown) yields 0.

    This is intentionally a horizon-total test, NOT a time-phased one: V1 answers
    "does this location hold more than it will ever need on the horizon", which
    is the safe lower bound on what it can give away. A time-phased "excess as of
    the ship bucket" refinement (a source may hold transient early-horizon excess
    it consumes later) is PR2+.

    The horizon window here is IDENTICAL to projected_deficits': demand is
    summed over `t < horizon_buckets` only (bucket keys at or beyond the horizon
    ceiling are ignored). One shared window for both functions is deliberate —
    a demand line that lands past the loaded horizon is not visible to either
    the deficit projection or the excess calculation, so a source's excess
    figure is never deflated by out-of-window demand the deficit side would
    never have seen either (see the loader for the matching clip on the
    customer-order side).

    Only coordinates present in on_hand_by_loc can carry excess (no stock =>
    nothing to give); the result omits coordinates with excess <= 0.
    """
    out: dict[tuple[str, str], float] = {}
    for coord, on_hand in on_hand_by_loc.items():
        total_demand = sum(
            q for t, q in demand_by_loc.get(coord, {}).items() if t < horizon_buckets
        )
        safety = safety_by_loc.get(coord, 0.0)
        excess = float(on_hand) - (total_demand + safety)
        if excess > 0:
            out[coord] = excess
    return out


def _resolve_candidate_links(
    dest_links: list[TransferLink], item: str
) -> list[TransferLink]:
    """Resolve the candidate links for ONE (item, destination) pair out of every
    active link landing at that destination, applying item scoping and
    SPECIFICITY-REFINEMENT de-duplication (#395 F2/F3, revised).

    Scoping: a link with item is None is GENERIC (any item may use it); a link
    with item set is usable ONLY by that exact item. A candidate for `item` is
    therefore every dest_link where `link.item is None or link.item == item`.

    Dedup is a REFINEMENT relation ACROSS specificity levels ONLY — it is NOT a
    single-winner-per-pair eviction, and it NEVER compares two links of the SAME
    specificity against each other to pick one:

      * If ONE OR MORE item-specific lanes exist for a (source, dest) pair, they
        REPLACE every generic lane on that SAME pair for this item — a
        specific lane is the master-data statement "route THIS item on THIS
        pair THIS way", which supersedes the generic default. This is the only
        case where a lane is excluded.
      * Two (or more) lanes of the SAME specificity on the same pair — two
        generic lanes, or two lanes both specific to this item — are NEVER
        deduped against each other. Both stay candidates: this is declared
        capacity from the master data (e.g. two genuinely parallel lanes on
        one physical lane, or a duplicate row), and the engine does not
        silently second-guess which one "wins" — that would make a capacity
        figure or a priority ordering depend on an arbitrary tie-break
        (upstream: comparing link_ref, a stringified UUID with no business
        order, previously picked a "winner" by lexical accident — dropping a
        real max_qty capacity or letting a lower-priority duplicate evict a
        higher-priority one). Lane-duplicate hygiene is a data-quality
        concern, not something this pure function arbitrates.
      * Same-specificity duplicates are therefore ALL kept as candidates and
        served SEQUENTIALLY by the existing deterministic order (priority ASC,
        source_location, link_ref — see below): the determinism guarantee
        (#395 F6) comes from that STABLE ordering, not from evicting one of
        them. A caller draining a deficit against two same-priority same-pair
        lanes (one capped, one uncapped) drains the FIRST in that order up to
        its cap, then falls through to the next.

    This refinement-only contract is a business default, documented and
    🎯-adjustable — a future revision could add real dedup of TRUE duplicate
    rows (same source, dest, item, priority, min/max — an actual data-entry
    accident), but that requires a positive signal this function does not have
    ("these two rows are the same lane recorded twice" vs "these are two
    genuinely parallel lanes"), so V1 never guesses.

    Implementation: partition item-eligible links by (source, dest) pair; a
    pair keeps ONLY its item-specific links if it has at least one, otherwise
    ONLY its generic links — no link-vs-link comparison, no per-pair single
    winner. Flatten and sort ONCE (priority ASC, source_location, link_ref) —
    the existing determinism contract, with link_ref (#395 F6) as the final
    tie-break for two rows that would otherwise compare equal (same source
    into the same dest at the same priority).
    """
    eligible = [lk for lk in dest_links if lk.item is None or lk.item == item]

    specific_by_pair: dict[tuple[str, str], list[TransferLink]] = {}
    generic_by_pair: dict[tuple[str, str], list[TransferLink]] = {}
    for link in eligible:
        pair = (link.source_location, link.dest_location)
        bucket = specific_by_pair if link.item is not None else generic_by_pair
        bucket.setdefault(pair, []).append(link)

    resolved: list[TransferLink] = []
    for pair, generics in generic_by_pair.items():
        if pair not in specific_by_pair:
            resolved.extend(generics)
    for specifics in specific_by_pair.values():
        resolved.extend(specifics)

    return sorted(resolved, key=lambda lk: (lk.priority, lk.source_location, lk.link_ref))


def transfer_signals(
    demand_by_loc: dict[tuple[str, str], dict[int, float]],
    on_hand_by_loc: dict[tuple[str, str], float],
    safety_by_loc: dict[tuple[str, str], float],
    links: list[TransferLink],
    horizon_buckets: int,
) -> list[TransferSignal]:
    """Canonical DRP transfer signal generation. PURE, DB-free, deterministic.

    For every (item, destination) coordinate in deficit (see projected_deficits),
    source the deficit from linked locations' distributable excess (see
    excess_by_location), greedily by link priority:

      * Candidate links for a destination = active links whose dest_location is
        that coordinate's location, scoped to links generic (item is None) OR
        specific to that coordinate's item, resolved by specificity-refinement
        per (source, dest) pair (see _resolve_candidate_links — a specific
        lane replaces every generic on its OWN pair; same-specificity
        duplicates are NEVER evicted against each other, both stay candidates)
        — and whose source_location holds excess of the SAME item — sorted
        (priority ASC, source_location, link_ref) for determinism.
      * qty = min(remaining deficit, remaining source excess, link.max_qty if
        not None).
      * MINIMUM-SHIPMENT RULE (business default, LOCKED but adjustable): if the
        computed qty is below link.min_qty, NO signal is emitted on that link.
        The deficit is neither inflated up to the minimum (we do not ship stock
        the destination does not need just to clear a lane minimum) nor served
        below the minimum (the lane physically cannot). The remaining deficit
        then falls through to the next-priority link. A deficit that no link can
        satisfy at or above its minimum is left uncovered (no signal) — honest.
      * GREEDY SHARED EXCESS: each source coordinate's excess is a single running
        counter decremented as links draw on it, so one unit of excess never
        funds two destinations (same shared-counter discipline as remaining_fc
        in mrp/core #349). Sources are drawn in the order destinations are
        processed (sorted, below), which makes the allocation reproducible.

    Timing per signal:
      * ship_bucket   = max(0, deficit_bucket - link.lead_buckets)
      * arrival_bucket = ship_bucket + link.lead_buckets
    When the deficit is closer than the transit lead time, ship_bucket floors at
    0 and arrival_bucket lands AT/AFTER deficit_bucket — the signal is still
    emitted (covered late is the truth of the plan; see TransferSignal).

    Output is sorted (item, dest_location, deficit_bucket, priority,
    source_location, link_ref) — a total order, so the signal list is
    byte-stable even across two genuinely duplicate lanes on the same pair at
    the same priority (#395 F6; link_ref is "" when unset, a no-op tie-break).

    NOTE (deliberately out of scope, V1): multi-hop relay, fair-share splitting
    of a scarce source across competing destinations, and network cost
    optimisation. See the module docstring.
    """
    deficits = projected_deficits(demand_by_loc, on_hand_by_loc, safety_by_loc, horizon_buckets)
    excess = excess_by_location(demand_by_loc, on_hand_by_loc, safety_by_loc, horizon_buckets)

    # Index active links by destination location ONLY as a first cheap filter
    # (independent of item). The item scoping + specificity-refinement dedup
    # happens per (item, dest) below via _resolve_candidate_links, since which
    # links are eligible now depends on the deficit's item, not just its
    # destination.
    links_by_dest: dict[str, list[TransferLink]] = {}
    for link in links:
        links_by_dest.setdefault(link.dest_location, []).append(link)

    # Carry the emitting link's priority and link_ref alongside each signal so
    # the output sort can key on (item, dest, deficit_bucket, priority, source,
    # link_ref) without a reverse lookup — priority/link_ref are not part of
    # TransferSignal's public shape (they are link-level evidence, not
    # signal-level), so they live only in this local sort-key tuple.
    keyed: list[tuple[str, str, int, int, str, str, TransferSignal]] = []
    # Process deficits in a deterministic coordinate order so the greedy draw on
    # shared source excess is reproducible (item, then dest_location).
    for (item, dest_location) in sorted(deficits):
        deficit_bucket, deficit_qty = deficits[(item, dest_location)]
        remaining_deficit = deficit_qty
        candidates = _resolve_candidate_links(links_by_dest.get(dest_location, []), item)
        for link in candidates:
            if remaining_deficit <= 0:
                break
            source_coord = (item, link.source_location)
            avail = excess.get(source_coord, 0.0)
            if avail <= 0:
                continue
            qty = min(remaining_deficit, avail)
            if link.max_qty is not None:
                qty = min(qty, link.max_qty)
            if qty < link.min_qty:
                # Below the lane minimum: emit nothing on this link, leave the
                # deficit for the next-priority link (see docstring).
                continue
            ship_bucket = max(0, deficit_bucket - link.lead_buckets)
            arrival_bucket = ship_bucket + link.lead_buckets
            signal = TransferSignal(
                item=item,
                source_location=link.source_location,
                dest_location=dest_location,
                qty=qty,
                ship_bucket=ship_bucket,
                arrival_bucket=arrival_bucket,
                deficit_bucket=deficit_bucket,
                deficit_qty=deficit_qty,
                source_excess_before=avail,
            )
            keyed.append((
                item, dest_location, deficit_bucket, link.priority,
                link.source_location, link.link_ref, signal,
            ))
            excess[source_coord] = avail - qty
            remaining_deficit -= qty

    keyed.sort(key=lambda k: (k[0], k[1], k[2], k[3], k[4], k[5]))
    return [k[6] for k in keyed]
