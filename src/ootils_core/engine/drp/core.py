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
  * FAIR-SHARE proportional allocation (#395 PR2a, ADR-028), NOT the earlier
    dest-first greedy and NOT network optimisation. A scarce source is split
    PROPORTIONALLY across the destinations it feeds instead of the first
    destination draining it (which starved its peers). The rule, in one place:
      - SOURCE-FIRST, priority-stratified. The outer loop is the PRIORITY level
        (ascending), then within a level the SOURCES in the fixed total order
        (priority_min of the source, source_location, item), each source
        splitting its remaining excess across the destinations it serves AT
        THAT level. Destinations are the inner dimension — the opposite of the
        old dest-first drain.
      - Proportional to RESIDUAL deficits: for one source at one level,
        part_dest = avail_source * (residual_deficit_dest / Σ residual_deficits)
        over the destinations it serves at that level whose residual is still
        > 0. avail_source is snapshotted at the start of the level so the split
        stays a true proportion (Σ of the ideal parts == the snapshot); the
        live counter is what is actually decremented, so no rounding/residual
        slack is ever over-drawn.
      - Priority x fair-share interaction: served by ASCENDING priority level,
        fair-share only AMONG lanes of EQUAL priority, greedy BETWEEN levels — a
        deficit a p1 lane covers is reduced (its residual shrinks) BEFORE any p2
        lane is even considered, GLOBALLY across sources, not just within one
        source. That is why the priority level, not the source, is the outer
        loop: it is the only ordering under which every p1 lane (any source)
        acts before any p2 lane, which is what "priority" means here.
      - Switch _FAIR_SHARE_RESPECTS_PRIORITY (module constant, default True,
        🎯 pilot-adjustable): False collapses every candidate into ONE virtual
        level — pure fair-share across all lanes, priority ignored. The pilot
        can flip the whole behaviour trivially.
  * LOGISTICS ROUNDING — rounded DOWN, remnant returned (#395 PR2a, ADR-028).
    Each lane carries a transfer_multiple (case/pallet unit, migration 065,
    DEFAULT 1 == no rounding). The transferred qty is FLOORED to the nearest
    whole multiple, bounded by the true need: qty = floor(min(part, avail,
    max_qty) / mult) * mult. This is a DELIBERATE divergence from MRP lot
    sizing (mrp/lot_sizing CEILs): the DRP MOVES finished stock, so
    over-transferring would strip the source — the conservative floor never
    ships more than needed. The DOWN-round remnant (qty_brute - qty_rounded) is
    NOT consumed: it stays in avail_source, available to the next destination /
    the next level (no remnant is ever lost). A rounded qty below the lane's
    min_qty yields 0 on that lane (the existing minimum-shipment rule, fall
    through to the next lane); a residual smaller than one multiple therefore
    also yields 0 (no micro-transfer). See _fair_share_round.
  * Net demand per (item, location) per bucket = simple max(orders, forecast)
    (see projected_deficits / DRPData construction in loader.py). The per-item
    forecast-consumption WINDOW of #349 (backward-before-forward cross-bucket
    consumption) is item-level machinery living in mrp/core.consume_demand; a
    per-location windowed variant is a PR2+ refinement, deliberately not
    reimplemented here. window==0 is exactly the golden-master max semantics.

Determinism: every public function returns results in a fully sorted order and
never relies on dict iteration order. The fair-share PROCESSING order (priority
level, then sources by (priority_min, source_location, item), then destinations
sorted) is fully figé; the OUTPUT sort key is unchanged (item, dest_location,
deficit_bucket, priority, source_location, link_ref) so the emitted plan is
byte-stable run-to-run and independent of input dict/list insertion order.
"""
from __future__ import annotations

import math
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
    distribution_links.distribution_link_id, stringified) used as a
    near-last-resort determinism tie-break (#395 F6) when two link rows are
    otherwise identical for sorting purposes — same source, same dest, same
    priority (e.g. two genuinely duplicate lanes on the same pair at the same
    priority). Without it, such a tie would be broken by physical scan/list
    order, which is not a business signal and would make the emitted plan
    depend on incidental row order. An empty string (the default — no known
    row identity, e.g. a hand-built test link) still participates in the sort
    key like any other value; it does not special-case anything.

    link_ref alone is NOT always sufficient (#395 PR2a review, MINOR fix):
    since the loader populates it from distribution_link_id (a UUID PRIMARY
    KEY), two REAL rows can never share one, but two HAND-BUILT links (tests,
    or any future caller) both left at the "" default DO collide on link_ref —
    at that point the previous sort key (priority, source_location, link_ref)
    was no longer a TOTAL order, and the outcome silently fell back to input
    insertion order. _sort_key (below) closes that gap with a further
    tie-break on the lane's remaining discriminating fields (max_qty, min_qty,
    transfer_multiple), so the order is a total order UNCONDITIONALLY: two
    links equal on every field are truly interchangeable (identical output
    whichever the caller passes first); two links differing in ANY field sort
    deterministically by that difference, never by call-site accident.

    transfer_multiple is the lane's logistics shipment multiple (case / pallet
    unit, distribution_links.transfer_multiple, migration 065). The planner
    rounds each transfer DOWN to a whole multiple of it (see _fair_share_round /
    the module §SCOPE). DEFAULT 1.0 == no rounding (continuous transfers), so a
    lane that predates the column, or a hand-built test link, keeps the exact
    pre-rounding behaviour. Must be > 0 (enforced by the DB CHECK; the core
    divides by it).

    Field order/default matter for ALL appended fields: item, link_ref and
    transfer_multiple are appended LAST, each with a default, specifically so
    every pre-existing 6-positional-argument construction site
    (TransferLink(source, dest, lead_buckets, min_qty, max_qty, priority)) keeps
    working unchanged and resolves to a generic (item=None) lane with no ref and
    a unit (1.0) multiple — backward compatible by construction, not by
    convention.
    """

    source_location: str
    dest_location: str
    lead_buckets: int
    min_qty: float
    max_qty: float | None
    priority: int
    item: str | None = None
    link_ref: str = ""
    transfer_multiple: float = 1.0


def _sort_key(
    link: TransferLink,
) -> tuple[int, str, str, float, float, float]:
    """The lane sort key used EVERYWHERE a set of candidate lanes is ordered
    (_resolve_candidate_links, the fair-share destination-lane loop, and the
    final output sort in transfer_signals) — ONE definition so the three sites
    can never silently drift apart (#395 PR2a review, MINOR fix).

    (priority, source_location, link_ref, max_qty, min_qty, transfer_multiple)
    — a TOTAL order UNCONDITIONALLY, not just "in practice" (see TransferLink's
    link_ref docstring for why link_ref alone is not always enough). max_qty is
    Optional (None == uncapped); mapped to +inf so an uncapped lane sorts AFTER
    every capped one at the same (priority, source, link_ref) — an arbitrary but
    fixed choice (the only requirement is a total, deterministic order, not a
    specific direction). Two links equal on every one of these fields ARE truly
    interchangeable: this key can never distinguish them, and the caller's own
    stable sort then preserves whichever input order it saw for that pair —
    which is fine, because by construction they are identical lanes.
    """
    return (
        link.priority,
        link.source_location,
        link.link_ref,
        link.max_qty if link.max_qty is not None else float("inf"),
        link.min_qty,
        link.transfer_multiple,
    )


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

    qty is the FINAL transferred quantity — floored to the lane's
    transfer_multiple (#395 PR2a). fair_share_qty and rounding_remnant are the
    fair-share/rounding evidence (explainability, ADR-004), computed IDENTICALLY
    by _fair_share_round whichever of the two draws below produced the signal
    (the field's MEANING is the bounded pre-round quantity either way — only
    the source of `raw_part` differs, not the formula):
      * fair_share_qty     = the qty BEFORE the logistics down-round, i.e.
                             min(raw_part, remaining source excess, remaining
                             max_qty). On a PROPORTIONAL draw (the main pass),
                             raw_part is this lane's proportional part
                             (part_dest, capped by the destination's residual).
                             On a REMNANT-SWEEP draw (#395 PR2a review, MAJOR
                             fix — see transfer_signals), raw_part is instead
                             the destination's full remaining residual: the
                             sweep is not proportional (it targets ONE neediest
                             destination with whatever the source has left), so
                             there is no "ideal share" to report — the bounded
                             need IS the closest analogue, and is what this
                             field carries for a sweep draw.
      * rounding_remnant   = fair_share_qty - qty, the sliver the down-round
                             shaved off (0 whenever transfer_multiple divides
                             fair_share_qty exactly, always true for the
                             DEFAULT multiple 1.0). NOT lost: it stays in the
                             source excess, offered to the next destination /
                             next priority level / the next sweep iteration.
    Both are required (TransferSignal is constructed ONLY in this module, so
    there is no backward-compatibility default to preserve — every construction
    site sets them).
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
    fair_share_qty: float
    rounding_remnant: float


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
        served SEQUENTIALLY by the existing deterministic order (_sort_key —
        see below): the determinism guarantee (#395 F6) comes from that STABLE
        ordering, not from evicting one of them. A caller draining a deficit
        against two same-priority same-pair lanes (one capped, one uncapped)
        drains the FIRST in that order up to its cap, then falls through to
        the next.

    This refinement-only contract is a business default, documented and
    🎯-adjustable — a future revision could add real dedup of TRUE duplicate
    rows (same source, dest, item, priority, min/max — an actual data-entry
    accident), but that requires a positive signal this function does not have
    ("these two rows are the same lane recorded twice" vs "these are two
    genuinely parallel lanes"), so V1 never guesses.

    Implementation: partition item-eligible links by (source, dest) pair; a
    pair keeps ONLY its item-specific links if it has at least one, otherwise
    ONLY its generic links — no link-vs-link comparison, no per-pair single
    winner. Flatten and sort ONCE by _sort_key — the existing determinism
    contract, TOTAL and unconditional (#395 PR2a review): link_ref (#395 F6) is
    the first tie-break for two rows that would otherwise compare equal (same
    source into the same dest at the same priority), and max_qty/min_qty/
    transfer_multiple close the (rare, hand-built-only) residual tie when
    link_ref ALSO collides.
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

    return sorted(resolved, key=_sort_key)


# Priority x fair-share interaction switch (#395 PR2a) — 🎯 pilot-adjustable.
# True (default): serve by ASCENDING priority level, fair-share only among lanes
# of EQUAL priority, greedy BETWEEN levels (a p1 lane's coverage shrinks the
# residual deficit before any p2 lane is considered, globally across sources).
# False: collapse every candidate lane into ONE virtual level — pure fair-share
# across all lanes regardless of priority. The pilot can flip this trivially to
# compare "priority-respecting fair-share" against "flat fair-share". See the
# module §SCOPE.
_FAIR_SHARE_RESPECTS_PRIORITY: bool = True


def _fair_share_round(
    raw_part: float,
    mult: float,
    min_qty: float,
    max_qty: float | None,
    avail: float,
) -> float:
    """Logistics down-rounding of one fair-share allocation (#395 PR2a, ADR-028).

    PURE, no side effects. Computes the FINAL transferable quantity on one lane
    from the proportional part it was allocated, the lane's shipment multiple,
    its min/max shipment bounds, and the source's remaining excess:

      qty_brute = min(raw_part, avail, max_qty if not None)   # bounded need
      qty       = floor(qty_brute / mult) * mult              # DOWN to a whole
                                                              # multiple

    raw_part is expected to already be bounded by the destination's residual
    deficit by the caller, so qty never exceeds what the destination is short.

    Rules (all business defaults, LOCKED but 🎯-adjustable):
      * DOWN-round, never up (deliberately OPPOSITE to MRP lot_size's CEIL): the
        DRP MOVES finished stock, so over-transferring would strip the source.
        The floor is the conservative bound — ship whole cases, never more than
        needed. The remnant (qty_brute - qty) is the caller's to keep in
        avail_source (this function does not decrement anything).
      * Below the lane minimum -> 0. If the rounded qty is < min_qty, emit
        nothing on this lane (the existing minimum-shipment rule): the lane
        physically cannot ship below its minimum, and we never inflate the need
        up to it. The caller falls through to the next lane.
      * Degenerate residual < one multiple -> 0. When qty_brute < mult the floor
        is already 0 (no partial-multiple micro-transfer), so a residual smaller
        than one case size yields no signal — covered by the floor itself, no
        special case.

    Returns the final qty (>= 0), a whole multiple of mult, or 0.0 when the lane
    is blocked by the minimum or there is less than one multiple to ship.
    """
    qty_brute = min(raw_part, avail)
    if max_qty is not None:
        qty_brute = min(qty_brute, max_qty)
    if qty_brute <= 0:
        return 0.0
    qty = math.floor(qty_brute / mult) * mult
    if qty < min_qty:
        return 0.0
    return qty


def transfer_signals(
    demand_by_loc: dict[tuple[str, str], dict[int, float]],
    on_hand_by_loc: dict[tuple[str, str], float],
    safety_by_loc: dict[tuple[str, str], float],
    links: list[TransferLink],
    horizon_buckets: int,
) -> list[TransferSignal]:
    """Canonical DRP transfer signal generation. PURE, DB-free, deterministic.

    FAIR-SHARE, source-first, priority-stratified, down-rounded, with a
    remnant-sweep pass (#395 PR2a, ADR-028). Full rationale in the module
    §SCOPE; the algorithm here:

      1. Compute per-(item, dest) deficits (projected_deficits) and per-(item,
         source) distributable excess (excess_by_location).
      2. Resolve, per (item, dest) in deficit, the candidate lanes via
         _resolve_candidate_links (item scoping + specificity-refinement dedup,
         unchanged) — then INVERT that into a source-keyed index: for each
         (item, source) coordinate, the lanes leaving it toward destinations in
         deficit. priority_min(source) = the lowest lane priority leaving it.
      3. Walk PRIORITY LEVELS ascending (one virtual level for all lanes when
         _FAIR_SHARE_RESPECTS_PRIORITY is False). At each level, walk the
         SOURCES active at that level in the fixed total order (priority_min,
         source_location, item). Each source SPLITS its remaining excess across
         the destinations it serves at that level, PROPORTIONALLY to their
         RESIDUAL deficits:
             part_dest = avail_snapshot * (residual_dest / Σ residual_dests)
         avail_snapshot is the source's excess at the START of this level, so
         the split is a true proportion; the live excess counter is what is
         decremented, so rounding/residual slack is never over-drawn.
      4. Distribute each destination's part across its lanes from this source at
         this level (sorted by _sort_key for determinism when duplicated),
         DOWN-rounding each to the lane's transfer_multiple via _fair_share_round
         (bounded by max_qty and remaining excess). A rounded qty below the
         lane's min_qty emits nothing and falls through to the next lane / next
         level (minimum-shipment rule, preserved). The down-round remnant stays
         in the live excess (returned, never lost).
      5. REMNANT SWEEP (#395 PR2a review, MAJOR fix). The proportional split in
         step 4 can leave a source's live excess UNDRAWN even though it could
         still ship a full multiple: e.g. excess=12, transfer_multiple=12, TWO
         same-level destinations both short 20 — the proportional ideal splits
         12 into 6/6, and _fair_share_round floors BOTH to 0 (0.5 of a pallet
         rounds to nothing), so NEITHER destination is served and the 12 units
         sit idle — WORSE than the old dest-first greedy, which would have
         shipped one full pallet to whichever destination it reached first, and
         a violation of the anti-starvation intent of fair-share (a scarce
         source should never end up serving NOBODY when at least one whole
         multiple is shippable to SOMEBODY). After the proportional pass for
         THIS (source, level), sweep: while the source's live excess can still
         ship >= 1 whole multiple to some destination with residual > 0 at this
         level, serve the MOST NEEDY destination (largest remaining residual;
         ties broken by the same deterministic destination order used above,
         i.e. dest_location) with the largest whole-multiple quantity that
         fits: qty = floor(min(residual, avail, remaining max_qty) / mult) *
         mult, tried lane-by-lane in _sort_key order (falling through past a
         lane whose remaining max_qty or min_qty blocks it, exactly like the
         proportional pass). "Most needy first" is a 🎯-adjustable business
         default (round-robin across tied destinations is an equally
         defensible alternative that spreads pallets instead of concentrating
         them — see the inline comment at the sweep loop). A destination that
         cannot receive a whole multiple from ANY of its lanes (blocked by
         min_qty, remaining max_qty, or genuinely < 1 multiple of residual) is
         marked exhausted for this (source, level) and never retried — since
         the source's live excess only ever DECREASES within the sweep, a
         destination that cannot be served now can never become servable later
         in the same sweep, which bounds the loop (each iteration either ships
         a multiple or permanently exhausts a destination — both are
         strictly-decreasing, finite quantities). A remaining max_qty is
         tracked PER LANE OBJECT (by identity, not by value — two lanes equal
         on every field, e.g. real duplicate rows, still have INDEPENDENT
         physical capacity and must never share one counter) across BOTH the
         proportional pass and the sweep, so the sweep never re-offers a cap
         the proportional pass already spent. mult=1 lanes are UNAFFECTED by
         this pass in practice: the proportional split already covers every
         residual down to whole units, so there is never an undrawn whole
         multiple left over (the sweep loop simply finds nothing to do and
         exits immediately).

    Determinism BETWEEN levels is greedy: a residual a p1 lane covers shrinks
    BEFORE any p2 lane is considered, GLOBALLY across sources (that is why the
    priority level, not the source, is the outer loop).

    Timing per signal:
      * ship_bucket   = max(0, deficit_bucket - link.lead_buckets)
      * arrival_bucket = ship_bucket + link.lead_buckets
    When the deficit is closer than the transit lead time, ship_bucket floors at
    0 and arrival_bucket lands AT/AFTER deficit_bucket — the signal is still
    emitted (covered late is the truth of the plan; see TransferSignal).

    Output is sorted (item, dest_location, deficit_bucket, priority,
    source_location, link_ref) — a total order (UNCHANGED from before the
    fair-share rewrite), so the signal list is byte-stable and independent of
    input dict/list insertion order, even across two genuinely duplicate lanes
    on the same pair at the same priority (#395 F6; link_ref is "" when unset, a
    no-op tie-break — see _sort_key for the residual tie-break when link_ref
    ALSO collides, #395 PR2a review MINOR fix).

    NOTE (deliberately out of scope, V1): multi-hop relay and network cost
    optimisation. See the module docstring.
    """
    deficits = projected_deficits(demand_by_loc, on_hand_by_loc, safety_by_loc, horizon_buckets)
    excess = excess_by_location(demand_by_loc, on_hand_by_loc, safety_by_loc, horizon_buckets)

    # Index active links by destination location ONLY as a first cheap filter
    # (independent of item). The item scoping + specificity-refinement dedup
    # happens per (item, dest) via _resolve_candidate_links, since which links
    # are eligible depends on the deficit's item, not just its destination.
    links_by_dest: dict[str, list[TransferLink]] = {}
    for link in links:
        links_by_dest.setdefault(link.dest_location, []).append(link)

    # Resolve the candidate lanes per (item, dest) in deficit, then INVERT into a
    # source-keyed index — the fair-share loop is source-first, so we need each
    # source's outgoing lanes, not each destination's incoming ones. Each entry
    # carries the destination coordinate + its deficit bucket so the inner loop
    # never re-reads `deficits`.
    lanes_by_source: dict[tuple[str, str], list[tuple[str, int, TransferLink]]] = {}
    for (item, dest_location) in sorted(deficits):
        deficit_bucket, _deficit_qty = deficits[(item, dest_location)]
        for link in _resolve_candidate_links(links_by_dest.get(dest_location, []), item):
            source_coord = (item, link.source_location)
            lanes_by_source.setdefault(source_coord, []).append(
                (dest_location, deficit_bucket, link)
            )

    # priority_min per source coordinate — the lowest-priority lane leaving it,
    # for the source ordering tie-break. When the priority switch is off, every
    # lane is treated as one virtual level (priority ignored for stratification;
    # the lane's real priority is still emitted on the signal + used in the
    # output sort).
    priority_min: dict[tuple[str, str], int] = {
        src: min(link.priority for _dest, _b, link in lanes)
        for src, lanes in lanes_by_source.items()
    }

    # Residual deficit per (item, dest) — the shared running counter fair-share
    # splits against; decremented as lanes cover it, globally across sources and
    # levels (the greedy-between-levels discipline).
    residual: dict[tuple[str, str], float] = {
        coord: qty for coord, (_bucket, qty) in deficits.items()
    }

    # Global priority levels, ascending. With the switch off, a single sentinel
    # level makes every lane share one fair-share pass (priority ignored).
    if _FAIR_SHARE_RESPECTS_PRIORITY:
        levels = sorted({link.priority for lanes in lanes_by_source.values() for _d, _b, link in lanes})
    else:
        levels = [0]

    # Carry the emitting lane itself alongside each signal so the output sort
    # can key on (item, dest, deficit_bucket) + _sort_key(link) — the SAME
    # total order used to pick candidates in the first place (#395 PR2a review,
    # MINOR fix) — without a reverse lookup. The lane is link-level evidence,
    # not part of TransferSignal's public shape, so it lives only in this
    # local tuple.
    keyed: list[tuple[str, str, int, TransferLink, TransferSignal]] = []

    for level in levels:
        # Sources active at this level, in the fixed total order.
        active_sources = sorted(
            (src for src in lanes_by_source),
            key=lambda src: (priority_min[src], src[1], src[0]),
        )
        for source_coord in active_sources:
            item, source_location = source_coord
            # This source's lanes AT this level (all lanes when the switch is
            # off). Group by destination so the fair-share split sees each
            # destination ONCE, whatever its lane count.
            level_lanes = [
                (dest_location, deficit_bucket, link)
                for (dest_location, deficit_bucket, link) in lanes_by_source[source_coord]
                if (not _FAIR_SHARE_RESPECTS_PRIORITY) or link.priority == level
            ]
            if not level_lanes:
                continue
            lanes_by_dest_here: dict[str, list[tuple[int, TransferLink]]] = {}
            for dest_location, deficit_bucket, link in level_lanes:
                lanes_by_dest_here.setdefault(dest_location, []).append((deficit_bucket, link))

            # Snapshot the excess at the START of the level so the proportion is
            # stable across this level's destinations; the live counter (excess)
            # is what actually funds and decrements.
            avail_snapshot = excess.get(source_coord, 0.0)
            if avail_snapshot <= 0:
                continue
            active_dests = sorted(
                dest for dest in lanes_by_dest_here if residual.get((item, dest), 0.0) > 0
            )
            total_residual = sum(residual[(item, dest)] for dest in active_dests)
            if total_residual <= 0:
                continue

            # Remaining max_qty per LANE OBJECT (by identity — id(link) — never
            # by value: two lanes equal on every field, e.g. genuine duplicate
            # rows, each have their OWN physical capacity and must never share
            # a counter). Shared across the proportional pass below AND the
            # remnant sweep, so the sweep never re-offers a cap the
            # proportional pass already spent on that exact lane instance.
            shipped_by_link: dict[int, float] = {}

            def _remaining_max(lk: TransferLink) -> float | None:
                if lk.max_qty is None:
                    return None
                return lk.max_qty - shipped_by_link.get(id(lk), 0.0)

            for dest_location in active_dests:
                dest_coord = (item, dest_location)
                # Proportional ideal, capped by the destination's own residual so
                # a single-destination source (ratio 1.0) never over-serves it.
                ideal = avail_snapshot * (residual[dest_coord] / total_residual)
                dest_part = min(ideal, residual[dest_coord])
                for deficit_bucket, link in sorted(
                    lanes_by_dest_here[dest_location], key=lambda t: _sort_key(t[1])
                ):
                    avail = excess.get(source_coord, 0.0)
                    if dest_part <= 0 or avail <= 0:
                        break
                    raw_part = min(dest_part, residual[dest_coord])
                    remaining_max = _remaining_max(link)
                    qty = _fair_share_round(
                        raw_part, link.transfer_multiple, link.min_qty, remaining_max, avail
                    )
                    if qty <= 0:
                        # Below the lane minimum / less than one multiple: emit
                        # nothing on this lane, leave the residual for the next
                        # lane (or the next level / source).
                        continue
                    fair_share_qty = min(raw_part, avail)
                    if remaining_max is not None:
                        fair_share_qty = min(fair_share_qty, remaining_max)
                    ship_bucket = max(0, deficit_bucket - link.lead_buckets)
                    arrival_bucket = ship_bucket + link.lead_buckets
                    signal = TransferSignal(
                        item=item,
                        source_location=source_location,
                        dest_location=dest_location,
                        qty=qty,
                        ship_bucket=ship_bucket,
                        arrival_bucket=arrival_bucket,
                        deficit_bucket=deficit_bucket,
                        deficit_qty=deficits[dest_coord][1],
                        source_excess_before=avail,
                        fair_share_qty=fair_share_qty,
                        rounding_remnant=fair_share_qty - qty,
                    )
                    keyed.append((item, dest_location, deficit_bucket, link, signal))
                    # Decrement the LIVE counters only (the down-round remnant
                    # stays in excess, available downstream). dest_part shrinks
                    # by the shipped qty so a duplicate lane picks up the rest.
                    excess[source_coord] = avail - qty
                    residual[dest_coord] -= qty
                    dest_part -= qty
                    shipped_by_link[id(link)] = shipped_by_link.get(id(link), 0.0) + qty

            # --- REMNANT SWEEP (#395 PR2a review, MAJOR fix) --------------
            # The proportional pass above can leave the source's live excess
            # undrawn even though a WHOLE multiple could still ship to SOME
            # destination (see the function docstring's worked example:
            # excess=12, mult=12, two destinations both short 20 -> the
            # proportional ideal splits 6/6, both floor to 0, and 12 units of
            # real capacity sit idle -> WORSE than the old greedy, and an
            # anti-starvation violation). Sweep: while this source can still
            # ship >= 1 whole multiple to a destination with residual > 0 at
            # this level, serve the MOST NEEDY one.
            #
            # "Most needy first" (largest remaining residual, tie-broken by
            # dest_location) is a 🎯-adjustable business default — the
            # equally-defensible alternative is round-robin across tied
            # destinations, which SPREADS whole multiples instead of
            # concentrating them on the single neediest destination. V1 picks
            # "most needy" because it is the more conservative starvation
            # fix (serve whoever is furthest from being covered); a future
            # revision could make this pluggable the same way the priority
            # switch above is.
            #
            # Bounded: each iteration either ships a multiple (residual and
            # excess both strictly decrease) or proves NO lane of the chosen
            # destination can ship one at the CURRENT (monotonically
            # decreasing) excess and permanently exhausts that destination —
            # since excess never increases within the sweep, an exhausted
            # destination can never become servable again in the same sweep,
            # so the destination set shrinks or excess shrinks every
            # iteration; both are finite.
            exhausted_dests: set[str] = set()
            while True:
                avail = excess.get(source_coord, 0.0)
                if avail <= 0:
                    break
                candidates = sorted(
                    (
                        dest
                        for dest in lanes_by_dest_here
                        if dest not in exhausted_dests and residual.get((item, dest), 0.0) > 0
                    ),
                    key=lambda dest: (-residual[(item, dest)], dest),
                )
                if not candidates:
                    break
                dest_location = candidates[0]
                dest_coord = (item, dest_location)
                shipped_this_round = False
                for deficit_bucket, link in sorted(
                    lanes_by_dest_here[dest_location], key=lambda t: _sort_key(t[1])
                ):
                    avail = excess.get(source_coord, 0.0)
                    if avail <= 0:
                        break
                    remaining_max = _remaining_max(link)
                    raw_part = residual[dest_coord]  # the FULL remaining need, not a share
                    qty = _fair_share_round(
                        raw_part, link.transfer_multiple, link.min_qty, remaining_max, avail
                    )
                    if qty <= 0:
                        continue
                    fair_share_qty = min(raw_part, avail)
                    if remaining_max is not None:
                        fair_share_qty = min(fair_share_qty, remaining_max)
                    ship_bucket = max(0, deficit_bucket - link.lead_buckets)
                    arrival_bucket = ship_bucket + link.lead_buckets
                    signal = TransferSignal(
                        item=item,
                        source_location=source_location,
                        dest_location=dest_location,
                        qty=qty,
                        ship_bucket=ship_bucket,
                        arrival_bucket=arrival_bucket,
                        deficit_bucket=deficit_bucket,
                        deficit_qty=deficits[dest_coord][1],
                        source_excess_before=avail,
                        fair_share_qty=fair_share_qty,
                        rounding_remnant=fair_share_qty - qty,
                    )
                    keyed.append((item, dest_location, deficit_bucket, link, signal))
                    excess[source_coord] = avail - qty
                    residual[dest_coord] -= qty
                    shipped_by_link[id(link)] = shipped_by_link.get(id(link), 0.0) + qty
                    shipped_this_round = True
                    break
                if not shipped_this_round:
                    # No lane of the neediest destination can ship a whole
                    # multiple at the CURRENT excess — permanently exhausted
                    # for this sweep (see the bounding argument above).
                    exhausted_dests.add(dest_location)

    # Total-order output sort: (item, dest, deficit_bucket) + _sort_key(link) —
    # the SAME lane key used to pick candidates, so the final tie-break is
    # never a different rule from the selection rule (#395 PR2a review, MINOR
    # fix). source_location is _sort_key's 2nd element, matching the documented
    # (item, dest_location, deficit_bucket, priority, source_location,
    # link_ref, ...) contract exactly, with max_qty/min_qty/transfer_multiple
    # closing the residual tie when link_ref ALSO collides.
    keyed.sort(key=lambda k: (k[0], k[1], k[2]) + _sort_key(k[3]))
    return [k[4] for k in keyed]
