"""
Demand-descent split-share engine (DESC-1, ADR-043 forthcoming).

PURE, deterministic, DB-free: zero I/O, zero clock, zero randomness. Every
public function here takes plain in-memory data and returns plain in-memory
data — no ``psycopg`` connection, no ``UUID``/``datetime.now()`` anywhere in
this module. Callers (the future descent run, PR-B) are responsible for
reading the DB rows into the dataclasses below, calling this engine, and
writing the result back.

Business model
--------------
The pilot's supply chain is PLANNED at the NATIONAL level (demand and
safety stock pooled across every distribution center — a local shortage at
one center is naturally absorbed by another center's stock) but EXECUTED
per CENTER (orders are dispatched by US state to a physical distribution
center — PAT/DCW/DAL — and purchase orders are placed against whichever
center's own projection shows the need). This module computes, for every
item, the SPLIT PERCENTAGE of national demand that should route to each
eligible distribution center (DC): the "descent" the run in PR-B applies to
turn pooled national demand nodes into per-DC demand nodes, without
inventing any physical DC-level history where none exists.

Two computation paths:

* ``compute_split_shares`` — the historical path. Joins the item x US-state
  demand history to the state -> DC dispatch table (``routes``), sums by
  DC, restricts to the DCs the item is allowed to ship from (``eligibility``)
  and normalizes into percentages that sum to exactly 1 per item.
* ``equal_split_shares`` — the cold-start path. No history, or not enough of
  it: split national demand EQUALLY across the item's eligible DCs, flagged
  ``cold_start=True`` so nothing downstream mistakes it for a calibrated
  split. An item with ZERO eligible DC gets NO share at all — the demand
  stays national rather than being invented onto an arbitrary center
  (fail-loudly, per the plan's cold-start rule).

``compute_split_computation`` composes the two: every item the caller asks
for ends up with a calibrated history split, an equal-split fallback, or (if
it has no eligible DC anywhere) explicitly nowhere — never silently dropped.

Domain mirror, not shared code
-------------------------------
``pyramide/hierarchy/reconcile.py:middle_out`` disaggregates a FORECAST
CURVE from one reconciliation node down to the leaves of a hierarchy tree by
historical proportions — the same underlying idea ("split by historical
share, guarantee the sum, flag what you cannot compute") applied to a
different domain (state -> DC routing, not a summing-matrix hierarchy) and a
different data shape (scalar shares here, not per-horizon curves, and no
sparse summing matrix). This module does not import from
``pyramide/hierarchy`` and shares no code with it — only the principle.

Determinism
-----------
Every output is built by iterating SORTED keys (never bare ``dict``/``set``
iteration order) and using ``Decimal`` for every quantity and percentage.
Rounding to ``_PCT_QUANTUM`` can leave a residual after normalization; that
residual is imputed, in full, to the single largest share of the item
(ties broken by the smallest ``dc_key``) — never smeared evenly, and always
the SAME rule in both computation paths (``_normalize_with_residual``, the
one place this happens). This guarantees Sigma(pct) == 1 EXACTLY per item.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

METHOD_HISTORY = "history"
METHOD_EQUAL_SPLIT = "equal_split"

_ZERO = Decimal("0")
_ONE = Decimal("1")

# Percentage precision: eight decimal places, matching demand_split_pct.pct's
# column type (NUMERIC(9,8), migration 083) exactly — so a caller persisting
# a SplitShare never has to further round/truncate at write time. Comfortably
# inside Decimal's default 28-digit context for every sum performed here.
_PCT_QUANTUM = Decimal("0.00000001")

# Confidence saturation point for the history path (see _confidence_from_basis
# below) — a business default, deliberately generic (the module has no idea
# whether the caller's quantities are pieces, cases, or pallets). 🎯
# pilot-adjustable via the ``confidence_saturation_qty`` keyword argument.
DEFAULT_CONFIDENCE_SATURATION_QTY = Decimal("1000")


class ShareComputationError(ValueError):
    """The inputs cannot produce a well-defined set of split shares.

    Raised for structural data problems the engine refuses to paper over:
    a state routed to two different DCs, an item marked both eligible and
    ineligible for the same DC, a negative quantity. Never raised for a
    legitimate "no history" / "no eligible DC" case — those are traced in
    the return values instead (see the module docstring).
    """


# ---------------------------------------------------------------------------
# Input dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StateDemandObservation:
    """One row of the item x US-state historical demand basis.

    The pre-aggregated total demand qty observed for ``item_key`` shipped
    to/ordered from ``state_code`` over whatever lookback window the caller
    chose (the window itself is a caller concern — this module only sums
    whatever rows it is given). Multiple rows for the same
    (item_key, state_code) are legitimate (e.g. one row per period) and are
    summed by the engine, never rejected as a duplicate.
    """

    item_key: str
    state_code: str
    qty: Decimal


@dataclass(frozen=True)
class StateDcRoute:
    """One row of the state -> DC EXECUTION dispatch table (ERP-sourced).

    A US state ships from exactly one distribution center in the dispatch
    contract; ``compute_split_shares`` fails loudly (``ShareComputationError``)
    if the input data contradicts that (the same state_code mapped to two
    different dc_key values across rows) rather than silently picking one.
    Identical duplicate rows (same state_code, same dc_key) are harmless.
    """

    state_code: str
    dc_key: str


@dataclass(frozen=True)
class DcEligibility:
    """Whether ``item_key`` is allowed to ship from ``dc_key``.

    An (item_key, dc_key) pair absent from the eligibility data is treated
    as NOT eligible — the engine never invents eligibility. A pair with
    ``eligible=False`` behaves identically to an absent pair; it exists so
    an explicit "not eligible" fact can be recorded and distinguished from
    "unknown" by the caller's own data, even though this module treats both
    the same way. Conflicting rows for the same pair (one True, one False)
    raise ``ShareComputationError`` — genuine master-data corruption, not a
    condition to guess through.
    """

    item_key: str
    dc_key: str
    eligible: bool


# ---------------------------------------------------------------------------
# Output dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SplitShare:
    """One item's split percentage to one distribution center.

    ``pct`` is this DC's share of ``item_key``'s national demand, in
    [0, 1]; the shares of a given item across every DC it was computed for
    sum to EXACTLY 1 (see ``_normalize_with_residual``).

    ``basis_qty`` is the total historical qty that funded the WHOLE item's
    split (eligible-DC-routed demand, before normalization) — the same
    value on every DC row of a given item's history-derived split; 0 for an
    ``equal_split`` row (there is no history basis).

    ``confidence`` is a simple, documented function of ``basis_qty``,
    capped at 1 (see ``_confidence_from_basis``), on the history path;
    ``None`` on the ``equal_split`` cold-start path (there is nothing to be
    confident about — the split is a structural placeholder, not a
    calibrated estimate). Deliberately typed ``float`` (not ``Decimal``): a
    diagnostic score, never fed back into a downstream quantity/pct
    computation.

    ``method`` is ``METHOD_HISTORY`` or ``METHOD_EQUAL_SPLIT``.
    """

    item_key: str
    dc_key: str
    pct: Decimal
    method: str
    confidence: float | None
    basis_qty: Decimal
    cold_start: bool


@dataclass(frozen=True)
class UnroutedState:
    """A (item_key, state_code) whose historical qty could not be routed
    to any DC because ``state_code`` has no row in the state -> DC dispatch
    table.

    Traced, never silently dropped: ``compute_split_shares`` still returns
    the routable part of an item's history; this is the explainability
    record of what was excluded and why. ``qty`` is the SUM of every
    unrouted observation for that (item_key, state_code) pair.
    """

    item_key: str
    state_code: str
    qty: Decimal


@dataclass(frozen=True)
class HistorySplitResult:
    """Full output of ``compute_split_shares`` — the calibrated shares plus
    every side trace, so nothing about why an item is absent from ``shares``
    is silent.

    ``insufficient_basis_items`` lists every item that had SOME routed
    observations but could not be split from history: either because it has
    zero DCs both routed-to and eligible (its eligible-weighted total is 0),
    or because that eligible-weighted total is below ``min_history_qty``.
    Both cases are structurally identical from this function's point of
    view (there is not enough usable evidence) — the caller (typically
    ``compute_split_computation``) is expected to fall back to
    ``equal_split_shares`` for these items.
    """

    shares: tuple[SplitShare, ...]
    unrouted_states: tuple[UnroutedState, ...]
    insufficient_basis_items: tuple[str, ...]


@dataclass(frozen=True)
class EqualSplitResult:
    """Full output of ``equal_split_shares``.

    ``no_eligible_dc`` lists every requested item that has ZERO eligible DC
    — these items get NO share at all (absent from ``shares``): the demand
    stays national rather than being invented onto an arbitrary center.
    """

    shares: tuple[SplitShare, ...]
    no_eligible_dc: tuple[str, ...]


@dataclass(frozen=True)
class SplitComputation:
    """The composed result of ``compute_split_computation``: every item in
    the requested universe ends up in exactly one of three buckets —
    covered by ``shares`` (history or equal-split), or in
    ``items_without_eligible_dc`` (no share, demand stays national) — plus
    the ``unrouted_states`` explainability trace and, of the items that DID
    get a share, which ones came from the cold-start path
    (``items_cold_start``, a subset of the item_keys present in ``shares``).
    """

    shares: tuple[SplitShare, ...]
    unrouted_states: tuple[UnroutedState, ...]
    items_without_eligible_dc: tuple[str, ...]
    items_cold_start: tuple[str, ...]


# ---------------------------------------------------------------------------
# Shared normalization (the ONE place Sigma=1 is guaranteed)
# ---------------------------------------------------------------------------


def _normalize_with_residual(weights: Mapping[str, Decimal]) -> dict[str, Decimal]:
    """Normalize positive weights into percentages summing to EXACTLY 1.

    Each pct is ``weight / total``, quantized to ``_PCT_QUANTUM``. Decimal
    quantization can leave a rounding residual (the quantized percentages
    sum to slightly above or below 1); that residual is imputed, IN FULL,
    to the entry with the LARGEST raw weight, ties broken by the smallest
    ``dc_key`` — a fixed, deterministic, auditable rule (not an even smear
    across every entry, which would perturb every DC's pct instead of just
    one). This is the ONLY place either computation path adjusts a pct
    after division, so both ``compute_split_shares`` and
    ``equal_split_shares`` share one Sigma=1 guarantee.

    Requires at least one entry and a strictly positive total; both
    computation paths only call this once they know that holds (an item
    with zero total weight is routed to the cold-start / no-eligible-dc
    trace instead of reaching here).
    """
    total = sum(weights.values(), _ZERO)
    if total <= _ZERO:
        raise ShareComputationError(
            f"cannot normalize shares: total weight is {total} (must be > 0)"
        )
    quantized = {
        dc_key: (weight / total).quantize(_PCT_QUANTUM)
        for dc_key, weight in weights.items()
    }
    residual = _ONE - sum(quantized.values(), _ZERO)
    if residual != _ZERO:
        target = min(weights, key=lambda dc_key: (-weights[dc_key], dc_key))
        quantized[target] = quantized[target] + residual
    return quantized


def _confidence_from_basis(basis_qty: Decimal, saturation_qty: Decimal) -> float:
    """Simple, documented confidence function of the history depth.

    ``confidence = min(1, basis_qty / saturation_qty)`` — linear ramp from 0
    (no usable history) to 1 (``basis_qty`` at or above the saturation
    point), capped at 1 by construction. ``basis_qty`` here is always > 0
    (the caller only reaches this once the item cleared the
    ``min_history_qty`` gate), so the result is always in (0, 1].
    """
    if saturation_qty <= _ZERO:
        raise ShareComputationError(
            f"confidence_saturation_qty must be > 0, got {saturation_qty}"
        )
    ratio = basis_qty / saturation_qty
    return float(min(_ONE, ratio))


# ---------------------------------------------------------------------------
# Route / eligibility lookup builders (fail loudly on conflicting data)
# ---------------------------------------------------------------------------


def _build_route_map(routes: Sequence[StateDcRoute]) -> dict[str, str]:
    """state_code -> dc_key, failing loudly on a genuine dispatch conflict.

    Iterates ``routes`` sorted by (state_code, dc_key) so that when two
    conflicting rows exist for the same state, the error message is
    deterministic regardless of the caller's input order.
    """
    mapping: dict[str, str] = {}
    for route in sorted(routes, key=lambda r: (r.state_code, r.dc_key)):
        existing = mapping.get(route.state_code)
        if existing is not None and existing != route.dc_key:
            raise ShareComputationError(
                f"state '{route.state_code}' routes to both '{existing}' and "
                f"'{route.dc_key}' — conflicting state_to_dc dispatch data"
            )
        mapping[route.state_code] = route.dc_key
    return mapping


def _build_eligibility_map(
    eligibility: Sequence[DcEligibility],
) -> dict[str, frozenset[str]]:
    """item_key -> the frozenset of dc_keys eligible for that item.

    An (item_key, dc_key) absent from ``eligibility`` is NOT eligible (the
    engine never invents eligibility). Raises ``ShareComputationError`` on a
    genuine conflict: the same (item_key, dc_key) pair recorded as both
    eligible and not eligible.
    """
    positive: dict[str, set[str]] = {}
    negative: dict[str, set[str]] = {}
    for record in sorted(
        eligibility, key=lambda e: (e.item_key, e.dc_key, e.eligible)
    ):
        bucket = positive if record.eligible else negative
        bucket.setdefault(record.item_key, set()).add(record.dc_key)

    for item_key in sorted(set(positive) & set(negative)):
        overlap = positive[item_key] & negative[item_key]
        if overlap:
            dc_key = min(overlap)
            raise ShareComputationError(
                f"item '{item_key}' has conflicting eligibility for dc "
                f"'{dc_key}' (both eligible=True and eligible=False)"
            )
    return {item_key: frozenset(dcs) for item_key, dcs in positive.items()}


# ---------------------------------------------------------------------------
# Public computation
# ---------------------------------------------------------------------------


def compute_split_shares(
    observations: Sequence[StateDemandObservation],
    routes: Sequence[StateDcRoute],
    eligibility: Sequence[DcEligibility],
    *,
    min_history_qty: Decimal = Decimal("0"),
    confidence_saturation_qty: Decimal = DEFAULT_CONFIDENCE_SATURATION_QTY,
) -> HistorySplitResult:
    """Historical split shares: item x state history -> item x DC pct.

    Algorithm, per item:
      1. Route every observation to its DC via ``routes`` (state -> DC).
         A state with no route is excluded from the sum and traced as an
         ``UnroutedState`` instead — never silently dropped.
      2. Sum routed qty by DC, then restrict to the item's ELIGIBLE DCs
         (``eligibility``). An ineligible DC's weight is thereby excluded
         from the item's split rather than "redistributed" as a separate
         step: this is mathematically IDENTICAL to redistributing it
         proportionally across the eligible DCs first — for weights
         w_1..w_n (eligible, total W) and an excluded weight x, giving each
         w_i a share x * w_i / W before renormalizing yields the exact same
         final percentage w_i / W as simply dropping x up front (the
         renormalization cancels the redistribution). Dropping is the
         simpler implementation of the identical result.
      3. If the item's total eligible-routed qty is 0 (no eligible DC ever
         received routed demand — including "zero eligible DC at all") or
         below ``min_history_qty``, the item is EXCLUDED from ``shares``
         and listed in ``insufficient_basis_items`` — not enough usable
         evidence for a calibrated split (fall back to
         ``equal_split_shares``).
      4. Otherwise, normalize to percentages (Sigma=1 exactly — see
         ``_normalize_with_residual``), method=``METHOD_HISTORY``,
         confidence = ``_confidence_from_basis`` of the item's total
         eligible-routed qty, cold_start=False.

    Only items present in ``observations`` are considered — an item with no
    history row at all is invisible to this function by construction (it is
    the caller's job, typically ``compute_split_computation``, to route
    such items to ``equal_split_shares``).

    Raises ``ShareComputationError`` on conflicting route/eligibility data
    or a negative observation qty.
    """
    if min_history_qty < _ZERO:
        raise ShareComputationError(
            f"min_history_qty must be >= 0, got {min_history_qty}"
        )
    route_by_state = _build_route_map(routes)
    eligible_by_item = _build_eligibility_map(eligibility)

    routed_weight: dict[str, dict[str, Decimal]] = {}
    unrouted_qty: dict[tuple[str, str], Decimal] = {}

    for obs in observations:
        if obs.qty < _ZERO:
            raise ShareComputationError(
                f"observation for item '{obs.item_key}' state "
                f"'{obs.state_code}' has negative qty ({obs.qty}) — history "
                "is only defined on non-negative demand"
            )
        dc_key = route_by_state.get(obs.state_code)
        if dc_key is None:
            trace_key = (obs.item_key, obs.state_code)
            unrouted_qty[trace_key] = unrouted_qty.get(trace_key, _ZERO) + obs.qty
            continue
        item_weights = routed_weight.setdefault(obs.item_key, {})
        item_weights[dc_key] = item_weights.get(dc_key, _ZERO) + obs.qty

    unrouted_states = tuple(
        UnroutedState(item_key=item_key, state_code=state_code, qty=qty)
        for (item_key, state_code), qty in sorted(unrouted_qty.items())
    )

    shares: list[SplitShare] = []
    insufficient_basis_items: list[str] = []

    for item_key in sorted(routed_weight):
        raw_weights = routed_weight[item_key]
        eligible_dcs = eligible_by_item.get(item_key, frozenset())
        # Strictly positive weight only: a DC that is eligible but never
        # actually received any routed qty contributes no evidence. (This
        # filter alone does NOT rule out a 0% share — a positive-but-tiny
        # weight can still quantize to zero; the re-normalization loop
        # below is what guarantees pct > 0 on every emitted share.)
        eligible_weights = {
            dc_key: weight
            for dc_key, weight in raw_weights.items()
            if dc_key in eligible_dcs and weight > _ZERO
        }
        total_eligible = sum(eligible_weights.values(), _ZERO)
        if total_eligible <= _ZERO or total_eligible < min_history_qty:
            insufficient_basis_items.append(item_key)
            continue

        pct_by_dc = _normalize_with_residual(eligible_weights)
        # A minuscule weight facing a huge one can quantize to 0.00000000
        # at 8 decimals (e.g. 1 vs 1e9). A zero share would violate
        # demand_split_pct's CHECK (pct > 0) downstream AND silently orphan
        # that DC's sliver of demand at persist time. Instead: drop the
        # vanishing DC(s) from the weight set and re-normalize over the
        # survivors — their < 5e-9 slice of demand is re-absorbed
        # proportionally, and Σ=1 exact is preserved by
        # _normalize_with_residual at every pass. Terminates: each pass
        # removes at least one DC; a single survivor gets pct=1.
        while any(pct == _ZERO for pct in pct_by_dc.values()):
            eligible_weights = {
                dc_key: weight
                for dc_key, weight in eligible_weights.items()
                if pct_by_dc[dc_key] > _ZERO
            }
            pct_by_dc = _normalize_with_residual(eligible_weights)
        confidence = _confidence_from_basis(total_eligible, confidence_saturation_qty)
        for dc_key in sorted(pct_by_dc):
            shares.append(
                SplitShare(
                    item_key=item_key,
                    dc_key=dc_key,
                    pct=pct_by_dc[dc_key],
                    method=METHOD_HISTORY,
                    confidence=confidence,
                    basis_qty=total_eligible,
                    cold_start=False,
                )
            )

    return HistorySplitResult(
        shares=tuple(shares),
        unrouted_states=unrouted_states,
        insufficient_basis_items=tuple(sorted(insufficient_basis_items)),
    )


def equal_split_shares(
    item_keys: Sequence[str],
    eligibility: Sequence[DcEligibility],
) -> EqualSplitResult:
    """Cold-start split shares: equal pct across each item's eligible DCs.

    For every DISTINCT item in ``item_keys`` (duplicates are harmless —
    this is a set of items to cold-start, not an ordered sequence):
      * zero eligible DC -> the item gets NO share; it is listed in
        ``no_eligible_dc`` instead (fail-loudly: demand stays national
        rather than being invented onto an arbitrary center);
      * N >= 1 eligible DCs -> each gets pct = 1/N (Sigma=1 exactly — see
        ``_normalize_with_residual``, same residual-imputation rule as the
        history path), method=``METHOD_EQUAL_SPLIT``, confidence=None
        (nothing calibrated backs an equal split), basis_qty=0,
        cold_start=True.

    Raises ``ShareComputationError`` on conflicting eligibility data.
    """
    eligible_by_item = _build_eligibility_map(eligibility)

    shares: list[SplitShare] = []
    no_eligible_dc: list[str] = []

    for item_key in sorted(set(item_keys)):
        eligible_dcs = eligible_by_item.get(item_key, frozenset())
        if not eligible_dcs:
            no_eligible_dc.append(item_key)
            continue

        weights = {dc_key: _ONE for dc_key in eligible_dcs}
        pct_by_dc = _normalize_with_residual(weights)
        for dc_key in sorted(pct_by_dc):
            shares.append(
                SplitShare(
                    item_key=item_key,
                    dc_key=dc_key,
                    pct=pct_by_dc[dc_key],
                    method=METHOD_EQUAL_SPLIT,
                    confidence=None,
                    basis_qty=_ZERO,
                    cold_start=True,
                )
            )

    return EqualSplitResult(
        shares=tuple(shares),
        no_eligible_dc=tuple(sorted(no_eligible_dc)),
    )


def compute_split_computation(
    item_keys: Sequence[str],
    observations: Sequence[StateDemandObservation],
    routes: Sequence[StateDcRoute],
    eligibility: Sequence[DcEligibility],
    *,
    min_history_qty: Decimal = Decimal("0"),
    confidence_saturation_qty: Decimal = DEFAULT_CONFIDENCE_SATURATION_QTY,
) -> SplitComputation:
    """Full descent computation for the requested item universe.

    Composes ``compute_split_shares`` and ``equal_split_shares`` so every
    item in ``item_keys`` ends up in exactly one of two places: covered by
    a share (history-calibrated or equal-split cold-start) or listed in
    ``items_without_eligible_dc`` (no share — demand stays national). An
    item outside ``item_keys`` that nonetheless has routable history in
    ``observations`` still produces a valid history share (this function
    does not filter ``compute_split_shares``' output down to
    ``item_keys`` — a caller reusing a broader observations dataset across
    several item universes gets every item it has evidence for, not just
    the ones it explicitly listed).

    Steps:
      1. Run ``compute_split_shares`` over ``observations``/``routes``/
         ``eligibility`` — the calibrated history path.
      2. Every requested item NOT covered by a history share (no history at
         all, or excluded for insufficient basis) falls back to
         ``equal_split_shares``.
      3. Merge: ``shares`` = history shares + equal-split shares, sorted by
         (item_key, dc_key). ``items_cold_start`` = the item_keys that
         landed in the equal-split path successfully (a subset of the
         fallback items — the ones that DID have >= 1 eligible DC).
         ``items_without_eligible_dc`` = the fallback items that had NONE.
         ``unrouted_states`` is passed through unchanged from step 1 (it is
         a property of ``observations``, independent of ``item_keys``).
    """
    history = compute_split_shares(
        observations,
        routes,
        eligibility,
        min_history_qty=min_history_qty,
        confidence_saturation_qty=confidence_saturation_qty,
    )
    covered = {share.item_key for share in history.shares}
    fallback_items = sorted(set(item_keys) - covered)
    equal = equal_split_shares(fallback_items, eligibility)

    items_cold_start = tuple(sorted({share.item_key for share in equal.shares}))
    all_shares = tuple(
        sorted(history.shares + equal.shares, key=lambda s: (s.item_key, s.dc_key))
    )

    return SplitComputation(
        shares=all_shares,
        unrouted_states=history.unrouted_states,
        items_without_eligible_dc=equal.no_eligible_dc,
        items_cold_start=items_cold_start,
    )
