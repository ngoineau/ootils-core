"""Property-based exactness net for the demand-descent split-share engine
(engine/descent/shares.py) — moteur-c1 C1.

The module's headline guarantee is Σ(pct) == 1 EXACTLY per item, held together
by a single residual-imputation rule (_normalize_with_residual). Fixed goldens
check that guarantee on a handful of hand-picked weight vectors; the properties
below assert it across thousands of magnitude mixes (1e-6 .. 1e9) — precisely
the regime where the 8-decimal rounding residual, the vanishing-share drop, and
the tie-break in the residual target actually bite.

All tests are pure (no DB, no clock). Determinism in CI comes from the 'ci'
Hypothesis profile (tests/conftest_hypothesis.py).
"""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from ootils_core.engine.descent.shares import (
    DcEligibility,
    StateDcRoute,
    StateDemandObservation,
    _normalize_with_residual,
    _PCT_QUANTUM,
    compute_split_shares,
    equal_split_shares,
)

# Positive Decimal weights spanning the full magnitude band the module claims to
# handle. The 1e-6 floor keeps every weight strictly positive (so the total is
# always > 0 and _normalize_with_residual never rejects the input); the 1e9
# ceiling is where a tiny share facing a huge one quantizes to 0.00000000.
positive_qty = st.decimals(
    min_value=Decimal("0.000001"),
    max_value=Decimal("1000000000"),
    places=6,
    allow_nan=False,
    allow_infinity=False,
)

_DC_POOL = [f"DC{i}" for i in range(8)]
_dc_key = st.sampled_from(_DC_POOL)
weight_dicts = st.dictionaries(keys=_dc_key, values=positive_qty, min_size=1, max_size=8)

_DCS = [f"D{i}" for i in range(4)]
_STATES = [f"S{i}" for i in range(6)]
_ITEMS = [f"I{i}" for i in range(3)]


@st.composite
def _weights_and_permutation(draw: st.DrawFn) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    weights = draw(weight_dicts)
    reordered = dict(draw(st.permutations(list(weights.items()))))
    return weights, reordered


@st.composite
def _weights_with_removable_key(draw: st.DrawFn) -> tuple[dict[str, Decimal], str]:
    weights = draw(st.dictionaries(_dc_key, positive_qty, min_size=2, max_size=8))
    victim = draw(st.sampled_from(sorted(weights)))
    return weights, victim


@st.composite
def _history_scenario(
    draw: st.DrawFn,
) -> tuple[list[StateDemandObservation], list[StateDcRoute], list[DcEligibility]]:
    # Routes: each state maps to EXACTLY one DC (built from a dict), so a route
    # conflict is impossible by construction — we test the numeric guarantees,
    # not the fail-loudly guards (those have their own unit coverage).
    route_map = {s: draw(st.sampled_from(_DCS)) for s in _STATES}
    routes = [StateDcRoute(state_code=s, dc_key=d) for s, d in route_map.items()]

    # Eligibility: only eligible=True rows, so no True/False conflict for a pair.
    eligibility: list[DcEligibility] = []
    for item in _ITEMS:
        elig = draw(st.lists(st.sampled_from(_DCS), unique=True, max_size=len(_DCS)))
        eligibility.extend(DcEligibility(item_key=item, dc_key=d, eligible=True) for d in elig)

    raw_obs = draw(
        st.lists(
            st.tuples(st.sampled_from(_ITEMS), st.sampled_from(_STATES), positive_qty),
            min_size=1,
            max_size=20,
        )
    )
    observations = [
        StateDemandObservation(item_key=i, state_code=s, qty=q) for i, s, q in raw_obs
    ]
    return observations, routes, eligibility


@st.composite
def _equal_split_scenario(draw: st.DrawFn) -> tuple[list[str], list[DcEligibility]]:
    items = draw(st.lists(st.sampled_from(_ITEMS), min_size=1, max_size=3, unique=True))
    eligibility: list[DcEligibility] = []
    for item in items:
        elig = draw(st.lists(st.sampled_from(_DCS), unique=True, min_size=1, max_size=len(_DCS)))
        eligibility.extend(DcEligibility(item_key=item, dc_key=d, eligible=True) for d in elig)
    return items, eligibility


@given(weight_dicts)
def test_normalize_sum_is_exactly_one(weights: dict[str, Decimal]) -> None:
    """CATCHES: a residual-imputation regression a fixed golden misses. The
    quantized shares of an ARBITRARY positive weight vector must sum to
    Decimal('1') EXACTLY — not 0.99999999 or 1.00000001. Goldens only check a
    few curated vectors; this asserts the invariant across thousands of
    magnitude mixes (1e-6 .. 1e9) where the 8-dp rounding residual really
    appears and has to be re-absorbed."""
    result = _normalize_with_residual(weights)
    assert sum(result.values(), Decimal("0")) == Decimal("1")


@given(_weights_and_permutation())
def test_normalize_is_permutation_invariant(
    pair: tuple[dict[str, Decimal], dict[str, Decimal]],
) -> None:
    """CATCHES: order-dependence in the residual tie-break that goldens never
    reveal (they fix one input order). The residual is imputed to the max
    weight, ties broken by smallest dc_key; if that selection ever leaked
    dict-iteration order or used a non-total tie-break, the SAME weights fed in
    a different insertion order would produce a different vector."""
    weights, reordered = pair
    assert _normalize_with_residual(weights) == _normalize_with_residual(reordered)


@given(_weights_with_removable_key())
def test_renormalize_after_dc_removal_still_sums_one(
    pair: tuple[dict[str, Decimal], str],
) -> None:
    """CATCHES: a renormalization that forgets to RE-impute the residual after
    the weight set shrinks — the module's real 'a DC drops out' path (the
    vanishing-share while-loop, the equal-split renorm) that a static golden
    cannot parametrize. Remove ANY one DC; the surviving shares must STILL sum
    to exactly 1."""
    weights, victim = pair
    survivors = {k: v for k, v in weights.items() if k != victim}
    result = _normalize_with_residual(survivors)
    assert sum(result.values(), Decimal("0")) == Decimal("1")


@given(weight_dicts)
def test_residual_imputed_to_a_single_share(weights: dict[str, Decimal]) -> None:
    """CATCHES: a residual SMEARED across entries instead of landing on exactly
    one (the module's documented, auditable rule). Reconstruct the pre-residual
    quantization and assert AT MOST ONE key was adjusted, and that it is the
    largest-weight key (ties: smallest dc_key). A golden checks the final vector
    for one input; it never checks WHICH entry absorbed the residual."""
    total = sum(weights.values(), Decimal("0"))
    quantized = {k: (v / total).quantize(_PCT_QUANTUM) for k, v in weights.items()}
    result = _normalize_with_residual(weights)
    diffs = [k for k in weights if result[k] != quantized[k]]
    assert len(diffs) <= 1
    if diffs:
        expected_target = min(weights, key=lambda k: (-weights[k], k))
        assert diffs[0] == expected_target


@given(_history_scenario())
def test_history_shares_strictly_positive(
    scenario: tuple[list[StateDemandObservation], list[StateDcRoute], list[DcEligibility]],
) -> None:
    """CATCHES: a persisted share that would violate demand_split_pct's pct > 0
    CHECK downstream — a case NO fixed golden exercises. A tiny weight facing a
    huge one quantizes to 0.00000000 at 8 dp; the vanishing-DC re-normalization
    loop must remove it so every EMITTED share is strictly in (0, 1].
    Randomized 1e-6..1e9 mixes routinely produce that near-zero share; a single
    golden almost never does."""
    observations, routes, eligibility = scenario
    result = compute_split_shares(observations, routes, eligibility)
    for share in result.shares:
        assert share.pct > Decimal("0")
        assert share.pct <= Decimal("1")


@given(_history_scenario())
def test_history_shares_sum_to_one_per_item(
    scenario: tuple[list[StateDemandObservation], list[StateDcRoute], list[DcEligibility]],
) -> None:
    """CATCHES: an item whose DC shares silently fail to close to 1 through the
    FULL public path (routing + eligibility filter + normalization + vanishing
    drop), not just the isolated normalizer. Every item that appears in the
    output must have its shares sum to exactly 1."""
    observations, routes, eligibility = scenario
    result = compute_split_shares(observations, routes, eligibility)
    by_item: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for share in result.shares:
        by_item[share.item_key] += share.pct
    for total in by_item.values():
        assert total == Decimal("1")


@given(_history_scenario(), st.data())
def test_history_split_independent_of_input_order(
    scenario: tuple[list[StateDemandObservation], list[StateDcRoute], list[DcEligibility]],
    data: st.DataObject,
) -> None:
    """CATCHES: any dependence of the calibrated split on the caller's input
    list order (a claim the module makes but a golden cannot verify with one
    fixed ordering). Shuffle observations/routes/eligibility; the emitted
    shares tuple must be byte-identical."""
    observations, routes, eligibility = scenario
    baseline = compute_split_shares(observations, routes, eligibility)
    shuffled = compute_split_shares(
        list(data.draw(st.permutations(observations))),
        list(data.draw(st.permutations(routes))),
        list(data.draw(st.permutations(eligibility))),
    )
    assert baseline.shares == shuffled.shares


@given(_equal_split_scenario())
def test_equal_split_sum_one_and_positive(
    scenario: tuple[list[str], list[DcEligibility]],
) -> None:
    """CATCHES: a cold-start (1/N) split that drifts off Σ=1 or emits a
    zero/negative share. The equal-split path reaches the SAME residual
    guarantee via a different construction than the history path; goldens
    rarely cover both. Asserts Σ=1 exact and pct > 0 for N in 1..4 across every
    eligible-DC subset."""
    items, eligibility = scenario
    result = equal_split_shares(items, eligibility)
    by_item: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for share in result.shares:
        assert share.pct > Decimal("0")
        by_item[share.item_key] += share.pct
    for item in {s.item_key for s in result.shares}:
        assert by_item[item] == Decimal("1")
