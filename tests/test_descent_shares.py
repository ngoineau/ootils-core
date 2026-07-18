"""
tests/test_descent_shares.py — hand-written golden-master tests of the PURE
demand-descent split-share engine (`engine/descent/shares.py`, DESC-1 PR-A,
ADR-043).

Same discipline as tests/test_drp_core_golden.py / test_mrp_core_golden.py:
a tiny hand-computed dataset whose expected outputs are derived STEP BY STEP
in the comments BEFORE running — none is copied back from an execution. If
the engine ever disagrees with a derivation here, the derivation is the
contract and the divergence is a bug to investigate, not a golden to "fix".

The module is DB-free and deterministic by contract (module docstring), so
these tests run with no database, no fixtures, no clock.

Rounding arithmetic used in the derivations below (Decimal default context,
prec=28, quantize to 8 dp with ROUND_HALF_EVEN):
    1/3 = 0.333333…33  -> 0.33333333   (9th digit 3, rounds down)
    2/3 = 0.666666…67  -> 0.66666667   (9th digit 6, rounds up)
    1/6 = 0.166666…67  -> 0.16666667   (9th digit 6, rounds up)
    1/7 = 0.142857142… -> 0.14285714   (9th digit 2, rounds down)
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from ootils_core.engine.descent.shares import (
    DEFAULT_CONFIDENCE_SATURATION_QTY,
    METHOD_EQUAL_SPLIT,
    METHOD_HISTORY,
    DcEligibility,
    ShareComputationError,
    StateDcRoute,
    StateDemandObservation,
    UnroutedState,
    compute_split_computation,
    compute_split_shares,
    equal_split_shares,
)

D = Decimal
ONE = D("1")


# ---------------------------------------------------------------------------
# Input builders (sugar only — no logic, no derived numbers)
# ---------------------------------------------------------------------------


def _obs(item: str, state: str, qty: str) -> StateDemandObservation:
    return StateDemandObservation(item_key=item, state_code=state, qty=D(qty))


def _route(state: str, dc: str) -> StateDcRoute:
    return StateDcRoute(state_code=state, dc_key=dc)


def _elig(item: str, dc: str, eligible: bool = True) -> DcEligibility:
    return DcEligibility(item_key=item, dc_key=dc, eligible=eligible)


def _by_dc(shares, item: str) -> dict:
    """dc_key -> SplitShare for one item."""
    return {s.dc_key: s for s in shares if s.item_key == item}


def _sigma(shares, item: str) -> Decimal:
    return sum((s.pct for s in shares if s.item_key == item), D("0"))


# The standing dispatch table for most cases: CA->PAT, TX->DCW, NY->DAL.
ROUTES = [_route("CA", "PAT"), _route("TX", "DCW"), _route("NY", "DAL")]


# ===========================================================================
# 1. Sigma = 1 EXACT per item — including the vicious rounding cases
# ===========================================================================


class TestSigmaExactlyOne:
    def test_clean_60_40_split_and_duplicate_rows_summed(self):
        """Item A: CA history 30 + 30 (two rows, summed — never rejected as a
        duplicate) -> PAT weight 60; TX 40 -> DCW weight 40. Total 100.
        pct(PAT) = 60/100 = 0.60000000, pct(DCW) = 40/100 = 0.40000000 —
        both exact, no residual. basis_qty = 100 on BOTH rows (the whole
        item's eligible basis, not the per-DC weight)."""
        result = compute_split_shares(
            [_obs("A", "CA", "30"), _obs("A", "CA", "30"), _obs("A", "TX", "40")],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DCW")],
        )
        by_dc = _by_dc(result.shares, "A")
        assert set(by_dc) == {"PAT", "DCW"}
        assert by_dc["PAT"].pct == D("0.60000000")
        assert by_dc["DCW"].pct == D("0.40000000")
        assert _sigma(result.shares, "A") == ONE
        for share in by_dc.values():
            assert share.method == METHOD_HISTORY
            assert share.cold_start is False
            assert share.basis_qty == D("100")
        assert result.unrouted_states == ()
        assert result.insufficient_basis_items == ()

    def test_vicious_third_third_third_residual_to_smallest_dc_key(self):
        """The 1/3-1/3-1/3 case. Item A, qty 1 to each of CA/TX/NY, all
        three DCs eligible. Raw pct each = 1/3 -> quantized 0.33333333;
        sum = 0.99999999, residual = +0.00000001. All raw weights tie (1),
        so the residual goes IN FULL to the smallest dc_key: DAL < DCW <
        PAT -> DAL gets 0.33333334, the others stay 0.33333333.
        Sigma = 0.33333334 + 0.33333333 + 0.33333333 = 1 EXACTLY."""
        result = compute_split_shares(
            [_obs("A", "CA", "1"), _obs("A", "TX", "1"), _obs("A", "NY", "1")],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DCW"), _elig("A", "DAL")],
        )
        by_dc = _by_dc(result.shares, "A")
        assert by_dc["DAL"].pct == D("0.33333334")
        assert by_dc["DCW"].pct == D("0.33333333")
        assert by_dc["PAT"].pct == D("0.33333333")
        assert _sigma(result.shares, "A") == ONE

    def test_two_thirds_one_third_needs_no_residual(self):
        """Item A: CA 2 -> PAT, TX 1 -> DCW. 2/3 -> 0.66666667 (rounds UP),
        1/3 -> 0.33333333 (rounds DOWN); the two roundings cancel and the
        quantized sum is already exactly 1 — the residual rule must NOT
        fire (both values stay untouched)."""
        result = compute_split_shares(
            [_obs("A", "CA", "2"), _obs("A", "TX", "1")],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DCW")],
        )
        by_dc = _by_dc(result.shares, "A")
        assert by_dc["PAT"].pct == D("0.66666667")
        assert by_dc["DCW"].pct == D("0.33333333")
        assert _sigma(result.shares, "A") == ONE

    def test_negative_residual_lands_on_largest_weight(self):
        """Weights PAT=4 (CA), DCW=1 (TX), DAL=1 (NY), total 6.
        4/6 -> 0.66666667 (up), 1/6 -> 0.16666667 (up) twice.
        Quantized sum = 1.00000001 -> residual = -0.00000001, imputed to
        the single LARGEST raw weight (PAT, no tie): PAT = 0.66666666.
        The two small shares stay exactly 0.16666667. Sigma = 1 EXACTLY."""
        result = compute_split_shares(
            [_obs("A", "CA", "4"), _obs("A", "TX", "1"), _obs("A", "NY", "1")],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DCW"), _elig("A", "DAL")],
        )
        by_dc = _by_dc(result.shares, "A")
        assert by_dc["PAT"].pct == D("0.66666666")
        assert by_dc["DCW"].pct == D("0.16666667")
        assert by_dc["DAL"].pct == D("0.16666667")
        assert _sigma(result.shares, "A") == ONE

    def test_vanishing_weight_dropped_and_renormalized_never_zero_pct(self):
        """Weights PAT=1e9 (CA), DCW=1 (TX). 1/1000000001 = 9.99…e-10 ->
        quantizes to 0.00000000 at 8 dp. A zero share would violate
        demand_split_pct's CHECK (pct > 0) at persist time — the engine
        must instead DROP the vanishing DC and re-normalize the survivors:
        PAT alone -> pct = 1 exactly. No share row may ever carry pct=0,
        and Sigma stays exactly 1 (PR-A review finding, 2026-07-18)."""
        result = compute_split_shares(
            [_obs("A", "CA", "1000000000"), _obs("A", "TX", "1")],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DCW")],
        )
        by_dc = _by_dc(result.shares, "A")
        assert set(by_dc) == {"PAT"}
        assert by_dc["PAT"].pct == ONE
        assert all(s.pct > D("0") for s in result.shares)
        assert _sigma(result.shares, "A") == ONE

    def test_vanishing_weight_among_three_renormalizes_survivors(self):
        """Weights PAT=1e9 (CA), DCW=1e9 (TX), DAL=1 (NY). DAL quantizes to
        0.00000000 -> dropped; survivors re-normalize to 1/2 each EXACTLY
        (0.50000000 + 0.50000000 = 1, no residual needed)."""
        result = compute_split_shares(
            [
                _obs("A", "CA", "1000000000"),
                _obs("A", "TX", "1000000000"),
                _obs("A", "NY", "1"),
            ],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DCW"), _elig("A", "DAL")],
        )
        by_dc = _by_dc(result.shares, "A")
        assert set(by_dc) == {"PAT", "DCW"}
        assert by_dc["PAT"].pct == D("0.50000000")
        assert by_dc["DCW"].pct == D("0.50000000")
        assert _sigma(result.shares, "A") == ONE

    def test_equal_split_seven_dcs_residual_to_smallest_dc_key(self):
        """Cold-start sibling of the 1/3 case: 7 eligible DCs. 1/7 ->
        0.14285714 (rounds down); x7 = 0.99999998 -> residual +0.00000002
        imputed IN FULL (never smeared) to the smallest dc_key, DC1:
        0.14285716. The other six stay 0.14285714. Sigma = 1 EXACTLY."""
        dcs = [f"DC{i}" for i in range(1, 8)]
        result = equal_split_shares(["A"], [_elig("A", dc) for dc in dcs])
        by_dc = _by_dc(result.shares, "A")
        assert by_dc["DC1"].pct == D("0.14285716")
        for dc in dcs[1:]:
            assert by_dc[dc].pct == D("0.14285714")
        assert _sigma(result.shares, "A") == ONE


# ===========================================================================
# 2. Determinism — identical calls, scrambled input orders
# ===========================================================================


def _zoo(obs_order, route_order, elig_order, item_order):
    """One full-computation zoo covering every bucket at once:
      ALPHA   — history split (CA 60 -> PAT, TX 40 -> DCW);
      BRAVO   — only unrouted history ('PR' has no route) -> cold start;
      CHARLIE — no history, no eligibility -> no share at all;
      DELTA   — history routed ONLY to an ineligible DC (NY -> DAL,
                eligible=False) but eligible at PAT -> insufficient basis
                -> cold start on PAT."""
    observations = [
        _obs("ALPHA", "CA", "60"),
        _obs("ALPHA", "TX", "40"),
        _obs("BRAVO", "PR", "12"),
        _obs("DELTA", "NY", "9"),
    ]
    routes = list(ROUTES)
    eligibility = [
        _elig("ALPHA", "PAT"),
        _elig("ALPHA", "DCW"),
        _elig("BRAVO", "PAT"),
        _elig("BRAVO", "DCW"),
        _elig("DELTA", "PAT"),
        _elig("DELTA", "DAL", eligible=False),
    ]
    items = ["ALPHA", "BRAVO", "CHARLIE", "DELTA"]
    return compute_split_computation(
        [items[i] for i in item_order],
        [observations[i] for i in obs_order],
        [routes[i] for i in route_order],
        [eligibility[i] for i in elig_order],
    )


class TestDeterminism:
    IDENTITY = (range(4), range(3), range(6), range(4))
    # Fixed scrambles (no randomness in a determinism test): reversed, and a
    # hand-interleaved order. Same multisets, different sequence orders.
    REVERSED = (
        [3, 2, 1, 0],
        [2, 1, 0],
        [5, 4, 3, 2, 1, 0],
        [3, 2, 1, 0],
    )
    INTERLEAVED = (
        [2, 0, 3, 1],
        [1, 2, 0],
        [4, 0, 5, 1, 3, 2],
        [1, 3, 0, 2],
    )

    def test_two_identical_calls_give_identical_results(self):
        assert _zoo(*self.IDENTITY) == _zoo(*self.IDENTITY)

    @pytest.mark.parametrize("order", [REVERSED, INTERLEAVED], ids=["reversed", "interleaved"])
    def test_scrambled_input_orders_give_identical_results(self, order):
        """Every output (shares, traces, orderings inside the tuples) must be
        byte-identical whatever order the caller supplies rows in — sorted
        iteration is the module's stated contract."""
        assert _zoo(*order) == _zoo(*self.IDENTITY)

    def test_zoo_buckets_are_the_hand_derived_partition(self):
        """Pin the zoo itself so the determinism tests compare something real:
        ALPHA history 0.6/0.4; BRAVO cold-start 0.5/0.5; CHARLIE nowhere;
        DELTA cold-start pct 1 on PAT. Sigma = 1 for every item present."""
        result = _zoo(*self.IDENTITY)
        alpha = _by_dc(result.shares, "ALPHA")
        assert alpha["PAT"].pct == D("0.60000000")
        assert alpha["DCW"].pct == D("0.40000000")
        assert alpha["PAT"].method == METHOD_HISTORY
        bravo = _by_dc(result.shares, "BRAVO")
        assert bravo["PAT"].pct == D("0.50000000")
        assert bravo["DCW"].pct == D("0.50000000")
        assert all(s.cold_start for s in bravo.values())
        delta = _by_dc(result.shares, "DELTA")
        assert set(delta) == {"PAT"}
        assert delta["PAT"].pct == ONE
        assert delta["PAT"].cold_start is True
        assert result.items_without_eligible_dc == ("CHARLIE",)
        assert result.items_cold_start == ("BRAVO", "DELTA")
        assert result.unrouted_states == (
            UnroutedState(item_key="BRAVO", state_code="PR", qty=D("12")),
        )
        for item in ("ALPHA", "BRAVO", "DELTA"):
            assert _sigma(result.shares, item) == ONE


# ===========================================================================
# 3. Ineligible DC — weight redistributed proportionally to eligible DCs
# ===========================================================================


class TestIneligibleRedistribution:
    def _run(self, dal_shape: str):
        """Item A routed weights PAT=60 (CA), DCW=30 (TX), DAL=10 (NY); DAL
        NOT eligible — either absent from the eligibility data or recorded
        as an explicit eligible=False row (the two must behave identically
        per the DcEligibility contract)."""
        eligibility = [_elig("A", "PAT"), _elig("A", "DCW")]
        if dal_shape == "explicit_false":
            eligibility.append(_elig("A", "DAL", eligible=False))
        return compute_split_shares(
            [_obs("A", "CA", "60"), _obs("A", "TX", "30"), _obs("A", "NY", "10")],
            ROUTES,
            eligibility,
        )

    @pytest.mark.parametrize("dal_shape", ["absent", "explicit_false"])
    def test_ineligible_weight_redistributed_proportionally(self, dal_shape):
        """Hand derivation — redistribution equivalence:
        redistributing DAL's 10 proportionally over the eligible weights
        (PAT 60 + 10*60/90 = 66.666…, DCW 30 + 10*30/90 = 33.333…, /100)
        gives the SAME final pcts as dropping the 10 up front (60/90, 30/90):
          pct(PAT) = 60/90 = 2/3 -> 0.66666667
          pct(DCW) = 30/90 = 1/3 -> 0.33333333    (sum exactly 1, no residual)
        basis_qty = 90 — the ELIGIBLE basis, never the raw 100."""
        result = self._run(dal_shape)
        by_dc = _by_dc(result.shares, "A")
        assert set(by_dc) == {"PAT", "DCW"}, "DAL must get no share row"
        assert by_dc["PAT"].pct == D("0.66666667")
        assert by_dc["DCW"].pct == D("0.33333333")
        assert _sigma(result.shares, "A") == ONE
        assert by_dc["PAT"].basis_qty == D("90")
        assert by_dc["DCW"].basis_qty == D("90")

    def test_explicit_false_identical_to_absent(self):
        """The whole result objects must match, not just the pcts."""
        assert self._run("absent") == self._run("explicit_false")


# ===========================================================================
# 4. Unrouted state — traced, never silent
# ===========================================================================


class TestUnroutedTrace:
    def test_unrouted_qty_traced_and_summed_routable_part_still_split(self):
        """Item A: CA 60 (routed -> PAT) plus TWO 'PR' rows (5 and 7 — 'PR'
        has no dispatch row). The 12 unrouted units appear as ONE summed
        UnroutedState trace; the routable 60 still yields a full split
        (single DC -> pct exactly 1, basis 60 — the unrouted 12 never
        contaminates the basis)."""
        result = compute_split_shares(
            [_obs("A", "CA", "60"), _obs("A", "PR", "5"), _obs("A", "PR", "7")],
            ROUTES,
            [_elig("A", "PAT")],
        )
        assert result.unrouted_states == (
            UnroutedState(item_key="A", state_code="PR", qty=D("12")),
        )
        by_dc = _by_dc(result.shares, "A")
        assert set(by_dc) == {"PAT"}
        assert by_dc["PAT"].pct == ONE
        assert by_dc["PAT"].basis_qty == D("60")

    def test_only_unrouted_history_falls_back_cold_start_with_trace(self):
        """An item whose ENTIRE history is unrouted has no usable evidence:
        in the composed computation it must (a) keep its unrouted trace AND
        (b) land in the cold-start bucket — never silently vanish."""
        result = compute_split_computation(
            ["A"],
            [_obs("A", "PR", "8")],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DCW")],
        )
        assert result.unrouted_states == (
            UnroutedState(item_key="A", state_code="PR", qty=D("8")),
        )
        by_dc = _by_dc(result.shares, "A")
        assert by_dc["PAT"].pct == D("0.50000000")
        assert by_dc["DCW"].pct == D("0.50000000")
        assert result.items_cold_start == ("A",)


# ===========================================================================
# 5. Cold start — equal split, explicitly flagged
# ===========================================================================


class TestColdStartFlagged:
    def test_equal_split_two_dcs_fully_flagged(self):
        """1/2 each, and every flag distinguishes it from a calibrated split:
        method equal_split, cold_start True, confidence None (nothing to be
        confident about), basis_qty 0 (no history basis)."""
        result = equal_split_shares(["A"], [_elig("A", "PAT"), _elig("A", "DCW")])
        by_dc = _by_dc(result.shares, "A")
        assert set(by_dc) == {"DCW", "PAT"}
        for share in by_dc.values():
            assert share.pct == D("0.50000000")
            assert share.method == METHOD_EQUAL_SPLIT
            assert share.cold_start is True
            assert share.confidence is None
            assert share.basis_qty == D("0")
        assert _sigma(result.shares, "A") == ONE
        assert result.no_eligible_dc == ()

    def test_single_eligible_dc_gets_full_share(self):
        result = equal_split_shares(["A"], [_elig("A", "PAT")])
        by_dc = _by_dc(result.shares, "A")
        assert set(by_dc) == {"PAT"}
        assert by_dc["PAT"].pct == ONE

    def test_duplicate_item_keys_are_harmless(self):
        """item_keys is a set of items to cold-start — passing 'A' three
        times must not triple the share rows."""
        once = equal_split_shares(["A"], [_elig("A", "PAT"), _elig("A", "DCW")])
        thrice = equal_split_shares(
            ["A", "A", "A"], [_elig("A", "PAT"), _elig("A", "DCW")]
        )
        assert once == thrice


# ===========================================================================
# 6. Zero eligible DC — absent from shares, listed in the dedicated trace
# ===========================================================================


class TestZeroEligibleDc:
    def test_equal_split_zero_eligible_goes_to_dedicated_list(self):
        result = equal_split_shares(["GHOST"], [])
        assert result.shares == ()
        assert result.no_eligible_dc == ("GHOST",)

    def test_explicit_false_only_rows_count_as_zero_eligible(self):
        result = equal_split_shares(
            ["GHOST"], [_elig("GHOST", "PAT", eligible=False)]
        )
        assert result.shares == ()
        assert result.no_eligible_dc == ("GHOST",)

    def test_composition_item_absent_from_shares_and_not_cold_start(self):
        """In the composed computation the item is in NEITHER shares NOR
        items_cold_start — only in items_without_eligible_dc (the demand
        stays national; nothing invented onto an arbitrary center)."""
        result = compute_split_computation(["GHOST"], [], ROUTES, [])
        assert result.shares == ()
        assert result.items_without_eligible_dc == ("GHOST",)
        assert result.items_cold_start == ()


# ===========================================================================
# 7. Confidence — increasing with history depth, capped at 1
# ===========================================================================


class TestConfidence:
    @staticmethod
    def _confidence_for(qty: str, **kwargs) -> float:
        result = compute_split_shares(
            [_obs("A", "CA", qty)], ROUTES, [_elig("A", "PAT")], **kwargs
        )
        (share,) = result.shares
        return share.confidence

    def test_linear_ramp_hand_values_default_saturation(self):
        """Default saturation 1000: confidence = min(1, basis/1000).
        100 -> 0.1; 250 -> 0.25; 1000 -> 1.0 (saturation point);
        4000 -> 1.0 (CAPPED — never above 1)."""
        assert DEFAULT_CONFIDENCE_SATURATION_QTY == D("1000")
        assert self._confidence_for("100") == 0.1
        assert self._confidence_for("250") == 0.25
        assert self._confidence_for("1000") == 1.0
        assert self._confidence_for("4000") == 1.0

    def test_monotonically_increasing_then_flat_at_cap(self):
        depths = ["10", "100", "500", "999", "1000", "5000"]
        confidences = [self._confidence_for(q) for q in depths]
        # Strictly increasing until the saturation point…
        assert confidences[:5] == sorted(set(confidences[:5]))
        # …then flat at the cap, never above it.
        assert confidences[4] == confidences[5] == 1.0
        assert all(0 < c <= 1.0 for c in confidences)

    def test_saturation_qty_is_a_parameter(self):
        """saturation 200: 50 -> 0.25; 200 -> 1.0; 300 -> capped 1.0."""
        sat = {"confidence_saturation_qty": D("200")}
        assert self._confidence_for("50", **sat) == 0.25
        assert self._confidence_for("200", **sat) == 1.0
        assert self._confidence_for("300", **sat) == 1.0


# ===========================================================================
# 8. min_history_qty — the calibration gate
# ===========================================================================


class TestMinHistoryQty:
    def test_below_threshold_is_insufficient_basis(self):
        """Eligible basis 5 (CA 3 -> PAT, TX 2 -> DCW) < min 10: no history
        share, item listed in insufficient_basis_items."""
        result = compute_split_shares(
            [_obs("A", "CA", "3"), _obs("A", "TX", "2")],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DCW")],
            min_history_qty=D("10"),
        )
        assert result.shares == ()
        assert result.insufficient_basis_items == ("A",)

    def test_exactly_at_threshold_passes(self):
        """The gate is strict-below (`total < min`): basis exactly 10 with
        min 10 IS calibratable — PAT 6/10 = 0.6, DCW 4/10 = 0.4."""
        result = compute_split_shares(
            [_obs("A", "CA", "6"), _obs("A", "TX", "4")],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DCW")],
            min_history_qty=D("10"),
        )
        by_dc = _by_dc(result.shares, "A")
        assert by_dc["PAT"].pct == D("0.60000000")
        assert by_dc["DCW"].pct == D("0.40000000")
        assert by_dc["PAT"].basis_qty == D("10")
        assert result.insufficient_basis_items == ()

    def test_only_ineligible_weight_is_insufficient(self):
        """History routed EXCLUSIVELY to an ineligible DC = eligible basis 0
        -> insufficient even with min_history_qty at its 0 default."""
        result = compute_split_shares(
            [_obs("A", "NY", "50")],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DAL", eligible=False)],
        )
        assert result.shares == ()
        assert result.insufficient_basis_items == ("A",)

    def test_composition_insufficient_falls_back_to_equal_split(self):
        """Basis 5 < min 10 -> the composed computation covers the item via
        the flagged cold-start path instead (0.5/0.5 on its eligible DCs)."""
        result = compute_split_computation(
            ["A"],
            [_obs("A", "CA", "3"), _obs("A", "TX", "2")],
            ROUTES,
            [_elig("A", "PAT"), _elig("A", "DCW")],
            min_history_qty=D("10"),
        )
        by_dc = _by_dc(result.shares, "A")
        assert by_dc["PAT"].pct == D("0.50000000")
        assert by_dc["DCW"].pct == D("0.50000000")
        assert all(s.cold_start for s in by_dc.values())
        assert result.items_cold_start == ("A",)
        assert _sigma(result.shares, "A") == ONE


# ===========================================================================
# Fail-loudly contract — structural data problems raise, never guess
# ===========================================================================


class TestFailLoudly:
    def test_conflicting_state_route_raises(self):
        with pytest.raises(ShareComputationError, match="routes to both"):
            compute_split_shares(
                [_obs("A", "CA", "1")],
                [_route("CA", "PAT"), _route("CA", "DCW")],
                [_elig("A", "PAT")],
            )

    def test_conflicting_eligibility_raises(self):
        with pytest.raises(ShareComputationError, match="conflicting eligibility"):
            compute_split_shares(
                [_obs("A", "CA", "1")],
                ROUTES,
                [_elig("A", "PAT", eligible=True), _elig("A", "PAT", eligible=False)],
            )

    def test_negative_observation_qty_raises(self):
        with pytest.raises(ShareComputationError, match="negative qty"):
            compute_split_shares(
                [_obs("A", "CA", "-1")], ROUTES, [_elig("A", "PAT")]
            )

    def test_negative_min_history_qty_raises(self):
        with pytest.raises(ShareComputationError, match="min_history_qty"):
            compute_split_shares(
                [_obs("A", "CA", "1")],
                ROUTES,
                [_elig("A", "PAT")],
                min_history_qty=D("-1"),
            )

    def test_non_positive_saturation_raises(self):
        with pytest.raises(ShareComputationError, match="confidence_saturation_qty"):
            compute_split_shares(
                [_obs("A", "CA", "1")],
                ROUTES,
                [_elig("A", "PAT")],
                confidence_saturation_qty=D("0"),
            )
