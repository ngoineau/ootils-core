"""
Pure golden-master tests (no DB, no mocks) for hierarchical
reconciliation (src/ootils_core/pyramide/hierarchy/reconcile.py) —
Pyramide axis A, PR3. Pattern: tests/test_mrp_core_golden.py /
tests/test_seasonal_forecaster_golden.py — tiny hand-computed fixtures
lock the arithmetic so any deviation fails CI instead of silently
reshaping a demand plan.

Golden mini-block — hierarchy 'h-recon', levels = (family, product),
ONE reconciliation node (FAM-X, the block root) and THREE leaves:

    FAM-X (family)            <- reconciliation level (base curve here)
    ├── PRD-X1 (product)
    │     ├── item-x1         (history total 50 -> share 0.5)
    │     └── item-x2         (history total 30 -> share 0.3)
    └── PRD-X2 (product)
          └── item-x3         (history total 20 -> share 0.2)

Base curve at FAM-X (known seasonal curve): [200, 50, 100, 50].

Hand-written disaggregation (leaf = share x base, value by value):

    item-x1 (0.5): [100, 25, 50, 25]
    item-x2 (0.3): [ 60, 15, 30, 15]
    item-x3 (0.2): [ 40, 10, 20, 10]

Hand-written aggregates (exact sums of their leaves):

    PRD-X1 = x1 + x2      = [160, 40, 80, 40]
    PRD-X2 = x3           = [ 40, 10, 20, 10]
    FAM-X  = x1 + x2 + x3 = [200, 50, 100, 50]   (== base exactly)
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from ootils_core.pyramide.hierarchy import (
    RECON_MIDDLEOUT,
    RECON_MINT_SHRINK,
    MintInputs,
    ReconciliationError,
    ReconciliationUnavailable,
    HierarchyNodeRow,
    build_summing_blocks,
    middle_out,
    mint_shrink,
    reconcile,
)

H = "h-recon"
LEVELS = ("family", "product")

NODES = [
    HierarchyNodeRow(code="FAM-X", level="family", parent_code=None),
    HierarchyNodeRow(code="PRD-X1", level="product", parent_code="FAM-X"),
    HierarchyNodeRow(code="PRD-X2", level="product", parent_code="FAM-X"),
]

MEMBERSHIPS = [
    ("item-x1", "PRD-X1"),
    ("item-x2", "PRD-X1"),
    ("item-x3", "PRD-X2"),
]

BASE = [Decimal("200"), Decimal("50"), Decimal("100"), Decimal("50")]
TOTALS = {"item-x1": Decimal("50"), "item-x2": Decimal("30"), "item-x3": Decimal("20")}

# series order (summing.py contract): FAM-X, PRD-X1, PRD-X2, x1, x2, x3
EXPECTED = {
    "FAM-X": (Decimal("200"), Decimal("50"), Decimal("100"), Decimal("50")),
    "PRD-X1": (Decimal("160"), Decimal("40"), Decimal("80"), Decimal("40")),
    "PRD-X2": (Decimal("40"), Decimal("10"), Decimal("20"), Decimal("10")),
    "item-x1": (Decimal("100"), Decimal("25"), Decimal("50"), Decimal("25")),
    "item-x2": (Decimal("60"), Decimal("15"), Decimal("30"), Decimal("15")),
    "item-x3": (Decimal("40"), Decimal("10"), Decimal("20"), Decimal("10")),
}


def _block(nodes=None, memberships=None):
    return build_summing_blocks(
        hierarchy_id=H,
        levels=LEVELS,
        nodes=nodes if nodes is not None else NODES,
        memberships=memberships if memberships is not None else MEMBERSHIPS,
    )[0]


def _by_key(result):
    return {ref.key: result.values[i] for i, ref in enumerate(result.series)}


class TestGoldenMiddleOut:
    def test_disaggregation_matches_hand_written_values(self):
        result = middle_out(_block(), "family", {"FAM-X": BASE}, TOTALS)
        assert result.recon_method == RECON_MIDDLEOUT
        assert result.recon_level == "family"
        assert _by_key(result) == EXPECTED

    def test_shares_recorded_for_explainability(self):
        result = middle_out(_block(), "family", {"FAM-X": BASE}, TOTALS)
        shares = {s.leaf: s for s in result.shares}
        assert shares["item-x1"].share == Decimal("0.5")
        assert shares["item-x2"].share == Decimal("0.3")
        assert shares["item-x3"].share == Decimal("0.2")
        assert all(s.recon_node == "FAM-X" for s in result.shares)
        assert not any(s.cold_start for s in result.shares)
        # sum of shares = 1 by construction
        assert sum(s.share for s in result.shares) == Decimal("1")

    def test_coherence_y_equals_s_times_b_value_by_value(self):
        """y_hat = S . b_hat checked through SummingBlock.multiply for
        every horizon step — coherence is exact, not within tolerance."""
        block = _block()
        result = middle_out(block, "family", {"FAM-X": BASE}, TOTALS)
        leaf_offset = len(result.series) - len(block.leaves)
        for t in range(len(BASE)):
            leaf_vector = [result.values[leaf_offset + j][t] for j in range(len(block.leaves))]
            assert block.multiply(leaf_vector) == [result.values[i][t] for i in range(len(result.series))]

    def test_aggregates_are_exact_sums(self):
        values = _by_key(middle_out(_block(), "family", {"FAM-X": BASE}, TOTALS))
        for t in range(len(BASE)):
            assert values["PRD-X1"][t] == values["item-x1"][t] + values["item-x2"][t]
            assert values["PRD-X2"][t] == values["item-x3"][t]
            assert values["FAM-X"][t] == (
                values["item-x1"][t] + values["item-x2"][t] + values["item-x3"][t]
            )

    def test_reconciliation_at_intermediate_level(self):
        """Middle-out at 'product': one base curve per product node.

        PRD-X1 base [100, 10], leaves x1/x2 weights 50/50 -> [50, 5] each;
        PRD-X2 base [40, 4] -> x3 [40, 4]; FAM-X = sums = [140, 14].
        """
        result = middle_out(
            _block(),
            "product",
            {"PRD-X1": [Decimal("100"), Decimal("10")],
             "PRD-X2": [Decimal("40"), Decimal("4")]},
            {"item-x1": Decimal("50"), "item-x2": Decimal("50"),
             "item-x3": Decimal("7")},
        )
        values = _by_key(result)
        assert values["item-x1"] == (Decimal("50"), Decimal("5"))
        assert values["item-x2"] == (Decimal("50"), Decimal("5"))
        assert values["item-x3"] == (Decimal("40"), Decimal("4"))
        assert values["FAM-X"] == (Decimal("140"), Decimal("14"))


class TestColdStartTwin:
    def test_cold_leaf_inherits_mean_of_sibling_weights(self):
        """item-x2 has no history; its twin under PRD-X1 (item-x1, weight
        30) gives it weight 30. item-x3 keeps its own 30. Total 90 ->
        every leaf gets share 1/3: base [90, 9] -> [30, 3] each."""
        result = middle_out(
            _block(),
            "family",
            {"FAM-X": [Decimal("90"), Decimal("9")]},
            {"item-x1": Decimal("30"), "item-x2": Decimal("0"),
             "item-x3": Decimal("30")},
        )
        values = _by_key(result)
        for leaf in ("item-x1", "item-x2", "item-x3"):
            assert values[leaf] == (Decimal("30"), Decimal("3"))
        shares = {s.leaf: s for s in result.shares}
        assert shares["item-x2"].cold_start is True
        assert shares["item-x2"].weight == Decimal("30")
        assert shares["item-x1"].cold_start is False
        assert shares["item-x3"].cold_start is False

    def test_leaf_without_history_or_twin_is_a_natural_zero(self):
        """item-x3 is alone under PRD-X2 with no history: no twin -> its
        share is a documented natural zero and its curve is flat 0; the
        demand goes entirely to the leaves that have history."""
        result = middle_out(
            _block(),
            "family",
            {"FAM-X": [Decimal("80")]},
            {"item-x1": Decimal("50"), "item-x2": Decimal("30")},
        )
        values = _by_key(result)
        assert values["item-x3"] == (Decimal("0"),)
        assert values["item-x1"] == (Decimal("50"),)
        assert values["item-x2"] == (Decimal("30"),)
        assert values["PRD-X2"] == (Decimal("0"),)
        assert values["FAM-X"] == (Decimal("80"),)
        assert any("natural zero" in w for w in result.warnings)
        shares = {s.leaf: s for s in result.shares}
        assert shares["item-x3"].share == Decimal("0")
        assert shares["item-x3"].cold_start is False


class TestDeterminism:
    def test_two_runs_are_bit_identical(self):
        first = middle_out(_block(), "family", {"FAM-X": BASE}, TOTALS)
        second = middle_out(_block(), "family", {"FAM-X": BASE}, TOTALS)
        assert first == second
        assert repr(first) == repr(second)

    def test_totals_dict_order_does_not_matter(self):
        shuffled = {"item-x3": Decimal("20"), "item-x1": Decimal("50"),
                    "item-x2": Decimal("30")}
        assert middle_out(_block(), "family", {"FAM-X": BASE}, shuffled) == \
            middle_out(_block(), "family", {"FAM-X": BASE}, TOTALS)


class TestFailLoudly:
    def test_unknown_recon_level(self):
        with pytest.raises(ReconciliationError, match="galaxy"):
            middle_out(_block(), "galaxy", {"FAM-X": BASE}, TOTALS)

    def test_missing_base_curve(self):
        with pytest.raises(ReconciliationError, match="PRD-X2"):
            middle_out(
                _block(), "product",
                {"PRD-X1": [Decimal("10")]},
                TOTALS,
            )

    def test_base_curve_for_unknown_series(self):
        with pytest.raises(ReconciliationError, match="NOT-A-NODE"):
            middle_out(
                _block(), "family",
                {"FAM-X": BASE, "NOT-A-NODE": BASE},
                TOTALS,
            )

    def test_inconsistent_horizons(self):
        with pytest.raises(ReconciliationError, match="horizon"):
            middle_out(
                _block(), "product",
                {"PRD-X1": [Decimal("10")], "PRD-X2": [Decimal("1"), Decimal("2")]},
                TOTALS,
            )

    def test_leaf_above_recon_level_is_not_covered(self):
        """An item attached directly to the family node cannot be reached
        by product-level disaggregation — must fail, not vanish."""
        memberships = MEMBERSHIPS + [("item-x0", "FAM-X")]
        with pytest.raises(ReconciliationError, match="item-x0"):
            middle_out(
                _block(memberships=memberships), "product",
                {"PRD-X1": [Decimal("10")], "PRD-X2": [Decimal("5")]},
                TOTALS,
            )

    def test_nonzero_base_with_zero_weights_everywhere(self):
        with pytest.raises(ReconciliationError, match="cannot disaggregate"):
            middle_out(_block(), "family", {"FAM-X": BASE}, {})

    def test_negative_weight_rejected(self):
        with pytest.raises(ReconciliationError, match="negative"):
            middle_out(
                _block(), "family", {"FAM-X": BASE},
                {"item-x1": Decimal("-1"), "item-x2": Decimal("3"),
                 "item-x3": Decimal("2")},
            )

    def test_negative_base_value_rejected(self):
        with pytest.raises(ReconciliationError, match="negative"):
            middle_out(
                _block(), "family",
                {"FAM-X": [Decimal("-5"), Decimal("10")]},
                TOTALS,
            )

    def test_block_without_leaves(self):
        with pytest.raises(ReconciliationError, match="no leaf columns"):
            middle_out(_block(memberships=[]), "family", {"FAM-X": BASE}, {})

    def test_zero_base_with_zero_weights_yields_zero_curves(self):
        """All-zero base + no history is a legitimate silent block: every
        curve is 0, no error (nothing to disaggregate)."""
        result = middle_out(
            _block(), "family",
            {"FAM-X": [Decimal("0"), Decimal("0")]},
            {},
        )
        assert all(v == (Decimal("0"), Decimal("0")) for v in result.values)


class TestDispatcher:
    def test_middleout_direct(self):
        result = reconcile(_block(), "family", {"FAM-X": BASE}, TOTALS)
        assert result.recon_method == RECON_MIDDLEOUT
        assert _by_key(result) == EXPECTED

    def test_unsupported_method_fails_loudly(self):
        with pytest.raises(ReconciliationError, match="unsupported"):
            reconcile(_block(), "family", {"FAM-X": BASE}, TOTALS,
                      method="topdown")

    def test_mint_without_inputs_falls_back_to_middleout(self):
        """The fallback is deterministic (no dependency involved: missing
        inputs short-circuit before any import) and provenance says the
        truth: recon_method == 'middleout' + explicit warning."""
        result = reconcile(
            _block(), "family", {"FAM-X": BASE}, TOTALS,
            method=RECON_MINT_SHRINK, mint_inputs=None,
        )
        assert result.recon_method == RECON_MIDDLEOUT
        assert any("fell back" in w for w in result.warnings)
        assert _by_key(result) == EXPECTED

    def test_mint_strict_raises_instead_of_falling_back(self):
        with pytest.raises(ReconciliationError, match="strict"):
            reconcile(
                _block(), "family", {"FAM-X": BASE}, TOTALS,
                method=RECON_MINT_SHRINK, mint_inputs=None, strict=True,
            )


class TestMintShrinkOptionalBackend:
    """Real MinT path — runs only when the optional [forecast] backend is
    installed; the coherence contract must hold whatever the backend
    returns (leaves clamped, aggregates re-derived through S)."""

    def _inputs(self, k: int = 12) -> MintInputs:
        leaf_hist = {
            "item-x1": [Decimal("10")] * k,
            "item-x2": [Decimal("6")] * k,
            "item-x3": [Decimal("4")] * k,
        }
        agg_hist = {
            "PRD-X1": [Decimal("16")] * k,
            "PRD-X2": [Decimal("4")] * k,
            "FAM-X": [Decimal("20")] * k,
        }
        fitted = lambda hist: (hist[0], *hist[:-1])  # noqa: E731
        return MintInputs(
            aggregate_curves={"FAM-X": [Decimal("20")] * 3,
                              "PRD-X1": [Decimal("16")] * 3,
                              "PRD-X2": [Decimal("4")] * 3},
            leaf_curves={"item-x1": [Decimal("10")] * 3,
                         "item-x2": [Decimal("6")] * 3,
                         "item-x3": [Decimal("4")] * 3},
            aggregate_insample=agg_hist,
            leaf_insample=leaf_hist,
            aggregate_fitted={c: fitted(h) for c, h in agg_hist.items()},
            leaf_fitted={c: fitted(h) for c, h in leaf_hist.items()},
        )

    def test_mint_shrink_produces_coherent_curves(self):
        pytest.importorskip("hierarchicalforecast")
        block = _block()
        try:
            result = mint_shrink(block, "family", self._inputs())
        except ReconciliationUnavailable as exc:
            pytest.skip(f"MinT backend unavailable in this environment: {exc}")
        assert result.recon_method == RECON_MINT_SHRINK
        leaf_offset = len(result.series) - len(block.leaves)
        horizon = len(result.values[0])
        for t in range(horizon):
            leaf_vector = [result.values[leaf_offset + j][t] for j in range(len(block.leaves))]
            assert all(v >= 0 for v in leaf_vector)
            assert block.multiply(leaf_vector) == [result.values[i][t] for i in range(len(result.series))]

    def test_mint_refuses_short_insample(self):
        """Whether the backend is installed (length check) or not (lazy
        import), the failure mode is the same fallback-able exception."""
        with pytest.raises(ReconciliationUnavailable):
            mint_shrink(_block(), "family", self._inputs(k=3))
