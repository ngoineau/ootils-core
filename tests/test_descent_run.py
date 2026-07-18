"""
tests/test_descent_run.py — pure, DB-free unit tests of the demand-descent
run's deterministic maths (engine/descent/run.py, DESC-1 PR-B, ADR-043):
the two residual-imputation stages (_normalize_pct at NUMERIC(9,8),
_impute_qty_residual at NUMERIC(18,6)) and the pure eligibility-gate +
renormalization composition (_resolve_item_shares). The DB-backed run
itself (POST /v1/demand/descend) is covered by
tests/integration/test_demand_descent_run_integration.py.

The shared rule under test (mirrors engine/descent/shares.py::
_normalize_with_residual, per run.py's RESIDUAL IMPUTATION doc): quantize
each entry independently, then impute the WHOLE residual onto the single
largest share, ties broken by the smallest str(dc_location_id) — never
smeared, always deterministic, so Sigma(pct) == 1 and SUM(qty_derived) ==
qty_source hold EXACTLY.
"""
from __future__ import annotations

from decimal import Decimal
from uuid import UUID

import pytest

from ootils_core.engine.descent.run import (
    DescentError,
    _impute_qty_residual,
    _normalize_pct,
    _resolve_item_shares,
)

# Fixed UUIDs with a known str() ordering for deterministic tie-breaks.
DC_1 = UUID("00000000-0000-0000-0000-00000000000a")
DC_2 = UUID("00000000-0000-0000-0000-00000000000b")
DC_3 = UUID("00000000-0000-0000-0000-00000000000c")
ITEM = UUID("00000000-0000-0000-0000-0000000000aa")

PCT_Q = Decimal("0.00000001")
QTY_Q = Decimal("0.000001")


class TestNormalizePct:
    def test_exact_split_is_preserved(self):
        out = _normalize_pct({DC_1: Decimal("0.6"), DC_2: Decimal("0.4")})
        assert out == {DC_1: Decimal("0.6"), DC_2: Decimal("0.4")}
        assert sum(out.values()) == 1

    def test_partial_overlay_renormalizes_to_exactly_one(self):
        # The fork-overrides-only-some-rows case run.py documents: raw set
        # sums to 1.2 — renormalized 2/3 + 1/3 at 8 dp, residual on largest.
        out = _normalize_pct({DC_1: Decimal("0.8"), DC_2: Decimal("0.4")})
        assert sum(out.values()) == 1
        assert out[DC_1] == Decimal("0.66666667")  # 0.66666666 + residual
        assert out[DC_2] == Decimal("0.33333333")

    def test_thirds_residual_goes_to_smallest_id_on_tie(self):
        out = _normalize_pct({DC_1: Decimal(1), DC_2: Decimal(1), DC_3: Decimal(1)})
        assert sum(out.values()) == 1
        # All weights equal → the tie-break hands the +0.00000001 residual
        # to the smallest str(dc_id), deterministically.
        assert out[DC_1] == Decimal("0.33333334")
        assert out[DC_2] == Decimal("0.33333333")
        assert out[DC_3] == Decimal("0.33333333")

    def test_quantum_is_numeric_9_8(self):
        out = _normalize_pct({DC_1: Decimal("0.8"), DC_2: Decimal("0.4")})
        for value in out.values():
            assert value == value.quantize(PCT_Q)

    def test_non_positive_total_raises(self):
        with pytest.raises(DescentError, match="must be > 0"):
            _normalize_pct({DC_1: Decimal("0")})


class TestImputeQtyResidual:
    def test_no_gap_is_untouched(self):
        qty = {DC_1: Decimal("60.000000"), DC_2: Decimal("40.000000")}
        pct = {DC_1: Decimal("0.6"), DC_2: Decimal("0.4")}
        out = _impute_qty_residual(dict(qty), pct, Decimal("100.000000"))
        assert out == qty

    def test_rounding_gap_lands_on_largest_pct(self):
        # A source small enough that quantizing the three shares at 6 dp
        # genuinely under-shoots: 0.000100 x 1/3 → 0.000033 each, sum
        # 0.000099 — a 0.000001 gap the imputation must close in full.
        source = Decimal("0.000100")
        pct = {
            DC_1: Decimal("0.33333334"),
            DC_2: Decimal("0.33333333"),
            DC_3: Decimal("0.33333333"),
        }
        qty = {dc: (source * p).quantize(QTY_Q) for dc, p in pct.items()}
        assert sum(qty.values()) != source  # the gap exists pre-imputation
        out = _impute_qty_residual(qty, pct, source)
        assert sum(out.values()) == source  # conservation of mass, exact
        assert out[DC_1] == Decimal("0.000034")  # residual on the largest pct
        assert out[DC_2] == out[DC_3] == Decimal("0.000033")

    def test_tie_on_pct_breaks_by_smallest_id(self):
        source = Decimal("1.000001")
        pct = {DC_2: Decimal("0.5"), DC_1: Decimal("0.5")}
        qty = {DC_1: Decimal("0.500000"), DC_2: Decimal("0.500000")}
        out = _impute_qty_residual(qty, pct, source)
        assert sum(out.values()) == source
        assert out[DC_1] == Decimal("0.500001")  # smallest str(id) wins the tie
        assert out[DC_2] == Decimal("0.500000")


class TestResolveItemShares:
    def test_absent_eligibility_pair_is_not_eligible(self):
        # DC_2 has NO eligibility row at all → excluded, DC_1 renormalized
        # to carry the full share ("never invent eligibility").
        out = _resolve_item_shares(
            [ITEM],
            {ITEM: {DC_1: Decimal("0.6"), DC_2: Decimal("0.4")}},
            {(ITEM, DC_1): True},
            ITEM,  # scenario_id — only used for logging
        )
        assert out == {ITEM: {DC_1: Decimal("1.00000000")}}

    def test_explicit_false_is_excluded(self):
        out = _resolve_item_shares(
            [ITEM],
            {ITEM: {DC_1: Decimal("0.6"), DC_2: Decimal("0.4")}},
            {(ITEM, DC_1): True, (ITEM, DC_2): False},
            ITEM,
        )
        assert out == {ITEM: {DC_1: Decimal("1.00000000")}}

    def test_item_with_no_usable_share_is_absent(self):
        # No raw shares at all, or every share ineligible → the item is
        # simply ABSENT (the caller reports it in items_without_shares).
        assert _resolve_item_shares([ITEM], {}, {}, ITEM) == {}
        assert _resolve_item_shares(
            [ITEM], {ITEM: {DC_1: Decimal("1")}}, {(ITEM, DC_1): False}, ITEM
        ) == {}

    def test_all_eligible_renormalizes_sigma_one(self):
        out = _resolve_item_shares(
            [ITEM],
            {ITEM: {DC_1: Decimal("0.8"), DC_2: Decimal("0.4")}},
            {(ITEM, DC_1): True, (ITEM, DC_2): True},
            ITEM,
        )
        assert sum(out[ITEM].values()) == 1
