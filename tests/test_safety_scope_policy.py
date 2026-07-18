"""
tests/test_safety_scope_policy.py — pure (no DB) tests of the shortage-detection
safety_scope policy (ADR-021 amendment, DESC-1 PR-C, pilot arbitration
2026-07-18 — `engine/kernel/shortage/policy.py`).

Two pure surfaces:

1. Policy RESOLUTION (`safety_scope()` / `is_national_scope()`):
   - unset `OOTILS_SAFETY_SCOPE` resolves to the arbitrated default 'national'
     (a documented decision, not a silent fallback),
   - explicit 'per_site' / 'national' resolve verbatim,
   - ANY other value (typo, casing, empty string) raises `ValueError` —
     fail-loudly (CONTRIBUTING.md), never an ambiguous policy.

2. The DETECTOR's safety_scope contract (`ShortageDetector.detect_with_params`,
   pure compute — db is never touched on the detection path, mirrored on
   tests/test_m4_shortage.py's pattern):
   - invalid scope raises `ValueError` (mirror of the resolution guard),
   - 'national' NULLs the per-site threshold (below_safety_stock can never
     fire) but leaves the physical-stockout branch untouched,
   - the [-EPS, 0) rounding-noise sliver stays silent under 'national' — the
     threshold is NULLed, never coerced to 0 (the exact leak the detector
     docstring documents),
   - the detector-level DEFAULT stays 'per_site' (pre-DESC-1 behaviour for
     standalone callers); the env-resolved policy is threaded in by the ONE
     production call site (`PropagationEngine._propagate`), covered by
     tests/integration/test_safety_scope_integration.py.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.kernel.shortage.detector import ShortageDetector
from ootils_core.engine.kernel.shortage.policy import (
    VALID_SAFETY_SCOPES,
    is_national_scope,
    safety_scope,
)
from ootils_core.models import Node

_ENV_VAR = "OOTILS_SAFETY_SCOPE"


# ---------------------------------------------------------------------------
# 1. Policy resolution — safety_scope() / is_national_scope()
# ---------------------------------------------------------------------------


class TestSafetyScopeResolution:
    def test_valid_scopes_whitelist_is_pinned(self):
        """The whitelist is part of the contract — a new scope value must be a
        deliberate decision (ADR amendment), not an accidental widening."""
        assert VALID_SAFETY_SCOPES == ("national", "per_site")

    def test_unset_env_defaults_to_national(self, monkeypatch):
        """Pilot arbitration 2026-07-18: the DEFAULT is 'national'."""
        monkeypatch.delenv(_ENV_VAR, raising=False)
        assert safety_scope() == "national"

    def test_explicit_national_resolves_national(self, monkeypatch):
        monkeypatch.setenv(_ENV_VAR, "national")
        assert safety_scope() == "national"

    def test_explicit_per_site_resolves_per_site(self, monkeypatch):
        """'per_site' = the historical pre-DESC-1 behaviour, kept intact."""
        monkeypatch.setenv(_ENV_VAR, "per_site")
        assert safety_scope() == "per_site"

    @pytest.mark.parametrize(
        "bad",
        ["regional", "", "NATIONAL", "National", "per-site", "site", " national"],
        ids=["typo", "empty", "upper", "title", "dash", "partial", "whitespace"],
    )
    def test_unknown_value_raises_value_error(self, monkeypatch, bad):
        """Never coerced, never ignored — a misconfigured env fails loudly with
        the variable name and the offending value in the message."""
        monkeypatch.setenv(_ENV_VAR, bad)
        with pytest.raises(ValueError, match=_ENV_VAR):
            safety_scope()

    def test_unknown_value_message_carries_offending_value(self, monkeypatch):
        monkeypatch.setenv(_ENV_VAR, "regional")
        with pytest.raises(ValueError, match="regional"):
            safety_scope()

    def test_is_national_scope_true_on_default(self, monkeypatch):
        monkeypatch.delenv(_ENV_VAR, raising=False)
        assert is_national_scope() is True

    def test_is_national_scope_false_on_per_site(self, monkeypatch):
        monkeypatch.setenv(_ENV_VAR, "per_site")
        assert is_national_scope() is False

    def test_is_national_scope_fails_loudly_like_safety_scope(self, monkeypatch):
        """The boolean convenience (the SQL engine's `%(safety_scope_national)s`
        builder, propagator_sql.shortage_params) must not swallow the guard."""
        monkeypatch.setenv(_ENV_VAR, "regional")
        with pytest.raises(ValueError, match=_ENV_VAR):
            is_national_scope()


# ---------------------------------------------------------------------------
# 2. Detector contract — detect_with_params(safety_scope=...) (pure compute)
# ---------------------------------------------------------------------------


def _pi_node(
    closing_stock: Optional[Decimal],
    item_id: Optional[UUID] = None,
    location_id: Optional[UUID] = None,
) -> Node:
    """Minimal in-memory ProjectedInventory node (pattern of
    tests/test_m4_shortage.py:make_pi_node) — 1-day bucket so
    severity_score == shortage_qty with the unpriced-item proxy cost."""
    return Node(
        node_id=uuid4(),
        node_type="ProjectedInventory",
        scenario_id=uuid4(),
        item_id=item_id or uuid4(),
        location_id=location_id or uuid4(),
        closing_stock=closing_stock,
        time_span_start=date(2026, 7, 18),
        time_span_end=date(2026, 7, 19),
        has_shortage=(closing_stock is not None and closing_stock < 0),
        shortage_qty=(
            abs(closing_stock)
            if (closing_stock is not None and closing_stock < 0)
            else Decimal("0")
        ),
    )


class TestDetectorSafetyScopeContract:
    def setup_method(self):
        self.detector = ShortageDetector()
        self.calc_run_id = uuid4()
        self.scenario_id = uuid4()

    def _detect(self, node, *, safety_stock_qty, safety_scope):
        return self.detector.detect_with_params(
            pi_node=node,
            calc_run_id=self.calc_run_id,
            scenario_id=self.scenario_id,
            db=None,  # detection path never touches the DB
            safety_stock_qty=safety_stock_qty,
            safety_scope=safety_scope,
        )

    def test_invalid_scope_raises_value_error(self):
        node = _pi_node(Decimal("3"))
        with pytest.raises(ValueError, match="safety_scope"):
            self._detect(node, safety_stock_qty=Decimal("10"), safety_scope="regional")

    def test_national_ignores_below_safety_stock(self):
        """closing 3, per-site SS 10: 'per_site' would flag below_safety_stock;
        'national' must return None — the per-site threshold is not a
        detection threshold under the arbitrated policy."""
        node = _pi_node(Decimal("3"))
        assert (
            self._detect(node, safety_stock_qty=Decimal("10"), safety_scope="national")
            is None
        )

    def test_per_site_detects_below_safety_stock(self):
        """The historical behaviour, byte-for-byte: qty = SS - closing."""
        node = _pi_node(Decimal("3"))
        record = self._detect(
            node, safety_stock_qty=Decimal("10"), safety_scope="per_site"
        )
        assert record is not None
        assert record.severity_class == "below_safety_stock"
        assert record.shortage_qty == Decimal("7")

    def test_national_still_detects_physical_stockout(self):
        """The stockout branch (closing < -EPS) is untouched in either scope."""
        node = _pi_node(Decimal("-15"))
        record = self._detect(
            node, safety_stock_qty=Decimal("10"), safety_scope="national"
        )
        assert record is not None
        assert record.severity_class == "stockout"
        assert record.shortage_qty == Decimal("15")

    def test_national_epsilon_sliver_stays_silent(self):
        """The NULL-not-0 rationale (detector docstring / SHORTAGES_SQL comment):
        a closing in [-EPS, 0) is rounding noise, not a shortage. Had 'national'
        coerced the threshold to a literal 0 instead of None, this closing would
        leak a near-zero below_safety_stock row."""
        node = _pi_node(Decimal("-1e-10"))  # inside [-EPS, 0), EPS = 1e-9
        assert (
            self._detect(node, safety_stock_qty=Decimal("10"), safety_scope="national")
            is None
        )

    def test_per_site_epsilon_sliver_still_below_safety(self):
        """Witness for the sliver test above: the SAME closing under 'per_site'
        with SS > 0 IS a below_safety_stock — proving the national silence comes
        from the scope gate, not from the closing value being undetectable."""
        node = _pi_node(Decimal("-1e-10"))
        record = self._detect(
            node, safety_stock_qty=Decimal("10"), safety_scope="per_site"
        )
        assert record is not None
        assert record.severity_class == "below_safety_stock"

    def test_detector_default_scope_is_per_site(self):
        """Standalone callers that don't resolve the policy keep the pre-DESC-1
        behaviour; only the production call site threads the env-resolved
        scope (PropagationEngine._propagate)."""
        node = _pi_node(Decimal("3"))
        record = self.detector.detect_with_params(
            pi_node=node,
            calc_run_id=self.calc_run_id,
            scenario_id=self.scenario_id,
            db=None,
            safety_stock_qty=Decimal("10"),
        )
        assert record is not None
        assert record.severity_class == "below_safety_stock"
