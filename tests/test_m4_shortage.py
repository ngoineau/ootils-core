"""
tests/test_m4_shortage.py — Sprint M4 Shortage Detection — pure (no DB) tests.

Covers the in-memory parts of ``ShortageDetector``:
  - detect() / detect_with_params() return None when no shortage
  - detect() returns a ShortageRecord with correct shape/qty/date
  - severity_score = qty × days_in_bucket × unit_cost (proxy)

Tests that exercise persistence, ``resolve_stale()`` or ``get_active_shortages()``
live in tests/integration/test_m4_shortage_integration.py — they require a real
PostgreSQL connection.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

from ootils_core.engine.kernel._ids import deterministic_uuid
from ootils_core.engine.kernel.shortage.detector import ShortageDetector
from ootils_core.models import Node, ShortageRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_pi_node(
    closing_stock: Optional[Decimal] = None,
    time_span_start: Optional[date] = None,
    time_span_end: Optional[date] = None,
    item_id: Optional[UUID] = None,
    location_id: Optional[UUID] = None,
    node_id: Optional[UUID] = None,
    scenario_id: Optional[UUID] = None,
) -> Node:
    return Node(
        node_id=node_id or uuid4(),
        node_type="ProjectedInventory",
        scenario_id=scenario_id or uuid4(),
        item_id=item_id,
        location_id=location_id,
        closing_stock=closing_stock,
        time_span_start=time_span_start,
        time_span_end=time_span_end,
        has_shortage=(closing_stock is not None and closing_stock < 0),
        shortage_qty=abs(closing_stock) if (closing_stock is not None and closing_stock < 0) else Decimal("0"),
    )


# ---------------------------------------------------------------------------
# 1. detect() — no shortage (DB is never touched on this path → pass None)
# ---------------------------------------------------------------------------

class TestDetectNoShortage:
    def setup_method(self):
        self.detector = ShortageDetector()
        self.calc_run_id = uuid4()
        self.scenario_id = uuid4()

    def test_returns_none_when_closing_stock_zero(self):
        node = make_pi_node(closing_stock=Decimal("0"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result is None

    def test_returns_none_when_closing_stock_positive(self):
        node = make_pi_node(closing_stock=Decimal("100"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result is None

    def test_returns_none_when_closing_stock_none(self):
        node = make_pi_node(closing_stock=None)
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result is None


# ---------------------------------------------------------------------------
# 2. detect() — shortage detected
# ---------------------------------------------------------------------------

class TestDetectShortage:
    def setup_method(self):
        self.detector = ShortageDetector()
        self.calc_run_id = uuid4()
        self.scenario_id = uuid4()
        self.item_id = uuid4()
        self.location_id = uuid4()

    def test_returns_shortage_record_when_closing_stock_negative(self):
        node = make_pi_node(
            closing_stock=Decimal("-50"),
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 4, 8),
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result is not None
        assert isinstance(result, ShortageRecord)

    def test_shortage_qty_is_abs_closing_stock(self):
        node = make_pi_node(closing_stock=Decimal("-75.5"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.shortage_qty == Decimal("75.5")

    def test_shortage_scenario_and_pi_node_set(self):
        node_id = uuid4()
        node = make_pi_node(
            closing_stock=Decimal("-10"),
            node_id=node_id,
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.scenario_id == self.scenario_id
        assert result.pi_node_id == node_id
        assert result.calc_run_id == self.calc_run_id

    def test_item_and_location_propagated(self):
        item_id = uuid4()
        location_id = uuid4()
        node = make_pi_node(
            closing_stock=Decimal("-10"),
            item_id=item_id,
            location_id=location_id,
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.item_id == item_id
        assert result.location_id == location_id

    def test_status_is_active(self):
        node = make_pi_node(closing_stock=Decimal("-1"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.status == "active"

    def test_severity_class_is_stockout(self):
        node = make_pi_node(closing_stock=Decimal("-1"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.severity_class == "stockout"

    def test_explanation_id_is_none_by_default(self):
        node = make_pi_node(closing_stock=Decimal("-1"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.explanation_id is None

    def test_shortage_date_is_time_span_start(self):
        start = date(2026, 5, 1)
        node = make_pi_node(
            closing_stock=Decimal("-20"),
            time_span_start=start,
            time_span_end=date(2026, 5, 8),
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.shortage_date == start

    def test_shortage_id_is_deterministic(self):
        """shortage_id derives from (scenario_id, calc_run_id, pi_node_id) via uuid5."""
        node_id = uuid4()
        node = make_pi_node(closing_stock=Decimal("-10"), node_id=node_id)
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        expected = deterministic_uuid(
            "shortage", self.scenario_id, self.calc_run_id, node_id,
        )
        assert result.shortage_id == expected


# ---------------------------------------------------------------------------
# 3. severity_score = qty × days × unit_cost (proxy = 1)
# ---------------------------------------------------------------------------

class TestSeverityScore:
    def setup_method(self):
        self.detector = ShortageDetector()
        self.calc_run_id = uuid4()
        self.scenario_id = uuid4()

    def test_severity_score_with_7_day_bucket(self):
        # shortage_qty = 100, days = 7 → score = 700
        node = make_pi_node(
            closing_stock=Decimal("-100"),
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 4, 8),
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.severity_score == Decimal("700")

    def test_severity_score_with_30_day_bucket(self):
        # shortage_qty = 50, days = 30 → score = 1500
        node = make_pi_node(
            closing_stock=Decimal("-50"),
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 5, 1),
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.severity_score == Decimal("1500")

    def test_severity_score_defaults_to_1_day_when_no_span(self):
        # No time_span_start/end → days = 1
        node = make_pi_node(closing_stock=Decimal("-200"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.severity_score == Decimal("200")

    def test_severity_score_with_fractional_qty(self):
        # shortage_qty = 10.5, days = 4 → score = 42.0
        node = make_pi_node(
            closing_stock=Decimal("-10.5"),
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 4, 5),
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, db=None)
        assert result.severity_score == Decimal("42.0")


# ---------------------------------------------------------------------------
# 4. detect_with_params() — below_safety_stock branch (pure compute)
# ---------------------------------------------------------------------------

class TestDetectBelowSafetyStock:
    def setup_method(self):
        self.detector = ShortageDetector()
        self.calc_run_id = uuid4()
        self.scenario_id = uuid4()

    def test_below_safety_stock_triggers_warning(self):
        node = make_pi_node(
            closing_stock=Decimal("5"),
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 4, 2),
        )
        result = self.detector.detect_with_params(
            pi_node=node,
            calc_run_id=self.calc_run_id,
            scenario_id=self.scenario_id,
            db=None,
            safety_stock_qty=Decimal("10"),
        )
        assert result is not None
        assert result.severity_class == "below_safety_stock"
        # shortage_qty = safety_stock - closing = 10 - 5 = 5
        assert result.shortage_qty == Decimal("5")

    def test_at_or_above_safety_stock_returns_none(self):
        node = make_pi_node(closing_stock=Decimal("10"))
        result = self.detector.detect_with_params(
            pi_node=node,
            calc_run_id=self.calc_run_id,
            scenario_id=self.scenario_id,
            db=None,
            safety_stock_qty=Decimal("10"),
        )
        assert result is None

    def test_negative_closing_still_stockout_even_with_safety(self):
        node = make_pi_node(closing_stock=Decimal("-3"))
        result = self.detector.detect_with_params(
            pi_node=node,
            calc_run_id=self.calc_run_id,
            scenario_id=self.scenario_id,
            db=None,
            safety_stock_qty=Decimal("10"),
        )
        assert result is not None
        assert result.severity_class == "stockout"
        assert result.shortage_qty == Decimal("3")

    def test_custom_unit_cost_scales_severity(self):
        node = make_pi_node(
            closing_stock=Decimal("-10"),
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 4, 8),
        )
        result = self.detector.detect_with_params(
            pi_node=node,
            calc_run_id=self.calc_run_id,
            scenario_id=self.scenario_id,
            db=None,
            unit_cost=Decimal("3"),
        )
        # qty=10, days=7, cost=3 → 210
        assert result.severity_score == Decimal("210")


# ---------------------------------------------------------------------------
# 5. is_stocking DETECTION gate (migration 081, PR-B) — pure half
# ---------------------------------------------------------------------------

class TestDetectIsStockingGate:
    """`is_stocking=False` gates DETECTION only: even the deepest stockout (or
    a below-safety-stock dip) returns None before the db is ever touched.
    Default True keeps every pre-081 call signature byte-identical in
    behaviour. The DB-backed halves (SHORTAGES_SQL's `locations` LEFT JOIN and
    the propagator's location_stocking_cache preload) live in
    tests/integration/test_is_stocking_integration.py."""

    def setup_method(self):
        self.detector = ShortageDetector()
        self.calc_run_id = uuid4()
        self.scenario_id = uuid4()

    def test_non_stocking_suppresses_stockout(self):
        node = make_pi_node(
            closing_stock=Decimal("-9400"),
            time_span_start=date(2026, 7, 17),
            time_span_end=date(2026, 7, 18),
        )
        result = self.detector.detect_with_params(
            pi_node=node,
            calc_run_id=self.calc_run_id,
            scenario_id=self.scenario_id,
            db=None,
            is_stocking=False,
        )
        assert result is None

    def test_non_stocking_suppresses_below_safety_stock_too(self):
        node = make_pi_node(closing_stock=Decimal("5"))
        result = self.detector.detect_with_params(
            pi_node=node,
            calc_run_id=self.calc_run_id,
            scenario_id=self.scenario_id,
            db=None,
            safety_stock_qty=Decimal("10"),
            is_stocking=False,
        )
        assert result is None

    def test_default_true_preserves_detection(self):
        # No is_stocking argument at all — the pre-081 call signature.
        node = make_pi_node(closing_stock=Decimal("-3"))
        result = self.detector.detect_with_params(
            pi_node=node,
            calc_run_id=self.calc_run_id,
            scenario_id=self.scenario_id,
            db=None,
        )
        assert result is not None
        assert result.severity_class == "stockout"

    def test_detect_passthrough_respects_gate(self):
        # detect() forwards is_stocking to detect_with_params.
        node = make_pi_node(closing_stock=Decimal("-3"))
        result = self.detector.detect(
            node, self.calc_run_id, self.scenario_id, db=None, is_stocking=False
        )
        assert result is None
