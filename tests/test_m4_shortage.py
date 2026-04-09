"""
tests/test_m4_shortage.py — Sprint M4 Shortage Detection tests.

Covers:
  - detect() returns None when no shortage
  - detect() returns ShortageRecord with correct shortage_qty
  - severity_score = qty × days computed correctly
  - get_active_shortages() sorted by severity_score DESC (mocked DB)
  - resolve_stale() returns correct count (mocked DB)
  - Propagator integration: shortage auto-generated when shortage_detector injected (mocked)
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional
from unittest.mock import MagicMock, call, patch
from uuid import UUID, uuid4

import pytest

from ootils_core.models import (
    Node,
    ShortageRecord,
)
from ootils_core.engine.kernel.shortage.detector import ShortageDetector


# ---------------------------------------------------------------------------
# Helpers / fixtures
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
# 1. detect() — no shortage
# ---------------------------------------------------------------------------

class TestDetectNoShortage:
    def setup_method(self):
        self.detector = ShortageDetector()
        self.calc_run_id = uuid4()
        self.scenario_id = uuid4()
        self.db = MagicMock()

    def test_returns_none_when_closing_stock_zero(self):
        node = make_pi_node(closing_stock=Decimal("0"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result is None

    def test_returns_none_when_closing_stock_positive(self):
        node = make_pi_node(closing_stock=Decimal("100"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result is None

    def test_returns_none_when_closing_stock_none(self):
        node = make_pi_node(closing_stock=None)
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
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
        self.db = MagicMock()

    def test_returns_shortage_record_when_closing_stock_negative(self):
        node = make_pi_node(
            closing_stock=Decimal("-50"),
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 4, 8),
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result is not None
        assert isinstance(result, ShortageRecord)

    def test_shortage_qty_is_abs_closing_stock(self):
        node = make_pi_node(closing_stock=Decimal("-75.5"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result.shortage_qty == Decimal("75.5")

    def test_shortage_scenario_and_pi_node_set(self):
        node_id = uuid4()
        node = make_pi_node(
            closing_stock=Decimal("-10"),
            node_id=node_id,
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
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
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result.item_id == item_id
        assert result.location_id == location_id

    def test_status_is_active(self):
        node = make_pi_node(closing_stock=Decimal("-1"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result.status == "active"

    def test_explanation_id_is_none_by_default(self):
        node = make_pi_node(closing_stock=Decimal("-1"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result.explanation_id is None

    def test_shortage_date_is_time_span_start(self):
        start = date(2026, 5, 1)
        node = make_pi_node(
            closing_stock=Decimal("-20"),
            time_span_start=start,
            time_span_end=date(2026, 5, 8),
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result.shortage_date == start


# ---------------------------------------------------------------------------
# 3. severity_score = qty × days
# ---------------------------------------------------------------------------

class TestSeverityScore:
    def setup_method(self):
        self.detector = ShortageDetector()
        self.calc_run_id = uuid4()
        self.scenario_id = uuid4()
        self.db = MagicMock()

    def test_severity_score_with_7_day_bucket(self):
        # shortage_qty = 100, days = 7 → score = 700
        node = make_pi_node(
            closing_stock=Decimal("-100"),
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 4, 8),
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result.severity_score == Decimal("700")

    def test_severity_score_with_30_day_bucket(self):
        # shortage_qty = 50, days = 30 → score = 1500
        node = make_pi_node(
            closing_stock=Decimal("-50"),
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 5, 1),
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result.severity_score == Decimal("1500")

    def test_severity_score_defaults_to_1_day_when_no_span(self):
        # No time_span_start/end → days = 1
        node = make_pi_node(closing_stock=Decimal("-200"))
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result.severity_score == Decimal("200")

    def test_severity_score_with_fractional_qty(self):
        # shortage_qty = 10.5, days = 4 → score = 42.0
        node = make_pi_node(
            closing_stock=Decimal("-10.5"),
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 4, 5),
        )
        result = self.detector.detect(node, self.calc_run_id, self.scenario_id, self.db)
        assert result.severity_score == Decimal("42.0")


# ---------------------------------------------------------------------------
# 4. get_active_shortages() — sorted by severity_score DESC
# ---------------------------------------------------------------------------

class TestGetActiveShortages:
    def setup_method(self):
        self.detector = ShortageDetector()
        self.scenario_id = uuid4()

    def _make_row(self, severity: str, shortage_id: Optional[UUID] = None) -> dict:
        calc_run_id = uuid4()
        return {
            "shortage_id": shortage_id or uuid4(),
            "scenario_id": self.scenario_id,
            "pi_node_id": uuid4(),
            "item_id": None,
            "location_id": None,
            "shortage_date": date(2026, 4, 1),
            "shortage_qty": Decimal("10"),
            "severity_score": Decimal(severity),
            "explanation_id": None,
            "calc_run_id": calc_run_id,
            "status": "active",
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }

    def test_returns_list_of_shortage_records(self):
        db = MagicMock()
        rows = [self._make_row("300"), self._make_row("700"), self._make_row("100")]
        db.execute.return_value.fetchall.return_value = rows

        results = self.detector.get_active_shortages(self.scenario_id, db)
        assert len(results) == 3
        assert all(isinstance(r, ShortageRecord) for r in results)

    def test_sorted_by_severity_desc(self):
        """get_active_shortages relies on ORDER BY in SQL — verify mapping is correct."""
        db = MagicMock()
        # Simulate DB returning already-sorted rows (ORDER BY severity_score DESC)
        rows = [
            self._make_row("700"),
            self._make_row("300"),
            self._make_row("100"),
        ]
        db.execute.return_value.fetchall.return_value = rows

        results = self.detector.get_active_shortages(self.scenario_id, db)
        scores = [r.severity_score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_sql_contains_order_by_severity_desc(self):
        """Verify the SQL query includes ORDER BY severity_score DESC."""
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []

        self.detector.get_active_shortages(self.scenario_id, db)

        call_args = db.execute.call_args
        sql = call_args[0][0]
        assert "SEVERITY_SCORE DESC" in sql.upper()

    def test_sql_filters_by_scenario_and_active_status(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []

        self.detector.get_active_shortages(self.scenario_id, db)

        call_args = db.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        assert "status" in sql
        assert self.scenario_id in params

    def test_returns_empty_list_when_no_shortages(self):
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []

        results = self.detector.get_active_shortages(self.scenario_id, db)
        assert results == []


# ---------------------------------------------------------------------------
# 5. resolve_stale() — correct count
# ---------------------------------------------------------------------------

class TestResolveStale:
    def setup_method(self):
        self.detector = ShortageDetector()
        self.scenario_id = uuid4()
        self.calc_run_id = uuid4()

    def test_returns_rowcount_from_db(self):
        db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        db.execute.return_value = mock_cursor

        count = self.detector.resolve_stale(self.scenario_id, self.calc_run_id, db)
        assert count == 3

    def test_returns_zero_when_nothing_to_resolve(self):
        db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        db.execute.return_value = mock_cursor

        count = self.detector.resolve_stale(self.scenario_id, self.calc_run_id, db)
        assert count == 0

    def test_sql_filters_correct_scenario_and_excludes_current_run(self):
        db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2
        db.execute.return_value = mock_cursor

        self.detector.resolve_stale(self.scenario_id, self.calc_run_id, db)

        call_args = db.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        # Should UPDATE to 'resolved'
        assert "resolved" in sql
        # Should filter on scenario_id and exclude calc_run_id
        assert self.scenario_id in params
        assert self.calc_run_id in params

    def test_returns_integer(self):
        db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        db.execute.return_value = mock_cursor

        count = self.detector.resolve_stale(self.scenario_id, self.calc_run_id, db)
        assert isinstance(count, int)


# ---------------------------------------------------------------------------
# 6. Propagator integration — shortage auto-generated (mocked)
# ---------------------------------------------------------------------------

class TestPropagatorIntegration:
    """
    Test that PropagationEngine calls ShortageDetector.detect() and persist()
    when a PI node has has_shortage=True and a shortage_detector is injected.
    """

    def _make_propagator(self, shortage_detector=None, explanation_builder=None):
        from ootils_core.engine.orchestration.propagator import PropagationEngine

        store = MagicMock()
        traversal = MagicMock()
        dirty = MagicMock()
        calc_run_mgr = MagicMock()
        kernel = MagicMock()

        engine = PropagationEngine(
            store=store,
            traversal=traversal,
            dirty=dirty,
            calc_run_mgr=calc_run_mgr,
            kernel=kernel,
            explanation_builder=explanation_builder,
            shortage_detector=shortage_detector,
        )
        return engine, store, kernel

    def test_shortage_detector_called_when_has_shortage(self):
        """ShortageDetector.detect + persist called for a PI node with shortage."""
        scenario_id = uuid4()
        node_id = uuid4()
        calc_run_id = uuid4()

        shortage_detector = MagicMock()
        mock_shortage = MagicMock(spec=ShortageRecord)
        shortage_detector.detect_with_params.return_value = mock_shortage

        engine, store, kernel = self._make_propagator(shortage_detector=shortage_detector)

        # Setup node with shortage
        fresh_node = make_pi_node(
            closing_stock=Decimal("-50"),
            node_id=node_id,
            scenario_id=scenario_id,
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 4, 8),
        )
        fresh_node.has_shortage = True

        # store.get_node returns node (both for initial and reload)
        store.get_node.return_value = fresh_node

        # kernel returns a shortage result
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("50"),
            "closing_stock": Decimal("-50"),
            "has_shortage": True,
            "shortage_qty": Decimal("50"),
        }

        db = MagicMock()

        engine._recompute_pi_node(
            node_id=node_id,
            scenario_id=scenario_id,
            calc_run_id=calc_run_id,
            db=db,
        )

        # detect_with_params() should have been called (propagator uses enhanced detection)
        shortage_detector.detect_with_params.assert_called_once()
        detect_call = shortage_detector.detect_with_params.call_args
        # detect_with_params() is called with keyword args: pi_node=, calc_run_id=, scenario_id=, db=
        pi_node_arg = detect_call.kwargs.get("pi_node") or (detect_call[0][0] if detect_call[0] else None)
        assert pi_node_arg is not None
        assert pi_node_arg.node_id == node_id

        # persist() should have been called with the mock shortage
        shortage_detector.persist.assert_called_once_with(shortage_detector.detect_with_params.return_value, db)

    def test_shortage_detector_not_called_when_none(self):
        """No crash and no call when shortage_detector is None (backward-compat)."""
        scenario_id = uuid4()
        node_id = uuid4()
        calc_run_id = uuid4()

        engine, store, kernel = self._make_propagator(shortage_detector=None)

        node = make_pi_node(
            closing_stock=Decimal("-10"),
            node_id=node_id,
            scenario_id=scenario_id,
        )
        store.get_node.return_value = node
        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("0"),
            "inflows": Decimal("0"),
            "outflows": Decimal("10"),
            "closing_stock": Decimal("-10"),
            "has_shortage": True,
            "shortage_qty": Decimal("10"),
        }

        db = MagicMock()

        # Should not raise
        result = engine._recompute_pi_node(
            node_id=node_id,
            scenario_id=scenario_id,
            calc_run_id=calc_run_id,
            db=db,
        )
        assert result is not None  # returns True/False for changed

    def test_persist_not_called_when_detect_returns_none(self):
        """persist() is NOT called if detect() returns None (no shortage)."""
        scenario_id = uuid4()
        node_id = uuid4()
        calc_run_id = uuid4()

        shortage_detector = MagicMock()
        shortage_detector.detect_with_params.return_value = None  # No shortage

        engine, store, kernel = self._make_propagator(shortage_detector=shortage_detector)

        node = make_pi_node(
            closing_stock=Decimal("100"),  # positive — no shortage
            node_id=node_id,
            scenario_id=scenario_id,
            time_span_start=date(2026, 4, 1),
            time_span_end=date(2026, 4, 8),
        )
        node.has_shortage = False
        store.get_node.return_value = node

        kernel.compute_pi_node.return_value = {
            "opening_stock": Decimal("50"),
            "inflows": Decimal("100"),
            "outflows": Decimal("50"),
            "closing_stock": Decimal("100"),
            "has_shortage": False,
            "shortage_qty": Decimal("0"),
        }

        db = MagicMock()

        engine._recompute_pi_node(
            node_id=node_id,
            scenario_id=scenario_id,
            calc_run_id=calc_run_id,
            db=db,
        )

        shortage_detector.detect_with_params.assert_called_once()
        shortage_detector.persist.assert_not_called()

    def test_propagator_backward_compatible_without_shortage_detector(self):
        """PropagationEngine can be instantiated without shortage_detector."""
        from ootils_core.engine.orchestration.propagator import PropagationEngine

        store = MagicMock()
        # Should not raise
        engine = PropagationEngine(
            store=store,
            traversal=MagicMock(),
            dirty=MagicMock(),
            calc_run_mgr=MagicMock(),
            kernel=MagicMock(),
        )
        assert engine._shortage_detector is None
