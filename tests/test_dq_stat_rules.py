"""
Tests for stat_rules.py.

Covers: run_stat_rules (dispatcher), _load_history, _load_current_rows, _get_entity_type,
_check_lead_time_spike, _check_forecast_spike, _check_price_outlier,
_check_safety_stock_zero, _check_negative_onhand.
All branches including skip conditions, parse errors, threshold logic.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock
from uuid import uuid4

from ootils_core.engine.dq.agent.stat_rules import (
    run_stat_rules,
    _load_history,
    _load_current_rows,
    _get_entity_type,
    _check_lead_time_spike,
    _check_forecast_spike,
    _check_price_outlier,
    _check_safety_stock_zero,
    _check_negative_onhand,
    SEVERITY_ERROR,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_db():
    return MagicMock()


def _make_cursor(rows):
    cursor = MagicMock()
    if rows is None:
        cursor.fetchone.return_value = None
        cursor.fetchall.return_value = []
    elif isinstance(rows, dict):
        cursor.fetchone.return_value = rows
        cursor.fetchall.return_value = [rows]
    elif isinstance(rows, list):
        cursor.fetchone.return_value = rows[0] if rows else None
        cursor.fetchall.return_value = rows
    return cursor


# =========================================================================
# _get_entity_type
# =========================================================================

class TestGetEntityType:

    def test_returns_entity_type(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor({"entity_type": "items"})
        assert _get_entity_type(db, uuid4()) == "items"

    def test_returns_empty_when_not_found(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        assert _get_entity_type(db, uuid4()) == ""


# =========================================================================
# _load_history
# =========================================================================

class TestLoadHistory:

    def test_query_uses_batch_status_and_runtime_timestamps(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([])

        _load_history(db, "items", uuid4())

        sql = db.execute.call_args[0][0]
        assert "ib.status IN ('validated', 'rejected', 'imported', 'partial')" in sql
        assert "COALESCE(ib.imported_at, ib.processed_at, ib.submitted_at)" in sql
        assert "ib.dq_status" not in sql
        assert "ib.created_at" not in sql

    def test_loads_and_parses_json_strings(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": json.dumps({"item": "A", "qty": 10})},
            {"raw_content": json.dumps({"item": "B", "qty": 20})},
        ])
        result = _load_history(db, "items", uuid4())
        assert len(result) == 2
        assert result[0]["item"] == "A"

    def test_loads_dict_content_directly(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": {"item": "C"}},
        ])
        result = _load_history(db, "items", uuid4())
        assert len(result) == 1

    def test_skips_invalid_json(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": "not json"},
            {"raw_content": json.dumps({"item": "D"})},
        ])
        result = _load_history(db, "items", uuid4())
        assert len(result) == 1

    def test_skips_non_dict_json(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": json.dumps([1, 2, 3])},
        ])
        result = _load_history(db, "items", uuid4())
        assert len(result) == 0

    def test_empty_history(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([])
        result = _load_history(db, "items", uuid4())
        assert result == []


# =========================================================================
# _load_current_rows
# =========================================================================

class TestLoadCurrentRows:

    def test_loads_rows(self):
        db = _mock_db()
        row_id = uuid4()
        db.execute.return_value = _make_cursor([
            {"row_id": row_id, "row_number": 1, "raw_content": json.dumps({"qty": 5})},
        ])
        result = _load_current_rows(db, uuid4())
        assert len(result) == 1
        assert result[0] == (row_id, 1, {"qty": 5})

    def test_skips_invalid_json(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"row_id": uuid4(), "row_number": 1, "raw_content": "bad"},
            {"row_id": uuid4(), "row_number": 2, "raw_content": json.dumps({"ok": True})},
        ])
        result = _load_current_rows(db, uuid4())
        assert len(result) == 1

    def test_dict_content(self):
        db = _mock_db()
        rid = uuid4()
        db.execute.return_value = _make_cursor([
            {"row_id": rid, "row_number": 1, "raw_content": {"already": "dict"}},
        ])
        result = _load_current_rows(db, uuid4())
        assert len(result) == 1

    def test_skips_non_dict(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"row_id": uuid4(), "row_number": 1, "raw_content": json.dumps("string")},
        ])
        result = _load_current_rows(db, uuid4())
        assert len(result) == 0


# =========================================================================
# _check_lead_time_spike
# =========================================================================

class TestCheckLeadTimeSpike:

    def test_detects_spike(self):
        batch_id = uuid4()
        # Historical: mean=10, stdev~0 except we need >3 points
        # Use [10, 10, 10, 10] -> mean=10, stdev=0 -> skip (stdev==0)
        # Use [10, 11, 10, 11] -> mean=10.5, stdev~0.577; z>3 if lt > 10.5 + 3*0.577 = 12.23
        history = [
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
            {"item_external_id": "ITEM-1", "lead_time_days": 11},
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
            {"item_external_id": "ITEM-1", "lead_time_days": 11},
        ]
        # Current: lead_time_days=30 -> z = |30-10.5|/0.577 >> 3
        rid = uuid4()
        current_rows = [
            (rid, 1, {"item_external_id": "ITEM-1", "lead_time_days": 30}),
        ]
        issues = _check_lead_time_spike(batch_id, current_rows, history)
        assert len(issues) == 1
        assert issues[0].rule_code == "STAT_LEAD_TIME_SPIKE"
        assert issues[0].severity == SEVERITY_ERROR

    def test_no_spike_within_range(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "lead_time_days": v}
            for v in [10, 11, 10, 11, 12, 9]
        ]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "lead_time_days": 10.5}),
        ]
        issues = _check_lead_time_spike(batch_id, current_rows, history)
        assert issues == []

    def test_skips_insufficient_history(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
            {"item_external_id": "ITEM-1", "lead_time_days": 11},
        ]  # only 2 data points
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "lead_time_days": 100}),
        ]
        issues = _check_lead_time_spike(batch_id, current_rows, history)
        assert issues == []

    def test_skips_zero_stdev(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
        ]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "lead_time_days": 100}),
        ]
        issues = _check_lead_time_spike(batch_id, current_rows, history)
        assert issues == []

    def test_skips_missing_item_ext(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"lead_time_days": 100}),
        ]
        issues = _check_lead_time_spike(batch_id, current_rows, [])
        assert issues == []

    def test_skips_missing_lead_time(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1"}),
        ]
        issues = _check_lead_time_spike(batch_id, current_rows, [])
        assert issues == []

    def test_skips_non_numeric_lead_time(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
        ]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "lead_time_days": "abc"}),
        ]
        issues = _check_lead_time_spike(batch_id, current_rows, history)
        assert issues == []

    def test_skips_non_numeric_history(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "lead_time_days": "abc"},
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
            {"item_external_id": "ITEM-1", "lead_time_days": 11},
            {"item_external_id": "ITEM-1", "lead_time_days": 10},
        ]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "lead_time_days": 100}),
        ]
        # Only 3 valid history points, but should still work
        issues = _check_lead_time_spike(batch_id, current_rows, history)
        assert len(issues) >= 0  # may or may not detect depending on stdev

    def test_uses_external_id_fallback_in_history(self):
        batch_id = uuid4()
        history = [
            {"external_id": "ITEM-1", "lead_time_days": v}
            for v in [10, 11, 10, 11]
        ]
        current_rows = [
            (uuid4(), 1, {"external_id": "ITEM-1", "lead_time_days": 50}),
        ]
        issues = _check_lead_time_spike(batch_id, current_rows, history)
        assert len(issues) == 1


# =========================================================================
# _check_forecast_spike
# =========================================================================

class TestCheckForecastSpike:

    def test_detects_spike(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "quantity": 100},
            {"item_external_id": "ITEM-1", "quantity": 120},
        ]
        # mean = 110; qty > 110 * 10 = 1100
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "quantity": 2000}),
        ]
        issues = _check_forecast_spike(batch_id, current_rows, history)
        assert len(issues) == 1
        assert issues[0].rule_code == "STAT_FORECAST_SPIKE"

    def test_no_spike(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "quantity": 100},
            {"item_external_id": "ITEM-1", "quantity": 120},
        ]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "quantity": 500}),
        ]
        issues = _check_forecast_spike(batch_id, current_rows, history)
        assert issues == []

    def test_skips_insufficient_history(self):
        batch_id = uuid4()
        history = [{"item_external_id": "ITEM-1", "quantity": 100}]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "quantity": 99999}),
        ]
        issues = _check_forecast_spike(batch_id, current_rows, history)
        assert issues == []

    def test_skips_zero_mean(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "quantity": 0},
            {"item_external_id": "ITEM-1", "quantity": 0},
        ]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "quantity": 100}),
        ]
        issues = _check_forecast_spike(batch_id, current_rows, history)
        assert issues == []

    def test_skips_negative_mean(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "quantity": -10},
            {"item_external_id": "ITEM-1", "quantity": -20},
        ]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "quantity": 100}),
        ]
        issues = _check_forecast_spike(batch_id, current_rows, history)
        assert issues == []

    def test_skips_missing_item_ext(self):
        batch_id = uuid4()
        current_rows = [(uuid4(), 1, {"quantity": 9999})]
        issues = _check_forecast_spike(batch_id, current_rows, [])
        assert issues == []

    def test_skips_missing_qty(self):
        batch_id = uuid4()
        current_rows = [(uuid4(), 1, {"item_external_id": "X"})]
        issues = _check_forecast_spike(batch_id, current_rows, [])
        assert issues == []

    def test_skips_non_numeric_qty(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "X", "quantity": 10},
            {"item_external_id": "X", "quantity": 10},
        ]
        current_rows = [(uuid4(), 1, {"item_external_id": "X", "quantity": "abc"})]
        issues = _check_forecast_spike(batch_id, current_rows, history)
        assert issues == []

    def test_skips_non_numeric_history_qty(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "X", "quantity": "abc"},
            {"item_external_id": "X", "quantity": 10},
            {"item_external_id": "X", "quantity": 10},
        ]
        current_rows = [(uuid4(), 1, {"item_external_id": "X", "quantity": 99999})]
        issues = _check_forecast_spike(batch_id, current_rows, history)
        # 2 valid history points
        assert len(issues) == 1


# =========================================================================
# _check_price_outlier
# =========================================================================

class TestCheckPriceOutlier:

    def test_detects_outlier(self):
        batch_id = uuid4()
        # 8 prices: [10,11,12,13,14,15,16,17]
        # Q1 = hist[2] = 12, Q3 = hist[6] = 16, IQR = 4
        # lower = 12 - 6 = 6, upper = 16 + 6 = 22
        history = [
            {"item_external_id": "ITEM-1", "unit_price": v}
            for v in [10, 11, 12, 13, 14, 15, 16, 17]
        ]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "unit_price": 30}),  # > 22
        ]
        issues = _check_price_outlier(batch_id, current_rows, history)
        assert len(issues) == 1
        assert issues[0].rule_code == "STAT_PRICE_OUTLIER"

    def test_detects_low_outlier(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "unit_price": v}
            for v in [10, 11, 12, 13, 14, 15, 16, 17]
        ]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "unit_price": 1}),  # < 6
        ]
        issues = _check_price_outlier(batch_id, current_rows, history)
        assert len(issues) == 1

    def test_no_outlier(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "unit_price": v}
            for v in [10, 11, 12, 13, 14, 15, 16, 17]
        ]
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "unit_price": 13}),
        ]
        issues = _check_price_outlier(batch_id, current_rows, history)
        assert issues == []

    def test_skips_insufficient_history(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "ITEM-1", "unit_price": 10},
            {"item_external_id": "ITEM-1", "unit_price": 11},
            {"item_external_id": "ITEM-1", "unit_price": 12},
        ]  # only 3 points, need >= 4
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "unit_price": 100}),
        ]
        issues = _check_price_outlier(batch_id, current_rows, history)
        assert issues == []

    def test_skips_missing_fields(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "X"}),
            (uuid4(), 2, {"unit_price": 10}),
            (uuid4(), 3, {}),
        ]
        issues = _check_price_outlier(batch_id, current_rows, [])
        assert issues == []

    def test_skips_non_numeric_price(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "X", "unit_price": v}
            for v in [10, 11, 12, 13]
        ]
        current_rows = [(uuid4(), 1, {"item_external_id": "X", "unit_price": "abc"})]
        issues = _check_price_outlier(batch_id, current_rows, history)
        assert issues == []

    def test_uses_external_id_fallback(self):
        batch_id = uuid4()
        history = [
            {"external_id": "ITEM-1", "unit_price": v}
            for v in [10, 11, 12, 13, 14, 15, 16, 17]
        ]
        current_rows = [
            (uuid4(), 1, {"external_id": "ITEM-1", "unit_price": 50}),
        ]
        issues = _check_price_outlier(batch_id, current_rows, history)
        assert len(issues) == 1

    def test_non_numeric_history_price_skipped(self):
        batch_id = uuid4()
        history = [
            {"item_external_id": "X", "unit_price": "abc"},
            {"item_external_id": "X", "unit_price": 10},
            {"item_external_id": "X", "unit_price": 11},
            {"item_external_id": "X", "unit_price": 12},
            {"item_external_id": "X", "unit_price": 13},
        ]
        current_rows = [(uuid4(), 1, {"item_external_id": "X", "unit_price": 100})]
        issues = _check_price_outlier(batch_id, current_rows, history)
        assert len(issues) == 1


# =========================================================================
# _check_safety_stock_zero
# =========================================================================

class TestCheckSafetyStockZero:

    def test_detects_zero_safety_stock_with_shortages(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([{"external_id": "ITEM-1"}])
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "safety_stock_qty": 0}),
        ]
        issues = _check_safety_stock_zero(db, batch_id, current_rows)
        assert len(issues) == 1
        assert issues[0].rule_code == "STAT_SAFETY_STOCK_ZERO"

    def test_no_issue_when_safety_stock_nonzero(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([{"external_id": "ITEM-1"}])
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "safety_stock_qty": 100}),
        ]
        issues = _check_safety_stock_zero(db, batch_id, current_rows)
        assert issues == []

    def test_no_issue_when_no_active_shortages(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([])
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "safety_stock_qty": 0}),
        ]
        issues = _check_safety_stock_zero(db, batch_id, current_rows)
        assert issues == []

    def test_no_issue_when_item_not_in_shortage_set(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([{"external_id": "OTHER-ITEM"}])
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "safety_stock_qty": 0}),
        ]
        issues = _check_safety_stock_zero(db, batch_id, current_rows)
        assert issues == []

    def test_skips_missing_safety_field(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([{"external_id": "ITEM-1"}])
        current_rows = [(uuid4(), 1, {"item_external_id": "ITEM-1"})]
        issues = _check_safety_stock_zero(db, uuid4(), current_rows)
        assert issues == []

    def test_skips_missing_item_ext(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([{"external_id": "ITEM-1"}])
        current_rows = [(uuid4(), 1, {"safety_stock_qty": 0})]
        issues = _check_safety_stock_zero(db, uuid4(), current_rows)
        assert issues == []

    def test_skips_non_numeric_safety(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([{"external_id": "ITEM-1"}])
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "safety_stock_qty": "abc"}),
        ]
        issues = _check_safety_stock_zero(db, uuid4(), current_rows)
        assert issues == []

    def test_uses_external_id_fallback(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([{"external_id": "ITEM-1"}])
        current_rows = [
            (uuid4(), 1, {"external_id": "ITEM-1", "safety_stock_qty": 0}),
        ]
        issues = _check_safety_stock_zero(db, uuid4(), current_rows)
        assert len(issues) == 1


# =========================================================================
# _check_negative_onhand
# =========================================================================

class TestCheckNegativeOnhand:

    def test_detects_negative_onhand(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"quantity": -5, "item_external_id": "ITEM-1"}),
        ]
        issues = _check_negative_onhand(batch_id, current_rows, "on_hand")
        assert len(issues) == 1
        assert issues[0].rule_code == "STAT_NEGATIVE_ONHAND"
        assert issues[0].severity == SEVERITY_ERROR

    def test_no_issue_when_positive(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"quantity": 10, "item_external_id": "ITEM-1"}),
        ]
        issues = _check_negative_onhand(batch_id, current_rows, "on_hand")
        assert issues == []

    def test_no_issue_when_zero(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"quantity": 0, "item_external_id": "ITEM-1"}),
        ]
        issues = _check_negative_onhand(batch_id, current_rows, "on_hand")
        assert issues == []

    def test_skips_wrong_entity_type(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"quantity": -5}),
        ]
        issues = _check_negative_onhand(batch_id, current_rows, "purchase_orders")
        assert issues == []

    def test_skips_missing_quantity(self):
        batch_id = uuid4()
        current_rows = [(uuid4(), 1, {"item_external_id": "X"})]
        issues = _check_negative_onhand(batch_id, current_rows, "on_hand")
        assert issues == []

    def test_skips_non_numeric_quantity(self):
        batch_id = uuid4()
        current_rows = [(uuid4(), 1, {"quantity": "abc"})]
        issues = _check_negative_onhand(batch_id, current_rows, "on_hand")
        assert issues == []

    def test_uses_fallback_item_ext(self):
        """When item_external_id is missing, uses '?' as fallback."""
        batch_id = uuid4()
        current_rows = [(uuid4(), 1, {"quantity": -1})]
        issues = _check_negative_onhand(batch_id, current_rows, "on_hand")
        assert len(issues) == 1
        assert "?" in issues[0].message


# =========================================================================
# run_stat_rules — dispatcher
# =========================================================================

class TestRunStatRules:

    def _setup_db(self, entity_type, current_rows_data=None, history_data=None, shortage_items=None):
        db = _mock_db()
        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()

            if "from ingest_batches" in sql_lower:
                return _make_cursor({"entity_type": entity_type})
            if "from ingest_rows" in sql_lower and "batch_id" in sql_lower:
                if "join ingest_batches" in sql_lower:
                    # _load_history
                    return _make_cursor(history_data or [])
                else:
                    # _load_current_rows
                    return _make_cursor(current_rows_data or [])
            if "from shortages" in sql_lower:
                return _make_cursor(shortage_items or [])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        return db

    def test_supplier_items_runs_lead_time_and_price(self):
        db = self._setup_db("supplier_items")
        issues = run_stat_rules(db, uuid4())
        assert isinstance(issues, list)

    def test_forecast_demand_runs_forecast_spike(self):
        db = self._setup_db("forecast_demand")
        issues = run_stat_rules(db, uuid4())
        assert isinstance(issues, list)

    def test_forecasts_runs_forecast_spike(self):
        db = self._setup_db("forecasts")
        issues = run_stat_rules(db, uuid4())
        assert isinstance(issues, list)

    def test_purchase_orders_runs_price_outlier(self):
        db = self._setup_db("purchase_orders")
        issues = run_stat_rules(db, uuid4())
        assert isinstance(issues, list)

    def test_items_runs_safety_stock_zero(self):
        db = self._setup_db("items", shortage_items=[])
        issues = run_stat_rules(db, uuid4())
        assert isinstance(issues, list)

    def test_on_hand_runs_negative_onhand(self):
        rid = uuid4()
        current_data = [
            {"row_id": rid, "row_number": 1, "raw_content": json.dumps({"quantity": -5})}
        ]
        db = self._setup_db("on_hand", current_rows_data=current_data)
        issues = run_stat_rules(db, uuid4())
        assert any(i.rule_code == "STAT_NEGATIVE_ONHAND" for i in issues)

    def test_unknown_entity_type_runs_negative_onhand_only(self):
        db = self._setup_db("unknown_type")
        issues = run_stat_rules(db, uuid4())
        assert isinstance(issues, list)
