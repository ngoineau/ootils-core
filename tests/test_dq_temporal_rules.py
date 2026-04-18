"""
Tests for temporal_rules.py.

Covers: run_temporal_rules, _load_batch_rows, _get_previous_batch_id, _get_entity_type,
_row_fingerprint, _check_duplicate_batch, _check_po_date_past,
_check_forecast_horizon_short, _check_mass_change.
All branches including skip conditions, date parsing, edge cases.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from unittest.mock import MagicMock
from uuid import uuid4

from ootils_core.engine.dq.agent.temporal_rules import (
    run_temporal_rules,
    _load_batch_rows,
    _get_previous_batch_id,
    _get_entity_type,
    _row_fingerprint,
    _check_duplicate_batch,
    _check_po_date_past,
    _check_forecast_horizon_short,
    _check_mass_change,
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
        db.execute.return_value = _make_cursor({"entity_type": "purchase_orders"})
        assert _get_entity_type(db, uuid4()) == "purchase_orders"

    def test_returns_empty_when_not_found(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        assert _get_entity_type(db, uuid4()) == ""


# =========================================================================
# _load_batch_rows
# =========================================================================

class TestLoadBatchRows:

    def test_loads_json_strings(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": json.dumps({"a": 1})},
            {"raw_content": json.dumps({"b": 2})},
        ])
        result = _load_batch_rows(db, uuid4())
        assert len(result) == 2

    def test_loads_dict_content(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": {"c": 3}},
        ])
        result = _load_batch_rows(db, uuid4())
        assert result == [{"c": 3}]

    def test_skips_invalid_json(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": "bad"},
            {"raw_content": json.dumps({"ok": True})},
        ])
        result = _load_batch_rows(db, uuid4())
        assert len(result) == 1

    def test_skips_non_dict(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor([
            {"raw_content": json.dumps([1, 2])},
        ])
        result = _load_batch_rows(db, uuid4())
        assert result == []


# =========================================================================
# _get_previous_batch_id
# =========================================================================

class TestGetPreviousBatchId:

    def test_query_uses_batch_status_and_runtime_timestamps(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)

        _get_previous_batch_id(db, "items", uuid4())

        sql = db.execute.call_args[0][0]
        assert "status IN ('validated', 'rejected', 'imported', 'partial')" in sql
        assert "COALESCE(imported_at, processed_at, submitted_at)" in sql
        assert "dq_status" not in sql
        assert "created_at" not in sql

    def test_returns_previous_batch(self):
        db = _mock_db()
        prev_id = uuid4()
        db.execute.return_value = _make_cursor({"batch_id": str(prev_id)})
        result = _get_previous_batch_id(db, "items", uuid4())
        assert result == prev_id

    def test_returns_none_when_no_previous(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        result = _get_previous_batch_id(db, "items", uuid4())
        assert result is None


# =========================================================================
# _row_fingerprint
# =========================================================================

class TestRowFingerprint:

    def test_creates_frozenset(self):
        fp = _row_fingerprint({"a": 1, "b": "hello"})
        assert isinstance(fp, frozenset)
        assert ("a", "1") in fp
        assert ("b", "hello") in fp

    def test_empty_dict(self):
        fp = _row_fingerprint({})
        assert fp == frozenset()


# =========================================================================
# _check_duplicate_batch
# =========================================================================

class TestCheckDuplicateBatch:

    def test_detects_duplicate(self):
        db = _mock_db()
        batch_id = uuid4()
        prev_id = uuid4()

        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                # Same rows in both batches
                rows = [
                    {"raw_content": json.dumps({"item": "A", "qty": 10})},
                    {"raw_content": json.dumps({"item": "B", "qty": 20})},
                ]
                return _make_cursor(rows)
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_duplicate_batch(db, batch_id, "items")
        assert len(issues) == 1
        assert issues[0].rule_code == "TEMP_DUPLICATE_BATCH"

    def test_no_duplicate(self):
        db = _mock_db()
        batch_id = uuid4()
        prev_id = uuid4()

        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                if call_count["n"] == 2:
                    # Current batch
                    return _make_cursor([
                        {"raw_content": json.dumps({"item": "A", "qty": 10})},
                        {"raw_content": json.dumps({"item": "B", "qty": 20})},
                    ])
                else:
                    # Previous batch - different
                    return _make_cursor([
                        {"raw_content": json.dumps({"item": "C", "qty": 30})},
                        {"raw_content": json.dumps({"item": "D", "qty": 40})},
                    ])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_duplicate_batch(db, batch_id, "items")
        assert issues == []

    def test_no_previous_batch(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        issues = _check_duplicate_batch(db, uuid4(), "items")
        assert issues == []

    def test_empty_current_rows(self):
        db = _mock_db()
        prev_id = uuid4()

        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                if call_count["n"] == 2:
                    return _make_cursor([])  # empty current
                return _make_cursor([{"raw_content": json.dumps({"a": 1})}])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_duplicate_batch(db, uuid4(), "items")
        assert issues == []

    def test_empty_previous_rows(self):
        db = _mock_db()
        prev_id = uuid4()

        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                if call_count["n"] == 2:
                    return _make_cursor([{"raw_content": json.dumps({"a": 1})}])
                return _make_cursor([])  # empty prev
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_duplicate_batch(db, uuid4(), "items")
        assert issues == []


# =========================================================================
# _check_po_date_past
# =========================================================================

class TestCheckPoDatePast:

    def test_detects_past_date(self):
        batch_id = uuid4()
        past_date = (date.today() - timedelta(days=10)).isoformat()
        current_rows = [
            (uuid4(), 1, {
                "status": "pending",
                "expected_delivery_date": past_date,
            }),
        ]
        issues = _check_po_date_past(batch_id, current_rows, "purchase_orders")
        assert len(issues) == 1
        assert issues[0].rule_code == "TEMP_PO_DATE_PAST"

    def test_no_issue_when_received(self):
        batch_id = uuid4()
        past_date = (date.today() - timedelta(days=10)).isoformat()
        current_rows = [
            (uuid4(), 1, {
                "status": "received",
                "expected_delivery_date": past_date,
            }),
        ]
        issues = _check_po_date_past(batch_id, current_rows, "purchase_orders")
        assert issues == []

    def test_no_issue_when_future_date(self):
        batch_id = uuid4()
        future_date = (date.today() + timedelta(days=10)).isoformat()
        current_rows = [
            (uuid4(), 1, {
                "status": "pending",
                "expected_delivery_date": future_date,
            }),
        ]
        issues = _check_po_date_past(batch_id, current_rows, "purchase_orders")
        assert issues == []

    def test_skips_wrong_entity_type(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"status": "pending", "expected_delivery_date": "2020-01-01"}),
        ]
        issues = _check_po_date_past(batch_id, current_rows, "items")
        assert issues == []

    def test_skips_missing_date(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"status": "pending"}),
        ]
        issues = _check_po_date_past(batch_id, current_rows, "purchase_orders")
        assert issues == []

    def test_skips_invalid_date_format(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"status": "pending", "expected_delivery_date": "not-a-date"}),
        ]
        issues = _check_po_date_past(batch_id, current_rows, "purchase_orders")
        assert issues == []

    def test_handles_date_object(self):
        batch_id = uuid4()
        past = date.today() - timedelta(days=5)
        current_rows = [
            (uuid4(), 1, {"status": "open", "expected_delivery_date": past}),
        ]
        issues = _check_po_date_past(batch_id, current_rows, "purchase_orders")
        assert len(issues) == 1

    def test_handles_non_string_non_date(self):
        batch_id = uuid4()
        current_rows = [
            (uuid4(), 1, {"status": "pending", "expected_delivery_date": 12345}),
        ]
        issues = _check_po_date_past(batch_id, current_rows, "purchase_orders")
        assert issues == []

    def test_default_status_empty_string(self):
        """When status key is missing, defaults to empty string (not 'received')."""
        batch_id = uuid4()
        past_date = (date.today() - timedelta(days=10)).isoformat()
        current_rows = [
            (uuid4(), 1, {"expected_delivery_date": past_date}),
        ]
        issues = _check_po_date_past(batch_id, current_rows, "purchase_orders")
        assert len(issues) == 1


# =========================================================================
# _check_forecast_horizon_short
# =========================================================================

class TestCheckForecastHorizonShort:

    def test_detects_short_horizon(self):
        db = _mock_db()
        batch_id = uuid4()

        # max_lead_time = 30 days
        # bucket_date only 10 days from now -> horizon < 30
        bucket_date = (date.today() + timedelta(days=10)).isoformat()

        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "bucket_date": bucket_date}),
        ]

        def execute_side_effect(sql, params=None):
            sql_lower = sql.strip().lower()
            if "max(si.lead_time_days)" in sql_lower:
                return _make_cursor({"max_lt": 30})
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_forecast_horizon_short(db, batch_id, current_rows, "forecast_demand")
        assert len(issues) == 1
        assert issues[0].rule_code == "TEMP_FORECAST_HORIZON_SHORT"

    def test_no_issue_sufficient_horizon(self):
        db = _mock_db()
        batch_id = uuid4()
        bucket_date = (date.today() + timedelta(days=60)).isoformat()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "ITEM-1", "bucket_date": bucket_date}),
        ]

        def execute_side_effect(sql, params=None):
            sql_lower = sql.strip().lower()
            if "max(si.lead_time_days)" in sql_lower:
                return _make_cursor({"max_lt": 30})
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_forecast_horizon_short(db, batch_id, current_rows, "forecast_demand")
        assert issues == []

    def test_skips_wrong_entity_type(self):
        db = _mock_db()
        issues = _check_forecast_horizon_short(db, uuid4(), [], "items")
        assert issues == []

    def test_skips_no_item_ext_ids(self):
        db = _mock_db()
        current_rows = [(uuid4(), 1, {"bucket_date": "2024-12-01"})]
        issues = _check_forecast_horizon_short(db, uuid4(), current_rows, "forecast_demand")
        assert issues == []

    def test_skips_no_lead_time_data(self):
        db = _mock_db()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "X", "bucket_date": "2024-12-01"}),
        ]
        db.execute.return_value = _make_cursor({"max_lt": None})
        issues = _check_forecast_horizon_short(db, uuid4(), current_rows, "forecasts")
        assert issues == []

    def test_skips_no_bucket_dates(self):
        db = _mock_db()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "X"}),
        ]

        def execute_side_effect(sql, params=None):
            sql_lower = sql.strip().lower()
            if "max(si.lead_time_days)" in sql_lower:
                return _make_cursor({"max_lt": 30})
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_forecast_horizon_short(db, uuid4(), current_rows, "forecast_demand")
        assert issues == []

    def test_skips_invalid_bucket_date(self):
        db = _mock_db()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "X", "bucket_date": "not-a-date"}),
        ]

        def execute_side_effect(sql, params=None):
            sql_lower = sql.strip().lower()
            if "max(si.lead_time_days)" in sql_lower:
                return _make_cursor({"max_lt": 30})
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_forecast_horizon_short(db, uuid4(), current_rows, "forecast_demand")
        assert issues == []

    def test_handles_date_object_bucket(self):
        db = _mock_db()
        batch_id = uuid4()
        bucket = date.today() + timedelta(days=5)
        current_rows = [
            (uuid4(), 1, {"item_external_id": "X", "bucket_date": bucket}),
        ]

        def execute_side_effect(sql, params=None):
            sql_lower = sql.strip().lower()
            if "max(si.lead_time_days)" in sql_lower:
                return _make_cursor({"max_lt": 30})
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_forecast_horizon_short(db, batch_id, current_rows, "forecast_demand")
        assert len(issues) == 1

    def test_max_bucket_from_multiple_rows(self):
        db = _mock_db()
        batch_id = uuid4()
        near = (date.today() + timedelta(days=5)).isoformat()
        far = (date.today() + timedelta(days=60)).isoformat()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "X", "bucket_date": near}),
            (uuid4(), 2, {"item_external_id": "X", "bucket_date": far}),
        ]

        def execute_side_effect(sql, params=None):
            sql_lower = sql.strip().lower()
            if "max(si.lead_time_days)" in sql_lower:
                return _make_cursor({"max_lt": 30})
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_forecast_horizon_short(db, batch_id, current_rows, "forecast_demand")
        # max_bucket = far (60 days) > 30 -> no issue
        assert issues == []

    def test_no_row_for_lead_time(self):
        """When the lead time query returns None row."""
        db = _mock_db()
        current_rows = [
            (uuid4(), 1, {"item_external_id": "X", "bucket_date": "2024-12-01"}),
        ]
        db.execute.return_value = _make_cursor(None)
        issues = _check_forecast_horizon_short(db, uuid4(), current_rows, "forecasts")
        assert issues == []


# =========================================================================
# _check_mass_change
# =========================================================================

class TestCheckMassChange:

    def test_detects_mass_change(self):
        db = _mock_db()
        batch_id = uuid4()
        prev_id = uuid4()

        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower and "entity_type" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                if call_count["n"] == 2:
                    # Current batch - field "qty" changed for all items
                    return _make_cursor([
                        {"raw_content": json.dumps({"external_id": "A", "qty": 999})},
                        {"raw_content": json.dumps({"external_id": "B", "qty": 999})},
                    ])
                else:
                    # Previous batch
                    return _make_cursor([
                        {"raw_content": json.dumps({"external_id": "A", "qty": 1})},
                        {"raw_content": json.dumps({"external_id": "B", "qty": 2})},
                    ])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_mass_change(db, batch_id, "items")
        # "qty" changed 100% > 30%
        qty_issues = [i for i in issues if i.field_name == "qty"]
        assert len(qty_issues) == 1
        assert qty_issues[0].rule_code == "TEMP_MASS_CHANGE"

    def test_no_mass_change(self):
        db = _mock_db()
        batch_id = uuid4()
        prev_id = uuid4()

        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                # Same data in both batches
                return _make_cursor([
                    {"raw_content": json.dumps({"external_id": "A", "qty": 10})},
                    {"raw_content": json.dumps({"external_id": "B", "qty": 20})},
                ])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_mass_change(db, batch_id, "items")
        assert issues == []

    def test_no_previous_batch(self):
        db = _mock_db()
        db.execute.return_value = _make_cursor(None)
        issues = _check_mass_change(db, uuid4(), "items")
        assert issues == []

    def test_empty_current_rows(self):
        db = _mock_db()
        prev_id = uuid4()
        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                if call_count["n"] == 2:
                    return _make_cursor([])  # empty current
                return _make_cursor([{"raw_content": json.dumps({"a": 1})}])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_mass_change(db, uuid4(), "items")
        assert issues == []

    def test_empty_previous_rows(self):
        db = _mock_db()
        prev_id = uuid4()
        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                if call_count["n"] == 2:
                    return _make_cursor([{"raw_content": json.dumps({"a": 1})}])
                return _make_cursor([])  # empty prev
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_mass_change(db, uuid4(), "items")
        assert issues == []

    def test_no_common_keys(self):
        """When external_ids don't overlap, change ratio is 0 or skipped."""
        db = _mock_db()
        prev_id = uuid4()
        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                if call_count["n"] == 2:
                    return _make_cursor([
                        {"raw_content": json.dumps({"external_id": "A", "qty": 10})},
                    ])
                return _make_cursor([
                    {"raw_content": json.dumps({"external_id": "B", "qty": 20})},
                ])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_mass_change(db, uuid4(), "items")
        # No common keys -> no mass change detected
        # But "external_id" field has common key index "0" (when no external_id, uses idx)
        # Actually both rows have external_id so keys are "A" and "B" - no overlap
        # Wait: the function uses str(r.get("external_id") or r.get("item_external_id") or idx)
        # So keys are "A" for current and "B" for prev -> no common keys
        # However, there could be a common key for the index-based fallback for other fields
        # In this case, the only field without "external_id" is "qty"
        # For "qty": current_map["A"] = "10", prev_map["B"] = "20" -> no common keys
        # For "external_id": current_map["A"] = "A", prev_map["B"] = "B" -> no common keys
        assert issues == []

    def test_uses_item_external_id_as_key(self):
        db = _mock_db()
        prev_id = uuid4()
        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                if call_count["n"] == 2:
                    return _make_cursor([
                        {"raw_content": json.dumps({"item_external_id": "A", "qty": 999})},
                    ])
                return _make_cursor([
                    {"raw_content": json.dumps({"item_external_id": "A", "qty": 1})},
                ])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_mass_change(db, uuid4(), "items")
        qty_issues = [i for i in issues if i.field_name == "qty"]
        assert len(qty_issues) == 1

    def test_uses_index_as_key_fallback(self):
        """When neither external_id nor item_external_id, use row index."""
        db = _mock_db()
        prev_id = uuid4()
        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()
            if "from ingest_batches" in sql_lower:
                return _make_cursor({"batch_id": str(prev_id)})
            if "from ingest_rows" in sql_lower:
                if call_count["n"] == 2:
                    return _make_cursor([
                        {"raw_content": json.dumps({"qty": 999})},
                    ])
                return _make_cursor([
                    {"raw_content": json.dumps({"qty": 1})},
                ])
            return _make_cursor([])

        db.execute.side_effect = execute_side_effect
        issues = _check_mass_change(db, uuid4(), "items")
        # Key "0" is common. qty changed 100% > 30%
        qty_issues = [i for i in issues if i.field_name == "qty"]
        assert len(qty_issues) == 1


# =========================================================================
# run_temporal_rules — dispatcher
# =========================================================================

class TestRunTemporalRules:

    def _setup_db(self, entity_type, raw_rows=None):
        db = _mock_db()
        call_count = {"n": 0}

        def execute_side_effect(sql, params=None):
            call_count["n"] += 1
            sql_lower = sql.strip().lower()

            if "from ingest_batches" in sql_lower and "entity_type" in sql_lower:
                # Could be _get_entity_type or _get_previous_batch_id
                if "select entity_type" in sql_lower:
                    return _make_cursor({"entity_type": entity_type})
                else:
                    return _make_cursor(None)  # no previous batch
            if "from ingest_rows" in sql_lower and "batch_id" in sql_lower:
                return _make_cursor(raw_rows or [])
            return _make_cursor(None)

        db.execute.side_effect = execute_side_effect
        return db

    def test_runs_all_rules(self):
        db = self._setup_db("purchase_orders")
        issues = run_temporal_rules(db, uuid4())
        assert isinstance(issues, list)

    def test_parses_current_rows(self):
        rid = uuid4()
        raw_rows = [
            {
                "row_id": rid,
                "row_number": 1,
                "raw_content": json.dumps({
                    "status": "pending",
                    "expected_delivery_date": (date.today() - timedelta(days=5)).isoformat(),
                }),
            }
        ]
        db = self._setup_db("purchase_orders", raw_rows=raw_rows)
        issues = run_temporal_rules(db, uuid4())
        po_issues = [i for i in issues if i.rule_code == "TEMP_PO_DATE_PAST"]
        assert len(po_issues) == 1

    def test_skips_invalid_json_in_rows(self):
        raw_rows = [
            {"row_id": uuid4(), "row_number": 1, "raw_content": "bad json"},
        ]
        db = self._setup_db("items", raw_rows=raw_rows)
        issues = run_temporal_rules(db, uuid4())
        assert isinstance(issues, list)

    def test_skips_non_dict_rows(self):
        raw_rows = [
            {"row_id": uuid4(), "row_number": 1, "raw_content": json.dumps("string")},
        ]
        db = self._setup_db("items", raw_rows=raw_rows)
        issues = run_temporal_rules(db, uuid4())
        assert isinstance(issues, list)

    def test_handles_dict_content(self):
        rid = uuid4()
        raw_rows = [
            {"row_id": rid, "row_number": 1, "raw_content": {"qty": 10}},
        ]
        db = self._setup_db("items", raw_rows=raw_rows)
        issues = run_temporal_rules(db, uuid4())
        assert isinstance(issues, list)
