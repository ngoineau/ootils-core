"""
Tests for ootils_core.engine.dq.engine — DQ pipeline.

Covers:
- run_dq happy paths for each entity_type
- L1 checks for every type_check (numeric_positive, numeric_nonneg, int_positive,
  str+max_len, date, uuid, missing mandatory)
- L2 referential checks with batch FK resolution
- _persist_issues, _update_row_statuses, _update_batch_status helpers
- Empty batch path
- Missing batch raises ValueError
- JSON parse error -> L1_INVALID_FORMAT
- DQ agent auto-trigger exception is swallowed
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest

from ootils_core.engine.dq import engine as dq_engine
from ootils_core.engine.dq.engine import (
    DQIssue,
    DQResult,
    _check_l1,
    _check_l2,
    _persist_issues,
    _update_row_statuses,
    _update_batch_status,
    _batch_resolve_items,
    _batch_resolve_locations,
    _batch_resolve_suppliers,
    run_dq,
)


# ---------------------------------------------------------------------------
# Fake db / cursor helpers
# ---------------------------------------------------------------------------

class _CursorCM:
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


def _make_db(execute_handler=None):
    """
    Build a MagicMock psycopg connection.

    `execute_handler(sql, params)` returns a list[dict] (fetchall) or dict (fetchone)
    or None. The returned object also supports .fetchone() / .fetchall().
    """
    db = MagicMock(name="db")

    def _exec(sql, params=None):
        result = MagicMock()
        if execute_handler is None:
            result.fetchone.return_value = None
            result.fetchall.return_value = []
            return result
        rv = execute_handler(sql, params)
        if isinstance(rv, list):
            result.fetchall.return_value = rv
            result.fetchone.return_value = rv[0] if rv else None
        elif isinstance(rv, dict):
            result.fetchone.return_value = rv
            result.fetchall.return_value = [rv]
        else:
            result.fetchone.return_value = None
            result.fetchall.return_value = []
        return result

    db.execute.side_effect = _exec
    db.cursor.return_value = _CursorCM(MagicMock(name="cursor"))
    return db


# ---------------------------------------------------------------------------
# _check_l1 — every branch
# ---------------------------------------------------------------------------

def test_l1_items_clean():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "external_id": "I1",
        "name": "Widget",
        "item_type": "RAW",
        "uom": "EA",
        "status": "active",
    }, "items")
    assert issues == []


def test_l1_missing_mandatory_field():
    rid = uuid4()
    issues = _check_l1(rid, 1, {"external_id": "X"}, "items")
    assert any(i.rule_code == "L1_MISSING_FIELD" and i.field_name == "name" for i in issues)


def test_l1_empty_string_treated_as_missing():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "external_id": "X", "name": "", "item_type": "RAW", "uom": "EA", "status": "active"
    }, "items")
    assert any(i.field_name == "name" and i.rule_code == "L1_MISSING_FIELD" for i in issues)


def test_l1_str_too_long():
    rid = uuid4()
    long = "x" * 300
    issues = _check_l1(rid, 1, {
        "external_id": long, "name": "n", "item_type": "RAW", "uom": "EA", "status": "active"
    }, "items")
    assert any(i.rule_code == "L1_INVALID_FORMAT" and i.field_name == "external_id" for i in issues)


def test_l1_numeric_positive_valid():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "external_id": "PO1",
        "item_external_id": "I1",
        "location_external_id": "L1",
        "supplier_external_id": "S1",
        "quantity": 10,
        "uom": "EA",
        "expected_delivery_date": "2026-04-01",
        "status": "open",
    }, "purchase_orders")
    assert issues == []


def test_l1_numeric_positive_zero_fails():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "external_id": "PO1", "item_external_id": "I1", "location_external_id": "L1",
        "supplier_external_id": "S1", "quantity": 0, "uom": "EA",
        "expected_delivery_date": "2026-04-01", "status": "open",
    }, "purchase_orders")
    assert any(i.field_name == "quantity" and i.rule_code == "L1_INVALID_TYPE" for i in issues)


def test_l1_numeric_positive_negative_fails():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "external_id": "PO1", "item_external_id": "I1", "location_external_id": "L1",
        "supplier_external_id": "S1", "quantity": -3, "uom": "EA",
        "expected_delivery_date": "2026-04-01", "status": "open",
    }, "purchase_orders")
    assert any(i.field_name == "quantity" for i in issues)


def test_l1_numeric_positive_non_numeric_fails():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "external_id": "PO1", "item_external_id": "I1", "location_external_id": "L1",
        "supplier_external_id": "S1", "quantity": "abc", "uom": "EA",
        "expected_delivery_date": "2026-04-01", "status": "open",
    }, "purchase_orders")
    assert any(i.field_name == "quantity" and i.rule_code == "L1_INVALID_TYPE" for i in issues)


def test_l1_numeric_nonneg_zero_ok():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "item_external_id": "I1", "location_external_id": "L1",
        "quantity": 0, "uom": "EA", "as_of_date": "2026-04-01",
    }, "on_hand")
    assert issues == []


def test_l1_numeric_nonneg_negative_fails():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "item_external_id": "I1", "location_external_id": "L1",
        "quantity": -1, "uom": "EA", "as_of_date": "2026-04-01",
    }, "on_hand")
    assert any(i.field_name == "quantity" for i in issues)


def test_l1_numeric_nonneg_non_numeric_fails():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "item_external_id": "I1", "location_external_id": "L1",
        "quantity": "xyz", "uom": "EA", "as_of_date": "2026-04-01",
    }, "on_hand")
    assert any(i.field_name == "quantity" and i.rule_code == "L1_INVALID_TYPE" for i in issues)


def test_l1_int_positive_valid():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "supplier_external_id": "S1", "item_external_id": "I1", "lead_time_days": 7
    }, "supplier_items")
    assert issues == []


def test_l1_int_positive_zero_fails():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "supplier_external_id": "S1", "item_external_id": "I1", "lead_time_days": 0
    }, "supplier_items")
    assert any(i.field_name == "lead_time_days" for i in issues)


def test_l1_int_positive_non_numeric_fails():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "supplier_external_id": "S1", "item_external_id": "I1", "lead_time_days": "abc"
    }, "supplier_items")
    assert any(i.field_name == "lead_time_days" for i in issues)


def test_l1_date_valid():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "item_external_id": "I1", "location_external_id": "L1",
        "quantity": 5, "bucket_date": "2026-04-01", "time_grain": "day",
    }, "forecast_demand")
    assert issues == []


def test_l1_date_bad_format():
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "item_external_id": "I1", "location_external_id": "L1",
        "quantity": 5, "bucket_date": "04/01/2026", "time_grain": "day",
    }, "forecast_demand")
    assert any(i.field_name == "bucket_date" and i.rule_code == "L1_INVALID_TYPE" for i in issues)


def test_l1_date_invalid_calendar_date():
    """Matches regex but Feb 30 is not a real date."""
    rid = uuid4()
    issues = _check_l1(rid, 1, {
        "item_external_id": "I1", "location_external_id": "L1",
        "quantity": 5, "bucket_date": "2026-02-30", "time_grain": "day",
    }, "forecast_demand")
    assert any(
        i.field_name == "bucket_date"
        and "valid calendar date" in i.message
        for i in issues
    )


def test_l1_uuid_valid():
    """Use a manual schema injection to exercise the uuid branch."""
    rid = uuid4()
    schema = [("ref", True, "uuid", None)]
    with patch.dict(dq_engine._SCHEMAS, {"_uuid_test": schema}, clear=False):
        issues = _check_l1(rid, 1, {"ref": str(uuid4())}, "_uuid_test")
    assert issues == []


def test_l1_uuid_invalid():
    rid = uuid4()
    schema = [("ref", True, "uuid", None)]
    with patch.dict(dq_engine._SCHEMAS, {"_uuid_test": schema}, clear=False):
        issues = _check_l1(rid, 1, {"ref": "not-a-uuid"}, "_uuid_test")
    assert any(i.rule_code == "L1_INVALID_TYPE" for i in issues)


def test_l1_unknown_entity_returns_empty():
    rid = uuid4()
    issues = _check_l1(rid, 1, {"foo": "bar"}, "no_such_type")
    assert issues == []


def test_l1_optional_field_none_skipped():
    rid = uuid4()
    schema = [("opt", False, "str", 10)]
    with patch.dict(dq_engine._SCHEMAS, {"_opt": schema}, clear=False):
        issues = _check_l1(rid, 1, {"opt": None}, "_opt")
    assert issues == []


# ---------------------------------------------------------------------------
# _batch_resolve_* helpers
# ---------------------------------------------------------------------------

def test_batch_resolve_items_empty():
    db = _make_db()
    assert _batch_resolve_items(db, []) == set()
    db.execute.assert_not_called()


def test_batch_resolve_items_returns_set():
    def handler(sql, params):
        return [{"external_id": "I1"}, {"external_id": "I2"}]
    db = _make_db(handler)
    result = _batch_resolve_items(db, ["I1", "I2", "I3"])
    assert result == {"I1", "I2"}


def test_batch_resolve_locations_empty():
    db = _make_db()
    assert _batch_resolve_locations(db, []) == set()


def test_batch_resolve_locations_returns_set():
    def handler(sql, params):
        return [{"external_id": "L1"}]
    db = _make_db(handler)
    assert _batch_resolve_locations(db, ["L1"]) == {"L1"}


def test_batch_resolve_suppliers_empty():
    db = _make_db()
    assert _batch_resolve_suppliers(db, []) == set()


def test_batch_resolve_suppliers_returns_set():
    def handler(sql, params):
        return [{"external_id": "S1"}]
    db = _make_db(handler)
    assert _batch_resolve_suppliers(db, ["S1"]) == {"S1"}


# ---------------------------------------------------------------------------
# _check_l2 — referential checks per entity type
# ---------------------------------------------------------------------------

def test_l2_purchase_orders_all_ok():
    rid = uuid4()

    def handler(sql, params):
        if "items" in sql:
            return [{"external_id": "I1"}]
        if "locations" in sql:
            return [{"external_id": "L1"}]
        if "suppliers" in sql:
            return [{"external_id": "S1"}]
        return []

    db = _make_db(handler)
    rows = [(rid, 1, {
        "item_external_id": "I1",
        "location_external_id": "L1",
        "supplier_external_id": "S1",
    })]
    issues = _check_l2(rows, "purchase_orders", db)
    assert issues == []


def test_l2_purchase_orders_unknown_refs():
    rid = uuid4()

    def handler(sql, params):
        return []  # nothing in any reference table

    db = _make_db(handler)
    rows = [(rid, 1, {
        "item_external_id": "I_MISSING",
        "location_external_id": "L_MISSING",
        "supplier_external_id": "S_MISSING",
    })]
    issues = _check_l2(rows, "purchase_orders", db)
    assert len(issues) == 3
    rule_codes = {i.rule_code for i in issues}
    assert rule_codes == {"L2_UNKNOWN_REF"}
    fields = {i.field_name for i in issues}
    assert fields == {"item_external_id", "location_external_id", "supplier_external_id"}


def test_l2_forecast_demand_unknown_refs():
    rid = uuid4()
    db = _make_db(lambda sql, p: [])
    rows = [(rid, 1, {
        "item_external_id": "I_MISSING",
        "location_external_id": "L_MISSING",
    })]
    issues = _check_l2(rows, "forecast_demand", db)
    assert len(issues) == 2


def test_l2_on_hand_unknown_refs():
    rid = uuid4()
    db = _make_db(lambda sql, p: [])
    rows = [(rid, 1, {
        "item_external_id": "I_MISSING",
        "location_external_id": "L_MISSING",
    })]
    issues = _check_l2(rows, "on_hand", db)
    assert len(issues) == 2


def test_l2_forecasts_alias_unknown_refs():
    rid = uuid4()
    db = _make_db(lambda sql, p: [])
    rows = [(rid, 1, {
        "item_external_id": "I_MISSING",
        "location_external_id": "L_MISSING",
    })]
    issues = _check_l2(rows, "forecasts", db)
    assert len(issues) == 2


def test_l2_supplier_items_unknown_refs():
    rid = uuid4()
    db = _make_db(lambda sql, p: [])
    rows = [(rid, 1, {
        "item_external_id": "I_MISSING",
        "supplier_external_id": "S_MISSING",
    })]
    issues = _check_l2(rows, "supplier_items", db)
    assert len(issues) == 2
    fields = {i.field_name for i in issues}
    assert fields == {"item_external_id", "supplier_external_id"}


def test_l2_items_no_referential_checks():
    """items, locations, suppliers are reference tables — no L2 checks."""
    rid = uuid4()
    db = _make_db(lambda sql, p: [])
    issues = _check_l2([(rid, 1, {"external_id": "X"})], "items", db)
    assert issues == []


# ---------------------------------------------------------------------------
# _persist_issues
# ---------------------------------------------------------------------------

def test_persist_issues_empty_noop():
    db = _make_db()
    _persist_issues(db, uuid4(), [])
    db.cursor.assert_not_called()


def test_persist_issues_inserts_all():
    db = _make_db()
    cursor_mock = MagicMock()
    db.cursor.return_value = _CursorCM(cursor_mock)

    issues = [
        DQIssue(
            row_id=uuid4(), row_number=1, dq_level=1,
            rule_code="L1_MISSING_FIELD", severity="error",
            field_name="name", raw_value=None, message="missing",
        ),
        DQIssue(
            row_id=uuid4(), row_number=2, dq_level=2,
            rule_code="L2_UNKNOWN_REF", severity="warning",
            field_name="item_external_id", raw_value="I1", message="not found",
        ),
    ]
    _persist_issues(db, uuid4(), issues)
    assert cursor_mock.execute.call_count == 2


# ---------------------------------------------------------------------------
# _update_row_statuses
# ---------------------------------------------------------------------------

def test_update_row_statuses_clean_pass():
    db = _make_db()
    rid = uuid4()
    _update_row_statuses(db, {}, [rid], {rid: 2})
    args = db.execute.call_args.args
    params = args[1]
    assert params[0] == "l2_pass"
    assert params[1] == 2
    assert params[2] == rid


def test_update_row_statuses_with_error_marks_rejected():
    db = _make_db()
    rid = uuid4()
    issue = DQIssue(
        row_id=rid, row_number=1, dq_level=1,
        rule_code="L1_MISSING_FIELD", severity="error",
        field_name="name", raw_value=None, message="missing",
    )
    _update_row_statuses(db, {rid: [issue]}, [rid], {rid: 1})
    params = db.execute.call_args.args[1]
    assert params[0] == "rejected"
    assert params[1] == 1


def test_update_row_statuses_with_warning_marks_l2_pass():
    db = _make_db()
    rid = uuid4()
    issue = DQIssue(
        row_id=rid, row_number=1, dq_level=2,
        rule_code="L2_UNKNOWN_REF", severity="warning",
        field_name="item_external_id", raw_value="I1", message="not found",
    )
    _update_row_statuses(db, {rid: [issue]}, [rid], {rid: 2})
    params = db.execute.call_args.args[1]
    assert params[0] == "l2_pass"


# ---------------------------------------------------------------------------
# _update_batch_status
# ---------------------------------------------------------------------------

def test_update_batch_status_clean_validated():
    db = _make_db()
    bid = uuid4()
    status = _update_batch_status(db, bid, [], 0)
    assert status == "validated"


def test_update_batch_status_with_warning_validated():
    db = _make_db()
    issues = [DQIssue(
        row_id=uuid4(), row_number=1, dq_level=2,
        rule_code="L2_UNKNOWN_REF", severity="warning",
        field_name="x", raw_value="x", message="x",
    )]
    status = _update_batch_status(db, uuid4(), issues, 1)
    assert status == "validated"


def test_update_batch_status_with_error_rejected():
    db = _make_db()
    issues = [DQIssue(
        row_id=uuid4(), row_number=1, dq_level=1,
        rule_code="L1_MISSING_FIELD", severity="error",
        field_name="x", raw_value=None, message="x",
    )]
    status = _update_batch_status(db, uuid4(), issues, 1)
    assert status == "rejected"


# ---------------------------------------------------------------------------
# run_dq — end-to-end with mocked db
# ---------------------------------------------------------------------------

def _build_run_dq_db(batch_id, entity_type, ingest_rows, ref_data=None):
    """
    ref_data: dict like {"items": ["I1"], "locations": ["L1"], "suppliers": ["S1"]}
    """
    ref_data = ref_data or {}

    def handler(sql, params=None):
        sql_low = sql.lower().strip()
        if "from ingest_batches" in sql_low and "select" in sql_low:
            return {"batch_id": batch_id, "entity_type": entity_type, "status": "uploaded"}
        if "from ingest_rows" in sql_low and "select" in sql_low:
            return list(ingest_rows)
        if "from items" in sql_low:
            return [{"external_id": eid} for eid in ref_data.get("items", [])]
        if "from locations" in sql_low:
            return [{"external_id": eid} for eid in ref_data.get("locations", [])]
        if "from suppliers" in sql_low:
            return [{"external_id": eid} for eid in ref_data.get("suppliers", [])]
        # UPDATE statements return None
        return None

    return _make_db(handler)


def test_run_dq_missing_batch_raises():
    db = _make_db(lambda s, p: None)
    with pytest.raises(ValueError, match="not found"):
        run_dq(db, uuid4())


def test_run_dq_empty_batch():
    bid = uuid4()
    db = _build_run_dq_db(bid, "items", [])
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert isinstance(result, DQResult)
    assert result.total_rows == 0
    assert result.batch_dq_status == "validated"
    assert result.issues == []


def test_run_dq_items_happy_path():
    bid = uuid4()
    rid = uuid4()
    rows = [{
        "row_id": rid, "row_number": 1,
        "raw_content": json.dumps({
            "external_id": "I1", "name": "Widget",
            "item_type": "RAW", "uom": "EA", "status": "active",
        }),
    }]
    db = _build_run_dq_db(bid, "items", rows)
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert result.total_rows == 1
    assert result.passed_rows == 1
    assert result.failed_rows == 0
    assert result.batch_dq_status == "validated"


def test_run_dq_locations_happy_path():
    bid = uuid4()
    rows = [{
        "row_id": uuid4(), "row_number": 1,
        "raw_content": json.dumps({
            "external_id": "L1", "name": "DC", "location_type": "DC",
        }),
    }]
    db = _build_run_dq_db(bid, "locations", rows)
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert result.passed_rows == 1


def test_run_dq_suppliers_happy_path():
    bid = uuid4()
    rows = [{
        "row_id": uuid4(), "row_number": 1,
        "raw_content": json.dumps({
            "external_id": "S1", "name": "Acme", "status": "active",
        }),
    }]
    db = _build_run_dq_db(bid, "suppliers", rows)
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert result.passed_rows == 1


def test_run_dq_purchase_orders_with_l2_resolution():
    bid = uuid4()
    rows = [{
        "row_id": uuid4(), "row_number": 1,
        "raw_content": json.dumps({
            "external_id": "PO1",
            "item_external_id": "I1",
            "location_external_id": "L1",
            "supplier_external_id": "S1",
            "quantity": 10, "uom": "EA",
            "expected_delivery_date": "2026-04-01",
            "status": "open",
        }),
    }]
    db = _build_run_dq_db(
        bid, "purchase_orders", rows,
        ref_data={"items": ["I1"], "locations": ["L1"], "suppliers": ["S1"]},
    )
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert result.passed_rows == 1
    assert result.failed_rows == 0


def test_run_dq_purchase_orders_l2_unknown_ref():
    bid = uuid4()
    rows = [{
        "row_id": uuid4(), "row_number": 1,
        "raw_content": json.dumps({
            "external_id": "PO1",
            "item_external_id": "I_MISSING",
            "location_external_id": "L_MISSING",
            "supplier_external_id": "S_MISSING",
            "quantity": 10, "uom": "EA",
            "expected_delivery_date": "2026-04-01",
            "status": "open",
        }),
    }]
    db = _build_run_dq_db(bid, "purchase_orders", rows, ref_data={})
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert result.failed_rows == 1
    assert result.batch_dq_status == "rejected"
    assert any(i.rule_code == "L2_UNKNOWN_REF" for i in result.issues)


def test_run_dq_forecast_demand_happy_path():
    bid = uuid4()
    rows = [{
        "row_id": uuid4(), "row_number": 1,
        "raw_content": json.dumps({
            "item_external_id": "I1",
            "location_external_id": "L1",
            "quantity": 5,
            "bucket_date": "2026-04-01",
            "time_grain": "day",
        }),
    }]
    db = _build_run_dq_db(
        bid, "forecast_demand", rows,
        ref_data={"items": ["I1"], "locations": ["L1"]},
    )
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert result.passed_rows == 1


def test_run_dq_on_hand_happy_path():
    bid = uuid4()
    rows = [{
        "row_id": uuid4(), "row_number": 1,
        "raw_content": json.dumps({
            "item_external_id": "I1",
            "location_external_id": "L1",
            "quantity": 0,
            "uom": "EA",
            "as_of_date": "2026-04-01",
        }),
    }]
    db = _build_run_dq_db(
        bid, "on_hand", rows,
        ref_data={"items": ["I1"], "locations": ["L1"]},
    )
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert result.passed_rows == 1


def test_run_dq_l1_failure_blocks_l2():
    """A row with an L1 error should not be sent to L2."""
    bid = uuid4()
    rows = [{
        "row_id": uuid4(), "row_number": 1,
        "raw_content": json.dumps({
            "external_id": "PO1",
            # Missing required fields -> L1 errors
            "quantity": 0,
        }),
    }]
    db = _build_run_dq_db(bid, "purchase_orders", rows, ref_data={})
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert result.failed_rows == 1
    assert any(i.dq_level == 1 for i in result.issues)


def test_run_dq_invalid_json_creates_parse_error():
    bid = uuid4()
    rows = [{
        "row_id": uuid4(), "row_number": 1,
        "raw_content": "not-json{",
    }]
    db = _build_run_dq_db(bid, "items", rows)
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert result.failed_rows == 1
    assert any(i.rule_code == "L1_INVALID_FORMAT" and i.message.startswith("raw_content") for i in result.issues)


def test_run_dq_non_dict_json_treated_as_empty():
    """If raw_content is JSON but not a dict, it becomes {} -> all mandatory fields missing."""
    bid = uuid4()
    rows = [{
        "row_id": uuid4(), "row_number": 1,
        "raw_content": json.dumps([1, 2, 3]),
    }]
    db = _build_run_dq_db(bid, "items", rows)
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    # All mandatory fields missing -> failed
    assert result.failed_rows == 1


def test_run_dq_agent_failure_swallowed():
    """Exception from run_dq_agent must NOT break run_dq."""
    bid = uuid4()
    rows = [{
        "row_id": uuid4(), "row_number": 1,
        "raw_content": json.dumps({
            "external_id": "I1", "name": "Widget",
            "item_type": "RAW", "uom": "EA", "status": "active",
        }),
    }]
    db = _build_run_dq_db(bid, "items", rows)
    with patch("ootils_core.engine.dq.engine.run_dq_agent", side_effect=Exception("agent boom"), create=True):
        result = run_dq(db, bid)
    assert result.passed_rows == 1


def test_run_dq_multiple_rows_mixed_status():
    bid = uuid4()
    rows = [
        {
            "row_id": uuid4(), "row_number": 1,
            "raw_content": json.dumps({
                "external_id": "I1", "name": "Good",
                "item_type": "RAW", "uom": "EA", "status": "active",
            }),
        },
        {
            "row_id": uuid4(), "row_number": 2,
            "raw_content": json.dumps({"external_id": "I2"}),  # missing fields
        },
    ]
    db = _build_run_dq_db(bid, "items", rows)
    with patch("ootils_core.engine.dq.engine.run_dq_agent", create=True):
        result = run_dq(db, bid)
    assert result.total_rows == 2
    assert result.passed_rows == 1
    assert result.failed_rows == 1
    assert result.batch_dq_status == "rejected"
