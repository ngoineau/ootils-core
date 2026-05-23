"""Unit tests for the L3 business rules (engine/dq/l3_rules.py).

Pure tests — no DB. Each test passes a row dict to the rule registry
and asserts the issues returned. Each rule is exercised both on the
happy path (no issue) and on at least one failing case.
"""
from __future__ import annotations

from uuid import uuid4

from ootils_core.engine.dq.l3_rules import check_l3


# A canonical row_id + row_number we reuse everywhere
RID = uuid4()
RN = 42


def _codes(issues) -> list[str]:
    return [i.rule_code for i in issues]


# ---------------------------------------------------------------------------
# items
# ---------------------------------------------------------------------------


def test_items_happy_path() -> None:
    row = {"external_id": "X1", "name": "Foo", "item_type": "finished_good", "uom": "EA", "status": "active"}
    assert check_l3([(RID, RN, row)], "items") == []


def test_items_invalid_item_type() -> None:
    row = {"item_type": "widget"}
    issues = check_l3([(RID, RN, row)], "items")
    assert _codes(issues) == ["L3_INVALID_ITEM_TYPE"]
    assert issues[0].severity == "error"
    assert issues[0].dq_level == 3
    assert issues[0].field_name == "item_type"


def test_items_invalid_status() -> None:
    row = {"status": "discontinued"}
    issues = check_l3([(RID, RN, row)], "items")
    assert "L3_INVALID_ITEM_STATUS" in _codes(issues)


def test_items_missing_fields_dont_double_flag() -> None:
    """L1 already flags missing required fields. L3 must NOT also flag
    them (otherwise the operator sees duplicate noise)."""
    issues = check_l3([(RID, RN, {})], "items")
    assert issues == []


# ---------------------------------------------------------------------------
# locations
# ---------------------------------------------------------------------------


def test_locations_invalid_type() -> None:
    issues = check_l3([(RID, RN, {"location_type": "spaceport"})], "locations")
    assert _codes(issues) == ["L3_INVALID_LOCATION_TYPE"]


def test_locations_dc_is_valid() -> None:
    assert check_l3([(RID, RN, {"location_type": "dc"})], "locations") == []


# ---------------------------------------------------------------------------
# suppliers
# ---------------------------------------------------------------------------


def test_suppliers_lead_time_zero_is_error() -> None:
    issues = check_l3([(RID, RN, {"lead_time_days": "0"})], "suppliers")
    codes = _codes(issues)
    assert "L3_LEAD_TIME_NONPOSITIVE" in codes


def test_suppliers_lead_time_huge_is_warning() -> None:
    issues = check_l3([(RID, RN, {"lead_time_days": "500"})], "suppliers")
    sus = [i for i in issues if i.rule_code == "L3_LEAD_TIME_SUSPICIOUS"]
    assert len(sus) == 1
    assert sus[0].severity == "warning"


def test_suppliers_reliability_out_of_range_high() -> None:
    issues = check_l3([(RID, RN, {"reliability_score": "1.5"})], "suppliers")
    assert "L3_RELIABILITY_OUT_OF_RANGE" in _codes(issues)


def test_suppliers_reliability_out_of_range_negative() -> None:
    issues = check_l3([(RID, RN, {"reliability_score": "-0.1"})], "suppliers")
    assert "L3_RELIABILITY_OUT_OF_RANGE" in _codes(issues)


def test_suppliers_reliability_boundary_one_is_valid() -> None:
    assert check_l3([(RID, RN, {"reliability_score": "1.0"})], "suppliers") == []


def test_suppliers_invalid_status_enum() -> None:
    issues = check_l3([(RID, RN, {"status": "pending"})], "suppliers")
    assert "L3_INVALID_SUPPLIER_STATUS" in _codes(issues)


# ---------------------------------------------------------------------------
# supplier_items
# ---------------------------------------------------------------------------


def test_supplier_items_moq_positive() -> None:
    issues = check_l3([(RID, RN, {"moq": "0"})], "supplier_items")
    assert "L3_QUANTITY_NONPOSITIVE" in _codes(issues)


def test_supplier_items_unit_cost_negative() -> None:
    issues = check_l3([(RID, RN, {"unit_cost": "-1.50"})], "supplier_items")
    assert "L3_UNIT_COST_NEGATIVE" in _codes(issues)


def test_supplier_items_valid_date_range_ok() -> None:
    row = {"valid_from": "2026-01-01", "valid_to": "2026-12-31"}
    assert check_l3([(RID, RN, row)], "supplier_items") == []


def test_supplier_items_inverted_date_range() -> None:
    row = {"valid_from": "2026-12-31", "valid_to": "2026-01-01"}
    issues = check_l3([(RID, RN, row)], "supplier_items")
    assert "L3_DATE_RANGE_INVERTED" in _codes(issues)


# ---------------------------------------------------------------------------
# purchase_orders
# ---------------------------------------------------------------------------


def test_purchase_orders_quantity_zero_is_error() -> None:
    issues = check_l3([(RID, RN, {"quantity": "0"})], "purchase_orders")
    assert "L3_QUANTITY_NONPOSITIVE" in _codes(issues)


def test_purchase_orders_quantity_too_large_is_warning() -> None:
    issues = check_l3([(RID, RN, {"quantity": "99999999999"})], "purchase_orders")
    sus = [i for i in issues if i.rule_code == "L3_QUANTITY_SUSPICIOUS"]
    assert len(sus) == 1
    assert sus[0].severity == "warning"


def test_purchase_orders_status_invalid() -> None:
    issues = check_l3([(RID, RN, {"status": "scheduled"})], "purchase_orders")
    assert "L3_INVALID_PO_STATUS" in _codes(issues)


def test_purchase_orders_date_in_1990_is_warning() -> None:
    issues = check_l3([(RID, RN, {"expected_delivery_date": "1995-01-01"})], "purchase_orders")
    assert "L3_DATE_OUT_OF_RANGE" in _codes(issues)


# ---------------------------------------------------------------------------
# work_orders / customer_orders / transfers / forecasts / on_hand
# ---------------------------------------------------------------------------


def test_work_orders_invalid_status() -> None:
    issues = check_l3([(RID, RN, {"status": "running"})], "work_orders")
    assert "L3_INVALID_WO_STATUS" in _codes(issues)


def test_customer_orders_due_date_in_future_is_warning() -> None:
    issues = check_l3([(RID, RN, {"due_date": "2150-06-01"})], "customer_orders")
    assert "L3_DATE_OUT_OF_RANGE" in _codes(issues)


def test_forecasts_zero_quantity_is_allowed() -> None:
    """Forecasts of 0 are legitimate (e.g. ramp-down of EOL items)."""
    assert check_l3([(RID, RN, {"quantity": "0"})], "forecasts") == []


def test_forecasts_negative_quantity_is_error() -> None:
    issues = check_l3([(RID, RN, {"quantity": "-5"})], "forecasts")
    assert "L3_QUANTITY_NEGATIVE" in _codes(issues)


def test_forecasts_period_range_inverted() -> None:
    row = {"period_start": "2026-06-01", "period_end": "2026-05-01"}
    issues = check_l3([(RID, RN, row)], "forecasts")
    assert "L3_DATE_RANGE_INVERTED" in _codes(issues)


def test_transfers_quantity_positive() -> None:
    issues = check_l3([(RID, RN, {"quantity": "0"})], "transfers")
    assert "L3_QUANTITY_NONPOSITIVE" in _codes(issues)


def test_on_hand_zero_is_allowed() -> None:
    """Zero on-hand is a perfectly normal state."""
    assert check_l3([(RID, RN, {"quantity": "0"})], "on_hand") == []


def test_on_hand_negative_is_error() -> None:
    issues = check_l3([(RID, RN, {"quantity": "-10"})], "on_hand")
    assert "L3_QUANTITY_NEGATIVE" in _codes(issues)


# ---------------------------------------------------------------------------
# Registry / dispatcher
# ---------------------------------------------------------------------------


def test_unknown_entity_type_returns_empty() -> None:
    """L3 is opt-in per entity. An unknown entity_type produces no issues
    rather than crashing — keeps the engine robust against future ones."""
    assert check_l3([(RID, RN, {"foo": "bar"})], "weather_data") == []


def test_multiple_rows_aggregate_issues() -> None:
    rows = [
        (uuid4(), 1, {"item_type": "bogus"}),
        (uuid4(), 2, {"status": "bogus"}),
        (uuid4(), 3, {"item_type": "raw_material", "status": "active"}),  # clean
    ]
    issues = check_l3(rows, "items")
    assert len(issues) == 2
    assert {i.row_number for i in issues} == {1, 2}


def test_unparseable_numeric_does_not_crash() -> None:
    """If the value is not a valid Decimal, L1 already flagged
    L1_INVALID_TYPE. L3 must silently skip it, not double-report."""
    issues = check_l3([(RID, RN, {"quantity": "not a number"})], "purchase_orders")
    # No L3 quantity-related issues should fire
    assert all("QUANTITY" not in c for c in _codes(issues))
