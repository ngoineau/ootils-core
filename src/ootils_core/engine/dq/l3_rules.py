"""
l3_rules.py — DQ Level 3 (business SC rules).

L3 catches the errors L1 (structural) and L2 (referential) let through:
typed-OK, FK-OK, but semantically absurd (`lead_time_days=0`,
`max_order_qty<min_order_qty`, `quantity<=0`, status outside the
domain enum, etc.).

Each rule is a small pure function that takes one row's parsed dict
and returns zero or more `DQIssue`. Rules are grouped per
`entity_type` via the `_L3_RULES` registry so `_check_l3` can dispatch
without a giant if/elif ladder.

Adding a new rule:
  1. Write a function `_l3_<name>(row_id, row_number, content) -> list[DQIssue]`
  2. Register it under the relevant entity_type(s) in `_L3_RULES`
  3. Add a unit test

Rules deliberately stay synchronous and DB-less. Anything that needs
to hit Postgres belongs in L2 (referential) or L4 (cross-batch).
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from uuid import UUID

from ootils_core.engine.dq.engine import DQIssue


# ---------------------------------------------------------------------------
# Domain enumerations — kept in sync with the CHECK constraints in
# migrations 002, 007, 008, 029. Single source of truth for L3.
# ---------------------------------------------------------------------------

_VALID_ITEM_TYPES = frozenset({
    "finished_good", "semi_finished", "component", "raw_material",
})
_VALID_ITEM_STATUSES = frozenset({"active", "phase_out", "obsolete"})
_VALID_LOCATION_TYPES = frozenset({
    "plant", "dc", "warehouse", "supplier_virtual", "customer_virtual",
})
_VALID_SUPPLIER_STATUSES = frozenset({"active", "inactive", "blocked"})
_VALID_PO_STATUSES = frozenset({
    "open", "in_transit", "received", "cancelled", "closed",
})
_VALID_WO_STATUSES = frozenset({
    "planned", "released", "in_progress", "completed", "cancelled",
})
_VALID_CO_STATUSES = frozenset({"open", "shipped", "delivered", "cancelled"})

# Sanity bounds — values outside these ranges are not strictly illegal
# but indicate a likely export bug (e.g. an ERP exporting epoch dates
# in the year 1900). Warning, not error.
_MAX_REASONABLE_LEAD_TIME_DAYS = 365
_MAX_REASONABLE_QUANTITY = Decimal("10000000")  # 10M units
_MIN_REASONABLE_DATE = date(2000, 1, 1)
_MAX_REASONABLE_DATE = date(2100, 1, 1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_decimal(v) -> Decimal | None:
    """Best-effort string -> Decimal. Returns None if not parseable."""
    if v is None or v == "":
        return None
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return None


def _to_int(v) -> int | None:
    """Best-effort string -> int. Accepts numeric strings; rejects floats with fraction."""
    if v is None or v == "":
        return None
    try:
        d = Decimal(str(v))
        if d == d.to_integral_value():
            return int(d)
        return None
    except (InvalidOperation, ValueError):
        return None


def _to_date(v) -> date | None:
    """Parse ISO 8601 yyyy-mm-dd. Returns None if absent or unparseable."""
    if v is None or v == "":
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        return date.fromisoformat(str(v))
    except (ValueError, TypeError):
        return None


def _issue(
    row_id: UUID,
    row_number: int,
    rule_code: str,
    severity: str,
    field_name: str | None,
    raw_value,
    message: str,
) -> DQIssue:
    return DQIssue(
        row_id=row_id,
        row_number=row_number,
        dq_level=3,
        rule_code=rule_code,
        severity=severity,
        field_name=field_name,
        raw_value=None if raw_value is None else str(raw_value)[:255],
        message=message,
    )


# ---------------------------------------------------------------------------
# Generic rules — apply to multiple entity types
# ---------------------------------------------------------------------------


def _l3_enum_field(
    row_id: UUID,
    row_number: int,
    content: dict,
    field: str,
    valid: frozenset[str],
    rule_code: str,
) -> list[DQIssue]:
    """Common helper: assert content[field] is in `valid`."""
    val = content.get(field)
    if val is None or val == "":
        return []  # L1 already flags missing; L3 doesn't double-flag
    if val not in valid:
        return [_issue(
            row_id, row_number, rule_code, "error",
            field, val,
            f"{field}={val!r} not in {sorted(valid)}",
        )]
    return []


def _l3_quantity_positive(
    row_id: UUID, row_number: int, content: dict, field: str = "quantity",
) -> list[DQIssue]:
    val = content.get(field)
    if val is None or val == "":
        return []
    qty = _to_decimal(val)
    if qty is None:
        # Will already be caught by L1_INVALID_TYPE
        return []
    out: list[DQIssue] = []
    if qty <= 0:
        out.append(_issue(
            row_id, row_number, "L3_QUANTITY_NONPOSITIVE", "error",
            field, val,
            f"{field} must be > 0, got {qty}",
        ))
    elif qty > _MAX_REASONABLE_QUANTITY:
        out.append(_issue(
            row_id, row_number, "L3_QUANTITY_SUSPICIOUS", "warning",
            field, val,
            f"{field}={qty} exceeds {_MAX_REASONABLE_QUANTITY} — check for unit error",
        ))
    return out


def _l3_quantity_nonneg(
    row_id: UUID, row_number: int, content: dict, field: str = "quantity",
) -> list[DQIssue]:
    """Like quantity_positive but allows zero (e.g. on-hand snapshots
    where 0 is meaningful)."""
    val = content.get(field)
    if val is None or val == "":
        return []
    qty = _to_decimal(val)
    if qty is None:
        return []
    out: list[DQIssue] = []
    if qty < 0:
        out.append(_issue(
            row_id, row_number, "L3_QUANTITY_NEGATIVE", "error",
            field, val,
            f"{field} must be >= 0, got {qty}",
        ))
    elif qty > _MAX_REASONABLE_QUANTITY:
        out.append(_issue(
            row_id, row_number, "L3_QUANTITY_SUSPICIOUS", "warning",
            field, val,
            f"{field}={qty} exceeds {_MAX_REASONABLE_QUANTITY} — check for unit error",
        ))
    return out


def _l3_lead_time_positive(
    row_id: UUID, row_number: int, content: dict, field: str = "lead_time_days",
) -> list[DQIssue]:
    val = content.get(field)
    if val is None or val == "":
        return []
    lt = _to_int(val)
    if lt is None:
        return []
    out: list[DQIssue] = []
    if lt <= 0:
        out.append(_issue(
            row_id, row_number, "L3_LEAD_TIME_NONPOSITIVE", "error",
            field, val,
            f"{field} must be > 0, got {lt}",
        ))
    elif lt > _MAX_REASONABLE_LEAD_TIME_DAYS:
        out.append(_issue(
            row_id, row_number, "L3_LEAD_TIME_SUSPICIOUS", "warning",
            field, val,
            f"{field}={lt} days exceeds {_MAX_REASONABLE_LEAD_TIME_DAYS} — check ERP export",
        ))
    return out


def _l3_date_in_reasonable_range(
    row_id: UUID, row_number: int, content: dict, field: str,
) -> list[DQIssue]:
    val = content.get(field)
    if val is None or val == "":
        return []
    d = _to_date(val)
    if d is None:
        return []
    if d < _MIN_REASONABLE_DATE or d > _MAX_REASONABLE_DATE:
        return [_issue(
            row_id, row_number, "L3_DATE_OUT_OF_RANGE", "warning",
            field, val,
            f"{field}={d.isoformat()} outside [{_MIN_REASONABLE_DATE}, {_MAX_REASONABLE_DATE}]",
        )]
    return []


def _l3_date_range_consistency(
    row_id: UUID, row_number: int, content: dict,
    from_field: str = "valid_from", to_field: str = "valid_to",
) -> list[DQIssue]:
    from_v = _to_date(content.get(from_field))
    to_v = _to_date(content.get(to_field))
    if from_v is None or to_v is None:
        return []
    if to_v < from_v:
        return [_issue(
            row_id, row_number, "L3_DATE_RANGE_INVERTED", "error",
            to_field, content.get(to_field),
            f"{to_field}={to_v.isoformat()} is before {from_field}={from_v.isoformat()}",
        )]
    return []


def _l3_reliability_in_zero_one(
    row_id: UUID, row_number: int, content: dict,
) -> list[DQIssue]:
    val = content.get("reliability_score")
    if val is None or val == "":
        return []
    r = _to_decimal(val)
    if r is None:
        return []
    if r < 0 or r > 1:
        return [_issue(
            row_id, row_number, "L3_RELIABILITY_OUT_OF_RANGE", "error",
            "reliability_score", val,
            f"reliability_score={r} must be in [0, 1]",
        )]
    return []


def _l3_moq_le_max_order(
    row_id: UUID, row_number: int, content: dict,
) -> list[DQIssue]:
    """If both `min_order_qty` and `max_order_qty` are set, the min
    must not exceed the max. Common ERP export bug."""
    moq = _to_decimal(content.get("min_order_qty"))
    mox = _to_decimal(content.get("max_order_qty"))
    if moq is None or mox is None:
        return []
    if moq > mox:
        return [_issue(
            row_id, row_number, "L3_MOQ_GT_MAX_ORDER", "error",
            "min_order_qty", content.get("min_order_qty"),
            f"min_order_qty={moq} > max_order_qty={mox}",
        )]
    return []


def _l3_unit_cost_nonneg(
    row_id: UUID, row_number: int, content: dict,
) -> list[DQIssue]:
    val = content.get("unit_cost")
    if val is None or val == "":
        return []
    c = _to_decimal(val)
    if c is None:
        return []
    if c < 0:
        return [_issue(
            row_id, row_number, "L3_UNIT_COST_NEGATIVE", "error",
            "unit_cost", val,
            f"unit_cost={c} must be >= 0",
        )]
    return []


# ---------------------------------------------------------------------------
# Entity-specific rule sets — compose the generic helpers
# ---------------------------------------------------------------------------


def _rules_items(row_id, row_number, content) -> list[DQIssue]:
    out: list[DQIssue] = []
    out += _l3_enum_field(row_id, row_number, content, "item_type", _VALID_ITEM_TYPES, "L3_INVALID_ITEM_TYPE")
    out += _l3_enum_field(row_id, row_number, content, "status", _VALID_ITEM_STATUSES, "L3_INVALID_ITEM_STATUS")
    return out


def _rules_locations(row_id, row_number, content) -> list[DQIssue]:
    return _l3_enum_field(row_id, row_number, content, "location_type",
                          _VALID_LOCATION_TYPES, "L3_INVALID_LOCATION_TYPE")


def _rules_suppliers(row_id, row_number, content) -> list[DQIssue]:
    out: list[DQIssue] = []
    out += _l3_enum_field(row_id, row_number, content, "status",
                          _VALID_SUPPLIER_STATUSES, "L3_INVALID_SUPPLIER_STATUS")
    out += _l3_lead_time_positive(row_id, row_number, content)
    out += _l3_reliability_in_zero_one(row_id, row_number, content)
    return out


def _rules_supplier_items(row_id, row_number, content) -> list[DQIssue]:
    out: list[DQIssue] = []
    out += _l3_lead_time_positive(row_id, row_number, content)
    out += _l3_quantity_positive(row_id, row_number, content, field="moq")
    out += _l3_unit_cost_nonneg(row_id, row_number, content)
    out += _l3_date_range_consistency(row_id, row_number, content,
                                      from_field="valid_from", to_field="valid_to")
    return out


def _rules_purchase_orders(row_id, row_number, content) -> list[DQIssue]:
    out: list[DQIssue] = []
    out += _l3_quantity_positive(row_id, row_number, content)
    out += _l3_enum_field(row_id, row_number, content, "status",
                          _VALID_PO_STATUSES, "L3_INVALID_PO_STATUS")
    out += _l3_date_in_reasonable_range(row_id, row_number, content, "expected_delivery_date")
    return out


def _rules_work_orders(row_id, row_number, content) -> list[DQIssue]:
    out: list[DQIssue] = []
    out += _l3_quantity_positive(row_id, row_number, content)
    out += _l3_enum_field(row_id, row_number, content, "status",
                          _VALID_WO_STATUSES, "L3_INVALID_WO_STATUS")
    out += _l3_date_in_reasonable_range(row_id, row_number, content, "expected_completion_date")
    return out


def _rules_customer_orders(row_id, row_number, content) -> list[DQIssue]:
    out: list[DQIssue] = []
    out += _l3_quantity_positive(row_id, row_number, content)
    out += _l3_enum_field(row_id, row_number, content, "status",
                          _VALID_CO_STATUSES, "L3_INVALID_CO_STATUS")
    out += _l3_date_in_reasonable_range(row_id, row_number, content, "due_date")
    return out


def _rules_forecasts(row_id, row_number, content) -> list[DQIssue]:
    out: list[DQIssue] = []
    out += _l3_quantity_nonneg(row_id, row_number, content)  # 0 forecast allowed
    out += _l3_date_in_reasonable_range(row_id, row_number, content, "period_start")
    out += _l3_date_in_reasonable_range(row_id, row_number, content, "period_end")
    out += _l3_date_range_consistency(row_id, row_number, content,
                                      from_field="period_start", to_field="period_end")
    return out


def _rules_transfers(row_id, row_number, content) -> list[DQIssue]:
    out: list[DQIssue] = []
    out += _l3_quantity_positive(row_id, row_number, content)
    out += _l3_date_in_reasonable_range(row_id, row_number, content, "expected_arrival_date")
    return out


def _rules_on_hand(row_id, row_number, content) -> list[DQIssue]:
    return _l3_quantity_nonneg(row_id, row_number, content)


# ---------------------------------------------------------------------------
# Registry + dispatcher
# ---------------------------------------------------------------------------


RuleFn = Callable[[UUID, int, dict], list[DQIssue]]

_L3_RULES: dict[str, RuleFn] = {
    "items":            _rules_items,
    "locations":        _rules_locations,
    "suppliers":        _rules_suppliers,
    "supplier_items":   _rules_supplier_items,
    "purchase_orders":  _rules_purchase_orders,
    "work_orders":      _rules_work_orders,
    "customer_orders":  _rules_customer_orders,
    "forecasts":        _rules_forecasts,
    "transfers":        _rules_transfers,
    "on_hand":          _rules_on_hand,
}


def check_l3(
    rows: Iterable[tuple[UUID, int, dict]],
    entity_type: str,
) -> list[DQIssue]:
    """Run all L3 rules for `entity_type` over the given rows.

    `rows` is an iterable of (row_id, row_number, content_dict). The
    function returns the flat list of issues; the caller is responsible
    for persisting them and updating row/batch statuses.

    Unknown entity_type returns no issues (L3 is opt-in per entity — if
    we add a new entity_type and forget to add rules, L3 is a no-op
    rather than an error).
    """
    fn = _L3_RULES.get(entity_type)
    if fn is None:
        return []
    out: list[DQIssue] = []
    for row_id, row_number, content in rows:
        out.extend(fn(row_id, row_number, content))
    return out
