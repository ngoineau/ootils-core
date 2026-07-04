"""
tests/test_param_overlay.py — pure unit tests for
ootils_core.engine.scenario.param_overlay (chantier #347, PR1 + PR3's
resolved_field_lateral_sql()).

No DB required: exercises ALLOWED_PARAM_FIELDS, the pure SQL-string builders
resolved_params_sql() / resolved_field_lateral_sql(), and the pure
write-time validators (_validate_value, plus the "fail before any DB
access" ordering of set/clear — provable with conn=None). DB round-trip
coverage (set/clear/list, precedence, COALESCE correctness, scenario_id=None
baseline-pure behaviour) lives in tests/integration/test_param_overlay_integration.py.
"""
from __future__ import annotations

import re
from uuid import uuid4

import pytest

from ootils_core.engine.scenario.param_overlay import (
    ALLOWED_PARAM_FIELDS,
    LOT_SIZE_RULE_VALUES,
    PARAM_FIELD_BOUNDS,
    ParamOverlayError,
    _RESERVED_INTERNAL_ALIAS_PREFIX,
    _validate_field_name,
    _validate_value,
    clear_param_override,
    resolved_field_lateral_sql,
    resolved_params_sql,
    set_param_override,
)


# ---------------------------------------------------------------------------
# Whitelist
# ---------------------------------------------------------------------------


def test_whitelist_is_nonempty_frozen_shape():
    assert len(ALLOWED_PARAM_FIELDS) == 15
    assert isinstance(ALLOWED_PARAM_FIELDS, dict)


def test_whitelist_matches_expected_fields():
    expected = {
        "lead_time_sourcing_days",
        "lead_time_manufacturing_days",
        "lead_time_transit_days",
        "safety_stock_qty",
        "safety_stock_days",
        "min_order_qty",
        "max_order_qty",
        "order_multiple_qty",
        "lot_size_rule",
        "economic_order_qty",
        "lot_size_poq_periods",
        "frozen_time_fence_days",
        "slashed_time_fence_days",
        "forecast_consumption_strategy",
        "consumption_window_days",
    }
    assert set(ALLOWED_PARAM_FIELDS) == expected


def test_cast_map_covers_100pct_of_whitelist_with_valid_sql_types():
    valid_casts = {"integer", "numeric", "text"}
    for field_name, cast_type in ALLOWED_PARAM_FIELDS.items():
        assert cast_type in valid_casts, f"{field_name} has unexpected cast {cast_type!r}"


def test_cast_map_integer_fields():
    integer_fields = {
        "lead_time_sourcing_days",
        "lead_time_manufacturing_days",
        "lead_time_transit_days",
        "lot_size_poq_periods",
        "frozen_time_fence_days",
        "slashed_time_fence_days",
        "consumption_window_days",
    }
    for field_name in integer_fields:
        assert ALLOWED_PARAM_FIELDS[field_name] == "integer"


def test_cast_map_numeric_fields():
    numeric_fields = {
        "safety_stock_qty",
        "safety_stock_days",
        "min_order_qty",
        "max_order_qty",
        "order_multiple_qty",
        "economic_order_qty",
    }
    for field_name in numeric_fields:
        assert ALLOWED_PARAM_FIELDS[field_name] == "numeric"


def test_cast_map_text_fields():
    text_fields = {"lot_size_rule", "forecast_consumption_strategy"}
    for field_name in text_fields:
        assert ALLOWED_PARAM_FIELDS[field_name] == "text"


def test_validate_field_name_accepts_whitelisted_field():
    _validate_field_name("safety_stock_qty")  # no raise


def test_validate_field_name_rejects_unknown_field():
    with pytest.raises(ParamOverlayError):
        _validate_field_name("not_a_real_column")


def test_validate_field_name_rejects_sql_injection_attempt():
    with pytest.raises(ParamOverlayError):
        _validate_field_name("safety_stock_qty; DROP TABLE items;--")


def test_validate_field_name_rejects_node_override_fields():
    # A field that IS whitelisted on scenario/manager.py's node-override
    # list must NOT leak into the planning-param whitelist (different
    # table, different contract).
    with pytest.raises(ParamOverlayError):
        _validate_field_name("closing_stock")


# ---------------------------------------------------------------------------
# resolved_params_sql — content assertions
# ---------------------------------------------------------------------------


def test_resolved_params_sql_contains_every_whitelisted_field():
    fragment = resolved_params_sql("ipp")
    for field_name in ALLOWED_PARAM_FIELDS:
        assert field_name in fragment, f"{field_name} missing from resolved_params_sql output"


def test_resolved_params_sql_selects_key_columns():
    fragment = resolved_params_sql("ipp")
    assert "ipp.param_id" in fragment
    assert "ipp.item_id" in fragment
    assert "ipp.location_id" in fragment


def test_resolved_params_sql_uses_current_row_predicate():
    fragment = resolved_params_sql("ipp")
    assert "ipp.effective_to IS NULL" in fragment
    assert "9999-12-31" in fragment


def test_resolved_params_sql_takes_scenario_id_as_single_named_param():
    fragment = resolved_params_sql("ipp")
    # The fragment's public contract is ONE parameter: the NAMED placeholder
    # %(scenario_id)s, reused in every per-field LATERAL join. psycopg3
    # binds a repeated named placeholder from a single dict key, so callers
    # execute with {"scenario_id": ...} exactly once.
    assert "%(scenario_id)s" in fragment
    assert fragment.count("%(scenario_id)s") == len(ALLOWED_PARAM_FIELDS)
    # No positional placeholder may remain: psycopg3 refuses to mix named
    # and positional styles, so a single stray %s would break every caller
    # composing with named parameters.
    assert fragment.replace("%(scenario_id)s", "").count("%s") == 0


def test_resolved_params_sql_respects_alias_parameter():
    fragment = resolved_params_sql("custom_alias")
    assert "custom_alias.item_id" in fragment
    assert "custom_alias.location_id" in fragment
    assert "FROM item_planning_params custom_alias" in fragment


def test_resolved_params_sql_rejects_unsafe_alias():
    with pytest.raises(ParamOverlayError):
        resolved_params_sql("ipp; DROP TABLE items;--")


def test_resolved_params_sql_rejects_alias_with_spaces():
    with pytest.raises(ParamOverlayError):
        resolved_params_sql("ipp alias")


def test_resolved_params_sql_no_suspicious_interpolation():
    """
    No field name in the generated fragment can come from anywhere other
    than ALLOWED_PARAM_FIELDS. We assert this indirectly: every column
    reference of the form `ipp.<name>` or `field_name = '<name>'` found in
    the fragment must be a whitelisted field (or one of the fixed
    structural columns param_id/item_id/location_id/effective_to).
    """
    fragment = resolved_params_sql("ipp")
    structural = {"param_id", "item_id", "location_id", "effective_to"}
    allowed = set(ALLOWED_PARAM_FIELDS) | structural

    for match in re.finditer(r"ipp\.([a-zA-Z_][a-zA-Z0-9_]*)", fragment):
        column = match.group(1)
        assert column in allowed, f"unexpected column reference ipp.{column}"

    for match in re.finditer(r"field_name = '([^']*)'", fragment):
        assert match.group(1) in ALLOWED_PARAM_FIELDS


def test_resolved_params_sql_lateral_join_scoped_to_item_and_location():
    fragment = resolved_params_sql("ipp")
    assert "__po_ov_lead_time_sourcing_days.item_id = ipp.item_id" in fragment
    assert "__po_ov_lead_time_sourcing_days.location_id = ipp.location_id" in fragment
    assert "__po_ov_lead_time_sourcing_days.location_id IS NULL" in fragment


def test_resolved_params_sql_rejects_alias_matching_reserved_prefix():
    with pytest.raises(ParamOverlayError):
        resolved_params_sql(f"{_RESERVED_INTERNAL_ALIAS_PREFIX}anything")


def test_resolved_params_sql_alias_collision_is_not_tautological():
    """
    Reproduction of the alias-collision bug: if a caller passes the outer
    alias 'o' (a legal, unremarkable identifier), the fragment must NOT
    contain a self-referential predicate like 'o.item_id = o.item_id' —
    that would make the correlation predicate a tautology, matching every
    row instead of correlating the override to the specific outer row.

    The internal LATERAL alias must be provably distinct from the caller's
    outer alias so `<internal>.item_id = o.item_id` is a real correlation,
    never `o.item_id = o.item_id`.
    """
    fragment = resolved_params_sql("o")

    assert "o.item_id = o.item_id" not in fragment
    assert "o.location_id = o.location_id" not in fragment

    # The real correlation predicate must reference the OUTER alias 'o' on
    # the right-hand side, keyed off an internal alias that is NOT 'o'.
    assert "__po_ov_lead_time_sourcing_days.item_id = o.item_id" in fragment
    assert "FROM item_planning_params o" in fragment


def test_resolved_params_sql_internal_aliases_stay_within_pg_identifier_limit():
    # PostgreSQL truncates/rejects identifiers over 63 bytes; the longest
    # whitelisted field name must still produce identifiers within budget.
    fragment = resolved_params_sql("ipp")
    longest_field = max(ALLOWED_PARAM_FIELDS, key=len)
    internal_alias = f"__po_base_{longest_field}"
    assert len(internal_alias) <= 63
    assert internal_alias in fragment


def test_resolved_params_sql_precedence_orders_exact_location_first():
    # ORDER BY location_id NULLS LAST -> a non-NULL (exact-location) match
    # sorts before the NULL (item-global) one, LIMIT 1 keeps the winner.
    fragment = resolved_params_sql("ipp")
    assert "ORDER BY __po_ov_lead_time_sourcing_days.location_id NULLS LAST" in fragment
    assert "LIMIT 1" in fragment


def test_resolved_params_sql_lot_size_rule_casts_both_sides():
    # COALESCE requires matching operand types; the base column is the
    # lot_size_rule_type ENUM, not text, so both sides must cast to ::text.
    fragment = resolved_params_sql("ipp")
    assert "ipp.lot_size_rule::text" in fragment


# ---------------------------------------------------------------------------
# _validate_value — write-time value validation (fail loudly at the write,
# never at an innocent reader's resolution)
# ---------------------------------------------------------------------------


def test_validate_value_integer_accepts_integer_literals():
    # lead_time_sourcing_days' CHECK is >= 0 (migration 007) so only
    # non-negative literals are legal here; the negative case moved to
    # test_validate_value_integer_rejects_business_bound_violation below.
    for ok in ("0", "21", "+7", " 42 "):
        _validate_value("lead_time_sourcing_days", ok)  # no raise


def test_validate_value_integer_rejects_garbage():
    for bad in ("abc", "1.5", "", "  ", "1e3", "0x10", "1_0", "42 days"):
        with pytest.raises(ParamOverlayError, match="integer"):
            _validate_value("lead_time_sourcing_days", bad)


def test_validate_value_integer_rejects_business_bound_violation():
    # RETURNED (was previously an "accepts" case): item_planning_params'
    # CHECK (lead_time_sourcing_days >= 0) (migration 007) means '-3' can
    # never be a legal value on the base table — an override carrying it
    # would be silently un-appliable / DB-illegal if ever promoted. The
    # validator must reject it at write time, mirroring the CHECK.
    with pytest.raises(ParamOverlayError, match="minimum"):
        _validate_value("lead_time_sourcing_days", "-3")


def test_validate_value_integer_rejects_non_ascii_digits():
    # \d in a naive regex matches Unicode decimal digits (e.g. Arabic-Indic
    # '١٢٣') that Python's re accepts but Postgres' int4in() rejects outright
    # — the write-time validator must not be looser than the cast target.
    with pytest.raises(ParamOverlayError, match="integer"):
        _validate_value("lead_time_sourcing_days", "١٢٣")


def test_validate_value_integer_rejects_out_of_int4_range():
    # lot_size_poq_periods has no upper business bound in PARAM_FIELD_BOUNDS
    # beyond the int4 ceiling, so this isolates the int4-range check itself.
    for bad in ("2147483648", "-2147483649", "99999999999999"):
        with pytest.raises(ParamOverlayError, match="int4 range"):
            _validate_value("lot_size_poq_periods", bad)


def test_validate_value_integer_accepts_int4_boundary_values():
    _validate_value("lead_time_sourcing_days", "2147483647")  # no raise
    _validate_value("lead_time_sourcing_days", "0")  # no raise (min bound)


def test_validate_value_integer_returns_stripped_normalized_value():
    assert _validate_value("lead_time_sourcing_days", " 42 ") == "42"
    assert _validate_value("lead_time_sourcing_days", "+7") == "+7"


def test_validate_value_numeric_accepts_numeric_literals():
    for ok in ("0", "42", "42.5", "10.0", "1e3", " 7.25 "):
        _validate_value("safety_stock_qty", ok)  # no raise


def test_validate_value_numeric_rejects_garbage():
    # THE motivating case: value='abc' for safety_stock_qty (numeric) used
    # to persist fine and only explode at resolution time.
    for bad in ("abc", "", "  ", "12,5", "1.2.3", "1_0"):
        with pytest.raises(ParamOverlayError, match="numeric"):
            _validate_value("safety_stock_qty", bad)


def test_validate_value_numeric_rejects_non_ascii_digits():
    # Decimal() itself is Unicode-aware (Decimal('١٢٣') == 123) — the
    # validator must add its own ASCII pre-check rather than trust Decimal
    # to reject what Postgres' numeric_in() would reject.
    with pytest.raises(ParamOverlayError, match="numeric"):
        _validate_value("safety_stock_qty", "١٢٣")


def test_validate_value_numeric_rejects_negative_for_strictly_positive_field():
    # RETURNED-equivalent: min_order_qty's CHECK is > 0 (migration 007), not
    # >= 0 — zero and negative values must both be refused.
    for bad in ("0", "-5", "-0.01"):
        with pytest.raises(ParamOverlayError, match="strictly greater than"):
            _validate_value("min_order_qty", bad)


def test_validate_value_numeric_rejects_negative_for_inclusive_zero_field():
    # safety_stock_qty's CHECK is >= 0 (migration 007) — zero is legal,
    # negative is not.
    _validate_value("safety_stock_qty", "0")  # no raise
    with pytest.raises(ParamOverlayError, match="minimum"):
        _validate_value("safety_stock_qty", "-1")


def test_validate_value_numeric_rejects_absurd_exponent():
    # '1e999999' is a finite Decimal but wildly exceeds any real planning
    # quantity — the pragmatic 10**12 ceiling exists specifically to refuse
    # values like this.
    with pytest.raises(ParamOverlayError, match="maximum"):
        _validate_value("safety_stock_qty", "1e999999")


def test_validate_value_numeric_returns_stripped_normalized_value():
    assert _validate_value("safety_stock_qty", " 42.5 ") == "42.5"


def test_validate_value_numeric_rejects_non_finite():
    # Decimal parses NaN/Infinity happily; a NaN safety stock is a silent
    # wrong answer, so the validator refuses non-finite values explicitly.
    for bad in ("NaN", "nan", "Infinity", "-Infinity", "inf"):
        with pytest.raises(ParamOverlayError, match="finite"):
            _validate_value("safety_stock_qty", bad)


def test_validate_value_text_rejects_empty():
    for field in ("forecast_consumption_strategy", "lot_size_rule"):
        for bad in ("", "   "):
            with pytest.raises(ParamOverlayError, match="non-empty"):
                _validate_value(field, bad)


def test_validate_value_text_accepts_nonempty_free_text():
    # forecast_consumption_strategy is a plain TEXT column (migration 021),
    # not an enum — any non-empty string is acceptable at this layer.
    _validate_value("forecast_consumption_strategy", "max_only")  # no raise


def test_validate_value_rejects_non_string():
    # The overlay contract is a serialized TEXT scalar; a raw int would also
    # break the INSERT into the TEXT column downstream.
    with pytest.raises(ParamOverlayError, match="str"):
        _validate_value("safety_stock_qty", 42)


def test_lot_size_rule_values_match_migrations():
    # Pinned against the enum's source migrations: 007 (CREATE TYPE) + 021
    # (ALTER TYPE ADD VALUE) — see the constant's comment in param_overlay.
    assert LOT_SIZE_RULE_VALUES == {
        "LOTFORLOT",
        "FIXED_QTY",
        "PERIOD_OF_SUPPLY",
        "MIN_MAX",
        "POQ",
        "EOQ",
        "MULTIPLE",
    }


def test_validate_value_lot_size_rule_accepts_every_enum_value():
    for rule in LOT_SIZE_RULE_VALUES:
        _validate_value("lot_size_rule", rule)  # no raise


def test_validate_value_lot_size_rule_rejects_non_enum_value():
    # NOTE: 'EOQ ' (trailing space) is deliberately NOT in this list — the
    # validator strips before membership-testing (see the "stored value must
    # be the normalized one" fix), so 'EOQ '.strip() == 'EOQ' is legitimate.
    for bad in ("NOT_A_REAL_RULE", "lotforlot", "EOQ  X"):
        with pytest.raises(ParamOverlayError, match="lot_size_rule"):
            _validate_value("lot_size_rule", bad)


def test_validate_value_lot_size_rule_strips_surrounding_whitespace():
    # RETURNED-equivalent: what used to be a rejection case ('EOQ ') is now
    # legitimate — validation strips before comparing against
    # LOT_SIZE_RULE_VALUES, consistent with storing the normalized value.
    assert _validate_value("lot_size_rule", "EOQ ") == "EOQ"
    assert _validate_value("lot_size_rule", " EOQ") == "EOQ"


# ---------------------------------------------------------------------------
# PARAM_FIELD_BOUNDS — pinned against the REAL CHECK constraints read off
# migrations 007 (item_planning_params base columns) and 021 (APICS
# lot-sizing additions). Any drift between this map and the SQL source is a
# correctness bug: an override could then carry a value the base table
# could never legally hold.
# ---------------------------------------------------------------------------


def test_param_field_bounds_covers_every_integer_and_numeric_field():
    # text-cast fields (lot_size_rule, forecast_consumption_strategy) carry
    # no numeric CHECK and are intentionally absent from the bounds map.
    numeric_and_integer_fields = {
        name for name, cast in ALLOWED_PARAM_FIELDS.items() if cast != "text"
    }
    assert set(PARAM_FIELD_BOUNDS) == numeric_and_integer_fields


def test_param_field_bounds_inclusive_zero_fields():
    # CHECK (... >= 0) fields — migration 007 for lead times/safety stock,
    # migration 021 for frozen_time_fence_days.
    inclusive_zero_fields = {
        "lead_time_sourcing_days",
        "lead_time_manufacturing_days",
        "lead_time_transit_days",
        "safety_stock_qty",
        "safety_stock_days",
        "frozen_time_fence_days",
    }
    for field in inclusive_zero_fields:
        bounds = PARAM_FIELD_BOUNDS[field]
        assert bounds.min_inclusive is True
        assert bounds.minimum == 0


def test_param_field_bounds_strictly_positive_fields():
    # CHECK (... > 0) fields — migration 007 for min/max_order_qty,
    # migration 021 for order_multiple_qty, economic_order_qty,
    # lot_size_poq_periods, slashed_time_fence_days, consumption_window_days.
    # NOTE: slashed_time_fence_days is > 0 in the real schema, NOT >= 0.
    strictly_positive_fields = {
        "min_order_qty",
        "max_order_qty",
        "order_multiple_qty",
        "economic_order_qty",
        "lot_size_poq_periods",
        "slashed_time_fence_days",
        "consumption_window_days",
    }
    for field in strictly_positive_fields:
        bounds = PARAM_FIELD_BOUNDS[field]
        assert bounds.min_inclusive is False
        assert bounds.minimum == 0


def test_param_field_bounds_int4_fields_capped_at_int4_max():
    int4_fields = {
        "lead_time_sourcing_days",
        "lead_time_manufacturing_days",
        "lead_time_transit_days",
        "lot_size_poq_periods",
        "frozen_time_fence_days",
        "slashed_time_fence_days",
        "consumption_window_days",
    }
    for field in int4_fields:
        assert PARAM_FIELD_BOUNDS[field].maximum == 2_147_483_647


def test_validate_value_slashed_time_fence_days_rejects_zero():
    # Confirms slashed_time_fence_days really is > 0 (not >= 0): zero is a
    # CHECK violation on the real column (migration 021).
    with pytest.raises(ParamOverlayError, match="strictly greater than"):
        _validate_value("slashed_time_fence_days", "0")


def test_validate_value_frozen_time_fence_days_accepts_zero():
    # Contrast case: frozen_time_fence_days is >= 0 (migration 021) —
    # zero is legal here, unlike slashed_time_fence_days.
    _validate_value("frozen_time_fence_days", "0")  # no raise


# ---------------------------------------------------------------------------
# set/clear fail loudly BEFORE any DB access on caller bugs — provable
# without a database: conn=None would raise AttributeError on the first
# conn.execute(), so getting ParamOverlayError proves validation ran first.
# ---------------------------------------------------------------------------


def test_set_param_override_validates_value_before_db():
    with pytest.raises(ParamOverlayError, match="numeric"):
        set_param_override(
            None, uuid4(), uuid4(), "safety_stock_qty", "abc", "test-agent"
        )


def test_set_param_override_validates_lot_size_rule_before_db():
    with pytest.raises(ParamOverlayError, match="lot_size_rule"):
        set_param_override(
            None, uuid4(), uuid4(), "lot_size_rule", "NOT_A_REAL_RULE", "test-agent"
        )


def test_clear_param_override_rejects_unknown_field_before_db():
    # Symmetry with set: a typo'd field is a caller bug, not a False no-op.
    with pytest.raises(ParamOverlayError, match="not in the allowed"):
        clear_param_override(None, uuid4(), uuid4(), "not_a_field")


# ---------------------------------------------------------------------------
# resolved_field_lateral_sql — chantier #347 PR3: the mono-field LATERAL
# helper used by the propagation call sites (SHORTAGES_SQL, the safety-stock
# cache preload, mrp.py's _get_planning_params) instead of the full
# resolved_params_sql() fan-out.
# ---------------------------------------------------------------------------


def test_resolved_field_lateral_sql_rejects_unknown_field():
    with pytest.raises(ParamOverlayError, match="not in the allowed"):
        resolved_field_lateral_sql("not_a_real_column", "ipp", "out")


def test_resolved_field_lateral_sql_rejects_sql_injection_attempt_in_field():
    with pytest.raises(ParamOverlayError):
        resolved_field_lateral_sql(
            "safety_stock_qty; DROP TABLE items;--", "ipp", "out"
        )


def test_resolved_field_lateral_sql_rejects_unsafe_base_alias():
    with pytest.raises(ParamOverlayError):
        resolved_field_lateral_sql("safety_stock_qty", "ipp; DROP TABLE items;--", "out")


def test_resolved_field_lateral_sql_rejects_unsafe_out_col():
    with pytest.raises(ParamOverlayError):
        resolved_field_lateral_sql("safety_stock_qty", "ipp", "out; DROP TABLE items;--")


def test_resolved_field_lateral_sql_rejects_base_alias_with_reserved_prefix():
    with pytest.raises(ParamOverlayError):
        resolved_field_lateral_sql(
            "safety_stock_qty", f"{_RESERVED_INTERNAL_ALIAS_PREFIX}x", "out"
        )


def test_resolved_field_lateral_sql_rejects_out_col_with_reserved_prefix():
    with pytest.raises(ParamOverlayError):
        resolved_field_lateral_sql(
            "safety_stock_qty", "ipp", f"{_RESERVED_INTERNAL_ALIAS_PREFIX}x"
        )


def test_resolved_field_lateral_sql_contains_single_scenario_id_placeholder():
    fragment = resolved_field_lateral_sql("safety_stock_qty", "ipp", "ss")
    # Exactly ONE %(scenario_id)s — this fragment resolves ONE field, unlike
    # resolved_params_sql() which repeats the placeholder once per whitelisted
    # field. Composing N calls (one per field) in one host query is safe
    # because psycopg3 binds every occurrence from the same dict key.
    assert fragment.count("%(scenario_id)s") == 1
    assert fragment.replace("%(scenario_id)s", "").count("%s") == 0


def test_resolved_field_lateral_sql_is_left_join_lateral():
    fragment = resolved_field_lateral_sql("safety_stock_qty", "ipp", "ss")
    assert "LEFT JOIN LATERAL" in fragment
    assert "scenario_planning_overrides" in fragment


def test_resolved_field_lateral_sql_correlates_on_base_alias():
    fragment = resolved_field_lateral_sql("safety_stock_qty", "custom", "ss")
    assert ".item_id = custom.item_id" in fragment
    assert ".location_id = custom.location_id" in fragment
    assert "custom.safety_stock_qty" in fragment


def test_resolved_field_lateral_sql_precedence_orders_exact_location_first():
    fragment = resolved_field_lateral_sql("safety_stock_qty", "ipp", "ss")
    assert "ORDER BY" in fragment
    assert "location_id NULLS LAST" in fragment
    assert "LIMIT 1" in fragment


def test_resolved_field_lateral_sql_uses_reserved_internal_alias_prefix():
    fragment = resolved_field_lateral_sql("safety_stock_qty", "ipp", "ss")
    assert _RESERVED_INTERNAL_ALIAS_PREFIX in fragment
    assert f"{_RESERVED_INTERNAL_ALIAS_PREFIX}ov_safety_stock_qty" in fragment


def test_resolved_field_lateral_sql_casts_both_sides_to_field_type():
    fragment = resolved_field_lateral_sql("safety_stock_qty", "ipp", "ss")
    assert "::numeric" in fragment
    fragment_int = resolved_field_lateral_sql("lead_time_sourcing_days", "ipp", "lt")
    assert "::integer" in fragment_int
    fragment_text = resolved_field_lateral_sql("lot_size_rule", "ipp", "rule")
    assert "::text" in fragment_text


def test_resolved_field_lateral_sql_output_column_matches_out_col():
    fragment = resolved_field_lateral_sql("safety_stock_qty", "ipp", "my_out")
    assert "AS my_out" in fragment
    assert ") my_out" in fragment


def test_resolved_field_lateral_sql_no_suspicious_interpolation():
    """No field name in the generated fragment can come from anywhere other
    than ALLOWED_PARAM_FIELDS — mirrors
    test_resolved_params_sql_no_suspicious_interpolation for the mono-field
    helper."""
    fragment = resolved_field_lateral_sql("safety_stock_qty", "ipp", "ss")
    for match in re.finditer(r"field_name = '([^']*)'", fragment):
        assert match.group(1) in ALLOWED_PARAM_FIELDS


def test_resolved_field_lateral_sql_distinct_out_col_per_field_is_composable():
    # Two fragments for two different fields, distinct out_col each — the
    # real usage pattern in propagator.py / mrp.py (multiple fields resolved
    # in one host query). No alias collision between the two fragments.
    frag_a = resolved_field_lateral_sql("safety_stock_qty", "ipp", "ss")
    frag_b = resolved_field_lateral_sql("min_order_qty", "ipp", "moq")
    combined = frag_a + frag_b
    assert "ss_ov" in combined and "moq_ov" in combined
    assert combined.count("%(scenario_id)s") == 2
