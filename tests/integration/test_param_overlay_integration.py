"""
tests/integration/test_param_overlay_integration.py — DB-backed tests for
ootils_core.engine.scenario.param_overlay (chantier #347, PR1: the resolver
foundation; NOT wired into any reader yet — this file only exercises the
module's own contract against real Postgres).

Mirrors the fixture style of tests/integration/test_scenario_fk_retention.py
(function-scoped `conn`, minimal direct-SQL seeding, no TestClient/app
machinery — this module has no router). Pure-Python coverage of the SQL
builder (whitelist shape, alias guard, injection surface) lives in
tests/test_param_overlay.py and is not duplicated here.

The chantier invariant these tests exist for is C0-type isolation: an
override set inside fork A changes ONLY fork A's resolved view; baseline
(scenario_id=None) and any sibling fork B resolve bit-identically to the
pre-override state. See test_c0_isolation_* below.

FK retention note: tests/integration/test_scenario_fk_retention.py's
test_all_scenario_fks_are_restrict is generic (pg_constraint introspection
over EVERY FK pointing at scenarios), so scenario_planning_overrides is
discovered automatically — no whitelist to extend there. The behavioural
half (DELETE blocked while an override exists) is asserted here.
"""
from __future__ import annotations

import re
from datetime import date
from uuid import UUID, uuid4

import psycopg
import pytest

from ootils_core.engine.scenario.param_overlay import (
    ALLOWED_PARAM_FIELDS,
    ParamOverlayError,
    clear_param_override,
    list_param_overrides,
    resolved_params_sql,
    set_param_override,
)

from .conftest import requires_db

pytestmark = requires_db

# Seeded by migration 002 (is_baseline=TRUE) — the only baseline scenario.
BASELINE = "00000000-0000-0000-0000-000000000001"


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_item(conn, name: str = "overlay-test-item"):
    row = conn.execute(
        "INSERT INTO items (item_id, name) VALUES (%s, %s) RETURNING item_id",
        (uuid4(), name),
    ).fetchone()
    return row["item_id"]


def _seed_location(conn, name: str = "overlay-test-loc"):
    row = conn.execute(
        "INSERT INTO locations (location_id, name) VALUES (%s, %s) RETURNING location_id",
        (uuid4(), name),
    ).fetchone()
    return row["location_id"]


def _seed_planning_params(
    conn,
    item_id,
    location_id,
    effective_from: date | None = None,
    effective_to: date | None = None,
    **overrides,
):
    """Insert an item_planning_params row (current by default: effective_to NULL)."""
    defaults = dict(
        lead_time_sourcing_days=14,
        lead_time_manufacturing_days=0,
        lead_time_transit_days=0,
        safety_stock_qty=10,
        safety_stock_days=None,
        min_order_qty=None,
        max_order_qty=None,
        order_multiple_qty=None,
        lot_size_rule="LOTFORLOT",
        economic_order_qty=None,
        lot_size_poq_periods=1,
        frozen_time_fence_days=0,
        slashed_time_fence_days=1,
        forecast_consumption_strategy="max_only",
        consumption_window_days=7,
    )
    defaults.update(overrides)
    conn.execute(
        """
        INSERT INTO item_planning_params (
            item_id, location_id, effective_from, effective_to,
            lead_time_sourcing_days, lead_time_manufacturing_days, lead_time_transit_days,
            safety_stock_qty, safety_stock_days,
            min_order_qty, max_order_qty, order_multiple_qty,
            lot_size_rule, economic_order_qty, lot_size_poq_periods,
            frozen_time_fence_days, slashed_time_fence_days,
            forecast_consumption_strategy, consumption_window_days
        ) VALUES (
            %(item_id)s, %(location_id)s, %(effective_from)s, %(effective_to)s,
            %(lead_time_sourcing_days)s, %(lead_time_manufacturing_days)s, %(lead_time_transit_days)s,
            %(safety_stock_qty)s, %(safety_stock_days)s,
            %(min_order_qty)s, %(max_order_qty)s, %(order_multiple_qty)s,
            %(lot_size_rule)s, %(economic_order_qty)s, %(lot_size_poq_periods)s,
            %(frozen_time_fence_days)s, %(slashed_time_fence_days)s,
            %(forecast_consumption_strategy)s, %(consumption_window_days)s
        )
        """,
        {
            "item_id": item_id,
            "location_id": location_id,
            "effective_from": effective_from or date.today(),
            "effective_to": effective_to,
            **defaults,
        },
    )


def _seed_scenario(conn, name: str = "overlay-test-scenario", is_baseline: bool = False):
    row = conn.execute(
        "INSERT INTO scenarios (scenario_id, name, is_baseline, status) "
        "VALUES (%s, %s, %s, 'active') RETURNING scenario_id",
        (uuid4(), name, is_baseline),
    ).fetchone()
    return row["scenario_id"]


def _seed_item_loc_params(conn, **params):
    """Common seed: one item + one location + one CURRENT planning-params row."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(conn, item_id, location_id, **params)
    return item_id, location_id


# ---------------------------------------------------------------------------
# Resolver execution helpers
# ---------------------------------------------------------------------------
#
# Parameter contract: the fragment expects exactly ONE parameter, the NAMED
# placeholder %(scenario_id)s (reused across the per-field LATERAL joins —
# psycopg3 binds a repeated name from a single dict key). Composition must
# therefore use named style throughout: psycopg3 refuses to mix %(named)s
# and positional %s in one statement.


def _resolve_rows(conn, scenario_id, item_id, location_id=None):
    fragment = resolved_params_sql("ipp")
    sql = f"SELECT rp.* FROM ({fragment}) rp WHERE rp.item_id = %(item_id)s"
    params: dict = {"scenario_id": scenario_id, "item_id": item_id}
    if location_id is not None:
        sql += " AND rp.location_id = %(location_id)s"
        params["location_id"] = location_id
    sql += " ORDER BY rp.location_id"
    return conn.execute(sql, params).fetchall()


def _resolve_one(conn, scenario_id, item_id, location_id):
    rows = _resolve_rows(conn, scenario_id, item_id, location_id)
    assert len(rows) == 1, f"expected exactly one resolved row, got {len(rows)}"
    return rows[0]


# ---------------------------------------------------------------------------
# set_param_override / clear_param_override / list_param_overrides round-trip
# ---------------------------------------------------------------------------


def test_set_param_override_round_trip(conn):
    """set -> list shows the override; value/applied_by/applied_at persisted verbatim."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    override_id = set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "42", "test-agent",
    )

    rows = list_param_overrides(conn, scenario_id)
    assert len(rows) == 1
    assert rows[0]["override_id"] == override_id
    assert rows[0]["field_name"] == "safety_stock_qty"
    assert rows[0]["value"] == "42"
    assert rows[0]["applied_by"] == "test-agent"
    assert rows[0]["applied_at"] is not None
    assert rows[0]["location_id"] is None


def test_set_param_override_stores_stripped_normalized_value(conn):
    """
    A value padded with a non-ASCII space (NBSP, \\xa0) must be stored in its
    STRIPPED form, not the raw form. Python's str.strip() treats \\xa0 as
    whitespace, so ' 42\\xa0' would pass write-time validation, but
    PostgreSQL's int4in()/numeric_in() do NOT strip \\xa0 at cast time — if
    the raw value were stored, resolved_params_sql()'s ::integer cast would
    explode for an innocent reader. Storing the validated, stripped value
    closes that gap; this test proves the round trip end to end against the
    real cast.
    """
    item_id, location_id = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    nbsp_padded = "\xa042\xa0"
    set_param_override(
        conn, scenario_id, item_id, "lead_time_sourcing_days", nbsp_padded, "t",
    )

    rows = list_param_overrides(conn, scenario_id)
    assert len(rows) == 1
    assert rows[0]["value"] == "42", (
        "the NBSP-padded raw value must NOT be what gets persisted — only "
        "the stripped '42' is safe for the resolver's ::integer cast"
    )

    # End-to-end proof: the resolver's real ::integer cast succeeds and
    # returns the correct value — it would have raised a Postgres cast
    # error had the raw NBSP-padded string been stored instead.
    fragment = resolved_params_sql("ipp")
    sql = f"SELECT rp.* FROM ({fragment}) rp WHERE rp.item_id = %(item_id)s AND rp.location_id = %(location_id)s"
    row = conn.execute(
        sql, {"scenario_id": scenario_id, "item_id": item_id, "location_id": location_id}
    ).fetchone()
    assert row["lead_time_sourcing_days"] == 42


def test_set_param_override_upsert_same_key_updates_value(conn):
    """Two item-global sets on the same (scenario, item, NULL location, field)
    produce ONE row (UNIQUE NULLS NOT DISTINCT) holding the SECOND value."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    first_id = set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "42", "agent-1",
    )
    second_id = set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "84", "agent-2",
    )

    rows = list_param_overrides(conn, scenario_id)
    assert len(rows) == 1, (
        "two item-global sets must upsert into ONE row — a second row means "
        "UNIQUE NULLS NOT DISTINCT is not doing its job on NULL location_id"
    )
    assert rows[0]["value"] == "84"
    assert rows[0]["applied_by"] == "agent-2"
    # ON CONFLICT DO UPDATE keeps the original PK: both calls report it.
    assert first_id == second_id == rows[0]["override_id"]


def test_set_param_override_rejects_unknown_field(conn):
    """field_name outside ALLOWED_PARAM_FIELDS -> ParamOverlayError (a ValueError),
    nothing written. `reorder_point_qty` is a REAL item_planning_params column
    deliberately absent from the V1 whitelist — the sharpest rejection case."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    # ParamOverlayError subclasses ValueError — callers may catch either.
    with pytest.raises(ValueError):
        set_param_override(conn, scenario_id, item_id, "not_a_field", "1", "t")
    with pytest.raises(ParamOverlayError):
        set_param_override(conn, scenario_id, item_id, "reorder_point_qty", "1", "t")

    assert list_param_overrides(conn, scenario_id) == []


def test_check_constraint_rejects_non_whitelisted_field_direct_insert(conn):
    """The DB-side belt: a direct INSERT bypassing the Python whitelist is
    rejected by the CHECK constraint on field_name (migration 060)."""
    item_id = _seed_item(conn)
    scenario_id = _seed_scenario(conn)

    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            """
            INSERT INTO scenario_planning_overrides
                (scenario_id, item_id, field_name, value, applied_by)
            VALUES (%s, %s, 'reorder_point_qty', '1', 'test-direct')
            """,
            (scenario_id, item_id),
        )


def test_python_whitelist_matches_sql_check_constraint(conn):
    """Belt and suspenders never drift: the field_name CHECK constraint in the
    live schema allows exactly the fields in ALLOWED_PARAM_FIELDS."""
    rows = conn.execute(
        """
        SELECT pg_get_constraintdef(oid) AS condef
        FROM pg_constraint
        WHERE conrelid = 'scenario_planning_overrides'::regclass
          AND contype = 'c'
        """
    ).fetchall()
    assert len(rows) == 1, (
        f"expected exactly one CHECK constraint on scenario_planning_overrides "
        f"(the field_name whitelist), found {len(rows)}"
    )

    fields_in_check = set(re.findall(r"'([A-Za-z_]+)'", rows[0]["condef"]))
    assert fields_in_check == set(ALLOWED_PARAM_FIELDS)


def test_set_param_override_rejects_baseline_scenario(conn):
    """Applying an override to the baseline scenario is refused; nothing written."""
    item_id, _ = _seed_item_loc_params(conn)

    with pytest.raises(ParamOverlayError, match="baseline"):
        set_param_override(
            conn, UUID(BASELINE), item_id, "safety_stock_qty", "1", "test-agent",
        )

    count = conn.execute(
        "SELECT count(*) AS n FROM scenario_planning_overrides"
    ).fetchone()
    assert count["n"] == 0


def test_set_param_override_rejects_unknown_scenario(conn):
    """A scenario_id absent from `scenarios` -> ParamOverlayError."""
    item_id, _ = _seed_item_loc_params(conn)

    with pytest.raises(ParamOverlayError, match="does not exist"):
        set_param_override(
            conn, uuid4(), item_id, "safety_stock_qty", "1", "test-agent",
        )


def test_set_param_override_rejects_invalid_value_for_field_type(conn):
    """Write-time value validation (fail loudly AT THE WRITE): a value that
    the resolver's cast would reject at read time is refused before any row
    is written — 'abc' into NUMERIC safety_stock_qty is THE motivating case."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    with pytest.raises(ParamOverlayError, match="numeric"):
        set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "abc", "t")
    with pytest.raises(ParamOverlayError, match="integer"):
        set_param_override(
            conn, scenario_id, item_id, "lead_time_sourcing_days", "1.5", "t"
        )
    with pytest.raises(ParamOverlayError, match="non-empty"):
        set_param_override(
            conn, scenario_id, item_id, "forecast_consumption_strategy", "  ", "t"
        )

    assert list_param_overrides(conn, scenario_id) == []


def test_set_param_override_rejects_negative_lead_time(conn):
    """Business-bound rejection (RETURNED case): item_planning_params'
    CHECK (lead_time_sourcing_days >= 0) (migration 007) means '-3' can
    never be legal on the base table — the write-time validator must refuse
    it, not just the resolver's ::integer cast (which would happily accept
    '-3' as a valid integer, just not a valid CHECK value)."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    with pytest.raises(ParamOverlayError, match="minimum"):
        set_param_override(
            conn, scenario_id, item_id, "lead_time_sourcing_days", "-3", "t"
        )

    assert list_param_overrides(conn, scenario_id) == []


def test_set_param_override_rejects_zero_for_strictly_positive_field(conn):
    """min_order_qty's CHECK is > 0 (migration 007), not >= 0 — zero must be
    refused at write time just like it would be by the base table's CHECK."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    with pytest.raises(ParamOverlayError, match="strictly greater than"):
        set_param_override(conn, scenario_id, item_id, "min_order_qty", "0", "t")

    assert list_param_overrides(conn, scenario_id) == []


# ---------------------------------------------------------------------------
# Orphan-target override rejection (chantier #347 review finding): a write
# against (item, location) or (item, *) with no CURRENT item_planning_params
# row is a silent no-op for every reader of resolved_params_sql() — the
# override persists but is permanently invisible. A watcher measuring
# before/after would see delta=0 and wrongly conclude "no impact".
# ---------------------------------------------------------------------------


def test_set_param_override_rejects_wrong_location_for_item(conn):
    """The item has a CURRENT row, but NOT at the target location — the
    override would be silently inert (no base row to COALESCE against)."""
    item_id, real_location_id = _seed_item_loc_params(conn)
    phantom_location_id = _seed_location(conn, "overlay-orphan-location")
    scenario_id = _seed_scenario(conn)

    with pytest.raises(ParamOverlayError, match="no current planning-params row"):
        set_param_override(
            conn, scenario_id, item_id, "safety_stock_qty", "42", "t",
            location_id=phantom_location_id,
        )

    assert list_param_overrides(conn, scenario_id) == []
    # The real location is unaffected and would still resolve normally.
    row = _resolve_one(conn, scenario_id, item_id, real_location_id)
    assert row["safety_stock_qty"] == 10


def test_set_param_override_rejects_item_with_no_planning_params_at_all(conn):
    """An item-global override (location_id=None) on an item with ZERO
    current item_planning_params rows anywhere would be silently inert."""
    item_id = _seed_item(conn)  # no _seed_planning_params call at all
    scenario_id = _seed_scenario(conn)

    with pytest.raises(ParamOverlayError, match="no current planning-params row"):
        set_param_override(
            conn, scenario_id, item_id, "safety_stock_qty", "42", "t",
        )

    assert list_param_overrides(conn, scenario_id) == []


def test_set_param_override_accepts_item_global_when_any_location_current(conn):
    """Contrast case: an item-global override (location_id=None) IS accepted
    as long as at least one CURRENT row exists for the item, at any
    location — it does not need to match every location the item has."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(conn, item_id, location_id)
    scenario_id = _seed_scenario(conn)

    # Should not raise.
    set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "42", "t")
    assert len(list_param_overrides(conn, scenario_id)) == 1


def test_set_param_override_rejects_orphan_only_superseded_scd2_row(conn):
    """An item whose ONLY item_planning_params row is a superseded (past
    effective_to) SCD2 version has no CURRENT row — the orphan check must
    reuse the same "current" definition the resolver uses
    (_CURRENT_ROW_PREDICATE), so this is refused exactly like "no row at
    all", not silently accepted because SOME row exists in the table."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(
        conn, item_id, location_id,
        effective_from=date(2020, 1, 1), effective_to=date(2024, 1, 1),
    )
    scenario_id = _seed_scenario(conn)

    with pytest.raises(ParamOverlayError, match="no current planning-params row"):
        set_param_override(
            conn, scenario_id, item_id, "safety_stock_qty", "42", "t",
            location_id=location_id,
        )


# ---------------------------------------------------------------------------
# Typed FK errors: a phantom item_id/location_id must surface as
# ParamOverlayError (with only the offending UUIDs, no raw psycopg message),
# never a bare psycopg.errors.ForeignKeyViolation leaking to the caller.
# ---------------------------------------------------------------------------


def test_set_param_override_rejects_phantom_item_id_with_typed_error(conn):
    """
    A phantom item_id is caught by the orphan-row check
    (_assert_current_planning_params_row_exists) BEFORE the INSERT even
    runs — no item_planning_params row can exist for an item that doesn't
    exist — so this test proves the end-to-end result is a typed
    ParamOverlayError, never a raw psycopg exception, for the common
    "made-up UUID" caller mistake.

    The INSERT-time ForeignKeyViolation catch in set_param_override is
    genuine defence-in-depth for a case this orphan check cannot observe
    (item_id/location_id passing the item_planning_params existence check
    yet still failing the INSERT's own FK, e.g. under exotic concurrent-
    delete timing) rather than a path reachable through the normal API in a
    single-threaded test — it is exercised structurally by
    test_set_param_override_rejects_wrong_location_for_item and
    test_set_param_override_rejects_item_with_no_planning_params_at_all
    already covering the realistic orphan-target scenarios; this test
    focuses on the observable contract: no psycopg.Error ever escapes.
    """
    scenario_id = _seed_scenario(conn)
    phantom_item_id = uuid4()

    with pytest.raises(ParamOverlayError) as exc_info:
        set_param_override(
            conn, scenario_id, phantom_item_id, "safety_stock_qty", "42", "t",
        )
    assert not isinstance(exc_info.value, psycopg.Error)


def test_set_param_override_rejects_empty_applied_by(conn):
    """applied_by='' or whitespace-only -> ParamOverlayError (DB NOT NULL is
    the belt; this is the suspenders that also catches blank strings)."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    with pytest.raises(ParamOverlayError, match="applied_by"):
        set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "1", "")
    with pytest.raises(ParamOverlayError, match="applied_by"):
        set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "1", "   ")

    assert list_param_overrides(conn, scenario_id) == []


def test_clear_param_override_deletes_existing_row_returns_true(conn):
    """clear on an existing override deletes the row and returns True."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)
    set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "42", "t")

    assert clear_param_override(conn, scenario_id, item_id, "safety_stock_qty") is True
    assert list_param_overrides(conn, scenario_id) == []


def test_clear_param_override_missing_row_returns_false(conn):
    """Clearing a (scenario, item, field) with no override is a no-op -> False."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    assert clear_param_override(conn, scenario_id, item_id, "safety_stock_qty") is False


def test_clear_param_override_rejects_unknown_scenario(conn):
    """Symmetry with set_param_override: a scenario_id absent from `scenarios`
    is a caller bug -> same ParamOverlayError as set, NOT a silent False."""
    item_id, _ = _seed_item_loc_params(conn)

    with pytest.raises(ParamOverlayError, match="does not exist"):
        clear_param_override(conn, uuid4(), item_id, "safety_stock_qty")


def test_clear_param_override_rejects_unknown_field(conn):
    """Symmetry with set_param_override: field_name outside the whitelist is
    a caller bug -> ParamOverlayError, NOT a silent False. An existing
    override on a VALID field must survive the failed call."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)
    set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "42", "t")

    with pytest.raises(ParamOverlayError, match="not in the allowed"):
        clear_param_override(conn, scenario_id, item_id, "reorder_point_qty")

    assert len(list_param_overrides(conn, scenario_id)) == 1


def test_clear_param_override_location_scoped_vs_item_global_independent(conn):
    """Clearing the item-global (location_id=None) override must NOT delete a
    location-scoped override for the same (scenario, item, field), and vice versa."""
    item_id, location_id = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "20", "t")
    set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "30", "t",
        location_id=location_id,
    )

    # Clear item-global -> location-scoped row survives.
    assert clear_param_override(conn, scenario_id, item_id, "safety_stock_qty") is True
    rows = list_param_overrides(conn, scenario_id)
    assert len(rows) == 1
    assert rows[0]["location_id"] == location_id

    # Re-seed the global one, then clear location-scoped -> global survives.
    set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "20", "t")
    assert (
        clear_param_override(
            conn, scenario_id, item_id, "safety_stock_qty", location_id=location_id,
        )
        is True
    )
    rows = list_param_overrides(conn, scenario_id)
    assert len(rows) == 1
    assert rows[0]["location_id"] is None


def test_list_param_overrides_scoped_to_scenario(conn):
    """list_param_overrides only returns rows for the requested scenario_id,
    even when another scenario overrides the same (item, field)."""
    item_id, _ = _seed_item_loc_params(conn)
    fork_a = _seed_scenario(conn, "overlay-fork-a")
    fork_b = _seed_scenario(conn, "overlay-fork-b")

    set_param_override(conn, fork_a, item_id, "safety_stock_qty", "111", "agent-a")
    set_param_override(conn, fork_b, item_id, "safety_stock_qty", "222", "agent-b")

    rows_a = list_param_overrides(conn, fork_a)
    assert len(rows_a) == 1
    assert rows_a[0]["scenario_id"] == fork_a
    assert rows_a[0]["value"] == "111"


# ---------------------------------------------------------------------------
# Resolver precedence: location-exact > item-global > base value
# ---------------------------------------------------------------------------


def test_resolver_returns_base_value_when_no_override(conn):
    """A scenario with zero overrides resolves bit-identically to baseline
    (scenario_id=None): pure item_planning_params base values."""
    item_id, location_id = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    row = _resolve_one(conn, scenario_id, item_id, location_id)
    assert row["safety_stock_qty"] == 10
    assert row["lead_time_sourcing_days"] == 14
    assert row["lot_size_rule"] == "LOTFORLOT"
    assert row == _resolve_one(conn, None, item_id, location_id)


def test_resolver_applies_item_global_override(conn):
    """An override with location_id=None applies to the (item, location) row
    when no more specific location-scoped override exists."""
    item_id, location_id = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    set_param_override(conn, scenario_id, item_id, "lead_time_sourcing_days", "21", "t")

    row = _resolve_one(conn, scenario_id, item_id, location_id)
    assert row["lead_time_sourcing_days"] == 21


def test_resolver_location_exact_override_wins_over_item_global(conn):
    """All three precedence tiers in ONE scenario: exact (item, location)
    override > item-global override > base value."""
    item_id = _seed_item(conn)
    loc_exact = _seed_location(conn, "overlay-loc-exact")
    loc_global_only = _seed_location(conn, "overlay-loc-global-only")
    _seed_planning_params(conn, item_id, loc_exact, safety_stock_qty=10)
    _seed_planning_params(conn, item_id, loc_global_only, safety_stock_qty=10)
    scenario_id = _seed_scenario(conn)

    set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "20", "t")
    set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "30", "t",
        location_id=loc_exact,
    )

    row_exact = _resolve_one(conn, scenario_id, item_id, loc_exact)
    row_global = _resolve_one(conn, scenario_id, item_id, loc_global_only)

    assert row_exact["safety_stock_qty"] == 30, "location-exact must beat item-global"
    assert row_global["safety_stock_qty"] == 20, "item-global applies where no exact match"
    assert row_exact["lead_time_sourcing_days"] == 14, "un-overridden field stays base"


def test_resolver_coalesce_correct_per_field_independent_precedence(conn):
    """COALESCE is per-field, not all-or-nothing: on the same (item, location)
    one field resolves from a location-scoped override, another from an
    item-global override, a third from base."""
    item_id, location_id = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)

    set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "77", "t",
        location_id=location_id,
    )
    set_param_override(conn, scenario_id, item_id, "lead_time_sourcing_days", "21", "t")

    row = _resolve_one(conn, scenario_id, item_id, location_id)
    assert row["safety_stock_qty"] == 77          # location-scoped override
    assert row["lead_time_sourcing_days"] == 21   # item-global override
    assert row["consumption_window_days"] == 7    # base value, untouched


def test_resolver_lot_size_rule_enum_cast_round_trips(conn):
    """lot_size_rule (base column: lot_size_rule_type ENUM) resolves through
    the ::text cast on both COALESCE operands — override 'EOQ' beats base
    'LOTFORLOT' without a Postgres type error."""
    item_id, location_id = _seed_item_loc_params(conn, lot_size_rule="LOTFORLOT")
    scenario_id = _seed_scenario(conn)

    set_param_override(
        conn, scenario_id, item_id, "lot_size_rule", "EOQ", "t",
        location_id=location_id,
    )
    row = _resolve_one(conn, scenario_id, item_id, location_id)
    assert row["lot_size_rule"] == "EOQ"

    # Write-time enum validation: because the resolver casts BOTH COALESCE
    # operands to ::text, a bogus value would flow through resolution as
    # plain text — so set_param_override validates lot_size_rule against
    # LOT_SIZE_RULE_VALUES (the real lot_size_rule_type enum values,
    # migrations 007+021) AT THE WRITE. The bogus value is refused and the
    # resolved view keeps the last legal override.
    with pytest.raises(ParamOverlayError, match="lot_size_rule"):
        set_param_override(
            conn, scenario_id, item_id, "lot_size_rule", "NOT_A_REAL_RULE", "t",
            location_id=location_id,
        )
    row = _resolve_one(conn, scenario_id, item_id, location_id)
    assert row["lot_size_rule"] == "EOQ"


# ---------------------------------------------------------------------------
# Isolation (type C0) — THE chantier invariant
# ---------------------------------------------------------------------------


def test_resolver_scenario_id_none_is_baseline_pure(conn):
    """scenario_id=None never matches any override row — every field falls
    back to base even when overrides exist for OTHER scenarios on the item."""
    item_id, location_id = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)
    set_param_override(
        conn, scenario_id, item_id, "safety_stock_qty", "999", "t",
        location_id=location_id,
    )

    row = _resolve_one(conn, None, item_id, location_id)
    assert row["safety_stock_qty"] == 10


def test_resolver_ignores_overrides_from_other_scenarios(conn):
    """An override set on fork A must not leak into fork B's resolved view."""
    item_id, location_id = _seed_item_loc_params(conn)
    fork_a = _seed_scenario(conn, "overlay-fork-a")
    fork_b = _seed_scenario(conn, "overlay-fork-b")

    set_param_override(
        conn, fork_a, item_id, "safety_stock_qty", "999", "t",
        location_id=location_id,
    )

    assert _resolve_one(conn, fork_b, item_id, location_id)["safety_stock_qty"] == 10


def test_c0_isolation_fork_override_leaves_baseline_and_sibling_identical(conn):
    """THE chantier #347 invariant: after an override in fork A, the FULL
    resolved row for baseline (None) and for sibling fork B is strictly
    identical to the pre-override state; only fork A sees the new value."""
    item_id, location_id = _seed_item_loc_params(conn)
    fork_a = _seed_scenario(conn, "overlay-c0-fork-a")
    fork_b = _seed_scenario(conn, "overlay-c0-fork-b")

    baseline_before = _resolve_one(conn, None, item_id, location_id)
    fork_b_before = _resolve_one(conn, fork_b, item_id, location_id)

    set_param_override(
        conn, fork_a, item_id, "safety_stock_qty", "999", "test-agent",
        location_id=location_id,
    )

    row_a = _resolve_one(conn, fork_a, item_id, location_id)
    assert row_a["safety_stock_qty"] == 999

    # Full-row strict equality — not just the overridden field: the overlay
    # must not perturb ANY resolved column outside its target scenario.
    assert _resolve_one(conn, None, item_id, location_id) == baseline_before
    assert _resolve_one(conn, fork_b, item_id, location_id) == fork_b_before
    assert fork_b_before == baseline_before


# ---------------------------------------------------------------------------
# SCD2: the resolver only sees the CURRENT item_planning_params row
# ---------------------------------------------------------------------------


def test_resolver_only_returns_current_scd2_row(conn):
    """A superseded (effective_to in the past) row is excluded: the resolver
    returns exactly one row per (item, location), built from the CURRENT
    version — and the override applies on top of that current row."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    # Closed historical version with DIFFERENT values on every asserted field.
    _seed_planning_params(
        conn, item_id, location_id,
        effective_from=date(2020, 1, 1), effective_to=date(2024, 1, 1),
        safety_stock_qty=111, lead_time_sourcing_days=99,
    )
    # Current version (effective_to IS NULL). Ranges are half-open ([from, to))
    # per the ipp_item_location_active_unique exclusion constraint, so
    # starting exactly at the historical row's effective_to is legal.
    _seed_planning_params(
        conn, item_id, location_id,
        effective_from=date(2024, 1, 1), effective_to=None,
        safety_stock_qty=10, lead_time_sourcing_days=14,
    )
    scenario_id = _seed_scenario(conn)
    set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "42", "t")

    rows = _resolve_rows(conn, scenario_id, item_id)
    assert len(rows) == 1, "historical SCD2 row must not produce a second resolved row"
    assert rows[0]["safety_stock_qty"] == 42        # override on top of current
    assert rows[0]["lead_time_sourcing_days"] == 14  # current base, not historical 99


def test_resolver_treats_sentinel_effective_to_as_current(conn):
    """The repo-wide SCD2 idiom tolerates effective_to='9999-12-31' as an
    open-ended sentinel: such a row is CURRENT for the resolver too."""
    item_id = _seed_item(conn)
    location_id = _seed_location(conn)
    _seed_planning_params(
        conn, item_id, location_id, effective_to=date(9999, 12, 31),
    )

    rows = _resolve_rows(conn, None, item_id)
    assert len(rows) == 1
    assert rows[0]["safety_stock_qty"] == 10


# ---------------------------------------------------------------------------
# FK retention (ADR-011): overrides block scenario hard-delete
# ---------------------------------------------------------------------------


def test_delete_scenario_with_active_override_raises_fk_violation(conn):
    """DELETE FROM scenarios fails with ForeignKeyViolation while an override
    references the scenario (ON DELETE RESTRICT, ADR-011 — soft-delete via
    status='archived' remains the only path)."""
    item_id, _ = _seed_item_loc_params(conn)
    scenario_id = _seed_scenario(conn)
    set_param_override(conn, scenario_id, item_id, "safety_stock_qty", "1", "t")

    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        conn.execute("DELETE FROM scenarios WHERE scenario_id = %s", (scenario_id,))
