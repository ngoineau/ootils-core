"""
param_overlay.py — chantier #347, PR1: the single resolver for scenario-scoped
planning-parameter overrides.

Design (see scenario/manager.py for the sibling node-override pattern this
mirrors):
  - `item_planning_params` is the SCD2 base table (migration 007/021):
    typed columns, one CURRENT row per (item_id, location_id) identified by
    `effective_to IS NULL` (the exclusion constraint also tolerates the
    '9999-12-31' open-ended sentinel — see the repo-wide idiom used in
    ingest.py / mrp.py / projection.py).
  - `scenario_planning_overrides` (migration 060) holds scenario-scoped
    field-level overrides on top of that base row: (scenario_id, item_id,
    location_id NULLABLE, field_name) -> value (TEXT).
  - `resolved_params_sql()` builds a composable SQL fragment that LEFT JOINs
    the overrides onto the current base row and COALESCEs field-by-field,
    so callers (PR2/PR3 read paths) get one resolved row per (item_id,
    location_id) without duplicating the resolution logic.

This module is NOT wired into any reader yet — that is PR2/PR3. It is safe
to import today: it only reads/writes `scenario_planning_overrides` and
reads `item_planning_params`; nothing else depends on it.

All DB access is psycopg3; the caller owns commit/rollback (same contract
as ScenarioManager — this module never calls conn.commit()/rollback()).
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import NamedTuple, Optional
from uuid import UUID, uuid4

import psycopg
from psycopg.errors import ForeignKeyViolation

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Whitelist — the ONLY source of field names ever interpolated into SQL by
# this module. Every reference to a field name in resolved_params_sql() /
# set_param_override() / clear_param_override() is resolved through this
# dict; nothing here is ever built from external input. The CHECK constraint
# on scenario_planning_overrides.field_name (migration 060) is the DB-side
# belt to this Python-side suspenders.
#
# Value = the Postgres cast target used to turn the override's TEXT value
# back into the base column's type before COALESCE-ing against it.
# Types below were read off the real columns (migrations 007 + 021):
#   - lead_time_*_days, lot_size_poq_periods, frozen/slashed_time_fence_days,
#     consumption_window_days -> INTEGER
#   - safety_stock_qty/days, min/max_order_qty, order_multiple_qty,
#     economic_order_qty -> NUMERIC
#   - lot_size_rule (DB type lot_size_rule_type ENUM), forecast_consumption_
#     strategy -> TEXT (resolved_params_sql casts BOTH sides — override
#     value and base column — to ::text for lot_size_rule, since COALESCE
#     requires matching operand types and the base column is the ENUM, not
#     text; callers consume the resolved value as text, matching how
#     ingest.py's PlanningParamsRow already treats it)
# ---------------------------------------------------------------------------
ALLOWED_PARAM_FIELDS: dict[str, str] = {
    "lead_time_sourcing_days": "integer",
    "lead_time_manufacturing_days": "integer",
    "lead_time_transit_days": "integer",
    "safety_stock_qty": "numeric",
    "safety_stock_days": "numeric",
    "min_order_qty": "numeric",
    "max_order_qty": "numeric",
    "order_multiple_qty": "numeric",
    "lot_size_rule": "text",
    "economic_order_qty": "numeric",
    "lot_size_poq_periods": "integer",
    "frozen_time_fence_days": "integer",
    "slashed_time_fence_days": "integer",
    "forecast_consumption_strategy": "text",
    "consumption_window_days": "integer",
}

# Frozen view for callers that only need membership testing (mirrors
# scenario/manager.py's _ALLOWED_FIELDS frozenset idiom).
_ALLOWED_FIELD_NAMES: frozenset[str] = frozenset(ALLOWED_PARAM_FIELDS)


class _FieldBounds(NamedTuple):
    """Business-rule bounds mirroring the real CHECK constraints on
    item_planning_params, so an override can never carry a value the base
    table itself could not hold (see PARAM_FIELD_BOUNDS)."""

    minimum: Decimal
    min_inclusive: bool
    maximum: Decimal


# Business-rule bounds, one entry per ALLOWED_PARAM_FIELDS key, mirroring the
# REAL CHECK constraints on item_planning_params (verified against the SQL
# source, not assumed):
#   - migration 007_import_pipeline.sql:
#       lead_time_sourcing_days / lead_time_manufacturing_days /
#       lead_time_transit_days      CHECK (... >= 0)
#       safety_stock_qty            CHECK (safety_stock_qty >= 0)
#       safety_stock_days           CHECK (safety_stock_days >= 0)
#       min_order_qty                CHECK (min_order_qty > 0)
#       max_order_qty                CHECK (max_order_qty > 0)
#   - migration 021_mrp_lot_sizing_params.sql:
#       economic_order_qty          CHECK (economic_order_qty > 0)
#       lot_size_poq_periods        CHECK (lot_size_poq_periods > 0)
#       order_multiple_qty          CHECK (order_multiple_qty > 0)
#       frozen_time_fence_days      CHECK (frozen_time_fence_days >= 0)
#       slashed_time_fence_days     CHECK (slashed_time_fence_days > 0)
#         NOTE: slashed_time_fence_days is strictly positive in the real
#         schema (NOT >= 0 — verify against the migration before changing).
#       consumption_window_days     CHECK (consumption_window_days > 0)
# forecast_consumption_strategy and lot_size_rule carry no numeric CHECK
# (text/enum fields) and are intentionally absent from this map;
# _validate_value() only consults it for integer/numeric cast targets.
#
# The upper bound is NOT a real DB constraint (none of these columns have a
# CHECK ceiling) — it is a pragmatic anti-garbage ceiling (10**12) so a
# value like '1e999999' (a finite-but-absurd Decimal) cannot be persisted;
# every quantity/day field in this domain is demo-to-production scale, not
# anywhere near 10**12.
_PRAGMATIC_MAX = Decimal(10) ** 12
_INT4_MIN = Decimal(-2_147_483_648)
_INT4_MAX = Decimal(2_147_483_647)

PARAM_FIELD_BOUNDS: dict[str, _FieldBounds] = {
    "lead_time_sourcing_days": _FieldBounds(Decimal(0), True, _INT4_MAX),
    "lead_time_manufacturing_days": _FieldBounds(Decimal(0), True, _INT4_MAX),
    "lead_time_transit_days": _FieldBounds(Decimal(0), True, _INT4_MAX),
    "safety_stock_qty": _FieldBounds(Decimal(0), True, _PRAGMATIC_MAX),
    "safety_stock_days": _FieldBounds(Decimal(0), True, _PRAGMATIC_MAX),
    "min_order_qty": _FieldBounds(Decimal(0), False, _PRAGMATIC_MAX),
    "max_order_qty": _FieldBounds(Decimal(0), False, _PRAGMATIC_MAX),
    "order_multiple_qty": _FieldBounds(Decimal(0), False, _PRAGMATIC_MAX),
    "economic_order_qty": _FieldBounds(Decimal(0), False, _PRAGMATIC_MAX),
    "lot_size_poq_periods": _FieldBounds(Decimal(0), False, _INT4_MAX),
    "frozen_time_fence_days": _FieldBounds(Decimal(0), True, _INT4_MAX),
    "slashed_time_fence_days": _FieldBounds(Decimal(0), False, _INT4_MAX),
    "consumption_window_days": _FieldBounds(Decimal(0), False, _INT4_MAX),
}

# Legal values of the Postgres ENUM lot_size_rule_type. Source of truth:
#   - migration 007_import_pipeline.sql (CREATE TYPE lot_size_rule_type):
#       LOTFORLOT, FIXED_QTY, PERIOD_OF_SUPPLY, MIN_MAX
#   - migration 021_mrp_lot_sizing_params.sql (ALTER TYPE ... ADD VALUE):
#       POQ, EOQ, MULTIPLE
# If a future migration extends the enum, extend this constant in the same
# change — set_param_override() validates lot_size_rule override values
# against it at write time (fail loudly at the write, not at some innocent
# reader's resolution).
LOT_SIZE_RULE_VALUES: frozenset[str] = frozenset(
    {
        "LOTFORLOT",
        "FIXED_QTY",
        "PERIOD_OF_SUPPLY",
        "MIN_MAX",
        "POQ",
        "EOQ",
        "MULTIPLE",
    }
)

# Strict integer literal (what Postgres' int4 input function accepts, modulo
# surrounding whitespace which we strip first). Deliberately stricter than
# Python's int(): int('1_0') is 10 in Python but '1_0'::integer is an error
# in Postgres — the write-time validator must not accept what the resolver's
# cast would later reject. ASCII-only ([0-9], not \d): \d matches Unicode
# decimal digits (e.g. Arabic-Indic '١٢٣') that Python's re accepts but
# Postgres' int4in()/numeric_in() reject outright — the write-time validator
# must not be looser than the cast it is protecting against.
_INTEGER_LITERAL = re.compile(r"^[+-]?[0-9]+$")

# Same ASCII-only rationale as _INTEGER_LITERAL: used as a pre-check before
# handing the string to Decimal(), because Decimal() itself is Unicode-aware
# (Decimal('١٢٣') parses to 123 without error) and would silently re-open
# the exact hole the integer regex closes. Deliberately permissive on WHICH
# ASCII characters (not just digit/sign/exponent) so legitimate ASCII
# Decimal tokens like 'NaN' / 'Infinity' / 'inf' still reach Decimal() and
# get refused by the explicit is_finite() check below with its clearer
# "finite" message — this regex's only job is excluding non-ASCII input,
# never judging numeric shape (Decimal() and the int4/business-bounds
# checks already do that).
_ASCII_ONLY = re.compile(r"^[\x00-\x7f]+$")

# SCD2 "current row" predicate, matching the repo-wide idiom (see ingest.py,
# mrp.py, projection.py, propagator.py): the exclusion constraint on
# item_planning_params uses '9999-12-31' as an open-ended sentinel, so a
# "current" row can show up either as effective_to IS NULL or as that
# sentinel value.
_CURRENT_ROW_PREDICATE = "(effective_to IS NULL OR effective_to = '9999-12-31'::DATE)"

# Reserved prefix for the INTERNAL aliases resolved_params_sql() generates
# inside each per-field LATERAL join (the override-row alias and the base
# column's local name). Callers must never be able to pass an outer `alias`
# that collides with one of these — see _RESERVED_INTERNAL_ALIAS_PREFIX
# below and the collision this prefix exists to prevent.
_RESERVED_INTERNAL_ALIAS_PREFIX = "__po_"

# A safe SQL identifier: letters/digits/underscore, must not start with a
# digit, and must not start with the reserved internal-alias prefix (that
# prefix is reserved for resolved_params_sql()'s own LATERAL-join aliases —
# see _RESERVED_INTERNAL_ALIAS_PREFIX). Used to guard the (currently
# always-literal) `alias` parameter of resolved_params_sql() before it is
# spliced into the fragment.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ParamOverlayError(ValueError):
    """Raised for invalid field names or invalid override targets."""


def _validate_field_name(field_name: str) -> None:
    if field_name not in _ALLOWED_FIELD_NAMES:
        raise ParamOverlayError(
            f"field_name {field_name!r} is not in the allowed planning-param "
            f"override list. Allowed: {sorted(_ALLOWED_FIELD_NAMES)}"
        )


def _check_business_bounds(field_name: str, parsed: Decimal, normalized: str) -> None:
    """Reject a value the base item_planning_params CHECK constraints could
    never hold (PARAM_FIELD_BOUNDS — mirrors migrations 007/021). An override
    must never carry a value that would be DB-illegal on the table it
    ultimately overlays."""
    bounds = PARAM_FIELD_BOUNDS.get(field_name)
    if bounds is None:
        return

    if bounds.min_inclusive:
        if parsed < bounds.minimum:
            raise ParamOverlayError(
                f"value {normalized!r} for field {field_name!r} is below the "
                f"minimum {bounds.minimum} (inclusive) allowed by "
                f"item_planning_params' CHECK constraint."
            )
    else:
        if parsed <= bounds.minimum:
            raise ParamOverlayError(
                f"value {normalized!r} for field {field_name!r} must be "
                f"strictly greater than {bounds.minimum}, per "
                f"item_planning_params' CHECK constraint."
            )

    if parsed > bounds.maximum:
        raise ParamOverlayError(
            f"value {normalized!r} for field {field_name!r} exceeds the "
            f"maximum {bounds.maximum} allowed for this field."
        )


def _validate_value(field_name: str, value: str) -> str:
    """
    Fail-loudly write-time validation of an override value against the cast
    target of its field (ALLOWED_PARAM_FIELDS) AND against the business
    bounds the base item_planning_params table itself enforces
    (PARAM_FIELD_BOUNDS). Without this, a bogus value (e.g. 'abc' for the
    NUMERIC safety_stock_qty, or a negative lead time) would be persisted
    fine — the TEXT column accepts anything — and only explode later, inside
    an innocent reader executing resolved_params_sql()'s cast, or worse,
    silently apply a value the base table could never legally hold.

    Rules mirror what the resolver's Postgres casts will accept:
      - integer  -> a strict ASCII integer literal (optional sign, ASCII
                    digits only — NOT Unicode \\d, see _INTEGER_LITERAL),
                    within the int4 range [-2147483648, 2147483647], and
                    within PARAM_FIELD_BOUNDS if the field has one.
      - numeric  -> ASCII-only characters, parseable by Decimal, finite
                    (NaN/Infinity refused: Postgres would take them, but a
                    NaN safety stock is a silent wrong answer — fail-loudly
                    principle), and within PARAM_FIELD_BOUNDS if the field
                    has one (including a pragmatic 10**12 ceiling against
                    absurd-but-finite exponents like '1e999999').
      - text     -> non-empty after strip; lot_size_rule additionally must
                    be a member of LOT_SIZE_RULE_VALUES (the base column is
                    the lot_size_rule_type ENUM — the resolver's ::text cast
                    would otherwise let any string flow through unchecked)

    Returns the NORMALIZED (stripped) value that must be the one persisted —
    validating the stripped form but inserting the raw form would let a
    value like ' 42' padded with a non-ASCII space (NBSP, \\xa0) pass Python's
    str.strip() (which treats \\xa0 as whitespace) while Postgres' int4in()/
    numeric_in() do NOT strip \\xa0 at cast time and would error in the
    resolver instead. Storing the validated, stripped value closes that gap.

    Raises ParamOverlayError (a ValueError) with the field, the offending
    value and the expected shape.
    """
    cast_type = ALLOWED_PARAM_FIELDS[field_name]

    if not isinstance(value, str):
        raise ParamOverlayError(
            f"value for {field_name!r} must be a str (serialized scalar, "
            f"scenario_planning_overrides.value is TEXT), got "
            f"{type(value).__name__}."
        )

    if cast_type == "integer":
        normalized = value.strip()
        if not _INTEGER_LITERAL.match(normalized):
            raise ParamOverlayError(
                f"value {value!r} is not a valid integer literal for field "
                f"{field_name!r} (cast target: integer)."
            )
        parsed = Decimal(normalized)
        if not (_INT4_MIN <= parsed <= _INT4_MAX):
            raise ParamOverlayError(
                f"value {value!r} for field {field_name!r} is outside the "
                f"PostgreSQL int4 range [{_INT4_MIN}, {_INT4_MAX}]."
            )
        _check_business_bounds(field_name, parsed, normalized)
        return normalized
    elif cast_type == "numeric":
        normalized = value.strip()
        # ASCII pre-check BEFORE handing to Decimal(): Decimal() itself is
        # Unicode-aware (Decimal('١٢٣') parses fine) and would silently
        # re-open the hole this validator exists to close — Postgres'
        # numeric_in() rejects non-ASCII digits outright.
        if not normalized or not _ASCII_ONLY.match(normalized):
            raise ParamOverlayError(
                f"value {value!r} is not a valid numeric literal for field "
                f"{field_name!r} (cast target: numeric)."
            )
        try:
            # PEP 515: Python's Decimal tolerates underscores ('1_0' -> 10)
            # but Postgres' numeric input function does not — refuse what
            # the resolver's ::numeric cast would later reject.
            if "_" in normalized:
                raise InvalidOperation
            parsed = Decimal(normalized)
        except InvalidOperation:
            raise ParamOverlayError(
                f"value {value!r} is not a valid numeric literal for field "
                f"{field_name!r} (cast target: numeric)."
            ) from None
        if not parsed.is_finite():
            raise ParamOverlayError(
                f"value {value!r} is not a finite number — NaN/Infinity are "
                f"refused for field {field_name!r} (silent-wrong-answer risk)."
            )
        _check_business_bounds(field_name, parsed, normalized)
        return normalized
    else:  # text
        normalized = value.strip()
        if not normalized:
            raise ParamOverlayError(
                f"value for text field {field_name!r} must be non-empty."
            )
        if field_name == "lot_size_rule" and normalized not in LOT_SIZE_RULE_VALUES:
            raise ParamOverlayError(
                f"value {normalized!r} is not a legal lot_size_rule — the "
                f"base column is the lot_size_rule_type ENUM. Allowed: "
                f"{sorted(LOT_SIZE_RULE_VALUES)}"
            )
        return normalized


def _fetch_scenario(conn: psycopg.Connection, scenario_id: UUID) -> dict:
    """Return the scenario row or raise ParamOverlayError if it doesn't exist.

    Shared by set_param_override and clear_param_override so both ends of
    the API fail identically on an unknown scenario_id.
    """
    row = conn.execute(
        "SELECT scenario_id, is_baseline FROM scenarios WHERE scenario_id = %s",
        (scenario_id,),
    ).fetchone()
    if row is None:
        raise ParamOverlayError(f"Scenario {scenario_id} does not exist.")
    return row


# ---------------------------------------------------------------------------
# resolved_params_sql
# ---------------------------------------------------------------------------


def resolved_params_sql(alias: str = "ipp") -> str:
    """
    Build the composable SQL fragment that resolves scenario-overlaid
    planning params: one row per (item_id, location_id) with the current
    base value for every whitelisted field, COALESCEd against any override
    the scenario carries for that field.

    Field-name safety: every column referenced below comes exclusively from
    ALLOWED_PARAM_FIELDS (module constant) — never from caller/request
    input — so there is no injection surface despite the string building.

    Parameter contract: the returned SQL expects exactly ONE parameter,
    the NAMED placeholder `%(scenario_id)s` — the scenario_id to resolve
    overrides for. The name is reused in every per-field LATERAL join, but
    psycopg3 binds a repeated named placeholder from a single dict key, so
    callers execute with `{"scenario_id": ...}` exactly once. Pass
    `{"scenario_id": None}` to get baseline-pure results (no override ever
    matches a NULL scenario_id, since scenario_planning_overrides.
    scenario_id is NOT NULL — the LEFT JOIN degrades to "no override row",
    which is exactly the baseline-pure behaviour).

    Precedence (per item_id): an override scoped to the exact
    (item_id, location_id) wins over an override scoped to
    (item_id, location_id IS NULL) ("item-global"), which wins over the
    item_planning_params base value.

    Implementation choice — one LEFT JOIN LATERAL per whitelisted field
    (picking the single best-matching override row per field via
    ORDER BY location_id NULLS LAST, LIMIT 1 — a non-NULL exact-location
    match sorts before the NULL/item-global one), each COALESCEd against
    its base column. This was preferred over a single "get all overrides,
    DISTINCT ON (item_id, location_id, field_name)" pre-aggregation because
    it keeps the whole resolver as ONE flat SELECT with N (whitelist-sized)
    LATERAL joins rather than a self-join across a pivoted CTE — easier for
    PR2/PR3 callers to wrap in their own JOIN/FROM and to EXPLAIN, at the
    cost of one small index-backed lookup per field per row (the table is
    scenario-scoped and override counts per scenario are expected to be
    small — audit/diagnostic scale, not bulk data).

    Callers compose with e.g.:
        f"SELECT rp.* FROM ({resolved_params_sql('ipp')}) rp "
        f"WHERE rp.item_id = %(item_id)s AND rp.location_id = %(location_id)s"
    executed with:
        {"scenario_id": ..., "item_id": ..., "location_id": ...}
    Note: psycopg3 refuses to mix named and positional placeholders in one
    statement, so a composed query must use the named style throughout.
    """
    if not _SAFE_IDENTIFIER.match(alias):
        raise ParamOverlayError(f"alias {alias!r} is not a safe SQL identifier.")
    if alias.startswith(_RESERVED_INTERNAL_ALIAS_PREFIX):
        raise ParamOverlayError(
            f"alias {alias!r} uses the reserved internal prefix "
            f"{_RESERVED_INTERNAL_ALIAS_PREFIX!r} — that prefix is reserved "
            f"for resolved_params_sql()'s own LATERAL-join aliases and must "
            f"never be assignable to a caller-supplied outer alias (alias "
            f"collision would make correlation predicates tautological)."
        )

    lateral_joins: list[str] = []
    select_fields: list[str] = []

    for field_name, cast_type in ALLOWED_PARAM_FIELDS.items():
        # Defensive re-assertion: field_name/cast_type only ever come from
        # ALLOWED_PARAM_FIELDS.items() above — this loop never sees external
        # input — but we assert anyway so a future refactor that widens the
        # iteration source fails loudly instead of opening an injection seam.
        assert field_name in ALLOWED_PARAM_FIELDS
        assert cast_type in {"integer", "numeric", "text"}
        # Reserved-prefix, collision-proof-by-construction internal aliases.
        # PREVIOUSLY the inner scenario_planning_overrides alias was the bare
        # letter 'o' — if a CALLER also happened to alias item_planning_params
        # as 'o' (a legal, unremarkable identifier _SAFE_IDENTIFIER would
        # have accepted), the inner 'o' silently shadowed the outer one:
        # `o.item_id = o.item_id` became a self-referential tautology instead
        # of a real correlation predicate, so an override on ANY item would
        # match every row. Prefixing with the reserved namespace makes the
        # inner alias impossible for a caller to reproduce (the prefix is
        # rejected by _SAFE_IDENTIFIER's check above), closing the hole by
        # construction rather than by blacklisting 'o' specifically.
        # Truncated to stay under PostgreSQL's 63-byte identifier limit —
        # the longest field name (forecast_consumption_strategy, 29 chars)
        # plus the "__po_base_" prefix (10 chars) is 39 chars, well within
        # limit, so no truncation is actually needed today, but the field
        # names are bounded by ALLOWED_PARAM_FIELDS, not by this code, so we
        # slice defensively rather than assume it forever.
        overrides_alias = f"__po_ov_{field_name}"[:63]
        join_alias = f"__po_base_{field_name}"[:63]
        lateral_joins.append(
            f"""
            LEFT JOIN LATERAL (
                SELECT {overrides_alias}.value
                FROM scenario_planning_overrides {overrides_alias}
                WHERE {overrides_alias}.scenario_id = %(scenario_id)s
                  AND {overrides_alias}.item_id = {alias}.item_id
                  AND ({overrides_alias}.location_id = {alias}.location_id
                       OR {overrides_alias}.location_id IS NULL)
                  AND {overrides_alias}.field_name = '{field_name}'
                ORDER BY {overrides_alias}.location_id NULLS LAST
                LIMIT 1
            ) {join_alias} ON TRUE
            """
        )
        # Base column is cast to the same target type as the override so
        # COALESCE always sees two operands of the same type — this matters
        # for lot_size_rule specifically: the base column is the
        # lot_size_rule_type ENUM, not text, and COALESCE(text, enum) is a
        # Postgres type-mismatch error without this explicit cast.
        select_fields.append(
            f"COALESCE({join_alias}.value::{cast_type}, {alias}.{field_name}::{cast_type}) AS {field_name}"
        )

    select_clause = ",\n        ".join(select_fields)
    joins_clause = "\n        ".join(lateral_joins)

    return f"""
    SELECT
        {alias}.param_id,
        {alias}.item_id,
        {alias}.location_id,
        {select_clause}
    FROM item_planning_params {alias}
    {joins_clause}
    WHERE {_CURRENT_ROW_PREDICATE.replace("effective_to", f"{alias}.effective_to")}
    """


# ---------------------------------------------------------------------------
# resolved_field_lateral_sql
# ---------------------------------------------------------------------------


def resolved_field_lateral_sql(field: str, base_alias: str, out_col: str) -> str:
    """
    Build a single composable `LEFT JOIN LATERAL ... AS <out_col>` fragment
    that resolves ONE whitelisted planning-param field, with the exact same
    precedence/COALESCE semantics as `resolved_params_sql()` (chantier #347
    PR3: propagation call sites — SHORTAGES_SQL, the safety-stock cache
    preload, mrp.py's `_get_planning_params` — read one or two fields inline
    inside an already-large host query and do not want a full per-field
    LATERAL fan-out for every whitelisted field they never touch).

    Field-name safety: `field` MUST be a key of ALLOWED_PARAM_FIELDS —
    ValueError (ParamOverlayError) otherwise. Exactly like
    resolved_params_sql(), no field name here is ever built from external
    input.

    Precedence, identical to resolved_params_sql(): an override scoped to
    the exact (item_id, location_id) wins over an override scoped to
    (item_id, location_id IS NULL) ("item-global"), via
    `ORDER BY location_id NULLS LAST LIMIT 1` on the current row of
    scenario_planning_overrides for `%(scenario_id)s`.

    `base_alias` is the alias of item_planning_params in the HOST query —
    the fragment correlates the override's item_id/location_id against
    `{base_alias}.item_id` / `{base_alias}.location_id`, so the caller must
    place this fragment's `LEFT JOIN LATERAL` immediately after the FROM/JOIN
    clause that introduces `base_alias`.

    `out_col` names the fragment's TWO internal joins — the override lookup
    (`{out_col}_ov`) and the COALESCE'd result (`{out_col}`), read downstream
    as `{out_col}.{out_col}` (see the usage example below). `out_col` must be
    a safe SQL identifier and must not collide with the reserved internal
    prefix used by this module's own aliases (_RESERVED_INTERNAL_ALIAS_PREFIX)
    — the same anti-tautology guard resolved_params_sql() enforces on its
    `alias` parameter. Callers composing more than one field in the same
    host query must pass a distinct `out_col` per field (e.g. the field name
    itself) — psycopg's parser would otherwise reject the duplicate alias.

    Cast: the override's TEXT value and the host query's base column are
    both cast to the field's ALLOWED_PARAM_FIELDS type before COALESCE, same
    rule as resolved_params_sql() (matters for lot_size_rule's ENUM base
    column).

    scenario_id = NULL (baseline): scenario_planning_overrides.scenario_id
    is NOT NULL, so the LATERAL's subquery matches nothing and
    COALESCE(NULL, base) degrades to the base column — baseline
    byte-identical, same guarantee as resolved_params_sql().

    Composable as:
        f'''
        SELECT {out_col}.{out_col}
        FROM item_planning_params {base_alias}
        {resolved_field_lateral_sql("safety_stock_qty", base_alias, out_col)}
        WHERE ...
        '''
    executed with `{"scenario_id": ..., ...}` — exactly ONE
    `%(scenario_id)s` placeholder per call, so composing N calls in one
    query (one per field, each with its own `out_col`) is safe (psycopg3
    binds the repeated named placeholder from the single dict key).
    """
    if field not in ALLOWED_PARAM_FIELDS:
        raise ParamOverlayError(
            f"field {field!r} is not in the allowed planning-param overlay "
            f"list. Allowed: {sorted(ALLOWED_PARAM_FIELDS)}"
        )
    if not _SAFE_IDENTIFIER.match(base_alias):
        raise ParamOverlayError(f"base_alias {base_alias!r} is not a safe SQL identifier.")
    if base_alias.startswith(_RESERVED_INTERNAL_ALIAS_PREFIX):
        raise ParamOverlayError(
            f"base_alias {base_alias!r} uses the reserved internal prefix "
            f"{_RESERVED_INTERNAL_ALIAS_PREFIX!r} — reserved for this "
            f"module's own LATERAL-join aliases."
        )
    if not _SAFE_IDENTIFIER.match(out_col):
        raise ParamOverlayError(f"out_col {out_col!r} is not a safe SQL identifier.")
    if out_col.startswith(_RESERVED_INTERNAL_ALIAS_PREFIX):
        raise ParamOverlayError(
            f"out_col {out_col!r} uses the reserved internal prefix "
            f"{_RESERVED_INTERNAL_ALIAS_PREFIX!r} — reserved for this "
            f"module's own LATERAL-join aliases."
        )

    cast_type = ALLOWED_PARAM_FIELDS[field]
    # Same reserved-prefix, collision-proof-by-construction alias as
    # resolved_params_sql() — see that function's inline comment for the
    # alias-collision/tautology bug this construction closes by design.
    overrides_alias = f"{_RESERVED_INTERNAL_ALIAS_PREFIX}ov_{field}"[:63]

    return f"""
    LEFT JOIN LATERAL (
        SELECT {overrides_alias}.value
        FROM scenario_planning_overrides {overrides_alias}
        WHERE {overrides_alias}.scenario_id = %(scenario_id)s
          AND {overrides_alias}.item_id = {base_alias}.item_id
          AND ({overrides_alias}.location_id = {base_alias}.location_id
               OR {overrides_alias}.location_id IS NULL)
          AND {overrides_alias}.field_name = '{field}'
        ORDER BY {overrides_alias}.location_id NULLS LAST
        LIMIT 1
    ) {out_col}_ov ON TRUE
    CROSS JOIN LATERAL (
        SELECT COALESCE({out_col}_ov.value::{cast_type}, {base_alias}.{field}::{cast_type}) AS {out_col}
    ) {out_col}
    """


# ---------------------------------------------------------------------------
# set_param_override / clear_param_override / list_param_overrides
# ---------------------------------------------------------------------------


def _assert_current_planning_params_row_exists(
    conn: psycopg.Connection,
    item_id: UUID,
    location_id: Optional[UUID],
) -> None:
    """
    Guard against a silently-inert override: set_param_override upserts into
    scenario_planning_overrides regardless of whether item_planning_params
    holds a matching CURRENT row, so an override on (item, wrong location) —
    or on an item with no planning params at all — used to persist fine and
    then never appear in ANY resolved_params_sql() output, because the
    resolver's FROM item_planning_params never produces a row to COALESCE
    against. A watcher measuring shortage delta before/after such an
    override would see delta=0 and wrongly conclude "no impact", when the
    real story is "the override was never visible to begin with".

    Reuses _CURRENT_ROW_PREDICATE (the same SCD2 "current row" definition
    resolved_params_sql() uses) so this check and the resolver can never
    silently disagree about what counts as current.

    location-scoped target (location_id is not None): requires a current
    row for the EXACT (item_id, location_id) pair — an item-global override
    could not apply there otherwise.
    item-global target (location_id is None): requires at least one current
    row for the item, at ANY location — that is the full set of rows the
    override could ever apply to.
    """
    if location_id is not None:
        row = conn.execute(
            f"""
            SELECT 1 FROM item_planning_params
            WHERE item_id = %s AND location_id = %s
              AND {_CURRENT_ROW_PREDICATE}
            LIMIT 1
            """,
            (item_id, location_id),
        ).fetchone()
        if row is None:
            raise ParamOverlayError(
                f"no current planning-params row for item {item_id} at "
                f"location {location_id} — the override would be silently "
                f"inert (resolved_params_sql() has no base row to overlay "
                f"it onto)."
            )
    else:
        row = conn.execute(
            f"""
            SELECT 1 FROM item_planning_params
            WHERE item_id = %s
              AND {_CURRENT_ROW_PREDICATE}
            LIMIT 1
            """,
            (item_id,),
        ).fetchone()
        if row is None:
            raise ParamOverlayError(
                f"no current planning-params row for item {item_id} at any "
                f"location — the item-global override would be silently "
                f"inert (resolved_params_sql() has no base row to overlay "
                f"it onto)."
            )


def set_param_override(
    conn: psycopg.Connection,
    scenario_id: UUID,
    item_id: UUID,
    field_name: str,
    value: str,
    applied_by: str,
    location_id: Optional[UUID] = None,
) -> UUID:
    """
    Upsert a scenario-scoped planning-param override.

    Validates:
      - field_name is in ALLOWED_PARAM_FIELDS (ParamOverlayError otherwise).
      - value parses against the field's cast target (integer / numeric /
        non-empty text; lot_size_rule against the real enum values — see
        _validate_value) AND against the business bounds the base table's
        own CHECK constraints enforce (PARAM_FIELD_BOUNDS). Fail loudly AT
        THE WRITE, not at resolution time inside an innocent reader. The
        NORMALIZED (stripped) value is what gets persisted — see
        _validate_value's docstring for why the raw value must not be used.
      - applied_by is non-empty (scenario_planning_overrides.applied_by is
        NOT NULL — migration 060: every override is attributed, no
        anonymous writes).
      - scenario_id exists and is NOT the baseline scenario (overrides only
        make sense inside a fork — same rule scenario/manager.py's
        apply_override implicitly relies on by only ever being called with
        a forked scenario_id).
      - a CURRENT item_planning_params row exists for the override's target
        (exact (item, location) if location-scoped, any location for the
        item if item-global) — see _assert_current_planning_params_row_exists.
        Without this, the override is written but permanently invisible to
        resolved_params_sql(), which would look like "no impact" to any
        caller measuring before/after.
      - item_id/location_id resolve to real FK targets. The row-existence
        check above already catches the common cases; the INSERT is still
        wrapped to retype any surviving ForeignKeyViolation (e.g. a phantom
        location_id used only in a location-scoped override where the
        row-existence check runs against item_planning_params, not
        locations directly) as a ParamOverlayError instead of a raw
        psycopg exception leaking to the caller.

    Upsert target: scenario_planning_overrides declares
    UNIQUE NULLS NOT DISTINCT (scenario_id, item_id, location_id, field_name)
    (migration 060, PG16) so a single ON CONFLICT clause covers both the
    location-scoped and the item-global (location_id IS NULL) override —
    no separate NULL-location code path needed.

    Returns the override_id (new or pre-existing, on conflict).
    """
    _validate_field_name(field_name)
    normalized_value = _validate_value(field_name, value)

    if not applied_by or not applied_by.strip():
        raise ParamOverlayError("applied_by must be a non-empty attribution string.")

    scenario_row = _fetch_scenario(conn, scenario_id)
    if scenario_row["is_baseline"]:
        raise ParamOverlayError(
            f"Scenario {scenario_id} is the baseline scenario. Planning-param "
            "overrides can only be applied inside a forked scenario."
        )

    _assert_current_planning_params_row_exists(conn, item_id, location_id)

    override_id = uuid4()
    now = datetime.now(timezone.utc)

    try:
        row = conn.execute(
            """
            INSERT INTO scenario_planning_overrides (
                override_id, scenario_id, item_id, location_id,
                field_name, value, applied_at, applied_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (scenario_id, item_id, location_id, field_name) DO UPDATE SET
                value      = EXCLUDED.value,
                applied_at = EXCLUDED.applied_at,
                applied_by = EXCLUDED.applied_by
            RETURNING override_id
            """,
            (
                override_id,
                scenario_id,
                item_id,
                location_id,
                field_name,
                normalized_value,
                now,
                applied_by,
            ),
        ).fetchone()
    except ForeignKeyViolation:
        logger.info(
            "param_overlay.set_rejected reason=fk_violation scenario=%s item=%s "
            "location=%s field=%s",
            scenario_id,
            item_id,
            location_id,
            field_name,
        )
        raise ParamOverlayError(
            f"item {item_id} or location {location_id} does not exist — "
            f"cannot set a planning-param override against it."
        ) from None

    persisted_override_id = UUID(str(row["override_id"]))

    logger.info(
        "param_overlay.set scenario=%s item=%s location=%s field=%s by=%s",
        scenario_id,
        item_id,
        location_id,
        field_name,
        applied_by,
    )

    return persisted_override_id


def clear_param_override(
    conn: psycopg.Connection,
    scenario_id: UUID,
    item_id: UUID,
    field_name: str,
    location_id: Optional[UUID] = None,
) -> bool:
    """
    Delete a scenario-scoped planning-param override.

    Validates symmetrically with set_param_override: field_name must be in
    ALLOWED_PARAM_FIELDS and scenario_id must exist (same ParamOverlayError
    messages) — a typo'd field or a phantom scenario is a caller bug, not a
    legitimate "nothing to delete" no-op. A MISSING override row, on the
    other hand, is a legitimate no-op: returns False. (No baseline check:
    baseline can never hold an override — set refuses it — so clearing on
    baseline is simply the False path.)

    Returns True if a row was deleted, False if no matching override
    existed. `location_id=None` targets the item-global override row
    (IS NOT DISTINCT FROM handles the NULL match).
    """
    _validate_field_name(field_name)
    _fetch_scenario(conn, scenario_id)

    result = conn.execute(
        """
        DELETE FROM scenario_planning_overrides
        WHERE scenario_id = %s
          AND item_id = %s
          AND location_id IS NOT DISTINCT FROM %s
          AND field_name = %s
        """,
        (scenario_id, item_id, location_id, field_name),
    )
    deleted = bool(result.rowcount)

    logger.info(
        "param_overlay.clear scenario=%s item=%s location=%s field=%s deleted=%s",
        scenario_id,
        item_id,
        location_id,
        field_name,
        deleted,
    )

    return deleted


def list_param_overrides(conn: psycopg.Connection, scenario_id: UUID) -> list[dict]:
    """Return every planning-param override for a scenario, for audit display."""
    rows = conn.execute(
        """
        SELECT override_id, scenario_id, item_id, location_id,
               field_name, value, applied_at, applied_by
        FROM scenario_planning_overrides
        WHERE scenario_id = %s
        ORDER BY item_id, location_id NULLS FIRST, field_name
        """,
        (scenario_id,),
    ).fetchall()
    return [dict(row) for row in rows]
