"""
test_location_aliases_unit.py — Pure unit tests for location aliases (#414,
ADR-031). No database: exercises the Pydantic boundary of LocationAliasRow
and the shape of the single-source SQL fragment _warehouse_codes_subquery().

DB-touching behaviour (folding, freshness, ingest, FK, migration idempotence)
lives in tests/integration/test_location_aliases_integration.py.
"""
from __future__ import annotations

import os

import pytest
from pydantic import ValidationError

# Auth token must be set BEFORE the router module (which imports the app auth
# layer) is imported — mirrors tests/test_router_ingest.py.
os.environ.setdefault("OOTILS_API_TOKEN", "test-token")

from ootils_core.api.routers.ingest import LocationAliasRow, LocationRow
from ootils_core.pyramide.repository import _warehouse_codes_subquery


# ---------------------------------------------------------------------------
# LocationAliasRow — Pydantic validation
# ---------------------------------------------------------------------------


class TestLocationAliasRowValidation:
    def test_default_source_system_is_default_sentinel(self):
        """source_system omitted → the '_default' sentinel (never nullable)."""
        row = LocationAliasRow(alias="87")
        assert row.source_system == "_default"
        assert row.alias == "87"

    def test_explicit_source_system_preserved(self):
        row = LocationAliasRow(alias="87", source_system="erp")
        assert row.source_system == "erp"

    def test_alias_is_stripped(self):
        """A padded alias is stored trimmed (mirrors the DB CHECK
        btrim(alias) = alias): ' 87 ' → '87'."""
        row = LocationAliasRow(alias="  87  ")
        assert row.alias == "87"

    def test_blank_alias_rejected(self):
        """Whitespace-only alias → ValidationError (field_validator raises)."""
        with pytest.raises(ValidationError):
            LocationAliasRow(alias="   ")

    def test_empty_alias_rejected(self):
        """Empty string → ValidationError (min_length=1 / non-blank)."""
        with pytest.raises(ValidationError):
            LocationAliasRow(alias="")

    def test_alias_stripped_to_blank_rejected(self):
        """A value that is non-empty but strips to blank is still rejected."""
        with pytest.raises(ValidationError):
            LocationAliasRow(alias=" \t ")


# ---------------------------------------------------------------------------
# LocationRow — aliases field default (backward compatibility)
# ---------------------------------------------------------------------------


class TestLocationRowAliasesField:
    def test_aliases_absent_defaults_to_empty_list(self):
        """A pre-#414 payload with no ``aliases`` key → [] (no alias work,
        identical behaviour to before the field existed)."""
        row = LocationRow(external_id="DC-ATL", name="Atlanta DC")
        assert row.aliases == []

    def test_aliases_parsed_into_rows(self):
        row = LocationRow(
            external_id="DC-ATL",
            name="Atlanta DC",
            aliases=[
                {"alias": "87", "source_system": "erp"},
                {"alias": "286"},
            ],
        )
        assert len(row.aliases) == 2
        assert isinstance(row.aliases[0], LocationAliasRow)
        assert row.aliases[0].source_system == "erp"
        assert row.aliases[1].source_system == "_default"

    def test_blank_alias_inside_location_row_rejected(self):
        """The nested LocationAliasRow validator fires when building a
        LocationRow, so a blank alias fails at parse time."""
        with pytest.raises(ValidationError):
            LocationRow(
                external_id="DC-ATL",
                name="Atlanta DC",
                aliases=[{"alias": "  "}],
            )


# ---------------------------------------------------------------------------
# _warehouse_codes_subquery — SQL fragment shape
# ---------------------------------------------------------------------------


class TestWarehouseCodesSubquery:
    def test_fragment_contains_union_of_both_tables(self):
        frag = _warehouse_codes_subquery()
        # UNION of the two code sources.
        assert "UNION" in frag
        # Both tables of the resolution.
        assert "locations" in frag
        assert "location_aliases" in frag
        # Selects the external_id (canonical code) and the alias code.
        assert "external_id" in frag
        assert "alias" in frag

    def test_fragment_binds_named_location_id_param(self):
        """Callers bind the location_id as the named placeholder — the
        fragment must reference %(location_id)s (and only that param)."""
        frag = _warehouse_codes_subquery()
        assert "%(location_id)s" in frag
        # No positional placeholders leaked into the fragment.
        assert "%s" not in frag.replace("%(location_id)s", "")

    def test_fragment_is_deterministic(self):
        """Single source of truth — two calls return the identical string."""
        assert _warehouse_codes_subquery() == _warehouse_codes_subquery()
