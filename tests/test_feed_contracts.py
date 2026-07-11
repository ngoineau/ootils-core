"""
tests/test_feed_contracts.py — Pure unit tests for the feed-contract registry
(INT-1 PR1, ADR-037). No database.

Covers the DB-free surfaces of src/ootils_core/interfaces/contracts.py:

  1. FeedContractSpec — the strict Pydantic boundary: an unknown field is a
     hard error NAMING the field (extra="forbid"); ``load_mode: delta`` is
     refused (V1 admits only 'full' — first line of defense, migration 073's
     CHECK being the second); criticality/format/entity_type outside their
     CHECK vocabularies are refused; padding/blank hygiene; the
     key_columns ⊆ mandatory_columns invariant; no self-dependency; the
     None-honest volume guards; model round-trip.
  2. parse_contract_file / load_contract_dir — pure parsing + cross-file
     validation (duplicate feed_key, depends_on referential integrity),
     ContractError messages carrying the file path + field name.
  3. The 3 pilot seed YAMLs under config/feed-contracts/ — read AS-IS from
     the repo (they are the pilot's deliverable: if a seed stops validating,
     this file is the tripwire).
  4. Enum lockstep between the Python Literals and the SQL CHECKs: migration
     073's entity_type CHECK vs ingest_batches' (migration 036, the last
     widening of 023 -> 035 -> 036), 073's format CHECK vs staging.uploads'
     file_format (migration 033), criticality, and the 'full'-only load_mode
     — the "keep the two in lockstep" contract written in both headers.
  5. The CLI's DB-free surface (scripts/load_feed_contracts.py): --dry-run
     exit 0 with no DB connection, exit 1 on a bad contract dir, exit 2 on a
     missing DATABASE_URL.

DB-touching behaviour (versioned upsert, traced no-op, append-only per
version, get_active_contract, migration 073 idempotence) lives in
tests/integration/test_feed_contracts_integration.py.
"""
from __future__ import annotations

import dataclasses
import re
import sys
from decimal import Decimal
from pathlib import Path
from typing import get_args

import pytest
import yaml
from pydantic import ValidationError

from ootils_core.interfaces.contracts import (
    _CONTENT_FIELDS,
    ContractError,
    Criticality,
    EntityType,
    FeedContract,
    FeedContractSpec,
    FeedFormat,
    LoadMode,
    load_contract_dir,
    parse_contract_file,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = _REPO_ROOT / "config" / "feed-contracts"
_MIGRATIONS = _REPO_ROOT / "src" / "ootils_core" / "db" / "migrations"
MIGRATION_073 = _MIGRATIONS / "073_feed_contracts.sql"
MIGRATION_036 = _MIGRATIONS / "036_ingest_batches_routings.sql"
MIGRATION_033 = _MIGRATIONS / "033_staging_schema.sql"

# Import seam: the loader CLI lives under scripts/ (outside the package),
# same pattern as tests/test_forecast_watcher.py.
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import load_feed_contracts  # noqa: E402

SEED_KEYS = {"on-hand", "open-purchase-orders", "open-work-orders"}

# Sentinel: pass a field as _OMIT to _payload() to REMOVE it from the payload
# (exercising defaults), as opposed to None which is a real value.
_OMIT = object()


def _payload(**over) -> dict:
    """A minimal, fully valid feed-contract payload (dict, as yaml.safe_load
    would return it). Override any field; pass _OMIT to drop one."""
    base = {
        "feed_key": "unit-feed",
        "entity_type": "on_hand",
        "source_system": "WMS-UNIT",
        "format": "csv",
        "key_columns": ["item_external_id", "location_external_id"],
        "mandatory_columns": [
            "item_external_id", "location_external_id", "quantity",
        ],
        "load_mode": "full",
        "cadence": "0 6 * * *",
        "arrival_window_minutes": 90,
        "owner": "unit-ops",
        "criticality": "blocking",
        "volume_guard_min_rows": 100,
        "volume_guard_max_pct_delta": 0.20,
        "depends_on": [],
    }
    base.update(over)
    return {k: v for k, v in base.items() if v is not _OMIT}


def _spec(**over) -> FeedContractSpec:
    return FeedContractSpec.model_validate(_payload(**over))


# ---------------------------------------------------------------------------
# 1. FeedContractSpec — strict Pydantic validation
# ---------------------------------------------------------------------------


class TestSpecValidation:
    def test_valid_payload_parses(self):
        spec = _spec()
        assert spec.feed_key == "unit-feed"
        assert spec.entity_type == "on_hand"
        assert spec.load_mode == "full"
        assert spec.volume_guard_max_pct_delta == Decimal("0.20")

    def test_unknown_field_rejected_naming_the_field(self):
        """extra="forbid": a pilot typo like ``mandatory_column`` (missing
        the trailing 's') must fail loudly, NAMING the offending field."""
        bad = _payload(mandatory_columns=_OMIT)
        bad["mandatory_column"] = ["item_external_id"]
        with pytest.raises(ValidationError) as exc:
            FeedContractSpec.model_validate(bad)
        assert "mandatory_column" in str(exc.value)

    def test_load_mode_delta_refused(self):
        """V1 admits ONLY 'full' (Literal["full"]) — a delta contract is
        rejected in Python before ever reaching migration 073's CHECK."""
        with pytest.raises(ValidationError) as exc:
            _spec(load_mode="delta")
        assert "load_mode" in str(exc.value)

    def test_load_mode_omitted_defaults_to_full(self):
        assert _spec(load_mode=_OMIT).load_mode == "full"

    def test_criticality_outside_check_refused(self):
        with pytest.raises(ValidationError) as exc:
            _spec(criticality="critical")
        assert "criticality" in str(exc.value)

    def test_format_outside_check_refused(self):
        with pytest.raises(ValidationError) as exc:
            _spec(format="xml")
        assert "format" in str(exc.value)

    def test_entity_type_outside_check_refused(self):
        with pytest.raises(ValidationError) as exc:
            _spec(entity_type="widgets")
        assert "entity_type" in str(exc.value)

    def test_padded_feed_key_rejected(self):
        with pytest.raises(ValidationError) as exc:
            _spec(feed_key=" on-hand ")
        assert "feed_key" in str(exc.value)

    def test_padded_owner_rejected(self):
        with pytest.raises(ValidationError) as exc:
            _spec(owner="ops ")
        assert "owner" in str(exc.value)

    def test_empty_feed_key_rejected(self):
        with pytest.raises(ValidationError):
            _spec(feed_key="")

    def test_blank_key_column_entry_rejected(self):
        with pytest.raises(ValidationError) as exc:
            _spec(key_columns=["item_external_id", ""])
        assert "key_columns" in str(exc.value)

    def test_padded_mandatory_column_entry_rejected(self):
        with pytest.raises(ValidationError) as exc:
            _spec(mandatory_columns=["item_external_id", " quantity "])
        assert "mandatory_columns" in str(exc.value)

    def test_empty_key_columns_list_rejected(self):
        """Zero key columns = a contract that validates nothing — refused
        here AND by migration 073's cardinality CHECK."""
        with pytest.raises(ValidationError) as exc:
            _spec(key_columns=[])
        assert "key_columns" in str(exc.value)

    def test_empty_mandatory_columns_list_rejected(self):
        with pytest.raises(ValidationError):
            _spec(mandatory_columns=[])

    def test_key_columns_must_be_subset_of_mandatory_columns(self):
        """A column identifying a row cannot itself be optional — the added
        invariant, and its message names the missing column."""
        with pytest.raises(ValidationError) as exc:
            _spec(
                key_columns=["external_id"],
                mandatory_columns=["item_external_id", "quantity"],
            )
        msg = str(exc.value)
        assert "external_id" in msg
        assert "mandatory_columns" in msg

    def test_self_dependency_rejected(self):
        with pytest.raises(ValidationError) as exc:
            _spec(depends_on=["unit-feed"])
        assert "unit-feed" in str(exc.value)

    def test_foreign_dependency_accepted_at_spec_level(self):
        """A depends_on pointing at ANOTHER feed_key is legal at the
        single-file level — referential integrity against the set of files
        is load_contract_dir's job, not the spec's."""
        assert _spec(depends_on=["other-feed"]).depends_on == ["other-feed"]

    def test_arrival_window_zero_rejected(self):
        with pytest.raises(ValidationError) as exc:
            _spec(arrival_window_minutes=0)
        assert "arrival_window_minutes" in str(exc.value)

    def test_arrival_window_negative_rejected(self):
        with pytest.raises(ValidationError):
            _spec(arrival_window_minutes=-5)

    def test_negative_volume_guard_min_rows_rejected(self):
        with pytest.raises(ValidationError) as exc:
            _spec(volume_guard_min_rows=-1)
        assert "volume_guard_min_rows" in str(exc.value)

    def test_pct_delta_negative_rejected(self):
        with pytest.raises(ValidationError):
            _spec(volume_guard_max_pct_delta=Decimal("-0.1"))

    def test_pct_delta_beyond_numeric_5_4_bound_rejected(self):
        """The NUMERIC(5,4) column tops out at 9.9999 — an out-of-range
        guard fails HERE with the field named, not as an opaque DB error."""
        with pytest.raises(ValidationError) as exc:
            _spec(volume_guard_max_pct_delta=Decimal("10"))
        assert "volume_guard_max_pct_delta" in str(exc.value)

    def test_volume_guards_none_honest(self):
        """Explicit null and omitted both mean 'no guard configured' (None)
        — never a fabricated default threshold."""
        explicit = _spec(
            volume_guard_min_rows=None, volume_guard_max_pct_delta=None
        )
        omitted = _spec(
            volume_guard_min_rows=_OMIT, volume_guard_max_pct_delta=_OMIT
        )
        for spec in (explicit, omitted):
            assert spec.volume_guard_min_rows is None
            assert spec.volume_guard_max_pct_delta is None

    def test_depends_on_omitted_defaults_to_empty_list(self):
        assert _spec(depends_on=_OMIT).depends_on == []

    def test_spec_is_frozen(self):
        spec = _spec()
        with pytest.raises(ValidationError):
            spec.feed_key = "mutated"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 2. Model round-trip + content/identity split
# ---------------------------------------------------------------------------


class TestModelRoundTrip:
    def test_python_mode_round_trip(self):
        spec = _spec()
        assert FeedContractSpec.model_validate(spec.model_dump()) == spec

    def test_json_mode_round_trip(self):
        spec = _spec(volume_guard_max_pct_delta=Decimal("0.25"))
        again = FeedContractSpec.model_validate_json(spec.model_dump_json())
        assert again == spec
        assert again.volume_guard_max_pct_delta == Decimal("0.25")

    def test_none_guards_survive_round_trip(self):
        spec = _spec(
            volume_guard_min_rows=None,
            volume_guard_max_pct_delta=None,
            criticality="advisory",
        )
        again = FeedContractSpec.model_validate(spec.model_dump())
        assert again.volume_guard_min_rows is None
        assert again.volume_guard_max_pct_delta is None

    def test_content_fields_are_exactly_spec_fields_minus_feed_key(self):
        """_CONTENT_FIELDS (the upsert no-op diff basis) must cover every
        spec field except the feed_key identity — drift here would make the
        loader blind to a changed field (silent no-op instead of version+1)."""
        assert set(_CONTENT_FIELDS) | {"feed_key"} == set(
            FeedContractSpec.model_fields
        )

    def test_content_tuple_covers_all_content_fields(self):
        spec = _spec()
        assert len(spec.content_tuple()) == len(_CONTENT_FIELDS) == 13

    def test_feed_contract_row_is_content_plus_identity_plus_bookkeeping(self):
        """The full-row dataclass = content fields + identity (id, feed_key,
        version) + bookkeeping (active, created_at, updated_at) — the same
        split migration 073's header documents."""
        row_fields = {f.name for f in dataclasses.fields(FeedContract)}
        assert row_fields == set(_CONTENT_FIELDS) | {
            "feed_contract_id",
            "feed_key",
            "version",
            "active",
            "created_at",
            "updated_at",
        }

    def test_contract_error_is_a_value_error(self):
        assert issubclass(ContractError, ValueError)


# ---------------------------------------------------------------------------
# 3. parse_contract_file — file-level parsing (tmp files)
# ---------------------------------------------------------------------------


def _write_yaml(dir_path: Path, name: str, payload: dict) -> Path:
    path = dir_path / name
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


class TestParseContractFile:
    def test_valid_file_parses(self, tmp_path):
        path = _write_yaml(tmp_path, "unit-feed.yaml", _payload())
        spec = parse_contract_file(path)
        assert spec == _spec()

    def test_unknown_field_names_file_and_field(self, tmp_path):
        bad = _payload()
        bad["mandatory_column"] = ["oops"]
        path = _write_yaml(tmp_path, "bad.yaml", bad)
        with pytest.raises(ContractError) as exc:
            parse_contract_file(path)
        msg = str(exc.value)
        assert "bad.yaml" in msg
        assert "mandatory_column" in msg

    def test_load_mode_delta_in_yaml_refused(self, tmp_path):
        path = _write_yaml(tmp_path, "delta.yaml", _payload(load_mode="delta"))
        with pytest.raises(ContractError) as exc:
            parse_contract_file(path)
        assert "load_mode" in str(exc.value)

    def test_invalid_yaml_syntax_is_contract_error(self, tmp_path):
        path = tmp_path / "broken.yaml"
        path.write_text("feed_key: [unclosed\n  nested: {", encoding="utf-8")
        with pytest.raises(ContractError) as exc:
            parse_contract_file(path)
        assert "invalid YAML" in str(exc.value)

    def test_top_level_list_rejected(self, tmp_path):
        path = tmp_path / "list.yaml"
        path.write_text("- a\n- b\n", encoding="utf-8")
        with pytest.raises(ContractError) as exc:
            parse_contract_file(path)
        assert "mapping" in str(exc.value)

    def test_missing_file_is_contract_error(self, tmp_path):
        with pytest.raises(ContractError) as exc:
            parse_contract_file(tmp_path / "nope.yaml")
        assert "nope.yaml" in str(exc.value)


# ---------------------------------------------------------------------------
# 4. load_contract_dir — cross-file validation (tmp dirs)
# ---------------------------------------------------------------------------


class TestLoadContractDir:
    def test_valid_dir_returns_specs_keyed_by_feed_key(self, tmp_path):
        _write_yaml(tmp_path, "a.yaml", _payload(feed_key="feed-a"))
        _write_yaml(tmp_path, "b.yaml", _payload(feed_key="feed-b"))
        specs = load_contract_dir(tmp_path)
        assert set(specs) == {"feed-a", "feed-b"}

    def test_duplicate_feed_key_across_files_rejected(self, tmp_path):
        _write_yaml(tmp_path, "a.yaml", _payload(feed_key="dup"))
        _write_yaml(tmp_path, "b.yaml", _payload(feed_key="dup"))
        with pytest.raises(ContractError) as exc:
            load_contract_dir(tmp_path)
        msg = str(exc.value)
        assert "duplicate" in msg
        assert "dup" in msg

    def test_depends_on_unknown_feed_key_rejected(self, tmp_path):
        _write_yaml(
            tmp_path, "a.yaml",
            _payload(feed_key="feed-a", depends_on=["ghost-feed"]),
        )
        with pytest.raises(ContractError) as exc:
            load_contract_dir(tmp_path)
        msg = str(exc.value)
        assert "ghost-feed" in msg
        assert "feed-a" in msg

    def test_depends_on_resolving_within_dir_accepted(self, tmp_path):
        _write_yaml(tmp_path, "a.yaml", _payload(feed_key="feed-a"))
        _write_yaml(
            tmp_path, "b.yaml",
            _payload(feed_key="feed-b", depends_on=["feed-a"]),
        )
        specs = load_contract_dir(tmp_path)
        assert specs["feed-b"].depends_on == ["feed-a"]

    def test_not_a_directory_rejected(self, tmp_path):
        with pytest.raises(ContractError) as exc:
            load_contract_dir(tmp_path / "nope")
        assert "not a directory" in str(exc.value)

    def test_empty_dir_returns_empty_mapping(self, tmp_path):
        assert load_contract_dir(tmp_path) == {}


# ---------------------------------------------------------------------------
# 5. The 3 pilot seed YAMLs — read AS-IS from config/feed-contracts/
# ---------------------------------------------------------------------------


class TestSeedContracts:
    def test_seed_dir_parses_and_cross_validates_as_is(self):
        """The pilot's deliverable: the 3 committed seeds must parse and
        cross-validate exactly as they sit in the repo."""
        specs = load_contract_dir(CONFIG_DIR)
        assert set(specs) == SEED_KEYS

    def test_feed_key_matches_filename_convention(self):
        for path in sorted(CONFIG_DIR.glob("*.yaml")):
            spec = parse_contract_file(path)
            assert spec.feed_key == path.stem, (
                f"{path.name}: feed_key {spec.feed_key!r} != file stem "
                f"(registry-browsing convention documented in the seeds)"
            )

    def test_on_hand_is_blocking_with_both_volume_guards(self):
        spec = load_contract_dir(CONFIG_DIR)["on-hand"]
        assert spec.entity_type == "on_hand"
        assert spec.criticality == "blocking"
        assert spec.volume_guard_min_rows is not None
        assert spec.volume_guard_max_pct_delta is not None
        # Snapshot feed: no transactional external_id, natural key is
        # (item, site).
        assert spec.key_columns == ["item_external_id", "location_external_id"]

    def test_open_purchase_orders_is_blocking_keyed_on_external_id(self):
        spec = load_contract_dir(CONFIG_DIR)["open-purchase-orders"]
        assert spec.entity_type == "purchase_orders"
        assert spec.criticality == "blocking"
        assert spec.key_columns == ["external_id"]
        assert spec.volume_guard_min_rows is not None
        assert spec.volume_guard_max_pct_delta is not None

    def test_open_work_orders_is_advisory_with_none_honest_guards(self):
        """Not every pilot customer runs an MES: advisory criticality and
        BOTH guards deliberately unconfigured (None, never a fabricated
        threshold)."""
        spec = load_contract_dir(CONFIG_DIR)["open-work-orders"]
        assert spec.entity_type == "work_orders"
        assert spec.criticality == "advisory"
        assert spec.volume_guard_min_rows is None
        assert spec.volume_guard_max_pct_delta is None

    def test_seed_pct_deltas_are_fractions_not_percent_integers(self):
        """0.20 means 20% — a seed carrying 20 would be the classic
        percent-integer bug the column comment warns about."""
        for feed_key, spec in load_contract_dir(CONFIG_DIR).items():
            if spec.volume_guard_max_pct_delta is not None:
                assert spec.volume_guard_max_pct_delta < 1, (
                    f"{feed_key}: volume_guard_max_pct_delta "
                    f"{spec.volume_guard_max_pct_delta} looks like a percent "
                    f"integer, the convention is a fraction (0.20 == 20%)"
                )

    def test_all_seeds_full_load_mode_no_dependencies(self):
        for feed_key, spec in load_contract_dir(CONFIG_DIR).items():
            assert spec.load_mode == "full", feed_key
            assert spec.arrival_window_minutes > 0, feed_key
            # 3 independent source systems (WMS/SAP/MES) — no invented
            # dependency between them (deliberate PR1 choice).
            assert spec.depends_on == [], feed_key


# ---------------------------------------------------------------------------
# 6. Enum lockstep — Python Literals vs SQL CHECKs
# ---------------------------------------------------------------------------


def _sql_enum(sql_text: str, column: str) -> set[str]:
    """Extract the quoted values of the first ``<column> IN (...)`` list in
    raw SQL. The lists under test are flat string enums (no nested parens),
    so a [^)]* capture is exact."""
    m = re.search(rf"{column}\s+IN\s*\(([^)]*)\)", sql_text)
    assert m is not None, f"no `{column} IN (...)` CHECK found"
    values = set(re.findall(r"'([a-z_]+)'", m.group(1)))
    assert values, f"`{column} IN (...)` matched but no quoted values parsed"
    return values


class TestEnumLockstepWithMigrations:
    def test_entity_type_literal_matches_073_check(self):
        sql = MIGRATION_073.read_text(encoding="utf-8")
        assert _sql_enum(sql, "entity_type") == set(get_args(EntityType))

    def test_073_entity_check_matches_ingest_batches_036(self):
        """073's header pins its entity enum to ingest_batches' CHECK as it
        stands after 023 -> 035 -> 036. 036 is the last widening: the two
        lists must be identical, value for value."""
        sql_073 = MIGRATION_073.read_text(encoding="utf-8")
        sql_036 = MIGRATION_036.read_text(encoding="utf-8")
        assert _sql_enum(sql_073, "entity_type") == _sql_enum(
            sql_036, "entity_type"
        )

    def test_seed_entity_types_need_no_enum_widening(self):
        seeds = {s.entity_type for s in load_contract_dir(CONFIG_DIR).values()}
        assert seeds == {"on_hand", "purchase_orders", "work_orders"}
        assert seeds <= set(get_args(EntityType))

    def test_format_literal_matches_073_and_staging_033(self):
        sql_073 = MIGRATION_073.read_text(encoding="utf-8")
        sql_033 = MIGRATION_033.read_text(encoding="utf-8")
        literal = set(get_args(FeedFormat))
        assert _sql_enum(sql_073, "format") == literal
        # Same 4-value universe as staging.uploads.file_format (ADR-013).
        assert _sql_enum(sql_033, "file_format") == literal

    def test_criticality_literal_matches_073_check(self):
        sql = MIGRATION_073.read_text(encoding="utf-8")
        assert _sql_enum(sql, "criticality") == set(get_args(Criticality))

    def test_load_mode_is_full_only_in_both_python_and_sql(self):
        """The fail-loudly trap has two teeth and they must bite the same:
        Literal["full"] in Python, CHECK (load_mode IN ('full')) in SQL."""
        sql = MIGRATION_073.read_text(encoding="utf-8")
        assert _sql_enum(sql, "load_mode") == {"full"}
        assert set(get_args(LoadMode)) == {"full"}


# ---------------------------------------------------------------------------
# 7. CLI — DB-free surface (scripts/load_feed_contracts.py)
# ---------------------------------------------------------------------------


class TestCliDbFree:
    def test_dry_run_on_seed_dir_exits_0_without_db(self):
        """--dry-run parses + cross-validates the committed seeds and never
        opens a DB connection (no DSN involved)."""
        assert load_feed_contracts.main(["--dry-run"]) == 0

    def test_dry_run_explicit_dir(self, tmp_path):
        _write_yaml(tmp_path, "feed-a.yaml", _payload(feed_key="feed-a"))
        rc = load_feed_contracts.main(["--dry-run", "--dir", str(tmp_path)])
        assert rc == 0

    def test_bad_contract_dir_exits_1(self, tmp_path):
        bad = _payload()
        bad["mandatory_column"] = ["oops"]
        _write_yaml(tmp_path, "bad.yaml", bad)
        rc = load_feed_contracts.main(["--dry-run", "--dir", str(tmp_path)])
        assert rc == 1

    def test_missing_dir_exits_1(self, tmp_path):
        rc = load_feed_contracts.main(
            ["--dry-run", "--dir", str(tmp_path / "nope")]
        )
        assert rc == 1

    def test_missing_database_url_exits_2(self, tmp_path, monkeypatch):
        """Without --dry-run a DSN is required: parse succeeds (empty dir),
        then the missing DATABASE_URL exits 2 before any DB touch."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        rc = load_feed_contracts.main(["--dir", str(tmp_path)])
        assert rc == 2
