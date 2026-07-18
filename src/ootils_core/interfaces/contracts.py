"""
contracts.py — feed-contract registry: strict YAML parsing, a versioned
idempotent loader, and the active-contract reader (INT-1 PR1, ADR-037).

A feed contract declares what a source feed (on-hand, open purchase orders,
open work orders, ...) is ALLOWED to look like before a daily run trusts it:
entity mapping, physical format, business key, mandatory columns, cadence,
arrival window, owner, criticality, volume guards. ``config/feed-contracts/
*.yaml`` is the pilot-editable source of truth; this module loads it into the
typed, versioned ``feed_contracts`` table (migration 073) — see that
migration's header for the full schema rationale.

Three layers, cleanly split:

  * ``FeedContractSpec`` — a strict pydantic model of ONE YAML file's content.
    ``extra="forbid"`` + closed-vocabulary ``Literal`` fields mean a typo or
    an unsupported value (e.g. ``load_mode: delta``, out of V1 scope per
    migration 073) is rejected HERE, in Python, with the offending field
    named in the message — before it ever reaches Postgres. Deliberately
    excludes everything the DB assigns: ``feed_contract_id``, ``version``,
    ``active``, ``created_at``/``updated_at`` (see "ACTIVE SEMANTICS" below).
  * ``parse_contract_file`` / ``load_contract_dir`` — pure, DB-free parsing +
    cross-file validation (feed_key uniqueness, ``depends_on`` referential
    integrity against the set of feed_keys actually present).
  * ``upsert_contract`` / ``get_active_contract`` — the DB half. Both take an
    open connection and NEVER commit/rollback (same convention as
    ``ScenarioManager``): the caller (the CLI, later a router) owns the
    transaction.

ACTIVE SEMANTICS (the interpretation this module implements — see migration
073's header for the DB-side contract it must honour): every YAML file that
``load_contract_dir`` picks up describes a feed the pilot wants registered
and in effect. There is no "load this contract but leave it inactive" case in
this PR's YAML surface (no ``active:`` field in ``FeedContractSpec``) — a
successful ``upsert_contract`` call always makes its spec's content the new
active version if the content differs from the current active version, and
supersedes (bookkeeping-flips to FALSE) whatever was active before. Retiring
a feed entirely (zero active rows) is a distinct operation this PR does not
expose a CLI flag for — it is a plain, explicit ``UPDATE feed_contracts SET
active = FALSE WHERE feed_key = %s AND active`` run by an operator; wiring a
``--retire`` flag is left for PR2/3 alongside the runtime that would actually
need it. Documented here because it is an interpretation of the plan, not
spelled out verbatim in it.

FAIL-LOUDLY, TWO LINES OF DEFENSE: the ``load_mode`` field is typed
``Literal["full"]`` — a YAML claiming ``load_mode: delta`` is rejected by
Pydantic (this module) before ever reaching the DB; migration 073's own CHECK
constraint is the second, DB-level line of defense per its header.

YAML LOADING: ``yaml.safe_load`` ONLY — a feed-contract file is pilot-edited
config, not code; never ``yaml.load``/``FullLoader``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import yaml
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

# Mirrors feed_contracts.entity_type CHECK (migration 073), itself pinned to
# ingest_batches_entity_type_check after migrations 023 -> 035 -> 036. Keep
# the two enums in lockstep: widen ingest_batches' CHECK (and migration 073's
# own CHECK) first, then this tuple, in a follow-up migration.
EntityType = Literal[
    "items", "locations", "suppliers", "supplier_items", "purchase_orders",
    "customer_orders", "forecasts", "work_orders", "transfers", "on_hand",
    "resources", "planning_params", "routings",
]

# Same 4-value universe as staging.uploads.file_format (migration 033,
# ADR-013).
FeedFormat = Literal["tsv", "csv", "xlsx", "json"]

Criticality = Literal["blocking", "advisory"]

# V1 admits ONLY 'full' (migration 073 header). A YAML with load_mode: delta
# fails HERE — see module docstring "FAIL-LOUDLY, TWO LINES OF DEFENSE".
LoadMode = Literal["full"]

# Content columns compared by upsert_contract's no-op check — everything
# feed_contracts stores EXCEPT identity (feed_contract_id, feed_key, version)
# and bookkeeping (active, created_at, updated_at). Order matches the SELECT
# in upsert_contract/get_active_contract so both stay easy to eyeball together.
_CONTENT_FIELDS: tuple[str, ...] = (
    "entity_type", "source_system", "format", "key_columns",
    "mandatory_columns", "load_mode", "cadence", "arrival_window_minutes",
    "owner", "criticality", "volume_guard_min_rows",
    "volume_guard_max_pct_delta", "depends_on",
)


class ContractError(ValueError):
    """Any feed-contract YAML/config problem: bad field, unknown key, a
    broken ``depends_on`` reference, a feed_key defined twice. The message
    always carries the offending file path / feed_key / field name and is
    safe to surface verbatim (same carve-out pattern as staging/diff.py's
    ``DiffError`` — config coordinates only, never a DSN or a stack trace).
    """


class FeedContractSpec(BaseModel):
    """Strict, pydantic-validated content of ONE feed-contract YAML file.

    Excludes everything the DB assigns/derives: ``feed_contract_id``,
    ``version`` (loader-computed, see ``upsert_contract``), ``active`` (see
    module docstring "ACTIVE SEMANTICS"), ``created_at``/``updated_at``. An
    unknown YAML field is a hard error naming the field (``extra="forbid"``)
    — a pilot typo like ``mandatory_column:`` (missing the trailing "s") must
    fail loudly, not be silently ignored as noise.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=False)

    feed_key: str = Field(min_length=1)
    entity_type: EntityType
    source_system: str = Field(min_length=1)
    format: FeedFormat
    key_columns: list[str] = Field(min_length=1)
    mandatory_columns: list[str] = Field(min_length=1)
    load_mode: LoadMode = "full"
    cadence: str = Field(min_length=1)
    arrival_window_minutes: int = Field(gt=0)
    owner: str = Field(min_length=1)
    criticality: Criticality
    # Nullable, None-honest: not every feed configures a volume guard in V1
    # (migration 073 header) — no fabricated default.
    volume_guard_min_rows: int | None = Field(default=None, ge=0)
    # Fraction (0.20 == 20%), not a percent integer — matches the DB column's
    # documented convention. Bounded to the NUMERIC(5,4) column's own legal
    # range so an out-of-range guard fails here, not as an opaque DB error.
    volume_guard_max_pct_delta: Decimal | None = Field(
        default=None, ge=0, le=Decimal("9.9999")
    )
    depends_on: list[str] = Field(default_factory=list)

    @field_validator("feed_key", "source_system", "cadence", "owner")
    @classmethod
    def _no_pad(cls, v: str, info: Any) -> str:
        if v != v.strip():
            raise ValueError(f"{info.field_name} must not have leading/trailing whitespace")
        return v

    @field_validator("key_columns", "mandatory_columns", "depends_on")
    @classmethod
    def _no_blank_entries(cls, v: list[str], info: Any) -> list[str]:
        for entry in v:
            if not entry or entry != entry.strip():
                raise ValueError(
                    f"{info.field_name} entries must be non-blank, un-padded strings "
                    f"(got {entry!r})"
                )
        return v

    @model_validator(mode="after")
    def _key_columns_subset_of_mandatory(self) -> "FeedContractSpec":
        missing = [c for c in self.key_columns if c not in self.mandatory_columns]
        if missing:
            raise ValueError(
                f"key_columns {missing} must also appear in mandatory_columns "
                "(a column identifying a row cannot itself be optional)"
            )
        return self

    @model_validator(mode="after")
    def _no_self_dependency(self) -> "FeedContractSpec":
        if self.feed_key in self.depends_on:
            raise ValueError(f"depends_on cannot reference its own feed_key {self.feed_key!r}")
        return self

    def content_tuple(self) -> tuple[Any, ...]:
        """The content fields as a tuple, in ``_CONTENT_FIELDS`` order —
        used by ``upsert_contract`` to diff against the current active DB
        row without a bespoke comparator per call site."""
        return tuple(getattr(self, f) for f in _CONTENT_FIELDS)


@dataclass(frozen=True)
class FeedContract:
    """One full ``feed_contracts`` row, as read back from the DB (typically
    via ``get_active_contract``)."""

    feed_contract_id: UUID
    feed_key: str
    version: int
    entity_type: str
    source_system: str
    format: str
    key_columns: list[str]
    mandatory_columns: list[str]
    load_mode: str
    cadence: str
    arrival_window_minutes: int
    owner: str
    criticality: str
    volume_guard_min_rows: int | None
    volume_guard_max_pct_delta: Decimal | None
    depends_on: list[str]
    active: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class LoadOutcome:
    """The result of one ``upsert_contract`` call — what the CLI/caller
    reports back to the operator."""

    feed_key: str
    action: Literal["created", "no_op"]
    version: int
    feed_contract_id: UUID


def _format_validation_error(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "<root>"
        parts.append(f"field {loc!r}: {err['msg']}")
    return "; ".join(parts)


def parse_contract_file(path: Path) -> FeedContractSpec:
    """Parse + strictly validate ONE feed-contract YAML file.

    ``yaml.safe_load`` only. Raises ``ContractError`` naming ``path`` and the
    offending field(s) on any read/parse/validation failure — never returns a
    partially-valid spec.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ContractError(f"{path}: cannot read file ({exc})") from exc

    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ContractError(f"{path}: invalid YAML ({exc})") from exc

    if not isinstance(data, dict):
        raise ContractError(
            f"{path}: expected a YAML mapping at the top level, got {type(data).__name__}"
        )

    try:
        return FeedContractSpec.model_validate(data)
    except ValidationError as exc:
        raise ContractError(f"{path}: {_format_validation_error(exc)}") from exc


def load_contract_dir(dir_path: Path) -> dict[str, FeedContractSpec]:
    """Parse every ``*.yaml`` file under ``dir_path`` and cross-validate the
    set as a whole (no DB involved): feed_key uniqueness across files, and
    ``depends_on`` referential integrity against the feed_keys actually
    present (mirrors the non-DB-enforced pattern already used for
    ``location_aliases``' cross-site invariant, ADR-031).

    Returns ``{feed_key: FeedContractSpec}``. Raises ``ContractError`` on the
    first problem found — a bad directory never partially loads (this
    function does not touch the DB, so "partial" only means "partial in
    memory", but the caller's whole-directory-transaction contract for the DB
    write relies on this function running to completion first).
    """
    if not dir_path.is_dir():
        raise ContractError(f"{dir_path}: not a directory")

    specs: dict[str, FeedContractSpec] = {}
    for file_path in sorted(dir_path.glob("*.yaml")):
        spec = parse_contract_file(file_path)
        if spec.feed_key in specs:
            raise ContractError(
                f"{file_path}: duplicate feed_key {spec.feed_key!r} — already "
                f"defined by another file under {dir_path}"
            )
        specs[spec.feed_key] = spec

    for spec in specs.values():
        unknown = [dep for dep in spec.depends_on if dep not in specs]
        if unknown:
            raise ContractError(
                f"{spec.feed_key!r}: depends_on references unknown feed_key(s) "
                f"{unknown} — not present in {dir_path}"
            )

    return specs


def get_active_contract(conn: DictRowConnection, feed_key: str) -> FeedContract | None:
    """Return the currently active version of ``feed_key``, or ``None`` if
    the feed is unregistered or retired.

    None-honest: zero active rows never falls back to the latest inactive
    version (migration 073 header) — the reader reports exactly what is in
    effect right now.
    """
    cur = conn.cursor(row_factory=dict_row)
    row = cur.execute(
        "SELECT feed_contract_id, feed_key, version, entity_type, "
        "source_system, format, key_columns, mandatory_columns, load_mode, "
        "cadence, arrival_window_minutes, owner, criticality, "
        "volume_guard_min_rows, volume_guard_max_pct_delta, depends_on, "
        "active, created_at, updated_at "
        "FROM feed_contracts WHERE feed_key = %s AND active",
        (feed_key,),
    ).fetchone()
    if row is None:
        return None
    return FeedContract(**row)


def list_known_feed_keys(conn: DictRowConnection) -> set[str]:
    """Every ``feed_key`` ever registered in ``feed_contracts`` — active OR
    retired (``SELECT DISTINCT``, no ``WHERE active``). Distinguishes a
    REGISTRY-KNOWN feed whose contract has since been deactivated (must be
    refused explicitly — see ``engine.ingest.daily_orchestrator``'s
    "contract deactivated" quarantine) from a feed_key the registry has
    NEVER heard of at all (a referential/on-demand entity, or a genuinely
    undeclared feed — still loaded ungoverned if the run is not escalated).
    Writes nothing. Empty set is a valid, honest result (no feed ever
    registered)."""
    cur = conn.cursor(row_factory=dict_row)
    rows = cur.execute("SELECT DISTINCT feed_key FROM feed_contracts").fetchall()
    return {row["feed_key"] for row in rows}


def list_active_contracts(conn: DictRowConnection) -> list[FeedContract]:
    """Every currently active ``feed_contracts`` row, ordered by ``feed_key``
    — the full "expected feeds" set a daily run cross-references its inbox
    scan against (ADR-042 decision 3 step 2: "scan de l'inbox croisé avec
    get_active_contract() ... pour chaque feed_key attendu"), consumed by
    ``engine.ingest.daily_orchestrator``. Writes nothing. Empty list is a
    valid, honest result (no feed registered yet) — callers must not treat
    it as an error on its own."""
    cur = conn.cursor(row_factory=dict_row)
    rows = cur.execute(
        "SELECT feed_contract_id, feed_key, version, entity_type, "
        "source_system, format, key_columns, mandatory_columns, load_mode, "
        "cadence, arrival_window_minutes, owner, criticality, "
        "volume_guard_min_rows, volume_guard_max_pct_delta, depends_on, "
        "active, created_at, updated_at "
        "FROM feed_contracts WHERE active ORDER BY feed_key"
    ).fetchall()
    return [FeedContract(**row) for row in rows]


def upsert_contract(conn: DictRowConnection, spec: FeedContractSpec) -> LoadOutcome:
    """Idempotently register ``spec`` as the (possibly new) active version of
    its ``feed_key``.

    Never commits/rolls back — the caller owns the transaction (same
    convention as ``ScenarioManager``). Content-identical to the current
    active version is a traced no-op (nothing written, logged at INFO,
    ``LoadOutcome.action == "no_op"``). Any diff — or no active version yet —
    bookkeeping-flips the previous active row to ``active = FALSE`` (if any)
    then INSERTs the new version as ``active = TRUE``; an existing version
    row's content columns are never UPDATEd (append-only per version,
    migration 073 header).
    """
    cur = conn.cursor(row_factory=dict_row)
    current = cur.execute(
        "SELECT feed_contract_id, version, entity_type, source_system, "
        "format, key_columns, mandatory_columns, load_mode, cadence, "
        "arrival_window_minutes, owner, criticality, volume_guard_min_rows, "
        "volume_guard_max_pct_delta, depends_on "
        "FROM feed_contracts WHERE feed_key = %s AND active",
        (spec.feed_key,),
    ).fetchone()

    if current is not None:
        current_tuple = tuple(current[f] for f in _CONTENT_FIELDS)
        if spec.content_tuple() == current_tuple:
            logger.info(
                "feed_contract no-op feed_key=%s version=%s (content unchanged)",
                spec.feed_key, current["version"],
            )
            return LoadOutcome(
                feed_key=spec.feed_key,
                action="no_op",
                version=current["version"],
                feed_contract_id=current["feed_contract_id"],
            )

    next_version_row = cur.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 AS next_version "
        "FROM feed_contracts WHERE feed_key = %s",
        (spec.feed_key,),
    ).fetchone()
    if next_version_row is None:
        # Unreachable in practice (COALESCE over an aggregate always yields
        # exactly one row) — narrows the type for mypy and fails loudly
        # instead of a silent KeyError if that assumption is ever wrong.
        raise RuntimeError(
            f"upsert_contract: next-version query returned no row for "
            f"feed_key={spec.feed_key!r}"
        )
    next_version = next_version_row["next_version"]

    if current is not None:
        cur.execute(
            "UPDATE feed_contracts SET active = FALSE, updated_at = now() "
            "WHERE feed_key = %s AND active",
            (spec.feed_key,),
        )

    inserted = cur.execute(
        "INSERT INTO feed_contracts ("
        "    feed_key, version, entity_type, source_system, format, "
        "    key_columns, mandatory_columns, load_mode, cadence, "
        "    arrival_window_minutes, owner, criticality, "
        "    volume_guard_min_rows, volume_guard_max_pct_delta, depends_on, "
        "    active"
        ") VALUES ("
        "    %(feed_key)s, %(version)s, %(entity_type)s, %(source_system)s, "
        "    %(format)s, %(key_columns)s, %(mandatory_columns)s, "
        "    %(load_mode)s, %(cadence)s, %(arrival_window_minutes)s, "
        "    %(owner)s, %(criticality)s, %(volume_guard_min_rows)s, "
        "    %(volume_guard_max_pct_delta)s, %(depends_on)s, TRUE"
        ") RETURNING feed_contract_id",
        {
            "feed_key": spec.feed_key,
            "version": next_version,
            "entity_type": spec.entity_type,
            "source_system": spec.source_system,
            "format": spec.format,
            "key_columns": spec.key_columns,
            "mandatory_columns": spec.mandatory_columns,
            "load_mode": spec.load_mode,
            "cadence": spec.cadence,
            "arrival_window_minutes": spec.arrival_window_minutes,
            "owner": spec.owner,
            "criticality": spec.criticality,
            "volume_guard_min_rows": spec.volume_guard_min_rows,
            "volume_guard_max_pct_delta": spec.volume_guard_max_pct_delta,
            "depends_on": spec.depends_on,
        },
    ).fetchone()
    if inserted is None:
        raise RuntimeError(
            f"upsert_contract: INSERT ... RETURNING yielded no row for "
            f"feed_key={spec.feed_key!r}"
        )

    logger.info(
        "feed_contract created feed_key=%s version=%s criticality=%s",
        spec.feed_key, next_version, spec.criticality,
    )
    return LoadOutcome(
        feed_key=spec.feed_key,
        action="created",
        version=next_version,
        feed_contract_id=inserted["feed_contract_id"],
    )
