"""
diff.py — compute the impact of approving a validated batch (ADR-013 D4).

Returns a `DiffResult` summarising what `POST /approve` would change in
the canonical tables if it ran right now:

  will_insert     external_ids in the batch with no canonical row yet
  will_update     external_ids in both, where one or more fields differ
  will_noop       external_ids in both, fields identical (batch reproduces
                  what's already there)
  will_soft_delete external_ids present in canonical for this
                  (entity_type, source_system) but absent from the batch

Plus a `deletion_ratio` (= soft_delete / current_active) with a 20%
threshold flag — ADR-013 D4 mandates that an approval crossing this
threshold requires an explicit `force=true` from the operator, so the
endpoint surfaces the warning here.

The endpoint is purely READ — no DB writes — and lives in
`api/routers/staging.py` as `GET /v1/staging/batches/{id}/diff`.

Currently supports the three master-data entity types (items,
locations, suppliers). Transactional entities (PO/WO/CO/transfers/etc)
use timestamps + status transitions and don't have stable upsert
semantics by external_id — they'll get their own diff logic when the
/approve step lands for them.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from uuid import UUID

from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

# Above this fraction of soft-deletes the approval needs `force=true`.
# Mirrors the threshold called out in ADR-013 D4.
DELETION_RATIO_THRESHOLD = 0.20

# Maximum sample rows per category in the response. Keeps the diff
# response payload manageable even for large batches.
MAX_SAMPLE_PER_CATEGORY = 10


class DiffError(ValueError):
    """Raised when the diff cannot be computed (bad batch state, etc.)."""


@dataclass
class DiffResult:
    """One diff response. All counts are non-negative; sample lists are
    truncated to MAX_SAMPLE_PER_CATEGORY external_ids."""
    batch_id: UUID
    entity_type: str
    source_system: str
    supported: bool
    # Counts
    total_in_batch: int = 0
    total_in_canonical_for_source: int = 0
    will_insert_count: int = 0
    will_update_count: int = 0
    will_noop_count: int = 0
    will_soft_delete_count: int = 0
    # Samples (external_id strings)
    will_insert_sample: list[str] = field(default_factory=list)
    will_update_sample: list[str] = field(default_factory=list)
    will_soft_delete_sample: list[str] = field(default_factory=list)
    # Deletion ratio guard
    deletion_ratio: float = 0.0
    deletion_ratio_threshold: float = DELETION_RATIO_THRESHOLD
    exceeds_deletion_threshold: bool = False
    # Diagnostic for unsupported entity_types
    unsupported_reason: str | None = None


# ---------------------------------------------------------------------------
# Per-entity field-comparison specs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _EntitySpec:
    """How to load + compare canonical rows for one entity_type."""
    canonical_table: str
    # SQL fields to SELECT (used both for loading + comparison)
    fields: tuple[str, ...]
    # The canonical PK column name (used by external_references.internal_id)
    pk_column: str
    # `external_references.entity_type` uses SINGULAR ('item', 'location', ...)
    # while `ingest_batches.entity_type` uses PLURAL ('items', 'locations').
    # This is the singular form for the join.
    ref_entity_type: str


_SPECS: dict[str, _EntitySpec] = {
    "items": _EntitySpec(
        canonical_table="items",
        fields=("external_id", "name", "item_type", "uom", "status"),
        pk_column="item_id",
        ref_entity_type="item",
    ),
    "locations": _EntitySpec(
        canonical_table="locations",
        fields=("external_id", "name", "location_type", "country", "timezone"),
        pk_column="location_id",
        ref_entity_type="location",
    ),
    "suppliers": _EntitySpec(
        canonical_table="suppliers",
        # lead_time_days + reliability_score may be NULL — comparison
        # normalises them via str() so '14' == 14 etc.
        fields=("external_id", "name", "country", "lead_time_days",
                "reliability_score", "status"),
        pk_column="supplier_id",
        ref_entity_type="supplier",
    ),
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def compute_diff(conn: DictRowConnection, batch_id: UUID) -> DiffResult:
    """Compute insert/update/soft-delete impact of approving this batch.

    Raises:
        DiffError if the batch doesn't exist or isn't in a status where
        a diff makes sense (must be 'validated' or 'pending' — anything
        else is past the decision point).
    """
    # ---------- 1. Load batch metadata ----------
    batch = conn.execute(
        "SELECT entity_type, source_system, status, dq_status "
        "FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    if batch is None:
        raise DiffError(f"batch {batch_id} not found")

    entity_type = batch["entity_type"] if isinstance(batch, dict) else batch[0]
    source_system = batch["source_system"] if isinstance(batch, dict) else batch[1]
    status = batch["status"] if isinstance(batch, dict) else batch[2]

    # Diff only meaningful before approval/rejection
    if status in ("imported", "rejected"):
        raise DiffError(
            f"batch is in terminal status {status!r}; /diff is only meaningful "
            "for batches in 'pending' or 'validated' state"
        )

    spec = _SPECS.get(entity_type)
    if spec is None:
        return DiffResult(
            batch_id=batch_id,
            entity_type=entity_type,
            source_system=source_system,
            supported=False,
            unsupported_reason=(
                f"entity_type {entity_type!r} does not yet have diff support — "
                "currently implemented for items / locations / suppliers"
            ),
        )

    # ---------- 2. Load batch rows ----------
    batch_records = _load_batch_records(conn, batch_id, spec)

    # ---------- 3. Load canonical rows scoped to (entity_type, source_system) ----------
    canonical_records = _load_canonical_records(conn, source_system, spec)

    # ---------- 4. Compute deltas ----------
    return _compute_deltas(batch_id, entity_type, source_system, spec,
                           batch_records, canonical_records)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_batch_records(
    conn: DictRowConnection,
    batch_id: UUID,
    spec: _EntitySpec,
) -> dict[str, dict]:
    """Parse `raw_content` JSON of every ingest_rows row in the batch,
    keyed by `external_id`. Rows missing an external_id are skipped (L1
    flagged them already). Duplicates resolve to the LAST occurrence."""
    rows = conn.execute(
        "SELECT row_number, raw_content FROM ingest_rows "
        "WHERE batch_id = %s ORDER BY row_number",
        (batch_id,),
    ).fetchall()
    out: dict[str, dict] = {}
    for row in rows:
        raw = row["raw_content"] if isinstance(row, dict) else row[1]
        try:
            content = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(content, dict):
            continue
        ext_id = content.get("external_id")
        if not ext_id:
            continue
        # Keep only the fields the canonical comparison cares about,
        # normalised to strings for the equality check below.
        normalised = {f: _norm(content.get(f)) for f in spec.fields}
        out[ext_id] = normalised
    return out


def _load_canonical_records(
    conn: DictRowConnection,
    source_system: str,
    spec: _EntitySpec,
) -> dict[str, dict]:
    """Load rows from the canonical table that were imported from this
    source_system — joined through `external_references` so each source's
    footprint stays isolated."""
    fields_sql = ", ".join(f"c.{f}" for f in spec.fields)
    rows = conn.execute(
        f"""
        SELECT {fields_sql}
        FROM {spec.canonical_table} c
        JOIN external_references r
          ON r.internal_id = c.{spec.pk_column}
        WHERE r.entity_type = %s
          AND r.source_system = %s
        """,
        (spec.ref_entity_type, source_system),
    ).fetchall()
    out: dict[str, dict] = {}
    for r in rows:
        if isinstance(r, dict):
            row_dict = r
        else:
            row_dict = dict(zip(spec.fields, r))
        ext_id = row_dict.get("external_id")
        if not ext_id:
            continue
        out[ext_id] = {f: _norm(row_dict.get(f)) for f in spec.fields}
    return out


def _compute_deltas(
    batch_id: UUID,
    entity_type: str,
    source_system: str,
    spec: _EntitySpec,
    batch_records: dict[str, dict],
    canonical_records: dict[str, dict],
) -> DiffResult:
    """Pure set-arithmetic on the two record dicts. No DB."""
    batch_ids = set(batch_records.keys())
    canonical_ids = set(canonical_records.keys())

    inserts = sorted(batch_ids - canonical_ids)
    deletes = sorted(canonical_ids - batch_ids)

    updates: list[str] = []
    noops: list[str] = []
    for ext_id in sorted(batch_ids & canonical_ids):
        if batch_records[ext_id] != canonical_records[ext_id]:
            updates.append(ext_id)
        else:
            noops.append(ext_id)

    n_canonical = len(canonical_ids)
    deletion_ratio = (len(deletes) / n_canonical) if n_canonical else 0.0
    exceeds = deletion_ratio > DELETION_RATIO_THRESHOLD

    return DiffResult(
        batch_id=batch_id,
        entity_type=entity_type,
        source_system=source_system,
        supported=True,
        total_in_batch=len(batch_ids),
        total_in_canonical_for_source=n_canonical,
        will_insert_count=len(inserts),
        will_update_count=len(updates),
        will_noop_count=len(noops),
        will_soft_delete_count=len(deletes),
        will_insert_sample=inserts[:MAX_SAMPLE_PER_CATEGORY],
        will_update_sample=updates[:MAX_SAMPLE_PER_CATEGORY],
        will_soft_delete_sample=deletes[:MAX_SAMPLE_PER_CATEGORY],
        deletion_ratio=round(deletion_ratio, 4),
        deletion_ratio_threshold=DELETION_RATIO_THRESHOLD,
        exceeds_deletion_threshold=exceeds,
    )


def _norm(v) -> str:
    """Normalise a scalar value to a string for equality comparison.

    Both sides of the diff pass through this so '14' (TSV) == 14 (DB int)
    == Decimal('14') (DB numeric). NULLs become '' so an empty TSV cell
    matches a NULL canonical column.
    """
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v).strip()
