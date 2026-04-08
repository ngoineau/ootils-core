"""
DQ Engine — Data Quality pipeline, L1 (structural) + L2 (referential).

Interface:
    run_dq(db: psycopg.Connection, batch_id: UUID) -> DQResult

L1 rules — structural checks on raw_content (JSON):
    - L1_MISSING_FIELD : mandatory field absent or None
    - L1_INVALID_TYPE  : field value has wrong type/format
    - L1_INVALID_FORMAT: string too long (> 255 chars)

L2 rules — referential integrity against live DB:
    - L2_UNKNOWN_REF   : referenced entity not found in DB

After execution:
    - ingest_rows.dq_status updated (pending → passed/failed/warning)
    - ingest_rows.dq_level_reached updated
    - ingest_batches.dq_status updated
    - data_quality_issues populated
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Result types
# ──────────────────────────────────────────────────────────────

@dataclass
class DQIssue:
    row_id: UUID
    row_number: int
    dq_level: int
    rule_code: str
    severity: str  # 'error' | 'warning' | 'info'
    field_name: str | None
    raw_value: str | None
    message: str


@dataclass
class DQResult:
    batch_id: UUID
    total_rows: int
    passed_rows: int
    failed_rows: int
    warning_rows: int
    issues: list[DQIssue] = field(default_factory=list)
    batch_dq_status: str = "pending"


# ──────────────────────────────────────────────────────────────
# Schema definitions per entity_type
# Each entry: field_name → (mandatory, type_check, max_len)
# type_check: 'str' | 'date' | 'numeric_positive' | 'uuid' | 'int_positive'
# ──────────────────────────────────────────────────────────────

_SCHEMAS: dict[str, list[tuple[str, bool, str, int | None]]] = {
    # (field_name, mandatory, type_check, max_len)
    "items": [
        ("external_id", True, "str", 255),
        ("name", True, "str", 255),
        ("item_type", True, "str", 64),
        ("uom", True, "str", 32),
        ("status", True, "str", 64),
    ],
    "locations": [
        ("external_id", True, "str", 255),
        ("name", True, "str", 255),
        ("location_type", True, "str", 64),
    ],
    "suppliers": [
        ("external_id", True, "str", 255),
        ("name", True, "str", 255),
        ("status", True, "str", 64),
    ],
    "supplier_items": [
        ("supplier_external_id", True, "str", 255),
        ("item_external_id", True, "str", 255),
        ("lead_time_days", True, "int_positive", None),
    ],
    "purchase_orders": [
        ("external_id", True, "str", 255),
        ("item_external_id", True, "str", 255),
        ("location_external_id", True, "str", 255),
        ("supplier_external_id", True, "str", 255),
        ("quantity", True, "numeric_positive", None),
        ("uom", True, "str", 32),
        ("expected_delivery_date", True, "date", None),
        ("status", True, "str", 64),
    ],
    "forecast_demand": [
        ("item_external_id", True, "str", 255),
        ("location_external_id", True, "str", 255),
        ("quantity", True, "numeric_positive", None),
        ("bucket_date", True, "date", None),
        ("time_grain", True, "str", 32),
    ],
    "on_hand": [
        ("item_external_id", True, "str", 255),
        ("location_external_id", True, "str", 255),
        ("quantity", True, "numeric_positive", None),
        ("uom", True, "str", 32),
        ("as_of_date", True, "date", None),
    ],
    # Fallback for unknown entity types — minimal structural checks
    "forecasts": [
        ("item_external_id", True, "str", 255),
        ("location_external_id", True, "str", 255),
        ("quantity", True, "numeric_positive", None),
        ("bucket_date", True, "date", None),
    ],
    "customer_orders": [
        ("external_id", True, "str", 255),
        ("item_external_id", True, "str", 255),
        ("location_external_id", True, "str", 255),
        ("quantity", True, "numeric_positive", None),
    ],
    "work_orders": [
        ("external_id", True, "str", 255),
        ("item_external_id", True, "str", 255),
        ("quantity", True, "numeric_positive", None),
    ],
    "transfers": [
        ("external_id", True, "str", 255),
        ("item_external_id", True, "str", 255),
        ("from_location_external_id", True, "str", 255),
        ("to_location_external_id", True, "str", 255),
        ("quantity", True, "numeric_positive", None),
    ],
}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


# ──────────────────────────────────────────────────────────────
# L1 — Structural checks
# ──────────────────────────────────────────────────────────────

def _check_l1(
    row_id: UUID,
    row_number: int,
    content: dict[str, Any],
    entity_type: str,
) -> list[DQIssue]:
    issues: list[DQIssue] = []
    schema = _SCHEMAS.get(entity_type, [])

    for field_name, mandatory, type_check, max_len in schema:
        value = content.get(field_name)

        # Missing / null mandatory field
        if mandatory and (value is None or value == ""):
            issues.append(DQIssue(
                row_id=row_id,
                row_number=row_number,
                dq_level=1,
                rule_code="L1_MISSING_FIELD",
                severity="error",
                field_name=field_name,
                raw_value=None,
                message=f"Mandatory field '{field_name}' is missing or null",
            ))
            continue  # No point type-checking a missing field

        if value is None:
            continue  # Optional field, skip

        raw_str = str(value)

        # Type checks
        type_error = False
        if type_check == "date":
            v = value if isinstance(value, str) else str(value)
            if not _DATE_RE.match(v):
                type_error = True
                issues.append(DQIssue(
                    row_id=row_id,
                    row_number=row_number,
                    dq_level=1,
                    rule_code="L1_INVALID_TYPE",
                    severity="error",
                    field_name=field_name,
                    raw_value=raw_str[:255],
                    message=f"Field '{field_name}' must be a date (YYYY-MM-DD), got: {raw_str[:100]}",
                ))
            else:
                # Also validate it's a real calendar date
                try:
                    datetime.strptime(v, "%Y-%m-%d")
                except ValueError:
                    type_error = True
                    issues.append(DQIssue(
                        row_id=row_id,
                        row_number=row_number,
                        dq_level=1,
                        rule_code="L1_INVALID_TYPE",
                        severity="error",
                        field_name=field_name,
                        raw_value=raw_str[:255],
                        message=f"Field '{field_name}' is not a valid calendar date: {raw_str[:100]}",
                    ))

        elif type_check == "numeric_positive":
            try:
                num = float(value)
                if num <= 0:
                    raise ValueError("not positive")
            except (TypeError, ValueError):
                type_error = True
                issues.append(DQIssue(
                    row_id=row_id,
                    row_number=row_number,
                    dq_level=1,
                    rule_code="L1_INVALID_TYPE",
                    severity="error",
                    field_name=field_name,
                    raw_value=raw_str[:255],
                    message=f"Field '{field_name}' must be a positive number, got: {raw_str[:100]}",
                ))

        elif type_check == "int_positive":
            try:
                num = int(value)
                if num <= 0:
                    raise ValueError("not positive")
            except (TypeError, ValueError):
                type_error = True
                issues.append(DQIssue(
                    row_id=row_id,
                    row_number=row_number,
                    dq_level=1,
                    rule_code="L1_INVALID_TYPE",
                    severity="error",
                    field_name=field_name,
                    raw_value=raw_str[:255],
                    message=f"Field '{field_name}' must be a positive integer, got: {raw_str[:100]}",
                ))

        elif type_check == "uuid":
            if not _UUID_RE.match(str(value)):
                type_error = True
                issues.append(DQIssue(
                    row_id=row_id,
                    row_number=row_number,
                    dq_level=1,
                    rule_code="L1_INVALID_TYPE",
                    severity="error",
                    field_name=field_name,
                    raw_value=raw_str[:255],
                    message=f"Field '{field_name}' must be a valid UUID, got: {raw_str[:100]}",
                ))

        # String max length check
        if not type_error and max_len is not None and isinstance(value, str) and len(value) > max_len:
            issues.append(DQIssue(
                row_id=row_id,
                row_number=row_number,
                dq_level=1,
                rule_code="L1_INVALID_FORMAT",
                severity="error",
                field_name=field_name,
                raw_value=raw_str[:255],
                message=f"Field '{field_name}' exceeds max length {max_len} (got {len(value)} chars)",
            ))

    return issues


# ──────────────────────────────────────────────────────────────
# L2 — Referential checks
# ──────────────────────────────────────────────────────────────

def _batch_resolve_items(db: psycopg.Connection, external_ids: list[str]) -> set[str]:
    """Return set of external_ids that exist in items table."""
    if not external_ids:
        return set()
    rows = db.execute(
        "SELECT external_id FROM items WHERE external_id = ANY(%s)",
        (external_ids,),
    ).fetchall()
    return {r["external_id"] for r in rows}


def _batch_resolve_locations(db: psycopg.Connection, external_ids: list[str]) -> set[str]:
    if not external_ids:
        return set()
    rows = db.execute(
        "SELECT external_id FROM locations WHERE external_id = ANY(%s)",
        (external_ids,),
    ).fetchall()
    return {r["external_id"] for r in rows}


def _batch_resolve_suppliers(db: psycopg.Connection, external_ids: list[str]) -> set[str]:
    if not external_ids:
        return set()
    rows = db.execute(
        "SELECT external_id FROM suppliers WHERE external_id = ANY(%s)",
        (external_ids,),
    ).fetchall()
    return {r["external_id"] for r in rows}


def _check_l2(
    rows_data: list[tuple[UUID, int, dict[str, Any]]],
    entity_type: str,
    db: psycopg.Connection,
) -> list[DQIssue]:
    """
    Batch L2 checks for all rows of a given entity_type.
    Returns a flat list of issues.
    """
    issues: list[DQIssue] = []

    if entity_type == "purchase_orders":
        item_ids = [r[2].get("item_external_id") for r in rows_data if r[2].get("item_external_id")]
        loc_ids = [r[2].get("location_external_id") for r in rows_data if r[2].get("location_external_id")]
        sup_ids = [r[2].get("supplier_external_id") for r in rows_data if r[2].get("supplier_external_id")]

        valid_items = _batch_resolve_items(db, list(set(item_ids)))
        valid_locs = _batch_resolve_locations(db, list(set(loc_ids)))
        valid_sups = _batch_resolve_suppliers(db, list(set(sup_ids)))

        for row_id, row_number, content in rows_data:
            for ref_field, valid_set, label in [
                ("item_external_id", valid_items, "items"),
                ("location_external_id", valid_locs, "locations"),
                ("supplier_external_id", valid_sups, "suppliers"),
            ]:
                val = content.get(ref_field)
                if val and val not in valid_set:
                    issues.append(DQIssue(
                        row_id=row_id,
                        row_number=row_number,
                        dq_level=2,
                        rule_code="L2_UNKNOWN_REF",
                        severity="error",
                        field_name=ref_field,
                        raw_value=str(val)[:255],
                        message=f"'{ref_field}' value '{val}' not found in {label}",
                    ))

    elif entity_type in ("forecast_demand", "forecasts", "on_hand"):
        item_ids = [r[2].get("item_external_id") for r in rows_data if r[2].get("item_external_id")]
        loc_ids = [r[2].get("location_external_id") for r in rows_data if r[2].get("location_external_id")]

        valid_items = _batch_resolve_items(db, list(set(item_ids)))
        valid_locs = _batch_resolve_locations(db, list(set(loc_ids)))

        for row_id, row_number, content in rows_data:
            for ref_field, valid_set, label in [
                ("item_external_id", valid_items, "items"),
                ("location_external_id", valid_locs, "locations"),
            ]:
                val = content.get(ref_field)
                if val and val not in valid_set:
                    issues.append(DQIssue(
                        row_id=row_id,
                        row_number=row_number,
                        dq_level=2,
                        rule_code="L2_UNKNOWN_REF",
                        severity="error",
                        field_name=ref_field,
                        raw_value=str(val)[:255],
                        message=f"'{ref_field}' value '{val}' not found in {label}",
                    ))

    elif entity_type == "supplier_items":
        item_ids = [r[2].get("item_external_id") for r in rows_data if r[2].get("item_external_id")]
        sup_ids = [r[2].get("supplier_external_id") for r in rows_data if r[2].get("supplier_external_id")]

        valid_items = _batch_resolve_items(db, list(set(item_ids)))
        valid_sups = _batch_resolve_suppliers(db, list(set(sup_ids)))

        for row_id, row_number, content in rows_data:
            for ref_field, valid_set, label in [
                ("item_external_id", valid_items, "items"),
                ("supplier_external_id", valid_sups, "suppliers"),
            ]:
                val = content.get(ref_field)
                if val and val not in valid_set:
                    issues.append(DQIssue(
                        row_id=row_id,
                        row_number=row_number,
                        dq_level=2,
                        rule_code="L2_UNKNOWN_REF",
                        severity="error",
                        field_name=ref_field,
                        raw_value=str(val)[:255],
                        message=f"'{ref_field}' value '{val}' not found in {label}",
                    ))

    # No L2 rules for items, locations, suppliers (they ARE the reference tables)

    return issues


# ──────────────────────────────────────────────────────────────
# Persist issues + update row/batch statuses
# ──────────────────────────────────────────────────────────────

def _persist_issues(db: psycopg.Connection, batch_id: UUID, issues: list[DQIssue]) -> None:
    if not issues:
        return
    with db.cursor() as cur:
        for issue in issues:
            cur.execute(
                """
                INSERT INTO data_quality_issues
                    (issue_id, batch_id, row_id, row_number, dq_level, rule_code,
                     severity, field_name, raw_value, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid4(),
                    batch_id,
                    issue.row_id,
                    issue.row_number,
                    issue.dq_level,
                    issue.rule_code,
                    issue.severity,
                    issue.field_name,
                    issue.raw_value,
                    issue.message,
                ),
            )


def _update_row_statuses(
    db: psycopg.Connection,
    row_issues: dict[UUID, list[DQIssue]],
    all_row_ids: list[UUID],
    max_level_reached: dict[UUID, int],
) -> None:
    """Update dq_status and dq_level_reached on each ingest_row."""
    for row_id in all_row_ids:
        row_issue_list = row_issues.get(row_id, [])
        has_error = any(i.severity == "error" for i in row_issue_list)
        has_warning = any(i.severity == "warning" for i in row_issue_list)

        if has_error:
            dq_status = "rejected"
        elif has_warning:
            dq_status = "l2_pass"  # passed both levels but with warnings
        else:
            dq_status = "l2_pass"  # clean pass

        level_reached = max_level_reached.get(row_id, 0)

        db.execute(
            """
            UPDATE ingest_rows
            SET dq_status = %s, dq_level_reached = %s
            WHERE row_id = %s
            """,
            (dq_status, level_reached, row_id),
        )


def _update_batch_status(
    db: psycopg.Connection,
    batch_id: UUID,
    all_issues: list[DQIssue],
    total_rows: int,
) -> str:
    """Compute and update ingest_batches.dq_status. Returns the new status."""
    has_error = any(i.severity == "error" for i in all_issues)
    has_warning = any(i.severity == "warning" for i in all_issues)

    if has_error:
        dq_status = "rejected"
    elif has_warning:
        dq_status = "validated"  # passed with warnings
    else:
        dq_status = "validated"

    db.execute(
        """
        UPDATE ingest_batches
        SET dq_status = %s, processed_at = now()
        WHERE batch_id = %s
        """,
        (dq_status, batch_id),
    )
    return dq_status


# ──────────────────────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────────────────────

def run_dq(db: psycopg.Connection, batch_id: UUID) -> DQResult:
    """
    Execute L1 + L2 DQ checks for all rows in a batch.

    Steps:
      1. Load batch metadata (entity_type)
      2. Load all ingest_rows for the batch
      3. Run L1 structural checks per row
      4. Run L2 referential checks (batched per entity_type)
      5. Persist all issues to data_quality_issues
      6. Update ingest_rows.dq_status / dq_level_reached
      7. Update ingest_batches.dq_status
      8. Return DQResult summary
    """
    # 1. Load batch
    batch_row = db.execute(
        "SELECT batch_id, entity_type, status FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()

    if not batch_row:
        raise ValueError(f"Batch {batch_id} not found")

    entity_type = batch_row["entity_type"]

    # 2. Load rows
    ingest_rows = db.execute(
        """
        SELECT row_id, row_number, raw_content
        FROM ingest_rows
        WHERE batch_id = %s
        ORDER BY row_number
        """,
        (batch_id,),
    ).fetchall()

    if not ingest_rows:
        # Empty batch — mark as validated
        db.execute(
            "UPDATE ingest_batches SET dq_status = 'validated', processed_at = now() WHERE batch_id = %s",
            (batch_id,),
        )
        return DQResult(
            batch_id=batch_id,
            total_rows=0,
            passed_rows=0,
            failed_rows=0,
            warning_rows=0,
            issues=[],
            batch_dq_status="validated",
        )

    all_issues: list[DQIssue] = []
    row_issues: dict[UUID, list[DQIssue]] = {}
    max_level_reached: dict[UUID, int] = {}
    rows_data: list[tuple[UUID, int, dict[str, Any]]] = []

    # 3. L1 structural checks
    for row in ingest_rows:
        row_id = row["row_id"]
        row_number = row["row_number"]

        # Parse raw_content
        try:
            content = json.loads(row["raw_content"])
            if not isinstance(content, dict):
                content = {}
        except (json.JSONDecodeError, TypeError):
            # Can't parse — generate a parse error issue
            parse_issue = DQIssue(
                row_id=row_id,
                row_number=row_number,
                dq_level=1,
                rule_code="L1_INVALID_FORMAT",
                severity="error",
                field_name=None,
                raw_value=str(row["raw_content"])[:255],
                message="raw_content is not valid JSON",
            )
            row_issues[row_id] = [parse_issue]
            all_issues.append(parse_issue)
            max_level_reached[row_id] = 0
            continue

        rows_data.append((row_id, row_number, content))
        l1_issues = _check_l1(row_id, row_number, content, entity_type)
        row_issues[row_id] = list(l1_issues)
        all_issues.extend(l1_issues)
        max_level_reached[row_id] = 1  # reached L1

    # 4. L2 referential checks — only for rows that passed L1 (no error issues)
    l2_candidates = [
        (row_id, row_number, content)
        for (row_id, row_number, content) in rows_data
        if not any(i.severity == "error" for i in row_issues.get(row_id, []))
    ]

    if l2_candidates:
        l2_issues = _check_l2(l2_candidates, entity_type, db)
        for issue in l2_issues:
            row_issues.setdefault(issue.row_id, []).append(issue)
        all_issues.extend(l2_issues)

    # Mark L2 reached for candidates
    for row_id, _, _ in l2_candidates:
        max_level_reached[row_id] = 2

    # 5. Persist issues
    _persist_issues(db, batch_id, all_issues)

    # 6. Update row statuses
    all_row_ids = [row["row_id"] for row in ingest_rows]
    _update_row_statuses(db, row_issues, all_row_ids, max_level_reached)

    # 7. Update batch status
    batch_dq_status = _update_batch_status(db, batch_id, all_issues, len(ingest_rows))

    # 8. Build result
    passed_rows = sum(
        1 for row_id in all_row_ids
        if not any(i.severity in ("error", "warning") for i in row_issues.get(row_id, []))
    )
    failed_rows = sum(
        1 for row_id in all_row_ids
        if any(i.severity == "error" for i in row_issues.get(row_id, []))
    )
    warning_rows = sum(
        1 for row_id in all_row_ids
        if (
            any(i.severity == "warning" for i in row_issues.get(row_id, []))
            and not any(i.severity == "error" for i in row_issues.get(row_id, []))
        )
    )

    logger.info(
        "dq.run batch_id=%s entity=%s total=%d passed=%d failed=%d warnings=%d issues=%d",
        batch_id, entity_type, len(ingest_rows), passed_rows, failed_rows, warning_rows, len(all_issues),
    )

    # Auto-trigger DQ Agent (stat + temporal + impact + LLM) after L1+L2
    try:
        from ootils_core.engine.dq.agent import run_dq_agent
        run_dq_agent(db, batch_id)
    except Exception as agent_exc:
        # Agent failure must never break the core DQ result
        logger.warning("dq_agent failed for batch %s (non-fatal): %s", batch_id, agent_exc)

    return DQResult(
        batch_id=batch_id,
        total_rows=len(ingest_rows),
        passed_rows=passed_rows,
        failed_rows=failed_rows,
        warning_rows=warning_rows,
        issues=all_issues,
        batch_dq_status=batch_dq_status,
    )
