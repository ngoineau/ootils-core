"""
temporal_rules.py — Catégorie 3 : anomalies temporelles.

Rules:
  TEMP_DUPLICATE_BATCH       : >95% valeurs identiques au batch précédent
  TEMP_PO_DATE_PAST          : PO expected_date passée et status != 'received'
  TEMP_FORECAST_HORIZON_SHORT: horizon < max(lead_time_days) des items
  TEMP_MASS_CHANGE           : >30% des valeurs d'un champ changent vs batch précédent
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from uuid import UUID, uuid4

import psycopg

from .stat_rules import AgentIssue, SEVERITY_WARNING, SEVERITY_ERROR

logger = logging.getLogger(__name__)


def _load_batch_rows(db: psycopg.Connection, batch_id: UUID) -> list[dict]:
    """Load all row contents for a batch."""
    rows = db.execute(
        "SELECT raw_content FROM ingest_rows WHERE batch_id = %s ORDER BY row_number",
        (batch_id,),
    ).fetchall()
    result = []
    for r in rows:
        try:
            content = json.loads(r["raw_content"]) if isinstance(r["raw_content"], str) else r["raw_content"]
            if isinstance(content, dict):
                result.append(content)
        except Exception:
            pass
    return result


def _get_previous_batch_id(
    db: psycopg.Connection,
    entity_type: str,
    current_batch_id: UUID,
) -> UUID | None:
    """Return the batch_id of the most recent completed batch before the current one."""
    row = db.execute(
        """
        SELECT batch_id
        FROM ingest_batches
        WHERE entity_type = %s
          AND batch_id != %s
          AND status IN ('validated', 'rejected', 'imported', 'partial')
        ORDER BY COALESCE(imported_at, processed_at, submitted_at) DESC
        LIMIT 1
        """,
        (entity_type, current_batch_id),
    ).fetchone()
    return UUID(str(row["batch_id"])) if row else None


def _get_entity_type(db: psycopg.Connection, batch_id: UUID) -> str:
    row = db.execute(
        "SELECT entity_type FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    return row["entity_type"] if row else ""


def _row_fingerprint(content: dict) -> frozenset:
    """Create a hashable fingerprint from a content dict."""
    return frozenset((k, str(v)) for k, v in content.items())


# ──────────────────────────────────────────────────────────────
# TEMP_DUPLICATE_BATCH
# ──────────────────────────────────────────────────────────────

def _check_duplicate_batch(
    db: psycopg.Connection,
    batch_id: UUID,
    entity_type: str,
) -> list[AgentIssue]:
    """Detect if >95% of batch values are identical to the previous batch."""
    prev_batch_id = _get_previous_batch_id(db, entity_type, batch_id)
    if prev_batch_id is None:
        return []

    current_rows = _load_batch_rows(db, batch_id)
    prev_rows = _load_batch_rows(db, prev_batch_id)

    if not current_rows or not prev_rows:
        return []

    current_fps = {_row_fingerprint(r) for r in current_rows}
    prev_fps = {_row_fingerprint(r) for r in prev_rows}

    intersection = current_fps & prev_fps
    similarity = len(intersection) / len(current_fps) if current_fps else 0

    if similarity > 0.95:
        return [AgentIssue(
            issue_id=uuid4(),
            batch_id=batch_id,
            row_id=None,
            row_number=None,
            dq_level=3,
            rule_code="TEMP_DUPLICATE_BATCH",
            severity=SEVERITY_WARNING,
            field_name=None,
            raw_value=None,
            message=(
                f"Batch dupliqué : {similarity:.0%} des lignes sont identiques "
                f"au batch précédent ({prev_batch_id})"
            ),
        )]

    return []


# ──────────────────────────────────────────────────────────────
# TEMP_PO_DATE_PAST
# ──────────────────────────────────────────────────────────────

def _check_po_date_past(
    batch_id: UUID,
    current_rows: list[tuple[UUID, int, dict]],
    entity_type: str,
) -> list[AgentIssue]:
    """PO expected_date dans le passé et status != 'received'."""
    issues: list[AgentIssue] = []
    if entity_type not in ("purchase_orders",):
        return issues

    today = date.today()

    for row_id, row_number, content in current_rows:
        status = content.get("status", "")
        if status == "received":
            continue

        date_raw = content.get("expected_delivery_date")
        if date_raw is None:
            continue

        try:
            if isinstance(date_raw, str):
                expected = datetime.strptime(date_raw, "%Y-%m-%d").date()
            elif isinstance(date_raw, date):
                expected = date_raw
            else:
                continue
        except ValueError:
            continue

        if expected < today:
            issues.append(AgentIssue(
                issue_id=uuid4(),
                batch_id=batch_id,
                row_id=row_id,
                row_number=row_number,
                dq_level=3,
                rule_code="TEMP_PO_DATE_PAST",
                severity=SEVERITY_WARNING,
                field_name="expected_delivery_date",
                raw_value=str(date_raw),
                message=(
                    f"PO expected_delivery_date={date_raw} est dans le passé "
                    f"mais status='{status}' (non reçu)"
                ),
            ))

    return issues


# ──────────────────────────────────────────────────────────────
# TEMP_FORECAST_HORIZON_SHORT
# ──────────────────────────────────────────────────────────────

def _check_forecast_horizon_short(
    db: psycopg.Connection,
    batch_id: UUID,
    current_rows: list[tuple[UUID, int, dict]],
    entity_type: str,
) -> list[AgentIssue]:
    """Horizon forecast < max(lead_time_days) des items importés."""
    issues: list[AgentIssue] = []
    if entity_type not in ("forecast_demand", "forecasts"):
        return issues

    # Get max lead_time_days across all items referenced in this batch
    item_ext_ids = list({
        c.get("item_external_id")
        for _, _, c in current_rows
        if c.get("item_external_id")
    })
    if not item_ext_ids:
        return issues

    row = db.execute(
        """
        SELECT MAX(si.lead_time_days) AS max_lt
        FROM supplier_items si
        JOIN items i ON i.item_id = si.item_id
        WHERE i.external_id = ANY(%s)
        """,
        (item_ext_ids,),
    ).fetchone()

    max_lead_time = row["max_lt"] if row and row["max_lt"] else None
    if max_lead_time is None:
        return issues

    today = date.today()

    # Find max bucket_date across batch rows
    max_bucket: date | None = None
    for _, _, content in current_rows:
        bd = content.get("bucket_date")
        if bd:
            try:
                bd_date = datetime.strptime(bd, "%Y-%m-%d").date() if isinstance(bd, str) else bd
                if max_bucket is None or bd_date > max_bucket:
                    max_bucket = bd_date
            except ValueError:
                pass

    if max_bucket is None:
        return issues

    horizon_days = (max_bucket - today).days
    if horizon_days < max_lead_time:
        issues.append(AgentIssue(
            issue_id=uuid4(),
            batch_id=batch_id,
            row_id=None,
            row_number=None,
            dq_level=3,
            rule_code="TEMP_FORECAST_HORIZON_SHORT",
            severity=SEVERITY_WARNING,
            field_name="bucket_date",
            raw_value=str(max_bucket),
            message=(
                f"Horizon forecast={horizon_days} jours < max(lead_time_days)={max_lead_time} "
                f"— couverture insuffisante pour planifier le réappro"
            ),
        ))

    return issues


# ──────────────────────────────────────────────────────────────
# TEMP_MASS_CHANGE
# ──────────────────────────────────────────────────────────────

def _check_mass_change(
    db: psycopg.Connection,
    batch_id: UUID,
    entity_type: str,
) -> list[AgentIssue]:
    """>30% des valeurs d'un champ changent entre deux batches successifs."""
    issues: list[AgentIssue] = []

    prev_batch_id = _get_previous_batch_id(db, entity_type, batch_id)
    if prev_batch_id is None:
        return []

    current_rows = _load_batch_rows(db, batch_id)
    prev_rows = _load_batch_rows(db, prev_batch_id)

    if not current_rows or not prev_rows:
        return []

    # Collect all field names
    all_fields: set[str] = set()
    for r in current_rows + prev_rows:
        all_fields.update(r.keys())

    def _rows_to_keyed(rows: list[dict], field: str) -> dict[str, str]:
        result = {}
        for idx, r in enumerate(rows):
            key = str(r.get("external_id") or r.get("item_external_id") or idx)
            result[key] = str(r.get(field, ""))
        return result

    for field_name in all_fields:
        current_map = _rows_to_keyed(current_rows, field_name)
        prev_map = _rows_to_keyed(prev_rows, field_name)
        common_keys = set(current_map) & set(prev_map)
        if not common_keys:
            continue

        changes = sum(1 for k in common_keys if current_map[k] != prev_map[k])
        change_ratio = changes / len(common_keys)

        if change_ratio > 0.30:
            issues.append(AgentIssue(
                issue_id=uuid4(),
                batch_id=batch_id,
                row_id=None,
                row_number=None,
                dq_level=3,
                rule_code="TEMP_MASS_CHANGE",
                severity=SEVERITY_ERROR,
                field_name=field_name,
                raw_value=None,
                message=(
                    f"Changement massif détecté sur champ '{field_name}' : "
                    f"{change_ratio:.0%} des valeurs ont changé vs batch précédent"
                ),
            ))

    return issues


# ──────────────────────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────────────────────

def run_temporal_rules(
    db: psycopg.Connection,
    batch_id: UUID,
) -> list[AgentIssue]:
    """Run all temporal rules for a batch. Returns list of AgentIssue."""
    entity_type = _get_entity_type(db, batch_id)

    # Load current rows for per-row rules
    rows_data = db.execute(
        """
        SELECT row_id, row_number, raw_content
        FROM ingest_rows
        WHERE batch_id = %s
        ORDER BY row_number
        """,
        (batch_id,),
    ).fetchall()

    current_rows: list[tuple[UUID, int, dict]] = []
    for r in rows_data:
        try:
            content = json.loads(r["raw_content"]) if isinstance(r["raw_content"], str) else r["raw_content"]
            if isinstance(content, dict):
                current_rows.append((r["row_id"], r["row_number"], content))
        except Exception:
            pass

    issues: list[AgentIssue] = []

    issues.extend(_check_duplicate_batch(db, batch_id, entity_type))
    issues.extend(_check_po_date_past(batch_id, current_rows, entity_type))
    issues.extend(_check_forecast_horizon_short(db, batch_id, current_rows, entity_type))
    issues.extend(_check_mass_change(db, batch_id, entity_type))

    logger.info(
        "temporal_rules batch_id=%s entity=%s issues=%d",
        batch_id, entity_type, len(issues),
    )
    return issues
