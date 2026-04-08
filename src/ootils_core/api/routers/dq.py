"""
DQ Router — Data Quality pipeline endpoints.

POST /v1/dq/run/{batch_id}  — Trigger DQ run on a batch (synchronous, returns result)
GET  /v1/dq/{batch_id}      — Get DQ results for a batch
GET  /v1/dq/issues          — List unresolved issues (filterable)
"""
from __future__ import annotations

import logging
from typing import Any, Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db
from ootils_core.engine.dq.engine import run_dq

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/dq", tags=["dq"])


# ─────────────────────────────────────────────────────────────
# Response models
# ─────────────────────────────────────────────────────────────

class DQIssueOut(BaseModel):
    issue_id: UUID
    batch_id: UUID
    row_id: Optional[UUID]
    row_number: Optional[int]
    dq_level: int
    rule_code: str
    severity: str
    field_name: Optional[str]
    raw_value: Optional[str]
    message: str
    auto_corrected: bool
    resolved: bool
    created_at: Any


class DQRunResponse(BaseModel):
    batch_id: UUID
    status: str
    total_rows: int
    passed_rows: int
    failed_rows: int
    warning_rows: int
    issue_count: int
    batch_dq_status: str


class DQBatchResponse(BaseModel):
    batch_id: UUID
    entity_type: str
    dq_status: Optional[str]
    total_rows: int
    issues: list[DQIssueOut]


class DQIssuesResponse(BaseModel):
    total: int
    issues: list[DQIssueOut]


# ─────────────────────────────────────────────────────────────
# POST /v1/dq/run/{batch_id}
# ─────────────────────────────────────────────────────────────

@router.post(
    "/run/{batch_id}",
    response_model=DQRunResponse,
    summary="Run DQ pipeline on a batch",
    description="Execute L1 (structural) + L2 (referential) checks on all rows of a batch.",
)
async def run_dq_batch(
    batch_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> DQRunResponse:
    # Verify batch exists
    batch = db.execute(
        "SELECT batch_id FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    if not batch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Batch {batch_id} not found",
        )

    try:
        result = run_dq(db, batch_id)
    except Exception as exc:
        logger.exception("DQ run failed for batch %s: %s", batch_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"DQ run failed: {exc}",
        )

    return DQRunResponse(
        batch_id=result.batch_id,
        status="completed",
        total_rows=result.total_rows,
        passed_rows=result.passed_rows,
        failed_rows=result.failed_rows,
        warning_rows=result.warning_rows,
        issue_count=len(result.issues),
        batch_dq_status=result.batch_dq_status,
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/dq/issues  (must come BEFORE /{batch_id} to avoid routing conflict)
# ─────────────────────────────────────────────────────────────

@router.get(
    "/issues",
    response_model=DQIssuesResponse,
    summary="List unresolved DQ issues",
    description="Returns all unresolved data quality issues. Filterable by severity, dq_level, entity_type.",
)
async def list_issues(
    severity: Optional[str] = Query(default=None, description="Filter by severity: error | warning | info"),
    dq_level: Optional[int] = Query(default=None, description="Filter by DQ level: 1 | 2 | 3 | 4"),
    entity_type: Optional[str] = Query(default=None, description="Filter by entity_type (e.g. purchase_orders)"),
    limit: int = Query(default=200, ge=1, le=1000, description="Max results"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> DQIssuesResponse:
    conditions: list[str] = ["i.resolved = FALSE"]
    params: list[Any] = []

    if severity:
        conditions.append("i.severity = %s")
        params.append(severity)
    if dq_level is not None:
        conditions.append("i.dq_level = %s")
        params.append(dq_level)
    if entity_type:
        conditions.append("b.entity_type = %s")
        params.append(entity_type)

    where_clause = " AND ".join(conditions)
    count_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM data_quality_issues i
        JOIN ingest_batches b ON b.batch_id = i.batch_id
        WHERE {where_clause}
    """
    total = db.execute(count_sql, params).fetchone()["cnt"]

    query_sql = f"""
        SELECT
            i.issue_id, i.batch_id, i.row_id, i.row_number,
            i.dq_level, i.rule_code, i.severity,
            i.field_name, i.raw_value, i.message,
            i.auto_corrected, i.resolved, i.created_at
        FROM data_quality_issues i
        JOIN ingest_batches b ON b.batch_id = i.batch_id
        WHERE {where_clause}
        ORDER BY i.created_at DESC
        LIMIT %s OFFSET %s
    """
    rows = db.execute(query_sql, params + [limit, offset]).fetchall()

    issues = [
        DQIssueOut(
            issue_id=r["issue_id"],
            batch_id=r["batch_id"],
            row_id=r["row_id"],
            row_number=r["row_number"],
            dq_level=r["dq_level"],
            rule_code=r["rule_code"],
            severity=r["severity"],
            field_name=r["field_name"],
            raw_value=r["raw_value"],
            message=r["message"],
            auto_corrected=r["auto_corrected"],
            resolved=r["resolved"],
            created_at=r["created_at"],
        )
        for r in rows
    ]

    return DQIssuesResponse(total=total, issues=issues)


# ─────────────────────────────────────────────────────────────
# GET /v1/dq/{batch_id}
# ─────────────────────────────────────────────────────────────

@router.get(
    "/{batch_id}",
    response_model=DQBatchResponse,
    summary="Get DQ results for a batch",
    description="Returns all DQ issues (resolved or not) for a specific batch.",
)
async def get_batch_dq(
    batch_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> DQBatchResponse:
    batch = db.execute(
        "SELECT batch_id, entity_type, dq_status, total_rows FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()

    if not batch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Batch {batch_id} not found",
        )

    rows = db.execute(
        """
        SELECT issue_id, batch_id, row_id, row_number, dq_level, rule_code,
               severity, field_name, raw_value, message,
               auto_corrected, resolved, created_at
        FROM data_quality_issues
        WHERE batch_id = %s
        ORDER BY row_number, dq_level
        """,
        (batch_id,),
    ).fetchall()

    issues = [
        DQIssueOut(
            issue_id=r["issue_id"],
            batch_id=r["batch_id"],
            row_id=r["row_id"],
            row_number=r["row_number"],
            dq_level=r["dq_level"],
            rule_code=r["rule_code"],
            severity=r["severity"],
            field_name=r["field_name"],
            raw_value=r["raw_value"],
            message=r["message"],
            auto_corrected=r["auto_corrected"],
            resolved=r["resolved"],
            created_at=r["created_at"],
        )
        for r in rows
    ]

    return DQBatchResponse(
        batch_id=batch["batch_id"],
        entity_type=batch["entity_type"],
        dq_status=batch.get("dq_status"),
        total_rows=batch["total_rows"] or 0,
        issues=issues,
    )
