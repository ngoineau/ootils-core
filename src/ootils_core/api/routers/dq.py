"""
DQ Router — Data Quality pipeline endpoints.

POST /v1/dq/run/{batch_id}          — Trigger DQ run on a batch (synchronous, returns result)
GET  /v1/dq/{batch_id}              — Get DQ results for a batch
GET  /v1/dq/issues                  — List unresolved issues (filterable)
POST /v1/dq/agent/run/{batch_id}    — Trigger DQ Agent on a batch
GET  /v1/dq/agent/report/{batch_id} — Full agent report for a batch
GET  /v1/dq/agent/runs              — History of agent runs
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


# ─────────────────────────────────────────────────────────────
# Agent response models
# ─────────────────────────────────────────────────────────────

class AgentRunRecord(BaseModel):
    run_id: UUID
    batch_id: UUID
    status: str
    model_used: Optional[str]
    started_at: Any
    completed_at: Any
    summary: Optional[Any]
    created_at: Any


class AgentIssueOut(BaseModel):
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
    impact_score: Optional[float]
    agent_run_id: Optional[UUID]
    llm_explanation: Optional[str]
    llm_suggestion: Optional[str]


class AgentReportResponse(BaseModel):
    batch_id: UUID
    entity_type: str
    run_id: Optional[UUID]
    status: Optional[str]
    analyzed_at: Any
    summary: Optional[Any]
    narrative: Optional[str]
    priority_actions: list[str]
    issues: list[AgentIssueOut]


class AgentRunResponse(BaseModel):
    run_id: UUID
    batch_id: UUID
    status: str
    agent_run_id: UUID


class AgentRunsResponse(BaseModel):
    total: int
    runs: list[AgentRunRecord]


# ─────────────────────────────────────────────────────────────
# POST /v1/dq/agent/run/{batch_id}
# ─────────────────────────────────────────────────────────────

@router.post(
    "/agent/run/{batch_id}",
    response_model=AgentRunResponse,
    summary="Trigger DQ Agent on a batch",
    description=(
        "Run the DQ Agent (stat + temporal + impact SC + LLM) on an already-ingested batch. "
        "The agent enriches data_quality_issues with impact_score and LLM insights."
    ),
)
async def run_agent_batch(
    batch_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> AgentRunResponse:
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
        from ootils_core.engine.dq.agent import run_dq_agent
        result = run_dq_agent(db, batch_id)
    except Exception as exc:
        logger.exception("DQ Agent run failed for batch %s: %s", batch_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"DQ Agent run failed: {exc}",
        )

    return AgentRunResponse(
        run_id=result.run_id,
        batch_id=result.batch_id,
        status=result.status,
        agent_run_id=result.run_id,
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/dq/agent/report/{batch_id}  (must come BEFORE /agent/runs)
# ─────────────────────────────────────────────────────────────

@router.get(
    "/agent/report/{batch_id}",
    response_model=AgentReportResponse,
    summary="Get DQ Agent report for a batch",
    description="Returns the full agent report: narrative, priority actions, and enriched issues.",
)
async def get_agent_report(
    batch_id: UUID,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> AgentReportResponse:
    batch = db.execute(
        "SELECT batch_id, entity_type FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    if not batch:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Batch {batch_id} not found",
        )

    # Get latest agent run for this batch
    agent_run = db.execute(
        """
        SELECT run_id, status, completed_at, summary, llm_narrative
        FROM dq_agent_runs
        WHERE batch_id = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (batch_id,),
    ).fetchone()

    # Load all issues (L1/L2 + agent) with impact data
    rows = db.execute(
        """
        SELECT issue_id, batch_id, row_id, row_number, dq_level, rule_code,
               severity, field_name, raw_value, message,
               impact_score, agent_run_id, llm_explanation, llm_suggestion
        FROM data_quality_issues
        WHERE batch_id = %s
        ORDER BY impact_score DESC NULLS LAST, row_number NULLS LAST
        """,
        (batch_id,),
    ).fetchall()

    issues = [
        AgentIssueOut(
            issue_id=r["issue_id"],
            batch_id=r["batch_id"],
            row_id=r.get("row_id"),
            row_number=r.get("row_number"),
            dq_level=r["dq_level"],
            rule_code=r["rule_code"],
            severity=r["severity"],
            field_name=r.get("field_name"),
            raw_value=r.get("raw_value"),
            message=r["message"],
            impact_score=float(r["impact_score"]) if r.get("impact_score") is not None else None,
            agent_run_id=r.get("agent_run_id"),
            llm_explanation=r.get("llm_explanation"),
            llm_suggestion=r.get("llm_suggestion"),
        )
        for r in rows
    ]

    summary = None
    narrative = None
    priority_actions: list[str] = []
    run_id = None
    run_status = None
    analyzed_at = None

    if agent_run:
        run_id = agent_run["run_id"]
        run_status = agent_run["status"]
        analyzed_at = agent_run["completed_at"]
        narrative = agent_run.get("llm_narrative")
        raw_summary = agent_run.get("summary")
        if raw_summary:
            import json as _json
            summary = _json.loads(raw_summary) if isinstance(raw_summary, str) else raw_summary
            priority_actions = summary.get("priority_actions", []) if isinstance(summary, dict) else []

    return AgentReportResponse(
        batch_id=batch["batch_id"],
        entity_type=batch["entity_type"],
        run_id=run_id,
        status=run_status,
        analyzed_at=analyzed_at,
        summary=summary,
        narrative=narrative,
        priority_actions=priority_actions,
        issues=issues,
    )


# ─────────────────────────────────────────────────────────────
# GET /v1/dq/agent/runs
# ─────────────────────────────────────────────────────────────

@router.get(
    "/agent/runs",
    response_model=AgentRunsResponse,
    summary="List DQ Agent run history",
    description="Returns the history of DQ Agent runs across all batches.",
)
async def list_agent_runs(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> AgentRunsResponse:
    total = db.execute(
        "SELECT COUNT(*) AS cnt FROM dq_agent_runs"
    ).fetchone()["cnt"]

    rows = db.execute(
        """
        SELECT run_id, batch_id, status, model_used, started_at, completed_at, summary, created_at
        FROM dq_agent_runs
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    ).fetchall()

    runs = [
        AgentRunRecord(
            run_id=r["run_id"],
            batch_id=r["batch_id"],
            status=r["status"],
            model_used=r.get("model_used"),
            started_at=r.get("started_at"),
            completed_at=r.get("completed_at"),
            summary=r.get("summary"),
            created_at=r["created_at"],
        )
        for r in rows
    ]

    return AgentRunsResponse(total=total, runs=runs)
