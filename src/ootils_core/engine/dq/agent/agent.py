"""
agent.py — DQ Agent dispatcher.

Entry point: run_dq_agent(db, batch_id) → AgentResult

Sequence:
  1. Load existing DQ issues (L1+L2) from data_quality_issues
  2. Run stat_rules (category 1)
  3. Run temporal_rules (category 3)
  4. Run impact_scorer (category 4)
  5. Call LLM reporter
  6. Persist dq_agent_runs + enrich data_quality_issues
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import UUID, uuid4

import psycopg

from .stat_rules import AgentIssue, run_stat_rules
from .temporal_rules import run_temporal_rules
from .impact_scorer import score_issues
from .llm_reporter import generate_llm_report, LLMReport

logger = logging.getLogger(__name__)


@dataclass
class AgentResult:
    run_id: UUID
    batch_id: UUID
    status: str  # 'completed' | 'failed'
    issues: list[AgentIssue]
    summary: dict
    narrative: str
    priority_actions: list[str]
    model_used: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


def _load_existing_issues(
    db: psycopg.Connection,
    batch_id: UUID,
) -> list[AgentIssue]:
    """Load L1+L2 issues already in data_quality_issues for this batch."""
    rows = db.execute(
        """
        SELECT issue_id, batch_id, row_id, row_number, dq_level,
               rule_code, severity, field_name, raw_value, message
        FROM data_quality_issues
        WHERE batch_id = %s
        ORDER BY row_number NULLS LAST
        """,
        (batch_id,),
    ).fetchall()

    issues = []
    for r in rows:
        issues.append(AgentIssue(
            issue_id=UUID(str(r["issue_id"])),
            batch_id=UUID(str(r["batch_id"])),
            row_id=UUID(str(r["row_id"])) if r.get("row_id") else None,
            row_number=r.get("row_number"),
            dq_level=r["dq_level"],
            rule_code=r["rule_code"],
            severity=r["severity"],
            field_name=r.get("field_name"),
            raw_value=r.get("raw_value"),
            message=r["message"],
        ))
    return issues


def _get_entity_type(db: psycopg.Connection, batch_id: UUID) -> str:
    row = db.execute(
        "SELECT entity_type, total_rows FROM ingest_batches WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    return (row["entity_type"] if row else "", row["total_rows"] if row else 0)


def _persist_agent_run(
    db: psycopg.Connection,
    run_id: UUID,
    batch_id: UUID,
    status: str,
    started_at: datetime,
    completed_at: datetime,
    summary: dict,
    narrative: str,
    model_used: str | None,
) -> None:
    """Insert a record into dq_agent_runs."""
    import json as _json
    db.execute(
        """
        INSERT INTO dq_agent_runs
            (run_id, batch_id, status, model_used, started_at, completed_at, summary, llm_narrative)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            status       = EXCLUDED.status,
            model_used   = EXCLUDED.model_used,
            completed_at = EXCLUDED.completed_at,
            summary      = EXCLUDED.summary,
            llm_narrative = EXCLUDED.llm_narrative
        """,
        (
            run_id,
            batch_id,
            status,
            model_used,
            started_at,
            completed_at,
            _json.dumps(summary),
            narrative,
        ),
    )


def _persist_new_issues(
    db: psycopg.Connection,
    batch_id: UUID,
    run_id: UUID,
    issues: list[AgentIssue],
) -> None:
    """Persist agent-generated issues (stat/temporal) to data_quality_issues."""
    for issue in issues:
        db.execute(
            """
            INSERT INTO data_quality_issues
                (issue_id, batch_id, row_id, row_number, dq_level, rule_code,
                 severity, field_name, raw_value, message,
                 impact_score, agent_run_id, llm_explanation, llm_suggestion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (issue_id) DO NOTHING
            """,
            (
                issue.issue_id,
                batch_id,
                issue.row_id,
                issue.row_number,
                issue.dq_level,
                issue.rule_code,
                issue.severity,
                issue.field_name,
                issue.raw_value,
                issue.message,
                issue.impact_score,
                run_id,
                issue.llm_explanation,
                issue.llm_suggestion,
            ),
        )


def _enrich_existing_issues(
    db: psycopg.Connection,
    run_id: UUID,
    issues: list[AgentIssue],
) -> None:
    """Update L1+L2 issues with impact_score, agent_run_id, llm fields."""
    for issue in issues:
        db.execute(
            """
            UPDATE data_quality_issues
            SET impact_score    = %s,
                agent_run_id    = %s,
                llm_explanation = %s,
                llm_suggestion  = %s
            WHERE issue_id = %s
            """,
            (
                issue.impact_score,
                run_id,
                issue.llm_explanation,
                issue.llm_suggestion,
                issue.issue_id,
            ),
        )


def run_dq_agent(
    db: psycopg.Connection,
    batch_id: UUID,
) -> AgentResult:
    """
    Main DQ Agent dispatcher.

    Steps:
      1. Load existing L1+L2 issues
      2. Run stat_rules
      3. Run temporal_rules
      4. Impact score all issues
      5. LLM report
      6. Persist everything
    """
    run_id = uuid4()
    started_at = datetime.now(timezone.utc)

    logger.info("dq_agent.run start batch_id=%s run_id=%s", batch_id, run_id)

    # Mark run as running
    db.execute(
        """
        INSERT INTO dq_agent_runs
            (run_id, batch_id, status, started_at)
        VALUES (%s, %s, 'running', %s)
        """,
        (run_id, batch_id, started_at),
    )
    db.commit()

    try:
        entity_type, total_rows = _get_entity_type(db, batch_id)

        # 1. Load existing L1+L2 issues
        existing_issues = _load_existing_issues(db, batch_id)

        # 2. Stat rules
        stat_issues = run_stat_rules(db, batch_id)

        # 3. Temporal rules
        temporal_issues = run_temporal_rules(db, batch_id)

        # All agent-generated issues (to persist as new rows)
        new_issues = stat_issues + temporal_issues

        # All issues for scoring (existing L1/L2 + new agent issues)
        all_issues = existing_issues + new_issues

        # 4. Impact scoring on all issues
        score_issues(db, batch_id, all_issues)

        # 5. LLM report
        report: LLMReport = generate_llm_report(
            all_issues, entity_type, batch_id, total_rows
        )

        completed_at = datetime.now(timezone.utc)

        # Build summary
        summary = {
            "total_rows": total_rows,
            "issues_count": len(all_issues),
            "critical_count": sum(1 for i in all_issues if i.severity == "error"),
            "warning_count": sum(1 for i in all_issues if i.severity == "warning"),
            "stat_issues": len(stat_issues),
            "temporal_issues": len(temporal_issues),
            "affected_items_count": len(
                set(item for i in all_issues for item in i.affected_items)
            ),
            "active_shortages_impacted": max(
                (i.active_shortages_count for i in all_issues), default=0
            ),
            "llm_available": report.llm_available,
            "priority_actions": report.priority_actions,
        }

        # 6. Persist
        # Update agent run record
        _persist_agent_run(
            db,
            run_id=run_id,
            batch_id=batch_id,
            status="completed",
            started_at=started_at,
            completed_at=completed_at,
            summary=summary,
            narrative=report.narrative,
            model_used=report.model_used,
        )

        # Persist new agent issues
        _persist_new_issues(db, batch_id, run_id, new_issues)

        # Enrich existing L1/L2 issues
        _enrich_existing_issues(db, run_id, existing_issues)

        db.commit()

        logger.info(
            "dq_agent.run complete batch_id=%s run_id=%s issues=%d stat=%d temporal=%d",
            batch_id, run_id, len(all_issues), len(stat_issues), len(temporal_issues),
        )

        return AgentResult(
            run_id=run_id,
            batch_id=batch_id,
            status="completed",
            issues=all_issues,
            summary=summary,
            narrative=report.narrative,
            priority_actions=report.priority_actions,
            model_used=report.model_used,
            started_at=started_at,
            completed_at=completed_at,
        )

    except Exception as exc:
        logger.exception("dq_agent.run failed batch_id=%s run_id=%s: %s", batch_id, run_id, exc)
        completed_at = datetime.now(timezone.utc)
        db.execute(
            """
            UPDATE dq_agent_runs
            SET status = 'failed', completed_at = %s
            WHERE run_id = %s
            """,
            (completed_at, run_id),
        )
        db.commit()
        raise
