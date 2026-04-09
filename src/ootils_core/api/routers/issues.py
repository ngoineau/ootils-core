"""
GET /v1/issues — Get active shortages and planning issues.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.engine.kernel.shortage.detector import ShortageDetector

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/issues", tags=["issues"])

_LOW_MAX = Decimal("100")
_MEDIUM_MAX = Decimal("1000")


def _classify_severity(score: Decimal) -> str:
    if score < _LOW_MAX:
        return "low"
    elif score <= _MEDIUM_MAX:
        return "medium"
    else:
        return "high"


class IssueRecord(BaseModel):
    node_id: UUID
    item_id: Optional[UUID]
    location_id: Optional[UUID]
    shortage_qty: Decimal
    severity_score: Decimal
    severity: str
    shortage_date: Optional[date]
    explanation_id: Optional[UUID]
    explanation_url: Optional[str]


class IssuesResponse(BaseModel):
    issues: list[IssueRecord]
    total: int
    limit: int
    offset: int
    as_of: str


@router.get("", response_model=IssuesResponse)
async def get_issues(
    severity: str = Query(default="all", description="low / medium / high / all"),
    horizon_days: int = Query(default=90, description="Look-ahead window in days"),
    item_id: Optional[str] = Query(default=None, description="Filter by item ID"),
    location_id: Optional[str] = Query(default=None, description="Filter by location ID"),
    limit: int = Query(default=200, ge=1, le=1000, description="Max results to return (1–1000)"),
    offset: int = Query(default=0, ge=0, description="Result offset for pagination"),
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> IssuesResponse:
    """Return active shortages filtered by severity, horizon, item, and location with pagination."""
    from datetime import datetime, timezone

    detector = ShortageDetector()
    all_shortages = detector.get_active_shortages(scenario_id, db)

    today = date.today()
    horizon_cutoff = today + timedelta(days=horizon_days)

    # Parse optional UUID filter params once
    item_uuid: Optional[UUID] = None
    if item_id is not None:
        try:
            item_uuid = UUID(item_id)
        except ValueError:
            item_uuid = None

    loc_uuid: Optional[UUID] = None
    if location_id is not None:
        try:
            loc_uuid = UUID(location_id)
        except ValueError:
            loc_uuid = None

    filtered: list[IssueRecord] = []

    for s in all_shortages:
        # Horizon filter
        if s.shortage_date is not None and s.shortage_date > horizon_cutoff:
            continue

        # Item filter
        if item_uuid is not None and s.item_id != item_uuid:
            continue

        # Location filter
        if loc_uuid is not None and s.location_id != loc_uuid:
            continue

        # Severity classification + filter
        sev = _classify_severity(s.severity_score)
        if severity != "all" and sev != severity:
            continue

        exp_url = (
            f"/v1/explain?node_id={s.pi_node_id}&scenario_id={scenario_id}"
            if s.explanation_id
            else None
        )

        filtered.append(
            IssueRecord(
                node_id=s.pi_node_id,
                item_id=s.item_id,
                location_id=s.location_id,
                shortage_qty=s.shortage_qty,
                severity_score=s.severity_score,
                severity=sev,
                shortage_date=s.shortage_date,
                explanation_id=s.explanation_id,
                explanation_url=exp_url,
            )
        )

    total = len(filtered)
    page = filtered[offset : offset + limit]

    as_of = datetime.now(timezone.utc).isoformat()
    logger.info(
        "issues.fetched scenario=%s total=%d returned=%d severity=%s horizon=%d limit=%d offset=%d",
        scenario_id,
        total,
        len(page),
        severity,
        horizon_days,
        limit,
        offset,
    )

    return IssuesResponse(
        issues=page,
        total=total,
        limit=limit,
        offset=offset,
        as_of=as_of,
    )
