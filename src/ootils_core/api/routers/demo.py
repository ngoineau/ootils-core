"""Demo endpoints for live product proof flows."""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from ootils_core.api.auth import require_auth
from ootils_core.demo.phase1 import run_phase1_demo_from_env

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/demo", tags=["demo"])


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


@router.post(
    "/phase1/run",
    summary="Run Phase 1 planning demo",
    description="Run the live Forecast -> MPS -> Approve -> MRP -> CRP -> ATP demo flow with unique seeded demo data.",
)
async def run_phase1_demo_endpoint(token: str = Depends(require_auth)) -> dict:
    """Run the executable Phase 1 demo flow."""
    try:
        return _json_safe(run_phase1_demo_from_env())
    except Exception as exc:  # pragma: no cover - tested through integration/demo gates
        logger.exception("phase1 demo failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Phase 1 demo flow failed",
        ) from exc
