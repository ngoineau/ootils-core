"""
POST /v1/simulate — Create a scenario with overrides and return delta.
"""
from __future__ import annotations

import logging
from typing import Optional
from uuid import UUID

import psycopg
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, field_validator

from ootils_core.api.auth import require_auth
from ootils_core.api.dependencies import BASELINE_SCENARIO_ID, get_db
from ootils_core.engine.scenario.manager import ScenarioManager, _ALLOWED_FIELDS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/simulate", tags=["simulate"])


class OverrideIn(BaseModel):
    node_id: UUID
    field_name: str
    new_value: str

    @field_validator("field_name")
    @classmethod
    def validate_field_name(cls, v: str) -> str:
        if v not in _ALLOWED_FIELDS:
            raise ValueError(
                f"field_name {v!r} is not allowed. "
                f"Allowed fields: {sorted(_ALLOWED_FIELDS)}"
            )
        return v


class SimulateRequest(BaseModel):
    scenario_name: str
    base_scenario_id: Optional[str] = None
    overrides: list[OverrideIn] = []


class ShortageChange(BaseModel):
    node_id: UUID
    item_id: Optional[UUID] = None
    location_id: Optional[UUID] = None
    shortage_date: Optional[str] = None
    shortage_qty: Optional[float] = None
    severity_score: Optional[float] = None


class SimulateDelta(BaseModel):
    new_shortages: list[ShortageChange] = []
    resolved_shortages: list[ShortageChange] = []
    net_shortage_change: int = 0


class SimulateResponse(BaseModel):
    scenario_id: UUID
    scenario_name: str
    status: str
    override_count: int
    failed_overrides: list[dict] = []
    base_scenario_id: UUID
    calc_run_id: Optional[UUID] = None
    nodes_recalculated: int = 0
    delta: SimulateDelta = SimulateDelta()


@router.post("", response_model=SimulateResponse, status_code=status.HTTP_201_CREATED)
async def create_simulation(
    body: SimulateRequest,
    db: psycopg.Connection = Depends(get_db),
    _token: str = Depends(require_auth),
) -> SimulateResponse:
    """Create a new scenario with overrides and compute the delta vs base."""
    # Resolve base scenario
    if body.base_scenario_id and body.base_scenario_id.lower() != "baseline":
        try:
            base_id = UUID(body.base_scenario_id)
        except ValueError:
            base_id = BASELINE_SCENARIO_ID
    else:
        base_id = BASELINE_SCENARIO_ID

    manager = ScenarioManager()
    try:
        scenario = manager.create_scenario(
            name=body.scenario_name,
            parent_scenario_id=base_id,
            db=db,
        )
    except Exception as exc:
        logger.exception("simulate.create_scenario_failed name=%s", body.scenario_name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create scenario: {exc}",
        )

    # Apply overrides
    applied = 0
    failed_overrides: list[dict] = []
    for override in body.overrides:
        try:
            manager.apply_override(
                scenario_id=scenario.scenario_id,
                node_id=override.node_id,
                field_name=override.field_name,
                new_value=override.new_value,
                applied_by="api",
                db=db,
            )
            applied += 1
        except Exception as exc:
            logger.warning(
                "simulate.override_failed node=%s field=%s: %s",
                override.node_id,
                override.field_name,
                exc,
            )
            failed_overrides.append({
                "node_id": str(override.node_id),
                "field_name": override.field_name,
                "error": str(exc),
            })

    # If all overrides failed, return 422 with details instead of 500
    if body.overrides and applied == 0 and failed_overrides:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": "All overrides failed validation — no changes applied.",
                "failed_overrides": failed_overrides,
            },
        )

    logger.info(
        "simulate.created scenario=%s base=%s overrides=%d",
        scenario.scenario_id,
        base_id,
        applied,
    )

    # Propagation + delta computation
    from ootils_core.api.routers.events import _build_propagation_engine
    from ootils_core.engine.kernel.shortage.detector import ShortageDetector

    calc_run_id = None
    nodes_recalculated = 0
    delta = SimulateDelta()

    if applied > 0:
        try:
            # Get baseline shortages before propagation
            detector = ShortageDetector()
            baseline_shortages = {
                str(s.pi_node_id): s
                for s in detector.get_active_shortages(base_id, db)
            }

            # Create a trigger event for full recompute
            from uuid import uuid4
            from datetime import datetime, timezone
            trigger_event_id = uuid4()
            db.execute(
                """
                INSERT INTO events (event_id, event_type, scenario_id, processed, source, created_at)
                VALUES (%s, 'calc_triggered', %s, FALSE, 'api', %s)
                """,
                (trigger_event_id, scenario.scenario_id, datetime.now(timezone.utc)),
            )

            # Run propagation — full recompute for new scenario
            engine = _build_propagation_engine(db)

            from ootils_core.engine.orchestration.calc_run import CalcRunManager
            from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager

            calc_run_mgr = CalcRunManager()
            calc_run = calc_run_mgr.start_calc_run(
                scenario_id=scenario.scenario_id,
                event_ids=[trigger_event_id],
                db=db,
            )

            if calc_run is not None:
                # Mark ALL PI nodes as dirty for full recompute
                all_pi_nodes = db.execute(
                    """
                    SELECT node_id FROM nodes
                    WHERE scenario_id = %s AND node_type = 'ProjectedInventory' AND active = TRUE
                    """,
                    (scenario.scenario_id,),
                ).fetchall()
                all_pi_ids = {UUID(str(r["node_id"])) for r in all_pi_nodes}

                if all_pi_ids:
                    dirty_mgr = DirtyFlagManager()
                    dirty_mgr.mark_dirty(all_pi_ids, scenario.scenario_id, calc_run.calc_run_id, db)
                    dirty_mgr.flush_to_postgres(calc_run.calc_run_id, scenario.scenario_id, db)
                    engine._propagate(calc_run, all_pi_ids, db)

                engine._finish_run(calc_run, scenario.scenario_id, db)
                calc_run_id = calc_run.calc_run_id
                nodes_recalculated = calc_run.nodes_recalculated or 0

                # Compute delta
                new_shortages = detector.get_active_shortages(scenario.scenario_id, db)
                scenario_shortage_ids = {str(s.pi_node_id) for s in new_shortages}
                baseline_shortage_ids = set(baseline_shortages.keys())

                new_ids = scenario_shortage_ids - baseline_shortage_ids
                resolved_ids = baseline_shortage_ids - scenario_shortage_ids

                delta = SimulateDelta(
                    new_shortages=[
                        ShortageChange(
                            node_id=s.pi_node_id,
                            item_id=s.item_id,
                            location_id=s.location_id,
                            shortage_date=str(s.shortage_date) if s.shortage_date else None,
                            shortage_qty=float(s.shortage_qty),
                            severity_score=float(s.severity_score),
                        )
                        for s in new_shortages if str(s.pi_node_id) in new_ids
                    ],
                    resolved_shortages=[
                        ShortageChange(
                            node_id=baseline_shortages[sid].pi_node_id,
                            item_id=baseline_shortages[sid].item_id,
                            location_id=baseline_shortages[sid].location_id,
                            shortage_date=str(baseline_shortages[sid].shortage_date) if baseline_shortages[sid].shortage_date else None,
                            shortage_qty=float(baseline_shortages[sid].shortage_qty),
                            severity_score=float(baseline_shortages[sid].severity_score),
                        )
                        for sid in resolved_ids
                    ],
                    net_shortage_change=len(new_ids) - len(resolved_ids),
                )

        except Exception as exc:
            logger.warning("simulate.propagation_failed scenario=%s: %s", scenario.scenario_id, exc)
            # Don't fail the simulate call — scenario is created, propagation is best-effort

    return SimulateResponse(
        scenario_id=scenario.scenario_id,
        scenario_name=body.scenario_name,
        status="created",
        override_count=applied,
        failed_overrides=failed_overrides,
        base_scenario_id=base_id,
        calc_run_id=calc_run_id,
        nodes_recalculated=nodes_recalculated,
        delta=delta,
    )
