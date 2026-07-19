"""
POST /v1/calc/run — Manually trigger propagation for a scenario.
"""
from __future__ import annotations

import logging
from uuid import UUID, uuid4
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/calc", tags=["calc"])


class CalcRunRequest(BaseModel):
    full_recompute: bool = False


class CalcRunResponse(BaseModel):
    calc_run_id: Optional[UUID] = None
    scenario_id: UUID
    status: str
    nodes_recalculated: int
    nodes_unchanged: int
    message: str


# governance AN-2 (#392, PR2a): scoped `calc:run`. Propagation is
# deterministic recalculation (ADR-003), not a decision — it re-derives
# ProjectedInventory/shortage state from events already committed to the graph,
# it introduces no new baseline facts, so no Decision Ladder human gate. But
# "deterministic != free": triggering a recompute is a costly run, so it sits
# behind the calc:run scope (an agent needs it explicitly; a read-only token
# cannot launch it).
@router.post("/run", response_model=CalcRunResponse)
def trigger_calc_run(
    body: CalcRunRequest,
    db: DictRowConnection = Depends(get_db),
    _principal: Principal = Depends(require_scope("calc:run")),
    scenario_id: UUID = Depends(resolve_scenario_id),
) -> CalcRunResponse:
    """Consume all unprocessed events for a scenario and run propagation."""
    from ootils_core.api.routers.events import _build_propagation_engine
    from ootils_core.engine.orchestration.calc_run import CalcRunManager
    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager

    # Create a trigger event
    trigger_event_id = uuid4()
    db.execute(
        """
        INSERT INTO events (event_id, event_type, scenario_id, processed, source, created_at)
        VALUES (%s, 'calc_triggered', %s, FALSE, 'api', %s)
        """,
        (trigger_event_id, scenario_id, datetime.now(timezone.utc)),
    )

    engine = _build_propagation_engine(db)

    if body.full_recompute:
        calc_run_mgr = CalcRunManager()
        calc_run = calc_run_mgr.start_calc_run(
            scenario_id=scenario_id,
            event_ids=[trigger_event_id],
            db=db,
        )
        if calc_run is None:
            return CalcRunResponse(
                calc_run_id=None,
                scenario_id=scenario_id,
                status="locked",
                nodes_recalculated=0,
                nodes_unchanged=0,
                message="Another calc run is in progress for this scenario",
            )

        dirty_mgr = DirtyFlagManager()
        all_pi = db.execute(
            "SELECT node_id FROM nodes WHERE scenario_id = %s AND node_type = 'ProjectedInventory' AND active = TRUE",
            (scenario_id,),
        ).fetchall()
        all_pi_ids = {UUID(str(r["node_id"])) for r in all_pi}

        if all_pi_ids:
            dirty_mgr.mark_dirty(all_pi_ids, scenario_id, calc_run.calc_run_id, db)
            dirty_mgr.flush_to_postgres(calc_run.calc_run_id, scenario_id, db)
            engine._propagate(calc_run, all_pi_ids, db)

        engine._finish_run(calc_run, scenario_id, db)
        return CalcRunResponse(
            calc_run_id=calc_run.calc_run_id,
            scenario_id=scenario_id,
            status="completed",
            nodes_recalculated=calc_run.nodes_recalculated or 0,
            nodes_unchanged=calc_run.nodes_unchanged or 0,
            message=f"Full recompute: {calc_run.nodes_recalculated or 0} nodes recalculated",
        )
    else:
        # Coalesce + propagate ALL pending events in ONE run (C3-PR2). The old
        # path handed the synthetic no-trigger `calc_triggered` event above to
        # process_event, which saw a NULL trigger_node_id and SKIPPED
        # propagation. process_pending instead coalesces every unprocessed event
        # (including that synthetic one, which contributes no series) and dirties
        # the WHOLE series of each real pending event's trigger node — ONE lock,
        # ONE calc_run, ONE ANALYZE. full_recompute=True keeps its inline path.
        calc_run = engine.process_pending(
            scenario_id=scenario_id,
            db=db,
        )
        if calc_run is None:
            return CalcRunResponse(
                calc_run_id=None,
                scenario_id=scenario_id,
                status="locked",
                nodes_recalculated=0,
                nodes_unchanged=0,
                message="Another calc run is in progress for this scenario",
            )
        return CalcRunResponse(
            calc_run_id=calc_run.calc_run_id,
            scenario_id=scenario_id,
            status="completed",
            nodes_recalculated=calc_run.nodes_recalculated or 0,
            nodes_unchanged=calc_run.nodes_unchanged or 0,
            message=f"{calc_run.nodes_recalculated or 0} nodes recalculated",
        )
