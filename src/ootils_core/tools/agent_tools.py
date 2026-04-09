"""
agent_tools.py — Tools for AI agents to interact with the Ootils planning engine.

These tools wrap the graph-based kernel API for use by LLM agents.

.. note::
    The old ``SupplyChainTools`` class (which wrapped the legacy
    ``decision_engine.py`` / ``policies.py`` API) has been replaced.
    Use the module-level functions below instead:

    - :func:`get_active_issues` — query active shortages for a scenario
    - :func:`simulate_override` — create a simulation scenario with one override
    - :func:`trigger_recalculation` — force a full recompute for a scenario
"""
from __future__ import annotations

from typing import Any
from uuid import UUID


def get_active_issues(db, scenario_id: str = "00000000-0000-0000-0000-000000000001") -> list[dict]:
    """Return active shortages for a scenario.

    Args:
        db: A psycopg3 connection (sync).
        scenario_id: UUID string of the scenario to query. Defaults to the
            baseline scenario.

    Returns:
        A list of shortage dicts with keys:
        ``node_id``, ``item_id``, ``location_id``, ``shortage_qty``,
        ``severity_score``, ``shortage_date``.
    """
    from ootils_core.engine.kernel.shortage.detector import ShortageDetector
    from uuid import UUID as _UUID

    detector = ShortageDetector()
    shortages = detector.get_active_shortages(_UUID(scenario_id), db)
    return [
        {
            "node_id": str(s.pi_node_id),
            "item_id": str(s.item_id) if s.item_id else None,
            "location_id": str(s.location_id) if s.location_id else None,
            "shortage_qty": float(s.shortage_qty),
            "severity_score": float(s.severity_score),
            "shortage_date": str(s.shortage_date) if s.shortage_date else None,
        }
        for s in shortages
    ]


def simulate_override(
    db,
    node_id: str,
    field_name: str,
    new_value: str,
    scenario_name: str = "agent-sim",
    base_scenario_id: str = "00000000-0000-0000-0000-000000000001",
) -> dict:
    """Create a simulation scenario with a single override and return delta.

    Args:
        db: A psycopg3 connection (sync).
        node_id: UUID string of the graph node to override.
        field_name: The node field to change (e.g. ``"expected_delivery_date"``).
        new_value: The new value as a string.
        scenario_name: Prefix for the new scenario name.
        base_scenario_id: UUID string of the scenario to branch from.

    Returns:
        A dict with ``scenario_id``, ``scenario_name``, ``status``, and
        ``override_applied`` keys.
    """
    from ootils_core.engine.scenario.manager import ScenarioManager
    from uuid import UUID as _UUID, uuid4

    manager = ScenarioManager()
    base_id = _UUID(base_scenario_id)

    scenario = manager.create_scenario(
        name=scenario_name + "-" + str(uuid4())[:8],
        parent_scenario_id=base_id,
        db=db,
    )
    manager.apply_override(
        scenario_id=scenario.scenario_id,
        node_id=_UUID(node_id),
        field_name=field_name,
        new_value=new_value,
        applied_by="agent",
        db=db,
    )
    return {
        "scenario_id": str(scenario.scenario_id),
        "scenario_name": scenario.name,
        "status": "created",
        "override_applied": True,
    }


def trigger_recalculation(db, scenario_id: str = "00000000-0000-0000-0000-000000000001") -> dict:
    """Trigger a full recompute for a scenario and return affected node count.

    Args:
        db: A psycopg3 connection (sync).
        scenario_id: UUID string of the scenario to recompute.

    Returns:
        A dict with ``status``, ``calc_run_id``, and ``nodes_recalculated``.
        If a calc run is already in progress, ``status`` will be ``"locked"``
        and ``nodes_recalculated`` will be ``0``.
    """
    from ootils_core.api.routers.events import _build_propagation_engine
    from ootils_core.engine.orchestration.calc_run import CalcRunManager
    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
    from uuid import UUID as _UUID, uuid4
    from datetime import datetime, timezone

    sid = _UUID(scenario_id)
    engine = _build_propagation_engine(db)

    trigger_event_id = uuid4()
    db.execute(
        "INSERT INTO events (event_id, event_type, scenario_id, processed, source, created_at) VALUES (%s, 'calc_triggered', %s, FALSE, 'agent', %s)",
        (trigger_event_id, sid, datetime.now(timezone.utc)),
    )

    calc_run_mgr = CalcRunManager()
    dirty_mgr = DirtyFlagManager()
    calc_run = calc_run_mgr.start_calc_run(scenario_id=sid, event_ids=[trigger_event_id], db=db)

    if calc_run is None:
        return {"status": "locked", "nodes_recalculated": 0}

    all_pi = db.execute(
        "SELECT node_id FROM nodes WHERE scenario_id = %s AND node_type = 'ProjectedInventory' AND active = TRUE",
        (sid,),
    ).fetchall()
    all_pi_ids = {_UUID(str(r["node_id"])) for r in all_pi}

    if all_pi_ids:
        dirty_mgr.mark_dirty(all_pi_ids, sid, calc_run.calc_run_id, db)
        dirty_mgr.flush_to_postgres(calc_run.calc_run_id, sid, db)
        engine._propagate(calc_run, all_pi_ids, db)

    engine._finish_run(calc_run, sid, db)

    return {
        "status": "completed",
        "calc_run_id": str(calc_run.calc_run_id),
        "nodes_recalculated": calc_run.nodes_recalculated or 0,
    }
