"""
agent_tools.py — Tools for AI agents to interact with the Ootils planning engine.

These tools wrap the graph-based kernel API for use by LLM agents.

.. note::
    The old ``SupplyChainTools`` class (which wrapped the legacy
    ``decision_engine.py`` / ``policies.py`` API) has been replaced.
    Use the module-level functions below instead:

    - :func:`get_active_issues` — query active shortages for a scenario
    - :func:`simulate_override` — create a simulation scenario with one override
    - :func:`simulate_overrides` — one fork, N overrides, propagate, shortage delta
      (in-process equivalent of ``POST /v1/simulate``; used by the watcher fleet)
    - :func:`archive_scenario` — TTL-archive a simulation scenario (never DELETE)
    - :func:`trigger_recalculation` — force a full recompute for a scenario
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)



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


def _shortage_as_dict(s) -> dict:
    """Serialize a ShortageRecord into the delta-entry shape of POST /v1/simulate."""
    return {
        "node_id": str(s.pi_node_id),
        "item_id": str(s.item_id) if s.item_id else None,
        "location_id": str(s.location_id) if s.location_id else None,
        "shortage_date": str(s.shortage_date) if s.shortage_date else None,
        "shortage_qty": float(s.shortage_qty),
        "severity_score": float(s.severity_score),
    }


def simulate_overrides(
    db,
    overrides: list[dict],
    scenario_name: str = "agent-sim",
    base_scenario_id: str = "00000000-0000-0000-0000-000000000001",
) -> dict:
    """Create ONE simulation scenario, apply N overrides, recompute, return the delta.

    In-process equivalent of ``POST /v1/simulate`` (same fork -> overrides ->
    propagate -> shortage-delta pipeline, same #339 contract: the fork is
    transactional, the recompute is best-effort and its outcome is surfaced
    via ``propagation_status`` / ``delta_computed`` — a failed recompute never
    masquerades as an empty delta). Built for callers holding a direct DB
    connection (the watcher fleet) instead of an HTTP client.

    Connection contract: ``db`` must be a DEDICATED psycopg3 connection opened
    with ``row_factory=dict_row`` (the engine kernels access rows by column
    name), NOT a connection with an in-flight caller transaction. This
    function OWNS the transaction and COMMITS twice: once after fork+overrides
    (the scenario must survive a failed recompute) and once at the end.

    Args:
        db: dedicated psycopg3 dict_row connection (sync).
        overrides: list of ``{"node_id": str, "field_name": str, "new_value": str}``.
            node_ids may come from the BASE scenario — ScenarioManager resolves
            them to the fork's nodes by business key. field_name must be in the
            ScenarioManager whitelist (a failed override is recorded, not fatal).
        scenario_name: FULL name for the fork (the caller includes its own
            agent tag / run timestamp, e.g. ``what-if-shortage_watcher-<ts>``).
        base_scenario_id: UUID string of the scenario to branch from.

    Returns:
        A dict with ``scenario_id``, ``scenario_name``, ``override_count``,
        ``failed_overrides``, ``calc_run_id``, ``nodes_recalculated``,
        ``propagation_status`` ('ok' | 'failed' | 'skipped'),
        ``delta_computed`` and ``delta`` = ``{"new_shortages": [...],
        "resolved_shortages": [...], "net_shortage_change": int}`` (entries
        shaped like the /v1/simulate ShortageChange, ids as str).
    """
    from datetime import datetime, timezone
    from uuid import UUID as _UUID, uuid4

    from ootils_core.engine.scenario.manager import ScenarioManager

    base_id = _UUID(base_scenario_id)
    manager = ScenarioManager()
    scenario = manager.create_scenario(
        name=scenario_name, parent_scenario_id=base_id, db=db
    )

    applied = 0
    failed_overrides: list[dict] = []
    for ov in overrides:
        try:
            manager.apply_override(
                scenario_id=scenario.scenario_id,
                node_id=_UUID(str(ov["node_id"])),
                field_name=ov["field_name"],
                new_value=str(ov["new_value"]),
                applied_by="agent",
                db=db,
            )
            applied += 1
        except Exception:
            logger.warning(
                "simulate_overrides.override_failed node=%s field=%s",
                ov.get("node_id"), ov.get("field_name"), exc_info=True,
            )
            failed_overrides.append({
                "node_id": str(ov.get("node_id")),
                "field_name": ov.get("field_name"),
                "error": "Override failed validation",
            })
    # The fork + applied overrides must survive a failed recompute (#339).
    db.commit()

    result: dict = {
        "scenario_id": str(scenario.scenario_id),
        "scenario_name": scenario.name,
        "override_count": applied,
        "failed_overrides": failed_overrides,
        "calc_run_id": None,
        "nodes_recalculated": 0,
        "propagation_status": "skipped",
        "delta_computed": False,
        "delta": {"new_shortages": [], "resolved_shortages": [], "net_shortage_change": 0},
    }
    if applied == 0:
        return result

    from ootils_core.api.routers.events import _build_propagation_engine
    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
    from ootils_core.engine.kernel.shortage.detector import ShortageDetector
    from ootils_core.engine.orchestration.calc_run import CalcRunManager

    detector = ShortageDetector()
    calc_run_mgr = CalcRunManager()
    calc_run = None
    calc_run_finished = False
    try:
        baseline_shortages = {
            str(s.pi_node_id): s for s in detector.get_active_shortages(base_id, db)
        }

        trigger_event_id = uuid4()
        db.execute(
            "INSERT INTO events (event_id, event_type, scenario_id, processed, source, created_at) "
            "VALUES (%s, 'calc_triggered', %s, FALSE, 'agent', %s)",
            (trigger_event_id, scenario.scenario_id, datetime.now(timezone.utc)),
        )

        engine = _build_propagation_engine(db)
        calc_run = calc_run_mgr.start_calc_run(
            scenario_id=scenario.scenario_id, event_ids=[trigger_event_id], db=db
        )
        if calc_run is None:
            # Concurrent calc run holds the scenario advisory lock — 'skipped'.
            db.commit()
            return result

        all_pi = db.execute(
            "SELECT node_id FROM nodes "
            "WHERE scenario_id = %s AND node_type = 'ProjectedInventory' AND active = TRUE",
            (scenario.scenario_id,),
        ).fetchall()
        all_pi_ids = {_UUID(str(r["node_id"])) for r in all_pi}
        if all_pi_ids:
            dirty_mgr = DirtyFlagManager()
            dirty_mgr.mark_dirty(all_pi_ids, scenario.scenario_id, calc_run.calc_run_id, db)
            dirty_mgr.flush_to_postgres(calc_run.calc_run_id, scenario.scenario_id, db)
            engine._propagate(calc_run, all_pi_ids, db)
        engine._finish_run(calc_run, scenario.scenario_id, db)
        calc_run_finished = True
        result["calc_run_id"] = str(calc_run.calc_run_id)
        result["nodes_recalculated"] = calc_run.nodes_recalculated or 0

        scen_shortages = detector.get_active_shortages(scenario.scenario_id, db)
        scen_ids = {str(s.pi_node_id) for s in scen_shortages}
        base_ids = set(baseline_shortages)
        new_ids = scen_ids - base_ids
        resolved_ids = base_ids - scen_ids
        result["delta"] = {
            "new_shortages": [
                _shortage_as_dict(s) for s in scen_shortages if str(s.pi_node_id) in new_ids
            ],
            "resolved_shortages": [
                _shortage_as_dict(baseline_shortages[i]) for i in resolved_ids
            ],
            "net_shortage_change": len(new_ids) - len(resolved_ids),
        }
        result["propagation_status"] = "ok"
        result["delta_computed"] = True
        db.commit()
    except Exception:
        # Fail-loudly, #339 vocabulary: the scenario exists and is usable, the
        # recompute failed. Surface it as propagation_status='failed' +
        # delta_computed=False — never a fabricated (empty) delta.
        logger.exception(
            "simulate_overrides.propagation_failed scenario=%s", scenario.scenario_id
        )
        try:
            db.rollback()
        except Exception:
            logger.exception("simulate_overrides.rollback_failed scenario=%s", scenario.scenario_id)
        result["propagation_status"] = "failed"
        result["delta_computed"] = False
        result["delta"] = {"new_shortages": [], "resolved_shortages": [], "net_shortage_change": 0}
        # Release the scenario advisory lock (session-scoped — a rollback does
        # NOT release it) and persist the failure record best-effort.
        if calc_run is not None and not calc_run_finished:
            try:
                calc_run_mgr.fail_calc_run(calc_run, "simulate_overrides propagation failed", db)
                db.commit()
            except Exception:
                logger.exception(
                    "simulate_overrides.fail_calc_run_failed scenario=%s run=%s",
                    scenario.scenario_id, calc_run.calc_run_id,
                )
    return result


def archive_scenario(db, scenario_id: str) -> None:
    """Archive a (simulation) scenario — TTL pattern, never DELETE.

    Used by the watcher fleet at the end of a run to retire its what-if fork
    while keeping it queryable as evidence (``scenarios.status='archived'``).
    Same connection contract as :func:`simulate_overrides`: dedicated
    connection, this function COMMITS.
    """
    db.execute(
        "UPDATE scenarios SET status = 'archived', updated_at = now() WHERE scenario_id = %s",
        (scenario_id,),
    )
    db.commit()
    logger.info("scenario.archived scenario_id=%s", scenario_id)


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
