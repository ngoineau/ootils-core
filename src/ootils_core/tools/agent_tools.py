"""
agent_tools.py — Tools for AI agents to interact with the Ootils planning engine.

These tools wrap the graph-based kernel API for use by LLM agents.

.. note::
    The old ``SupplyChainTools`` class (which wrapped the legacy
    ``decision_engine.py`` / ``policies.py`` API) has been replaced.
    Use the module-level functions below instead:

    - :func:`get_active_issues` — query active shortages for a scenario
    - :func:`simulate_override` — create a simulation scenario with one override
    - :func:`simulate_overrides` — one fork, N node overrides, propagate, shortage
      delta (in-process equivalent of ``POST /v1/simulate``; used by the watcher
      fleet)
    - :func:`simulate_param_overrides` — one fork, N planning-param overlay
      overrides (chantier #347 PR4), same fork/propagate/delta core as
      :func:`simulate_overrides` via the shared :func:`_fork_propagate_delta`
      helper, used by the lot_policy watcher and callers of the param-overlay
      REST endpoints that want a counter-factual before proposing a change
    - :func:`archive_scenario` — TTL-archive a simulation scenario (never DELETE)
    - :func:`trigger_recalculation` — force a full recompute for a scenario
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID as _UUID
from uuid import uuid4

from ootils_core.db.types import DictRowConnection

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


def _empty_result(scenario_id: _UUID, scenario_name: str) -> dict:
    """The shared base shape returned by both simulate_* entry points before
    any propagation has run — factored so the two callers can't drift on the
    key set / defaults (calc_run_id, propagation_status='skipped', etc.)."""
    return {
        "scenario_id": str(scenario_id),
        "scenario_name": scenario_name,
        "override_count": 0,
        "failed_overrides": [],
        "calc_run_id": None,
        "nodes_recalculated": 0,
        "propagation_status": "skipped",
        "delta_computed": False,
        "delta": {"new_shortages": [], "resolved_shortages": [], "net_shortage_change": 0},
    }


def _fork_propagate_delta(
    db: DictRowConnection,
    scenario_id: _UUID,
    scenario_name: str,
    base_id: _UUID,
    result: dict,
) -> dict:
    """Shared fork->propagate->shortage-delta core, independent of the nature
    of the overrides already applied to ``scenario_id`` (node overrides for
    :func:`simulate_overrides`, planning-param overlay overrides for
    :func:`simulate_param_overrides`). Callers must have already created the
    fork, applied their overrides, and committed that work (the fork must
    survive a failed recompute — #339) before calling this.

    Mutates and returns ``result`` in place: fills in ``calc_run_id``,
    ``nodes_recalculated``, ``propagation_status`` ('ok' | 'failed' |
    'skipped'), ``delta_computed`` and ``delta``. Same fail-loudly contract
    as the original simulate_overrides: a failed recompute is surfaced via
    ``propagation_status='failed'`` + ``delta_computed=False``, never a
    fabricated (empty) delta.

    ``db`` must be the same dedicated dict_row connection the caller used to
    create the fork and apply overrides on — this function owns the
    transaction from here on (commits on success/skip, rolls back + records
    the calc_run failure on exception).
    """
    from ootils_core.api.routers.events import _build_propagation_engine
    from ootils_core.engine.kernel.graph.dirty import DirtyFlagManager
    from ootils_core.engine.kernel.shortage import match_shortage_delta
    from ootils_core.engine.kernel.shortage.detector import ShortageDetector
    from ootils_core.engine.orchestration.calc_run import CalcRunManager

    detector = ShortageDetector()
    calc_run_mgr = CalcRunManager()
    calc_run = None
    calc_run_finished = False
    try:
        baseline_shortages = detector.get_active_shortages(base_id, db)

        trigger_event_id = uuid4()
        # source='engine': the events CHECK (migration 002) allows
        # api/ingestion/engine/user/test — no 'agent' value. This in-process
        # path is engine-side tooling; the calling agent is attributed via
        # user_ref (scenario_name already carries what-if-<agent>-<ts>).
        db.execute(
            "INSERT INTO events (event_id, event_type, scenario_id, processed, source, user_ref, created_at) "
            "VALUES (%s, 'calc_triggered', %s, FALSE, 'engine', %s, %s)",
            (trigger_event_id, scenario_id, scenario_name, datetime.now(timezone.utc)),
        )

        engine = _build_propagation_engine(db)
        calc_run = calc_run_mgr.start_calc_run(
            scenario_id=scenario_id, event_ids=[trigger_event_id], db=db
        )
        if calc_run is None:
            # Concurrent calc run holds the scenario advisory lock — 'skipped'.
            db.commit()
            return result

        all_pi = db.execute(
            "SELECT node_id FROM nodes "
            "WHERE scenario_id = %s AND node_type = 'ProjectedInventory' AND active = TRUE",
            (scenario_id,),
        ).fetchall()
        all_pi_ids = {_UUID(str(r["node_id"])) for r in all_pi}
        if all_pi_ids:
            dirty_mgr = DirtyFlagManager()
            dirty_mgr.mark_dirty(all_pi_ids, scenario_id, calc_run.calc_run_id, db)
            dirty_mgr.flush_to_postgres(calc_run.calc_run_id, scenario_id, db)
            engine._propagate(calc_run, all_pi_ids, db)
        engine._finish_run(calc_run, scenario_id, db)
        calc_run_finished = True
        result["calc_run_id"] = str(calc_run.calc_run_id)
        result["nodes_recalculated"] = calc_run.nodes_recalculated or 0

        # Match fork vs baseline shortages by BUSINESS key
        # (item_id, location_id, shortage_date), NOT raw pi_node_id: the fork
        # deep-copies nodes with fresh UUIDs, so baseline and fork ids are
        # disjoint by construction (match_shortage_delta docstring). This makes
        # new/resolved honest for the watcher evidence trail (#340) — a fork
        # identical to the baseline yields new=[] and resolved=[].
        scen_shortages = detector.get_active_shortages(scenario_id, db)
        new_shortages, resolved_shortages = match_shortage_delta(
            baseline_shortages, scen_shortages
        )
        result["delta"] = {
            "new_shortages": [_shortage_as_dict(s) for s in new_shortages],
            "resolved_shortages": [_shortage_as_dict(s) for s in resolved_shortages],
            "net_shortage_change": len(scen_shortages) - len(baseline_shortages),
        }
        result["propagation_status"] = "ok"
        result["delta_computed"] = True
        db.commit()
    except Exception:
        # Fail-loudly, #339 vocabulary: the scenario exists and is usable, the
        # recompute failed. Surface it as propagation_status='failed' +
        # delta_computed=False — never a fabricated (empty) delta.
        logger.exception(
            "agent_tools.propagation_failed scenario=%s", scenario_id
        )
        try:
            db.rollback()
        except Exception:
            logger.exception("agent_tools.rollback_failed scenario=%s", scenario_id)
        result["propagation_status"] = "failed"
        result["delta_computed"] = False
        result["delta"] = {"new_shortages": [], "resolved_shortages": [], "net_shortage_change": 0}
        # Release the scenario advisory lock (session-scoped — a rollback does
        # NOT release it) and persist the failure record best-effort.
        if calc_run is not None and not calc_run_finished:
            try:
                calc_run_mgr.fail_calc_run(calc_run, "agent_tools propagation failed", db)
                db.commit()
            except Exception:
                logger.exception(
                    "agent_tools.fail_calc_run_failed scenario=%s run=%s",
                    scenario_id, calc_run.calc_run_id,
                )
    return result


def simulate_overrides(
    db: DictRowConnection,
    overrides: list[dict],
    scenario_name: str = "agent-sim",
    base_scenario_id: str = "00000000-0000-0000-0000-000000000001",
) -> dict:
    """Create ONE simulation scenario, apply N node overrides, recompute, return the delta.

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
    (the scenario must survive a failed recompute) and once at the end (inside
    :func:`_fork_propagate_delta`).

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

    result = _empty_result(scenario.scenario_id, scenario.name)
    result["override_count"] = applied
    result["failed_overrides"] = failed_overrides
    if applied == 0:
        return result

    return _fork_propagate_delta(db, scenario.scenario_id, scenario_name, base_id, result)


def simulate_param_overrides(
    db: DictRowConnection,
    param_overrides: list[dict],
    scenario_name: str = "agent-sim",
    base_scenario_id: str = "00000000-0000-0000-0000-000000000001",
    applied_by: str = "agent",
) -> dict:
    """Create ONE simulation scenario, apply N planning-param overlay overrides,
    recompute, return the delta (chantier #347 PR4).

    Same contract and shape as :func:`simulate_overrides` — dedicated
    connection, this function commits, fail-loudly propagation — except the
    per-override application step calls
    :func:`ootils_core.engine.scenario.param_overlay.set_param_override`
    instead of ``ScenarioManager.apply_override``. The fork->propagate->delta
    core is shared verbatim via :func:`_fork_propagate_delta` — it has no
    knowledge of what kind of override was applied.

    Args:
        db: dedicated psycopg3 dict_row connection (sync).
        param_overrides: list of ``{"item_id": str, "location_id": str|None,
            "field_name": str, "value": str}``. ``field_name`` must be in
            ``ALLOWED_PARAM_FIELDS`` (chantier #347 whitelist). A rejected
            override (whitelist miss, illegal value, orphaned target,
            baseline scenario) is recorded in ``failed_overrides`` with the
            ``ParamOverlayError`` message (UUID/field-only — no DSN, no raw
            psycopg leak, same carve-out as api/routers/staging.py) — never
            silently dropped and never fabricates a delta.
        scenario_name: FULL name for the fork.
        base_scenario_id: UUID string of the scenario to branch from.
        applied_by: attribution string persisted on every override row.

    Returns:
        Same shape as :func:`simulate_overrides`.
    """
    from ootils_core.engine.scenario.manager import ScenarioManager
    from ootils_core.engine.scenario.param_overlay import (
        ParamOverlayError,
        set_param_override,
    )

    base_id = _UUID(base_scenario_id)
    manager = ScenarioManager()
    scenario = manager.create_scenario(
        name=scenario_name, parent_scenario_id=base_id, db=db
    )

    applied = 0
    failed_overrides: list[dict] = []
    for ov in param_overrides:
        location_raw = ov.get("location_id")
        try:
            set_param_override(
                db,
                scenario_id=scenario.scenario_id,
                item_id=_UUID(str(ov["item_id"])),
                field_name=str(ov["field_name"]),
                value=str(ov["value"]),
                applied_by=applied_by,
                location_id=_UUID(str(location_raw)) if location_raw else None,
            )
            applied += 1
        except ParamOverlayError as exc:
            logger.warning(
                "simulate_param_overrides.override_failed item=%s field=%s: %s",
                ov.get("item_id"), ov.get("field_name"), exc,
            )
            failed_overrides.append({
                "item_id": str(ov.get("item_id")),
                "location_id": str(location_raw) if location_raw else None,
                "field_name": ov.get("field_name"),
                "error": str(exc),
            })
    # The fork + applied overrides must survive a failed recompute (#339).
    db.commit()

    result = _empty_result(scenario.scenario_id, scenario.name)
    result["override_count"] = applied
    result["failed_overrides"] = failed_overrides
    if applied == 0:
        return result

    return _fork_propagate_delta(db, scenario.scenario_id, scenario_name, base_id, result)


def archive_scenario(db, scenario_id: str) -> None:
    """Archive a (simulation) scenario — TTL pattern, never DELETE.

    Used by the watcher fleet at the end of a run to retire its what-if fork
    while keeping it queryable as evidence (``scenarios.status='archived'``).
    Same connection contract as :func:`simulate_overrides`: dedicated
    connection, this function COMMITS.
    """
    db.execute(
        """
        UPDATE scenarios
        SET status = 'archived', archived_at = now(), updated_at = now()
        WHERE scenario_id = %s
        """,
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
