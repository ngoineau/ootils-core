"""
tests/test_agent_tools.py — Unit tests for ootils_core.tools.agent_tools.

The module exposes three functions for LLM agents to drive the planning engine.
Before this file, the module was at 0% coverage — its only tests lived under
tests/legacy/ which targets a removed SupplyChainTools class.

These tests mock the underlying detector / manager / propagation engine to
isolate the tool wrappers. They do not touch a real DB.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

from ootils_core.models import Scenario, ShortageRecord
from ootils_core.tools.agent_tools import (
    get_active_issues,
    simulate_override,
    trigger_recalculation,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_shortage(
    *,
    pi_node_id: UUID | None = None,
    item_id: UUID | None = None,
    location_id: UUID | None = None,
    shortage_qty: Decimal = Decimal("10"),
    severity_score: Decimal = Decimal("100"),
    shortage_date: date | None = None,
) -> ShortageRecord:
    return ShortageRecord(
        shortage_id=uuid4(),
        scenario_id=uuid4(),
        pi_node_id=pi_node_id or uuid4(),
        item_id=item_id,
        location_id=location_id,
        shortage_date=shortage_date or date(2026, 6, 1),
        shortage_qty=shortage_qty,
        severity_score=severity_score,
        explanation_id=None,
        calc_run_id=uuid4(),
    )


# ---------------------------------------------------------------------------
# get_active_issues
# ---------------------------------------------------------------------------


def test_get_active_issues_returns_serialized_dicts():
    item_id = uuid4()
    location_id = uuid4()
    node_id = uuid4()
    shortage = _make_shortage(
        pi_node_id=node_id,
        item_id=item_id,
        location_id=location_id,
        shortage_qty=Decimal("42.5"),
        severity_score=Decimal("1234.5"),
    )

    detector_mock = MagicMock()
    detector_mock.get_active_shortages.return_value = [shortage]

    with patch("ootils_core.engine.kernel.shortage.detector.ShortageDetector", return_value=detector_mock):
        result = get_active_issues(db=MagicMock(), scenario_id=str(uuid4()))

    assert result == [
        {
            "node_id": str(node_id),
            "item_id": str(item_id),
            "location_id": str(location_id),
            "shortage_qty": 42.5,
            "severity_score": 1234.5,
            "shortage_date": "2026-06-01",
        }
    ]


def test_get_active_issues_uses_baseline_scenario_by_default():
    detector_mock = MagicMock()
    detector_mock.get_active_shortages.return_value = []
    db = MagicMock()

    with patch("ootils_core.engine.kernel.shortage.detector.ShortageDetector", return_value=detector_mock):
        get_active_issues(db=db)

    detector_mock.get_active_shortages.assert_called_once()
    scenario_arg = detector_mock.get_active_shortages.call_args.args[0]
    assert scenario_arg == UUID("00000000-0000-0000-0000-000000000001")


def test_get_active_issues_handles_missing_item_and_location():
    shortage = _make_shortage(item_id=None, location_id=None)
    detector_mock = MagicMock()
    detector_mock.get_active_shortages.return_value = [shortage]

    with patch("ootils_core.engine.kernel.shortage.detector.ShortageDetector", return_value=detector_mock):
        result = get_active_issues(db=MagicMock(), scenario_id=str(uuid4()))

    assert result[0]["item_id"] is None
    assert result[0]["location_id"] is None


def test_get_active_issues_passes_db_to_detector():
    detector_mock = MagicMock()
    detector_mock.get_active_shortages.return_value = []
    db = MagicMock()

    with patch("ootils_core.engine.kernel.shortage.detector.ShortageDetector", return_value=detector_mock):
        get_active_issues(db=db, scenario_id=str(uuid4()))

    detector_mock.get_active_shortages.assert_called_once()
    assert detector_mock.get_active_shortages.call_args.args[1] is db


# ---------------------------------------------------------------------------
# simulate_override
# ---------------------------------------------------------------------------


def test_simulate_override_creates_scenario_and_applies_override():
    new_scenario_id = uuid4()
    parent_scenario_id = UUID("00000000-0000-0000-0000-000000000001")
    target_node_id = uuid4()

    scenario = Scenario(
        scenario_id=new_scenario_id,
        name="agent-sim-abcd1234",
        parent_scenario_id=parent_scenario_id,
        is_baseline=False,
        status="active",
        created_at=datetime.now(timezone.utc),
    )

    manager_mock = MagicMock()
    manager_mock.create_scenario.return_value = scenario

    with patch("ootils_core.engine.scenario.manager.ScenarioManager", return_value=manager_mock):
        result = simulate_override(
            db=MagicMock(),
            node_id=str(target_node_id),
            field_name="time_ref",
            new_value="2026-07-01",
        )

    manager_mock.create_scenario.assert_called_once()
    manager_mock.apply_override.assert_called_once()
    override_kwargs = manager_mock.apply_override.call_args.kwargs
    assert override_kwargs["scenario_id"] == new_scenario_id
    assert override_kwargs["node_id"] == target_node_id
    assert override_kwargs["field_name"] == "time_ref"
    assert override_kwargs["new_value"] == "2026-07-01"
    assert override_kwargs["applied_by"] == "agent"

    assert result == {
        "scenario_id": str(new_scenario_id),
        "scenario_name": "agent-sim-abcd1234",
        "status": "created",
        "override_applied": True,
    }


def test_simulate_override_uses_baseline_as_default_parent():
    scenario = Scenario(
        scenario_id=uuid4(),
        name="agent-sim-xx",
        parent_scenario_id=UUID("00000000-0000-0000-0000-000000000001"),
        is_baseline=False,
        status="active",
        created_at=datetime.now(timezone.utc),
    )
    manager_mock = MagicMock()
    manager_mock.create_scenario.return_value = scenario

    with patch("ootils_core.engine.scenario.manager.ScenarioManager", return_value=manager_mock):
        simulate_override(
            db=MagicMock(),
            node_id=str(uuid4()),
            field_name="quantity",
            new_value="100",
        )

    parent = manager_mock.create_scenario.call_args.kwargs["parent_scenario_id"]
    assert parent == UUID("00000000-0000-0000-0000-000000000001")


# ---------------------------------------------------------------------------
# trigger_recalculation
# ---------------------------------------------------------------------------


def test_trigger_recalculation_returns_locked_when_calc_run_in_progress():
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = []

    calc_run_mgr_mock = MagicMock()
    calc_run_mgr_mock.start_calc_run.return_value = None  # lock held
    engine_mock = MagicMock()

    with patch("ootils_core.api.routers.events._build_propagation_engine", return_value=engine_mock), \
         patch("ootils_core.engine.orchestration.calc_run.CalcRunManager", return_value=calc_run_mgr_mock), \
         patch("ootils_core.engine.kernel.graph.dirty.DirtyFlagManager"):
        result = trigger_recalculation(db=db, scenario_id=str(uuid4()))

    assert result == {"status": "locked", "nodes_recalculated": 0}


def test_trigger_recalculation_completes_and_returns_run_id():
    scenario_id = uuid4()
    calc_run_id = uuid4()
    node_id = uuid4()

    calc_run = MagicMock()
    calc_run.calc_run_id = calc_run_id
    calc_run.nodes_recalculated = 7

    calc_run_mgr_mock = MagicMock()
    calc_run_mgr_mock.start_calc_run.return_value = calc_run

    dirty_mgr_mock = MagicMock()
    engine_mock = MagicMock()

    db = MagicMock()
    # First call: INSERT INTO events (no return value used)
    # Second call: SELECT node_id FROM nodes ... → fetchall returns one row
    db.execute.return_value.fetchall.return_value = [{"node_id": str(node_id)}]

    with patch("ootils_core.api.routers.events._build_propagation_engine", return_value=engine_mock), \
         patch("ootils_core.engine.orchestration.calc_run.CalcRunManager", return_value=calc_run_mgr_mock), \
         patch("ootils_core.engine.kernel.graph.dirty.DirtyFlagManager", return_value=dirty_mgr_mock):
        result = trigger_recalculation(db=db, scenario_id=str(scenario_id))

    assert result == {
        "status": "completed",
        "calc_run_id": str(calc_run_id),
        "nodes_recalculated": 7,
    }
    dirty_mgr_mock.mark_dirty.assert_called_once()
    dirty_mgr_mock.flush_to_postgres.assert_called_once()
    engine_mock._propagate.assert_called_once()
    engine_mock._finish_run.assert_called_once_with(calc_run, scenario_id, db)
