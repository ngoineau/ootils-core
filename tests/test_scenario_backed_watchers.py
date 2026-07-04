"""
tests/test_scenario_backed_watchers.py — PURE unit tests for chantier #340.

No DB, no mocks: the decision-ladder mapping (agent_governance.decision_level)
and the simulation-harness pure helpers (agent_simulation.build_expedite_override,
effective_confidence, simulation_evidence).

The failed-propagation path (d) is covered HERE at the pure-function level:
effective_confidence must demote a simulated candidate to NEEDS_DATA_REVIEW
when propagation_status != 'ok' — the integration battery cannot force a
deterministic propagation crash against a real Postgres without mocks.
"""
from __future__ import annotations

import datetime as _dt
import sys
from pathlib import Path

import pytest

# Import seam: the watcher fleet lives under scripts/ (outside the package).
_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import agent_simulation  # noqa: E402
from agent_governance import decision_level  # noqa: E402


# ---------------------------------------------------------------------------
# decision_level — ONE deterministic mapping for the whole fleet
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("action,expected", [
    # new-order drafts (shortage watcher)
    ("ORDER_NOW", "L1"),
    ("ORDER_RUSH", "L1"),
    # expedite of an EXISTING order (shortage + material watchers)
    ("EXPEDITE", "L2"),
    # parameter proposals (lot policy watcher)
    ("RENEGOTIATE_MOQ", "L1"),
    ("REVIEW_MULTIPLE", "L1"),
    ("SET_LOT_RULE", "L1"),
    # E&O dispositions
    ("STOP_BUY", "L1"),
    ("REVIEW", "L1"),
    ("HOLD", "L1"),
])
def test_decision_level_mapping(action, expected):
    assert decision_level(action) == expected


def test_decision_level_unknown_action_fails_loudly():
    with pytest.raises(ValueError, match="unknown watcher action"):
        decision_level("PUSH_TO_ERP")
    with pytest.raises(ValueError):
        decision_level("")


# ---------------------------------------------------------------------------
# build_expedite_override — earliest receipt strictly AFTER the need date
# ---------------------------------------------------------------------------

_D = _dt.date


def test_build_expedite_override_picks_earliest_future_receipt():
    receipts = {"item-1": [(_D(2026, 7, 10), "n-early"), (_D(2026, 8, 1), "n-late")]}
    ov = agent_simulation.build_expedite_override(receipts, "item-1", _D(2026, 7, 5))
    assert ov == {
        "node_id": "n-early",
        "field_name": "time_ref",
        "new_value": "2026-07-05",
        "receipt_time_ref": "2026-07-10",
    }


def test_build_expedite_override_skips_receipts_not_after_need_date():
    # A receipt ON the need date brings nothing forward — must pick the later one.
    receipts = {"item-1": [(_D(2026, 7, 5), "n-same"), (_D(2026, 7, 20), "n-after")]}
    ov = agent_simulation.build_expedite_override(receipts, "item-1", _D(2026, 7, 5))
    assert ov["node_id"] == "n-after"


def test_build_expedite_override_none_when_nothing_advanceable():
    receipts = {"item-1": [(_D(2026, 7, 1), "n-past")]}
    assert agent_simulation.build_expedite_override(receipts, "item-1", _D(2026, 7, 5)) is None
    assert agent_simulation.build_expedite_override({}, "item-x", _D(2026, 7, 5)) is None


# ---------------------------------------------------------------------------
# effective_confidence — fail-loudly demotion (the (d) failed path, pure level)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["failed", "skipped", None])
def test_simulated_candidate_without_delta_is_needs_data_review(status):
    assert agent_simulation.effective_confidence("HIGH", True, status) == "NEEDS_DATA_REVIEW"


def test_simulated_candidate_with_ok_propagation_keeps_confidence():
    assert agent_simulation.effective_confidence("HIGH", True, "ok") == "HIGH"


def test_non_simulated_candidate_keeps_confidence_even_on_failure():
    # A non-simulated candidate never claimed a counter-factual — no demotion.
    assert agent_simulation.effective_confidence("MEDIUM", False, "failed") == "MEDIUM"
    assert agent_simulation.effective_confidence("LOW", False, None) == "LOW"


# ---------------------------------------------------------------------------
# simulation_evidence — the #340 evidence contract
# ---------------------------------------------------------------------------

def test_simulation_evidence_simulated_carries_fork_and_delta():
    summary = {"scenario_id": "sid-123", "propagation_status": "ok"}
    result = {"simulated": True, "reason": None,
              "delta": {"new_shortages": 0, "resolved_shortages": 1, "net_change": -1},
              "override": {"node_id": "n1", "field_name": "time_ref", "new_value": "2026-07-05"}}
    ev = agent_simulation.simulation_evidence(summary, result)
    assert ev["simulation_scenario_id"] == "sid-123"
    assert ev["simulated"] is True
    assert ev["propagation_status"] == "ok"
    assert ev["delta"]["net_change"] == -1
    assert ev["override"]["field_name"] == "time_ref"
    assert "not_simulated_reason" not in ev


def test_simulation_evidence_not_simulated_carries_marker_and_no_delta():
    summary = {"scenario_id": None, "propagation_status": None}
    result = {"simulated": False, "reason": agent_simulation.NOT_SIMULABLE_NEW_ORDER,
              "delta": None, "override": None}
    ev = agent_simulation.simulation_evidence(summary, result)
    assert ev["simulated"] is False
    assert ev["delta"] is None
    assert ev["not_simulated_reason"] == agent_simulation.NOT_SIMULABLE_NEW_ORDER


def test_simulation_evidence_failed_propagation_has_no_fabricated_delta():
    summary = {"scenario_id": "sid-456", "propagation_status": "failed"}
    result = {"simulated": True, "reason": None, "delta": None,
              "override": {"node_id": "n1", "field_name": "time_ref", "new_value": "2026-07-05"}}
    ev = agent_simulation.simulation_evidence(summary, result)
    assert ev["simulated"] is True
    assert ev["propagation_status"] == "failed"
    assert ev["delta"] is None
