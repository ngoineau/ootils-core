"""
Unit tests for the governed reschedule EMISSION layer (#346 PR-B). DB-free.

Three units are exercised, each pure and deterministic:

  * agent_governance.decision_level  — the fleet-wide action->Decision-Ladder
    mapping for the four reschedule actions (RESCHEDULE_IN/OUT/DEFER = L2,
    CANCEL = L3, unknown => ValueError, per the existing fail-loudly contract).
  * engine.recommendation.reschedule.reschedule_recommendation_id — the
    deterministic uuid5 idempotence key: STABLE for an unchanged signal,
    CHANGES when the proposed need date moves (a moved need = a new message,
    not a mutation), CANCEL (proposed_date None) stable-and-distinct, distinct
    per node.
  * engine.recommendation.reschedule.build_recommendation — the signal->row
    mapping: action/item/target/dates carried verbatim, decision_level taken
    from the mapping (never hardcoded), evidence carries the signal detail, and
    the NOT-NULL migration-039 columns (shortage_date / deficit_qty ==
    recommended_qty == qty) filled per the RESCHEDULE-vs-CANCEL convention.

No database, no mocks: the RescheduleSignal fields are built in memory. The
core signal-generation logic (reschedule_signals) is PR-A's concern and is
covered by tests/test_reschedule_signals.py — not re-tested here.

decision_level lives under scripts/ (outside the package), so scripts/ is put
on sys.path exactly as the integration harness does.
"""
from __future__ import annotations

import datetime as dt
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

# Import seam: agent_governance lives under scripts/ (outside the package),
# mirroring tests/integration/test_agent_fleet_smoke.py.
_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from agent_governance import decision_level  # noqa: E402

from ootils_core.engine.recommendation.reschedule import (  # noqa: E402
    RESCHEDULE_ACTIONS,
    build_recommendation,
    reschedule_recommendation_id,
)

# A stable, non-baseline scenario id used across the identity tests.
SCEN = "11111111-1111-1111-1111-111111111111"
NODE_A = "aaaaaaaa-0000-0000-0000-000000000001"
NODE_B = "bbbbbbbb-0000-0000-0000-000000000002"
PROP = dt.date(2026, 11, 21)
PROP2 = dt.date(2026, 12, 5)
CUR = dt.date(2026, 8, 1)


# ---------------------------------------------------------------------------
# 1. decision_level — the reschedule actions on the Decision Ladder.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "action,expected",
    [
        ("RESCHEDULE_IN", "L2"),   # re-date an existing order: reversible move
        ("RESCHEDULE_OUT", "L2"),
        ("DEFER", "L2"),
        ("CANCEL", "L3"),          # release an engaged order: irreversible on the vendor side
    ],
)
def test_decision_level_for_reschedule_actions(action, expected):
    assert decision_level(action) == expected


def test_cancel_is_the_only_reschedule_l3():
    """CANCEL is the single L3 among the reschedule actions — the first
    watcher-emitted L3 (the human gate lives in the state machine, not here)."""
    levels = {a: decision_level(a) for a in ("RESCHEDULE_IN", "RESCHEDULE_OUT", "DEFER", "CANCEL")}
    assert levels["CANCEL"] == "L3"
    assert [a for a, lvl in levels.items() if lvl == "L3"] == ["CANCEL"]


def test_decision_level_unknown_action_raises():
    """Fail-loudly: an action outside the mapping raises ValueError (no silent
    default level would misclassify governance risk) — matches the existing
    contract the other watchers rely on."""
    with pytest.raises(ValueError):
        decision_level("TELEPORT_ORDER")


def test_reschedule_actions_constant_matches_the_mapping():
    """Every action the emitter advertises in RESCHEDULE_ACTIONS must resolve to
    a level (no advertised-but-unmapped action)."""
    for action in RESCHEDULE_ACTIONS:
        assert decision_level(action) in {"L2", "L3"}


# ---------------------------------------------------------------------------
# 2. reschedule_recommendation_id — deterministic uuid5 idempotence key.
# ---------------------------------------------------------------------------


def test_id_is_stable_for_identical_signal():
    """Same (scenario, node, action, proposed_date) => same uuid on every call.
    This is the idempotence key: a re-emitted identical signal upserts to a
    no-op."""
    a = reschedule_recommendation_id(SCEN, NODE_A, "RESCHEDULE_OUT", PROP)
    b = reschedule_recommendation_id(SCEN, NODE_A, "RESCHEDULE_OUT", PROP)
    assert a == b
    assert isinstance(a, uuid.UUID)


def test_id_changes_when_proposed_date_moves():
    """A need that moves is a genuinely NEW message (different proposed_date =>
    different id => a new DRAFT row), not a silent mutation of the prior one."""
    a = reschedule_recommendation_id(SCEN, NODE_A, "RESCHEDULE_OUT", PROP)
    b = reschedule_recommendation_id(SCEN, NODE_A, "RESCHEDULE_OUT", PROP2)
    assert a != b


def test_cancel_id_is_stable_and_distinct_from_reschedule():
    """CANCEL has proposed_date None: its identity (scenario, node, 'CANCEL',
    None) is stable across calls and never collides with a RESCHEDULE id for
    the same node."""
    c1 = reschedule_recommendation_id(SCEN, NODE_A, "CANCEL", None)
    c2 = reschedule_recommendation_id(SCEN, NODE_A, "CANCEL", None)
    assert c1 == c2
    resched = reschedule_recommendation_id(SCEN, NODE_A, "RESCHEDULE_OUT", PROP)
    assert c1 != resched


def test_id_differs_per_node():
    """Two different target nodes => two different ids, even for the same action
    and proposed date."""
    a = reschedule_recommendation_id(SCEN, NODE_A, "RESCHEDULE_OUT", PROP)
    b = reschedule_recommendation_id(SCEN, NODE_B, "RESCHEDULE_OUT", PROP)
    assert a != b


def test_id_differs_per_action():
    """RESCHEDULE_IN and RESCHEDULE_OUT on the same node/date are distinct
    messages (the action is part of the identity)."""
    a = reschedule_recommendation_id(SCEN, NODE_A, "RESCHEDULE_IN", PROP)
    b = reschedule_recommendation_id(SCEN, NODE_A, "RESCHEDULE_OUT", PROP)
    assert a != b


def test_id_differs_per_scenario():
    """The same signal in two scenarios (baseline vs a fork) gets two ids, so
    baseline and a fork never collide in the recommendations table."""
    other = "22222222-2222-2222-2222-222222222222"
    a = reschedule_recommendation_id(SCEN, NODE_A, "RESCHEDULE_OUT", PROP)
    b = reschedule_recommendation_id(other, NODE_A, "RESCHEDULE_OUT", PROP)
    assert a != b


# ---------------------------------------------------------------------------
# 3. build_recommendation — signal -> governed row mapping.
# ---------------------------------------------------------------------------


def _build(action, proposed_date, *, qty=100.0, node_id=NODE_A, node_type="PurchaseOrderSupply",
           is_firm=True):
    """Build a recommendation the way the watcher does: decision_level resolved
    from the action by the shared mapping (never hardcoded)."""
    return build_recommendation(
        scenario_id=SCEN,
        item_external_id="ITM-1",
        action=action,
        decision_level=decision_level(action),
        node_id=node_id,
        item_id="item-uuid-1",
        current_receipt_date=CUR,
        proposed_date=proposed_date,
        qty=qty,
        node_type=node_type,
        is_firm=is_firm,
    )


def test_build_maps_reschedule_out_fields():
    r = _build("RESCHEDULE_OUT", PROP)
    assert r.action == "RESCHEDULE_OUT"
    assert r.item_id == "item-uuid-1"
    assert r.item_external_id == "ITM-1"
    assert r.target_node_id == NODE_A          # target_node_id == node_id
    assert r.current_receipt_date == CUR
    assert r.proposed_date == PROP
    # decision_level came from the mapping, not a hardcode.
    assert r.decision_level == decision_level("RESCHEDULE_OUT") == "L2"
    # The deterministic id matches the standalone key builder.
    assert r.recommendation_id == reschedule_recommendation_id(SCEN, NODE_A, "RESCHEDULE_OUT", PROP)
    assert r.confidence == "HIGH"


def test_build_reschedule_notnull_columns_anchor_on_proposed_date():
    """Migration-039 NOT-NULL columns for a RESCHEDULE: shortage_date is the
    proposed need date (where the order SHOULD land); deficit_qty ==
    recommended_qty == the receipt qty (V1 re-dates the whole receipt)."""
    r = _build("RESCHEDULE_OUT", PROP, qty=100.0)
    assert r.shortage_date == PROP
    assert r.deficit_qty == Decimal("100.0")
    assert r.recommended_qty == Decimal("100.0")
    assert r.deficit_qty == r.recommended_qty


def test_build_cancel_notnull_columns_anchor_on_current_date():
    """A CANCEL has no proposed date, so the NOT-NULL shortage_date column is
    anchored on the current receipt date instead; proposed_date is None and the
    level is L3."""
    r = _build("CANCEL", None, qty=50.0)
    assert r.action == "CANCEL"
    assert r.proposed_date is None
    assert r.shortage_date == CUR              # anchored on current date, not proposed
    assert r.deficit_qty == Decimal("50.0") == r.recommended_qty
    assert r.decision_level == "L3"
    assert r.recommendation_id == reschedule_recommendation_id(SCEN, NODE_A, "CANCEL", None)


def test_build_evidence_carries_signal_detail():
    """Evidence is the forensic trail: qty, delta_days, node_type, is_firm, both
    dates, and the action — the signal IS the evidence (no fork)."""
    r = _build("RESCHEDULE_OUT", PROP, qty=100.0, node_type="PurchaseOrderSupply", is_firm=True)
    ev = r.evidence
    assert ev["signal"] == "RESCHEDULE_OUT"
    assert ev["qty"] == 100.0
    assert ev["node_type"] == "PurchaseOrderSupply"
    assert ev["is_firm"] is True
    assert ev["target_node_id"] == NODE_A
    assert ev["current_receipt_date"] == CUR.isoformat()
    assert ev["proposed_date"] == PROP.isoformat()
    # delta_days = proposed - current, in days.
    assert ev["delta_days"] == (PROP - CUR).days
    assert "rule" in ev


def test_build_cancel_evidence_has_null_proposed_and_delta():
    """For a CANCEL the evidence proposed_date and delta_days are null (no new
    date), while the current date and qty still describe the surplus receipt."""
    r = _build("CANCEL", None, qty=50.0)
    ev = r.evidence
    assert ev["signal"] == "CANCEL"
    assert ev["proposed_date"] is None
    assert ev["delta_days"] is None
    assert ev["current_receipt_date"] == CUR.isoformat()
    assert ev["qty"] == 50.0


def test_build_reschedule_in_delta_is_negative():
    """RESCHEDULE_IN pulls a receipt earlier: proposed < current => negative
    delta_days in the evidence."""
    earlier = dt.date(2026, 6, 1)
    r = _build("RESCHEDULE_IN", earlier)
    assert r.evidence["delta_days"] == (earlier - CUR).days
    assert r.evidence["delta_days"] < 0


def test_build_is_pure_and_deterministic():
    """Same inputs => identical row (same id, same evidence). Purity is the
    property the idempotent upsert leans on."""
    a = _build("RESCHEDULE_OUT", PROP)
    b = _build("RESCHEDULE_OUT", PROP)
    assert a == b
