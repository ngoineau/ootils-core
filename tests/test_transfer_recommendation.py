"""
Unit tests for the governed DRP transfer EMISSION mapping (#395 PR2b). DB-free.

The direct sibling of tests/test_reschedule_recommendation.py, applied to the
distribution echelon. Three pure, deterministic units are exercised:

  * agent_governance.decision_level('TRANSFER') — the fleet-wide action ->
    Decision-Ladder mapping: a DRP transfer is a NEW-order draft (a physical
    relocation of finished stock, reversible until executed), so it is L1, the
    same class as an ORDER_NOW new-order draft.
  * engine.recommendation.transfer.transfer_recommendation_id — the
    deterministic uuid5 idempotence key: STABLE for an unchanged signal, and it
    CHANGES whenever any identity component moves (ship_date, item, source
    location, dest location, scenario) — a moved deficit is a genuinely NEW
    message, not a silent mutation of the prior one.
  * engine.recommendation.transfer.build_transfer_recommendation — the
    TransferSignal -> governed row mapping: action='TRANSFER', decision_level
    taken from the mapping (never hardcoded), the fair-share quantity/dates
    carried verbatim, the source/dest coordinate filled, the evidence carrying
    the fair-share forensic detail (the signal IS its own evidence, no fork),
    and byte-identical output for identical inputs (the property the idempotent
    ON CONFLICT upsert leans on).

No database, no mocks: the TransferSignal fields are built in memory. The core
signal-generation logic (drp_core.transfer_signals) is PR2a's concern and is
covered by tests/test_drp_core_golden.py — not re-tested here.

decision_level lives under scripts/ (outside the package), so scripts/ is put
on sys.path exactly as the integration harness and the reschedule unit tests do.
"""
from __future__ import annotations

import datetime as dt
import sys
import uuid
from decimal import Decimal
from pathlib import Path

# Import seam: agent_governance lives under scripts/ (outside the package),
# mirroring tests/test_reschedule_recommendation.py and the fleet smoke.
_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from agent_governance import decision_level  # noqa: E402

from ootils_core.engine.drp.core import TransferSignal  # noqa: E402
from ootils_core.engine.recommendation.transfer import (  # noqa: E402
    TRANSFER_ACTION,
    TRANSFER_DECISION_LEVEL,
    build_transfer_recommendation,
    transfer_recommendation_id,
)

# Stable, non-baseline coordinates reused across the identity tests. The
# planning coordinate is COALESCE(external_id, uuid::text) — a plain string in
# the DRP core, so these are the human-readable business keys here.
SCEN = "11111111-1111-1111-1111-111111111111"
ITEM = "VALVE-02"
SRC = "DC-ATL"
DST = "DC-LAX"
SHIP = dt.date(2026, 11, 21)
SHIP2 = dt.date(2026, 12, 5)

# Real UUIDs the caller resolves from the coordinate strings (the typed
# recommendations columns). Distinct from the coordinate strings, which live
# only in the recommendation_id + evidence.
HORIZON_START = dt.date(2026, 7, 6)
ITEM_UUID = uuid.UUID("a0000000-0000-0000-0000-000000000001")
SRC_UUID = uuid.UUID("b0000000-0000-0000-0000-000000000001")
DST_UUID = uuid.UUID("c0000000-0000-0000-0000-000000000001")


def _signal(
    *,
    item: str = ITEM,
    source: str = SRC,
    dest: str = DST,
    qty: float = 120.0,
    ship_bucket: int = 1,
    arrival_bucket: int = 2,
    deficit_bucket: int = 2,
    deficit_qty: float = 130.0,
    source_excess_before: float = 500.0,
    fair_share_qty: float = 120.0,
    rounding_remnant: float = 0.0,
) -> TransferSignal:
    """One TransferSignal built in memory the way drp_core.transfer_signals
    emits it. Defaults model a nominal single-lane draw (arrival at the deficit
    bucket, no down-round remnant); each test overrides only what it exercises."""
    return TransferSignal(
        item=item,
        source_location=source,
        dest_location=dest,
        qty=qty,
        ship_bucket=ship_bucket,
        arrival_bucket=arrival_bucket,
        deficit_bucket=deficit_bucket,
        deficit_qty=deficit_qty,
        source_excess_before=source_excess_before,
        fair_share_qty=fair_share_qty,
        rounding_remnant=rounding_remnant,
    )


# ---------------------------------------------------------------------------
# 1. decision_level — TRANSFER on the Decision Ladder.
# ---------------------------------------------------------------------------


def test_decision_level_transfer_is_l1():
    """A DRP transfer is a NEW-order draft (reversible until executed) => L1,
    the same class as ORDER_NOW. The mapping is the single fleet-wide source of
    truth (scripts/agent_governance.py), which itself SOURCES the literal from
    transfer.TRANSFER_DECISION_LEVEL — assert both agree."""
    assert decision_level("TRANSFER") == "L1"
    assert decision_level("TRANSFER") == TRANSFER_DECISION_LEVEL


def test_transfer_decision_level_constant_is_l1():
    """The engine module is the ONE place the TRANSFER literal 'L1' is written;
    the governance table re-uses it. Pin the constant so a change is a conscious
    Decision-Ladder edit, not a silent drift."""
    assert TRANSFER_DECISION_LEVEL == "L1"


# ---------------------------------------------------------------------------
# 2. transfer_recommendation_id — deterministic uuid5 idempotence key.
# ---------------------------------------------------------------------------


def test_id_is_stable_for_identical_inputs():
    """Same (scenario, item, source, dest, ship_date) => same uuid on every
    call. THIS is the idempotence key: a re-emitted identical signal upserts to
    a no-op (ON CONFLICT DO NOTHING)."""
    a = transfer_recommendation_id(SCEN, ITEM, SRC, DST, SHIP)
    b = transfer_recommendation_id(SCEN, ITEM, SRC, DST, SHIP)
    assert a == b
    assert isinstance(a, uuid.UUID)


def test_id_changes_when_ship_date_moves():
    """The ship_date participates in the identity on purpose: if the underlying
    deficit moves (e.g. a safety-stock overlay shifts the deficit bucket, which
    shifts the ship bucket), the signal is a genuinely NEW message (different
    ship_date => different id => a new DRAFT row), not a silent mutation."""
    a = transfer_recommendation_id(SCEN, ITEM, SRC, DST, SHIP)
    b = transfer_recommendation_id(SCEN, ITEM, SRC, DST, SHIP2)
    assert a != b


def test_id_changes_when_item_changes():
    a = transfer_recommendation_id(SCEN, ITEM, SRC, DST, SHIP)
    b = transfer_recommendation_id(SCEN, "PUMP-01", SRC, DST, SHIP)
    assert a != b


def test_id_changes_when_source_changes():
    """The SAME item's SAME deficit can be served from two different sources in
    two runs if the excess picture changes — those are two distinct transfer
    proposals (ship from A vs ship from B), each its own row."""
    a = transfer_recommendation_id(SCEN, ITEM, SRC, DST, SHIP)
    b = transfer_recommendation_id(SCEN, ITEM, "DC-CHI", DST, SHIP)
    assert a != b


def test_id_changes_when_dest_changes():
    a = transfer_recommendation_id(SCEN, ITEM, SRC, DST, SHIP)
    b = transfer_recommendation_id(SCEN, ITEM, SRC, "DC-NYC", SHIP)
    assert a != b


def test_id_changes_when_scenario_changes():
    """The same signal in two scenarios (baseline vs a fork) gets two ids, so
    baseline and a fork never collide in the recommendations table."""
    other = "22222222-2222-2222-2222-222222222222"
    a = transfer_recommendation_id(SCEN, ITEM, SRC, DST, SHIP)
    b = transfer_recommendation_id(other, ITEM, SRC, DST, SHIP)
    assert a != b


def test_id_distinct_for_two_different_coordinates():
    """Two entirely different transfer coordinates never collide onto one id
    (the identity is the full tuple, not a lossy hash of part of it)."""
    a = transfer_recommendation_id(SCEN, ITEM, SRC, DST, SHIP)
    b = transfer_recommendation_id(SCEN, "PUMP-01", "DC-CHI", "DC-NYC", SHIP2)
    assert a != b


# ---------------------------------------------------------------------------
# 3. build_transfer_recommendation — TransferSignal -> governed row mapping.
# ---------------------------------------------------------------------------


def _build(signal: TransferSignal, *, confidence: str = "HIGH") -> object:
    """Build a recommendation the way the emitter does: decision_level resolved
    from the action by the shared fleet mapping (never hardcoded here)."""
    return build_transfer_recommendation(
        signal=signal,
        scenario_id=SCEN,
        item_id=ITEM_UUID,
        item_external_id=ITEM,
        source_location_id=SRC_UUID,
        dest_location_id=DST_UUID,
        decision_level=decision_level("TRANSFER"),
        horizon_start=HORIZON_START,
        confidence=confidence,
    )


def test_build_maps_action_and_level():
    r = _build(_signal())
    assert r.action == "TRANSFER" == TRANSFER_ACTION
    # decision_level came from the mapping, not a hardcode.
    assert r.decision_level == decision_level("TRANSFER") == "L1"


def test_build_maps_ids_and_coordinate():
    """The typed UUID columns are the caller-resolved row identifiers; both the
    source and the destination location are filled (the coordinate an inter-site
    transfer carries and no other action does)."""
    r = _build(_signal())
    assert r.item_id == ITEM_UUID
    assert r.item_external_id == ITEM
    assert r.source_location_id == SRC_UUID
    assert r.dest_location_id == DST_UUID
    assert r.confidence == "HIGH"


def test_build_recommendation_id_matches_standalone_key():
    """The row's deterministic id is exactly transfer_recommendation_id over the
    signal's coordinate strings + the derived ship_date — the same key an
    upsert de-dupes on."""
    sig = _signal(ship_bucket=1)
    r = _build(sig)
    expected_ship = HORIZON_START + dt.timedelta(weeks=sig.ship_bucket)
    assert r.recommendation_id == transfer_recommendation_id(
        SCEN, sig.item, sig.source_location, sig.dest_location, expected_ship
    )


def test_build_dates_derive_from_horizon_and_buckets():
    """ship_date = horizon_start + ship_bucket weeks (-> proposed_date);
    deficit_date = horizon_start + deficit_bucket weeks (-> shortage_date).
    horizon_start is the DB-side CURRENT_DATE anchor; bucket N == +N*7 days."""
    sig = _signal(ship_bucket=1, deficit_bucket=2)
    r = _build(sig)
    assert r.proposed_date == HORIZON_START + dt.timedelta(weeks=1)
    assert r.shortage_date == HORIZON_START + dt.timedelta(weeks=2)


def test_build_maps_quantities_as_decimal():
    """recommended_qty == signal.qty (the fair-share + transfer_multiple
    DOWN-rounded quantity actually proposed to move); deficit_qty ==
    signal.deficit_qty (the projected shortfall at the destination). Both land
    on the NUMERIC columns as Decimal."""
    sig = _signal(qty=120.0, deficit_qty=130.0)
    r = _build(sig)
    assert r.recommended_qty == Decimal("120.0")
    assert r.deficit_qty == Decimal("130.0")
    assert isinstance(r.recommended_qty, Decimal)
    assert isinstance(r.deficit_qty, Decimal)


def test_build_evidence_carries_fair_share_detail():
    """Evidence is the forensic trail (explainability, ADR-004): the deficit it
    covers, the source excess that funded it, the fair-share/rounding numbers,
    and the ship/arrival timing — the signal IS its own evidence, no fork."""
    sig = _signal(
        qty=120.0,
        deficit_qty=130.0,
        source_excess_before=500.0,
        fair_share_qty=125.0,
        rounding_remnant=5.0,
        ship_bucket=1,
        arrival_bucket=2,
        deficit_bucket=2,
    )
    r = _build(sig)
    ev = r.evidence
    assert ev["signal"] == "TRANSFER"
    assert ev["item"] == ITEM
    assert ev["source_location"] == SRC
    assert ev["dest_location"] == DST
    assert ev["qty"] == 120.0
    assert ev["deficit_qty"] == 130.0
    assert ev["source_excess_before"] == 500.0
    assert ev["fair_share_qty"] == 125.0
    assert ev["rounding_remnant"] == 5.0
    assert ev["ship_bucket"] == 1
    assert ev["arrival_bucket"] == 2
    assert ev["deficit_bucket"] == 2
    # Dates echoed in ISO for a human reader of the trail.
    assert ev["ship_date"] == (HORIZON_START + dt.timedelta(weeks=1)).isoformat()
    assert ev["arrival_date"] == (HORIZON_START + dt.timedelta(weeks=2)).isoformat()
    assert ev["deficit_date"] == (HORIZON_START + dt.timedelta(weeks=2)).isoformat()
    assert "rule" in ev


def test_build_covered_late_false_when_arrival_at_deficit():
    """covered_late is arrival_bucket > deficit_bucket (STRICT): a transfer that
    ARRIVES exactly at the deficit bucket is on time -> False. This is the
    nominal case (the transit lead time fit before the need)."""
    r = _build(_signal(arrival_bucket=2, deficit_bucket=2))
    assert r.evidence["covered_late"] is False


def test_build_covered_late_true_when_arrival_after_deficit():
    """When ship_bucket floored at 0 the arrival can land AFTER the deficit
    bucket (the transit lead time did not fit) — the signal is STILL emitted and
    covered_late flags that honest state for a consumer."""
    r = _build(_signal(ship_bucket=0, arrival_bucket=3, deficit_bucket=2))
    assert r.evidence["covered_late"] is True


def test_build_is_pure_and_deterministic():
    """Same inputs => byte-identical row (same id, same evidence, same dates).
    Purity is the property the idempotent ON CONFLICT upsert leans on — a
    re-emitted identical signal must re-derive the SAME row every time."""
    sig = _signal()
    a = _build(sig)
    b = _build(sig)
    assert a == b


def test_build_confidence_defaults_high_and_is_overridable():
    """A transfer signal is a deterministic fact from the loaded plan, so
    confidence defaults HIGH; the caller may downgrade it (e.g. on provably
    stale demand). Assert both the default and an override flow through."""
    assert _build(_signal()).confidence == "HIGH"
    assert _build(_signal(), confidence="NEEDS_DATA_REVIEW").confidence == "NEEDS_DATA_REVIEW"
