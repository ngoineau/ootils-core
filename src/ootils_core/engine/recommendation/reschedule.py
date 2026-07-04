"""
reschedule.py — pure signal->recommendation mapping for the reschedule emitter
(#346 PR-B). DB-free, deterministic, mypy-checked.

The deterministic MRP core (engine/mrp/core.py:reschedule_signals) produces a
list of RescheduleSignal dampened action messages. This module turns ONE signal
into ONE governed recommendation row — the typed columns that
agent_reschedule_watcher writes into the `recommendations` table (migration 039
+ the reschedule columns of migration 061). Keeping the mapping here (not inline
in the script) makes it unit-testable and mypy-checked; the script stays a thin
orchestrator (load -> compute signals -> build rows -> upsert).

Idempotence is the whole point of #346 (stability): the recommendation_id is a
DETERMINISTIC uuid5 over (scenario_id, target_node_id, action, proposed_date).
Re-running the watcher on an unchanged plan re-derives the SAME id for the same
signal, so an ``INSERT ... ON CONFLICT (recommendation_id) DO NOTHING`` upsert
turns a re-emitted identical signal into a no-op — zero new rows. This mirrors
the kernel's deterministic_uuid contract for shortages (ADR-003): same input
state, same UUID, replay-safe. It is deliberately STRONGER than the
supersede-then-reinsert pattern of the shortage/material watchers, which mint a
fresh UUID every run; a reschedule signal is a stable fact ("this order is
mis-dated vs its need"), not a re-costed proposal, so the stable-identity upsert
is the correct model and satisfies the central invariant (unchanged plan => 0
new recommendations).

The proposed_date participates in the identity on purpose: if the underlying
need moves (e.g. a lead-time overlay in a fork shifts the need date), the signal
is a genuinely NEW message (different proposed_date => different id => a new
DRAFT row), not a silent mutation of the prior one. A CANCEL has proposed_date
None, so its identity is (scenario, node, 'CANCEL', None) — a re-emitted CANCEL
of the same node is idempotent.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional

# Same fixed namespace the kernel uses for deterministic UUIDs (ADR-003,
# engine/kernel/_ids.py). Re-declared here rather than imported so this module
# stays free of any kernel dependency (the recommendation layer sits above the
# kernel and must not import from it upward); the value is the invariant.
_RECO_NAMESPACE = uuid.UUID("89e1e24e-42d7-5c31-87c7-c64e50e24131")

# Reschedule actions this emitter maps. DEFER is in the migration-061 CHECK
# vocabulary and carries an L2 decision level, but the deterministic core does
# NOT emit it (reserved for manual/agent use — see engine/mrp/core.py); it is
# therefore not produced here either. The mapping accepts whatever action the
# signal carries — validation of the action string is the DB CHECK's job.
RESCHEDULE_ACTIONS: frozenset[str] = frozenset(
    {"RESCHEDULE_IN", "RESCHEDULE_OUT", "CANCEL", "DEFER"}
)


def reschedule_recommendation_id(
    scenario_id: str,
    target_node_id: str,
    action: str,
    proposed_date: Optional[date],
) -> uuid.UUID:
    """Stable uuid5 identity of a reschedule recommendation (idempotence key).

    Same (scenario, target node, action, proposed date) => same UUID, so a
    re-emitted identical signal upserts to a no-op. proposed_date is rendered
    ISO (or the literal 'None' for CANCEL) inside the name so a date change is
    a genuinely different message.
    """
    name = "|".join(
        [
            "reschedule_reco",
            str(scenario_id),
            str(target_node_id),
            action,
            proposed_date.isoformat() if proposed_date is not None else "None",
        ]
    )
    return uuid.uuid5(_RECO_NAMESPACE, name)


@dataclass(frozen=True)
class RescheduleRecommendation:
    """One governed recommendation row built from a RescheduleSignal.

    Field names match the `recommendations` columns written by the watcher.
    Purely a data-transfer object: no DB, no side effects. The evidence dict is
    the forensic JSONB trail (the signal detail — the signal IS the evidence
    for a reschedule; no counter-factual fork is needed, unlike the shortage
    watcher #340).
    """

    recommendation_id: uuid.UUID
    scenario_id: str
    item_id: str
    item_external_id: str
    action: str
    decision_level: str
    target_node_id: str
    current_receipt_date: date
    proposed_date: Optional[date]
    # NOT-NULL business columns of migration 039 reused for a reschedule
    # message: shortage_date is the date at issue (the proposed need date, or
    # the current date for a CANCEL which has no new date); deficit_qty /
    # recommended_qty are the receipt's own quantity — the unit being re-dated
    # or cancelled (V1 does not split a receipt).
    shortage_date: date
    deficit_qty: Decimal
    recommended_qty: Decimal
    confidence: str
    evidence: dict


def build_recommendation(
    *,
    scenario_id: str,
    item_external_id: str,
    action: str,
    decision_level: str,
    node_id: str,
    item_id: str,
    current_receipt_date: date,
    proposed_date: Optional[date],
    qty: float,
    node_type: str,
    is_firm: bool,
    confidence: str = "HIGH",
) -> RescheduleRecommendation:
    """Map one reschedule signal's fields to a governed recommendation row.

    Pure and deterministic: same inputs => byte-identical row, same
    recommendation_id. ``decision_level`` is passed in (resolved by the caller
    via agent_governance.decision_level(action) — never hardcoded here) so the
    single fleet-wide ladder mapping stays the one source of truth.

    ``confidence`` defaults to HIGH: a reschedule signal is a deterministic fact
    derived from the loaded plan (this order is mis-dated vs the computed need),
    not a probabilistic forecast — the signal itself is the evidence. The caller
    may downgrade it (e.g. NEEDS_DATA_REVIEW on a stale demand book) by passing a
    different value.
    """
    qty_dec = Decimal(str(qty))
    # For a re-date the "date at issue" is where the order SHOULD land (the
    # proposed need date); for a CANCEL there is no proposed date, so the
    # non-null shortage_date column is anchored on the current receipt date.
    shortage_date = proposed_date if proposed_date is not None else current_receipt_date
    delta_days = (
        (proposed_date - current_receipt_date).days if proposed_date is not None else None
    )
    evidence = {
        "signal": action,
        "target_node_id": node_id,
        "node_type": node_type,
        "is_firm": is_firm,
        "qty": qty,
        "current_receipt_date": current_receipt_date.isoformat(),
        "proposed_date": proposed_date.isoformat() if proposed_date is not None else None,
        "delta_days": delta_days,
        "rule": (
            "deterministic reschedule signal from mrp_core.reschedule_signals "
            "(#346): receipt date vs median-unit need date, dampened by "
            "reschedule_min_days. The signal is its own evidence — no fork."
        ),
    }
    return RescheduleRecommendation(
        recommendation_id=reschedule_recommendation_id(
            scenario_id, node_id, action, proposed_date
        ),
        scenario_id=scenario_id,
        item_id=item_id,
        item_external_id=item_external_id,
        action=action,
        decision_level=decision_level,
        target_node_id=node_id,
        current_receipt_date=current_receipt_date,
        proposed_date=proposed_date,
        shortage_date=shortage_date,
        deficit_qty=qty_dec,
        recommended_qty=qty_dec,
        confidence=confidence,
        evidence=evidence,
    )
