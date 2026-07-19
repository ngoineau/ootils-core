"""
transfer.py — pure signal->recommendation mapping for the DRP transfer emitter
(#395 PR2b). DB-free, deterministic, mypy-checked.

The deterministic DRP core (engine/drp/core.py:transfer_signals) produces a
list of TransferSignal fair-share inter-site moves. This module turns ONE
signal into ONE governed recommendation row — the typed columns that
agent_transfer_watcher writes into the `recommendations` table (migration 039
+ the reschedule columns of migration 061 reused for qty/date + the source/dest
location columns of migration 066). Keeping the mapping here (not inline in the
script) makes it unit-testable and mypy-checked; the script/endpoint stay thin
orchestrators (load -> compute signals -> build rows -> upsert).

This is the DIRECT sibling of engine/recommendation/reschedule.py — same
pattern, same idempotence contract, same "the signal IS its own evidence, no
counter-factual fork" model (#346, ADR-026), applied to the distribution
echelon instead of the reschedule one. A DRP transfer is a NEW-order draft
(action='TRANSFER', decision level L1, per agent_governance.decision_level) —
the physical relocation of finished stock between two locations, reversible
until executed.

Idempotence is the whole point (stability): the recommendation_id is a
DETERMINISTIC uuid5 over (scenario_id, item, source_location, dest_location,
ship_date). Re-running the emitter on an unchanged plan re-derives the SAME id
for the same signal, so an ``INSERT ... ON CONFLICT (recommendation_id) DO
NOTHING`` upsert turns a re-emitted identical signal into a no-op — zero new
rows. This mirrors the reschedule emitter's deterministic-identity contract and
the kernel's deterministic_uuid contract for shortages (ADR-003): same input
state, same UUID, replay-safe.

The ship_date participates in the identity on purpose: if the underlying
deficit moves (e.g. a safety-stock overlay in a fork shifts the deficit bucket,
which shifts the ship bucket), the signal is a genuinely NEW message (different
ship_date => different id => a new DRAFT row), not a silent mutation of the
prior one.

Location coordinates (source_location / dest_location) are part of the identity
because the SAME item's SAME deficit can, in principle, be served from two
different sources in two runs if the excess picture changes — those are two
distinct transfer proposals (ship from A vs ship from B), each its own row.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb

from ootils_core.engine.drp.core import TransferSignal
from ootils_core.engine.drp.loader import DRPData, load_drp_data

logger = logging.getLogger(__name__)

# Same fixed namespace the kernel + the reschedule emitter use for deterministic
# UUIDs (ADR-003, engine/kernel/_ids.py; engine/recommendation/reschedule.py).
# Re-declared here rather than imported so this module stays free of any kernel
# dependency (the recommendation layer sits above the kernel and must not import
# from it upward); the value is the invariant.
_RECO_NAMESPACE = uuid.UUID("89e1e24e-42d7-5c31-87c7-c64e50e24131")

# The single action this emitter maps. Validation of the action string against
# the DB CHECK vocabulary (migration 066 widened it with 'TRANSFER') is the
# database's job.
TRANSFER_ACTION = "TRANSFER"

# Canonical Decision-Ladder level of a DRP transfer — the SINGLE place the
# literal 'L1' for TRANSFER is written. A transfer is a NEW-order draft (a
# physical relocation of finished stock, reversible until executed), same class
# as an ORDER_NOW new-order draft. The fleet-wide ladder table
# (scripts/agent_governance._ACTION_DECISION_LEVELS) SOURCES this constant
# rather than re-typing the literal, and the REST endpoint (api/routers/drp.py)
# uses it too, so the value is defined once. Both callers still pass the level
# into emit_transfer_recommendations explicitly — this constant is the source,
# not a hidden default.
TRANSFER_DECISION_LEVEL = "L1"


def transfer_recommendation_id(
    scenario_id: str,
    item_coord: str,
    source_location_coord: str,
    dest_location_coord: str,
    ship_date: date,
) -> uuid.UUID:
    """Stable uuid5 identity of a transfer recommendation (idempotence key).

    Same (scenario, item, source location, dest location, ship date) => same
    UUID, so a re-emitted identical signal upserts to a no-op. ship_date is
    rendered ISO inside the name so a date shift is a genuinely different
    message. The item/location arguments are the DRP planning-coordinate
    strings (COALESCE(external_id, uuid::text)) — stable per row across runs, so
    the identity is stable whether or not a business external_id is set.
    """
    name = "|".join(
        [
            "transfer_reco",
            str(scenario_id),
            item_coord,
            source_location_coord,
            dest_location_coord,
            ship_date.isoformat(),
        ]
    )
    return uuid.uuid5(_RECO_NAMESPACE, name)


@dataclass(frozen=True)
class TransferRecommendation:
    """One governed recommendation row built from a TransferSignal.

    Field names match the `recommendations` columns written by the watcher.
    Purely a data-transfer object: no DB, no side effects. The evidence dict is
    the forensic JSONB trail (the fair-share detail — the signal IS the evidence
    for a transfer; no counter-factual fork is needed, exactly like the
    reschedule watcher #346 and unlike the shortage watcher #340).

    UUID-typed columns (item_id, source_location_id, dest_location_id) are the
    real row identifiers the caller resolved from the DRP coordinate strings;
    the coordinate strings themselves live only in the recommendation_id and the
    evidence (readable trail).
    """

    recommendation_id: uuid.UUID
    scenario_id: str
    item_id: uuid.UUID
    item_external_id: str
    action: str
    decision_level: str
    source_location_id: uuid.UUID
    dest_location_id: uuid.UUID
    # migration-039 NOT-NULL business columns reused for a transfer message:
    # shortage_date is the deficit date the transfer covers; deficit_qty is the
    # projected shortfall at the destination; recommended_qty is the (fair-share
    # + transfer_multiple DOWN-rounded) quantity actually proposed to move.
    shortage_date: date
    deficit_qty: Decimal
    recommended_qty: Decimal
    # migration-061 column reused: the proposed ship date of the transfer.
    proposed_date: date
    confidence: str
    evidence: dict


def build_transfer_recommendation(
    *,
    signal: TransferSignal,
    scenario_id: str,
    item_id: uuid.UUID,
    item_external_id: str,
    source_location_id: uuid.UUID,
    dest_location_id: uuid.UUID,
    decision_level: str,
    horizon_start: date,
    confidence: str = "HIGH",
) -> TransferRecommendation:
    """Map one DRP TransferSignal to a governed recommendation row.

    Pure and deterministic: same inputs => byte-identical row, same
    recommendation_id. ``decision_level`` is passed in (resolved by the caller
    via agent_governance.decision_level('TRANSFER') == 'L1' — never hardcoded
    here) so the single fleet-wide ladder mapping stays the one source of truth.

    The DRP core works in weekly buckets; ``horizon_start`` (DRPData.horizon_start,
    the DB-side CURRENT_DATE anchor) converts the integer ship/deficit buckets
    back to calendar dates the same way the loader bucketed them (bucket N ==
    horizon_start + N*7 days). ship_date = horizon_start + ship_bucket weeks is
    the proposed_date; deficit_date = horizon_start + deficit_bucket weeks is the
    non-null shortage_date column.

    ``confidence`` defaults to HIGH: a transfer signal is a deterministic fact
    derived from the loaded plan (a projected deficit at one site against a
    computed excess at another), not a probabilistic forecast — the signal
    itself is the evidence. The caller may downgrade it if it ever runs on
    provably stale demand.
    """
    ship_date = horizon_start + timedelta(weeks=signal.ship_bucket)
    deficit_date = horizon_start + timedelta(weeks=signal.deficit_bucket)
    arrival_date = horizon_start + timedelta(weeks=signal.arrival_bucket)

    qty_dec = Decimal(str(signal.qty))
    deficit_dec = Decimal(str(signal.deficit_qty))

    evidence = {
        "signal": TRANSFER_ACTION,
        "item": signal.item,
        "source_location": signal.source_location,
        "dest_location": signal.dest_location,
        "qty": signal.qty,
        "ship_bucket": signal.ship_bucket,
        "arrival_bucket": signal.arrival_bucket,
        "deficit_bucket": signal.deficit_bucket,
        "ship_date": ship_date.isoformat(),
        "arrival_date": arrival_date.isoformat(),
        "deficit_date": deficit_date.isoformat(),
        "deficit_qty": signal.deficit_qty,
        "source_excess_before": signal.source_excess_before,
        "fair_share_qty": signal.fair_share_qty,
        "rounding_remnant": signal.rounding_remnant,
        # arrival at/after the deficit bucket == "covered late" (the transit
        # lead time did not fit before the need). The signal is still emitted;
        # this flag surfaces that honest state for a consumer.
        "covered_late": signal.arrival_bucket > signal.deficit_bucket,
        "rule": (
            "deterministic DRP fair-share transfer signal from "
            "drp_core.transfer_signals (#395): projected per-site deficit "
            "served from a linked source's excess, priority-stratified, "
            "transfer_multiple DOWN-rounded. The signal is its own evidence — "
            "no fork."
        ),
    }
    return TransferRecommendation(
        recommendation_id=transfer_recommendation_id(
            scenario_id,
            signal.item,
            signal.source_location,
            signal.dest_location,
            ship_date,
        ),
        scenario_id=scenario_id,
        item_id=item_id,
        item_external_id=item_external_id,
        action=TRANSFER_ACTION,
        decision_level=decision_level,
        source_location_id=source_location_id,
        dest_location_id=dest_location_id,
        shortage_date=deficit_date,
        deficit_qty=deficit_dec,
        recommended_qty=qty_dec,
        proposed_date=ship_date,
        confidence=confidence,
        evidence=evidence,
    )


# ---------------------------------------------------------------------------
# Orchestration — load DRP data, compute signals, upsert governed DRAFT rows.
# Read-only on the graph; writes ONLY into `recommendations` (ADR-021: the DRP
# never writes into `shortages`). Callable identically from the watcher
# (scripts/agent_transfer_watcher.py, inside a governed_run) and the REST
# endpoint (api/routers/drp.py). The agent_run lifecycle (RUNNING/COMPLETED/
# FAILED bookkeeping in agent_runs) is the CALLER's — this function only needs
# the run_id to stamp the FK, so both callers share one emission code path.
# ---------------------------------------------------------------------------

# Columns written per transfer recommendation. The three NOT-NULL business
# columns of migration 039 (shortage_date/deficit_qty/recommended_qty) are
# reused per build_transfer_recommendation; proposed_date comes from migration
# 061; source_location_id/dest_location_id come from migration 066.
_COLUMNS: tuple[str, ...] = (
    "recommendation_id",
    "agent_name",
    "agent_run_id",
    "scenario_id",
    "item_id",
    "item_external_id",
    "shortage_date",
    "deficit_qty",
    "recommended_qty",
    "proposed_date",
    "action",
    "decision_level",
    "source_location_id",
    "dest_location_id",
    "status",
    "confidence",
    "evidence",
    "anchor_date",
    "stream_seq_hwm",
)


def _current_stream_seq_hwm(conn: psycopg.Connection[Any], scenario: str) -> int:
    """Current MAX(events.stream_seq) for a scenario — the decision-basis HWM
    stamp (C2 §3).

    Re-declared here (a one-line keyset read) rather than importing
    scripts/agent_subscribe.current_max_seq: the recommendation layer sits ABOVE
    the kernel and must not import upward from the watcher scripts — the same
    "declare the invariant locally, never import upward" posture already applied
    to _RECO_NAMESPACE above. The transfer watcher has no --subscribe mode, so
    the HWM is simply the live max at emit time. Returns 0 on an empty stream
    (COALESCE), never NULL. stream_seq is an opaque high-water mark (migration
    063), compared with > only. Pinned to tuple_row so the positional read is
    correct regardless of the parent connection's row_factory (same reason as
    _resolve_coord_maps below).
    """
    cur = conn.cursor(row_factory=tuple_row)
    row = cur.execute(
        "SELECT COALESCE(MAX(stream_seq), 0) FROM events WHERE scenario_id = %s",
        (scenario,),
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _resolve_coord_maps(
    conn: psycopg.Connection[Any],
) -> tuple[dict[str, uuid.UUID], dict[str, str], dict[str, uuid.UUID]]:
    """Build the DRP-coordinate -> UUID resolution maps.

    The DRP core keys everything by the planning-coordinate string
    COALESCE(external_id, uuid::text) (see engine/drp/loader._item_key_sql /
    _loc_key_sql). To write a recommendation row we need the real item_id /
    location_id UUIDs back. This resolves EACH table with the SAME COALESCE key
    convention the loader used, so a coordinate produced by the loader always
    resolves here — whether or not a business external_id is set.

    Returns (item_coord -> item_id, item_coord -> item_external_id,
    loc_coord -> location_id). item_external_id is the human-readable label for
    the recommendation row; it falls back to the coordinate string (the UUID)
    when the item has no external_id — never None (the column is NOT NULL).

    Pinned to tuple_row (matching engine/drp/loader.py's own cursors) so the
    positional unpacking below (`for item_id, external_id in ...`) is correct
    REGARDLESS of the parent connection's configured row_factory. Without this
    pin, a dict_row connection (the app pool's default via OotilsDB /
    Depends(get_db) — see api/dependencies.py / db/connection.py) makes
    conn.cursor() inherit dict_row, and iterating a list of 2-key dicts unpacks
    their KEYS ("item_id", "external_id" — the literal column names), not their
    values: item_id_by_coord silently fills with {"external_id": "item_id"}
    instead of the real coordinate -> UUID mapping, so every real coordinate
    lookup misses and every signal is counted as unresolved_coords with ZERO
    recommendations emitted — with NO exception raised (a real bug found via
    POST /v1/drp/run, which runs on the app's dict_row pool; the CLI watcher
    never surfaced it because psycopg.connect() with no row_factory argument
    defaults to tuple_row, masking the defect on that one call path only).
    """
    cur = conn.cursor(row_factory=tuple_row)
    item_id_by_coord: dict[str, uuid.UUID] = {}
    item_ext_by_coord: dict[str, str] = {}
    for item_id, external_id in cur.execute(
        "SELECT item_id, external_id FROM items"
    ).fetchall():
        coord = external_id if external_id is not None else str(item_id)
        item_id_by_coord[coord] = item_id
        item_ext_by_coord[coord] = external_id if external_id is not None else str(item_id)

    loc_id_by_coord: dict[str, uuid.UUID] = {}
    for location_id, external_id in cur.execute(
        "SELECT location_id, external_id FROM locations"
    ).fetchall():
        coord = external_id if external_id is not None else str(location_id)
        loc_id_by_coord[coord] = location_id

    return item_id_by_coord, item_ext_by_coord, loc_id_by_coord


def _upsert(conn: psycopg.Connection[Any], rows: list[tuple[Any, ...]]) -> tuple[int, list[uuid.UUID]]:
    """Idempotent insert of transfer recommendations.

    ON CONFLICT (recommendation_id) DO NOTHING: a re-emitted identical signal
    (same deterministic id) is a no-op. Returns (inserted_count, affirmed_ids)
    where affirmed_ids is EVERY id we tried to write (inserted or already
    present) — the caller uses it to NOT expire the still-valid prior DRAFTs.
    SQL is composed via psycopg.sql (no f-strings in the SQL path). Mirrors
    engine/recommendation/reschedule's watcher upsert exactly.
    """
    if not rows:
        return 0, []
    col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in _COLUMNS)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in _COLUMNS)
    query = sql.SQL(
        "INSERT INTO recommendations ({cols}) VALUES ({vals}) "
        "ON CONFLICT (recommendation_id) DO NOTHING "
        "RETURNING recommendation_id"
    ).format(cols=col_ids, vals=placeholders)
    inserted = 0
    cur = conn.cursor()
    for r in rows:
        got = cur.execute(query, r).fetchone()
        if got is not None:
            inserted += 1
    affirmed = [r[0] for r in rows]
    return inserted, affirmed


def _expire_stale_drafts(
    conn: psycopg.Connection[Any],
    agent_name: str,
    scenario_id: str,
    keep_ids: list[uuid.UUID],
) -> int:
    """EXPIRE this agent/scenario's prior TRANSFER DRAFTs whose signal no longer
    fires.

    A DRAFT that is NOT in keep_ids (the ids the current run affirmed) means the
    deficit it covered was resolved (the plan changed) — mark it EXPIRED so the
    queue reflects reality. Rows in keep_ids are left untouched (their identity
    was just re-affirmed by the idempotent upsert). Scoped to this
    agent + scenario + action='TRANSFER' so it never touches another agent's,
    another fork's, or a non-transfer row. Mirrors reschedule's expiration.
    """
    cur = conn.cursor()
    if keep_ids:
        cur.execute(
            "UPDATE recommendations SET status='EXPIRED', updated_at=now() "
            "WHERE agent_name=%s AND scenario_id=%s AND action='TRANSFER' "
            "AND status='DRAFT' AND NOT (recommendation_id = ANY(%s))",
            (agent_name, scenario_id, keep_ids),
        )
        return cur.rowcount
    cur.execute(
        "UPDATE recommendations SET status='EXPIRED', updated_at=now() "
        "WHERE agent_name=%s AND scenario_id=%s AND action='TRANSFER' "
        "AND status='DRAFT'",
        (agent_name, scenario_id),
    )
    return cur.rowcount


def emit_transfer_recommendations(
    conn: psycopg.Connection[Any],
    scenario: str,
    horizon_days: int,
    *,
    agent_name: str,
    agent_run_id: Any,
    decision_level: str,
    confidence: str = "HIGH",
) -> dict[str, Any]:
    """Load DRP data, compute transfer signals, upsert governed DRAFT rows.

    The single emission code path shared by the transfer watcher and the DRP
    REST endpoint. Read-only on the graph (load_drp_data is SELECT-only,
    core.transfer_signals is pure); writes ONLY into `recommendations` — NEVER
    `shortages` (ADR-021). Does NOT commit (the caller owns the transaction:
    governed_run in the watcher, get_db in the endpoint).

    ``decision_level`` is resolved by the caller via
    agent_governance.decision_level('TRANSFER') == 'L1' — passed in, never
    hardcoded here, so the fleet-wide ladder stays the one source of truth.

    Idempotence: recommendation_id is deterministic (transfer_recommendation_id),
    upserted ON CONFLICT DO NOTHING — an unchanged plan re-run inserts ZERO new
    rows. Prior DRAFTs of this agent/scenario whose signal no longer fires are
    EXPIRED.

    A signal whose item/location coordinate cannot be resolved back to a UUID
    (a data-integrity impossibility on a loader-produced coordinate, but guarded
    rather than crashing mid-emission) is SKIPPED and counted — never silently
    dropped without a trace.

    Returns a metrics dict (signals, recommendations_affirmed / _inserted /
    _idempotent_noop, expired_stale_drafts, unresolved_coords) for the caller to
    log / store on the agent_runs row.
    """
    data: DRPData = load_drp_data(conn, horizon_days=horizon_days, scenario=scenario)
    signals = _compute_signals(data)

    item_id_by_coord, item_ext_by_coord, loc_id_by_coord = _resolve_coord_maps(conn)

    # Decision-basis stamps (C2 §3) carried on every reco: anchor_date =
    # data.horizon_start (the DRP as-of anchor) and stream_seq_hwm = the current
    # events high-water mark for this scenario. Computed once per run; the
    # transfer watcher has no --subscribe mode, so this is the live max at emit.
    anchor_date = data.horizon_start
    stream_seq_hwm = _current_stream_seq_hwm(conn, scenario)

    rows: list[tuple[Any, ...]] = []
    unresolved = 0
    for sig in signals:
        item_id = item_id_by_coord.get(sig.item)
        source_location_id = loc_id_by_coord.get(sig.source_location)
        dest_location_id = loc_id_by_coord.get(sig.dest_location)
        if item_id is None or source_location_id is None or dest_location_id is None:
            unresolved += 1
            logger.warning(
                "transfer.unresolved_coord item=%s source=%s dest=%s scenario=%s",
                sig.item, sig.source_location, sig.dest_location, scenario,
            )
            continue
        reco = build_transfer_recommendation(
            signal=sig,
            scenario_id=scenario,
            item_id=item_id,
            item_external_id=item_ext_by_coord.get(sig.item, str(item_id)),
            source_location_id=source_location_id,
            dest_location_id=dest_location_id,
            decision_level=decision_level,
            horizon_start=data.horizon_start,
            confidence=confidence,
        )
        rows.append((
            reco.recommendation_id, agent_name, agent_run_id, scenario,
            reco.item_id, reco.item_external_id, reco.shortage_date,
            reco.deficit_qty, reco.recommended_qty, reco.proposed_date,
            reco.action, reco.decision_level, reco.source_location_id,
            reco.dest_location_id, "DRAFT", reco.confidence, Jsonb(reco.evidence),
            anchor_date, stream_seq_hwm,
        ))

    inserted, affirmed = _upsert(conn, rows)
    expired = _expire_stale_drafts(conn, agent_name, scenario, affirmed)

    return {
        "signals": len(signals),
        "recommendations_affirmed": len(affirmed),
        "recommendations_inserted": inserted,
        "recommendations_idempotent_noop": len(affirmed) - inserted,
        "expired_stale_drafts": expired,
        "unresolved_coords": unresolved,
    }


def _compute_signals(data: DRPData) -> list[TransferSignal]:
    """Run the pure DRP core over loaded data. Isolated so a test can stub it
    and so the horizon-bucket count (DRPData.n_buckets) is derived in exactly
    one place. Mirrors the watcher's load->consume->signals seam in reschedule."""
    from ootils_core.engine.drp.core import transfer_signals

    return transfer_signals(
        data.demand_by_loc,
        data.on_hand_by_loc,
        data.safety_by_loc,
        data.links,
        data.n_buckets,
    )
