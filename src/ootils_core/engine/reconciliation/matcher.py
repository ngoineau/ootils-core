"""
matcher.py — heuristic reconciliation of inbound ERP purchase orders against
already-exported governed recommendations (ADR-042 decision 4, PR-5b; the
second half of the "Le sortant et la réconciliation" doctrine that PR-5a's
``engine/reporting/outbound_export.py`` left open).

WHAT THIS DOES, IN ONE SENTENCE. Once the daily run has loaded today's
purchase-order feed (the ERP's own PO numbers land as
``PurchaseOrderSupply`` nodes, migration 002, tracked via
``external_references``), this module heuristically pairs each inbound PO with
an exported recommendation on business attributes — item, quantity ±
tolerance, date ± window — and, for an UNAMBIGUOUS pair only, STAMPS
``recommendations.fulfilled_at`` + ``fulfilled_erp_id`` (the inbound PO
number). It is an OBSERVATION, never an applied write: it never touches
``recommendations.status`` nor the state machine's HUMAN_ONLY_TARGETS
(``engine/recommendation/state_machine.py``) — migration 086's header and
ADR-042 §"Décision Ladder" ("la réconciliation reste une observation, jamais
L3+/appliquée automatiquement").

SAME THREE-WAY DB-BOUNDARY SPLIT as every other engine module touching this
concern (``engine/maintenance/purge.py``'s ``plan_*``/``apply_*``,
``engine/reporting/outbound_export.py``'s load/render/execute):

  (a) ``match_candidates`` — the PURE, DB-FREE core. Given a tuple of
      ``RecoCandidate`` and a tuple of ``InboundPO`` (plus the two
      tolerances), it returns a ``MatchResult`` deterministically (every
      collection sorted first — same input, same output, every time,
      independently unit-testable with zero DB fixture). It stamps NOTHING
      and knows NOTHING about Postgres.
  (b) ``_load_candidates`` / ``_load_inbound_pos`` — the SELECT-only loaders.
      ``_load_candidates`` is modelled on ``outbound_export.py``'s
      ``_PENDING_EXPORT_SQL`` but with the WHERE restricted to
      ``exported_at IS NOT NULL AND fulfilled_at IS NULL`` (the
      ``ix_reco_pending_reconciliation`` partial index, migration 086) — every
      already-reconciled row is invisible by construction, which IS the
      idempotence (a re-run with no new PO stamps zero new rows, see below).
  (c) ``run_reconciliation`` — the sole writer. Loads (b), matches (a), stamps
      the unambiguous pairs, INSERTs exactly ONE ``reconciliation_runs`` row
      (append-only audit, migration 086) and emits exactly ONE
      ``reconciliation_completed`` stream event (migration 086, the typed
      contract documented in ``engine/events/emit.py``). Never commits/rolls
      back — the caller owns the transaction (same convention as every other
      engine module here). A ``dry_run=True`` variant computes the same match
      and returns the same counts + pairs while writing NOTHING (the CLI's
      preview).

THE HEURISTIC (V1, deterministic — no ``ootils_ref`` échoable exists in the
pilot's ERP, ADR-042 PR-5a amendment). A recommendation R is *plausible* with
an inbound PO P iff ALL of:
  * same item (``item_external_id`` — mandatory, both sides always carry it);
  * P was created STRICTLY AFTER R was exported (``nodes.created_at >
    recommendations.exported_at``) — a PO that pre-existed and was merely
    re-upserted today keeps its original ``created_at`` and is CORRECTLY
    excluded (it cannot be the ERP's response to a reco exported later);
  * quantity within ± ``QTY_TOLERANCE_PCT`` of R's recommended quantity;
  * need date within ± ``DATE_TOLERANCE_DAYS`` of P's expected delivery date;
  * supplier equality — ENFORCED ONLY WHEN BOTH SIDES CARRY A SUPPLIER (see
    KNOWN GAP 1 below); and
  * for a TRANSFER reco only, the reception (dest) location must equal P's
    receiving site (see KNOWN GAP 2 below).

AMBIGUITY IS NEVER SILENTLY RESOLVED (ADR-042 decision 4). If a reco has ≥2
plausible POs, OR a PO has ≥2 plausible recos, NOTHING is stamped for the
contested entities — they are COUNTED (``ambiguous_reco_ids`` /
``ambiguous_po_ids``) and published in the daily report's ambiguity signal,
never guessed. Only a MUTUALLY UNIQUE pair (R's single plausible PO is P, and
P's single plausible reco is R) is stamped.

IDEMPOTENCE. The candidate scan filters ``fulfilled_at IS NULL``, so an
already-stamped reco is never a candidate again — a re-run on the same DB
state (no new PO) produces zero new stamps. Each run still legitimately
appends its own ``reconciliation_runs`` row and emits its own event (a run is
an event even when it matched nothing — ADR-042 decision 4: "un run = UNE
ligne + UN event"; a reconciliation run always has a definite tally, unlike
an all-green ``daily_run_completed`` that has no culprits to report).

BASELINE-ONLY, BY NATURE (ADR-030's rationale, verbatim). A reconciliation
pairs OBSERVED inbound ERP POs with exported baseline recommendations
(``outbound_export.py`` is itself hardcoded ``BASELINE_SCENARIO_ID``-only) —
this is a fact about the real world, not a fork's simulated working state.
Both loaders filter ``scenario_id = BASELINE_SCENARIO_ID`` with no parameter
to override it, exactly as ``inventory_snapshots`` / ``recommendation_outcomes``
/ ``daily_runs`` already do.

────────────────────────────────────────────────────────────────────────────
KNOWN GAP 1 — THE PO NODE CARRIES NO SUPPLIER (verified against migration 002
+ the PO ingest path). ``nodes`` has no supplier column, and
``api/routers/ingest.py``'s ``ingest_purchase_orders`` VALIDATES
``supplier_external_id`` (the FK check) but NEVER persists it onto the
``PurchaseOrderSupply`` node — the supplier is validated-then-discarded. So an
``InboundPO``'s ``supplier_external_id`` is ALWAYS ``None`` in V1, and the
supplier criterion above (enforced only when BOTH sides carry a supplier)
NEVER constrains a real match today. This is a genuine structural gap, the
direct sibling of the location gap ADR-042's PR-5a amendment documented for
``recommendations`` — and it corrects migration 086's own header, which
optimistically described the heuristic as "item+supplier+qty+date" assuming
the supplier was available on the PO side. It is not. The consequence is a
HIGHER ambiguity rate (supplier cannot disambiguate two recos for the same
item/qty/date), which is exactly why ADR-042 mandates the ambiguity rate be
counted and published rather than hidden. The pure core keeps the supplier
comparison so the day PO ingest starts persisting the supplier, the criterion
activates with zero code change here.

KNOWN GAP 2 — NON-TRANSFER RECOS CARRY NO LOCATION (ADR-042 PR-5a amendment).
``recommendations`` has no generic site column; only a TRANSFER reco carries
``source_location_id`` / ``dest_location_id`` (migration 066). So the location
criterion is applied ONLY to the TRANSFER family (where the reco's dest
location must equal the PO's receiving site); po_drafts / reschedule_messages
match on item+qty+date only. Note also that an inbound ``PurchaseOrderSupply``
is not the natural fulfilment object of a TRANSFER (an inter-site move is
fulfilled by a ``TransferSupply``, not a PO) — so TRANSFER matches against the
PO feed are expected to be rare in practice; the family is nonetheless handled
correctly (dest-location equality required) rather than silently mis-matched.
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from psycopg.rows import dict_row

from ootils_core.constants import BASELINE_SCENARIO_ID
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.events.emit import emit_stream_event

# Single source of the outbound family vocabulary — reused, never re-declared,
# so "which action needs a dest location" can never drift from the export side.
# (import of a reporting constant is safe: outbound_export imports nothing from
# this package, so there is no import cycle.)
from ootils_core.engine.reporting.outbound_export import TRANSFER_ACTIONS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 🎯 PILOT-TUNABLE TOLERANCES (module constants, ADR-042 decision 4:
# "Tolerances +/-5% qty et +/-7j date = constantes module, commentees
# ajustables pilote"). A wider tolerance catches more real matches at the cost
# of more ambiguity (published, never hidden); a narrower one is stricter.
# ---------------------------------------------------------------------------
QTY_TOLERANCE_PCT: Decimal = Decimal("5")  # +/- 5 % of the reco's recommended qty
DATE_TOLERANCE_DAYS: int = 7               # +/- 7 calendar days around the need date


# ---------------------------------------------------------------------------
# Pure-core data model (DB-free)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class RecoCandidate:
    """One already-exported, not-yet-reconciled recommendation — every field
    the pure matcher needs, already resolved by the loader so
    ``match_candidates`` never touches the DB.

    ``need_date`` is the reco's effective need date (``proposed_date`` when the
    reco carries one — reschedule/transfer — else ``shortage_date``, which is
    NOT NULL in ``recommendations``). ``requires_dest_location`` is ``True``
    for the TRANSFER family only (see KNOWN GAP 2)."""

    recommendation_id: UUID
    item_external_id: str
    supplier_external_id: Optional[str]
    dest_location_external_id: Optional[str]
    requires_dest_location: bool
    quantity: Decimal
    need_date: date
    exported_at: datetime


@dataclass(frozen=True)
class InboundPO:
    """One inbound ERP purchase order — a baseline, active
    ``PurchaseOrderSupply`` node carrying an ERP PO number
    (``external_references``). ``supplier_external_id`` is ALWAYS ``None`` in
    V1 (KNOWN GAP 1 — the node carries no supplier)."""

    po_external_id: str
    item_external_id: Optional[str]
    location_external_id: Optional[str]
    supplier_external_id: Optional[str]
    quantity: Decimal
    delivery_date: date
    created_at: datetime


@dataclass(frozen=True)
class MatchResult:
    """The deterministic outcome of one ``match_candidates`` pass.

    * ``matched`` — the mutually-unique pairs ``(recommendation_id,
      po_external_id)`` safe to stamp, sorted by recommendation_id.
    * ``ambiguous_reco_ids`` — recos with ≥2 plausible POs (never stamped).
    * ``ambiguous_po_ids`` — POs with ≥2 plausible recos (never stamped).
    * ``unmatched_reco_ids`` — recos with zero plausible PO.

    These four buckets are honest tallies and NEED NOT partition the input:
    a reco whose single plausible PO is itself contested (that PO has ≥2
    plausible recos) is in NONE of them — it is simply not stamped this run
    (its ``fulfilled_at`` stays NULL, it is reconsidered next run), while the
    contested PO is surfaced in ``ambiguous_po_ids``. This mirrors migration
    086's header: "matched + ambiguous + unmatched need not equal
    recos_candidates by construction — ambiguity can be counted from either
    side of the pairing"."""

    matched: list[tuple[UUID, str]]
    ambiguous_reco_ids: list[UUID]
    ambiguous_po_ids: list[str]
    unmatched_reco_ids: list[UUID]


def _qty_within(reco_qty: Decimal, po_qty: Decimal, qty_tol_pct: Decimal) -> bool:
    """``|po - reco| <= (qty_tol_pct/100) * |reco|``. When the reco qty is 0
    (never expected — recommended_qty is a real ordered amount) the tolerance
    collapses to 0, i.e. exact equality required — a safe, non-crashing
    degenerate case, never a divide."""
    tol = (qty_tol_pct / Decimal(100)) * abs(reco_qty)
    return abs(po_qty - reco_qty) <= tol


def _is_plausible(
    reco: RecoCandidate, po: InboundPO, qty_tol_pct: Decimal, date_tol_days: int
) -> bool:
    """Every criterion of the V1 heuristic (see module docstring "THE
    HEURISTIC"). Pure, side-effect-free, total (returns a bool for any input
    pair)."""
    # Item — mandatory, both sides always carry it.
    if reco.item_external_id != po.item_external_id:
        return False
    # PO created strictly after the reco was exported (a pre-existing upserted
    # PO keeps its old created_at and is correctly excluded).
    if not (po.created_at > reco.exported_at):
        return False
    # Quantity within tolerance.
    if not _qty_within(reco.quantity, po.quantity, qty_tol_pct):
        return False
    # Need date within tolerance.
    if abs((po.delivery_date - reco.need_date).days) > date_tol_days:
        return False
    # Supplier — enforced ONLY when BOTH sides carry one (KNOWN GAP 1: the PO
    # side is always None in V1, so this never constrains today; forward-compatible).
    if (
        reco.supplier_external_id is not None
        and po.supplier_external_id is not None
        and reco.supplier_external_id != po.supplier_external_id
    ):
        return False
    # TRANSFER only — reception (dest) location must equal the PO's site
    # (KNOWN GAP 2: only the TRANSFER family carries a location).
    if reco.requires_dest_location:
        if reco.dest_location_external_id is None or po.location_external_id is None:
            return False
        if reco.dest_location_external_id != po.location_external_id:
            return False
    return True


def match_candidates(
    recos: tuple[RecoCandidate, ...],
    pos: tuple[InboundPO, ...],
    qty_tol_pct: Decimal = QTY_TOLERANCE_PCT,
    date_tol_days: int = DATE_TOLERANCE_DAYS,
) -> MatchResult:
    """The PURE, DB-free, DETERMINISTIC matcher core (a).

    Builds the bipartite plausibility relation between ``recos`` and ``pos``
    (by index, so a duplicated ``po_external_id`` never conflates two POs),
    then classifies: a mutually-unique plausible pair is a match; a reco with
    ≥2 plausible POs or a PO with ≥2 plausible recos is ambiguous (never
    stamped); a reco with zero plausible PO is unmatched. Every input is
    sorted first (recos by recommendation_id, POs by po_external_id) so the
    output ordering is stable across runs and machines."""
    recos_sorted = sorted(recos, key=lambda r: str(r.recommendation_id))
    pos_sorted = sorted(pos, key=lambda p: p.po_external_id)

    # O(R×P) cross product — tolerable for a daily best-effort batch at pilot
    # scale (~2.6K recos × ~9K POs, the item short-circuit in _is_plausible
    # keeps Decimal work to same-item pairs). If unmatched accumulation makes
    # this grow, bucket both sides by item_external_id first (R×P → Σ r_i×p_i).
    reco_to_pos: dict[int, list[int]] = {ri: [] for ri in range(len(recos_sorted))}
    po_to_recos: dict[int, list[int]] = {pi: [] for pi in range(len(pos_sorted))}
    for ri, reco in enumerate(recos_sorted):
        for pi, po in enumerate(pos_sorted):
            if _is_plausible(reco, po, qty_tol_pct, date_tol_days):
                reco_to_pos[ri].append(pi)
                po_to_recos[pi].append(ri)

    matched: list[tuple[UUID, str]] = []
    ambiguous_reco_ids: list[UUID] = []
    unmatched_reco_ids: list[UUID] = []
    for ri, reco in enumerate(recos_sorted):
        plausible = reco_to_pos[ri]
        if not plausible:
            unmatched_reco_ids.append(reco.recommendation_id)
        elif len(plausible) >= 2:
            ambiguous_reco_ids.append(reco.recommendation_id)
        else:
            pi = plausible[0]
            if len(po_to_recos[pi]) == 1:
                matched.append((reco.recommendation_id, pos_sorted[pi].po_external_id))
            # else: the PO is contested (surfaced in ambiguous_po_ids); this
            # reco is simply not stamped this run — see MatchResult docstring.

    ambiguous_po_ids = [
        pos_sorted[pi].po_external_id
        for pi in range(len(pos_sorted))
        if len(po_to_recos[pi]) >= 2
    ]

    return MatchResult(
        matched=matched,
        ambiguous_reco_ids=ambiguous_reco_ids,
        ambiguous_po_ids=ambiguous_po_ids,
        unmatched_reco_ids=unmatched_reco_ids,
    )


# ---------------------------------------------------------------------------
# DB orchestration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ReconciliationRunResult:
    """The outcome of one ``run_reconciliation`` call. ``run_id`` / ``event_id``
    are ``None`` for a dry-run preview (nothing persisted, nothing emitted).
    ``match`` carries the detailed pairing so a CLI can print the pairs
    without a second pass; the four scalar counts are the honest run-level
    tally persisted to ``reconciliation_runs``."""

    run_id: Optional[UUID]
    run_date: date
    candidates: int
    matched: int
    ambiguous: int
    unmatched: int
    event_id: Optional[UUID]
    match: MatchResult


# Candidate scan — modelled on outbound_export.py's _PENDING_EXPORT_SQL, WHERE
# restricted to the reconciliation-pending predicate (the
# ix_reco_pending_reconciliation partial index, migration 086). BASELINE-only
# (see module docstring). No status filter: exported_at IS NOT NULL already
# implies the row was APPROVED/APPLIED at export time (only those get stamped,
# outbound_export.py), and the task's candidate rule is exactly
# "exported_at IS NOT NULL AND fulfilled_at IS NULL".
_CANDIDATES_SQL = """
    SELECT
        r.recommendation_id,
        r.action,
        r.item_external_id,
        r.supplier_external_id,
        r.recommended_qty,
        r.shortage_date,
        r.proposed_date,
        r.exported_at,
        dst_loc.external_id AS dest_location_external_id
    FROM recommendations r
    LEFT JOIN locations dst_loc ON dst_loc.location_id = r.dest_location_id
    WHERE r.scenario_id = %s
      AND r.exported_at IS NOT NULL
      AND r.fulfilled_at IS NULL
    ORDER BY r.recommendation_id
"""

# Inbound PO scan — every baseline, active PurchaseOrderSupply node that
# carries an ERP PO number (INNER JOIN external_references restricts to exactly
# the inbound ERP POs; an MRP-planned firm order with no ERP number is
# correctly excluded). No supplier column exists on the node (KNOWN GAP 1) — it
# is loaded as NULL. The per-pair created_at > exported_at gate (in the pure
# matcher) is what enforces temporal eligibility; no global created_at filter
# here, so a legitimately old PO simply never matches a newer reco.
_INBOUND_POS_SQL = """
    SELECT
        po_ref.external_id AS po_external_id,
        n.quantity,
        n.time_ref,
        n.created_at,
        i.external_id   AS item_external_id,
        loc.external_id AS location_external_id
    FROM nodes n
    JOIN external_references po_ref
        ON po_ref.entity_type = 'purchase_order'
       AND po_ref.internal_id = n.node_id
    LEFT JOIN items i ON i.item_id = n.item_id
    LEFT JOIN locations loc ON loc.location_id = n.location_id
    WHERE n.scenario_id = %s
      AND n.node_type = 'PurchaseOrderSupply'
      AND n.active = TRUE
    ORDER BY po_ref.external_id
"""


def _to_decimal(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _coerce_uuid(value: Any) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def _load_candidates(conn: DictRowConnection) -> tuple[RecoCandidate, ...]:
    """SELECT-only (b): every baseline reco eligible for reconciliation
    (exported, not yet fulfilled). Writes nothing."""
    cur = conn.cursor(row_factory=dict_row)
    rows = cur.execute(_CANDIDATES_SQL, (BASELINE_SCENARIO_ID,)).fetchall()
    candidates: list[RecoCandidate] = []
    for row in rows:
        need_date = row["proposed_date"] if row["proposed_date"] is not None else row["shortage_date"]
        if need_date is None:
            # shortage_date is NOT NULL in recommendations, so this is
            # unreachable in practice — guarded anyway so a future nullable
            # date column never silently crashes the daily run.
            logger.warning(
                "reconciliation.candidate_no_date recommendation_id=%s — skipped",
                row["recommendation_id"],
            )
            continue
        candidates.append(
            RecoCandidate(
                recommendation_id=_coerce_uuid(row["recommendation_id"]),
                item_external_id=row["item_external_id"],
                supplier_external_id=row["supplier_external_id"],
                dest_location_external_id=row["dest_location_external_id"],
                requires_dest_location=row["action"] in TRANSFER_ACTIONS,
                quantity=_to_decimal(row["recommended_qty"]),
                need_date=need_date,
                exported_at=row["exported_at"],
            )
        )
    return tuple(candidates)


def _load_inbound_pos(conn: DictRowConnection) -> tuple[InboundPO, ...]:
    """SELECT-only (b): every baseline, active inbound ERP PO
    (PurchaseOrderSupply node with a purchase_order external reference).
    ``supplier_external_id`` is always NULL (KNOWN GAP 1). Writes nothing."""
    cur = conn.cursor(row_factory=dict_row)
    rows = cur.execute(_INBOUND_POS_SQL, (BASELINE_SCENARIO_ID,)).fetchall()
    return tuple(
        InboundPO(
            po_external_id=row["po_external_id"],
            item_external_id=row["item_external_id"],
            location_external_id=row["location_external_id"],
            supplier_external_id=None,  # KNOWN GAP 1 — no supplier on the node
            quantity=_to_decimal(row["quantity"]),
            delivery_date=row["time_ref"],
            created_at=row["created_at"],
        )
        for row in rows
    )


def run_reconciliation(
    conn: DictRowConnection,
    run_date: date,
    *,
    now: Optional[datetime] = None,
    dry_run: bool = False,
) -> ReconciliationRunResult:
    """The sole writer (c): load the pending candidates + inbound POs, match
    them (pure core), and — unless ``dry_run`` — STAMP each unambiguous pair's
    ``fulfilled_at`` / ``fulfilled_erp_id``, INSERT one ``reconciliation_runs``
    row, and emit one ``reconciliation_completed`` stream event.

    ``dry_run=True``: computes the identical match and returns the same counts
    + pairs, writing NOTHING (no stamp, no run row, no event) — the CLI's
    read-only preview. ``run_id`` / ``event_id`` are ``None``.

    One run = one ``reconciliation_runs`` row + one event, ALWAYS (even when
    it matched nothing) — ADR-042 decision 4. Never commits/rolls back — the
    caller owns the transaction.
    """
    stamped_at = now if now is not None else datetime.now(timezone.utc)

    recos = _load_candidates(conn)
    pos = _load_inbound_pos(conn)
    result = match_candidates(recos, pos, QTY_TOLERANCE_PCT, DATE_TOLERANCE_DAYS)

    candidates = len(recos)
    matched = len(result.matched)
    ambiguous = len(result.ambiguous_reco_ids) + len(result.ambiguous_po_ids)
    unmatched = len(result.unmatched_reco_ids)

    if dry_run:
        logger.info(
            "reconciliation.preview run_date=%s candidates=%d matched=%d ambiguous=%d unmatched=%d",
            run_date, candidates, matched, ambiguous, unmatched,
        )
        return ReconciliationRunResult(
            run_id=None, run_date=run_date, candidates=candidates, matched=matched,
            ambiguous=ambiguous, unmatched=unmatched, event_id=None, match=result,
        )

    _stamp_matches(conn, result.matched, stamped_at)
    run_id = _insert_run(conn, run_date, candidates, matched, ambiguous, unmatched, stamped_at)
    event_id = emit_stream_event(
        conn,
        "reconciliation_completed",
        BASELINE_SCENARIO_ID,
        field_changed="reconciliation_completed",
        new_text=str(run_id),
        new_quantity=matched,
        old_text=f"ambiguous={ambiguous},unmatched={unmatched}",
        source="engine",
    )

    logger.info(
        "reconciliation.applied run_date=%s run_id=%s candidates=%d matched=%d "
        "ambiguous=%d unmatched=%d event_id=%s",
        run_date, run_id, candidates, matched, ambiguous, unmatched, event_id,
    )
    return ReconciliationRunResult(
        run_id=run_id, run_date=run_date, candidates=candidates, matched=matched,
        ambiguous=ambiguous, unmatched=unmatched, event_id=event_id, match=result,
    )


def _stamp_matches(
    conn: DictRowConnection, matched: list[tuple[UUID, str]], now: datetime
) -> None:
    """Stamp ``fulfilled_at`` + ``fulfilled_erp_id`` for each unambiguous pair —
    OBSERVATION ONLY (no status change, no state-machine coupling). Re-guarded
    by ``fulfilled_at IS NULL`` (defense in depth against a concurrent run
    racing the same reco); a no-op stamp is logged but never fatal (best-effort
    observation — a serial daily batch never hits this)."""
    for reco_id, po_external_id in matched:
        cur = conn.execute(
            "UPDATE recommendations "
            "SET fulfilled_at = %s, fulfilled_erp_id = %s, updated_at = now() "
            "WHERE recommendation_id = %s AND fulfilled_at IS NULL",
            (now, po_external_id, reco_id),
        )
        if (cur.rowcount or 0) != 1:
            logger.warning(
                "reconciliation.stamp_noop recommendation_id=%s po=%s — already "
                "fulfilled (concurrent run?), not re-stamped",
                reco_id, po_external_id,
            )


def _insert_run(
    conn: DictRowConnection,
    run_date: date,
    candidates: int,
    matched: int,
    ambiguous: int,
    unmatched: int,
    now: datetime,
) -> UUID:
    """INSERT exactly one append-only ``reconciliation_runs`` row and return
    its ``run_id``. Honest tallies from the SAME match pass (migration 086's
    header) — no arithmetic CHECK ties the four counts together."""
    cur = conn.cursor(row_factory=dict_row)
    row = cur.execute(
        """
        INSERT INTO reconciliation_runs (
            run_date, recos_candidates, matched, ambiguous, unmatched, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING run_id
        """,
        (run_date, candidates, matched, ambiguous, unmatched, now),
    ).fetchone()
    if row is None:  # INSERT ... RETURNING yields exactly one row — fail loudly
        raise RuntimeError("reconciliation_runs INSERT ... RETURNING yielded no row")
    return _coerce_uuid(row["run_id"])
