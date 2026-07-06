"""
evaluator.py — deterministic recommendation-outcome classifier (chantier #393
A3-PR2, ADR-030). The proof-of-value core: chain a governed recommendation
(migration 039) to its observed real-world result and, when computable, value
the shortage $ it avoided.

Split, mirroring the engine's DB-boundary convention (the classifier is pure,
the orchestrator owns SQL):

  * ``evaluate_outcome`` is a PURE function: given a reco row, the shortage
    actually observed at its coordinate on ``evaluated_as_of``, the observation
    snapshot (if any) and the observation date, it returns one frozen
    ``OutcomeRow`` — a deterministic 5-way verdict + the NULL-honest $ figures.
    It touches NO database and NO clock — every input is passed in, so the five
    branches are golden-testable in isolation with in-memory dataclasses.
  * ``evaluate_and_persist`` is the only DB-touching entry point: it loads the
    eligible recos + the observed ``shortages`` + the observation snapshots for
    a scenario, calls ``evaluate_outcome`` per reco, and idempotently upserts
    each verdict into ``recommendation_outcomes`` (migration 069). It is
    READ-ONLY on ``recommendations`` / ``shortages`` / ``inventory_snapshots``
    (ADR-021: we READ ``shortages``, we NEVER write it) and writes ONLY
    ``recommendation_outcomes``. It does NOT commit — the caller owns the
    transaction (``get_db`` for the API, the CLI's own connection context).

DETERMINISTIC, NEVER AN LLM (North Star: agents propose, the deterministic core
scores). The verdict is a pure function of the loaded facts; re-running it on the
same facts yields the same five-way classification and the same $ figures.

THE PREDICTED SHORTAGE is extracted from the reco itself: the typed columns
``shortage_date`` / ``deficit_qty`` (migration 039, NOT NULL) are the primary
source; the ``evidence`` JSONB is a fallback for the $ basis (``unit_cost``).
No re-computation of MRP here — the reco already froze what it predicted.

THE OBSERVED SHORTAGE is the ``shortages`` row the orchestrator resolved for the
reco's (item, location) coordinate at ``evaluated_as_of`` — the canonical
$-valued shortage truth owned by ShortageDetector (ADR-021). ``None`` means "no
active shortage was observed at that coordinate" (the deficit was avoided).

THE PREDICTED $ BASIS — an honest proxy, documented: the reco does NOT persist
the ShortageDetector days-weighted ``severity_score``; it persists the deficit
quantity and (in evidence) a per-unit cost. So the predicted $ credited as
avoided is ``predicted_deficit_qty x unit_cost`` — a deficit-value figure, NOT
the days x qty x unit_cost severity the ``shortages`` table stores. This is the
best self-contained valuation derivable from a reco alone; when no unit cost is
derivable the figure is NULL-honest (``avoided_severity_usd = None`` — "no cost
basis"), never a masked 0. The threshold/basis knobs below are documented and
tunable.
"""
from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from psycopg.rows import dict_row

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.mrp.core import BASELINE

logger = logging.getLogger(__name__)

# The five deterministic verdicts. Kept in sync with the migration-069
# evaluation_status CHECK — this frozenset is the Python half of that contract.
VALID_STATUSES: frozenset[str] = frozenset(
    {"AVOIDED", "MATERIALIZED", "PARTIAL", "NOT_APPLICABLE", "INDETERMINATE"}
)

# The reco statuses that count as "the reco actually acted" (was approved into
# the plan). Only these can be credited an AVOIDED / PARTIAL $ figure — a reco
# that stayed DRAFT/REJECTED/EXPIRED never influenced reality, so its outcome is
# a counter-factual (NOT_APPLICABLE), never a credit. APPLIED is the
# post-approval executed state; both are "acted".
ACTED_STATUSES: frozenset[str] = frozenset({"APPROVED", "APPLIED"})

# ---------------------------------------------------------------------------
# Classification thresholds (🎯 tunable — documented defaults, no hidden magic)
# ---------------------------------------------------------------------------
# AVOIDED vs PARTIAL vs MATERIALIZED is decided PURELY by the ratio
# observed_deficit_qty / predicted_deficit_qty — three MUTUALLY EXCLUSIVE,
# EXHAUSTIVE bands over ratio in [0, +inf):
#
#   * MATERIALIZED_FLOOR_RATIO (default 0.90) — ratio >= this => the shortage
#     happened essentially as predicted (MATERIALIZED, no meaningful reduction).
#     Within 10% of the prediction is "not reduced". TESTED FIRST (see
#     evaluate_outcome): a fully-materialized shortage (ratio == 1.0) must never
#     fall into AVOIDED regardless of the absolute scale of predicted_qty.
#   * AVOIDED_EPS_RATIO (default 0.05) — ratio <= this => the deficit is
#     effectively gone (AVOIDED). A residual under 5% of what was predicted is
#     noise, not a surviving shortage.
#   * Otherwise (AVOIDED_EPS_RATIO < ratio < MATERIALIZED_FLOOR_RATIO) — the
#     deficit was genuinely reduced but not eliminated => PARTIAL.
#
# The three bands are [0, AVOIDED_EPS_RATIO] AVOIDED / (AVOIDED_EPS_RATIO,
# MATERIALIZED_FLOOR_RATIO) PARTIAL / [MATERIALIZED_FLOOR_RATIO, +inf)
# MATERIALIZED — they partition ratio>=0 with NO overlap and NO gap. There is
# deliberately NO absolute floor mixed into these bands (a prior version used
# max(predicted * AVOIDED_EPS_RATIO, AVOIDED_EPS_ABS) as the AVOIDED ceiling,
# which for predicted <= AVOIDED_EPS_ABS / AVOIDED_EPS_RATIO = 20 made the
# absolute floor dominate the ratio and score a ratio-1.0 fully-materialized
# shortage as AVOIDED with full $ credit — a MAJOR defect for a proof machine
# that must never overstate value; see the fix history for the concrete case).
#
# AVOIDED_EPS_ABS is retained ONLY for the degenerate predicted_qty <= 0 / None
# case, where there is no ratio to compute at all — see the "Degenerate case
# FIRST" branch in evaluate_outcome below. It never re-enters the ratio bands.
#
# These live HERE, not in the DB and not in the consumer — the classification of
# a physical observation is the evaluator's job. (Decision-Ladder score
# thresholds, by contrast, belong to consumers — cf. the confidence composer.)
AVOIDED_EPS_RATIO = Decimal("0.05")
AVOIDED_EPS_ABS = Decimal("1")
MATERIALIZED_FLOOR_RATIO = Decimal("0.90")


@dataclass(frozen=True)
class ObservedShortage:
    """The shortage actually observed at a reco's coordinate on the observation
    date — a projection of the canonical ``shortages`` row (ADR-021). Passed IN
    to the pure evaluator (the evaluator never queries). ``severity_usd`` is the
    ShortageDetector $-valued ``severity_score``; ``deficit_qty`` its
    ``shortage_qty``.
    """

    item_id: UUID
    location_id: Optional[UUID]
    shortage_date: _dt.date
    deficit_qty: Decimal
    severity_usd: Decimal


@dataclass(frozen=True)
class OutcomeRow:
    """One deterministic verdict — an in-memory row destined for
    ``recommendation_outcomes`` (migration 069).

    NULL-honest: ``avoided_severity_usd`` is None when NOT computable (no cost
    basis / INDETERMINATE / a reco that never acted), NEVER a masked 0 — 0 is
    reserved for the genuine "nothing was at stake" case. ``predicted_*`` are
    None when the reco carried no such figure. ``snapshot_id`` is the
    observation snapshot pointer (nullable).
    """

    recommendation_id: UUID
    evaluated_as_of: _dt.date
    evaluation_status: str
    predicted_shortage_date: Optional[_dt.date]
    predicted_deficit_qty: Optional[Decimal]
    observed_deficit_qty: Optional[Decimal]
    avoided_severity_usd: Optional[Decimal]
    snapshot_id: Optional[UUID]


def _as_decimal(value: Any) -> Optional[Decimal]:
    """Coerce a NUMERIC/int/float/str to Decimal without float drift; None-safe.

    psycopg already yields Decimal for NUMERIC columns; this also handles a
    number pulled from the evidence JSONB (int/float/str) via ``str()`` so no
    binary-float artifact leaks into a $ figure.
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _predicted_unit_cost(reco_row: dict[str, Any]) -> Optional[Decimal]:
    """Per-unit cost basis for the predicted-$ figure, extracted from the reco.

    Precedence (all self-contained in the reco — no DB, no master-data lookup):
      1. evidence['unit_cost'] — the shortage/material watcher persists the
         negotiated/standard unit cost it valued the shortage with (ADR-021
         cost_of precedence, already resolved at emission).
      2. estimated_cost / recommended_qty — back out the per-unit cost from the
         order-value column when evidence carries no unit_cost (estimated_cost =
         recommended_qty x unit_cost at emission).
    Returns None when neither is derivable => the $ figure is NULL-honest.
    """
    evidence = reco_row.get("evidence")
    if isinstance(evidence, dict):
        uc = _as_decimal(evidence.get("unit_cost"))
        if uc is not None and uc > 0:
            return uc

    est = _as_decimal(reco_row.get("estimated_cost"))
    qty = _as_decimal(reco_row.get("recommended_qty"))
    if est is not None and qty is not None and qty > 0:
        return est / qty
    return None


def _predicted_severity_usd(
    reco_row: dict[str, Any],
    predicted_deficit_qty: Optional[Decimal],
) -> Optional[Decimal]:
    """Deterministic predicted $ credited as avoided, from the reco alone.

    ``predicted_deficit_qty x unit_cost`` — a deficit-VALUE proxy (see the module
    docstring: the reco does not persist the days-weighted ShortageDetector
    severity, so this is the honest self-contained valuation). NULL-honest: None
    when there is no deficit figure or no derivable unit cost basis — never a
    masked 0.
    """
    if predicted_deficit_qty is None:
        return None
    unit_cost = _predicted_unit_cost(reco_row)
    if unit_cost is None:
        return None
    return predicted_deficit_qty * unit_cost


def evaluate_outcome(
    reco_row: dict[str, Any],
    observed_shortage: Optional[ObservedShortage],
    snapshot_row: Optional[dict[str, Any]],
    evaluated_as_of: _dt.date,
) -> OutcomeRow:
    """Classify one recommendation's observed outcome — PURE, deterministic.

    Inputs (all passed in — no DB, no clock):
      * ``reco_row`` — a ``recommendations`` row (dict). The predicted shortage
        is its ``shortage_date`` / ``deficit_qty``; its ``status`` decides
        whether the reco actually acted; its ``evidence`` / ``estimated_cost``
        give the $ basis.
      * ``observed_shortage`` — the shortage the orchestrator resolved for this
        reco's coordinate at ``evaluated_as_of`` (canonical ``shortages`` truth),
        or None when NO active shortage was observed there.
      * ``snapshot_row`` — the ``inventory_snapshots`` observation row for the
        coordinate/day, or None. Its ABSENCE is what makes an ACTED reco
        INDETERMINATE: without a point-in-time observation we cannot honestly
        assert the deficit was avoided (no-shortage could just mean not-yet-
        observed). ``snapshot_id`` is carried onto the verdict when present.
      * ``evaluated_as_of`` — the observation date the verdict is anchored to.

    The five branches (see migration 069 header + the thresholds above):

      NOT_APPLICABLE — the reco never acted (status not in ACTED_STATUSES:
        DRAFT/REVIEWED/REJECTED/EXPIRED). Counter-factual: we RECORD whether the
        shortage occurred (``observed_deficit_qty`` = the observed deficit, or 0
        if none) as the cost-of-inaction signal, but credit NO ``avoided_severity_
        usd`` (the reco did not act — None, not 0).

      INDETERMINATE — an ACTED reco with NO observation snapshot at the
        coordinate/day: we cannot classify honestly. Everything observed/avoided
        is None (honest), predicted_* still reported.

      For an ACTED reco with an observation snapshot present, the remaining
      three verdicts are decided by ``ratio = observed_qty / predicted_qty`` in
      three MUTUALLY EXCLUSIVE, EXHAUSTIVE bands (MATERIALIZED is tested FIRST —
      see the thresholds section above for why the test order matters):

      MATERIALIZED — ratio >= MATERIALIZED_FLOOR_RATIO (the shortage happened
        anyway, not meaningfully reduced, INCLUDING a fully-materialized
        ratio == 1.0 regardless of predicted_qty's absolute scale):
        ``avoided_severity_usd`` = 0 (genuinely nothing avoided — distinct from
        NULL "not computable"). Also the prudent fallback for the DEGENERATE
        predicted_qty None/<=0 case when observed_qty exceeds the ABSOLUTE noise
        floor (AVOIDED_EPS_ABS) — no ratio can be formed, so a shortage that
        clears the noise floor is honestly MATERIALIZED (nothing proven
        avoided), while one AT or BELOW it is AVOIDED (see below).

      AVOIDED — ratio <= AVOIDED_EPS_RATIO (the predicted shortage did not
        materialise, allowing for noise-level residual): ``observed_deficit_qty``
        = 0, ``avoided_severity_usd`` = the predicted $ (NULL-honest if no cost
        basis). For the DEGENERATE predicted_qty None/<=0 case (no ratio
        possible), the same verdict applies when observed_qty <= AVOIDED_EPS_ABS
        (the noise-floor reading of "nothing meaningful observed").

      PARTIAL — AVOIDED_EPS_RATIO < ratio < MATERIALIZED_FLOOR_RATIO (reduced,
        not eliminated): ``avoided_severity_usd`` = the avoided FRACTION of the
        predicted $ = predicted_$ x (1 - ratio), NULL-honest.
    """
    reco_id = _coerce_uuid(reco_row["recommendation_id"])
    snapshot_id = (
        _coerce_uuid(snapshot_row["snapshot_id"]) if snapshot_row is not None else None
    )

    predicted_date = reco_row.get("shortage_date")
    predicted_qty = _as_decimal(reco_row.get("deficit_qty"))
    predicted_severity = _predicted_severity_usd(reco_row, predicted_qty)

    observed_qty = (
        observed_shortage.deficit_qty if observed_shortage is not None else Decimal(0)
    )

    status = str(reco_row.get("status", "")).upper()
    acted = status in ACTED_STATUSES

    # --- Branch 1: the reco never acted -> counter-factual, no credit. --------
    if not acted:
        return OutcomeRow(
            recommendation_id=reco_id,
            evaluated_as_of=evaluated_as_of,
            evaluation_status="NOT_APPLICABLE",
            predicted_shortage_date=predicted_date,
            predicted_deficit_qty=predicted_qty,
            # Cost-of-inaction signal: record the deficit that occurred (0 if
            # none). NOT credited to the reco (it did not act).
            observed_deficit_qty=observed_qty,
            avoided_severity_usd=None,
            snapshot_id=snapshot_id,
        )

    # --- Branch 2: acted, but no observation snapshot -> INDETERMINATE. -------
    # An ACTED reco needs a point-in-time observation to honestly assert the
    # deficit was avoided; without one, "no shortage row" is ambiguous (could be
    # not-yet-observed). Everything observed/avoided is None — honest.
    if snapshot_row is None:
        return OutcomeRow(
            recommendation_id=reco_id,
            evaluated_as_of=evaluated_as_of,
            evaluation_status="INDETERMINATE",
            predicted_shortage_date=predicted_date,
            predicted_deficit_qty=predicted_qty,
            observed_deficit_qty=None,
            avoided_severity_usd=None,
            snapshot_id=None,
        )

    # --- Branches 3-5: acted + observation present -> compare deficits. -------
    # Degenerate case FIRST: no predicted quantity to form a ratio against at
    # all. This is the ONLY place AVOIDED_EPS_ABS applies — "noise on an unknown
    # prediction" — and it is entirely separate from the ratio bands below (it
    # never re-enters them). A shortage below the absolute noise floor with no
    # prediction to compare is AVOIDED (nothing meaningful observed); at or
    # above it, a shortage exists with nothing proven avoided => MATERIALIZED.
    if predicted_qty is None or predicted_qty <= 0:
        if observed_qty <= AVOIDED_EPS_ABS:
            return OutcomeRow(
                recommendation_id=reco_id,
                evaluated_as_of=evaluated_as_of,
                evaluation_status="AVOIDED",
                predicted_shortage_date=predicted_date,
                predicted_deficit_qty=predicted_qty,
                observed_deficit_qty=Decimal(0),
                avoided_severity_usd=predicted_severity,
                snapshot_id=snapshot_id,
            )
        return OutcomeRow(
            recommendation_id=reco_id,
            evaluated_as_of=evaluated_as_of,
            evaluation_status="MATERIALIZED",
            predicted_shortage_date=predicted_date,
            predicted_deficit_qty=predicted_qty,
            observed_deficit_qty=observed_qty,
            avoided_severity_usd=Decimal(0),
            snapshot_id=snapshot_id,
        )

    # Ratio bands — pure, mutually exclusive, exhaustive over ratio in [0, +inf).
    # MATERIALIZED IS TESTED FIRST: a fully-materialized shortage (ratio == 1.0,
    # or anything >= the floor) must never be reachable via a later AVOIDED
    # check, regardless of predicted_qty's absolute scale (see the thresholds
    # section for the concrete defect this ordering fixes).
    ratio = observed_qty / predicted_qty

    if ratio >= MATERIALIZED_FLOOR_RATIO:
        # MATERIALIZED — happened essentially as predicted, not reduced.
        return OutcomeRow(
            recommendation_id=reco_id,
            evaluated_as_of=evaluated_as_of,
            evaluation_status="MATERIALIZED",
            predicted_shortage_date=predicted_date,
            predicted_deficit_qty=predicted_qty,
            observed_deficit_qty=observed_qty,
            avoided_severity_usd=Decimal(0),
            snapshot_id=snapshot_id,
        )

    if ratio <= AVOIDED_EPS_RATIO:
        # AVOIDED — the predicted shortage did not materialise (noise-level
        # residual at most).
        return OutcomeRow(
            recommendation_id=reco_id,
            evaluated_as_of=evaluated_as_of,
            evaluation_status="AVOIDED",
            predicted_shortage_date=predicted_date,
            predicted_deficit_qty=predicted_qty,
            observed_deficit_qty=Decimal(0),
            avoided_severity_usd=predicted_severity,
            snapshot_id=snapshot_id,
        )

    # PARTIAL — strictly between the two ratio thresholds: reduced but not
    # eliminated. Credit the avoided FRACTION of the predicted $ (proportional
    # to the deficit reduction). NULL-honest if no cost basis.
    avoided_partial: Optional[Decimal]
    if predicted_severity is None:
        avoided_partial = None
    else:
        avoided_partial = predicted_severity * (Decimal(1) - ratio)
    return OutcomeRow(
        recommendation_id=reco_id,
        evaluated_as_of=evaluated_as_of,
        evaluation_status="PARTIAL",
        predicted_shortage_date=predicted_date,
        predicted_deficit_qty=predicted_qty,
        observed_deficit_qty=observed_qty,
        avoided_severity_usd=avoided_partial,
        snapshot_id=snapshot_id,
    )


def _coerce_uuid(value: Any) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


# ---------------------------------------------------------------------------
# Orchestration — load eligible recos + observed shortages + snapshots, classify,
# upsert. Read-only on recommendations/shortages/inventory_snapshots (ADR-021);
# writes ONLY recommendation_outcomes. Does NOT commit (caller owns the txn).
# ---------------------------------------------------------------------------

_OUTCOME_COLUMNS: tuple[str, ...] = (
    "recommendation_id",
    "evaluated_as_of",
    "evaluation_status",
    "predicted_shortage_date",
    "predicted_deficit_qty",
    "observed_deficit_qty",
    "avoided_severity_usd",
    "snapshot_id",
)


def evaluate_and_persist(
    conn: DictRowConnection,
    scenario: str = BASELINE,
    evaluated_as_of: Optional[_dt.date] = None,
) -> dict[str, Any]:
    """Load, classify and upsert outcomes for every eligible reco in ``scenario``.

    Read-only on ``recommendations`` / ``shortages`` / ``inventory_snapshots``;
    writes ONLY ``recommendation_outcomes`` via an idempotent upsert (ON CONFLICT
    (recommendation_id, evaluated_as_of) DO UPDATE) — re-running for the same
    observation date overwrites the verdict, never duplicates. Does NOT commit.

    ``evaluated_as_of`` defaults to the DB CURRENT_DATE, resolved from the same
    connection so the observation day matches the scenario's own clock (as the
    MRP/DRP/snapshot loaders anchor theirs).

    The observed shortage for a reco is the MOST SEVERE active ``shortages`` row
    at or before ``evaluated_as_of`` for the reco's (item, location) coordinate;
    the observation snapshot is the coordinate's snapshot for that day. Both are
    resolved by the reco's raw ``item_id`` (+ ``location_id`` when the reco
    carries one — reschedule/transfer recos do; procurement recos may not, in
    which case the shortage is matched item-pooled: the WORST (max severity_usd)
    observation across the item's locations — see ``_load_observed_shortages``).

    Returns a metrics dict (evaluated / by_status / with_avoided_usd) for the
    caller to log / store.
    """
    if evaluated_as_of is None:
        cur = conn.cursor(row_factory=dict_row)
        today_row = cur.execute("SELECT CURRENT_DATE AS d").fetchone()
        if today_row is None:
            raise RuntimeError(
                "evaluate_and_persist: SELECT CURRENT_DATE yielded no row"
            )
        evaluated_as_of = today_row["d"]

    recos = _load_recommendations(conn, scenario)
    observed_by_coord = _load_observed_shortages(conn, scenario, evaluated_as_of)
    snapshots_by_coord = _load_snapshots(conn, scenario, evaluated_as_of)

    by_status: dict[str, int] = {s: 0 for s in sorted(VALID_STATUSES)}
    with_avoided_usd = 0
    rows: list[tuple[Any, ...]] = []

    for reco in recos:
        item_id = _coerce_uuid(reco["item_id"])
        loc_id = _coerce_uuid(reco["location_id"]) if reco.get("location_id") else None

        observed = _match_observed(observed_by_coord, item_id, loc_id)
        snapshot = _match_snapshot(snapshots_by_coord, item_id, loc_id)

        outcome = evaluate_outcome(reco, observed, snapshot, evaluated_as_of)
        by_status[outcome.evaluation_status] = (
            by_status.get(outcome.evaluation_status, 0) + 1
        )
        if outcome.avoided_severity_usd is not None:
            with_avoided_usd += 1

        rows.append(
            (
                outcome.recommendation_id,
                outcome.evaluated_as_of,
                outcome.evaluation_status,
                outcome.predicted_shortage_date,
                outcome.predicted_deficit_qty,
                outcome.observed_deficit_qty,
                outcome.avoided_severity_usd,
                outcome.snapshot_id,
            )
        )

    upserted = _upsert_outcomes(conn, rows)

    logger.info(
        "outcome.evaluate scenario=%s as_of=%s evaluated=%d upserted=%d by_status=%s",
        scenario, evaluated_as_of, len(rows), upserted, by_status,
    )

    return {
        "scenario_id": scenario,
        "evaluated_as_of": evaluated_as_of.isoformat(),
        "evaluated": len(rows),
        "upserted": upserted,
        "by_status": by_status,
        "with_avoided_usd": with_avoided_usd,
    }


def _load_recommendations(
    conn: DictRowConnection, scenario: str
) -> list[dict[str, Any]]:
    """SELECT the recos to evaluate for a scenario (read-only).

    Every reco with a shortage_date/deficit_qty is eligible — including DRAFT/
    REJECTED (they become NOT_APPLICABLE counter-factuals, feeding the
    cost-of-inaction KPI). The evidence JSONB is read for the $ basis.
    """
    cur = conn.cursor(row_factory=dict_row)
    # recommendations has NO bare location_id column: a shortage reco is
    # item-level (item_id + shortage_date), and only transfers carry
    # source/dest_location_id (migrations 039/061/066). The outcome matcher
    # resolves a reco with no location item-wise via the observed shortages'
    # (item_id, None) fallback key (reco.get("location_id") -> None). Selecting a
    # non-existent location_id here crashed evaluate_and_persist with
    # UndefinedColumn on a real DB.
    return cur.execute(
        """
        SELECT recommendation_id, scenario_id, item_id, item_external_id,
               shortage_date, deficit_qty, recommended_qty,
               estimated_cost, currency, action, decision_level, status,
               confidence, evidence
        FROM recommendations
        WHERE scenario_id = %s
        """,
        (scenario,),
    ).fetchall()


def _load_observed_shortages(
    conn: DictRowConnection, scenario: str, as_of: _dt.date
) -> dict[tuple[UUID, Optional[UUID]], ObservedShortage]:
    """Load the observed active shortages for a scenario AS OF ``as_of``.

    Reads the canonical ``shortages`` table (ADR-021 — READ ONLY). Keyed by
    (item_id, location_id) — the per-location observation, one row per
    coordinate, most severe active shortage at/before ``as_of`` (DISTINCT ON
    severity DESC) — AND (item_id, None), the item-POOLED fallback for a reco
    with no location (a shortage/procurement reco carries no location_id;
    reschedule/transfer recos do — see migrations 039/061/066).

    The pooled key is NOT derived from SQL row order (DISTINCT ON orders by
    (item_id, location_id), i.e. by location first — the row FOR THE LOWEST
    location_id, not the worst one, would land first per item; relying on
    "first seen wins" silently picked an ARBITRARY location's severity, which
    for an unlocated reco could match a nearly-resolved shortage at location A
    while location B fully materialized the SAME predicted deficit — a
    proof-machine MAJOR: crediting AVOIDED + full $ for a shortage that, pooled
    correctly, plainly happened). This function therefore computes the pooled
    entry in an EXPLICIT SECOND PASS over the already-fetched per-location rows,
    independent of SQL ordering: the item-pooled ``ObservedShortage`` is the one
    with MAX severity_usd across all of the item's locations — the single worst
    observation, which is the only defensible pooled reading for an unlocated
    reco (crediting AVOIDED against the item's WORST site, never its mildest).
    """
    cur = conn.cursor(row_factory=dict_row)
    rows = cur.execute(
        """
        SELECT DISTINCT ON (item_id, location_id)
               item_id, location_id, shortage_date, shortage_qty, severity_score
        FROM shortages
        WHERE scenario_id = %s
          AND status = 'active'
          AND shortage_date <= %s
          AND item_id IS NOT NULL
        ORDER BY item_id, location_id, severity_score DESC, shortage_date
        """,
        (scenario, as_of),
    ).fetchall()

    out: dict[tuple[UUID, Optional[UUID]], ObservedShortage] = {}
    per_item: dict[UUID, list[ObservedShortage]] = {}
    for r in rows:
        item_id = _coerce_uuid(r["item_id"])
        loc_id = _coerce_uuid(r["location_id"]) if r["location_id"] else None
        obs = ObservedShortage(
            item_id=item_id,
            location_id=loc_id,
            shortage_date=r["shortage_date"],
            deficit_qty=_as_decimal(r["shortage_qty"]) or Decimal(0),
            severity_usd=_as_decimal(r["severity_score"]) or Decimal(0),
        )
        out[(item_id, loc_id)] = obs
        per_item.setdefault(item_id, []).append(obs)

    # Second pass: the pooled (item, None) key is the MAX-severity observation
    # across every location of the item — explicit, order-independent.
    for item_id, observations in per_item.items():
        out[(item_id, None)] = max(observations, key=lambda o: o.severity_usd)
    return out


def _load_snapshots(
    conn: DictRowConnection, scenario: str, as_of: _dt.date
) -> dict[tuple[UUID, Optional[UUID]], dict[str, Any]]:
    """Load the observation snapshots for a scenario on ``as_of``.

    Reads ``inventory_snapshots`` (migration 067 — READ ONLY). Keyed by
    (item_id, location_id) — the per-location observation — AND an item-pooled
    (item_id, None) fallback so an unlocated reco still has an observation
    anchor. The snapshot's MERE PRESENCE is what lets an ACTED reco be
    classified rather than INDETERMINATE (see evaluate_outcome) — no field of
    the snapshot row itself is read by the classifier, so which location's
    snapshot backs the pooled key has NO effect on the verdict beyond "a
    snapshot exists for this item on this day".

    The pooled entry is nonetheless picked EXPLICITLY (an independent second
    pass over the per-location rows, by MAX as_of_date per item — the item's
    single MOST RECENT observation) rather than relying on SQL row order: the
    DISTINCT ON above orders by (item_id, location_id), i.e. by location first,
    so "first row per item" would be an ARBITRARY (lowest-location-id)
    snapshot, not the latest one. Correctness of the verdict does not depend on
    this choice today (presence-only gate), but the comment must describe what
    the code actually does, not an SQL-order accident.
    """
    cur = conn.cursor(row_factory=dict_row)
    rows = cur.execute(
        """
        SELECT DISTINCT ON (item_id, location_id)
               snapshot_id, item_id, location_id, as_of_date
        FROM inventory_snapshots
        WHERE scenario_id = %s
          AND as_of_date <= %s
        ORDER BY item_id, location_id, as_of_date DESC
        """,
        (scenario, as_of),
    ).fetchall()

    out: dict[tuple[UUID, Optional[UUID]], dict[str, Any]] = {}
    per_item: dict[UUID, list[dict[str, Any]]] = {}
    for r in rows:
        item_id = _coerce_uuid(r["item_id"])
        loc_id = _coerce_uuid(r["location_id"]) if r["location_id"] else None
        out[(item_id, loc_id)] = r
        per_item.setdefault(item_id, []).append(r)

    # Second pass: the pooled (item, None) key is the MOST RECENT snapshot
    # (max as_of_date) across every location of the item — explicit,
    # order-independent.
    for item_id, snap_rows in per_item.items():
        out[(item_id, None)] = max(snap_rows, key=lambda s: s["as_of_date"])
    return out


def _match_observed(
    observed_by_coord: dict[tuple[UUID, Optional[UUID]], ObservedShortage],
    item_id: UUID,
    loc_id: Optional[UUID],
) -> Optional[ObservedShortage]:
    """Resolve the observed shortage for a reco coordinate: exact (item, loc)
    first, then the item-pooled fallback for an unlocated reco."""
    if loc_id is not None:
        hit = observed_by_coord.get((item_id, loc_id))
        if hit is not None:
            return hit
    return observed_by_coord.get((item_id, None))


def _match_snapshot(
    snapshots_by_coord: dict[tuple[UUID, Optional[UUID]], dict[str, Any]],
    item_id: UUID,
    loc_id: Optional[UUID],
) -> Optional[dict[str, Any]]:
    """Resolve the observation snapshot for a reco coordinate: exact (item, loc)
    first, then the item-pooled fallback."""
    if loc_id is not None:
        hit = snapshots_by_coord.get((item_id, loc_id))
        if hit is not None:
            return hit
    return snapshots_by_coord.get((item_id, None))


def _upsert_outcomes(
    conn: DictRowConnection, rows: list[tuple[Any, ...]]
) -> int:
    """Idempotently upsert outcome rows into ``recommendation_outcomes``.

    ON CONFLICT (recommendation_id, evaluated_as_of) DO UPDATE — re-evaluating
    the same reco for the same observation date overwrites the verdict + $
    figures and re-stamps ``evaluated_at``, never duplicates. Static column list,
    parameterized values (the house SQL idiom). Returns the number of rows
    written. Does NOT commit.
    """
    if not rows:
        return 0
    written = 0
    for row in rows:
        conn.execute(
            """
            INSERT INTO recommendation_outcomes
                (recommendation_id, evaluated_as_of, evaluation_status,
                 predicted_shortage_date, predicted_deficit_qty,
                 observed_deficit_qty, avoided_severity_usd, snapshot_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (recommendation_id, evaluated_as_of) DO UPDATE SET
                evaluation_status       = EXCLUDED.evaluation_status,
                predicted_shortage_date = EXCLUDED.predicted_shortage_date,
                predicted_deficit_qty   = EXCLUDED.predicted_deficit_qty,
                observed_deficit_qty    = EXCLUDED.observed_deficit_qty,
                avoided_severity_usd    = EXCLUDED.avoided_severity_usd,
                snapshot_id             = EXCLUDED.snapshot_id,
                evaluated_at            = now()
            """,
            row,
        )
        written += 1
    return written
