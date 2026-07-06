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
# AVOIDED vs PARTIAL vs MATERIALIZED is decided by comparing the observed
# deficit against the predicted one. Two knobs, both on the OBSERVED/predicted
# ratio so they are unit-free and scale-free:
#
#   * AVOIDED_EPS_RATIO — observed <= predicted * this => the deficit is
#     effectively gone (AVOIDED). Default 0.05: a residual under 5% of what was
#     predicted is noise, not a surviving shortage. (An absolute floor,
#     AVOIDED_EPS_ABS, additionally treats a sub-unit residual as zero so a tiny
#     predicted deficit is not held to an impossibly tight relative bar.)
#   * MATERIALIZED_FLOOR_RATIO — observed >= predicted * this => the shortage
#     happened essentially as predicted (MATERIALIZED, no meaningful reduction).
#     Default 0.90: within 10% of the prediction is "not reduced".
#
# Between the two (AVOIDED_EPS_RATIO < observed/predicted < MATERIALIZED_FLOOR_
# RATIO) the deficit was genuinely reduced but not eliminated => PARTIAL.
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

      AVOIDED — an ACTED reco, snapshot present, observed deficit effectively
        zero (<= max(predicted x AVOIDED_EPS_RATIO, AVOIDED_EPS_ABS)):
        ``observed_deficit_qty`` = 0, ``avoided_severity_usd`` = the predicted $
        (NULL-honest if no cost basis).

      MATERIALIZED — an ACTED reco, snapshot present, observed deficit >=
        predicted x MATERIALIZED_FLOOR_RATIO (the shortage happened anyway, not
        meaningfully reduced): ``avoided_severity_usd`` = 0 (genuinely nothing
        avoided — distinct from NULL "not computable").

      PARTIAL — an ACTED reco, snapshot present, observed deficit strictly
        between the AVOIDED ceiling and the MATERIALIZED floor (reduced, not
        eliminated): ``avoided_severity_usd`` = the avoided FRACTION of the
        predicted $ = predicted_$ x (1 - observed/predicted), NULL-honest.
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
    avoided_ceiling = _avoided_ceiling(predicted_qty)

    if observed_qty <= avoided_ceiling:
        # AVOIDED — the predicted shortage did not materialise.
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

    # From here observed_qty > 0. If we have no predicted quantity to compare
    # against, we cannot grade AVOIDED/PARTIAL/MATERIALIZED — a shortage exists
    # but its relation to the (absent) prediction is unknowable => MATERIALIZED
    # is the prudent honest call (a shortage happened; nothing proven avoided).
    if predicted_qty is None or predicted_qty <= 0:
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

    # PARTIAL — reduced but not eliminated. Credit the avoided FRACTION of the
    # predicted $ (proportional to the deficit reduction). NULL-honest if no
    # cost basis.
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


def _avoided_ceiling(predicted_qty: Optional[Decimal]) -> Decimal:
    """The observed-deficit ceiling below which a shortage counts as AVOIDED.

    max(predicted x AVOIDED_EPS_RATIO, AVOIDED_EPS_ABS): a relative floor (5% of
    the prediction) OR an absolute sub-unit floor, whichever is larger — so a
    tiny predicted deficit is not held to an impossibly tight relative bar, and a
    large one still tolerates a small residual. With no prediction the absolute
    floor applies alone.
    """
    if predicted_qty is None or predicted_qty <= 0:
        return AVOIDED_EPS_ABS
    return max(predicted_qty * AVOIDED_EPS_RATIO, AVOIDED_EPS_ABS)


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

    The observed shortage for a reco is the MOST RECENT active ``shortages`` row
    at or before ``evaluated_as_of`` for the reco's (item, location) coordinate;
    the observation snapshot is the coordinate's snapshot for that day. Both are
    resolved by the reco's raw ``item_id`` (+ ``location_id`` when the reco
    carries one — reschedule/transfer recos do; procurement recos may not, in
    which case the shortage is matched on item alone, item-pooled, matching the
    canonical shortage grain).

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
    return cur.execute(
        """
        SELECT recommendation_id, scenario_id, item_id, item_external_id,
               location_id, shortage_date, deficit_qty, recommended_qty,
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
    (item_id, location_id) AND (item_id, None) so a reco with or without a
    location resolves. The most severe active shortage per coordinate at/before
    ``as_of`` is kept (DISTINCT ON severity DESC) — an observed shortage is a
    materialised deficit; the worst one is the honest observation.
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
        # Item-pooled fallback: keep the worst per item so an unlocated reco
        # resolves. Rows are severity-desc within an item, so the first seen is
        # the worst — do not overwrite with a lesser one.
        pooled_key = (item_id, None)
        if pooled_key not in out:
            out[pooled_key] = obs
    return out


def _load_snapshots(
    conn: DictRowConnection, scenario: str, as_of: _dt.date
) -> dict[tuple[UUID, Optional[UUID]], dict[str, Any]]:
    """Load the observation snapshots for a scenario on ``as_of``.

    Reads ``inventory_snapshots`` (migration 067 — READ ONLY). Keyed by
    (item_id, location_id) and an item-pooled (item_id, None) fallback (latest
    snapshot for the item) so an unlocated reco still has an observation anchor.
    The snapshot's presence is what lets an ACTED reco be classified rather than
    INDETERMINATE.
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
    for r in rows:
        item_id = _coerce_uuid(r["item_id"])
        loc_id = _coerce_uuid(r["location_id"]) if r["location_id"] else None
        out[(item_id, loc_id)] = r
        pooled_key = (item_id, None)
        if pooled_key not in out:
            out[pooled_key] = r
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
