"""
/v1/outcomes + /v1/recommendations/{id}/outcome — the proof-of-value surface
(chantier #393 A3-PR2, ADR-030).

The HTTP surface over the recommendation-outcome backbone (engine/outcome +
``recommendation_outcomes``, migration 069):

  * ``GET /v1/recommendations/{id}/outcome`` — the latest observed outcome of
    one recommendation (its deterministic verdict + the NULL-honest $ figures).
  * ``GET /v1/outcomes/summary`` — the FIVE proof KPIs (SQL aggregates), scoped
    to a scenario. Every KPI is NULL/0-honest: NULL distinguishes "no data to
    compute this" from a real 0. The ``from``/``to`` window is an OBSERVATION
    window on ``evaluated_as_of`` (recommendation_outcomes) — it filters KPIs
    1/2/5 (which read that table) but NOT KPI 3 (Pyramide run accuracy) or
    KPI 4 (reco approval rate), which have no ``evaluated_as_of`` of their own
    and are scenario-scope-LIFETIME by construction (see the per-KPI note
    below and each KPI's own docstring — the contract is documented, not a
    silent mix of windowed/unwindowed data).
  * ``POST /v1/outcomes/evaluate`` — governed trigger of an evaluation pass
    (scope ``ingest``, kill switch ``OOTILS_OUTCOMES_ENABLED``). Writes verdicts
    into ``recommendation_outcomes`` (idempotent). The CLI
    (scripts/evaluate_outcomes.py) is the cron path; this endpoint is the
    on-demand one.

SCOPES (chantier #392):
  * The two GETs require ``read`` — plain query paths over already-computed
    outcomes.
  * POST /evaluate requires ``ingest`` — it WRITES persistent operational rows
    (verdicts), the same class of action as the snapshot capture it depends on
    (api/routers/snapshots.py holds ``ingest`` for the same reason). A write must
    not ride a read scope. The legacy admin token satisfies ``ingest``.

SCENARIO_ID ON EVERY READ PATH (North Star): the summary resolves scenario_id
(``Depends(resolve_scenario_id)``) and slices its aggregates by the reco's
scenario (``recommendations.scenario_id`` — an outcome inherits its scenario
through its reco, migration 069 carries no scenario_id of its own). V1 evaluates
baseline only, but the read surface is scenario-scoped by construction.

KPI 3 (FVA): the summary reports ``avg_fva_wape`` — the mean of the real
Forecast-Value-Added on the WAPE axis (``pyramide_accuracy_metrics.fva_wape`` =
naive_wape − wape, migration 068), over the aggregate (all-horizons) rows of the
runs in scope. NULL-honest: rows whose ``fva_wape`` is NULL (naive not
computable, or a stat operand NULL) are ignored, and the KPI is None when no
non-NULL aggregate ``fva_wape`` exists. A positive value means the statistical
model beats a trivial seasonal-naive baseline; a negative value is a legitimate,
un-clamped result (the naive can win on a strongly seasonal series).
NOT windowed by ``from``/``to`` — a Pyramide run has no ``evaluated_as_of``;
scoping is scenario-only, over EVERY run in the scenario's history.

KPI 4 (approval rate): scoped by ``recommendations.scenario_id`` only, NOT
windowed by ``from``/``to`` — the denormalized ``status`` column carries no
``evaluated_as_of`` either (a reco's lifetime approval state, not an
observation). Windowing it would require the reco's OWN emission/transition
date, a different axis than the outcome observation window; deferred rather
than silently approximated.

Kill switch: ``OOTILS_OUTCOMES_ENABLED`` (default enabled). Falsy → 503 on the
evaluate verb, checked AFTER auth/scope but BEFORE the DB pool — mirrors
api/routers/snapshots.py / drp.py.
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
from typing import Any, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from pydantic import BaseModel, Field

from ootils_core.api.auth import Principal, require_scope
from ootils_core.api.dependencies import get_db, resolve_scenario_id
from ootils_core.db.types import DictRowConnection
from ootils_core.engine.outcome import evaluate_and_persist

logger = logging.getLogger(__name__)

router = APIRouter(tags=["outcomes"])

_TRUTHY = {"1", "true", "yes", "on"}


def _outcomes_enabled() -> bool:
    """Kill switch, default ON. Falsy OOTILS_OUTCOMES_ENABLED -> 503 on evaluate."""
    return os.environ.get("OOTILS_OUTCOMES_ENABLED", "1").strip().lower() in _TRUTHY


def require_outcomes_enabled() -> None:
    """FastAPI dependency — checked AFTER auth/scope but BEFORE ``Depends(get_db)``
    in the evaluate endpoint (dependencies resolve in signature order and
    short-circuit on the first HTTPException). Auth-first so an unauthenticated
    caller always gets 401 and cannot probe the switch; kill-switch-before-DB so
    a disabled evaluator answers 503 without touching the DB pool."""
    if not _outcomes_enabled():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Outcome evaluation is disabled (OOTILS_OUTCOMES_ENABLED).",
        )


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class OutcomeOut(BaseModel):
    """One recommendation-outcome verdict.

    NULL-honest: ``avoided_severity_usd`` is None when NOT computable (no cost
    basis / INDETERMINATE / a reco that never acted), never a masked 0. The
    ``predicted_*`` are None when the reco carried no such figure."""

    outcome_id: UUID
    recommendation_id: UUID
    evaluated_as_of: _dt.date
    evaluation_status: str
    predicted_shortage_date: Optional[_dt.date] = None
    predicted_deficit_qty: Optional[float] = None
    observed_deficit_qty: Optional[float] = None
    avoided_severity_usd: Optional[float] = None
    snapshot_id: Optional[UUID] = None
    evaluated_at: _dt.datetime


class OutcomeSummaryOut(BaseModel):
    """The five proof KPIs over a scenario. ``from_date``/``to_date`` echo the
    OUTCOME observation window applied to KPI 1/2/5 only — KPI 3/4 are always
    scenario-scope-lifetime regardless of the window (see the router docstring).

    Every field is NULL/0-honest: None means "no data to compute this KPI"
    (e.g. zero acted recos for the rate, no WAPE rows for the accuracy), a real
    number (including 0) means the KPI computed to that value. The ``*_basis``
    counters make the denominators explicit so a consumer can tell a genuine 0
    from an empty set."""

    scenario_id: UUID
    from_date: Optional[_dt.date] = None
    to_date: Optional[_dt.date] = None

    # KPI 1 — share of acted-on shortages that were avoided.
    pct_shortages_avoided: Optional[float] = None
    avoided_basis_count: int = Field(
        ..., description="AVOIDED+MATERIALIZED+PARTIAL on acted recos (KPI 1 denominator)."
    )
    # KPI 2 — total $ proven avoided.
    avoided_severity_usd_total: Optional[float] = None
    # KPI 3 — mean Forecast-Value-Added (fva_wape = naive_wape − wape, mig 068)
    # of the aggregate rows in scope; positive = model beats seasonal-naive.
    avg_fva_wape: Optional[float] = None
    fva_basis_count: int = Field(
        ..., description="Non-NULL aggregate WAPE rows averaged (KPI 3 denominator)."
    )
    # KPI 4 — reco approval rate.
    reco_approval_rate: Optional[float] = None
    reco_total_count: int = Field(
        ..., description="Total recos emitted in scope (KPI 4 denominator)."
    )
    # KPI 5 — $ cost of inaction (materialised shortages on never-approved recos).
    cost_of_inaction_usd: Optional[float] = None


class OutcomeEvaluateRequest(BaseModel):
    """Body of POST /v1/outcomes/evaluate. scenario_id is resolved from the query
    param / X-Scenario-ID header (Depends(resolve_scenario_id)), NOT the body."""

    as_of: Optional[_dt.date] = Field(
        default=None,
        description="Observation day (YYYY-MM-DD). Defaults to the DB CURRENT_DATE.",
    )


class OutcomeEvaluateResponse(BaseModel):
    """Response from an evaluation pass."""

    scenario_id: UUID
    evaluated_as_of: _dt.date
    evaluated: int
    upserted: int
    with_avoided_usd: int
    by_status: dict[str, int]


# ---------------------------------------------------------------------------
# GET /v1/recommendations/{id}/outcome — one reco's latest verdict
# ---------------------------------------------------------------------------


@router.get(
    "/v1/recommendations/{recommendation_id}/outcome",
    response_model=OutcomeOut,
    summary="Get a recommendation's observed outcome",
    description=(
        "The latest observed outcome of one recommendation: its deterministic "
        "verdict (AVOIDED / MATERIALIZED / PARTIAL / NOT_APPLICABLE / "
        "INDETERMINATE) plus the NULL-honest predicted/observed/avoided $ "
        "figures. 404 if the reco has no outcome yet."
    ),
)
def get_recommendation_outcome(
    recommendation_id: UUID = Path(..., description="The recommendation UUID."),
    _principal: Principal = Depends(require_scope("read")),
    db: DictRowConnection = Depends(get_db),
) -> OutcomeOut:
    """Return the most recent outcome for a reco (by evaluated_as_of DESC).

    Read-only. If several observation dates were evaluated, the newest verdict
    is returned (the latest word on this reco). 404 when none exists."""
    row = db.execute(
        """
        SELECT outcome_id, recommendation_id, evaluated_as_of, evaluation_status,
               predicted_shortage_date, predicted_deficit_qty, observed_deficit_qty,
               avoided_severity_usd, snapshot_id, evaluated_at
        FROM recommendation_outcomes
        WHERE recommendation_id = %s
        ORDER BY evaluated_as_of DESC, evaluated_at DESC
        LIMIT 1
        """,
        (recommendation_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No outcome recorded for recommendation {recommendation_id}",
        )
    return _row_to_outcome(row)


# ---------------------------------------------------------------------------
# GET /v1/outcomes/summary — the five proof KPIs
# ---------------------------------------------------------------------------


@router.get(
    "/v1/outcomes/summary",
    response_model=OutcomeSummaryOut,
    summary="Proof-of-value KPI summary",
    description=(
        "The five proof KPIs for a scenario: pct_shortages_avoided, "
        "avoided_severity_usd_total, avg_fva_wape (see FVA caveat), "
        "reco_approval_rate, cost_of_inaction_usd. Every KPI is NULL/0-honest "
        "(NULL = no data, 0 = genuine zero). Fully parameterized SQL, "
        "scenario-scoped. `from`/`to` window recommendation_outcomes.evaluated_as_of "
        "and therefore filter ONLY KPIs 1/2/5 (avoided rate, avoided $, cost of "
        "inaction); KPI 3 (Pyramide run accuracy) and KPI 4 (reco approval rate) "
        "have no evaluated_as_of of their own and are ALWAYS scenario-scope-"
        "lifetime, window or not."
    ),
)
def outcomes_summary(
    _principal: Principal = Depends(require_scope("read")),
    scenario_id: UUID = Depends(resolve_scenario_id),
    from_: Optional[_dt.date] = Query(
        default=None, alias="from",
        description=(
            "Window start (inclusive) on the OUTCOME observation date "
            "(recommendation_outcomes.evaluated_as_of). Filters KPIs 1/2/5 only — "
            "KPI 3/4 have no evaluated_as_of and ignore this filter (always "
            "scenario-scope-lifetime)."
        ),
    ),
    to: Optional[_dt.date] = Query(
        default=None,
        description=(
            "Window end (inclusive) on the OUTCOME observation date "
            "(recommendation_outcomes.evaluated_as_of). Filters KPIs 1/2/5 only — "
            "KPI 3/4 have no evaluated_as_of and ignore this filter (always "
            "scenario-scope-lifetime)."
        ),
    ),
    db: DictRowConnection = Depends(get_db),
) -> OutcomeSummaryOut:
    """Compute the five proof KPIs. scenario_id always constrains the aggregates
    (North Star: every read path is scenario-scoped) via the reco's own
    scenario_id (an outcome inherits it). The observation window (from/to) is an
    optional inclusive filter on ``recommendation_outcomes.evaluated_as_of`` —
    it applies ONLY to KPI 1/2/5 (``_kpi_outcome_stats``, which reads that
    table). KPI 3 (``_kpi_avg_fva_wape``) and KPI 4 (``_kpi_approval_rate``) have
    no ``evaluated_as_of`` of their own (a Pyramide run / a reco's current
    status carry no observation date) and are ALWAYS scenario-scope-lifetime,
    window or not — a deliberate, documented asymmetry (see the module
    docstring), never a silent mix of windowed and unwindowed data. All SQL is
    parameterized; the assembled window predicates are static ``col <= %s``
    fragments with their values bound positionally — never caller data in the
    SQL text."""
    scenario_str = str(scenario_id)

    outcome_stats = _kpi_outcome_stats(db, scenario_str, from_, to)
    wape_stats = _kpi_avg_fva_wape(db, scenario_str)
    approval_stats = _kpi_approval_rate(db, scenario_str)

    return OutcomeSummaryOut(
        scenario_id=scenario_id,
        from_date=from_,
        to_date=to,
        pct_shortages_avoided=outcome_stats["pct_shortages_avoided"],
        avoided_basis_count=outcome_stats["avoided_basis_count"],
        avoided_severity_usd_total=outcome_stats["avoided_severity_usd_total"],
        avg_fva_wape=wape_stats["avg_fva_wape"],
        fva_basis_count=wape_stats["fva_basis_count"],
        reco_approval_rate=approval_stats["reco_approval_rate"],
        reco_total_count=approval_stats["reco_total_count"],
        cost_of_inaction_usd=outcome_stats["cost_of_inaction_usd"],
    )


# ---------------------------------------------------------------------------
# POST /v1/outcomes/evaluate — governed on-demand evaluation
# ---------------------------------------------------------------------------


@router.post(
    "/v1/outcomes/evaluate",
    response_model=OutcomeEvaluateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Evaluate recommendation outcomes",
    description=(
        "Classify every eligible recommendation's observed outcome for `as_of` "
        "(default CURRENT_DATE) and upsert the verdicts (idempotent: a re-run "
        "for the same day overwrites, never duplicates). Read-only on "
        "recommendations/shortages/inventory_snapshots (ADR-021); writes only "
        "recommendation_outcomes. Requires the `ingest` scope."
    ),
)
def evaluate_outcomes(
    body: OutcomeEvaluateRequest,
    _principal: Principal = Depends(require_scope("ingest")),
    _enabled: None = Depends(require_outcomes_enabled),
    scenario_id: UUID = Depends(resolve_scenario_id),
    db: DictRowConnection = Depends(get_db),
) -> OutcomeEvaluateResponse:
    """Run an evaluation pass for a scenario. get_db owns commit/rollback; the
    engine's ``evaluate_and_persist`` is read-only on its inputs and the single
    idempotent writer of ``recommendation_outcomes``."""
    summary = evaluate_and_persist(db, str(scenario_id), body.as_of)
    evaluated_as_of = _dt.date.fromisoformat(summary["evaluated_as_of"])

    logger.info(
        "outcomes.evaluate scenario_id=%s as_of=%s evaluated=%d upserted=%d",
        scenario_id, evaluated_as_of, summary["evaluated"], summary["upserted"],
    )

    return OutcomeEvaluateResponse(
        scenario_id=scenario_id,
        evaluated_as_of=evaluated_as_of,
        evaluated=summary["evaluated"],
        upserted=summary["upserted"],
        with_avoided_usd=summary["with_avoided_usd"],
        by_status=summary["by_status"],
    )


# ---------------------------------------------------------------------------
# KPI SQL — each fully parameterized, NULL/0-honest.
# ---------------------------------------------------------------------------


def _window_predicates(
    from_: Optional[_dt.date], to: Optional[_dt.date]
) -> tuple[str, list[Any]]:
    """Build the optional observation-window WHERE fragment for the outcome
    aggregates. Returns (sql_fragment, params). Static ``o.evaluated_as_of``
    predicates, values bound positionally — never caller data in the text."""
    conds: list[str] = []
    params: list[Any] = []
    if from_ is not None:
        conds.append("o.evaluated_as_of >= %s")
        params.append(from_)
    if to is not None:
        conds.append("o.evaluated_as_of <= %s")
        params.append(to)
    fragment = (" AND " + " AND ".join(conds)) if conds else ""
    return fragment, params


def _kpi_outcome_stats(
    db: DictRowConnection,
    scenario: str,
    from_: Optional[_dt.date],
    to: Optional[_dt.date],
) -> dict[str, Any]:
    """KPIs 1, 2 and 5 in one scan over recommendation_outcomes x recommendations.

    KPI 1 — pct_shortages_avoided = AVOIDED / (AVOIDED+MATERIALIZED+PARTIAL) over
      ACTED (APPROVED/APPLIED) recos. NULL when the denominator is 0 (no acted
      shortage outcome to grade) — NULLIF guards the divide.
    KPI 2 — avoided_severity_usd_total = SUM(avoided_severity_usd). COALESCE to 0
      only when at least one avoided-$ row exists; NULL-honest when NONE do (no
      avoided-$ ever computed => NULL, not a masked 0). The FILTERed count
      distinguishes the two.
    KPI 5 — cost_of_inaction_usd = SUM(observed_deficit_qty x reco unit_cost) over
      NOT_APPLICABLE outcomes on NEVER-APPROVED recos that DID materialise
      (observed_deficit_qty > 0). NULL when no such row exists.

    All three share the reco join (for status + the $ basis) and the scenario /
    window filter. Parameterized throughout; the only assembled text is the
    static window fragment.
    """
    window_sql, window_params = _window_predicates(from_, to)
    # The reco unit-cost basis mirrors the evaluator's _predicted_unit_cost
    # EXACTLY (including its ``uc > 0`` guard — a negative/zero unit_cost is
    # rejected there and MUST be rejected here too, or the same reco values
    # differently on the Python side vs this SQL side): evidence->>'unit_cost'
    # first (ONLY when > 0), else estimated_cost / recommended_qty. Kept in SQL
    # here so KPI 5 (cost of inaction on never-approved recos) values the
    # materialised deficit with the SAME per-unit basis the evaluator used for
    # the avoided-$ side — one valuation convention, both directions. The
    # evidence cast is GUARDED by a numeric-shape regex: a malformed unit_cost
    # (non-numeric string) would otherwise abort the whole aggregate — instead
    # it is ignored and the estimated_cost/qty basis takes over (NULL-honest).
    sql = (
        """
        WITH scored AS (
            SELECT
                o.evaluation_status,
                o.observed_deficit_qty,
                o.avoided_severity_usd,
                r.status AS reco_status,
                COALESCE(
                    CASE WHEN (r.evidence->>'unit_cost') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                              AND (r.evidence->>'unit_cost')::numeric > 0
                         THEN (r.evidence->>'unit_cost')::numeric END,
                    CASE WHEN r.recommended_qty > 0
                         THEN r.estimated_cost / r.recommended_qty END
                ) AS unit_cost
            FROM recommendation_outcomes o
            JOIN recommendations r ON r.recommendation_id = o.recommendation_id
            WHERE r.scenario_id = %s
        """
        + window_sql
        + """
        )
        SELECT
            -- KPI 1 denominator: acted shortage outcomes.
            COUNT(*) FILTER (
                WHERE reco_status IN ('APPROVED', 'APPLIED')
                  AND evaluation_status IN ('AVOIDED', 'MATERIALIZED', 'PARTIAL')
            ) AS avoided_basis_count,
            COUNT(*) FILTER (
                WHERE reco_status IN ('APPROVED', 'APPLIED')
                  AND evaluation_status = 'AVOIDED'
            ) AS avoided_count,
            -- KPI 2: total avoided $, and how many rows contributed one.
            COUNT(*) FILTER (WHERE avoided_severity_usd IS NOT NULL) AS avoided_usd_rows,
            SUM(avoided_severity_usd) AS avoided_severity_usd_total,
            -- KPI 5: cost of inaction on never-approved, materialised recos.
            COUNT(*) FILTER (
                WHERE evaluation_status = 'NOT_APPLICABLE'
                  AND reco_status NOT IN ('APPROVED', 'APPLIED')
                  AND observed_deficit_qty IS NOT NULL
                  AND observed_deficit_qty > 0
                  AND unit_cost IS NOT NULL
            ) AS inaction_rows,
            SUM(
                CASE WHEN evaluation_status = 'NOT_APPLICABLE'
                      AND reco_status NOT IN ('APPROVED', 'APPLIED')
                      AND observed_deficit_qty IS NOT NULL
                      AND observed_deficit_qty > 0
                      AND unit_cost IS NOT NULL
                     THEN observed_deficit_qty * unit_cost END
            ) AS cost_of_inaction_usd
        FROM scored
        """
    )
    params: list[Any] = [scenario, *window_params]
    row = db.execute(sql, params).fetchone()
    if row is None:  # pragma: no cover — an aggregate always returns one row
        return {
            "pct_shortages_avoided": None,
            "avoided_basis_count": 0,
            "avoided_severity_usd_total": None,
            "cost_of_inaction_usd": None,
        }

    basis = int(row["avoided_basis_count"] or 0)
    avoided = int(row["avoided_count"] or 0)
    pct = round(avoided / basis, 6) if basis > 0 else None

    avoided_usd_rows = int(row["avoided_usd_rows"] or 0)
    avoided_total = (
        float(row["avoided_severity_usd_total"]) if avoided_usd_rows > 0 else None
    )

    inaction_rows = int(row["inaction_rows"] or 0)
    inaction = (
        float(row["cost_of_inaction_usd"]) if inaction_rows > 0 else None
    )

    return {
        "pct_shortages_avoided": pct,
        "avoided_basis_count": basis,
        "avoided_severity_usd_total": avoided_total,
        "cost_of_inaction_usd": inaction,
    }


def _kpi_avg_fva_wape(db: DictRowConnection, scenario: str) -> dict[str, Any]:
    """KPI 3 — mean Forecast-Value-Added (fva_wape) of the Pyramide runs.

    Joins pyramide_accuracy_metrics (aggregate row: horizon IS NULL) to
    pyramide_runs for the scenario filter. NULL-honest: only non-NULL WAPE rows
    are averaged (a NULL wape means "not computable on this data" per the
    accuracy contract — NEVER a masked 0), and the whole KPI is None when NO
    non-NULL aggregate WAPE row exists for the scenario.

    The REAL Forecast-Value-Added on the WAPE axis: ``fva_wape`` = naive_wape −
    wape (migration 068). Positive = the model beats a trivial seasonal-naive;
    negative is a legitimate un-clamped result. NULL-honest: NULL ``fva_wape``
    rows (naive not computable) are ignored, and the KPI is None when none exist.

    NOT windowed by from/to (deliberately — see the module + endpoint
    docstrings): a Pyramide run has no ``evaluated_as_of``. Scoped by
    ``scenario`` alone, over the ENTIRE run history of the scenario.
    """
    row = db.execute(
        """
        SELECT AVG(m.fva_wape) AS avg_fva_wape,
               COUNT(m.fva_wape) AS fva_basis_count
        FROM pyramide_accuracy_metrics m
        JOIN pyramide_runs pr ON pr.run_id = m.run_id
        WHERE pr.scenario_id = %s
          AND m.horizon IS NULL
          AND m.fva_wape IS NOT NULL
        """,
        (scenario,),
    ).fetchone()
    if row is None:  # pragma: no cover — aggregate always returns a row
        return {"avg_fva_wape": None, "fva_basis_count": 0}
    count = int(row["fva_basis_count"] or 0)
    return {
        "avg_fva_wape": float(row["avg_fva_wape"]) if count > 0 else None,
        "fva_basis_count": count,
    }


def _kpi_approval_rate(db: DictRowConnection, scenario: str) -> dict[str, Any]:
    """KPI 4 — reco approval rate = APPROVED(+APPLIED) / total recos emitted.

    Uses the denormalized ``recommendations.status`` (an APPLIED reco was
    approved first, so both count as approved). NULL-honest: None when NO reco
    was emitted in the scenario (empty set), a real ratio (including 0.0)
    otherwise. Scenario-scoped.

    NOT windowed by from/to (deliberately — see the module + endpoint
    docstrings): ``recommendations.status`` is a current-state column with no
    ``evaluated_as_of`` of its own. Scoped by ``scenario`` alone, over the
    ENTIRE reco history of the scenario.
    """
    row = db.execute(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status IN ('APPROVED', 'APPLIED')) AS approved
        FROM recommendations
        WHERE scenario_id = %s
        """,
        (scenario,),
    ).fetchone()
    if row is None:  # pragma: no cover
        return {"reco_approval_rate": None, "reco_total_count": 0}
    total = int(row["total"] or 0)
    approved = int(row["approved"] or 0)
    return {
        "reco_approval_rate": round(approved / total, 6) if total > 0 else None,
        "reco_total_count": total,
    }


def _row_to_outcome(row: dict[str, Any]) -> OutcomeOut:
    pdq = row["predicted_deficit_qty"]
    odq = row["observed_deficit_qty"]
    avoided = row["avoided_severity_usd"]
    return OutcomeOut(
        outcome_id=UUID(str(row["outcome_id"])),
        recommendation_id=UUID(str(row["recommendation_id"])),
        evaluated_as_of=row["evaluated_as_of"],
        evaluation_status=row["evaluation_status"],
        predicted_shortage_date=row["predicted_shortage_date"],
        predicted_deficit_qty=float(pdq) if pdq is not None else None,
        observed_deficit_qty=float(odq) if odq is not None else None,
        avoided_severity_usd=float(avoided) if avoided is not None else None,
        snapshot_id=UUID(str(row["snapshot_id"])) if row["snapshot_id"] else None,
        evaluated_at=row["evaluated_at"],
    )
