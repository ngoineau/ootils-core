from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Sequence
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from ootils_core.api.dependencies import BASELINE_SCENARIO_ID

from .accuracy import AccuracyReport
from .models import PyramideRunResult
from .routing import RoutingDecision

logger = logging.getLogger(__name__)


class PyramideAggregateCommitError(ValueError):
    """Commit requested on an AGGREGATE (hierarchy-node) run.

    Aggregate forecasts are never materialized as graph demand: the graph
    only carries leaf (item, location) ForecastDemand nodes; aggregates
    stay queryable in forecasts / forecast_values (migration 053). The
    router maps this to a 409.
    """


@dataclass(frozen=True)
class PyramidePersistedRun:
    # snapshot_id is None for AGGREGATE runs: pyramide_snapshots keeps its
    # leaf NOT NULL contract (migration 038) — snapshots exist to feed the
    # graph commit path, which is leaf-only by design.
    run_id: UUID
    snapshot_id: UUID | None
    forecast_id: UUID


@dataclass(frozen=True)
class PyramideRunSummary:
    """
    Leaf runs carry (item_id, location_id); aggregate runs (migration 053)
    carry (hierarchy_id, level, node_code) instead — the unused side is
    None (DB CHECK: leaf XOR aggregate). Consumers must not assume
    item_id/location_id are set. snapshot_id is None for aggregate runs
    (no pyramide_snapshots row — leaf NOT NULL contract of migration 038).
    """
    run_id: UUID
    snapshot_id: UUID | None
    forecast_id: UUID
    item_id: UUID | None
    location_id: UUID | None
    hierarchy_id: str | None
    level: str | None
    node_code: str | None
    scenario_id: UUID
    horizon_start: date
    horizon_end: date
    granularity: str
    method: str
    model_strategy: str
    recon_method: str
    random_seed: int
    code_version: str
    selected_model: str
    engine_backend: str
    source_history_count: int
    status: str
    deterministic_artifact: str
    value_count: int
    total_quantity: Decimal
    created_at: datetime
    committed_at: datetime | None
    # ADR-023 (migration 056): TRUE when the run was generated while the
    # demand_history ingest age exceeded the freshness SLA. FALSE means
    # "not proven stale at run time", never "proven fresh".
    stale_demand: bool


@dataclass(frozen=True)
class PyramideForecastValue:
    value_id: UUID
    forecast_date: date
    quantity: Decimal
    method: str


@dataclass(frozen=True)
class PyramideSnapshotSummary:
    snapshot_id: UUID
    run_id: UUID
    forecast_id: UUID
    item_id: UUID
    location_id: UUID
    scenario_id: UUID
    horizon_start: date
    horizon_end: date
    granularity: str
    method: str
    frozen_at: datetime
    value_count: int
    total_quantity: Decimal


@dataclass(frozen=True)
class PyramideCommitResult:
    summary: PyramideRunSummary
    demand_node_count: int


@dataclass(frozen=True)
class PyramideAccuracyMetric:
    """One row of pyramide_accuracy_metrics (migration 055).

    ``horizon is None`` = the all-horizons aggregate row; ``horizon >= 1``
    = the per-horizon row. A ``None`` metric means "not computable on this
    data" (None-honest contract of accuracy.py) — per-horizon rows only
    carry the residual-derivable metrics (bias + counts): mase / wape /
    smape / coverage need the per-horizon actuals, which the report does
    not contain, so they stay None there.
    """

    metric_id: UUID
    run_id: UUID
    horizon: int | None
    mase: Decimal | None
    wape: Decimal | None
    smape: Decimal | None
    bias: Decimal | None
    coverage: Decimal | None
    n_cutoffs: int
    n_observations: int
    created_at: datetime


@dataclass(frozen=True)
class DemandFreshness:
    """Freshness of the demand_history signal (migration 047) — the raw
    material of the confidence score (pyramide/confidence.py, ADR-023).

    All fields are None when no qualifying row exists: freshness is NEVER
    invented — an unknown freshness degrades the confidence score through
    its prudent default instead of pretending the pipeline is alive.

    - ``ingest_age_days``   : full days since MAX(ingested_at) — is the
      ingestion PIPELINE alive? This is what the freshness SLA gates.
    - ``coverage_lag_days`` : days between CURRENT_DATE and
      MAX(booked_date) — how far behind the booking SIGNAL itself is
      (a healthy pipeline can still carry an old extract).
    Both are computed on the DB server clock (no app/DB clock skew).
    """

    last_booked_date: date | None
    max_ingested_at: datetime | None
    ingest_age_days: int | None
    coverage_lag_days: int | None


def get_demand_freshness(
    db: psycopg.Connection,
    item_id: UUID | None = None,
    warehouse_id: str | None = None,
) -> DemandFreshness:
    """Freshness of demand_history — global, per item, or per
    item x warehouse (``warehouse_id`` is the ERP DC text code stored in
    demand_history.warehouse_id, i.e. locations.external_id).

    Deliberately NO stream/business predicates: freshness measures the
    ingestion pipeline, not the training signal — a warranty-only or
    inter-entity-only ingest still proves the pipeline ran. MAX() ignores
    NULL booked_date rows, so last_booked_date stays honest.
    """
    filters = []
    params: dict[str, object] = {}
    if item_id is not None:
        filters.append("dh.item_id = %(item_id)s")
        params["item_id"] = item_id
    if warehouse_id is not None:
        filters.append("dh.warehouse_id = %(warehouse_id)s")
        params["warehouse_id"] = warehouse_id
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    row = db.execute(
        f"""
        SELECT
            MAX(dh.booked_date) AS last_booked_date,
            MAX(dh.ingested_at) AS max_ingested_at,
            (CURRENT_DATE - MAX(dh.booked_date)) AS coverage_lag_days,
            FLOOR(
                EXTRACT(EPOCH FROM (now() - MAX(dh.ingested_at))) / 86400
            )::int AS ingest_age_days
        FROM demand_history dh
        {where_clause}
        """,
        params,
    ).fetchone()

    if row is None or row["max_ingested_at"] is None:
        return DemandFreshness(
            last_booked_date=None,
            max_ingested_at=None,
            ingest_age_days=None,
            coverage_lag_days=None,
        )
    # Sub-day clock effects can floor to -1 on a row ingested "just now"
    # with a server clock marginally behind: clamp to 0, never negative age.
    ingest_age_days = max(0, row["ingest_age_days"])
    return DemandFreshness(
        last_booked_date=row["last_booked_date"],
        max_ingested_at=row["max_ingested_at"],
        ingest_age_days=ingest_age_days,
        coverage_lag_days=row["coverage_lag_days"],
    )


# Ledger identity of the freshness gate on Pyramide runs (agent_runs /
# dq_findings) — a NAME, not business logic.
FRESHNESS_GATE_AGENT_NAME = "pyramide_freshness_gate"


def record_stale_demand_finding(
    db: psycopg.Connection,
    *,
    run_id: UUID,
    scenario_id: UUID,
    item_id: UUID,
    item_external_id: str,
    warehouse_external_id: str | None,
    freshness: DemandFreshness,
    sla_days: int,
) -> UUID:
    """Emit ONE dq_findings STALE_DEMAND row for a Pyramide run executed on
    demand whose ingest age PROVABLY exceeds the freshness SLA (parameter,
    default 7 days — ADR-023). Called at most once per run by the router,
    so a run never spams findings; the finding's evidence carries the
    run_id, so governance agents can trace forecast -> stale input.

    dq_findings.agent_run_id is a NOT NULL FK to the agent_runs work
    ledger (migration 039/044): the gate logs its own COMPLETED ledger row
    — audit is a feature, every write is attributable.
    """
    if freshness.ingest_age_days is None or freshness.ingest_age_days <= sla_days:
        raise ValueError(
            "record_stale_demand_finding requires a PROVEN stale freshness "
            f"(ingest_age_days={freshness.ingest_age_days}, sla_days={sla_days})"
        )
    agent_run_id = uuid4()
    db.execute(
        """
        INSERT INTO agent_runs (
            agent_run_id, agent_name, scenario_id, status, finished_at, notes
        ) VALUES (%s, %s, %s, 'COMPLETED', now(), %s)
        """,
        (
            agent_run_id,
            FRESHNESS_GATE_AGENT_NAME,
            scenario_id,
            f"freshness gate on pyramide run {run_id}: stale demand "
            f"(ingest age {freshness.ingest_age_days}d > SLA {sla_days}d)",
        ),
    )
    dq_finding_id = uuid4()
    db.execute(
        """
        INSERT INTO dq_findings (
            dq_finding_id, agent_name, agent_run_id, scenario_id,
            rule_code, entity_type, entity_id, entity_external_id,
            severity, description, impact_metric, impact_value,
            suggested_action, evidence
        ) VALUES (%s, %s, %s, %s, 'STALE_DEMAND', 'item', %s, %s,
                  'MEDIUM', %s, 'ingest_age_days', %s, %s, %s)
        """,
        (
            dq_finding_id,
            FRESHNESS_GATE_AGENT_NAME,
            agent_run_id,
            scenario_id,
            item_id,
            item_external_id,
            (
                f"Pyramide run executed on stale demand: last demand_history "
                f"ingest is {freshness.ingest_age_days} day(s) old, over the "
                f"freshness SLA of {sla_days} day(s)"
            ),
            freshness.ingest_age_days,
            "Re-run the demand_history ingestion before trusting or "
            "committing this forecast (agents must not act on stale=True)",
            # JSONB carve-out of dq_findings (migration 044): forensic
            # evidence behind the finding.
            Jsonb(
                {
                    "pyramide_run_id": str(run_id),
                    "warehouse_external_id": warehouse_external_id,
                    "ingest_age_days": freshness.ingest_age_days,
                    "coverage_lag_days": freshness.coverage_lag_days,
                    "last_booked_date": (
                        str(freshness.last_booked_date)
                        if freshness.last_booked_date is not None
                        else None
                    ),
                    "max_ingested_at": (
                        freshness.max_ingested_at.isoformat()
                        if freshness.max_ingested_at is not None
                        else None
                    ),
                    "sla_days": sla_days,
                }
            ),
        ),
    )
    return dq_finding_id


def resolve_item_uuid(db: psycopg.Connection, external_id: str) -> UUID | None:
    row = db.execute(
        "SELECT item_id FROM items WHERE external_id = %s AND status != 'obsolete'",
        (external_id,),
    ).fetchone()
    return row["item_id"] if row else None


def resolve_location_uuid(db: psycopg.Connection, external_id: str) -> UUID | None:
    row = db.execute(
        "SELECT location_id FROM locations WHERE external_id = %s",
        (external_id,),
    ).fetchone()
    return row["location_id"] if row else None


def resolve_scenario_uuid(scenario_id: str | None) -> UUID:
    if scenario_id is None or scenario_id.lower() == "baseline":
        return BASELINE_SCENARIO_ID
    return UUID(scenario_id)


# Shared business predicates of the demand-history training signal,
# split in two layers so they can never drift apart across consumers:
#
# _DEMAND_HISTORY_STREAM_PREDICATES — the pure business rules:
#   - stream='regular' only (warranty is a separate forecast),
#   - inter-entity flows excluded (PPS→PCC double-count, migration 048),
#   - booked_date present (forecast-on-booking signal).
#   Consumed directly by the reconciliation bench (hierarchy/bench.py),
#   which needs the SAME rules over an EXPLICIT historical window
#   (train/holdout split at a past cutoff — CURRENT_DATE bounds would
#   leak holdout data into training).
#
# _DEMAND_HISTORY_BUSINESS_PREDICATES — the rules PLUS the live window
#   (strict past because today is a partial day, bounded lookback).
#   Single definition consumed by every runtime reader (leaf pair,
#   hierarchy node, per-item, totals). Uses the named placeholder
#   %(lookback_days)s — callers pass params as a dict.
_DEMAND_HISTORY_STREAM_PREDICATES = """
              dh.stream = 'regular'
              AND (dh.fulfillment IS NULL OR dh.fulfillment <> 'inter_entity')
              AND dh.booked_date IS NOT NULL
"""

_DEMAND_HISTORY_BUSINESS_PREDICATES = _DEMAND_HISTORY_STREAM_PREDICATES + """
              AND dh.booked_date < CURRENT_DATE
              AND dh.booked_date >= CURRENT_DATE - (%(lookback_days)s::int * INTERVAL '1 day')
"""


def get_historical_demand(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID,
    lookback_days: int,
    scenario_id: UUID = BASELINE_SCENARIO_ID,
) -> list[Decimal]:
    """
    Historical demand series for (item, location): daily booked sums, sorted
    date ASC. The series is sparse — days without demand are absent, not
    zero-filled (contract consumed by ForecastingEngine / PyramideRunner).

    Primary source: ``demand_history`` booking facts (migration 047), the
    forecast-on-booking signal — stream='regular' only (warranty is a
    separate forecast), inter-entity flows excluded (PPS→PCC double-count,
    migration 048), strict past (booked_date < today). The location is
    mapped through ``locations.external_id = demand_history.warehouse_id``
    (warehouse_id stores the ERP DC code; resolution happens at read time
    per migration 047). Rows with NULL/unmatched warehouse_id drop out of
    the per-site series by design.

    ``demand_history`` deliberately carries no scenario_id: actuals are
    invariant across scenarios. ``scenario_id`` is used ONLY by the
    degraded fallback below.

    Degraded fallback: if demand_history has no rows for the pair, read
    past CustomerOrderDemand graph nodes filtered by ``scenario_id``
    (fork copies carry the fork's scenario_id, so no baseline+fork union).
    NEVER ForecastDemand — a forecast must not train on forecasts (#333).
    The fallback keeps fresh installs (orders ingested as graph nodes,
    ingest_demand_history never run) usable; a warning logs the degraded
    mode (fail-loudly).
    """
    loc_row = db.execute(
        "SELECT external_id FROM locations WHERE location_id = %s",
        (location_id,),
    ).fetchone()
    warehouse_external_id = loc_row["external_id"] if loc_row else None

    if warehouse_external_id is not None:
        rows = db.execute(
            f"""
            SELECT dh.booked_date AS demand_date,
                   COALESCE(SUM(dh.ordered_quantity), 0) AS total_qty
            FROM demand_history dh
            WHERE dh.item_id = %(item_id)s
              AND dh.warehouse_id = %(warehouse_id)s
              AND {_DEMAND_HISTORY_BUSINESS_PREDICATES}
            GROUP BY dh.booked_date
            ORDER BY dh.booked_date ASC
            """,
            {
                "item_id": item_id,
                "warehouse_id": warehouse_external_id,
                "lookback_days": lookback_days,
            },
        ).fetchall()
        if rows:
            return [Decimal(str(row["total_qty"])) for row in rows]

    logger.warning(
        "historical demand: no demand_history rows in the %s-day lookback "
        "window for item=%s location=%s (external_id=%s) — falling back to "
        "CustomerOrderDemand nodes (degraded, scenario=%s)",
        lookback_days, item_id, location_id, warehouse_external_id, scenario_id,
    )
    rows = db.execute(
        """
        SELECT COALESCE(time_span_start, time_ref)::date AS demand_date,
               COALESCE(SUM(quantity), 0) AS total_qty
        FROM nodes
        WHERE node_type = 'CustomerOrderDemand'
          AND scenario_id = %s
          AND item_id = %s
          AND location_id = %s
          AND active = TRUE
          AND COALESCE(time_span_start, time_ref) IS NOT NULL
          AND COALESCE(time_span_start, time_ref)::date < CURRENT_DATE
          AND COALESCE(time_span_start, time_ref)::date >= CURRENT_DATE - (%s::int * INTERVAL '1 day')
        GROUP BY 1
        ORDER BY 1 ASC
        """,
        (scenario_id, item_id, location_id, lookback_days),
    ).fetchall()
    return [Decimal(str(row["total_qty"])) for row in rows]


def get_historical_demand_by_node(
    db: psycopg.Connection,
    hierarchy_id: str,
    node_code: str,
    lookback_days: int,
) -> list[Decimal]:
    """
    Historical demand series AGGREGATED at a hierarchy node: daily booked
    sums over every item attached (via item_hierarchy) to the node's
    subtree, sorted date ASC. Same sparse contract as
    ``get_historical_demand`` (days without demand are absent).

    Business filters are the shared ``_DEMAND_HISTORY_BUSINESS_PREDICATES``
    — identical to the leaf reader by construction: stream='regular' only,
    inter-entity excluded, strict past, bounded lookback.

    Scope differences vs the leaf reader, by design:
      - ALL sites: an aggregate node's series sums demand across DCs
        (site-level split is the reconciliation/DRP layer's job), so
        there is no warehouse filter. Rows with NULL/unmatched
        warehouse_id therefore DO count here.
      - No degraded graph-node fallback: aggregate nodes have no
        CustomerOrderDemand equivalent; an empty series is an empty
        series.
      - demand_history is scenario-invariant (actuals), so there is no
        scenario parameter at all.

    Fails loudly (ValueError) if the node does not exist in the
    hierarchy — silence here would be indistinguishable from "no demand".
    """
    exists = db.execute(
        "SELECT 1 AS ok FROM hierarchy_node WHERE hierarchy_id = %s AND code = %s",
        (hierarchy_id, node_code),
    ).fetchone()
    if exists is None:
        raise ValueError(
            f"node '{node_code}' not found in hierarchy '{hierarchy_id}'"
        )

    rows = db.execute(
        f"""
        WITH RECURSIVE subtree AS (
            SELECT code
            FROM hierarchy_node
            WHERE hierarchy_id = %(hierarchy_id)s AND code = %(node_code)s
            UNION
            -- UNION (not UNION ALL): hierarchy_node has no self-FK, so a
            -- bad parent_code cycle must terminate instead of recursing
            -- forever; dedup guarantees termination.
            SELECT hn.code
            FROM hierarchy_node hn
            JOIN subtree st ON hn.parent_code = st.code
            WHERE hn.hierarchy_id = %(hierarchy_id)s
        )
        SELECT dh.booked_date AS demand_date,
               COALESCE(SUM(dh.ordered_quantity), 0) AS total_qty
        FROM demand_history dh
        JOIN item_hierarchy ih
          ON ih.item_id = dh.item_id
         AND ih.hierarchy_id = %(hierarchy_id)s
        JOIN subtree st ON st.code = ih.leaf_code
        WHERE {_DEMAND_HISTORY_BUSINESS_PREDICATES}
        GROUP BY dh.booked_date
        ORDER BY dh.booked_date ASC
        """,
        {
            "hierarchy_id": hierarchy_id,
            "node_code": node_code,
            "lookback_days": lookback_days,
        },
    ).fetchall()
    return [Decimal(str(row["total_qty"])) for row in rows]


def get_historical_demand_by_item(
    db: psycopg.Connection,
    item_id: UUID,
    lookback_days: int,
) -> list[Decimal]:
    """
    Historical demand series for one item across ALL sites: daily booked
    sums, sorted date ASC, same sparse contract as the other readers
    (days without demand are absent). Business filters are the shared
    ``_DEMAND_HISTORY_BUSINESS_PREDICATES``.

    This is the LEAF series of the hierarchy layer: summing-block columns
    are items (item_hierarchy), site-agnostic — the site split belongs to
    the reconciliation/DRP layer, exactly like the node reader. No
    warehouse filter, no graph fallback, no scenario parameter (actuals
    are scenario-invariant).
    """
    rows = db.execute(
        f"""
        SELECT dh.booked_date AS demand_date,
               COALESCE(SUM(dh.ordered_quantity), 0) AS total_qty
        FROM demand_history dh
        WHERE dh.item_id = %(item_id)s
          AND {_DEMAND_HISTORY_BUSINESS_PREDICATES}
        GROUP BY dh.booked_date
        ORDER BY dh.booked_date ASC
        """,
        {"item_id": item_id, "lookback_days": lookback_days},
    ).fetchall()
    return [Decimal(str(row["total_qty"])) for row in rows]


def get_historical_demand_totals_by_items(
    db: psycopg.Connection,
    item_ids: Sequence[UUID],
    lookback_days: int,
) -> dict[str, Decimal]:
    """
    Booked totals per item over the lookback window, across ALL sites —
    the middle-out share numerators (reconcile.py). Keys are stringified
    item UUIDs (the summing-block leaf column keys). Items with no
    qualifying row are ABSENT from the dict (callers default them to 0 —
    the cold-start / natural-zero rules of the reconciler apply).

    Same shared business predicates as every demand-history reader.
    """
    if not item_ids:
        return {}
    rows = db.execute(
        f"""
        SELECT dh.item_id, COALESCE(SUM(dh.ordered_quantity), 0) AS total_qty
        FROM demand_history dh
        WHERE dh.item_id = ANY(%(item_ids)s)
          AND {_DEMAND_HISTORY_BUSINESS_PREDICATES}
        GROUP BY dh.item_id
        """,
        {"item_ids": list(item_ids), "lookback_days": lookback_days},
    ).fetchall()
    return {str(row["item_id"]): Decimal(str(row["total_qty"])) for row in rows}


_ONE = Decimal("1")


def accuracy_metric_rows(
    report: AccuracyReport,
    bias_scale: Decimal = _ONE,
) -> list[tuple[int | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None, int, int]]:
    """
    Pure mapping AccuracyReport -> pyramide_accuracy_metrics rows
    (migration 055). Returns tuples
    ``(horizon, mase, wape, smape, bias, coverage, n_cutoffs, n_observations)``:

    - one AGGREGATE row (horizon None) with the report's pooled metrics,
      as-is (None stays None — the None-honest contract of accuracy.py);
    - one row PER HORIZON with only the residual-derivable metrics:
      ``bias_h = -mean(residuals_h)`` (report residuals are
      ``actual - forecast`` while the bias contract is
      ``forecast - actual``, positive = over-forecast — hence the sign
      flip), and ``n_cutoffs = n_observations = len(residuals_h)`` (one
      residual per cutoff that reached horizon h). mase/wape/smape/
      coverage need the per-horizon actuals, which the report does not
      carry: they stay None. Nothing is invented.

    ``bias_scale`` is the middle-out transport factor for LEAF runs of a
    hierarchical block (leaf = share x node): the scale-free metrics
    (mase/wape/smape/coverage) are invariant under proportional
    disaggregation and pass through unchanged, while bias — the only
    scale-DEPENDENT metric persisted — is multiplied by the share,
    exactly like the conformal offsets in ``engines.conformal_bounds``
    (same documented V1 approximation). Default 1 = the report describes
    the run's own series.
    """
    rows: list[tuple[int | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None, Decimal | None, int, int]] = [
        (
            None,
            report.mase,
            report.wape,
            report.smape,
            report.bias * bias_scale,
            report.coverage,
            report.n_cutoffs,
            report.n_observations,
        )
    ]
    for horizon, residuals in sorted(report.per_horizon_residuals.items()):
        if not residuals:
            continue
        mean_residual = sum(residuals, Decimal(0)) / Decimal(len(residuals))
        rows.append(
            (
                horizon,
                None,  # mase: needs per-horizon actuals — not in the report
                None,  # wape: idem
                None,  # smape: idem
                -mean_residual * bias_scale,
                None,  # coverage: no intervals evaluated per horizon
                len(residuals),
                len(residuals),
            )
        )
    return rows


def persist_accuracy_metrics(
    db: psycopg.Connection,
    run_id: UUID,
    report: AccuracyReport,
    bias_scale: Decimal = _ONE,
) -> int:
    """
    Persist a run's backtest report into pyramide_accuracy_metrics
    (migration 055): the aggregate row + one row per horizon (see
    ``accuracy_metric_rows`` for the exact mapping and the ``bias_scale``
    transport semantics).

    Idempotence: DELETE + INSERT of the run's full row set. The report is
    an atomic artifact of ONE backtest — replacing the whole set keeps
    the (run_id, horizon) UNIQUE NULLS NOT DISTINCT invariant trivially
    and never leaves a stale mix of two backtests (an upsert would, if a
    re-run produced fewer horizons). Returns the number of rows written.
    """
    db.execute(
        "DELETE FROM pyramide_accuracy_metrics WHERE run_id = %s",
        (run_id,),
    )
    rows = accuracy_metric_rows(report, bias_scale=bias_scale)
    for horizon, mase, wape, smape, bias_value, coverage, n_cutoffs, n_observations in rows:
        db.execute(
            """
            INSERT INTO pyramide_accuracy_metrics (
                run_id, horizon, mase, wape, smape, bias, coverage,
                n_cutoffs, n_observations
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id, horizon, mase, wape, smape, bias_value, coverage,
                n_cutoffs, n_observations,
            ),
        )
    return len(rows)


def fetch_accuracy_metrics(
    db: psycopg.Connection, run_id: UUID
) -> list[PyramideAccuracyMetric]:
    """
    Backtest metrics of a run, aggregate row FIRST (horizon NULL), then
    per-horizon rows in ascending horizon order. Empty list = the run
    was persisted without a backtest report (documented absence, e.g.
    ENSEMBLE_STAT blend or external backend) — never invented rows.
    """
    rows = db.execute(
        """
        SELECT metric_id, run_id, horizon, mase, wape, smape, bias,
               coverage, n_cutoffs, n_observations, created_at
        FROM pyramide_accuracy_metrics
        WHERE run_id = %s
        ORDER BY horizon ASC NULLS FIRST
        """,
        (run_id,),
    ).fetchall()
    return [
        PyramideAccuracyMetric(
            metric_id=row["metric_id"],
            run_id=row["run_id"],
            horizon=row["horizon"],
            mase=_optional_decimal(row["mase"]),
            wape=_optional_decimal(row["wape"]),
            smape=_optional_decimal(row["smape"]),
            bias=_optional_decimal(row["bias"]),
            coverage=_optional_decimal(row["coverage"]),
            n_cutoffs=row["n_cutoffs"],
            n_observations=row["n_observations"],
            created_at=row["created_at"],
        )
        for row in rows
    ]


def fetch_latest_aggregate_wape(
    db: psycopg.Connection,
    *,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
) -> Decimal | None:
    """Most recent aggregate backtest WAPE (horizon IS NULL row of
    pyramide_accuracy_metrics) for a (item, location, scenario) series —
    the ``accuracy`` input of the confidence score (ADR-023).

    None = no run of this series ever persisted an aggregate WAPE
    (no run, no backtest report, or WAPE not computable on that data):
    the confidence score then falls back to its PRUDENT default component
    — never an invented accuracy.
    """
    row = db.execute(
        """
        SELECT pam.wape
        FROM pyramide_accuracy_metrics pam
        JOIN pyramide_runs pr ON pr.run_id = pam.run_id
        WHERE pam.horizon IS NULL
          AND pam.wape IS NOT NULL
          AND pr.item_id = %(item_id)s
          AND pr.location_id = %(location_id)s
          AND pr.scenario_id = %(scenario_id)s
        ORDER BY pr.created_at DESC, pam.created_at DESC
        LIMIT 1
        """,
        {
            "item_id": item_id,
            "location_id": location_id,
            "scenario_id": scenario_id,
        },
    ).fetchone()
    return _optional_decimal(row["wape"]) if row else None


def _optional_decimal(value) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def persist_run(
    db: psycopg.Connection,
    result: PyramideRunResult,
    *,
    stale_demand: bool = False,
    routing: RoutingDecision | None = None,
) -> PyramidePersistedRun:
    """``stale_demand`` (ADR-023, migration 056): the caller measured the
    demand_history freshness at run time and it PROVABLY exceeded the SLA.
    Default False = not proven stale (also the value for write paths that
    do not measure freshness yet — never a claim of freshness).

    ``routing`` (migration 058, PR-B1, opt-in): the head/tail router's
    decision for this series when the caller routed it — persisted as the
    routed_method/routed_level/routing_reason provenance columns. None
    (the default and the historical behaviour) = the method was requested
    explicitly, columns stay NULL."""
    run_id = uuid4()
    snapshot_id = uuid4()
    forecast_id = uuid4()
    config = result.config

    db.execute(
        """
        INSERT INTO forecasts (
            forecast_id, item_id, location_id, scenario_id,
            horizon_start, horizon_end, granularity, method
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            forecast_id,
            config.item_id,
            config.location_id,
            config.scenario_id,
            config.horizon_start,
            config.horizon_end,
            config.granularity,
            config.method,
        ),
    )

    for value in result.values:
        # confidence_interval_lower/upper (migration 026) : bornes conformal
        # calibrées sur les résidus de backtest du modèle servi
        # (engines.conformal_bounds). NULL quand aucune calibration honnête
        # n'existe — la provenance vit dans result.warnings.
        db.execute(
            """
            INSERT INTO forecast_values (
                value_id, forecast_id, forecast_date, quantity, method,
                confidence_interval_lower, confidence_interval_upper
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                uuid4(), forecast_id, value.forecast_date, value.quantity,
                value.method, value.confidence_lower, value.confidence_upper,
            ),
        )

    db.execute(
        """
        INSERT INTO pyramide_runs (
            run_id, forecast_id, item_id, location_id, scenario_id,
            horizon_start, horizon_end, granularity, method,
            model_strategy, recon_method, random_seed, code_version,
            selected_model, engine_backend, source_history_count, status,
            deterministic_artifact, stale_demand,
            routed_method, routed_level, routing_reason
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'generated', 'forecast_values', %s, %s, %s, %s)
        """,
        (
            run_id,
            forecast_id,
            config.item_id,
            config.location_id,
            config.scenario_id,
            config.horizon_start,
            config.horizon_end,
            config.granularity,
            config.method,
            config.model_strategy,
            config.recon_method,
            config.random_seed,
            config.code_version,
            result.selected_model,
            result.engine_backend,
            result.source_history_count,
            stale_demand,
            routing.method if routing is not None else None,
            routing.level if routing is not None else None,
            routing.reason if routing is not None else None,
        ),
    )

    db.execute(
        """
        INSERT INTO pyramide_snapshots (
            snapshot_id, run_id, forecast_id, item_id, location_id, scenario_id,
            horizon_start, horizon_end, granularity, method, value_count, total_quantity,
            immutable_artifact
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'forecast_values')
        """,
        (
            snapshot_id,
            run_id,
            forecast_id,
            config.item_id,
            config.location_id,
            config.scenario_id,
            config.horizon_start,
            config.horizon_end,
            config.granularity,
            config.method,
            len(result.values),
            result.total_quantity,
        ),
    )
    # Métriques de backtest (migration 055) : uniquement quand un rapport
    # honnête existe — run sans rapport (ENSEMBLE_STAT, backend externe,
    # historique trop court) = zéro ligne, documenté.
    if result.accuracy_report is not None:
        persist_accuracy_metrics(db, run_id, result.accuracy_report)
    return PyramidePersistedRun(run_id=run_id, snapshot_id=snapshot_id, forecast_id=forecast_id)


def persist_series_run(
    db: psycopg.Connection,
    *,
    scenario_id: UUID,
    horizon_start: date,
    horizon_end: date,
    granularity: str,
    method: str,
    model_strategy: str,
    recon_method: str,
    random_seed: int,
    code_version: str,
    selected_model: str,
    engine_backend: str,
    source_history_count: int,
    bucket_dates: Sequence[date],
    quantities: Sequence[Decimal],
    value_method: str,
    item_id: UUID | None = None,
    location_id: UUID | None = None,
    hierarchy_id: str | None = None,
    level: str | None = None,
    node_code: str | None = None,
    lowers: Sequence[Decimal | None] | None = None,
    uppers: Sequence[Decimal | None] | None = None,
    accuracy_report: AccuracyReport | None = None,
    accuracy_bias_scale: Decimal = _ONE,
    routing: RoutingDecision | None = None,
) -> PyramidePersistedRun:
    """
    Persist ONE series of a hierarchical run (migration 053): either a
    LEAF (item_id + location_id) or an AGGREGATE (hierarchy_id + level +
    node_code) — never both (mirrors the DB CHECK, validated here first
    so the error is readable).

    ``accuracy_report`` is the rolling-origin backtest report of the
    model that produced this series' values; when non-None it is
    persisted into pyramide_accuracy_metrics (migration 055). None →
    zero metric rows (documented absence, never invented metrics).
    ``accuracy_bias_scale`` transports the scale-dependent bias for a
    LEAF whose values are share x reconciliation-node curve — same V1
    middle-out approximation as the conformal bounds (see
    ``accuracy_metric_rows``).

    ``lowers`` / ``uppers`` are per-bucket conformal bounds
    (confidence_interval_lower/upper, migration 026), aligned with
    ``quantities``; omitted or None → NULL columns. NON-OBJECTIF V1 pour
    les AGRÉGATS réconciliés : la réconciliation d'intervalles
    hiérarchiques est frontier (spec Pyramide §2.D) — les appelants
    laissent NULL pour les nœuds et ne remplissent que les feuilles.

    Writes forecasts + forecast_values + pyramide_runs for every series;
    pyramide_snapshots ONLY for leaves. Rationale: snapshots exist to
    feed the graph commit path (_commit_snapshot_to_demand_nodes), which
    materializes LEAF ForecastDemand nodes only — aggregates stay
    queryable in the forecast tables and never enter the graph, so a
    snapshot row (whose leaf columns are NOT NULL by migration 038)
    would be meaningless for them.

    ``recon_method`` must be the method EFFECTIVELY applied (the
    reconciler reports it) — this is what makes the column real.

    ``routing`` (migration 058, PR-B1, opt-in): the head/tail router's
    decision for THIS series — persisted as routed_method / routed_level /
    routing_reason. None (default) = not routed, columns stay NULL.
    """
    is_leaf = item_id is not None and location_id is not None
    is_aggregate = (
        hierarchy_id is not None and level is not None and node_code is not None
    )
    if is_leaf == is_aggregate:
        raise ValueError(
            "persist_series_run targets a leaf (item_id + location_id) XOR "
            "an aggregate (hierarchy_id + level + node_code); got "
            f"item_id={item_id}, location_id={location_id}, "
            f"hierarchy_id={hierarchy_id}, level={level}, node_code={node_code}"
        )
    if len(bucket_dates) != len(quantities):
        raise ValueError(
            f"{len(bucket_dates)} bucket dates but {len(quantities)} quantities"
        )
    if lowers is not None and len(lowers) != len(quantities):
        raise ValueError(
            f"{len(lowers)} lower bounds but {len(quantities)} quantities"
        )
    if uppers is not None and len(uppers) != len(quantities):
        raise ValueError(
            f"{len(uppers)} upper bounds but {len(quantities)} quantities"
        )

    run_id = uuid4()
    forecast_id = uuid4()
    db.execute(
        """
        INSERT INTO forecasts (
            forecast_id, item_id, location_id, hierarchy_id, level, node_code,
            scenario_id, horizon_start, horizon_end, granularity, method
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            forecast_id, item_id, location_id, hierarchy_id, level, node_code,
            scenario_id, horizon_start, horizon_end, granularity, method,
        ),
    )
    total_quantity = Decimal("0")
    for index, (bucket_date, quantity) in enumerate(zip(bucket_dates, quantities)):
        total_quantity += quantity
        db.execute(
            """
            INSERT INTO forecast_values (
                value_id, forecast_id, forecast_date, quantity, method,
                confidence_interval_lower, confidence_interval_upper
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                uuid4(), forecast_id, bucket_date, quantity, value_method,
                lowers[index] if lowers is not None else None,
                uppers[index] if uppers is not None else None,
            ),
        )
    db.execute(
        """
        INSERT INTO pyramide_runs (
            run_id, forecast_id, item_id, location_id,
            hierarchy_id, level, node_code, scenario_id,
            horizon_start, horizon_end, granularity, method,
            model_strategy, recon_method, random_seed, code_version,
            selected_model, engine_backend, source_history_count, status,
            deterministic_artifact,
            routed_method, routed_level, routing_reason
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, 'generated', 'forecast_values',
                  %s, %s, %s)
        """,
        (
            run_id, forecast_id, item_id, location_id,
            hierarchy_id, level, node_code, scenario_id,
            horizon_start, horizon_end, granularity, method,
            model_strategy, recon_method, random_seed, code_version,
            selected_model, engine_backend, source_history_count,
            routing.method if routing is not None else None,
            routing.level if routing is not None else None,
            routing.reason if routing is not None else None,
        ),
    )

    snapshot_id: UUID | None = None
    if is_leaf:
        snapshot_id = uuid4()
        db.execute(
            """
            INSERT INTO pyramide_snapshots (
                snapshot_id, run_id, forecast_id, item_id, location_id,
                scenario_id, horizon_start, horizon_end, granularity, method,
                value_count, total_quantity, immutable_artifact
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      'forecast_values')
            """,
            (
                snapshot_id, run_id, forecast_id, item_id, location_id,
                scenario_id, horizon_start, horizon_end, granularity, method,
                len(quantities), total_quantity,
            ),
        )
    if accuracy_report is not None:
        persist_accuracy_metrics(
            db, run_id, accuracy_report, bias_scale=accuracy_bias_scale
        )
    return PyramidePersistedRun(
        run_id=run_id, snapshot_id=snapshot_id, forecast_id=forecast_id
    )


def fetch_run_summary(db: psycopg.Connection, run_id: UUID) -> PyramideRunSummary | None:
    # LEFT JOIN: aggregate runs (migration 053) have no snapshot — their
    # value_count/total come straight from the frozen forecast_values.
    row = db.execute(
        """
        SELECT
            pr.run_id, ps.snapshot_id, pr.forecast_id, pr.item_id, pr.location_id,
            pr.hierarchy_id, pr.level, pr.node_code,
            pr.scenario_id, pr.horizon_start, pr.horizon_end, pr.granularity,
            pr.method, pr.model_strategy, pr.recon_method, pr.random_seed,
            pr.code_version, pr.selected_model, pr.engine_backend,
            pr.source_history_count, pr.status,
            pr.deterministic_artifact, pr.created_at, pr.committed_at,
            pr.stale_demand,
            COALESCE(ps.value_count, vc.value_count) AS value_count,
            COALESCE(ps.total_quantity, vc.total_quantity) AS total_quantity
        FROM pyramide_runs pr
        LEFT JOIN pyramide_snapshots ps ON ps.run_id = pr.run_id
        LEFT JOIN LATERAL (
            SELECT COUNT(*)::int AS value_count,
                   COALESCE(SUM(fv.quantity), 0) AS total_quantity
            FROM forecast_values fv
            WHERE fv.forecast_id = pr.forecast_id
        ) vc ON TRUE
        WHERE pr.run_id = %s
        """,
        (run_id,),
    ).fetchone()
    return _summary_from_row(row) if row else None


def fetch_run_values(db: psycopg.Connection, run_id: UUID) -> list[PyramideForecastValue] | None:
    run = db.execute(
        "SELECT forecast_id FROM pyramide_runs WHERE run_id = %s",
        (run_id,),
    ).fetchone()
    if not run:
        return None

    rows = db.execute(
        """
        SELECT value_id, forecast_date, quantity, method
        FROM forecast_values
        WHERE forecast_id = %s
        ORDER BY forecast_date ASC, value_id ASC
        """,
        (run["forecast_id"],),
    ).fetchall()
    return [
        PyramideForecastValue(
            value_id=row["value_id"],
            forecast_date=row["forecast_date"],
            quantity=Decimal(str(row["quantity"])),
            method=row["method"],
        )
        for row in rows
    ]


def commit_run(db: psycopg.Connection, run_id: UUID) -> PyramideCommitResult | None:
    """
    Materialize a LEAF run's frozen values as ForecastDemand graph nodes.

    LEAVES ONLY: an aggregate (hierarchy-node) run raises
    PyramideAggregateCommitError — aggregates stay queryable in
    forecasts/forecast_values and never enter the graph (the graph's
    demand contract is (item, location); a node-level demand quantity
    would double-count its leaves in propagation).
    """
    summary = fetch_run_summary(db, run_id)
    if summary is None:
        return None
    if summary.node_code is not None:
        raise PyramideAggregateCommitError(
            f"Pyramide run '{run_id}' targets aggregate node "
            f"'{summary.node_code}' (hierarchy '{summary.hierarchy_id}', "
            f"level '{summary.level}') — aggregate forecasts are never "
            "materialized as graph demand; commit the block's leaf "
            "(item, location) runs instead"
        )

    demand_node_count = _commit_snapshot_to_demand_nodes(db, summary)
    db.execute(
        """
        UPDATE pyramide_runs
        SET status = 'committed',
            committed_at = COALESCE(committed_at, now()),
            updated_at = now()
        WHERE run_id = %s
        """,
        (run_id,),
    )
    updated = fetch_run_summary(db, run_id)
    if updated is None:
        return None
    return PyramideCommitResult(summary=updated, demand_node_count=demand_node_count)


def list_snapshots(
    db: psycopg.Connection,
    item_id: UUID | None = None,
    location_id: UUID | None = None,
    limit: int = 100,
) -> list[PyramideSnapshotSummary]:
    filters = []
    params: list[object] = []
    if item_id is not None:
        filters.append("item_id = %s")
        params.append(item_id)
    if location_id is not None:
        filters.append("location_id = %s")
        params.append(location_id)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = db.execute(
        f"""
        SELECT snapshot_id, run_id, forecast_id, item_id, location_id, scenario_id,
               horizon_start, horizon_end, granularity, method, frozen_at,
               value_count, total_quantity
        FROM pyramide_snapshots
        {where_clause}
        ORDER BY frozen_at DESC
        LIMIT %s
        """,
        (*params, limit),
    ).fetchall()
    return [_snapshot_from_row(row) for row in rows]


def fetch_snapshot_values(db: psycopg.Connection, snapshot_id: UUID) -> list[PyramideForecastValue] | None:
    row = db.execute(
        "SELECT forecast_id FROM pyramide_snapshots WHERE snapshot_id = %s",
        (snapshot_id,),
    ).fetchone()
    if not row:
        return None

    values = db.execute(
        """
        SELECT value_id, forecast_date, quantity, method
        FROM forecast_values
        WHERE forecast_id = %s
        ORDER BY forecast_date ASC, value_id ASC
        """,
        (row["forecast_id"],),
    ).fetchall()
    return [
        PyramideForecastValue(
            value_id=value["value_id"],
            forecast_date=value["forecast_date"],
            quantity=Decimal(str(value["quantity"])),
            method=value["method"],
        )
        for value in values
    ]


def _summary_from_row(row) -> PyramideRunSummary:
    return PyramideRunSummary(
        run_id=row["run_id"],
        snapshot_id=row["snapshot_id"],
        forecast_id=row["forecast_id"],
        item_id=row["item_id"],
        location_id=row["location_id"],
        hierarchy_id=row["hierarchy_id"],
        level=row["level"],
        node_code=row["node_code"],
        scenario_id=row["scenario_id"],
        horizon_start=row["horizon_start"],
        horizon_end=row["horizon_end"],
        granularity=row["granularity"],
        method=row["method"],
        model_strategy=row["model_strategy"],
        recon_method=row["recon_method"],
        random_seed=row["random_seed"],
        code_version=row["code_version"],
        selected_model=row["selected_model"],
        engine_backend=row["engine_backend"],
        source_history_count=row["source_history_count"],
        status=row["status"],
        deterministic_artifact=row["deterministic_artifact"],
        value_count=row["value_count"],
        total_quantity=Decimal(str(row["total_quantity"])),
        created_at=row["created_at"],
        committed_at=row["committed_at"],
        stale_demand=row["stale_demand"],
    )


def _snapshot_from_row(row) -> PyramideSnapshotSummary:
    return PyramideSnapshotSummary(
        snapshot_id=row["snapshot_id"],
        run_id=row["run_id"],
        forecast_id=row["forecast_id"],
        item_id=row["item_id"],
        location_id=row["location_id"],
        scenario_id=row["scenario_id"],
        horizon_start=row["horizon_start"],
        horizon_end=row["horizon_end"],
        granularity=row["granularity"],
        method=row["method"],
        frozen_at=row["frozen_at"],
        value_count=row["value_count"],
        total_quantity=Decimal(str(row["total_quantity"])),
    )


def _commit_snapshot_to_demand_nodes(db: psycopg.Connection, summary: PyramideRunSummary) -> int:
    """
    LEAF-ONLY materialization contract: only leaf (item, location) series
    ever become ForecastDemand nodes. Aggregate series live exclusively
    in the forecast tables (migration 053) — pyramide_snapshots keeps its
    leaf NOT NULL contract (migration 038) precisely because a snapshot's
    sole purpose is this graph-commit path. commit_run() guards the
    aggregate case with a typed error before reaching here; this check is
    the defensive backstop.
    """
    if (
        summary.item_id is None
        or summary.location_id is None
        or summary.snapshot_id is None
    ):
        raise PyramideAggregateCommitError(
            f"run '{summary.run_id}' has no leaf (item, location) snapshot — "
            "only leaf runs are materialized to the graph"
        )
    _ensure_projection_series_window(
        db=db,
        item_id=summary.item_id,
        location_id=summary.location_id,
        scenario_id=summary.scenario_id,
        horizon_start=summary.horizon_start,
        horizon_end=summary.horizon_end,
    )

    rows = db.execute(
        """
        SELECT fv.value_id, fv.forecast_date, fv.quantity
        FROM forecast_values fv
        WHERE fv.forecast_id = %s
        ORDER BY fv.forecast_date ASC, fv.value_id ASC
        """,
        (summary.forecast_id,),
    ).fetchall()
    if not rows:
        return 0

    created_or_existing = 0
    horizon_stop = summary.horizon_end + timedelta(days=1)

    for index, row in enumerate(rows):
        value_id = row["value_id"]
        existing = db.execute(
            """
            SELECT demand_node_id
            FROM pyramide_snapshot_demand_nodes
            WHERE snapshot_id = %s AND value_id = %s
            """,
            (summary.snapshot_id, value_id),
        ).fetchone()
        if existing:
            created_or_existing += 1
            continue

        bucket_start = row["forecast_date"]
        if index + 1 < len(rows):
            bucket_end = min(rows[index + 1]["forecast_date"], horizon_stop)
        else:
            bucket_end = min(_bucket_end(bucket_start, summary.granularity), horizon_stop)
        if bucket_end <= bucket_start:
            bucket_end = bucket_start + timedelta(days=1)

        demand_node_id = uuid4()
        quantity = Decimal(str(row["quantity"]))
        db.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                quantity, qty_uom, time_grain, time_ref,
                time_span_start, time_span_end, is_dirty, active,
                created_at, updated_at
            ) VALUES (
                %s, 'ForecastDemand', %s, %s, %s,
                %s, 'EA', %s, %s,
                %s, %s, TRUE, TRUE,
                now(), now()
            )
            """,
            (
                demand_node_id,
                summary.scenario_id,
                summary.item_id,
                summary.location_id,
                quantity,
                _time_grain(summary.granularity),
                bucket_start,
                bucket_start,
                bucket_end,
            ),
        )
        db.execute(
            """
            INSERT INTO pyramide_snapshot_demand_nodes (snapshot_id, value_id, demand_node_id)
            VALUES (%s, %s, %s)
            """,
            (summary.snapshot_id, value_id, demand_node_id),
        )
        _wire_demand_node_to_pi(
            db=db,
            demand_node_id=demand_node_id,
            item_id=summary.item_id,
            location_id=summary.location_id,
            scenario_id=summary.scenario_id,
            bucket_start=bucket_start,
            bucket_end=bucket_end,
        )
        _emit_commit_event(db, summary.scenario_id, demand_node_id, quantity)
        created_or_existing += 1

    return created_or_existing


def _ensure_projection_series_window(
    db: psycopg.Connection,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
    horizon_start: date,
    horizon_end: date,
) -> None:
    row = db.execute(
        """
        SELECT series_id, horizon_start, horizon_end
        FROM projection_series
        WHERE item_id = %s AND location_id = %s AND scenario_id = %s
        """,
        (item_id, location_id, scenario_id),
    ).fetchone()

    if row:
        series_id = row["series_id"]
        series_start = min(row["horizon_start"], horizon_start)
        series_end = max(row["horizon_end"], horizon_end)
        db.execute(
            """
            UPDATE projection_series
            SET horizon_start = %s, horizon_end = %s, updated_at = now()
            WHERE series_id = %s
            """,
            (series_start, series_end, series_id),
        )
    else:
        series_id = uuid4()
        series_start = horizon_start
        series_end = horizon_end
        db.execute(
            """
            INSERT INTO projection_series (
                series_id, item_id, location_id, scenario_id,
                horizon_start, horizon_end, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, now(), now())
            """,
            (series_id, item_id, location_id, scenario_id, series_start, series_end),
        )

    day = horizon_start
    while day <= horizon_end:
        day_end = day + timedelta(days=1)
        bucket_sequence = (day - series_start).days
        db.execute(
            """
            INSERT INTO nodes (
                node_id, node_type, scenario_id, item_id, location_id,
                time_grain, time_span_start, time_span_end, time_ref,
                projection_series_id, bucket_sequence,
                opening_stock, inflows, outflows, closing_stock,
                has_shortage, shortage_qty, is_dirty, active,
                created_at, updated_at
            )
            SELECT
                %s, 'ProjectedInventory', %s, %s, %s,
                'day', %s, %s, %s,
                %s, %s,
                0, 0, 0, 0,
                FALSE, 0, TRUE, TRUE,
                now(), now()
            WHERE NOT EXISTS (
                SELECT 1 FROM nodes
                WHERE node_type = 'ProjectedInventory'
                  AND scenario_id = %s
                  AND item_id = %s
                  AND location_id = %s
                  AND time_span_start = %s
                  AND active = TRUE
            )
            """,
            (
                uuid4(),
                scenario_id,
                item_id,
                location_id,
                day,
                day_end,
                day,
                series_id,
                bucket_sequence,
                scenario_id,
                item_id,
                location_id,
                day,
            ),
        )
        day = day_end


def _wire_demand_node_to_pi(
    db: psycopg.Connection,
    demand_node_id: UUID,
    item_id: UUID,
    location_id: UUID,
    scenario_id: UUID,
    bucket_start: date,
    bucket_end: date,
) -> None:
    db.execute(
        """
        INSERT INTO edges (
            edge_id, edge_type, from_node_id, to_node_id, scenario_id,
            priority, weight_ratio, effective_start, effective_end, active, created_at
        )
        SELECT
            gen_random_uuid(), 'consumes', %s, n_pi.node_id, %s,
            0, 1.0, %s, %s, TRUE, now()
        FROM nodes n_pi
        WHERE n_pi.node_type = 'ProjectedInventory'
          AND n_pi.item_id = %s
          AND n_pi.location_id = %s
          AND n_pi.scenario_id = %s
          AND n_pi.active = TRUE
          AND n_pi.time_span_start >= %s
          AND n_pi.time_span_start < %s
          AND NOT EXISTS (
              SELECT 1 FROM edges e
              WHERE e.from_node_id = %s
                AND e.to_node_id = n_pi.node_id
                AND e.edge_type = 'consumes'
                AND e.active = TRUE
          )
        """,
        (
            demand_node_id,
            scenario_id,
            bucket_start,
            bucket_end,
            item_id,
            location_id,
            scenario_id,
            bucket_start,
            bucket_end,
            demand_node_id,
        ),
    )


def _emit_commit_event(
    db: psycopg.Connection,
    scenario_id: UUID,
    demand_node_id: UUID,
    quantity: Decimal,
) -> None:
    db.execute(
        """
        INSERT INTO events (
            event_id, event_type, scenario_id, trigger_node_id,
            field_changed, new_quantity, processed, source, created_at
        ) VALUES (%s, 'ingestion_complete', %s, %s, 'quantity', %s, FALSE, 'api', %s)
        """,
        (uuid4(), scenario_id, demand_node_id, quantity, datetime.now(timezone.utc)),
    )


def _time_grain(granularity: str) -> str:
    return {"daily": "day", "weekly": "week", "monthly": "month"}[granularity]


def _bucket_end(bucket_start: date, granularity: str) -> date:
    if granularity == "daily":
        return bucket_start + timedelta(days=1)
    if granularity == "weekly":
        return bucket_start + timedelta(days=7)
    return _add_months(bucket_start, 1)


def _add_months(start: date, months: int) -> date:
    month_index = start.month - 1 + months
    year = start.year + month_index // 12
    month = month_index % 12 + 1
    day = min(start.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)
