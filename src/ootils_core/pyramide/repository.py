from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import psycopg

from ootils_core.api.dependencies import BASELINE_SCENARIO_ID

from .models import PyramideRunResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PyramidePersistedRun:
    run_id: UUID
    snapshot_id: UUID
    forecast_id: UUID


@dataclass(frozen=True)
class PyramideRunSummary:
    run_id: UUID
    snapshot_id: UUID
    forecast_id: UUID
    item_id: UUID
    location_id: UUID
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
            """
            SELECT dh.booked_date AS demand_date,
                   COALESCE(SUM(dh.ordered_quantity), 0) AS total_qty
            FROM demand_history dh
            WHERE dh.item_id = %s
              AND dh.warehouse_id = %s
              AND dh.stream = 'regular'
              AND (dh.fulfillment IS NULL OR dh.fulfillment <> 'inter_entity')
              AND dh.booked_date IS NOT NULL
              AND dh.booked_date < CURRENT_DATE
              AND dh.booked_date >= CURRENT_DATE - (%s::int * INTERVAL '1 day')
            GROUP BY dh.booked_date
            ORDER BY dh.booked_date ASC
            """,
            (item_id, warehouse_external_id, lookback_days),
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


def persist_run(db: psycopg.Connection, result: PyramideRunResult) -> PyramidePersistedRun:
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
        db.execute(
            """
            INSERT INTO forecast_values (
                value_id, forecast_id, forecast_date, quantity, method
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (uuid4(), forecast_id, value.forecast_date, value.quantity, value.method),
        )

    db.execute(
        """
        INSERT INTO pyramide_runs (
            run_id, forecast_id, item_id, location_id, scenario_id,
            horizon_start, horizon_end, granularity, method,
            model_strategy, recon_method, random_seed, code_version,
            selected_model, engine_backend, source_history_count, status,
            deterministic_artifact
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'generated', 'forecast_values')
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
    return PyramidePersistedRun(run_id=run_id, snapshot_id=snapshot_id, forecast_id=forecast_id)


def fetch_run_summary(db: psycopg.Connection, run_id: UUID) -> PyramideRunSummary | None:
    row = db.execute(
        """
        SELECT
            pr.run_id, ps.snapshot_id, pr.forecast_id, pr.item_id, pr.location_id,
            pr.scenario_id, pr.horizon_start, pr.horizon_end, pr.granularity,
            pr.method, pr.model_strategy, pr.recon_method, pr.random_seed,
            pr.code_version, pr.selected_model, pr.engine_backend,
            pr.source_history_count, pr.status,
            pr.deterministic_artifact, pr.created_at, pr.committed_at,
            ps.value_count, ps.total_quantity
        FROM pyramide_runs pr
        JOIN pyramide_snapshots ps ON ps.run_id = pr.run_id
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
    summary = fetch_run_summary(db, run_id)
    if summary is None:
        return None

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
