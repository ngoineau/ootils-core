"""
APICS-Compliant Multi-Level MRP Engine — write path for POST /v1/mrp/run.

Since ADR-020 PAS 4 / #423 PR2 this engine no longer implements MRP math of its
own: it DELEGATES the whole calculation to the consolidated core
(``engine/mrp/core.py`` + ``loader.py``, the single source of MRP truth the
agents already use) and keeps ONLY its genuine job — materializing the result
into the graph and bookkeeping the run:

1. Full-regeneration purge of the previous plan (FPO-safe, ``cleanup_previous_run``).
2. Load planning data via the core loader (scenario-scoped, overlay-aware).
3. Run the core cascade: forecast consumption + time-phased LLC netting +
   lot sizing + pegging (``consume_demand`` → ``run_timephased``).
4. Materialize the core's planned-order tuples into PlannedSupply nodes +
   replenishes/requires/pegged_to edges (``graph_integration``), persist bucket
   records, and record the run in ``mrp_runs``.

The forecast-consumption / gross-to-net / lot-sizing / time-fence
re-implementations this engine used to carry are gone; parity with the core is
now a hard CI guard (``scripts/parity_mrp_engines.py``).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Optional, Set
from uuid import UUID, uuid4

import json

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.mrp.core import PlanningData, consume_demand, run_timephased
from ootils_core.engine.mrp.gross_to_net import BucketRecord
from ootils_core.engine.mrp.graph_integration import GraphIntegration
from ootils_core.engine.mrp.llc_calculator import LLCCalculator
from ootils_core.engine.mrp.loader import load_planning_data

logger = logging.getLogger(__name__)

BASELINE_SCENARIO_ID = UUID("00000000-0000-0000-0000-000000000001")


class UnsupportedBucketGrainError(ValueError):
    """Raised when a delegated MRP run requests a non-weekly bucket grain.

    The consolidated core (ADR-020) plans exclusively in weekly buckets; the
    day/month grains this engine used to honour (via its own gross-to-net
    calculator) no longer exist as a math path. Refusing loudly here — instead
    of silently running the weekly core under a 'DAY'/'MONTH' label — keeps
    ``mrp_runs.bucket_type`` honest (#423 PR2 review fix). A domain exception,
    not ``fastapi.HTTPException``: routers (the only callers that know about
    HTTP) translate it to a 422.
    """


# Shared wording (#423 PR2 review fix): both live entry points into this
# engine — POST /v1/mrp/run (apics_mode=true) and the deprecated
# POST /v1/mrp/apics/run — accept forecast_strategy/consumption_window_days
# for backward compatibility but no longer feed them to the core (per-item
# item_planning_params drives consumption). A single constant keeps the
# advisory wording identical across both routers instead of two hand-copied
# strings drifting apart.
ADVISORY_CONSUMPTION_WARNING = (
    "advisory since #423 — per-item item_planning_params drive consumption "
    "(forecast_consumption_strategy / consumption_window_days)"
)


@dataclass
class MrpRunConfig:
    """Configuration for an MRP run."""
    scenario_id: UUID = BASELINE_SCENARIO_ID
    location_id: Optional[UUID] = None
    item_ids: Optional[List[UUID]] = None
    horizon_days: int = 90
    bucket_grain: str = "week"
    start_date: Optional[date] = None
    recalculate_llc: bool = False
    forecast_strategy: str = "MAX"
    consumption_window_days: int = 7
    # Advisory messages the caller (router) precomputed — e.g. "you passed
    # forecast_strategy/consumption_window_days but the core ignores them,
    # per-item planning params drive consumption now" (#423 PR2 review fix).
    # The engine only carries these through into MrpRunResult.warnings /
    # mrp_runs.warnings; it does not generate them (it has no view of which
    # request fields were explicitly set vs defaulted).
    advisory_warnings: List[str] = field(default_factory=list)


@dataclass
class MrpRunResult:
    """Result of an MRP run."""
    run_id: UUID
    scenario_id: UUID
    status: str
    items_processed: int
    total_records: int
    action_messages: int
    nodes_created: int
    edges_created: int
    elapsed_ms: float
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class MrpApicsEngine:
    """
    APICS multi-level MRP write path — the graph-materialization surcouche over
    the consolidated core (ADR-020 PAS 4 / #423 PR2).

    The forecast consumption, gross-to-net LLC cascade, lot sizing, lead-time
    offset and BOM explosion all happen in ``engine/mrp/core.py`` (the single
    MRP truth). This engine loads via the core loader, runs the core cascade,
    and materializes the resulting planned orders into PlannedSupply nodes /
    edges + ``mrp_bucket_records`` while bookkeeping the ``mrp_runs`` record and
    the FPO-safe regeneration purge.
    """

    def __init__(self, db: DictRowConnection):
        self.db = db
        self.llc_calculator = LLCCalculator(db)
        self.graph = GraphIntegration(db, BASELINE_SCENARIO_ID)

    def run(self, config: MrpRunConfig) -> MrpRunResult:
        """
        Execute a full APICS MRP run by delegating the math to the core.

        Steps:
        1. Start an MRP run record and purge the previous plan (FPO-safe).
        2. Load planning data via the core loader (scenario-scoped).
        3. Run the core cascade (consume_demand → run_timephased).
        4. Materialize the core's planned orders into the graph + bucket records.

        Args:
            config: MRP run configuration

        Returns:
            MrpRunResult with run statistics

        Raises:
            UnsupportedBucketGrainError: config.bucket_grain != 'week'. Raised
                BEFORE any mrp_runs row is created, so a rejected request never
                leaves a bogus 'DAY'/'MONTH' run record behind.
        """
        if config.bucket_grain != "week":
            raise UnsupportedBucketGrainError(
                "the consolidated MRP core plans in weekly buckets (ADR-020); "
                "day/month grains were removed in #423"
            )

        start_time = time.monotonic()
        run_id = uuid4()
        errors: List[str] = []
        warnings: List[str] = list(config.advisory_warnings)

        self.graph.scenario_id = config.scenario_id

        if config.start_date is None:
            config.start_date = date.today()

        try:
            # 1. Create MRP run record
            self._create_run_record(run_id, config)

            # 1b. Regeneration contract: every APICS run rebuilds the FULL
            # planned-supply picture for the scenario, so deactivate ALL
            # previous PlannedSupply nodes/edges first (run_id=None → scenario
            # scope). Without this, each re-run stacks new PlannedSupply on
            # top of the previous run's, double-counting planned supply
            # (issue #337). Firm Planned Orders (FPO, migration 061, #346)
            # are excluded from this purge (cleanup_previous_run) AND netted
            # as engaged scheduled receipts by the core loader below — the two
            # go together, or a surviving FPO would double-plan its own
            # demand. Runs in the same transaction as the persist steps
            # below: on failure the except branch rolls back, restoring the
            # previous plan.
            self.graph.cleanup_previous_run()

            # 2. LLC drives the core's level-by-level cascade order (the loader
            # reads bom_lines.llc). Refresh it only on explicit request;
            # otherwise the loader reads the persisted codes, exactly like the
            # read-only core CLIs and the parity harness do.
            if config.recalculate_llc:
                self.llc_calculator.calculate_all()

            # 3. Delegate ALL math to the consolidated core (ADR-020 PAS 4).
            # Pass the scenario as a STRING so a baseline run degenerates the
            # overlay resolver to "no override" byte-for-byte like the parity
            # harness's run_core (str(BASELINE_SCENARIO_ID) == the loader's
            # BASELINE sentinel), and a fork run resolves its param overlay +
            # reads its own on-hand.
            planning = load_planning_data(
                self.db, config.horizon_days, scenario=str(config.scenario_id)
            )
            gross = consume_demand(planning)
            cascade = run_timephased(planning, gross)

            # 4. Materialize the core result into the graph (the kept métier).
            item_filter: Optional[Set[str]] = (
                {str(i) for i in config.item_ids} if config.item_ids else None
            )
            records = self._materialize_core_plan(
                planning, cascade["planned"], config.location_id, item_filter
            )

            # planning_params_map is empty: each record carries its release date
            # from the core (release_period_start), so the graph writer never
            # needs the lead-time offset it used to read from this map.
            receipt_node_ids, nodes_created, edges_created = self.graph.persist_planned_orders(
                run_id=run_id,
                records=records,
                planning_params_map={},
            )

            messages_created = self.graph.persist_action_messages(
                run_id=run_id,
                records=records,
            )

            self._persist_bucket_records(run_id, records)

            # Emit ingestion_complete events so PropagationEngine recalculates PI
            events_emitted = self.graph.emit_ingestion_events(receipt_node_ids)
            logger.info(
                "MRP run %s: %d planned orders materialized, %d ingestion_complete events",
                run_id, len(records), events_emitted,
            )

            items_processed = (
                len(config.item_ids) if config.item_ids else len(planning.involved)
            )

            elapsed_ms = (time.monotonic() - start_time) * 1000
            self._complete_run_record(run_id, "COMPLETED", elapsed_ms, warnings=warnings)

            return MrpRunResult(
                run_id=run_id,
                scenario_id=config.scenario_id,
                status="COMPLETED",
                items_processed=items_processed,
                total_records=len(records),
                action_messages=messages_created,
                nodes_created=nodes_created,
                edges_created=edges_created,
                elapsed_ms=elapsed_ms,
                errors=errors,
                warnings=warnings,
            )

        except Exception as e:
            logger.exception("MRP run failed: %s", e)
            errors.append(str(e))
            elapsed_ms = (time.monotonic() - start_time) * 1000

            # Coherence on failure: run() does not re-raise (it returns a
            # FAILED result), so the caller's commit-on-success path would
            # otherwise persist the cleanup (1b) and any partial writes.
            # Roll back to restore the previous planned-supply picture, then
            # re-record the run as FAILED (the original run record was part
            # of the rolled-back transaction).
            try:
                self.db.rollback()
                self._create_run_record(run_id, config)
                self._complete_run_record(run_id, "FAILED", elapsed_ms, str(e), warnings=warnings)
            except Exception:
                logger.exception("Failed to record FAILED status for MRP run %s", run_id)

            return MrpRunResult(
                run_id=run_id,
                scenario_id=config.scenario_id,
                status="FAILED",
                items_processed=0,
                total_records=0,
                action_messages=0,
                nodes_created=0,
                edges_created=0,
                elapsed_ms=elapsed_ms,
                errors=errors,
                warnings=warnings,
            )

    def _materialize_core_plan(
        self,
        planning: PlanningData,
        planned: list,
        location_id: Optional[UUID],
        item_filter: Optional[Set[str]],
    ) -> List[BucketRecord]:
        """Map the core cascade's planned-order tuples to materializable rows.

        Each core tuple is (item, qty, release_bucket, need_bucket, kind,
        past_due). The receipt lands at the need bucket, the release at the
        release bucket; BOTH dates are anchored on the core's horizon_start
        (never Monday-snapped), so the graph matches the core's schedule exactly
        (ADR-020). ``item_filter`` (string item ids) restricts what gets
        materialized to a requested subset AFTER the full cascade has run, so a
        single-item API run still benefits from the complete BOM explosion.
        """
        horizon_start = planning.horizon_start
        records: List[BucketRecord] = []
        for item, qty, release_bucket, need_bucket, _kind, _past_due in planned:
            if item_filter is not None and str(item) not in item_filter:
                continue
            item_uuid = item if isinstance(item, UUID) else UUID(str(item))
            order_qty = Decimal(str(qty))
            need = int(need_bucket)
            receipt_date = horizon_start + timedelta(weeks=need)
            release_date = horizon_start + timedelta(weeks=int(release_bucket))
            records.append(BucketRecord(
                bucket_id=uuid4(),
                item_id=item_uuid,
                location_id=location_id,
                period_start=receipt_date,
                period_end=receipt_date + timedelta(days=7),
                bucket_sequence=need,
                gross_requirements=order_qty,
                scheduled_receipts=Decimal("0"),
                projected_on_hand=Decimal("0"),
                net_requirements=order_qty,
                planned_order_receipts=order_qty,
                planned_order_releases=order_qty,
                projected_on_hand_after=Decimal("0"),
                has_shortage=True,
                shortage_qty=order_qty,
                llc=int(planning.llc.get(item, 0) or 0),
                release_period_start=release_date,
            ))
        return records

    def _create_run_record(self, run_id: UUID, config: MrpRunConfig):
        """Create the mrp_runs record.

        bucket_type is always 'WEEK': run() refuses any config.bucket_grain !=
        'week' (UnsupportedBucketGrainError) BEFORE this is ever called, so the
        persisted grain can never lie about the core's weekly-only cascade
        (#423 PR2 review fix — the old day/month mapping is gone, not just
        unreachable).
        """
        self.db.execute("""
            INSERT INTO mrp_runs (
                run_id, scenario_id, location_id,
                status, run_type, horizon_days,
                bucket_type, llc_regeneration,
                started_at
            ) VALUES (
                %s, %s, %s,
                'running', 'APICS_FULL', %s,
                'WEEK', %s,
                NOW()
            )
        """, (
            run_id,
            config.scenario_id,
            config.location_id,
            config.horizon_days,
            config.recalculate_llc,
        ))

    def _complete_run_record(
        self,
        run_id: UUID,
        status: str,
        elapsed_ms: float,
        error_msg: Optional[str] = None,
        warnings: Optional[List[str]] = None,
    ):
        """Update the mrp_runs record with completion status."""
        # Map our status to DB enum values (lowercase)
        db_status = status.lower() if status else 'failed'
        errors_json = [] if not error_msg else [{"error": error_msg}]
        warnings_json = warnings or []

        self.db.execute("""
            UPDATE mrp_runs
            SET status = %s,
                completed_at = NOW(),
                execution_time_ms = %s,
                errors = %s,
                warnings = %s
            WHERE run_id = %s
        """, (
            db_status, int(elapsed_ms), json.dumps(errors_json),
            json.dumps(warnings_json), run_id,
        ))

    def _persist_bucket_records(self, run_id: UUID, records: List[BucketRecord]):
        """Persist MRP bucket records."""
        for record in records:
            # location_id is NOT NULL in DB; use a sentinel if None
            loc_id = record.location_id or UUID("00000000-0000-0000-0000-000000000001")

            self.db.execute("""
                INSERT INTO mrp_bucket_records (
                    bucket_id, run_id, item_id, location_id,
                    period_start, period_end, bucket_sequence,
                    gross_requirements, scheduled_receipts,
                    projected_on_hand, net_requirements,
                    planned_order_receipts, planned_order_releases,
                    has_shortage, shortage_qty, llc,
                    time_fence_zone, lot_size_rule_applied
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s
                )
            """, (
                record.bucket_id,
                run_id,
                record.item_id,
                loc_id,
                record.period_start,
                record.period_end,
                record.bucket_sequence,
                record.gross_requirements,
                record.scheduled_receipts,
                record.projected_on_hand,
                record.net_requirements,
                record.planned_order_receipts,
                record.planned_order_releases,
                record.has_shortage,
                record.shortage_qty,
                record.llc,
                record.time_fence_zone,
                record.lot_size_rule_applied,
            ))
