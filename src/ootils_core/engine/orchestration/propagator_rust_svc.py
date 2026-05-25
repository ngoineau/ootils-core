"""
propagator_rust_svc.py — adapter to the standalone Rust engine service
(ADR-017 Architecture B, phase 8 production integration).

This adapter lets the existing FastAPI flow use the Rust gRPC service
without changing API contracts. It overrides `process_event` (not
just `_propagate`) because the Architecture B service handles the
entire propagation lifecycle (graph in RAM, WAL, async Postgres
write-behind) — there's no in-process Python work to do beyond the
gRPC call.

Trade-offs vs. the other engines:
- ✅ Sub-millisecond compute, sustained 5K rps validated.
- ✅ Multi-scenario via COW, fork in 30 ms.
- ⚠️ Requires a running `ootils-engine` process accessible via gRPC.
  The endpoint is read from `OOTILS_ENGINE_ADDR` (default
  127.0.0.1:50051). If the endpoint is unreachable the engine
  factory falls back to SQL — opt-in safety net.
- ⚠️ Postgres state is eventually consistent (write-behind ~100ms
  lag). Reads that need read-your-writes consistency should go via
  the Rust service's GetNode RPC (future Phase 8.5: read proxy).
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Optional
from uuid import UUID, uuid4

import psycopg

from ootils_core.engine.orchestration.calc_run import CalcRunManager
from ootils_core.engine.orchestration.propagator import PropagationEngine
from ootils_core.engine_rust_service import EngineClient

if TYPE_CHECKING:
    from ootils_core.models import CalcRun

logger = logging.getLogger(__name__)


class RustServicePropagationEngine(PropagationEngine):
    """
    Delegates propagation to the standalone ootils-engine gRPC service.

    Architecture-B integration: the engine OWNS the in-RAM graph and
    the Postgres write-behind. Python is reduced to:
    1. Insert the event in `events` table (audit trail).
    2. Reserve a calc_run row (so `calc_runs` tracks the audit chain).
    3. Call `Propagate` via gRPC.
    4. Mark the calc_run complete with the result counts.

    Notably skipped vs. the SQL/Python engines:
    - DirtyFlagManager.mark_dirty + flush_to_postgres
      (Rust service uses its own in-RAM dirty cascade)
    - SHORTAGES_SQL + CLEAR_DIRTY_SQL
      (Rust service detects shortages in-memory + clears its own state)

    Configuration via env vars:
        OOTILS_ENGINE_ADDR : gRPC endpoint (default 127.0.0.1:50051)
    """

    def __init__(self, *args, addr: Optional[str] = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._addr = addr or os.environ.get("OOTILS_ENGINE_ADDR", "127.0.0.1:50051")
        # The gRPC channel is created lazily on first use — keeps test
        # boot cheap when the engine isn't actually exercised.
        self._client: Optional[EngineClient] = None

    def _ensure_client(self) -> EngineClient:
        if self._client is None:
            logger.info("RustServicePropagationEngine: connecting to %s", self._addr)
            self._client = EngineClient.connect(self._addr)
        return self._client

    def process_event(
        self,
        event_id: UUID,
        scenario_id: UUID,
        db: psycopg.Connection,
    ) -> Optional["CalcRun"]:
        """
        Override the full lifecycle (not just `_propagate`). The Rust
        service is authoritative for the propagation graph, so the
        Python-side dirty tracking + SHORTAGES query are skipped.
        """
        # Load the event from DB to get trigger info.
        event_row = db.execute(
            "SELECT event_type, trigger_node_id FROM events WHERE event_id = %s",
            (event_id,),
        ).fetchone()
        if event_row is None:
            logger.warning("event %s not found", event_id)
            return None

        if isinstance(event_row, dict):
            event_type = event_row["event_type"]
            trigger_node_id = event_row.get("trigger_node_id")
        else:
            event_type = event_row[0]
            trigger_node_id = event_row[1]

        if trigger_node_id is None:
            logger.info(
                "event %s has no trigger_node_id, skipping rust-svc propagation",
                event_id,
            )
            return None

        # Acquire the scenario advisory lock + create the calc_run row.
        # We still do this on the Python side so calc_runs stays a
        # reliable audit trail.
        calc_run = self._calc_run_mgr.start_calc_run(
            scenario_id=scenario_id,
            event_ids=[event_id],
            db=db,
        )
        if calc_run is None:
            logger.info(
                "scenario %s locked by another calc_run, rust-svc propagation skipped",
                scenario_id,
            )
            return None

        # Call the Rust service. The compute happens in its in-RAM
        # graph; write-behind handles Postgres asynchronously.
        client = self._ensure_client()
        try:
            result = client.propagate(
                scenario_id=scenario_id,
                event_id=event_id,
                event_type=event_type,
                trigger_node_id=UUID(str(trigger_node_id)),
            )
        except Exception as exc:
            logger.exception(
                "RustServicePropagationEngine: gRPC Propagate failed for event %s",
                event_id,
            )
            self._calc_run_mgr.fail_calc_run(calc_run, str(exc), db)
            raise

        # Mirror the counters into calc_run for audit.
        calc_run.nodes_recalculated = result.nodes_processed
        calc_run.dirty_node_count = result.nodes_processed

        logger.info(
            "RustServicePropagationEngine: event=%s processed=%d changed=%d "
            "shortages=%d compute_ms=%.2f total_ms=%.2f",
            event_id,
            result.nodes_processed,
            result.nodes_changed,
            result.shortages_detected,
            result.compute_ms,
            result.total_ms,
        )

        # Complete the calc_run. Note: we DON'T reconcile shortages
        # via shortage_detector.resolve_stale here — the Rust service
        # owns the shortage detection. A consistency check between
        # the Rust service's view + Postgres is a future task.
        self._finish_run_without_shortage_resolve(calc_run, scenario_id, db)
        return calc_run

    def _finish_run_without_shortage_resolve(
        self,
        calc_run: "CalcRun",
        scenario_id: UUID,
        db: psycopg.Connection,
    ) -> None:
        """Bare-minimum completion: mark calc_run complete, flag event
        as processed. Skips the shortage_detector.resolve_stale step
        because the Rust service handles that internally."""
        from ootils_core.models import Scenario

        # Reuse the parent's completion logic minus shortage detector.
        scenario_row = db.execute(
            "SELECT * FROM scenarios WHERE scenario_id = %s",
            (scenario_id,),
        ).fetchone()
        if scenario_row:
            if isinstance(scenario_row, dict):
                scenario = Scenario(
                    scenario_id=UUID(str(scenario_row["scenario_id"])),
                    name=scenario_row["name"],
                    baseline_snapshot_id=(
                        UUID(str(scenario_row["baseline_snapshot_id"]))
                        if scenario_row.get("baseline_snapshot_id")
                        else None
                    ),
                    is_baseline=bool(scenario_row.get("is_baseline", False)),
                )
            else:
                scenario = Scenario(scenario_id=scenario_id, name="unknown")
        else:
            scenario = Scenario(scenario_id=scenario_id, name="unknown")

        self._calc_run_mgr.complete_calc_run(calc_run, scenario, db)

        if calc_run.triggered_by_event_ids:
            db.execute(
                "UPDATE events SET processed = TRUE WHERE event_id = ANY(%s)",
                (list(calc_run.triggered_by_event_ids),),
            )
