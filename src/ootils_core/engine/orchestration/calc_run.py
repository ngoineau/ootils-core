"""
calc_run.py — CalcRun lifecycle management with PostgreSQL advisory locking.

Advisory lock strategy:
  pg_try_advisory_lock(hashtext(str(scenario_id)))
  Returns NULL if already locked — caller gets None back.

State machine: pending → running → completed | completed_stale | interrupted | failed
"""
from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from ootils_core.db.types import DictRowConnection
from ootils_core.engine.events import emit_stream_event
from ootils_core.models import CalcRun, Scenario

logger = logging.getLogger(__name__)


def _resolve_code_version() -> str:
    """Resolve the code identity ONCE, at module import (decision basis, C2 §3).

    Precedence: the OOTILS_CODE_VERSION env var (the production-safe path — a
    deployment stamps it), else the short git sha of the working tree, else the
    literal 'unknown'. The git call is a subprocess run EXACTLY ONCE here at
    import — never per calc run — and is fully guarded (timeout + broad except),
    so a non-repo deploy (installed wheel, no git binary) degrades to 'unknown'
    rather than raising. Output is captured so the child stays silent (no
    stdout/stderr leak into the process, honouring the no-print rule).
    """
    env = os.environ.get("OOTILS_CODE_VERSION")
    if env and env.strip():
        return env.strip()
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        sha = completed.stdout.strip()
        if sha:
            return sha
    except Exception:  # noqa: BLE001 — git absent / not a repo / timeout => 'unknown'
        logger.debug("code_version: git short sha unavailable, defaulting to 'unknown'")
    return "unknown"


# Resolved ONCE at import (C2 §3: "code_version résolu UNE FOIS à l'import …
# JAMAIS un subprocess par run"). Every calc run stamps this constant.
_CODE_VERSION: str = _resolve_code_version()


def _resolve_engine_flavor() -> str:
    """Normalised OOTILS_ENGINE flavour that ran this run — provenance stamp.

    Mirrors the dispatch in api/routers/events._build_propagation_engine (the
    authority): 'sql'/'' -> sql, 'python' -> python, 'rust' -> rust,
    'rust-svc'/'rust_svc' -> rust-svc, anything else -> sql (the SAME silent
    fallback the dispatcher applies), so the stamp reflects the engine that
    ACTUALLY ran, not the raw env string. A per-run env read is a cheap
    deterministic string op (not a subprocess).
    """
    flavor = os.environ.get("OOTILS_ENGINE", "sql").strip().lower()
    if flavor in ("sql", ""):
        return "sql"
    if flavor == "python":
        return "python"
    if flavor == "rust":
        return "rust"
    if flavor in ("rust-svc", "rust_svc"):
        return "rust-svc"
    return "sql"


class CalcRunManager:
    """
    Manages CalcRun lifecycle with advisory locking.

    One active calc run per scenario at a time (enforced via pg_try_advisory_lock).
    """

    def start_calc_run(
        self,
        scenario_id: UUID,
        event_ids: list[UUID],
        db: DictRowConnection,
    ) -> Optional[CalcRun]:
        """
        Try to acquire an advisory lock for this scenario and start a calc run.

        Returns None if another run is already in progress (lock held).

        Coalesces all pending (unprocessed) events for this scenario,
        not just the triggering event_ids.
        """
        # Try advisory lock — hashtext() returns a native int32 (PostgreSQL internal);
        # using it directly avoids the MD5-128bit→64bit truncation that caused hash
        # collisions between unrelated scenario_ids (fix for issue #156).
        row = db.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s)::bigint) AS locked",
            (str(scenario_id),),
        ).fetchone()

        if not row or not row["locked"]:
            return None

        # Coalesce all unprocessed events for this scenario
        pending_rows = db.execute(
            """
            SELECT event_id FROM events
            WHERE scenario_id = %s AND processed = FALSE
            ORDER BY created_at ASC
            """,
            (scenario_id,),
        ).fetchall()

        all_event_ids = [UUID(str(r["event_id"])) for r in pending_rows]
        # Merge with the provided event_ids (dedup)
        merged = list({*all_event_ids, *event_ids})

        now = datetime.now(timezone.utc)
        run = CalcRun(
            calc_run_id=uuid4(),
            scenario_id=scenario_id,
            triggered_by_event_ids=merged,
            is_full_recompute=False,
            status="running",
            started_at=now,
            created_at=now,
        )

        # Decision-basis stamps (C2 §3). anchor_date is resolved IN-SQL as
        # COALESCE(scenarios.as_of_date, CURRENT_DATE) so the PAST-principle
        # anchor is the DB's own date (no app/DB timezone drift) and matches the
        # projection kernel's own "today"; a scenario_id absent from `scenarios`
        # (never expected — baseline + forks are always present) still degrades
        # cleanly to CURRENT_DATE via COALESCE. engine_flavor + code_version are
        # provenance, NULL-honest for any pre-C2 / uninstrumented caller.
        db.execute(
            """
            INSERT INTO calc_runs (
                calc_run_id, scenario_id, triggered_by_event_ids,
                is_full_recompute, dirty_node_count,
                nodes_recalculated, nodes_unchanged,
                status, started_at, created_at,
                anchor_date, engine_flavor, code_version
            ) VALUES (
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                COALESCE(
                    (SELECT s.as_of_date FROM scenarios s WHERE s.scenario_id = %s),
                    CURRENT_DATE
                ),
                %s, %s
            )
            """,
            (
                run.calc_run_id,
                run.scenario_id,
                run.triggered_by_event_ids,
                run.is_full_recompute,
                run.dirty_node_count,
                run.nodes_recalculated,
                run.nodes_unchanged,
                run.status,
                run.started_at,
                run.created_at,
                run.scenario_id,           # anchor_date COALESCE subquery arg
                _resolve_engine_flavor(),  # engine_flavor
                _CODE_VERSION,             # code_version (resolved once at import)
            ),
        )

        return run

    def complete_calc_run(
        self,
        run: CalcRun,
        scenario: Scenario,
        db: DictRowConnection,
    ) -> None:
        """
        Mark a calc run as completed or completed_stale.

        completed_stale: the scenario has diverged from baseline
        (scenario.baseline_snapshot_id is set).
        """
        if scenario.baseline_snapshot_id is not None:
            final_status = "completed_stale"
        else:
            final_status = "completed"

        now = datetime.now(timezone.utc)
        run.status = final_status
        run.completed_at = now

        db.execute(
            """
            UPDATE calc_runs
            SET status = %s,
                completed_at = %s,
                nodes_recalculated = %s,
                nodes_unchanged = %s
            WHERE calc_run_id = %s
            """,
            (
                final_status,
                now,
                run.nodes_recalculated,
                run.nodes_unchanged,
                run.calc_run_id,
            ),
        )

        # Mark all coalesced events as processed
        if run.triggered_by_event_ids:
            db.execute(
                """
                UPDATE events
                SET processed = TRUE, processed_at = %s
                WHERE event_id = ANY(%s)
                """,
                (now, run.triggered_by_event_ids),
            )

        # Fleet emission (#401 AN-1). GRANULARITY = RUN: exactly one
        # calc_run_finished per terminal run, and one shortage_detected iff this
        # run PERSISTED shortages. Both go on the SAME connection/transaction as
        # the UPDATE above — atomic with the run's completion (a rolled-back
        # completion emits nothing). The shortage count comes from the
        # authoritative persistence system (the `shortages` table, ADR-021,
        # ShortageDetector-owned) via COUNT WHERE calc_run_id: the propagator
        # persists shortages one PI at a time and never aggregates a per-run
        # count, so reading the table back here is the ONE place the run-level
        # count exists without threading a counter through the (SQL AND Python)
        # propagator public signatures. Works identically for both engines.
        self._emit_run_events(run, final_status, db)

        # Release advisory lock (must use same hash as acquire)
        db.execute(
            "SELECT pg_advisory_unlock(hashtext(%s)::bigint)",
            (str(run.scenario_id),),
        )

    def _emit_run_events(
        self,
        run: CalcRun,
        final_status: str,
        db: DictRowConnection,
    ) -> None:
        """Emit the RUN-level fleet events for a terminal calc run (#401 AN-1).

        Always one ``calc_run_finished``; additionally one ``shortage_detected``
        when this run persisted >=1 shortage row. The shortage count is read from
        the ``shortages`` table (the canonical persistence system, ADR-021) by
        this run's calc_run_id — the only place a per-run aggregate exists. Same
        transaction as the completion write (atomic); never a swallowed except —
        an emission failure must surface so the run is not silently unstreamed.
        """
        emit_stream_event(
            db,
            "calc_run_finished",
            run.scenario_id,
            field_changed=final_status,
            new_text=str(run.calc_run_id),
            new_quantity=run.nodes_recalculated,
        )

        shortage_row = db.execute(
            "SELECT COUNT(*) AS n FROM shortages WHERE calc_run_id = %s",
            (run.calc_run_id,),
        ).fetchone()
        shortage_count = int(shortage_row["n"]) if shortage_row else 0
        if shortage_count > 0:
            emit_stream_event(
                db,
                "shortage_detected",
                run.scenario_id,
                field_changed="shortage_detected",
                new_quantity=shortage_count,
                new_text=str(run.calc_run_id),
            )

    def fail_calc_run(
        self,
        run: CalcRun,
        error: str,
        db: DictRowConnection,
    ) -> None:
        """Mark a calc run as failed with an error message.

        Uses autocommit on the failure UPDATE so the audit record is persisted
        even if the caller's transaction rolls back (HIGH-4).

        No ``calc_run_finished`` fleet event is emitted here (#401 AN-1): this is
        the EXCEPTION path — the propagator has already done ROLLBACK TO SAVEPOINT
        and the caller re-raises, so the transaction is being torn down. An
        ``events`` INSERT on a rolling-back transaction leaves no row (it only
        burns a stream_seq, migration 063), so a failed-run event would be a
        phantom the stream never sees. calc_run_finished is emitted only from
        complete_calc_run, on the COMMITTED terminal path (completed |
        completed_stale). Surfacing a run failure to the fleet is the caller's
        job (the 503 the events router raises), not a best-effort stream write.
        """
        now = datetime.now(timezone.utc)
        run.status = "failed"
        run.completed_at = now
        run.error_message = error

        # Persist failure record using the connection directly (psycopg3 compatible).
        # db.connection.cursor() does not exist in psycopg3 — use db.execute() directly.
        import logging as _logging
        _logging.getLogger(__name__).warning(
            "fail_calc_run: persisting failure for run %s (error: %s)",
            run.calc_run_id, error,
        )
        try:
            db.execute(
                """
                UPDATE calc_runs
                SET status = 'failed',
                    completed_at = %s,
                    error_message = %s
                WHERE calc_run_id = %s
                """,
                (now, error, run.calc_run_id),
            )
        except Exception:
            pass

        # Release advisory lock (best-effort on failure — same hash as acquire)
        try:
            db.execute(
                "SELECT pg_advisory_unlock(hashtext(%s)::bigint)",
                (str(run.scenario_id),),
            )
        except Exception:
            pass

    def recover_pending_runs(self, db: DictRowConnection) -> list[CalcRun]:
        """
        On startup: transition all 'running' runs to 'interrupted'.
        Return all replayable runs (pending + interrupted) so the engine can retry them.

        Called once on engine startup.

        Advisory lock note:
        PostgreSQL advisory locks are session-scoped. If the engine process crashes,
        the DB connection is closed, and Postgres automatically releases all advisory
        locks held by that session. There is therefore no risk of orphaned advisory
        locks surviving a crash — they are cleaned up at the transport level.

        What this method handles: the *calc_runs table state*, which does not auto-recover.
        Running rows must be transitioned to 'interrupted' so the engine does not think
        a run is in progress when it restarts, while keeping the reason explicit.
        """
        now = datetime.now(timezone.utc)

        # Mark running runs as interrupted (they crashed mid-flight)
        db.execute(
            """
            UPDATE calc_runs
            SET status = 'interrupted',
                completed_at = %s,
                error_message = 'Recovered on startup — previous run was interrupted'
            WHERE status = 'running'
            """,
            (now,),
        )

        # Fetch replayable runs
        rows = db.execute(
            """
            SELECT * FROM calc_runs
            WHERE status IN ('pending', 'interrupted')
            ORDER BY created_at ASC
            """,
        ).fetchall()

        return [_row_to_calc_run(r) for r in rows]


def _row_to_calc_run(row: dict) -> CalcRun:
    """Convert a DB row to a CalcRun dataclass."""
    return CalcRun(
        calc_run_id=UUID(str(row["calc_run_id"])),
        scenario_id=UUID(str(row["scenario_id"])),
        triggered_by_event_ids=[UUID(str(e)) for e in (row.get("triggered_by_event_ids") or [])],
        is_full_recompute=bool(row.get("is_full_recompute", False)),
        dirty_node_count=row.get("dirty_node_count"),
        nodes_recalculated=int(row.get("nodes_recalculated", 0)),
        nodes_unchanged=int(row.get("nodes_unchanged", 0)),
        status=row.get("status", "pending"),
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        error_message=row.get("error_message"),
        created_at=row.get("created_at") or datetime.now(timezone.utc),
    )
