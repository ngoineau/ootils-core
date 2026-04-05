"""
calc_run.py — CalcRun lifecycle management with PostgreSQL advisory locking.

Advisory lock strategy:
  pg_try_advisory_lock(hashtext(str(scenario_id)))
  Returns NULL if already locked — caller gets None back.

State machine: pending → running → completed | completed_stale | failed
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from ootils_core.models import CalcRun, Scenario


class CalcRunManager:
    """
    Manages CalcRun lifecycle with advisory locking.

    One active calc run per scenario at a time (enforced via pg_try_advisory_lock).
    """

    def start_calc_run(
        self,
        scenario_id: UUID,
        event_ids: list[UUID],
        db,
    ) -> Optional[CalcRun]:
        """
        Try to acquire an advisory lock for this scenario and start a calc run.

        Returns None if another run is already in progress (lock held).

        Coalesces all pending (unprocessed) events for this scenario,
        not just the triggering event_ids.
        """
        # Try advisory lock — hashtext truncates to int32 internally
        row = db.execute(
            "SELECT pg_try_advisory_lock(('x' || md5(%s))::bit(64)::bigint) AS locked",
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

        db.execute(
            """
            INSERT INTO calc_runs (
                calc_run_id, scenario_id, triggered_by_event_ids,
                is_full_recompute, dirty_node_count,
                nodes_recalculated, nodes_unchanged,
                status, started_at, created_at
            ) VALUES (
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s
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
            ),
        )

        return run

    def complete_calc_run(
        self,
        run: CalcRun,
        scenario: Scenario,
        db,
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

        # Release advisory lock
        db.execute(
            "SELECT pg_advisory_unlock(('x' || md5(%s))::bit(64)::bigint)",
            (str(run.scenario_id),),
        )

    def fail_calc_run(
        self,
        run: CalcRun,
        error: str,
        db,
    ) -> None:
        """Mark a calc run as failed with an error message.

        Uses autocommit on the failure UPDATE so the audit record is persisted
        even if the caller's transaction rolls back (HIGH-4).
        """
        now = datetime.now(timezone.utc)
        run.status = "failed"
        run.completed_at = now
        run.error_message = error

        # Persist failure record independently of the caller's transaction.
        # autocommit=True ensures the UPDATE commits immediately even if the
        # outer transaction is about to roll back.
        try:
            with db.connection.cursor() as fail_cur:
                prev_autocommit = db.connection.autocommit
                db.connection.autocommit = True
                try:
                    fail_cur.execute(
                        """
                        UPDATE calc_runs
                        SET status = 'failed',
                            completed_at = %s,
                            error_message = %s
                        WHERE calc_run_id = %s
                        """,
                        (now, error, run.calc_run_id),
                    )
                finally:
                    db.connection.autocommit = prev_autocommit
        except Exception:
            # Last-resort fallback: use the connection as-is
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

        # Release advisory lock (best-effort on failure) — HIGH-3: use 64-bit hash
        try:
            db.execute(
                "SELECT pg_advisory_unlock(('x' || md5(%s))::bit(64)::bigint)",
                (str(run.scenario_id),),
            )
        except Exception:
            pass

    def recover_pending_runs(self, db) -> list[CalcRun]:
        """
        On startup: transition all 'running' runs to 'failed' (they were interrupted).
        Return all 'pending' runs so the engine can retry them.

        Called once on engine startup.

        Advisory lock note:
        PostgreSQL advisory locks are session-scoped. If the engine process crashes,
        the DB connection is closed, and Postgres automatically releases all advisory
        locks held by that session. There is therefore no risk of orphaned advisory
        locks surviving a crash — they are cleaned up at the transport level.

        What this method handles: the *calc_runs table state*, which does not auto-recover.
        Running rows must be transitioned to 'failed' so the engine does not think
        a run is in progress when it restarts.
        """
        now = datetime.now(timezone.utc)

        # Mark running runs as failed (they crashed)
        db.execute(
            """
            UPDATE calc_runs
            SET status = 'failed',
                completed_at = %s,
                error_message = 'Recovered on startup — previous run was interrupted'
            WHERE status = 'running'
            """,
            (now,),
        )

        # Fetch pending runs
        rows = db.execute(
            """
            SELECT * FROM calc_runs
            WHERE status = 'pending'
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
