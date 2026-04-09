"""
zone_transition.py — ZoneTransitionEngine: calendar-triggered zone roll-forward.

Handles two transition types (ADR-002d, ADR-006):
  - weekly_to_daily: fires every Monday — the approaching week enters the daily zone.
  - monthly_to_weekly: fires on the 1st of each month — the approaching month enters weekly zone.

Key design decisions (ADR-006):
  - Per-series atomic transaction (savepoint per series, archive+create+rewire+dirty atomic)
  - Progress tracking via zone_transition_runs with UNIQUE(job_type, transition_date)
  - Idempotent: safe to re-run N times — already-done series are skipped
  - All DB writes go through GraphStore (no inline SQL in business logic)
  - Global advisory lock via pg_try_advisory_lock('zone_transition') prevents concurrent runs
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import psycopg

from ootils_core.engine.kernel.graph.store import GraphStore
from ootils_core.models import Node, NodeTypeTemporalPolicy

logger = logging.getLogger(__name__)

# Advisory lock key for all zone transition jobs
_ZONE_TRANSITION_LOCK_KEY = "zone_transition"


# ---------------------------------------------------------------------------
# Calendar boundary helpers (from ADR-002d)
# ---------------------------------------------------------------------------


def next_weekly_boundary(today: date, daily_horizon_weeks: int) -> date:
    """
    Return the Monday of the week that will enter the daily zone.

    daily_horizon_weeks: number of full weeks in the daily zone (e.g. 13 for ~90 days).
    """
    cutoff = today + timedelta(weeks=daily_horizon_weeks)
    # Snap to Monday of that week (weekday(): Monday=0, Sunday=6)
    return cutoff - timedelta(days=cutoff.weekday())


def next_monthly_boundary(today: date, weekly_horizon_months: int) -> date:
    """
    Return the 1st of the month that will enter the weekly zone.

    weekly_horizon_months: number of months in the weekly zone (e.g. 3 for ~90 days).
    """
    target_month = today.month + weekly_horizon_months
    target_year = today.year + (target_month - 1) // 12
    target_month = ((target_month - 1) % 12) + 1
    return date(target_year, target_month, 1)


def is_monday(d: date) -> bool:
    """Return True if d is a Monday (weekday() == 0)."""
    return d.weekday() == 0


def is_first_of_month(d: date) -> bool:
    """Return True if d is the 1st of a month."""
    return d.day == 1


# ---------------------------------------------------------------------------
# ZoneTransitionEngine
# ---------------------------------------------------------------------------


class ZoneTransitionEngine:
    """
    Executes zone roll-forward transitions for a projection series.

    Responsibilities:
    - Detect which transition(s) are due on as_of_date
    - Ensure idempotency via zone_transition_runs table
    - Delegate structural mutations to GraphStore
    - Use advisory lock to prevent concurrent transitions

    No raw SQL — all DB interaction goes through GraphStore or dedicated
    transition helpers that use the provided connection.
    """

    def run_transition(
        self,
        series_id: UUID,
        scenario_id: UUID,
        as_of_date: date,
        db: psycopg.Connection,
    ) -> dict[str, bool]:
        """
        Execute the applicable zone transition(s) for a series on as_of_date.

        Determines which jobs apply:
          - is_monday → weekly_to_daily
          - is_first_of_month → monthly_to_weekly
          - both → combined (monthly first, then weekly, per ADR-006 B4)

        Each job is individually idempotent — already-completed transitions
        for (job_type, transition_date) are skipped without error.

        Returns a dict indicating which jobs were executed:
          {
            "weekly_to_daily": bool,
            "monthly_to_weekly": bool,
          }

        Args:
            series_id: The projection series to process.
            scenario_id: The scenario owning this series.
            as_of_date: The planning date (today for scheduled runs).
            db: psycopg Connection with dict_row factory.
        """
        store = GraphStore(db)

        results: dict[str, bool] = {
            "weekly_to_daily": False,
            "monthly_to_weekly": False,
        }

        _monday = is_monday(as_of_date)
        _first = is_first_of_month(as_of_date)

        if not _monday and not _first:
            logger.debug(
                "run_transition: %s is neither Monday nor 1st — no transition due",
                as_of_date,
            )
            return results

        # Acquire advisory lock to prevent concurrent zone transitions
        locked = self._try_acquire_lock(db)
        if not locked:
            raise RuntimeError(
                "Zone transition advisory lock is held by another process. "
                "Aborting to prevent concurrent structural mutations."
            )

        try:
            if _first and _monday:
                # Combined: monthly first, then weekly (ADR-006 B4)
                logger.info(
                    "run_transition: combined transition on %s for series=%s",
                    as_of_date, series_id,
                )
                results["monthly_to_weekly"] = self._run_monthly_to_weekly(
                    series_id, scenario_id, as_of_date, db, store
                )
                results["weekly_to_daily"] = self._run_weekly_to_daily(
                    series_id, scenario_id, as_of_date, db, store
                )
            elif _monday:
                logger.info(
                    "run_transition: weekly_to_daily on %s for series=%s",
                    as_of_date, series_id,
                )
                results["weekly_to_daily"] = self._run_weekly_to_daily(
                    series_id, scenario_id, as_of_date, db, store
                )
            elif _first:
                logger.info(
                    "run_transition: monthly_to_weekly on %s for series=%s",
                    as_of_date, series_id,
                )
                results["monthly_to_weekly"] = self._run_monthly_to_weekly(
                    series_id, scenario_id, as_of_date, db, store
                )
        finally:
            self._release_lock(db)

        return results

    # ------------------------------------------------------------------
    # Transition implementations
    # ------------------------------------------------------------------

    def _run_weekly_to_daily(
        self,
        series_id: UUID,
        scenario_id: UUID,
        as_of_date: date,
        db: psycopg.Connection,
        store: GraphStore,
    ) -> bool:
        """
        Weekly → daily transition.

        Runs every Monday. The week that is now entering the daily zone
        (was weekly last week) is split into 7 daily PI buckets.

        Returns True if the transition was executed, False if already done (idempotent skip).
        """
        job_type = "weekly_to_daily"
        idempotency_key = f"{job_type}:{series_id}:{as_of_date.isoformat()}"

        if self._is_transition_done(idempotency_key, db):
            logger.info(
                "_run_weekly_to_daily: already completed for series=%s date=%s — skipping",
                series_id, as_of_date,
            )
            return False

        run_id = self._start_transition_run(job_type, as_of_date, idempotency_key, db)

        try:
            # Find weekly nodes that are now entering the daily zone
            # (weekly bucket whose time_span_start is within the new daily zone horizon)
            nodes = store.get_nodes_by_series(series_id)
            weekly_nodes = [
                n for n in nodes
                if n.time_grain == "week" and n.active
                and n.time_span_start is not None
            ]

            if not weekly_nodes:
                logger.debug(
                    "_run_weekly_to_daily: no weekly nodes found for series=%s", series_id
                )
                self._complete_transition_run(run_id, 0, 0, db)
                return True

            # Find the first weekly bucket — the one entering the daily zone
            target_bucket = min(weekly_nodes, key=lambda n: n.time_span_start)

            logger.debug(
                "_run_weekly_to_daily: splitting week %s–%s into daily buckets for series=%s",
                target_bucket.time_span_start, target_bucket.time_span_end, series_id,
            )

            new_nodes = self._split_weekly_to_daily(
                source_node=target_bucket,
                scenario_id=scenario_id,
                series_id=series_id,
                db=db,
                store=store,
            )

            self._complete_transition_run(run_id, 1, len(new_nodes), db)
            logger.info(
                "_run_weekly_to_daily: done — split 1 weekly → %d daily for series=%s",
                len(new_nodes), series_id,
            )
            return True

        except Exception:
            self._fail_transition_run(run_id, db)
            raise

    def _run_monthly_to_weekly(
        self,
        series_id: UUID,
        scenario_id: UUID,
        as_of_date: date,
        db: psycopg.Connection,
        store: GraphStore,
    ) -> bool:
        """
        Monthly → weekly transition.

        Runs on the 1st of each month. The month entering the weekly zone
        is split into ~4–5 weekly PI buckets.

        Returns True if the transition was executed, False if already done.
        """
        job_type = "monthly_to_weekly"
        idempotency_key = f"{job_type}:{series_id}:{as_of_date.isoformat()}"

        if self._is_transition_done(idempotency_key, db):
            logger.info(
                "_run_monthly_to_weekly: already completed for series=%s date=%s — skipping",
                series_id, as_of_date,
            )
            return False

        run_id = self._start_transition_run(job_type, as_of_date, idempotency_key, db)

        try:
            nodes = store.get_nodes_by_series(series_id)
            monthly_nodes = [
                n for n in nodes
                if n.time_grain == "month" and n.active
                and n.time_span_start is not None
            ]

            if not monthly_nodes:
                logger.debug(
                    "_run_monthly_to_weekly: no monthly nodes found for series=%s", series_id
                )
                self._complete_transition_run(run_id, 0, 0, db)
                return True

            # First monthly bucket — the one entering the weekly zone
            target_bucket = min(monthly_nodes, key=lambda n: n.time_span_start)

            logger.debug(
                "_run_monthly_to_weekly: splitting month %s–%s into weekly buckets for series=%s",
                target_bucket.time_span_start, target_bucket.time_span_end, series_id,
            )

            new_nodes = self._split_monthly_to_weekly(
                source_node=target_bucket,
                scenario_id=scenario_id,
                series_id=series_id,
                db=db,
                store=store,
            )

            self._complete_transition_run(run_id, 1, len(new_nodes), db)
            logger.info(
                "_run_monthly_to_weekly: done — split 1 monthly → %d weekly for series=%s",
                len(new_nodes), series_id,
            )
            return True

        except Exception:
            self._fail_transition_run(run_id, db)
            raise

    # ------------------------------------------------------------------
    # Structural mutation helpers
    # ------------------------------------------------------------------

    def _split_weekly_to_daily(
        self,
        source_node: Node,
        scenario_id: UUID,
        series_id: UUID,
        db: psycopg.Connection,
        store: GraphStore,
    ) -> list[Node]:
        """
        Archive the source weekly bucket and create 7 daily buckets.

        Each new daily bucket:
          - Is marked is_dirty=True (must be recomputed by propagation engine)
          - Carries no pre-computed stock values (set to 0, will be recomputed)
          - Gets a bucket_sequence starting from source_node.bucket_sequence

        The source node is soft-deleted (active=False).
        Returns the list of newly created Node objects.
        """
        span_start = source_node.time_span_start
        span_end = source_node.time_span_end

        new_nodes: list[Node] = []
        base_sequence = source_node.bucket_sequence or 0

        # Enumerate 7 days in the week span
        day_spans: list[tuple[date, date]] = []
        current = span_start
        while current < span_end:
            day_end = current + timedelta(days=1)
            if day_end > span_end:
                day_end = span_end
            day_spans.append((current, day_end))
            current = day_end

        for idx, (day_start, day_end) in enumerate(day_spans):
            new_node = Node(
                node_id=uuid4(),
                node_type=source_node.node_type,
                scenario_id=scenario_id,
                item_id=source_node.item_id,
                location_id=source_node.location_id,
                time_grain="day",
                time_ref=day_start,
                time_span_start=day_start,
                time_span_end=day_end,
                projection_series_id=series_id,
                bucket_sequence=base_sequence + idx,
                opening_stock=Decimal("0"),
                inflows=Decimal("0"),
                outflows=Decimal("0"),
                closing_stock=Decimal("0"),
                has_shortage=False,
                shortage_qty=Decimal("0"),
                is_dirty=True,  # must be recomputed
                active=True,
            )
            store.upsert_node(new_node)
            new_nodes.append(new_node)

        # Archive the source weekly bucket
        source_node.active = False
        store.upsert_node(source_node)

        # Rewire: move inbound edges from old source node to the first new daily node,
        # and outbound edges to the last new daily node.
        # feeds_forward edges chain the new daily buckets together internally.
        if new_nodes:
            _rewire_edges(source_node, new_nodes, scenario_id, db, store)

        logger.debug(
            "_split_weekly_to_daily: archived node=%s, created %d daily nodes",
            source_node.node_id, len(new_nodes),
        )
        return new_nodes

    def _split_monthly_to_weekly(
        self,
        source_node: Node,
        scenario_id: UUID,
        series_id: UUID,
        db: psycopg.Connection,
        store: GraphStore,
    ) -> list[Node]:
        """
        Archive the source monthly bucket and create weekly buckets.

        Weekly buckets align to Monday boundaries (ISO week start).
        The first weekly bucket may start at span_start even if it's not a Monday.
        The last weekly bucket end is clamped to span_end.

        Returns the list of newly created Node objects.
        """
        span_start = source_node.time_span_start
        span_end = source_node.time_span_end
        base_sequence = source_node.bucket_sequence or 0

        # Enumerate ISO-week buckets within [span_start, span_end)
        week_spans: list[tuple[date, date]] = []
        current = span_start
        while current < span_end:
            # Align to Monday of current week if we're not already on one
            week_start = current - timedelta(days=current.weekday())
            if week_start < current:
                week_start = current  # first bucket may start mid-week
            week_end = week_start + timedelta(weeks=1)
            if week_end > span_end:
                week_end = span_end
            week_spans.append((week_start, week_end))
            # Advance to next Monday
            next_monday = (current - timedelta(days=current.weekday())) + timedelta(weeks=1)
            current = next_monday

        new_nodes: list[Node] = []
        for idx, (wk_start, wk_end) in enumerate(week_spans):
            new_node = Node(
                node_id=uuid4(),
                node_type=source_node.node_type,
                scenario_id=scenario_id,
                item_id=source_node.item_id,
                location_id=source_node.location_id,
                time_grain="week",
                time_ref=wk_start,
                time_span_start=wk_start,
                time_span_end=wk_end,
                projection_series_id=series_id,
                bucket_sequence=base_sequence + idx,
                opening_stock=Decimal("0"),
                inflows=Decimal("0"),
                outflows=Decimal("0"),
                closing_stock=Decimal("0"),
                has_shortage=False,
                shortage_qty=Decimal("0"),
                is_dirty=True,
                active=True,
            )
            store.upsert_node(new_node)
            new_nodes.append(new_node)

        # Archive the source monthly bucket
        source_node.active = False
        store.upsert_node(source_node)

        # Rewire: move inbound edges to first new node, outbound to last new node.
        if new_nodes:
            _rewire_edges(source_node, new_nodes, scenario_id, db, store)

        logger.debug(
            "_split_monthly_to_weekly: archived node=%s, created %d weekly nodes",
            source_node.node_id, len(new_nodes),
        )
        return new_nodes

    # ------------------------------------------------------------------
    # Idempotency helpers (zone_transition_runs table)
    # ------------------------------------------------------------------

    def _is_transition_done(
        self,
        idempotency_key: str,
        db: psycopg.Connection,
    ) -> bool:
        """Return True if a completed transition run exists for this idempotency_key."""
        row = db.execute(
            """
            SELECT status FROM zone_transition_runs
            WHERE idempotency_key = %s
            """,
            (idempotency_key,),
        ).fetchone()
        if row is None:
            return False
        return row["status"] == "completed"

    def _start_transition_run(
        self,
        job_type: str,
        transition_date: date,
        idempotency_key: str,
        db: psycopg.Connection,
    ) -> UUID:
        """
        Insert a zone_transition_runs record in 'running' status.

        Returns the new run ID.
        """
        run_id = uuid4()
        db.execute(
            """
            INSERT INTO zone_transition_runs (
                id, job_type, transition_date, idempotency_key,
                status, series_total, series_done, started_at
            ) VALUES (
                %s, %s, %s, %s,
                'running', NULL, 0, now()
            )
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            (run_id, job_type, transition_date, idempotency_key),
        )
        # Re-fetch the actual run_id in case ON CONFLICT skipped our insert
        row = db.execute(
            "SELECT id FROM zone_transition_runs WHERE idempotency_key = %s",
            (idempotency_key,),
        ).fetchone()
        return UUID(str(row["id"])) if row else run_id

    def _complete_transition_run(
        self,
        run_id: UUID,
        series_total: int,
        series_done: int,
        db: psycopg.Connection,
    ) -> None:
        """Mark a zone_transition_runs record as completed."""
        db.execute(
            """
            UPDATE zone_transition_runs
            SET status = 'completed',
                series_total = %s,
                series_done = %s,
                completed_at = now()
            WHERE id = %s
            """,
            (series_total, series_done, run_id),
        )

    def _fail_transition_run(
        self,
        run_id: UUID,
        db: psycopg.Connection,
    ) -> None:
        """Mark a zone_transition_runs record as failed."""
        db.execute(
            """
            UPDATE zone_transition_runs
            SET status = 'failed',
                completed_at = now()
            WHERE id = %s
            """,
            (run_id,),
        )

    # ------------------------------------------------------------------
    # Advisory lock helpers
    # ------------------------------------------------------------------

    def _try_acquire_lock(self, db: psycopg.Connection) -> bool:
        """
        Try to acquire the zone_transition global advisory lock.
        Returns True if acquired, False if already held.
        """
        row = db.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s)) AS locked",
            (_ZONE_TRANSITION_LOCK_KEY,),
        ).fetchone()
        return bool(row["locked"]) if row else False

    def _release_lock(self, db: psycopg.Connection) -> None:
        """Release the zone_transition global advisory lock."""
        db.execute(
            "SELECT pg_advisory_unlock(hashtext(%s))",
            (_ZONE_TRANSITION_LOCK_KEY,),
        )


# ---------------------------------------------------------------------------
# Edge rewiring helper (module-level, not a method)
# ---------------------------------------------------------------------------


def _rewire_edges(
    source_node: "Node",
    new_nodes: list["Node"],
    scenario_id: UUID,
    db: psycopg.Connection,
    store: "GraphStore",
) -> None:
    """
    After a source PI node is split into new_nodes, rewire all active edges:

    - Inbound edges to source_node  → redirected to new_nodes[0]  (the first bucket)
    - Outbound edges from source_node → redirected to new_nodes[-1] (the last bucket)
    - feeds_forward edges are handled separately: they connect the new buckets in a
      chain internally, and the terminal inbound/outbound ones are rewired above.

    The old edges on source_node are deactivated (active = FALSE).
    """
    first_new = new_nodes[0]
    last_new = new_nodes[-1]

    # Deactivate + redirect inbound edges (to_node_id == source_node)
    inbound = store.get_edges_to(source_node.node_id, scenario_id)
    for edge in inbound:
        # Deactivate old edge
        db.execute(
            "UPDATE edges SET active = FALSE WHERE edge_id = %s",
            (edge.edge_id,),
        )
        # Create new edge pointing at first new bucket
        db.execute(
            """
            INSERT INTO edges (
                edge_id, edge_type, from_node_id, to_node_id, scenario_id,
                priority, weight_ratio, effective_start, effective_end,
                active, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, now())
            """,
            (
                uuid4(),
                edge.edge_type,
                edge.from_node_id,
                first_new.node_id,
                scenario_id,
                edge.priority,
                edge.weight_ratio,
                edge.effective_start,
                edge.effective_end,
            ),
        )

    # Deactivate + redirect outbound edges (from_node_id == source_node)
    outbound = store.get_edges_from(source_node.node_id, scenario_id)
    for edge in outbound:
        # Deactivate old edge
        db.execute(
            "UPDATE edges SET active = FALSE WHERE edge_id = %s",
            (edge.edge_id,),
        )
        # Create new edge originating from last new bucket
        db.execute(
            """
            INSERT INTO edges (
                edge_id, edge_type, from_node_id, to_node_id, scenario_id,
                priority, weight_ratio, effective_start, effective_end,
                active, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, now())
            """,
            (
                uuid4(),
                edge.edge_type,
                last_new.node_id,
                edge.to_node_id,
                scenario_id,
                edge.priority,
                edge.weight_ratio,
                edge.effective_start,
                edge.effective_end,
            ),
        )

    logger.debug(
        "_rewire_edges: source=%s rewired %d inbound + %d outbound edges → %d new nodes",
        source_node.node_id,
        len(inbound),
        len(outbound),
        len(new_nodes),
    )
