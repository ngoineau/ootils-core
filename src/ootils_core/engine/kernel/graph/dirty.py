"""
dirty.py — Two-tier dirty tracking for incremental propagation.

Tier 1: in-memory Python set — fast, lost on crash.
Tier 2: Postgres dirty_nodes table — durable, used for crash recovery.

Callers own commit/rollback on the db connection.
"""
from __future__ import annotations

from uuid import UUID

from ootils_core.engine.kernel._clock import Clock, SystemClock


class DirtyFlagManager:
    """
    Two-tier dirty tracking: in-memory Python set (fast) + Postgres dirty_nodes (durable).

    Key: (scenario_id, calc_run_id) → set of node_ids

    Optional ``clock`` (ADR-003): pass a ``FrozenClock`` from tests so
    ``marked_at`` values on dirty_nodes are reproducible.
    """

    def __init__(self, clock: Clock | None = None) -> None:
        self._dirty: dict[tuple[UUID, UUID], set[UUID]] = {}
        self._clock = clock or SystemClock()

    def _key(self, scenario_id: UUID, calc_run_id: UUID) -> tuple[UUID, UUID]:
        return (scenario_id, calc_run_id)

    # ------------------------------------------------------------------
    # In-memory operations
    # ------------------------------------------------------------------

    def mark_dirty(
        self,
        node_ids: set[UUID],
        scenario_id: UUID,
        calc_run_id: UUID,
        db,
    ) -> None:
        """Mark node_ids as dirty in memory. Does NOT write to Postgres."""
        key = self._key(scenario_id, calc_run_id)
        if key not in self._dirty:
            self._dirty[key] = set()
        self._dirty[key].update(node_ids)

    def clear_dirty(
        self,
        node_id: UUID,
        scenario_id: UUID,
        calc_run_id: UUID,
        db,
    ) -> None:
        """Remove a single node from the in-memory dirty set and delete from Postgres."""
        key = self._key(scenario_id, calc_run_id)
        if key in self._dirty:
            self._dirty[key].discard(node_id)

        db.execute(
            """
            DELETE FROM dirty_nodes
            WHERE calc_run_id = %s AND node_id = %s AND scenario_id = %s
            """,
            (calc_run_id, node_id, scenario_id),
        )

    def clear_dirty_batch(
        self,
        node_ids: list[UUID],
        scenario_id: UUID,
        calc_run_id: UUID,
        db,
    ) -> None:
        """Clear many dirty flags in one DELETE.

        Used by the propagator's batch write path — replaces N
        per-node DELETEs with a single round-trip
        (REVIEW-2026-05 R2 Tier 2 follow-up).
        """
        if not node_ids:
            return
        key = self._key(scenario_id, calc_run_id)
        if key in self._dirty:
            self._dirty[key].difference_update(node_ids)
        db.execute(
            """
            DELETE FROM dirty_nodes
            WHERE calc_run_id = %s AND scenario_id = %s AND node_id = ANY(%s)
            """,
            (calc_run_id, scenario_id, list(node_ids)),
        )

    def get_dirty_nodes(
        self,
        calc_run_id: UUID,
        scenario_id: UUID,
        db,
    ) -> set[UUID]:
        """
        Return the current dirty set from memory.
        Falls back to Postgres if no in-memory state (e.g., after crash recovery).
        """
        key = self._key(scenario_id, calc_run_id)
        if key in self._dirty:
            return set(self._dirty[key])
        # Fall back to Postgres
        self.load_from_postgres(calc_run_id, scenario_id, db)
        return set(self._dirty.get(key, set()))

    def is_dirty(
        self,
        node_id: UUID,
        scenario_id: UUID,
        calc_run_id: UUID,
    ) -> bool:
        """Check if a node is dirty in memory (no DB query)."""
        key = self._key(scenario_id, calc_run_id)
        return node_id in self._dirty.get(key, set())

    # ------------------------------------------------------------------
    # Postgres persistence
    # ------------------------------------------------------------------

    def flush_to_postgres(
        self,
        calc_run_id: UUID,
        scenario_id: UUID,
        db,
    ) -> None:
        """
        Batch INSERT all in-memory dirty nodes into dirty_nodes table.
        Uses ON CONFLICT DO NOTHING for idempotency.
        """
        key = self._key(scenario_id, calc_run_id)
        node_ids = self._dirty.get(key, set())
        if not node_ids:
            return

        now = self._clock.now()
        # Build batch values
        rows = [(calc_run_id, node_id, scenario_id, now) for node_id in node_ids]

        sql = """
            INSERT INTO dirty_nodes (calc_run_id, node_id, scenario_id, marked_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (calc_run_id, node_id, scenario_id) DO NOTHING
        """

        executemany = getattr(db, "executemany", None)
        if callable(executemany):
            executemany(sql, rows)
        else:
            # psycopg3 connections do not expose executemany(), so fall back to a cursor.
            with db.cursor() as cur:
                cur.executemany(sql, rows)

        # Real constraint (2026-07 VM re-bench): this bulk INSERT is read
        # right back by PROPAGATE_SQL/SHORTAGES_SQL (and the Rust writeback
        # path once its session opens post-commit). Without fresh stats on
        # dirty_nodes, the planner estimates rows=1 on the just-inserted
        # batch, picks a Nested Loop Left Join, and re-runs the
        # inflows/outflows GroupAggregate PER ROW instead of once --
        # measured 200x slowdown (43 nodes/s vs the expected throughput) on
        # a 2000-row dirty set. ANALYZE takes a ShareUpdateExclusive lock
        # that self-conflicts across concurrent scenario calc runs (their
        # ANALYZEs briefly serialize) but never blocks INSERT/SELECT/DELETE
        # on this table -- an accepted, bounded cost against an O(N^2) plan.
        db.execute("ANALYZE dirty_nodes")

    def load_from_postgres(
        self,
        calc_run_id: UUID,
        scenario_id: UUID,
        db,
    ) -> None:
        """
        Load dirty nodes from Postgres into memory.
        Used for crash recovery: re-populate in-memory state from durable store.
        """
        rows = db.execute(
            """
            SELECT node_id FROM dirty_nodes
            WHERE calc_run_id = %s AND scenario_id = %s
            """,
            (calc_run_id, scenario_id),
        ).fetchall()

        key = self._key(scenario_id, calc_run_id)
        self._dirty[key] = {UUID(str(r["node_id"])) for r in rows}
