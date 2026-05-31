"""
agent_governance.py — Governed-run context manager for the watcher fleet.

Every watcher agent must: open a work-ledger row in agent_runs with status
RUNNING before doing any work, supersede its prior active artifact rows,
insert the new artifact set, then close the run as COMPLETED — or FAILED on
any unhandled exception. Without this bookkeeping a crashed watcher leaves a
RUNNING orphan that is invisible to operators and breaks the idempotency
contract.

``governed_run`` centralises that lifecycle so individual watchers stay thin.

Usage::

    with governed_run(conn, "shortage_watcher", scenario_id) as run:
        run_id = run.run_id          # available immediately after __enter__
        superseded = run.supersede("recommendations", "DRAFT", "EXPIRED")
        run.insert(
            "recommendations",
            ["agent_name", "agent_run_id", ...],
            rows,
        )
        run.set_metrics({"recommendations": len(rows), ...})
    # __exit__ writes COMPLETED + elapsed_s + calls conn.commit().
    # On exception: writes FAILED (best-effort) + conn.commit() + re-raises.

SQL safety: table names go through psycopg.sql.Identifier. Column lists are
composed with psycopg.sql.SQL.join. No f-strings in SQL paths.
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Generator, Sequence

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)


class _Run:
    """Handle exposed to the ``with`` block.

    Callers use ``run.run_id`` (UUID) to build artifact tuples and call
    ``run.supersede`` / ``run.insert`` / ``run.set_metrics`` to perform the
    governed database operations.
    """

    def __init__(self, conn: psycopg.Connection, run_id: object, t0: float) -> None:
        self._conn = conn
        self._run_id = run_id
        self._t0 = t0
        self._metrics: dict[str, Any] = {}
        self._agent_name: str = ""
        self._scenario_id: object = None

    @property
    def run_id(self) -> object:
        """UUID of the agent_runs row opened by __enter__."""
        return self._run_id

    def supersede(self, table: str, active_status: str, new_status: str) -> int:
        """UPDATE rows in *table* for this agent/scenario from active_status to new_status.

        Returns the rowcount (number of rows superseded). The agent_name and
        scenario_id are taken from the agent_runs row opened by __enter__.
        Table name is quoted via sql.Identifier — no dynamic SQL injection risk.
        """
        query = sql.SQL(
            "UPDATE {tbl} SET status=%s, updated_at=now() "
            "WHERE agent_name=%s AND scenario_id=%s AND status=%s"
        ).format(tbl=sql.Identifier(table))
        cur = self._conn.cursor()
        cur.execute(query, (new_status, self._agent_name, self._scenario_id, active_status))
        return cur.rowcount

    def insert(self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
        """executemany INSERT into *table* with the given column list and row tuples.

        Table name and column identifiers are quoted via sql.Identifier /
        sql.SQL.join — no string interpolation in the SQL path.
        """
        if not rows:
            return
        col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
        query = sql.SQL("INSERT INTO {tbl} ({cols}) VALUES ({vals})").format(
            tbl=sql.Identifier(table),
            cols=col_ids,
            vals=placeholders,
        )
        self._conn.cursor().executemany(query, rows)

    def set_metrics(self, metrics: dict[str, Any]) -> None:
        """Store the metrics dict that will be written to agent_runs on exit."""
        self._metrics = metrics

    @property
    def metrics(self) -> dict[str, Any]:
        """The metrics dict set via set_metrics (read-back for summary logging)."""
        return self._metrics

    # Internal helpers called by the context manager.
    def _bind(self, agent_name: str, scenario_id: object) -> None:
        self._agent_name = agent_name
        self._scenario_id = scenario_id

    def _elapsed(self) -> float:
        return round(time.perf_counter() - self._t0, 2)

    def _close(self, status: str) -> None:
        metrics = dict(self._metrics)
        metrics["elapsed_s"] = self._elapsed()
        self._conn.cursor().execute(
            "UPDATE agent_runs SET status=%s, finished_at=now(), metrics=%s "
            "WHERE agent_run_id=%s",
            (status, Jsonb(metrics), self._run_id),
        )


@contextmanager
def governed_run(
    conn: psycopg.Connection,
    agent_name: str,
    scenario_id: object,
    t0: float | None = None,
) -> Generator[_Run, None, None]:
    """Context manager that governs an agent watcher run lifecycle.

    On entry: INSERTs a row into agent_runs with status='RUNNING', creates a
    _Run handle and yields it. The caller uses ``run.run_id`` immediately.

    On clean exit: writes status='COMPLETED', finished_at=now(), and the
    metrics dict (with elapsed_s appended), then calls conn.commit().

    On exception: writes status='FAILED' (best-effort, does not suppress
    secondary errors) with finished_at and metrics, calls conn.commit(), then
    re-raises the original exception.

    The caller owns the connection lifecycle (open before, close after).
    governed_run only drives commit/rollback of the transaction.

    Args:
        conn:        An open psycopg connection (autocommit=False).
        agent_name:  Value stored in agent_runs.agent_name.
        scenario_id: UUID (or string UUID) stored in agent_runs.scenario_id.
        t0:          perf_counter start time. If None a new timer is started
                     at entry, which excludes planning-data load time. Pass
                     the caller's t0 to include the full elapsed time.
    """
    _t0 = t0 if t0 is not None else time.perf_counter()
    cur = conn.cursor()
    run_id = cur.execute(
        "INSERT INTO agent_runs (agent_name, scenario_id, status) "
        "VALUES (%s, %s, 'RUNNING') RETURNING agent_run_id",
        (agent_name, scenario_id),
    ).fetchone()[0]
    logger.debug("agent=%s scenario=%s run_id=%s status=RUNNING", agent_name, scenario_id, run_id)

    run = _Run(conn, run_id, _t0)
    run._bind(agent_name, scenario_id)

    try:
        yield run
    except Exception:
        # Best-effort: write FAILED. If the UPDATE itself errors we still
        # commit what we can so the RUNNING row is not left hanging.
        try:
            run._close("FAILED")
            conn.commit()
        except Exception:
            logger.exception("agent=%s run_id=%s: error writing FAILED status", agent_name, run_id)
        logger.error("agent=%s run_id=%s status=FAILED", agent_name, run_id)
        raise
    else:
        run._close("COMPLETED")
        conn.commit()
        logger.debug("agent=%s run_id=%s status=COMPLETED elapsed_s=%.2f", agent_name, run_id, run._elapsed())
