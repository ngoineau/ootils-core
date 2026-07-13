"""
replica_role.py — shared FK-trigger derogation helper (ADR-040).

Encadres disabling and re-enabling Postgres row-level trigger firing
(`session_replication_role`) around a bulk INSERT/DELETE whose target rows
are ALREADY known-valid by construction — i.e. the caller's own invariant
guarantees the write is FK-safe, so Postgres's per-row trigger-driven
re-validation is pure, measured overhead, not a correctness necessity.

Two call sites share this module today:
  - `engine/scenario/manager.py`'s `ScenarioManager._copy_nodes` — the
    ORIGINAL site (#460): the two bulk `INSERT…SELECT` statements that copy
    `nodes`/`edges` into a freshly-forked scenario.
  - `engine/maintenance/purge.py`'s `_delete_whitelist_for_scenario` — the
    SECOND site (ADR-040's 2026-07-12 extension): the `PURGE_WHITELIST`
    DELETE loop, whose FK-safe table ordering (module docstring of
    `purge.py`) makes row-by-row RI re-validation on each DELETE redundant
    by the same argument.

Both callers wrap the derogation in a SAVEPOINT — a permission-denied `SET`
aborts the enclosing Postgres transaction — and fall back transparently to
the ordinary triggers-on path on `psycopg.errors.InsufficientPrivilege`; any
OTHER exception on the `SET` propagates unchanged (no blanket `except`).
Neither `enable_replica_role` nor `restore_origin_role` commits or rolls
back the enclosing transaction — the caller owns it, and each caller is
responsible for its OWN compensatory set-based integrity check after the
derogated write (see `ScenarioManager._copy_nodes`'s node-FK/orphan-edge
checks and `purge.py`'s `_verify_whitelist_emptied`) — this module only
manages the trigger-firing window, never verifies what happened inside it.
"""
from __future__ import annotations

import logging

import psycopg

from ootils_core.db.types import DictRowConnection

logger = logging.getLogger(__name__)

# Default savepoint name — the original (and still most common) caller,
# ScenarioManager._copy_nodes, keeps this exact name so its statement text
# (and the existing unit tests pinning that text) is unchanged by this
# refactor. Other callers (e.g. purge.py) pass their own name so two
# concurrent derogation windows on the same connection never collide.
DEFAULT_SAVEPOINT_NAME = "scenario_fork_replica_role"


def enable_replica_role(
    db: DictRowConnection,
    *,
    savepoint_name: str = DEFAULT_SAVEPOINT_NAME,
    log_event: str = "scenario.fork_fast_path_denied",
    fallback_description: str = "triggers-on copy path for this fork",
) -> bool:
    """
    Attempt to disable trigger firing (`session_replication_role =
    'replica'`, SET LOCAL) for the duration of the caller's derogated bulk
    write. See ADR-040 and the derogation comment at each call site for why
    this is safe there.

    Requires the connection's role to hold SET privilege on the
    `session_replication_role` GUC (PG15+: `GRANT SET ON PARAMETER
    session_replication_role TO <role>`; earlier versions restrict it to
    superuser). A permission-denied SET aborts the enclosing Postgres
    transaction, so the attempt is wrapped in a SAVEPOINT: on failure we
    roll back to the savepoint (undoing only the failed SET — nothing else
    in the caller's transaction is touched), release it (both branches
    leave no savepoint behind), log ONE warning, and the caller falls
    through to the ordinary triggers-on path. Only `InsufficientPrivilege`
    is treated as the expected "no grant" case; any other error propagates
    (fail-loudly — this is not a blanket except).

    `savepoint_name` MUST be unique among any derogation windows nested or
    sequenced on the SAME connection/transaction — every call site in this
    repo uses its own name (see `DEFAULT_SAVEPOINT_NAME`'s docstring note).
    `log_event`/`fallback_description` let each call site's warning read
    naturally (e.g. "for this fork" vs "for this purge") while sharing one
    implementation and one log line shape.

    Returns True if replica mode is now active for this transaction (the
    caller MUST call `restore_origin_role` before any further work), False
    if the fallback (triggers-on) path must be used.
    """
    # savepoint_name is always a hardcoded Python constant supplied by a
    # call site in this repo, never caller/request data — SAVEPOINT/
    # ROLLBACK TO/RELEASE names cannot be bound as ordinary SQL parameters
    # (%s), so this mirrors the same "whitelisted identifier, not user
    # input" precedent as purge.py's _build_table_queries.
    db.execute(f"SAVEPOINT {savepoint_name}")  # noqa: S608
    try:
        db.execute("SET LOCAL session_replication_role = 'replica'")
    except psycopg.errors.InsufficientPrivilege:
        db.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")  # noqa: S608
        db.execute(f"RELEASE SAVEPOINT {savepoint_name}")  # noqa: S608
        logger.warning(
            "%s role lacks SET privilege on session_replication_role "
            "(PG15+: GRANT SET ON PARAMETER session_replication_role TO "
            "<role>) — falling back to the %s",
            log_event,
            fallback_description,
        )
        return False
    else:
        db.execute(f"RELEASE SAVEPOINT {savepoint_name}")  # noqa: S608
        return True


def restore_origin_role(db: DictRowConnection) -> None:
    """
    Re-enable trigger firing before any further work happens on the
    connection. `SET LOCAL session_replication_role = 'replica'` already
    reverts automatically at COMMIT/ROLLBACK, but callers reset explicitly
    right after their derogated write so the window during which triggers
    are off is as narrow as possible — see ADR-040.
    """
    db.execute("SET LOCAL session_replication_role = 'origin'")
