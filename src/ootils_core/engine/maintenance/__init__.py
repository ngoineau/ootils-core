"""
Scenario-fork purge + shortage-retention lifecycle (PURGE-1, migration 076).

purge: ``plan_fork_purge``/``apply_fork_purge`` — TTL-driven deletion of an
    archived, non-baseline scenario's child data (never the ``scenarios`` row
    itself), through the FK-safe ``PURGE_WHITELIST``. ``plan_shortage_
    retention``/``apply_shortage_retention`` — a separate, narrower sweep over
    long-``resolved`` ``shortages`` rows, never touching ``status='active'``
    or a scenario's own latest completed calc_run. Every ``plan_*`` is
    SELECT-only; every ``apply_*`` is the sole writer and does NOT commit —
    the caller owns the transaction.
"""
from ootils_core.engine.maintenance.purge import (
    PURGE_EXEMPT_TABLES,
    PURGE_WHITELIST,
    PurgeCandidate,
    PurgeGuardError,
    PurgePlan,
    PurgeRunResult,
    ShortageRetentionCandidate,
    ShortageRetentionPlan,
    apply_fork_purge,
    apply_shortage_retention,
    plan_fork_purge,
    plan_shortage_retention,
)

__all__ = [
    "PURGE_EXEMPT_TABLES",
    "PURGE_WHITELIST",
    "PurgeCandidate",
    "PurgeGuardError",
    "PurgePlan",
    "PurgeRunResult",
    "ShortageRetentionCandidate",
    "ShortageRetentionPlan",
    "apply_fork_purge",
    "apply_shortage_retention",
    "plan_fork_purge",
    "plan_shortage_retention",
]
