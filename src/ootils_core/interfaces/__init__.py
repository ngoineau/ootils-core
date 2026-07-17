"""
External-interface contracts (INT-1/ADR-037, daily-run guards/ADR-042 PR-2).

``contracts.py`` is the Python half of the feed-contract registry: strict
pydantic parsing of ``config/feed-contracts/*.yaml``, a versioned/idempotent
loader into the ``feed_contracts`` table (migration 073), and the
``get_active_contract`` reader.

``guards.py`` is the pure (DB-free) runtime-guard evaluator ADR-037 §6
describes (arrival window, volume floor/delta, deletion ratio) — see its
module docstring. ``daily_run.py`` is the DB-touching persistence layer
(``daily_runs`` table, migration 078) that gathers a feed's active contract
+ previous run stats and records one guard-evaluation attempt. The governed
auto-approve/escalate DECISION that consumes these verdicts is PR-3
territory (``engine/ingest/apply.py``, not yet written).
"""
from ootils_core.interfaces.contracts import (
    ContractError,
    FeedContract,
    FeedContractSpec,
    LoadOutcome,
    get_active_contract,
    load_contract_dir,
    parse_contract_file,
    upsert_contract,
)
from ootils_core.interfaces.daily_run import (
    DailyRunGuardError,
    DailyRunGuardPlan,
    DailyRunObservation,
    DailyRunRecord,
    plan_daily_run_guard_check,
    record_daily_run,
)
from ootils_core.interfaces.guards import (
    DELETION_RATIO_THRESHOLD,
    FeedGuardEvaluation,
    GuardResult,
    GuardStatus,
    compute_expected_arrival_deadline,
    evaluate_arrival_window_guard,
    evaluate_deletion_ratio_guard,
    evaluate_feed_guards,
    evaluate_volume_delta_guard,
    evaluate_volume_floor_guard,
)

__all__ = [
    "ContractError",
    "FeedContract",
    "FeedContractSpec",
    "LoadOutcome",
    "get_active_contract",
    "load_contract_dir",
    "parse_contract_file",
    "upsert_contract",
    "DailyRunGuardError",
    "DailyRunGuardPlan",
    "DailyRunObservation",
    "DailyRunRecord",
    "plan_daily_run_guard_check",
    "record_daily_run",
    "DELETION_RATIO_THRESHOLD",
    "FeedGuardEvaluation",
    "GuardResult",
    "GuardStatus",
    "compute_expected_arrival_deadline",
    "evaluate_arrival_window_guard",
    "evaluate_deletion_ratio_guard",
    "evaluate_feed_guards",
    "evaluate_volume_delta_guard",
    "evaluate_volume_floor_guard",
]
