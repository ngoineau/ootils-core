"""
External-interface contracts (INT-1/ADR-037, daily-run guards/ADR-042).

``contracts.py`` is the Python half of the feed-contract registry: strict
pydantic parsing of ``config/feed-contracts/*.yaml``, a versioned/idempotent
loader into the ``feed_contracts`` table (migration 073), the
``get_active_contract`` single-feed reader, and ``list_active_contracts``
(the full active set — what a daily run cross-references its inbox scan
against).

``guards.py`` is the pure (DB-free) runtime-guard evaluator ADR-037 §6
describes (arrival window, volume floor/delta, deletion ratio) — see its
module docstring. ``daily_run.py`` is the DB-touching persistence layer
(``daily_runs`` table, migration 078) that gathers a feed's active contract
+ previous run stats and records one guard-evaluation attempt. The governed
auto-approve/escalate DECISION that consumes these verdicts lives in
``engine/ingest/apply.py`` (ADR-042 PR-3). ``ingest_exec.py`` is the
canonical TSV-ingest execution primitives (filename grammar, payload
builders, in-process API call, archiving) shared by
``scripts/ingest_file.py`` (manual/dev CLI) and
``engine/ingest/daily_orchestrator.py`` (the governed daily run, ADR-042
PR-4b) — see its module docstring for why it is not simply imported from
the script.
"""
from ootils_core.interfaces.contracts import (
    ContractError,
    FeedContract,
    FeedContractSpec,
    LoadOutcome,
    get_active_contract,
    list_active_contracts,
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
    "list_active_contracts",
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
