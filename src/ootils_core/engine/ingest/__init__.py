"""
Governed daily-run pipeline (ADR-042 decisions 3/PR-3/PR-4b, absorbs
ADR-037's INT-1 PR3, migrations 078/079).

``apply.py`` combines the per-feed guard verdicts persisted by PR-2
(``interfaces.daily_run``/``interfaces.guards``, ``daily_runs`` table,
migration 078) with each feed's DQ status into ONE governed decision per
run_date (ADR-037 §0 option (a)): auto-approve iff every feed is green,
escalate to a human via the L3 webhook when a ``blocking`` feed is red,
degrade confidence without blocking when only an ``advisory`` feed is red.
See ``apply.py``'s module docstring for the full scope boundary (in
particular: what this PR does NOT yet do — extract the canonical
multi-entity upsert/writer from ``api/routers/ingest.py``, that stays PR-1
of ADR-042's plan; and how a feed's DQ status is supplied, since no
DB wiring from a ``daily_runs`` row to an ``ingest_batches`` row exists
yet).

``daily_orchestrator.py`` (PR-4b) is the conductor that actually RUNS a
daily cycle: scans an inbox for today's dated TSV drops, resolves each
feed_key's active contract, feeds PR-2's guards + PR-3's decision, and —
gated all-or-nothing on the decision — loads every feed whose OWN guard
verdict is green via ``interfaces.ingest_exec``'s canonical primitives. See
its module docstring for the full scope (the feed_key/entity_type mismatch,
BOM being out of scope in V1, no automatic recompute, baseline-only).
"""
from ootils_core.engine.ingest.apply import (
    DailyRunDecision,
    DailyRunDecisionError,
    DailyRunDecisionPlan,
    FeedDecisionInput,
    FeedDecisionResult,
    RunDecisionStatus,
    decide_daily_run,
    plan_daily_run_decision,
    record_daily_run_decision,
)
from ootils_core.engine.ingest.daily_orchestrator import (
    LOAD_ORDER,
    DailyRunEvaluation,
    FeedLoadOutcome,
    FeedLoadStatus,
    FeedRunEvaluation,
    InboxScan,
    ScanIssue,
    ScannedFeedFile,
    apply_daily_run,
    load_eligible_feeds,
    plan_daily_run,
    scan_inbox,
)

__all__ = [
    "DailyRunDecision",
    "DailyRunDecisionError",
    "DailyRunDecisionPlan",
    "FeedDecisionInput",
    "FeedDecisionResult",
    "RunDecisionStatus",
    "decide_daily_run",
    "plan_daily_run_decision",
    "record_daily_run_decision",
    "LOAD_ORDER",
    "DailyRunEvaluation",
    "FeedLoadOutcome",
    "FeedLoadStatus",
    "FeedRunEvaluation",
    "InboxScan",
    "ScanIssue",
    "ScannedFeedFile",
    "apply_daily_run",
    "load_eligible_feeds",
    "plan_daily_run",
    "scan_inbox",
]
