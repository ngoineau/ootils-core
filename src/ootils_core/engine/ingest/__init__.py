"""
Governed daily-run decision engine (ADR-042 PR-3, absorbs ADR-037's INT-1
PR3, migration 079).

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
]
