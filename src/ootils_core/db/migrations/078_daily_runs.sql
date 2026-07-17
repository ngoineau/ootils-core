-- ============================================================
-- Migration 078 — daily_runs (guard-evaluation audit trail) + recommendations.exported_at
-- ============================================================
-- ADR-042 (docs/ADR-042-interface-doctrine.md) decision 3 step 10 / decision
-- 4, PR-2 of the pilot-decided delivery order ("la valeur d'abord"), which
-- absorbs ADR-037's INT-1 PR2 (docs/ADR-037-daily-run-and-governed-ingest.md
-- §5): "Table daily_runs (+ FK vers feed_contracts) ; évaluation runtime par
-- flux : fenêtre d'arrivée (cadence + arrival_window_minutes vs l'horodatage
-- réel d'upload), gardes de volume (volume_guard_min_rows/
-- volume_guard_max_pct_delta vs le run précédent), lues via
-- get_active_contract()."
--
-- SCOPE OF THIS PR: schema + the pure/persisted guard-evaluation audit
-- trail only (src/ootils_core/interfaces/guards.py + daily_run.py). NOT in
-- scope here (see those modules' docstrings for the full boundary):
--   * the governed auto-approve/escalate DECISION (ADR-037 §0 option (a))
--     that combines these guard verdicts with a batch's DQ status — PR-3's
--     engine/ingest/apply.py territory.
--   * a `daily_run_completed` events.event_type CHECK widening — ADR-042
--     ties that event to the RUN as a whole (every feed evaluated + the
--     governed decision taken), a granularity only PR-3's decision engine
--     can know it has reached. Adding the CHECK value here, unused, would
--     invite exactly the kind of un-exercised schema drift migration 073's
--     own header warns against for daily_runs' FK. Deferred to the PR that
--     actually emits it (same discipline as migration 076's purge_executed,
--     added in the SAME PR that started emitting it).
--   * computing row deltas from a real file diff — the TSV-vs-canonical
--     diffing service does not exist yet in this worktree; row_count/
--     deleted_count are recorded as OBSERVED inputs the caller supplies.
--
-- WHY daily_runs HAS NO scenario_id: a governed daily run evaluates ERP
-- feed interfaces (on-hand, POs, WOs, customer orders, ...) — it is not
-- scenario-scoped working state (unlike nodes/edges/shortages). It is
-- baseline-by-nature, same rationale ADR-030 already established for
-- inventory_snapshots/recommendation_outcomes: an observed ERP feed
-- evaluation is a fact, not a fork's simulated state.
-- tests/test_purge_whitelist_guard.py re-derives scenario-scoped tables by
-- scanning for a literal scenario_id column — daily_runs correctly does not
-- appear in that scan, exactly like feed_contracts before it.
--
-- APPEND-ONLY AUDIT TRAIL, NOT UPSERTED: no UNIQUE(feed_key, run_date). A
-- feed's guard verdict can legitimately be (re-)evaluated more than once on
-- the same run_date (e.g. reported missing at the arrival deadline, then
-- re-evaluated once the file lands later that day) — each attempt is its
-- own honest row, same philosophy as calc_runs/maintenance_purge_runs
-- (076). The "current" verdict for a (feed_key, run_date) is simply the
-- most recent row by observed_at — see interfaces/daily_run.py's
-- plan_daily_run_guard_check, which reads the latest PRIOR-DAY row for the
-- volume-delta baseline.
--
-- feed_contract_id FK: ON DELETE RESTRICT. feed_contracts rows are never
-- hard-deleted (append-only per version, migration 073) — RESTRICT is a
-- pure safety net, mirroring the FK-to-append-only-audit-table convention
-- used elsewhere (maintenance_purge_runs -> scenarios, migration 076).
--
-- Guard status vocabulary ('ok' | 'failed' | 'not_evaluated') on the four
-- per-guard columns mirrors interfaces/guards.py's GuardStatus enum exactly
-- (kept in lockstep — widen one, widen the other). 'not_evaluated' is the
-- None-honest state: the guard was not configured on the active contract,
-- or had no baseline to compare against — never a fabricated 'ok'.
-- overall_status is narrower ('ok' | 'failed' only): FeedGuardEvaluation.
-- overall_status never returns NOT_EVALUATED (see guards.py) — a run's
-- overall verdict is always a real ok/failed call.
--
-- exported_at on recommendations: bundled into this SAME migration per
-- ADR-042 decision 4 ("Colonne exported_at sur recommendations (migration
-- 078, la même migration que daily_runs)"). Schema-only in THIS PR — the
-- idempotent outbound-export/reconciliation logic that stamps it lands in
-- PR-5. NULL means "not yet exported"; a stamped row is never re-exported
-- (a plain `WHERE exported_at IS NULL` is the idempotency check, ADR-042
-- decision 4).
--
-- Idempotence (pattern from migration 063's header, mandatory — the runner
-- wraps each file in ONE transaction and ABORTS on any error, it does NOT
-- swallow "already exists", so a fixed migration re-attempted at the next
-- boot re-runs every statement from scratch):
--   * CREATE TABLE IF NOT EXISTS   — no-op on re-run.
--   * CREATE INDEX IF NOT EXISTS   — no-op on re-run.
--   * ADD COLUMN IF NOT EXISTS     — no-op on re-run.
--
-- No JSONB: every column here is typed and business-queryable (guard
-- verdicts feed the daily report, ADR-042 §5 — not diagnostic-only detail).
--
-- ref: ADR-042 (PR-2), ADR-037 (INT-1 PR2, absorbed).
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- daily_runs — one row per (feed_key, run_date) guard-evaluation attempt
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_runs (
    daily_run_id           UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),

    -- The contract version this evaluation was run against. RESTRICT: see
    -- header (feed_contracts rows are never hard-deleted).
    feed_contract_id       UUID        NOT NULL REFERENCES feed_contracts(feed_contract_id) ON DELETE RESTRICT,

    -- Denormalized from feed_contracts for query convenience (same
    -- denormalization pattern as shortages' item_external_id, migration
    -- 004) — a feed_key spans many contract versions, so this is NOT
    -- redundant with a simple join key, it is the stable identifier the
    -- daily-run runtime actually keys evaluations on.
    feed_key               TEXT        NOT NULL,

    -- Calendar day this evaluation covers (the daily run's own "today").
    run_date               DATE        NOT NULL,

    -- When THIS evaluation attempt ran (may be re-evaluated intra-day —
    -- see header "APPEND-ONLY AUDIT TRAIL").
    observed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- When the feed's file actually landed, NULL if it never arrived by
    -- the time this evaluation ran.
    file_arrived_at        TIMESTAMPTZ,

    -- Observed row count in the arrived file, NULL if the file never
    -- arrived (None-honest: distinct from a genuine zero-row file).
    row_count              INTEGER     CHECK (row_count IS NULL OR row_count >= 0),

    -- The previous evaluation's row_count for this feed_key (the volume-
    -- delta guard's baseline), denormalized at write time so a daily_runs
    -- row is a self-contained audit record even if earlier rows are later
    -- purged/retained differently.
    previous_row_count     INTEGER     CHECK (previous_row_count IS NULL OR previous_row_count >= 0),

    -- Rows that disappeared vs the previous run's canonical picture (the
    -- deletion-ratio guard's numerator). NULL until the real file-diff
    -- service (PR-3/4) supplies it — see module docstring for the current
    -- caller-supplied-observation boundary.
    deleted_count          INTEGER     CHECK (deleted_count IS NULL OR deleted_count >= 0),

    -- Denormalized from feed_contracts at evaluation time (a contract's
    -- criticality could in principle change between versions; this column
    -- freezes what was actually in effect for THIS evaluation).
    criticality            TEXT        NOT NULL CHECK (criticality IN ('blocking', 'advisory')),

    -- Per-guard verdicts — kept in lockstep with interfaces/guards.py's
    -- GuardStatus enum (see header).
    arrival_status         TEXT        NOT NULL CHECK (arrival_status        IN ('ok', 'failed', 'not_evaluated')),
    volume_floor_status    TEXT        NOT NULL CHECK (volume_floor_status   IN ('ok', 'failed', 'not_evaluated')),
    volume_delta_status    TEXT        NOT NULL CHECK (volume_delta_status   IN ('ok', 'failed', 'not_evaluated')),
    deletion_ratio_status  TEXT        NOT NULL CHECK (deletion_ratio_status IN ('ok', 'failed', 'not_evaluated')),

    -- FAILED iff any of the four guards above is FAILED — never
    -- NOT_EVALUATED itself (see interfaces/guards.py:FeedGuardEvaluation.overall_status).
    overall_status         TEXT        NOT NULL CHECK (overall_status IN ('ok', 'failed')),

    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE daily_runs IS
    'Guard-evaluation audit trail for the daily-run governed-ingest pipeline '
    '(ADR-042 PR-2, absorbs ADR-037 INT-1 PR2). One row per (feed_key, '
    'run_date) evaluation ATTEMPT — append-only, not upserted (a feed can be '
    'legitimately re-evaluated intra-day). The governed auto-approve/'
    'escalate decision that CONSUMES these verdicts is PR-3 territory '
    '(engine/ingest/apply.py), not written here.';

COMMENT ON COLUMN daily_runs.feed_contract_id IS
    'The feed_contracts version this evaluation ran against. ON DELETE '
    'RESTRICT: feed_contracts rows are never hard-deleted (append-only per '
    'version), this is a pure safety net.';

COMMENT ON COLUMN daily_runs.deleted_count IS
    'Rows that disappeared vs the previous run''s canonical picture — the '
    'deletion_ratio guard''s numerator. NULL until the real file-diff '
    'service (PR-3/4) supplies it; interfaces/guards.py treats NULL as '
    'NOT_EVALUATED, never a fabricated pass.';

COMMENT ON COLUMN daily_runs.overall_status IS
    'FAILED iff any of the four per-guard columns is failed. Never '
    '''not_evaluated'' itself — an overall verdict is always a real ok/'
    'failed call (interfaces/guards.py:FeedGuardEvaluation.overall_status).';

-- Guard-evaluation history for one feed, newest first (the daily report,
-- ADR-042 §5, and plan_daily_run_guard_check's previous-day lookup both
-- read this way).
CREATE INDEX IF NOT EXISTS idx_daily_runs_feed_key_run_date
    ON daily_runs (feed_key, run_date DESC, observed_at DESC);

-- Cross-feed "what failed today" query (the daily report's headline view).
CREATE INDEX IF NOT EXISTS idx_daily_runs_run_date_overall_status
    ON daily_runs (run_date, overall_status);

-- ------------------------------------------------------------
-- recommendations.exported_at — idempotent-export marker (ADR-042 decision 4)
-- ------------------------------------------------------------
-- Schema-only in this PR: nothing stamps this column yet (the outbound
-- export/reconciliation write-side lands in PR-5). NULL = not yet exported.
ALTER TABLE recommendations ADD COLUMN IF NOT EXISTS exported_at TIMESTAMPTZ;

COMMENT ON COLUMN recommendations.exported_at IS
    'When this recommendation was written to the ootils-outbox as a '
    'po_draft/reschedule_message (ADR-042 decision 4, PR-5). NULL = not yet '
    'exported. A stamped row is NEVER re-exported: a plain WHERE '
    'exported_at IS NULL is the idempotency check. Schema-only in PR-2 — '
    'nothing stamps this column until PR-5.';

-- The pending-export scan ("every APPROVED reco not yet exported") is a
-- partial index on the common case (most recos, once exported, are never
-- queried by this predicate again).
CREATE INDEX IF NOT EXISTS ix_reco_pending_export
    ON recommendations (status)
    WHERE exported_at IS NULL;

COMMIT;
