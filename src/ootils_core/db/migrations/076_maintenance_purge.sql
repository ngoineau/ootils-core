-- ============================================================
-- Migration 076 — PURGE-1: scenario archive/purge lifecycle + maintenance_purge_runs
-- ============================================================
-- Chantier PURGE-1. Ootils uses a soft-delete pattern on scenarios
-- (status='archived', migration 002/015/032) — an archived fork is never
-- hard-deleted by application code, and every FK pointing at
-- scenarios(scenario_id) is ON DELETE RESTRICT (migration 032), so a
-- referenced scenario cannot be hard-deleted at all. Over time this leaves
-- an unbounded number of archived scenarios (and their forked node/edge/
-- shortage/recommendation rows) accumulating with no retention mechanism.
-- PURGE-1 introduces the retention lifecycle: WHEN a scenario was archived
-- (new column), a TTL-driven purge run that walks per-table deletes for
-- old archived scenarios, and an audit trail of every purge execution
-- (dry-run or apply). This migration is schema-only — no purge logic runs
-- here; it lands the columns/table the PURGE-1 engine code (a later PR)
-- reads and writes.
--
-- Idempotence (pattern from migration 063's header, mandatory — the runner
-- wraps each file in ONE transaction and ABORTS on any error, it does NOT
-- swallow "already exists", so a fixed migration re-attempted at the next
-- boot re-runs every statement from scratch):
--   * ADD COLUMN IF NOT EXISTS       — no-op on re-run.
--   * UPDATE ... WHERE ... IS NULL   — the backfill only ever touches rows
--     still missing archived_at; a re-run finds none left and is a no-op.
--   * DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT — the events.event_type
--     widening idiom used by every prior CHECK extension (006, 051, 062,
--     071): a bare ADD would fail on re-run because the constraint already
--     exists; DROP-first makes it replay-safe.
--   * CREATE TABLE IF NOT EXISTS     — no-op on re-run.
--
-- JSONB carve-out (see CLAUDE.md "no JSONB for business data — explicit
-- carve-outs for diagnostic/staging payloads"):
--   `maintenance_purge_runs.per_table_counts` stores the per-table row-
--   deletion (or would-delete, for dry_run) counts of a single purge run —
--   e.g. {"shortages": 1204, "nodes": 340, "recommendations": 12, ...}.
--   The set of tables a purge run touches is an implementation detail of
--   the purge engine (it grows as new scenario-scoped tables are added to
--   the schema) and is never queried by key — only rendered whole for
--   operator audit/triage (dashboards, incident review), exactly like the
--   documented carve-outs it joins: `dq_agent_runs.summary` (mig 012),
--   `mrp_runs.errors`/`warnings` (mig 021), `demo_runs.artifact` (mig 031).
--   `rows_deleted_total` (typed BIGINT, below) is the one aggregate figure
--   actually queried/alerted on — the JSONB column is diagnostic detail
--   only, never business data.
-- ============================================================

BEGIN;

-- ============================================================
-- 1. scenarios — archive/purge lifecycle timestamps
-- ============================================================
-- archived_at: when the scenario transitioned to status='archived'.
-- Backfilled from updated_at for scenarios already archived before this
-- migration (the closest available approximation — status transitions are
-- not separately audited pre-PURGE-1).
-- purged_at: when a purge run actually deleted this scenario's rows.
-- NULL means "never purged" (the overwhelming default). Both nullable:
-- most scenarios are neither archived nor purged.

ALTER TABLE scenarios
    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS purged_at   TIMESTAMPTZ;

UPDATE scenarios
   SET archived_at = updated_at
 WHERE status = 'archived'
   AND archived_at IS NULL;

-- ============================================================
-- 2. events.event_type CHECK += 'purge_executed'
-- ============================================================
-- Complete list reconstructed from migrations 002 + 006 + 051 + 062 + 071
-- (071 is the latest widening — verified no events.event_type CHECK
-- widening exists between 071 and this migration). Adds exactly one new
-- type: 'purge_executed', emitted once per maintenance_purge_runs row
-- (RUN granularity, per ADR-027 / migration 071's convention — never once
-- per deleted row). Follow-up (outside this migration's scope, tracked for
-- the PURGE-1 engine PR): keep VALID_EVENT_TYPES in
-- src/ootils_core/api/routers/events.py in sync with this CHECK.

ALTER TABLE events DROP CONSTRAINT IF EXISTS events_event_type_check;
ALTER TABLE events ADD CONSTRAINT events_event_type_check
    CHECK (event_type IN (
        -- migrations 002 + 006 + 051 + 062 + 071 (existing, unchanged)
        'supply_date_changed', 'supply_qty_changed',
        'demand_qty_changed', 'onhand_updated',
        'policy_changed', 'structure_changed',
        'scenario_created', 'calc_triggered',
        'ingestion_complete', 'po_date_changed',
        'test_event', 'scenario_merge',
        'recommendation_transition',
        'node_firm_changed',
        'recommendation_created', 'shortage_detected',
        'calc_run_finished', 'snapshot_captured',
        'outcome_evaluated',
        -- migration 076 (PURGE-1)
        'purge_executed'
    ));

-- ============================================================
-- 3. maintenance_purge_runs — audit trail of every purge execution
-- ============================================================
-- One row per purge run, dry_run or apply. scenario_id references the
-- scenario the run purged (RESTRICT per migration 032's blanket policy on
-- every FK to scenarios(scenario_id) — a purge-run audit row must never be
-- silently orphaned by a later hard-delete of its own subject scenario;
-- test_scenario_fk_retention.py asserts this for every FK on scenarios).
-- No index: scenarios (and therefore purge runs) are small in volume at
-- current demo/pilot scale (docs/SCALABILITY.md) and this table is read by
-- operators/audit tooling, not on any propagation hot path.

CREATE TABLE IF NOT EXISTS maintenance_purge_runs (
    run_id              UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id         UUID        NOT NULL REFERENCES scenarios(scenario_id) ON DELETE RESTRICT,
    mode                TEXT        NOT NULL CHECK (mode IN ('dry_run', 'apply')),
    ttl_days            INTEGER     NOT NULL,
    per_table_counts    JSONB,      -- diagnostic payload (see header comment)
    rows_deleted_total  BIGINT      NOT NULL DEFAULT 0,
    executed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    executed_by         TEXT        NOT NULL
);

COMMIT;
