-- ============================================================
-- Migration 079 — events.event_type CHECK += 'daily_run_completed'
-- ============================================================
-- ADR-042 (docs/ADR-042-interface-doctrine.md) decision 3 step 10 / PR-3 of
-- the pilot-decided delivery order ("la valeur d'abord"), which absorbs
-- ADR-037's INT-1 PR3 (docs/ADR-037-daily-run-and-governed-ingest.md §5):
-- "Le moteur de décision gouvernée (option a) : combine le statut DQ
-- existant du batch avec le verdict des gardes PR2 par flux ; auto-approuve
-- le run entier ssi tout est vert ; toute garde rouge sur un flux blocking
-- bloque l'auto-approbation et escalade via le webhook L3 ; une garde rouge
-- advisory dégrade la confiance du run sans le bloquer."
--
-- Migration 078's header explicitly deferred this exact widening to "the PR
-- that actually emits it (same discipline as migration 076's
-- purge_executed, added in the SAME PR that started emitting it)" — this is
-- that PR. The emitter is engine/ingest/apply.py:record_daily_run_decision.
--
-- ONE event per RUN (ADR-027 / migration 071 convention, reused verbatim by
-- migration 076 for purge_executed): 'daily_run_completed' is emitted once
-- per record_daily_run_decision() call, at the granularity of "every feed
-- evaluated for run_date + the governed decision taken" — never once per
-- feed (that per-feed granularity is already the daily_runs table itself,
-- migration 078).
--
-- NO NEW TABLE in this migration, unlike migration 076's
-- maintenance_purge_runs. The run-level decision is DERIVED, on read, from
-- the already-persisted daily_runs rows (migration 078) for a given
-- run_date (the most recent row per feed_key, per that migration's own
-- documented "current verdict" rule) plus a batch's DQ status supplied by
-- the caller (see engine/ingest/apply.py's module docstring for why DQ
-- status is caller-supplied rather than DB-derived in this PR — there is no
-- wiring yet from a daily_runs row to an ingest_batches/dq_status row, that
-- wiring is real-file-loading territory, not yet built). The
-- 'daily_run_completed' event row IS the durable audit record of the
-- decision actually taken (typed columns: field_changed = decision status,
-- new_date = run_date, new_quantity = feeds evaluated, old_text =
-- comma-joined feed_keys that were NOT green) — see
-- engine/events/emit.py's typed-column contract block.
--
-- Idempotence (pattern from migration 063's header, mandatory — the runner
-- wraps each file in ONE transaction and ABORTS on any error, it does NOT
-- swallow "already exists", so a fixed migration re-attempted at the next
-- boot re-runs every statement from scratch):
--   * DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT — the events.event_type
--     widening idiom used by every prior CHECK extension (006, 051, 062,
--     071, 076): a bare ADD would fail on re-run because the constraint
--     already exists; DROP-first makes it replay-safe.
--
-- Complete list reconstructed from migrations 002 + 006 + 051 + 062 + 071 +
-- 076 (076 is the latest widening before this one — verified no
-- events.event_type CHECK widening exists between 076 and this migration).
-- Adds exactly one new type: 'daily_run_completed'. Follow-up (same
-- discipline as 076's own follow-up note): keep VALID_EVENT_TYPES in
-- src/ootils_core/api/routers/events.py AND FLEET_EVENT_TYPES in
-- src/ootils_core/engine/events/emit.py in sync with this CHECK — both are
-- updated in THIS PR, not deferred.
-- ============================================================

BEGIN;

ALTER TABLE events DROP CONSTRAINT IF EXISTS events_event_type_check;
ALTER TABLE events ADD CONSTRAINT events_event_type_check
    CHECK (event_type IN (
        -- migrations 002 + 006 + 051 + 062 + 071 + 076 (existing, unchanged)
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
        'purge_executed',
        -- migration 079 (ADR-042 PR-3, absorbs ADR-037 INT-1 PR3)
        'daily_run_completed'
    ));

COMMIT;
