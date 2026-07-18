-- ============================================================
-- Migration 085 — events.event_type CHECK += 'export_executed'
-- ============================================================
-- ADR-042 (docs/ADR-042-interface-doctrine.md) decision 4 ("Le sortant et la
-- réconciliation"): "Un event `export_executed` par run (même granularité que
-- `daily_run_completed`)." This is chantier PR-5 of the pilot-decided
-- delivery order ("Sortant idempotent (`exported_at`, `export_executed`) +
-- réconciliation heuristique -> `recommendation_outcomes`, ADR-030").
-- `recommendations.exported_at` (migration 078) already carries the
-- idempotency marker this event announces (`WHERE exported_at IS NULL` is
-- the pending-export scan, decision 4); this migration only widens the
-- events.event_type CHECK so the outbound-export write-side (this PR's
-- engine code, not this migration) can emit it.
--
-- ONE event per outbound-export RUN (ADR-027 run-granularity, migration
-- 071's convention, reused verbatim by migration 076 for purge_executed,
-- migration 079 for daily_run_completed and migration 084 for
-- demand_descended): 'export_executed' is emitted once per export run that
-- writes the ootils-outbox (dropbox:ootils-outbox, ADR-042 §4) — never once
-- per recommendation row stamped exported_at. The per-run count travels in
-- the event's new_quantity column, same discipline as every prior fleet
-- emission (see engine/events/emit.py's typed-column contract block,
-- updated in THIS PR alongside this CHECK widening).
--
-- NO NEW TABLE in this migration, same posture as migrations 079/084: the
-- durable per-row idempotency marker already exists
-- (recommendations.exported_at, migration 078); this event is the RUN-level
-- confirmation row on top of it, not a duplicate ledger. Unlike
-- calc_run_finished/shortage_detected/outcome_evaluated/demand_descended
-- (whose new_text carries a companion-table run id), there is no
-- export_runs audit table — same "no companion audit table" posture as
-- daily_run_completed (migration 079) — so new_text instead carries the
-- comma-joined list of file names actually written to the outbox (see
-- engine/events/emit.py's contract block), the artifact a fleet subscriber
-- can act on directly.
--
-- Idempotence (pattern from migration 063's header, mandatory — the runner
-- wraps each file in ONE transaction and ABORTS on any error, it does NOT
-- swallow "already exists", so a fixed migration re-attempted at the next
-- boot re-runs every statement from scratch):
--   * DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT — the events.event_type
--     widening idiom used by every prior CHECK extension (006, 051, 062,
--     071, 076, 079, 084): a bare ADD would fail on re-run because the
--     constraint already exists; DROP-first makes it replay-safe.
--
-- Complete list reconstructed from migrations 002 + 006 + 051 + 062 + 071 +
-- 076 + 079 + 084 (084 is the latest widening before this one — verified no
-- events.event_type CHECK widening exists between 084 and this migration).
-- Adds exactly one new type: 'export_executed'. Follow-up (same discipline
-- as 076's/079's/084's own follow-up notes, applied consciously in THIS PR,
-- not deferred — the PURGE-1/PR-3 lesson): keep VALID_EVENT_TYPES in
-- src/ootils_core/api/routers/events.py AND FLEET_EVENT_TYPES in
-- src/ootils_core/engine/events/emit.py (plus its pinned unit/integration
-- test derivations, tests/test_emit_stream_event.py and
-- tests/integration/test_fleet_events_integration.py) in sync with this
-- CHECK — all updated in THIS PR.
-- ============================================================

BEGIN;

ALTER TABLE events DROP CONSTRAINT IF EXISTS events_event_type_check;
ALTER TABLE events ADD CONSTRAINT events_event_type_check
    CHECK (event_type IN (
        -- migrations 002 + 006 + 051 + 062 + 071 + 076 + 079 + 084 (existing, unchanged)
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
        'daily_run_completed',
        'demand_descended',
        -- migration 085 (ADR-042 decision 4, PR-5)
        'export_executed'
    ));

COMMIT;
