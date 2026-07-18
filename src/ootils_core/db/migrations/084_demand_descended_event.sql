-- ============================================================
-- Migration 084 — events.event_type CHECK += 'demand_descended'
-- ============================================================
-- ADR-043 (docs/ADR-043-demand-descent.md), chantier DESC-1 PR-B ("le run de
-- descente — activer le lien"). §1 point 5 of that ADR: "Event typé
-- `demand_descended` — un événement par run, même granularité qu'ADR-027/
-- ADR-039's `purge_executed` et ADR-042's `daily_run_completed` (un event de
-- confirmation par exécution, pas par ligne)."
--
-- ONE event per demand-descent RUN (ADR-027 run-granularity, migration 071's
-- convention, reused verbatim by migration 076 for purge_executed and by
-- migration 079 for daily_run_completed): 'demand_descended' is emitted once
-- per POST /v1/demand/descend execution — never once per demand_descent_lines
-- row (migration 083), never once per derived node written. The per-run line
-- count travels in the event's new_quantity column, same discipline as every
-- prior fleet emission (see engine/events/emit.py's typed-column contract
-- block, updated in THIS PR alongside this CHECK widening).
--
-- NO NEW TABLE in this migration, same posture as migration 079: the durable
-- per-line audit trail already exists (demand_descent_lines, migration 083);
-- this event is the RUN-level confirmation row on top of it, not a duplicate
-- ledger.
--
-- Idempotence (pattern from migration 063's header, mandatory — the runner
-- wraps each file in ONE transaction and ABORTS on any error, it does NOT
-- swallow "already exists", so a fixed migration re-attempted at the next
-- boot re-runs every statement from scratch):
--   * DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT — the events.event_type
--     widening idiom used by every prior CHECK extension (006, 051, 062,
--     071, 076, 079): a bare ADD would fail on re-run because the constraint
--     already exists; DROP-first makes it replay-safe.
--
-- Complete list reconstructed from migrations 002 + 006 + 051 + 062 + 071 +
-- 076 + 079 (079 is the latest widening before this one — verified no
-- events.event_type CHECK widening exists between 079 and this migration).
-- Adds exactly one new type: 'demand_descended'. Follow-up (same discipline
-- as 076's and 079's own follow-up notes, applied consciously in THIS PR,
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
        -- migrations 002 + 006 + 051 + 062 + 071 + 076 + 079 (existing, unchanged)
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
        -- migration 084 (ADR-043, DESC-1 PR-B)
        'demand_descended'
    ));

COMMIT;
