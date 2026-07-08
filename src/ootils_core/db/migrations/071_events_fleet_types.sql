-- ============================================================
-- Migration 071 — events.event_type += 5 fleet-emission types
-- ============================================================
-- Chantier AN-1 (#401, "events emission"). North Star "Streamable":
-- every state-changing capability MUST write a typed `events` row so the
-- fleet subscribes to deltas via GET /v1/stream (keyset cursor on
-- events.stream_seq, migration 063) instead of polling. Until now four
-- fleet-relevant capabilities emitted nothing and were invisible to the
-- stream. This migration widens the events.event_type CHECK with the 5
-- new types they emit — NO new column (the payload reuses the existing
-- typed columns of `events`, migration 002) and NO new index (all 5 share
-- idx_events_stream_seq, the (scenario_id, stream_seq) keyset index).
--
-- The 5 new types:
--   recommendation_created  — a governed DRAFT recommendation was written
--                             (ShortageDetector / watchers / reschedule).
--   shortage_detected       — a ShortageDetector run persisted shortages.
--   calc_run_finished       — a propagation / calc run reached a terminal
--                             state (completed | completed_stale | failed).
--   snapshot_captured       — an inventory-snapshot capture run persisted
--                             on-hand rows (ADR-030, migration 067).
--   outcome_evaluated       — a reco→outcome evaluation run classified
--                             recommendations (ADR-030, migration 069).
--
-- GRANULARITY = RUN, NEVER PER-ITEM (ADR-027). One event per governed
-- run / calc run / capture batch / evaluation batch — NOT one per
-- recommendation, shortage, snapshot row or outcome. A calc run touching
-- 200 items emits ONE calc_run_finished; a ShortageDetector pass over 50
-- items emits ONE shortage_detected. This keeps the stream a feed of
-- decisions/runs the fleet acts on, not a firehose of row-level noise, and
-- keeps stream_seq cursors cheap. The per-run count lives in new_quantity
-- (see mapping below) so a subscriber sees "how many" without a join.
--
-- TYPED-COLUMN CONTRACT per new type (no JSONB — migration 002 columns):
--   recommendation_created:
--       trigger_node_id = target node of the reco (nullable if none)
--       field_changed   = the reco action (e.g. 'EXPEDITE','ORDER_NOW',
--                         'RESCHEDULE_IN','TRANSFER') — the discriminant
--       new_text        = recommendation_id (UUID as text)
--       old_text        = source/agent ref (e.g. 'shortage_watcher')
--       new_quantity    = count of recos created in the run
--   shortage_detected:
--       field_changed   = 'shortage_detected' (discriminant)
--       new_quantity    = count of shortages persisted in the run
--       new_text        = calc_run_id / detector run ref (UUID as text)
--   calc_run_finished:
--       field_changed   = terminal status ('completed'|'completed_stale'
--                         |'failed') — the discriminant
--       new_text        = calc_run_id (UUID as text)
--       new_quantity    = count of nodes (re)computed in the run
--   snapshot_captured:
--       field_changed   = 'snapshot_captured' (discriminant)
--       new_date        = as_of_date of the capture
--       new_quantity    = count of snapshot rows persisted
--       new_text        = capture run ref (UUID as text)
--   outcome_evaluated:
--       field_changed   = 'outcome_evaluated' (discriminant)
--       new_quantity    = count of recommendations classified in the run
--       new_text        = evaluation run ref (UUID as text)
-- In all cases scenario_id (NOT NULL, migration 002) scopes the event to
-- the fork/baseline; snapshot_captured/outcome_evaluated are baseline-only
-- by nature (ADR-030) but the column contract is identical.
--
-- Idempotence (runner wraps each file in ONE transaction and ABORTS on any
-- error — it does NOT swallow "already exists"; see migration 063 header).
-- DROP CONSTRAINT IF EXISTS *before* ADD CONSTRAINT is the replay-safe
-- idiom used by every prior widening (006, 051, 062): on a re-run the DROP
-- removes the constraint this migration just added, then ADD re-creates the
-- identical one — a clean no-op. (A bare ADD would fail on re-run because
-- events_event_type_check already exists; the DROP-first pattern is what
-- makes DROP+ADD re-executable.) No payload rewrite, no UPDATE — events
-- stays immutable on its payload (ADR-005 D2).
--
-- Keep this list in sync with VALID_EVENT_TYPES in
-- src/ootils_core/api/routers/events.py
-- (migrations 002 + 006 + 051 + 062 + 071).
-- ============================================================

BEGIN;

ALTER TABLE events DROP CONSTRAINT IF EXISTS events_event_type_check;
ALTER TABLE events ADD CONSTRAINT events_event_type_check
    CHECK (event_type IN (
        -- migrations 002 + 006 + 051 + 062 (existing, unchanged)
        'supply_date_changed', 'supply_qty_changed',
        'demand_qty_changed', 'onhand_updated',
        'policy_changed', 'structure_changed',
        'scenario_created', 'calc_triggered',
        'ingestion_complete', 'po_date_changed',
        'test_event', 'scenario_merge',
        'recommendation_transition',
        'node_firm_changed',
        -- migration 071 (#401 AN-1, fleet emission)
        'recommendation_created', 'shortage_detected',
        'calc_run_finished', 'snapshot_captured',
        'outcome_evaluated'
    ));

COMMIT;
