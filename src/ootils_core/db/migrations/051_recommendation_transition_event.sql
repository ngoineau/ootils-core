-- ============================================================
-- Migration 051 — events.event_type += 'recommendation_transition'
-- ============================================================
-- The recommendation governance API (POST /v1/recommendations/{id}/
-- transition, chantier #341a) emits one event per state-machine
-- transition so agents can subscribe to governance changes instead
-- of polling (Streamable principle). The event carries the delta in
-- typed columns: field_changed='status', old_text=from_status,
-- new_text=to_status, user_ref=actor.
--
-- Keep this list in sync with VALID_EVENT_TYPES in
-- src/ootils_core/api/routers/events.py (migrations 002 + 006 + 051).
-- ============================================================

ALTER TABLE events DROP CONSTRAINT IF EXISTS events_event_type_check;
ALTER TABLE events ADD CONSTRAINT events_event_type_check
    CHECK (event_type IN (
        'supply_date_changed', 'supply_qty_changed',
        'demand_qty_changed', 'onhand_updated',
        'policy_changed', 'structure_changed',
        'scenario_created', 'calc_triggered',
        'ingestion_complete', 'po_date_changed',
        'test_event', 'scenario_merge',
        'recommendation_transition'
    ));
