-- ============================================================
-- Migration 062 — events.event_type += 'node_firm_changed'
-- ============================================================
-- #346 PR-C: POST/DELETE /v1/nodes/{node_id}/firm (firm/unfirm a
-- PlannedSupply into a Firm Planned Order, migration 061 nodes.is_firm)
-- emits one event per mutation so agents can subscribe instead of
-- polling (Streamable principle). Typed delta columns:
-- field_changed='is_firm', old_text/new_text='false'/'true',
-- trigger_node_id=the PlannedSupply node, user_ref=actor.
--
-- Keep this list in sync with VALID_EVENT_TYPES in
-- src/ootils_core/api/routers/events.py (migrations 002 + 006 + 051 + 062).
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
        'recommendation_transition',
        'node_firm_changed'
    ));
