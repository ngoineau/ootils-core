-- ============================================================
-- Ootils Core — Migration 006: Sprint M5 Scenarios
-- Adds scenario_overrides and scenario_diffs tables.
-- Also extends events.event_type CHECK to include 'scenario_merge'.
-- ============================================================

-- ============================================================
-- 0. Extend events.event_type CHECK constraint
--    (scenario_merge is a first-class event per EXPERT doc Q4.5)
-- ============================================================

ALTER TABLE events DROP CONSTRAINT IF EXISTS events_event_type_check;
ALTER TABLE events ADD CONSTRAINT events_event_type_check
    CHECK (event_type IN (
        'supply_date_changed', 'supply_qty_changed',
        'demand_qty_changed', 'onhand_updated',
        'policy_changed', 'structure_changed',
        'scenario_created', 'calc_triggered',
        'ingestion_complete', 'po_date_changed',
        'test_event', 'scenario_merge'
    ));

-- ============================================================
-- 1. SCENARIO OVERRIDES
-- User intent layer: typed as TEXT per ADR (no JSONB).
-- UNIQUE per (scenario, node, field) — one active override.
-- ============================================================

CREATE TABLE IF NOT EXISTS scenario_overrides (
    override_id     UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id     UUID        NOT NULL REFERENCES scenarios(scenario_id),
    node_id         UUID        NOT NULL REFERENCES nodes(node_id),
    field_name      TEXT        NOT NULL,   -- e.g. 'quantity', 'time_ref'
    old_value       TEXT,                   -- serialized previous value (TEXT, nullable)
    new_value       TEXT        NOT NULL,   -- serialized new value
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_by      TEXT,                   -- user/agent reference
    UNIQUE (scenario_id, node_id, field_name)  -- one active override per (scenario, node, field)
);

-- ============================================================
-- 2. SCENARIO DIFFS
-- Computed result: baseline calc_run vs scenario calc_run, node by node.
-- Persisted after each diff() call.
-- ============================================================

CREATE TABLE IF NOT EXISTS scenario_diffs (
    diff_id                 UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id             UUID        NOT NULL REFERENCES scenarios(scenario_id),
    baseline_calc_run_id    UUID        NOT NULL REFERENCES calc_runs(calc_run_id),
    scenario_calc_run_id    UUID        NOT NULL REFERENCES calc_runs(calc_run_id),
    node_id                 UUID        NOT NULL REFERENCES nodes(node_id),
    field_name              TEXT        NOT NULL,
    baseline_value          TEXT,
    scenario_value          TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (scenario_id, baseline_calc_run_id, scenario_calc_run_id, node_id, field_name)
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_scenario_overrides_scenario
    ON scenario_overrides (scenario_id);

CREATE INDEX IF NOT EXISTS idx_scenario_overrides_node
    ON scenario_overrides (node_id);

CREATE INDEX IF NOT EXISTS idx_scenario_diffs_scenario
    ON scenario_diffs (scenario_id);

CREATE INDEX IF NOT EXISTS idx_scenario_diffs_node
    ON scenario_diffs (node_id);
