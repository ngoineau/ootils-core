-- ============================================================
-- Migration 060 — Scenario planning parameter overlay (#347)
-- ============================================================
-- Chantier #347: agents (and humans, inside a scenario) need to test
-- "what if lead_time_sourcing_days was 21 instead of 14" or "what if
-- safety_stock_qty was doubled" WITHOUT writing to item_planning_params
-- (the SCD2 master-data table, migration 007) and without forking that
-- table per scenario. scenario_planning_overrides is a pure SIMULATION
-- overlay: a (scenario, item, [location], field) → value row that the
-- planning-params resolver layers on top of the CURRENT SCD2 row
-- (effective_to IS NULL) at read time. It is never promoted into
-- item_planning_params directly — promotion (if any) is a distinct,
-- explicit, governed write to master data, out of scope here. Overrides
-- live only inside their scenario and are dropped/ignored on merge
-- unless a future promotion flow says otherwise.
--
-- Value is a serialized TEXT scalar, cast by the resolver against the
-- target column's real type (INTEGER / NUMERIC / TEXT enum) — same
-- pattern as scenario_overrides.new_value (migration 006). This is NOT
-- a JSONB carve-out: the value is always a single scalar, never a
-- variable-shape payload, so a typed TEXT column (not JSONB) is the
-- correct fit per the repo's "no JSONB for business data" rule.
--
-- FK policy:
--   * scenario_id → scenarios(scenario_id) ON DELETE RESTRICT — mandatory
--     per ADR-011 (docs/ADR-011-scenario-retention.md): every FK on
--     scenarios(scenario_id) is RESTRICT, scenarios are soft-deleted
--     (status='archived') never hard-deleted. Guarded by
--     tests/integration/test_scenario_fk_retention.py.
--   * item_id → items(item_id) ON DELETE CASCADE — an override is
--     meaningless once its item is gone; items are reference data that
--     in practice is never hard-deleted in normal operation (status flag
--     instead, migration 002), so CASCADE here is a safety net, not a
--     live path.
--   * location_id → locations(location_id) ON DELETE CASCADE, NULLABLE —
--     same reasoning as item_id for the CASCADE; NULL is a first-class
--     value here (see whitelist / uniqueness note below), meaning
--     "applies to the item at every location" (item-global override).
--
-- Uniqueness: location_id is nullable and part of the natural key, so a
-- plain UNIQUE(scenario_id, item_id, location_id, field_name) would NOT
-- prevent two "item-global" (location_id NULL) overrides for the same
-- (scenario, item, field) — Postgres treats NULLs as distinct by
-- default. Two ways to fix this: (a) two partial unique indexes (one
-- WHERE location_id IS NULL, one WHERE location_id IS NOT NULL), or
-- (b) a single UNIQUE ... NULLS NOT DISTINCT constraint (PG15+). We are
-- on PG16 (see CLAUDE.md), so (b) is available and chosen: it expresses
-- the "one active override per (scenario, item, location incl. NULL,
-- field)" rule as ONE constraint instead of two overlapping indexes,
-- which is both simpler to read and avoids any risk of the two partial
-- indexes drifting apart under a future edit. The underlying index this
-- constraint creates also satisfies the resolver's
-- (scenario_id, item_id, location_id) lookup pattern, so no separate
-- index is added.
--
-- field_name whitelist (V1, #347 architect plan) is CHECKed against the
-- planning fields the resolver is scoped to touch in V1. Cross-checked
-- against the real item_planning_params columns (migration 007
-- baseline + migration 021 lot-sizing additions):
--   lead_time_sourcing_days, lead_time_manufacturing_days,
--   lead_time_transit_days, safety_stock_qty, safety_stock_days,
--   min_order_qty, max_order_qty, order_multiple_qty, lot_size_rule,
--   economic_order_qty, lot_size_poq_periods, frozen_time_fence_days,
--   slashed_time_fence_days, forecast_consumption_strategy,
--   consumption_window_days
-- All fifteen are confirmed real columns on item_planning_params — none
-- dropped from the requested whitelist. Note for future readers:
-- item_planning_params ALSO has a legacy `order_multiple` column
-- (migration 007, pre-APICS) distinct from `order_multiple_qty`
-- (migration 021, APICS lot-sizing engine); mrp_apics_engine.py already
-- COALESCEs the two when reading. This overlay intentionally targets
-- only `order_multiple_qty` (the APICS-engine-facing column) — adding
-- `order_multiple` to the whitelist as a second, competing target is a
-- V2 decision if a caller ever needs to override the legacy column.
--
-- Idempotence (repo migration policy: a re-run must not fail):
--   * CREATE TABLE IF NOT EXISTS — no-op on re-run.
-- No JSONB. Typed columns only.
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS scenario_planning_overrides (
    override_id     UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id     UUID        NOT NULL REFERENCES scenarios(scenario_id) ON DELETE RESTRICT,
    item_id         UUID        NOT NULL REFERENCES items(item_id) ON DELETE CASCADE,
    location_id     UUID                 REFERENCES locations(location_id) ON DELETE CASCADE,
    field_name      TEXT        NOT NULL CHECK (field_name IN (
                        'lead_time_sourcing_days',
                        'lead_time_manufacturing_days',
                        'lead_time_transit_days',
                        'safety_stock_qty',
                        'safety_stock_days',
                        'min_order_qty',
                        'max_order_qty',
                        'order_multiple_qty',
                        'lot_size_rule',
                        'economic_order_qty',
                        'lot_size_poq_periods',
                        'frozen_time_fence_days',
                        'slashed_time_fence_days',
                        'forecast_consumption_strategy',
                        'consumption_window_days'
                    )),
    value           TEXT        NOT NULL,   -- serialized scalar, cast by the resolver against the target column type
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_by      TEXT        NOT NULL,   -- agent id or user id — audit, always attributed

    -- One active override per (scenario, item, location, field). NULLS NOT
    -- DISTINCT so a NULL location_id (item-global override) is itself
    -- unique per (scenario, item, field) — see rationale above.
    CONSTRAINT scenario_planning_overrides_natural_key
        UNIQUE NULLS NOT DISTINCT (scenario_id, item_id, location_id, field_name)
);

COMMENT ON TABLE scenario_planning_overrides IS
    'Scenario-scoped simulation overlay on item_planning_params fields '
    '(#347). Never promoted automatically; applies on top of the current '
    '(effective_to IS NULL) SCD2 row for the scenario''s lifetime only.';

COMMIT;
