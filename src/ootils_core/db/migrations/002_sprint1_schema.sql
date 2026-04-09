-- ============================================================
-- Ootils Core — Migration 002: Sprint 1 Schema
-- PostgreSQL-native schema for graph-based supply chain planning engine
-- 
-- Conventions:
--   - All PKs are UUID
--   - All timestamps are TIMESTAMPTZ UTC
--   - No JSONB for structured data — typed columns everywhere
--   - Typed CHECK constraints on all enum-like columns
-- ============================================================

-- ============================================================
-- 0. REFERENCE TABLES (scenario-independent)
-- ============================================================

CREATE TABLE IF NOT EXISTS scenarios (
    scenario_id          UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 TEXT        NOT NULL,
    description          TEXT,
    parent_scenario_id   UUID        REFERENCES scenarios(scenario_id),
    is_baseline          BOOLEAN     NOT NULL DEFAULT FALSE,
    -- baseline_snapshot_id: set when this scenario diverges from baseline
    -- NULL means no divergence — completed calc_runs are 'completed' (not 'completed_stale')
    baseline_snapshot_id UUID,
    status               TEXT        NOT NULL DEFAULT 'active'
                         CHECK (status IN ('active', 'running', 'archived', 'failed')),
    as_of_date           DATE,       -- PAST principle anchor; NULL = use today
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed: baseline scenario
INSERT INTO scenarios (scenario_id, name, is_baseline, status)
VALUES ('00000000-0000-0000-0000-000000000001'::UUID, 'Baseline', TRUE, 'active')
ON CONFLICT (scenario_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS items (
    item_id      UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT        NOT NULL,
    item_type    TEXT        NOT NULL DEFAULT 'finished_good'
                 CHECK (item_type IN ('finished_good', 'component', 'raw_material', 'semi_finished')),
    uom          TEXT        NOT NULL DEFAULT 'EA',
    status       TEXT        NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active', 'obsolete', 'phase_out')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS locations (
    location_id   UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT        NOT NULL,
    location_type TEXT        NOT NULL DEFAULT 'dc'
                  CHECK (location_type IN ('plant', 'dc', 'warehouse', 'supplier_virtual', 'customer_virtual')),
    country       TEXT,
    timezone      TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- 1. NODES
-- ============================================================

CREATE TABLE IF NOT EXISTS nodes (
    -- Identity
    node_id             UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    node_type           TEXT        NOT NULL
                        CHECK (node_type IN (
                            'Item', 'Location', 'PurchaseOrderSupply', 'OnHandSupply',
                            'ProjectedInventory', 'ForecastDemand', 'CustomerOrderDemand',
                            'WorkOrderSupply', 'TransferSupply', 'PlannedSupply',
                            'DependentDemand', 'TransferDemand', 'Shortage'
                        )),
    scenario_id         UUID        NOT NULL REFERENCES scenarios(scenario_id),
    item_id             UUID        REFERENCES items(item_id),
    location_id         UUID        REFERENCES locations(location_id),

    -- Quantity (supply/demand amount)
    quantity            NUMERIC,
    qty_uom             TEXT,

    -- Temporal fields (typed per ADR-002; no JSONB)
    time_grain          TEXT        CHECK (time_grain IN ('exact_date', 'day', 'week', 'month', 'timeless')),
    time_ref            DATE,       -- Anchor date: PO due date, OH as_of_date, etc.
    time_span_start     DATE,       -- Inclusive bucket start (PI nodes)
    time_span_end       DATE,       -- Exclusive bucket end (PI nodes)

    -- Engine state
    is_dirty            BOOLEAN     NOT NULL DEFAULT FALSE,
    last_calc_run_id    UUID,       -- FK added below (after calc_runs table exists)
    active              BOOLEAN     NOT NULL DEFAULT TRUE,

    -- PI-specific: projection series grouping
    projection_series_id UUID,      -- FK added below (after projection_series table exists)
    bucket_sequence     INTEGER,    -- 0-indexed position within series

    -- PI computation results (typed columns, no JSONB)
    opening_stock       NUMERIC,
    inflows             NUMERIC,
    outflows            NUMERIC,
    closing_stock       NUMERIC,
    has_shortage        BOOLEAN     NOT NULL DEFAULT FALSE,
    shortage_qty        NUMERIC     NOT NULL DEFAULT 0,

    -- Grain mix tracking for PI nodes (typed columns, no JSONB)
    -- Tracks what types of inputs contributed to this bucket
    has_exact_date_inputs  BOOLEAN  NOT NULL DEFAULT FALSE,
    has_week_inputs        BOOLEAN  NOT NULL DEFAULT FALSE,
    has_month_inputs       BOOLEAN  NOT NULL DEFAULT FALSE,

    -- Audit
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- 2. EDGES
-- ============================================================

CREATE TABLE IF NOT EXISTS edges (
    edge_id      UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    edge_type    TEXT        NOT NULL
                 CHECK (edge_type IN (
                     'replenishes',    -- PurchaseOrderSupply/WorkOrderSupply → ProjectedInventory
                     'feeds_forward',  -- ProjectedInventory[t] → ProjectedInventory[t+1]
                     'consumes',       -- demand node → PI
                     'depends_on',     -- generic dependency
                     'transfers_to',   -- location-to-location transfer
                     'pegged_to',      -- demand pegged to supply
                     'governed_by'     -- node governed by policy
                 )),
    from_node_id UUID        NOT NULL REFERENCES nodes(node_id),
    to_node_id   UUID        NOT NULL REFERENCES nodes(node_id),
    scenario_id  UUID        NOT NULL REFERENCES scenarios(scenario_id),

    -- Edge semantics
    priority     INTEGER     NOT NULL DEFAULT 0,   -- Lower = higher priority
    weight_ratio NUMERIC     NOT NULL DEFAULT 1.0, -- Qty ratio, BOM fraction
    effective_start DATE,
    effective_end   DATE,

    active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- 3. PROJECTION SERIES
-- Groups all PI nodes for one (item, location, scenario) planning horizon.
-- ============================================================

CREATE TABLE IF NOT EXISTS projection_series (
    series_id       UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id         UUID        NOT NULL REFERENCES items(item_id),
    location_id     UUID        NOT NULL REFERENCES locations(location_id),
    scenario_id     UUID        NOT NULL REFERENCES scenarios(scenario_id),
    horizon_start   DATE        NOT NULL,  -- Planning horizon start (PAST principle)
    horizon_end     DATE        NOT NULL,  -- Planning horizon end
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (item_id, location_id, scenario_id)
);

-- Add FK from nodes to projection_series
ALTER TABLE nodes
    ADD CONSTRAINT fk_nodes_projection_series
    FOREIGN KEY (projection_series_id) REFERENCES projection_series(series_id)
    DEFERRABLE INITIALLY DEFERRED;

-- ============================================================
-- 4. NODE TYPE POLICIES
-- Configurable temporal zone breakpoints per node type.
-- Avoids hardcoding 0/90/180 in application code.
-- ============================================================

CREATE TABLE IF NOT EXISTS node_type_policies (
    policy_id           UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    node_type           TEXT        NOT NULL,   -- e.g., 'ProjectedInventory'
    -- Zone 1: short-term (exact_date or daily)
    zone1_grain         TEXT        NOT NULL DEFAULT 'day',
    zone1_end_days      INTEGER     NOT NULL DEFAULT 90,   -- days from planning_start
    -- Zone 2: medium-term (weekly)
    zone2_grain         TEXT        NOT NULL DEFAULT 'week',
    zone2_end_days      INTEGER     NOT NULL DEFAULT 180,  -- days from planning_start
    -- Zone 3: long-term (monthly; beyond zone2_end_days)
    zone3_grain         TEXT        NOT NULL DEFAULT 'month',
    -- Week start day for weekly grain: 0=Monday (ISO), 6=Sunday
    week_start_dow      INTEGER     NOT NULL DEFAULT 0,
    -- Active policy version
    active              BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (node_type, active)  -- only one active policy per node_type
);

-- Seed: default ProjectedInventory temporal policy
INSERT INTO node_type_policies (node_type, zone1_grain, zone1_end_days, zone2_grain, zone2_end_days, zone3_grain, week_start_dow, active)
VALUES ('ProjectedInventory', 'day', 90, 'week', 180, 'month', 0, TRUE)
ON CONFLICT DO NOTHING;

-- ============================================================
-- 5. EVENTS (IMMUTABLE AUDIT LOG)
-- ============================================================

CREATE TABLE IF NOT EXISTS events (
    event_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type      TEXT        NOT NULL
                    CHECK (event_type IN (
                        'supply_date_changed', 'supply_qty_changed',
                        'demand_qty_changed', 'onhand_updated',
                        'policy_changed', 'structure_changed',
                        'scenario_created', 'calc_triggered',
                        'ingestion_complete', 'po_date_changed',
                        'test_event', 'scenario_merge'
                    )),
    scenario_id     UUID        NOT NULL REFERENCES scenarios(scenario_id),
    trigger_node_id UUID        REFERENCES nodes(node_id),

    -- Typed delta payload — no JSONB
    -- What changed on the trigger node:
    field_changed   TEXT,       -- e.g. 'time_ref', 'quantity'
    old_date        DATE,
    new_date        DATE,
    old_quantity    NUMERIC,
    new_quantity    NUMERIC,
    old_text        TEXT,
    new_text        TEXT,

    -- Processing state
    processed       BOOLEAN     NOT NULL DEFAULT FALSE,
    processed_at    TIMESTAMPTZ,

    -- Provenance
    source          TEXT        NOT NULL DEFAULT 'api'
                    CHECK (source IN ('api', 'ingestion', 'engine', 'user', 'test')),
    user_ref        TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    -- Events are immutable — no updated_at
);

-- ============================================================
-- 6. CALCULATION RUNS
-- State machine: pending → running → completed | completed_stale | failed
-- ============================================================

CREATE TABLE IF NOT EXISTS calc_runs (
    calc_run_id          UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id          UUID        NOT NULL REFERENCES scenarios(scenario_id),

    -- Coalesced event IDs that triggered this run (UUID array — structured, not JSONB)
    triggered_by_event_ids UUID[]    NOT NULL DEFAULT ARRAY[]::UUID[],

    is_full_recompute    BOOLEAN     NOT NULL DEFAULT FALSE,

    -- Counters (updated during run)
    dirty_node_count     INTEGER,
    nodes_recalculated   INTEGER     NOT NULL DEFAULT 0,
    nodes_unchanged      INTEGER     NOT NULL DEFAULT 0,

    -- State machine
    status               TEXT        NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending', 'running', 'completed', 'completed_stale', 'failed')),
    started_at           TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    error_message        TEXT,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Add FK from nodes to calc_runs
ALTER TABLE nodes
    ADD CONSTRAINT fk_nodes_last_calc_run
    FOREIGN KEY (last_calc_run_id) REFERENCES calc_runs(calc_run_id)
    DEFERRABLE INITIALLY DEFERRED;

-- ============================================================
-- 7. DIRTY NODES
-- Authoritative dirty flag store per calc run.
-- Two-tier: in-memory set (fast) + this table (durable).
-- ============================================================

CREATE TABLE IF NOT EXISTS dirty_nodes (
    calc_run_id  UUID        NOT NULL REFERENCES calc_runs(calc_run_id),
    node_id      UUID        NOT NULL REFERENCES nodes(node_id),
    scenario_id  UUID        NOT NULL REFERENCES scenarios(scenario_id),
    marked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (calc_run_id, node_id, scenario_id)
);

-- ============================================================
-- 8. ZONE TRANSITION RUNS
-- Idempotent zone transition tracking.
-- When time zones shift (e.g., daily→weekly boundary moves),
-- PI nodes may need re-bucketing. This table tracks those runs.
-- ============================================================

CREATE TABLE IF NOT EXISTS zone_transition_runs (
    transition_run_id   UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id         UUID        NOT NULL REFERENCES scenarios(scenario_id),
    series_id           UUID        NOT NULL REFERENCES projection_series(series_id),
    -- Date range affected by this transition
    affected_start      DATE        NOT NULL,
    affected_end        DATE        NOT NULL,
    -- Idempotency key: transition runs for same (scenario, series, affected dates) are deduped
    idempotency_key     TEXT        NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (idempotency_key)
);

-- ============================================================
-- INDEXES (propagation access patterns)
-- ============================================================

-- nodes: primary engine access patterns
CREATE INDEX IF NOT EXISTS idx_nodes_scenario_type
    ON nodes (scenario_id, node_type) WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_nodes_item_location_scenario
    ON nodes (item_id, location_id, scenario_id) WHERE active = TRUE;

-- Temporal window scans (most frequent: find PI node for a date)
CREATE INDEX IF NOT EXISTS idx_nodes_time_window
    ON nodes (scenario_id, item_id, location_id, time_span_start, time_span_end)
    WHERE active = TRUE;

-- PI series traversal: get all nodes in a series ordered by sequence
CREATE INDEX IF NOT EXISTS idx_nodes_projection_series_seq
    ON nodes (projection_series_id, bucket_sequence)
    WHERE projection_series_id IS NOT NULL;

-- Dirty flag scan (incremental propagation)
CREATE INDEX IF NOT EXISTS idx_nodes_dirty
    ON nodes (scenario_id, is_dirty) WHERE is_dirty = TRUE;

-- edges: bidirectional traversal
CREATE INDEX IF NOT EXISTS idx_edges_from
    ON edges (from_node_id, edge_type) WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_edges_to
    ON edges (to_node_id, edge_type) WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_edges_scenario
    ON edges (scenario_id, edge_type) WHERE active = TRUE;

-- events: unprocessed event queue
CREATE INDEX IF NOT EXISTS idx_events_unprocessed
    ON events (scenario_id, created_at) WHERE processed = FALSE;

CREATE INDEX IF NOT EXISTS idx_events_trigger_node
    ON events (trigger_node_id, created_at DESC);

-- calc_runs: status monitoring
CREATE INDEX IF NOT EXISTS idx_calc_runs_scenario_status
    ON calc_runs (scenario_id, status, created_at DESC);

-- dirty_nodes: per-run dirty set retrieval
CREATE INDEX IF NOT EXISTS idx_dirty_nodes_run
    ON dirty_nodes (calc_run_id, scenario_id);

-- projection_series: lookup by (item, location, scenario)
CREATE INDEX IF NOT EXISTS idx_projection_series_lookup
    ON projection_series (item_id, location_id, scenario_id);

-- zone_transition_runs: by scenario/series
CREATE INDEX IF NOT EXISTS idx_zone_transition_scenario_series
    ON zone_transition_runs (scenario_id, series_id, status);
-- ============================================================
-- NOTE: shortages table is intentionally NOT created here.
-- It is created by migration 005_m4_shortages.sql with the canonical
-- schema used by ShortageDetector (pi_node_id, severity_score, calc_run_id,
-- status, explanation_id columns).  Creating it here with different columns
-- would cause migration 005 to silently skip the CREATE (IF NOT EXISTS)
-- and leave the shortages table with the wrong schema, crashing the engine.
-- ============================================================

