-- ============================================================
-- Ootils Core — Migration 001: DEPRECATED (SQLite legacy)
-- ============================================================
-- This file was the original SQLite schema (pre-Sprint 1).
-- The engine migrated to PostgreSQL in Sprint 1 (migration 002).
-- This file is intentionally a no-op — kept for historical reference only.
-- All PostgreSQL schema is in migrations 002 and above.
-- ============================================================

-- No-op: migration 002 defines the full PostgreSQL schema.
SELECT 1;

-- ============================================================
-- 0. REFERENCE TABLES (scenario-independent)
-- ============================================================

CREATE TABLE IF NOT EXISTS items (
    item_id      TEXT NOT NULL PRIMARY KEY,
    name         TEXT NOT NULL,
    item_type    TEXT NOT NULL DEFAULT 'finished_good'
                 CHECK (item_type IN ('finished_good','component','raw_material','semi_finished')),
    uom          TEXT NOT NULL DEFAULT 'EA',
    status       TEXT NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active','obsolete','phase_out')),
    attributes   TEXT NOT NULL DEFAULT '{}',   -- JSON: lead_time_days, moq, batch_size, safety_stock_policy_ref
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS locations (
    location_id   TEXT NOT NULL PRIMARY KEY,
    name          TEXT NOT NULL,
    location_type TEXT NOT NULL DEFAULT 'dc'
                  CHECK (location_type IN ('plant','dc','warehouse','supplier_virtual','customer_virtual')),
    country       TEXT,
    timezone      TEXT,
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id       TEXT NOT NULL PRIMARY KEY,
    name              TEXT NOT NULL,
    reliability_score REAL NOT NULL DEFAULT 1.0
                      CHECK (reliability_score BETWEEN 0.0 AND 1.0),
    attributes        TEXT NOT NULL DEFAULT '{}',   -- JSON: lead_time_days, payment_terms, moq
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS policies (
    policy_id         TEXT NOT NULL PRIMARY KEY,
    name              TEXT NOT NULL,
    policy_type       TEXT NOT NULL
                      CHECK (policy_type IN (
                          'safety_stock','allocation_priority','frozen_zone',
                          'moq','sourcing','custom'
                      )),
    -- Scope: NULL = global; set to narrow application
    scope_item_id     TEXT REFERENCES items(item_id),
    scope_location_id TEXT REFERENCES locations(location_id),
    effective_start   TEXT,                          -- ISO date; NULL = always
    effective_end     TEXT,                          -- ISO date; NULL = no end
    parameters        TEXT NOT NULL DEFAULT '{}',   -- JSON: type-specific rule content
    version_no        INTEGER NOT NULL DEFAULT 1,
    active            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- ============================================================
-- 1. SCENARIOS
-- ============================================================

CREATE TABLE IF NOT EXISTS scenarios (
    scenario_id        TEXT NOT NULL PRIMARY KEY,   -- 'baseline' or UUID
    name               TEXT NOT NULL,
    description        TEXT,
    parent_scenario_id TEXT REFERENCES scenarios(scenario_id),  -- NULL = root
    is_baseline        BOOLEAN NOT NULL DEFAULT FALSE,
    status             TEXT NOT NULL DEFAULT 'active'
                       CHECK (status IN ('active','running','archived','failed')),
    created_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- Seed: exactly one baseline scenario
INSERT OR IGNORE INTO scenarios (scenario_id, name, is_baseline, status)
VALUES ('baseline', 'Baseline', TRUE, 'active');

-- ============================================================
-- 2. NODES
-- ============================================================

CREATE TABLE IF NOT EXISTS nodes (
    -- Identity
    node_id      TEXT NOT NULL PRIMARY KEY,         -- UUID v4
    node_type    TEXT NOT NULL,                     -- See node-dictionary.md for valid values:
                                                    -- Item | Location | Resource | Supplier | Policy
                                                    -- ForecastDemand | CustomerOrderDemand
                                                    -- DependentDemand | TransferDemand
                                                    -- OnHandSupply | PurchaseOrderSupply
                                                    -- WorkOrderSupply | TransferSupply | PlannedSupply
                                                    -- CapacityBucket | MaterialConstraint
                                                    -- ProjectedInventory | Shortage
    business_key TEXT NOT NULL,                     -- Source system ref; e.g. "CO-778-LINE-3"
    scenario_id  TEXT NOT NULL REFERENCES scenarios(scenario_id),

    -- Common planning dimensions
    item_id      TEXT REFERENCES items(item_id),
    location_id  TEXT REFERENCES locations(location_id),
    supplier_id  TEXT REFERENCES suppliers(supplier_id),

    -- Quantity
    qty          REAL,
    qty_uom      TEXT,

    -- Time (object-local per ADR-002)
    time_grain      TEXT CHECK (time_grain IN (
                        'exact_datetime','day','week','month','quarter','timeless'
                    )),
    time_ref        TEXT,               -- ISO date / period anchor: "2026-04-15" or "2026-04"
    time_span_start TEXT,               -- ISO date (inclusive)
    time_span_end   TEXT,               -- ISO date (inclusive)

    -- Engine state
    is_dirty        BOOLEAN NOT NULL DEFAULT FALSE, -- Incremental propagation dirty flag
    status          TEXT NOT NULL DEFAULT 'active',
    active          BOOLEAN NOT NULL DEFAULT TRUE,

    -- Type-specific fields (JSON per node-dictionary.md contracts)
    attributes      TEXT NOT NULL DEFAULT '{}',

    -- Audit
    version_no      INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- ============================================================
-- 3. EDGES
-- ============================================================

CREATE TABLE IF NOT EXISTS edges (
    edge_id      TEXT NOT NULL PRIMARY KEY,         -- UUID v4
    edge_type    TEXT NOT NULL,                     -- See edge-dictionary.md:
                                                    -- replenishes | consumes | depends_on
                                                    -- requires_component | produces | uses_capacity
                                                    -- bounded_by | governed_by | transfers_to
                                                    -- originates_from | pegged_to
                                                    -- substitutes_for | prioritized_over | impacts
    from_node_id TEXT NOT NULL REFERENCES nodes(node_id),
    to_node_id   TEXT NOT NULL REFERENCES nodes(node_id),
    scenario_id  TEXT NOT NULL REFERENCES scenarios(scenario_id),

    -- Edge semantics
    priority     INTEGER NOT NULL DEFAULT 0,        -- Lower = higher priority
    weight_ratio REAL    NOT NULL DEFAULT 1.0,      -- Qty ratio, BOM ratio, allocation fraction
    effective_start TEXT,                           -- ISO date; NULL = always
    effective_end   TEXT,                           -- ISO date; NULL = no end

    -- Type-specific
    attributes   TEXT NOT NULL DEFAULT '{}',

    -- State
    active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- ============================================================
-- 4. EVENTS (IMMUTABLE INSERT-ONLY AUDIT LOG)
-- ============================================================

CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT NOT NULL PRIMARY KEY,      -- UUID v4
    event_type      TEXT NOT NULL,                  -- supply_date_changed | demand_qty_changed
                                                    -- onhand_updated | policy_changed
                                                    -- structure_changed | scenario_created
                                                    -- calc_triggered | ingestion_complete
    scenario_id     TEXT NOT NULL REFERENCES scenarios(scenario_id),
    trigger_node_id TEXT REFERENCES nodes(node_id), -- The node that changed; NULL for structural events

    -- Delta payload: {"before": {...}, "after": {...}}
    payload         TEXT NOT NULL DEFAULT '{}',

    -- Provenance
    source          TEXT NOT NULL DEFAULT 'api'
                    CHECK (source IN ('api','ingestion','engine','user','test')),
    user_ref        TEXT,                           -- Optional: who/what triggered this event

    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))

    -- DO NOT ADD: updated_at, deleted_at. Events are immutable.
);

-- ============================================================
-- 5. CALCULATION RUNS
-- ============================================================

CREATE TABLE IF NOT EXISTS calc_runs (
    calc_run_id          TEXT NOT NULL PRIMARY KEY, -- UUID v4
    scenario_id          TEXT NOT NULL REFERENCES scenarios(scenario_id),
    trigger_event_id     TEXT REFERENCES events(event_id),  -- NULL for full recompute
    is_full_recompute    BOOLEAN NOT NULL DEFAULT FALSE,

    -- Scope counters (populated during / after run)
    dirty_node_count     INTEGER,                   -- Nodes dirty at start of run
    nodes_recalculated   INTEGER NOT NULL DEFAULT 0,
    nodes_unchanged      INTEGER NOT NULL DEFAULT 0,-- Early-stop count (delta = 0)

    status               TEXT NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending','running','complete','failed')),
    started_at           TEXT,
    completed_at         TEXT,
    error_message        TEXT,                      -- NULL unless failed

    created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- ============================================================
-- 6. EXPLANATIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS explanations (
    explanation_id      TEXT NOT NULL PRIMARY KEY,  -- UUID v4
    calc_run_id         TEXT NOT NULL REFERENCES calc_runs(calc_run_id),
    target_node_id      TEXT NOT NULL REFERENCES nodes(node_id),
    target_type         TEXT NOT NULL,              -- Shortage | ProjectedInventory
    root_cause_node_id  TEXT REFERENCES nodes(node_id),  -- Terminal in causal chain

    -- Human-readable (see ADR-004 for level definitions)
    summary             TEXT,                       -- 1-line plain English
    detail              TEXT,                       -- Full prose narrative

    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS explanation_steps (
    step_id          TEXT NOT NULL PRIMARY KEY,     -- UUID v4
    explanation_id   TEXT NOT NULL REFERENCES explanations(explanation_id),
    step_order       INTEGER NOT NULL,              -- 1-indexed; ascending = cause → effect
    node_id          TEXT REFERENCES nodes(node_id),-- NULL for policy/rule steps
    node_type        TEXT,                          -- Copied from node at generation time (immutable)
    edge_type        TEXT,                          -- Edge connecting this step to next
    fact             TEXT NOT NULL,                 -- Plain-English causal statement

    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),

    UNIQUE (explanation_id, step_order)
);

-- ============================================================
-- 7. SCENARIO OVERRIDES (DELTA / SIMULATION LAYER)
-- ============================================================

CREATE TABLE IF NOT EXISTS scenario_overrides (
    override_id    TEXT NOT NULL PRIMARY KEY,       -- UUID v4
    scenario_id    TEXT NOT NULL REFERENCES scenarios(scenario_id),
    node_id        TEXT NOT NULL REFERENCES nodes(node_id),  -- Always a baseline node

    override_type  TEXT NOT NULL                    -- qty | date | status | attribute | full_replace
                   CHECK (override_type IN ('qty','date','status','attribute','full_replace')),

    -- JSON: {"field": "due_date", "value": "2026-04-18"}  for field overrides
    --       full attributes blob                           for full_replace
    override_value TEXT NOT NULL,

    rationale      TEXT,                            -- Why this override exists
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),

    UNIQUE (scenario_id, node_id, override_type)
);

-- ============================================================
-- INDEXES
-- ============================================================

-- nodes: primary engine access patterns
CREATE INDEX IF NOT EXISTS idx_nodes_scenario_type
    ON nodes (scenario_id, node_type) WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_nodes_item_location_scenario
    ON nodes (item_id, location_id, scenario_id) WHERE active = TRUE;

-- Temporal Bridge: time-window scans (most frequent read pattern)
CREATE INDEX IF NOT EXISTS idx_nodes_time_window
    ON nodes (scenario_id, item_id, location_id, time_span_start, time_span_end)
    WHERE active = TRUE;

-- Incremental propagation: dirty flag flush
CREATE INDEX IF NOT EXISTS idx_nodes_dirty
    ON nodes (scenario_id, is_dirty) WHERE is_dirty = TRUE;

-- Business key lookup (ingest deduplication, API queries)
CREATE INDEX IF NOT EXISTS idx_nodes_business_key
    ON nodes (business_key);

-- edges: bidirectional traversal
CREATE INDEX IF NOT EXISTS idx_edges_from
    ON edges (from_node_id, edge_type) WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_edges_to
    ON edges (to_node_id, edge_type) WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_edges_scenario_type
    ON edges (scenario_id, edge_type) WHERE active = TRUE;

-- events: chronological + by trigger node
CREATE INDEX IF NOT EXISTS idx_events_scenario_type_time
    ON events (scenario_id, event_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_trigger_node
    ON events (trigger_node_id, created_at DESC);

-- explanations: lookup by result node
CREATE INDEX IF NOT EXISTS idx_explanations_target
    ON explanations (target_node_id, calc_run_id);

-- explanation steps: ordered step fetch
CREATE INDEX IF NOT EXISTS idx_explanation_steps_explanation
    ON explanation_steps (explanation_id, step_order);

-- scenario overrides: engine resolution
CREATE INDEX IF NOT EXISTS idx_overrides_scenario_node
    ON scenario_overrides (scenario_id, node_id) WHERE active = TRUE;

-- calc_runs: status checks
CREATE INDEX IF NOT EXISTS idx_calc_runs_scenario_status
    ON calc_runs (scenario_id, status, created_at DESC);
