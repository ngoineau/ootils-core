-- ============================================================
-- Migration 047 — Demand module foundation (Pyramide — D1)
-- ============================================================
-- Builds the three-layer demand master needed before any Pyramide
-- forecast run can produce reconcilable, hierarchy-aware output.
--
-- Why one migration?  All four DDL blocks form a single referential
-- closure: demand_history.item_id → items, item_hierarchy →
-- hierarchy_node → hierarchy.  A partial apply would leave a schema
-- that either has orphaned FKs or a half-usable demand_history table.
-- Rolling back is clean because every object is new (no mutation of
-- existing production tables, except the additive item_cost column).
--
-- Business context
-- ----------------
-- Ootils tracks BOOKING events, not shipments, as the demand signal
-- (rule locked in memory/demand-business-rule-booking.md).  The pool
-- business has three streams: regular (booked → forecast driver),
-- warranty (after-sales, separate forecast), and interco/dropship
-- (excluded from external ASP).  Per-DC granularity in demand_history
-- drives DRP site-level netting; the central MRP consumes the rolled-up
-- signal (ADR-020).
--
-- Objects created
-- ---------------
--   hierarchy           — registry of named hierarchies (product-local,
--                         product-corporate, region-climate, channel …)
--   hierarchy_node      — nodes (any depth) within a hierarchy
--   item_hierarchy      — leaf membership: which node each item sits in
--                         for each hierarchy
--   items.item_cost     — at-cost basis column (CDS, source T_DTK.CDS);
--                         distinct from the BOM-rollup items.standard_cost
--                         added in migration 042
--   demand_history      — granular booking/shipping facts after
--                         classification and sign-rule application
--
-- No JSONB used.  All columns are typed; CHECK constraints are applied
-- on the enum-like columns following the project convention
-- (cf. migrations 017, 044, 045).
-- ============================================================

BEGIN;

-- ============================================================
-- 1. HIERARCHY REGISTRY
-- ============================================================
-- Supports multiple coexisting hierarchies for the same domain
-- (e.g. product/local from the ERP, product/corporate from Oracle BI).
-- is_default marks the hierarchy used by default for Pyramide
-- reconciliation within its domain (should be the local one per
-- the Pyramide design: reconciliation drives down to the ERP leaf).
-- ============================================================

CREATE TABLE IF NOT EXISTS hierarchy (
    hierarchy_id  TEXT        NOT NULL PRIMARY KEY,
    -- Expected: 'product' | 'region' | 'channel' | 'customer'
    -- Left permissive (TEXT + CHECK) so new domains can be added via
    -- migration without an ALTER TABLE on the enum type.
    domain        TEXT        NOT NULL
                  CHECK (domain IN ('product', 'region', 'channel', 'customer')),
    -- Expected: 'local' | 'corporate'
    scope         TEXT        NOT NULL
                  CHECK (scope IN ('local', 'corporate')),
    label         TEXT,
    -- Ordered level names, e.g. local product: '{family,group,product}'
    -- corporate product: '{sector,solution,category,family,line}'
    levels        TEXT[]      NOT NULL,
    -- TRUE for the hierarchy used by default for forecast/reconciliation
    -- in this domain.  Enforced at application layer; DB allows multiple
    -- (no unique constraint) to avoid a partial-unique maintenance trap
    -- during hierarchy migrations.
    is_default    BOOLEAN     NOT NULL DEFAULT false,
    notes         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE hierarchy IS
    'Registry of named item/region/channel hierarchies. Multiple hierarchies '
    'can coexist for the same domain (local ERP vs corporate BI). The '
    'is_default flag identifies the one used for Pyramide reconciliation.';

COMMENT ON COLUMN hierarchy.levels IS
    'Ordered level names from root to leaf, e.g. ''{family,group,product}'' '
    'for the local product hierarchy.';

COMMENT ON COLUMN hierarchy.is_default IS
    'Marks the default reconciliation hierarchy for this domain. Multiple '
    'rows may have is_default=true (e.g. one per domain); enforced at the '
    'application layer.';

-- ============================================================
-- 2. HIERARCHY NODES
-- ============================================================
-- Stores every node at every level of a hierarchy.
-- parent_code is NULL at the root.  We deliberately avoid a strict
-- self-referential FK on (hierarchy_id, parent_code) to keep bulk
-- inserts order-independent during ERP sync — the application layer
-- validates parent existence.
-- ============================================================

CREATE TABLE IF NOT EXISTS hierarchy_node (
    hierarchy_id  TEXT        NOT NULL REFERENCES hierarchy(hierarchy_id) ON DELETE RESTRICT,
    code          TEXT        NOT NULL,
    level         TEXT        NOT NULL,
    description   TEXT,
    -- NULL at root; no self-referential FK (see comment above)
    parent_code   TEXT,

    PRIMARY KEY (hierarchy_id, code)
);

COMMENT ON TABLE hierarchy_node IS
    'Nodes (at any depth) within a named hierarchy. parent_code is NULL '
    'at the root. No self-referential FK: bulk ERP syncs insert children '
    'before parents in some extract orderings.';

COMMENT ON COLUMN hierarchy_node.parent_code IS
    'Code of the parent node within the same hierarchy. NULL = root. '
    'No DB FK constraint (insert-order-independent); validated at the '
    'application layer.';

-- Index to navigate upward/sideways: "give me all children of node X"
CREATE INDEX IF NOT EXISTS idx_hierarchy_node_parent
    ON hierarchy_node (hierarchy_id, parent_code);

-- Index on level to support "all nodes at level family in hierarchy X"
CREATE INDEX IF NOT EXISTS idx_hierarchy_node_level
    ON hierarchy_node (hierarchy_id, level);

-- ============================================================
-- 3. ITEM → HIERARCHY LEAF MEMBERSHIP
-- ============================================================
-- One row per (item, hierarchy): the leaf node to which this item
-- belongs.  An item may belong to several hierarchies simultaneously
-- (product-local, product-corporate, region-climate, channel …).
--
-- FK on (hierarchy_id, leaf_code) enforces that the leaf actually
-- exists in the hierarchy.  ON DELETE RESTRICT on items: an item
-- referenced in demand history must not vanish silently.
-- ============================================================

CREATE TABLE IF NOT EXISTS item_hierarchy (
    item_id       UUID        NOT NULL REFERENCES items(item_id) ON DELETE RESTRICT,
    hierarchy_id  TEXT        NOT NULL,
    leaf_code     TEXT        NOT NULL,

    PRIMARY KEY (item_id, hierarchy_id),
    FOREIGN KEY (hierarchy_id, leaf_code)
        REFERENCES hierarchy_node (hierarchy_id, code)
        ON DELETE RESTRICT
);

COMMENT ON TABLE item_hierarchy IS
    'Leaf membership of each item in each hierarchy. One row per '
    '(item, hierarchy). Supports multiple simultaneous hierarchies '
    '(local product, corporate product, channel …).';

-- Supports "which items sit under leaf X / hierarchy Y" (Pyramide
-- top-down allocation and middle-out reconciliation)
CREATE INDEX IF NOT EXISTS idx_item_hierarchy_leaf
    ON item_hierarchy (hierarchy_id, leaf_code);

-- ============================================================
-- 4. ITEM AT-COST BASIS (CDS)
-- ============================================================
-- item_cost is the SINGLE at-cost value used for ALL demand-module
-- calculations (ASP, E&O valuation, demand-value aggregation).
-- Business name: CDS.  Source: T_DTK.CDS in the ERP.
--
-- This is DISTINCT from items.standard_cost (migration 042), which
-- is the BOM-rollup / supplier-fallback used by the supply/MRP side.
-- The two columns coexist intentionally: demand-side valuation and
-- supply-side costing differ in scope and update cadence.
-- ============================================================

ALTER TABLE items ADD COLUMN IF NOT EXISTS item_cost NUMERIC;

COMMENT ON COLUMN items.item_cost IS
    'Item at-cost basis — single cost used for demand-module calculations '
    '(ASP, E&O valuation, demand-value aggregation). Business name: CDS. '
    'Source: T_DTK.CDS. Distinct from items.standard_cost (migration 042, '
    'BOM roll-up / supply-side costing).';

-- ============================================================
-- 5. DEMAND HISTORY
-- ============================================================
-- Granular booking and shipping facts, loaded from SALES_MARINE
-- (or equivalent ERP extract) after classification and sign-rule
-- application (positives only at ingestion time; returns are a
-- separate stream at a higher layer).
--
-- Design notes:
-- - item_id is NULLABLE: a row whose item code has not yet been
--   resolved to an items.item_id may still be stored for audit and
--   deferred resolution (item_code preserves the source key).
-- - booked_date drives the forecast signal (forecast-on-booking rule).
--   shipment_date is stored for shipping-series analytics only.
-- - warehouse_id (TEXT, not FK to locations) stores the DC identifier
--   as it arrives from the ERP.  Resolution to a locations.location_id
--   happens at the DRP layer, not at ingestion.
-- - value_ext = 0 for flows that are excluded from ASP computation
--   (warranty, interco, dropship); counts_for_asp is the explicit flag.
-- - fulfillment drives DRP routing: 'standard' = via DC, 'direct' =
--   customer-direct, 'inter_entity' = interco flow.
-- - bigserial PK: demand_history is an append-only fact table; UUID
--   PKs add 8 bytes per row overhead for no join benefit here.
-- ============================================================

CREATE TABLE IF NOT EXISTS demand_history (
    id                BIGSERIAL   NOT NULL PRIMARY KEY,

    -- Item resolution
    -- item_id is nullable: unresolved codes land here for deferred linking
    item_id           UUID        REFERENCES items(item_id) ON DELETE RESTRICT,
    item_code         TEXT        NOT NULL,

    -- Demand stream
    -- 'regular': standard customer bookings (main forecast driver)
    -- 'warranty': after-sales / warranty replacements (separate forecast)
    stream            TEXT        NOT NULL
                      CHECK (stream IN ('regular', 'warranty')),

    -- Temporal axes
    booked_date       DATE,       -- booking date — primary forecast signal
    shipment_date     DATE,       -- actual or promised ship date

    -- Quantities
    ordered_quantity  NUMERIC,    -- booked demand; positive only at ingestion
    fulfilled_quantity NUMERIC,   -- shipped qty (shipping series)

    -- Value / ASP
    value_ext         NUMERIC,    -- extended value ($); 0 if excluded from ASP
    counts_for_asp    BOOLEAN     NOT NULL DEFAULT false,

    -- Geography
    ship_state        TEXT,
    ship_country      TEXT,

    -- Distribution routing
    -- warehouse_id: DC identifier from the ERP; resolved to
    -- locations.location_id at the DRP layer, not at ingestion.
    warehouse_id      TEXT,

    -- Sales channel
    channel           TEXT,

    -- Fulfillment routing (drives DRP netting path)
    -- 'standard'     = fulfilled via DC stock
    -- 'direct'       = customer-direct (bypasses DC)
    -- 'inter_entity' = intercompany / interco flow
    fulfillment       TEXT
                      CHECK (fulfillment IN ('standard', 'direct', 'inter_entity')),

    -- ERP traceability
    order_number      TEXT,
    line_id           TEXT,

    -- Audit
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE demand_history IS
    'Granular demand facts (bookings + shipments) loaded from SALES_MARINE '
    'after classification and sign-rule application. Append-only at '
    'ingestion; returns/corrections are a separate adjustment stream. '
    'Primary forecast signal is booked_date (forecast-on-booking rule).';

COMMENT ON COLUMN demand_history.item_id IS
    'Resolved items FK. NULLABLE: unresolved item codes land here for '
    'deferred resolution. item_code always preserves the source ERP key.';

COMMENT ON COLUMN demand_history.value_ext IS
    'Extended booking value ($). Set to 0 for flows excluded from ASP '
    '(warranty, interco, dropship). See counts_for_asp.';

COMMENT ON COLUMN demand_history.warehouse_id IS
    'DC identifier as received from the ERP extract (text key, not a FK). '
    'Resolution to locations.location_id is done at the DRP layer.';

COMMENT ON COLUMN demand_history.fulfillment IS
    'DRP routing: standard = via DC, direct = customer-direct (bypasses DC '
    'stock), inter_entity = intercompany flow excluded from external demand.';

-- Hot-path: time-series aggregation by item (Pyramide bottom-up,
-- DRP per-item netting, E&O coverage calculation)
CREATE INDEX IF NOT EXISTS idx_demand_history_item_booked
    ON demand_history (item_id, booked_date);

-- Hot-path: per-DC demand aggregation for DRP site-level netting
CREATE INDEX IF NOT EXISTS idx_demand_history_warehouse_booked
    ON demand_history (warehouse_id, booked_date);

-- Geographic aggregation (region-climate hierarchy, ASP by state)
CREATE INDEX IF NOT EXISTS idx_demand_history_state_booked
    ON demand_history (ship_state, booked_date);

-- Stream filter: most analytical queries slice by stream first
CREATE INDEX IF NOT EXISTS idx_demand_history_stream
    ON demand_history (stream, booked_date);

COMMIT;
