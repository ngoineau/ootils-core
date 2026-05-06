-- ============================================================
-- Ootils Core — Migration 027: MPS (Master Production Schedule)
-- MPS-001: MPS Node Model and Time Buckets
-- ============================================================
-- Creates tables for MPS consolidation of demand for finished goods.
-- Bridges demand forecasting to MRP supply planning.
-- ============================================================

-- ============================================================
-- 1. Add MPS status enum type
-- ============================================================

DO $$ BEGIN
    CREATE TYPE mps_status AS ENUM ('DRAFT', 'REVIEWED', 'APPROVED', 'RELEASED');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;


-- ============================================================
-- 2. Table mps_nodes — Master Production Schedule nodes
-- ============================================================

CREATE TABLE IF NOT EXISTS mps_nodes (
    mps_id              UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id             UUID        NOT NULL REFERENCES items(item_id),
    location_id         UUID        NOT NULL REFERENCES locations(location_id),
    scenario_id         UUID        NOT NULL REFERENCES scenarios(scenario_id),
    time_bucket         TEXT        NOT NULL,
    time_bucket_start   DATE        NOT NULL,
    time_bucket_end     DATE        NOT NULL,
    time_grain          TEXT        NOT NULL CHECK (time_grain IN ('daily', 'weekly', 'monthly')),
    
    -- Source demand quantities
    forecast_quantity   NUMERIC(18,6) NOT NULL DEFAULT 0,
    sales_orders_quantity NUMERIC(18,6) NOT NULL DEFAULT 0,
    total_demand        NUMERIC(18,6) NOT NULL DEFAULT 0,
    
    -- Planning output
    planned_quantity    NUMERIC(18,6) NOT NULL DEFAULT 0,
    status              mps_status  NOT NULL DEFAULT 'DRAFT',
    
    -- Audit trail
    created_by          TEXT,
    reviewed_by         TEXT,
    approved_by         TEXT,
    released_by         TEXT,
    reviewed_at         TIMESTAMPTZ,
    approved_at         TIMESTAMPTZ,
    released_at         TIMESTAMPTZ,
    
    -- Metadata
    notes               TEXT,
    active              BOOLEAN     NOT NULL DEFAULT true,
    
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    -- Constraints
    CONSTRAINT chk_mps_bucket_dates CHECK (time_bucket_end >= time_bucket_start),
    CONSTRAINT chk_mps_quantities_positive CHECK (
        forecast_quantity >= 0 AND 
        sales_orders_quantity >= 0 AND 
        total_demand >= 0 AND 
        planned_quantity >= 0
    ),
    CONSTRAINT chk_mps_total_demand CHECK (
        total_demand = forecast_quantity + sales_orders_quantity
    )
);

-- Unique constraint: one active MPS node per (item, location, time_bucket, scenario)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mps_nodes_unique_active 
    ON mps_nodes (item_id, location_id, scenario_id, time_bucket) 
    WHERE active = true;

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_mps_nodes_item_location ON mps_nodes (item_id, location_id);
CREATE INDEX IF NOT EXISTS idx_mps_nodes_scenario ON mps_nodes (scenario_id);
CREATE INDEX IF NOT EXISTS idx_mps_nodes_time_bucket ON mps_nodes (time_bucket_start, time_bucket_end);
CREATE INDEX IF NOT EXISTS idx_mps_nodes_status ON mps_nodes (status);
CREATE INDEX IF NOT EXISTS idx_mps_nodes_item_scenario_bucket ON mps_nodes (item_id, scenario_id, time_bucket);


-- ============================================================
-- 3. Table mps_planned_for_edges — MPSNode to Item relationship
-- ============================================================

CREATE TABLE IF NOT EXISTS mps_planned_for_edges (
    edge_id         UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    mps_node_id     UUID        NOT NULL REFERENCES mps_nodes(mps_id) ON DELETE CASCADE,
    item_id         UUID        NOT NULL REFERENCES items(item_id),
    scenario_id     UUID        NOT NULL REFERENCES scenarios(scenario_id),
    active          BOOLEAN     NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mps_planned_for_mps_node ON mps_planned_for_edges (mps_node_id);
CREATE INDEX IF NOT EXISTS idx_mps_planned_for_item ON mps_planned_for_edges (item_id);
CREATE INDEX IF NOT EXISTS idx_mps_planned_for_scenario ON mps_planned_for_edges (scenario_id);


-- ============================================================
-- 4. Table mps_supplies_edges — MPSNode to PlannedSupply relationship
-- ============================================================

CREATE TABLE IF NOT EXISTS mps_supplies_edges (
    edge_id                 UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    mps_node_id             UUID        NOT NULL REFERENCES mps_nodes(mps_id) ON DELETE CASCADE,
    planned_supply_node_id  UUID        NOT NULL,  -- Will reference planned_supply nodes when MRP is implemented
    scenario_id             UUID        NOT NULL REFERENCES scenarios(scenario_id),
    quantity_pegged         NUMERIC(18,6) NOT NULL DEFAULT 0,
    active                  BOOLEAN     NOT NULL DEFAULT true,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    CONSTRAINT chk_mps_supplies_quantity_positive CHECK (quantity_pegged >= 0)
);

CREATE INDEX IF NOT EXISTS idx_mps_supplies_mps_node ON mps_supplies_edges (mps_node_id);
CREATE INDEX IF NOT EXISTS idx_mps_supplies_planned_supply ON mps_supplies_edges (planned_supply_node_id);
CREATE INDEX IF NOT EXISTS idx_mps_supplies_scenario ON mps_supplies_edges (scenario_id);


-- ============================================================
-- 5. Add 'mps_planned_for' and 'mps_supplies' edge types to edges.edge_type
-- ============================================================

DO $$
DECLARE
    v_constraint_def TEXT;
BEGIN
    SELECT pg_get_constraintdef(c.oid)
    INTO v_constraint_def
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE c.contype = 'c'
      AND c.conname LIKE '%edge_type%'
      AND t.relname = 'edges'
      AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
    LIMIT 1;

    IF v_constraint_def IS NOT NULL AND v_constraint_def NOT LIKE '%mps_planned_for%' THEN
        ALTER TABLE edges DROP CONSTRAINT IF EXISTS edges_edge_type_check;
        ALTER TABLE edges ADD CONSTRAINT edges_edge_type_check CHECK (
            edge_type IN (
                'replenishes', 'feeds_forward', 'consumes', 'depends_on',
                'transfers_to', 'pegged_to', 'governed_by',
                'transfers', 'requires', 'substitutes',
                'fulfills', 'produces', 'ghost_member',
                'bom_component', 'consumes_resource',
                'forecasted_demand_for',
                'mps_planned_for', 'mps_supplies'
            )
        );
    ELSIF v_constraint_def IS NULL THEN
        ALTER TABLE edges ADD CONSTRAINT edges_edge_type_check CHECK (
            edge_type IN (
                'replenishes', 'feeds_forward', 'consumes', 'depends_on',
                'transfers_to', 'pegged_to', 'governed_by',
                'transfers', 'requires', 'substitutes',
                'fulfills', 'produces', 'ghost_member',
                'bom_component', 'consumes_resource',
                'forecasted_demand_for',
                'mps_planned_for', 'mps_supplies'
            )
        );
    END IF;
END $$;


-- ============================================================
-- 6. Add updated_at trigger for mps_nodes table
-- ============================================================

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'mps_nodes') THEN
        DROP TRIGGER IF EXISTS trg_mps_nodes_updated_at ON mps_nodes;
        CREATE TRIGGER trg_mps_nodes_updated_at
            BEFORE UPDATE ON mps_nodes
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;


-- ============================================================
-- 7. Function to validate MPS node uniqueness before insert
-- ============================================================

CREATE OR REPLACE FUNCTION validate_mps_node_uniqueness()
RETURNS TRIGGER AS $$
DECLARE
    v_conflict_count INTEGER;
BEGIN
    -- Check for existing active MPS node with same (item, location, scenario, time_bucket)
    SELECT COUNT(*) INTO v_conflict_count
    FROM mps_nodes
    WHERE item_id = NEW.item_id
      AND location_id = NEW.location_id
      AND scenario_id = NEW.scenario_id
      AND time_bucket = NEW.time_bucket
      AND active = true
      AND mps_id != COALESCE(NEW.mps_id, '00000000-0000-0000-0000-000000000000'::uuid);
    
    IF v_conflict_count > 0 THEN
        RAISE EXCEPTION 'Duplicate MPS node: active node already exists for (item=%, location=%, scenario=%, time_bucket=%)',
            NEW.item_id, NEW.location_id, NEW.scenario_id, NEW.time_bucket;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop and recreate trigger to ensure it's up to date
DROP TRIGGER IF EXISTS trg_mps_node_uniqueness ON mps_nodes;
CREATE TRIGGER trg_mps_node_uniqueness
    BEFORE INSERT OR UPDATE ON mps_nodes
    FOR EACH ROW EXECUTE FUNCTION validate_mps_node_uniqueness();


-- ============================================================
-- Comments
-- ============================================================

COMMENT ON TABLE mps_nodes IS 'Master Production Schedule nodes consolidating demand for finished goods';
COMMENT ON COLUMN mps_nodes.time_bucket IS 'Time period identifier (e.g., 2026-W15 for weekly buckets)';
COMMENT ON COLUMN mps_nodes.time_grain IS 'Granularity: daily, weekly, or monthly';
COMMENT ON COLUMN mps_nodes.forecast_quantity IS 'Quantity from statistical forecast';
COMMENT ON COLUMN mps_nodes.sales_orders_quantity IS 'Quantity from confirmed sales orders';
COMMENT ON COLUMN mps_nodes.total_demand IS 'Sum of forecast and sales orders (auto-computed)';
COMMENT ON COLUMN mps_nodes.planned_quantity IS 'Approved production/distribution quantity';
COMMENT ON COLUMN mps_nodes.status IS 'Workflow status: DRAFT -> REVIEWED -> APPROVED -> RELEASED';

COMMENT ON TABLE mps_planned_for_edges IS 'Edges linking MPS nodes to their planned items';
COMMENT ON TABLE mps_supplies_edges IS 'Edges linking MPS nodes to MRP planned supplies (pegging)';

COMMENT ON FUNCTION validate_mps_node_uniqueness() IS 'Ensures one active MPS node per (item, location, scenario, time_bucket)';
