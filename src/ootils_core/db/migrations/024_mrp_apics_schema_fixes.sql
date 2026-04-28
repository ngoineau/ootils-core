-- ============================================================
-- Ootils Core — Migration 024: MRP APICS Schema Fixes
-- Adds missing tables/columns for APICS MRP engine
-- ============================================================

-- 1. Add llc column to items table (required by LLC calculator)
ALTER TABLE items
ADD COLUMN IF NOT EXISTS llc INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_items_llc ON items (llc);

-- 2. Add MRP-specific columns to nodes table (required by graph_integration.py)
ALTER TABLE nodes
ADD COLUMN IF NOT EXISTS mrp_run_id UUID,
ADD COLUMN IF NOT EXISTS planned_order_type TEXT
    CHECK (planned_order_type IS NULL OR planned_order_type IN ('RECEIPT', 'RELEASE')),
ADD COLUMN IF NOT EXISTS parent_node_id UUID REFERENCES nodes(node_id);

CREATE INDEX IF NOT EXISTS idx_nodes_mrp_run ON nodes (mrp_run_id) WHERE mrp_run_id IS NOT NULL;

-- 3. Drop existing forecast_consumption_log if it has wrong schema
-- (Created by earlier dev work with incorrect columns)
DROP TABLE IF EXISTS forecast_consumption_log CASCADE;

-- 3. Create forecast_consumption_log table with correct schema (required by forecast consumer)
CREATE TABLE forecast_consumption_log (
    log_id              UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id              UUID,
    item_id             UUID        NOT NULL REFERENCES items(item_id),
    location_id         UUID        NOT NULL REFERENCES locations(location_id),
    
    -- Period boundaries
    period_start        DATE        NOT NULL,
    period_end          DATE        NOT NULL,
    
    -- Forecast and demand values
    original_forecast   NUMERIC     NOT NULL DEFAULT 0,
    customer_orders     NUMERIC     NOT NULL DEFAULT 0,
    consumed_qty        NUMERIC     NOT NULL DEFAULT 0,
    remaining_forecast  NUMERIC     NOT NULL DEFAULT 0,
    
    -- Carry adjustments (for PRIORITY strategy)
    carry_forward       NUMERIC     NOT NULL DEFAULT 0,
    carry_backward      NUMERIC     NOT NULL DEFAULT 0,
    
    -- Strategy used
    strategy            TEXT        NOT NULL
                        CHECK (strategy IN ('max_only', 'consume_forward', 'consume_backward', 'priority')),
    
    -- Computed net demand
    net_demand          NUMERIC     NOT NULL DEFAULT 0,
    
    -- Audit
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_forecast_consumption_log_item_location 
    ON forecast_consumption_log (item_id, location_id);

CREATE INDEX IF NOT EXISTS idx_forecast_consumption_log_run 
    ON forecast_consumption_log (run_id);

CREATE INDEX IF NOT EXISTS idx_forecast_consumption_log_period 
    ON forecast_consumption_log (period_start, period_end);

COMMENT ON TABLE forecast_consumption_log IS 'Logs forecast consumption results from APICS MRP runs';
COMMENT ON COLUMN forecast_consumption_log.strategy IS 'Consumption strategy: max_only, consume_forward, consume_backward, priority';
