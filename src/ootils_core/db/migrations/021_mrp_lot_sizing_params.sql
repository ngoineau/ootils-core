-- Migration 021: Add Phase 0 MRP lot sizing / consumption params
-- Compatible with fresh databases and current mrp_* schema used by the engine.

ALTER TYPE lot_size_rule_type ADD VALUE IF NOT EXISTS 'POQ';
ALTER TYPE lot_size_rule_type ADD VALUE IF NOT EXISTS 'EOQ';
ALTER TYPE lot_size_rule_type ADD VALUE IF NOT EXISTS 'MULTIPLE';
ALTER TYPE lot_size_rule_type ADD VALUE IF NOT EXISTS 'FIXED_QTY';

ALTER TABLE item_planning_params
    ADD COLUMN IF NOT EXISTS economic_order_qty NUMERIC(18,6) CHECK (economic_order_qty > 0),
    ADD COLUMN IF NOT EXISTS lot_size_poq_periods INTEGER DEFAULT 1 CHECK (lot_size_poq_periods > 0),
    ADD COLUMN IF NOT EXISTS order_multiple_qty NUMERIC(18,6) CHECK (order_multiple_qty > 0),
    ADD COLUMN IF NOT EXISTS frozen_time_fence_days INTEGER DEFAULT 7 CHECK (frozen_time_fence_days >= 0),
    ADD COLUMN IF NOT EXISTS slashed_time_fence_days INTEGER DEFAULT 30 CHECK (slashed_time_fence_days > 0),
    ADD COLUMN IF NOT EXISTS forecast_consumption_strategy TEXT DEFAULT 'max_only',
    ADD COLUMN IF NOT EXISTS consumption_window_days INTEGER DEFAULT 7 CHECK (consumption_window_days > 0),
    ADD COLUMN IF NOT EXISTS reorder_point_qty NUMERIC(18,6) CHECK (reorder_point_qty >= 0);

CREATE TABLE IF NOT EXISTS mrp_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id UUID NOT NULL,
    location_id UUID,
    run_type TEXT NOT NULL DEFAULT 'APICS_FULL',
    status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    horizon_days INTEGER NOT NULL DEFAULT 90 CHECK (horizon_days > 0 AND horizon_days <= 365),
    bucket_type TEXT NOT NULL DEFAULT 'WEEK' CHECK (bucket_type IN ('DAY', 'WEEK', 'MONTH')),
    llc_regeneration BOOLEAN NOT NULL DEFAULT false,
    forecast_consumption_applied BOOLEAN NOT NULL DEFAULT false,
    time_fence_enforced BOOLEAN NOT NULL DEFAULT false,
    items_processed INTEGER NOT NULL DEFAULT 0,
    planned_orders_created INTEGER NOT NULL DEFAULT 0,
    planned_orders_modified INTEGER NOT NULL DEFAULT 0,
    planned_orders_cancelled INTEGER NOT NULL DEFAULT 0,
    shortages_detected INTEGER NOT NULL DEFAULT 0,
    execution_time_ms INTEGER,
    errors JSONB DEFAULT '[]'::jsonb,
    warnings JSONB DEFAULT '[]'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by TEXT
);

ALTER TABLE mrp_runs
    ADD COLUMN IF NOT EXISTS status TEXT,
    ADD COLUMN IF NOT EXISTS bucket_type TEXT,
    ADD COLUMN IF NOT EXISTS llc_regeneration BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS forecast_consumption_applied BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS time_fence_enforced BOOLEAN DEFAULT false,
    ADD COLUMN IF NOT EXISTS items_processed INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS planned_orders_created INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS planned_orders_modified INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS planned_orders_cancelled INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS shortages_detected INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS execution_time_ms INTEGER,
    ADD COLUMN IF NOT EXISTS errors JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS warnings JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS created_by TEXT;

ALTER TABLE mrp_runs ALTER COLUMN status SET DEFAULT 'running';
ALTER TABLE mrp_runs ALTER COLUMN bucket_type SET DEFAULT 'WEEK';

CREATE TABLE IF NOT EXISTS mrp_bucket_records (
    bucket_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES mrp_runs(run_id) ON DELETE CASCADE,
    item_id UUID NOT NULL,
    location_id UUID NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    bucket_sequence INTEGER NOT NULL,
    gross_requirements NUMERIC(18,6) NOT NULL DEFAULT 0,
    scheduled_receipts NUMERIC(18,6) NOT NULL DEFAULT 0,
    projected_on_hand NUMERIC(18,6) NOT NULL DEFAULT 0,
    net_requirements NUMERIC(18,6) NOT NULL DEFAULT 0,
    planned_order_receipts NUMERIC(18,6) NOT NULL DEFAULT 0,
    planned_order_releases NUMERIC(18,6) NOT NULL DEFAULT 0,
    has_shortage BOOLEAN NOT NULL DEFAULT false,
    shortage_qty NUMERIC(18,6) NOT NULL DEFAULT 0,
    llc INTEGER NOT NULL DEFAULT 0,
    time_fence_zone TEXT,
    lot_size_rule_applied TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mrp_action_messages (
    message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES mrp_runs(run_id) ON DELETE CASCADE,
    item_id UUID NOT NULL,
    location_id UUID NOT NULL,
    node_id UUID,
    message_type TEXT NOT NULL CHECK (message_type IN ('EXPEDITE', 'DEFER', 'CANCEL', 'RELEASE', 'RESCHEDULE')),
    priority TEXT NOT NULL CHECK (priority IN ('HIGH', 'MEDIUM', 'LOW')),
    description TEXT NOT NULL,
    reference_date DATE,
    proposed_date DATE,
    current_qty NUMERIC(18,6),
    proposed_qty NUMERIC(18,6),
    status TEXT NOT NULL DEFAULT 'NEW' CHECK (status IN ('NEW', 'REVIEWED', 'ACCEPTED', 'REJECTED', 'APPLIED')),
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    rejection_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE mrp_action_messages
    ADD COLUMN IF NOT EXISTS node_id UUID,
    ADD COLUMN IF NOT EXISTS priority TEXT,
    ADD COLUMN IF NOT EXISTS description TEXT,
    ADD COLUMN IF NOT EXISTS reference_date DATE,
    ADD COLUMN IF NOT EXISTS proposed_date DATE,
    ADD COLUMN IF NOT EXISTS current_qty NUMERIC(18,6),
    ADD COLUMN IF NOT EXISTS proposed_qty NUMERIC(18,6),
    ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'NEW',
    ADD COLUMN IF NOT EXISTS reviewed_by TEXT,
    ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rejection_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_mrp_runs_scenario ON mrp_runs (scenario_id);
CREATE INDEX IF NOT EXISTS idx_mrp_runs_scenario_date ON mrp_runs (scenario_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mrp_runs_status ON mrp_runs (status) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_mrp_bucket_records_run ON mrp_bucket_records (run_id);
CREATE INDEX IF NOT EXISTS idx_mrp_bucket_records_item ON mrp_bucket_records (item_id, period_start);
CREATE INDEX IF NOT EXISTS idx_mrp_action_messages_run ON mrp_action_messages (run_id);
CREATE INDEX IF NOT EXISTS idx_mrp_messages_run ON mrp_action_messages (run_id, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_mrp_messages_item ON mrp_action_messages (item_id, location_id, status) WHERE status = 'NEW';
