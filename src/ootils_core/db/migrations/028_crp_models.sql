-- ============================================================
-- Ootils Core — Migration 028: CRP (Capacity Requirements Planning)
-- CRP-001: Work Center & Routing Model Extension
-- ============================================================
-- Creates tables for detailed capacity planning at work center level.
-- Supports routing definitions with operation-level capacity tracking.
-- ============================================================

-- ============================================================
-- 1. Table work_centers — Production resources
-- ============================================================

CREATE TABLE IF NOT EXISTS work_centers (
    work_center_id    UUID            NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    code              TEXT            NOT NULL,
    description       TEXT,
    capacity_per_day  NUMERIC(18,6)   NOT NULL DEFAULT 0,
    efficiency        NUMERIC(5,4)    NOT NULL DEFAULT 1.0,
    calendar_id       UUID,
    active            BOOLEAN         NOT NULL DEFAULT true,
    
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    
    -- Constraints
    CONSTRAINT chk_work_center_code_unique UNIQUE (code),
    CONSTRAINT chk_work_center_efficiency CHECK (efficiency >= 0 AND efficiency <= 1),
    CONSTRAINT chk_work_center_capacity_positive CHECK (capacity_per_day >= 0)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_work_centers_code ON work_centers (code);
CREATE INDEX IF NOT EXISTS idx_work_centers_active ON work_centers (active);
CREATE INDEX IF NOT EXISTS idx_work_centers_calendar ON work_centers (calendar_id);


-- ============================================================
-- 2. Table routings — Manufacturing process definitions
-- ============================================================

CREATE TABLE IF NOT EXISTS routings (
    routing_id      UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id         UUID        NOT NULL REFERENCES items(item_id),
    sequence        INTEGER     NOT NULL DEFAULT 1,
    description     TEXT,
    active          BOOLEAN     NOT NULL DEFAULT true,
    
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    -- Unique constraint: one active routing per (item_id, sequence)
    CONSTRAINT unique_routing_per_item_sequence UNIQUE (item_id, sequence)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_routings_item ON routings (item_id);
CREATE INDEX IF NOT EXISTS idx_routings_active ON routings (active);
CREATE INDEX IF NOT EXISTS idx_routings_item_active ON routings (item_id, active);


-- ============================================================
-- 3. Table routing_operations — Operations within routings
-- ============================================================

CREATE TABLE IF NOT EXISTS routing_operations (
    operation_id      UUID            NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    routing_id        UUID            NOT NULL REFERENCES routings(routing_id) ON DELETE CASCADE,
    sequence          INTEGER         NOT NULL,
    work_center_id    UUID            NOT NULL REFERENCES work_centers(work_center_id),
    setup_time        NUMERIC(10,4)   NOT NULL DEFAULT 0,
    run_time_per_unit NUMERIC(10,6)   NOT NULL DEFAULT 0,
    description       TEXT,
    active            BOOLEAN         NOT NULL DEFAULT true,
    
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    
    -- Constraints
    CONSTRAINT chk_routing_operation_sequence_positive CHECK (sequence > 0),
    CONSTRAINT chk_routing_operation_times_positive CHECK (setup_time >= 0 AND run_time_per_unit >= 0),
    -- Unique sequence within a routing
    CONSTRAINT unique_operation_sequence_per_routing UNIQUE (routing_id, sequence)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_routing_operations_routing ON routing_operations (routing_id);
CREATE INDEX IF NOT EXISTS idx_routing_operations_work_center ON routing_operations (work_center_id);
CREATE INDEX IF NOT EXISTS idx_routing_operations_sequence ON routing_operations (routing_id, sequence);


-- ============================================================
-- 4. Table work_center_calendar_edges — WorkCenter to Calendar relationship
-- ============================================================

CREATE TABLE IF NOT EXISTS work_center_calendar_edges (
    edge_id         UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    work_center_id  UUID        NOT NULL REFERENCES work_centers(work_center_id) ON DELETE CASCADE,
    calendar_id     UUID        NOT NULL,
    active          BOOLEAN     NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_work_center_calendar_work_center ON work_center_calendar_edges (work_center_id);
CREATE INDEX IF NOT EXISTS idx_work_center_calendar_calendar ON work_center_calendar_edges (calendar_id);


-- ============================================================
-- 5. Table routing_requires_capacity_edges — Operation to WorkCenter capacity requirement
-- ============================================================

CREATE TABLE IF NOT EXISTS routing_requires_capacity_edges (
    edge_id          UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    routing_id       UUID        NOT NULL REFERENCES routings(routing_id) ON DELETE CASCADE,
    operation_id     UUID        NOT NULL REFERENCES routing_operations(operation_id) ON DELETE CASCADE,
    work_center_id   UUID        NOT NULL REFERENCES work_centers(work_center_id),
    scenario_id      UUID        NOT NULL REFERENCES scenarios(scenario_id),
    active           BOOLEAN     NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_routing_requires_capacity_routing ON routing_requires_capacity_edges (routing_id);
CREATE INDEX IF NOT EXISTS idx_routing_requires_capacity_operation ON routing_requires_capacity_edges (operation_id);
CREATE INDEX IF NOT EXISTS idx_routing_requires_capacity_work_center ON routing_requires_capacity_edges (work_center_id);
CREATE INDEX IF NOT EXISTS idx_routing_requires_capacity_scenario ON routing_requires_capacity_edges (scenario_id);


-- ============================================================
-- 6. Add updated_at triggers
-- ============================================================

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'work_centers') THEN
        DROP TRIGGER IF EXISTS trg_work_centers_updated_at ON work_centers;
        CREATE TRIGGER trg_work_centers_updated_at
            BEFORE UPDATE ON work_centers
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'routings') THEN
        DROP TRIGGER IF EXISTS trg_routings_updated_at ON routings;
        CREATE TRIGGER trg_routings_updated_at
            BEFORE UPDATE ON routings
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'routing_operations') THEN
        DROP TRIGGER IF EXISTS trg_routing_operations_updated_at ON routing_operations;
        CREATE TRIGGER trg_routing_operations_updated_at
            BEFORE UPDATE ON routing_operations
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;


-- ============================================================
-- 7. Add 'work_center_requires_calendar' and 'requires_capacity' edge types
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

    IF v_constraint_def IS NOT NULL AND v_constraint_def NOT LIKE '%work_center_requires_calendar%' THEN
        ALTER TABLE edges DROP CONSTRAINT IF EXISTS edges_edge_type_check;
        ALTER TABLE edges ADD CONSTRAINT edges_edge_type_check CHECK (
            edge_type IN (
                'replenishes', 'feeds_forward', 'consumes', 'depends_on',
                'transfers_to', 'pegged_to', 'governed_by',
                'transfers', 'requires', 'substitutes',
                'fulfills', 'produces', 'ghost_member',
                'bom_component', 'consumes_resource',
                'forecasted_demand_for',
                'mps_planned_for', 'mps_supplies',
                'work_center_requires_calendar', 'requires_capacity'
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
                'mps_planned_for', 'mps_supplies',
                'work_center_requires_calendar', 'requires_capacity'
            )
        );
    END IF;
END $$;


-- ============================================================
-- Comments
-- ============================================================

COMMENT ON TABLE work_centers IS 'Production resources for detailed capacity planning';
COMMENT ON COLUMN work_centers.code IS 'Short alphanumeric identifier (e.g., WC-001)';
COMMENT ON COLUMN work_centers.capacity_per_day IS 'Maximum output capacity per day (standard hours or units)';
COMMENT ON COLUMN work_centers.efficiency IS 'Efficiency factor (0.0 to 1.0)';
COMMENT ON COLUMN work_centers.calendar_id IS 'Reference to calendar defining working days/hours';

COMMENT ON TABLE routings IS 'Manufacturing process definitions linking items to operations';
COMMENT ON COLUMN routings.sequence IS 'Routing sequence for alternate routings (1 = primary)';
COMMENT ON COLUMN routings.item_id IS 'The item this routing produces';

COMMENT ON TABLE routing_operations IS 'Individual operations within a routing';
COMMENT ON COLUMN routing_operations.sequence IS 'Operation sequence within the routing (1-indexed)';
COMMENT ON COLUMN routing_operations.setup_time IS 'Setup time in hours (independent of quantity)';
COMMENT ON COLUMN routing_operations.run_time_per_unit IS 'Run time per unit in hours';

COMMENT ON TABLE work_center_calendar_edges IS 'Edges linking work centers to their calendars';
COMMENT ON TABLE routing_requires_capacity_edges IS 'Edges linking operations to work center capacity requirements';
