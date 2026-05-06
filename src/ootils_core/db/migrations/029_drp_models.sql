-- ============================================================
-- Ootils Core — Migration 029: DRP (Distribution Requirements Planning)
-- DRP-001: Distribution Network Model
-- ============================================================
-- Creates tables for distribution network modeling including
-- distribution links and transportation lanes for multi-echelon planning.
-- ============================================================

BEGIN;

-- ============================================================
-- 1. Table distribution_links — Transfer channels between locations
-- ============================================================

CREATE TABLE IF NOT EXISTS distribution_links (
    distribution_link_id  UUID            NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    upstream_location_id  UUID            NOT NULL REFERENCES locations(location_id),
    downstream_location_id UUID           NOT NULL REFERENCES locations(location_id),
    item_id               UUID            REFERENCES items(item_id),
    transit_lead_time_days NUMERIC(10,2)  NOT NULL DEFAULT 7,
    transit_cost_per_unit NUMERIC(18,6),
    transit_cost_fixed    NUMERIC(18,6),
    minimum_shipment_qty  NUMERIC(18,6)   NOT NULL DEFAULT 1,
    maximum_shipment_qty  NUMERIC(18,6),
    shipment_frequency    TEXT,
    shipment_days         INTEGER[],
    active                BOOLEAN         NOT NULL DEFAULT true,
    priority              INTEGER         NOT NULL DEFAULT 100,
    
    created_at            TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ     NOT NULL DEFAULT now(),
    
    -- Constraints
    CONSTRAINT chk_distribution_link_locations_different CHECK (upstream_location_id != downstream_location_id),
    CONSTRAINT chk_distribution_link_lead_time_positive CHECK (transit_lead_time_days >= 0),
    CONSTRAINT chk_distribution_link_minimum_qty_positive CHECK (minimum_shipment_qty >= 0),
    CONSTRAINT chk_distribution_link_maximum_qty CHECK (
        maximum_shipment_qty IS NULL OR maximum_shipment_qty >= minimum_shipment_qty
    ),
    CONSTRAINT chk_distribution_link_priority_positive CHECK (priority >= 1)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_distribution_links_upstream ON distribution_links (upstream_location_id);
CREATE INDEX IF NOT EXISTS idx_distribution_links_downstream ON distribution_links (downstream_location_id);
CREATE INDEX IF NOT EXISTS idx_distribution_links_item ON distribution_links (item_id);
CREATE INDEX IF NOT EXISTS idx_distribution_links_active ON distribution_links (active);
CREATE INDEX IF NOT EXISTS idx_distribution_links_locations ON distribution_links (upstream_location_id, downstream_location_id);
CREATE INDEX IF NOT EXISTS idx_distribution_links_item_active ON distribution_links (item_id, active);


-- ============================================================
-- 2. Table transportation_lanes — Transportation options for distribution links
-- ============================================================

CREATE TABLE IF NOT EXISTS transportation_lanes (
    lane_id                 UUID            NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    distribution_link_id    UUID            NOT NULL REFERENCES distribution_links(distribution_link_id) ON DELETE CASCADE,
    carrier                 TEXT,
    mode                    TEXT            NOT NULL DEFAULT 'truck',
    service_level           TEXT            NOT NULL DEFAULT 'standard',
    transit_time_min_days   NUMERIC(10,2)   NOT NULL DEFAULT 1,
    transit_time_max_days   NUMERIC(10,2)   NOT NULL DEFAULT 7,
    cost_per_unit           NUMERIC(18,6),
    cost_per_shipment       NUMERIC(18,6),
    minimum_weight          NUMERIC(18,6),
    maximum_weight          NUMERIC(18,6),
    equipment_type          TEXT,
    active                  BOOLEAN         NOT NULL DEFAULT true,
    
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT now(),
    
    -- Constraints
    CONSTRAINT chk_lane_transit_time_positive CHECK (transit_time_min_days >= 0 AND transit_time_max_days >= 0),
    CONSTRAINT chk_lane_transit_time_range CHECK (transit_time_max_days >= transit_time_min_days),
    CONSTRAINT chk_lane_minimum_weight_positive CHECK (minimum_weight IS NULL OR minimum_weight >= 0),
    CONSTRAINT chk_lane_maximum_weight CHECK (
        maximum_weight IS NULL OR minimum_weight IS NULL OR maximum_weight >= minimum_weight
    )
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_transportation_lanes_distribution_link ON transportation_lanes (distribution_link_id);
CREATE INDEX IF NOT EXISTS idx_transportation_lanes_mode ON transportation_lanes (mode);
CREATE INDEX IF NOT EXISTS idx_transportation_lanes_carrier ON transportation_lanes (carrier);
CREATE INDEX IF NOT EXISTS idx_transportation_lanes_active ON transportation_lanes (active);
CREATE INDEX IF NOT EXISTS idx_transportation_lanes_service_level ON transportation_lanes (service_level);


-- ============================================================
-- 3. Table distribution_link_edges — Explicit edges for network topology
-- ============================================================

CREATE TABLE IF NOT EXISTS distribution_link_edges (
    edge_id                 UUID            NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    distribution_link_id    UUID            NOT NULL REFERENCES distribution_links(distribution_link_id) ON DELETE CASCADE,
    upstream_location_id    UUID            NOT NULL REFERENCES locations(location_id),
    downstream_location_id  UUID            NOT NULL REFERENCES locations(location_id),
    item_id                 UUID            REFERENCES items(item_id),
    active                  BOOLEAN         NOT NULL DEFAULT true,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_distribution_link_edges_distribution_link ON distribution_link_edges (distribution_link_id);
CREATE INDEX IF NOT EXISTS idx_distribution_link_edges_upstream ON distribution_link_edges (upstream_location_id);
CREATE INDEX IF NOT EXISTS idx_distribution_link_edges_downstream ON distribution_link_edges (downstream_location_id);
CREATE INDEX IF NOT EXISTS idx_distribution_link_edges_item ON distribution_link_edges (item_id);


-- ============================================================
-- 4. Table lane_requires_link_edges — Edges linking lanes to distribution links
-- ============================================================

CREATE TABLE IF NOT EXISTS lane_requires_link_edges (
    edge_id                 UUID            NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    lane_id                 UUID            NOT NULL REFERENCES transportation_lanes(lane_id) ON DELETE CASCADE,
    distribution_link_id    UUID            NOT NULL REFERENCES distribution_links(distribution_link_id),
    active                  BOOLEAN         NOT NULL DEFAULT true,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_lane_requires_link_edges_lane ON lane_requires_link_edges (lane_id);
CREATE INDEX IF NOT EXISTS idx_lane_requires_link_edges_distribution_link ON lane_requires_link_edges (distribution_link_id);


-- ============================================================
-- 5. Add updated_at triggers
-- ============================================================

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'distribution_links') THEN
        DROP TRIGGER IF EXISTS trg_distribution_links_updated_at ON distribution_links;
        CREATE TRIGGER trg_distribution_links_updated_at
            BEFORE UPDATE ON distribution_links
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'transportation_lanes') THEN
        DROP TRIGGER IF EXISTS trg_transportation_lanes_updated_at ON transportation_lanes;
        CREATE TRIGGER trg_transportation_lanes_updated_at
            BEFORE UPDATE ON transportation_lanes
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;


-- ============================================================
-- 6. Add edge types to edges table constraint
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

    IF v_constraint_def IS NOT NULL AND v_constraint_def NOT LIKE '%distribution_link%' THEN
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
                'work_center_requires_calendar', 'requires_capacity',
                'distribution_link', 'lane_requires_link'
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
                'work_center_requires_calendar', 'requires_capacity',
                'distribution_link', 'lane_requires_link'
            )
        );
    END IF;
END $$;


-- ============================================================
-- Comments
-- ============================================================

COMMENT ON TABLE distribution_links IS 'Transfer channels between locations in the distribution network';
COMMENT ON COLUMN distribution_links.upstream_location_id IS 'Source location (plant, upstream DC)';
COMMENT ON COLUMN distribution_links.downstream_location_id IS 'Destination location (downstream DC, warehouse)';
COMMENT ON COLUMN distribution_links.item_id IS 'Optional item-specific link (NULL = generic for all items)';
COMMENT ON COLUMN distribution_links.transit_lead_time_days IS 'Transit time in days';
COMMENT ON COLUMN distribution_links.transit_cost_per_unit IS 'Variable cost per unit transferred';
COMMENT ON COLUMN distribution_links.transit_cost_fixed IS 'Fixed cost per shipment';
COMMENT ON COLUMN distribution_links.minimum_shipment_qty IS 'Minimum quantity per shipment';
COMMENT ON COLUMN distribution_links.maximum_shipment_qty IS 'Maximum quantity per shipment (optional)';
COMMENT ON COLUMN distribution_links.shipment_frequency IS 'Frequency constraint (daily, weekly, biweekly, monthly, on_demand)';
COMMENT ON COLUMN distribution_links.shipment_days IS 'Allowed shipment days (1=Mon, 7=Sun)';
COMMENT ON COLUMN distribution_links.priority IS 'Priority for sourcing (1=highest)';

COMMENT ON TABLE transportation_lanes IS 'Transportation options and constraints for distribution links';
COMMENT ON COLUMN transportation_lanes.carrier IS 'Carrier name or code';
COMMENT ON COLUMN transportation_lanes.mode IS 'Transportation mode (truck, rail, air, ocean, intermodal)';
COMMENT ON COLUMN transportation_lanes.service_level IS 'Service level (standard, expedited, economy)';
COMMENT ON COLUMN transportation_lanes.transit_time_min_days IS 'Minimum transit time in days';
COMMENT ON COLUMN transportation_lanes.transit_time_max_days IS 'Maximum transit time in days';
COMMENT ON COLUMN transportation_lanes.cost_per_unit IS 'Variable cost per unit';
COMMENT ON COLUMN transportation_lanes.cost_per_shipment IS 'Fixed cost per shipment';
COMMENT ON COLUMN transportation_lanes.equipment_type IS 'Equipment type (e.g., "53ft dry van", "40ft container")';

COMMENT ON TABLE distribution_link_edges IS 'Explicit edges for distribution link network topology';
COMMENT ON TABLE lane_requires_link_edges IS 'Edges linking transportation lanes to distribution links';

COMMIT;
