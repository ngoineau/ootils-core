-- ============================================================
-- Ootils Core — Migration 009: Resource entity + RCCP
-- Entités Resource + Resource Capacity Overrides
-- Edge type consumes_resource (ADR-010 V1)
-- ============================================================

-- ============================================================
-- 1. Table resources — master data des ressources contraintes
-- ============================================================

CREATE TABLE IF NOT EXISTS resources (
    resource_id         UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id         TEXT        NOT NULL UNIQUE,
    name                TEXT        NOT NULL,
    resource_type       TEXT        NOT NULL CHECK (resource_type IN ('machine', 'line', 'team', 'tool')),
    location_id         UUID        REFERENCES locations(location_id),
    capacity_per_day    NUMERIC(18,4) NOT NULL DEFAULT 1.0,
    capacity_unit       TEXT        NOT NULL DEFAULT 'units',
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_resources_external_id ON resources (external_id);
CREATE INDEX IF NOT EXISTS idx_resources_location ON resources (location_id);
CREATE INDEX IF NOT EXISTS idx_resources_type ON resources (resource_type);


-- ============================================================
-- 2. Table resource_capacity_overrides — surcharges ponctuelles
-- ============================================================

CREATE TABLE IF NOT EXISTS resource_capacity_overrides (
    override_id     UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_id     UUID        NOT NULL REFERENCES resources(resource_id),
    override_date   DATE        NOT NULL,
    capacity        NUMERIC(18,4) NOT NULL,
    reason          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (resource_id, override_date)
);

CREATE INDEX IF NOT EXISTS idx_rco_resource_date ON resource_capacity_overrides (resource_id, override_date);


-- ============================================================
-- 3. Ajouter external_id sur la table nodes (pour les nœuds Resource)
-- ============================================================

ALTER TABLE nodes ADD COLUMN IF NOT EXISTS external_id TEXT;
CREATE INDEX IF NOT EXISTS idx_nodes_external_id ON nodes (external_id) WHERE external_id IS NOT NULL;


-- ============================================================
-- 4. Ajouter 'Resource' au CHECK constraint de nodes.node_type
--    Pattern idempotent via DO $$ avec recréation du constraint
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
      AND c.conname LIKE '%node_type%'
      AND t.relname = 'nodes'
      AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
    LIMIT 1;

    IF v_constraint_def IS NOT NULL AND v_constraint_def NOT LIKE '%Resource%' THEN
        ALTER TABLE nodes DROP CONSTRAINT IF EXISTS nodes_node_type_check;
        ALTER TABLE nodes ADD CONSTRAINT nodes_node_type_check CHECK (
            node_type IN (
                'Item', 'Location', 'PurchaseOrderSupply', 'OnHandSupply',
                'ProjectedInventory', 'ForecastDemand', 'CustomerOrderDemand',
                'WorkOrderSupply', 'TransferSupply', 'PlannedSupply',
                'DependentDemand', 'TransferDemand', 'Shortage', 'Ghost', 'Resource'
            )
        );
    ELSIF v_constraint_def IS NULL THEN
        ALTER TABLE nodes ADD CONSTRAINT nodes_node_type_check CHECK (
            node_type IN (
                'Item', 'Location', 'PurchaseOrderSupply', 'OnHandSupply',
                'ProjectedInventory', 'ForecastDemand', 'CustomerOrderDemand',
                'WorkOrderSupply', 'TransferSupply', 'PlannedSupply',
                'DependentDemand', 'TransferDemand', 'Shortage', 'Ghost', 'Resource'
            )
        );
    END IF;
    -- Si 'Resource' est déjà dans le constraint : no-op
END $$;


-- ============================================================
-- 5. Edge type consumes_resource dans le graphe
--    Extension du CHECK constraint sur edges.edge_type
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

    IF v_constraint_def IS NOT NULL AND v_constraint_def NOT LIKE '%consumes_resource%' THEN
        ALTER TABLE edges DROP CONSTRAINT IF EXISTS edges_edge_type_check;
        ALTER TABLE edges ADD CONSTRAINT edges_edge_type_check CHECK (
            edge_type IN (
                'replenishes', 'feeds_forward', 'consumes', 'depends_on',
                'transfers_to', 'pegged_to', 'governed_by',
                'transfers', 'requires', 'substitutes',
                'fulfills', 'produces', 'ghost_member',
                'bom_component', 'consumes_resource'
            )
        );
    ELSIF v_constraint_def IS NULL THEN
        ALTER TABLE edges ADD CONSTRAINT edges_edge_type_check CHECK (
            edge_type IN (
                'replenishes', 'feeds_forward', 'consumes', 'depends_on',
                'transfers_to', 'pegged_to', 'governed_by',
                'transfers', 'requires', 'substitutes',
                'fulfills', 'produces', 'ghost_member',
                'bom_component', 'consumes_resource'
            )
        );
    END IF;
END $$;
