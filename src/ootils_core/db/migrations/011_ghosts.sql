-- ============================================================
-- Ootils Core — Migration 011: Ghost nodes + Ghost members
-- ADR-010: Ghosts V1 — phase_transition + capacity_aggregate
-- ============================================================

-- ============================================================
-- 1. Table ghost_nodes
-- ============================================================

CREATE TABLE IF NOT EXISTS ghost_nodes (
    ghost_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT        NOT NULL,
    ghost_type      TEXT        NOT NULL
                    CHECK (ghost_type IN ('phase_transition', 'capacity_aggregate')),
    scenario_id     UUID        REFERENCES scenarios(scenario_id),
    resource_id     UUID        REFERENCES resources(resource_id),
    node_id         UUID        REFERENCES nodes(node_id),
    status          TEXT        NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'archived', 'draft')),
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ghost_nodes_scenario  ON ghost_nodes (scenario_id);
CREATE INDEX IF NOT EXISTS idx_ghost_nodes_resource  ON ghost_nodes (resource_id);
CREATE INDEX IF NOT EXISTS idx_ghost_nodes_type      ON ghost_nodes (ghost_type);
CREATE INDEX IF NOT EXISTS idx_ghost_nodes_status    ON ghost_nodes (status);


-- ============================================================
-- 2. Table ghost_members
-- ============================================================

CREATE TABLE IF NOT EXISTS ghost_members (
    member_id               UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    ghost_id                UUID        NOT NULL REFERENCES ghost_nodes(ghost_id) ON DELETE CASCADE,
    item_id                 UUID        NOT NULL REFERENCES items(item_id),
    role                    TEXT        NOT NULL
                            CHECK (role IN ('incoming', 'outgoing', 'member')),
    transition_start_date   DATE,
    transition_end_date     DATE,
    transition_curve        TEXT        NOT NULL DEFAULT 'linear'
                            CHECK (transition_curve IN ('linear', 'step', 'sigmoid')),
    weight_at_start         NUMERIC(5,4) NOT NULL DEFAULT 1.0
                            CHECK (weight_at_start BETWEEN 0.0 AND 1.0),
    weight_at_end           NUMERIC(5,4) NOT NULL DEFAULT 0.0
                            CHECK (weight_at_end BETWEEN 0.0 AND 1.0),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ghost_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_ghost_members_ghost   ON ghost_members (ghost_id);
CREATE INDEX IF NOT EXISTS idx_ghost_members_item    ON ghost_members (item_id);
CREATE INDEX IF NOT EXISTS idx_ghost_members_role    ON ghost_members (role);


-- ============================================================
-- 3. Ensure 'Ghost' in nodes.node_type CHECK constraint
--    (added in migration 009 already — idempotent check)
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

    IF v_constraint_def IS NOT NULL AND v_constraint_def NOT LIKE '%Ghost%' THEN
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
END $$;


-- ============================================================
-- 4. Ensure 'ghost_member' in edges.edge_type CHECK constraint
--    (added in migration 009 already — idempotent check)
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

    IF v_constraint_def IS NOT NULL AND v_constraint_def NOT LIKE '%ghost_member%' THEN
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
