-- Migration 019: add feeds_forward to edge_type CHECK constraint + create edges
--
-- The propagator chains PI buckets via 'feeds_forward' edges:
--   PI[bucket_sequence=N].closing_stock → PI[bucket_sequence=N+1].opening_stock
--
-- The seed never created these edges, so every bucket restarted from 0 instead
-- of carrying forward the closing stock from the previous bucket. This caused
-- all what-if simulations to produce incorrect projections.
--
-- This migration creates the missing feeds_forward edges for all existing
-- ProjectedInventory nodes across all scenarios, using projection_series_id
-- + bucket_sequence to identify consecutive pairs.

-- Step 1: Add feeds_forward to the edge_type CHECK constraint
ALTER TABLE edges DROP CONSTRAINT IF EXISTS edges_edge_type_check;
ALTER TABLE edges ADD CONSTRAINT edges_edge_type_check CHECK (
    edge_type = ANY (ARRAY[
        'replenishes', 'transfers', 'requires', 'substitutes',
        'fulfills', 'consumes', 'produces', 'ghost_member',
        'bom_component', 'consumes_resource', 'feeds_forward',
        'pegged_to'
    ])
);

-- Step 2: Create feeds_forward edges between consecutive PI nodes
INSERT INTO edges (
    edge_id,
    edge_type,
    from_node_id,
    to_node_id,
    scenario_id,
    priority,
    weight_ratio,  -- 1.0 = full pass-through of closing_stock
    effective_start,
    effective_end,
    active,
    created_at
)
SELECT
    gen_random_uuid(),
    'feeds_forward',
    n1.node_id,
    n2.node_id,
    n1.scenario_id,
    0,
    1.0,
    n1.time_span_start,
    n2.time_span_end,
    TRUE,
    now()
FROM nodes n1
JOIN nodes n2
    ON  n2.projection_series_id = n1.projection_series_id
    AND n2.bucket_sequence      = n1.bucket_sequence + 1
    AND n2.active               = TRUE
WHERE n1.node_type = 'ProjectedInventory'
  AND n1.active    = TRUE
  -- Skip pairs that already have a feeds_forward edge (idempotent)
  AND NOT EXISTS (
      SELECT 1 FROM edges e
      WHERE e.from_node_id = n1.node_id
        AND e.to_node_id   = n2.node_id
        AND e.edge_type    = 'feeds_forward'
        AND e.active       = TRUE
  );
