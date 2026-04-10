-- Migration 020: create missing consumes edges from demand nodes to PI buckets
--
-- The seed created only one 'consumes' edge per demand node (→ first PI bucket
-- of the span). ForecastDemand and CustomerOrderDemand nodes have a time_span
-- covering multiple daily PI buckets, but were only wired to the first one.
--
-- The propagator distributes demand over overlapping PI buckets: for each
-- demand, it iterates edges e.to_node_id = PI node and computes the overlap.
-- Without all the edges, only the first bucket receives the demand outflow —
-- producing a sawtooth pattern where outflows spike every 7 days then drop
-- to zero for 6 days.
--
-- Fix: for every demand node (ForecastDemand, CustomerOrderDemand) in every
-- scenario, create a 'consumes' edge to each PI bucket whose time_span_start
-- falls within [demand.time_span_start, demand.time_span_end). Idempotent.

INSERT INTO edges (
    edge_id,
    edge_type,
    from_node_id,
    to_node_id,
    scenario_id,
    priority,
    weight_ratio,
    effective_start,
    effective_end,
    active,
    created_at
)
SELECT
    gen_random_uuid(),
    'consumes',
    n_demand.node_id,
    n_pi.node_id,
    n_demand.scenario_id,
    0,
    1.0,
    n_demand.time_span_start,
    n_demand.time_span_end,
    TRUE,
    now()
FROM nodes n_demand
JOIN nodes n_pi
    ON  n_pi.item_id      = n_demand.item_id
    AND n_pi.location_id  = n_demand.location_id
    AND n_pi.scenario_id  = n_demand.scenario_id
    AND n_pi.node_type    = 'ProjectedInventory'
    AND n_pi.active       = TRUE
    AND n_pi.time_span_start >= n_demand.time_span_start
    AND n_pi.time_span_start <  n_demand.time_span_end
WHERE n_demand.node_type IN ('ForecastDemand', 'CustomerOrderDemand')
  AND n_demand.active = TRUE
  -- Skip pairs that already have a consumes edge (idempotent)
  AND NOT EXISTS (
      SELECT 1 FROM edges e
      WHERE e.from_node_id = n_demand.node_id
        AND e.to_node_id   = n_pi.node_id
        AND e.edge_type    = 'consumes'
        AND e.active       = TRUE
  );
