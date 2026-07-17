-- ============================================================
-- Migration 080 — backfill missing feeds_forward edges
-- (2026-07-17 ingest lifecycle incremental-propagation fix)
-- ============================================================
-- Root cause: migration 019 chained every ProjectedInventory series that
-- existed AT THAT TIME with 'feeds_forward' edges (PI[N].closing_stock ->
-- PI[N+1].opening_stock) — the edge GraphTraversal.expand_dirty_subgraph
-- (engine/kernel/graph/traversal.py) walks to cascade an incremental
-- propagation (POST /v1/events) forward through a projection series.
--
-- `_ensure_projection_series` (api/routers/ingest.py) — the function that
-- creates a ProjectionSeries + its 90 daily PI buckets for every real
-- ingest of a NEW (item, location) pair — was added without ever creating
-- this chain, so every series created through the real ingest API (as
-- opposed to the demo seed migration 019 backfilled) has ZERO
-- feeds_forward edges. Incremental propagation still "works" in the sense
-- that it recomputes the ONE PI bucket directly wired to the triggering
-- supply/demand node, but the closing_stock change never cascades to the
-- rest of the horizon: every later bucket keeps its STALE closing_stock
-- until the next full recompute silently papers over it (full recompute
-- marks every PI node dirty directly — see POST /v1/calc/run
-- {"full_recompute": true} in api/routers/calc.py — bypassing edges
-- entirely, which is why this was invisible until a test drove the
-- incremental path in isolation). This is the exact failure mode
-- migration 019's own header describes ("all what-if simulations ...
-- incorrect projections"), re-introduced by a later ingest-path addition
-- that never got the same treatment.
--
-- `_ensure_projection_series` itself is fixed in the same change (it now
-- creates the 89 feeds_forward edges alongside the 90 buckets), so no NEW
-- gap opens after this migration runs; this migration only backfills
-- whatever series already exist in this database without the chain.
--
-- Idempotent by construction (same NOT EXISTS guard as migration 019): a
-- from/to pair that already has an active feeds_forward edge is skipped,
-- so a migration re-attempt after a partial failure — or simply running
-- this against a database where every series already has its chain — is
-- a clean no-op.
-- ============================================================

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
