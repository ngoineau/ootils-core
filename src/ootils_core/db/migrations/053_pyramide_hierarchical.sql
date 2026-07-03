-- ============================================================
-- Migration 053 — Pyramide hierarchical forecasts (axis A, PR2)
-- ============================================================
-- Arbitrated design: EXTEND the existing forecast tables (no separate
-- aggregate-forecast table). A forecasts / pyramide_runs row now targets
-- either:
--   * a LEAF      — (item_id, location_id), the historical contract, or
--   * an AGGREGATE — a node of a registered hierarchy (migration 047):
--                    (hierarchy_id, level, node_code).
-- A CHECK constraint enforces "leaf XOR aggregate"; hybrid rows are
-- impossible at the database level.
--
-- Why the same tables?  Aggregate forecasts share the full lifecycle of
-- leaf forecasts (values, adjustments, runs, snapshots-to-come) and the
-- reconciliation step (S matrix) needs both in one queryable namespace.
--
-- Idempotence notes (repo migration policy: a re-run must not fail):
--   * ADD COLUMN IF NOT EXISTS            — no-op on re-run.
--   * ALTER COLUMN ... DROP NOT NULL      — Postgres treats dropping an
--     absent NOT NULL as a plain no-op (no error raised, any run count).
--   * DROP CONSTRAINT IF EXISTS + ADD     — deterministic re-create.
--   * CREATE INDEX IF NOT EXISTS          — no-op on re-run.
--
-- Existing leaf rows satisfy the new CHECK trivially (item_id and
-- location_id are populated; hierarchy_id / level / node_code are
-- freshly added, hence NULL): no backfill needed.
-- No JSONB. Typed columns only.
--
-- NOTE — hierarchy_id is TEXT, not UUID: it references
-- hierarchy(hierarchy_id), whose primary key is TEXT (migration 047).
-- ============================================================

BEGIN;

-- ============================================================
-- 1. forecasts — aggregate-node addressing
-- ============================================================

ALTER TABLE forecasts
    ADD COLUMN IF NOT EXISTS hierarchy_id TEXT
        REFERENCES hierarchy(hierarchy_id) ON DELETE RESTRICT;
ALTER TABLE forecasts ADD COLUMN IF NOT EXISTS level     TEXT;
ALTER TABLE forecasts ADD COLUMN IF NOT EXISTS node_code TEXT;

-- Leaf columns become nullable: an aggregate forecast has no item/location.
ALTER TABLE forecasts ALTER COLUMN item_id     DROP NOT NULL;
ALTER TABLE forecasts ALTER COLUMN location_id DROP NOT NULL;

-- Leaf XOR aggregate (exact arbitrated predicate).
-- Leaf branch pins ALL aggregate columns to NULL: an orphan hierarchy_id
-- on a leaf row would be silently ignored by the composite FK below
-- (MATCH SIMPLE skips the check when node_code is NULL), so the CHECK
-- must forbid it. Aggregate branch requires level too. "level is the
-- actual level of node_code in the registry" stays an application-layer
-- invariant (a CHECK cannot subquery hierarchy_node).
ALTER TABLE forecasts DROP CONSTRAINT IF EXISTS chk_forecasts_leaf_xor_aggregate;
ALTER TABLE forecasts ADD CONSTRAINT chk_forecasts_leaf_xor_aggregate CHECK (
    (item_id IS NOT NULL AND location_id IS NOT NULL
     AND hierarchy_id IS NULL AND level IS NULL AND node_code IS NULL)
    OR
    (node_code IS NOT NULL AND hierarchy_id IS NOT NULL AND level IS NOT NULL
     AND item_id IS NULL AND location_id IS NULL)
);

-- The referenced aggregate node must exist in the hierarchy registry.
-- MATCH SIMPLE (default): leaf rows (node_code NULL) are not checked.
ALTER TABLE forecasts DROP CONSTRAINT IF EXISTS fk_forecasts_hierarchy_node;
ALTER TABLE forecasts ADD CONSTRAINT fk_forecasts_hierarchy_node
    FOREIGN KEY (hierarchy_id, node_code)
    REFERENCES hierarchy_node (hierarchy_id, code)
    ON DELETE RESTRICT;

-- Aggregate lookups: "forecasts for node X of hierarchy H at level L".
CREATE INDEX IF NOT EXISTS idx_forecasts_hierarchy_node
    ON forecasts (hierarchy_id, level, node_code)
    WHERE node_code IS NOT NULL;

COMMENT ON COLUMN forecasts.hierarchy_id IS
    'Aggregate forecasts only: hierarchy this forecast''s node belongs to '
    '(migration 047 registry). NULL on leaf (item, location) forecasts.';
COMMENT ON COLUMN forecasts.level IS
    'Aggregate forecasts only: level name of node_code within the hierarchy '
    '(e.g. one of hierarchy.levels). NULL on leaf forecasts.';
COMMENT ON COLUMN forecasts.node_code IS
    'Aggregate forecasts only: hierarchy_node.code this forecast targets. '
    'CHECK chk_forecasts_leaf_xor_aggregate enforces leaf XOR aggregate.';

-- ============================================================
-- 2. pyramide_runs — a run can target an aggregate node too
-- ============================================================

ALTER TABLE pyramide_runs
    ADD COLUMN IF NOT EXISTS hierarchy_id TEXT
        REFERENCES hierarchy(hierarchy_id) ON DELETE RESTRICT;
ALTER TABLE pyramide_runs ADD COLUMN IF NOT EXISTS level     TEXT;
ALTER TABLE pyramide_runs ADD COLUMN IF NOT EXISTS node_code TEXT;

ALTER TABLE pyramide_runs ALTER COLUMN item_id     DROP NOT NULL;
ALTER TABLE pyramide_runs ALTER COLUMN location_id DROP NOT NULL;

-- Same tightened predicate as forecasts (see the comment there): leaf
-- rows carry no aggregate column at all, aggregate rows require level.
ALTER TABLE pyramide_runs DROP CONSTRAINT IF EXISTS chk_pyramide_runs_leaf_xor_aggregate;
ALTER TABLE pyramide_runs ADD CONSTRAINT chk_pyramide_runs_leaf_xor_aggregate CHECK (
    (item_id IS NOT NULL AND location_id IS NOT NULL
     AND hierarchy_id IS NULL AND level IS NULL AND node_code IS NULL)
    OR
    (node_code IS NOT NULL AND hierarchy_id IS NOT NULL AND level IS NOT NULL
     AND item_id IS NULL AND location_id IS NULL)
);

ALTER TABLE pyramide_runs DROP CONSTRAINT IF EXISTS fk_pyramide_runs_hierarchy_node;
ALTER TABLE pyramide_runs ADD CONSTRAINT fk_pyramide_runs_hierarchy_node
    FOREIGN KEY (hierarchy_id, node_code)
    REFERENCES hierarchy_node (hierarchy_id, code)
    ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_pyramide_runs_hierarchy_node
    ON pyramide_runs (hierarchy_id, level, node_code)
    WHERE node_code IS NOT NULL;

COMMENT ON COLUMN pyramide_runs.hierarchy_id IS
    'Aggregate runs only: hierarchy of the target node. NULL on leaf runs.';
COMMENT ON COLUMN pyramide_runs.level IS
    'Aggregate runs only: level name of node_code. NULL on leaf runs.';
COMMENT ON COLUMN pyramide_runs.node_code IS
    'Aggregate runs only: hierarchy_node.code the run forecasts. '
    'CHECK chk_pyramide_runs_leaf_xor_aggregate enforces leaf XOR aggregate.';

COMMIT;
