-- ============================================================
-- Migration 014: Missing indexes — hot path optimisation
-- ============================================================
-- Adds critical and medium-priority indexes identified in DBA review #130.
-- Drops redundant / low-cardinality indexes that waste write amplification.
-- All CREATE INDEX statements use IF NOT EXISTS for idempotency.
-- Conditional tables (ghost_nodes, dq_agent_runs, data_quality_issues)
-- are wrapped in existence checks.
-- ============================================================

-- ============================================================
-- CRITICAL INDEXES
-- ============================================================

-- 1. Edge 4-column composite lookup (allocation upsert per demand node)
CREATE INDEX IF NOT EXISTS idx_edges_composite_lookup
    ON edges (from_node_id, to_node_id, edge_type, scenario_id);

-- 2. Node supply load for ghost engines (day-by-day loop)
CREATE INDEX IF NOT EXISTS idx_nodes_item_scenario_type_timeref
    ON nodes (item_id, scenario_id, node_type, time_ref)
    WHERE active = TRUE;

-- 3. Active shortages by scenario (post-propagation hot path)
CREATE INDEX IF NOT EXISTS idx_shortages_scenario_active
    ON shortages (scenario_id, severity_score DESC)
    WHERE status = 'active';

-- 4. Active shortages by item (impact scoring)
CREATE INDEX IF NOT EXISTS idx_shortages_item_active
    ON shortages (item_id) WHERE status = 'active';

-- 5. Latest completed calc_run per scenario
CREATE INDEX IF NOT EXISTS idx_calc_runs_scenario_completed
    ON calc_runs (scenario_id, completed_at DESC)
    WHERE status = 'completed';

-- ============================================================
-- MEDIUM PRIORITY INDEXES
-- ============================================================

-- 6. DQ pipeline: batch by entity type and status
CREATE INDEX IF NOT EXISTS idx_ingest_batches_entity_dq
    ON ingest_batches (entity_type, dq_status, created_at DESC);

-- 7. DQ pipeline: rows by batch and row number
CREATE INDEX IF NOT EXISTS idx_ingest_rows_batch_rownum
    ON ingest_rows (batch_id, row_number);

-- 8. DQ issues by batch and impact (table may not exist in all environments)
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'data_quality_issues') THEN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes
                       WHERE schemaname = 'public'
                         AND indexname = 'idx_dq_issues_batch_impact') THEN
            EXECUTE 'CREATE INDEX idx_dq_issues_batch_impact
                         ON data_quality_issues (batch_id, impact_score DESC NULLS LAST)';
        END IF;
    END IF;
END $$;

-- 9. Ghost nodes lookup (table may not exist in all environments)
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'ghost_nodes') THEN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes
                       WHERE schemaname = 'public'
                         AND indexname = 'idx_ghost_nodes_name_type_scenario') THEN
            EXECUTE 'CREATE INDEX idx_ghost_nodes_name_type_scenario
                         ON ghost_nodes (name, ghost_type, scenario_id)';
        END IF;
    END IF;
END $$;

-- 10. DQ agent runs by batch (table may not exist in all environments)
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'dq_agent_runs') THEN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes
                       WHERE schemaname = 'public'
                         AND indexname = 'idx_dq_agent_runs_batch_created') THEN
            EXECUTE 'CREATE INDEX idx_dq_agent_runs_batch_created
                         ON dq_agent_runs (batch_id, created_at DESC)';
        END IF;
    END IF;
END $$;

-- ============================================================
-- DROP REDUNDANT / LOW-CARDINALITY INDEXES
-- ============================================================

-- Duplicated by shortages_scenario_id_idx (already covers scenario_id lookup)
DROP INDEX IF EXISTS idx_shortages_scenario;

-- Duplicated by existing UNIQUE constraint on projection_series
DROP INDEX IF EXISTS idx_projection_series_lookup;

-- Duplicated by existing UNIQUE constraint on resources.external_id
DROP INDEX IF EXISTS idx_resources_external_id;

-- 3 distinct values — not selective enough to be useful
DROP INDEX IF EXISTS idx_ghost_members_role;

-- 3 distinct values — not selective enough to be useful
DROP INDEX IF EXISTS idx_ghost_nodes_status;

-- 2 distinct values — boolean-like, not selective enough
DROP INDEX IF EXISTS idx_ghost_nodes_type;

-- 4 distinct values — not selective enough to be useful
DROP INDEX IF EXISTS idx_resources_type;

-- ============================================================
-- LOW: Active BOM header lookup
-- Used by: bom.py _get_active_bom()
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_bom_headers_parent_active
    ON bom_headers (parent_item_id, effective_from DESC)
    WHERE status = 'active';

-- ============================================================
-- LOW: Items/locations name lookup for API resolution
-- Used by: graph.py, projection.py — resolve by name
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_items_name
    ON items (name);

CREATE INDEX IF NOT EXISTS idx_locations_name
    ON locations (name);
