-- 014_critical_indexes.sql
--
-- Adds missing indexes identified by DBA review.
-- All statements are idempotent (IF NOT EXISTS).
-- No table locks beyond brief index creation.

-- ============================================================
-- CRITICAL: Edge 4-column lookup for allocation upsert
-- Used by: store.py upsert_edge — called per demand during allocation
-- Without this, every upsert_edge does a seq scan on edges.
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_edges_composite_lookup
    ON edges (from_node_id, to_node_id, edge_type, scenario_id)
    WHERE active = TRUE;

-- ============================================================
-- CRITICAL: Node lookup by (item, scenario, type, time_ref)
-- Used by: capacity_aggregate _get_supply_load (day-by-day loop)
--          phase_transition _get_projected_inventory (day-by-day loop)
--          propagator supply/demand resolution
-- Without this, ghost engines do N×days seq scans on nodes.
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_nodes_item_scenario_type_timeref
    ON nodes (item_id, scenario_id, node_type, time_ref)
    WHERE active = TRUE;

-- ============================================================
-- HIGH: Active shortages by scenario for resolve_stale + get_active
-- Used by: detector.py resolve_stale(), get_active_shortages()
-- Partial index avoids scanning resolved shortages.
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_shortages_scenario_active
    ON shortages (scenario_id, severity DESC)
    WHERE status = 'active';

-- ============================================================
-- HIGH: Active shortages by item for impact scoring
-- Used by: impact_scorer.py _get_active_shortages_for_items()
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_shortages_item_active
    ON shortages (item_id)
    WHERE status = 'active';

-- ============================================================
-- HIGH: Latest completed calc_run per scenario
-- Used by: manager.py _latest_calc_run()
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_calc_runs_scenario_completed
    ON calc_runs (scenario_id, completed_at DESC)
    WHERE status = 'completed';

-- ============================================================
-- MEDIUM: Ingest batches by entity_type + dq_status
-- Used by: stat_rules.py _load_history(), temporal_rules.py
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_ingest_batches_entity_dq
    ON ingest_batches (entity_type, dq_status, created_at DESC);

-- ============================================================
-- MEDIUM: Ingest rows ordered by row_number within batch
-- Used by: dq/engine.py, stat_rules.py, temporal_rules.py
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_ingest_rows_batch_rownum
    ON ingest_rows (batch_id, row_number);

-- ============================================================
-- MEDIUM: DQ issues by batch ordered by impact score
-- Used by: dq.py get_agent_report()
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_dq_issues_batch_impact
    ON data_quality_issues (batch_id, impact_score DESC NULLS LAST);

-- ============================================================
-- MEDIUM: Ghost nodes composite lookup for upsert
-- Used by: ghosts.py ingest_ghost()
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_ghost_nodes_name_type_scenario
    ON ghost_nodes (name, ghost_type, scenario_id);

-- ============================================================
-- MEDIUM: DQ agent runs — latest by batch
-- Used by: dq.py get_agent_report()
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_dq_agent_runs_batch_created
    ON dq_agent_runs (batch_id, created_at DESC);

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
