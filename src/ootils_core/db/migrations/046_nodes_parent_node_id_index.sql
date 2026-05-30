-- ============================================================
-- Migration 046: index on nodes.parent_node_id (self-FK support)
-- ============================================================
-- Purpose: make every DELETE from `nodes` fast.
--
-- Background (field incident, daily-load slowness):
-- `nodes` carries a self-referential foreign key
--     nodes_parent_node_id_fkey: parent_node_id REFERENCES nodes(node_id)
-- but NO index covered parent_node_id. Postgres does NOT auto-create an
-- index on the *referencing* column of a FK. As a result, deleting any
-- node forced a sequential scan of the whole table to verify that no
-- surviving row still references it via parent_node_id.
--
-- With ~750k rows that turned every reload DELETE (on_hand, PO, CO,
-- transfers — all "full-reload of nodes of this type") into an O(n²)
-- operation: a single on_hand reload (~16k rows) was observed taking
-- 463s (and the orphan-ProjectedInventory purge hung for 30+ minutes).
-- After adding this index the same on_hand reload dropped to ~8s and the
-- full daily_load (load + LLC + cost roll-up + validate) to ~123s.
--
-- Schema impact:
-- - Partial btree on parent_node_id WHERE parent_node_id IS NOT NULL
--   (the vast majority of rows have a NULL parent; only linked nodes
--   carry one). Keeps the index small while still covering the FK
--   referential-integrity probe `... WHERE parent_node_id = $deleted_id`
--   (that predicate only matches non-null values).
-- - Also accelerates any pegging / parent-child traversal that filters
--   by parent_node_id.
--
-- Note: created non-CONCURRENTLY to stay inside the migration
-- transaction, consistent with the other migrations in this tree. It
-- takes a brief SHARE lock on `nodes` during the build; run during a
-- maintenance window if the table is large and under live write load.
-- ============================================================

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'nodes'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_nodes_parent_node_id
            ON nodes (parent_node_id)
            WHERE parent_node_id IS NOT NULL;

        COMMENT ON INDEX idx_nodes_parent_node_id IS
            'Supports the nodes_parent_node_id_fkey self-FK referential '
            'check so DELETE FROM nodes does not seq-scan the table per '
            'deleted row. Partial (parent_node_id IS NOT NULL) to stay '
            'small. See migration 046.';
    END IF;
END $$;
