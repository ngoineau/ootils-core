-- ============================================================
-- Migration 016: updated_at auto-maintenance triggers
-- ============================================================
-- Ensures all tables with an updated_at column keep it current
-- automatically on every UPDATE, eliminating the need for
-- application-layer timestamp management.
--
-- Pattern: CREATE OR REPLACE FUNCTION + DROP/CREATE TRIGGER per table.
-- Triggers are idempotent: DROP IF EXISTS before CREATE.
-- Each table is wrapped in an existence check so this migration
-- is safe to run against partial schemas or test databases.
-- ============================================================

-- ============================================================
-- Shared trigger function (idempotent via CREATE OR REPLACE)
-- ============================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Apply trigger to each table with updated_at
-- ============================================================

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'scenarios') THEN
        DROP TRIGGER IF EXISTS trg_scenarios_updated_at ON scenarios;
        CREATE TRIGGER trg_scenarios_updated_at
            BEFORE UPDATE ON scenarios
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'items') THEN
        DROP TRIGGER IF EXISTS trg_items_updated_at ON items;
        CREATE TRIGGER trg_items_updated_at
            BEFORE UPDATE ON items
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'nodes') THEN
        DROP TRIGGER IF EXISTS trg_nodes_updated_at ON nodes;
        CREATE TRIGGER trg_nodes_updated_at
            BEFORE UPDATE ON nodes
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'projection_series') THEN
        DROP TRIGGER IF EXISTS trg_projection_series_updated_at ON projection_series;
        CREATE TRIGGER trg_projection_series_updated_at
            BEFORE UPDATE ON projection_series
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'node_type_policies') THEN
        DROP TRIGGER IF EXISTS trg_node_type_policies_updated_at ON node_type_policies;
        CREATE TRIGGER trg_node_type_policies_updated_at
            BEFORE UPDATE ON node_type_policies
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'events') THEN
        DROP TRIGGER IF EXISTS trg_events_updated_at ON events;
        CREATE TRIGGER trg_events_updated_at
            BEFORE UPDATE ON events
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'shortages') THEN
        DROP TRIGGER IF EXISTS trg_shortages_updated_at ON shortages;
        CREATE TRIGGER trg_shortages_updated_at
            BEFORE UPDATE ON shortages
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'external_references') THEN
        DROP TRIGGER IF EXISTS trg_external_references_updated_at ON external_references;
        CREATE TRIGGER trg_external_references_updated_at
            BEFORE UPDATE ON external_references
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'suppliers') THEN
        DROP TRIGGER IF EXISTS trg_suppliers_updated_at ON suppliers;
        CREATE TRIGGER trg_suppliers_updated_at
            BEFORE UPDATE ON suppliers
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'item_planning_params') THEN
        DROP TRIGGER IF EXISTS trg_item_planning_params_updated_at ON item_planning_params;
        CREATE TRIGGER trg_item_planning_params_updated_at
            BEFORE UPDATE ON item_planning_params
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'resources') THEN
        DROP TRIGGER IF EXISTS trg_resources_updated_at ON resources;
        CREATE TRIGGER trg_resources_updated_at
            BEFORE UPDATE ON resources
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'ghost_nodes') THEN
        DROP TRIGGER IF EXISTS trg_ghost_nodes_updated_at ON ghost_nodes;
        CREATE TRIGGER trg_ghost_nodes_updated_at
            BEFORE UPDATE ON ghost_nodes
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'ghost_members') THEN
        DROP TRIGGER IF EXISTS trg_ghost_members_updated_at ON ghost_members;
        CREATE TRIGGER trg_ghost_members_updated_at
            BEFORE UPDATE ON ghost_members
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;
