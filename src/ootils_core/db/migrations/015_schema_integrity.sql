-- ============================================================
-- Migration 015: Schema integrity — constraints and indexes
-- ============================================================
-- Fixes broken UNIQUE constraints, adds missing CHECK constraints,
-- and addresses UUID default gaps identified in DBA review #131.
--
-- NOTE: ON DELETE CASCADE on scenarios intentionally NOT added.
-- Ootils uses soft-delete pattern (status='archived'). Hard-deleting a scenario
-- should be a deliberate admin operation with explicit table cleanup.
-- See docs/DBA-RUNBOOK.md for manual scenario cleanup procedure.
-- ============================================================

-- ============================================================
-- 1. Fix node_type_policies UNIQUE constraint
-- ============================================================
-- Current UNIQUE(node_type, active) prevents multiple inactive policies
-- for the same node_type (only one row with active=FALSE allowed per type).
-- The business rule is: at most one ACTIVE policy per node_type.
-- Replace the pair-wise UNIQUE with a partial unique index.

ALTER TABLE node_type_policies
    DROP CONSTRAINT IF EXISTS node_type_policies_node_type_active_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_node_type_policies_active
    ON node_type_policies (node_type) WHERE active = TRUE;

-- ============================================================
-- 2. Fix uom_conversions NULL duplicate issue
-- ============================================================
-- NULL != NULL in standard SQL UNIQUE, so global conversions (item_id IS NULL)
-- can be inserted multiple times on re-run without triggering uniqueness errors.
-- Deduplicate first (keep the row with the highest conversion_id), then add
-- a partial unique index covering only global (item_id IS NULL) rows.

DELETE FROM uom_conversions
WHERE item_id IS NULL
  AND ctid NOT IN (
      SELECT MAX(ctid)
      FROM uom_conversions
      WHERE item_id IS NULL
      GROUP BY from_uom, to_uom
  );

CREATE UNIQUE INDEX IF NOT EXISTS uq_uom_global
    ON uom_conversions (from_uom, to_uom) WHERE item_id IS NULL;

-- ============================================================
-- 3. Missing edge uniqueness constraint
-- ============================================================
-- Prevents duplicate (from_node_id, to_node_id, edge_type, scenario_id) active edges.
-- Uses partial unique index so soft-deleted (active=FALSE) edges are excluded,
-- allowing re-activation patterns.

CREATE UNIQUE INDEX IF NOT EXISTS uq_edges_composite
    ON edges (from_node_id, to_node_id, edge_type, scenario_id)
    WHERE active = TRUE;

-- ============================================================
-- 4. CHECK constraints for date ordering
-- ============================================================
-- Prevent valid_to <= valid_from and effective_to <= effective_from.
-- Uses exception handler for idempotency (constraint already exists = no-op).

DO $$ BEGIN
    BEGIN
        ALTER TABLE supplier_items
            ADD CONSTRAINT chk_supplier_items_dates
            CHECK (valid_to IS NULL OR valid_to > valid_from);
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
END $$;

DO $$ BEGIN
    BEGIN
        ALTER TABLE bom_headers
            ADD CONSTRAINT chk_bom_headers_dates
            CHECK (effective_to IS NULL OR effective_to > effective_from);
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
END $$;

-- ============================================================
-- 5. resource_capacity_overrides: no negative capacity
-- ============================================================
-- Table may not exist in all environments (created later in the migration chain
-- or in some deploy configurations).

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public'
                 AND table_name = 'resource_capacity_overrides') THEN
        BEGIN
            ALTER TABLE resource_capacity_overrides
                ADD CONSTRAINT chk_capacity_nonneg CHECK (capacity >= 0);
        EXCEPTION WHEN duplicate_object THEN NULL;
        END;
    END IF;
END $$;

-- ============================================================
-- 6. Fix explanations/causal_steps missing DEFAULT uuid
-- ============================================================
-- If the PK columns were created without DEFAULT gen_random_uuid(),
-- application code that omits the PK on INSERT will fail.
-- Only alters if DEFAULT is currently absent.

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'explanations'
          AND column_name  = 'explanation_id'
          AND column_default IS NULL
    ) THEN
        ALTER TABLE explanations
            ALTER COLUMN explanation_id SET DEFAULT gen_random_uuid();
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'causal_steps'
          AND column_name  = 'step_id'
          AND column_default IS NULL
    ) THEN
        ALTER TABLE causal_steps
            ALTER COLUMN step_id SET DEFAULT gen_random_uuid();
    END IF;
END $$;
