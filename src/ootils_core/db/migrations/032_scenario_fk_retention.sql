-- ============================================================
-- Migration 032: Make scenario FK retention policy explicit
-- ============================================================
-- Context
--   Migration 015 documents the intent: "ON DELETE CASCADE on scenarios
--   intentionally NOT added. Ootils uses soft-delete pattern (status='archived').
--   Hard-deleting a scenario should be a deliberate admin operation."
--
--   That intent has been carried by PostgreSQL's default (NO ACTION), which
--   behaves like RESTRICT but is silent at the schema level. This migration
--   replaces every FK pointing at scenarios(scenario_id) with an explicit
--   ON DELETE RESTRICT. No behavioral change — pure clarification.
--
--   It also fixes a missing FK on mrp_runs.scenario_id (migration 021 created
--   the column UUID NOT NULL but forgot the REFERENCES clause), which left
--   the table referentially unprotected.
--
-- See: docs/ADR-011-scenario-retention.md
-- ============================================================

BEGIN;

-- ============================================================
-- 1. Rewrite every FK on scenarios(scenario_id) with ON DELETE RESTRICT
-- ============================================================
-- Discover all FKs pointing at scenarios via pg_constraint, drop and recreate
-- each with explicit RESTRICT. Idempotent: a re-run finds no NO_ACTION FKs
-- (they all become RESTRICT) and the loop is a no-op.

DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT
            c.conname,
            c.conrelid::regclass::text AS table_name,
            a.attname                   AS col_name,
            c.confdeltype               AS delete_action
        FROM pg_constraint c
        JOIN pg_attribute  a
          ON a.attrelid = c.conrelid
         AND a.attnum   = ANY (c.conkey)
        WHERE c.contype  = 'f'
          AND c.confrelid = 'scenarios'::regclass
          AND c.confdeltype <> 'r'  -- 'r' = RESTRICT; skip already-correct FKs
    LOOP
        EXECUTE format(
            'ALTER TABLE %I DROP CONSTRAINT %I',
            rec.table_name, rec.conname
        );
        EXECUTE format(
            'ALTER TABLE %I ADD CONSTRAINT %I '
            'FOREIGN KEY (%I) REFERENCES scenarios(scenario_id) ON DELETE RESTRICT',
            rec.table_name, rec.conname, rec.col_name
        );
    END LOOP;
END $$;

-- ============================================================
-- 2. Add the missing FK on mrp_runs.scenario_id
-- ============================================================
-- Migration 021 declared mrp_runs.scenario_id UUID NOT NULL but omitted
-- REFERENCES scenarios(scenario_id). This adds the FK with the same RESTRICT
-- policy as the rest of the schema.
--
-- Idempotent: ADD CONSTRAINT IF NOT EXISTS isn't supported for FKs, so we
-- guard with pg_constraint lookup.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_attribute  a
          ON a.attrelid = c.conrelid
         AND a.attnum   = ANY (c.conkey)
        WHERE c.contype  = 'f'
          AND c.conrelid = 'mrp_runs'::regclass
          AND c.confrelid = 'scenarios'::regclass
          AND a.attname  = 'scenario_id'
    ) THEN
        ALTER TABLE mrp_runs
            ADD CONSTRAINT mrp_runs_scenario_id_fkey
            FOREIGN KEY (scenario_id) REFERENCES scenarios(scenario_id)
            ON DELETE RESTRICT;
    END IF;
END $$;

COMMIT;
