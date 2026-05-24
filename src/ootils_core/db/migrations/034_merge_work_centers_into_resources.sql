-- ============================================================
-- Ootils Core — Migration 034: Fusion work_centers → resources (ADR-014)
--
-- Implements:
--   D1 — Single `resources` table replaces `work_centers`.
--        resource_type enum extended with 'work_center'. Existing
--        UUIDs are preserved so all downstream FKs (and any external
--        references) remain valid.
--   D2 — capacity_unit / time_unit ENUM CHECK constraint:
--        only 'unit' and 'minute' allowed in DB. 'hour' is converted
--        to 'minute' at the ingest layer (×60), not in DB.
--   D2 — `routing_operations.time_unit` added (default 'unit').
--
-- DROP TABLE rationale (CLAUDE.md "migrations are idempotent" carve-out):
--   DROP TABLE work_centers below is a one-shot consolidation per
--   ADR-014. Data is migrated into the unified `resources` table
--   first, preserving UUIDs. Subsequent migrations rely on
--   `resources` as the single source of truth. Re-running 034 after
--   the fact is a no-op because every ALTER uses IF NOT EXISTS /
--   IF EXISTS, the INSERT FROM is wrapped in a DO $$ guard on
--   work_centers existence, and DROP TABLE IF EXISTS is the final step.
-- ============================================================

-- ------------------------------------------------------------
-- 1. Enrich resources schema (efficiency, calendar_id, active)
-- ------------------------------------------------------------

ALTER TABLE resources
    ADD COLUMN IF NOT EXISTS efficiency NUMERIC(5,4) NOT NULL DEFAULT 1.0,
    ADD COLUMN IF NOT EXISTS calendar_id UUID,
    ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE;

-- Efficiency constraint (drop+recreate for idempotence)
ALTER TABLE resources DROP CONSTRAINT IF EXISTS chk_resources_efficiency;
ALTER TABLE resources ADD CONSTRAINT chk_resources_efficiency
    CHECK (efficiency >= 0 AND efficiency <= 1);

-- ------------------------------------------------------------
-- 2. Normalize capacity_unit + enforce ENUM ('unit' | 'minute')
-- ------------------------------------------------------------

-- Backfill legacy values BEFORE adding the CHECK constraint
UPDATE resources SET capacity_unit = 'unit'   WHERE capacity_unit IN ('units', 'unit', '');
UPDATE resources SET capacity_unit = 'minute' WHERE capacity_unit IN ('hour', 'hours', 'minutes', 'min', 'mn');

-- Anything else gets caught — fail loudly rather than silently coerce
-- (e.g. 'kg', 'tonne' etc. are not supported as a capacity unit).
DO $$
DECLARE
    bad_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO bad_count
    FROM resources
    WHERE capacity_unit NOT IN ('unit', 'minute');
    IF bad_count > 0 THEN
        RAISE EXCEPTION
            'Migration 034: % resources row(s) have capacity_unit outside the new enum (unit|minute). '
            'Please normalize before re-running.', bad_count;
    END IF;
END $$;

ALTER TABLE resources ALTER COLUMN capacity_unit SET DEFAULT 'unit';

ALTER TABLE resources DROP CONSTRAINT IF EXISTS chk_resources_capacity_unit;
ALTER TABLE resources ADD CONSTRAINT chk_resources_capacity_unit
    CHECK (capacity_unit IN ('unit', 'minute'));

-- ------------------------------------------------------------
-- 3. Extend resource_type enum with 'work_center'
-- ------------------------------------------------------------
--
-- The original CHECK constraint was unnamed (anonymous, auto-named
-- by PG). We look it up by definition and drop it before adding the
-- new named constraint.

DO $$
DECLARE
    old_constraint_name TEXT;
BEGIN
    SELECT conname INTO old_constraint_name
    FROM pg_constraint
    WHERE conrelid = 'resources'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%resource_type%'
      AND conname <> 'chk_resources_resource_type'  -- not the one we add below
    LIMIT 1;

    IF old_constraint_name IS NOT NULL THEN
        EXECUTE 'ALTER TABLE resources DROP CONSTRAINT ' || quote_ident(old_constraint_name);
    END IF;
END $$;

ALTER TABLE resources DROP CONSTRAINT IF EXISTS chk_resources_resource_type;
ALTER TABLE resources ADD CONSTRAINT chk_resources_resource_type
    CHECK (resource_type IN ('machine', 'line', 'team', 'tool', 'work_center'));

-- ------------------------------------------------------------
-- 4. Migrate work_centers data into resources (preserve UUIDs)
-- ------------------------------------------------------------
--
-- Each row in work_centers becomes a resources row with the same UUID.
-- This is what keeps every FK pointing into work_centers valid after
-- the rename in step 5.
--
-- Default capacity_unit for migrated rows is 'unit' — ADR-014 D2 says
-- existing work_centers are presumed to be in the 'unit' world unless
-- the operator explicitly migrates them to 'minute' (manual step,
-- out of scope here). The pattern matches APICS default.

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = 'public' AND table_name = 'work_centers') THEN
        INSERT INTO resources (
            resource_id,
            external_id,
            name,
            resource_type,
            location_id,
            capacity_per_day,
            capacity_unit,
            efficiency,
            calendar_id,
            active,
            created_at,
            updated_at,
            notes
        )
        SELECT
            wc.work_center_id,
            wc.code,
            COALESCE(wc.description, wc.code),
            'work_center',
            NULL,  -- work_centers had no location_id; can be backfilled later
            wc.capacity_per_day,
            'unit',
            wc.efficiency,
            wc.calendar_id,
            wc.active,
            wc.created_at,
            wc.updated_at,
            NULL
        FROM work_centers wc
        ON CONFLICT (resource_id) DO NOTHING;
    END IF;
END $$;

-- ------------------------------------------------------------
-- 5. routing_operations: rename FK + add time_unit
-- ------------------------------------------------------------

-- 5a. Add time_unit column (ADR-014 D2)
ALTER TABLE routing_operations
    ADD COLUMN IF NOT EXISTS time_unit TEXT NOT NULL DEFAULT 'unit';

ALTER TABLE routing_operations DROP CONSTRAINT IF EXISTS chk_routing_operations_time_unit;
ALTER TABLE routing_operations ADD CONSTRAINT chk_routing_operations_time_unit
    CHECK (time_unit IN ('unit', 'minute'));

-- 5b. Rename column work_center_id → resource_id (idempotent)
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'routing_operations'
          AND column_name = 'work_center_id'
    ) THEN
        ALTER TABLE routing_operations RENAME COLUMN work_center_id TO resource_id;
    END IF;
END $$;

-- 5c. Drop old FK to work_centers (any name) and add FK to resources
DO $$
DECLARE
    old_fk_name TEXT;
BEGIN
    SELECT c.conname INTO old_fk_name
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'routing_operations'
      AND c.contype = 'f'
      AND pg_get_constraintdef(c.oid) LIKE '%REFERENCES work_centers%'
    LIMIT 1;

    IF old_fk_name IS NOT NULL THEN
        EXECUTE 'ALTER TABLE routing_operations DROP CONSTRAINT ' || quote_ident(old_fk_name);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'routing_operations'
          AND c.contype = 'f'
          AND pg_get_constraintdef(c.oid) LIKE '%REFERENCES resources%'
    ) THEN
        ALTER TABLE routing_operations
            ADD CONSTRAINT fk_routing_operations_resource
            FOREIGN KEY (resource_id) REFERENCES resources(resource_id);
    END IF;
END $$;

-- ------------------------------------------------------------
-- 6. work_center_calendar_edges: rename FK column + rebase FK
-- ------------------------------------------------------------
--
-- This table is currently unread by the engine (ADR-014 §Ouvertures
-- flags it as a follow-up). We keep the table but rebase its FK to
-- resources so the work_centers DROP at the end doesn't fail.

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'work_center_calendar_edges'
          AND column_name = 'work_center_id'
    ) THEN
        ALTER TABLE work_center_calendar_edges RENAME COLUMN work_center_id TO resource_id;
    END IF;
END $$;

DO $$
DECLARE
    old_fk_name TEXT;
BEGIN
    SELECT c.conname INTO old_fk_name
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'work_center_calendar_edges'
      AND c.contype = 'f'
      AND pg_get_constraintdef(c.oid) LIKE '%REFERENCES work_centers%'
    LIMIT 1;

    IF old_fk_name IS NOT NULL THEN
        EXECUTE 'ALTER TABLE work_center_calendar_edges DROP CONSTRAINT ' || quote_ident(old_fk_name);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'work_center_calendar_edges'
          AND c.contype = 'f'
          AND pg_get_constraintdef(c.oid) LIKE '%REFERENCES resources%'
    ) THEN
        ALTER TABLE work_center_calendar_edges
            ADD CONSTRAINT fk_wcc_edges_resource
            FOREIGN KEY (resource_id) REFERENCES resources(resource_id) ON DELETE CASCADE;
    END IF;
END $$;

-- ------------------------------------------------------------
-- 7. routing_requires_capacity_edges: rename FK column + rebase FK
-- ------------------------------------------------------------

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'routing_requires_capacity_edges'
          AND column_name = 'work_center_id'
    ) THEN
        ALTER TABLE routing_requires_capacity_edges RENAME COLUMN work_center_id TO resource_id;
    END IF;
END $$;

DO $$
DECLARE
    old_fk_name TEXT;
BEGIN
    SELECT c.conname INTO old_fk_name
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'routing_requires_capacity_edges'
      AND c.contype = 'f'
      AND pg_get_constraintdef(c.oid) LIKE '%REFERENCES work_centers%'
    LIMIT 1;

    IF old_fk_name IS NOT NULL THEN
        EXECUTE 'ALTER TABLE routing_requires_capacity_edges DROP CONSTRAINT ' || quote_ident(old_fk_name);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'routing_requires_capacity_edges'
          AND c.contype = 'f'
          AND pg_get_constraintdef(c.oid) LIKE '%REFERENCES resources%'
    ) THEN
        ALTER TABLE routing_requires_capacity_edges
            ADD CONSTRAINT fk_rrc_edges_resource
            FOREIGN KEY (resource_id) REFERENCES resources(resource_id);
    END IF;
END $$;

-- ------------------------------------------------------------
-- 8. Drop work_centers (one-shot, see header carve-out)
-- ------------------------------------------------------------

DROP TABLE IF EXISTS work_centers;
