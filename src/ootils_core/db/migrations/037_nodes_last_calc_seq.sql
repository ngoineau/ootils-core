-- ============================================================
-- Migration 037: nodes.last_calc_seq column for write-behind seq guard
-- ============================================================
-- Purpose: prevent the Rust engine's write-behind queue from
-- clobbering newer Postgres state with older WAL-replayed deltas.
--
-- Background (audit findings F-001/F-014, Cluster A response):
-- The Architecture-B Rust engine appends every propagation result to
-- a local WAL (with a monotonic sequence number) and asynchronously
-- bulk-UPDATEs Postgres. If the engine crashes after writing the WAL
-- but before Postgres has caught up, the recovered records are
-- re-flushed on boot. WITHOUT this column, an older WAL-replayed
-- value could silently overwrite a newer in-PG value (e.g. one
-- written by the SQL engine during a mixed-mode canary).
--
-- The seq-guard:
--   UPDATE nodes SET ... WHERE last_calc_seq IS NULL OR last_calc_seq < $seq
-- causes the older replay to be a no-op. Every successful write
-- advances last_calc_seq.
--
-- Schema impact:
-- - Nullable bigint column. NULL = node never written by rust-svc
--   engine (still updateable freely by SQL/Python engines until the
--   first rust-svc write claims it).
-- - No index needed: the column is checked only in the WHERE clause
--   of the rust-svc bulk UPDATE, and that UPDATE is already keyed by
--   node_id (PK), so PG uses the PK lookup and then checks the seq
--   value cheaply on the matched row.
--
-- Safe to apply concurrently with running engines: ALTER TABLE ADD
-- COLUMN with a NULL default takes a brief schema lock but does not
-- rewrite the table on PG 11+.
-- ============================================================

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'nodes'
    ) THEN
        ALTER TABLE nodes
            ADD COLUMN IF NOT EXISTS last_calc_seq bigint;

        COMMENT ON COLUMN nodes.last_calc_seq IS
            'Monotonic sequence number of the latest write-behind flush '
            'from the Rust engine service (Architecture B). Used as a '
            'WHERE-guard to prevent older WAL-replayed values from '
            'overwriting newer state. NULL = never written by rust-svc.';
    END IF;
END $$;
