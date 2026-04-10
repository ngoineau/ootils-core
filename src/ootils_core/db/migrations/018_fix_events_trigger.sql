-- Migration 018: fix events table — remove updated_at trigger, add updated_at column
--
-- Migration 016 created trg_events_updated_at but the events table has no
-- updated_at column. Every INSERT/UPDATE on events caused:
--   record "new" has no field "updated_at"
-- which made simulate/propagation fail silently.
--
-- Fix: add updated_at column to events (defaulting to created_at for existing rows),
-- then the existing trigger in migration 016 will work correctly.

ALTER TABLE events
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Backfill existing rows: set updated_at = created_at
UPDATE events SET updated_at = created_at WHERE updated_at IS DISTINCT FROM created_at;
