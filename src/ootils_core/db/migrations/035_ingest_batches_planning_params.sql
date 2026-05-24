-- ============================================================
-- Ootils Core — Migration 035: allow 'planning_params' in ingest_batches.entity_type
--
-- POST /v1/ingest/planning-params (ADR-014 D3) creates an
-- ingest_batches row with entity_type='planning_params'. The CHECK
-- constraint from migration 023 doesn't include this value, so
-- the INSERT was rejected with check_violation.
--
-- Idempotent: drop the existing constraint (any name found by lookup)
-- and re-add it with the extended enum.
-- ============================================================

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ingest_batches_entity_type_check'
          AND conrelid = 'ingest_batches'::regclass
    ) THEN
        ALTER TABLE ingest_batches DROP CONSTRAINT ingest_batches_entity_type_check;
    END IF;
END $$;

ALTER TABLE ingest_batches
    ADD CONSTRAINT ingest_batches_entity_type_check
    CHECK (
        entity_type IN (
            'items', 'locations', 'suppliers', 'supplier_items',
            'purchase_orders', 'customer_orders', 'forecasts',
            'work_orders', 'transfers', 'on_hand', 'resources',
            'planning_params'
        )
    );
