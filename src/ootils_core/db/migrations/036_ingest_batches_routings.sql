-- ============================================================
-- Ootils Core — Migration 036: allow 'routings' in ingest_batches.entity_type
--
-- POST /v1/ingest/routings (ADR-014 D2 — Phase F) creates an
-- ingest_batches row with entity_type='routings'. Migration 035
-- only added 'planning_params'. This one adds 'routings' too.
--
-- Idempotent: DROP+ADD via existing constraint name lookup.
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
            'planning_params', 'routings'
        )
    );
