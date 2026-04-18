-- ============================================================
-- Ootils Core — Migration 023: API audit log + ingest idempotency metadata
-- ============================================================

ALTER TABLE ingest_batches
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT,
    ADD COLUMN IF NOT EXISTS request_hash TEXT,
    ADD COLUMN IF NOT EXISTS correlation_id TEXT,
    ADD COLUMN IF NOT EXISTS response_json TEXT;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
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
            'work_orders', 'transfers', 'on_hand', 'resources'
        )
    );

CREATE UNIQUE INDEX IF NOT EXISTS idx_ingest_batches_idempotency_key
    ON ingest_batches (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ingest_batches_correlation_id
    ON ingest_batches (correlation_id)
    WHERE correlation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS api_request_log (
    request_id      UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    correlation_id  TEXT,
    token_prefix    TEXT,
    method          TEXT        NOT NULL,
    path            TEXT        NOT NULL,
    status_code     INTEGER     NOT NULL,
    latency_ms      INTEGER     NOT NULL,
    client_ip       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_api_request_log_created_at
    ON api_request_log (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_api_request_log_correlation_id
    ON api_request_log (correlation_id)
    WHERE correlation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_api_request_log_path_created_at
    ON api_request_log (path, created_at DESC);
