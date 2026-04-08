-- ============================================================
-- Migration 010: DQ Engine V1 — add dq_status to ingest_batches
-- ============================================================

-- Add dq_status column to ingest_batches (tracks pipeline outcome)
ALTER TABLE ingest_batches ADD COLUMN IF NOT EXISTS dq_status TEXT
    CHECK (dq_status IN ('pending', 'running', 'validated', 'rejected', 'warning'));

-- Index for DQ status queries
CREATE INDEX IF NOT EXISTS idx_ingest_batches_dq_status
    ON ingest_batches (dq_status, entity_type);

-- Index for fast issue lookups by rule_code
CREATE INDEX IF NOT EXISTS idx_dq_issues_rule_code
    ON data_quality_issues (rule_code, severity, resolved);
