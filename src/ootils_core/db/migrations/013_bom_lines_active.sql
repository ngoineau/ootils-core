-- ============================================================
-- Ootils Core — Migration 013: Add active column to bom_lines
-- Enables soft-delete of removed BOM components on re-ingest
-- ============================================================

ALTER TABLE bom_lines
    ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE bom_lines
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_bom_lines_active ON bom_lines (bom_id, active) WHERE active = TRUE;
