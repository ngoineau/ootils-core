-- ============================================================
-- Ootils Core — Migration 017: Add severity_class to shortages
-- Issue #122: Safety stock detection in kernel
-- ============================================================

ALTER TABLE shortages
    ADD COLUMN IF NOT EXISTS severity_class TEXT
        CHECK (severity_class IN ('stockout', 'below_safety_stock'));
