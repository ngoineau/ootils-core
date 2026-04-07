-- ============================================================
-- Ootils Core — Migration 008: Bill of Materials
-- BOM minimale fonctionnelle pour le MRP
-- ============================================================

-- Table bom_headers : en-tête BOM par item parent
CREATE TABLE IF NOT EXISTS bom_headers (
    bom_id          UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_item_id  UUID        NOT NULL REFERENCES items(item_id),
    bom_version     TEXT        NOT NULL DEFAULT '1.0',
    effective_from  DATE        NOT NULL DEFAULT CURRENT_DATE,
    effective_to    DATE,
    status          TEXT        NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'inactive')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (parent_item_id, bom_version)
);

-- Table bom_lines : composants de la BOM
CREATE TABLE IF NOT EXISTS bom_lines (
    line_id             UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    bom_id              UUID        NOT NULL REFERENCES bom_headers(bom_id) ON DELETE CASCADE,
    component_item_id   UUID        NOT NULL REFERENCES items(item_id),
    quantity_per        NUMERIC     NOT NULL CHECK (quantity_per > 0),
    uom                 TEXT        NOT NULL DEFAULT 'EA',
    scrap_factor        NUMERIC     NOT NULL DEFAULT 0.0
                        CHECK (scrap_factor >= 0 AND scrap_factor < 1),
    -- LLC = Low-Level Code : niveau le plus bas où apparaît ce composant dans toutes les BOMs
    -- Calculé et mis à jour après chaque import BOM
    llc                 INTEGER     NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bom_id, component_item_id)
);

CREATE INDEX IF NOT EXISTS idx_bom_lines_component ON bom_lines (component_item_id);
CREATE INDEX IF NOT EXISTS idx_bom_headers_parent  ON bom_headers (parent_item_id);
