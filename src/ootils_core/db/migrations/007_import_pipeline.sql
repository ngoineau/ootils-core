-- ============================================================
-- Ootils Core — Migration 007: Import Pipeline 2 étapes
-- Staging zone, DQ pipeline, données techniques SC, master data audit
--
-- Conventions :
--   - Tous les PKs sont UUID
--   - Tous les timestamps sont TIMESTAMPTZ UTC
--   - IF NOT EXISTS partout pour idempotence
--   - CREATE TYPE via DO $$ pour idempotence (PostgreSQL < 16)
-- ============================================================


-- ============================================================
-- 0. Extensions requises
-- ============================================================

-- btree_gist est nécessaire pour la contrainte EXCLUDE USING gist
-- sur des colonnes non-géométriques (item_id, location_id, daterange).
CREATE EXTENSION IF NOT EXISTS btree_gist;


-- ============================================================
-- 1. external_id sur items et locations (backfill safe)
-- ============================================================

ALTER TABLE items ADD COLUMN IF NOT EXISTS external_id TEXT;
UPDATE items SET external_id = item_id::TEXT WHERE external_id IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'items_external_id_unique'
          AND conrelid = 'items'::regclass
    ) THEN
        ALTER TABLE items ADD CONSTRAINT items_external_id_unique UNIQUE (external_id);
    END IF;
END $$;

ALTER TABLE locations ADD COLUMN IF NOT EXISTS external_id TEXT;
UPDATE locations SET external_id = location_id::TEXT WHERE external_id IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'locations_external_id_unique'
          AND conrelid = 'locations'::regclass
    ) THEN
        ALTER TABLE locations ADD CONSTRAINT locations_external_id_unique UNIQUE (external_id);
    END IF;
END $$;


-- ============================================================
-- 2. Table external_references — mapping ERP codes → UUID internes
-- ============================================================

CREATE TABLE IF NOT EXISTS external_references (
    ref_id          UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type     TEXT        NOT NULL CHECK (entity_type IN (
                        'item', 'location', 'supplier', 'purchase_order',
                        'customer_order', 'work_order', 'transfer', 'forecast'
                    )),
    external_id     TEXT        NOT NULL,
    source_system   TEXT        NOT NULL,
    internal_id     UUID        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (entity_type, external_id, source_system)
);


-- ============================================================
-- 3. Tables staging — ingest_batches + ingest_rows
-- ============================================================

CREATE TABLE IF NOT EXISTS ingest_batches (
    batch_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type     TEXT        NOT NULL CHECK (entity_type IN (
                        'items', 'locations', 'suppliers', 'supplier_items',
                        'purchase_orders', 'customer_orders', 'forecasts',
                        'work_orders', 'transfers', 'on_hand'
                    )),
    source_system   TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending' CHECK (status IN (
                        'pending', 'processing', 'validated', 'rejected',
                        'importing', 'imported', 'partial'
                    )),
    total_rows      INTEGER,
    valid_rows      INTEGER,
    error_rows      INTEGER,
    warning_rows    INTEGER,
    submitted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ,
    imported_at     TIMESTAMPTZ,
    submitted_by    TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS ingest_rows (
    row_id          UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID        NOT NULL REFERENCES ingest_batches(batch_id),
    row_number      INTEGER     NOT NULL,
    raw_content     TEXT        NOT NULL,
    -- Colonnes extraites brutes (TEXT partout — pas de conversion à ce stade)
    col_01 TEXT, col_02 TEXT, col_03 TEXT, col_04 TEXT, col_05 TEXT,
    col_06 TEXT, col_07 TEXT, col_08 TEXT, col_09 TEXT, col_10 TEXT,
    col_11 TEXT, col_12 TEXT, col_13 TEXT, col_14 TEXT, col_15 TEXT,
    -- Statut DQ
    dq_status       TEXT        NOT NULL DEFAULT 'pending' CHECK (dq_status IN (
                        'pending', 'l1_pass', 'l2_pass', 'l3_pass', 'l4_pass',
                        'rejected', 'imported'
                    )),
    dq_level_reached INTEGER    DEFAULT 0,
    -- Metadata
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, row_number)
);

CREATE INDEX IF NOT EXISTS idx_ingest_rows_batch ON ingest_rows (batch_id, dq_status);
CREATE INDEX IF NOT EXISTS idx_ingest_batches_status ON ingest_batches (status, entity_type);


-- ============================================================
-- 4. Table data_quality_issues
-- ============================================================

CREATE TABLE IF NOT EXISTS data_quality_issues (
    issue_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID        NOT NULL REFERENCES ingest_batches(batch_id),
    row_id          UUID        REFERENCES ingest_rows(row_id),
    row_number      INTEGER,
    dq_level        INTEGER     NOT NULL CHECK (dq_level BETWEEN 1 AND 4),
    rule_code       TEXT        NOT NULL,
    severity        TEXT        NOT NULL CHECK (severity IN ('error', 'warning', 'info')),
    field_name      TEXT,
    raw_value       TEXT,
    message         TEXT        NOT NULL,
    auto_corrected  BOOLEAN     NOT NULL DEFAULT FALSE,
    correction_detail TEXT,
    resolved        BOOLEAN     NOT NULL DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dq_issues_batch ON data_quality_issues (batch_id, severity, resolved);


-- ============================================================
-- 5. Tables suppliers et supplier_items
-- ============================================================

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id     UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id     TEXT        UNIQUE,
    name            TEXT        NOT NULL,
    country         TEXT,
    lead_time_days  INTEGER     CHECK (lead_time_days > 0),
    reliability_score NUMERIC(4,3) CHECK (reliability_score BETWEEN 0 AND 1),
    status          TEXT        NOT NULL DEFAULT 'active' CHECK (status IN (
                        'active', 'inactive', 'blocked'
                    )),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS supplier_items (
    supplier_item_id UUID       NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_id     UUID        NOT NULL REFERENCES suppliers(supplier_id),
    item_id         UUID        NOT NULL REFERENCES items(item_id),
    lead_time_days  INTEGER     NOT NULL CHECK (lead_time_days > 0),
    moq             NUMERIC     CHECK (moq > 0),
    unit_cost       NUMERIC,
    currency        TEXT        DEFAULT 'EUR',
    is_preferred    BOOLEAN     NOT NULL DEFAULT FALSE,
    valid_from      DATE,
    valid_to        DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (supplier_id, item_id)
);


-- ============================================================
-- 6. Types ENUM pour item_planning_params
-- ============================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'lot_size_rule_type') THEN
        CREATE TYPE lot_size_rule_type AS ENUM (
            'LOTFORLOT', 'FIXED_QTY', 'PERIOD_OF_SUPPLY', 'MIN_MAX'
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'planning_source_type') THEN
        CREATE TYPE planning_source_type AS ENUM (
            'erp', 'manual', 'ai_suggested', 'default'
        );
    END IF;
END $$;


-- ============================================================
-- 7. Table item_planning_params — données techniques versionnées SCD2
-- ============================================================

CREATE TABLE IF NOT EXISTS item_planning_params (
    param_id                    UUID                NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id                     UUID                NOT NULL REFERENCES items(item_id),
    location_id                 UUID                NOT NULL REFERENCES locations(location_id),

    -- Lead times
    lead_time_sourcing_days     INTEGER             CHECK (lead_time_sourcing_days >= 0),
    lead_time_manufacturing_days INTEGER            CHECK (lead_time_manufacturing_days >= 0),
    lead_time_transit_days      INTEGER             CHECK (lead_time_transit_days >= 0),
    lead_time_total_days        INTEGER             GENERATED ALWAYS AS (
                                    COALESCE(lead_time_sourcing_days, 0) +
                                    COALESCE(lead_time_manufacturing_days, 0) +
                                    COALESCE(lead_time_transit_days, 0)
                                ) STORED,

    -- Safety stock
    safety_stock_qty            NUMERIC             CHECK (safety_stock_qty >= 0),
    safety_stock_days           NUMERIC             CHECK (safety_stock_days >= 0),

    -- Reorder
    reorder_point_qty           NUMERIC             CHECK (reorder_point_qty >= 0),
    min_order_qty               NUMERIC             CHECK (min_order_qty > 0),
    max_order_qty               NUMERIC             CHECK (max_order_qty > 0),
    order_multiple              NUMERIC             CHECK (order_multiple > 0),

    -- Policy
    lot_size_rule               lot_size_rule_type  NOT NULL DEFAULT 'LOTFORLOT',
    planning_horizon_days       INTEGER             NOT NULL DEFAULT 90 CHECK (planning_horizon_days > 0),
    is_make                     BOOLEAN             NOT NULL DEFAULT FALSE,
    preferred_supplier_id       UUID                REFERENCES suppliers(supplier_id),

    -- Versioning temporel SCD2
    effective_from              DATE                NOT NULL DEFAULT CURRENT_DATE,
    effective_to                DATE,

    -- Metadata
    source                      planning_source_type NOT NULL DEFAULT 'manual',
    created_at                  TIMESTAMPTZ         NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ         NOT NULL DEFAULT now(),

    CONSTRAINT ipp_effective_order CHECK (effective_to IS NULL OR effective_to > effective_from),

    -- Contrainte d'exclusion : pas de chevauchement temporel par (item, location)
    -- Nécessite l'extension btree_gist (activée ci-dessus)
    CONSTRAINT ipp_item_location_active_unique EXCLUDE USING gist (
        item_id WITH =,
        location_id WITH =,
        daterange(effective_from, COALESCE(effective_to, '9999-12-31'::DATE)) WITH &&
    )
);

CREATE INDEX IF NOT EXISTS idx_ipp_item_location ON item_planning_params (item_id, location_id);
CREATE INDEX IF NOT EXISTS idx_ipp_active ON item_planning_params (item_id, location_id, effective_from)
    WHERE effective_to IS NULL;


-- ============================================================
-- 8. Table uom_conversions
-- ============================================================

CREATE TABLE IF NOT EXISTS uom_conversions (
    conversion_id   UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    from_uom        TEXT        NOT NULL,
    to_uom          TEXT        NOT NULL,
    item_id         UUID        REFERENCES items(item_id),
    -- factor : 1 from_uom = factor × to_uom
    -- ex: PALLET → EA, factor=48 signifie 1 PALLET = 48 EA
    factor          NUMERIC     NOT NULL CHECK (factor > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (from_uom, to_uom, item_id)
);

-- Conversions globales de base (item_id NULL = applicable à tous les items)
INSERT INTO uom_conversions (from_uom, to_uom, item_id, factor) VALUES
    ('PALLET', 'EA',  NULL, 48),
    ('BOX',    'EA',  NULL, 12),
    ('KG',     'G',   NULL, 1000),
    ('T',      'KG',  NULL, 1000)
ON CONFLICT DO NOTHING;


-- ============================================================
-- 9. Table operational_calendars
-- ============================================================

CREATE TABLE IF NOT EXISTS operational_calendars (
    calendar_id     UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id     UUID        NOT NULL REFERENCES locations(location_id),
    calendar_date   DATE        NOT NULL,
    is_working_day  BOOLEAN     NOT NULL DEFAULT TRUE,
    shift_count     SMALLINT    DEFAULT 1 CHECK (shift_count BETWEEN 0 AND 3),
    -- capacity_factor : 1.0 = pleine capacité, 0.5 = demi-capacité, 0.0 = fermé
    capacity_factor NUMERIC(4,3) DEFAULT 1.0 CHECK (capacity_factor BETWEEN 0 AND 2),
    notes           TEXT,
    UNIQUE (location_id, calendar_date)
);

CREATE INDEX IF NOT EXISTS idx_calendar_location_date ON operational_calendars (location_id, calendar_date);
CREATE INDEX IF NOT EXISTS idx_calendar_non_working ON operational_calendars (location_id, calendar_date)
    WHERE is_working_day = FALSE;


-- ============================================================
-- 10. Table master_data_audit_log
-- ============================================================

CREATE TABLE IF NOT EXISTS master_data_audit_log (
    audit_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type     TEXT        NOT NULL,
    entity_id       UUID        NOT NULL,
    field_name      TEXT        NOT NULL,
    old_value       TEXT,
    new_value       TEXT,
    changed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    changed_by      TEXT,
    source_system   TEXT,
    batch_id        UUID        REFERENCES ingest_batches(batch_id)
);

CREATE INDEX IF NOT EXISTS idx_audit_entity ON master_data_audit_log (entity_type, entity_id, changed_at);
