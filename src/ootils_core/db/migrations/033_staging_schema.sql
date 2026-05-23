-- ============================================================
-- Ootils Core — Migration 033: Staging schema (ADR-013 step 1)
--
-- Creates the dedicated `staging` schema and the NEW tables that
-- support the file-upload + approval workflow defined in ADR-013:
--   - staging.uploads        : one row per uploaded file (sha256,
--                              filename, format, link to ingest_batches)
--   - staging.transform_runs : one row per approval execution
--                              (timing, counts, approver, status)
--
-- NOT in this migration (deferred to migration 034 with synchronised
-- Python code updates to avoid breaking the existing ingest router):
--   ALTER TABLE ingest_batches      SET SCHEMA staging
--   ALTER TABLE ingest_rows         SET SCHEMA staging
--   ALTER TABLE data_quality_issues SET SCHEMA staging
--   ALTER TABLE external_id_mapping SET SCHEMA staging
--
-- Conventions (same as the rest of the migrations folder):
--   - All PKs are UUID
--   - All timestamps are TIMESTAMPTZ UTC
--   - IF NOT EXISTS everywhere for idempotency
-- ============================================================


-- ============================================================
-- 1. Create the staging schema
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;

COMMENT ON SCHEMA staging IS
    'External-data ingestion staging zone (ADR-013). Holds uploaded '
    'files, raw rows, DQ issues, and approval audit. Tables in public.* '
    'reference this schema during the staging-to-canonical transform.';


-- ============================================================
-- 2. staging.uploads — one row per uploaded file
-- ============================================================
--
-- Tracks the source file behind every ingest_batch. Lets ops answer:
--   - "Which file produced batch X?"
--   - "Is this file content already loaded?" (sha256 lookup)
--   - "How big was the upload?" (capacity planning)
--
-- The file content itself is NOT stored here (only metadata + sha256).
-- The parsed rows live in public.ingest_rows.

CREATE TABLE IF NOT EXISTS staging.uploads (
    upload_id          UUID         NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Link to the batch produced by this upload (1-to-1 in practice;
    -- the FK is optional because the upload row is created BEFORE
    -- the batch row in the upload endpoint).
    batch_id           UUID         REFERENCES public.ingest_batches(batch_id)
                                    ON DELETE CASCADE,

    -- Source-file metadata captured at upload time
    filename           TEXT         NOT NULL,
    file_size_bytes    BIGINT       NOT NULL CHECK (file_size_bytes >= 0),
    file_format        TEXT         NOT NULL CHECK (file_format IN (
                                        'tsv', 'csv', 'xlsx', 'json'
                                    )),
    -- SHA-256 of the raw file bytes. Used for deduplication and to
    -- catch silent corruption between upload and parsing.
    sha256             CHAR(64)     NOT NULL,
    -- Detected encoding (utf-8, cp-1252, etc.); only meaningful for
    -- text formats. NULL for xlsx/json.
    encoding           TEXT,
    -- Content-Type as declared by the uploader (informational only;
    -- the parser routes on file_format above).
    content_type       TEXT,

    -- Who uploaded — pulled from the auth token. Free text so a future
    -- shift to OIDC subjects doesn't require a schema change.
    uploaded_by        TEXT,
    uploaded_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_staging_uploads_batch
    ON staging.uploads (batch_id);
CREATE INDEX IF NOT EXISTS idx_staging_uploads_sha256
    ON staging.uploads (sha256);


-- ============================================================
-- 3. staging.transform_runs — one row per /approve execution
-- ============================================================
--
-- Records the approval action (when, who, with what outcome). The
-- staging-to-canonical transform is wrapped in a single transaction;
-- this table is the durable audit log of that transaction.
--
-- ADR-013 D4 — approval mandatory; ADR-013 D3 — full reload counters.

CREATE TABLE IF NOT EXISTS staging.transform_runs (
    run_id             UUID         NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id           UUID         NOT NULL REFERENCES public.ingest_batches(batch_id),

    -- Lifecycle: 'running' set on transaction start; 'completed' on
    -- successful commit; 'failed' on exception (with error_message);
    -- 'rolled_back' if a /reject lands after a partial transform.
    status             TEXT         NOT NULL DEFAULT 'running' CHECK (status IN (
                                        'running', 'completed', 'failed', 'rolled_back'
                                    )),

    -- Approver identity + free-form note from the /approve payload
    approved_by        TEXT         NOT NULL,
    approval_notes     TEXT,

    -- Per-entity counters of what the full-reload transform changed
    -- in public.* tables (ADR-013 D3). NULL until the transaction
    -- completes; on failure these stay NULL and error_message is set.
    rows_inserted      INTEGER,
    rows_updated       INTEGER,
    rows_soft_deleted  INTEGER,
    rows_skipped       INTEGER,

    -- Deletion-ratio safeguard (ADR-013 D4): if the planned soft-delete
    -- count exceeds 20% of the current active rows for this
    -- (entity_type, source_system), the approval must be forced.
    forced_approval    BOOLEAN      NOT NULL DEFAULT FALSE,

    -- Timings
    started_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at       TIMESTAMPTZ,

    -- Populated when status='failed'. The raw exception string is
    -- intentionally kept here (not parsed) — it's the ops debug path.
    error_message      TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_transform_runs_batch
    ON staging.transform_runs (batch_id);
CREATE INDEX IF NOT EXISTS idx_staging_transform_runs_status_started
    ON staging.transform_runs (status, started_at DESC);


-- ============================================================
-- 4. Reserved for migration 034 (deferred)
-- ============================================================
--
-- The four existing import tables (ingest_batches, ingest_rows,
-- data_quality_issues, external_id_mapping) stay in `public` for now.
-- Moving them requires synchronised updates to ~7 Python files
-- (ingest router, dq engine + agent) and is scoped to migration 034
-- to keep this migration purely additive and zero-risk.
