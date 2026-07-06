-- ============================================================
-- Migration 067 — Inventory snapshots (proof machine foundation)
-- ============================================================
-- Chantier #393 A3-PR1. ADR-030 (à venir): "Inventory historisation +
-- outcome/proof machine".
--
-- WHAT: a per-day, per-(item, location) point-in-time record of on-hand
-- stock and the projected-shortage picture at that coordinate. This is the
-- historisation backbone the proof machine reads to compare "what we said
-- would happen" against "what actually happened" over time. One snapshot per
-- coordinate per day (the natural capture grain).
--
-- SCENARIO_ID (schema-consistent, V1 baseline-only): every stateful table in
-- the graph carries scenario_id (nodes/edges/shortages, migrations 002/005)
-- so forks are first-class. inventory_snapshots follows the house rule for
-- schema consistency and future extension, but V1 CAPTURES BASELINE ONLY —
-- the capturers (CLI/cron/API) always write the baseline scenario. A hard FK
-- to scenarios(scenario_id) with the default RESTRICT is correct here (unlike
-- the audit-record SET NULL of migration 066): a snapshot is meaningless
-- without its scenario coordinate, and baseline is never hard-deleted.
--
-- SEVERITY $-VALUED, NULL-HONEST (ADR-021 alignment): shortage_severity_usd
-- mirrors the canonical $-valued severity owned by ShortageDetector (the
-- shortages table is the canonical persistence/query system per ADR-021).
-- This column is a DENORMALISED capture of the severity AS OF the snapshot
-- day, NOT a second source of shortage truth — the snapshot never writes into
-- shortages and ShortageDetector never reads this. NULL is the honest value
-- when the coordinate had NO projected shortage on as_of_date (no shortage =>
-- no $-severity), exactly as first_shortage_date is NULL then. A row with
-- first_shortage_date IS NULL MUST have shortage_severity_usd IS NULL, and
-- vice-versa; the two are captured together from the same MRP projection.
--
-- FK POLICY: item_id / location_id are hard FKs to items/locations with the
-- default RESTRICT — the house pattern (item_planning_params, nodes,
-- shortages … migrations 002/005). A snapshot is master-data-anchored; master
-- data referenced by a stock history must not be silently orphaned.
--
-- PRECISION: on_hand_qty and shortage_severity_usd are NUMERIC(18,6) — the
-- canonical scaled precision for quantities across the engine (MRP buckets
-- migration 021, forecast 026, MPS 027, DRP 029) and a safe superset for a $
-- value (money columns migration 029). Never FLOAT/REAL on a quantity or a
-- cost.
--
-- IDEMPOTENCE (repo migration policy — the runner in db/connection.py wraps
-- each file in its own transaction and, on ANY error, ROLLS BACK and ABORTS;
-- it does NOT swallow "already exists"): every statement re-runs as a clean
-- no-op — CREATE TABLE/INDEX IF NOT EXISTS. The UNIQUE (scenario_id, item_id,
-- location_id, as_of_date) additionally lets the capturer upsert the same day
-- with ON CONFLICT ... DO UPDATE, so a re-capture of the same coordinate/day
-- is idempotent at the application level too (re-running today's capture
-- overwrites, never duplicates).
--
-- No JSONB. Typed columns only.
--
-- Rolling-safe: brand-new, additive table — no reader depends on it yet, no
-- existing object is altered.
--
-- ref: ADR-030 (proof machine), #393 A3-PR1.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- inventory_snapshots: per-day per-coordinate stock history
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory_snapshots (
    snapshot_id            UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario_id            UUID          NOT NULL REFERENCES scenarios(scenario_id),
    item_id                UUID          NOT NULL REFERENCES items(item_id),
    location_id            UUID          NOT NULL REFERENCES locations(location_id),
    as_of_date             DATE          NOT NULL,
    on_hand_qty            NUMERIC(18,6) NOT NULL,
    first_shortage_date    DATE,
    shortage_severity_usd  NUMERIC(18,6),
    source                 TEXT          NOT NULL
                                         CHECK (source IN ('cli', 'api', 'cron')),
    captured_at            TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- One snapshot per coordinate per day. Enables ON CONFLICT DO UPDATE
    -- (idempotent re-capture of the same day).
    UNIQUE (scenario_id, item_id, location_id, as_of_date)
);

COMMENT ON TABLE inventory_snapshots IS
    'Per-day, per-(item, location) point-in-time stock history — the proof '
    'machine foundation (#393 A3-PR1, ADR-030). V1 captures BASELINE ONLY '
    '(scenario_id present for schema consistency + future forkability). One '
    'row per (scenario, item, location, as_of_date); the UNIQUE key backs an '
    'idempotent ON CONFLICT DO UPDATE re-capture.';

COMMENT ON COLUMN inventory_snapshots.scenario_id IS
    'Scenario coordinate. V1 baseline-only (capturers always write baseline); '
    'column exists for schema consistency and future fork historisation.';

COMMENT ON COLUMN inventory_snapshots.as_of_date IS
    'The captured day — the point in time this stock/shortage picture '
    'represents.';

COMMENT ON COLUMN inventory_snapshots.on_hand_qty IS
    'On-hand stock at the coordinate on as_of_date. NUMERIC(18,6) — canonical '
    'scaled quantity precision.';

COMMENT ON COLUMN inventory_snapshots.first_shortage_date IS
    'First projected shortage date at this coordinate as of as_of_date; NULL '
    'if no shortage was projected. Captured together with '
    'shortage_severity_usd (both NULL or both set).';

COMMENT ON COLUMN inventory_snapshots.shortage_severity_usd IS
    'Denormalised $-valued shortage severity (ADR-021 semantics) AS OF '
    'as_of_date — NOT a second source of shortage truth (ShortageDetector '
    'stays canonical; this row never writes into shortages). NULL-honest: '
    'NULL when the coordinate had no projected shortage on as_of_date '
    '(mirrors first_shortage_date IS NULL).';

COMMENT ON COLUMN inventory_snapshots.source IS
    'Capture channel: cli (manual/script), api (endpoint), cron (scheduled).';

-- ------------------------------------------------------------
-- Indexes
-- ------------------------------------------------------------
-- Query-by-date within a scenario ("all snapshots for day D"): the proof
-- machine's daily scan.
CREATE INDEX IF NOT EXISTS idx_inventory_snapshots_scenario_date
    ON inventory_snapshots (scenario_id, as_of_date);

-- Latest snapshot for a coordinate ("what did we last know about this item @
-- location?"): DESC on as_of_date makes the newest row the leading match.
CREATE INDEX IF NOT EXISTS idx_inventory_snapshots_coord_latest
    ON inventory_snapshots (item_id, location_id, as_of_date DESC);

COMMIT;
