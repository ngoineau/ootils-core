-- ============================================================
-- Migration 072 — Forecast-drift recommendations (DEM-1 demand watcher)
-- ============================================================
-- Chantier DEM-1 PR-2. The FIRST demand-side exception watcher
-- (agent_forecast_watcher, landing in this PR) observes realized forecast
-- accuracy and emits governed L1 DRAFT FORECAST_DRIFT recommendations when a
-- series has degraded (MASE up) or is sustainedly biased. Same Decision Ladder
-- + state-machine backbone as procurement recos (#341), but a distinct typed
-- shape.
--
-- WHY A SEPARATE TABLE (not `recommendations`): the canonical `recommendations`
-- table (migration 039) is SUPPLY-ONLY by CHECK/NOT NULL — shortage_date DATE
-- NOT NULL + deficit_qty NUMERIC NOT NULL (039:47-48), and its action CHECK is
-- the procurement/reschedule vocabulary (039:61-62 widened by 061:92-98). A
-- forecast-drift signal carries NEITHER a shortage date NOR a deficit qty; it
-- is a demand-accuracy verdict. Forcing it into `recommendations` would mean
-- NULLing NOT NULL columns and widening a supply CHECK with a non-supply
-- action — exactly the reason eando (transfer_recommendations, 066) and the
-- param-piloting fleet (parameter_recommendations, 041) each got their own
-- typed table. This table calques parameter_recommendations' governance shape
-- (status / decision_level / agent_name / agent_run_id / created_at /
-- updated_at / evidence) with drift-specific typed measures.
--
-- FK POLICY (every reference explicit, per the modern discipline of migrations
-- 067/069 — Postgres' default FK action is NO ACTION, never rely on it):
--   scenario_id  -> scenarios  ON DELETE RESTRICT. REQUIRED explicit: the
--     repo-wide guard test_scenario_fk_retention (migration 032) scans
--     pg_constraint for every FK with confrelid='scenarios' and asserts
--     confdeltype='r' — a scenario must never be hard-deleted out from under
--     a reco that references it (soft-delete via status='archived' only).
--   item_id      -> items      ON DELETE RESTRICT. A drift verdict is
--     master-data-anchored; its item must not be silently orphaned.
--   location_id  -> locations  ON DELETE RESTRICT, NULLABLE. Drift can be
--     assessed at an item-only (all-locations) grain — NULL then; when scoped
--     to a site, the site must not be orphaned.
--   pyramide_run_id -> pyramide_runs(run_id) ON DELETE SET NULL, NULLABLE.
--     The reco cites the Pyramide run whose metrics triggered it, but must
--     SURVIVE that run being purged/rolled-up — the drift verdict is frozen on
--     this row; the run pointer is a traceability nicety that may be nulled.
--     (NB: pyramide_runs' PK is run_id, migration 038, NOT pyramide_run_id.)
--   agent_run_id -> agent_runs ON DELETE RESTRICT. The work ledger (039) that
--     produced this reco; a reco must not outlive its run record.
--
-- JSONB CARVE-OUT (CLAUDE.md policy — forensic payload of unbounded shape):
-- `evidence` is the ONLY JSONB column here and is a diagnostic trail, not
-- business data. Its shape is genuinely unbounded and per-run: per-horizon
-- metric series (MASE / bias / tracking-signal over each forecast horizon),
-- the backtest cutoff windows compared, seasonal-naive baselines, and the
-- threshold-crossing detail that justifies the verdict. This is the same
-- rationale as dq_agent_runs.summary and parameter_recommendations.evidence
-- (migrations 012/041). Everything business-QUERYABLE (the drift_kind, the
-- measures mase/bias/tracking_ratio, the thresholds, cadence) is a TYPED
-- column; only the free-form forensic trail is JSONB.
--
-- CHECK / VALID_* SYNC: action ('FORECAST_DRIFT'), decision_level ('L1' —
-- forecast drift is always L1: a demand-accuracy flag is reversible/low-risk,
-- so the CHECK is intentionally tighter than the full ladder of 039/041),
-- drift_kind ('MASE_DEGRADED','BIAS_SUSTAINED','BOTH') and status must stay in
-- sync with the forthcoming agent_forecast_watcher's VALID_* sets. The status
-- set is copied EXACTLY from parameter_recommendations (041:46) /
-- recommendations (039:68) so this table shares the #341 state machine.
--
-- IDEMPOTENCE (repo migration policy — the runner in db/connection.py wraps
-- each file in its own transaction and, on ANY error, ROLLS BACK and ABORTS;
-- it does NOT swallow "already exists"): every statement re-runs as a clean
-- no-op — CREATE TABLE/INDEX IF NOT EXISTS. See migration 063's header for the
-- canonical defensive-idempotence pattern.
--
-- Rolling-safe: brand-new, additive table — no reader depends on it yet, no
-- existing object is altered.
--
-- ref: chantier DEM-1 PR-2 (demand exception watcher).
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- forecast_drift_recommendations — demand-accuracy verdicts, governed
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS forecast_drift_recommendations (
    -- Deterministic uuid5 minted by the watcher (scenario/item/location/run
    -- coordinate) — NO DEFAULT: a re-run of the watcher on an unchanged drift
    -- re-derives the SAME id, so the application can upsert ON CONFLICT DO
    -- NOTHING and emit zero duplicate rows.
    recommendation_id    UUID        NOT NULL PRIMARY KEY,

    agent_name           TEXT        NOT NULL,
    agent_run_id         UUID        NOT NULL
                         REFERENCES agent_runs(agent_run_id) ON DELETE RESTRICT,

    -- Scenario coordinate. Explicit ON DELETE RESTRICT is MANDATORY here
    -- (test_scenario_fk_retention / migration 032 requires confdeltype='r').
    scenario_id          UUID        NOT NULL
                         REFERENCES scenarios(scenario_id) ON DELETE RESTRICT,

    -- What series drifted. location_id NULL = item-level (all-locations) grain.
    item_id              UUID        NOT NULL
                         REFERENCES items(item_id) ON DELETE RESTRICT,
    location_id          UUID
                         REFERENCES locations(location_id) ON DELETE RESTRICT,

    -- The Pyramide run whose accuracy metrics triggered this reco. Nullable +
    -- SET NULL: the verdict survives the run being purged (see header).
    pyramide_run_id      UUID
                         REFERENCES pyramide_runs(run_id) ON DELETE SET NULL,

    -- Typed verdict (business-queryable).
    action               TEXT        NOT NULL
                         CHECK (action = 'FORECAST_DRIFT'),
    decision_level       TEXT        NOT NULL DEFAULT 'L1'
                         CHECK (decision_level = 'L1'),
    drift_kind           TEXT        NOT NULL
                         CHECK (drift_kind IN ('MASE_DEGRADED', 'BIAS_SUSTAINED', 'BOTH')),

    -- Cadence of the drifted series (e.g. daily/weekly/monthly); free text,
    -- nullable — a drift verdict does not require it.
    cadence              TEXT,

    -- Measures — ALL NULLABLE, None-honest: a BIAS_SUSTAINED drift may carry no
    -- MASE, a MASE_DEGRADED drift may carry no bias. Bare NUMERIC (unscaled):
    -- these are statistical ratios/errors, NOT quantities or money, so they
    -- take arbitrary precision (matching parameter_recommendations' measures,
    -- 041:38-40) rather than the NUMERIC(18,6) reserved for qty/$.
    mase                 NUMERIC,
    bias                 NUMERIC,
    tracking_ratio       NUMERIC,
    threshold_mase       NUMERIC,
    threshold_bias_ratio NUMERIC,

    -- Governance — status set copied EXACTLY from parameter_recommendations
    -- (041:46) / recommendations (039:68): shared #341 state machine.
    status               TEXT        NOT NULL DEFAULT 'DRAFT'
                         CHECK (status IN ('DRAFT', 'REVIEWED', 'APPROVED', 'REJECTED', 'APPLIED', 'EXPIRED')),

    -- Deterministic [0,1] confidence score (ADR-023 world), nullable — a
    -- drift verdict may be emitted without a composed score. CHECK bounds it
    -- to the legal range: fail-loudly on a malformed score rather than
    -- silently persisting an impossible confidence.
    confidence           NUMERIC
                         CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),

    -- JSONB carve-out (see header): forensic per-horizon metric series /
    -- backtest windows. The only non-typed column.
    evidence             JSONB,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE forecast_drift_recommendations IS
    'Governed L1 FORECAST_DRIFT recommendations from the DEM-1 demand watcher '
    '(agent_forecast_watcher). Separate from `recommendations` because that '
    'table is supply-only by CHECK/NOT NULL (shortage_date + deficit_qty, '
    'migration 039). recommendation_id is a deterministic uuid5 minted by the '
    'watcher (no DEFAULT) for idempotent ON CONFLICT DO NOTHING upsert. Shares '
    'the #341 governance state machine; evidence is a JSONB forensic carve-out.';

COMMENT ON COLUMN forecast_drift_recommendations.recommendation_id IS
    'Deterministic uuid5 (scenario/item/location/run) minted by the watcher — '
    'NO DEFAULT: an unchanged drift re-run re-derives the same id (upsert ON '
    'CONFLICT DO NOTHING => zero duplicate rows).';

COMMENT ON COLUMN forecast_drift_recommendations.scenario_id IS
    'Scenario coordinate. FK ON DELETE RESTRICT is MANDATORY (guard '
    'test_scenario_fk_retention requires confdeltype=''r'' on every FK to '
    'scenarios) — hard-delete blocked, archive via status only.';

COMMENT ON COLUMN forecast_drift_recommendations.location_id IS
    'Site of the drifted series; NULL = item-level (all-locations) drift grain.';

COMMENT ON COLUMN forecast_drift_recommendations.pyramide_run_id IS
    'Pyramide run (migration 038, PK run_id) whose accuracy metrics triggered '
    'this reco. ON DELETE SET NULL: the frozen verdict survives a run purge; '
    'the pointer is a traceability nicety.';

COMMENT ON COLUMN forecast_drift_recommendations.drift_kind IS
    'MASE_DEGRADED / BIAS_SUSTAINED / BOTH. Keep in sync with the '
    'agent_forecast_watcher VALID_* set (this CHECK is the DB half).';

COMMENT ON COLUMN forecast_drift_recommendations.mase IS
    'Realized MASE of the series; NULL-honest (a pure BIAS drift may carry '
    'none). Bare NUMERIC — a statistical ratio, not a quantity/$.';

COMMENT ON COLUMN forecast_drift_recommendations.confidence IS
    'Deterministic [0,1] confidence score (ADR-023 world), nullable. CHECK '
    'bounds it to the legal range — a malformed score fails loudly rather '
    'than persisting silently.';

COMMENT ON COLUMN forecast_drift_recommendations.evidence IS
    'JSONB carve-out (CLAUDE.md policy): forensic, unbounded-shape trail — '
    'per-horizon metric series, backtest windows, threshold-crossing detail. '
    'NOT business data (all queryable fields are typed columns above).';

-- ------------------------------------------------------------
-- Indexes
-- ------------------------------------------------------------
-- Governance queue scan ("all DRAFT drift recos to review").
CREATE INDEX IF NOT EXISTS ix_forecast_drift_reco_status
    ON forecast_drift_recommendations (status);

-- Coordinate lookup ("drift history for this item @ location"); item_id is the
-- leading column so it also supports the item FK's join path.
CREATE INDEX IF NOT EXISTS ix_forecast_drift_reco_item
    ON forecast_drift_recommendations (item_id, location_id);

-- Per-run listing + the agent_run FK's supporting index.
CREATE INDEX IF NOT EXISTS ix_forecast_drift_reco_run
    ON forecast_drift_recommendations (agent_run_id);

-- Scenario-scoped queue scan ("all DRAFT/REVIEWED drift recos for this fork"):
-- North Star "queryable from a scenario" + the watcher's own read pattern
-- (list this scenario's drift queue by status) — scenario_id leading, status
-- trailing, mirrors idx_events_stream_seq's (scenario_id, cursor) shape.
CREATE INDEX IF NOT EXISTS idx_fdr_scenario_status
    ON forecast_drift_recommendations (scenario_id, status);

COMMIT;
