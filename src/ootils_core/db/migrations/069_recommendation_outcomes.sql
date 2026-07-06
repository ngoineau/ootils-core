-- ============================================================
-- Migration 069 — Recommendation outcomes (the proof-of-value core)
-- ============================================================
-- Chantier #393 A3-PR2. ADR-030 ("Inventory historisation +
-- outcome/proof machine").
--
-- WHAT: the deterministic chaining of a recommendation to its observed
-- real-world result. For each governed recommendation (migration 039)
-- the proof machine, on a given observation date, classifies what
-- actually happened at that coordinate against what the reco predicted,
-- and (when computable) values the shortage $ that was avoided. This is
-- the table that turns "we said to order" into "and here is the deficit
-- that did NOT materialise, worth $X" — the value story of the fleet.
--
-- DETERMINISTIC CLASSIFICATION: evaluation_status is one of exactly five
-- machine-decided outcomes, never an LLM verdict (North Star: agents
-- propose, the deterministic core scores):
--   AVOIDED        — predicted shortage did not occur (reco worked / need
--                    no longer projected); observed_deficit_qty = 0.
--   MATERIALIZED   — the predicted shortage happened anyway.
--   PARTIAL        — a smaller deficit than predicted still occurred.
--   NOT_APPLICABLE — the reco's premise no longer holds (e.g. item/demand
--                    withdrawn) — outcome is not a hit/miss, exclude from
--                    avoided-$ aggregates.
--   INDETERMINATE  — insufficient/too-fresh data to classify honestly
--                    (mirrors the NULL-honest discipline below).
-- Keep in sync with the Python VALID_* set for outcome status (A3-PR2
-- evaluator); this CHECK is the DB half of that contract.
--
-- SCENARIO_ID — DELIBERATELY ABSENT (inherited, not redundant): an outcome
-- is the REAL observed result, always baseline. It carries no scenario_id
-- column of its own; the scenario coordinate is inherited through
-- recommendation_id -> recommendations.scenario_id. KPI aggregates that
-- need to slice by scenario JOIN recommendations (r.scenario_id) — the
-- reco already persists it (migration 039). This is intentional on two
-- counts: (1) no duplicated, drift-prone coordinate; (2) it keeps this
-- table OUT of the repo-wide scenario-FK guard (test_scenario_fk_retention
-- / migration 032), which requires EVERY FK referencing scenarios to be
-- ON DELETE RESTRICT — an outcome deletion policy (CASCADE from the reco,
-- see below) would otherwise conflict with that RESTRICT rule. No
-- scenario_id column => no scenarios FK => no conflict, and the audit
-- semantics stay correct.
--
-- FK POLICY (two references, two intentional ON DELETE actions):
--   recommendation_id -> recommendations ON DELETE CASCADE. An outcome is
--     meaningless without the reco it evaluates; if the reco is hard-
--     deleted the outcome goes with it (it is a dependent evaluation, not
--     an independent audit record). This is also why scenario retention is
--     inherited: the reco's own scenario FK (RESTRICT) already protects the
--     scenario from disappearing under live data.
--   snapshot_id -> inventory_snapshots ON DELETE SET NULL. The observation
--     snapshot (migration 067) is the evidence we read to classify, but the
--     AUDIT of the classification must SURVIVE a snapshot purge/rollup: the
--     verdict + $ figures are frozen on this row, the snapshot pointer is a
--     nicety that may be nulled without invalidating the outcome. Nullable
--     by design (an INDETERMINATE/NOT_APPLICABLE outcome may have no
--     snapshot at all).
--
-- AVOIDED $ — NULL-HONEST (ADR-021 severity alignment): avoided_severity_usd
-- mirrors the canonical $-valued shortage severity owned by ShortageDetector
-- (ADR-021: the shortages table is the canonical severity system). Here it
-- is the $ the reco is credited with avoiding. NULL is the honest value when
-- the figure is NOT computable (no cost basis, INDETERMINATE status, missing
-- inputs) — a masked 0 would silently understate or overstate the fleet's
-- proven value, which the proof machine must never do. 0 is reserved for the
-- genuine "nothing was at stake" case, distinct from "we could not compute".
--
-- PRECISION: predicted/observed quantities and avoided_severity_usd are
-- NUMERIC(18,6) — the canonical scaled precision across the engine (MRP
-- buckets 021, forecast 026, DRP 029, snapshots 067) and a safe superset
-- for a $ value. Never FLOAT/REAL on a quantity or a cost.
--
-- IDEMPOTENCE (repo migration policy — the runner in db/connection.py wraps
-- each file in its own transaction and, on ANY error, ROLLS BACK and ABORTS;
-- it does NOT swallow "already exists"): every statement re-runs as a clean
-- no-op — CREATE TABLE/INDEX IF NOT EXISTS. The UNIQUE
-- (recommendation_id, evaluated_as_of) additionally lets the evaluator
-- upsert with ON CONFLICT ... DO UPDATE, so re-evaluating the same reco for
-- the same observation date is idempotent at the application level too
-- (a re-run overwrites the verdict, never duplicates it).
--
-- No JSONB. Typed columns only.
--
-- Rolling-safe: brand-new, additive table — no reader depends on it yet, no
-- existing object is altered.
--
-- ref: ADR-030 (proof machine), #393 A3-PR2.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- recommendation_outcomes: reco -> observed result, deterministically scored
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recommendation_outcomes (
    outcome_id             UUID          PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Dependent evaluation of a governed reco: CASCADE (see header FK POLICY).
    recommendation_id      UUID          NOT NULL
                                         REFERENCES recommendations(recommendation_id)
                                         ON DELETE CASCADE,

    -- The observation date this verdict is anchored to.
    evaluated_as_of        DATE          NOT NULL,

    -- Deterministic 5-way classification (sync with Python VALID_* set).
    evaluation_status      TEXT          NOT NULL
                                         CHECK (evaluation_status IN (
                                             'AVOIDED',
                                             'MATERIALIZED',
                                             'PARTIAL',
                                             'NOT_APPLICABLE',
                                             'INDETERMINATE'
                                         )),

    -- What the reco predicted (extracted from its evidence trail). NULL when
    -- the reco carried no such figure.
    predicted_shortage_date DATE,
    predicted_deficit_qty   NUMERIC(18,6),

    -- What was actually observed. 0 when the shortage was avoided.
    observed_deficit_qty    NUMERIC(18,6),

    -- $ credited as avoided. NULL-honest: NULL = not computable (never a
    -- masked 0); 0 = nothing was genuinely at stake. See header.
    avoided_severity_usd    NUMERIC(18,6),

    -- Observation snapshot we read to classify (migration 067). SET NULL on
    -- snapshot purge — the verdict survives its evidence pointer.
    snapshot_id            UUID          REFERENCES inventory_snapshots(snapshot_id)
                                         ON DELETE SET NULL,

    evaluated_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- One verdict per reco per observation date; backs the evaluator's
    -- idempotent ON CONFLICT DO UPDATE re-evaluation.
    UNIQUE (recommendation_id, evaluated_as_of)
);

COMMENT ON TABLE recommendation_outcomes IS
    'Deterministic chaining of a recommendation (migration 039) to its '
    'observed real-world result — the proof-of-value core (#393 A3-PR2, '
    'ADR-030). Baseline-only (the real observed outcome); scenario is '
    'INHERITED via recommendation_id -> recommendations.scenario_id, no '
    'redundant scenario_id column here. One verdict per (recommendation, '
    'evaluated_as_of); the UNIQUE key backs an idempotent ON CONFLICT DO '
    'UPDATE re-evaluation.';

COMMENT ON COLUMN recommendation_outcomes.recommendation_id IS
    'The evaluated reco. ON DELETE CASCADE: an outcome is a dependent '
    'evaluation, meaningless without its reco. Scenario retention is '
    'inherited from this reco''s own RESTRICT FK to scenarios.';

COMMENT ON COLUMN recommendation_outcomes.evaluated_as_of IS
    'Observation date the verdict is anchored to.';

COMMENT ON COLUMN recommendation_outcomes.evaluation_status IS
    'Deterministic 5-way outcome (never an LLM verdict): AVOIDED / '
    'MATERIALIZED / PARTIAL / NOT_APPLICABLE / INDETERMINATE. NOT_APPLICABLE '
    'and INDETERMINATE are excluded from avoided-$ aggregates. Kept in sync '
    'with the Python VALID_* set (A3-PR2 evaluator).';

COMMENT ON COLUMN recommendation_outcomes.observed_deficit_qty IS
    'Deficit actually observed at the coordinate; 0 when the predicted '
    'shortage was avoided. NUMERIC(18,6) canonical scaled quantity.';

COMMENT ON COLUMN recommendation_outcomes.avoided_severity_usd IS
    '$-valued shortage severity credited as avoided (ADR-021 semantics). '
    'NULL-HONEST: NULL when NOT computable (no cost basis / INDETERMINATE / '
    'missing inputs) — NEVER a masked 0. 0 is reserved for the genuine '
    '"nothing was at stake" case.';

COMMENT ON COLUMN recommendation_outcomes.snapshot_id IS
    'Observation snapshot (migration 067) read to classify this outcome. '
    'ON DELETE SET NULL: the frozen verdict + $ figures survive a snapshot '
    'purge/rollup; nullable (some statuses have no snapshot).';

-- ------------------------------------------------------------
-- Indexes
-- ------------------------------------------------------------
-- Timeline / re-evaluation lookup for a single reco ("all verdicts for this
-- reco"). Also the FK's supporting index.
CREATE INDEX IF NOT EXISTS idx_reco_outcomes_recommendation
    ON recommendation_outcomes (recommendation_id);

-- KPI aggregates roll up by status ("count/sum $ AVOIDED vs MATERIALIZED").
CREATE INDEX IF NOT EXISTS idx_reco_outcomes_status
    ON recommendation_outcomes (evaluation_status);

-- Daily proof-machine scan / period reporting by observation date.
CREATE INDEX IF NOT EXISTS idx_reco_outcomes_evaluated_as_of
    ON recommendation_outcomes (evaluated_as_of);

COMMIT;
