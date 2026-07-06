-- ============================================================
-- Migration 068 — Forecast Value Added (FVA) on the backtest report (#393 A3-PR3)
-- ============================================================
-- Chantier #393 axis A3. ADR-030 (à venir): "Forecast Value Added — is the
-- stat model worth its complexity vs a trivial seasonal-naive baseline?".
--
-- WHAT: four additive, typed, nullable columns on pyramide_accuracy_metrics
-- (migration 055) — the naive-baseline error (naive_wape/naive_mase) and the
-- value the stat model ADDS over it (fva_wape/fva_mase). FVA is NOT a sibling
-- table: it is an ATTRIBUTE of the very same rolling-origin backtest — same
-- run_id, same per-horizon granularity, same None-honest contract — computed on
-- the exact same cutoffs/observations as the stat metrics it sits next to. A
-- new table would duplicate (run_id, horizon) and force a join to compare a
-- model against its own baseline; four columns keep the comparison on one row.
--
-- BASELINE = seasonal-naive (deliberately trivial). FVA is only credible if the
-- benchmark is a benchmark nobody would defend deploying: the seasonal-naive
-- forecast (repeat the value one season ago). If the stat pipeline cannot beat
-- "same week last year", its complexity is not paying for itself. The baseline
-- is scored on the SAME backtest so naive_wape/wape (and naive_mase/mase) are
-- apples-to-apples.
--
-- FVA DEFINITION (the load-bearing sign convention): fva = naive - stat.
--   * fva_wape = naive_wape - wape ; POSITIVE = the stat model beats the naive
--     baseline (lower WAPE is better, so the model REMOVED error → value added).
--   * fva_mase = naive_mase - mase ; POSITIVE = the stat model beats the naive.
--   A NEGATIVE FVA is a legitimate, honest result — the stat model LOST to the
--   trivial baseline on this data — so there is intentionally NO sign CHECK on
--   the fva_* columns (unlike a quantity). Consumers (Decision Ladder) decide
--   what a non-positive FVA means; the module never clamps it to 0.
--
-- NULL-honest contract (INHERITED verbatim from migration 055): a NULL here
-- means "not computable on this data" — NEVER a masked 0.
--   * naive_wape/naive_mase are NULL when the seasonal-naive baseline itself is
--     undefined: it needs >= 1 full season of history to have a value one season
--     ago (and, for MASE, a usable naive-scaling denominator). Short history →
--     NULL baseline, never a fabricated 0.
--   * fva_wape/fva_mase are NULL whenever EITHER operand is NULL (naive missing,
--     or the stat wape/mase already NULL per 055 — e.g. per-horizon rows carry
--     no actuals, so wape/mase and therefore fva stay NULL there). A NULL FVA is
--     "not comparable", NOT "no value added".
--
-- TYPING: NUMERIC (bare, NULL, no DEFAULT) — byte-for-byte the type of the
-- existing wape/mase columns in migration 055 (which are bare NUMERIC, not
-- NUMERIC(p,s)). fva_* is a difference of two NUMERICs, so it shares the type.
-- No FLOAT/REAL on a metric.
--
-- Rolling-deploy safe: purely additive nullable columns with NO default. Old
-- writers (pre-FVA repository.persist_accuracy_metrics) simply leave them NULL,
-- which reads exactly as the honest "baseline not computed" — no backfill, no
-- rewrite of existing rows, no behavioural change to the 055 metrics.
--
-- Idempotence (repo migration policy — the runner in db/connection.py wraps
-- each file in its own transaction and, on ANY error, ROLLS BACK and ABORTS; it
-- does NOT swallow "already exists"): ADD COLUMN IF NOT EXISTS re-runs as a
-- clean no-op.
-- No index: the FVA columns are read alongside their run's metric rows (already
-- covered by idx_pyramide_accuracy_metrics_run from migration 055), never as a
-- filter/join predicate.
-- No JSONB. Typed columns only.
--
-- ref: ADR-030 (FVA), #393 A3-PR3. Baseline table: migration 055.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- FVA columns on the existing per-run / per-horizon backtest report
-- ------------------------------------------------------------
-- Seasonal-naive baseline error (naive_*) + value the stat model adds over it
-- (fva_* = naive - stat). All bare NUMERIC NULL, matching wape/mase in 055.
ALTER TABLE pyramide_accuracy_metrics
    ADD COLUMN IF NOT EXISTS naive_wape NUMERIC NULL,
    ADD COLUMN IF NOT EXISTS naive_mase NUMERIC NULL,
    ADD COLUMN IF NOT EXISTS fva_wape   NUMERIC NULL,
    ADD COLUMN IF NOT EXISTS fva_mase   NUMERIC NULL;

COMMENT ON COLUMN pyramide_accuracy_metrics.naive_wape IS
    'WAPE of the seasonal-naive baseline (repeat value one season ago), scored '
    'on the SAME rolling-origin backtest as wape (#393 A3-PR3, ADR-030). NULL = '
    'not computable — the seasonal-naive needs >= 1 full season of history to '
    'have a value one season ago (None-honest contract, migration 055); NEVER a '
    'masked 0.';
COMMENT ON COLUMN pyramide_accuracy_metrics.naive_mase IS
    'MASE of the seasonal-naive baseline, scored on the SAME backtest as mase '
    '(#393 A3-PR3, ADR-030). NULL = not computable (needs >= 1 season of '
    'history and a usable naive-scaling denominator); NEVER a masked 0.';
COMMENT ON COLUMN pyramide_accuracy_metrics.fva_wape IS
    'Forecast Value Added on WAPE = naive_wape - wape (#393 A3-PR3, ADR-030). '
    'POSITIVE = the stat model beats the trivial seasonal-naive baseline (it '
    'removed error). NEGATIVE is a legitimate honest result (model lost to the '
    'baseline) — deliberately NOT clamped. NULL when EITHER operand is NULL '
    '(baseline undefined, or wape NULL per migration 055, e.g. per-horizon '
    'rows) = not comparable; NEVER a masked 0.';
COMMENT ON COLUMN pyramide_accuracy_metrics.fva_mase IS
    'Forecast Value Added on MASE = naive_mase - mase (#393 A3-PR3, ADR-030). '
    'POSITIVE = the stat model beats the seasonal-naive baseline. NEGATIVE is '
    'legitimate (model lost) — NOT clamped. NULL when EITHER operand is NULL '
    '(baseline undefined, or mase NULL per migration 055) = not comparable; '
    'NEVER a masked 0.';

COMMIT;
