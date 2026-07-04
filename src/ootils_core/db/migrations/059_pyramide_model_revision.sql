-- ============================================================
-- Migration 059 — Pyramide FM weights seal (axis B, PR-B2)
-- ============================================================
-- FM_CHRONOS now runs the real Chronos wrapper
-- (src/ootils_core/pyramide/foundation.py). A foundation-model forecast
-- is a dated, seeded, versioned artifact (spec §2.B "déterminisme"):
-- (random_seed, code_version) were already persisted; model_revision
-- completes the seal with the identity of the WEIGHTS that produced the
-- values — the HuggingFace commit SHA of the loaded snapshot when the
-- backend exposes it, else the pinned revision or the chronos package
-- version (foundation.LoadedPipeline.revision_source documents which;
-- never a fabricated SHA).
--
-- NULL = the values were not produced by a foundation model: every
-- non-FM method, and FM requests served by the deterministic
-- AUTO_SELECT fallback (the fallback's provenance lives in
-- selected_model/engine_backend + warnings, not here).
--
-- Idempotence (repo migration policy: a re-run must not fail):
--   * ADD COLUMN IF NOT EXISTS — no-op on re-run.
-- No JSONB. Typed column only.
-- ============================================================

BEGIN;

ALTER TABLE pyramide_runs ADD COLUMN IF NOT EXISTS model_revision TEXT;

COMMENT ON COLUMN pyramide_runs.model_revision IS
    'Scellé des poids du modèle de fondation ayant produit les valeurs '
    '(SHA de commit HuggingFace du snapshot chargé quand le backend '
    'l''expose, sinon révision demandée ou version du package chronos — '
    'jamais un SHA fabriqué). NULL pour les méthodes non-FM et pour les '
    'requêtes FM servies par le fallback déterministe.';

COMMIT;
