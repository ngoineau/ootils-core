-- ============================================================
-- Migration 081 — locations.is_stocking (virtual demand-channel exclusion)
-- ============================================================
-- Context (2026-07-17 plan modélisation, PR-B, ahead of the first real ERP
-- load — 14 TSV feeds): the first real load exposes ~9 400 dollar-valued
-- phantom shortages on three virtual demand channels (USA/CAN/ICO) that
-- carry real forecast/CO demand but ZERO supply of any kind (no PO, no
-- transfer, no on_hand, no planning params) — a virtual routing/allocation
-- node, not a physical stocking site. DSH (drop-ship) is the converse case:
-- fully modeled with real supply and stays a stocking location. Today
-- `locations` (migration 002) has no way to tell these apart — every
-- location is implicitly "stocking" for shortage-detection purposes.
--
-- WHAT this column is for: `is_stocking` is read ONLY by shortage
-- DETECTION (ADR-021's `shortages` table / `/v1/issues`), in a follow-up
-- backend change (not in this migration) that joins `locations.is_stocking
-- = TRUE` into PROPAGATE_SQL's / SHORTAGES_SQL's shortage CTE and the
-- Python detector (`engine/kernel/shortage/detector.py`), so a non-stocking
-- location's negative-closing PI buckets are never materialized as
-- `shortages` rows. The PROJECTION itself (ProjectedInventory) is computed
-- for every location regardless of this flag — explainability (ADR-004)
-- requires the numbers to exist and be inspectable even where they are not
-- surfaced as an actionable shortage. This is deliberately NOT modeled as
-- "safety_stock = 0": the phantom shortages are negative CLOSING STOCK
-- (demand with no supply at all), not a below-safety-stock condition, and
-- zeroing safety stock would neither suppress them nor be honest about why.
--
-- DEFAULT TRUE, existing behaviour unchanged: every location today
-- (including USA/CAN/ICO, once they exist) keeps participating in shortage
-- detection exactly as before until explicitly opted out. This migration
-- flips no switches — it only adds the switch.
--
-- NO UPDATE STATEMENT HERE, INTENTIONALLY (per the plan: "les valeurs
-- USA/CAN/ICO=FALSE sont posées AU CHARGEMENT, jamais dans la migration").
-- Two independent reasons, either one sufficient on its own:
--   1. Sequencing: this migration runs AHEAD of the first real ERP load —
--      the USA/CAN/ICO location rows do not exist in this database yet at
--      migration-apply time, so a schema-migration UPDATE targeting them
--      would be a no-op today and, worse, would have to hardcode
--      client-specific external_id/name matches into DDL to have any future
--      effect — the wrong layer for a business-data decision.
--   2. Ownership: which locations are non-stocking is a fact about THIS
--      load's real topology, decided by the ingest/loading code path (which
--      already has the real location roster and the domain judgement to
--      classify it), not a fact this codebase can bake into a generic
--      migration that ships to every environment. A schema migration adds
--      capability; it does not make the business call.
-- A location's is_stocking can be flipped later (by the loader, or an
-- operator) via a plain UPDATE outside this file — see the follow-up
-- backend change for the read-side consequence, and its ADR-021 amendment
-- for the "flipping after load requires an explicit full recompute" caveat
-- (no re-dirty is wired for a bare is_stocking flip in V1).
--
-- No index added: `locations` is a demo-scale reference table (a handful to
-- low hundreds of rows per docs/SCALABILITY.md's current scale), and the
-- follow-up read joins on the existing `location_id` PK with `is_stocking`
-- as a cheap in-memory filter on an already-tiny row set — a dedicated
-- index would add write overhead for no measurable read benefit at this
-- scale. Revisit if/when `locations` stops being reference-table-sized.
--
-- Idempotence (pattern from migration 063's header, mandatory — the runner
-- wraps each file in ONE transaction and ABORTS on any error, it does NOT
-- swallow "already exists", so a fixed migration re-attempted at the next
-- boot re-runs every statement from scratch):
--   * ADD COLUMN IF NOT EXISTS — skips the column entirely on re-run
--     (PG16 backfills a constant DEFAULT without a full table rewrite, so
--     this is also cheap on the first run against a populated table).
--   * COMMENT ON COLUMN         — re-applies harmlessly (COMMENT is not
--     additive; it replaces the prior comment, same idiom as migration 063).
--
-- No JSONB: is_stocking is a plain typed boolean flag, not a diagnostic
-- payload.
--
-- ref: plan-modélisation (2026-07-17, PR-B); ADR-021 (shortage truth —
-- amendment tracked in the follow-up backend PR, not this migration).
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- locations.is_stocking — shortage-detection eligibility flag
-- ------------------------------------------------------------
ALTER TABLE locations
    ADD COLUMN IF NOT EXISTS is_stocking BOOLEAN NOT NULL DEFAULT TRUE;

COMMENT ON COLUMN locations.is_stocking IS
    'Whether this location participates in shortage DETECTION (ADR-021 '
    '`shortages` table / /v1/issues). Default TRUE preserves existing '
    'behaviour for every location. Set to FALSE for virtual demand-only '
    'channels that carry forecast/CO demand but no supply of any kind '
    '(no PO, no transfer, no on_hand, no planning params) so their negative '
    'closing stock is not materialized as a dollar-valued phantom shortage. '
    'The ProjectedInventory PROJECTION is still computed for every location '
    'regardless of this flag (explainability, ADR-004) — only the '
    '`shortages` write is gated. Values for specific real-world locations '
    '(e.g. virtual routing channels) are set by the ingest/loading code '
    'path at data-load time, never by a schema migration. Flipping this '
    'flag after data has already been loaded requires an explicit full '
    'recompute (POST /v1/calc/run {"full_recompute": true}) — no incremental '
    're-dirty is wired for a bare is_stocking change in V1.';

COMMIT;
