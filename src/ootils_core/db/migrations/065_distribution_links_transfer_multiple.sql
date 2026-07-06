-- ============================================================
-- Migration 065 — distribution_links.transfer_multiple (DRP fair-share, #395 PR2a)
-- ============================================================
-- Chantier #395 DRP fair-share. ADR-028 (à venir): "DRP transfer rounding —
-- a per-lane logistics shipment multiple, rounded DOWN".
--
-- WHAT: one additive, typed column on distribution_links (migration 029) —
-- transfer_multiple, the logistics shipment multiple of a lane (case /
-- pallet / full-truck unit). The pilot has decided replenishment transfers
-- must respect a per-lane logistical rounding: you ship whole cases, not
-- fractional units. DEFAULT 1 means "no rounding", so every existing lane
-- keeps its current continuous behaviour untouched (rolling-deploy safe,
-- fully backward-compatible) until an operator sets a real multiple.
--
-- WHY ROUNDED DOWN (the load-bearing semantic, and a DELIBERATE divergence
-- from MRP lot sizing): DRP MOVES already-finished stock between locations;
-- it does not create it. Rounding a transfer UP would ship MORE finished
-- goods downstream than the true net requirement — starving the source
-- location and over-committing physical inventory it may still need. So the
-- DRP rounding is the CONSERVATIVE floor: round the transfer DOWN to the
-- nearest whole multiple (bounded by what is actually needed), never up.
-- This is intentionally OPPOSITE to MRP lot sizing (engine/mrp/lot_sizing),
-- where lot_size does a CEIL: MRP MAKES or BUYS supply, so rounding a
-- production/purchase order UP to a lot multiple is correct (you satisfy the
-- need and carry a little extra). Same "respect a multiple" idea, opposite
-- rounding direction, because one engine creates supply and the other only
-- relocates it. This divergence is assumed and documented (ADR-028), not an
-- inconsistency to reconcile with lot_size.
--
-- TYPING: NUMERIC(18,6), matching every other quantity on distribution_links
-- (minimum_shipment_qty, maximum_shipment_qty, transit_cost_per_unit/fixed
-- are all NUMERIC(18,6) in migration 029) — no FLOAT/REAL on a quantity.
-- CHECK (transfer_multiple > 0): a multiple must be strictly positive (a
-- zero or negative "case size" is meaningless and would make the DOWN-round
-- ill-defined / divide-by-zero). Strictly > 0, not >= 0, unlike the
-- migration-029 minimum_shipment_qty >= 0 (a zero MINIMUM is a legitimate
-- "no floor"; a zero MULTIPLE is not a legitimate "no rounding" — that is
-- what DEFAULT 1 expresses).
--
-- Idempotence (repo migration policy — the runner in db/connection.py wraps
-- each file in its own transaction and, on ANY error, ROLLS BACK and ABORTS;
-- it does NOT swallow "already exists"): every statement re-runs as a clean
-- no-op.
--   * ADD COLUMN IF NOT EXISTS          — skips the column on re-run.
--   * DROP CONSTRAINT IF EXISTS + ADD   — deterministic re-create; the only
--     replay-safe way to (re)assert a named CHECK, since PG has no CREATE OR
--     REPLACE / ALTER for a CHECK constraint (matching migrations 061/064).
-- The constraint is added out-of-line with an explicit canonical name
-- (distribution_links_transfer_multiple_check) precisely so DROP IF EXISTS +
-- ADD is name-deterministic on replay.
--
-- No index: transfer_multiple is a per-lane parameter read alongside the
-- lane row, never a filter/join predicate.
-- No JSONB. Typed column only.
--
-- ref: ADR-028 (DRP transfer rounding), #395 PR2a.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- Per-lane logistics shipment multiple (rounded DOWN by the DRP planner)
-- ------------------------------------------------------------
ALTER TABLE distribution_links
    ADD COLUMN IF NOT EXISTS transfer_multiple NUMERIC(18,6) NOT NULL DEFAULT 1;

-- Named, out-of-line CHECK asserted idempotently (DROP IF EXISTS + ADD) so a
-- partial-then-replayed migration re-creates it deterministically.
ALTER TABLE distribution_links
    DROP CONSTRAINT IF EXISTS distribution_links_transfer_multiple_check;
ALTER TABLE distribution_links
    ADD CONSTRAINT distribution_links_transfer_multiple_check
    CHECK (transfer_multiple > 0);

COMMENT ON COLUMN distribution_links.transfer_multiple IS
    'Logistics shipment multiple of this lane — case/pallet size in item '
    'units (#395 PR2a, ADR-028). The DRP planner rounds each transfer DOWN '
    'to the nearest whole multiple, bounded by the true net requirement: '
    'DRP MOVES finished stock, so it stays conservative and never '
    'over-transfers (deliberately the OPPOSITE of MRP lot_size, which CEILs '
    'because MRP creates supply). 1 (default) = no rounding, continuous '
    'transfers. Must be > 0.';

COMMIT;
