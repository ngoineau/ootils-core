-- ============================================================
-- Migration 066 — Governed inter-site TRANSFER recommendations (#395 PR2b)
-- ============================================================
-- Chantier #395 DRP. ADR-028 (à venir): "DRP transfer rounding + governed
-- inter-site transfer recommendations".
--
-- WHAT: the DRP planner will emit inter-site replenishment TRANSFER moves as
-- GOVERNED recommendations (same recommendations table + state machine as the
-- procurement / reschedule fleet, migrations 039/061), not as free-floating
-- rows. A TRANSFER is a supply *relocation* between two locations: it has a
-- SOURCE (ship-from) and a DESTINATION (ship-to) — a coordinate that no
-- existing recommendation action carries (EXPEDITE/ORDER_*/RESCHEDULE_*/DEFER/
-- CANCEL all act on a single node or supplier, never on a location pair). Two
-- additive, typed, nullable location columns fill that gap.
--
-- DECISION LADDER: a DRP transfer is a NEW-ORDER draft — it commits a physical
-- move of finished stock and, until executed, is reversible — so it is an L1
-- reco (same class as ORDER_NOW new-order drafts, per
-- scripts/agent_governance.py:decision_level), a human-approved DRAFT in the
-- #341 state machine, NOT an autonomous write. The decision_level column
-- already defaults to 'L1'; the concrete level is stamped by
-- agent_governance.decision_level(action), never hardcoded here.
--
-- REUSE (no new column for qty/date — deliberate): a transfer's quantity and
-- proposed ship/receive date already have canonical homes on recommendations:
--   * recommended_qty (NUMERIC NOT NULL, migration 039) = the transfer qty
--     (already DRP fair-share + transfer_multiple DOWN-rounded, migration 065).
--   * proposed_date   (DATE, migration 061)            = the proposed transfer
--     date. NULL-able there; a TRANSFER reco populates it.
-- This migration therefore adds ONLY the source/dest coordinate. If the DRP
-- later needs a distinct ship-vs-receive date pair, that is a follow-up column,
-- not smuggled in here.
--
-- FK POLICY (mirrors the migration-061 target_node_id rationale exactly):
-- REFERENCES locations(location_id) ON DELETE SET NULL, both columns NULLABLE.
--   * Hard FK to locations(location_id) is the house pattern (item_planning_
--     params, nodes, resources, mps_nodes … all declare one — migrations
--     002/007/009/027).
--   * NULLABLE: the OTHER actions (EXPEDITE/ORDER_*/RESCHEDULE_*/DEFER/CANCEL)
--     have no source/dest location pair, so these columns are NULL for every
--     non-TRANSFER reco — exactly as target_node_id/current_receipt_date/
--     proposed_date are NULL for the non-reschedule actions (migration 061).
--   * ON DELETE SET NULL (not the default RESTRICT, not CASCADE): a
--     recommendation is an AUDIT record. If a location is ever genuinely
--     hard-deleted, the reco must SURVIVE (its item_id / recommended_qty /
--     proposed_date still carry the message) with the dangling location nulled
--     out — it must neither block the delete (RESTRICT) nor be destroyed
--     (CASCADE). The audit outlives the master-data row.
--
-- CHECK (recommendations.action): widened to add 'TRANSFER'. The constraint is
-- the inline single-column check created in migration 039 and last (re)defined
-- in migration 061, PG-auto-named 'recommendations_action_check'
-- (<table>_<column>_check). Keep this vocabulary in sync with the VALID action
-- set in scripts/agent_governance.py:decision_level and the recommendation
-- writers.
--
-- Idempotence (repo migration policy — the runner in db/connection.py wraps
-- each file in its own transaction and, on ANY error, ROLLS BACK and ABORTS;
-- it does NOT swallow "already exists"): every statement re-runs as a clean
-- no-op.
--   * ADD COLUMN IF NOT EXISTS          — skips the column on re-run.
--   * DROP CONSTRAINT IF EXISTS + ADD   — deterministic re-create; the only
--     replay-safe way to WIDEN a named CHECK, since PG has no ALTER/REPLACE for
--     a CHECK constraint (matching migrations 061/065).
-- No JSONB. Typed columns only.
--
-- ref: ADR-028 (DRP), #395 PR2b.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- (a) Widen the governed action vocabulary with TRANSFER
-- ------------------------------------------------------------
-- Keep the full existing set (EXPEDITE/ORDER_RUSH/ORDER_NOW from migration
-- 039; DEFER/CANCEL/RESCHEDULE_IN/RESCHEDULE_OUT from migration 061) and add
-- TRANSFER. DROP IF EXISTS + ADD is the replay-safe widen.
ALTER TABLE recommendations DROP CONSTRAINT IF EXISTS recommendations_action_check;
ALTER TABLE recommendations ADD CONSTRAINT recommendations_action_check CHECK (
    action IN (
        'EXPEDITE', 'ORDER_RUSH', 'ORDER_NOW',
        'DEFER', 'CANCEL', 'RESCHEDULE_IN', 'RESCHEDULE_OUT',
        'TRANSFER'
    )
);

-- ------------------------------------------------------------
-- (b) Inter-site transfer coordinate: source + destination location
-- ------------------------------------------------------------
-- NULL for every non-TRANSFER action; populated (both) for a TRANSFER reco.
-- ON DELETE SET NULL so the audit record outlives a hard-deleted location.
ALTER TABLE recommendations
    ADD COLUMN IF NOT EXISTS source_location_id UUID REFERENCES locations(location_id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS dest_location_id   UUID REFERENCES locations(location_id) ON DELETE SET NULL;

COMMENT ON COLUMN recommendations.source_location_id IS
    'Ship-from location of a DRP inter-site TRANSFER reco (#395 PR2b, '
    'ADR-028). NULL for all non-TRANSFER actions (EXPEDITE/ORDER_*/'
    'RESCHEDULE_*/DEFER/CANCEL have no location pair). ON DELETE SET NULL: '
    'the reco survives as an audit record if the location is hard-deleted.';
COMMENT ON COLUMN recommendations.dest_location_id IS
    'Ship-to location of a DRP inter-site TRANSFER reco (#395 PR2b, '
    'ADR-028). NULL for all non-TRANSFER actions. ON DELETE SET NULL: the '
    'reco survives as an audit record if the location is hard-deleted.';

-- Transfer recos are looked up by destination (e.g. "what inbound transfers
-- replenish this DC?") and by source (e.g. "what is this plant shipping
-- out?"). Both are FK columns joined against locations — index each. Partial
-- (WHERE ... IS NOT NULL) keeps them lean, since the columns are NULL for the
-- majority (non-TRANSFER) rows — mirroring ix_reco_target_node (migration 061).
CREATE INDEX IF NOT EXISTS ix_reco_dest_location
    ON recommendations (dest_location_id)
    WHERE dest_location_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_reco_source_location
    ON recommendations (source_location_id)
    WHERE source_location_id IS NOT NULL;

COMMIT;
