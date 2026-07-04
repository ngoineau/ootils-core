-- ============================================================
-- Migration 061 — Reschedule messages + Firm Planned Orders + dampening (#346)
-- ============================================================
-- Chantier #346 foundation. ADR-026 (à venir) : "Reschedule action
-- messages, Firm Planned Orders (FPO) and message dampening".
--
-- Three coordinated schema additions, all baseline-safe and idempotent:
--
--   (a) nodes.is_firm — the Firm Planned Order flag. A PlannedSupply with
--       is_firm=TRUE is EXCLUDED from the MRP full-regeneration purge
--       (engine/mrp/graph_integration.py:cleanup_previous_run, run_id=None
--       scope: "firmed orders must survive this purge", already
--       anticipated in the code comments) and is treated as a CLOSED /
--       committed receipt in netting (gross-to-net counts it as scheduled
--       supply, not a re-plannable planned order). It stays RE-DATABLE by a
--       reschedule message — that is precisely the APICS point of an FPO:
--       the planner (or a governed agent reco) owns its date, MRP no longer
--       regenerates it, but a RESCHEDULE_IN/OUT can still move it. Only a
--       tiny minority of PlannedSupply rows are firmed, hence a PARTIAL
--       index (WHERE is_firm) rather than a full index.
--
--   (b) recommendations — extend the governed action state machine
--       (migration 039) with the reschedule/defer/cancel vocabulary and
--       add the three typed columns that carry a reschedule message
--       (target node + current date + proposed date). All three are NULL
--       for the existing EXPEDITE/ORDER_* actions (which reference no
--       specific supply node), populated for RESCHEDULE_*/DEFER/CANCEL.
--
--   (c) item_planning_params — dampening thresholds (anti message-storm):
--       don't disturb the planner for a sub-threshold date/qty nudge.
--
-- Idempotence (repo migration policy: a re-run marked "already exists" is
-- recorded as applied, so every statement must be safe to replay):
--   * ADD COLUMN IF NOT EXISTS              — no-op on re-run.
--   * CREATE INDEX IF NOT EXISTS            — no-op on re-run.
--   * DROP CONSTRAINT IF EXISTS + ADD       — deterministic re-create (the
--     only safe way to WIDEN a CHECK; PG has no ALTER CONSTRAINT for CHECK).
-- No JSONB. Typed columns only.
--
-- SCD2 / GENERATED note: item_planning_params (migration 007) is a SCD2
-- table (effective_from/to, a btree_gist EXCLUDE constraint on
-- (item_id, location_id, daterange), and a GENERATED ALWAYS STORED column
-- lead_time_total_days). ADD COLUMN ... DEFAULT is safe here: the new
-- columns are not referenced by the generated expression nor by the
-- exclusion constraint, so no version row is invalidated and no GiST
-- recheck is triggered. The DEFAULT backfills every existing SCD2 version
-- row (all historical + current) uniformly, which is the intended
-- behaviour for a newly-introduced planning threshold.
--
-- Scenario overlay note (#347): reschedule_min_days /
-- reschedule_qty_tolerance_pct are DELIBERATELY NOT added to the
-- scenario_planning_overrides whitelist (migration 060). Forkability of
-- the dampening thresholds is a deferred V2 refinement — what actually
-- shifts need-dates inside a fork (lead times, safety stock) is ALREADY
-- forkable through the overlay, which is the substance. These two knobs
-- stay baseline-only for this V1.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- (a) Firm Planned Order flag on nodes
-- ------------------------------------------------------------
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS is_firm BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN nodes.is_firm IS
    'Firm Planned Order flag (#346). TRUE on a PlannedSupply node means: '
    'excluded from the MRP full-regeneration purge (cleanup_previous_run) '
    'and netted as a CLOSED/committed receipt, but still re-datable by a '
    'RESCHEDULE_IN/OUT message. FALSE (default) = ordinary re-generatable '
    'planned order. Only meaningful for node_type=PlannedSupply.';

-- FPOs are a small minority of PlannedSupply rows → partial index, matching
-- the scenario-scoped (scenario_id, item_id) lookup the netting/reschedule
-- passes use to find firmed supply.
CREATE INDEX IF NOT EXISTS idx_nodes_firm
    ON nodes (scenario_id, item_id)
    WHERE is_firm;

-- ------------------------------------------------------------
-- (b) Governed recommendation actions: reschedule/defer/cancel vocabulary
-- ------------------------------------------------------------
-- The action CHECK created in migration 039 is an inline single-column
-- constraint (action TEXT NOT NULL CHECK (action IN (...))), so Postgres
-- auto-named it 'recommendations_action_check' (<table>_<column>_check).
-- Widen it: keep the three existing actions, add the #346 reschedule set.
-- DEFER/CANCEL apply to an existing (planned or firm) supply node;
-- RESCHEDULE_IN pulls a receipt earlier, RESCHEDULE_OUT pushes it later.
-- Keep this list in sync with the VALID action vocabulary in the agent /
-- recommendation code (scripts/agent_governance.py:decision_level and the
-- recommendation writers).
ALTER TABLE recommendations DROP CONSTRAINT IF EXISTS recommendations_action_check;
ALTER TABLE recommendations ADD CONSTRAINT recommendations_action_check CHECK (
    action IN (
        'EXPEDITE', 'ORDER_RUSH', 'ORDER_NOW',
        'DEFER', 'CANCEL', 'RESCHEDULE_IN', 'RESCHEDULE_OUT'
    )
);

-- Reschedule-message payload: which supply node, its current date, the
-- engine-proposed date. NULL for the existing order/expedite actions.
--
-- target_node_id FK policy: REFERENCES nodes(node_id) ON DELETE SET NULL,
-- nullable.
--   * A hard FK to nodes(node_id) is the house pattern (edges, events,
--     explanations, shortages all declare one — migrations 002/004/005).
--   * nodes are SOFT-deleted in normal operation (active=FALSE, never a
--     hard DELETE — see graph_integration.py cleanup_previous_run), so the
--     ON DELETE branch is a safety net, not a live path.
--   * ON DELETE SET NULL (not the default RESTRICT, not CASCADE) is the
--     right semantics for an audit message: if the target supply node is
--     ever genuinely hard-deleted, the recommendation must SURVIVE as an
--     audit record (its current_receipt_date / proposed_date / item_id
--     still carry the message) with a dangling target nulled out — it must
--     neither block the delete (RESTRICT) nor be destroyed (CASCADE).
ALTER TABLE recommendations
    ADD COLUMN IF NOT EXISTS target_node_id       UUID REFERENCES nodes(node_id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS current_receipt_date DATE,
    ADD COLUMN IF NOT EXISTS proposed_date        DATE;

COMMENT ON COLUMN recommendations.target_node_id IS
    'PlannedSupply/FPO node this reschedule message targets (#346). NULL '
    'for EXPEDITE/ORDER_* (no specific target node). ON DELETE SET NULL: '
    'the reco survives as an audit record if the node is hard-deleted.';
COMMENT ON COLUMN recommendations.current_receipt_date IS
    'Current receipt date of the targeted order (#346). NULL for '
    'EXPEDITE/ORDER_* actions.';
COMMENT ON COLUMN recommendations.proposed_date IS
    'Engine-proposed new receipt date for the targeted order (#346). NULL '
    'for EXPEDITE/ORDER_* actions.';

-- Reschedule messages are looked up by their target node (e.g. "is there a
-- pending reschedule on this FPO?"); partial index keeps it lean.
CREATE INDEX IF NOT EXISTS ix_reco_target_node
    ON recommendations (target_node_id)
    WHERE target_node_id IS NOT NULL;

-- ------------------------------------------------------------
-- (c) Dampening thresholds on item_planning_params (baseline-only, #346)
-- ------------------------------------------------------------
-- Below reschedule_min_days of date movement OR within
-- reschedule_qty_tolerance_pct of quantity change, no message is emitted —
-- the planner is not disturbed by nervous, sub-material nudges. Domain
-- CHECKs match the >= 0 style of the other item_planning_params thresholds.
ALTER TABLE item_planning_params
    ADD COLUMN IF NOT EXISTS reschedule_min_days INTEGER NOT NULL DEFAULT 3
        CHECK (reschedule_min_days >= 0),
    ADD COLUMN IF NOT EXISTS reschedule_qty_tolerance_pct NUMERIC(6,2) NOT NULL DEFAULT 5.0
        CHECK (reschedule_qty_tolerance_pct >= 0);

COMMENT ON COLUMN item_planning_params.reschedule_min_days IS
    'Dampening (#346): minimum receipt-date movement in days before a '
    'reschedule message is emitted. Below this, no message. Baseline-only '
    '(not in the #347 scenario overlay whitelist for V1).';
COMMENT ON COLUMN item_planning_params.reschedule_qty_tolerance_pct IS
    'Dampening (#346): quantity-change tolerance in percent before a '
    'reschedule message is emitted. Within this band, no message. '
    'Baseline-only (not in the #347 scenario overlay whitelist for V1).';

COMMIT;
