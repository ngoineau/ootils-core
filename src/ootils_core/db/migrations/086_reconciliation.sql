-- ============================================================
-- Migration 086 — Reconciliation heuristique (ADR-042 decision 4, PR-5b)
-- ============================================================
-- ADR-042 (docs/ADR-042-interface-doctrine.md) decision 4 ("Le sortant et la
-- réconciliation"), the second half not covered by PR-5a (migrations
-- 078/085, exported_at + export_executed): "un watcher déterministe
-- baseline-only ... rapproche un PO entrant avec une reco APPROVED déjà
-- exportée sur (item, location, supplier, qty ± tolérance, date ± fenêtre),
-- et stampe fulfilled_at + fulfilled_erp_id. C'est une OBSERVATION, jamais
-- une écriture appliquée automatiquement." See also the PR-5a amendment
-- (§"PR-5b — nommée, non livrée : gap-location identifié") for the known
-- structural gap: `recommendations` carries no generic site column for
-- po_drafts/reschedule_messages (only TRANSFER does, via
-- source_location_id/dest_location_id, migration 066) — the matcher's
-- heuristic for those two families is therefore item+supplier+qty+date only,
-- no site.
--
-- KNOWN GAP 1 — supplier NOT persisted on the PO node (verified during PR-5b
-- against migration 002 + the PO ingest path; corrects the "item+supplier+qty+
-- date" wording just above). `api/routers/ingest.py`'s ingest_purchase_orders
-- VALIDATES supplier_external_id (the FK check) but NEVER persists it onto the
-- PurchaseOrderSupply node — the supplier is validated-then-discarded, and
-- `nodes` has no supplier column. So the inbound-PO side of the match ALWAYS
-- carries a NULL supplier in V1, and the supplier criterion only constrains a
-- pair when BOTH sides carry a supplier (i.e. never today). The heuristic that
-- actually runs is therefore item+qty+date (plus dest-site for the TRANSFER
-- family), supplier held in reserve. This is the exact twin of the location
-- gap the PR-5a amendment documented for `recommendations`: the consequence is
-- a potentially HIGHER ambiguity rate (supplier cannot disambiguate two recos
-- for the same item/qty/date) — always COUNTED and PUBLISHED in the daily
-- report's ambiguity signal, never hidden or silently resolved (ADR-042
-- decision 4). The matcher keeps the supplier comparison in its pure core, so
-- the day PO ingest starts persisting the supplier the criterion activates
-- with ZERO code change (see engine/reconciliation/matcher.py's "KNOWN GAP 1").
--
-- SCOPE OF THIS MIGRATION: schema only (the two observation columns, the
-- append-only run ledger, the events.event_type CHECK widening). The
-- matcher engine itself (the heuristic scan, the ambiguity classification)
-- is separate PR-5b code — engine/reconciliation/matcher.py, delivered in
-- the SAME PR, not in this file — same "schema-first" split as migration
-- 078's daily_runs/exported_at ahead of PR-3's decision engine. Chaining
-- reconciliation facts into recommendation_outcomes (ADR-030's evaluator)
-- is DEFERRED, not delivered in PR-5b: the evaluator does not read
-- fulfilled_at today.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- (1) recommendations.fulfilled_at / fulfilled_erp_id — OBSERVATION ONLY
-- ------------------------------------------------------------
-- These two columns are a FACT stamped by the reconciliation matcher when it
-- heuristically pairs an inbound ERP PO with an already-exported
-- recommendation. They are deliberately NOT a status: no CHECK, no trigger,
-- no coupling whatsoever to `recommendations.status` or to
-- `engine/recommendation/state_machine.py`'s ALLOWED_TRANSITIONS /
-- HUMAN_ONLY_TARGETS (migration 039/051) — that state machine remains the
-- SOLE governor of DRAFT -> REVIEWED -> APPROVED -> APPLIED/REJECTED, and
-- APPROVED/APPLIED stay human-only gates (ADR-042 §"Décision Ladder": "la
-- réconciliation reste une observation, jamais L3+/appliquée
-- automatiquement"). A recommendation can be stamped fulfilled_at while its
-- status stays exactly what it already was (typically APPROVED or APPLIED,
-- since only an exported row — exported_at IS NOT NULL — is ever a matcher
-- candidate, but that eligibility rule lives in the matcher's own WHERE
-- clause, never in a schema constraint here, same posture as
-- outbound_export.py's own "status IN (...) is the ENTIRE gate" doctrine).
--
-- fulfilled_erp_id is TEXT, not a FK/UUID: per the ADR-042 PR-5a amendment
-- ("recommendation_id = référence humaine, jamais échoable"), there is no
-- ootils_ref round-trip field in the pilot's ERP — this column holds
-- whatever heuristically-matched identifier the inbound PO feed carries
-- (e.g. a PO number), an opaque external string, never validated against a
-- foreign key.
--
-- NULL = not yet reconciled (or never matched — see reconciliation_runs
-- below for the honest ambiguous/unmatched counts, never silently folded
-- into a fabricated match).
ALTER TABLE recommendations ADD COLUMN IF NOT EXISTS fulfilled_at TIMESTAMPTZ;
ALTER TABLE recommendations ADD COLUMN IF NOT EXISTS fulfilled_erp_id TEXT;

COMMENT ON COLUMN recommendations.fulfilled_at IS
    'When the reconciliation matcher (ADR-042 decision 4, PR-5b) heuristically '
    'paired this exported recommendation with an inbound ERP PO. OBSERVATION '
    'ONLY -- never a status, no CHECK/trigger tying it to recommendations.status '
    'or to the state machine''s HUMAN_ONLY_TARGETS (engine/recommendation/'
    'state_machine.py). NULL = not yet reconciled.';

COMMENT ON COLUMN recommendations.fulfilled_erp_id IS
    'The heuristically-matched inbound ERP identifier (e.g. a PO number), an '
    'opaque external string -- no ootils_ref round-trip field exists in the '
    'pilot''s ERP (ADR-042 PR-5a amendment). Not a foreign key. NULL until '
    'fulfilled_at is stamped (both are always set together by the matcher, '
    'though this is an application-level invariant, not a DB constraint, '
    'same posture as exported_at/the outbound-export gate).';

-- The matcher's pending-reconciliation scan: every exported recommendation
-- not yet reconciled. Indexed on the two attributes the heuristic actually
-- joins an inbound PO row on for the po_drafts/reschedule_messages families
-- (item + supplier -- no site available for those two families, see header;
-- qty ± tolerance / date ± window are range comparisons, not usefully
-- indexed here). Same partial-index idiom as ix_reco_pending_export
-- (migration 078): the common case (an already-reconciled row) is never
-- rescanned by this predicate again.
CREATE INDEX IF NOT EXISTS ix_reco_pending_reconciliation
    ON recommendations (item_external_id, supplier_external_id)
    WHERE exported_at IS NOT NULL AND fulfilled_at IS NULL;

-- ------------------------------------------------------------
-- (2) reconciliation_runs — append-only matcher-run audit trail
-- ------------------------------------------------------------
-- WHY NO scenario_id (mirrors migration 078's daily_runs rationale
-- verbatim): a reconciliation run heuristically pairs OBSERVED inbound ERP
-- POs with already-exported baseline recommendations (outbound_export.py is
-- itself hardcoded BASELINE_SCENARIO_ID-only, migration 085's header) --
-- this is a fact about the real world, not scenario-scoped working state
-- (unlike nodes/edges/shortages). Same rationale ADR-030 already established
-- for inventory_snapshots/recommendation_outcomes/daily_runs: an observed
-- ERP reconciliation is baseline-by-nature, a fork stays simulated and is
-- never reconciled against a real PO.
--
-- WHY test_purge_whitelist_guard.py DOES NOT NEED an entry for this table
-- (neither PURGE_WHITELIST nor PURGE_EXEMPT_TABLES): that guard's
-- _discover_scenario_scoped_tables() (tests/test_purge_whitelist_guard.py)
-- flags a table ONLY when its CREATE TABLE body contains a literal
-- `scenario_id` column (plus two hand-verified indirect exceptions,
-- explanations/causal_steps -- not applicable here). reconciliation_runs
-- carries no such column, so the regex-based scan correctly never discovers
-- it as scenario-scoped -- exactly the same "correctly does not appear in
-- that scan" outcome documented for daily_runs/feed_contracts. No purge
-- reclaim path is needed for a baseline-only, append-only, low-cardinality
-- (one row per matcher run, not per recommendation) audit table.
--
-- APPEND-ONLY, NOT UPSERTED: one row per matcher execution, same philosophy
-- as daily_runs/calc_runs/maintenance_purge_runs -- a run_date can
-- legitimately have more than one attempt (re-run after a late-arriving PO
-- file); the "current" picture for a run_date is the most recent row by
-- created_at.
--
-- Counts are honest tallies from the SAME matcher pass, never independently
-- derived: recos_candidates = exported rows the matcher considered this run
-- (the ix_reco_pending_reconciliation scan's cardinality at run time),
-- matched = candidates the heuristic paired unambiguously (fulfilled_at
-- stamped this run), ambiguous = candidates with more than one plausible PO
-- match OR a PO with more than one plausible candidate ("ou l'inverse",
-- ADR-042 decision 4) -- never silently resolved, always counted and
-- reported (the daily report's published ambiguity rate), unmatched = PO
-- rows or candidates with zero plausible pairing. No arithmetic CHECK ties
-- these four together (matched + ambiguous + unmatched need not equal
-- recos_candidates by construction -- ambiguity can be counted from either
-- side of the pairing) -- only non-negativity is enforced here; the matcher
-- code owns the actual semantics.
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    run_id           UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    run_date         DATE        NOT NULL,
    recos_candidates INTEGER     NOT NULL CHECK (recos_candidates >= 0),
    matched          INTEGER     NOT NULL CHECK (matched >= 0),
    ambiguous        INTEGER     NOT NULL CHECK (ambiguous >= 0),
    unmatched        INTEGER     NOT NULL CHECK (unmatched >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE reconciliation_runs IS
    'Append-only audit trail for the heuristic PO-vs-recommendation '
    'reconciliation matcher (ADR-042 decision 4, PR-5b). One row per matcher '
    'RUN (never per recommendation/PO) -- baseline-only by nature, '
    'deliberately no scenario_id (see migration header). The published '
    'ambiguity rate (ambiguous / recos_candidates) is the daily report''s '
    'honesty signal: never hidden, never silently resolved.';

-- Newest-first lookup for a given run_date (re-evaluation intra-day) and the
-- daily report's "latest run" read -- same shape as
-- idx_daily_runs_feed_key_run_date (migration 078).
CREATE INDEX IF NOT EXISTS idx_reconciliation_runs_run_date
    ON reconciliation_runs (run_date DESC, created_at DESC);

-- ------------------------------------------------------------
-- (3) events.event_type CHECK += 'reconciliation_completed'
-- ------------------------------------------------------------
-- ONE event per reconciliation matcher RUN (ADR-027 run-granularity, the
-- same convention reused verbatim by migrations 076/079/084/085):
-- 'reconciliation_completed' is emitted once per matcher execution, never
-- once per recommendation stamped fulfilled_at. Companion audit table EXISTS
-- this time (reconciliation_runs, above) -- unlike daily_run_completed/
-- export_executed (migrations 079/085, no companion table, artifact/decision
-- carried directly in new_text), this event follows the OTHER idiom already
-- used by calc_run_finished/shortage_detected/outcome_evaluated/
-- demand_descended: new_text carries the companion table's run id (as text)
-- so a subscriber can join reconciliation_runs for the full
-- candidates/matched/ambiguous/unmatched breakdown. See
-- engine/events/emit.py's typed-column contract block (updated in THIS PR
-- alongside this CHECK widening) for the exact new_quantity/new_text/
-- old_text mapping.
--
-- EMISSION SITE (dated 2026-07-18, reworded 2026-07-19 at review): the
-- matcher engine IS delivered in this same PR —
-- engine/reconciliation/matcher.py, whose run_reconciliation() is the one
-- real emission site for this type (an earlier draft of this header,
-- written when the schema landed ahead of the engine, said the site was
-- "not written yet"; that became false the moment the matcher landed in
-- the same branch). FLEET_EVENT_TYPES / VALID_EVENT_TYPES / the derivation
-- tests are updated in THIS PR so the CHECK, the emitter's local validation
-- and the router's validation never drift apart.
--
-- Idempotence (pattern from migration 063's header, mandatory -- the runner
-- wraps each file in ONE transaction and ABORTS on any error, it does NOT
-- swallow "already exists", so a fixed migration re-attempted at the next
-- boot re-runs every statement from scratch):
--   * DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT -- the events.event_type
--     widening idiom used by every prior CHECK extension (006, 051, 062,
--     071, 076, 079, 084, 085): a bare ADD would fail on re-run because the
--     constraint already exists; DROP-first makes it replay-safe.
--
-- Complete list reconstructed from migrations 002 + 006 + 051 + 062 + 071 +
-- 076 + 079 + 084 + 085 (085 is the latest widening before this one --
-- verified no events.event_type CHECK widening exists between 085 and this
-- migration). Adds exactly one new type: 'reconciliation_completed' (the
-- 24th). Follow-up (same discipline as every prior widening migration's own
-- follow-up note, applied consciously in THIS PR, not deferred -- the
-- PURGE-1/PR-3 lesson): keep VALID_EVENT_TYPES in
-- src/ootils_core/api/routers/events.py AND FLEET_EVENT_TYPES in
-- src/ootils_core/engine/events/emit.py (plus its pinned unit/integration
-- test derivations, tests/test_emit_stream_event.py and
-- tests/integration/test_fleet_events_integration.py) in sync with this
-- CHECK -- all updated in THIS PR.

ALTER TABLE events DROP CONSTRAINT IF EXISTS events_event_type_check;
ALTER TABLE events ADD CONSTRAINT events_event_type_check
    CHECK (event_type IN (
        -- migrations 002 + 006 + 051 + 062 + 071 + 076 + 079 + 084 + 085 (existing, unchanged)
        'supply_date_changed', 'supply_qty_changed',
        'demand_qty_changed', 'onhand_updated',
        'policy_changed', 'structure_changed',
        'scenario_created', 'calc_triggered',
        'ingestion_complete', 'po_date_changed',
        'test_event', 'scenario_merge',
        'recommendation_transition',
        'node_firm_changed',
        'recommendation_created', 'shortage_detected',
        'calc_run_finished', 'snapshot_captured',
        'outcome_evaluated',
        'purge_executed',
        'daily_run_completed',
        'demand_descended',
        'export_executed',
        -- migration 086 (ADR-042 decision 4, PR-5b)
        'reconciliation_completed'
    ));

COMMIT;
