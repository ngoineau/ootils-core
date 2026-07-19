-- ============================================================
-- Migration 088 — C2 : value-ledger event types + decision-basis stamps
-- ============================================================
-- Chantier 2 du programme "moteur d'exception" (dossier
-- DOSSIER-MOTEUR-EXCEPTION-2026-07-19.md §A1). Two orthogonal schema needs:
--
--   (1) DECISION BASIS (doctrine §3) — calc_runs learns WHICH as-of date, WHICH
--       propagation flavour and WHICH code identity produced it; the supply
--       watchers stamp WHICH as-of date and WHICH event high-water mark they
--       decided against onto every recommendation they emit. Prerequisite for
--       bit-identical replayability and for an auditor to reconstruct the exact
--       basis of a decision.
--   (2) VALUE LEDGER (doctrine §1/§2) — the CHANGED-ONLY ingest path (VOLET 1,
--       api/routers/ingest.py) emits typed old/new events on a real business
--       change; this migration widens events.event_type's CHECK so the four
--       NEW C2 types can be inserted. The reused types (onhand_updated,
--       supply_qty_changed, supply_date_changed, demand_qty_changed) are
--       ALREADY in the CHECK since migration 002 — no addition needed for them.
--
-- Defensive-idempotence header (pattern from migration 063's header, mandatory):
-- the runner (db/connection.py) wraps THIS file in ONE transaction and ABORTS on
-- any error WITHOUT swallowing "already exists"; a migration that errors is NOT
-- recorded as applied and re-runs every statement from scratch at the next boot.
-- So every statement below is a clean no-op on a second attempt: ADD COLUMN
-- IF NOT EXISTS, and DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT for the CHECK.
--
-- >>> PIÈGE CHECK (doctrine C2 §4) — READ BEFORE EDITING THE LIST BELOW <<<
-- The events.event_type list below REPARTS de migration 086 (24 types, INCLUANT
-- 'reconciliation_completed', PR-5b — copiée en référence hors-arbre au moment
-- d'écrire) + les 4 nouveaux types C2 = 28 types. Elle NE REPART PAS de la liste
-- de main (085 = 23 types). Raison : les migrations s'appliquent en ordre
-- numérique et chaque widening fait DROP + ADD (il REMPLACE la contrainte). Au
-- merge final la séquence est ...085 -> 086 -> 088 ; si 088 repartait de 085 il
-- ferait SAUTER 'reconciliation_completed' de la contrainte à l'application de
-- 088, cassant tout INSERT de ce type émis par 086. La liste finale portée par
-- la migration au plus haut numéro (088) DOIT donc être le sur-ensemble complet.
--
-- >>> À VÉRIFIER AU REBASE <<< : 086 et 087 sont réservés par d'autres chantiers
-- et PAS ENCORE dans cet arbre (l'arbre culmine à 085). Si, au rebase, 087
-- (contenu inconnu à l'écriture) élargit AUSSI events.event_type, ajouter ses
-- types à la liste ci-dessous — sinon l'application séquentielle 086 -> 087 ->
-- 088 les ferait sauter de la contrainte. À l'application dans CET arbre (085 ->
-- 088, sans 086), 'reconciliation_completed' est simplement autorisé sans être
-- encore émis — inoffensif, même discipline "ahead of wiring" que 084/085.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- (1) calc_runs — decision basis (doctrine §3)
-- ------------------------------------------------------------
-- Tous NULLables : les lignes calc_runs pré-C2, ainsi que les sites d'INSERT non
-- instrumentés, restent NULL-honnêtes (jamais un 0/'' inventé). Stampés à
-- start_calc_run (engine/orchestration/calc_run.py) : anchor_date =
-- COALESCE(scenarios.as_of_date, CURRENT_DATE) ; engine_flavor = flavour
-- OOTILS_ENGINE normalisée ; code_version = OOTILS_CODE_VERSION sinon git short
-- sha sinon 'unknown', résolu UNE FOIS à l'import du module.
ALTER TABLE calc_runs ADD COLUMN IF NOT EXISTS anchor_date   DATE;
ALTER TABLE calc_runs ADD COLUMN IF NOT EXISTS engine_flavor TEXT;
ALTER TABLE calc_runs ADD COLUMN IF NOT EXISTS code_version  TEXT;

COMMENT ON COLUMN calc_runs.anchor_date IS
    'PAST-principle as-of date the run computed against '
    '(COALESCE(scenarios.as_of_date, CURRENT_DATE) at start_calc_run). '
    'Prerequisite for bit-identical replayability. NULL = run pre-C2.';
COMMENT ON COLUMN calc_runs.engine_flavor IS
    'Normalised OOTILS_ENGINE flavour that ran this run (sql|python|rust|'
    'rust-svc), mirroring the dispatch in events._build_propagation_engine. '
    'Provenance only, no CHECK. NULL = run pre-C2.';
COMMENT ON COLUMN calc_runs.code_version IS
    'Code identity: OOTILS_CODE_VERSION else the git short sha else ''unknown'', '
    'resolved ONCE at module import (never a subprocess per run). NULL = run pre-C2.';

-- ------------------------------------------------------------
-- (2) recommendations — decision basis stamped by the supply watchers
-- ------------------------------------------------------------
-- NULLable : stampés par les watchers supply (shortage, material, reschedule,
-- transfer) à l'INSERT de reco. Les AUTRES tables de reco
-- (parameter_recommendations / forecast_drift_recommendations /
-- eando_recommendations) suivront le même pattern plus tard — leurs lignes,
-- comme toute reco pré-C2 de cette table, restent NULL-honnêtes en attendant.
ALTER TABLE recommendations ADD COLUMN IF NOT EXISTS anchor_date    DATE;
ALTER TABLE recommendations ADD COLUMN IF NOT EXISTS stream_seq_hwm BIGINT;

COMMENT ON COLUMN recommendations.anchor_date IS
    'Decision-basis as-of date of the watcher run (the planning horizon_start it '
    'loaded against). NULL = reco pre-C2 or a non-supply writer not yet instrumented.';
COMMENT ON COLUMN recommendations.stream_seq_hwm IS
    'High-water mark of events.stream_seq the watcher had observed when deciding '
    '(the --subscribe drained cursor, else the current MAX(stream_seq) for the '
    'scenario). Opaque, compared with > only (migration 063). NULL = reco pre-C2.';

-- ------------------------------------------------------------
-- (3) events.event_type CHECK += 4 C2 value-ledger types
-- ------------------------------------------------------------
-- DROP + ADD (the widening idiom of every prior CHECK extension: 006, 051, 062,
-- 071, 076, 079, 084, 085) — replay-safe. Liste INTÉGRALE = 085 (23) +
-- 'reconciliation_completed' de 086 (24) + les 4 types C2 (28). Voir le PIÈGE
-- CHECK en tête de fichier avant toute modification de cette liste.
--
-- Les 4 types C2 sont émis par le chemin INGEST (VOLET 1, api/routers/ingest.py)
-- via une frozenset de validation LOCALE, jamais par emit_stream_event / la
-- famille FLEET_EVENT_TYPES (granularité run) — ils restent donc HORS de
-- FLEET_EVENT_TYPES et de VALID_EVENT_TYPES à dessein (types par-nœud).
ALTER TABLE events DROP CONSTRAINT IF EXISTS events_event_type_check;
ALTER TABLE events ADD CONSTRAINT events_event_type_check
    CHECK (event_type IN (
        -- migrations 002 + 006 + 051 + 062 + 071 + 076 + 079 + 084 + 085 (existants)
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
        -- migration 086 (ADR-042 décision 4, PR-5b) — NE PAS OUBLIER (voir PIÈGE CHECK)
        'reconciliation_completed',
        -- migration 088 (C2 value-ledger — types par-nœud émis à l'ingest)
        'supply_status_changed',
        'supply_uom_changed',
        'demand_date_changed',
        'demand_status_changed'
    ));

COMMIT;
