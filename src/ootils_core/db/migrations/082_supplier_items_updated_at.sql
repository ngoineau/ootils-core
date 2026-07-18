-- ============================================================
-- Migration 082 — supplier_items.updated_at (bras UPDATE jamais exercé)
-- ============================================================
-- Découvert au PREMIER CHARGEMENT RÉEL (2026-07-18) : le bras UPDATE de
-- POST /v1/ingest/supplier-items (api/routers/ingest.py) stampe
-- `updated_at = now()` — mais AUCUNE migration n'a jamais créé cette
-- colonne sur supplier_items (007_import_pipeline la crée sans). Toutes
-- les tables sœurs mutables (items, suppliers, nodes, resources,
-- item_planning_params…) portent updated_at ; supplier_items était la
-- seule exception, et son bras UPDATE la référence quand même.
--
-- Pourquoi invisible jusqu'ici : sur une base VIDE, chaque supplier_item
-- est nouveau → seul le bras INSERT (qui ne mentionne pas updated_at)
-- s'exécute — la répétition générale sur schéma frais passe. Sur la base
-- pilote, des liens fournisseur-article préexistaient (bootstrap) → le
-- re-push du bundle prend le bras UPDATE → UndefinedColumn, 500, fichier
-- rejeté. Même famille de bug que les arêtes feeds_forward manquantes
-- (#468) : un chemin de code jamais piloté de bout en bout.
--
-- Le fix aligne le SCHÉMA sur l'intention du code (la colonne doit
-- exister, comme partout ailleurs) plutôt que l'inverse. Le test
-- d'intégration compagnon (test_supplier_items_repush_integration.py)
-- exerce désormais explicitement le bras UPDATE — le test qui manquait.
--
-- Idempotence défensive (convention migration 063) : ADD COLUMN IF NOT
-- EXISTS est un no-op propre au re-run comme sur toute base déjà saine.
-- Le DEFAULT now() peuple les lignes existantes au moment de l'ALTER
-- (PG16 : metadata-only) — honnête : « jamais modifié depuis la pose de
-- la colonne » vaut mieux qu'un NULL qui casserait le NOT NULL des sœurs.
-- ============================================================

ALTER TABLE supplier_items
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

COMMENT ON COLUMN supplier_items.updated_at IS
    'Stampé par le bras UPDATE de /v1/ingest/supplier-items (082, 2026-07-18).';
