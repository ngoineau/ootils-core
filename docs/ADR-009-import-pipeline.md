# ADR-009 : Architecture Pipeline d'Import — Staging + DQ + Core

## Statut
ACCEPTED — 2026-04-05

## Contexte

Les outils APS de référence (Kinaxis, SAP IBP, Blue Yonder) partagent tous le même problème structurel : ils traitent les données sources immédiatement à la réception, sans zone tampon. Conséquences directes en production :

- **Données perdues** : si un import échoue après transformation, les données originales sont inaccessibles.
- **Historique absent** : un `lead_time_days` écrasé par upsert ne laisse aucune trace. Impossible d'auditer une décision de planification prise 3 semaines plus tôt.
- **Dégradation silencieuse** : un item passe de 14 à 0 jours de lead time sans alerte. Le moteur génère des alertes de rupture décalées. Les planificateurs commandent en urgence.
- **Cross-entité non vérifiée** : un PO arrive en BOX alors que le stock est en EA. Sans table de conversion UOM, le moteur interprète 10 BOX comme 10 EA — erreur de facteur 24.

Le contexte Ootils amplifie ces risques : le moteur de planification SC (graphe, projections, allocations) consomme directement les données master. Une donnée corrompue en entrée génère des projections aberrantes sans signal d'alerte.

La review architecturale (`REVIEW-IMPORT-ARCHITECTURE.md`) et la review SC expert (`REVIEW-IMPORT-SC-EXPERT.md`) convergent sur une architecture 2 étapes obligatoire : **Staging → Core**, avec un pipeline DQ intercalé.

## Décisions

### D1 — Staging zone : tout accepter sauf encoding invalide

Toutes les données brutes ERP/WMS arrivent en staging sans transformation. Même les lignes malformées sont stockées avec flag. Principe : **ne jamais perdre une donnée source**.

La table `ingest_batches` enregistre chaque batch dès sa réception (HTTP 202 immédiat). La table `ingest_rows` stocke le `raw_content` intégral + les colonnes extraites en TEXT sans conversion. Un batch est toujours créé, même si le payload est partiellement illisible — l'erreur de parsing devient une issue DQ de type `parse_error`, pas un rejet HTTP.

Conséquence directe : rejouer un import depuis le staging est toujours possible. Aucun "qu'est-ce qui nous a été envoyé ?" n'est sans réponse.

### D2 — DQ Pipeline en 4 niveaux séquentiels

```
L1 Structurel → L2 Référentiel → L3 Métier SC → L4 Croisé
```

- **L1 Structurel** : types de colonnes, valeurs obligatoires, formats (date, numérique, UUID).
- **L2 Référentiel** : `item_id` existe dans `items`, `location_id` dans `locations`, etc.
- **L3 Métier SC** : lead time à 0 → WARNING, `max_order_qty < min_order_qty` → ERROR, UOM non convertible → ERROR.
- **L4 Croisé** : items actifs sans `item_planning_params` pour leur location, `supplier_items` sans fournisseur actif, doublons inter-batch.

Une ligne peut passer L1 et échouer L3 — statut partiel possible (`dq_level_reached` enregistre le niveau atteint). Les issues sont tracées dans `data_quality_issues` avec `severity` (error/warning/info), `rule_code`, `field_name`, `raw_value`.

Un batch avec au moins une issue de sévérité `error` ne peut pas être approuvé sans intervention humaine explicite. Les warnings n'bloquent pas l'approbation mais sont visibles.

### D3 — Données techniques (item_planning_params) versionnées SCD2

La table `item_planning_params` est versionnée temporellement via `effective_from` / `effective_to`. Une ligne active par item × location à tout moment. L'historique complet est conservé.

Règle de lecture : le moteur charge la version dont `effective_from <= planning_date < effective_to` (ou `effective_to IS NULL` pour la version courante). Une contrainte d'exclusion GIST garantit l'absence de chevauchement à la base de données.

Ne jamais écraser les paramètres en place : à l'import, fermer la version active (`effective_to = today`) et créer une nouvelle ligne. L'`import_audit_log` / `master_data_audit_log` trace chaque changement avec source et batch d'origine.

### D4 — Pas de fast-import qui bypasse le staging

Décision ferme : **même en mode urgent, toute donnée passe par staging**. Pas d'exception.

Un "fast-import" qui écrit directement en core tables contourne le DQ pipeline, perd l'historique source, et crée une brèche d'audit. Le délai introduit par le staging (ingest_rows → DQ → approve) est de quelques secondes pour des batches normaux. Ce coût est non-négociable.

Si la vitesse est critique, optimiser le pipeline DQ (parallélisation des règles L1/L2) — pas bypasser le staging.

### D5 — UOM conversions globales et item-specific

Table `uom_conversions` avec résolution hiérarchique : **item-specific > global**.

Le moteur cherche d'abord une conversion `(from_uom, to_uom, item_id=<id>)`. Si absente, il cherche `(from_uom, to_uom, item_id=NULL)`. Si toujours absente : erreur DQ `UOM_CONVERSION_MISSING` sur toute ligne utilisant ces UOM pour cet item.

Le `factor` est toujours le multiplicateur de `from_uom` vers `to_uom` : `1 from_uom = factor × to_uom`. La conversion inverse est `1/factor`. Le stockage du sens canonique est documenté dans chaque insert.

### D6 — Calendriers opérationnels par location

1 row par location × date. **Absence = jour ouvré par défaut** (convention safe-by-default).

Ne pas stocker des "patterns de calendrier" (ex: "fermé tous les dimanches") — les cas limites (ponts, jours fériés décalés) génèrent des bugs non reproductibles. L'import charge une plage de dates explicite. Le moteur fait un lookup direct sur `(location_id, calendar_date)`. Absence → `is_working_day=TRUE, capacity_factor=1.0`.

## Conséquences

### Positives
- **Auditabilité complète** : chaque donnée en production est traçable jusqu'à son batch d'origine, sa ligne source, et l'utilisateur qui a approuvé l'import.
- **Rejeu possible** : un import raté peut être re-soumis depuis le staging sans re-télécharger le fichier source.
- **Dégradation détectable** : les issues DQ L4 (croisé) détectent activement les régressions de qualité entre imports.
- **Moteur protégé** : le core engine ne consomme que des données ayant passé le DQ pipeline. Garbage-in/garbage-out est éliminé par construction.
- **Historique master data** : les décisions de planification passées sont reconstituables (lead times, politiques de sécurité stock, paramètres fournisseurs).

### Négatives / Points de vigilance
- **Latence d'import** : le pipeline 2 étapes introduit une latence entre la réception et la disponibilité en core. Les planificateurs ne voient pas les données "instantanément". Mitigation : afficher le statut du batch en temps réel, notifier à la fin du DQ.
- **Volume staging** : `ingest_rows` stocker le `raw_content` de chaque ligne grossit rapidement. Prévoir une politique de rétention (ex: purge des batches `imported` datant de > 90 jours).
- **Complexité DQ** : 4 niveaux de règles à maintenir par entity_type. Risque de règles L3/L4 trop strictes bloquant des imports valides. Mitigation : démarrer conservateur (peu de règles L3, aucune L4), ajouter progressivement basé sur les incidents observés.
- **Extension btree_gist requise** : la contrainte d'exclusion GIST sur `item_planning_params` requiert `CREATE EXTENSION IF NOT EXISTS btree_gist`. À vérifier sur l'environnement de production.
- **BOM et routings** : hors scope de ce sprint. Sans BOM, le manufacturing planning reste limité. Prévoir ADR-010.

## Tables créées par cette migration (007)

| Table | Rôle |
|-------|------|
| `ingest_batches` | Registre des batches d'import (staging) |
| `ingest_rows` | Lignes brutes avec statut DQ |
| `data_quality_issues` | Issues détectées par le pipeline DQ |
| `external_references` | Mapping ERP codes → UUID internes |
| `suppliers` | Référentiel fournisseurs |
| `supplier_items` | Conditions d'approvisionnement par (fournisseur × item) |
| `item_planning_params` | Paramètres de planification versionnés SCD2 |
| `uom_conversions` | Conversions d'unités avec priorité item-specific |
| `operational_calendars` | Calendriers opérationnels par site |
| `master_data_audit_log` | Log d'audit des modifications master data |

## Références
- `docs/REVIEW-IMPORT-ARCHITECTURE.md` — Architecture review détaillée
- `docs/REVIEW-IMPORT-SC-EXPERT.md` — Review expert SC (Kinaxis/o9/IBP)
- `src/ootils_core/db/migrations/007_import_pipeline.sql` — DDL complet
