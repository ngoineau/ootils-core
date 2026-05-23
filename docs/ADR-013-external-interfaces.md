# ADR-013 : Interfaces externes — formats fichiers, full reload, approval

## Statut
DRAFT — 2026-05-23

## Contexte

ADR-009 a posé l'architecture du pipeline d'import : staging zone immuable, DQ pipeline 4 niveaux (L1 structurel → L4 croisé), SCD2 sur master data, UOM conversions, calendriers opérationnels. Cette base est en production avec les niveaux L1 et L2 implémentés et testés.

Trois questions sont restées ouvertes parce qu'elles concernent le **contrat externe** (entre Ootils et les outils tiers : SAP IBP, Kinaxis, Oracle, exports Excel ad-hoc), pas l'architecture interne :

1. **Quels formats fichiers** accepte-t-on en entrée ? L'API actuelle ne supporte que JSON, ce qui élimine de facto 90 % des outils qui dumpent en CSV/TSV/XLSX.
2. **Semantique de refresh** : un import remplace-t-il l'état existant ou le complète-t-il ? ADR-009 reste muet ; en pratique l'upsert par `external_id` actuel implémente un comportement hybride flou.
3. **Workflow d'approbation** : l'humain-dans-la-boucle est-il obligatoire avant injection en core, ou peut-on auto-approuver les batches DQ-verts ?

Ce besoin émerge maintenant parce qu'on veut commencer à interfacer Ootils avec des outils réels (ERP du client, exports planificateurs, données historiques). Sans décisions tranchées sur ces trois points, chaque intégration réinventerait son propre contrat.

## Décisions

### D1 — Formats fichiers : TSV (principal) + CSV + XLSX + JSON

L'API accepte quatre formats en upload :

- **TSV (`.tab`, `.tsv`)** — séparateur `\t`. **Format recommandé pour les exports ERP** : les valeurs métier contiennent rarement des tabs, contrairement aux virgules/points-virgules/guillemets qui pourrissent les CSV. Encodage UTF-8 sans BOM.
- **CSV (`.csv`)** — séparateur `,` ou `;` (auto-détection via `csv.Sniffer` sur le header). Quoting `"` standard. UTF-8 sans BOM, fallback CP-1252 si décodage UTF-8 échoue (clients Windows).
- **XLSX (`.xlsx`)** — première sheet utilisée par défaut, override via header HTTP `X-Sheet-Name`. Cellules vides → NULL côté staging. `openpyxl` en read-only pour les gros fichiers.
- **JSON (`.json`)** — déjà supporté. Tableau d'objets, un objet = une ligne.

Tous les formats convergent vers le même schéma `ingest_rows` (col_01..col_15 en TEXT). La première ligne du fichier est obligatoire et porte les **noms canoniques de colonnes** définis par entité (voir D5). Le parser fait le mapping `header_name` → `col_N` côté staging.

Détection du format : par extension du fichier si extension présente, sinon par sniff du contenu (start byte `PK` → XLSX, `{` ou `[` → JSON, première ligne contient `\t` → TSV, sinon CSV).

### D2 — Schéma `staging` dédié dans la même DB

Création d'un schéma Postgres `staging` qui regroupe les tables existantes (`ingest_batches`, `ingest_rows`, `data_quality_issues`, `external_id_mapping`) + les nouvelles tables (`staging.uploads`, `staging.transform_runs`). Les tables canoniques (`items`, `nodes`, `edges`, etc.) restent dans `public`.

Bénéfices :
- Séparation visuelle claire dans DBeaver/psql (`\dn`)
- Permissions Postgres distinctes : les jobs d'import peuvent avoir `GRANT ALL ON SCHEMA staging` sans accès direct à `public`
- Backups partitionnables (staging peut avoir une retention courte, public longue)

Anti-décision : pas de DB séparée (`ootils_staging`) — ça forcerait des Foreign Data Wrappers pour les joins L2/L4 et romprait la simplicité des transactions atomiques sur l'approval.

La migration `033_staging_schema.sql` :
- Crée le schéma `staging`
- `ALTER TABLE ... SET SCHEMA staging` pour `ingest_batches`, `ingest_rows`, `data_quality_issues`, `external_id_mapping`
- Crée `staging.uploads` (1 row par fichier uploadé : filename, size, format, sha256, batch_id)
- Crée `staging.transform_runs` (1 row par exécution d'approval : batch_id, started_at, completed_at, rows_inserted, rows_updated, rows_deleted)

### D3 — Semantique : full reload par `(entity_type, source_system)`

Chaque batch approuvé **remplace intégralement** l'état canonique pour son couple `(entity_type, source_system)`. Concrètement, sur approval :

```
BEGIN;
  -- 1. Pour chaque external_id présent dans le batch : UPSERT dans la canonique
  --    Insert si nouveau, update si existant. Trace dans master_data_audit_log.
  --    Pour les entités SCD2 (item_planning_params) : ferme la version active
  --    + crée une nouvelle ligne (ne JAMAIS écraser une version historique).

  -- 2. Pour chaque external_id absent du batch mais présent en canonique
  --    avec le même source_system : marquer active=FALSE (soft delete).
  --    Garder l'historique pour audit + rollback.

  -- 3. Recalculer L4 (cross-batch) sur l'état résultant : items orphelins,
  --    planning_params sans item parent, etc.
COMMIT;
```

Pourquoi full reload et pas delta :
- Un export ERP est par nature un snapshot
- Le client n'a pas toujours un `modified_at` fiable côté source
- Les deletes implicites (item retiré du fichier) sont détectés naturellement
- Idempotent par construction : rejouer le même batch deux fois est un no-op

Multi-source : deux ERPs peuvent alimenter la même entité (ex: items master depuis SAP, lifecycle status depuis MES). Chacun maintient son périmètre via `source_system`. Un item peut avoir des champs alimentés par deux sources : chaque source possède sa colonne — résolu via D5 (colonnes par source).

### D4 — Approval obligatoire, pas d'auto-approve

Même quand DQ pipeline retourne zéro erreur et zéro warning, le batch reste en status `validated` et requiert une action humaine explicite : `POST /v1/staging/batches/{batch_id}/approve`.

Justifications :
- Un batch peut être DQ-vert et **structurellement délétère** (ex: scope drastique réduit involontairement → 80 % des items disparaissent par soft-delete D3)
- La traçabilité audit demande un identifiant humain sur chaque injection
- Évite les boucles d'auto-approbation entre systèmes

Le payload `/approve` accepte un commentaire libre (`notes`) et est tracé dans `staging.transform_runs.approved_by` + `approved_at` + `approval_notes`. Le `approved_by` est extrait du JWT/token de l'appelant (le mécanisme d'authentification reste hors scope de cet ADR).

Endpoint complémentaire : `POST /v1/staging/batches/{batch_id}/reject` avec raison obligatoire (`reason`). Marque le batch `rejected`, les ingest_rows restent stockées (rejeu possible après correction côté source).

Diff pre-approval : un endpoint `GET /v1/staging/batches/{batch_id}/diff` retourne le delta (`{will_insert: N, will_update: M, will_soft_delete: K}`) avec un sample de 10 lignes par catégorie. Permet à l'humain de visualiser l'impact avant d'approuver — c'est le garde-fou principal contre les imports destructeurs.

### D5 — Templates CSV/TSV par entité avec colonnes canoniques

Chaque entité a un schéma de fichier documenté dans `docs/staging-templates/<entity>.md`, avec les colonnes obligatoires, optionnelles, et le mapping vers les tables canoniques. Exemple pour `items` :

```
external_id     (obligatoire, UNIQUE par source_system)  -> items.external_id
name            (obligatoire, max 200 chars)             -> items.name
item_type       (obligatoire ∈ {finished_good, semi_finished, component, raw_material})
uom             (obligatoire, code UOM connu de uom_conversions)
status          (optionnel, défaut 'active')             -> items.status
description     (optionnel)                              -> items.description (à ajouter)
```

Le header de la première ligne du fichier doit matcher ces noms (case-insensitive, espaces tolérés). Colonnes inconnues → issue DQ L1 `UNKNOWN_COLUMN` (severity warning, pas bloquant). Colonnes obligatoires manquantes → erreur L1 bloquante.

Un fichier exemple `docs/staging-templates/items.tsv` est versionné dans le repo pour servir de référence.

### D6 — Lifecycle d'un batch (state machine)

```
                   upload                  L1-L2-L3-L4              approve
   external file ────────> pending ────────────────────> validated ─────────> imported
                              │                              │                    │
                              │  L1 fatal (encoding, format) │  reject             │
                              └──────────────> rejected      └─────> rejected      ▼
                                                                                public.*
                                                                              (canonical
                                                                              tables)
```

Transitions autorisées et seules ces transitions :
- `pending → validated` : DQ pipeline complete, aucune issue de niveau `error` (warnings tolérés)
- `pending → rejected` : DQ pipeline a déclenché un blocage (encoding invalide, format non reconnu, schema header invalide, etc.) OU `POST /reject` appelé
- `validated → imported` : `POST /approve` réussi (transaction commit)
- `validated → rejected` : `POST /reject` manuel
- `imported` : terminal, immutable

Aucun batch ne revient en arrière. Pour rejouer après correction côté ERP : nouvel upload, nouveau batch_id.

## Conséquences

### Positives
- **Contrat externe lisible** : un fournisseur de données sait exactement quel format pousser, sur quel endpoint, dans quel template
- **Idempotent par construction** : full reload + soft delete = rejouer N fois donne le même état final
- **Audit complet** : chaque ligne en `public` est traçable au batch → fichier → utilisateur qui a approuvé
- **Pas de surprise** : `/diff` montre l'impact avant `/approve`, l'humain garde le contrôle même quand DQ est vert
- **Multi-source clean** : SAP peut alimenter le master, MES le lifecycle, sans collision

### Négatives
- **Latence d'import** : l'approval manuel ajoute ~minutes-heures entre upload et disponibilité en planning. Acceptable pour master data (refresh hebdo/quotidien) ; sera douloureux pour les transactions temps réel (POs, customer orders) qui pourraient justifier un fast-path à étudier en v2.
- **Friction sur les hot-fixes** : corriger un seul item demande de re-pousser le fichier complet (D3 full reload). Mitigé par la possibilité de batches "patch" sur un `source_system` distinct.
- **Stockage staging** : conserver les `raw_content` indéfiniment fait croître `staging.*`. Politique de retention à définir hors ADR (suggestion : 90 jours, configurable).

### Risques
- **Soft-delete cascade** : un batch incomplet (export tronqué) pourrait soft-deleter 90 % des items. Mitigation : l'endpoint `/diff` doit refuser l'approval si le ratio `soft_delete / current_active > 20 %` sauf flag `--force` explicite + commentaire obligatoire.
- **Encoding sur fichiers ERP legacy** : CP-1252, Latin-1, etc. circulent encore. Le parser doit tenter UTF-8 d'abord, log un warning si fallback à CP-1252, refuser au-delà.

## Roadmap d'implémentation

| # | Livrable | Effort | Bloquant pour |
|---|----------|--------|---------------|
| 1 | Migration `033_staging_schema.sql` (création schéma + move tables) | 1 j | Tout le reste |
| 2 | `staging/parser.py` (TSV/CSV/XLSX/JSON unifié, sniffing, encoding) | 2-3 j | Tests de bout en bout |
| 3 | `staging/loader.py` (upload → ingest_batches + ingest_rows) | 1 j | Endpoint upload |
| 4 | Endpoint `POST /v1/staging/upload` (multipart + entity_type) | 1 j | Première intégration externe possible |
| 5 | DQ L3 (règles métier SC, ~15 règles initiales) | 3-4 j | Approval significatif |
| 6 | DQ L4 (cross-batch dedup + orphan check) | 2 j | Multi-source clean |
| 7 | Endpoint `GET /diff` (preview impact pre-approval) | 1-2 j | Approval safe |
| 8 | Endpoint `POST /approve` avec semantics full-reload + soft delete | 2-3 j | Le cœur du flux |
| 9 | Endpoint `POST /reject` + audit | 0.5 j | Operations |
| 10 | Templates `docs/staging-templates/*.md` + `*.tsv` exemples | 2 j | Onboarding intégration externe |
| 11 | Tests integration end-to-end (upload TSV → DQ → diff → approve) | 2 j | Production-ready |
| 12 | Documentation utilisateur (README staging, exemples curl) | 1 j | Self-service |

**Total** : ~3-4 semaines focus.

## Références

- ADR-009 : Architecture Pipeline d'Import (le socle staging+DQ que cet ADR complète)
- ADR-005 : Storage Layer (conventions schemas Postgres)
- `src/ootils_core/api/routers/ingest.py` (l'implémentation actuelle JSON-only à étendre)
- `src/ootils_core/engine/dq/` (le DQ engine L1+L2 actuel)
- Migration `007_import_pipeline.sql` (le schéma initial)
