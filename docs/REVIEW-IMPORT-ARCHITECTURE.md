# Review Architecturale — Pipeline d'Import Ootils

> **Auteur :** Architecture Review (Senior)  
> **Date :** 2026-04-05  
> **Branche :** `review/import-architecture`  
> **Scope :** Pipeline d'import 2 étapes, données techniques, migration 004-import  
> **Statut :** RÉFÉRENCE — opinions fermes, DDL complet

---

## Table des matières

1. [Architecture globale du pipeline d'import](#1-architecture-globale)
2. [Modèle de données techniques](#2-modèle-de-données-techniques)
3. [Séparation des responsabilités Python](#3-séparation-des-responsabilités)
4. [API design — pipeline 2 étapes](#4-api-design)
5. [Migration prioritaire (004-import)](#5-migration-prioritaire)
6. [Anti-patterns à éviter absolument](#6-anti-patterns)
7. [Décisions architecturales clés](#7-décisions-architecturales)

---

## 1. Architecture globale

### Schéma d'architecture (ASCII)

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              SYSTÈMES SOURCES                                           │
│   ERP (SAP/Dynamics)     WMS/EDI     Excel/TSV     API tierces     Saisie manuelle      │
└───────────────────────────────────────┬─────────────────────────────────────────────────┘
                                        │  multipart/form-data (TSV) | application/json
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                         COUCHE INGESTION  (ootils_core/ingestion/)                      │
│                                                                                         │
│   POST /v1/ingest/{type}                                                                │
│   ┌─────────────────────────────────────────────────────────────────────────────┐       │
│   │  IngestRouter                                                               │       │
│   │  - Accepte TOUT (pas de validation métier ici)                              │       │
│   │  - Parse : TSV → rows[] | JSON → rows[]                                    │       │
│   │  - Génère batch_id (UUID)                                                   │       │
│   │  - Écrit atomiquement dans ingest_batches + ingest_rows (staging)           │       │
│   │  - Répond HTTP 202 immédiatement avec batch_id                              │       │
│   └─────────────────────────────────────────────────────────────────────────────┘       │
│                                                                                         │
│   Tables PostgreSQL : ingest_batches, ingest_rows                                       │
│   Garantie : tout ce qui arrive est stocké — jamais perdu, jamais ignoré                │
└───────────────────────────────────────┬─────────────────────────────────────────────────┘
                                        │  trigger async (background task ou queue)
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                      COUCHE DATA QUALITY  (ootils_core/dq/)                             │
│                                                                                         │
│   DQPipeline.run(batch_id)                                                              │
│   ┌─────────────────────────────────────────────────────────────────────────────┐       │
│   │  EntityRuleRegistry                                                         │       │
│   │  - Charge les règles par entity_type                                        │       │
│   │  - Exécute : structural → referential → business → cross-row               │       │
│   │  - Écrit les issues dans dq_issues                                          │       │
│   │  - Met à jour ingest_batches.status :                                       │       │
│   │      pending → processing → validated | rejected                            │       │
│   └─────────────────────────────────────────────────────────────────────────────┘       │
│                                                                                         │
│   Tables PostgreSQL : dq_issues                                                         │
│   Garantie : chaque issue est localisée (batch_id, row_index, field)                    │
│   Garantie : batch rejeté = zéro écriture en core                                       │
└───────────────────────────────────────┬─────────────────────────────────────────────────┘
                                        │
                       ┌────────────────┴───────────────────┐
                       │                                    │
                  VALIDATED                            REJECTED
                       │                                    │
                       ▼                                    ▼
        POST /v1/ingest/{batch_id}/approve        GET /v1/ingest/{batch_id}/issues
        POST /v1/ingest/{batch_id}/fix            → client corrige et recrée un batch
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                     COUCHE IMPORT  (ootils_core/import_pipeline/)                       │
│                                                                                         │
│   ImportService.approve(batch_id)                                                       │
│   ┌─────────────────────────────────────────────────────────────────────────────┐       │
│   │  - Lit ingest_rows WHERE batch_id = ? AND dq_status = 'valid'              │       │
│   │  - Résout external_id → UUID interne (external_references)                  │       │
│   │  - Upsert en core tables (transactionnel)                                   │       │
│   │  - Écrit import_audit_log                                                   │       │
│   │  - Émet event 'ingestion_complete' dans events                              │       │
│   │  - Met à jour ingest_batches.status → imported                              │       │
│   └─────────────────────────────────────────────────────────────────────────────┘       │
│                                                                                         │
│   Tables PostgreSQL core : items, locations, suppliers, supplier_items,                 │
│                            item_planning_params, uom_conversions,                       │
│                            operational_calendars, external_references                   │
│   Garantie : transaction unique — tout ou rien                                          │
└───────────────────────────────────────┬─────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                            CORE ENGINE  (ootils_core/engine/)                           │
│                                                                                         │
│   GraphPropagator  →  ProjectionCalc  →  ShortageDetector  →  AllocationEngine         │
│                                                                                         │
│   Tables : scenarios, nodes, edges, projection_series,                                  │
│            calc_runs, shortages, events                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Flux de données et leurs garanties

| Étape | Entrée | Sortie | Garantie |
|-------|--------|--------|---------|
| Ingestion | Payload brut | `ingest_rows` | Rien n'est perdu. Le batch est toujours créé, même si le fichier est illisible (error = issue de type `parse_error`). |
| DQ | `ingest_rows` | `dq_issues` + statut batch | Aucune donnée invalide n'atteint le core. Le pipeline DQ s'exécute de manière idempotente. |
| Approbation | `ingest_rows` validées | Core tables | Transaction atomique. L'import est tout-ou-rien. `import_audit_log` trace chaque upsert. |
| Engine | Core tables | Nodes, projections | Le moteur lit des données qualifiées. Garbage-in/garbage-out est impossible par construction. |

---

## 2. Modèle de données techniques

Les données techniques sont **le cœur manquant** du schéma actuel. Sans elles, le moteur de planification tourne avec des hypothèses par défaut hardcodées — c'est inacceptable en production.

### 2.1 Table `item_planning_params`

```sql
-- ============================================================
-- item_planning_params : Paramètres de planification par item × location
-- Versioning temporel : effective_from / effective_to
-- Source tracking : qui a fourni ce paramètre
-- ============================================================

CREATE TYPE lot_size_rule_enum AS ENUM ('LOTFORLOT', 'FIXED_QTY', 'PERIOD_OF_SUPPLY');
CREATE TYPE planning_param_source_enum AS ENUM ('erp', 'manual', 'ai_suggested');

CREATE TABLE IF NOT EXISTS item_planning_params (
    param_id                    UUID            NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Dimension : item × location (obligatoire)
    item_id                     UUID            NOT NULL REFERENCES items(item_id),
    location_id                 UUID            NOT NULL REFERENCES locations(location_id),

    -- ── Lead times (en jours, NUMERIC pour fractions acceptées) ──
    lead_time_sourcing_days     NUMERIC         CHECK (lead_time_sourcing_days >= 0),
    lead_time_manufacturing_days NUMERIC        CHECK (lead_time_manufacturing_days >= 0),
    lead_time_transit_days      NUMERIC         NOT NULL DEFAULT 0
                                CHECK (lead_time_transit_days >= 0),
    -- Lead time total calculé = sourcing + manufacturing + transit
    -- Ne pas stocker le total calculé : il se calcule à la lecture

    -- ── Stocks de sécurité ──
    safety_stock_qty            NUMERIC         NOT NULL DEFAULT 0
                                CHECK (safety_stock_qty >= 0),
    safety_stock_days_of_coverage NUMERIC       CHECK (safety_stock_days_of_coverage >= 0),
    -- Règle : si les deux sont fournis, safety_stock_qty est prioritaire.
    -- safety_stock_days_of_coverage sert au calcul dynamique par le moteur.

    -- ── Point de réapprovisionnement ──
    reorder_point_qty           NUMERIC         CHECK (reorder_point_qty >= 0),
    reorder_point_days          NUMERIC         CHECK (reorder_point_days >= 0),

    -- ── Quantités de commande ──
    min_order_qty               NUMERIC         NOT NULL DEFAULT 1
                                CHECK (min_order_qty > 0),
    max_order_qty               NUMERIC         CHECK (max_order_qty IS NULL OR max_order_qty >= min_order_qty),
    order_multiple              NUMERIC         NOT NULL DEFAULT 1
                                CHECK (order_multiple > 0),

    -- ── Politique de lotissement ──
    lot_size_rule               lot_size_rule_enum NOT NULL DEFAULT 'LOTFORLOT',
    -- Pour FIXED_QTY : le moteur utilise min_order_qty comme taille fixe de lot
    -- Pour PERIOD_OF_SUPPLY : planning_horizon_days définit la période couverte par lot

    -- ── Horizon de planification ──
    planning_horizon_days       INTEGER         NOT NULL DEFAULT 90
                                CHECK (planning_horizon_days > 0),

    -- ── Make vs Buy ──
    is_make                     BOOLEAN         NOT NULL DEFAULT FALSE,
    -- is_make = TRUE  → WorkOrderSupply (fabrication interne)
    -- is_make = FALSE → PurchaseOrderSupply (achat externe)

    -- ── Fournisseur préféré ──
    preferred_supplier_id       UUID            REFERENCES suppliers(supplier_id),
    -- NULL si is_make = TRUE ou si non défini

    -- ── Versioning temporel ──
    effective_from              DATE            NOT NULL DEFAULT CURRENT_DATE,
    effective_to                DATE,           -- NULL = toujours valide
    CHECK (effective_to IS NULL OR effective_to > effective_from),

    -- ── Traçabilité source ──
    source                      planning_param_source_enum NOT NULL DEFAULT 'manual',
    source_batch_id             UUID,           -- Référence ingest_batches si source = erp
    created_by                  TEXT,           -- user_ref ou system_ref
    notes                       TEXT,

    -- ── Audit ──
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT now(),

    -- Contrainte : une seule config active par (item, location) à une date donnée.
    -- Le moteur charge la version dont effective_from <= planning_date < effective_to.
    -- Pas de UNIQUE simple : le versioning temporel le gère.
    -- Index composite couvre l'unicité de la version courante.
    UNIQUE NULLS NOT DISTINCT (item_id, location_id, effective_from)
);

-- Accès principal : paramètres actifs pour un item × location à une date
CREATE INDEX IF NOT EXISTS idx_ipp_item_location_active
    ON item_planning_params (item_id, location_id, effective_from, effective_to);

-- Accès par source (audit des paramètres ERP vs manual vs AI)
CREATE INDEX IF NOT EXISTS idx_ipp_source
    ON item_planning_params (source, updated_at DESC);

-- Lookup fournisseur préféré (pour résolution à l'import)
CREATE INDEX IF NOT EXISTS idx_ipp_preferred_supplier
    ON item_planning_params (preferred_supplier_id)
    WHERE preferred_supplier_id IS NOT NULL;
```

**Décision ferme sur le versioning temporel :**
L'approche `effective_from` / `effective_to` est **obligatoire** dès le départ. Ne pas la mettre maintenant, c'est garantir une migration douloureuse en production quand un client veut planifier selon les paramètres "d'avant la renégociation tarifaire". La contrainte `UNIQUE NULLS NOT DISTINCT (item_id, location_id, effective_from)` empêche deux versions démarrant le même jour — le cas de bord est géré à l'application (la nouvelle version ferme la précédente).

### 2.2 Table `uom_conversions`

```sql
-- ============================================================
-- uom_conversions : Conversions d'unités de mesure
-- item_id NULL = conversion globale (EA → PAL pour tous les articles)
-- item_id non-NULL = conversion spécifique à un article
-- ============================================================

CREATE TABLE IF NOT EXISTS uom_conversions (
    conversion_id   UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),

    from_uom        TEXT        NOT NULL,   -- ex: EA, KG, L, M
    to_uom          TEXT        NOT NULL,   -- ex: PAL, BOX, TON

    -- NULL = conversion universelle applicable à tout article dans ces UOM
    -- Non-NULL = override pour cet article spécifique
    item_id         UUID        REFERENCES items(item_id),

    -- factor : 1 from_uom = factor to_uom
    -- ex: 1 PAL = 48 EA → factor = 48 (from=PAL, to=EA)
    -- La conversion inverse se calcule : 1/factor
    factor          NUMERIC     NOT NULL CHECK (factor > 0),

    -- Validité temporelle (optionnel, pour coûts/capacités qui changent)
    effective_from  DATE        NOT NULL DEFAULT CURRENT_DATE,
    effective_to    DATE,
    CHECK (effective_to IS NULL OR effective_to > effective_from),

    -- Audit
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Unicité : une seule conversion (from_uom, to_uom) par article à une date
    UNIQUE NULLS NOT DISTINCT (from_uom, to_uom, item_id, effective_from)
);

-- Lookup par UOM (moteur cherche souvent : "comment convertir EA en PAL ?")
CREATE INDEX IF NOT EXISTS idx_uom_conv_pair
    ON uom_conversions (from_uom, to_uom, item_id);

-- Lookup article-specific conversions
CREATE INDEX IF NOT EXISTS idx_uom_conv_item
    ON uom_conversions (item_id)
    WHERE item_id IS NOT NULL;
```

**Décision ferme sur la résolution :**
À la lecture, le moteur applique la priorité suivante : conversion item-specific > conversion globale. Ne jamais retourner une erreur "UOM inconnu" sans avoir cherché dans les deux niveaux. Le `factor` est toujours stocké dans un sens canonique (le plus grand vers le plus petit en général, ou l'ordre alphabétique) — la direction est documentée dans une table séparée `uom_registry` (hors scope de cette migration, mais prévoir le slot).

### 2.3 Table `operational_calendars`

```sql
-- ============================================================
-- operational_calendars : Calendriers opérationnels par site
-- Grain : un enregistrement par (location, date)
-- ============================================================

CREATE TABLE IF NOT EXISTS operational_calendars (
    calendar_id     UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),

    location_id     UUID        NOT NULL REFERENCES locations(location_id),
    date            DATE        NOT NULL,

    -- Jour ouvrable ?
    is_working_day  BOOLEAN     NOT NULL DEFAULT TRUE,

    -- Nombre de shifts actifs ce jour (0 si non-working, 1-3 typiquement)
    shift_count     SMALLINT    NOT NULL DEFAULT 1
                    CHECK (shift_count BETWEEN 0 AND 3),

    -- Facteur de capacité (1.0 = pleine capacité, 0.5 = demi-journée, 0.0 = fermé)
    capacity_factor NUMERIC     NOT NULL DEFAULT 1.0
                    CHECK (capacity_factor BETWEEN 0.0 AND 1.0),

    -- Raison (férié national, maintenance planifiée, etc.)
    calendar_type   TEXT        CHECK (calendar_type IN (
                        'standard', 'public_holiday', 'plant_shutdown',
                        'reduced_capacity', 'extended_capacity'
                    )),
    notes           TEXT,

    -- Audit
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Un seul enregistrement par (location, date)
    UNIQUE (location_id, date)
);

-- Accès principal : plage de dates pour un site
CREATE INDEX IF NOT EXISTS idx_opcal_location_date
    ON operational_calendars (location_id, date);

-- Accès des jours non ouvrables (fréquent pour calcul lead time)
CREATE INDEX IF NOT EXISTS idx_opcal_non_working
    ON operational_calendars (location_id, date)
    WHERE is_working_day = FALSE;
```

**Décision ferme sur la granularité :**
Un enregistrement par (location, date) est la seule approche viable. Ne pas stocker des "patterns de calendrier" (style "toujours fermé le dimanche") — ça paraît élégant mais génère des bugs dans les cas limites (ponts, jours fériés tombant un lundi). L'import charge une plage explicite. Le moteur fait un lookup direct. Si la date n'existe pas dans le calendrier, il suppose `is_working_day=TRUE, capacity_factor=1.0` — comportement safe-by-default documenté.

---

## 3. Séparation des responsabilités

### Structure des modules Python

```
src/ootils_core/
│
├── ingestion/                    # Couche 1 : réception brute
│   ├── __init__.py
│   ├── router.py                 # FastAPI router : POST /v1/ingest/{type}
│   ├── batch_writer.py           # Écrit ingest_batches + ingest_rows
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── tsv_parser.py         # TSV → list[dict]
│   │   └── json_parser.py        # JSON payload → list[dict]
│   └── models.py                 # Pydantic : IngestRequest, BatchResponse
│
├── dq/                           # Couche 2 : Data Quality
│   ├── __init__.py
│   ├── pipeline.py               # DQPipeline.run(batch_id) — point d'entrée
│   ├── registry.py               # EntityRuleRegistry — charge les règles par type
│   ├── issue_writer.py           # Écrit dq_issues
│   ├── rules/
│   │   ├── __init__.py
│   │   ├── base.py               # Classe abstraite DQRule
│   │   ├── items.py              # Règles DQ pour entity_type='items'
│   │   ├── locations.py
│   │   ├── suppliers.py
│   │   ├── supplier_items.py
│   │   ├── item_planning_params.py
│   │   ├── uom_conversions.py
│   │   └── operational_calendars.py
│   └── models.py                 # Pydantic : DQIssue, DQResult
│
├── import_pipeline/              # Couche 3 : import validé → core
│   ├── __init__.py
│   ├── service.py                # ImportService.approve(batch_id)
│   ├── resolvers.py              # external_id → UUID via external_references
│   ├── upsert/
│   │   ├── __init__.py
│   │   ├── items.py
│   │   ├── locations.py
│   │   ├── suppliers.py
│   │   ├── supplier_items.py
│   │   ├── item_planning_params.py
│   │   ├── uom_conversions.py
│   │   └── operational_calendars.py
│   └── audit_writer.py           # Écrit import_audit_log
│
└── api/
    └── routers/
        └── ingest.py             # Regroupe les endpoints /v1/ingest/*
```

### Interfaces entre couches

**Règle absolue : aucune couche n'importe depuis une couche "plus bas".**

```
ingestion/ → ne connaît pas dq/ ni import_pipeline/
dq/        → ne connaît pas import_pipeline/
import_pipeline/ → ne connaît pas ingestion/ ni dq/
```

Le couplage se fait uniquement via la **base de données** (tables partagées) et via des **interfaces de service explicites** :

```python
# ingestion/batch_writer.py
class BatchWriter:
    async def write_batch(self, entity_type: str, rows: list[dict]) -> UUID:
        """Écrit en staging. Retourne batch_id. Ne valide rien."""
        ...

# dq/pipeline.py
class DQPipeline:
    async def run(self, batch_id: UUID) -> DQResult:
        """Lit staging, écrit issues, met à jour statut batch."""
        ...

# import_pipeline/service.py
class ImportService:
    async def approve(self, batch_id: UUID) -> ImportResult:
        """Lit staging validé, upsert en core, écrit audit. Transaction unique."""
        ...
```

**Déclenchement asynchrone :**
L'ingestion déclenche la DQ via un `BackgroundTask` FastAPI (suffisant pour MVP). Quand le volume monte, remplacer par une queue légère (PostgreSQL LISTEN/NOTIFY ou Redis Queue) sans changer l'interface DQPipeline.

```python
# ingestion/router.py
@router.post("/v1/ingest/{entity_type}")
async def ingest(entity_type: str, ..., background_tasks: BackgroundTasks):
    batch_id = await batch_writer.write_batch(entity_type, rows)
    background_tasks.add_task(dq_pipeline.run, batch_id)  # ← couplage minimal
    return BatchResponse(batch_id=batch_id, status="pending")
```

---

## 4. API design — Pipeline 2 étapes

### Endpoints

```
POST /v1/ingest/{entity_type}
  → Staging immédiat, réponse HTTP 202 avec batch_id
  → entity_type : items | locations | suppliers | supplier_items |
                  item_planning_params | uom_conversions | operational_calendars

GET  /v1/ingest/{batch_id}
  → Statut complet du batch

GET  /v1/ingest/{batch_id}/issues
  → Liste paginée des problèmes DQ

POST /v1/ingest/{batch_id}/fix
  → Correction d'une ligne avant approbation

POST /v1/ingest/{batch_id}/approve
  → Import vers core (nécessite status=validated)
```

### Schéma de réponses

#### `POST /v1/ingest/{entity_type}` — Réponse 202

```json
{
  "batch_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "entity_type": "items",
  "status": "pending",
  "row_count": 142,
  "received_at": "2026-04-05T10:00:00Z",
  "_links": {
    "status": "/v1/ingest/a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "issues": "/v1/ingest/a1b2c3d4-e5f6-7890-abcd-ef1234567890/issues",
    "approve": "/v1/ingest/a1b2c3d4-e5f6-7890-abcd-ef1234567890/approve"
  }
}
```

#### `GET /v1/ingest/{batch_id}` — Statut complet

```json
{
  "batch_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "entity_type": "items",
  "status": "validated",
  "row_count": 142,
  "dq_summary": {
    "valid_rows": 139,
    "invalid_rows": 3,
    "warnings": 7,
    "error_types": ["missing_required_field", "invalid_enum_value"]
  },
  "received_at": "2026-04-05T10:00:00Z",
  "dq_completed_at": "2026-04-05T10:00:02Z",
  "imported_at": null
}
```

#### `GET /v1/ingest/{batch_id}/issues` — Problèmes DQ

```json
{
  "batch_id": "a1b2c3d4-...",
  "total_issues": 3,
  "issues": [
    {
      "issue_id": "b2c3d4e5-...",
      "row_index": 7,
      "external_id": "SKU-BAD-001",
      "field": "item_type",
      "severity": "error",
      "rule": "enum_validation",
      "message": "Valeur 'spare_part' invalide. Valeurs autorisées : finished_good, component, raw_material, semi_finished",
      "raw_value": "spare_part",
      "suggestion": "component"
    },
    {
      "issue_id": "c3d4e5f6-...",
      "row_index": 23,
      "external_id": null,
      "field": "external_id",
      "severity": "error",
      "rule": "required_field",
      "message": "Le champ 'external_id' est obligatoire et ne peut pas être vide",
      "raw_value": "",
      "suggestion": null
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 50,
    "total_pages": 1
  }
}
```

#### `POST /v1/ingest/{batch_id}/fix` — Correction

```json
// Request
{
  "fixes": [
    {
      "row_index": 7,
      "field": "item_type",
      "corrected_value": "component"
    }
  ]
}

// Response
{
  "batch_id": "a1b2c3d4-...",
  "fixes_applied": 1,
  "status": "pending",
  "message": "Correction appliquée. La validation DQ est relancée automatiquement."
}
```

**Décision ferme sur le `/fix` :**
Une correction relance automatiquement le pipeline DQ complet sur le batch (pas seulement la ligne corrigée). C'est plus coûteux mais c'est la seule approche correcte — une correction peut résoudre une erreur de référence croisée sur d'autres lignes.

#### `POST /v1/ingest/{batch_id}/approve` — Import

```json
// Request (vide, le batch_id suffit)
{}

// Response 200 — succès
{
  "batch_id": "a1b2c3d4-...",
  "entity_type": "items",
  "status": "imported",
  "import_summary": {
    "inserted": 120,
    "updated": 19,
    "skipped": 0
  },
  "event_id": "d4e5f6g7-...",
  "imported_at": "2026-04-05T10:05:33Z"
}

// Response 409 — batch non validé
{
  "error": "batch_not_validated",
  "message": "Ce batch a le statut 'rejected'. Corrigez les issues avant d'approuver.",
  "batch_id": "a1b2c3d4-...",
  "status": "rejected",
  "open_issues": 3
}
```

### Machine d'états du batch

```
         ┌──────────────────────────────────────────────────────┐
         │                                                      │
         ▼                                                      │
     [pending]                                                  │
         │                                                      │
         │  DQPipeline.run() déclenché                          │
         ▼                                                      │
    [processing]                                                │
         │                                                      │
    ┌────┴────┐                                                  │
    │         │                                                  │
    ▼         ▼                                                  │
[validated] [rejected] ←─── POST /fix ──────────────────────────┘
    │                    (relance DQ → retour pending)
    │  POST /approve
    ▼
 [importing]
    │
    ├── succès ──→ [imported]
    └── erreur ──→ [import_failed]  (rollback garanti)
```

---

## 5. Migration prioritaire (004-import)

La migration `004_import_pipeline.sql` est la **priorité absolue** avant toute implémentation des endpoints d'import.

```sql
-- ============================================================
-- Ootils Core — Migration 004: Import Pipeline & Technical Data
-- 
-- Crée :
--   1. external_references   (mapping codes ERP → UUIDs internes)
--   2. ingest_batches        (table staging metadata)
--   3. ingest_rows           (table staging données brutes)
--   4. dq_issues             (problèmes Data Quality)
--   5. import_audit_log      (traçabilité des upserts en core)
--   6. suppliers             (master data fournisseurs)
--   7. supplier_items        (relation item × fournisseur)
--   8. item_planning_params  (données techniques planification)
--   9. uom_conversions       (conversions unités de mesure)
--  10. operational_calendars (calendriers opérationnels)
--  11. external_id sur items et locations
-- ============================================================

-- ─────────────────────────────────────────────────────────────
-- TYPES ENUM (idempotents)
-- ─────────────────────────────────────────────────────────────

DO $$ BEGIN
    CREATE TYPE lot_size_rule_enum AS ENUM ('LOTFORLOT', 'FIXED_QTY', 'PERIOD_OF_SUPPLY');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE planning_param_source_enum AS ENUM ('erp', 'manual', 'ai_suggested');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ─────────────────────────────────────────────────────────────
-- 1. Ajouter external_id sur items et locations
-- ─────────────────────────────────────────────────────────────

ALTER TABLE items
    ADD COLUMN IF NOT EXISTS external_id TEXT;

-- Backfill : les items existants reçoivent leur item_id en external_id temporaire
UPDATE items
    SET external_id = item_id::TEXT
    WHERE external_id IS NULL;

ALTER TABLE items
    ALTER COLUMN external_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_items_external_id
    ON items (external_id);


ALTER TABLE locations
    ADD COLUMN IF NOT EXISTS external_id TEXT;

UPDATE locations
    SET external_id = location_id::TEXT
    WHERE external_id IS NULL;

ALTER TABLE locations
    ALTER COLUMN external_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_locations_external_id
    ON locations (external_id);

-- ─────────────────────────────────────────────────────────────
-- 2. external_references : mapping codes ERP → UUIDs Ootils
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS external_references (
    ref_id          UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type     TEXT        NOT NULL
                    CHECK (entity_type IN (
                        'item', 'location', 'supplier', 'supplier_item',
                        'item_planning_params'
                    )),
    external_id     TEXT        NOT NULL,
    source_system   TEXT        NOT NULL DEFAULT 'default',  -- 'sap_ecc', 'dynamics', 'manual', etc.
    internal_id     UUID        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (entity_type, external_id, source_system)
);

CREATE INDEX IF NOT EXISTS idx_extref_lookup
    ON external_references (entity_type, external_id, source_system);

CREATE INDEX IF NOT EXISTS idx_extref_internal
    ON external_references (internal_id, entity_type);

-- ─────────────────────────────────────────────────────────────
-- 3. ingest_batches : metadata des lots d'import en staging
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ingest_batches (
    batch_id            UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type         TEXT        NOT NULL,               -- 'items', 'suppliers', etc.
    source_system       TEXT        NOT NULL DEFAULT 'api', -- 'api', 'sftp', 'manual'
    original_filename   TEXT,                               -- Nom du fichier TSV si upload
    row_count           INTEGER     NOT NULL DEFAULT 0,
    status              TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (status IN (
                            'pending', 'processing', 'validated',
                            'rejected', 'importing', 'imported', 'import_failed'
                        )),
    -- DQ summary (dénormalisé pour affichage rapide)
    dq_valid_rows       INTEGER,
    dq_invalid_rows     INTEGER,
    dq_warning_count    INTEGER,
    -- Timestamps du pipeline
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    dq_started_at       TIMESTAMPTZ,
    dq_completed_at     TIMESTAMPTZ,
    importing_started_at TIMESTAMPTZ,
    imported_at         TIMESTAMPTZ,
    -- Provenance
    submitted_by        TEXT,       -- user_ref ou system_ref
    conflict_strategy   TEXT        NOT NULL DEFAULT 'upsert'
                        CHECK (conflict_strategy IN ('upsert', 'reject_duplicates', 'ignore_duplicates')),
    -- Import results (après approve)
    import_inserted     INTEGER,
    import_updated      INTEGER,
    import_skipped      INTEGER,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingest_batches_status
    ON ingest_batches (status, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingest_batches_entity_type
    ON ingest_batches (entity_type, status);

-- ─────────────────────────────────────────────────────────────
-- 4. ingest_rows : données brutes en staging
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ingest_rows (
    row_id          UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID        NOT NULL REFERENCES ingest_batches(batch_id) ON DELETE CASCADE,
    row_index       INTEGER     NOT NULL,    -- Position 0-indexée dans le fichier original
    raw_data        JSONB       NOT NULL,    -- Ligne brute parsée (colonnes → valeurs)
    dq_status       TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (dq_status IN ('pending', 'valid', 'warning', 'error')),
    -- Stocke la version transformée/normalisée après DQ réussie
    normalized_data JSONB,

    UNIQUE (batch_id, row_index)
);

-- Note : raw_data est JSONB ici UNIQUEMENT pour la couche staging.
-- Les core tables n'utilisent jamais JSONB (convention du projet : typed columns everywhere).

CREATE INDEX IF NOT EXISTS idx_ingest_rows_batch
    ON ingest_rows (batch_id, dq_status);

CREATE INDEX IF NOT EXISTS idx_ingest_rows_batch_valid
    ON ingest_rows (batch_id)
    WHERE dq_status IN ('valid', 'warning');

-- ─────────────────────────────────────────────────────────────
-- 5. dq_issues : problèmes Data Quality par ligne
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dq_issues (
    issue_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID        NOT NULL REFERENCES ingest_batches(batch_id) ON DELETE CASCADE,
    row_id          UUID        REFERENCES ingest_rows(row_id) ON DELETE CASCADE,
    row_index       INTEGER,    -- Dénormalisé pour affichage sans JOIN
    external_id     TEXT,       -- Code métier de la ligne (pour faciliter le debug)
    field           TEXT,       -- Colonne concernée (NULL si erreur de structure globale)
    severity        TEXT        NOT NULL
                    CHECK (severity IN ('error', 'warning', 'info')),
    rule            TEXT        NOT NULL,   -- ex: 'required_field', 'enum_validation', 'referential_integrity'
    message         TEXT        NOT NULL,
    raw_value       TEXT,       -- Valeur brute qui a causé l'erreur
    suggestion      TEXT,       -- Correction suggérée si possible
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    -- Issues are immutable — no updated_at
    -- Une correction via /fix recrée les issues (on efface + revalide)
);

CREATE INDEX IF NOT EXISTS idx_dq_issues_batch
    ON dq_issues (batch_id, severity);

CREATE INDEX IF NOT EXISTS idx_dq_issues_batch_row
    ON dq_issues (batch_id, row_index);

-- ─────────────────────────────────────────────────────────────
-- 6. import_audit_log : traçabilité des upserts en core
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS import_audit_log (
    audit_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID        NOT NULL REFERENCES ingest_batches(batch_id),
    entity_type     TEXT        NOT NULL,
    entity_id       UUID        NOT NULL,   -- PK de l'entité créée/mise à jour
    external_id     TEXT,
    action          TEXT        NOT NULL
                    CHECK (action IN ('inserted', 'updated', 'skipped')),
    -- Snapshot avant/après (limité aux champs clés pour ne pas exploser le volume)
    previous_values JSONB,      -- NULL si inserted
    new_values      JSONB,
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    -- Immutable
);

CREATE INDEX IF NOT EXISTS idx_audit_batch
    ON import_audit_log (batch_id);

CREATE INDEX IF NOT EXISTS idx_audit_entity
    ON import_audit_log (entity_type, entity_id, imported_at DESC);

-- ─────────────────────────────────────────────────────────────
-- 7. suppliers
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id         UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id         TEXT        NOT NULL UNIQUE,
    name                TEXT        NOT NULL,
    location_id         UUID        REFERENCES locations(location_id),
    lead_time_days      NUMERIC     NOT NULL DEFAULT 14 CHECK (lead_time_days >= 0),
    reliability_score   NUMERIC     NOT NULL DEFAULT 1.0 CHECK (reliability_score BETWEEN 0 AND 1),
    moq                 NUMERIC     CHECK (moq IS NULL OR moq >= 0),
    unit_cost_override  NUMERIC     CHECK (unit_cost_override IS NULL OR unit_cost_override >= 0),
    currency            TEXT        NOT NULL DEFAULT 'USD',
    status              TEXT        NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'inactive', 'approved', 'blocked')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_suppliers_external_id ON suppliers (external_id);
CREATE INDEX IF NOT EXISTS idx_suppliers_status ON suppliers (status) WHERE status = 'active';

-- ─────────────────────────────────────────────────────────────
-- 8. supplier_items
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS supplier_items (
    supplier_item_id    UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_id         UUID        NOT NULL REFERENCES suppliers(supplier_id),
    item_id             UUID        NOT NULL REFERENCES items(item_id),
    supplier_item_code  TEXT,
    lead_time_days      NUMERIC     NOT NULL DEFAULT 14 CHECK (lead_time_days >= 0),
    unit_cost           NUMERIC     CHECK (unit_cost IS NULL OR unit_cost >= 0),
    moq                 NUMERIC     CHECK (moq IS NULL OR moq >= 0),
    lot_multiple        NUMERIC     CHECK (lot_multiple IS NULL OR lot_multiple >= 1),
    reliability_score   NUMERIC     CHECK (reliability_score IS NULL OR reliability_score BETWEEN 0 AND 1),
    preferred           BOOLEAN     NOT NULL DEFAULT FALSE,
    effective_start     DATE,
    effective_end       DATE,
    CHECK (effective_end IS NULL OR effective_end > effective_start),
    status              TEXT        NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'inactive', 'approved')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (supplier_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_supplier_items_item ON supplier_items (item_id);
CREATE INDEX IF NOT EXISTS idx_supplier_items_preferred ON supplier_items (item_id, preferred) WHERE preferred = TRUE;

-- ─────────────────────────────────────────────────────────────
-- 9. item_planning_params
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS item_planning_params (
    param_id                        UUID            NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id                         UUID            NOT NULL REFERENCES items(item_id),
    location_id                     UUID            NOT NULL REFERENCES locations(location_id),
    lead_time_sourcing_days         NUMERIC         CHECK (lead_time_sourcing_days >= 0),
    lead_time_manufacturing_days    NUMERIC         CHECK (lead_time_manufacturing_days >= 0),
    lead_time_transit_days          NUMERIC         NOT NULL DEFAULT 0 CHECK (lead_time_transit_days >= 0),
    safety_stock_qty                NUMERIC         NOT NULL DEFAULT 0 CHECK (safety_stock_qty >= 0),
    safety_stock_days_of_coverage   NUMERIC         CHECK (safety_stock_days_of_coverage >= 0),
    reorder_point_qty               NUMERIC         CHECK (reorder_point_qty >= 0),
    reorder_point_days              NUMERIC         CHECK (reorder_point_days >= 0),
    min_order_qty                   NUMERIC         NOT NULL DEFAULT 1 CHECK (min_order_qty > 0),
    max_order_qty                   NUMERIC         CHECK (max_order_qty IS NULL OR max_order_qty >= min_order_qty),
    order_multiple                  NUMERIC         NOT NULL DEFAULT 1 CHECK (order_multiple > 0),
    lot_size_rule                   lot_size_rule_enum NOT NULL DEFAULT 'LOTFORLOT',
    planning_horizon_days           INTEGER         NOT NULL DEFAULT 90 CHECK (planning_horizon_days > 0),
    is_make                         BOOLEAN         NOT NULL DEFAULT FALSE,
    preferred_supplier_id           UUID            REFERENCES suppliers(supplier_id),
    effective_from                  DATE            NOT NULL DEFAULT CURRENT_DATE,
    effective_to                    DATE,
    CHECK (effective_to IS NULL OR effective_to > effective_from),
    source                          planning_param_source_enum NOT NULL DEFAULT 'manual',
    source_batch_id                 UUID            REFERENCES ingest_batches(batch_id),
    created_by                      TEXT,
    notes                           TEXT,
    created_at                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                      TIMESTAMPTZ     NOT NULL DEFAULT now(),

    UNIQUE NULLS NOT DISTINCT (item_id, location_id, effective_from)
);

CREATE INDEX IF NOT EXISTS idx_ipp_item_location_active
    ON item_planning_params (item_id, location_id, effective_from, effective_to);

CREATE INDEX IF NOT EXISTS idx_ipp_source
    ON item_planning_params (source, updated_at DESC);

-- ─────────────────────────────────────────────────────────────
-- 10. uom_conversions
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS uom_conversions (
    conversion_id   UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    from_uom        TEXT        NOT NULL,
    to_uom          TEXT        NOT NULL,
    item_id         UUID        REFERENCES items(item_id),
    factor          NUMERIC     NOT NULL CHECK (factor > 0),
    effective_from  DATE        NOT NULL DEFAULT CURRENT_DATE,
    effective_to    DATE,
    CHECK (effective_to IS NULL OR effective_to > effective_from),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE NULLS NOT DISTINCT (from_uom, to_uom, item_id, effective_from)
);

CREATE INDEX IF NOT EXISTS idx_uom_conv_pair
    ON uom_conversions (from_uom, to_uom, item_id);

-- ─────────────────────────────────────────────────────────────
-- 11. operational_calendars
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS operational_calendars (
    calendar_id     UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id     UUID        NOT NULL REFERENCES locations(location_id),
    date            DATE        NOT NULL,
    is_working_day  BOOLEAN     NOT NULL DEFAULT TRUE,
    shift_count     SMALLINT    NOT NULL DEFAULT 1 CHECK (shift_count BETWEEN 0 AND 3),
    capacity_factor NUMERIC     NOT NULL DEFAULT 1.0 CHECK (capacity_factor BETWEEN 0.0 AND 1.0),
    calendar_type   TEXT        CHECK (calendar_type IN (
                        'standard', 'public_holiday', 'plant_shutdown',
                        'reduced_capacity', 'extended_capacity'
                    )),
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (location_id, date)
);

CREATE INDEX IF NOT EXISTS idx_opcal_location_date
    ON operational_calendars (location_id, date);

CREATE INDEX IF NOT EXISTS idx_opcal_non_working
    ON operational_calendars (location_id, date)
    WHERE is_working_day = FALSE;
```

### Ordre de priorité d'implémentation

| Priorité | Table | Pourquoi |
|----------|-------|---------|
| **P0** | `ingest_batches` + `ingest_rows` + `dq_issues` | Sans elles, le pipeline 2 étapes est impossible |
| **P0** | `external_id` sur `items` + `locations` | Sans ça, aucun import ne peut faire la résolution ERP→UUID |
| **P0** | `external_references` | Seule table de mapping cross-système |
| **P1** | `suppliers` + `supplier_items` | Master data manquante, bloque les imports relationnels |
| **P1** | `item_planning_params` | Le cœur des données techniques — sans ça le moteur tourne à l'aveugle |
| **P2** | `uom_conversions` | Critique pour les calculs de quantités multi-UOM |
| **P2** | `operational_calendars` | Critique pour les calculs de lead time précis |
| **P3** | `import_audit_log` | Traçabilité, peut démarrer sans mais ne doit pas rester sans |

---

## 6. Anti-patterns à éviter absolument

### AP-1 : Import direct en core sans staging ❌

**Pattern à bannir :**
```python
# INTERDIT : valider ET persister en une seule passe
@router.post("/v1/import/items")
async def import_items(data: list[ItemIn]):
    for item in data:
        validate(item)      # si ça échoue → rollback partiel ou état incohérent
        await db.upsert(item)
```

**Pourquoi c'est fatal :**
Un import de 10 000 lignes qui plante à la ligne 9 999 laisse 9 998 lignes commitées et 2 en suspens. Le client ne sait pas quoi relancer. L'état de la base est indéterminé.

**Solution :** staging → DQ → approve (pipeline 2 étapes).

---

### AP-2 : Stocker les paramètres de planification dans le code Python ❌

**Pattern à bannir :**
```python
# INTERDIT : hardcoder les lead times dans le moteur
DEFAULT_LEAD_TIME = 14
SAFETY_STOCK_MULTIPLIER = 1.5
```

**Pourquoi c'est fatal :**
Chaque client a ses propres paramètres. Un changement de paramètre nécessite un redéploiement. Les simulations de scénarios ("que se passe-t-il si le lead time passe à 21 jours ?") deviennent impossibles sans modifier le code.

**Solution :** `item_planning_params` en base. Le moteur lit toujours depuis la DB, jamais depuis des constantes.

---

### AP-3 : Utiliser JSONB pour les données techniques ❌

**Pattern à bannir :**
```sql
-- INTERDIT : paramètres dans un blob JSONB
ALTER TABLE items ADD COLUMN planning_params JSONB;
```

**Pourquoi c'est fatal :**
- Impossible d'indexer `lead_time_sourcing_days >= 14`
- Pas de CHECK constraints sur les valeurs
- Migrations impossibles (comment ajouter un champ obligatoire dans un JSONB ?)
- Le moteur ne peut pas joindre sur des critères de planification

**Exception acceptable :** JSONB pour les données *brutes en staging* (`ingest_rows.raw_data`) uniquement — là où la structure est intentionnellement inconnue.

---

### AP-4 : Une seule table de politiques pour tout ❌

**Pattern à bannir :**
```sql
-- INTERDIT : tout dans une seule table fourrée
CREATE TABLE config (key TEXT, value TEXT, item_id UUID, location_id UUID);
```

**Pourquoi c'est fatal :**
Impossible de valider les types. Impossible d'indexer efficacement. Les jointures deviennent des pivots coûteux. Le schéma ne documente rien.

**Solution :** une table dédiée par sémantique : `item_planning_params`, `uom_conversions`, `operational_calendars`. Chaque table a ses propres contraintes et index.

---

### AP-5 : Pas de versioning temporel sur les paramètres ❌

**Pattern à bannir :**
```sql
-- INTERDIT : un seul enregistrement par (item, location)
UNIQUE (item_id, location_id)  -- écrase l'historique à chaque mise à jour
```

**Pourquoi c'est fatal :**
- Impossible de simuler "comment le plan aurait évolué si j'avais eu ces paramètres en janvier ?"
- Impossible d'auditer "qui a changé le MOQ et quand ?"
- Impossible de planifier une transition graduelle (nouveau contrat fournisseur à partir du 1er juillet)

**Solution :** `effective_from` / `effective_to` sur toutes les tables de paramètres. La version courante = `effective_from <= today < effective_to`.

---

### AP-6 : Couplage fort entre les couches du pipeline ❌

**Pattern à bannir :**
```python
# INTERDIT : l'ingestion appelle directement le service d'import
from ootils_core.import_pipeline.service import ImportService

@router.post("/v1/ingest/{type}")
async def ingest(data, import_service: ImportService = Depends()):
    batch_id = await stage(data)
    await import_service.approve(batch_id)  # ← court-circuite la DQ
```

**Pourquoi c'est fatal :**
Ça rend le pipeline 2 étapes inutile. En cas d'erreur en production, on ne sait plus à quelle étape le système a failli.

**Solution :** les couches communiquent uniquement via la base de données (statut du batch) et des interfaces de service explicites. Jamais d'imports directs cross-couche.

---

### AP-7 : Import sans résolution external_id → UUID ❌

**Pattern à bannir :**
```python
# INTERDIT : utiliser le code ERP comme PK en base Ootils
INSERT INTO nodes (item_id, ...) VALUES ('SKU-PUMP-01', ...)
```

**Pourquoi c'est fatal :**
Les codes ERP ne sont pas stables (restructuration SI, migration ERP, fusion-acquisition). Un client qui change son SAP casse tout.

**Solution :** `external_references` est la seule passerelle. Les core tables n'exposent que des UUIDs internes. La résolution `external_id → UUID` est une opération explicite et traçable.

---

### AP-8 : Pas de gestion d'erreur atomique sur les imports relationnels ❌

**Pattern à bannir :**
```python
# INTERDIT : import multi-entités sans transaction
await import_items(rows_items)
await import_locations(rows_locations)  # si ça plante ici, items sont commitées sans locations
await import_supplier_items(rows_supplier_items)
```

**Solution :** les imports relationnels (ex: `supplier_items` qui dépend de `suppliers` et `items`) s'exécutent dans une transaction unique. Soit tout passe, soit rien.

---

## 7. Décisions architecturales clés

### DA-1 : HTTP 202 à l'ingestion, pas 200

L'ingest répond **202 Accepted** (et non 200 OK) parce que la DQ est asynchrone. Retourner 200 impliquerait que la validation est terminée. C'est un contrat API qu'on ne peut pas tenir si le fichier fait 50 000 lignes.

### DA-2 : JSONB autorisé uniquement en staging

La convention du projet ("no JSONB for structured data — typed columns everywhere") s'applique à toutes les core tables. Exception explicite : `ingest_rows.raw_data` et `import_audit_log.previous_values/new_values` où la structure est intentionnellement dynamique.

### DA-3 : Pas de soft-delete sur les données techniques

Les paramètres de planification ne sont pas soft-deletés — ils sont **clôturés** via `effective_to`. La différence sémantique est importante : une version clôturée reste consultable pour l'historique et les simulations. Un soft-delete masque l'information.

### DA-4 : Le moteur ne lit jamais ingest_rows ou dq_issues

Le moteur de planification (`engine/`) ne connaît pas l'existence du pipeline d'import. Il lit uniquement les core tables. Cette séparation garantit que le moteur ne peut pas être pollué par des données non validées en staging.

### DA-5 : Le pipeline 2 étapes est non-négociable même pour les petits imports

On pourrait être tenté d'ajouter un mode "fast import" pour les petits fichiers (< 100 lignes) qui bypasse le staging. **Non.** La cohérence de l'API est plus précieuse que les 200ms gagnées. Les clients intègrent une fois le pattern `POST → poll → approve` et il fonctionne à toutes les échelles.

---

*Document maintenu par : Architecture Ootils*  
*Révision suivante : avant implémentation de la migration 004*
