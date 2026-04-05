# REVIEW — Import Data Engineering & Data Quality

> **Version :** 1.0.0  
> **Date :** 2026-04-05  
> **Auteur :** Data Engineering Review  
> **Périmètre :** SPEC-IMPORT-STATIC.md + SPEC-IMPORT-DYNAMIC.md  
> **Branche :** `review/import-data-engineering`

---

## Table des matières

1. [Architecture Staging Zone](#1-architecture-staging-zone)
2. [Pipeline de Data Quality](#2-pipeline-de-data-quality)
3. [Gestion des anomalies](#3-gestion-des-anomalies)
4. [Historisation et audit (SCDs)](#4-historisation-et-audit-scds)
5. [Détection de dérives statistiques](#5-détection-de-dérives-statistiques)
6. [Recommandations architecturales — Migration 003+](#6-recommandations-architecturales--migration-003)

---

## 1. Architecture Staging Zone

### 1.1 Principe fondateur : Tout accepter en staging

La staging zone est une **zone de quarantaine brute** : on accepte tout, même les données malformées. Rien n'est rejeté avant d'avoir été enregistré. Le traitement commence *après* la réception.

```
ERP/WMS/EDI
    │
    ├── TSV / JSON / CSV
    │
    ▼
┌─────────────────────────────────────┐
│          STAGING ZONE               │  ← Tout TEXT, aucun type forcé
│  stg_items, stg_purchase_orders...  │  ← Métadonnées batch complètes
│  Acceptation : 100% des lignes      │  ← Flagging des anomalies
└─────────────────────────────────────┘
    │
    ▼  (pipeline DQ asynchrone)
┌─────────────────────────────────────┐
│       PRODUCTION TABLES             │  ← Types stricts, contraintes FK
│  items, purchase_orders, nodes...   │  ← Données qualifiées seulement
└─────────────────────────────────────┘
```

**Pourquoi tout accepter ?**
- Traçabilité complète : même une ligne invalide est auditée
- Pas de perte silencieuse : chaque rejet est explicite et consultable
- Replay possible : si une règle DQ change, on peut re-processer le staging

### 1.2 Tables de staging — Schéma générique

Chaque flux a sa table `stg_*`. Toutes partagent la même structure de métadonnées :

```sql
-- Colonnes de métadonnées communes (template pour toutes stg_*)
-- batch_id       : identifie l'import (un fichier = un batch_id)
-- row_number     : numéro de ligne dans le fichier source (pour debug)
-- source_system  : identifiant du système source (ex: 'SAP-ECC-PROD', 'WMS-MANHATTAN')
-- imported_at    : timestamp de réception
-- raw_content    : ligne brute complète (JSON ou TSV line as text)
-- stg_status     : 'pending' | 'valid' | 'invalid' | 'promoted' | 'quarantined'
-- dq_level_reached : dernier niveau DQ atteint (1-4), NULL si pas encore traité
-- error_count    : nombre d'erreurs DQ détectées
```

### 1.3 Schéma SQL complet des tables staging

```sql
-- ============================================================
-- STG_ITEMS — Master Data : Articles
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_items (
    stg_id              UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            UUID        NOT NULL,
    row_number          INTEGER     NOT NULL,
    source_system       TEXT        NOT NULL DEFAULT 'unknown',
    imported_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_content         TEXT,                  -- Ligne TSV brute ou JSON sérialisé

    -- Colonnes brutes : tout en TEXT, aucun cast en staging
    external_id         TEXT,
    name                TEXT,
    item_type           TEXT,
    uom                 TEXT,
    status              TEXT,

    -- Statut DQ
    stg_status          TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (stg_status IN ('pending', 'valid', 'invalid', 'promoted', 'quarantined')),
    dq_level_reached    SMALLINT,              -- NULL = pas encore traité
    error_count         INTEGER     NOT NULL DEFAULT 0,
    promoted_at         TIMESTAMPTZ,           -- Quand la ligne a été injectée en prod
    production_id       UUID                   -- FK vers items.item_id après promotion
);

CREATE INDEX IF NOT EXISTS idx_stg_items_batch ON stg_items (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_items_status ON stg_items (stg_status);
CREATE INDEX IF NOT EXISTS idx_stg_items_external_id ON stg_items (external_id);


-- ============================================================
-- STG_LOCATIONS — Master Data : Sites
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_locations (
    stg_id              UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            UUID        NOT NULL,
    row_number          INTEGER     NOT NULL,
    source_system       TEXT        NOT NULL DEFAULT 'unknown',
    imported_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_content         TEXT,

    external_id         TEXT,
    name                TEXT,
    location_type       TEXT,
    country             TEXT,
    timezone            TEXT,
    parent_external_id  TEXT,

    stg_status          TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (stg_status IN ('pending', 'valid', 'invalid', 'promoted', 'quarantined')),
    dq_level_reached    SMALLINT,
    error_count         INTEGER     NOT NULL DEFAULT 0,
    promoted_at         TIMESTAMPTZ,
    production_id       UUID
);

CREATE INDEX IF NOT EXISTS idx_stg_locations_batch ON stg_locations (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_locations_status ON stg_locations (stg_status);


-- ============================================================
-- STG_SUPPLIERS — Master Data : Fournisseurs
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_suppliers (
    stg_id              UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            UUID        NOT NULL,
    row_number          INTEGER     NOT NULL,
    source_system       TEXT        NOT NULL DEFAULT 'unknown',
    imported_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_content         TEXT,

    external_id         TEXT,
    name                TEXT,
    location_external_id TEXT,
    lead_time_days      TEXT,       -- TEXT en staging : "14", "14.5", "two weeks" → tout passe
    reliability_score   TEXT,
    moq                 TEXT,
    unit_cost_override  TEXT,
    currency            TEXT,
    status              TEXT,

    stg_status          TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (stg_status IN ('pending', 'valid', 'invalid', 'promoted', 'quarantined')),
    dq_level_reached    SMALLINT,
    error_count         INTEGER     NOT NULL DEFAULT 0,
    promoted_at         TIMESTAMPTZ,
    production_id       UUID
);


-- ============================================================
-- STG_SUPPLIER_ITEMS — Relation Supplier × Item
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_supplier_items (
    stg_id                  UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                UUID        NOT NULL,
    row_number              INTEGER     NOT NULL,
    source_system           TEXT        NOT NULL DEFAULT 'unknown',
    imported_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_content             TEXT,

    supplier_external_id    TEXT,
    item_external_id        TEXT,
    supplier_item_code      TEXT,
    lead_time_days          TEXT,
    unit_cost               TEXT,
    moq                     TEXT,
    lot_multiple            TEXT,
    reliability_score       TEXT,
    preferred               TEXT,       -- "true", "1", "yes" → normalisé en boolean au processing
    effective_start         TEXT,
    effective_end           TEXT,
    status                  TEXT,

    stg_status              TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (stg_status IN ('pending', 'valid', 'invalid', 'promoted', 'quarantined')),
    dq_level_reached        SMALLINT,
    error_count             INTEGER     NOT NULL DEFAULT 0,
    promoted_at             TIMESTAMPTZ,
    production_id           UUID
);


-- ============================================================
-- STG_PURCHASE_ORDERS — Données dynamiques : PO
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_purchase_orders (
    stg_id                  UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                UUID        NOT NULL,
    row_number              INTEGER     NOT NULL,
    source_system           TEXT        NOT NULL DEFAULT 'unknown',
    imported_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_content             TEXT,

    po_number               TEXT,
    line_number             TEXT,
    item_external_id        TEXT,
    supplier_external_id    TEXT,
    location_external_id    TEXT,
    quantity                TEXT,
    uom                     TEXT,
    expected_date           TEXT,
    status                  TEXT,

    stg_status              TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (stg_status IN ('pending', 'valid', 'invalid', 'promoted', 'quarantined')),
    dq_level_reached        SMALLINT,
    error_count             INTEGER     NOT NULL DEFAULT 0,
    promoted_at             TIMESTAMPTZ,
    production_id           UUID
);

CREATE INDEX IF NOT EXISTS idx_stg_po_batch ON stg_purchase_orders (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_po_status ON stg_purchase_orders (stg_status);
CREATE INDEX IF NOT EXISTS idx_stg_po_keys ON stg_purchase_orders (po_number, line_number);


-- ============================================================
-- STG_CUSTOMER_ORDERS
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_customer_orders (
    stg_id                  UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                UUID        NOT NULL,
    row_number              INTEGER     NOT NULL,
    source_system           TEXT        NOT NULL DEFAULT 'unknown',
    imported_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_content             TEXT,

    order_number            TEXT,
    line_number             TEXT,
    item_external_id        TEXT,
    location_external_id    TEXT,
    quantity                TEXT,
    uom                     TEXT,
    requested_date          TEXT,
    confirmed_date          TEXT,
    status                  TEXT,

    stg_status              TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (stg_status IN ('pending', 'valid', 'invalid', 'promoted', 'quarantined')),
    dq_level_reached        SMALLINT,
    error_count             INTEGER     NOT NULL DEFAULT 0,
    promoted_at             TIMESTAMPTZ,
    production_id           UUID
);


-- ============================================================
-- STG_FORECASTS
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_forecasts (
    stg_id                  UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                UUID        NOT NULL,
    row_number              INTEGER     NOT NULL,
    source_system           TEXT        NOT NULL DEFAULT 'unknown',
    imported_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_content             TEXT,

    item_external_id        TEXT,
    location_external_id    TEXT,
    forecast_date           TEXT,
    quantity                TEXT,
    uom                     TEXT,
    bucket_type             TEXT,
    source                  TEXT,

    stg_status              TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (stg_status IN ('pending', 'valid', 'invalid', 'promoted', 'quarantined')),
    dq_level_reached        SMALLINT,
    error_count             INTEGER     NOT NULL DEFAULT 0,
    promoted_at             TIMESTAMPTZ,
    production_id           UUID
);


-- ============================================================
-- STG_WORK_ORDERS
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_work_orders (
    stg_id                  UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                UUID        NOT NULL,
    row_number              INTEGER     NOT NULL,
    source_system           TEXT        NOT NULL DEFAULT 'unknown',
    imported_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_content             TEXT,

    wo_number               TEXT,
    item_external_id        TEXT,
    location_external_id    TEXT,
    quantity                TEXT,
    uom                     TEXT,
    start_date              TEXT,
    end_date                TEXT,
    status                  TEXT,

    stg_status              TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (stg_status IN ('pending', 'valid', 'invalid', 'promoted', 'quarantined')),
    dq_level_reached        SMALLINT,
    error_count             INTEGER     NOT NULL DEFAULT 0,
    promoted_at             TIMESTAMPTZ,
    production_id           UUID
);


-- ============================================================
-- STG_TRANSFERS
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_transfers (
    stg_id                  UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                UUID        NOT NULL,
    row_number              INTEGER     NOT NULL,
    source_system           TEXT        NOT NULL DEFAULT 'unknown',
    imported_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_content             TEXT,

    transfer_number         TEXT,
    item_external_id        TEXT,
    from_location           TEXT,
    to_location             TEXT,
    quantity                TEXT,
    uom                     TEXT,
    ship_date               TEXT,
    expected_receipt_date   TEXT,
    status                  TEXT,

    stg_status              TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (stg_status IN ('pending', 'valid', 'invalid', 'promoted', 'quarantined')),
    dq_level_reached        SMALLINT,
    error_count             INTEGER     NOT NULL DEFAULT 0,
    promoted_at             TIMESTAMPTZ,
    production_id           UUID
);


-- ============================================================
-- STG_IMPORT_BATCHES — Registre de tous les imports
-- ============================================================
CREATE TABLE IF NOT EXISTS stg_import_batches (
    batch_id            UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type         TEXT        NOT NULL,   -- 'items', 'purchase_orders', etc.
    source_system       TEXT        NOT NULL DEFAULT 'unknown',
    import_mode         TEXT        NOT NULL DEFAULT 'delta'
                        CHECK (import_mode IN ('full_replace', 'delta', 'upsert')),
    filename            TEXT,
    file_size_bytes     BIGINT,
    total_rows          INTEGER     NOT NULL DEFAULT 0,
    rows_accepted       INTEGER     NOT NULL DEFAULT 0,
    rows_valid          INTEGER     NOT NULL DEFAULT 0,
    rows_promoted       INTEGER     NOT NULL DEFAULT 0,
    rows_invalid        INTEGER     NOT NULL DEFAULT 0,
    rows_quarantined    INTEGER     NOT NULL DEFAULT 0,
    batch_status        TEXT        NOT NULL DEFAULT 'receiving'
                        CHECK (batch_status IN ('receiving', 'dq_pending', 'dq_running',
                                                'dq_complete', 'promoting', 'complete',
                                                'blocked', 'failed')),
    error_rate          NUMERIC     GENERATED ALWAYS AS (
                            CASE WHEN total_rows > 0
                            THEN ROUND(rows_invalid::NUMERIC / total_rows * 100, 2)
                            ELSE 0 END
                        ) STORED,
    blocking_reason     TEXT,       -- Pourquoi le batch est bloqué (si batch_status = 'blocked')
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    dq_started_at       TIMESTAMPTZ,
    dq_completed_at     TIMESTAMPTZ,
    promoted_at         TIMESTAMPTZ,
    initiated_by        TEXT        NOT NULL DEFAULT 'api',   -- 'api', 'scheduler', 'manual'
    dry_run             BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_stg_batches_entity ON stg_import_batches (entity_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_stg_batches_status ON stg_import_batches (batch_status);
```

### 1.4 Règles d'acceptation en staging

**Politique : TOUT ACCEPTER avec flagging immédiat**

```
Réception fichier TSV/JSON
    │
    ├── Encodage invalide (non-UTF8) ?
    │       → Rejeter le fichier entier (avant staging)
    │       → Enregistrer le reject dans stg_import_batches (batch_status = 'failed')
    │       → C'est la SEULE raison de rejeter avant staging
    │
    ├── Header TSV absent / colonnes requises manquantes ?
    │       → Rejeter le fichier entier (structurellement impossible à parser)
    │       → batch_status = 'failed', blocking_reason = 'missing_required_columns'
    │
    └── Sinon → Insérer TOUTES les lignes en stg_*
                Toutes les lignes = stg_status = 'pending'
                Pipeline DQ lancé en async
```

**Pourquoi pas de filtrage en amont ?**
- Une ligne avec `quantity = "N/A"` doit être loggée et tracée, pas silencieusement perdue
- Permet le retry avec correction sans reprise complète
- Garantit que 100% du flux source est visible dans l'audit log

### 1.5 Rétention des données staging

```sql
-- Politique de rétention recommandée
-- Staging données dynamiques (PO, CO, forecasts, onhand) : 90 jours
-- Staging master data (items, locations, suppliers) : 365 jours
-- Staging promu avec erreurs (stg_status = 'quarantined') : 180 jours

-- Job de purge (à exécuter quotidiennement via pg_cron ou scheduler externe)
DELETE FROM stg_purchase_orders
WHERE imported_at < now() - INTERVAL '90 days'
  AND stg_status IN ('promoted', 'invalid');

DELETE FROM stg_items
WHERE imported_at < now() - INTERVAL '365 days'
  AND stg_status IN ('promoted', 'invalid');

-- Les lignes 'quarantined' ne sont jamais purgées automatiquement
-- Elles nécessitent une résolution manuelle ou une décision explicite
```

---

## 2. Pipeline de Data Quality

### 2.1 Séquencement général

```
stg_* (status = 'pending')
    │
    ▼
[NIVEAU 1] Validation structurelle
    │── Erreur → stg_status = 'invalid', dq_level_reached = 1
    │── OK     → continue
    ▼
[NIVEAU 2] Validation référentielle
    │── Erreur → stg_status = 'invalid', dq_level_reached = 2
    │── OK     → continue
    ▼
[NIVEAU 3] Validation métier supply chain
    │── Erreur bloquante → stg_status = 'invalid', dq_level_reached = 3
    │── Warning          → continue avec flag
    ▼
[NIVEAU 4] Validation croisée
    │── Erreur → stg_status = 'quarantined', dq_level_reached = 4
    │── OK     → stg_status = 'valid'
    ▼
[SEUIL BATCH] Vérification taux d'erreurs
    │── error_rate > seuil → batch_status = 'blocked'
    │── error_rate ≤ seuil → promotion en tables production
    ▼
Promotion → stg_status = 'promoted', production_id = <uuid>
```

### 2.2 Niveau 1 — Validation structurelle

#### Pour tous les flux

```sql
-- Exemple de règles N1 sous forme de requêtes d'audit
-- Ces requêtes alimentent la table data_quality_issues

-- Règle N1-001 : external_id non null et non vide
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT
    batch_id,
    stg_id,
    'stg_items',
    'N1-001',
    'blocking',
    'external_id is required and cannot be null or empty',
    external_id
FROM stg_items
WHERE batch_id = :batch_id
  AND (external_id IS NULL OR trim(external_id) = '');

-- Règle N1-002 : name non vide
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_items', 'N1-002', 'blocking',
       'name is required and cannot be null or empty', name
FROM stg_items
WHERE batch_id = :batch_id AND (name IS NULL OR trim(name) = '');

-- Règle N1-003 : lead_time_days parseable en NUMERIC (supplier)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_suppliers', 'N1-003', 'blocking',
       'lead_time_days must be a valid number', lead_time_days
FROM stg_suppliers
WHERE batch_id = :batch_id
  AND lead_time_days IS NOT NULL
  AND lead_time_days !~ '^[0-9]+(\.[0-9]+)?$';

-- Règle N1-004 : expected_date parseable en DATE (PO)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_purchase_orders', 'N1-004', 'blocking',
       'expected_date must be a valid ISO 8601 date (YYYY-MM-DD)', expected_date
FROM stg_purchase_orders
WHERE batch_id = :batch_id
  AND expected_date IS NOT NULL
  AND (
    expected_date !~ '^\d{4}-\d{2}-\d{2}$'
    OR expected_date::date IS NULL  -- raises exception → use TRY_CAST equivalent
  );

-- Règle N1-005 : quantity parseable en NUMERIC
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_purchase_orders', 'N1-005', 'blocking',
       'quantity must be a valid number', quantity
FROM stg_purchase_orders
WHERE batch_id = :batch_id
  AND (quantity IS NULL OR quantity !~ '^-?[0-9]+(\.[0-9]+)?$');

-- Règle N1-006 : external_id unique dans le batch (doublons intra-fichier)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT s.batch_id, s.stg_id, 'stg_items', 'N1-006', 'blocking',
       'Duplicate external_id within the same batch', s.external_id
FROM stg_items s
JOIN (
    SELECT batch_id, external_id
    FROM stg_items
    WHERE batch_id = :batch_id AND external_id IS NOT NULL
    GROUP BY batch_id, external_id
    HAVING COUNT(*) > 1
) dups USING (batch_id, external_id)
WHERE s.batch_id = :batch_id;
```

#### Catalogue règles N1 par flux

| Code     | Flux       | Champ              | Sévérité | Règle                                        |
|----------|------------|--------------------|----------|----------------------------------------------|
| N1-001   | items      | external_id        | blocking | Non null, non vide                           |
| N1-002   | items      | name               | blocking | Non null, non vide                           |
| N1-003   | suppliers  | lead_time_days     | blocking | Parseable en NUMERIC si fourni               |
| N1-004   | PO         | expected_date      | blocking | Format YYYY-MM-DD valide                     |
| N1-005   | PO/CO/WO   | quantity           | blocking | Parseable en NUMERIC, non null               |
| N1-006   | tous       | external_id        | blocking | Unique dans le batch                         |
| N1-007   | PO/CO      | (key, line)        | blocking | (po_number, line_number) unique dans batch   |
| N1-008   | suppliers  | reliability_score  | blocking | Si fourni : parseable entre 0 et 1           |
| N1-009   | locations  | country            | warning  | 2 caractères alpha si fourni                 |
| N1-010   | forecasts  | forecast_date      | blocking | Format selon bucket_type (YYYY-MM-DD / YYYY-Www / YYYY-MM) |
| N1-011   | transfers  | ship_date          | blocking | Format YYYY-MM-DD valide                     |
| N1-012   | supplier_items | effective_end  | blocking | Format YYYY-MM-DD si fourni                  |

### 2.3 Niveau 2 — Validation référentielle

```sql
-- Règle N2-001 : item_external_id résolu dans items (PO)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT s.batch_id, s.stg_id, 'stg_purchase_orders', 'N2-001', 'blocking',
       'item_external_id not found in items master data', s.item_external_id
FROM stg_purchase_orders s
LEFT JOIN items i ON i.external_id = s.item_external_id
WHERE s.batch_id = :batch_id
  AND s.item_external_id IS NOT NULL
  AND i.item_id IS NULL;

-- Règle N2-002 : location_external_id résolu dans locations
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT s.batch_id, s.stg_id, 'stg_purchase_orders', 'N2-002', 'blocking',
       'location_external_id not found in locations master data', s.location_external_id
FROM stg_purchase_orders s
LEFT JOIN locations l ON l.external_id = s.location_external_id
WHERE s.batch_id = :batch_id
  AND l.location_id IS NULL;

-- Règle N2-003 : supplier_external_id résolu (PO — warning si absent car optionnel)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT s.batch_id, s.stg_id, 'stg_purchase_orders', 'N2-003', 'warning',
       'supplier_external_id not found in suppliers — PO will import without supplier link',
       s.supplier_external_id
FROM stg_purchase_orders s
LEFT JOIN suppliers sup ON sup.external_id = s.supplier_external_id
WHERE s.batch_id = :batch_id
  AND s.supplier_external_id IS NOT NULL
  AND sup.supplier_id IS NULL;

-- Règle N2-004 : détection de cycles dans la hiérarchie locations
-- (Exécuté via CTE récursive avant insertion en prod)
WITH RECURSIVE hierarchy_check AS (
    -- Base : lignes avec un parent
    SELECT stg_id, external_id, parent_external_id, ARRAY[external_id] AS path, FALSE AS cycle
    FROM stg_locations
    WHERE batch_id = :batch_id AND parent_external_id IS NOT NULL

    UNION ALL

    -- Récursion : suivre le parent
    SELECT child.stg_id, parent.external_id, parent.parent_external_id,
           h.path || parent.external_id,
           parent.external_id = ANY(h.path)
    FROM stg_locations parent
    JOIN hierarchy_check h ON h.parent_external_id = parent.external_id
    WHERE NOT h.cycle
)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT DISTINCT :batch_id, stg_id, 'stg_locations', 'N2-005', 'blocking',
       'Circular reference detected in location hierarchy',
       array_to_string(path, ' → ')
FROM hierarchy_check
WHERE cycle;

-- Règle N2-005 : supplier_items — item ET supplier doivent exister
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT s.batch_id, s.stg_id, 'stg_supplier_items', 'N2-006', 'blocking',
       'Both supplier and item must exist before importing supplier_items',
       s.supplier_external_id || ' / ' || s.item_external_id
FROM stg_supplier_items s
LEFT JOIN suppliers sup ON sup.external_id = s.supplier_external_id
LEFT JOIN items i ON i.external_id = s.item_external_id
WHERE s.batch_id = :batch_id
  AND (sup.supplier_id IS NULL OR i.item_id IS NULL);
```

### 2.4 Niveau 3 — Validation métier supply chain

```sql
-- Règle N3-001 : lead_time_days > 0 (fournisseur actif)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_suppliers', 'N3-001', 'blocking',
       'lead_time_days must be > 0 for active suppliers',
       lead_time_days
FROM stg_suppliers
WHERE batch_id = :batch_id
  AND lead_time_days IS NOT NULL
  AND lead_time_days ~ '^[0-9]+(\.[0-9]+)?$'
  AND lead_time_days::NUMERIC <= 0
  AND (status IS NULL OR status = 'active');

-- Règle N3-002 : quantity > 0 pour PO actifs (non annulation)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_purchase_orders', 'N3-002', 'blocking',
       'quantity must be > 0 for active PO (use status=cancelled for cancellations)',
       quantity
FROM stg_purchase_orders
WHERE batch_id = :batch_id
  AND quantity ~ '^-?[0-9]+(\.[0-9]+)?$'
  AND quantity::NUMERIC <= 0
  AND status NOT IN ('cancelled');

-- Règle N3-003 : expected_date >= today pour PO confirmed/pending
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_purchase_orders', 'N3-003', 'warning',
       'expected_date is in the past for an active PO — possible data lag or error',
       expected_date
FROM stg_purchase_orders
WHERE batch_id = :batch_id
  AND expected_date ~ '^\d{4}-\d{2}-\d{2}$'
  AND expected_date::date < CURRENT_DATE
  AND status IN ('confirmed', 'pending');

-- Règle N3-004 : safety_stock < max_stock (item_location_policies)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT s.batch_id, s.stg_id, 'stg_item_location_policies', 'N3-004', 'blocking',
       'safety_stock_qty must be < max_stock when replenishment_type = min_max',
       'safety=' || s.safety_stock_qty || ' max=' || s.max_stock
FROM stg_item_location_policies s
WHERE batch_id = :batch_id
  AND replenishment_type = 'min_max'
  AND safety_stock_qty ~ '^[0-9]+(\.[0-9]+)?$'
  AND max_stock ~ '^[0-9]+(\.[0-9]+)?$'
  AND safety_stock_qty::NUMERIC >= max_stock::NUMERIC;

-- Règle N3-005 : UOM cohérent avec le référentiel article
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT s.batch_id, s.stg_id, 'stg_purchase_orders', 'N3-005', 'warning',
       'UOM in PO does not match item master UOM — conversion may be needed',
       s.uom || ' (item master: ' || i.uom || ')'
FROM stg_purchase_orders s
JOIN items i ON i.external_id = s.item_external_id
WHERE s.batch_id = :batch_id
  AND s.uom IS NOT NULL
  AND s.uom <> i.uom;

-- Règle N3-006 : effective_end > effective_start (supplier_items)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_supplier_items', 'N3-006', 'blocking',
       'effective_end must be strictly after effective_start',
       effective_start || ' → ' || effective_end
FROM stg_supplier_items
WHERE batch_id = :batch_id
  AND effective_start ~ '^\d{4}-\d{2}-\d{2}$'
  AND effective_end ~ '^\d{4}-\d{2}-\d{2}$'
  AND effective_end::date <= effective_start::date;

-- Règle N3-007 : reliability_score ∈ [0, 1]
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_suppliers', 'N3-007', 'blocking',
       'reliability_score must be between 0.0 and 1.0',
       reliability_score
FROM stg_suppliers
WHERE batch_id = :batch_id
  AND reliability_score ~ '^[0-9]+(\.[0-9]+)?$'
  AND (reliability_score::NUMERIC < 0 OR reliability_score::NUMERIC > 1);
```

### 2.5 Niveau 4 — Validation croisée

```sql
-- Règle N4-001 : Transfers — from_location ≠ to_location
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_transfers', 'N4-001', 'blocking',
       'Transfer from_location and to_location cannot be the same',
       from_location || ' = ' || to_location
FROM stg_transfers
WHERE batch_id = :batch_id
  AND from_location IS NOT NULL
  AND from_location = to_location;

-- Règle N4-002 : Transfers — chemin valide (les deux locations existent)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT s.batch_id, s.stg_id, 'stg_transfers', 'N4-002', 'blocking',
       'Transfer path invalid: from_location or to_location not found',
       s.from_location || ' → ' || s.to_location
FROM stg_transfers s
LEFT JOIN locations lf ON lf.external_id = s.from_location
LEFT JOIN locations lt ON lt.external_id = s.to_location
WHERE s.batch_id = :batch_id
  AND (lf.location_id IS NULL OR lt.location_id IS NULL);

-- Règle N4-003 : expected_receipt_date > ship_date (transfers)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_transfers', 'N4-003', 'warning',
       'expected_receipt_date should be after ship_date',
       ship_date || ' → ' || expected_receipt_date
FROM stg_transfers
WHERE batch_id = :batch_id
  AND ship_date ~ '^\d{4}-\d{2}-\d{2}$'
  AND expected_receipt_date ~ '^\d{4}-\d{2}-\d{2}$'
  AND expected_receipt_date::date < ship_date::date;

-- Règle N4-004 : WO end_date > start_date
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT batch_id, stg_id, 'stg_work_orders', 'N4-004', 'blocking',
       'end_date must be after start_date',
       start_date || ' → ' || end_date
FROM stg_work_orders
WHERE batch_id = :batch_id
  AND start_date ~ '^\d{4}-\d{2}-\d{2}$'
  AND end_date ~ '^\d{4}-\d{2}-\d{2}$'
  AND end_date::date <= start_date::date;

-- Règle N4-005 : PO pour item avec politique push doit avoir un forecast
-- (Warning seulement : le PO est accepté mais signalé)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT s.batch_id, s.stg_id, 'stg_purchase_orders', 'N4-005', 'warning',
       'PO for push-policy item has no active forecast — review replenishment logic',
       s.item_external_id || ' @ ' || s.location_external_id
FROM stg_purchase_orders s
JOIN items i ON i.external_id = s.item_external_id
JOIN item_location_policies ilp ON ilp.item_id = i.item_id
    AND EXISTS (SELECT 1 FROM locations l WHERE l.external_id = s.location_external_id AND l.location_id = ilp.location_id)
WHERE s.batch_id = :batch_id
  AND ilp.replenishment_type = 'jit'                    -- politique push/JIT
  AND s.status IN ('confirmed', 'pending')
  AND NOT EXISTS (
    SELECT 1 FROM nodes n
    JOIN items fi ON fi.item_id = (n.payload->>'item_id')::uuid
    WHERE fi.external_id = s.item_external_id
      AND n.node_type = 'ForecastDemand'
      AND n.is_active = TRUE
  );
```

---

## 3. Gestion des anomalies

### 3.1 Table `data_quality_issues`

```sql
CREATE TABLE IF NOT EXISTS data_quality_issues (
    issue_id            UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            UUID        NOT NULL REFERENCES stg_import_batches(batch_id),
    stg_id              UUID        NOT NULL,           -- FK vers la ligne staging concernée
    stg_table           TEXT        NOT NULL,           -- 'stg_items', 'stg_purchase_orders', etc.
    rule_code           TEXT        NOT NULL,           -- 'N1-001', 'N3-002', etc.
    severity            TEXT        NOT NULL
                        CHECK (severity IN ('blocking', 'warning', 'info')),
    dq_level            SMALLINT    NOT NULL            -- 1, 2, 3, ou 4
                        GENERATED ALWAYS AS (
                            CASE WHEN rule_code LIKE 'N1%' THEN 1
                                 WHEN rule_code LIKE 'N2%' THEN 2
                                 WHEN rule_code LIKE 'N3%' THEN 3
                                 WHEN rule_code LIKE 'N4%' THEN 4
                                 ELSE 0 END
                        ) STORED,
    description         TEXT        NOT NULL,
    raw_value           TEXT,                           -- Valeur brute en cause
    field_name          TEXT,                           -- Colonne en cause (si applicable)
    resolution_status   TEXT        NOT NULL DEFAULT 'open'
                        CHECK (resolution_status IN ('open', 'acknowledged', 'corrected',
                                                     'overridden', 'wont_fix')),
    resolution_note     TEXT,                           -- Commentaire de résolution
    resolved_by         TEXT,                           -- Identité du résolveur
    resolved_at         TIMESTAMPTZ,
    auto_corrected      BOOLEAN     NOT NULL DEFAULT FALSE,  -- Correction automatique appliquée ?
    auto_correction_detail TEXT,    -- Description de la correction automatique
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dqi_batch ON data_quality_issues (batch_id);
CREATE INDEX IF NOT EXISTS idx_dqi_stg ON data_quality_issues (stg_id, stg_table);
CREATE INDEX IF NOT EXISTS idx_dqi_status ON data_quality_issues (resolution_status) WHERE resolution_status = 'open';
CREATE INDEX IF NOT EXISTS idx_dqi_severity ON data_quality_issues (severity, batch_id);
```

### 3.2 Stratégie de gestion par sévérité

| Sévérité | Action immédiate | Impact sur batch | Workflow |
|----------|-----------------|-----------------|---------|
| `blocking` | Ligne → `stg_status = 'invalid'` | Compte pour taux d'erreur | Résolution manuelle requise avant re-import |
| `warning` | Ligne acceptée, issue loggée | Ne compte pas pour le taux d'erreur de blocage | Notification, résolution optionnelle |
| `info` | Log seulement | Aucun | Audit trail |

### 3.3 Corrections automatiques (smart substitutions)

Certaines anomalies admettent une correction automatique documentée :

```sql
-- Correction auto N1-009 : country code en majuscules
-- "fr" → "FR", "us" → "US"
UPDATE stg_locations
SET country = UPPER(country),
    -- Documenter la correction
    raw_content = raw_content  -- immutable, la correction est dans la colonne typée
WHERE batch_id = :batch_id
  AND country IS NOT NULL
  AND country <> UPPER(country);

INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity,
    description, raw_value, auto_corrected, auto_correction_detail)
SELECT batch_id, stg_id, 'stg_locations', 'N1-009-AUTO', 'info',
       'country code normalized to uppercase',
       country,
       TRUE,
       'auto-corrected: ' || country || ' → ' || UPPER(country)
FROM stg_locations_before_correction  -- snapshot avant correction
WHERE ...;
```

**Corrections automatiques autorisées :**

| Cas | Correction | Niveau |
|-----|-----------|--------|
| `country` en minuscules | UPPER() | Auto |
| `uom` avec espaces parasites | TRIM() | Auto |
| `status` avec casse mixte | LOWER() | Auto |
| `item_type` absent | Default `finished_good` | Auto (avec log) |
| `currency` absent | Default `USD` | Auto (avec log) |
| `lead_time_days` absent sur supplier | Default `14` | Auto (avec log) |

**Corrections INTERDITES (rejet strict) :**

| Cas | Raison |
|-----|--------|
| `external_id` absent | Impossible de déduire une clé métier |
| `quantity` non numérique | Risque de perte de données silencieuse |
| Date invalide | Aucune substitution raisonnable |
| Référence inconnue (FK) | Données orphelines = garbage |

### 3.4 Seuils d'acceptation du batch

```sql
-- Logique de vérification des seuils avant promotion
-- Exécutée après le pipeline DQ complet

WITH batch_stats AS (
    SELECT
        b.batch_id,
        b.entity_type,
        b.total_rows,
        b.rows_invalid,
        b.error_rate,
        -- Compter les erreurs bloquantes
        COUNT(CASE WHEN dqi.severity = 'blocking' THEN 1 END) AS blocking_issues,
        -- Vérifier si des règles critiques ont été déclenchées
        COUNT(CASE WHEN dqi.rule_code IN ('N2-001', 'N2-002') THEN 1 END) AS referential_failures
    FROM stg_import_batches b
    LEFT JOIN data_quality_issues dqi ON dqi.batch_id = b.batch_id
    WHERE b.batch_id = :batch_id
    GROUP BY b.batch_id, b.entity_type, b.total_rows, b.rows_invalid, b.error_rate
)
UPDATE stg_import_batches
SET batch_status = CASE
    -- Blocage si > 20% d'erreurs sur les flux master data
    WHEN entity_type IN ('items', 'locations', 'suppliers')
         AND error_rate > 20 THEN 'blocked'
    -- Blocage si > 10% d'erreurs sur les flux transactionnels
    WHEN entity_type IN ('purchase_orders', 'customer_orders', 'forecasts')
         AND error_rate > 10 THEN 'blocked'
    -- Blocage si > 5% d'erreurs sur on-hand (full replace = très sensible)
    WHEN entity_type = 'on_hand' AND error_rate > 5 THEN 'blocked'
    -- Accepté : promotion possible
    ELSE 'dq_complete'
END,
blocking_reason = CASE
    WHEN error_rate > 20 AND entity_type IN ('items', 'locations', 'suppliers')
    THEN 'Error rate ' || error_rate || '% exceeds 20% threshold for master data'
    WHEN error_rate > 10 AND entity_type IN ('purchase_orders', 'customer_orders', 'forecasts')
    THEN 'Error rate ' || error_rate || '% exceeds 10% threshold for transactional data'
    WHEN error_rate > 5 AND entity_type = 'on_hand'
    THEN 'Error rate ' || error_rate || '% exceeds 5% threshold for on-hand full replace'
    ELSE NULL
END
FROM batch_stats
WHERE stg_import_batches.batch_id = batch_stats.batch_id;
```

**Tableau des seuils :**

| Flux | Seuil blocage | Justification |
|------|--------------|---------------|
| `items`, `locations`, `suppliers` | > 20% | Master data : plus tolérant (imports initiaux souvent imparfaits) |
| `purchase_orders`, `customer_orders` | > 10% | Transactionnel : moins tolérant |
| `forecasts` | > 10% | Full replace : les erreurs propagent |
| `on_hand` | > 5% | Full replace critique : stock = source de vérité |
| `transfers`, `work_orders` | > 10% | Transactionnel standard |

### 3.5 Workflow de résolution

```
Batch 'blocked' ou lignes 'invalid'
        │
        ▼
┌───────────────────────────────────┐
│  Notification automatique         │
│  → data steward / intégrateur    │
│  → email/webhook avec détail     │
│  → lien vers UI de résolution    │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  Interface de résolution          │
│  GET /v1/imports/{batch_id}/issues│
│  → liste issues par sévérité     │
│  → détail raw_value + suggestion  │
└───────────────────────────────────┘
        │
        ├── Correction côté source → re-upload du fichier corrigé
        │   POST /v1/import/{type} (nouveau batch_id)
        │
        ├── Override manuel d'une issue
        │   PATCH /v1/imports/issues/{issue_id}
        │   { "resolution_status": "overridden", "resolution_note": "ERP bug, valeur correcte" }
        │
        └── Forcer la promotion malgré les erreurs (admin only)
            POST /v1/imports/{batch_id}/force-promote
            { "justification": "Known data gap, ops team notified" }
            → Crée un audit log avec l'identité du décideur
```

---

## 4. Historisation et audit (SCDs)

### 4.1 Stratégie SCD par champ

Les master data Ootils suivent des **SCDs mixtes** : Type 1 pour les champs opérationnels courants, Type 2 pour les champs qui impactent les calculs historiques.

| Entité | Champ | SCD Type | Justification |
|--------|-------|----------|---------------|
| `items` | `name` | Type 1 | Libellé : correction fréquente, pas d'impact historique |
| `items` | `item_type` | **Type 2** | Changement de type = reclassification métier critique |
| `items` | `uom` | **Type 2** | Changement UOM = recalcul de tous les historiques |
| `items` | `status` | Type 1 | Statut courant suffit |
| `locations` | `name` | Type 1 | Cosmétique |
| `locations` | `location_type` | **Type 2** | Reclassification réseau = impact graphe |
| `locations` | `timezone` | Type 1 | Rarement impactant |
| `suppliers` | `lead_time_days` | **Type 2** | Impacte tous les calculs de dates de dispo |
| `suppliers` | `reliability_score` | **Type 2** | Impacte les simulations de risque |
| `suppliers` | `status` | Type 1 | Statut courant |
| `item_location_policies` | `safety_stock_qty` | **Type 2** | Change le niveau de risque historique |
| `item_location_policies` | `replenishment_type` | **Type 2** | Changement de politique = rupture de logique |
| `supplier_items` | `unit_cost` | **Type 2** | Traçabilité prix pour costing |
| `supplier_items` | `lead_time_days` | **Type 2** | Voir suppliers |

### 4.2 Implémentation SCD Type 2

```sql
-- Extension des tables master data pour SCD Type 2
-- Pattern : colonnes valid_from / valid_to / is_current sur chaque entité

-- Exemple sur items (les autres tables suivent le même pattern)
ALTER TABLE items
    ADD COLUMN IF NOT EXISTS valid_from     DATE        NOT NULL DEFAULT CURRENT_DATE,
    ADD COLUMN IF NOT EXISTS valid_to       DATE,                   -- NULL = enregistrement courant
    ADD COLUMN IF NOT EXISTS is_current     BOOLEAN     NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS version        INTEGER     NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS superseded_by  UUID        REFERENCES items(item_id);

-- Index pour accéder à la version courante
CREATE INDEX IF NOT EXISTS idx_items_current ON items (external_id, is_current) WHERE is_current = TRUE;

-- Fonction de mise à jour SCD Type 2 pour items
CREATE OR REPLACE FUNCTION upsert_item_scd2(
    p_external_id   TEXT,
    p_name          TEXT,
    p_item_type     TEXT,
    p_uom           TEXT,
    p_status        TEXT,
    p_batch_id      UUID
) RETURNS UUID AS $$
DECLARE
    v_existing_id   UUID;
    v_new_id        UUID := gen_random_uuid();
    v_needs_scd2    BOOLEAN := FALSE;
    v_existing_row  items%ROWTYPE;
BEGIN
    -- Chercher la version courante
    SELECT * INTO v_existing_row
    FROM items
    WHERE external_id = p_external_id AND is_current = TRUE
    FOR UPDATE;

    IF NOT FOUND THEN
        -- Premier import : INSERT simple
        INSERT INTO items (item_id, external_id, name, item_type, uom, status, valid_from, is_current, version)
        VALUES (v_new_id, p_external_id, p_name, p_item_type, p_uom, p_status, CURRENT_DATE, TRUE, 1);
        RETURN v_new_id;
    END IF;

    v_existing_id := v_existing_row.item_id;

    -- Vérifier si un champ SCD Type 2 a changé
    IF (p_item_type IS NOT NULL AND p_item_type <> v_existing_row.item_type)
    OR (p_uom IS NOT NULL AND p_uom <> v_existing_row.uom) THEN
        v_needs_scd2 := TRUE;
    END IF;

    IF v_needs_scd2 THEN
        -- Fermer la version courante
        UPDATE items
        SET valid_to = CURRENT_DATE - 1,
            is_current = FALSE,
            superseded_by = v_new_id,
            updated_at = now()
        WHERE item_id = v_existing_id;

        -- Créer la nouvelle version
        INSERT INTO items (item_id, external_id, name, item_type, uom, status,
                           valid_from, is_current, version)
        VALUES (v_new_id, p_external_id,
                COALESCE(p_name, v_existing_row.name),
                COALESCE(p_item_type, v_existing_row.item_type),
                COALESCE(p_uom, v_existing_row.uom),
                COALESCE(p_status, v_existing_row.status),
                CURRENT_DATE, TRUE,
                v_existing_row.version + 1);

        -- Audit log
        INSERT INTO master_data_audit_log (entity_type, entity_id, external_id, change_type,
            field_changed, old_value, new_value, batch_id)
        VALUES ('item', v_new_id, p_external_id, 'scd2_version',
                CASE WHEN p_item_type <> v_existing_row.item_type THEN 'item_type' ELSE 'uom' END,
                CASE WHEN p_item_type <> v_existing_row.item_type THEN v_existing_row.item_type::TEXT ELSE v_existing_row.uom END,
                CASE WHEN p_item_type <> v_existing_row.item_type THEN p_item_type ELSE p_uom END,
                p_batch_id);

        RETURN v_new_id;
    ELSE
        -- Mise à jour Type 1 (champs cosmétiques)
        UPDATE items
        SET name = COALESCE(p_name, name),
            status = COALESCE(p_status, status),
            updated_at = now()
        WHERE item_id = v_existing_id;

        -- Audit log Type 1
        IF p_name IS NOT NULL AND p_name <> v_existing_row.name THEN
            INSERT INTO master_data_audit_log (entity_type, entity_id, external_id, change_type,
                field_changed, old_value, new_value, batch_id)
            VALUES ('item', v_existing_id, p_external_id, 'type1_update',
                    'name', v_existing_row.name, p_name, p_batch_id);
        END IF;

        RETURN v_existing_id;
    END IF;
END;
$$ LANGUAGE plpgsql;
```

### 4.3 Table d'audit log

```sql
CREATE TABLE IF NOT EXISTS master_data_audit_log (
    audit_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type     TEXT        NOT NULL,           -- 'item', 'location', 'supplier', etc.
    entity_id       UUID        NOT NULL,           -- UUID de l'enregistrement modifié (nouvelle version)
    external_id     TEXT        NOT NULL,           -- Code métier ERP
    change_type     TEXT        NOT NULL
                    CHECK (change_type IN ('insert', 'type1_update', 'scd2_version',
                                           'soft_delete', 'reactivation', 'override')),
    field_changed   TEXT,                           -- Colonne modifiée
    old_value       TEXT,
    new_value       TEXT,
    batch_id        UUID        REFERENCES stg_import_batches(batch_id),
    changed_by      TEXT        NOT NULL DEFAULT 'import_pipeline',  -- 'import_pipeline', 'api', 'user:xxx'
    changed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason          TEXT                            -- Optionnel : justification métier
);

CREATE INDEX IF NOT EXISTS idx_audit_entity ON master_data_audit_log (entity_type, external_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_batch ON master_data_audit_log (batch_id);
```

### 4.4 Traçabilité source → staging → production

```sql
-- Vue de traçabilité complète (exemple pour items)
CREATE OR REPLACE VIEW v_item_import_lineage AS
SELECT
    i.external_id,
    i.item_id,
    i.version,
    i.item_type,
    i.uom,
    i.valid_from,
    i.valid_to,
    i.is_current,
    si.stg_id,
    si.batch_id,
    si.raw_content,
    si.imported_at,
    b.source_system,
    b.filename,
    b.initiated_by,
    b.created_at AS batch_created_at,
    (SELECT COUNT(*) FROM data_quality_issues dqi
     WHERE dqi.stg_id = si.stg_id) AS dq_issues_count
FROM items i
JOIN stg_items si ON si.production_id = i.item_id
JOIN stg_import_batches b ON b.batch_id = si.batch_id;
```

---

## 5. Détection de dérives statistiques

### 5.1 Architecture de détection

La détection de dérives s'intercale entre le **Niveau 3 DQ** et la **promotion** en tables production. Elle utilise les statistiques des imports précédents pour détecter des anomalies qui seraient valides individuellement mais aberrantes en contexte.

```
Données DQ-validées (stg_status = 'valid')
        │
        ▼
[DRIFT DETECTION ENGINE]
        │
        ├── Calcul statistiques sur le batch courant
        ├── Comparaison avec baseline historique
        ├── Détection dépassement de seuils adaptatifs
        │
        ├── Anomalie détectée → stg_status = 'quarantined'
        │                     → alerte + issue dans data_quality_issues
        │
        └── OK → passage en promotion
```

### 5.2 Table des baselines statistiques

```sql
CREATE TABLE IF NOT EXISTS import_drift_baselines (
    baseline_id         UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type         TEXT        NOT NULL,       -- 'on_hand', 'purchase_orders', etc.
    metric_name         TEXT        NOT NULL,       -- 'quantity_mean', 'row_count', etc.
    item_external_id    TEXT,                       -- Scoped par item si applicable
    location_external_id TEXT,                     -- Scoped par location si applicable
    -- Statistiques calculées sur les N derniers imports
    sample_count        INTEGER     NOT NULL,       -- Nombre d'imports dans la baseline
    metric_mean         NUMERIC     NOT NULL,
    metric_stddev       NUMERIC     NOT NULL,
    metric_p5           NUMERIC,                   -- Percentile 5
    metric_p95          NUMERIC,                   -- Percentile 95
    metric_min          NUMERIC,
    metric_max          NUMERIC,
    -- Seuils adaptatifs (calculés automatiquement)
    alert_threshold_low  NUMERIC,                  -- mean - N*stddev
    alert_threshold_high NUMERIC,                  -- mean + N*stddev
    stddev_multiplier    NUMERIC    NOT NULL DEFAULT 3.0,  -- Sensibilité : 2=sensible, 3=normal, 4=tolérant
    -- Fenêtre de calcul
    computed_from       DATE        NOT NULL,       -- Début de la fenêtre
    computed_to         DATE        NOT NULL,       -- Fin de la fenêtre (généralement today)
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_current          BOOLEAN     NOT NULL DEFAULT TRUE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_drift_baseline_current
    ON import_drift_baselines (entity_type, metric_name, item_external_id, location_external_id)
    WHERE is_current = TRUE;
```

### 5.3 Règles de détection de dérives

```sql
-- ============================================================
-- DRIFT-001 : On-Hand quantity outlier par item × location
-- ============================================================
-- Détecte : stock soudainement 10x supérieur à la normale

WITH current_onhand AS (
    SELECT
        item_external_id,
        location_external_id,
        quantity::NUMERIC AS qty
    FROM stg_on_hand
    WHERE batch_id = :batch_id
      AND stg_status = 'valid'
      AND quantity ~ '^[0-9]+(\.[0-9]+)?$'
),
baselines AS (
    SELECT
        item_external_id,
        location_external_id,
        metric_mean,
        metric_stddev,
        alert_threshold_high,
        alert_threshold_low,
        stddev_multiplier
    FROM import_drift_baselines
    WHERE entity_type = 'on_hand'
      AND metric_name = 'quantity'
      AND is_current = TRUE
)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value, auto_corrected)
SELECT
    :batch_id,
    s.stg_id,
    'stg_on_hand',
    'DRIFT-001',
    'warning',
    format('On-hand quantity anomaly for %s @ %s: value=%s, baseline_mean=%s ± %s (threshold: [%s, %s])',
           oh.item_external_id, oh.location_external_id,
           oh.qty, b.metric_mean, b.metric_stddev * b.stddev_multiplier,
           b.alert_threshold_low, b.alert_threshold_high),
    oh.qty::TEXT,
    FALSE
FROM current_onhand oh
JOIN baselines b USING (item_external_id, location_external_id)
JOIN stg_on_hand s ON s.batch_id = :batch_id
    AND s.item_external_id = oh.item_external_id
    AND s.location_external_id = oh.location_external_id
WHERE oh.qty > b.alert_threshold_high
   OR oh.qty < b.alert_threshold_low;


-- ============================================================
-- DRIFT-002 : Row count anormal dans le batch
-- ============================================================
-- Détecte : un batch on-hand avec 50% moins de lignes que d'habitude
-- (signe d'export partiel ou d'incident ERP)

WITH batch_row_count AS (
    SELECT COUNT(*) AS current_count
    FROM stg_on_hand
    WHERE batch_id = :batch_id
),
baseline_rows AS (
    SELECT metric_mean, metric_stddev, alert_threshold_low
    FROM import_drift_baselines
    WHERE entity_type = 'on_hand'
      AND metric_name = 'row_count'
      AND is_current = TRUE
)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT
    :batch_id,
    :batch_id,  -- stg_id = batch_id pour les alertes de niveau batch
    'stg_import_batches',
    'DRIFT-002',
    'blocking',  -- Critique : si on a 50% moins de lignes, bloquer le full replace
    format('Batch row count anomaly: %s rows received, baseline mean=%s (threshold_low=%s). Possible incomplete ERP export.',
           brc.current_count, bl.metric_mean, bl.alert_threshold_low),
    brc.current_count::TEXT
FROM batch_row_count brc, baseline_rows bl
WHERE brc.current_count < bl.alert_threshold_low;


-- ============================================================
-- DRIFT-003 : Somme des quantités PO anormale
-- ============================================================
-- Détecte : total des PO confirmés × 10 ce mois = explosion de commande suspecte

WITH batch_po_sum AS (
    SELECT
        item_external_id,
        location_external_id,
        SUM(quantity::NUMERIC) AS total_qty
    FROM stg_purchase_orders
    WHERE batch_id = :batch_id
      AND status IN ('confirmed', 'pending')
      AND quantity ~ '^[0-9]+(\.[0-9]+)?$'
    GROUP BY item_external_id, location_external_id
),
baselines AS (
    SELECT item_external_id, location_external_id, alert_threshold_high, metric_mean
    FROM import_drift_baselines
    WHERE entity_type = 'purchase_orders'
      AND metric_name = 'total_qty_per_item_location'
      AND is_current = TRUE
)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT
    :batch_id,
    :batch_id,
    'stg_purchase_orders',
    'DRIFT-003',
    'warning',
    format('PO total quantity spike for %s @ %s: %s vs baseline %s',
           ps.item_external_id, ps.location_external_id,
           ps.total_qty, b.metric_mean),
    ps.total_qty::TEXT
FROM batch_po_sum ps
JOIN baselines b USING (item_external_id, location_external_id)
WHERE ps.total_qty > b.alert_threshold_high * 2;  -- × 2 pour éviter les faux positifs sur achats saisonniers


-- ============================================================
-- DRIFT-004 : Zero-quantity items soudainement nombreux
-- ============================================================
-- Détecte : 80% des lignes à quantity = 0 dans un batch on-hand
-- (export ERP tronqué ou bug transformation)

WITH zero_rate AS (
    SELECT
        COUNT(*) FILTER (WHERE quantity ~ '^[0-9]+(\.[0-9]+)?$' AND quantity::NUMERIC = 0) AS zeros,
        COUNT(*) AS total
    FROM stg_on_hand
    WHERE batch_id = :batch_id
)
INSERT INTO data_quality_issues (batch_id, stg_id, stg_table, rule_code, severity, description, raw_value)
SELECT
    :batch_id, :batch_id, 'stg_on_hand', 'DRIFT-004', 'blocking',
    format('Abnormal zero-quantity rate: %s%% of lines have qty=0 (threshold: 30%%)',
           ROUND(zeros::NUMERIC / total * 100, 1)),
    zeros || '/' || total
FROM zero_rate
WHERE total > 0 AND zeros::NUMERIC / total > 0.30;
```

### 5.4 Mise à jour des baselines (job périodique)

```sql
-- Recalcul des baselines toutes les nuits (ou après chaque import réussi)
-- Fenêtre glissante sur les 30 derniers imports valides

CREATE OR REPLACE PROCEDURE refresh_drift_baselines(p_entity_type TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    -- Marquer les baselines actuelles comme obsolètes
    UPDATE import_drift_baselines
    SET is_current = FALSE
    WHERE entity_type = p_entity_type AND is_current = TRUE;

    -- On-Hand : baseline quantité par item × location
    IF p_entity_type = 'on_hand' THEN
        INSERT INTO import_drift_baselines (entity_type, metric_name, item_external_id, location_external_id,
            sample_count, metric_mean, metric_stddev, metric_p5, metric_p95, metric_min, metric_max,
            alert_threshold_low, alert_threshold_high, stddev_multiplier,
            computed_from, computed_to, is_current)
        SELECT
            'on_hand',
            'quantity',
            item_external_id,
            location_external_id,
            COUNT(*)::INTEGER,
            AVG(qty),
            STDDEV(qty),
            PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY qty),
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY qty),
            MIN(qty),
            MAX(qty),
            -- Seuil bas : mean - 3σ (jamais < 0)
            GREATEST(0, AVG(qty) - 3 * STDDEV(qty)),
            -- Seuil haut : mean + 3σ
            AVG(qty) + 3 * STDDEV(qty),
            3.0,
            MIN(batch_date),
            MAX(batch_date),
            TRUE
        FROM (
            -- 30 derniers imports on-hand valides
            SELECT
                s.item_external_id,
                s.location_external_id,
                s.quantity::NUMERIC AS qty,
                b.created_at::DATE AS batch_date
            FROM stg_on_hand s
            JOIN stg_import_batches b ON b.batch_id = s.batch_id
            WHERE b.entity_type = 'on_hand'
              AND b.batch_status = 'complete'
              AND s.stg_status = 'promoted'
              AND b.created_at >= now() - INTERVAL '90 days'
        ) hist
        GROUP BY item_external_id, location_external_id
        HAVING COUNT(*) >= 5;  -- Minimum 5 imports pour calculer une baseline fiable
    END IF;

    -- Baselines row count
    INSERT INTO import_drift_baselines (entity_type, metric_name,
        sample_count, metric_mean, metric_stddev,
        alert_threshold_low, alert_threshold_high, stddev_multiplier,
        computed_from, computed_to, is_current)
    SELECT
        p_entity_type,
        'row_count',
        COUNT(*)::INTEGER,
        AVG(total_rows),
        STDDEV(total_rows),
        GREATEST(1, AVG(total_rows) - 2 * STDDEV(total_rows)),  -- 2σ pour row count (plus sensible)
        AVG(total_rows) + 2 * STDDEV(total_rows),
        2.0,
        MIN(created_at::DATE),
        MAX(created_at::DATE),
        TRUE
    FROM stg_import_batches
    WHERE entity_type = p_entity_type
      AND batch_status = 'complete'
      AND created_at >= now() - INTERVAL '90 days';
END;
$$;
```

---

## 6. Recommandations architecturales — Migration 003+

### 6.1 Ce qui doit être créé dès maintenant

**Migration 003 — Périmètre minimal obligatoire :**

```sql
-- ====================================================================
-- MIGRATION 003 — Data Engineering Foundation
-- ====================================================================

BEGIN;

-- 1. Ajouter external_id sur les tables existantes
ALTER TABLE items
    ADD COLUMN IF NOT EXISTS external_id    TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS valid_from     DATE NOT NULL DEFAULT CURRENT_DATE,
    ADD COLUMN IF NOT EXISTS valid_to       DATE,
    ADD COLUMN IF NOT EXISTS is_current     BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS version        INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS superseded_by  UUID REFERENCES items(item_id);

ALTER TABLE locations
    ADD COLUMN IF NOT EXISTS external_id    TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS valid_from     DATE NOT NULL DEFAULT CURRENT_DATE,
    ADD COLUMN IF NOT EXISTS valid_to       DATE,
    ADD COLUMN IF NOT EXISTS is_current     BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS version        INTEGER NOT NULL DEFAULT 1;

-- Backfill : utiliser item_id comme external_id temporaire pour les données existantes
UPDATE items SET external_id = item_id::TEXT WHERE external_id IS NULL;
UPDATE locations SET external_id = location_id::TEXT WHERE external_id IS NULL;

ALTER TABLE items ALTER COLUMN external_id SET NOT NULL;
ALTER TABLE locations ALTER COLUMN external_id SET NOT NULL;

-- 2. Tables master data (voir schémas sections 6.1, 7.1, 8.1 de SPEC-IMPORT-STATIC)
CREATE TABLE IF NOT EXISTS suppliers ( ... );  -- Schéma complet dans SPEC-IMPORT-STATIC §6.1
CREATE TABLE IF NOT EXISTS supplier_items ( ... );   -- §7.1
CREATE TABLE IF NOT EXISTS item_location_policies ( ... );  -- §8.1

-- 3. Staging zone complète
-- (tous les CREATE TABLE stg_* de la section 1.3 de ce document)

-- 4. Infrastructure DQ
CREATE TABLE IF NOT EXISTS data_quality_issues ( ... );   -- Section 3.1
CREATE TABLE IF NOT EXISTS master_data_audit_log ( ... ); -- Section 4.3
CREATE TABLE IF NOT EXISTS import_drift_baselines ( ... ); -- Section 5.2

-- 5. Index critiques
CREATE INDEX IF NOT EXISTS idx_items_current ON items (external_id) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_items_external ON items (external_id);
CREATE INDEX IF NOT EXISTS idx_locations_external ON locations (external_id);

COMMIT;
```

### 6.2 Points ouverts issus des specs à résoudre avant migration 003

| # | Point | Spec source | Recommandation |
|---|-------|-------------|---------------|
| P1 | `external_id` sur items/locations | SPEC-STATIC §11 P1 | **Faire maintenant** — backfill avec item_id::TEXT, NOT NULL après |
| P2 | Import asynchrone au-delà de 10k lignes | SPEC-STATIC §11 P5 | Job table `import_jobs` + polling endpoint. À concevoir avant prod |
| P3 | Multi-tenant `org_id` scoping | SPEC-STATIC §11 P8 | Ajouter `org_id UUID` sur TOUTES les tables (items, locations, stg_*, etc.) si multi-tenant prévu. Rétrofit coûteux |
| P4 | Forecasts full replace : scoped par source ou global | SPEC-DYNAMIC §10 #6 | **Scoped par (item × location × source)** — permet coexistence stat + budget |
| P5 | `on_hand_update` vs `onhand_updated` | SPEC-DYNAMIC §10 #2 | Canonique = `onhand_updated`, déjà dans la contrainte CHECK DB |
| P6 | PO `partially_received` : champ `received_quantity` | SPEC-DYNAMIC §10 #9 | Ajouter `received_quantity TEXT` dans `stg_purchase_orders` et `NUMERIC` en prod |
| P7 | Endpoint lookup `GET /v1/items?external_id=...` | SPEC-STATIC §11 P6 | **Obligatoire** pour les intégrations — à créer sprint 2 |
| P8 | Staging On-Hand | Non couvert dans specs | Ajouter `stg_on_hand` (identique aux autres stg_*) |

### 6.3 Catalogue complet des règles DQ (référence)

| Code | Niveau | Flux | Sévérité | Description |
|------|--------|------|----------|-------------|
| N1-001 | 1 | items | blocking | external_id non null/vide |
| N1-002 | 1 | items | blocking | name non null/vide |
| N1-003 | 1 | suppliers | blocking | lead_time_days parseable |
| N1-004 | 1 | PO | blocking | expected_date format valide |
| N1-005 | 1 | PO/CO/WO/ON | blocking | quantity parseable |
| N1-006 | 1 | tous | blocking | external_id unique dans batch |
| N1-007 | 1 | PO/CO | blocking | (po_number, line) unique dans batch |
| N1-008 | 1 | suppliers | blocking | reliability_score parseable et ∈ [0,1] |
| N1-009 | 1 | locations | warning | country = 2 chars alpha |
| N1-010 | 1 | forecasts | blocking | forecast_date format selon bucket_type |
| N1-011 | 1 | transfers | blocking | ship_date format valide |
| N1-012 | 1 | supplier_items | blocking | effective_end format si fourni |
| N2-001 | 2 | PO/CO/WO/TR/FC | blocking | item_external_id résolu en prod |
| N2-002 | 2 | PO/CO/WO/TR/FC/ON | blocking | location_external_id résolu en prod |
| N2-003 | 2 | PO | warning | supplier_external_id résolu si fourni |
| N2-004 | 2 | locations | blocking | Pas de cycle dans hiérarchie parent |
| N2-005 | 2 | supplier_items | blocking | supplier ET item existent en prod |
| N3-001 | 3 | suppliers | blocking | lead_time_days > 0 pour actifs |
| N3-002 | 3 | PO/CO/WO | blocking | quantity > 0 sauf annulations |
| N3-003 | 3 | PO | warning | expected_date >= today pour actifs |
| N3-004 | 3 | policies | blocking | safety_stock < max_stock (min_max) |
| N3-005 | 3 | PO/CO/WO | warning | UOM cohérent avec référentiel item |
| N3-006 | 3 | supplier_items | blocking | effective_end > effective_start |
| N3-007 | 3 | suppliers | blocking | reliability_score ∈ [0.0, 1.0] |
| N4-001 | 4 | transfers | blocking | from_location ≠ to_location |
| N4-002 | 4 | transfers | blocking | Les deux locations existent |
| N4-003 | 4 | transfers | warning | expected_receipt_date > ship_date |
| N4-004 | 4 | WO | blocking | end_date > start_date |
| N4-005 | 4 | PO | warning | PO pour item push sans forecast |
| DRIFT-001 | — | on_hand | warning | Quantité hors plage statistique |
| DRIFT-002 | — | on_hand | blocking | Row count anormal (export partiel) |
| DRIFT-003 | — | PO | warning | Somme quantités PO × 2 threshold |
| DRIFT-004 | — | on_hand | blocking | > 30% de lignes à quantité zéro |

### 6.4 Décision : partial_commit vs atomique

La spec actuelle laisse le choix via `partial_commit=true`. **Recommandation :**

- **Défaut : atomique** pour master data (items, locations, suppliers). Un seul enregistrement invalide ne doit pas corrompre le référentiel partiellement.
- **Défaut : partial_commit** pour données dynamiques (PO, CO, forecasts). En opérations, il vaut mieux importer 490/500 PO que bloquer les 500. Les 10 en erreur sont trackées et résolues.

```python
# Suggestion : paramètre par défaut dans l'API selon le type
IMPORT_DEFAULT_ATOMICITY = {
    'items': 'atomic',
    'locations': 'atomic',
    'suppliers': 'atomic',
    'supplier_items': 'atomic',
    'item_location_policies': 'atomic',
    'purchase_orders': 'partial_commit',
    'customer_orders': 'partial_commit',
    'forecasts': 'atomic',          # Full replace : atomique ou rien
    'work_orders': 'partial_commit',
    'transfers': 'partial_commit',
    'on_hand': 'atomic',            # Full replace : atomique ou rien
}
```

### 6.5 Gap critique identifié : `stg_on_hand` absente des specs

La spec SPEC-IMPORT-DYNAMIC décrit le flux On-Hand mais ne mentionne pas de table de staging dédiée. **À ajouter impérativement :**

```sql
CREATE TABLE IF NOT EXISTS stg_on_hand (
    stg_id                  UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id                UUID        NOT NULL,
    row_number              INTEGER     NOT NULL,
    source_system           TEXT        NOT NULL DEFAULT 'unknown',
    imported_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_content             TEXT,

    item_external_id        TEXT,
    location_external_id    TEXT,
    quantity                TEXT,
    uom                     TEXT,
    as_of_date              TEXT,
    lot_number              TEXT,

    stg_status              TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (stg_status IN ('pending', 'valid', 'invalid', 'promoted', 'quarantined')),
    dq_level_reached        SMALLINT,
    error_count             INTEGER     NOT NULL DEFAULT 0,
    promoted_at             TIMESTAMPTZ,
    production_id           UUID
);

CREATE INDEX IF NOT EXISTS idx_stg_onhand_batch ON stg_on_hand (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_onhand_keys ON stg_on_hand (item_external_id, location_external_id, as_of_date);
```

---

*Document produit le 2026-04-05 — Review Data Engineering, Ootils Core*  
*Branche : `review/import-data-engineering`*
