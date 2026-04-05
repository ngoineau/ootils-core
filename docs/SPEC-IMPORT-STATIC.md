# SPEC-IMPORT-STATIC — Spécification des Points d'Entrée Master Data

> **Version:** 1.0.0-draft  
> **Auteur:** Architecture Ootils  
> **Date:** 2026-04-05  
> **Statut:** DRAFT — en attente de revue produit

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Principes directeurs](#2-principes-directeurs)
3. [Ordre d'import recommandé](#3-ordre-dimport-recommandé)
4. [Entité : Item](#4-entité--item)
5. [Entité : Location](#5-entité--location)
6. [Entité : Supplier](#6-entité--supplier)
7. [Relation : SupplierItem (Item × Supplier)](#7-relation--supplieritem-item--supplier)
8. [Relation : ItemLocationPolicy (politiques par nœud)](#8-relation--itemlocationpolicy-politiques-par-nœud)
9. [Matrice de validation](#9-matrice-de-validation)
10. [Comportements sur conflit (Upsert Strategy)](#10-comportements-sur-conflit-upsert-strategy)
11. [Points ouverts](#11-points-ouverts)

---

## 1. Vue d'ensemble

### Périmètre master data

Le moteur Ootils repose sur un graphe de planification. Les **données statiques** sont les référentiels qui structurent ce graphe — elles ne varient pas au fil du temps de planification mais définissent les dimensions sur lesquelles les nœuds supply/demande sont rattachés.

| Entité | Table SQL | Statut |
|---|---|---|
| `Item` | `items` | ✅ Existe (migration 002) |
| `Location` | `locations` | ✅ Existe (migration 002) |
| `Supplier` | `suppliers` | ❌ **À créer** (migration 003) |
| `SupplierItem` | `supplier_items` | ❌ **À créer** (migration 003) |
| `ItemLocationPolicy` | `item_location_policies` | ❌ **À créer** (migration 003) |

### Schéma actuel

Les tables `items` et `locations` existent dans la migration 002 avec le schéma suivant :

```sql
-- items
item_id UUID PK | name TEXT | item_type TEXT (enum) | uom TEXT | status TEXT (enum)

-- locations  
location_id UUID PK | name TEXT | location_type TEXT (enum) | country TEXT | timezone TEXT
```

> **Gap critique :** Il n'existe pas de table `suppliers` dans le schéma actuel. Le modèle `Supplier` legacy (avec `lead_time_days`, `reliability_score`) vit dans du code Python non persisté en base. Cette spec inclut la création de cette table.

---

## 2. Principes directeurs

1. **UUID natif** — Tous les PKs sont des UUID v4 générés côté serveur. Les imports TSV utilisent des `external_id` (code métier) mappés à l'UUID interne.
2. **Upsert par défaut** — Le comportement standard est `INSERT ... ON CONFLICT DO UPDATE`. Aucun import ne rejette silencieusement des données existantes sans flag explicite.
3. **Validation avant persistence** — Les erreurs de validation sont retournées en bloc (pas row-by-row) avec références aux lignes TSV concernées.
4. **Idempotence** — Un import rejoué avec les mêmes données ne doit pas changer l'état du système.
5. **Traçabilité** — Chaque import génère un `ingestion_complete` event dans la table `events`.
6. **Format TSV universel** — Premier format car compatible avec tous les ERP/WMS (SAP, Oracle, JDE). Le JSON est accepté pour les intégrations API directes.

---

## 3. Ordre d'import recommandé

Les dépendances entre entités imposent l'ordre suivant :

```
1. Items          (aucune dépendance externe)
2. Locations      (aucune dépendance externe)
3. Suppliers      (aucune dépendance externe)
4. SupplierItems  (dépend de Items + Suppliers)
5. ItemLocationPolicies  (dépend de Items + Locations)
```

> En import batch (fichier ZIP ou multi-fichiers), l'API valide cet ordre et refuse de traiter les relations avant leurs référentiels parents.

---

## 4. Entité : Item

### 4.1 Champs

| Champ | Requis | Type | Contraintes | Défaut | Description |
|---|---|---|---|---|---|
| `external_id` | ✅ | `string` | max 255, unique dans le fichier | — | Code article ERP (ex: SKU, article SAP) |
| `name` | ✅ | `string` | max 500, non vide | — | Libellé article |
| `item_type` | ❌ | `enum` | `finished_good`, `component`, `raw_material`, `semi_finished` | `finished_good` | Type de l'article |
| `uom` | ❌ | `string` | max 20 (ISO 80000) | `EA` | Unité de mesure (EA, KG, L, M, BOX…) |
| `status` | ❌ | `enum` | `active`, `obsolete`, `phase_out` | `active` | Statut cycle de vie |

> **Note :** `item_id` (UUID interne) est généré par le serveur. L'`external_id` est le référentiel côté ERP. Il est stocké en colonne dédiée et indexé pour les lookups.

### 4.2 Format TSV

**Nom de fichier recommandé :** `items.tsv`

```tsv
external_id	name	item_type	uom	status
SKU-PUMP-01	Hydraulic Pump 12V	finished_good	EA	active
SKU-PUMP-02	Hydraulic Pump 24V	finished_good	EA	active
SKU-COMP-001	Impeller Blade	component	EA	active
SKU-RAW-STEEL	Steel Sheet 2mm	raw_material	KG	active
SKU-SEMI-BODY	Pump Body (machined)	semi_finished	EA	phase_out
SKU-BOX-STD	Standard Packaging Box	component	EA	active
SKU-FILTER-01	Oil Filter 10µm	component	EA	active
SKU-MOTOR-12V	DC Motor 12V 5A	component	EA	active
SKU-SEAL-KIT	Hydraulic Seal Kit	component	EA	obsolete
SKU-FG-ASSEMBLY	Pump Assembly Unit	finished_good	EA	active
```

**Colonnes obligatoires :** `external_id`, `name`  
**Colonnes facultatives :** toutes les autres (valeurs par défaut appliquées si absent)  
**Encodage :** UTF-8, séparateur `\t`, pas de guillemets requis

> **Pourquoi TSV plutôt que CSV ?**
> Les données supply chain contiennent fréquemment des virgules dans les libellés (noms d'articles, descriptions, adresses). Le tab comme séparateur élimine ce risque sans guillemets d'échappement. SAP exporte nativement en tab (transactions SE16, MB52, ME2M). L'extension `.tsv` est reconnue par Excel, pandas, et tous les outils ETL standard.

### 4.3 Endpoint API

#### `POST /v1/import/items`

**Content-Type supportés :**
- `multipart/form-data` (upload TSV)
- `application/json` (tableau JSON)

**Request — TSV upload :**
```http
POST /v1/import/items
Content-Type: multipart/form-data

file=<items.tsv>
conflict_strategy=upsert    # upsert | reject_duplicates
dry_run=false               # true = validation sans persistence
```

**Request — JSON direct :**
```http
POST /v1/import/items
Content-Type: application/json

{
  "items": [
    {
      "external_id": "SKU-PUMP-01",
      "name": "Hydraulic Pump 12V",
      "item_type": "finished_good",
      "uom": "EA",
      "status": "active"
    },
    {
      "external_id": "SKU-COMP-001",
      "name": "Impeller Blade",
      "item_type": "component"
    }
  ],
  "conflict_strategy": "upsert",
  "dry_run": false
}
```

**Response — succès (HTTP 200) :**
```json
{
  "status": "ok",
  "summary": {
    "total_rows": 10,
    "inserted": 8,
    "updated": 2,
    "skipped": 0,
    "errors": 0
  },
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "items": [
    {
      "external_id": "SKU-PUMP-01",
      "item_id": "a1b2c3d4-...",
      "action": "inserted"
    },
    {
      "external_id": "SKU-PUMP-02",
      "item_id": "b2c3d4e5-...",
      "action": "updated"
    }
  ]
}
```

**Response — erreurs de validation (HTTP 422) :**
```json
{
  "status": "validation_error",
  "errors": [
    {
      "row": 3,
      "external_id": "SKU-BAD",
      "field": "item_type",
      "message": "Invalid value 'spare_part'. Allowed: finished_good, component, raw_material, semi_finished"
    },
    {
      "row": 7,
      "external_id": null,
      "field": "name",
      "message": "Field 'name' is required and cannot be empty"
    }
  ],
  "hint": "Fix validation errors and retry. No data was persisted."
}
```

### 4.4 Règles de validation

| Règle | Niveau | Description |
|---|---|---|
| `external_id` non vide | ERROR | Bloque la ligne |
| `name` non vide | ERROR | Bloque la ligne |
| `item_type` dans l'enum | ERROR | Bloque la ligne |
| `uom` non vide si fourni | ERROR | Bloque la ligne |
| `status` dans l'enum | ERROR | Bloque la ligne |
| `external_id` unique dans le fichier | ERROR | Bloque l'import complet |
| `name` longueur ≤ 500 | WARNING | Log seulement, troncature à 500 |

---

## 5. Entité : Location

### 5.1 Champs

| Champ | Requis | Type | Contraintes | Défaut | Description |
|---|---|---|---|---|---|
| `external_id` | ✅ | `string` | max 255, unique | — | Code site ERP/WMS |
| `name` | ✅ | `string` | max 500, non vide | — | Libellé du site |
| `location_type` | ❌ | `enum` | `plant`, `dc`, `warehouse`, `supplier_virtual`, `customer_virtual` | `dc` | Type de nœud réseau |
| `country` | ❌ | `string` | ISO 3166-1 alpha-2 (2 chars) | `null` | Pays (FR, US, DE…) |
| `timezone` | ❌ | `string` | IANA tz (ex: Europe/Paris) | `null` | Fuseau horaire |
| `parent_external_id` | ❌ | `string` | Référence un `external_id` de location | `null` | Hiérarchie site (ex: entrepôt → DC) |

> **Hiérarchie :** La colonne `parent_external_id` permet de modéliser les sites enfants (un entrepôt rattaché à un DC). L'auto-référence est résolue après insertion de toutes les lignes du fichier (tri topologique interne).

### 5.2 Format TSV

**Nom de fichier recommandé :** `locations.tsv`

```tsv
external_id	name	location_type	country	timezone	parent_external_id
DC-ATL	Atlanta Distribution Center	dc	US	America/New_York	
DC-PARIS	Paris DC	dc	FR	Europe/Paris	
WH-ATL-01	Atlanta Warehouse 1	warehouse	US	America/New_York	DC-ATL
WH-ATL-02	Atlanta Warehouse 2	warehouse	US	America/New_York	DC-ATL
PLANT-LYON	Lyon Manufacturing Plant	plant	FR	Europe/Paris	
PLANT-DETROIT	Detroit Assembly Plant	plant	US	America/Detroit	
SUP-VIRT-ACME	ACME Corp (Virtual)	supplier_virtual	US		
CUST-VIRT-WALMART	Walmart (Virtual)	customer_virtual	US		
WH-PARIS-01	Paris Entrepôt Central	warehouse	FR	Europe/Paris	DC-PARIS
DC-CHICAGO	Chicago Distribution Hub	dc	US	America/Chicago	
```

### 5.3 Endpoint API

#### `POST /v1/import/locations`

**Request JSON :**
```http
POST /v1/import/locations
Content-Type: application/json

{
  "locations": [
    {
      "external_id": "DC-ATL",
      "name": "Atlanta Distribution Center",
      "location_type": "dc",
      "country": "US",
      "timezone": "America/New_York"
    },
    {
      "external_id": "WH-ATL-01",
      "name": "Atlanta Warehouse 1",
      "location_type": "warehouse",
      "country": "US",
      "timezone": "America/New_York",
      "parent_external_id": "DC-ATL"
    }
  ],
  "conflict_strategy": "upsert",
  "dry_run": false
}
```

**Response — succès (HTTP 200) :**
```json
{
  "status": "ok",
  "summary": {
    "total_rows": 10,
    "inserted": 9,
    "updated": 1,
    "skipped": 0,
    "errors": 0
  },
  "event_id": "660e8400-e29b-41d4-a716-446655440001",
  "locations": [
    {
      "external_id": "DC-ATL",
      "location_id": "c3d4e5f6-...",
      "action": "inserted"
    }
  ]
}
```

**Response — cycle détecté (HTTP 422) :**
```json
{
  "status": "validation_error",
  "errors": [
    {
      "row": 5,
      "external_id": "WH-ATL-01",
      "field": "parent_external_id",
      "message": "Circular reference detected: WH-ATL-01 → DC-ATL → WH-ATL-01"
    }
  ]
}
```

### 5.4 Règles de validation

| Règle | Niveau | Description |
|---|---|---|
| `external_id` non vide | ERROR | Bloque la ligne |
| `name` non vide | ERROR | Bloque la ligne |
| `location_type` dans l'enum | ERROR | Bloque la ligne |
| `country` = 2 chars ISO alpha-2 si fourni | WARNING | Log, valeur stockée telle quelle |
| `timezone` valide IANA si fourni | WARNING | Log, valeur stockée telle quelle |
| `parent_external_id` existe dans le fichier ou en base | ERROR | Bloque la ligne |
| Pas de cycle dans la hiérarchie parent | ERROR | Bloque l'import complet |
| `external_id` unique dans le fichier | ERROR | Bloque l'import complet |

---

## 6. Entité : Supplier

> **Migration requise :** Création de la table `suppliers` (migration 003).

### 6.1 Schéma SQL à créer (migration 003)

```sql
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id         UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id         TEXT        NOT NULL UNIQUE,    -- Code fournisseur ERP
    name                TEXT        NOT NULL,
    location_id         UUID        REFERENCES locations(location_id),  -- Site virtuel du fournisseur
    lead_time_days      NUMERIC     NOT NULL DEFAULT 14 CHECK (lead_time_days >= 0),
    reliability_score   NUMERIC     NOT NULL DEFAULT 1.0 CHECK (reliability_score BETWEEN 0 AND 1),
    moq                 NUMERIC,    -- Minimum Order Quantity
    unit_cost_override  NUMERIC,    -- Override global (généralement défini au niveau supplier_items)
    currency            TEXT        NOT NULL DEFAULT 'USD',
    status              TEXT        NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'inactive', 'approved', 'blocked')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_suppliers_external_id ON suppliers (external_id);
CREATE INDEX IF NOT EXISTS idx_suppliers_status ON suppliers (status) WHERE status = 'active';
```

### 6.2 Champs

| Champ | Requis | Type | Contraintes | Défaut | Description |
|---|---|---|---|---|---|
| `external_id` | ✅ | `string` | max 255, unique | — | Code fournisseur ERP (ex: VENDOR-001) |
| `name` | ✅ | `string` | max 500, non vide | — | Raison sociale |
| `location_external_id` | ❌ | `string` | Référence `locations.external_id` | `null` | Site virtuel fournisseur |
| `lead_time_days` | ❌ | `number` | ≥ 0, décimales acceptées | `14` | Délai d'approvisionnement en jours |
| `reliability_score` | ❌ | `number` | 0.0 – 1.0 | `1.0` | Score fiabilité livraison (1.0 = 100%) |
| `moq` | ❌ | `number` | ≥ 0 | `null` | Quantité minimum de commande |
| `unit_cost_override` | ❌ | `number` | ≥ 0 | `null` | Prix unitaire global (override par SupplierItem) |
| `currency` | ❌ | `string` | ISO 4217 (3 chars) | `USD` | Devise de facturation |
| `status` | ❌ | `enum` | `active`, `inactive`, `approved`, `blocked` | `active` | Statut fournisseur |

### 6.3 Format TSV

**Nom de fichier recommandé :** `suppliers.tsv`

```tsv
external_id	name	location_external_id	lead_time_days	reliability_score	moq	unit_cost_override	currency	status
VENDOR-ACME	ACME Corporation	SUP-VIRT-ACME	7	0.97	100		USD	active
VENDOR-GLOBEX	Globex Industries		14	0.92	50	45.00	USD	active
VENDOR-INITECH	Initech Parts Co		21	0.85	200		EUR	active
VENDOR-UMBRELLA	Umbrella Supply GmbH		10	0.99	10		EUR	approved
VENDOR-SOYLENT	Soylent Industrial		30	0.78	500	12.50	USD	active
VENDOR-VERIDIAN	Veridian Dynamics		5	0.95	25		USD	active
VENDOR-MASSIVE	Massive Dynamic		18	0.88	75	88.00	USD	inactive
VENDOR-WEYLAND	Weyland-Yutani Corp		45	0.70	1000		USD	blocked
VENDOR-PAWNEE	Pawnee Industrial		12	0.93	30		USD	active
VENDOR-STERLING	Sterling Cooper Supply		8	0.96	100	22.00	USD	active
```

### 6.4 Endpoint API

#### `POST /v1/import/suppliers`

**Request JSON :**
```http
POST /v1/import/suppliers
Content-Type: application/json

{
  "suppliers": [
    {
      "external_id": "VENDOR-ACME",
      "name": "ACME Corporation",
      "location_external_id": "SUP-VIRT-ACME",
      "lead_time_days": 7,
      "reliability_score": 0.97,
      "moq": 100,
      "currency": "USD",
      "status": "active"
    },
    {
      "external_id": "VENDOR-GLOBEX",
      "name": "Globex Industries",
      "lead_time_days": 14,
      "reliability_score": 0.92,
      "moq": 50,
      "unit_cost_override": 45.00
    }
  ],
  "conflict_strategy": "upsert",
  "dry_run": false
}
```

**Response — succès (HTTP 200) :**
```json
{
  "status": "ok",
  "summary": {
    "total_rows": 10,
    "inserted": 10,
    "updated": 0,
    "skipped": 0,
    "errors": 0
  },
  "event_id": "770e8400-e29b-41d4-a716-446655440002",
  "suppliers": [
    {
      "external_id": "VENDOR-ACME",
      "supplier_id": "d4e5f6g7-...",
      "action": "inserted"
    }
  ]
}
```

### 6.5 Règles de validation

| Règle | Niveau | Description |
|---|---|---|
| `external_id` non vide | ERROR | Bloque la ligne |
| `name` non vide | ERROR | Bloque la ligne |
| `lead_time_days` ≥ 0 | ERROR | Bloque la ligne |
| `reliability_score` dans [0.0, 1.0] | ERROR | Bloque la ligne |
| `moq` ≥ 0 si fourni | ERROR | Bloque la ligne |
| `currency` = 3 chars ISO 4217 si fourni | WARNING | Log, valeur stockée |
| `status` dans l'enum | ERROR | Bloque la ligne |
| `location_external_id` existe si fourni | ERROR | Bloque la ligne |
| `external_id` unique dans le fichier | ERROR | Bloque l'import complet |

---

## 7. Relation : SupplierItem (Item × Supplier)

> **Migration requise :** Création de la table `supplier_items` (migration 003).

Un `Item` peut être sourcé chez plusieurs `Supplier`. Cette table porte les conditions commerciales spécifiques à chaque couple (item, fournisseur).

### 7.1 Schéma SQL à créer (migration 003)

```sql
CREATE TABLE IF NOT EXISTS supplier_items (
    supplier_item_id    UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_id         UUID        NOT NULL REFERENCES suppliers(supplier_id),
    item_id             UUID        NOT NULL REFERENCES items(item_id),
    supplier_item_code  TEXT,       -- Code article chez le fournisseur (peut différer du external_id item)
    lead_time_days      NUMERIC     NOT NULL DEFAULT 14 CHECK (lead_time_days >= 0),  -- Override du lead time global supplier
    unit_cost           NUMERIC,    -- Prix unitaire pour ce couple (supplier, item)
    moq                 NUMERIC,    -- MOQ spécifique pour cet article
    lot_multiple        NUMERIC,    -- Lot multiple (ex: commande par multiple de 12)
    reliability_score   NUMERIC     CHECK (reliability_score BETWEEN 0 AND 1),
    preferred           BOOLEAN     NOT NULL DEFAULT FALSE,  -- Fournisseur préféré pour cet article
    effective_start     DATE,
    effective_end       DATE,
    status              TEXT        NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'inactive', 'approved')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (supplier_id, item_id)   -- Un seul enregistrement par couple (supplier, item)
);

CREATE INDEX IF NOT EXISTS idx_supplier_items_item ON supplier_items (item_id);
CREATE INDEX IF NOT EXISTS idx_supplier_items_supplier ON supplier_items (supplier_id);
CREATE INDEX IF NOT EXISTS idx_supplier_items_preferred ON supplier_items (item_id, preferred) WHERE preferred = TRUE;
```

### 7.2 Champs

| Champ | Requis | Type | Contraintes | Défaut | Description |
|---|---|---|---|---|---|
| `supplier_external_id` | ✅ | `string` | Référence `suppliers.external_id` | — | FK vers Supplier |
| `item_external_id` | ✅ | `string` | Référence `items.external_id` | — | FK vers Item |
| `supplier_item_code` | ❌ | `string` | max 255 | `null` | Référence article chez le fournisseur |
| `lead_time_days` | ❌ | `number` | ≥ 0 | hérité du Supplier | Délai spécifique à cet article |
| `unit_cost` | ❌ | `number` | ≥ 0 | `null` | Prix unitaire pour ce couple |
| `moq` | ❌ | `number` | ≥ 0 | hérité du Supplier | MOQ spécifique |
| `lot_multiple` | ❌ | `number` | ≥ 1 | `null` | Multiple de lotissement |
| `reliability_score` | ❌ | `number` | 0.0 – 1.0 | hérité du Supplier | Override fiabilité |
| `preferred` | ❌ | `boolean` | — | `false` | Fournisseur préféré pour cet article |
| `effective_start` | ❌ | `date` | ISO 8601 | `null` | Date début validité |
| `effective_end` | ❌ | `date` | ISO 8601, > effective_start | `null` | Date fin validité |
| `status` | ❌ | `enum` | `active`, `inactive`, `approved` | `active` | — |

### 7.3 Format TSV

**Nom de fichier recommandé :** `supplier_items.tsv`

```tsv
supplier_external_id	item_external_id	supplier_item_code	lead_time_days	unit_cost	moq	lot_multiple	reliability_score	preferred	effective_start	effective_end	status
VENDOR-ACME	SKU-PUMP-01	ACME-HYD-12V	7	125.00	50		0.97	true	2026-01-01		active
VENDOR-ACME	SKU-PUMP-02	ACME-HYD-24V	7	145.00	50		0.97	false	2026-01-01		active
VENDOR-GLOBEX	SKU-PUMP-01	GLX-P12V	14	118.00	100	10	0.92	false	2026-01-01		active
VENDOR-ACME	SKU-COMP-001	ACME-IMP-001	5	8.50	500	100	0.99	true	2026-01-01		active
VENDOR-UMBRELLA	SKU-COMP-001	UMB-BLADE-01	10	9.00	200	50	0.99	false	2026-01-01		active
VENDOR-VERIDIAN	SKU-MOTOR-12V	VD-DCM-5A	5	32.00	25		0.95	true	2026-01-01		active
VENDOR-GLOBEX	SKU-FILTER-01	GLX-F10U	14	4.50	1000	100	0.92	true	2026-01-01		active
VENDOR-STERLING	SKU-SEAL-KIT	SCS-SK-HYD	8	22.00	100		0.96	true	2026-01-01		active
VENDOR-ACME	SKU-FG-ASSEMBLY		14	280.00	10		0.97	false	2026-01-01	2026-12-31	active
VENDOR-INITECH	SKU-RAW-STEEL	ITC-SS2MM	21	3.20	500	100	0.85	true	2026-01-01		active
```

### 7.4 Endpoint API

#### `POST /v1/import/supplier-items`

**Request JSON :**
```http
POST /v1/import/supplier-items
Content-Type: application/json

{
  "supplier_items": [
    {
      "supplier_external_id": "VENDOR-ACME",
      "item_external_id": "SKU-PUMP-01",
      "supplier_item_code": "ACME-HYD-12V",
      "lead_time_days": 7,
      "unit_cost": 125.00,
      "moq": 50,
      "preferred": true,
      "effective_start": "2026-01-01",
      "status": "active"
    }
  ],
  "conflict_strategy": "upsert",
  "dry_run": false
}
```

**Response — succès (HTTP 200) :**
```json
{
  "status": "ok",
  "summary": {
    "total_rows": 10,
    "inserted": 9,
    "updated": 1,
    "skipped": 0,
    "errors": 0
  },
  "event_id": "880e8400-e29b-41d4-a716-446655440003"
}
```

### 7.5 Règles de validation

| Règle | Niveau | Description |
|---|---|---|
| `supplier_external_id` existe en base | ERROR | Bloque la ligne |
| `item_external_id` existe en base | ERROR | Bloque la ligne |
| `lead_time_days` ≥ 0 si fourni | ERROR | Bloque la ligne |
| `unit_cost` ≥ 0 si fourni | ERROR | Bloque la ligne |
| `effective_end` > `effective_start` si les deux fournis | ERROR | Bloque la ligne |
| `reliability_score` dans [0.0, 1.0] si fourni | ERROR | Bloque la ligne |
| `lot_multiple` ≥ 1 si fourni | ERROR | Bloque la ligne |
| Un seul `preferred=true` par `item_external_id` dans le fichier | WARNING | Log — dernier lu gagne |
| `(supplier_external_id, item_external_id)` unique dans le fichier | ERROR | Bloque l'import complet |

---

## 8. Relation : ItemLocationPolicy (politiques par nœud)

> **Migration requise :** Création de la table `item_location_policies` (migration 003).  
> **Note :** Table optionnelle au premier sprint — les politiques peuvent être fixées par défaut et affinées plus tard.

Définit les paramètres de planification pour un couple (item, location) : stock de sécurité, horizon de planification, politique de réapprovisionnement.

### 8.1 Schéma SQL à créer (migration 003)

```sql
CREATE TABLE IF NOT EXISTS item_location_policies (
    policy_id           UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id             UUID        NOT NULL REFERENCES items(item_id),
    location_id         UUID        NOT NULL REFERENCES locations(location_id),
    -- Inventory policy
    safety_stock_qty    NUMERIC     NOT NULL DEFAULT 0 CHECK (safety_stock_qty >= 0),
    safety_stock_days   NUMERIC,    -- Alternative: jours de couverture (mutuellement exclusif avec qty)
    reorder_point       NUMERIC,    -- Point de déclenchement commande
    -- Replenishment policy
    replenishment_type  TEXT        NOT NULL DEFAULT 'eoq'
                        CHECK (replenishment_type IN ('eoq', 'fixed_qty', 'min_max', 'jit', 'manual')),
    fixed_order_qty     NUMERIC,    -- Pour replenishment_type = 'fixed_qty'
    min_stock           NUMERIC,    -- Pour replenishment_type = 'min_max'
    max_stock           NUMERIC,    -- Pour replenishment_type = 'min_max'
    -- Planning horizon
    planning_horizon_days INTEGER   NOT NULL DEFAULT 180,
    -- Sourcing
    preferred_supplier_id UUID      REFERENCES suppliers(supplier_id),
    -- Validity
    effective_start     DATE,
    effective_end       DATE,
    status              TEXT        NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'inactive')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (item_id, location_id)  -- Une seule politique active par (item, location)
);
```

### 8.2 Format TSV

**Nom de fichier recommandé :** `item_location_policies.tsv`

```tsv
item_external_id	location_external_id	safety_stock_qty	safety_stock_days	reorder_point	replenishment_type	fixed_order_qty	min_stock	max_stock	planning_horizon_days	preferred_supplier_external_id	status
SKU-PUMP-01	DC-ATL	50		100	eoq				180	VENDOR-ACME	active
SKU-PUMP-01	DC-PARIS	30		60	fixed_qty	50			180	VENDOR-ACME	active
SKU-COMP-001	DC-ATL	500		1000	min_max		500	5000	90	VENDOR-ACME	active
SKU-MOTOR-12V	DC-ATL	25		50	eoq				180	VENDOR-VERIDIAN	active
SKU-FILTER-01	DC-ATL	1000		2000	fixed_qty	1000			90	VENDOR-GLOBEX	active
SKU-FG-ASSEMBLY	DC-ATL	5		10	jit				30		active
SKU-PUMP-02	DC-CHICAGO	20		40	eoq				180	VENDOR-ACME	active
SKU-RAW-STEEL	PLANT-LYON		15					20000	365	VENDOR-INITECH	active
SKU-SEAL-KIT	DC-ATL	200		400	fixed_qty	200			90	VENDOR-STERLING	active
SKU-SEMI-BODY	PLANT-DETROIT	100		200	min_max		100	1000	180		active
```

### 8.3 Endpoint API

#### `POST /v1/import/item-location-policies`

**Request JSON :**
```http
POST /v1/import/item-location-policies
Content-Type: application/json

{
  "policies": [
    {
      "item_external_id": "SKU-PUMP-01",
      "location_external_id": "DC-ATL",
      "safety_stock_qty": 50,
      "reorder_point": 100,
      "replenishment_type": "eoq",
      "planning_horizon_days": 180,
      "preferred_supplier_external_id": "VENDOR-ACME",
      "status": "active"
    }
  ],
  "conflict_strategy": "upsert",
  "dry_run": false
}
```

---

## 9. Matrice de validation

### Règles globales (tous endpoints)

| Règle | Action | HTTP Code |
|---|---|---|
| Fichier TSV vide ou 0 ligne de données | Reject tout | 400 |
| Header TSV manquant (colonnes obligatoires absentes) | Reject tout | 400 |
| Encodage non-UTF-8 | Reject tout | 400 |
| JSON malformé | Reject tout | 400 |
| `dry_run=true` — aucune persistence | Simulate, retourne le résultat simulé | 200 |
| `conflict_strategy` invalide | Reject tout | 400 |

### Niveaux de sévérité

| Niveau | Comportement |
|---|---|
| `ERROR` | Ligne rejetée. Si ≥1 erreur ERROR → aucune donnée persistée (atomique) |
| `WARNING` | Ligne acceptée avec log. La donnée est persistée avec la valeur normalisée ou tronquée |
| `INFO` | Log informatif seulement |

### Comportement atomique

**Par défaut :** L'import est **tout ou rien**. Si une ligne contient une erreur ERROR, aucune ligne du fichier n'est persistée. Le client reçoit la liste complète des erreurs et doit corriger puis renvoyer le fichier.

**Option `partial_commit=true` :** Les lignes valides sont persistées, les lignes en erreur sont retournées dans le rapport. À utiliser avec précaution (risque d'état partiel).

---

## 10. Comportements sur conflit (Upsert Strategy)

### `conflict_strategy: "upsert"` (défaut)

```
Si external_id existe déjà :
  → UPDATE tous les champs fournis (non-null dans la requête)
  → Champs non fournis : conservés tels quels (COALESCE / NULLIF)
  → updated_at = now()
Sinon :
  → INSERT avec gen_random_uuid() comme PK
```

**Implémentation SQL type :**
```sql
INSERT INTO items (item_id, external_id, name, item_type, uom, status)
VALUES (gen_random_uuid(), :external_id, :name, :item_type, :uom, :status)
ON CONFLICT (external_id) DO UPDATE SET
    name      = EXCLUDED.name,
    item_type = EXCLUDED.item_type,
    uom       = EXCLUDED.uom,
    status    = EXCLUDED.status,
    updated_at = now();
```

### `conflict_strategy: "reject_duplicates"`

```
Si external_id existe déjà :
  → ERROR sur la ligne concernée
  → Si mode atomique : tout l'import est rejeté
Sinon :
  → INSERT
```

### `conflict_strategy: "ignore_duplicates"`

```
Si external_id existe déjà :
  → Ligne ignorée (skipped), pas d'erreur
  → action = "skipped" dans le rapport
Sinon :
  → INSERT
```

### Hiérarchie de résolution des champs (upsert)

Pour les champs hiérarchiques (ex: `lead_time_days` dans `supplier_items` vs `suppliers`) :

```
supplier_items.lead_time_days  (le plus spécifique)
  → si NULL : hérite de suppliers.lead_time_days
  → si NULL : valeur par défaut système (14 jours)
```

Le moteur de planification lit toujours la valeur la plus spécifique disponible.

---

## 11. Points ouverts

| # | Question | Impact | Propriétaire suggéré |
|---|---|---|---|
| P1 | **Colonne `external_id` à ajouter à `items` et `locations`** — le schéma actuel n'a pas de colonne pour le code ERP. La PK UUID est interne. Faut-il ajouter `external_id TEXT UNIQUE` ou utiliser le `name` comme clé de déduplication ? | ⚠️ Breaking change si `name` est déjà utilisé comme clé dans les intégrations existantes | Architecture |
| P2 | **Migration 003 à créer** — `suppliers`, `supplier_items`, `item_location_policies` sont spécifiés mais pas encore en base. Décision : créer la migration maintenant ou attendre la démo M8 ? | Medium | Backend lead |
| P3 | **Authentification des endpoints import** — les endpoints `/v1/import/*` doivent-ils nécessiter un scope spécifique (`import:write`) distinct des scopes API ordinaires ? | Security | Platform |
| P4 | **Taille maximale des fichiers TSV** — pas de limite définie. À borner (ex: 50 000 lignes par fichier, 50 MB max) pour éviter les OOM en prod. | Reliability | DevOps |
| P5 | **Import asynchrone pour gros volumes** — au-delà d'un seuil (ex: 10 000 lignes), l'import devrait être traité en background avec un job_id retourné immédiatement et un webhook/polling pour le résultat. | Performance | Backend lead |
| P6 | **`item_id` sur les noeuds du graphe** — les nœuds `nodes` référencent `items.item_id` (UUID). Si un `Item` est importé via `external_id`, le mapping `external_id → item_id` doit être exposé via un endpoint `GET /v1/items?external_id=SKU-PUMP-01` pour que les intégrations puissent récupérer l'UUID interne. | Integration | Backend lead |
| P7 | **Gestion des suppressions** — cette spec couvre l'import (create/update). La désactivation (`status=obsolete`) est couverte via upsert. La suppression physique n'est pas spécifiée. Recommandation : soft-delete uniquement via `status`. | Data integrity | Product |
| P8 | **Multi-tenant / multi-org** — si Ootils devient multi-tenant, les `external_id` doivent être scopés par `org_id`. À anticiper dans le schéma maintenant. | Architecture | CTO |
| P9 | **Format d'import alternatif : Excel (.xlsx)** — les équipes planning travaillent souvent sous Excel. Vaut-il la peine d'accepter `.xlsx` en plus du TSV ? Coût : dépendance `openpyxl`. | UX / Adoption | Product |
| P10 | **Versioning des imports** — tracer quel import a créé/modifié quel enregistrement master data (au minimum stocker un `import_batch_id` sur chaque ligne). | Auditability | Architecture |

---

## Annexe A — Résumé des endpoints à créer

| Endpoint | Méthode | Entité | Statut |
|---|---|---|---|
| `/v1/import/items` | POST | Item | À créer |
| `/v1/import/locations` | POST | Location | À créer |
| `/v1/import/suppliers` | POST | Supplier | À créer |
| `/v1/import/supplier-items` | POST | SupplierItem | À créer |
| `/v1/import/item-location-policies` | POST | ItemLocationPolicy | À créer |
| `/v1/items` | GET | Item (lookup by external_id) | À créer |
| `/v1/locations` | GET | Location (lookup by external_id) | À créer |
| `/v1/suppliers` | GET | Supplier (lookup by external_id) | À créer |

## Annexe B — Migration 003 (squelette)

```sql
-- Migration 003: Master Data Relations
-- Ajouter external_id sur items et locations
ALTER TABLE items ADD COLUMN IF NOT EXISTS external_id TEXT UNIQUE;
ALTER TABLE locations ADD COLUMN IF NOT EXISTS external_id TEXT UNIQUE;

-- Backfill: utiliser le name comme external_id temporaire pour les données existantes
UPDATE items SET external_id = item_id::TEXT WHERE external_id IS NULL;
UPDATE locations SET external_id = location_id::TEXT WHERE external_id IS NULL;

-- Rendre external_id NOT NULL après backfill
ALTER TABLE items ALTER COLUMN external_id SET NOT NULL;
ALTER TABLE locations ALTER COLUMN external_id SET NOT NULL;

-- Créer les nouvelles tables
-- (voir schémas SQL dans les sections 6.1, 7.1, 8.1 ci-dessus)
```

---

*Document généré le 2026-04-05 — Architecture Ootils*
