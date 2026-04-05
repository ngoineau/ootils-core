# REVIEW-IMPORT-SC-EXPERT — Review Critique Architecture d'Import

> **Auteur :** Expert Intégration Supply Chain Planning (15 ans APS — Kinaxis, o9, Blue Yonder, SAP IBP)  
> **Date :** 2026-04-05  
> **Scope :** SPEC-IMPORT-STATIC, SPEC-IMPORT-DYNAMIC, SPEC-INTEGRATION-STRATEGY  
> **Verdict global :** Architecture solide pour un MVP, avec des lacunes critiques sur la gestion de la qualité de données qui — si non adressées maintenant — reproduiront exactement les erreurs de Kinaxis.

---

## Table des matières

1. [Verdict général](#1-verdict-général)
2. [Ce qui manque vs les meilleures pratiques APS](#2-ce-qui-manque-vs-les-meilleures-pratiques-aps)
3. [Risques opérationnels concrets](#3-risques-opérationnels-concrets)
4. [La couche Staging — ce qu'elle doit faire](#4-la-couche-staging--ce-quelle-doit-faire)
5. [La couche Processing — ce qu'elle doit faire](#5-la-couche-processing--ce-quelle-doit-faire)
6. [Données techniques supply chain — champs critiques](#6-données-techniques-supply-chain--champs-critiques)
7. [Ce que Kinaxis fait mal — et comment Ootils peut faire mieux](#7-ce-que-kinaxis-fait-mal--et-comment-ootils-peut-faire-mieux)
8. [Recommandations prioritaires P0/P1/P2](#8-recommandations-prioritaires-p0p1p2)

---

## 1. Verdict général

Les trois specs couvrent correctement les cas nominaux : schéma des entités, API endpoints, formats TSV, upsert strategy. C'est le travail d'une équipe qui a réfléchi aux intégrations ERP.

**Ce qui est bien :**
- La table `external_references` comme interface universelle ERP ↔ Ootils — c'est une bonne décision architecturale (DA-1)
- Le mapping déclaratif YAML par source — extensible et auditable (DA-2)
- Le choix TSV avec justification — pragmatique et aligné avec les réalités SAP
- La philosophie human-in-the-loop sur les outputs — non-négociable en supply chain
- La validation atomique (tout ou rien par défaut) — bonne base

**Ce qui manque et qui va casser en production :**
1. **Aucune couche Staging** — les données arrivent directement en traitement sans zone tampon
2. **Validation des données techniques insuffisante** — lead times à 0 acceptés, UOM non validés entre entités, pas de cohérence croisée
3. **Pas de détection de dégradation de données** — rien pour détecter qu'un import a silencieusement corrompu le référentiel
4. **Pas d'historique des master data** — impossible de savoir quand un lead time a changé, par qui, depuis quelle source
5. **Données manquantes traitées comme erreurs** — au lieu de gérer intelligemment les defaults et substitutions avec alertes
6. **Pas de BOM** — absent des specs, mais critique pour le manufacturing planning
7. **Pas de calendriers opérationnels** — les lead times sans calendriers sont inutilisables

---

## 2. Ce qui manque vs les meilleures pratiques APS

### 2.1 La couche Staging est absente

Tous les bons systèmes (SAP IBP, o9, Blue Yonder) séparent **la réception des données brutes** de **leur traitement**. Ootils reçoit les données et les traite immédiatement. Il n'y a aucune zone où stocker les données source telles qu'elles arrivent de l'ERP, avant transformation.

**Conséquence directe :** si un import se passe mal (transformation incorrecte, bug dans le mapping YAML), les données originales sont perdues. Il est impossible de rejouer proprement.

### 2.2 Pas d'historique des master data (SCD — Slowly Changing Dimensions)

SAP IBP gère les master data avec des dates de validité. Blue Yonder maintient un historique complet des changements. Kinaxis ne fait rien — et c'est son problème principal.

Les specs actuelles font des upserts purs : quand un `lead_time_days` passe de 14 à 7 jours sur un `SupplierItem`, l'ancienne valeur est écrasée. Il est impossible de savoir :
- Quelle était la valeur avant ?
- Quand a-t-elle changé ?
- Quel import a causé ce changement ?

Pour un moteur de planification, c'est inacceptable. Les projections utilisent les lead times. Si une rupture survient et qu'on veut comprendre pourquoi, l'absence d'historique rend le debug impossible.

### 2.3 Pas de gestion des calendriers opérationnels

Les lead times en `days` sont inutilisables sans calendriers. 14 jours calendaires ≠ 14 jours ouvrés. Un lead time de 3 jours qui inclut un week-end change la date de réception. 

Aucune des specs ne mentionne :
- Calendriers de travail par location (jours ouvrés, jours fériés)
- Calendriers fournisseurs (le fournisseur ne livre pas le vendredi)
- Calendriers clients (le client ne réceptionne pas certains jours)
- Shifts de production par usine

SAP IBP a une entité `WorkCalendar` complète. Blue Yonder aussi. C'est un prérequis pour que les dates de planification soient exploitables.

### 2.4 Pas de BOM (Bill of Materials)

La spec mentionne les Work Orders et les "consommations BOM" comme point ouvert (point 10 de SPEC-IMPORT-DYNAMIC), mais il n'y a aucun schéma pour importer une BOM.

Sans BOM, le moteur ne peut pas :
- Calculer les besoins en composants à partir des work orders
- Détecter les pénuries de composants qui bloquent la production
- Faire du MRP (Material Requirements Planning)

C'est une entité fondamentale du manufacturing planning. Si Ootils cible des industriels avec des usines, c'est P0.

### 2.5 Pas de routings de production

Les Work Orders ont `start_date` et `end_date` mais rien sur les ressources (machines, lignes de production). Sans capacité, le moteur peut planifier des work orders sur des équipements qui n'existent pas ou qui sont déjà surchargés. Les meilleurs APS (Kinaxis, o9) modélisent les resources/routings même au niveau basique.

### 2.6 Pas de détection de dégradation de données

Un bon système doit détecter quand un import diminue la qualité des données :
- Un champ autrefois renseigné devient vide (ex: lead_time_days passa de 14 à null → prend le default 14 silencieusement)
- Un ensemble d'enregistrements passe de 500 lignes à 50 (perte de 90% sans alerte)
- Des `supplier_items` actifs disparaissent du fichier sans être explicitement `cancelled`

Kinaxis ne détecte rien. Les données se dégradent silencieusement. Les planificateurs découvrent le problème 3 semaines plus tard en debuggant des projections aberrantes.

### 2.7 Pas de validation cross-entité lors de l'import

Les validations actuelles sont **intra-entité** (les champs d'une ligne sont valides entre eux). Il n'y a pas de validation **cross-entité** comme :
- Tous les items d'un WO ont-ils une politique de planification `ItemLocationPolicy` dans leur location de production ?
- Tous les `SupplierItem` actifs pour un item ont-ils un `preferred=true` ?
- Un item avec `replenishment_type=eoq` a-t-il les données suffisantes (unit_cost, demand historique) pour calculer l'EOQ ?

### 2.8 Multi-UOM absent

La spec définit un champ `uom` sur `items` (EA, KG, etc.) et sur les données transactionnelles. Mais il n'y a pas de table de **coefficients de conversion UOM**.

En réalité, un même item a souvent plusieurs UOM selon le contexte :
- UOM de stockage : EA (pièce)
- UOM de commande : BOX (boîte de 12)
- UOM de production : KG (poids pour les formules)

Si un PO arrive en BOX et que le stock est en EA, le moteur doit convertir. Sans table de conversion, la seule option est de tout rejeter si l'UOM ne correspond pas — ce qui arrive constamment avec des ERP multi-UOM comme SAP.

---

## 3. Risques opérationnels concrets

Ces risques sont tirés de situations réelles vécues en production sur des déploiements Kinaxis, o9, et IBP.

### Risque 1 : Lead times à zéro — le silent killer

**Scenario :** L'ERP SAP exporte les supplier_items via une extraction MARA/EINA. Pour certains articles, le champ `PLIFZ` (planned delivery time) est vide ou à 0 parce que l'article a été créé "temporairement" par un acheteur sans remplir tous les champs.

**Ce que la spec actuelle fait :** `lead_time_days` a un `DEFAULT 14 CHECK (lead_time_days >= 0)`. Un lead time à 0 passe la validation. Le moteur planifie une livraison le jour même. Les projections d'inventaire sont correctes en apparence mais les dates de réapprovisionnement sont toutes décalées.

**Impact :** Les alerts de rupture arrivent trop tard. Le planificateur passe les commandes en urgence. Surcoûts fret express, pénalités clients.

**Fix nécessaire :** Un lead time à 0 doit déclencher une alerte `WARNING` avec flag `requires_review`. Un seuil minimum configurable par `item_type` (ex: `raw_material` ≥ 1 jour). Pas de rejet automatique — mais visibilité obligatoire.

### Risque 2 : UOM incohérents entre PO et stock

**Scenario :** Le stock On-Hand pour SKU-001 est en EA (pièces). Le fournisseur livre en BOX de 24. Le PO arrive avec `uom=BOX`, `quantity=10`. Le moteur interprète 10 EA et non 240 EA.

**Impact :** Le projeté d'inventaire est 23x trop faible. Des alertes de rupture fictives se déclenchent. Des commandes d'urgence sont passées. Overstock massif.

**Ce que la spec actuelle fait :** Le champ UOM est validé comme string non-vide. Aucune cohérence entre l'UOM d'un PO et l'UOM de base de l'item n'est vérifiée.

**Fix nécessaire :** Table `uom_conversions` (voir section 6). Validation obligatoire : si l'UOM du PO ≠ UOM de base de l'item, une conversion doit exister. Sinon, rejet de la ligne avec `UOM_CONVERSION_MISSING`.

### Risque 3 : PO en doublon inter-imports

**Scenario :** L'ERP envoie un batch quotidien de POs en "Delta" (selon la spec). Un PO modifié dans l'ERP apparaît dans deux exports consécutifs avec des quantités différentes (bug d'extraction côté ERP). L'UPSERT sur `(po_number, line_number)` résout le doublon intra-fichier, mais si les deux exports arrivent dans la même fenêtre (retard SFTP + job planifié), deux imports traitent le même PO avec des states différents.

**Impact :** La quantité du PO oscille entre deux valeurs selon l'ordre d'exécution des imports. Le projeté d'inventaire est instable.

**Fix nécessaire :** La staging zone capture chaque import avec un timestamp. Si deux imports pour le même `po_number+line_number` arrivent à moins de N minutes d'intervalle avec des valeurs différentes, une alerte `CONCURRENCY_CONFLICT` est générée. Le dernier l'emporte, mais avec log explicite.

### Risque 4 : Forecasts non normalisés

**Scenario :** La spec accepte des forecasts en `day`, `week`, et `month`. Un client envoie des forecasts mensuels (2026-04) mais les work orders ont des `start_date` journaliers. Pour calculer si un WO du 15 avril peut être lancé, le moteur doit "désagréger" le forecast mensuel.

**Ce que la spec prévoit :** La clé de déduplication inclut `bucket_type`. Mais il n'y a pas de règle de désagrégation (uniform spread ? courbe historique ? peak en fin de mois ?).

**Impact :** Le moteur utilise implicitement une règle de désagrégation non documentée. Les planificateurs voient des projections qui ne correspondent pas à leur intuition. Perte de confiance dans l'outil.

**Fix nécessaire :** Documenter la règle de désagrégation par défaut (uniform spread = 1/N par jour). Permettre une override via `ItemLocationPolicy`. C'est un paramètre de planning fondamental.

### Risque 5 : Master data périmée silencieuse

**Scenario :** Un fournisseur change ses conditions en cours d'année : le `lead_time_days` passe de 14 à 30 jours. L'acheteur met à jour l'ERP. La nuit suivante, le batch import upserte le `SupplierItem` avec le nouveau lead time. Le moteur recalcule. 

Mais 3 mois plus tard, il y a un litige : "pourquoi ce PO a-t-il été passé si tôt ?" Il est impossible de prouver que le lead time était de 14 jours à l'époque où la décision a été prise.

**Fix nécessaire :** SCD Type 2 sur les données master critiques (voir section 4).

### Risque 6 : Items sans politique de planification

**Scenario :** 2000 items sont importés. `ItemLocationPolicy` est optionnelle selon la spec. 800 items n'ont pas de politique définie. Le moteur utilise des defaults (`safety_stock_qty=0`, `replenishment_type=eoq`). Un EOQ calculé sans `unit_cost` ni données de demand retourne une quantité arbitraire ou une division par zéro.

**Impact :** Des commandes de réapprovisionnement absurdes sont générées pour ces 800 items.

**Fix nécessaire :** Après chaque import d'items, vérifier que chaque item actif a une `ItemLocationPolicy` pour chaque location où il a du stock. Générer un rapport des "items sans politique" — pas un blocage, mais une visibilité obligatoire.

---

## 4. La couche Staging — ce qu'elle doit faire

### 4.1 Principe fondamental

La staging zone est un **miroir brut des données sources**. Rien n'est transformé, rien n'est validé métier. C'est un log immuable de ce que l'ERP a envoyé, avec horodatage.

**Règle d'or :** On ne perd jamais les données brutes. Si le processing échoue ou produit une erreur, on peut toujours rejouer depuis le staging.

### 4.2 Schéma de la staging zone

```sql
-- Table centrale : tout import arrive ici en premier
CREATE TABLE staging_batches (
    batch_id            UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system       TEXT        NOT NULL,           -- 'sap_ecc', 'dynamics', 'manual_upload', 'api'
    entity_type         TEXT        NOT NULL,           -- 'items', 'suppliers', 'purchase_orders', etc.
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    file_name           TEXT,                           -- Nom du fichier original si applicable
    file_hash           TEXT,                           -- SHA-256 du fichier reçu (idempotence)
    row_count           INTEGER,                        -- Nombre de lignes détectées
    raw_content         TEXT,                           -- Contenu brut UTF-8 (TSV ou JSON stringifié)
    content_type        TEXT        NOT NULL,           -- 'tsv' | 'json'
    status              TEXT        NOT NULL DEFAULT 'received'
                        CHECK (status IN ('received', 'processing', 'processed', 'failed', 'skipped_duplicate')),
    processing_started_at   TIMESTAMPTZ,
    processing_completed_at TIMESTAMPTZ,
    processing_result   JSONB,                          -- Summary du processing (inserted/updated/errors)
    triggered_by        TEXT,                           -- user_id ou system job name
    metadata            JSONB                           -- Champs libres : version ERP, org_id, etc.
);

-- Lignes individuelles du staging (optionnel mais recommandé pour debug)
CREATE TABLE staging_rows (
    row_id              UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            UUID        NOT NULL REFERENCES staging_batches(batch_id),
    row_number          INTEGER     NOT NULL,
    raw_data            JSONB       NOT NULL,           -- Ligne parsée en JSON brut (avant transformation)
    status              TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'accepted', 'rejected', 'warned')),
    validation_messages JSONB,                          -- Liste des erreurs/warnings détectés
    processed_entity_id UUID,                          -- UUID de l'entité créée/modifiée (après processing)
    processed_at        TIMESTAMPTZ,
    UNIQUE (batch_id, row_number)
);

-- Index pour les opérations courantes
CREATE INDEX idx_staging_batches_entity_source ON staging_batches (entity_type, source_system, received_at DESC);
CREATE INDEX idx_staging_batches_file_hash ON staging_batches (file_hash) WHERE file_hash IS NOT NULL;
CREATE INDEX idx_staging_rows_batch ON staging_rows (batch_id, status);
CREATE INDEX idx_staging_rows_entity ON staging_rows (processed_entity_id) WHERE processed_entity_id IS NOT NULL;
```

### 4.3 Règles d'acceptation des données brutes

**La staging accepte tout, avec conditions minimales :**

| Condition | Action si non respectée |
|-----------|------------------------|
| Fichier non vide | Reject `staging_batch.status = 'failed'`, pas de staging_rows créées |
| Encodage UTF-8 détectable | Tentative de re-encoding depuis latin-1/cp1252 → log WARNING, on continue |
| Header présent en ligne 1 | Reject `status = 'failed'` |
| Taille ≤ limite configurée (défaut 100MB) | Reject `status = 'failed'` |
| SHA-256 déjà vu dans les dernières 24h | `status = 'skipped_duplicate'` — idempotence garantie |

**Ce que la staging NE fait PAS :**
- Ne valide pas les valeurs métier (lead times, dates, quantités)
- Ne résout pas les external_ids
- Ne transforme pas les enums
- Ne rejette pas les lignes avec des champs vides

**Pourquoi ?** Parce que la staging doit capturer ce que l'ERP a vraiment envoyé, même si c'est imparfait. La validation métier est le rôle du processing.

### 4.4 Traçabilité source-à-cible

Chaque enregistrement dans la base Ootils doit porter une référence à son origine staging :

```sql
-- Sur toutes les tables master data et transactionnelles
ALTER TABLE items ADD COLUMN IF NOT EXISTS 
    last_import_batch_id UUID REFERENCES staging_batches(batch_id);

ALTER TABLE supplier_items ADD COLUMN IF NOT EXISTS 
    last_import_batch_id UUID REFERENCES staging_batches(batch_id);

-- (idem pour locations, suppliers, purchase_orders, etc.)
```

Cela permet de répondre à : "Quel import a mis ce lead_time_days à 0 ?" en une seule requête :

```sql
SELECT sb.received_at, sb.source_system, sb.file_name, sb.triggered_by
FROM supplier_items si
JOIN staging_batches sb ON si.last_import_batch_id = sb.batch_id
WHERE si.item_id = :item_id AND si.supplier_id = :supplier_id;
```

---

## 5. La couche Processing — ce qu'elle doit faire

### 5.1 Pipeline de validation — ordre et logique

Le processing lit les `staging_rows` en état `pending` et les passe à travers le pipeline suivant :

```
STAGING_ROW (raw_data JSON)
  │
  ├─ [ÉTAPE 1] Validation structure
  │    • Colonnes obligatoires présentes ?
  │    • Types corrects (nombre, date, string) ?
  │    • Longueurs max respectées ?
  │    ↳ Echec → status='rejected', message MISSING_FIELD / TYPE_ERROR
  │
  ├─ [ÉTAPE 2] Résolution des external_ids
  │    • external_id → UUID interne via external_references
  │    • Pour master data : non trouvé → création automatique (pré-enregistrement)
  │    • Pour transactionnel : non trouvé → REJECT avec ENTITY_NOT_FOUND
  │    ↳ Echec → status='rejected'
  │
  ├─ [ÉTAPE 3] Application du mapping YAML
  │    • Transformation des valeurs (enum_map, date format, normalisation string)
  │    • Valeurs non mappées → valeur 'default' du YAML, ou WARNING si absent
  │    ↳ Echec mapping → status='warned', valeur default appliquée + log
  │
  ├─ [ÉTAPE 4] Validation métier intra-ligne
  │    • lead_time_days > 0 ? (0 accepté mais WARNING 'LEAD_TIME_ZERO')
  │    • effective_end > effective_start ?
  │    • Quantités ≥ 0 ?
  │    • UOM cohérent avec l'item (lookup dans uom_conversions) ?
  │    • Dates dans un horizon raisonnable (pas en 1900, pas en 2099) ?
  │    ↳ Violations ERROR → status='rejected'
  │    ↳ Violations WARNING → status='warned', processing continue
  │
  ├─ [ÉTAPE 5] Validation cross-entité
  │    • Si ItemLocationPolicy : l'item existe avec status=active ?
  │    • Si SupplierItem preferred=true : existe-t-il déjà un preferred pour cet item×location ?
  │    • Si PO : le supplier a-t-il un SupplierItem actif pour cet item ?
  │    • Si WorkOrder : l'item a-t-il une ItemLocationPolicy dans la location de production ?
  │    ↳ Violations → WARNING (pas de reject, mais visible dans le rapport)
  │
  ├─ [ÉTAPE 6] Détection de dégradation de données
  │    • Comparaison avec la valeur actuelle en base
  │    • Champ critique passe de valeur renseignée à NULL/vide → WARNING 'DATA_REGRESSION'
  │    • Variation > seuil sur champ critique (ex: lead_time change > 50%) → WARNING 'ANOMALY_DETECTED'
  │    • Nombre de lignes du batch < 80% du dernier import de même type → WARNING 'VOLUME_DROP'
  │    ↳ Toutes violations → WARNING (jamais reject — mais dashboard de qualité)
  │
  └─ [ÉTAPE 7] Upsert en base + événements
       • INSERT ... ON CONFLICT DO UPDATE (selon conflict_strategy)
       • Champs modifiés → événements delta détectés
       • last_import_batch_id = staging_batch_id courant
       • updated_at = now()
       ↳ staging_row.status = 'accepted'
```

### 5.2 Gestion des données techniques manquantes

**Philosophie : defaults intelligents avec visibilité, pas de rejet silencieux.**

| Champ manquant | Comportement | Alerte générée |
|----------------|-------------|----------------|
| `lead_time_days` absent | Applique default du `replenishment_type` ou 14j | WARNING `DEFAULT_APPLIED` |
| `lead_time_days = 0` | Accepté, flag `requires_review = true` | WARNING `LEAD_TIME_ZERO` |
| `uom` absent | Hérite de l'item parent | INFO `UOM_INHERITED` |
| `safety_stock_qty` absent | Calculé si données dispo (demand history × coverage_days) | INFO `SAFETY_STOCK_COMPUTED` |
| `preferred` manquant avec plusieurs suppliers | Premier fournisseur actif = preferred par défaut | WARNING `PREFERRED_AUTO_SET` |
| `ItemLocationPolicy` absente pour item actif | Politique default créée automatiquement | WARNING `DEFAULT_POLICY_CREATED` |
| `reliability_score` absent | Default 1.0 appliqué | INFO `DEFAULT_APPLIED` |

**Règle de substitution vs rejet :**

- **Substitution autorisée** : champs de paramétrage où un default raisonnable existe et est documenté
- **Rejet obligatoire** : champs de référence (external_ids de FK), champs business-critical sans default possible (ex: `quantity` sur un PO)
- **Jamais de substitution silencieuse** : chaque substitution est loggée dans `staging_rows.validation_messages`

### 5.3 Validation croisée inter-entités — règles concrètes

```python
# Ces règles s'exécutent APRÈS l'import batch de chaque entité
# et génèrent des rapports dans une table data_quality_alerts

CROSS_ENTITY_RULES = [
    {
        "id": "CER-001",
        "name": "Item actif sans politique de planification",
        "query": """
            SELECT i.external_id, l.external_id as location_external_id
            FROM items i
            CROSS JOIN locations l
            INNER JOIN nodes n ON n.item_id = i.item_id AND n.location_id = l.location_id
            LEFT JOIN item_location_policies p ON p.item_id = i.item_id AND p.location_id = l.location_id
            WHERE i.status = 'active' AND p.policy_id IS NULL
        """,
        "severity": "WARNING",
        "message": "Item {item} dans location {location} sans politique de planification"
    },
    {
        "id": "CER-002",
        "name": "Item sans fournisseur préféré",
        "query": """
            SELECT i.external_id
            FROM items i
            WHERE i.status = 'active'
            AND i.item_type IN ('finished_good', 'component', 'raw_material')
            AND NOT EXISTS (
                SELECT 1 FROM supplier_items si 
                WHERE si.item_id = i.item_id AND si.preferred = true AND si.status = 'active'
            )
        """,
        "severity": "WARNING",
        "message": "Item {item} sans fournisseur préféré actif"
    },
    {
        "id": "CER-003",
        "name": "PO référençant un supplier inactif ou bloqué",
        "severity": "ERROR",
        "check": "purchase_orders.supplier_id → suppliers.status NOT IN ('active', 'approved')"
    },
    {
        "id": "CER-004",
        "name": "UOM PO ≠ UOM item sans conversion définie",
        "severity": "ERROR",
        "check": "purchase_orders.uom ≠ items.uom AND uom_conversions(from, to) IS NULL"
    },
    {
        "id": "CER-005",
        "name": "Stock On-Hand pour item obsolète",
        "severity": "WARNING",
        "check": "on_hand.quantity > 0 AND items.status = 'obsolete'"
    }
]
```

### 5.4 Table de qualité des données (dashboard de santé)

```sql
CREATE TABLE data_quality_alerts (
    alert_id            UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            UUID        REFERENCES staging_batches(batch_id),
    rule_id             TEXT        NOT NULL,           -- 'CER-001', 'LEAD_TIME_ZERO', etc.
    severity            TEXT        NOT NULL CHECK (severity IN ('ERROR', 'WARNING', 'INFO')),
    entity_type         TEXT        NOT NULL,
    entity_external_id  TEXT,
    message             TEXT        NOT NULL,
    context             JSONB,                          -- Données brutes qui ont déclenché l'alerte
    acknowledged        BOOLEAN     NOT NULL DEFAULT FALSE,
    acknowledged_by     TEXT,
    acknowledged_at     TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at         TIMESTAMPTZ                     -- NULL = alerte active
);

CREATE INDEX idx_dqa_severity_resolved ON data_quality_alerts (severity, resolved_at) 
    WHERE resolved_at IS NULL;
CREATE INDEX idx_dqa_entity ON data_quality_alerts (entity_type, entity_external_id);
```

Un endpoint `GET /v1/data-quality/alerts` expose ces alertes à l'UI. C'est le dashboard de santé des données — absent de toutes les APS du marché, et pourtant la fonctionnalité qui génère le plus de valeur en production.

---

## 6. Données techniques supply chain — champs critiques

### 6.1 Lead Times — ce qui doit être validé rigoureusement

Les lead times sont les paramètres les plus critiques du planning. Un lead time faux donne une date de commande fausse, qui donne une rupture ou un overstock.

**Structure recommandée pour les lead times (à ajouter à `SupplierItem` et `ItemLocationPolicy`) :**

```sql
-- Sur supplier_items : décomposer le lead time
lead_time_sourcing_days     NUMERIC NOT NULL DEFAULT 0,   -- Temps de traitement fournisseur
lead_time_manufacturing_days NUMERIC NOT NULL DEFAULT 0,  -- Si le fournisseur fabrique à la commande
lead_time_transit_days      NUMERIC NOT NULL DEFAULT 0,   -- Transit logistique
-- lead_time_days = sourcing + manufacturing + transit (calculé)

-- Sur item_location_policies : lead time de fabrication interne
manufacturing_lead_time_days NUMERIC DEFAULT NULL,        -- Si produit en interne
inspection_lead_time_days   NUMERIC NOT NULL DEFAULT 0,   -- Contrôle qualité à réception
```

**Règles de validation :**
- `lead_time_days = 0` → WARNING obligatoire (flag `requires_review`)
- `lead_time_days > 365` → WARNING `ANOMALY_LEAD_TIME_HIGH` (probablement une erreur de saisie)
- `lead_time_days` modifié de plus de 50% en un seul import → WARNING `ANOMALY_LEAD_TIME_CHANGE`
- Un item `finished_good` avec `lead_time_manufacturing_days = 0` dans une usine → WARNING

### 6.2 Safety Stock / Reorder Points / MOQ

```sql
-- Dans item_location_policies, renforcer avec :
safety_stock_method         TEXT CHECK (safety_stock_method IN 
                            ('fixed_qty', 'fixed_days', 'statistical', 'manual')),
safety_stock_service_level  NUMERIC CHECK (safety_stock_service_level BETWEEN 0 AND 1),
                            -- Pour méthode 'statistical' : ex 0.95 = 95% service level
demand_variability_cv       NUMERIC,  -- Coefficient de variation de la demande
lead_time_variability_days  NUMERIC,  -- Écart-type du lead time (pour calcul SS statistique)
```

**Règles :**
- `safety_stock_qty` ET `safety_stock_days` tous les deux renseignés → ERROR `AMBIGUOUS_SAFETY_STOCK`
- `reorder_point < safety_stock_qty` → ERROR (on passerait commande après avoir entamé le SS)
- `max_stock < min_stock` (pour min_max) → ERROR
- `fixed_order_qty < moq` → WARNING `ORDER_QTY_BELOW_MOQ`

### 6.3 Calendriers opérationnels — entité manquante à créer

```sql
CREATE TABLE work_calendars (
    calendar_id         UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id         TEXT        NOT NULL UNIQUE,
    name                TEXT        NOT NULL,
    timezone            TEXT        NOT NULL,           -- IANA timezone
    working_days        TEXT[]      NOT NULL DEFAULT ARRAY['mon','tue','wed','thu','fri'],
    calendar_type       TEXT        NOT NULL CHECK (calendar_type IN 
                        ('plant', 'supplier', 'customer', 'logistics')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE calendar_exceptions (
    exception_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    calendar_id         UUID        NOT NULL REFERENCES work_calendars(calendar_id),
    exception_date      DATE        NOT NULL,
    is_working_day      BOOLEAN     NOT NULL DEFAULT FALSE,  -- FALSE = jour férié, TRUE = jour travaillé exceptionnel
    description         TEXT,
    UNIQUE (calendar_id, exception_date)
);

-- Lier les calendriers aux entités
ALTER TABLE locations ADD COLUMN work_calendar_id UUID REFERENCES work_calendars(calendar_id);
ALTER TABLE suppliers ADD COLUMN work_calendar_id UUID REFERENCES work_calendars(calendar_id);
```

**Import TSV des exceptions :**
```tsv
calendar_external_id	exception_date	is_working_day	description
CAL-FRANCE	2026-05-01	false	Fête du Travail
CAL-FRANCE	2026-05-08	false	Victoire 1945
CAL-US	2026-07-04	false	Independence Day
CAL-USINE-LYON	2026-08-03	false	Fermeture estivale début
```

**Sans calendriers, chaque lead_time_days calculé par le moteur est faux.**

### 6.4 Coefficients de conversion UOM — table manquante

```sql
CREATE TABLE uom_conversions (
    conversion_id       UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    item_id             UUID        REFERENCES items(item_id),  -- NULL = conversion universelle
    from_uom            TEXT        NOT NULL,
    to_uom              TEXT        NOT NULL,
    conversion_factor   NUMERIC     NOT NULL CHECK (conversion_factor > 0),
                        -- to_qty = from_qty * conversion_factor
    is_bidirectional    BOOLEAN     NOT NULL DEFAULT TRUE,
    effective_start     DATE,
    effective_end       DATE,
    UNIQUE (item_id, from_uom, to_uom)  -- item_id peut être NULL pour les conversions universelles
);

-- Exemples :
-- item_id=NULL, from_uom='BOX', to_uom='EA', factor=12 → 1 BOX = 12 EA (universel)
-- item_id=SKU-001, from_uom='KG', to_uom='EA', factor=0.5 → 1 EA = 2 KG (item-spécifique)
```

**Règle d'import transactionnel :** Si `uom` du PO/CO/stock ≠ `uom` de base de l'item :
1. Cherche une conversion dans `uom_conversions` (d'abord item-spécifique, puis universelle)
2. Convertit la quantité en UOM de base avant stockage
3. Log la conversion avec les valeurs d'origine et converties
4. Si aucune conversion trouvée → REJECT `UOM_CONVERSION_MISSING`

### 6.5 Politiques de planning par Item×Location — champs à renforcer

La spec actuelle `ItemLocationPolicy` est correcte dans sa structure. Voici les champs à ajouter :

```sql
-- Ajouter à item_location_policies :
demand_horizon_days         INTEGER NOT NULL DEFAULT 90,  -- Horizon de lecture de la demande
freeze_horizon_days         INTEGER NOT NULL DEFAULT 14,  -- Horizon gelé (no-auto-reschedule)
lot_size_rounding           TEXT CHECK (lot_size_rounding IN ('up', 'down', 'nearest')) DEFAULT 'up',
service_level_target        NUMERIC DEFAULT 0.95,         -- Cible taux de service
abc_class                   TEXT CHECK (abc_class IN ('A', 'B', 'C', 'X', 'Y', 'Z')),
                                                          -- Classification ABC pour priorisation
make_or_buy                 TEXT CHECK (make_or_buy IN ('make', 'buy', 'either')) DEFAULT 'buy',
work_calendar_id            UUID REFERENCES work_calendars(calendar_id),
```

---

## 7. Ce que Kinaxis fait mal — et comment Ootils peut faire mieux

### 7.1 Garbage-in sans détection

**Ce que Kinaxis fait :** Les données arrivent via des connecteurs (SAP, Oracle) et entrent directement dans le "cube" de données. Il n'y a pas de staging zone. Les données brutes ne sont pas conservées. Si une extraction produit des valeurs aberrantes (lead times à 0, items sans UOM, POs avec quantités négatives), Kinaxis les ingère sans broncher.

**Symptôme en production :** Les planificateurs voient des Planned Orders absurdes. Le debug prend des jours parce qu'il n'y a aucun moyen de retracer "quelle extraction a causé ça".

**Ce qu'Ootils doit faire :** La staging zone (section 4) + la table `data_quality_alerts` (section 5.4). Chaque anomalie est visible, horodatée, traçable. Le planificateur voit le dashboard de qualité avant même d'ouvrir les projections.

### 7.2 Pas d'historique des master data

**Ce que Kinaxis fait :** Un upsert sur un `SupplierItem` écrase la valeur précédente. Point. Il n'y a aucun journal des changements de paramètres.

**Situation réelle vécue :** Un acheteur change le lead time d'un fournisseur de 21 à 7 jours dans SAP (erreur de saisie — il voulait mettre 17). La nuit suivante, Kinaxis ingère le 7. Les Planned Orders pour ce fournisseur avancent tous de 14 jours. Des commandes sont passées trop tôt. Deux semaines plus tard, le planificateur réalise qu'il y a un overstock. Il ne peut pas prouver que c'était une erreur de lead time car Kinaxis a écrasé la valeur.

**Ce qu'Ootils doit faire :** SCD Type 2 sur les champs critiques. Pas nécessairement sur toutes les tables — mais au minimum :

```sql
CREATE TABLE master_data_audit (
    audit_id            UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type         TEXT        NOT NULL,
    entity_id           UUID        NOT NULL,
    entity_external_id  TEXT,
    field_name          TEXT        NOT NULL,
    old_value           TEXT,
    new_value           TEXT,
    changed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    changed_by_batch_id UUID        REFERENCES staging_batches(batch_id),
    changed_by_user     TEXT
);

-- Trigger sur supplier_items pour les champs critiques
-- Champs trackés : lead_time_days, reliability_score, moq, preferred, status, effective_end
```

### 7.3 Pas de détection de chargement partiel

**Ce que Kinaxis fait :** Si une extraction SAP produit 100 lignes au lieu de 1000 (timeout, bug d'extraction, filtre mal configuré), Kinaxis ingère les 100 lignes. Pour les 900 items manquants, rien ne change dans Kinaxis — les anciennes données restent.

**Problème :** Le planificateur ne sait pas que 900 items n'ont pas été mis à jour. Il planifie avec des données potentiellement périmées de 3 jours.

**Ce qu'Ootils doit faire :** Tracking de la volumétrie par type d'entité et source :

```sql
CREATE TABLE import_volume_baselines (
    entity_type         TEXT        NOT NULL,
    source_system       TEXT        NOT NULL,
    last_known_count    INTEGER     NOT NULL,
    baseline_updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (entity_type, source_system)
);

-- Au processing : si batch_row_count < last_known_count * 0.80 → WARNING 'VOLUME_DROP_20PCT'
-- Si batch_row_count < last_known_count * 0.50 → ERROR 'VOLUME_DROP_50PCT' (bloque le processing)
```

### 7.4 Pas de dry_run sur le master data en production

**Ce que Kinaxis fait :** Il n'y a pas de mode simulation pour un import de master data. Pour tester l'impact d'un changement de paramètre, il faut soit modifier en prod soit monter un environnement sandbox complet.

**Ce qu'Ootils fait déjà :** `dry_run=true` — c'est bien. Il faut en plus un `impact_preview=true` qui montre non seulement les lignes qui seraient modifiées, mais aussi l'impact estimé sur les projections (ex: "ce changement de lead time affectera 47 Planned Orders").

### 7.5 Verrouillage des données en cours d'utilisation

**Ce que Kinaxis fait :** Pendant un recalcul du moteur, il est possible d'importer de nouvelles données. Le moteur peut utiliser un mix de données anciennes et nouvelles. Les résultats sont indéterministes.

**Ce qu'Ootils doit faire :** Soit un lock optimiste (le processing de staging attend que le moteur ait fini son cycle), soit une versioning des données par snapshot. Le staging zone aide ici : les imports s'accumulent en staging pendant le recalcul, et sont processés en batch une fois le cycle terminé.

---

## 8. Recommandations prioritaires P0/P1/P2

### P0 — À faire avant le premier POC client (bloquant)

**P0-1 : Créer la staging zone**
- Tables `staging_batches` + `staging_rows` (schéma section 4.2)
- Modifier tous les endpoints `/v1/import/*` pour écrire d'abord en staging avant de processer
- Ajouter `last_import_batch_id` sur toutes les tables master data
- Effort estimé : 3-4 jours backend

**P0-2 : Alerte lead_time_days = 0**
- Validation étape 4 du pipeline : `lead_time_days = 0` → WARNING `LEAD_TIME_ZERO`
- Champ `requires_review` flag sur `supplier_items` et `item_location_policies`
- Endpoint `GET /v1/data-quality/alerts?severity=WARNING&rule_id=LEAD_TIME_ZERO`
- Effort estimé : 1 jour

**P0-3 : Table `master_data_audit` avec triggers sur champs critiques**
- Tracker lead_time_days, reliability_score, moq, status sur supplier_items et item_location_policies
- Effort estimé : 1-2 jours

**P0-4 : Détection de volume drop**
- Comparer le nombre de lignes du batch avec le dernier import de même type
- WARNING si < 80%, ERROR bloquant si < 50%
- Effort estimé : 0.5 jour

**P0-5 : Rapport post-import de qualité**
- Après chaque batch, générer un `data_quality_report` avec les statistiques et alertes
- Retourner ce rapport dans la réponse de l'API (ou via webhook si async)
- Effort estimé : 1 jour

### P1 — Avant onboarding premier client récurrent

**P1-1 : Table `uom_conversions`**
- Schéma section 6.4
- Validation croisée UOM lors de l'import des données transactionnelles (PO, CO, On-Hand)
- Templates TSV pour les conversions standard + import via `/v1/import/uom-conversions`
- Effort estimé : 2-3 jours

**P1-2 : Entité `WorkCalendar` + exceptions**
- Schéma section 6.3
- Liaison aux locations et suppliers
- Utilisation dans le moteur pour le calcul des dates de livraison
- Effort estimé : 3-5 jours (+ intégration moteur)

**P1-3 : Validation cross-entité post-import**
- Règles CER-001 à CER-005 (section 5.3)
- Table `data_quality_alerts`
- Dashboard UI basic (liste des alertes actives, acknowledge, résolution)
- Effort estimé : 3-4 jours

**P1-4 : BOM (Bill of Materials)**
- Entité critique pour les clients manufacturing
- Schéma minimal : `bom_headers` (parent item) + `bom_lines` (component, quantity, uom, scrap_factor)
- Import TSV + endpoint API
- Effort estimé : 2-3 jours schéma + import

**P1-5 : Décomposition du lead time (sourcing + manufacturing + transit)**
- Modifier `supplier_items` pour porter les 3 composantes séparément
- Migration des `lead_time_days` existants vers `lead_time_sourcing_days` (valeur par défaut)
- Effort estimé : 1 jour

**P1-6 : SCD Type 2 complet sur ItemLocationPolicy**
- Gérer les dates d'effectivité avec SCD Type 2 (nouvelle ligne à chaque changement)
- La politique active = celle dont `effective_start <= today <= effective_end`
- Effort estimé : 2-3 jours

### P2 — Différenciateurs compétitifs post-PMF

**P2-1 : Impact preview avant import**
- Extension du `dry_run=true` pour montrer l'impact sur le moteur
- "Si vous modifiez ce lead time, X Planned Orders seront affectés"
- Effort estimé : 3-5 jours (nécessite intégration avec le moteur de simulation)

**P2-2 : Détection d'anomalies ML sur les master data**
- Détecter automatiquement les valeurs statistiquement aberrantes par rapport à l'historique
- "Ce lead time de 180 jours est 12x la moyenne de ce fournisseur — vérifiez"
- Effort estimé : 5-10 jours (modèle + pipeline)

**P2-3 : Import asynchrone avec job tracking**
- Pour les fichiers > 10 000 lignes : retourner immédiatement un `job_id`
- Polling `GET /v1/jobs/{job_id}` ou webhook sur completion
- Effort estimé : 3-4 jours (queue + worker)

**P2-4 : Réconciliation automatique inter-sources**
- Détecter les conflits quand deux sources (SAP + WMS) décrivent le même item différemment
- Règles de prédominance configurables par source et champ
- Effort estimé : 5+ jours

**P2-5 : Gouvernance des masters avec workflow d'approbation**
- Certains changements de master data (ex: lead time > 50% de variation) nécessitent une approbation avant application
- File d'attente `pending_master_changes` avec UI de validation
- Effort estimé : 5-7 jours

---

## Synthèse des gaps par criticité

| Gap | Impact | Effort | Priorité |
|-----|--------|--------|----------|
| Absence staging zone | Irréversible si bug processing | 3-4j | **P0** |
| Lead time = 0 non alerté | Ruptures non détectées | 0.5j | **P0** |
| Pas d'audit trail master data | Debug impossible | 1-2j | **P0** |
| Volume drop non détecté | Données périmées silencieuses | 0.5j | **P0** |
| UOM sans table de conversion | Quantités erronées | 2-3j | **P1** |
| Pas de calendriers opérationnels | Dates de livraison fausses | 3-5j | **P1** |
| Pas de BOM | Pas de MRP possible | 2-3j | **P1** |
| Validation cross-entité absente | Incohérences non détectées | 3-4j | **P1** |
| Pas de SCD sur policies | Historique planning perdu | 2-3j | **P1** |
| Impact preview absent | Risque modification aveugle | 3-5j | P2 |

---

## Conclusion

L'architecture actuelle est un bon point de départ — les fondamentaux (external_ids, upsert strategy, audit log d'import, human-in-the-loop) sont solides. Ce sont des décisions que Kinaxis n'a jamais prises correctement.

Mais sans staging zone et sans pipeline de qualité des données, Ootils reproduira les mêmes problèmes que Kinaxis à l'échelle. Pas parce que le moteur est mauvais — parce que les données qui l'alimentent seront mauvaises, et personne ne le verra.

La promesse d'Ootils — données de qualité, planning fiable — se joue ici, dans ces couches invisibles. Le planificateur ne voit pas la staging zone. Il voit les résultats. Si les données sont propres, les résultats sont fiables. C'est aussi simple que ça.

**Prochaines étapes immédiates recommandées :**
1. Valider ce document avec l'équipe architecture
2. Créer les tickets P0 cette semaine
3. Implémenter staging zone avant tout autre feature d'import
4. Définir le BOM schema (blocker pour les clients manufacturing)

---

*Review produite par expertise terrain — Supply Chain Planning Integration, 2026-04-05*
